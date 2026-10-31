-module(dgen_registry_member).
-behaviour(gen_statem).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("../../include/dgen_eta.hrl").
-eta_observe(
    {data, [
        member_id,
        leader,
        replica,
        synced,
        intents,
        pipeline
    ]}
).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Per-node member of a `dgen_registry`: local name cache and consistent read/write
%% proxy. Section references (`§N.N`) point to `docs/design/dgen_registry_design.md`;
%% TLA+ action names to `formal/DgenRegistryReplication.tla`.
%%
%% ## Roles
%%
%% - Follower: applies the leader's `{names_batch, …}` / `{apply_names_snapshot, …}`
%%   casts to its replica (a batch is one whole group commit, applied all or nothing),
%%   and forwards writes and consistent reads to the leader as casts correlated by a
%%   `Ref`. The loop never blocks on a call to another member: a blocking forward
%%   could deadlock against the leader's replicate-before-ack wait. All replication
%%   messages come from the leader process, so per-pair FIFO orders a snapshot before
%%   any later batch, and a batch before the reply to a write it carries.
%% - Leader: the sole writer. At handoff it reconstructs the name map from the
%%   freshest reachable replica (gathered off the loop; there is no durable name
%%   state, §4.4), serves all writes through one group-commit pipeline, monitors
%%   registered pids, and replicates each committed batch. A registration is acked
%%   `yes` only once two members hold it (§8): a forwarding follower is the second
%%   holder for free; a direct registration waits for a follower's `replicate_ack`
%%   (degrade-open on timeout unless `strict_replication`).
%%
%% With no reachable leader a write is never answered `no`; the caller blocks as an
%% intent until a leader appears or its own call times out (§3).
%%
%% ## Where things live
%%
%% - `dgen_registry_replica`: the `protected` ETS table (rows `{Name, Pid, Index, Data}`,
%%   read lock-free by callers), inverted index, applied version and epoch, and the
%%   batch/snapshot admission rules. This process is its only writer; it is not durable.
%% - `dgen_registry_intents`: requests this member accepted but cannot decide, and
%%   their re-drive rules on leader change.
%% - `dgen_registry_pipeline`: the leader's commit queue, in-flight commit, pending
%%   replicate acks, and the surrender rule for ops it cannot finish.
%% - `dgen_registry_conflicts`: release trail, kill budget, `degraded` flag (§5.6).
%% - `dgen_registry_connector`: keeps the distribution mesh in step with membership.
%% - `dgen_registry_elector`: elects the leader and asks this member to assume.
%%
%% Multi-key queries run on this member's mailbox, between batch applies, so they see a
%% whole-batch-consistent view (§6); single-key snapshot reads bypass the member.
%%
%% ## States
%%
%% A `gen_statem` whose state is a projection of data (`state_of/1`, recomputed after
%% every event by `next/2`), so it cannot drift:
%%
%%   searching   no leader known.
%%   assuming    a handoff gather is in flight (`assume_ref` armed).
%%   leader      this member leads; entering runs `assume_leadership/1`, leaving runs
%%               `relinquish_leadership/1`.
%%   follower    following a known leader.
%%
%% Gap/resync, leader reachability and the commit pipeline are deliberately data, not
%% state: each is orthogonal to role (a deposed-but-uninformed leader can still gap,
%% as the spec's RecvBcastGap has no leaderView guard).
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

%% Per-member bound on the handoff gather; a member that misses it is skipped.
-define(GATHER_TIMEOUT, 2000).
%% Pause between re-gathers while the assume gather is behind the durable version key
%% (the SafeAssume fence in the TLA+ spec); bounded overall by ?GATHER_TIMEOUT.
-define(ASSUME_CATCHUP_INTERVAL, 25).
%% Best-effort bound on pulling durable subscriptions from the elector at handoff (§4.9).
-define(SUBS_PULL_TIMEOUT, 1000).

%% Period of the maintenance pass pruning the release trail and kill budget (§5.6).
%% Trail pruning is suspended while any member is disconnected: it misses unregister
%% broadcasts entirely, so the trail must outlive the partition.
-define(PRUNE_INTERVAL, 30000).

%% How long a follower waits after requesting a resync (it observed a gap in the
%% leader's broadcast stream) before it may request again.
-define(RESYNC_RETRY, 2000).

%% Names monitored per `{monitor_sweep, …}` step (see assume_leadership/1).
-define(MONITOR_SWEEP_CHUNK, 5000).

%% Period of the leader's replication heartbeat: an empty `{names_batch, [], …}` at its
%% applied version. Without it a follower that lost the stream's tail (or its resync
%% request) would not detect the gap until the next write.
-define(REPLICA_HEARTBEAT_INTERVAL, 5000).

%% Delay before re-enqueueing (or, if deposed, surrendering) the ops of a failed batch,
%% so a persistently failing backend does not spin.
-define(REQUEUE_DELAY, 1000).

%% Bound on the per-peer aliveness probe used when adjudicating a suspected conflict.
-define(ALIVE_PROBE_TIMEOUT, 1000).

-export([start_link/2]).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    code_change/4
]).
%% State functions, called from handle_event/4 (not by gen_statem directly).
-export([
    searching/3,
    assuming/3,
    leader/3,
    follower/3
]).
%% Exported for unit tests of the batch planner.
-export([plan_batch/4]).

%% Presence subscription query, internal tagged form (see dgen_registry:query/0).
-type query() :: {'all', #{term() => term()}}.

%% Application-supplied presence subscription id (§4.9).
-type sub_id() :: term().

-record(data, {
    member_id :: dgen_registry_elector:member_id(),
    %% Discovered once after init; the `one_for_all` supervisor restarts us with the
    %% elector, so this pid is never stale.
    elector :: pid() | undefined,
    %% Backend handle for the leader's fenced version-key commit (dgen_registry_names).
    tenant :: dgen_backend:tenant(),
    tuid :: dgen_server:tuid(),
    leader :: dgen_registry_elector:member_id() | undefined,
    %% Local replica: ETS rows, inverted index, applied version, epoch.
    replica :: dgen_registry_replica:t(),
    %% Leader-only (§4.9): `#{SubId => {Watch, Notify}}`, seeded at assume and advanced
    %% by `{presence_update, …}`.
    subs = #{} :: #{sub_id() => {query(), query()}},
    %% Leader-only: names matching each watch query; diffed per commit for joined/left.
    sub_matches = #{} :: #{sub_id() => #{term() => pid()}},
    %% Leader-only: pids matching each notify query; a pid entering it is sent the
    %% current watch snapshot.
    notify_matches = #{} :: #{sub_id() => #{term() => pid()}},
    %% Peer-member monitors (all members)
    members :: #{dgen_registry_elector:member_id() => reference()},
    monitors :: #{reference() => dgen_registry_elector:member_id()},
    %% Registered-process monitors (leader only). No reverse map: each monitor is
    %% tagged `{down, Name}`, so its DOWN names the binding.
    name_to_ref :: #{term() => reference()},
    %% Requests awaiting a leader's decision; see dgen_registry_intents.
    intents = dgen_registry_intents:new() :: dgen_registry_intents:t(),
    %% Leader-only write ops not yet answered; see dgen_registry_pipeline.
    pipeline = dgen_registry_pipeline:new() :: dgen_registry_pipeline:t(),
    %% The epoch in which a fenced commit last made this member re-announce itself
    %% (rejoin_once_fenced/1), so it does so at most once per epoch.
    fence_rejoined = undefined :: undefined | non_neg_integer(),
    %% Token for our join; refreshed on nodeup so a stale
    %% `{member_down, Self, OldToken}` is discarded.
    join_token :: reference(),
    %% Peer join tokens, echoed in `{member_down, PeerId, Token}` so the elector can
    %% discard a stale DOWN.
    peer_tokens :: #{dgen_registry_elector:member_id() => reference()},
    %% Outstanding resync request timer; cleared when any snapshot arrives.
    resync_timer = undefined :: undefined | reference(),
    %% Ever synced (snapshot applied or leadership assumed)? Joins announce
    %% `fresh = not synced`; the first sync re-announces with `fresh = false`.
    synced = false :: boolean(),
    %% Tags the in-flight off-loop handoff gather (§5.7); `{assume_gathered, …}` applies
    %% only if it still matches, so a superseded gather is harmless.
    assume_ref = undefined :: undefined | reference(),
    %% Conflict repair state (§5.6); see dgen_registry_conflicts.
    conflicts = dgen_registry_conflicts:new() :: dgen_registry_conflicts:t(),
    %% Per-registry tuning options, resolved through dgen_config.
    config = #{} :: dgen_config:config()
}).

%% ---------------------------------------------------------------------------
%% Public API
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Starts the member process registered as `Name`.".
-endif.
-spec start_link(Name :: atom(), Args :: map()) -> gen_statem:start_ret().
start_link(Name, Args) ->
    gen_statem:start_link({local, Name}, ?MODULE, Args, []).

%% ---------------------------------------------------------------------------
%% gen_statem callbacks
%% ---------------------------------------------------------------------------

callback_mode() ->
    [handle_event_function, state_enter].

init(#{name := Name, tenant := Tenant, tuid := Tuid} = Args) ->
    Config = maps:get(config, Args, #{}),
    MemberId = {node(), Name},
    %% Recreated empty on every (re)start (no heir) and rebuilt from the leader's
    %% snapshot.
    Replica = dgen_registry_replica:new(dgen_registry:names_table(Name)),
    %% On `{nodeup, _}` the member re-announces its `{join}` (partition heal).
    net_kernel:monitor_nodes(true),
    %% Resolve the cached telemetry lookup now, while the tree starts: done lazily it is
    %% a synchronous `code_server` call inside whichever member emits first, which under
    %% `eta_run` simulation blocks on a process the scheduler does not own.
    _ = telemetry_available(),
    schedule_timer(?PRUNE_INTERVAL, prune_released),
    schedule_timer(?REPLICA_HEARTBEAT_INTERVAL, replica_heartbeat),
    %% Announcing needs the elector's pid; `discover_elector` runs as an internal event
    %% because `supervisor:which_children/1` would deadlock while the supervisor still
    %% waits on this init/1.
    {ok, searching,
        #data{
            member_id = MemberId,
            elector = undefined,
            tenant = Tenant,
            tuid = Tuid,
            leader = undefined,
            replica = Replica,
            members = #{},
            monitors = #{},
            name_to_ref = #{},
            join_token = make_ref(),
            peer_tokens = #{},
            config = Config
        },
        [{next_event, internal, discover_elector}]}.

%% ---------------------------------------------------------------------------
%% handle_event/4 — enter, events shared by every state, then the state functions
%% ---------------------------------------------------------------------------

%% The leadership lifecycle lives on the role edges, in the state functions' enter
%% clauses: leader/3 assumes, non_leader_enter/2 relinquishes.
handle_event(enter, OldState, State, Data) ->
    apply(?MODULE, State, [enter, OldState, Data]);
%% -- calls ------------------------------------------------------------------

%% Readiness probe (dgen_registry:await_ready/2): a leader is known and state synced.
handle_event({call, From}, ready, _State, Data = #data{leader = Leader, synced = Synced}) ->
    next(Data, [{reply, From, Leader =/= undefined andalso Synced}]);
%% Status probe (dgen_registry:status/1). Reports this member's own *belief* about
%% leadership, which can lag the elector's committed view during a handoff (§5.1).
handle_event({call, From}, status, _State, Data) ->
    #data{member_id = Self, leader = Leader, synced = Synced} = Data,
    Status = #{
        member_id => Self,
        leader => Leader,
        is_leader => Leader =:= Self,
        epoch => epoch(Data),
        synced => Synced,
        applied_version => applied_version(Data)
    },
    next(Data, [{reply, From, Status}]);
%% Routing (leader / follower / no leader) is in route_register/5; the only immediate
%% reply is an adjudicated `no`.
handle_event({call, From}, {register, LogicalName, Pid, Meta}, _State, Data) ->
    next(route_register(LogicalName, Pid, Meta, From, Data), []);
handle_event({call, From}, {unregister, LogicalName}, _State, Data) ->
    {noreply, Data1} = route_unregister(LogicalName, From, Data),
    next(Data1, []);
%% Indexed query (on the mailbox, so batch-consistent, §6).
handle_event({call, From}, {query, Constraints}, _State, Data = #data{replica = R}) ->
    next(Data, [{reply, From, dgen_registry_replica:query_constraints(R, Constraints)}]);
%% Snapshot reads are served lock-free by the caller (`dgen_registry:whereis_name/1`).
%%
%% Handoff gather (any member): our records, applied version and release trail, for a
%% new leader's freshest-wins reconstruction (§5.7 / §4.4) and trail merge (§5.6).
handle_event({call, From}, get_names_snapshot, _State, Data = #data{replica = R, conflicts = C}) ->
    %% Shipped as an off-heap binary; see dgen_registry_replica:encode/1.
    Released = dgen_registry_conflicts:trail(C),
    Reply = {dgen_registry_replica:encode(R), dgen_registry_replica:version(R), Released},
    next(Data, [{reply, From, Reply}]);
%% -- casts ------------------------------------------------------------------

%% Follower: a forwarded unregister committed (FIFO: after its {names_batch}).
handle_event(cast, {unregister_reply, Ref}, _State, Data) ->
    next(intent_reply(Ref, unregister, Data), []);
%% Follower: FIFO puts the matching {names_batch} ahead of this, so read-after-write
%% holds for a local get_metadata.
handle_event(cast, {set_meta_reply, Ref, Result}, _State, Data) ->
    next(intent_reply(Ref, {set_meta, Result}, Data), []);
%% Follower: a forwarded registration's verdict at commit Version. A `yes` is
%% version-guarded (§5.5): answered only once our replica has applied Version, so we
%% are a second holder the handoff gather (§5.7) is guaranteed to see. Normally true
%% (FIFO: the batch precedes the reply); if the batch was gap-refused the ack waits
%% for the resync. See dgen_registry_intents.
handle_event(cast, {register_reply, Ref, Result, Version}, _State, Data) ->
    next(intent_reply(Ref, {register, Result, Version}, Data), []);
%% Rolling upgrade: unversioned reply from a pre-version leader; `legacy`, not 0 (see
%% dgen_registry_intents:on_reply/4).
handle_event(cast, {register_reply, Ref, Result}, _State, Data) ->
    next(intent_reply(Ref, {register, Result, legacy}, Data), []);
%% Follower: confirm we hold a batch's bindings (FIFO: after its {names_batch}).
%% Version-guarded: if we have not applied that version (gap, stale epoch) we must not
%% claim to hold them.
handle_event(
    cast, {replicate_sync, BatchRef, LeaderId, Version}, _State, Data = #data{member_id = Self}
) ->
    case applied_version(Data) >= Version of
        true -> cast_to_member(LeaderId, {replicate_ack, BatchRef, Self});
        false -> ok
    end,
    next(Data, []);
%% Leader: once `register_replicas` distinct followers ack, answer the batch's direct
%% registrations `yes`.
handle_event(cast, {replicate_ack, BatchRef, FollowerId}, _State, Data = #data{pipeline = P}) ->
    case dgen_registry_pipeline:ack(BatchRef, FollowerId, P) of
        {done, Direct, TimerRef, P1} ->
            erlang:cancel_timer(TimerRef),
            deliver_direct(Direct),
            next(Data#data{pipeline = P1}, []);
        {waiting, P1} ->
            next(Data#data{pipeline = P1}, [])
    end;
%% Replication broadcast: one message per committed batch. dgen_registry_replica
%% admits it only if contiguous (PrevV), so our replica stays a prefix of the leader's
%% stream; a gap requests a resync. Applying it as a unit keeps "same applied_version
%% implies same content" true (see broadcast_batch/5).
handle_event(
    cast, {names_batch, Ops, Epoch, PrevV, Version, LeaderId}, _State, Data = #data{replica = R}
) ->
    Data1 =
        case dgen_registry_replica:offer_batch(Epoch, PrevV, Version, Ops, R) of
            {applied, R1} ->
                intents_advanced(record_released(Ops, cancel_resync(Data#data{replica = R1})));
            gap ->
                request_resync(LeaderId, Data);
            _StaleEpochOrDuplicate ->
                Data
        end,
    next(Data1, []);
%% Leadership snapshot from the new leader: replica, leader transition and member
%% monitors apply in one cast, so nothing interleaves. The version re-baselines us.
handle_event(
    cast,
    {apply_names_snapshot, RecordsPayload, NewLeader, ExtraMembers, Tokens, Epoch, Version},
    _State,
    Data = #data{member_id = Self, leader = OldLeader, replica = R}
) ->
    %% Version-monotonic, not merely epoch-guarded: a late snapshot can never roll the
    %% replica backward (the handoff-gather race in the TLA+ spec). A refused one is
    %% harmless; gap/resync re-baselines us.
    Records = dgen_registry_replica:decode_records(RecordsPayload),
    case dgen_registry_replica:offer_snapshot(Epoch, Version, Records, R) of
        {accepted, R1} ->
            %% Replica before the leader transition: assume_leadership reads the
            %% names back from the table.
            Data0 = cancel_resync(Data#data{replica = R1}),
            Data1 = do_leader_changed(NewLeader, OldLeader, Self, Data0),
            %% Supersedes any in-flight assume gather.
            Data2 = Data1#data{assume_ref = undefined},
            Data3 = add_member_monitors(ExtraMembers, Data2),
            Data4 = merge_peer_tokens(Tokens, Data3),
            next(
                maybe_start_commit(redrive_intents(moved, intents_advanced(mark_synced(Data4)))),
                []
            );
        stale ->
            next(Data, [])
    end;
%% Leader fast path onboarding a joiner: an up-to-date follower only monitors it and
%% records its token (so a later DOWN is fenced with the elector's token).
handle_event(cast, {peer_joined, MemberId, Tokens}, _State, Data) ->
    next(merge_peer_tokens(Tokens, add_member_monitors([MemberId], Data)), []);
%% -- infos ------------------------------------------------------------------

%% Too few replica acks for these direct registrations in time (§8): degrade open
%% (ack `yes` leader-only, telemetry) or, with `strict_replication`, fail closed
%% (reject and retract).
handle_event(
    info, {replicate_timeout, BatchRef}, _State, Data = #data{pipeline = P, config = Config}
) ->
    case dgen_registry_pipeline:ack_timeout(BatchRef, P) of
        {expired, Direct, Needed, Got, P1} ->
            Data1 = Data#data{pipeline = P1},
            case dgen_config:strict_replication(Config) of
                false ->
                    notify_degrade_open(Needed, Got, length(Direct)),
                    deliver_direct(Direct),
                    next(Data1, []);
                true ->
                    next(reject_and_retract_direct(Direct, Data1), [])
            end;
        none ->
            next(Data, [])
    end;
handle_event(info, {dgen_transaction, Ref, Reply}, _State, Data = #data{pipeline = P}) ->
    case dgen_registry_pipeline:finish(Ref, P) of
        {ok, Plan, MRef, P1} ->
            %% The result precedes the worker's DOWN, so flush drops it.
            erlang:demonitor(MRef, [flush]),
            Data1 = commit_result(Reply, Plan, Data#data{pipeline = P1}),
            next(maybe_start_commit(Data1), []);
        stale ->
            next(Data, [])
    end;
%% A registered process died (leader only). The DOWN carries Ref so the plan's
%% ref-match guard ignores a stale DOWN for a name already re-bound. If we no longer
%% lead, the new leader (which monitors the pid itself) drives the removal.
handle_event(info, {{down, LogicalName}, Ref, process, _Pid, _Reason}, _State, Data) ->
    next(enqueue_op({down, LogicalName, Ref}, Data), []);
handle_event(info, {'DOWN', Ref, process, _Pid, _Reason}, _State, Data) ->
    #data{
        monitors = Monitors,
        elector = Elector,
        member_id = Self
    } = Data,
    case maps:get(Ref, Monitors, undefined) of
        undefined ->
            %% Not a peer monitor: the commit worker, or a late straggler.
            case dgen_registry_pipeline:worker_down(Ref, Data#data.pipeline) of
                {ok, Plan, P1} ->
                    Data1 = Data#data{pipeline = P1},
                    next(maybe_start_commit(surrender(failed, maps:get(ops, Plan), Data1)), []);
                stale ->
                    next(Data, [])
            end;
        Self ->
            next(Data, []);
        DeadMemberId ->
            %% Fence {member_down} with the elector's *current* token for the peer, not
            %% our cached one, which a dropped snapshot may have left stale (a stale
            %% token would strand a dead member and block failover, §5.7). The read runs
            %% in a helper, never inline: a call to the elector could deadlock against
            %% its own `{elector_assume_and_distribute}` call into this member.
            Cached = maps:get(DeadMemberId, Data#data.peer_tokens, undefined),
            spawn_member_down(Elector, DeadMemberId, Cached),
            next(remove_member(DeadMemberId, Data), [])
    end;
handle_event(info, {nodeup, _Node}, _State, Data = #data{elector = Elector, member_id = Self}) ->
    %% We may have been removed during a partition: re-announce with a fresh token
    %% so a queued stale {member_down, Self, OldToken} is discarded.
    NewToken = make_ref(),
    dgen_registry_elector:join(Elector, Self, NewToken, not Data#data.synced),
    %% Partition heal: if our leader is reachable, re-drive intents now (idempotent)
    %% so a held or swallowed unregister is never lost (Non-goal 5), without waiting
    %% for the re-onboarding snapshot.
    Data1 = Data#data{join_token = NewToken},
    case intent_route(Data1) of
        none -> next(Data1, []);
        _Reachable -> next(redrive_intents(all, Data1), [])
    end;
%% Periodic maintenance: prune the release trail and kill budget (§5.6) and stale
%% intents.
handle_event(
    info, prune_released, _State, Data = #data{conflicts = C, members = Members, config = Config}
) ->
    schedule_timer(?PRUNE_INTERVAL, prune_released),
    Now = erlang:system_time(millisecond),
    AllConnected = lists:all(
        fun({Node, _Name}) -> dgen_utils:node_reachable(Node) end,
        maps:keys(Members)
    ),
    C1 = dgen_registry_conflicts:prune(Now, AllConnected, Config, C),
    next(prune_intents(Data#data{conflicts = C1}), []);
%% Unanswered resync request: clear the guard so the next gap (or heartbeat) retries.
handle_event(info, resync_timeout, _State, Data) ->
    next(Data#data{resync_timer = undefined}, []);
%% From the connector: the durable election epoch is ahead of ours, so a handoff never
%% reached us. Re-announcing triggers a fresh assume/fan-out.
handle_event(info, {durable_epoch, DurableEpoch}, _State, Data) when is_integer(DurableEpoch) ->
    case DurableEpoch > epoch(Data) of
        true ->
            #data{elector = Elector, member_id = Self, join_token = Token, synced = Synced} =
                Data,
            dgen_registry_elector:join(Elector, Self, Token, not Synced);
        false ->
            ok
    end,
    next(Data, []);
handle_event(info, {durable_epoch, _DurableEpoch}, _State, Data) ->
    next(Data, []);
%% -- internal ---------------------------------------------------------------

%% Safe here: the supervisor is no longer blocked on our init/1 (see init/1).
handle_event(
    internal,
    discover_elector,
    _State,
    Data = #data{member_id = {_Node, Name} = MemberId, join_token = Token}
) ->
    Elector = dgen_registry:elector_pid(Name),
    true = is_pid(Elector),
    %% `fresh = true`: never synced, so this member cannot be hiding any bindings.
    dgen_registry_elector:join(Elector, MemberId, Token, true),
    next(Data#data{elector = Elector}, []);
%% -- everything else is the current state's ----------------------------------

handle_event(EventType, EventContent, State, Data) ->
    apply(?MODULE, State, [EventType, EventContent, Data]).

%% `assuming` wins: a mid-assume member clears its leader view on purpose.
state_of(#data{assume_ref = Ref}) when Ref =/= undefined -> assuming;
state_of(#data{leader = undefined}) -> searching;
state_of(#data{leader = Self, member_id = Self}) -> leader;
state_of(#data{}) -> follower.

%% A same-state `next_state` keeps timeouts and fires no enter call, so deriving on
%% every event is harmless.
next(Data, Actions) ->
    {next_state, state_of(Data), Data, Actions}.

epoch(#data{replica = R}) -> dgen_registry_replica:epoch(R).
applied_version(#data{replica = R}) -> dgen_registry_replica:version(R).

%% ---------------------------------------------------------------------------
%% State functions
%% ---------------------------------------------------------------------------
%%
%% Called from handle_event/4 for the enter call and for every event it does not
%% handle itself. Because the state is derived from data, `leader` is exactly
%% `leader = member_id`, `follower` a known other leader, and `searching`/`assuming`
%% no leader (`assume_ref` is only ever armed alongside `leader = undefined`).
%%
%% Fallthrough: searching and assuming -> leaderless/3 -> non_leader/3 -> unhandled/3;
%% follower -> non_leader/3; leader -> unhandled/3.

%% -- leader -----------------------------------------------------------------

%% The assume/relinquish pairing lives on the role edges so no transition path can
%% forget half of it. Deliberately not here: redrive_intents (fires on non-edge
%% leadership events too) and fail_pending_acks (stays in do_leader_changed; a
%% re-assumed leader's stale acks are already fenced by BatchRef).
leader(enter, _OldState, Data) ->
    {keep_state, assume_leadership(Data)};
%% A continuing leader absorbs a join on the fast path; otherwise a genuine change.
%% The epoch guard stays on Data: the `leader` state does not say which term.
leader(
    {call, From},
    {elector_assume_and_distribute, MemberId, AllIds, Tokens, FreshIds, Epoch},
    Data = #data{synced = Synced}
) ->
    {reply, Reply, Data1} =
        case Synced andalso MemberId =/= undefined andalso epoch(Data) =:= Epoch of
            true -> assume_fast_path(MemberId, AllIds, Tokens, FreshIds, Epoch, Data);
            false -> assume_genuine(MemberId, AllIds, Tokens, FreshIds, Epoch, Data)
        end,
    next(Data1, [{reply, From, Reply}]);
leader({call, From}, {set_metadata, LogicalName, Index, MetaData}, Data) ->
    next(enqueue_op({set_meta, LogicalName, Index, MetaData, {meta, From}}, Data), []);
%% Consistent reads are fenced (Guarantee 5): the value is captured now, and a spawned
%% helper verifies leadership against the durable leader key before replying, so a
%% deposed leader cannot answer authoritatively.
leader({call, From}, {get_metadata, LogicalName}, Data = #data{replica = R}) ->
    reply_fenced(From, dgen_registry_replica:lookup_metadata(R, LogicalName), undefined, Data),
    next(Data, []);
leader({call, From}, {query_consistent, Constraints}, Data = #data{replica = R}) ->
    reply_fenced(From, dgen_registry_replica:query_constraints(R, Constraints), [], Data),
    next(Data, []);
leader({call, From}, {whereis, LogicalName}, Data = #data{replica = R}) ->
    reply_fenced(From, dgen_registry_replica:lookup_pid(R, LogicalName), undefined, Data),
    next(Data, []);
%% A forwarded registration, answered via {register_reply} once committed.
leader(cast, {register_req, Ref, FollowerId, LogicalName, Pid, Meta}, Data) ->
    case reject_new_when_degraded(LogicalName, Data) of
        true ->
            cast_to_member(FollowerId, {register_reply, Ref, no}),
            next(Data, []);
        false ->
            next(enqueue_op({add, LogicalName, Pid, Meta, {forward, FollowerId, Ref}}, Data), [])
    end;
%% A forwarded unregister, answered via {unregister_reply} once committed.
leader(cast, {unregister_req, Ref, FollowerId, LogicalName}, Data = #data{replica = R}) ->
    ReleasedPid = dgen_registry_replica:lookup_pid(R, LogicalName),
    next(
        enqueue_op(
            {remove, LogicalName, ReleasedPid, {forward_unreg, FollowerId, Ref}},
            row_delete(Data, LogicalName)
        ),
        []
    );
%% A forwarded set_metadata, answered via {set_meta_reply} once committed.
leader(cast, {set_meta_req, Ref, FollowerId, LogicalName, Index, MetaData}, Data) ->
    next(
        enqueue_op(
            {set_meta, LogicalName, Index, MetaData, {forward_meta, FollowerId, Ref}}, Data
        ),
        []
    );
%% Forwarded consistent reads answer the caller's `From` directly, fenced.
leader(cast, {get_metadata_req, LogicalName, From}, Data = #data{replica = R}) ->
    reply_fenced(From, dgen_registry_replica:lookup_metadata(R, LogicalName), undefined, Data),
    next(Data, []);
leader(cast, {query_req, Constraints, From}, Data = #data{replica = R}) ->
    reply_fenced(From, dgen_registry_replica:query_constraints(R, Constraints), [], Data),
    next(Data, []);
leader(cast, {whereis_req, LogicalName, From}, Data = #data{replica = R}) ->
    reply_fenced(From, dgen_registry_replica:lookup_pid(R, LogicalName), undefined, Data),
    next(Data, []);
%% Answer a resync request with a snapshot at our epoch and applied version.
leader(cast, {resync_req, FollowerId}, Data = #data{leader = Leader, replica = R}) ->
    RecordsBin = dgen_registry_replica:encode(R),
    cast_to_member(
        FollowerId,
        {apply_names_snapshot, RecordsBin, Leader, [], #{}, dgen_registry_replica:epoch(R),
            dgen_registry_replica:version(R)}
    ),
    next(Data, []);
%% A pid-guarded retract (strict_replication fail-closed, §8) forwarded by a member
%% that could not commit it, so a binding answered `no` cannot live on. The pid guard
%% never clobbers a name since re-registered.
leader(cast, {retract_req, LogicalName, Pid}, Data) ->
    next(enqueue_op({retract, LogicalName, Pid}, Data), []);
%% Rolling upgrade: legacy fire-and-forget unregister casts from pre-call nodes.
leader(cast, {unregister, LogicalName}, Data = #data{replica = R}) ->
    %% Capture the bound pid before the optimistic delete, or plan_op/3 could not
    %% tell a real unregister from a no-op.
    ReleasedPid = dgen_registry_replica:lookup_pid(R, LogicalName),
    next(
        enqueue_op({remove, LogicalName, ReleasedPid, undefined}, row_delete(Data, LogicalName)),
        []
    );
%% Presence subscription delta from the elector (§4.9); a stale push is dropped (the
%% next leader is reseeded on assume).
leader(cast, {presence_update, Update, Epoch}, Data) ->
    case Epoch >= epoch(Data) of
        true -> next(apply_presence_update(Update, Data), []);
        false -> next(Data, [])
    end;
%% Fast-path continuation for a rejoining (non-fresh) member: adjudicate its replica
%% against our authoritative table (§5.6), merge its trail, then onboard it. A killed
%% authority's DOWN drives the normal cleanup. Applies only in the term that spawned it.
leader(
    info,
    {joiner_gathered, MemberId, AllIds, Tokens, Epoch, Result},
    Data = #data{members = Members}
) when is_map_key(MemberId, Members) ->
    case epoch(Data) =:= Epoch of
        true ->
            Data1 =
                case Result of
                    {ok, {Records, _Version, Released}, Alive} ->
                        %% The resolved map is discarded: repair only kills and alarms.
                        OwnRecords = dgen_registry_replica:records(Data#data.replica),
                        {_Records, D1} = repair_conflicts(
                            OwnRecords, [Records], [Released], Alive, Data
                        ),
                        D1;
                    error ->
                        %% Unanswered: onboard anyway; divergence surfaces at the next
                        %% gather.
                        Data
                end,
            next(onboard_joiner(MemberId, AllIds, Tokens, Data1), []);
        false ->
            {keep_state, Data}
    end;
%% Deferred monitor sweep (see assume_leadership/1), one chunk per message, only in
%% the term that armed it. A name already monitored (committed since the sweep began)
%% or re-bound since its chunk was selected is skipped. A pid that died before its
%% chunk fires an immediate `noproc` DOWN, so deferral loses nothing.
leader(info, {monitor_sweep, Epoch, Cont0}, Data = #data{replica = R}) ->
    case epoch(Data) =:= Epoch of
        true -> next(monitor_sweep(Epoch, Cont0, R, Data), []);
        false -> {keep_state, Data}
    end;
%% Retry a failed batch's ops while we still lead, dropping registrations whose caller
%% died.
leader(info, {requeue_ops, Ops}, Data) ->
    Live = [Op || Op <- Ops, caller_alive(Op)],
    next(maybe_start_commit(lists:foldl(fun enqueue_op/2, Data, Live)), []);
%% Everyone keeps the timer running so a later election heartbeats immediately.
leader(info, replica_heartbeat, Data) ->
    broadcast_heartbeat(Data),
    schedule_timer(?REPLICA_HEARTBEAT_INTERVAL, replica_heartbeat),
    {keep_state, Data};
leader(EventType, EventContent, Data) ->
    unhandled(EventType, EventContent, Data).

%% -- follower ---------------------------------------------------------------

follower(enter, OldState, Data) ->
    non_leader_enter(OldState, Data);
%% Consistent reads: the leader answers `From` directly so this loop never blocks on
%% it; an unreachable leader is answered now.
follower({call, From}, {get_metadata, LogicalName}, Data = #data{leader = Leader}) ->
    forward_read(Leader, {get_metadata_req, LogicalName, From}, From, undefined, Data);
follower({call, From}, {query_consistent, Constraints}, Data = #data{leader = Leader}) ->
    forward_read(Leader, {query_req, Constraints, From}, From, [], Data);
follower({call, From}, {whereis, LogicalName}, Data = #data{leader = Leader}) ->
    forward_read(Leader, {whereis_req, LogicalName, From}, From, undefined, Data);
follower(cast, {retract_req, LogicalName, Pid}, Data = #data{leader = Leader}) ->
    cast_to_member(Leader, {retract_req, LogicalName, Pid}),
    next(Data, []);
follower(cast, {unregister, LogicalName}, Data = #data{leader = Leader}) ->
    cast_to_member(Leader, {unregister, LogicalName}),
    next(row_delete(Data, LogicalName), []);
follower(EventType, EventContent, Data) ->
    non_leader(EventType, EventContent, Data).

%% -- searching --------------------------------------------------------------

searching(enter, OldState, Data) ->
    non_leader_enter(OldState, Data);
searching(EventType, EventContent, Data) ->
    leaderless(EventType, EventContent, Data).

%% -- assuming ---------------------------------------------------------------

%% A leader entering `assuming` relinquishes at assume start; a death in the gather
%% window is caught by the next sweep's immediate `noproc` DOWN.
assuming(enter, OldState, Data) ->
    non_leader_enter(OldState, Data);
%% The off-loop handoff gather returned, for the assume that armed it (the ref);
%% otherwise it was superseded and is dropped. The freshest map is the reconstructed
%% state: broadcasts are totally ordered and every replica is a prefix of the stream.
%% MaxVersion re-baselines the followers.
assuming(
    info,
    {assume_gathered, {MemberId, AllIds, Tokens, FreshIds, Epoch, Ref}, SelfRecords,
        FreshestRecords, {MaxVersion, Frontier}, PeerResults, Subs, Alive},
    Data = #data{member_id = Self, assume_ref = Ref}
) ->
    OtherIds = lists:delete(Self, AllIds),
    Peers = maps:values(PeerResults),
    Degraded = dgen_registry_conflicts:degraded(
        MaxVersion, Frontier, OtherIds, FreshIds, PeerResults
    ),
    %% Repair the conflicts the gather exposes (§5.6): conflicted names are dropped.
    {Records, Data0} = repair_conflicts(
        FreshestRecords,
        [SelfRecords | [Recs || {Recs, _V, _Rel} <- Peers]],
        [Rel || {_Recs, _V, Rel} <- Peers],
        Alive,
        Data
    ),
    %% Clearing assume_ref enters `leader`, whose enter call runs assume_leadership
    %% against the reconstructed replica and `Subs` (§4.9).
    Data2 = Data0#data{
        replica = dgen_registry_replica:adopt(Epoch, MaxVersion, Records, Data0#data.replica),
        leader = Self,
        conflicts = dgen_registry_conflicts:set_degraded(Degraded, Data0#data.conflicts),
        assume_ref = undefined,
        subs = Subs
    },
    Data3 = add_member_monitors(extra_member_ids(MemberId, AllIds, Self), Data2),
    Data4 = merge_peer_tokens(Tokens, Data3),
    %% Encode once; every cast shares the refc binary.
    RecordsBin = dgen_registry_replica:encode_records(Records),
    lists:foreach(
        fun(Id) ->
            cast_to_member(
                Id,
                {apply_names_snapshot, RecordsBin, Self, extra_member_ids(MemberId, AllIds, Id),
                    Tokens, Epoch, MaxVersion}
            )
        end,
        OtherIds
    ),
    %% Re-drive every unresolved intent into our own pipeline.
    next(maybe_start_commit(redrive_intents(moved, mark_synced(cancel_resync(Data4)))), []);
%% A forwarded registration reaching a member mid-assume is postponed until the assume
%% resolves; if superseded, it stays silent and the forwarder re-drives.
assuming(cast, {register_req, _Ref, _FollowerId, _Name, _Pid, _Meta}, Data) ->
    {keep_state, Data, [postpone]};
assuming(EventType, EventContent, Data) ->
    leaderless(EventType, EventContent, Data).

%% -- shared by the non-leader states ----------------------------------------

non_leader_enter(leader, Data) ->
    {keep_state, relinquish_leadership(Data)};
non_leader_enter(_OldState, Data) ->
    {keep_state, Data}.

%% `searching` and `assuming`: no leader to forward to.
leaderless({call, From}, {get_metadata, _LogicalName}, Data) ->
    next(Data, [{reply, From, undefined}]);
leaderless({call, From}, {query_consistent, _Constraints}, Data) ->
    next(Data, [{reply, From, []}]);
leaderless({call, From}, {whereis, _LogicalName}, Data) ->
    next(Data, [{reply, From, undefined}]);
%% No leader (likely mid-handoff): hold the retract as an answered unregister intent so
%% it is re-driven rather than lost.
leaderless(cast, {retract_req, LogicalName, Pid}, Data) ->
    next(submit_intent({unregister, LogicalName, Pid}, answered, Data), []);
leaderless(EventType, EventContent, Data) ->
    non_leader(EventType, EventContent, Data).

%% `searching`, `assuming` and `follower`.
non_leader(
    {call, From},
    {elector_assume_and_distribute, MemberId, AllIds, Tokens, FreshIds, Epoch},
    Data
) ->
    {reply, Reply, Data1} = assume_genuine(MemberId, AllIds, Tokens, FreshIds, Epoch, Data),
    next(Data1, [{reply, From, Reply}]);
%% Forward as an intent. The reply routes back through this member so the leader's
%% `{names_batch}` (FIFO, ahead of it) has updated our row first: read-after-write for
%% a local snapshot read.
non_leader({call, From}, {set_metadata, LogicalName, Index, MetaData}, Data) ->
    next(submit_intent({set_meta, LogicalName, Index, MetaData}, From, Data), []);
%% Stale belief that we lead: reject so the caller retries.
non_leader(cast, {set_meta_req, Ref, FollowerId, _LogicalName, _Index, _MetaData}, Data) ->
    cast_to_member(FollowerId, {set_meta_reply, Ref, {error, no_leader}}),
    next(Data, []);
%% Stale belief that we lead: answer the empty value; the caller can retry.
non_leader(cast, {get_metadata_req, _LogicalName, From}, Data) ->
    gen_server:reply(From, undefined),
    next(Data, []);
non_leader(cast, {query_req, _Constraints, From}, Data) ->
    gen_server:reply(From, []),
    next(Data, []);
non_leader(cast, {whereis_req, _LogicalName, From}, Data) ->
    gen_server:reply(From, undefined),
    next(Data, []);
%% Deposed: surrender a failed batch's ops (dgen_registry_pipeline:surrender/2).
non_leader(info, {requeue_ops, Ops}, Data) ->
    next(surrender(deposed, Ops, Data), []);
non_leader(info, replica_heartbeat, Data) ->
    schedule_timer(?REPLICA_HEARTBEAT_INTERVAL, replica_heartbeat),
    {keep_state, Data};
non_leader(EventType, EventContent, Data) ->
    unhandled(EventType, EventContent, Data).

%% Ignored in this state. A non-leader stays silent on leader-only protocol messages
%% (`register_req`, `unregister_req`, `resync_req`, `presence_update`) rather than give
%% a non-adjudicated answer (§3); the forwarder re-drives. Superseded gather results
%% and stale sweep steps land here too.
unhandled({call, From}, _Request, Data) ->
    next(Data, [{reply, From, {error, unknown_call}}]);
unhandled(_EventType, _EventContent, Data) ->
    next(Data, []).

forward_read(Leader, Req, From, Default, Data) ->
    case member_reachable(Leader) of
        true ->
            cast_to_member(Leader, Req),
            next(Data, []);
        false ->
            next(Data, [{reply, From, Default}])
    end.

%% ---------------------------------------------------------------------------
%% Elector calls (dispatched from the state functions)
%% ---------------------------------------------------------------------------

%% Bodies of `{elector_assume_and_distribute, …}`. `MemberId` is the joiner that
%% triggered it (`undefined` for a member_down); `AllIds` the full member list.
%%
%% Fast path: a continuing, synced leader for this epoch is authoritative (every
%% follower is a prefix of its stream), so it skips the gather and re-snapshotting
%% the followers; it only monitors and onboards the joiner.
assume_fast_path(MemberId, AllIds, Tokens, FreshIds, Epoch, Data = #data{member_id = Self}) ->
    Data1 = merge_peer_tokens(
        Tokens, add_member_monitors(extra_member_ids(MemberId, AllIds, Self), Data)
    ),
    %% A fresh joiner holds no bindings: onboard now. A rejoining previously-synced
    %% member may hold divergent bindings (§5.6 partition heal), so gather its replica
    %% off the loop and adjudicate it in {joiner_gathered} first (best-effort).
    case MemberId =:= Self orelse lists:member(MemberId, FreshIds) of
        true ->
            {reply, ok, onboard_joiner(MemberId, AllIds, Tokens, Data1)};
        false ->
            spawn_joiner_gather(MemberId, AllIds, Tokens, Epoch, Data1),
            {reply, ok, Data1}
    end.

assume_genuine(MemberId, AllIds, Tokens, FreshIds, Epoch, Data = #data{member_id = Self}) ->
    %% Genuine change: the freshest-wins gather is a network fan-out, so it runs off
    %% the loop and the elector is answered `ok` now (an inline gather would time out
    %% its call and stall queued membership changes). Our own records and version are
    %% captured now so a broadcast landing mid-gather cannot skew freshest-wins.
    SelfRecords = dgen_registry_replica:records(Data#data.replica),
    OtherIds = lists:delete(Self, AllIds),
    Ref = make_ref(),
    spawn_assume_gather(
        {MemberId, AllIds, Tokens, FreshIds, Epoch, Ref},
        SelfRecords,
        applied_version(Data),
        OtherIds,
        Data#data.elector,
        self(),
        Data#data.tenant,
        Data#data.tuid,
        Data#data.conflicts
    ),
    %% No leader for the window: writes block and are re-driven by the continuation
    %% rather than forwarded to the old, possibly-dead leader.
    {reply, ok, Data#data{assume_ref = Ref, leader = undefined}}.

apply_presence_update({subscribe, SubId, Watch, Notify}, Data = #data{subs = Subs}) ->
    seed_subscription(SubId, Data#data{subs = Subs#{SubId => {Watch, Notify}}});
apply_presence_update(
    {unsubscribe, SubId}, Data = #data{subs = Subs, sub_matches = SM, notify_matches = NM}
) ->
    Data#data{
        subs = maps:remove(SubId, Subs),
        sub_matches = maps:remove(SubId, SM),
        notify_matches = maps:remove(SubId, NM)
    };
apply_presence_update(unsubscribe_all, Data) ->
    Data#data{subs = #{}, sub_matches = #{}, notify_matches = #{}}.

%% One chunk of the deferred monitor sweep; re-sends itself until the table is done.
monitor_sweep(Epoch, Cont0, R, Data) ->
    Tab = dgen_registry_replica:tab(R),
    Select =
        case Cont0 of
            start ->
                ets:select(
                    Tab, [{{'$1', '$2', '_', '_'}, [], [{{'$1', '$2'}}]}], ?MONITOR_SWEEP_CHUNK
                );
            _ ->
                ets:select(Cont0)
        end,
    case Select of
        '$end_of_table' ->
            Data;
        {Pairs, Cont1} ->
            NTR1 =
                lists:foldl(
                    fun({Name, Pid}, Acc) ->
                        case
                            is_map_key(Name, Acc) orelse
                                dgen_registry_replica:lookup_pid(R, Name) =/= Pid
                        of
                            true -> Acc;
                            false -> Acc#{Name => monitor_name(Name, Pid)}
                        end
                    end,
                    Data#data.name_to_ref,
                    Pairs
                ),
            self() ! {monitor_sweep, Epoch, Cont1},
            Data#data{name_to_ref = NTR1}
    end.

terminate(_Reason, _StatemState, _Data) ->
    net_kernel:monitor_nodes(false),
    ok.

code_change(_OldVsn, StatemState, Data, _Extra) ->
    {ok, StatemState, Data}.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

do_leader_changed(NewLeader, OldLeader, Self, Data) ->
    %% Forwarded intents are re-routed by the caller's redrive_intents/2; the
    %% assume/relinquish halves run in the state enter calls.
    if
        OldLeader =:= Self, NewLeader =/= Self ->
            %% Lost leadership: resolve direct registrations awaiting replica acks
            %% so their timers do not fire into a follower.
            fail_pending_acks(Data#data{leader = NewLeader});
        true ->
            Data#data{leader = NewLeader}
    end.

%% Registered-process monitors are not created inline but by chunked, epoch-fenced
%% {monitor_sweep, …} self-sends that interleave with live traffic. Safe because a
%% monitor on an already-dead (or unreachable) pid fires an immediate DOWN, so a death
%% before its chunk is caught, never lost; dead-pid cleanup has no promptness bound.
assume_leadership(Data) ->
    self() ! {monitor_sweep, epoch(Data), start},
    %% Seed watch sets so the first commit's deltas are computed correctly (§4.9).
    recompute_sub_matches(Data).

%% Rebuild every subscription's watch and notify sets from the replica (on assume
%% only; apply_committed_plan maintains them after). A silent reseed: no snapshots.
recompute_sub_matches(Data = #data{subs = Subs, replica = R}) ->
    SM = maps:map(
        fun(_SubId, {Watch, _Notify}) -> dgen_registry_replica:query_match_names(R, Watch) end,
        Subs
    ),
    NM = maps:map(
        fun(_SubId, {_Watch, Notify}) -> dgen_registry_replica:query_match_names(R, Notify) end,
        Subs
    ),
    Data#data{sub_matches = SM, notify_matches = NM}.

%% Demonitor all registered pids; keep the replica for snapshot reads. The pipeline's
%% cached commit version is dropped so it cannot pin a later term's read version.
relinquish_leadership(Data = #data{name_to_ref = NTR}) ->
    maps:foreach(
        fun(_LogicalName, Ref) ->
            erlang:demonitor(Ref, [flush])
        end,
        NTR
    ),
    Data#data{
        name_to_ref = #{}, pipeline = dgen_registry_pipeline:new_term(Data#data.pipeline)
    }.

%% Freshest-wins gather across our replica and every reachable peer. Broadcasts are
%% totally ordered and FIFO and every replica is a prefix of the stream, so the highest
%% applied_version holds the freshest binding for every name, and a version tie implies
%% identical content. Peer snapshots are also returned for conflict detection (§5.6)
%% and the trail merge. Per-peer calls run in parallel.
gather_maps(SelfNames, SelfVersion, OtherIds) ->
    PeerResults = gather_peer_snapshots(OtherIds),
    {FreshestNames, MaxVersion} = maps:fold(
        fun(_Id, {Names, Version, _Released}, {_BestNames, BestVersion} = Best) ->
            case Version > BestVersion of
                true -> {Names, Version};
                false -> Best
            end
        end,
        {SelfNames, SelfVersion},
        PeerResults
    ),
    {FreshestNames, MaxVersion, PeerResults}.

%% Fast-path onboarding: ship the joiner our snapshot (with the member list) and tell
%% existing followers to monitor it via {peer_joined}; their replicas are untouched.
onboard_joiner(MemberId, AllIds, Tokens, Data = #data{member_id = Self, replica = R}) ->
    RecordsBin = dgen_registry_replica:encode(R),
    cast_to_member(
        MemberId,
        {apply_names_snapshot, RecordsBin, Self, AllIds, Tokens, dgen_registry_replica:epoch(R),
            dgen_registry_replica:version(R)}
    ),
    lists:foreach(
        fun(Id) -> cast_to_member(Id, {peer_joined, MemberId, Tokens}) end,
        [Id || Id <- AllIds, Id =/= Self, Id =/= MemberId]
    ),
    Data.

%% Fetch a rejoining member's replica and probe conflict pids' liveness off the
%% leader's loop (both are RPCs), reporting {joiner_gathered, …}. Our `protected` table
%% is readable from this helper.
spawn_joiner_gather(MemberId, AllIds, Tokens, Epoch, Data) ->
    #data{replica = Replica, conflicts = C} = Data,
    Owner = self(),
    _ = spawn(fun() ->
        Result =
            case member_names(MemberId) of
                {ok, {Records, _Version, Released} = Snapshot} ->
                    Trail = dgen_registry_conflicts:trail(
                        dgen_registry_conflicts:merge([Released], C)
                    ),
                    %% Our table dies with us; then there is no one to report to.
                    Alive =
                        try dgen_registry_replica:records(Replica) of
                            Own ->
                                probe_alive(
                                    dgen_registry_conflicts:conflict_pids(Own, [Records], Trail)
                                )
                        catch
                            error:badarg -> #{}
                        end,
                    {ok, Snapshot, Alive};
                error ->
                    error
            end,
        Owner ! {joiner_gathered, MemberId, AllIds, Tokens, Epoch, Result}
    end),
    ok.

%% The full handoff gather (§5.7), off the loop, reporting {assume_gathered, …}. `Ctx`
%% carries the assume payload and the ref matched against `assume_ref`. The durable
%% subscriptions (§4.9) and liveness probes (§5.6) are fetched here too: a synchronous
%% call to the elector from the member loop could deadlock against the elector's own
%% assume call into this member.
spawn_assume_gather(Ctx, SelfRecords, SelfVersion, OtherIds, Elector, Owner, Tenant, Tuid, C) ->
    _ = spawn(fun() ->
        %% gather_caught_up falls back to a self-only reconstruction if the gather
        %% throws, so the continuation always fires.
        {FreshestRecords, MaxVersion, PeerResults, Frontier} =
            gather_caught_up(SelfRecords, SelfVersion, OtherIds, Tenant, Tuid),
        Subs = fetch_subscriptions(Elector),
        Peers = maps:values(PeerResults),
        Merged = dgen_registry_conflicts:trail(
            dgen_registry_conflicts:merge([Rel || {_Recs, _V, Rel} <- Peers], C)
        ),
        Witnesses = [SelfRecords | [Recs || {Recs, _V, _Rel} <- Peers]],
        Alive = probe_alive(
            dgen_registry_conflicts:conflict_pids(FreshestRecords, Witnesses, Merged)
        ),
        Owner !
            {assume_gathered, Ctx, SelfRecords, FreshestRecords, {MaxVersion, Frontier},
                PeerResults, Subs, Alive}
    end),
    ok.

%% Re-gather until MaxVersion reaches the durable version key (the SafeAssume fence in
%% the TLA+ spec) or ?GATHER_TIMEOUT. A live member is never ahead of that key, so a
%% lower MaxVersion means the gather raced a committed broadcast still queued in a
%% peer's mailbox; fanning out that stale map would drop an acked registration. Past
%% the deadline we proceed (the continuation flags `degraded`).
gather_caught_up(SelfRecords, SelfVersion, OtherIds, Tenant, Tuid) ->
    Deadline = erlang:monotonic_time(millisecond) + ?GATHER_TIMEOUT,
    gather_caught_up(SelfRecords, SelfVersion, OtherIds, Tenant, Tuid, Deadline).

gather_caught_up(SelfRecords, SelfVersion, OtherIds, Tenant, Tuid, Deadline) ->
    {FreshestRecords, MaxVersion, PeerResults} =
        try
            gather_maps(SelfRecords, SelfVersion, OtherIds)
        catch
            _:_ -> {SelfRecords, SelfVersion, #{}}
        end,
    %% Best-effort, on the applied_version scale. `unknown` (backend hiccup, test
    %% double) disables the fence rather than wedging the handoff.
    Frontier =
        try
            dgen_registry_names:read_committed_frontier(Tenant, Tuid)
        catch
            _:_ -> unknown
        end,
    case
        Frontier =:= unknown orelse MaxVersion >= Frontier orelse
            erlang:monotonic_time(millisecond) >= Deadline
    of
        true ->
            {FreshestRecords, MaxVersion, PeerResults, Frontier};
        false ->
            timer:sleep(?ASSUME_CATCHUP_INTERVAL),
            gather_caught_up(SelfRecords, SelfVersion, OtherIds, Tenant, Tuid, Deadline)
    end.

%% Best-effort, short-bounded subscription pull from the elector; off-loop callers only.
%% On timeout the empty set is reconciled by a later delta or handoff.
fetch_subscriptions(Elector) when is_pid(Elector) ->
    try dgen_registry_elector:priority_get_subscriptions(Elector, ?SUBS_PULL_TIMEOUT) of
        Subs when is_map(Subs) -> Subs;
        _ -> #{}
    catch
        _:_ -> #{}
    end;
fetch_subscriptions(_Elector) ->
    #{}.

%% One member_names/1 helper per peer under one deadline; late answers are dropped by
%% unhandled/3.
gather_peer_snapshots([]) ->
    #{};
gather_peer_snapshots(OtherIds) ->
    Ref = make_ref(),
    Parent = self(),
    lists:foreach(
        fun(MemberId) ->
            spawn(fun() -> Parent ! {Ref, MemberId, member_names(MemberId)} end)
        end,
        OtherIds
    ),
    Deadline = erlang:monotonic_time(millisecond) + ?GATHER_TIMEOUT + 500,
    collect_gather(Ref, OtherIds, Deadline, #{}).

collect_gather(_Ref, [], _Deadline, Acc) ->
    Acc;
collect_gather(Ref, Waiting, Deadline, Acc) ->
    Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
    _ = ?ETA_LOG({gather_wait, length(Waiting), Timeout}),
    receive
        {Ref, MemberId, {ok, Snapshot}} ->
            collect_gather(Ref, lists:delete(MemberId, Waiting), Deadline, Acc#{
                MemberId => Snapshot
            });
        {Ref, MemberId, error} ->
            collect_gather(Ref, lists:delete(MemberId, Waiting), Deadline, Acc)
    after Timeout ->
        _ = ?ETA_LOG({gather_timed_out, length(Waiting)}),
        Acc
    end.

%% Detect and repair conflicts (§5.6) per dgen_registry_conflicts:resolve/5, carrying
%% out its kills and alarms. Returns the reconstruction with conflicted names dropped.
repair_conflicts(Authority, Witnesses, Trails, Alive, Data = #data{conflicts = C0}) ->
    C1 = dgen_registry_conflicts:merge(Trails, C0),
    Trail = dgen_registry_conflicts:trail(C1),
    Conflicts = dgen_registry_conflicts:detect(Authority, Witnesses, Trail, probed(Alive)),
    Now = erlang:system_time(millisecond),
    {Records, C2, Effects} =
        dgen_registry_conflicts:resolve(Conflicts, Authority, Data#data.config, Now, C1),
    lists:foreach(
        fun
            ({kill, Pid}) ->
                exit(Pid, kill);
            ({alarm, {Name, APid, Divergent}, Action}) ->
                alarm_conflict(Name, APid, Divergent, Action)
        end,
        Effects
    ),
    {Records, Data#data{conflicts = C2}}.

%% Parallel liveness probe; runs in the gather helpers, never on the member's loop.
probe_alive([]) ->
    #{};
probe_alive(Pids) ->
    Ref = make_ref(),
    Parent = self(),
    lists:foreach(
        fun(Pid) -> spawn(fun() -> Parent ! {Ref, Pid, is_pid_alive(Pid)} end) end, Pids
    ),
    Deadline = erlang:monotonic_time(millisecond) + ?ALIVE_PROBE_TIMEOUT + 500,
    collect_alive(Ref, length(Pids), Deadline, #{}).

collect_alive(_Ref, 0, _Deadline, Acc) ->
    Acc;
collect_alive(Ref, Waiting, Deadline, Acc) ->
    Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {Ref, Pid, Alive} -> collect_alive(Ref, Waiting - 1, Deadline, Acc#{Pid => Alive})
    after Timeout -> Acc
    end.

%% An unprobed pid is checked only if local (no RPC); a remote one counts as dead so
%% it is never killed on evidence we did not gather.
probed(Alive) ->
    fun(Pid) ->
        case Alive of
            #{Pid := A} -> A;
            _ when node(Pid) =:= node() -> is_process_alive(Pid);
            _ -> false
        end
    end.

alarm_conflict(Name, Authority, Divergent, Action) ->
    logger:error(
        "dgen_registry: name conflict — name=~p authority=~p divergent=~p action=~p. "
        "Two different live pids held one name (single-fault uniqueness breach, §5.6).",
        [Name, Authority, Divergent, Action]
    ),
    %% `action` budget_exhausted is the one an operator must act on.
    emit_telemetry(
        [dgen_registry, conflict],
        #{divergent_count => length(Divergent)},
        #{name => Name, authority => Authority, action => Action}
    ).

%% An unreachable node counts as not-alive, so a conflict is never escalated on a
%% binding we cannot verify.
is_pid_alive(Pid) when node(Pid) =:= node() ->
    is_process_alive(Pid);
is_pid_alive(Pid) ->
    Node = node(Pid),
    case lists:member(Node, nodes()) of
        false ->
            false;
        true ->
            try erpc:call(Node, erlang, is_process_alive, [Pid], ?ALIVE_PROBE_TIMEOUT) of
                Alive when is_boolean(Alive) -> Alive;
                _ -> false
            catch
                _:_ -> false
            end
    end.

%% Bounded snapshot read of a peer's records, version and trail; `error` if
%% disconnected or slow (its bindings are then unavailable to this gather).
member_names({Node, Name}) ->
    case lists:member(Node, nodes()) of
        false ->
            error;
        true ->
            %% All inside `try`: decode_records can fault on a corrupt payload.
            try
                {Payload, Version, Released} =
                    gen_statem:call({Name, Node}, get_names_snapshot, ?GATHER_TIMEOUT),
                Names = dgen_registry_replica:decode_records(Payload),
                true = is_map(Names) andalso is_integer(Version) andalso is_map(Released),
                {ok, {Names, Version, Released}}
            catch
                _:_ -> error
            end
    end.

%% §5.6 prevention (opt-in `reject_when_degraded`): while degraded, refuse names the
%% leader does not hold; they could be committed bindings the reconstruction missed.
reject_new_when_degraded(Name, #data{conflicts = C, replica = R, config = Config}) ->
    dgen_registry_conflicts:degraded(C) andalso dgen_config:reject_when_degraded(Config) andalso
        not dgen_registry_replica:exists(R, Name).

%% With no (reachable) leader a registration is never answered `no` (that would be a
%% false `already_started`); it blocks as an intent until a leader appears.
route_register(Name, Pid, Meta, From, Data = #data{leader = Leader, member_id = Self}) ->
    if
        Leader =:= Self ->
            enqueue_register(Name, Pid, Meta, From, Data);
        true ->
            submit_intent({register, Name, Pid, Meta}, From, Data)
    end.

%% Mirrors route_register. The bound pid is captured before the optimistic row_delete
%% so the leader can clear it and record it in the trail (§5.6). Leader: commit, answer
%% `ok` on commit. Follower: forward as a tracked intent, or with no reachable leader
%% answer `ok` now and hold the intent until one is (CP: never lost).
route_unregister(
    LogicalName, From, Data = #data{leader = Leader, member_id = Self, replica = R}
) ->
    ReleasedPid = dgen_registry_replica:lookup_pid(R, LogicalName),

    if
        Leader =:= Self ->
            {noreply,
                enqueue_op(
                    {remove, LogicalName, ReleasedPid, {unreg, From}},
                    row_delete(Data, LogicalName)
                )};
        true ->
            Data1 = row_delete(Data, LogicalName),
            {noreply, submit_intent({unregister, LogicalName, ReleasedPid}, From, Data1)}
    end.

enqueue_register(Name, Pid, Meta, From, Data) ->
    case reject_new_when_degraded(Name, Data) of
        true ->
            gen_server:reply(From, no),
            Data;
        false ->
            enqueue_op({add, Name, Pid, Meta, {local, From}}, Data)
    end.

%% ---------------------------------------------------------------------------
%% Intents — requests this member cannot decide itself (dgen_registry_intents)
%% ---------------------------------------------------------------------------

intent_route(#data{leader = Self, member_id = Self}) ->
    self;
intent_route(#data{leader = undefined}) ->
    none;
intent_route(#data{leader = Leader}) ->
    case member_reachable(Leader) of
        true -> {remote, Leader};
        false -> none
    end.

submit_intent(Kind, From, Data = #data{intents = I}) ->
    Now = erlang:system_time(millisecond),
    apply_intents(dgen_registry_intents:submit(Kind, From, Now, intent_route(Data), I), Data).

intent_reply(Ref, Reply, Data = #data{intents = I}) ->
    apply_intents(dgen_registry_intents:on_reply(Ref, Reply, applied_version(Data), I), Data).

%% Our replica advanced: release the gap-deferred `yes` acks it now covers.
intents_advanced(Data = #data{intents = I}) ->
    apply_intents(dgen_registry_intents:on_version(applied_version(Data), I), Data).

%% Re-drive unresolved intents: `Scope` is `moved` for a new leadership view, `all` for
%% a restored link (see dgen_registry_intents:redrive/5).
redrive_intents(Scope, Data = #data{intents = I, config = Config}) ->
    Now = erlang:system_time(millisecond),
    TTL = dgen_config:register_timeout(Config),
    Route = intent_route(Data),
    apply_intents(dgen_registry_intents:redrive(Route, Scope, Now, TTL, I), Data).

%% Drop intents whose callers timed out, bounding growth during a long partition.
prune_intents(Data = #data{intents = I, config = Config}) ->
    Now = erlang:system_time(millisecond),
    TTL = dgen_config:register_timeout(Config),
    Data#data{intents = dgen_registry_intents:prune(Now, TTL, I)}.

apply_intents({Intents, Effects}, Data) ->
    lists:foldl(fun intent_effect/2, Data#data{intents = Intents}, Effects).

intent_effect({reply, From, Value}, Data) ->
    gen_server:reply(From, Value),
    Data;
intent_effect({forward, Leader, Ref, Kind}, Data = #data{member_id = Self}) ->
    cast_to_member(Leader, forward_msg(Ref, Self, Kind)),
    Data;
intent_effect({retract, Leader, Name, Pid}, Data) ->
    forward_retract(Leader, Name, Pid),
    Data;
intent_effect({enqueue, {register, Name, Pid, Meta}, From}, Data) ->
    enqueue_register(Name, Pid, Meta, From, Data);
intent_effect({enqueue, {unregister, Name, Pid}, answered}, Data) ->
    enqueue_op({retract, Name, Pid}, Data);
intent_effect({resync, Leader}, Data) ->
    request_resync(Leader, Data);
intent_effect({insert_row, Name, Pid, {Index, MetaData}}, Data) ->
    row_insert(Data, Name, Pid, Index, MetaData).

forward_msg(Ref, Self, {register, Name, Pid, Meta}) ->
    {register_req, Ref, Self, Name, Pid, Meta};
forward_msg(Ref, Self, {unregister, Name, _ReleasedPid}) ->
    {unregister_req, Ref, Self, Name};
forward_msg(Ref, Self, {set_meta, Name, Index, MetaData}) ->
    {set_meta_req, Ref, Self, Name, Index, MetaData}.

%% Ops arriving during an in-flight commit accumulate and ride the next batch.
enqueue_op(Op, Data = #data{pipeline = P}) ->
    maybe_start_commit(Data#data{pipeline = dgen_registry_pipeline:enqueue(Op, P)}).

%% At most one commit runs at a time, in a dgen_transaction worker off this loop. A
%% member that no longer leads surrenders the queue instead.
maybe_start_commit(Data = #data{pipeline = P, leader = Leader, member_id = Self}) ->
    case {dgen_registry_pipeline:in_flight(P), dgen_registry_pipeline:num_pending(P)} of
        {true, _} ->
            Data;
        {false, 0} ->
            Data;
        {false, _} when Leader =/= Self ->
            {Ops, P1} = dgen_registry_pipeline:take_pending(P),
            surrender(deposed, Ops, Data#data{pipeline = P1});
        {false, _} ->
            start_commit(Data)
    end.

start_commit(Data) ->
    Epoch = epoch(Data),
    Tab = dgen_registry_replica:tab(Data#data.replica),
    #data{
        member_id = Self,
        tenant = Tenant,
        tuid = Tuid,
        name_to_ref = NTR,
        pipeline = P,
        config = Config
    } = Data,
    {ThisBatch, P1} = dgen_registry_pipeline:take_batch(dgen_config:commit_batch_size(Config), P),
    LastVersion = dgen_registry_pipeline:read_version(P1),
    Data1 = Data#data{pipeline = P1},
    %% Planning is O(batch): bindings resolve lazily (see plan_batch/4).
    Plan = plan_batch(ThisBatch, Tab, NTR, Epoch),
    #{dbop := DBOp, replies := RepliesRev} = Plan,
    case map_size(DBOp) =:= 0 of
        true ->
            %% Only rejections: no commit needed.
            send_replies(lists:reverse(RepliesRev)),
            maybe_start_commit(Data1);
        false ->
            Ref = make_ref(),
            %% Pin last_version as the read version to skip a GRV; if stale, the
            %% leader-key read raises too-old and dgen_transaction retries with a fresh
            %% one. The worker is monitored so its death still resolves the commit. Its
            %% only write is the fenced version bump; the name delta applies in memory.
            {ok, {_Pid, MRef}} = dgen_registry_names:start_commit(
                Tenant,
                Tuid,
                Self,
                #{owner => self(), ref => Ref, epoch => Epoch, read_version => LastVersion}
            ),
            Data1#data{pipeline = dgen_registry_pipeline:start(Ref, MRef, Plan, P1)}
    end.

%% Apply a committed batch: update the replica, (de)monitor pids, replicate, and
%% answer callers.
apply_committed_plan(Plan, Version, Data) ->
    #{
        dbop := DBOp,
        names := WNames,
        meta := WMeta,
        replies := RepliesRev,
        bcasts := BcastsRev,
        released := Released,
        direct_meta := DirectMeta
    } = Plan,
    #data{
        member_id = Self,
        name_to_ref = NTR0,
        members = Members,
        replica = R0
    } = Data,
    PrevVersion = dgen_registry_replica:version(R0),
    %% Only DBOp names are touched, so a concurrent optimistic unregister of an
    %% untouched name is never clobbered.
    Data0 = Data#data{replica = dgen_registry_replica:commit(Version, DBOp, WNames, WMeta, R0)},
    NTR1 = apply_monitor_ops(DBOp, WNames, NTR0),
    %% Broadcast first, so the replicate_sync (FIFO behind it) reaches a follower only
    %% after it holds the batch. One message per batch, never per name: a partially
    %% received batch would report the full version with partial content, breaking
    %% "same version implies same content" that the handoff gather relies on (§5.7).
    broadcast_batch(Members, lists:reverse(BcastsRev), PrevVersion, Version, Self),
    Now = erlang:system_time(millisecond),
    Data1 = Data0#data{
        name_to_ref = NTR1,
        pipeline = dgen_registry_pipeline:committed(Version, Data0#data.pipeline),
        %% Explicit releases extend the conflict trail (§5.6).
        conflicts = dgen_registry_conflicts:released(Released, Now, Data0#data.conflicts)
    },
    %% Forwarded registrations and rejections are answered now (a forwarded `yes` is
    %% already two-holder); a direct `yes` waits for a follower's replica ack (§8).
    {DirectYes, Immediate} = lists:partition(fun is_direct_yes/1, lists:reverse(RepliesRev)),
    lists:foreach(
        fun({Origin, Result}) -> deliver_committed_reply(Origin, Result, Version) end, Immediate
    ),
    %% {Name, Pid} lets strict_replication roll a direct binding back.
    EnrichedDirect = [
        {Origin, Name, Pid}
     || {Origin, yes} <- DirectYes, {Name, Pid} <- [maps:get(Origin, DirectMeta)]
    ],
    %% Presence notifications (§4.9), computed against the committed replica.
    Data2 = fire_presence(DBOp, Data1),
    confirm_direct(EnrichedDirect, Version, Data2).

%% Presence for a committed batch (leader only, §4.9), diffing the changed names per
%% subscription: watch-set changes yield `joined`/`left` for continuing notify targets;
%% a pid newly entering the notify set gets the full watch snapshot.
fire_presence(_DBOp, Data = #data{subs = Subs}) when map_size(Subs) =:= 0 ->
    Data;
fire_presence(DBOp, Data = #data{replica = R, subs = Subs}) ->
    Tab = dgen_registry_replica:tab(R),
    RealNames = maps:keys(DBOp),
    maps:fold(
        fun(SubId, {Watch, Notify}, S) ->
            diff_subscription(SubId, Watch, Notify, RealNames, Tab, S)
        end,
        Data,
        Subs
    ).

%% Seed a (re)subscription: compute both sets and send the watch snapshot to the
%% current notify targets.
seed_subscription(
    SubId,
    Data = #data{
        subs = Subs, replica = R, sub_matches = SM, notify_matches = NM
    }
) ->
    case Subs of
        #{SubId := {Watch, Notify}} ->
            WatchMatches = dgen_registry_replica:query_match_names(R, Watch),
            NotifyMatches = dgen_registry_replica:query_match_names(R, Notify),
            send_presence(SubId, snapshot_events(WatchMatches), pidset(NotifyMatches)),
            Data#data{
                sub_matches = SM#{SubId => WatchMatches},
                notify_matches = NM#{SubId => NotifyMatches}
            };
        _ ->
            Data
    end.

%% Diff one subscription and fire its notifications. Done on pid-sets, so a pid already
%% a target under another name gets no duplicate snapshot.
diff_subscription(SubId, Watch, Notify, RealNames, Tab, Data) ->
    #data{sub_matches = SM, notify_matches = NM} = Data,
    {WatchEvents, WatchAfter} = diff_query_matches(Watch, RealNames, maps:get(SubId, SM, #{}), Tab),
    NotifyBefore = maps:get(SubId, NM, #{}),
    {_NotifyEvents, NotifyAfter} = diff_query_matches(Notify, RealNames, NotifyBefore, Tab),
    AfterPids = pidset(NotifyAfter),
    NewTargets = AfterPids -- pidset(NotifyBefore),
    ContinuingTargets = AfterPids -- NewTargets,
    send_presence(SubId, WatchEvents, ContinuingTargets),
    send_presence(SubId, snapshot_events(WatchAfter), NewTargets),
    Data#data{sub_matches = SM#{SubId => WatchAfter}, notify_matches = NM#{SubId => NotifyAfter}}.

%% Diff a query's membership over the changed names: `joined`/`left` events and the
%% updated `#{Name => Pid}` map.
diff_query_matches(Query, RealNames, Before, Tab) ->
    lists:foldl(
        fun(Name, {EvAcc, MatchAcc}) ->
            WasPid = maps:get(Name, Before, undefined),
            NowMatch =
                case ets:lookup(Tab, Name) of
                    [{_N, RowPid, Index, _Data} | _] ->
                        case dgen_registry_replica:query_satisfies(Query, Index) of
                            true -> {true, RowPid};
                            false -> false
                        end;
                    [] ->
                        false
                end,
            case {WasPid, NowMatch} of
                {undefined, {true, Pid}} ->
                    {[{joined, Name, Pid} | EvAcc], MatchAcc#{Name => Pid}};
                {undefined, false} ->
                    {EvAcc, MatchAcc};
                {WasPid, {true, WasPid}} ->
                    {EvAcc, MatchAcc};
                {OldPid, {true, NewPid}} ->
                    {[{joined, Name, NewPid}, {left, Name, OldPid} | EvAcc], MatchAcc#{
                        Name => NewPid
                    }};
                {OldPid, false} ->
                    {[{left, Name, OldPid} | EvAcc], maps:remove(Name, MatchAcc)}
            end
        end,
        {[], Before},
        RealNames
    ).

snapshot_events(Matches) ->
    [{joined, Name, Pid} || {Name, Pid} <- maps:to_list(Matches)].

pidset(Matches) ->
    lists:usort(maps:values(Matches)).

send_presence(_SubId, [], _Pids) ->
    ok;
send_presence(_SubId, _Events, []) ->
    ok;
send_presence(SubId, Events, Pids) ->
    Msg = {dgen_presence, SubId, Events},
    lists:foreach(fun(Pid) -> Pid ! Msg end, Pids).

is_direct_yes({{local, _From}, yes}) -> true;
is_direct_yes(_Other) -> false.

%% Replicate-before-ack for direct registrations (§5.5, §8): wait for
%% `register_replicas` distinct follower acks (capped by follower count; none needed if
%% there are no followers), or the `strict_replication` policy on timeout.
confirm_direct([], _Version, Data) ->
    Data;
confirm_direct(
    Direct,
    Version,
    Data = #data{members = Members, member_id = Self, pipeline = P, config = Config}
) ->
    Needed = min(dgen_config:register_replicas(Config), map_size(Members)),
    case Needed =< 0 of
        true ->
            deliver_direct(Direct),
            Data;
        false ->
            BatchRef = make_ref(),
            %% Carries Version so a gapped follower that lacks the batch never acks.
            broadcast_to_peers(Members, {replicate_sync, BatchRef, Self, Version}),
            TimerRef = schedule_timer(
                dgen_config:replicate_timeout(Config), {replicate_timeout, BatchRef}
            ),
            Data#data{
                pipeline = dgen_registry_pipeline:await_acks(BatchRef, Direct, TimerRef, Needed, P)
            }
    end.

deliver_direct(Direct) ->
    lists:foreach(fun({Origin, _Name, _Pid}) -> deliver_reply(Origin, yes) end, Direct).

%% strict_replication fail-closed: reject and retract (a pid-guarded, committed clear).
reject_and_retract_direct(Direct, Data) ->
    lists:foldl(
        fun({Origin, Name, Pid}, S) ->
            deliver_reply(Origin, no),
            enqueue_op({retract, Name, Pid}, S)
        end,
        Data,
        Direct
    ).

%% Degrade-open event (§8): telemetry and a log warning.
notify_degrade_open(Needed, Got, Count) ->
    logger:warning(
        "dgen_registry: degrade-open — acked ~b direct registration(s) leader-only "
        "(needed ~b replica ack(s), got ~b) on replicate timeout (§8).",
        [Count, Needed, Got]
    ),
    emit_telemetry(
        [dgen_registry, register, degrade_open],
        #{count => Count},
        #{needed => Needed, got => Got}
    ).

%% `telemetry` is a soft dependency. Availability is resolved once per VM and cached:
%% `code:ensure_loaded/1` on a missing module calls `code_server` every time, which
%% also breaks reproducibility under eta simulation.
-define(TELEMETRY_KEY, {?MODULE, telemetry_available}).

emit_telemetry(Event, Measurements, Metadata) ->
    case telemetry_available() of
        true -> apply(telemetry, execute, [Event, Measurements, Metadata]);
        false -> ok
    end.

telemetry_available() ->
    case persistent_term:get(?TELEMETRY_KEY, undefined) of
        undefined ->
            Available = code:ensure_loaded(telemetry) =:= {module, telemetry},
            persistent_term:put(?TELEMETRY_KEY, Available),
            Available;
        Available ->
            Available
    end.

%% Answer a write op's origin: gen_server:reply/2 locally, or a reply cast to the
%% forwarding member.
deliver_reply({local, From}, Result) ->
    gen_server:reply(From, Result);
deliver_reply({meta, From}, Result) ->
    gen_server:reply(From, Result);
deliver_reply({unreg, From}, Result) ->
    gen_server:reply(From, Result);
deliver_reply({forward, MemberId, Ref}, Result) ->
    cast_to_member(MemberId, {register_reply, Ref, Result});
deliver_reply({forward_meta, MemberId, Ref}, Result) ->
    cast_to_member(MemberId, {set_meta_reply, Ref, Result});
deliver_reply({forward_unreg, MemberId, Ref}, _Result) ->
    cast_to_member(MemberId, {unregister_reply, Ref}).

%% A forwarded registration's reply carries the commit version so the follower can
%% version-guard its `yes` (see the {register_reply, _, _, _} handler).
deliver_committed_reply({forward, MemberId, Ref}, Result, Version) ->
    cast_to_member(MemberId, {register_reply, Ref, Result, Version});
deliver_committed_reply(Origin, Result, _Version) ->
    deliver_reply(Origin, Result).

%% Fold the ordered ops into a plan: the working overlay, the net durable delta (`dbop`,
%% last write wins), and replies and broadcasts (newest first). `wntr` tracks monitor
%% refs for the `down` ref-match guard; a name bound in this batch carries a
%% `{pending, Pid}` placeholder that never matches a real ref. `names` starts empty:
%% each op resolves its one name via seed_lookup/3, so planning is O(batch).
plan_batch(Ops, Tab, NTR, Epoch) ->
    Init = #{
        tab => Tab,
        names => #{},
        meta => #{},
        wntr => NTR,
        dbop => #{},
        replies => [],
        bcasts => [],
        released => [],
        direct_meta => #{},
        %% A plan whose epoch is no longer current is not applied; broadcasts carry it.
        epoch => Epoch,
        %% Kept so a failed batch can be surrendered.
        ops => Ops
    },
    lists:foldl(fun(Op, Acc) -> plan_op(Op, Epoch, Acc) end, Init, Ops).

%% A name's pid partway through this batch: the batch overlay `WN` first, else ETS.
%% `removed` marks a name cleared earlier in this batch: only unregister deletes from
%% ETS eagerly, so a prior `retract` or `down` is visible only through the overlay.
seed_lookup(WN, Tab, Name) ->
    case WN of
        #{Name := removed} -> undefined;
        #{Name := Pid} -> Pid;
        _ -> lookup_name(Tab, Name)
    end.

plan_op({add, Name, Pid, {Index, MetaData} = Meta, Origin}, Epoch, Acc) ->
    #{
        tab := Tab,
        names := WN,
        meta := WMeta,
        wntr := WNTR,
        dbop := DB,
        replies := Rs,
        bcasts := Bs,
        direct_meta := DM
    } = Acc,
    case seed_lookup(WN, Tab, Name) of
        Taken when is_pid(Taken), Taken =/= Pid ->
            %% Held by a different pid: reject (the singleton contract).
            Acc#{replies := [{Origin, no} | Rs]};
        _Free_or_same ->
            %% Free, or the same pid: an idempotent `yes` (§5.2), so a caller whose
            %% call timed out can re-drive and get a decisive answer. Direct-origin
            %% bindings are recorded for strict_replication rollback.
            DM1 =
                case Origin of
                    {local, _From} -> DM#{Origin => {Name, Pid}};
                    _ -> DM
                end,
            Acc#{
                names := WN#{Name => Pid},
                meta := WMeta#{Name => Meta},
                wntr := WNTR#{Name => {pending, Pid}},
                dbop := DB#{Name => {set, node(Pid)}},
                replies := [{Origin, yes} | Rs],
                bcasts := [{name_registered, Name, Pid, Index, MetaData, Epoch} | Bs],
                direct_meta := DM1
            }
    end;
plan_op({set_meta, Name, Index, MetaData, Origin}, Epoch, Acc) ->
    #{tab := Tab, names := WN, meta := WMeta, dbop := DB, replies := Rs, bcasts := Bs} = Acc,
    case seed_lookup(WN, Tab, Name) of
        undefined ->
            Acc#{replies := [{Origin, {error, not_registered}} | Rs]};
        Pid ->
            %% Keep an in-batch {set,…} so the new pid is still monitored; otherwise
            %% {meta,…} leaves the existing monitor alone.
            DBVal =
                case maps:get(Name, DB, undefined) of
                    {set, _} = SetOp -> SetOp;
                    _ -> {meta, node(Pid)}
                end,
            Acc#{
                meta := WMeta#{Name => {Index, MetaData}},
                dbop := DB#{Name => DBVal},
                replies := [{Origin, ok} | Rs],
                bcasts := [{metadata_set, Name, Index, MetaData, Epoch} | Bs]
            }
    end;
plan_op({remove, Name, ReleasedPid, Origin}, Epoch, Acc) ->
    #{
        tab := Tab,
        names := WN,
        wntr := WNTR,
        dbop := DB,
        replies := Rs,
        bcasts := Bs,
        released := Rel
    } = Acc,
    %% Clear if the name was bound at enqueue (ReleasedPid; the optimistic delete
    %% already removed it from ETS) or is bound now in this batch (seed_lookup). The
    %% released pair joins the conflict trail (§5.6). The origin gets `ok` either way.
    Rs1 = unreg_reply(Origin, Rs),
    WasBound = is_pid(ReleasedPid),
    Current = seed_lookup(WN, Tab, Name),
    %% Guard against the *current* holder: the optimistic row_delete frees the name at
    %% arrival, so a register queued ahead of this remove may already have rebound it.
    %% Clearing that new holder would break Guarantee 1. A captured pid clears only if
    %% the current holder is that pid or nobody; a capture of `undefined` clears
    %% whatever is bound (unregister by name).
    Rebound = WasBound andalso is_pid(Current) andalso Current =/= ReleasedPid,
    case (WasBound orelse Current =/= undefined) andalso not Rebound of
        true ->
            Rel1 =
                case WasBound of
                    true -> [{Name, ReleasedPid} | Rel];
                    false -> Rel
                end,
            %% The released pid rides the broadcast into every member's trail (§5.6).
            ReleasedOut =
                case WasBound of
                    true -> ReleasedPid;
                    false -> undefined
                end,
            Acc#{
                names := WN#{Name => removed},
                wntr := maps:remove(Name, WNTR),
                dbop := DB#{Name => clear},
                replies := Rs1,
                bcasts := [{name_unregistered, Name, ReleasedOut, Epoch} | Bs],
                released := Rel1
            };
        false ->
            Acc#{replies := Rs1}
    end;
plan_op({retract, Name, Pid}, Epoch, Acc) ->
    #{tab := Tab, names := WN, wntr := WNTR, dbop := DB, bcasts := Bs, released := Rel} = Acc,
    %% Pid-guarded (strict_replication fail-closed): clear only if still this pid's,
    %% and trail it so a lagging follower's copy is not mistaken for a conflict.
    case seed_lookup(WN, Tab, Name) of
        Pid ->
            Acc#{
                names := WN#{Name => removed},
                wntr := maps:remove(Name, WNTR),
                dbop := DB#{Name => clear},
                bcasts := [{name_unregistered, Name, Pid, Epoch} | Bs],
                released := [{Name, Pid} | Rel]
            };
        _ ->
            Acc
    end;
plan_op({down, Name, Ref}, Epoch, Acc) ->
    #{names := WN, wntr := WNTR, dbop := DB, bcasts := Bs} = Acc,
    case maps:get(Name, WNTR, undefined) of
        Ref ->
            %% The pid is dead, so nothing is trailed (the trail is for live releases).
            Acc#{
                names := WN#{Name => removed},
                wntr := maps:remove(Name, WNTR),
                dbop := DB#{Name => clear},
                bcasts := [{name_unregistered, Name, undefined, Epoch} | Bs]
            };
        _ ->
            %% Stale DOWN (re-registered or already removed).
            Acc
    end.

%% A remove with origin `undefined` (legacy cast or retract) has no caller.
unreg_reply(undefined, Rs) -> Rs;
unreg_reply(Origin, Rs) -> [{Origin, ok} | Rs].

%% Monitor side effects of the delta: clear demonitors, set re-monitors the new pid,
%% meta leaves the monitor alone.
apply_monitor_ops(DBOp, WNames, NTR) ->
    maps:fold(
        fun
            (_Name, {meta, _PidNode}, Acc) ->
                Acc;
            (Name, clear, NTRacc) ->
                demonitor_name(Name, NTRacc);
            (Name, {set, _PidNode}, NTRacc) ->
                NTR1 = demonitor_name(Name, NTRacc),
                Pid = maps:get(Name, WNames),
                NTR1#{Name => monitor_name(Name, Pid)}
        end,
        NTR,
        DBOp
    ).

send_replies(Replies) ->
    lists:foreach(fun({Origin, Reply}) -> deliver_reply(Origin, Reply) end, Replies).

%% Extend the conflict trail (§5.6) from an applied batch. Keyed by {Name, Pid}, so a
%% release under one name never masks a conflict on another name the same pid holds.
record_released(Ops, Data = #data{conflicts = C}) ->
    Pairs = [{Name, Pid} || {name_unregistered, Name, Pid} <- Ops, is_pid(Pid)],
    Now = erlang:system_time(millisecond),
    Data#data{conflicts = dgen_registry_conflicts:released(Pairs, Now, C)}.

%% One `{names_batch, Ops, Epoch, PrevV, Version, LeaderId}` per committed batch; the
%% epoch is carried once on the envelope. Followers use PrevV/Version for gap detection
%% and LeaderId as the resync target. An empty batch sends nothing.
-ifdef(MUTATION_PARTIAL_BATCH).
%% MUTATION (test-only): reintroduces one message per op sharing {PrevV, Version}, so a
%% partially received batch diverges silently. Paired with dgen_registry_replica.
broadcast_batch(_Members, [], _PrevV, _Version, _Self) ->
    ok;
broadcast_batch(Members, Ops, PrevV, Version, Self) ->
    lists:foreach(
        fun(Op) ->
            broadcast_to_peers(
                Members,
                {names_batch, [strip_bcast_epoch(Op)], bcast_epoch(Op), PrevV, Version, Self}
            )
        end,
        Ops
    ).
-else.
broadcast_batch(_Members, [], _PrevV, _Version, _Self) ->
    ok;
broadcast_batch(Members, [First | _] = Ops, PrevV, Version, Self) ->
    Epoch = bcast_epoch(First),
    Stripped = [strip_bcast_epoch(Op) || Op <- Ops],
    broadcast_to_peers(Members, {names_batch, Stripped, Epoch, PrevV, Version, Self}).
-endif.

-ifdef(MUTATION_QUIET_RESYNC).
%% MUTATION (test-only, `DGEN_MUTATION=quiet_resync`): the heartbeat advertises
%% nothing, so a follower that loses the stream's tail stays diverged while quiet.
broadcast_heartbeat(_Data) ->
    ok.
-else.
%% The leader's periodic empty batch at {Applied, Applied}; see
%% ?REPLICA_HEARTBEAT_INTERVAL.
broadcast_heartbeat(Data = #data{members = Members, member_id = Self}) ->
    Applied = applied_version(Data),
    broadcast_to_peers(Members, {names_batch, [], epoch(Data), Applied, Applied, Self}),
    ok.
-endif.

bcast_epoch({name_registered, _Name, _Pid, _Index, _Data, Epoch}) -> Epoch;
bcast_epoch({metadata_set, _Name, _Index, _Data, Epoch}) -> Epoch;
bcast_epoch({name_unregistered, _Name, _ReleasedPid, Epoch}) -> Epoch.

strip_bcast_epoch({name_registered, Name, Pid, Index, MetaData, _Epoch}) ->
    {name_registered, Name, Pid, Index, MetaData};
strip_bcast_epoch({metadata_set, Name, Index, MetaData, _Epoch}) ->
    {metadata_set, Name, Index, MetaData};
strip_bcast_epoch({name_unregistered, Name, ReleasedPid, _Epoch}) ->
    {name_unregistered, Name, ReleasedPid}.

%% Ask `LeaderId` for a snapshot after a gap, at most once per ?RESYNC_RETRY.
request_resync(LeaderId, Data = #data{resync_timer = undefined, member_id = Self}) ->
    cast_to_member(LeaderId, {resync_req, Self}),
    Ref = schedule_timer(?RESYNC_RETRY, resync_timeout),
    Data#data{resync_timer = Ref};
request_resync(_LeaderId, Data) ->
    Data.

cancel_resync(Data = #data{resync_timer = undefined}) ->
    Data;
cancel_resync(Data = #data{resync_timer = Ref}) ->
    erlang:cancel_timer(Ref),
    Data#data{resync_timer = undefined}.

%% Mark synced, re-announcing once with `fresh = false`.
mark_synced(Data = #data{synced = true}) ->
    Data;
mark_synced(Data = #data{elector = Elector, member_id = Self, join_token = Token}) ->
    dgen_registry_elector:join(Elector, Self, Token, false),
    Data#data{synced = true}.

%% Connected, so a cast will not be dropped?
member_reachable({Node, _Name}) ->
    dgen_utils:node_reachable(Node).

%% Report a peer's death with the elector's current token (fallback `Cached`), from a
%% helper so this loop never blocks on the elector (see the DOWN handler).
spawn_member_down(Elector, MemberId, Cached) ->
    _ = spawn(fun() ->
        Token = dgen_registry_elector:get_member_token(Elector, MemberId, Cached),
        dgen_registry_elector:member_down(Elector, MemberId, Token)
    end),
    ok.

%% Guarantee 5: answer a consistent read only after verifying against the durable
%% leader key that we are still the leader for our epoch; a deposed leader on the
%% minority side of a partition would otherwise serve its frozen replica. `Value` is
%% captured at the mailbox point; the verify runs in a helper so the DB round-trip
%% never blocks the loop. Linearizable because leadership is continuous within an
%% epoch. Any failure answers `Denied` (the CP refusal).
reply_fenced(From, Value, Denied, Data = #data{tenant = Tenant, tuid = Tuid, member_id = Self}) ->
    Epoch = epoch(Data),

    _ = spawn(fun() ->
        Reply =
            case dgen_registry_names:verify_leader(Tenant, Tuid, Self, Epoch) of
                true -> Value;
                false -> Denied
            end,
        gen_server:reply(From, Reply)
    end),
    ok.

%% Pid-guarded retract to a reachable leader (an unregister intent's re-drive).
forward_retract(Leader, Name, Pid) ->
    cast_to_member(Leader, {retract_req, Name, Pid}).

%% Applied only if committed under the leadership view it was planned in.
commit_result(Reply, Plan, Data = #data{leader = Leader, member_id = Self}) ->
    case Reply of
        {committed, Version} when Leader =:= Self ->
            case maps:get(epoch, Plan) =:= epoch(Data) of
                true ->
                    apply_committed_plan(Plan, Version, Data);
                false ->
                    %% Older epoch: applying or broadcasting an old-term plan would
                    %% diverge replicas. Only the version bump was durable, so retry.
                    surrender(failed, maps:get(ops, Plan), Data)
            end;
        {aborted, fenced} ->
            %% A newer leader exists and we have not heard: ask now.
            surrender(failed, maps:get(ops, Plan), rejoin_once_fenced(Data));
        _Deposed_or_error ->
            surrender(failed, maps:get(ops, Plan), Data)
    end.

%% A fenced commit proves we were deposed unheard: re-announce now rather than wait
%% for the connector's durable-epoch check. Once per epoch, since each join is a
%% durable queue entry and a partitioned member keeps being fenced.
rejoin_once_fenced(Data = #data{fence_rejoined = Epoch}) when Epoch =/= undefined ->
    case epoch(Data) of
        Epoch -> Data;
        _ -> rejoin_once_fenced(Data#data{fence_rejoined = undefined})
    end;
rejoin_once_fenced(Data) ->
    #data{elector = Elector, member_id = Self, join_token = Token, synced = Synced} = Data,
    dgen_registry_elector:join(Elector, Self, Token, not Synced),
    Data#data{fence_rejoined = epoch(Data)}.

%% Carry out dgen_registry_pipeline:surrender/2: replies, intents, delayed retries.
surrender(Reason, Ops, Data) ->
    lists:foldl(
        fun
            ({reply, Origin, Value}, S) ->
                deliver_reply(Origin, Value),
                S;
            ({intent, Kind, From}, S) ->
                submit_intent(Kind, From, S);
            ({retry, Retry}, S) ->
                schedule_timer(?REQUEUE_DELAY, {requeue_ops, Retry}),
                S
        end,
        Data,
        dgen_registry_pipeline:surrender(Reason, Ops)
    ).

%% Only a local direct registration's caller can be checked; anything else is kept.
caller_alive({add, _Name, _Pid, _Meta, {local, {Caller, _Tag}}}) when node(Caller) =:= node() ->
    is_process_alive(Caller);
caller_alive(_Op) ->
    true.

%% Logs an `arm` event so a simulated `{clock, Ms}` trace entry is attributable;
%% `?ETA_LOG` is a no-op in production builds.
schedule_timer(Delay, Msg) ->
    _ = ?ETA_LOG({arm, Msg, Delay}),
    erlang:send_after(Delay, self(), Msg).

%% Losing leadership with direct registrations awaiting acks: resolve them by the
%% replicate-timeout policy (§8). Fail-closed hands the pid-guarded retract to the
%% intents so the caller's `no` cannot leave the binding alive.
fail_pending_acks(Data = #data{pipeline = P, config = Config}) ->
    {Batches, P1} = dgen_registry_pipeline:take_acks(P),
    Strict = dgen_config:strict_replication(Config),
    lists:foldl(
        fun({Direct, TimerRef, Needed, Got}, S) ->
            erlang:cancel_timer(TimerRef),
            case Strict of
                false ->
                    notify_degrade_open(Needed, Got, length(Direct)),
                    deliver_direct(Direct),
                    S;
                true ->
                    lists:foldl(
                        fun({Origin, Name, Pid}, S1) ->
                            deliver_reply(Origin, no),
                            submit_intent({unregister, Name, Pid}, answered, S1)
                        end,
                        S,
                        Direct
                    )
            end
        end,
        Data#data{pipeline = P1},
        Batches
    ).

%% ---------------------------------------------------------------------------
%% Row writes outside the stream (see dgen_registry_replica)
%% ---------------------------------------------------------------------------

%% The optimistic delete on unregister, and the legacy unversioned reply's insert
%% (rolling upgrade). Neither moves the replica's version.
row_delete(S = #data{replica = R}, Name) ->
    S#data{replica = dgen_registry_replica:delete(R, Name)}.

row_insert(S = #data{replica = R}, Name, Pid, Index, MetaData) ->
    S#data{replica = dgen_registry_replica:insert(R, Name, Pid, Index, MetaData)}.

%% Bare table, so plan_batch/4 stays testable against a plain ETS table.
lookup_name(Tab, Name) ->
    case ets:lookup(Tab, Name) of
        [{_Name, Pid, _Index, _Data} | _] -> Pid;
        [] -> undefined
    end.

add_member_monitors(MemberIds, Data) ->
    #data{member_id = Self, members = Members, monitors = Monitors} = Data,
    {NewMembers, NewMonitors} = lists:foldl(
        fun(MemberId, {MA, MonA}) ->
            case MemberId =:= Self orelse maps:is_key(MemberId, MA) of
                true ->
                    {MA, MonA};
                false ->
                    {Node, Name} = MemberId,
                    Ref = erlang:monitor(process, {Name, Node}),
                    {MA#{MemberId => Ref}, MonA#{Ref => MemberId}}
            end
        end,
        {Members, Monitors},
        MemberIds
    ),
    Data#data{members = NewMembers, monitors = NewMonitors}.

remove_member(MemberId, Data = #data{members = Members, monitors = Monitors}) ->
    case maps:get(MemberId, Members, undefined) of
        undefined ->
            Data;
        Ref ->
            erlang:demonitor(Ref, [flush]),
            %% Drop the peer's token (the caller already read it). The departure time
            %% keeps trail pruning suspended for a TTL window (§5.6).
            Data#data{
                members = maps:remove(MemberId, Members),
                monitors = maps:remove(Ref, Monitors),
                peer_tokens = maps:remove(MemberId, Data#data.peer_tokens),
                conflicts = dgen_registry_conflicts:departed(
                    erlang:system_time(millisecond), Data#data.conflicts
                )
            }
    end.

demonitor_name(LogicalName, NTR) ->
    case maps:get(LogicalName, NTR, undefined) of
        undefined ->
            NTR;
        OldRef ->
            erlang:demonitor(OldRef, [flush]),
            maps:remove(LogicalName, NTR)
    end.

%% The one place a registered-process monitor is created; tagged `{down, Name}`.
monitor_name(Name, Pid) ->
    erlang:monitor(process, Pid, [{tag, {down, Name}}]).

%% Extra member monitors on a leadership transition: none for member_down; all peers
%% for the joiner itself; just the joiner for everyone else.
extra_member_ids(undefined, _AllIds, _Id) -> [];
extra_member_ids(MemberId, AllIds, MemberId) -> AllIds;
extra_member_ids(MemberId, _AllIds, _Id) -> [MemberId].

%% The elector is authoritative and always sends current tokens, so overwrite.
merge_peer_tokens(Tokens, Data = #data{peer_tokens = PeerTokens}) ->
    Data#data{peer_tokens = maps:merge(PeerTokens, Tokens)}.

broadcast_to_peers(Members, Msg) ->
    maps:foreach(fun(MemberId, _) -> cast_to_member(MemberId, Msg) end, Members).

%% Every inter-member message goes through here. A cast to a disconnected node would
%% auto-reconnect distribution; during a partition that heals it before either side
%% observes the departure. So drop the cast unless the node is already connected.
%% Under `-ifdef(DST)` `eta_transform` rewrites the cast to `eta_net:cast/2`.
cast_to_member(To, Msg) ->
    do_cast_to_member(To, Msg).

do_cast_to_member({Node, Name}, Msg) when Node =:= node() ->
    gen_statem:cast({Name, Node}, Msg);
do_cast_to_member({Node, Name}, Msg) ->
    case lists:member(Node, nodes()) of
        true -> gen_statem:cast({Name, Node}, Msg);
        false -> ok
    end.
