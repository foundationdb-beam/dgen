-module(dgen_registry_member).
-behaviour(gen_statem).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("../../include/dgen_eta.hrl").
-include("dgen_registry_member.hrl").
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

%% The per-node member of a `dgen_registry`. It keeps a local cache of the names and
%% proxies consistent reads and writes to the leader. `§N.N` refers to
%% `docs/design/dgen_registry_design.md`, and TLA+ action names refer to
%% `formal/DgenRegistryReplication.tla`.
%%
%% ## Roles
%%
%% - Follower: applies the leader's `{names_batch, …}` and `{apply_names_snapshot, …}`
%%   casts to its replica. Each batch is one whole group commit, and we apply it all
%%   or nothing. The follower forwards writes and consistent reads to the leader as
%%   casts tagged with a `Ref`. The loop never blocks on a call to another member,
%%   because a blocking forward could deadlock against the leader while it waits for
%%   replica acks. All replication messages come from the leader process, so
%%   per-pair FIFO delivers a snapshot before any later batch, and a batch before the
%%   reply to a write it carries.
%% - Leader: the only writer. There is no durable name state (§4.4), so at handoff the
%%   leader rebuilds the name map from the freshest replica it can reach. It gathers
%%   those replicas off the loop. It serves all writes through one group-commit
%%   pipeline, monitors registered pids, and replicates each committed batch. It only
%%   answers a registration `yes` once two members hold it (§8). When a follower
%%   forwarded the registration, that follower is already the second holder. A
%%   direct registration waits for a follower's `replicate_ack`. On timeout the
%%   leader answers `yes` anyway (degrade-open), unless `strict_replication` is set.
%%
%% When no leader is reachable, we never answer a write `no`. The caller waits as an
%% intent until a leader appears or its own call times out (§3).
%%
%% ## Where things live
%%
%% - `dgen_registry_replica`: the `protected` ETS table, the inverted index, the
%%   applied version and epoch, and the rules for accepting a batch or snapshot. Rows
%%   are `{Name, Pid, Index, Data}`, and callers read them without locking. This
%%   process is the only writer. The table is not durable.
%% - `dgen_registry_intents`: requests this member accepted but can't decide itself,
%%   and the rules for re-driving them when the leader changes.
%% - `dgen_registry_pipeline`: the leader's commit queue, the in-flight commit, the
%%   pending replica acks, and the surrender rule for ops it can't finish.
%% - `dgen_registry_conflicts`: the release trail, the kill budget and the `degraded`
%%   flag (§5.6).
%% - `dgen_registry_connector`: keeps the distribution mesh in step with membership.
%% - `dgen_registry_elector`: elects the leader and asks this member to assume.
%%
%% Multi-key queries run in this member's mailbox, between batch applies, so they
%% never see half a batch (§6). Single-key snapshot reads skip the member entirely.
%%
%% ## States
%%
%% This is a `gen_statem` whose state mirrors the `leader` and `assume_ref` fields.
%% Only three events write those fields, and each returns an explicit `next_state`:
%%   - an `{elector_assume_and_distribute}` that starts a gather moves to `assuming`.
%%   - that gather's `{assume_gathered}` moves to `leader`.
%%   - an accepted `{apply_names_snapshot}` moves to the role the snapshot names.
%% Every other event returns `keep_state`.
%%
%%   searching   No leader is known.
%%   assuming    A handoff gather is in flight, and `assume_ref` is set.
%%   leader      This member leads. Entering starts the monitor sweep and seeds the
%%               watch sets. Leaving demonitors every registered pid.
%%   follower    We follow a known leader.
%%
%% Gap detection and resync, leader reachability and the commit pipeline are kept in
%% data on purpose, not in the state. None of them depends on the role. For example, a
%% leader that was deposed but hasn't heard yet can still see a gap, because the
%% spec's RecvBcastGap has no leaderView guard.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

%% How long the handoff gather waits for each member. We skip a member that misses it.
-define(GATHER_TIMEOUT, 2000).
%% The pause between re-gathers while the assume gather is behind the durable version
%% key. This is the SafeAssume guard in the TLA+ spec. ?GATHER_TIMEOUT bounds the total
%% wait.
-define(ASSUME_CATCHUP_INTERVAL, 25).
%% How long we try to pull the durable subscriptions from the elector at handoff
%% (§4.9). This is best-effort.
-define(SUBS_PULL_TIMEOUT, 1000).

%% How often the maintenance pass prunes the release trail and kill budget (§5.6).
%% We stop pruning the trail while any member is disconnected. That member misses the
%% unregister broadcasts entirely, so the trail has to outlast the partition.
-define(PRUNE_INTERVAL, 30000).

%% How long a follower waits after asking for a resync before it may ask again. It asks
%% when it sees a gap in the leader's broadcast stream.
-define(RESYNC_RETRY, 2000).

%% How many names each `{monitor_sweep, …}` step monitors. See the `leader` enter call.
-define(MONITOR_SWEEP_CHUNK, 5000).

%% How often the leader sends its replication heartbeat, an empty
%% `{names_batch, [], …}` at its applied version. Without it, a follower that lost the
%% end of the stream, or lost its resync request, wouldn't notice the gap until the
%% next write.
-define(REPLICA_HEARTBEAT_INTERVAL, 5000).

%% How long we wait before re-enqueueing the ops of a failed batch, or surrendering
%% them if we were deposed. The delay keeps a backend that keeps failing from spinning.
-define(REQUEUE_DELAY, 1000).

%% How long we wait for each peer to answer a liveness probe while checking a
%% suspected conflict.
-define(ALIVE_PROBE_TIMEOUT, 1000).

-export([start_link/2]).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    code_change/4
]).
%% State functions. handle_event/4 calls them, not gen_statem.
-export([
    searching/3,
    assuming/3,
    leader/3,
    follower/3
]).
%% Exported for unit tests of the batch planner.
-export([plan_batch/4]).

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
    %% The table has no heir, so every start or restart creates it empty. The leader's
    %% snapshot fills it.
    Replica = dgen_registry_replica:new(dgen_registry:names_table(Name)),
    %% When a partition heals, `{nodeup, _}` makes the member send its `{join}` again.
    net_kernel:monitor_nodes(true),
    %% Look up telemetry now, while the tree starts, and cache the answer. Done lazily,
    %% it would be a synchronous `code_server` call inside whichever member emits
    %% first. Under `eta_run` simulation that call blocks on a process the scheduler
    %% doesn't own.
    _ = is_telemetry_available(),
    schedule_timer(?PRUNE_INTERVAL, prune_released),
    schedule_timer(?REPLICA_HEARTBEAT_INTERVAL, replica_heartbeat),
    %% We need the elector's pid to announce ourselves. `discover_elector` runs as an
    %% internal event, because `supervisor:which_children/1` would deadlock while the
    %% supervisor is still waiting on this init/1.
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

%% Taking and giving up leadership happens when the role changes, in the enter clauses
%% of the state functions. leader/3 takes it, and non_leader_enter/2 gives it up.
handle_event(enter, OldState, State, Data) ->
    apply(?MODULE, State, [enter, OldState, Data]);
%% -- calls ------------------------------------------------------------------

%% Answers the readiness probe from dgen_registry:await_ready/2. We are ready once we
%% know a leader and have synced.
handle_event({call, From}, ready, _State, Data) ->
    #data{leader = Leader, synced = Synced} = Data,
    {keep_state_and_data, [{reply, From, Leader =/= undefined andalso Synced}]};
%% Answers the status probe from dgen_registry:status/1. It reports what this member
%% *believes* about leadership, which can lag behind the elector during a handoff
%% (§5.1).
handle_event({call, From}, status, _State, Data) ->
    #data{
        member_id = Self,
        leader = Leader,
        synced = Synced,
        replica = #replica{epoch = Epoch, version = Applied}
    } = Data,
    Status = #{
        member_id => Self,
        leader => Leader,
        is_leader => Leader =:= Self,
        epoch => Epoch,
        synced => Synced,
        applied_version => Applied
    },
    {keep_state_and_data, [{reply, From, Status}]};
%% route_register/5 decides where the registration goes. The only reply sent right
%% away is a `no` from a leader that refuses it.
handle_event({call, From}, {register, LogicalName, Pid, Meta}, _State, Data) ->
    {keep_state, route_register(LogicalName, Pid, Meta, From, Data)};
handle_event({call, From}, {unregister, LogicalName}, _State, Data) ->
    {noreply, Data1} = route_unregister(LogicalName, From, Data),
    {keep_state, Data1};
%% Runs an indexed query. It runs in the mailbox, so it never sees half a batch (§6).
handle_event({call, From}, {query, Constraints}, _State, #data{replica = R}) ->
    {keep_state_and_data, [{reply, From, dgen_registry_replica:query_constraints(R, Constraints)}]};
%% Snapshot reads don't come here. The caller reads the table directly, without locks,
%% in `dgen_registry:whereis_name/1`.
%%
%% Answers the handoff gather, on any member. We return our records, applied version
%% and release trail. A new leader uses them to rebuild the names from the freshest
%% replica (§5.7, §4.4) and to merge trails (§5.6). We also say whether the leader we
%% followed is gone, which tells the new leader our replica can't catch up any further.
handle_event({call, From}, get_names_snapshot, _State, Data) ->
    #data{replica = R = #replica{version = Version}, conflicts = #conflicts{trail = Released}} =
        Data,
    %% We send the records as an off-heap binary. See dgen_registry_replica:encode/1.
    Reply = {dgen_registry_replica:encode(R), Version, Released, Data#data.leader_down},
    {keep_state_and_data, [{reply, From, Reply}]};
%% -- casts ------------------------------------------------------------------

%% Follower: the leader committed an unregister we forwarded. FIFO delivers its
%% {names_batch} first.
handle_event(cast, {unregister_reply, Ref}, _State, Data) ->
    {keep_state, apply_intent_reply(Ref, unregister, Data)};
%% Follower: FIFO delivers the matching {names_batch} before this reply, so a local
%% get_metadata right after it sees the new value.
handle_event(cast, {set_meta_reply, Ref, Result}, _State, Data) ->
    {keep_state, apply_intent_reply(Ref, {set_meta, Result}, Data)};
%% Follower: the leader's answer to a registration we forwarded, committed at
%% `Version` in `Epoch`. We only pass a `yes` on once our replica has applied
%% `Version` in that epoch (§5.5). That makes us a second holder the handoff gather
%% (§5.7) is sure to see. Usually FIFO has already delivered the batch. If we refused
%% the batch because of a gap, the `yes` waits for the resync. See
%% dgen_registry_intents.
handle_event(cast, {register_reply, Ref, Result, Version, Epoch}, _State, Data) ->
    {keep_state, apply_intent_reply(Ref, {register, Result, Version, Epoch}, Data)};
%% Follower: the leader refused a registration we forwarded without committing
%% anything, so there is no version. Only a `no` arrives without a version. A `yes`
%% always carries one.
handle_event(cast, {register_reply, Ref, no}, _State, Data) ->
    {keep_state, apply_intent_reply(Ref, {register, no, undefined, undefined}, Data)};
%% Follower: confirms to the leader that we hold a batch's bindings. FIFO delivers its
%% {names_batch} first. If we haven't applied that version, because of a gap or a stale
%% epoch, we must not claim to hold them.
handle_event(cast, {replicate_sync, BatchRef, LeaderId, Version}, _State, Data) ->
    #data{member_id = Self, replica = #replica{version = Applied}} = Data,
    case Applied >= Version of
        true -> cast_to_member(LeaderId, {replicate_ack, BatchRef, Self});
        false -> ok
    end,
    keep_state_and_data;
%% Leader: once `register_replicas` different followers have acked, we answer the
%% batch's direct registrations `yes`.
handle_event(cast, {replicate_ack, BatchRef, FollowerId}, _State, Data = #data{pipeline = P}) ->
    case dgen_registry_pipeline:record_ack(BatchRef, FollowerId, P) of
        {done, Direct, TimerRef, P1} ->
            erlang:cancel_timer(TimerRef),
            deliver_direct_yes(Direct),
            {keep_state, Data#data{pipeline = P1}};
        {waiting, P1} ->
            {keep_state, Data#data{pipeline = P1}}
    end;
%% The replication broadcast, one message per committed batch. dgen_registry_replica
%% only accepts it if `PrevV` matches our version, so our replica stays a prefix of the
%% leader's stream. On a gap we ask for a resync. Applying the batch as a unit keeps
%% "same applied_version means same content" true. See broadcast_batch/5.
handle_event(
    cast, {names_batch, Ops, Epoch, PrevV, Version, LeaderId}, _State, Data = #data{replica = R}
) ->
    Data1 =
        case dgen_registry_replica:offer_batch(Epoch, PrevV, Version, Ops, R) of
            {applied, R1} ->
                release_deferred_acks(record_releases(Ops, cancel_resync(Data#data{replica = R1})));
            gap ->
                request_resync(LeaderId, Data);
            _StaleEpochOrDuplicate ->
                Data
        end,
    {keep_state, Data1};
%% A snapshot from the new leader. The replica, the leader change and the member
%% monitors all arrive in one cast, so nothing can slip in between them. Its version
%% becomes our new starting point.
handle_event(
    cast,
    {apply_names_snapshot, RecordsPayload, NewLeader, ExtraMembers, Tokens, Epoch, Version},
    _State,
    Data
) ->
    #data{member_id = Self, leader = OldLeader, replica = R} = Data,
    %% We check the version as well as the epoch, so a late snapshot can never move
    %% the replica backward. The TLA+ spec found this race in the handoff gather.
    %% Refusing a snapshot is harmless, because gap detection and resync catch us up.
    Records = dgen_registry_replica:decode_records(RecordsPayload),
    case dgen_registry_replica:offer_snapshot(Epoch, Version, Records, R) of
        {accepted, R1} ->
            %% Update the replica before the leader changes, because the `leader`
            %% enter call reads the names back from the table.
            Data0 = cancel_resync(Data#data{replica = R1}),
            %% redrive_intents/2 below re-routes forwarded intents. The state enter
            %% calls take or give up leadership. If we just lost leadership, we
            %% resolve direct registrations still waiting for replica acks, so their
            %% timers don't fire on a follower.
            Data1 =
                case OldLeader =:= Self andalso NewLeader =/= Self of
                    true -> fail_pending_acks(Data0#data{leader = NewLeader, leader_down = false});
                    false -> Data0#data{leader = NewLeader, leader_down = false}
                end,
            %% The snapshot replaces any assume gather still in flight.
            Data2 = Data1#data{assume_ref = undefined},
            Data3 = add_member_monitors(ExtraMembers, Data2),
            Data4 = merge_peer_tokens(Tokens, Data3),
            Data5 = maybe_start_commit(
                redrive_intents(moved, release_deferred_acks(mark_synced(Data4)))
            ),
            %% Move to the role the snapshot names. This works from any state,
            %% including `assuming`, whose gather the snapshot replaces.
            NextState =
                case NewLeader of
                    Self -> leader;
                    undefined -> searching;
                    _ -> follower
                end,
            {next_state, NextState, Data5};
        stale ->
            keep_state_and_data
    end;
%% Sent by a leader taking the fast path for a joiner. An up-to-date follower only
%% monitors the joiner and records its token. The token lets the elector tell a later
%% DOWN for this join apart from a stale one.
handle_event(cast, {peer_joined, MemberId, Tokens}, _State, Data) ->
    {keep_state, merge_peer_tokens(Tokens, add_member_monitors([MemberId], Data))};
%% -- infos ------------------------------------------------------------------

%% Too few replicas acked these direct registrations in time (§8). By default we
%% degrade open: we answer `yes` with only the leader holding the name, and emit
%% telemetry. With `strict_replication` we fail closed: we answer `no` and retract the
%% bindings.
handle_event(info, {replicate_timeout, BatchRef}, _State, Data) ->
    #data{pipeline = P, config = Config} = Data,
    case dgen_registry_pipeline:handle_ack_timeout(BatchRef, P) of
        {expired, Direct, Needed, Got, P1} ->
            Data1 = Data#data{pipeline = P1},
            case dgen_config:strict_replication(Config) of
                false ->
                    notify_degrade_open(Needed, Got, length(Direct)),
                    deliver_direct_yes(Direct),
                    {keep_state, Data1};
                true ->
                    {keep_state, reject_and_retract_direct(Direct, Data1)}
            end;
        none ->
            keep_state_and_data
    end;
handle_event(info, {dgen_transaction, Ref, Reply}, _State, Data = #data{pipeline = P}) ->
    case dgen_registry_pipeline:finish_commit(Ref, P) of
        {ok, Plan, MRef, P1} ->
            %% The result arrives before the worker's DOWN, so `flush` drops the DOWN.
            erlang:demonitor(MRef, [flush]),
            Data1 = handle_commit_result(Reply, Plan, Data#data{pipeline = P1}),
            {keep_state, maybe_start_commit(Data1)};
        stale ->
            keep_state_and_data
    end;
%% A registered process died. Only the leader monitors them. The DOWN carries `Ref`,
%% and the planner ignores it if the name has since been bound again under a new
%% monitor. If we no longer lead, the new leader monitors the pid itself and removes
%% the name.
handle_event(info, {{down, LogicalName}, Ref, process, _Pid, _Reason}, _State, Data) ->
    {keep_state, enqueue_op({down, LogicalName, Ref}, Data)};
handle_event(info, {'DOWN', Ref, process, _Pid, _Reason}, _State, Data) ->
    #data{
        monitors = Monitors,
        elector = Elector,
        member_id = Self,
        leader = Leader,
        leader_down = LeaderDown
    } = Data,
    case maps:get(Ref, Monitors, undefined) of
        undefined ->
            %% Not a peer monitor. It is the commit worker or a late leftover.
            case dgen_registry_pipeline:handle_worker_down(Ref, Data#data.pipeline) of
                {ok, Plan, P1} ->
                    Data1 = Data#data{pipeline = P1},
                    {keep_state, maybe_start_commit(surrender(failed, maps:get(ops, Plan), Data1))};
                stale ->
                    keep_state_and_data
            end;
        Self ->
            keep_state_and_data;
        DeadMemberId ->
            %% Send {member_down} with the elector's *current* token for the peer, not
            %% our cached one. A dropped snapshot may have left ours stale, and the
            %% elector would ignore it. The dead member would then stay listed and
            %% block failover (§5.7). A helper reads the token, never this loop,
            %% because a call to the elector could deadlock against its own
            %% `{elector_assume_and_distribute}` call into this member.
            Cached = maps:get(DeadMemberId, Data#data.peer_tokens, undefined),
            spawn_member_down(Elector, DeadMemberId, Cached),
            %% If it was our leader, nothing more from it can reach us now.
            Data1 = Data#data{leader_down = LeaderDown orelse DeadMemberId =:= Leader},
            {keep_state, remove_member(DeadMemberId, Data1)}
    end;
handle_event(info, {nodeup, _Node}, _State, Data) ->
    #data{elector = Elector, member_id = Self} = Data,
    %% We may have been removed during a partition. We join again with a fresh token,
    %% so the elector discards any queued {member_down, Self, OldToken}.
    NewToken = make_ref(),
    dgen_registry_elector:join(Elector, Self, NewToken, not Data#data.synced),
    %% The partition healed. If our leader is reachable, we re-drive intents now
    %% instead of waiting for the onboarding snapshot. Re-driving is idempotent, and it
    %% makes sure an unregister we were holding, or one that was lost, still happens
    %% (Non-goal 5).
    Data1 = Data#data{join_token = NewToken},
    case get_intent_route(Data1) of
        none -> {keep_state, Data1};
        _Reachable -> {keep_state, redrive_intents(all, Data1)}
    end;
%% Periodic maintenance. Prunes the release trail and kill budget (§5.6), and drops
%% stale intents.
handle_event(info, prune_released, _State, Data) ->
    #data{conflicts = C, members = Members, config = Config} = Data,
    schedule_timer(?PRUNE_INTERVAL, prune_released),
    Now = erlang:system_time(millisecond),
    AllConnected = lists:all(
        fun({Node, _Name}) -> dgen_utils:node_reachable(Node) end,
        maps:keys(Members)
    ),
    C1 = dgen_registry_conflicts:prune(Now, AllConnected, Config, C),
    {keep_state, prune_intents(Data#data{conflicts = C1})};
%% Nobody answered our resync request. We clear the timer so the next gap or heartbeat
%% can ask again.
handle_event(info, resync_timeout, _State, Data) ->
    {keep_state, Data#data{resync_timer = undefined}};
%% The connector saw that the durable election epoch is ahead of ours, so a handoff
%% never reached us. Joining again makes the leader assume and send snapshots again.
handle_event(info, {durable_epoch, DurableEpoch}, _State, Data) when is_integer(DurableEpoch) ->
    #data{replica = #replica{epoch = Epoch}} = Data,
    case DurableEpoch > Epoch of
        true ->
            #data{elector = Elector, member_id = Self, join_token = Token, synced = Synced} =
                Data,
            dgen_registry_elector:join(Elector, Self, Token, not Synced);
        false ->
            ok
    end,
    keep_state_and_data;
handle_event(info, {durable_epoch, _DurableEpoch}, _State, _Data) ->
    keep_state_and_data;
%% -- internal ---------------------------------------------------------------

%% This is safe here because the supervisor is no longer waiting on our init/1.
handle_event(
    internal, discover_elector, _State, Data = #data{member_id = {_Node, Name} = MemberId}
) ->
    #data{join_token = Token} = Data,
    Elector = dgen_registry:elector_pid(Name),
    true = is_pid(Elector),
    %% We pass `fresh = true`. We have never synced, so we can't hold any bindings.
    dgen_registry_elector:join(Elector, MemberId, Token, true),
    {keep_state, Data#data{elector = Elector}};
%% -- the current state handles everything else ------------------------------

handle_event(EventType, EventContent, State, Data) ->
    apply(?MODULE, State, [EventType, EventContent, Data]).

%% ---------------------------------------------------------------------------
%% State functions
%% ---------------------------------------------------------------------------
%%
%% handle_event/4 calls these for the enter call and for every event it doesn't handle
%% itself. The state mirrors the data. In `leader`, the `leader` field equals
%% `member_id`. In `follower`, it names another member. In `searching` and `assuming`
%% it is `undefined`. We only ever set `assume_ref` while `leader` is `undefined`.
%%
%% Events a state doesn't handle fall through like this:
%%   searching, assuming -> leaderless/3 -> non_leader/3 -> unhandled/3
%%   follower -> non_leader/3 -> unhandled/3
%%   leader -> unhandled/3

%% -- leader -----------------------------------------------------------------

%% Taking and giving up leadership both happen in enter calls, so no path between
%% states can do one without the other. Two things are deliberately not here.
%% redrive_intents/2 also runs on leadership events that don't change the state.
%% fail_pending_acks/1 stays in the apply_names_snapshot handler, because when a leader
%% assumes again, `BatchRef` already makes its old acks harmless.
%%
%% We don't monitor registered processes here all at once. Instead we send ourselves
%% {monitor_sweep, …} messages that each monitor one chunk, tagged with the epoch, and
%% live traffic runs between them. This is safe because monitoring a pid that is
%% already dead or unreachable fires a DOWN right away. A process that dies before its
%% chunk is still caught. We just don't promise to clean it up promptly.
leader(enter, _OldState, Data) ->
    #data{replica = #replica{epoch = Epoch}} = Data,
    self() ! {monitor_sweep, Epoch, start},
    %% Seed the watch sets so the first commit computes its changes correctly (§4.9).
    {keep_state, rebuild_sub_matches(Data)};
%% If we stay leader in the same epoch, we handle a join on the fast path. Otherwise
%% leadership really is changing. We check the epoch in the data, because the `leader`
%% state doesn't say which term we lead.
leader(
    {call, From},
    {elector_assume_and_distribute, MemberId, AllIds, Tokens, FreshIds, Epoch},
    Data
) ->
    #data{synced = Synced, replica = #replica{epoch = CurrentEpoch}} = Data,
    case Synced andalso MemberId =/= undefined andalso CurrentEpoch =:= Epoch of
        true ->
            Data1 = assume_leadership_fast_path(MemberId, AllIds, Tokens, FreshIds, Epoch, Data),
            {keep_state, Data1, [{reply, From, ok}]};
        false ->
            %% Give up leadership and start a gather. See
            %% prepare_to_assume_leadership/6.
            Data1 = prepare_to_assume_leadership(MemberId, AllIds, Tokens, FreshIds, Epoch, Data),
            {next_state, assuming, Data1, [{reply, From, ok}]}
    end;
leader({call, From}, {set_metadata, LogicalName, Index, MetaData}, Data) ->
    {keep_state, enqueue_op({set_meta, LogicalName, Index, MetaData, {meta, From}}, Data)};
%% Consistent reads (Guarantee 5). We capture the value now. A spawned helper then
%% checks the durable leader key before it replies, so a deposed leader can't give an
%% authoritative answer.
leader({call, From}, {get_metadata, LogicalName}, Data = #data{replica = R}) ->
    reply_fenced(From, dgen_registry_replica:lookup_metadata(R, LogicalName), undefined, Data),
    keep_state_and_data;
leader({call, From}, {query_consistent, Constraints}, Data = #data{replica = R}) ->
    reply_fenced(From, dgen_registry_replica:query_constraints(R, Constraints), [], Data),
    keep_state_and_data;
leader({call, From}, {whereis, LogicalName}, Data = #data{replica = R}) ->
    reply_fenced(From, dgen_registry_replica:lookup_pid(R, LogicalName), undefined, Data),
    keep_state_and_data;
%% A forwarded registration. We answer with {register_reply} once it commits.
leader(cast, {register_req, Ref, FollowerId, LogicalName, Pid, Meta}, Data) ->
    case should_reject_when_degraded(LogicalName, Data) of
        true ->
            cast_to_member(FollowerId, {register_reply, Ref, no}),
            keep_state_and_data;
        false ->
            {keep_state,
                enqueue_op({add, LogicalName, Pid, Meta, {forward, FollowerId, Ref}}, Data)}
    end;
%% A forwarded unregister. We answer with {unregister_reply} once it commits.
leader(cast, {unregister_req, Ref, FollowerId, LogicalName}, Data = #data{replica = R}) ->
    ReleasedPid = dgen_registry_replica:lookup_pid(R, LogicalName),
    {keep_state,
        enqueue_op(
            {remove, LogicalName, ReleasedPid, {forward_unreg, FollowerId, Ref}},
            delete_row(Data, LogicalName)
        )};
%% A forwarded set_metadata. We answer with {set_meta_reply} once it commits.
leader(cast, {set_meta_req, Ref, FollowerId, LogicalName, Index, MetaData}, Data) ->
    {keep_state,
        enqueue_op(
            {set_meta, LogicalName, Index, MetaData, {forward_meta, FollowerId, Ref}}, Data
        )};
%% A forwarded consistent read. We reply straight to the caller's `From`, after the
%% same leadership check as a local read.
leader(cast, {get_metadata_req, LogicalName, From}, Data = #data{replica = R}) ->
    reply_fenced(From, dgen_registry_replica:lookup_metadata(R, LogicalName), undefined, Data),
    keep_state_and_data;
leader(cast, {query_req, Constraints, From}, Data = #data{replica = R}) ->
    reply_fenced(From, dgen_registry_replica:query_constraints(R, Constraints), [], Data),
    keep_state_and_data;
leader(cast, {whereis_req, LogicalName, From}, Data = #data{replica = R}) ->
    reply_fenced(From, dgen_registry_replica:lookup_pid(R, LogicalName), undefined, Data),
    keep_state_and_data;
%% Answers a resync request with a snapshot at our epoch and applied version.
leader(cast, {resync_req, FollowerId}, Data) ->
    #data{leader = Leader, replica = R = #replica{epoch = Epoch, version = Version}} = Data,
    RecordsBin = dgen_registry_replica:encode(R),
    cast_to_member(
        FollowerId, {apply_names_snapshot, RecordsBin, Leader, [], #{}, Epoch, Version}
    ),
    keep_state_and_data;
%% A retract forwarded by a member that couldn't commit it. With `strict_replication`,
%% a binding we answered `no` must not stay registered (§8), so it gets retracted. The
%% retract only clears the name if the same pid still holds it, so it never removes a
%% later registration.
leader(cast, {retract_req, LogicalName, Pid}, Data) ->
    {keep_state, enqueue_op({retract, LogicalName, Pid}, Data)};
%% A change to the presence subscriptions, pushed by the elector (§4.9). We drop a
%% push from an older epoch. The next leader pulls the full set when it assumes.
leader(cast, {presence_update, Update, Epoch}, Data) ->
    #data{replica = #replica{epoch = CurrentEpoch}} = Data,
    case Epoch >= CurrentEpoch of
        true -> {keep_state, apply_presence_update(Update, Data)};
        false -> keep_state_and_data
    end;
%% The second half of the fast path, for a member rejoining with old data. We check
%% its replica for conflicts against our table, which is authoritative (§5.6), merge
%% its trail, and then onboard it. If repair kills a pid we hold, its DOWN cleans up
%% the name as usual. We only act on this in the term that spawned the gather.
leader(
    info,
    {joiner_gathered, MemberId, AllIds, Tokens, Epoch, Result},
    Data = #data{members = Members}
) when is_map_key(MemberId, Members) ->
    #data{replica = R = #replica{epoch = CurrentEpoch}} = Data,
    case CurrentEpoch =:= Epoch of
        true ->
            Data1 =
                case Result of
                    {ok, {Records, _Version, Released}, Alive} ->
                        %% We discard the resolved map. Here, repair only kills
                        %% processes and raises alarms.
                        OwnRecords = dgen_registry_replica:get_records(R),
                        {_Records, D1} = repair_conflicts(
                            OwnRecords, [Records], [Released], Alive, Data
                        ),
                        D1;
                    error ->
                        %% The joiner didn't answer. We onboard it anyway, and any
                        %% divergence shows up at the next gather.
                        Data
                end,
            {keep_state, cast_data_to_new_member(MemberId, AllIds, Tokens, Data1)};
        false ->
            keep_state_and_data
    end;
%% One step of the monitor sweep that the `leader` enter call started. Each message
%% handles one chunk, and only in the epoch that started the sweep. We skip a name
%% that is already monitored because it committed after the sweep began. We also skip
%% a name bound to a different pid since its chunk was read. A pid that died before
%% its chunk gets an immediate `noproc` DOWN, so the delay loses nothing.
leader(info, {monitor_sweep, Epoch, Cont0}, Data) ->
    #data{replica = R = #replica{epoch = CurrentEpoch}} = Data,
    case CurrentEpoch =:= Epoch of
        true -> {keep_state, monitor_sweep(Epoch, Cont0, R, Data)};
        false -> keep_state_and_data
    end;
%% Retries the ops of a failed batch while we still lead. We drop registrations whose
%% caller has died.
leader(info, {requeue_ops, Ops}, Data) ->
    Live = [Op || Op <- Ops, is_caller_alive(Op)],
    {keep_state, maybe_start_commit(lists:foldl(fun enqueue_op/2, Data, Live))};
%% Every member keeps this timer running, so a member that later becomes leader
%% starts sending heartbeats right away.
leader(info, replica_heartbeat, Data) ->
    broadcast_heartbeat(Data),
    schedule_timer(?REPLICA_HEARTBEAT_INTERVAL, replica_heartbeat),
    keep_state_and_data;
leader(EventType, EventContent, Data) ->
    unhandled(EventType, EventContent, Data).

%% -- follower ---------------------------------------------------------------

follower(enter, OldState, Data) ->
    non_leader_enter(OldState, Data);
%% Consistent reads. The leader replies to `From` directly, so this loop never waits
%% on it. If the leader is unreachable, we answer with the default value now.
follower({call, From}, {get_metadata, LogicalName}, Data = #data{leader = Leader}) ->
    forward_read(Leader, {get_metadata_req, LogicalName, From}, From, undefined, Data);
follower({call, From}, {query_consistent, Constraints}, Data = #data{leader = Leader}) ->
    forward_read(Leader, {query_req, Constraints, From}, From, [], Data);
follower({call, From}, {whereis, LogicalName}, Data = #data{leader = Leader}) ->
    forward_read(Leader, {whereis_req, LogicalName, From}, From, undefined, Data);
follower(cast, {retract_req, LogicalName, Pid}, #data{leader = Leader}) ->
    cast_to_member(Leader, {retract_req, LogicalName, Pid}),
    keep_state_and_data;
follower(EventType, EventContent, Data) ->
    non_leader(EventType, EventContent, Data).

%% -- searching --------------------------------------------------------------

searching(enter, OldState, Data) ->
    non_leader_enter(OldState, Data);
searching(EventType, EventContent, Data) ->
    leaderless(EventType, EventContent, Data).

%% -- assuming ---------------------------------------------------------------

%% A leader that moves to `assuming` gives up leadership as the assume starts. If a
%% registered process dies during the gather, the next monitor sweep gets an immediate
%% `noproc` DOWN for it.
assuming(enter, OldState, Data) ->
    non_leader_enter(OldState, Data);
%% The handoff gather finished. This clause only matches if `Ref` is still our
%% `assume_ref`. Otherwise a newer assume replaced it, and the result is dropped. The
%% freshest map becomes our state. That is safe because broadcasts are totally ordered
%% and every replica is a prefix of the stream. `MaxVersion` becomes the followers'
%% new starting version.
assuming(
    info,
    {assume_gathered, {MemberId, AllIds, Tokens, FreshIds, Epoch, Ref}, SelfRecords,
        FreshestRecords, {MaxVersion, Frontier}, PeerResults, Subs, Alive},
    Data = #data{assume_ref = Ref}
) ->
    #data{member_id = Self} = Data,
    OtherIds = lists:delete(Self, AllIds),
    Peers = maps:values(PeerResults),
    Degraded = dgen_registry_conflicts:is_reconstruction_degraded(
        MaxVersion, Frontier, OtherIds, FreshIds, PeerResults
    ),
    %% Repair the conflicts the gather found (§5.6). Names in conflict are dropped.
    {Records, Data0} = repair_conflicts(
        FreshestRecords,
        [SelfRecords | [Recs || {Recs, _V, _Rel} <- Peers]],
        [Rel || {_Recs, _V, Rel} <- Peers],
        Alive,
        Data
    ),
    %% We lead now. The move to `leader` below runs its enter call against the rebuilt
    %% replica and `Subs` (§4.9).
    Data2 = Data0#data{
        replica = dgen_registry_replica:adopt(Epoch, MaxVersion, Records, Data0#data.replica),
        leader = Self,
        leader_down = false,
        conflicts = dgen_registry_conflicts:set_degraded(Degraded, Data0#data.conflicts),
        assume_ref = undefined,
        subs = Subs
    },
    Data3 = add_member_monitors(get_extra_member_ids(MemberId, AllIds, Self), Data2),
    Data4 = merge_peer_tokens(Tokens, Data3),
    %% Encode once. Every cast shares the same refc binary.
    RecordsBin = dgen_registry_replica:encode_records(Records),
    lists:foreach(
        fun(Id) ->
            cast_to_member(
                Id,
                {apply_names_snapshot, RecordsBin, Self, get_extra_member_ids(MemberId, AllIds, Id),
                    Tokens, Epoch, MaxVersion}
            )
        end,
        OtherIds
    ),
    %% Re-drive every unresolved intent into our own pipeline.
    {next_state, leader,
        maybe_start_commit(redrive_intents(moved, mark_synced(cancel_resync(Data4))))};
%% We postpone a forwarded registration that arrives during an assume until the assume
%% finishes. If another assume replaces ours, we never answer it, and the forwarder
%% re-drives it.
assuming(cast, {register_req, _Ref, _FollowerId, _Name, _Pid, _Meta}, _Data) ->
    {keep_state_and_data, [postpone]};
assuming(EventType, EventContent, Data) ->
    leaderless(EventType, EventContent, Data).

%% -- shared by the non-leader states ----------------------------------------

%% Leaving `leader`. We demonitor every registered pid but keep the replica for
%% snapshot reads. We also drop the commit version the pipeline cached, so a later
%% term can't use it as its read version.
non_leader_enter(leader, Data) ->
    #data{name_to_ref = NTR, pipeline = P} = Data,
    maps:foreach(fun(_LogicalName, Ref) -> erlang:demonitor(Ref, [flush]) end, NTR),
    {keep_state, Data#data{name_to_ref = #{}, pipeline = dgen_registry_pipeline:start_new_term(P)}};
non_leader_enter(_OldState, _Data) ->
    keep_state_and_data.

%% Shared by `searching` and `assuming`, where there is no leader to forward to.
leaderless({call, From}, {get_metadata, _LogicalName}, _Data) ->
    {keep_state_and_data, [{reply, From, undefined}]};
leaderless({call, From}, {query_consistent, _Constraints}, _Data) ->
    {keep_state_and_data, [{reply, From, []}]};
leaderless({call, From}, {whereis, _LogicalName}, _Data) ->
    {keep_state_and_data, [{reply, From, undefined}]};
%% There is no leader, most likely because a handoff is under way. We hold the retract
%% as an unregister intent marked `answered`, so it is re-driven later instead of lost.
leaderless(cast, {retract_req, LogicalName, Pid}, Data) ->
    {keep_state, submit_intent({unregister, LogicalName, Pid}, answered, Data)};
leaderless(EventType, EventContent, Data) ->
    non_leader(EventType, EventContent, Data).

%% Shared by `searching`, `assuming` and `follower`.
non_leader(
    {call, From}, {elector_assume_and_distribute, MemberId, AllIds, Tokens, FreshIds, Epoch}, Data
) ->
    %% Move to `assuming`. We do this even from `assuming`, because the newer gather
    %% replaces ours.
    Data1 = prepare_to_assume_leadership(MemberId, AllIds, Tokens, FreshIds, Epoch, Data),
    {next_state, assuming, Data1, [{reply, From, ok}]};
%% Forwards the write as an intent. The reply comes back through this member. FIFO
%% delivers the leader's `{names_batch}` before it, so our row is already updated and
%% a local snapshot read right after sees the new value.
non_leader({call, From}, {set_metadata, LogicalName, Index, MetaData}, Data) ->
    {keep_state, submit_intent({set_meta, LogicalName, Index, MetaData}, From, Data)};
%% The sender thinks we lead, but we don't. We reject it so the caller retries.
non_leader(cast, {set_meta_req, Ref, FollowerId, _LogicalName, _Index, _MetaData}, _Data) ->
    cast_to_member(FollowerId, {set_meta_reply, Ref, {error, no_leader}}),
    keep_state_and_data;
%% The sender thinks we lead, but we don't. We answer with the empty value, and the
%% caller can retry.
non_leader(cast, {get_metadata_req, _LogicalName, From}, _Data) ->
    gen_server:reply(From, undefined),
    keep_state_and_data;
non_leader(cast, {query_req, _Constraints, From}, _Data) ->
    gen_server:reply(From, []),
    keep_state_and_data;
non_leader(cast, {whereis_req, _LogicalName, From}, _Data) ->
    gen_server:reply(From, undefined),
    keep_state_and_data;
%% We were deposed, so we surrender the ops of the failed batch. See
%% dgen_registry_pipeline:surrender/2.
non_leader(info, {requeue_ops, Ops}, Data) ->
    {keep_state, surrender(deposed, Ops, Data)};
non_leader(info, replica_heartbeat, _Data) ->
    schedule_timer(?REPLICA_HEARTBEAT_INTERVAL, replica_heartbeat),
    keep_state_and_data;
non_leader(EventType, EventContent, Data) ->
    unhandled(EventType, EventContent, Data).

%% Events the current state ignores. A non-leader doesn't answer leader-only messages
%% such as `register_req`, `unregister_req`, `resync_req` and `presence_update`. Only
%% the leader may decide them (§3), and the forwarder re-drives. Gather results that
%% were replaced and sweep steps from an old epoch also end up here.
unhandled({call, From}, _Request, _Data) ->
    {keep_state_and_data, [{reply, From, {error, unknown_call}}]};
unhandled(_EventType, _EventContent, _Data) ->
    keep_state_and_data.

forward_read(Leader, Req, From, Default, _Data) ->
    case is_member_reachable(Leader) of
        true ->
            cast_to_member(Leader, Req),
            keep_state_and_data;
        false ->
            {keep_state_and_data, [{reply, From, Default}]}
    end.

%% ---------------------------------------------------------------------------
%% Elector calls (dispatched from the state functions)
%% ---------------------------------------------------------------------------

%% These handle `{elector_assume_and_distribute, …}`. `MemberId` is the member whose
%% join triggered the call, or `undefined` when a member went down. `AllIds` is the
%% full member list.
%%
%% The fast path. A leader that stays leader in this epoch and has synced is
%% authoritative, because every follower holds a prefix of its stream. So it skips the
%% gather and doesn't resend snapshots to the followers. It only monitors and onboards
%% the joiner.
assume_leadership_fast_path(
    MemberId, AllIds, Tokens, FreshIds, Epoch, Data = #data{member_id = Self}
) ->
    Data1 = merge_peer_tokens(
        Tokens, add_member_monitors(get_extra_member_ids(MemberId, AllIds, Self), Data)
    ),
    %% A fresh joiner holds no bindings, so we onboard it now. A member that synced
    %% before and is rejoining may hold bindings that disagree with ours, for example
    %% after a partition heals (§5.6). We gather its replica off the loop and check it
    %% for conflicts in {joiner_gathered} first. This check is best-effort.
    case MemberId =:= Self orelse lists:member(MemberId, FreshIds) of
        true ->
            cast_data_to_new_member(MemberId, AllIds, Tokens, Data1);
        false ->
            spawn_member_gather(MemberId, AllIds, Tokens, Epoch, Data1),
            Data1
    end.

prepare_to_assume_leadership(MemberId, AllIds, Tokens, FreshIds, Epoch, Data) ->
    #data{
        member_id = Self,
        replica = R = #replica{version = Applied},
        elector = Elector,
        tenant = Tenant,
        tuid = Tuid,
        conflicts = C
    } = Data,
    %% Leadership is really changing. The gather asks every peer for its replica, so it
    %% runs off the loop and we answer the elector `ok` now. A gather on the loop would
    %% time out the elector's call and hold up queued membership changes. We capture
    %% our own records and version now, so a broadcast that lands during the gather
    %% can't change which replica counts as freshest.
    SelfRecords = dgen_registry_replica:get_records(R),
    OtherIds = lists:delete(Self, AllIds),
    Ref = make_ref(),
    spawn_leadership_gather(
        {MemberId, AllIds, Tokens, FreshIds, Epoch, Ref},
        SelfRecords,
        {Applied, Data#data.leader_down},
        OtherIds,
        Elector,
        self(),
        Tenant,
        Tuid,
        C
    ),
    %% We have no leader until the gather finishes. Writes wait as intents and are
    %% re-driven once it does, instead of going to the old leader, which may be dead.
    Data#data{assume_ref = Ref, leader = undefined, leader_down = false}.

apply_presence_update({subscribe, SubId, Watch, Notify}, Data = #data{subs = Subs}) ->
    seed_subscription(SubId, Data#data{subs = Subs#{SubId => {Watch, Notify}}});
apply_presence_update({unsubscribe, SubId}, Data) ->
    #data{subs = Subs, sub_matches = SM, notify_matches = NM} = Data,
    Data#data{
        subs = maps:remove(SubId, Subs),
        sub_matches = maps:remove(SubId, SM),
        notify_matches = maps:remove(SubId, NM)
    };
apply_presence_update(unsubscribe_all, Data) ->
    Data#data{subs = #{}, sub_matches = #{}, notify_matches = #{}}.

%% Monitors one chunk of the sweep, then sends the next step to ourselves until the
%% table is done.
monitor_sweep(Epoch, Cont0, R, Data) ->
    #replica{tab = Tab} = R,
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

%% Rebuilds the watch and notify sets of every subscription from the replica. This only
%% runs on assume, and apply_committed_plan/3 keeps the sets current after that. It
%% sends no snapshots.
rebuild_sub_matches(Data) ->
    #data{subs = Subs, replica = R} = Data,
    SM = maps:map(
        fun(_SubId, {Watch, _Notify}) -> dgen_registry_replica:query_match_names(R, Watch) end,
        Subs
    ),
    NM = maps:map(
        fun(_SubId, {_Watch, Notify}) -> dgen_registry_replica:query_match_names(R, Notify) end,
        Subs
    ),
    Data#data{sub_matches = SM, notify_matches = NM}.

%% Gathers our replica and every reachable peer's, and picks the freshest. Broadcasts
%% are totally ordered and FIFO, and every replica is a prefix of the stream. So the
%% replica with the highest applied_version has the latest binding for every name, and
%% two replicas at the same version have the same content. We also return the peer
%% snapshots for conflict detection (§5.6) and the trail merge, and whether every peer
%% answered with its leader gone. The peer calls run in parallel.
gather_maps(SelfNames, SelfVersion, OtherIds) ->
    {PeerResults, GoneIds} = gather_peer_snapshots(OtherIds),
    PeersSettled = lists:sort(GoneIds) =:= lists:sort(OtherIds),
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
    {FreshestNames, MaxVersion, PeerResults, PeersSettled}.

%% Onboards a joiner on the fast path. We send the joiner our snapshot and the member
%% list. We tell the existing followers to monitor it with {peer_joined}, and leave
%% their replicas alone.
cast_data_to_new_member(MemberId, AllIds, Tokens, Data) ->
    #data{member_id = Self, replica = R = #replica{epoch = Epoch, version = Version}} = Data,
    RecordsBin = dgen_registry_replica:encode(R),
    cast_to_member(
        MemberId, {apply_names_snapshot, RecordsBin, Self, AllIds, Tokens, Epoch, Version}
    ),
    lists:foreach(
        fun(Id) -> cast_to_member(Id, {peer_joined, MemberId, Tokens}) end,
        [Id || Id <- AllIds, Id =/= Self, Id =/= MemberId]
    ),
    Data.

%% Fetches a rejoining member's replica and checks whether the pids in conflict are
%% alive, then reports {joiner_gathered, …}. Both steps are RPCs, so they run in a
%% helper instead of on the leader's loop. The helper can read our table because it is
%% `protected`.
spawn_member_gather(MemberId, AllIds, Tokens, Epoch, Data) ->
    #data{replica = Replica, conflicts = C} = Data,
    Owner = self(),
    _ = spawn(fun() ->
        Result =
            case get_peer_snapshot(MemberId) of
                {ok, {Records, _Version, Released} = Snapshot, _LeaderGone} ->
                    #conflicts{trail = Trail} = dgen_registry_conflicts:merge([Released], C),
                    %% Our table goes away if we die. Then there is no one to report
                    %% to anyway.
                    Alive =
                        try dgen_registry_replica:get_records(Replica) of
                            Own ->
                                check_alive(
                                    dgen_registry_conflicts:get_conflict_pids(Own, [Records], Trail)
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

%% Runs the full handoff gather (§5.7) off the loop and reports {assume_gathered, …}.
%% `Ctx` carries the assume arguments and the ref we match against `assume_ref`. The
%% helper also fetches the durable subscriptions (§4.9) and runs the liveness probes
%% (§5.6). A synchronous call to the elector from the member loop could deadlock
%% against the elector's own assume call into this member.
spawn_leadership_gather(Ctx, SelfRecords, SelfState, OtherIds, Elector, Owner, Tenant, Tuid, C) ->
    _ = spawn(fun() ->
        %% If the gather throws, gather_until_caught_up/5 falls back to our own records,
        %% so we always send {assume_gathered, …}.
        {FreshestRecords, MaxVersion, PeerResults, Frontier} =
            gather_until_caught_up(SelfRecords, SelfState, OtherIds, Tenant, Tuid),
        Subs = fetch_subscriptions(Elector),
        Peers = maps:values(PeerResults),
        #conflicts{trail = Merged} =
            dgen_registry_conflicts:merge([Rel || {_Recs, _V, Rel} <- Peers], C),
        Witnesses = [SelfRecords | [Recs || {Recs, _V, _Rel} <- Peers]],
        Alive = check_alive(
            dgen_registry_conflicts:get_conflict_pids(FreshestRecords, Witnesses, Merged)
        ),
        Owner !
            {assume_gathered, Ctx, SelfRecords, FreshestRecords, {MaxVersion, Frontier},
                PeerResults, Subs, Alive}
    end),
    ok.

%% Gathers again until `MaxVersion` reaches the durable version key, or until
%% ?GATHER_TIMEOUT passes. This is the SafeAssume guard in the TLA+ spec. No live
%% member is ever ahead of that key. A lower `MaxVersion` means a committed broadcast
%% was still queued in some peer's mailbox when we gathered. Sending out that stale map
%% would lose a registration we already acked. After the deadline we go ahead anyway,
%% and the {assume_gathered} handler marks the leader `degraded`.
%%
%% We stop early when waiting can't help. The old leader may have died after a commit
%% but before broadcasting it, and then no live member will ever reach the frontier.
%% Erlang delivers a process's messages before its DOWN. So once we and every peer have
%% handled the old leader's DOWN, nothing more from it can arrive, and we go ahead
%% `degraded` without waiting out the deadline. `SelfLeaderGone` is our own answer,
%% taken when we captured our records.
gather_until_caught_up(SelfRecords, {SelfVersion, SelfLeaderGone}, OtherIds, Tenant, Tuid) ->
    Deadline = erlang:monotonic_time(millisecond) + ?GATHER_TIMEOUT,
    gather_until_caught_up(
        SelfRecords, SelfVersion, SelfLeaderGone, OtherIds, Tenant, Tuid, Deadline
    ).

gather_until_caught_up(
    SelfRecords, SelfVersion, SelfLeaderGone, OtherIds, Tenant, Tuid, Deadline
) ->
    {FreshestRecords, MaxVersion, PeerResults, PeersSettled} =
        try
            gather_maps(SelfRecords, SelfVersion, OtherIds)
        catch
            _:_ -> {SelfRecords, SelfVersion, #{}, false}
        end,
    %% This read is best-effort, and it returns a value on the applied_version scale.
    %% If it fails, for example on a backend hiccup or with a test double, we get
    %% `unknown`. That turns the check off rather than stalling the handoff.
    Frontier =
        try
            dgen_registry_names:read_committed_frontier(Tenant, Tuid)
        catch
            _:_ -> unknown
        end,
    case
        Frontier =:= unknown orelse MaxVersion >= Frontier orelse
            (SelfLeaderGone andalso PeersSettled) orelse
            erlang:monotonic_time(millisecond) >= Deadline
    of
        true ->
            {FreshestRecords, MaxVersion, PeerResults, Frontier};
        false ->
            timer:sleep(?ASSUME_CATCHUP_INTERVAL),
            gather_until_caught_up(
                SelfRecords, SelfVersion, SelfLeaderGone, OtherIds, Tenant, Tuid, Deadline
            )
    end.

%% Pulls the subscriptions from the elector, with a short timeout. Only call it from
%% off the loop. On timeout we return no subscriptions, and a later update or handoff
%% fills them in.
fetch_subscriptions(Elector) when is_pid(Elector) ->
    try dgen_registry_elector:priority_get_subscriptions(Elector, ?SUBS_PULL_TIMEOUT) of
        Subs when is_map(Subs) -> Subs;
        _ -> #{}
    catch
        _:_ -> #{}
    end;
fetch_subscriptions(_Elector) ->
    #{}.

%% Spawns one get_peer_snapshot/1 helper per peer, all under one deadline. Answers
%% that arrive late are dropped by unhandled/3.
gather_peer_snapshots([]) ->
    {#{}, []};
gather_peer_snapshots(OtherIds) ->
    Ref = make_ref(),
    Parent = self(),
    lists:foreach(
        fun(MemberId) ->
            spawn(fun() -> Parent ! {Ref, MemberId, get_peer_snapshot(MemberId)} end)
        end,
        OtherIds
    ),
    Deadline = erlang:monotonic_time(millisecond) + ?GATHER_TIMEOUT + 500,
    collect_gather(Ref, OtherIds, Deadline, {#{}, []}).

%% Collects `{PeerResults, GoneIds}`. `GoneIds` lists the peers whose leader is gone.
collect_gather(_Ref, [], _Deadline, Acc) ->
    Acc;
collect_gather(Ref, Waiting, Deadline, {Results, GoneIds} = Acc) ->
    Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
    _ = ?ETA_LOG({gather_wait, length(Waiting), Timeout}),
    receive
        {Ref, MemberId, {ok, Snapshot, LeaderGone}} ->
            Results1 = Results#{MemberId => Snapshot},
            GoneIds1 =
                case LeaderGone of
                    true -> [MemberId | GoneIds];
                    false -> GoneIds
                end,
            collect_gather(Ref, lists:delete(MemberId, Waiting), Deadline, {Results1, GoneIds1});
        {Ref, MemberId, error} ->
            collect_gather(Ref, lists:delete(MemberId, Waiting), Deadline, Acc)
    after Timeout ->
        _ = ?ETA_LOG({gather_timed_out, length(Waiting)}),
        Acc
    end.

%% Detects and repairs conflicts (§5.6) using dgen_registry_conflicts:resolve/5, and
%% carries out the kills and alarms it asks for. Returns the rebuilt names without the
%% names in conflict.
repair_conflicts(Authority, Witnesses, Trails, Alive, Data = #data{conflicts = C0}) ->
    C1 = #conflicts{trail = Trail} = dgen_registry_conflicts:merge(Trails, C0),
    %% For a pid we didn't probe, we only check it if it is local, which needs no RPC. A
    %% remote one counts as dead, so we never kill it on evidence we didn't gather.
    IsAlive = fun(Pid) ->
        case Alive of
            #{Pid := A} -> A;
            _ when node(Pid) =:= node() -> is_process_alive(Pid);
            _ -> false
        end
    end,
    Conflicts = dgen_registry_conflicts:detect(Authority, Witnesses, Trail, IsAlive),
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

%% Checks in parallel whether each pid is alive. It runs in the gather helpers, never
%% on the member's loop.
check_alive([]) ->
    #{};
check_alive(Pids) ->
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

alarm_conflict(Name, Authority, Divergent, Action) ->
    logger:error(
        "dgen_registry: name conflict — name=~p authority=~p divergent=~p action=~p. "
        "Two different live pids held one name (single-fault uniqueness breach, §5.6).",
        [Name, Authority, Divergent, Action]
    ),
    %% An operator must act when `action` is `budget_exhausted`.
    emit_telemetry(
        [dgen_registry, conflict],
        #{divergent_count => length(Divergent)},
        #{name => Name, authority => Authority, action => Action}
    ).

%% A pid on an unreachable node counts as dead, so we never act on a conflict over a
%% binding we can't verify.
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

%% Reads a peer's records, version and trail, and whether its leader is gone, with a
%% timeout. Returns `error` if the peer is disconnected or too slow. This gather then
%% goes ahead without its bindings.
get_peer_snapshot({Node, Name}) ->
    case dgen_utils:node_reachable(Node) of
        false ->
            error;
        true ->
            %% Everything is inside the `try`, because decode_records/1 can fail on a
            %% corrupt payload.
            try
                {Payload, Version, Released, LeaderGone} =
                    gen_statem:call({Name, Node}, get_names_snapshot, ?GATHER_TIMEOUT),
                Names = dgen_registry_replica:decode_records(Payload),
                true =
                    is_map(Names) andalso is_integer(Version) andalso is_map(Released) andalso
                        is_boolean(LeaderGone),
                {ok, {Names, Version, Released}, LeaderGone}
            catch
                _:_ -> error
            end
    end.

%% Prevention from §5.6, turned on with `reject_when_degraded`. While the leader is
%% degraded, it refuses names it doesn't hold. Such a name could have a committed
%% binding that the rebuild missed.
should_reject_when_degraded(Name, Data) ->
    #data{conflicts = #conflicts{degraded = Degraded}, replica = R, config = Config} = Data,
    Degraded andalso dgen_config:reject_when_degraded(Config) andalso
        not dgen_registry_replica:exists(R, Name).

%% Without a reachable leader we never answer a registration `no`, because the caller
%% would wrongly see `already_started`. It waits as an intent until a leader appears.
route_register(Name, Pid, Meta, From, Data) ->
    #data{leader = Leader, member_id = Self} = Data,
    if
        Leader =:= Self ->
            enqueue_register(Name, Pid, Meta, From, Data);
        true ->
            submit_intent({register, Name, Pid, Meta}, From, Data)
    end.

%% Works like route_register/5. We read the bound pid before the optimistic row delete,
%% so the leader can clear it and record it in the trail (§5.6). A leader commits the
%% unregister and answers `ok` once it commits. Any other member forwards it as an
%% intent. If no leader is reachable, we answer `ok` now and keep the intent until a
%% leader is reachable, so the unregister is never lost.
route_unregister(LogicalName, From, Data) ->
    #data{leader = Leader, member_id = Self, replica = R} = Data,
    ReleasedPid = dgen_registry_replica:lookup_pid(R, LogicalName),

    if
        Leader =:= Self ->
            {noreply,
                enqueue_op(
                    {remove, LogicalName, ReleasedPid, {unreg, From}},
                    delete_row(Data, LogicalName)
                )};
        true ->
            Data1 = delete_row(Data, LogicalName),
            {noreply, submit_intent({unregister, LogicalName, ReleasedPid}, From, Data1)}
    end.

enqueue_register(Name, Pid, Meta, From, Data) ->
    case should_reject_when_degraded(Name, Data) of
        true ->
            gen_server:reply(From, no),
            Data;
        false ->
            enqueue_op({add, Name, Pid, Meta, {local, From}}, Data)
    end.

%% ---------------------------------------------------------------------------
%% Intents: requests this member can't decide itself (dgen_registry_intents)
%% ---------------------------------------------------------------------------

get_intent_route(#data{leader = Self, member_id = Self}) ->
    self;
get_intent_route(#data{leader = undefined}) ->
    none;
get_intent_route(#data{leader = Leader}) ->
    case is_member_reachable(Leader) of
        true -> {remote, Leader};
        false -> none
    end.

submit_intent(Kind, From, Data = #data{intents = I}) ->
    Now = erlang:system_time(millisecond),
    apply_intents(dgen_registry_intents:submit(Kind, From, Now, get_intent_route(Data), I), Data).

apply_intent_reply(Ref, Reply, Data) ->
    #data{intents = I, replica = #replica{epoch = Epoch, version = Applied}} = Data,
    Route = get_intent_route(Data),
    apply_intents(dgen_registry_intents:on_reply(Ref, Reply, {Epoch, Applied}, Route, I), Data).

%% Our replica advanced. Releases the held `yes` replies whose version it now covers.
release_deferred_acks(Data) ->
    #data{intents = I, replica = #replica{epoch = Epoch, version = Applied}} = Data,
    apply_intents(dgen_registry_intents:on_version({Epoch, Applied}, I), Data).

%% Re-drives unresolved intents. `Scope` is `moved` when the leader changed, and `all`
%% when a connection came back. See dgen_registry_intents:redrive/5.
redrive_intents(Scope, Data) ->
    #data{intents = I, config = Config} = Data,
    Now = erlang:system_time(millisecond),
    TTL = dgen_config:register_timeout(Config),
    Route = get_intent_route(Data),
    apply_intents(dgen_registry_intents:redrive(Route, Scope, Now, TTL, I), Data).

%% Drops intents whose callers timed out, so the map can't grow without bound during
%% a long partition.
prune_intents(Data) ->
    #data{intents = I, config = Config} = Data,
    Now = erlang:system_time(millisecond),
    TTL = dgen_config:register_timeout(Config),
    Data#data{intents = dgen_registry_intents:prune(Now, TTL, I)}.

apply_intents({Intents, Effects}, Data) ->
    lists:foldl(fun apply_intent_effect/2, Data#data{intents = Intents}, Effects).

apply_intent_effect({reply, From, Value}, Data) ->
    gen_server:reply(From, Value),
    Data;
apply_intent_effect({forward, Leader, Ref, Kind}, Data = #data{member_id = Self}) ->
    cast_to_member(Leader, get_forward_msg(Ref, Self, Kind)),
    Data;
%% Re-drives an unregister intent as a retract to a reachable leader. The retract only
%% clears the name if the same pid still holds it.
apply_intent_effect({retract, Leader, Name, Pid}, Data) ->
    cast_to_member(Leader, {retract_req, Name, Pid}),
    Data;
apply_intent_effect({enqueue, {register, Name, Pid, Meta}, From}, Data) ->
    enqueue_register(Name, Pid, Meta, From, Data);
apply_intent_effect({enqueue, {unregister, Name, Pid}, answered}, Data) ->
    enqueue_op({retract, Name, Pid}, Data);
apply_intent_effect({resync, Leader}, Data) ->
    request_resync(Leader, Data).

get_forward_msg(Ref, Self, {register, Name, Pid, Meta}) ->
    {register_req, Ref, Self, Name, Pid, Meta};
get_forward_msg(Ref, Self, {unregister, Name, _ReleasedPid}) ->
    {unregister_req, Ref, Self, Name};
get_forward_msg(Ref, Self, {set_meta, Name, Index, MetaData}) ->
    {set_meta_req, Ref, Self, Name, Index, MetaData}.

%% Ops that arrive while a commit is in flight queue up and go in the next batch.
enqueue_op(Op, Data = #data{pipeline = P}) ->
    maybe_start_commit(Data#data{pipeline = dgen_registry_pipeline:enqueue(Op, P)}).

%% At most one commit runs at a time, in a dgen_transaction worker off this loop. A
%% member that no longer leads surrenders the queue instead. No timer drives this.
%% enqueue_op/2 starts a commit when none is running, and the worker's
%% `{dgen_transaction, …}` result starts the next one. This is the group commit from
%% §4.3.
maybe_start_commit(Data) ->
    #data{
        pipeline = P = #pipeline{committing = Committing, num_pending = NumPending},
        leader = Leader,
        member_id = Self
    } = Data,
    case {Committing =/= undefined, NumPending} of
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
    #data{
        replica = #replica{epoch = Epoch, tab = Tab},
        member_id = Self,
        tenant = Tenant,
        tuid = Tuid,
        name_to_ref = NTR,
        pipeline = P,
        config = Config
    } = Data,
    {ThisBatch, P1} = dgen_registry_pipeline:take_batch(dgen_config:commit_batch_size(Config), P),
    #pipeline{last_version = LastVersion} = P1,
    Data1 = Data#data{pipeline = P1},
    %% Planning costs O(batch), because it looks bindings up only as it needs them. See
    %% plan_batch/4.
    Plan = plan_batch(ThisBatch, Tab, NTR, Epoch),
    #{dbop := DBOp, replies := RepliesRev} = Plan,
    case map_size(DBOp) =:= 0 of
        true ->
            %% The batch only has rejections, so there is nothing to commit.
            send_replies(lists:reverse(RepliesRev)),
            maybe_start_commit(Data1);
        false ->
            Ref = make_ref(),
            %% We reuse `last_version` as the read version to skip a GRV. If it is
            %% stale, the leader-key read raises too-old, and dgen_transaction retries
            %% with a fresh one. We monitor the worker so that its death still ends the
            %% commit. The worker only writes the version key, and only if we still
            %% lead. The name changes are applied in memory.
            {ok, {_Pid, MRef}} = dgen_registry_names:start_commit(
                Tenant,
                Tuid,
                Self,
                #{owner => self(), ref => Ref, epoch => Epoch, read_version => LastVersion}
            ),
            Data1#data{pipeline = dgen_registry_pipeline:start_commit(Ref, MRef, Plan, P1)}
    end.

%% Applies a committed batch. Updates the replica, monitors and demonitors pids,
%% replicates the batch, and answers callers.
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
        replica = R0 = #replica{version = PrevVersion, epoch = Epoch}
    } = Data,
    %% We only touch the names in `DBOp`. An optimistic unregister of some other name
    %% that happened meanwhile is left alone.
    Data0 = Data#data{replica = dgen_registry_replica:commit(Version, DBOp, WNames, WMeta, R0)},
    NTR1 = apply_monitor_ops(DBOp, WNames, NTR0),
    %% Broadcast first. FIFO then delivers the replicate_sync to a follower only after
    %% it holds the batch. We send one message per batch, never one per name. If a
    %% follower received only part of a batch, it would report the full version with
    %% only part of the content. The handoff gather relies on "same version means same
    %% content" (§5.7).
    broadcast_batch(Members, lists:reverse(BcastsRev), PrevVersion, Version, Self),
    Now = erlang:system_time(millisecond),
    Data1 = Data0#data{
        name_to_ref = NTR1,
        pipeline = dgen_registry_pipeline:set_last_version(Version, Data0#data.pipeline),
        %% Explicit releases go into the conflict trail (§5.6).
        conflicts = dgen_registry_conflicts:record_releases(Released, Now, Data0#data.conflicts)
    },
    %% We answer forwarded registrations and rejections now, because the follower that
    %% forwarded a `yes` is already the second holder. A direct `yes` waits for a
    %% follower's replica ack (§8).
    {DirectYes, Immediate} = lists:partition(
        fun
            ({{local, _From}, yes}) -> true;
            (_Other) -> false
        end,
        lists:reverse(RepliesRev)
    ),
    lists:foreach(
        fun({Origin, Result}) -> deliver_committed_reply(Origin, Result, Version, Epoch) end,
        Immediate
    ),
    %% We keep `{Name, Pid}` so `strict_replication` can roll a direct binding back.
    EnrichedDirect = [
        {Origin, Name, Pid}
     || {Origin, yes} <- DirectYes, {Name, Pid} <- [maps:get(Origin, DirectMeta)]
    ],
    %% Send presence notifications (§4.9), computed against the committed replica.
    Data2 = send_presence_events(DBOp, Data1),
    await_replica_acks(EnrichedDirect, Version, Data2).

%% Sends presence events for a committed batch. Only the leader does this (§4.9). For
%% each subscription we diff the changed names. Changes to the watch set become
%% `joined` and `left` events for the notify targets that were already there. A pid
%% that newly enters the notify set gets the full watch snapshot instead.
send_presence_events(_DBOp, Data = #data{subs = Subs}) when map_size(Subs) =:= 0 ->
    Data;
send_presence_events(DBOp, Data) ->
    #data{replica = #replica{tab = Tab}, subs = Subs} = Data,
    RealNames = maps:keys(DBOp),
    maps:fold(
        fun(SubId, {Watch, Notify}, S) ->
            diff_subscription(SubId, Watch, Notify, RealNames, Tab, S)
        end,
        Data,
        Subs
    ).

%% Seeds a new or replaced subscription. Computes both sets and sends the watch
%% snapshot to the current notify targets.
seed_subscription(SubId, Data) ->
    #data{subs = Subs, replica = R, sub_matches = SM, notify_matches = NM} = Data,
    case Subs of
        #{SubId := {Watch, Notify}} ->
            WatchMatches = dgen_registry_replica:query_match_names(R, Watch),
            NotifyMatches = dgen_registry_replica:query_match_names(R, Notify),
            send_presence(SubId, get_snapshot_events(WatchMatches), get_unique_pids(NotifyMatches)),
            Data#data{
                sub_matches = SM#{SubId => WatchMatches},
                notify_matches = NM#{SubId => NotifyMatches}
            };
        _ ->
            Data
    end.

%% Diffs one subscription and sends its notifications. We compare sets of pids, so a
%% pid that is already a target under another name doesn't get a second snapshot.
diff_subscription(SubId, Watch, Notify, RealNames, Tab, Data) ->
    #data{sub_matches = SM, notify_matches = NM} = Data,
    {WatchEvents, WatchAfter} = diff_query_matches(Watch, RealNames, maps:get(SubId, SM, #{}), Tab),
    NotifyBefore = maps:get(SubId, NM, #{}),
    {_NotifyEvents, NotifyAfter} = diff_query_matches(Notify, RealNames, NotifyBefore, Tab),
    AfterPids = get_unique_pids(NotifyAfter),
    NewTargets = AfterPids -- get_unique_pids(NotifyBefore),
    ContinuingTargets = AfterPids -- NewTargets,
    send_presence(SubId, WatchEvents, ContinuingTargets),
    send_presence(SubId, get_snapshot_events(WatchAfter), NewTargets),
    Data#data{sub_matches = SM#{SubId => WatchAfter}, notify_matches = NM#{SubId => NotifyAfter}}.

%% Diffs which of the changed names match a query. Returns the `joined` and `left`
%% events and the updated `#{Name => Pid}` map.
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

get_snapshot_events(Matches) ->
    [{joined, Name, Pid} || {Name, Pid} <- maps:to_list(Matches)].

get_unique_pids(Matches) ->
    lists:usort(maps:values(Matches)).

send_presence(_SubId, [], _Pids) ->
    ok;
send_presence(_SubId, _Events, []) ->
    ok;
send_presence(SubId, Events, Pids) ->
    Msg = {dgen_presence, SubId, Events},
    lists:foreach(fun(Pid) -> Pid ! Msg end, Pids).

%% Makes direct registrations wait for replication before we ack them (§5.5, §8). We
%% wait for acks from `register_replicas` different followers, capped at the number of
%% followers. With no followers we answer right away. On timeout the
%% `strict_replication` setting decides the answer.
await_replica_acks([], _Version, Data) ->
    Data;
await_replica_acks(Direct, Version, Data) ->
    #data{members = Members, member_id = Self, pipeline = P, config = Config} = Data,
    Needed = min(dgen_config:register_replicas(Config), map_size(Members)),
    case Needed =< 0 of
        true ->
            deliver_direct_yes(Direct),
            Data;
        false ->
            BatchRef = make_ref(),
            %% It carries `Version`, so a follower that missed the batch never acks.
            broadcast_to_peers(Members, {replicate_sync, BatchRef, Self, Version}),
            TimerRef = schedule_timer(
                dgen_config:replicate_timeout(Config), {replicate_timeout, BatchRef}
            ),
            Data#data{
                pipeline = dgen_registry_pipeline:await_acks(BatchRef, Direct, TimerRef, Needed, P)
            }
    end.

deliver_direct_yes(Direct) ->
    lists:foreach(fun({Origin, _Name, _Pid}) -> deliver_reply(Origin, yes) end, Direct).

%% Fails closed under `strict_replication`. Answers each caller `no` and commits a
%% retract, which only clears the name if the same pid still holds it.
reject_and_retract_direct(Direct, Data) ->
    lists:foldl(
        fun({Origin, Name, Pid}, S) ->
            deliver_reply(Origin, no),
            enqueue_op({retract, Name, Pid}, S)
        end,
        Data,
        Direct
    ).

%% Reports a degrade-open (§8) with telemetry and a log warning.
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

%% `telemetry` is an optional dependency. We check once per VM whether it is loaded and
%% cache the answer. `code:ensure_loaded/1` on a missing module calls `code_server`
%% every time, which is slow and also makes runs under eta simulation unreproducible.
-define(TELEMETRY_KEY, {?MODULE, telemetry_available}).

emit_telemetry(Event, Measurements, Metadata) ->
    case is_telemetry_available() of
        true -> apply(telemetry, execute, [Event, Measurements, Metadata]);
        false -> ok
    end.

is_telemetry_available() ->
    case persistent_term:get(?TELEMETRY_KEY, undefined) of
        undefined ->
            Available = code:ensure_loaded(telemetry) =:= {module, telemetry},
            persistent_term:put(?TELEMETRY_KEY, Available),
            Available;
        Available ->
            Available
    end.

%% Answers whoever sent a write op. A local caller gets gen_server:reply/2. A
%% forwarding member gets a reply cast.
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

%% The reply to a forwarded registration carries the commit version and our epoch.
%% The follower holds a `yes` until its replica reaches that version in that epoch.
%% See the {register_reply, _, _, _, _} handler.
deliver_committed_reply({forward, MemberId, Ref}, Result, Version, Epoch) ->
    cast_to_member(MemberId, {register_reply, Ref, Result, Version, Epoch});
deliver_committed_reply(Origin, Result, _Version, _Epoch) ->
    deliver_reply(Origin, Result).

%% Folds the ops, in order, into a plan. The plan holds the batch overlay, the net
%% change per name in `dbop` where the last write wins, and the replies and broadcasts,
%% newest first. `wntr` tracks monitor refs, so a `down` op only clears a name if its
%% ref still matches. A name bound in this batch gets a `{pending, Pid}` placeholder
%% that never matches a real ref. `names` starts empty. Each op looks up its one name
%% with lookup_in_batch/3, so planning costs O(batch).
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
        %% We don't apply a plan whose epoch is no longer current. Broadcasts carry it.
        epoch => Epoch,
        %% Kept so we can surrender the ops if the batch fails.
        ops => Ops
    },
    lists:foldl(fun(Op, Acc) -> plan_op(Op, Epoch, Acc) end, Init, Ops).

%% Returns a name's pid as of this point in the batch. We check the batch overlay `WN`
%% first, then ETS. `removed` marks a name cleared earlier in this batch. Only
%% unregister deletes from ETS right away, so an earlier `retract` or `down` shows up
%% only in the overlay.
lookup_in_batch(WN, Tab, Name) ->
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
    case lookup_in_batch(WN, Tab, Name) of
        Taken when is_pid(Taken), Taken =/= Pid ->
            %% A different pid holds the name, so we reject. One name, one pid.
            Acc#{replies := [{Origin, no} | Rs]};
        _Free_or_same ->
            %% The name is free, or the same pid holds it. Either way we answer `yes`
            %% (§5.2), so a caller whose call timed out can retry and get a clear
            %% answer. We record direct bindings so `strict_replication` can roll them
            %% back.
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
    case lookup_in_batch(WN, Tab, Name) of
        undefined ->
            Acc#{replies := [{Origin, {error, not_registered}} | Rs]};
        Pid ->
            %% If this batch already has a {set, …} for the name, we keep it so the new
            %% pid still gets monitored. Otherwise {meta, …} leaves the existing
            %% monitor alone.
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
    %% We clear the name if it was bound when the op was enqueued, or if it is bound
    %% now in this batch. In the first case `ReleasedPid` is that pid, and the
    %% optimistic delete already removed the row from ETS. The released `{Name, Pid}`
    %% goes into the conflict trail (§5.6). The caller gets `ok` either way.
    Rs1 = maybe_add_unreg_reply(Origin, Rs),
    WasBound = is_pid(ReleasedPid),
    Current = lookup_in_batch(WN, Tab, Name),
    %% Check the *current* holder. The optimistic row delete frees the name as soon as
    %% the unregister arrives, so a register queued ahead of this remove may already
    %% have bound it again. Clearing that new holder would break Guarantee 1. If we
    %% captured a pid, we only clear the name when that pid or nobody holds it. If we
    %% captured `undefined`, we clear whatever is bound, as an unregister by name.
    Rebound = WasBound andalso is_pid(Current) andalso Current =/= ReleasedPid,
    case (WasBound orelse Current =/= undefined) andalso not Rebound of
        true ->
            Rel1 =
                case WasBound of
                    true -> [{Name, ReleasedPid} | Rel];
                    false -> Rel
                end,
            %% The broadcast carries the released pid, so every member adds it to its
            %% trail (§5.6).
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
    %% We only clear the name if this pid still holds it. We also add it to the trail,
    %% so a lagging follower that still has the binding isn't taken for a conflict.
    case lookup_in_batch(WN, Tab, Name) of
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
            %% The pid is dead, so we add nothing to the trail. The trail is only for
            %% pids released while alive.
            Acc#{
                names := WN#{Name => removed},
                wntr := maps:remove(Name, WNTR),
                dbop := DB#{Name => clear},
                bcasts := [{name_unregistered, Name, undefined, Epoch} | Bs]
            };
        _ ->
            %% A stale DOWN. The name was registered again or already removed.
            Acc
    end.

%% A remove whose origin is `undefined` has no caller to answer. That happens when we
%% retry a surrendered remove.
maybe_add_unreg_reply(undefined, Rs) -> Rs;
maybe_add_unreg_reply(Origin, Rs) -> [{Origin, ok} | Rs].

%% Updates monitors to match the batch. `clear` demonitors, `set` monitors the new pid
%% in place of the old one, and `meta` leaves the monitor alone.
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

%% Adds the releases in an applied batch to the conflict trail (§5.6). The trail is
%% keyed by `{Name, Pid}`, so releasing one name never hides a conflict on another
%% name the same pid holds.
record_releases(Ops, Data = #data{conflicts = C}) ->
    Pairs = [{Name, Pid} || {name_unregistered, Name, Pid} <- Ops, is_pid(Pid)],
    Now = erlang:system_time(millisecond),
    Data#data{conflicts = dgen_registry_conflicts:record_releases(Pairs, Now, C)}.

%% Sends one `{names_batch, Ops, Epoch, PrevV, Version, LeaderId}` per committed batch.
%% The epoch appears once, on the message, not on each op. Followers use `PrevV` and
%% `Version` to detect gaps, and ask `LeaderId` for a resync. An empty batch sends
%% nothing.
-ifdef(MUTATION_PARTIAL_BATCH).
%% MUTATION, for tests only. Sends one message per op, all with the same
%% `{PrevV, Version}`, so a follower that receives only part of a batch diverges
%% without noticing. It pairs with the same mutation in dgen_registry_replica.
broadcast_batch(_Members, [], _PrevV, _Version, _Self) ->
    ok;
broadcast_batch(Members, Ops, PrevV, Version, Self) ->
    lists:foreach(
        fun(Op) ->
            broadcast_to_peers(
                Members,
                {names_batch, [strip_bcast_epoch(Op)], get_bcast_epoch(Op), PrevV, Version, Self}
            )
        end,
        Ops
    ).
-else.
broadcast_batch(_Members, [], _PrevV, _Version, _Self) ->
    ok;
broadcast_batch(Members, [First | _] = Ops, PrevV, Version, Self) ->
    Epoch = get_bcast_epoch(First),
    Stripped = [strip_bcast_epoch(Op) || Op <- Ops],
    broadcast_to_peers(Members, {names_batch, Stripped, Epoch, PrevV, Version, Self}).
-endif.

-ifdef(MUTATION_QUIET_RESYNC).
%% MUTATION, for tests only (`DGEN_MUTATION=quiet_resync`). The heartbeat sends
%% nothing, so a follower that lost the end of the stream stays behind while there
%% are no writes.
broadcast_heartbeat(_Data) ->
    ok.
-else.
%% Sends the leader's periodic empty batch at `{Applied, Applied}`. See
%% ?REPLICA_HEARTBEAT_INTERVAL.
broadcast_heartbeat(Data) ->
    #data{
        members = Members,
        member_id = Self,
        replica = #replica{epoch = Epoch, version = Applied}
    } = Data,
    broadcast_to_peers(Members, {names_batch, [], Epoch, Applied, Applied, Self}),
    ok.
-endif.

get_bcast_epoch({name_registered, _Name, _Pid, _Index, _Data, Epoch}) -> Epoch;
get_bcast_epoch({metadata_set, _Name, _Index, _Data, Epoch}) -> Epoch;
get_bcast_epoch({name_unregistered, _Name, _ReleasedPid, Epoch}) -> Epoch.

strip_bcast_epoch({name_registered, Name, Pid, Index, MetaData, _Epoch}) ->
    {name_registered, Name, Pid, Index, MetaData};
strip_bcast_epoch({metadata_set, Name, Index, MetaData, _Epoch}) ->
    {metadata_set, Name, Index, MetaData};
strip_bcast_epoch({name_unregistered, Name, ReleasedPid, _Epoch}) ->
    {name_unregistered, Name, ReleasedPid}.

%% Asks `LeaderId` for a snapshot after a gap, at most once per ?RESYNC_RETRY.
request_resync(LeaderId, Data = #data{resync_timer = undefined}) ->
    #data{member_id = Self} = Data,
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

%% Marks us synced. The first time, we join again with `fresh = false`.
mark_synced(Data = #data{synced = true}) ->
    Data;
mark_synced(Data) ->
    #data{elector = Elector, member_id = Self, join_token = Token} = Data,
    dgen_registry_elector:join(Elector, Self, Token, false),
    Data#data{synced = true}.

%% Returns whether the member's node is connected, so a cast to it won't be dropped.
is_member_reachable({Node, _Name}) ->
    dgen_utils:node_reachable(Node).

%% Reports a peer's death with the elector's current token, falling back to `Cached`.
%% It runs in a helper so this loop never blocks on the elector. See the DOWN handler.
spawn_member_down(Elector, MemberId, Cached) ->
    _ = spawn(fun() ->
        Token = dgen_registry_elector:get_member_token(Elector, MemberId, Cached),
        dgen_registry_elector:member_down(Elector, MemberId, Token)
    end),
    ok.

%% Guarantee 5. Answers a consistent read only after the durable leader key confirms we
%% still lead in our epoch. Otherwise a deposed leader on the minority side of a
%% partition would serve its frozen replica. We capture `Value` at this point in the
%% mailbox. The check runs in a helper so the database round trip never blocks the
%% loop. The read is linearizable because leadership doesn't change within an epoch.
%% If the check fails for any reason, the caller gets `Denied`.
reply_fenced(From, Value, Denied, Data) ->
    #data{tenant = Tenant, tuid = Tuid, member_id = Self, replica = #replica{epoch = Epoch}} =
        Data,

    _ = spawn(fun() ->
        Reply =
            case dgen_registry_names:verify_leader(Tenant, Tuid, Self, Epoch) of
                true -> Value;
                false -> Denied
            end,
        gen_server:reply(From, Reply)
    end),
    ok.

%% Applies the commit result, but only if we still lead in the epoch the plan was made
%% in.
handle_commit_result(Reply, Plan, Data) ->
    #data{leader = Leader, member_id = Self, replica = #replica{epoch = Epoch}} = Data,
    case Reply of
        {committed, Version} when Leader =:= Self ->
            case maps:get(epoch, Plan) =:= Epoch of
                true ->
                    apply_committed_plan(Plan, Version, Data);
                false ->
                    %% The plan is from an older epoch. Applying or broadcasting it
                    %% would make replicas diverge. Only the version bump was written,
                    %% so we retry the ops.
                    surrender(failed, maps:get(ops, Plan), Data)
            end;
        {aborted, fenced} ->
            %% A newer leader exists and we haven't heard about it. We ask now.
            surrender(failed, maps:get(ops, Plan), maybe_rejoin(Data));
        _Deposed_or_error ->
            surrender(failed, maps:get(ops, Plan), Data)
    end.

%% A commit aborted with `fenced` proves we were deposed without hearing about it. We
%% join again now instead of waiting for the connector's durable epoch check. We do
%% this at most once per epoch. Each join is a durable queue entry, and a partitioned
%% member keeps getting `fenced`.
maybe_rejoin(Data = #data{fence_rejoined = Fenced}) when Fenced =/= undefined ->
    #data{replica = #replica{epoch = Epoch}} = Data,
    case Epoch of
        Fenced -> Data;
        _ -> maybe_rejoin(Data#data{fence_rejoined = undefined})
    end;
maybe_rejoin(Data) ->
    #data{
        elector = Elector,
        member_id = Self,
        join_token = Token,
        synced = Synced,
        replica = #replica{epoch = Epoch}
    } = Data,
    dgen_registry_elector:join(Elector, Self, Token, not Synced),
    Data#data{fence_rejoined = Epoch}.

%% Carries out what dgen_registry_pipeline:surrender/2 returns. That is replies,
%% intents and delayed retries.
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

%% We can only check the caller of a local direct registration. Any other op is kept.
is_caller_alive({add, _Name, _Pid, _Meta, {local, {Caller, _Tag}}}) when node(Caller) =:= node() ->
    is_process_alive(Caller);
is_caller_alive(_Op) ->
    true.

%% Logs an `arm` event, so a `{clock, Ms}` entry in a simulation trace can be traced
%% back to its timer. `?ETA_LOG` does nothing in production builds.
schedule_timer(Delay, Msg) ->
    _ = ?ETA_LOG({arm, Msg, Delay}),
    erlang:send_after(Delay, self(), Msg).

%% We lost leadership while direct registrations were waiting for acks. Resolves them
%% the same way as a replicate timeout (§8). When failing closed, we submit the
%% retract as an intent, so a caller told `no` can't leave the binding in place.
fail_pending_acks(Data) ->
    #data{pipeline = P, config = Config} = Data,
    {Batches, P1} = dgen_registry_pipeline:take_acks(P),
    Strict = dgen_config:strict_replication(Config),
    lists:foldl(
        fun({Direct, TimerRef, Needed, Got}, S) ->
            erlang:cancel_timer(TimerRef),
            case Strict of
                false ->
                    notify_degrade_open(Needed, Got, length(Direct)),
                    deliver_direct_yes(Direct),
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

%% Deletes the row optimistically on unregister. It doesn't change the replica version.
delete_row(S = #data{replica = R}, Name) ->
    S#data{replica = dgen_registry_replica:delete(R, Name)}.

%% Takes the bare table, so plan_batch/4 can be tested against a plain ETS table.
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

remove_member(MemberId, Data) ->
    #data{members = Members, monitors = Monitors} = Data,
    case maps:get(MemberId, Members, undefined) of
        undefined ->
            Data;
        Ref ->
            erlang:demonitor(Ref, [flush]),
            %% Drop the peer's token, which the caller has already read. Recording the
            %% departure time pauses trail pruning for a while (§5.6).
            Data#data{
                members = maps:remove(MemberId, Members),
                monitors = maps:remove(Ref, Monitors),
                peer_tokens = maps:remove(MemberId, Data#data.peer_tokens),
                conflicts = dgen_registry_conflicts:record_departure(
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

%% The only place we monitor a registered process. The monitor is tagged
%% `{down, Name}`.
monitor_name(Name, Pid) ->
    erlang:monitor(process, Pid, [{tag, {down, Name}}]).

%% Returns which members `Id` should start monitoring on a leadership change. After a
%% member_down there are none. The joiner monitors every member, and everyone else
%% monitors just the joiner.
get_extra_member_ids(undefined, _AllIds, _Id) -> [];
get_extra_member_ids(MemberId, AllIds, MemberId) -> AllIds;
get_extra_member_ids(MemberId, _AllIds, _Id) -> [MemberId].

%% The elector always sends the current tokens, so its values overwrite ours.
merge_peer_tokens(Tokens, Data = #data{peer_tokens = PeerTokens}) ->
    Data#data{peer_tokens = maps:merge(PeerTokens, Tokens)}.

broadcast_to_peers(Members, Msg) ->
    maps:foreach(fun(MemberId, _) -> cast_to_member(MemberId, Msg) end, Members).

%% Every message between members goes through here. A cast to a disconnected node
%% would make distribution reconnect on its own. During a partition, that would heal it
%% before either side noticed the other had left. So we drop the cast unless the node
%% is already connected. Under `-ifdef(DST)`, `eta_transform` rewrites the cast to
%% `eta_net:cast/2`.
cast_to_member({Node, Name} = To, Msg) ->
    case is_member_reachable(To) of
        true -> gen_statem:cast({Name, Node}, Msg);
        false -> ok
    end.
