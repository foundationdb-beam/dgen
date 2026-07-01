-module(dgen_registry_elector).
-behaviour(dgen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).
-define(SnapshotTimeout, 2000).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% `dgen_server` callback module that tracks registry membership and elects a leader.
%%
%% Section references (e.g. `§5.7`) point to `docs/dgen_registry_design.md`, the
%% registry's design-and-guarantees document.
%%
%% The elector's state (member map + current leader) is stored durably in the
%% backend and is shared across every node that runs the same named registry.
%% Messages arrive via a durable FIFO queue, so membership changes are processed
%% one at a time, serialised through the backend.
%%
%% ## Leader election
%%
%% The incumbent leader is kept as long as it remains a member — leadership
%% only changes when the incumbent leaves or no leader has been elected yet.
%% This prevents thrashing when a non-leader node happens to win a backend
%% transaction race.
%%
%% When a new leader must be chosen (no valid incumbent), the node that wins
%% the backend transaction race is preferred: if `{node(), MemberName}` is a
%% current member, that node becomes leader. If not (transient window during
%% startup), `lists:min/1` over live member IDs is used as a deterministic
%% fallback.
%%
%% ## Replication on a membership change
%%
%% A membership change (`{join}` always; `{member_down}` when the leader itself
%% departs) routes through the current leader so it reconstructs and fans out the
%% names snapshot.  This runs as a **post-commit action** (the `Actions` list
%% returned from `handle_cast_tx`, executed by `dgen_server` after the membership
%% transaction commits) — **no distributed lock**:
%%
%% 1. **Commit** — the backend transaction commits the new member set and leader key
%%    atomically.  This is the serialization point: the moment the new leader key is
%%    committed, the *old* leader is fenced — its next batch commit conflicts on the
%%    leader key and aborts (`dgen_registry_names`), so it can no longer accept a
%%    registration.  The handoff window is therefore naturally quiescent; the
%%    cross-consumer lock the registry formerly used is unnecessary (§5.7 of the
%%    design doc).
%% 2. **Leader assumption** — the action calls the leader via
%%    `{elector_assume_and_distribute, TriggerMember, AllIds, Tokens, Epoch}`. The
%%    leader reconstructs the names map itself by gathering every reachable member's
%%    replica (`gather_maps/3`) and taking the freshest — no snapshot is passed, no
%%    occupancy read, and no synchronous old-leader hand-off is needed. It resolves any
%%    conflicts (§5.6), stores the epoch, sets up `erlang:monitor/2` for every
%%    registered pid, and casts `{apply_names_snapshot, ..., Epoch}` to every follower
%%    from its own process. The call is wrapped in `try/catch`: an unreachable leader
%%    does not fail the action — the membership change is already committed and
%%    affected members self-correct on the next membership event.
%% 3. **Follower sync** — each follower receives `{apply_names_snapshot}`,
%%    overwrites its names map, and updates its leader field. Because these
%%    casts originate from the same process as subsequent `{name_registered}`
%%    broadcasts, Erlang's per-pair FIFO guarantee ensures every follower
%%    sees the snapshot before any registration that post-dates the transition.
%%
%% ## Leader key in the backend
%%
%% ```
%% Key path: {Tuid, <<"leader">>}
%% Value:    term_to_binary(MemberId | undefined)
%% ```

-export([
    init/1,
    handle_cast_tx/3,
    handle_call/3,
    leader_db_key/2
]).

-export_type([member_id/0, member_info/0, registry_state/0]).

-type member_id() :: {node(), atom()}.
-type member_info() :: #{joined_at := integer(), join_token := reference()}.
-type registry_state() :: #{
    name := atom(),
    members := #{member_id() => member_info()},
    leader := member_id() | undefined,
    epoch := non_neg_integer()
}.

%% ---------------------------------------------------------------------------
%% dgen_server callbacks
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Initialises the elector state with an empty member map and undefined leader.".
-endif.
-spec init(#{name := atom()}) -> {ok, dgen_server:tuid(), registry_state()}.
init(#{name := Name}) ->
    Tuid = {<<"dgen_registry.">>, atom_to_binary(Name)},
    State = #{name => Name, members => #{}, leader => undefined, epoch => 0},
    {ok, Tuid, State}.

-if(?DOCATTRS).
-doc """
Processes membership change messages within a backend transaction.

Handles `{join, MemberId, Token}` and `{member_down, MemberId, Token}`. Returns
`{noreply, NewState, Actions}` when the handoff must be routed through the leader
(every join; a leader-changing member_down), else plain `{noreply, NewState}`. The
`Actions` run after the transaction commits — no distributed lock (§5.7).

Each `{join}` carries a unique token (a `reference()` generated by the member
process before enqueuing).  The elector stores this token in `member_info`.

A `{member_down, MemberId, Token}` is silently discarded when its token does not
match the stored token for that member — this means the member has rejoined with
a new token since the DOWN was detected, so the message is stale.  This prevents
a partition-recovery race where a `{member_down}` enqueued during the disconnect
is processed after the subsequent `{join}` that heals the cluster.
""".
-endif.
-spec handle_cast_tx(dgen_server:tx_ctx(), term(), registry_state()) ->
    dgen_server:noreply_ret().

handle_cast_tx(TxCtx, {join, MemberId, Token}, State) ->
    #{name := Name, members := Members, leader := OldLeader, epoch := OldEpoch} = State,
    MemberInfo = #{joined_at => erlang:system_time(millisecond), join_token => Token},
    NewMembers = Members#{MemberId => MemberInfo},
    {NewLeader, NewEpoch} = elect_leader(TxCtx, Name, NewMembers, OldLeader, OldEpoch),
    NewState = State#{members => NewMembers, leader => NewLeader, epoch => NewEpoch},
    %% A join always routes through the leader so it gathers + distributes the names
    %% snapshot (delivering it to the joiner, and, on a leadership change, having the
    %% new leader assume).  All follower messages then share one sender (FIFO with
    %% subsequent name broadcasts).  This runs as a post-commit action — the
    %% leader-key commit fences the old leader, so the handoff window is naturally
    %% quiescent and no distributed lock is needed (§5.7).
    {noreply, NewState, [assume_distribute_action(MemberId)]};
handle_cast_tx(TxCtx, {member_down, MemberId, Token}, State) ->
    #{name := Name, members := Members, leader := OldLeader, epoch := OldEpoch} = State,
    case maps:get(MemberId, Members, undefined) of
        undefined ->
            %% Already removed — idempotent.
            {noreply, State};
        #{join_token := StoredToken} when StoredToken =/= Token ->
            %% Stale: member rejoined with a new token since this DOWN was detected.
            {noreply, State};
        _ ->
            NewMembers = maps:remove(MemberId, Members),
            {NewLeader, NewEpoch} = elect_leader(TxCtx, Name, NewMembers, OldLeader, OldEpoch),
            NewState = State#{members => NewMembers, leader => NewLeader, epoch => NewEpoch},
            case NewLeader =/= OldLeader of
                true ->
                    %% Leadership changed (the leader itself went down) — the new
                    %% leader must gather + distribute.  Post-commit action; the commit
                    %% fences the old leader, so no distributed lock is needed (§5.7).
                    {noreply, NewState, [assume_distribute_action(undefined)]};
                false ->
                    %% Leader unchanged — each surviving member detected the death
                    %% via its own erlang:monitor DOWN signal; no broadcast needed.
                    {noreply, NewState}
            end
    end.

%% Build the post-commit action that routes a membership change through the current
%% leader: it calls `{elector_assume_and_distribute, …}` so the leader gathers every
%% reachable member's names map, resolves conflicts (§5.6), and fans the snapshot out
%% to all followers.  Runs after the membership transaction commits (see the
%% `dgen_server` `Actions` contract).  `TriggerMember` is the joining member, or
%% `undefined` for a member_down.  A `try/catch` keeps an unreachable leader from
%% failing the action — the membership change is already committed and affected
%% members self-correct on the next membership event.
assume_distribute_action(TriggerMember) ->
    fun(#{members := All, leader := Leader, epoch := Epoch}) ->
        case Leader of
            undefined ->
                ok;
            _ ->
                try
                    call_to_member(
                        Leader,
                        {elector_assume_and_distribute, TriggerMember, maps:keys(All),
                            member_tokens(All), Epoch}
                    )
                catch
                    exit:_ -> ok
                end
        end
    end.

%% ---------------------------------------------------------------------------
%% handle_call/3  (read-only priority_calls — bypasses queue and locks)
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Handles read-only priority calls: `get_leader` and `get_members`.".
-endif.
-spec handle_call(term(), dgen_server:from(), registry_state()) ->
    dgen_server:reply_ret().

handle_call(get_leader, _From, State) ->
    {reply, maps:get(leader, State, undefined), State};
handle_call(get_members, _From, State) ->
    {reply, maps:keys(maps:get(members, State, #{})), State};
handle_call(get_epoch, _From, State) ->
    {reply, maps:get(epoch, State, 0), State}.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

%% Prefer the incumbent leader if it is still a member (sticky leadership).
%% This prevents thrashing when a non-leader node wins a queue transaction.
%% Falls back to the local node, then lists:min/1, when the incumbent is gone.
%% Returns {Leader, Epoch}. Epoch increments only when a new leader is chosen.
-spec elect_leader(
    dgen_server:tx_ctx(),
    atom(),
    #{member_id() => member_info()},
    member_id() | undefined,
    non_neg_integer()
) -> {member_id() | undefined, non_neg_integer()}.
elect_leader(#{td := {Tx, Dir}, tuid := Tuid}, Name, Members, OldLeader, OldEpoch) ->
    case OldLeader =/= undefined andalso maps:is_key(OldLeader, Members) of
        true ->
            B = dgen_config:backend(),
            B:set(Tx, B:dir_pack(Dir, leader_key_tuple(Tuid)), term_to_binary(OldLeader)),
            {OldLeader, OldEpoch};
        false ->
            LocalId = {node(), dgen_registry:member_name(Name)},
            Leader =
                case maps:is_key(LocalId, Members) of
                    true ->
                        LocalId;
                    false ->
                        case maps:keys(Members) of
                            [] -> undefined;
                            Ids -> lists:min(Ids)
                        end
                end,
            B = dgen_config:backend(),
            B:set(Tx, B:dir_pack(Dir, leader_key_tuple(Tuid)), term_to_binary(Leader)),
            {Leader, OldEpoch + 1}
    end.

-if(?DOCATTRS).
-doc """
Returns the packed backend key for the leader value.

Exported so callers can set up a backend watch without going through the
elector process.
""".
-endif.
-spec leader_db_key(dgen_backend:dir(), dgen_server:tuid()) -> dgen_backend:key().
leader_db_key(Dir, Tuid) ->
    B = dgen_config:backend(),
    B:dir_pack(Dir, leader_key_tuple(Tuid)).

leader_key_tuple(Tuid) ->
    dgen_key:extend(Tuid, <<"leader">>).

%% Extract {MemberId => Token} from the members map for passing to member processes.
member_tokens(Members) ->
    maps:map(fun(_, #{join_token := T}) -> T end, Members).

call_to_member({Node, Name}, Msg) ->
    %% Guard against automatic distribution reconnect: if the target node is
    %% not currently connected, exit immediately rather than letting
    %% gen_server:call trigger a reconnect.  An unintended reconnect during the
    %% handoff fires {nodeup} on both sides, causing the peer to re-join with a new
    %% token — the old {member_down} is then discarded as stale and the partition is
    %% never detected.  (This guard is independent of the now-removed distributed
    %% lock; keep it regardless — §5.7.)
    case Node =:= node() orelse lists:member(Node, nodes()) of
        true -> gen_server:call({Name, Node}, Msg, ?SnapshotTimeout);
        false -> exit({nodedown, Node})
    end.
