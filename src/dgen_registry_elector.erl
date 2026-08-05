-module(dgen_registry_elector).
-behaviour(dgen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("../include/dgen_eta.hrl").
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
%%    `{elector_assume_and_distribute, TriggerMember, AllIds, Tokens, FreshIds, Epoch}`. The
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
%% Value:    term_to_binary({MemberId | undefined, Epoch})
%% ```
%%
%% The value carries the **epoch alongside the member id** so the fence is a real
%% fencing token, not just an identity check: a leader that is deposed and later
%% re-elected gets a *higher* epoch, so a commit it started in its earlier term can
%% never slip past the fence during its later term (the ABA case). Values written by
%% older versions (a bare MemberId) are tolerated by the fence readers in
%% `dgen_registry_names` until the next election rewrites the key.

-export([
    init/1,
    handle_cast_tx/3,
    handle_call_tx/4,
    leader_db_key/2,
    tuid/1,
    member_token/3
]).

-export_type([member_id/0, member_info/0, registry_state/0]).

-type member_id() :: {node(), atom()}.
%% `fresh` records whether the member had ever synced any registry state when it
%% announced itself (`true` = a brand-new/empty member).  A fresh member holds no
%% bindings by definition, so a leader whose handoff gather cannot reach it is not
%% actually missing anything — it is excluded from the `degraded` computation
%% (§5.6 prevention).  The member re-announces with `fresh = false` after its first
%% snapshot, so the flag converges.
-type member_info() :: #{
    joined_at := integer(), join_token := reference(), fresh => boolean()
}.
-type registry_state() :: #{
    name := atom(),
    members := #{member_id() => member_info()},
    leader := member_id() | undefined,
    epoch := non_neg_integer(),
    %% Durable presence subscriptions (§4.9): `SubId => {Watch, Notify}`, keyed by an
    %% application-supplied id.  Stored in the elector's *durable* dgen_server state, so
    %% they outlive the Erlang cluster entirely — an application can tie a subscription
    %% to a database entity's lifetime and have it survive a scale-to-zero and come back
    %% intact.  The current leader is fed this set (a delta on each change, the full map
    %% on a handoff) and computes the notifications; the elector is the source of truth.
    subscriptions => #{term() => {term(), term()}}
}.

%% ---------------------------------------------------------------------------
%% dgen_server callbacks
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Initialises the elector state with an empty member map and undefined leader.

`name` is the co-located member's **identity** — it is what `elect_leader/5` turns
into the local `member_id/0` when preferring the local member for leadership.
`keyspace` is the registry's durable **prefix**, and is what the tuid is derived
from; it defaults to `name`. The two differ only when a registry is started with
`dgen_registry:start_link/3`'s `keyspace` option, which lets several members of one
registry share a VM (see that function's docs).
""".
-endif.
-spec init(#{name := atom(), keyspace => atom()}) ->
    {ok, dgen_server:tuid(), registry_state()}.
init(Args = #{name := Name}) ->
    State = #{
        name => Name, members => #{}, leader => undefined, epoch => 0, subscriptions => #{}
    },
    {ok, tuid(maps:get(keyspace, Args, Name)), State}.

-if(?DOCATTRS).
-doc """
The transaction-unit id (backend keyspace prefix) for registry `Name`.

The single source of truth for the registry's backend key prefix: the elector, the
member (its version-key commit and leader-key fence), and the top-level supervisor
must all use the *same* tuid, so they derive it here rather than each rebuilding the
literal.  Exported for that reason.
""".
-endif.
-spec tuid(atom()) -> dgen_server:tuid().
tuid(Name) ->
    {<<"dgen_registry.">>, atom_to_binary(Name)}.

-if(?DOCATTRS).
-doc """
Processes membership change messages within a backend transaction.

Handles `{join, MemberId, Token, Fresh}` (a legacy 3-tuple `{join, MemberId,
Token}` is accepted as `Fresh = false`) and `{member_down, MemberId, Token}`.
Returns `{noreply, NewState, Actions}` when the handoff must be routed through the
leader (every join; a leader-changing member_down), else plain
`{noreply, NewState}`. The `Actions` run after the transaction commits — no
distributed lock (§5.7).

Each `{join}` carries a unique token (a `reference()` generated by the member
process before enqueuing) and a freshness flag (whether the member has ever synced
registry state).  The elector stores both in `member_info`.

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
    %% Legacy 3-tuple join (pre-`fresh`): treat as non-fresh — the conservative
    %% choice, preserving the old degraded computation for that member.
    handle_cast_tx(TxCtx, {join, MemberId, Token, false}, State);
handle_cast_tx(TxCtx, {join, MemberId, Token, Fresh}, State) ->
    #{name := Name, members := Members, leader := OldLeader, epoch := OldEpoch} = State,
    MemberInfo = #{
        joined_at => erlang:system_time(millisecond), join_token => Token, fresh => Fresh
    },
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
    end;
%% Presence subscription writes (§4.9).  Cast, not call: the public API fires these
%% fire-and-forget for latency, so `ok` means "accepted for durable processing", not yet
%% "committed" — the same posture as the membership casts above.  Durability still holds
%% (the change commits in this consume transaction and survives a full cluster restart),
%% and because subscriptions are idempotent upserts keyed by an application-supplied id,
%% a write lost in the pre-consume window is recoverable by re-subscribing.  Each returns
%% a post-commit action that pushes the change to the current leader.
handle_cast_tx(_TxCtx, {subscribe, SubId, Watch, Notify}, State) ->
    Subs = subscriptions_of(State),
    NewState = State#{subscriptions => Subs#{SubId => {Watch, Notify}}},
    {noreply, NewState, [presence_push_action({subscribe, SubId, Watch, Notify})]};
handle_cast_tx(_TxCtx, {unsubscribe, SubId}, State) ->
    Subs = subscriptions_of(State),
    NewState = State#{subscriptions => maps:remove(SubId, Subs)},
    {noreply, NewState, [presence_push_action({unsubscribe, SubId})]};
handle_cast_tx(_TxCtx, unsubscribe_all, State) ->
    {noreply, State#{subscriptions => #{}}, [presence_push_action(unsubscribe_all)]}.

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
                    %% A newly-assuming leader seeds its presence state (§4.9) by pulling
                    %% the durable subscription set from its *co-located* elector when it
                    %% finishes assuming — so we do not thread it through here.
                    call_to_member(
                        Leader,
                        {elector_assume_and_distribute, TriggerMember, maps:keys(All),
                            member_tokens(All), fresh_ids(All), Epoch}
                    )
                catch
                    exit:Reason ->
                        depose_if_dead(Leader, Reason, All)
                end
        end
    end.

%% Read the durable subscription map, tolerating durable state written by a
%% pre-presence version (no `subscriptions` key).
subscriptions_of(State) ->
    maps:get(subscriptions, State, #{}).

%% The assume call to the leader failed.  If it failed with `noproc` — we *reached* the
%% leader's node but found no member process registered there — the leader's registry is
%% gone (e.g. a graceful supervisor stop) while its node stays up.  Leaving it as leader
%% would strand the cluster: a dead leader distributes nothing, and this can persist when
%% a `{join, Leader, NewToken}` a member enqueued on a nodeup is processed *after* the
%% member died, resurrecting it as leader.  So enqueue a `{member_down}` fenced with the
%% leader's current token (from `All`, so a genuine concurrent re-join is not clobbered)
%% to force a re-election.
%%
%% Only `noproc` is treated as death: it means the node answered.  A *not-connected*
%% failure is deliberately NOT deposed here — it is ambiguous (a live leader simply not
%% yet meshed to whichever node consumed this change would be wrongly unseated), and any
%% liveness probe that could disambiguate would have to open a distribution connection,
%% which would defeat partition detection elsewhere (see dgen_registry_member's mesh
%% suppression).  A member on a genuinely dead *node* is instead removed by its peers'
%% own monitor DOWNs (fenced with the elector's current token — see the member's DOWN
%% handler).  A plain call timeout on a connected node is a live-but-slow leader and is
%% left alone.
depose_if_dead(Leader, Reason, All) ->
    case is_noproc(Reason) andalso maps:get(Leader, All, undefined) of
        #{join_token := Token} ->
            dgen_server:cast(self(), {member_down, Leader, Token});
        _ ->
            ok
    end.

is_noproc(noproc) -> true;
is_noproc({noproc, _}) -> true;
is_noproc(_) -> false.

%% ---------------------------------------------------------------------------
%% handle_call_tx/4  (all elector calls)
%% ---------------------------------------------------------------------------

%% dgen_server dispatches *every* call — priority reads (`priority_call`, which still
%% bypass the durable queue and locks) and durable queued calls alike — to the `_tx`
%% variant when a module exports it (see dgen_server:invoke_tx_callback/4).  The elector
%% only ever receives priority reads here (the presence *writes* are casts, handled by
%% handle_cast_tx), so all clauses just reply.  (There is deliberately no `handle_call/3`:
%% exporting it too would be dead code, since the `_tx` variant always wins the dispatch.)
-if(?DOCATTRS).
-doc """
Handles the elector's read-only priority calls: `get_leader`, `get_members`, `get_epoch`,
`{get_member_token, _}`, and `get_subscriptions` (the durable subscription map, §4.9).
The presence writes are casts — see handle_cast_tx/3.
""".
-endif.
-spec handle_call_tx(dgen_server:tx_ctx(), term(), dgen_server:from(), registry_state()) ->
    dgen_server:reply_ret().
handle_call_tx(_TxCtx, get_leader, _From, State) ->
    {reply, maps:get(leader, State, undefined), State};
handle_call_tx(_TxCtx, get_members, _From, State) ->
    {reply, maps:keys(maps:get(members, State, #{})), State};
handle_call_tx(_TxCtx, get_epoch, _From, State) ->
    {reply, maps:get(epoch, State, 0), State};
%% Return the current stored join token for a member (or `undefined` if it is not a
%% member).  Read fresh from the durable elector state, so a member reporting a peer's
%% death can fence its `{member_down}` with the token the elector *actually* holds — not
%% a locally cached one that a dropped snapshot may have left stale.  Without this, a
%% member whose peer re-announced a fresh token (on a nodeup) that never reached this
%% node would fence with the old token, and a genuine death would be discarded as stale,
%% stranding the dead member in the set (§5.7).
handle_call_tx(_TxCtx, {get_member_token, MemberId}, _From, State) ->
    Token =
        case maps:get(MemberId, maps:get(members, State, #{}), undefined) of
            #{join_token := T} -> T;
            _ -> undefined
        end,
    {reply, Token, State};
%% Read the durable subscription map (§4.9) — reflects the elector's committed set.
handle_call_tx(_TxCtx, get_subscriptions, _From, State) ->
    {reply, subscriptions_of(State), State}.

%% Post-commit action that pushes a subscription change to the current leader as an
%% epoch-stamped `{presence_update, …}` cast.  Fire-and-forget: the leader applies it
%% if the epoch is current, and a drop (unreachable leader) is caught up by the reseed
%% on the next handoff.  No leader yet → nothing to push; the reseed covers it.
presence_push_action(Update) ->
    fun(#{leader := Leader, epoch := Epoch}) ->
        case Leader of
            undefined -> ok;
            _ -> push_to_leader(Leader, {presence_update, Update, Epoch})
        end
    end.

%% Cast to the leader member, guarding against an automatic distribution reconnect to a
%% disconnected node (the same hazard call_to_member/2 guards against — see §5.7).
push_to_leader({Node, Name}, Msg) ->
    case dgen_utils:node_reachable(Node) of
        true -> gen_server:cast({Name, Node}, Msg);
        false -> ok
    end.

-if(?DOCATTRS).
-doc """
Client-side helper: read the token the elector holds for `MemberId` right now,
falling back to `Default` if the member is unknown or the read fails.

A priority read (bypasses the durable queue).  This is the fence for a
`{member_down}`: reporting the elector's *current* token means a peer that has
genuinely rejoined (advancing its token) is not clobbered by a stale DOWN, while a
truly dead member is still reaped.  Pure — safe to call from an off-loop helper; the
member's peer-DOWN path and the connector's reap both use it.
""".
-endif.
-spec member_token(pid(), member_id(), Default) -> reference() | Default.
member_token(Elector, MemberId, Default) ->
    try dgen_server:priority_call(Elector, {get_member_token, MemberId}) of
        T when is_reference(T) -> T;
        _ -> Default
    catch
        _:_ -> Default
    end.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

%% Prefer the incumbent leader if it is still a member (sticky leadership).
%% This prevents thrashing when a non-leader node wins a queue transaction.
%% Falls back to the local node, then lists:min/1, when the incumbent is gone.
%% Returns {Leader, Epoch}. Epoch increments only when a new leader is chosen.
%%
%% The leader key's value is `{Leader, Epoch}` — the epoch makes the fence a
%% monotonic fencing token (see the "Leader key in the backend" moduledoc note): a
%% commit started under an older epoch of the *same* member cannot pass the fence
%% after a depose-and-re-elect, because the re-election carries a higher epoch.
-spec elect_leader(
    dgen_server:tx_ctx(),
    atom(),
    #{member_id() => member_info()},
    member_id() | undefined,
    non_neg_integer()
) -> {member_id() | undefined, non_neg_integer()}.
elect_leader(#{td := {Tx, Dir}, tuid := Tuid}, Name, Members, OldLeader, OldEpoch) ->
    {Leader, Epoch} =
        case OldLeader =/= undefined andalso maps:is_key(OldLeader, Members) of
            true ->
                {OldLeader, OldEpoch};
            false ->
                LocalId = {node(), dgen_registry:member_name(Name)},
                NewLeader =
                    case maps:is_key(LocalId, Members) of
                        true ->
                            LocalId;
                        false ->
                            case maps:keys(Members) of
                                [] -> undefined;
                                Ids -> lists:min(Ids)
                            end
                    end,
                {NewLeader, OldEpoch + 1}
        end,
    B = dgen_config:backend(),
    B:set(Tx, B:dir_pack(Dir, leader_key_tuple(Tuid)), term_to_binary({Leader, Epoch})),
    {Leader, Epoch}.

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

%% Member ids currently marked fresh (never-synced), for the leader's `degraded`
%% computation — an unreachable fresh member cannot be holding bindings.
fresh_ids(Members) ->
    [Id || {Id, Info} <- maps:to_list(Members), maps:get(fresh, Info, false)].

call_to_member({Node, Name}, Msg) ->
    %% Guard against automatic distribution reconnect: if the target node is
    %% not currently connected, exit immediately rather than letting
    %% gen_server:call trigger a reconnect.  An unintended reconnect during the
    %% handoff fires {nodeup} on both sides, causing the peer to re-join with a new
    %% token — the old {member_down} is then discarded as stale and the partition is
    %% never detected.  (This guard is independent of the now-removed distributed
    %% lock; keep it regardless — §5.7.)
    case dgen_utils:node_reachable(Node) of
        true -> gen_server:call({Name, Node}, Msg, ?SnapshotTimeout);
        false -> exit({nodedown, Node})
    end.
