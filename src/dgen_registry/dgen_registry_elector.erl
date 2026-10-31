-module(dgen_registry_elector).
-behaviour(dgen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("../../include/dgen_eta.hrl").

% Actions (aka side effects) taken by the elector
-define(ActionAssumeDistribute(MemberId), dgen_registry_elector_actions:assume_distribute(MemberId)).
-define(ActionPresencePush(Msg), dgen_registry_elector_actions:presence_push(Msg)).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% `dgen_server` callback tracking registry membership and electing a leader.  State
%% is durable and shared by every node of the registry; messages arrive via a durable
%% FIFO queue. See docs/design/dgen_registry_design.md.
%%
%% Leadership is sticky (kept while the incumbent is a member) to avoid thrashing.
%% A membership change routes through the leader as a post-commit action, with no
%% distributed lock (§5.7): committing the new leader key fences the old leader, whose
%% next batch commit then aborts.  The leader gathers replicas, resolves conflicts
%% (§5.6) and casts the snapshot to followers from its own process, so per-pair FIFO
%% orders it before any later `{name_registered}`.
%%
%% Leader key `{Tuid, <<"leader">>}` holds `term_to_binary({MemberId | undefined,
%% Epoch})`: the epoch makes it a real fencing token (no ABA on depose-and-re-elect).
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([
    join/4,
    member_down/3,
    get_members/1,

    subscribe/4,
    unsubscribe/2,
    unsubscribe_all/1,

    priority_get_members/1,
    priority_get_leader/1,
    priority_get_epoch/1,
    priority_get_subscriptions/1,
    priority_get_subscriptions/2,
    get_member_token/3,
    leader_db_key/2,
    tuid/1,

    init/1,
    handle_cast_tx/3,
    handle_call_tx/4
]).

-export_type([member_id/0, member_info/0, registry_state/0]).

-type member_id() :: {node(), atom()}.
-type member_info() :: #{
    % Each member joins with a token so that the system can differentiate messages when
    % members go down. ABA-protection
    join_token := reference(),

    % `true` when the member has not yet synced any state. Used in state replication
    fresh => boolean()
}.

% Stored in FDB. Any proposed changes to the state have implications on preexisting registries.
-type registry_state() :: #{
    % The registry name
    name := atom(),

    % The list of members who have joined
    members := #{member_id() => member_info()},

    % The authoritative leader. The `dgen_registry_elector` is solely responsible for choosing the leader.
    leader := member_id() | undefined,

    % A monotonic integer for leadership changes. ABA protection.
    epoch := non_neg_integer(),

    %% Durable presence subscriptions (§4.9), `SubId => {Watch, Notify}`; survive a
    %% full cluster restart.  Source of truth; the leader is fed deltas.
    subscriptions => #{term() => {term(), term()}}
}.

%% ---------------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------------

join(Elector, MemberId, Token, Fresh) ->
    dgen_server:cast(Elector, {join, MemberId, Token, Fresh}).

member_down(Elector, MemberId, Token) ->
    dgen_server:cast(Elector, {member_down, MemberId, Token}).

get_members(Elector) ->
    dgen_server:call(Elector, get_members).

subscribe(Elector, SubId, Watch, Notify) ->
    dgen_server:cast(Elector, {subscribe, SubId, Watch, Notify}).

unsubscribe(Elector, SubId) ->
    dgen_server:cast(Elector, {unsubscribe, SubId}).

unsubscribe_all(Elector) ->
    dgen_server:cast(Elector, unsubscribe_all).

priority_get_members(Elector) ->
    dgen_server:priority_call(Elector, get_members).

priority_get_leader(Elector) ->
    dgen_server:priority_call(Elector, get_leader).

priority_get_epoch(Elector) ->
    dgen_server:priority_call(Elector, get_epoch).

priority_get_subscriptions(Elector) ->
    dgen_server:priority_call(Elector, get_subscriptions).

priority_get_subscriptions(Elector, Timeout) ->
    dgen_server:priority_call(Elector, get_subscriptions, Timeout).

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
-spec get_member_token(pid(), member_id(), Default) -> reference() | Default.
get_member_token(Elector, MemberId, Default) ->
    try dgen_server:priority_call(Elector, {get_member_token, MemberId}) of
        T when is_reference(T) -> T;
        _ -> Default
    catch
        _:_ -> Default
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

-if(?DOCATTRS).
-doc """
The tenant-unique id (backend keyspace prefix) for registry `Name`.

The single source of truth for the registry's backend key prefix: the elector, the
member (its version-key commit and leader-key fence), and the top-level supervisor
must all use the *same* tuid, so they derive it here rather than each rebuilding the
literal.  Exported for that reason.
""".
-endif.
-spec tuid(atom()) -> dgen_server:tuid().
tuid(Name) ->
    {<<"dgen_registry.">>, atom_to_binary(Name)}.

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
    % Reminder: init/1 is always called, but the initial state is only relevant when there
    % exists no state in FDB for this tuid.
    State = #{
        name => Name, members => #{}, leader => undefined, epoch => 0, subscriptions => #{}
    },
    {ok, tuid(maps:get(keyspace, Args, Name)), State}.

-if(?DOCATTRS).
-doc """
Processes membership change messages within a backend transaction.

Handles `{join, MemberId, Token, Fresh}` and `{member_down, MemberId, Token}`.
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

handle_cast_tx(TxCtx, {join, MemberId, Token, Fresh}, State) ->
    #{name := Name, members := Members, leader := OldLeader, epoch := OldEpoch} = State,
    MemberInfo = #{join_token => Token, fresh => Fresh},
    NewMembers = Members#{MemberId => MemberInfo},
    {NewLeader, NewEpoch} = elect_leader(TxCtx, Name, NewMembers, OldLeader, OldEpoch),
    NewState = State#{members => NewMembers, leader => NewLeader, epoch => NewEpoch},
    %% Every join routes through the leader so the joiner gets the snapshot from the same
    %% sender as later name broadcasts (FIFO).
    {noreply, NewState, [?ActionAssumeDistribute(MemberId)]};
handle_cast_tx(TxCtx, {member_down, MemberId, Token}, State) ->
    #{name := Name, members := Members, leader := OldLeader, epoch := OldEpoch} = State,
    case maps:get(MemberId, Members, undefined) of
        undefined ->
            {noreply, State};
        #{join_token := StoredToken} when StoredToken =/= Token ->
            %% Stale: member rejoined with a new token since this DOWN was detected.
            {noreply, State};
        _ ->
            NewMembers = maps:remove(MemberId, Members),
            {NewLeader, NewEpoch} = elect_leader(TxCtx, Name, NewMembers, OldLeader, OldEpoch),
            NewState = State#{members => NewMembers, leader => NewLeader, epoch => NewEpoch},
            case NewLeader =:= OldLeader of
                true ->
                    %% Survivors saw the death via their own monitors; no broadcast.
                    {noreply, NewState};
                false ->
                    {noreply, NewState, [?ActionAssumeDistribute(undefined)]}
            end
    end;
%% Presence writes (§4.9) are casts for latency: `ok` means accepted, not committed.
%% Idempotent upserts by application id, so a write lost before consume is recoverable.
handle_cast_tx(_TxCtx, {subscribe, SubId, Watch, Notify}, State) ->
    Subs = get_subscriptions(State),
    NewState = State#{subscriptions => Subs#{SubId => {Watch, Notify}}},
    {noreply, NewState, [?ActionPresencePush({subscribe, SubId, Watch, Notify})]};
handle_cast_tx(_TxCtx, {unsubscribe, SubId}, State) ->
    Subs = get_subscriptions(State),
    NewState = State#{subscriptions => maps:remove(SubId, Subs)},
    {noreply, NewState, [?ActionPresencePush({unsubscribe, SubId})]};
handle_cast_tx(_TxCtx, unsubscribe_all, State) ->
    {noreply, State#{subscriptions => #{}}, [?ActionPresencePush(unsubscribe_all)]}.

get_subscriptions(State) ->
    maps:get(subscriptions, State, #{}).

%% ---------------------------------------------------------------------------
%% handle_call_tx/4  (all elector calls)
%% ---------------------------------------------------------------------------

%% dgen_server dispatches every call (including priority reads) to the `_tx` variant
%% when exported, so there is deliberately no `handle_call/3`.
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
%% Fresh from durable state, so a `{member_down}` is fenced with the token the elector
%% actually holds, not a stale cached one that would get a genuine death discarded (§5.7).
handle_call_tx(_TxCtx, {get_member_token, MemberId}, _From, State) ->
    Token =
        case maps:get(MemberId, maps:get(members, State, #{}), undefined) of
            #{join_token := T} -> T;
            _ -> undefined
        end,
    {reply, Token, State};
handle_call_tx(_TxCtx, get_subscriptions, _From, State) ->
    {reply, get_subscriptions(State), State}.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

%% Sticky incumbent, else the local member (the transaction-race winner), else
%% lists:min/1.  Epoch increments only when a new leader is chosen.
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

leader_key_tuple(Tuid) ->
    dgen_key:extend(Tuid, <<"leader">>).
