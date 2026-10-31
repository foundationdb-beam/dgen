-module(dgen_registry_elector).
-behaviour(dgen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("../../include/dgen_eta.hrl").

% Actions (aka side effects) taken by the elector
-define(ActionPushLeaderToMembers(MemberId),
    dgen_registry_elector_actions:push_leader_to_members(MemberId)
).
-define(ActionPushPresenceToLeader(Msg),
    dgen_registry_elector_actions:push_presence_to_leader(Msg)
).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% A `dgen_server` callback that tracks registry membership and elects a leader. Its
%% state is durable and shared by every node of the registry, and messages reach it
%% through a durable FIFO queue. See docs/design/dgen_registry_design.md.
%%
%% Leadership is sticky. The current leader keeps the role as long as it is a member,
%% which avoids thrashing. A membership change reaches the leader as a post-commit
%% action, with no distributed lock (§5.7). Committing a new leader key fences the old
%% leader, so its next batch commit aborts. The leader gathers replicas, resolves
%% conflicts (§5.6) and casts the snapshot to followers from its own process. Erlang
%% delivers messages between two processes in order, so each follower gets the
%% snapshot before any later `{name_registered}`.
%%
%% The leader key `{Tuid, <<"leader">>}` holds `term_to_binary({MemberId | undefined,
%% Epoch})`. The epoch is what makes it a real fencing token. Without it, deposing a
%% leader and re-electing the same member would leave the key unchanged.
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
    get_leader_db_key/2,
    tuid/1,

    init/1,
    handle_cast_tx/3,
    handle_call_tx/4
]).

-export_type([member_id/0, member_info/0, registry_state/0]).

-type member_id() :: {node(), atom()}.
-type member_info() :: #{
    % Each member joins with a fresh token. When a member goes down, the token tells
    % us whether the DOWN refers to this join or an earlier one.
    join_token := reference(),

    % `true` when the member has not yet synced any state. Used in state replication.
    fresh => boolean()
}.

% Stored in FDB. Any proposed changes to the state have implications on preexisting registries.
-type registry_state() :: #{
    % The registry name
    name := atom(),

    % The list of members who have joined
    members := #{member_id() => member_info()},

    % The authoritative leader. Only `dgen_registry_elector` chooses the leader.
    leader := member_id() | undefined,

    % Increases with every leadership change, so re-electing the same member still
    % changes the leader key.
    epoch := non_neg_integer(),

    %% Durable presence subscriptions (§4.9), as `SubId => {Watch, Notify}`. They
    %% survive a full cluster restart. This map is the source of truth, and the leader
    %% receives changes to it as deltas.
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
Returns the token the elector holds for `MemberId` right now. Returns `Default` if
the member is unknown or the read fails.

This is a priority read, so it skips the durable queue. Callers attach the result to
a `{member_down}`. Because it is the elector's *current* token, a DOWN for a peer that
has since rejoined with a new token is ignored, while a member that is really dead is
still reaped. It has no side effects, so it is safe to call from a spawned helper. The
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
-spec get_leader_db_key(dgen_backend:dir(), dgen_server:tuid()) -> dgen_backend:key().
get_leader_db_key(Dir, Tuid) ->
    B = dgen_config:backend(),
    B:dir_pack(Dir, leader_key_tuple(Tuid)).

-if(?DOCATTRS).
-doc """
Returns the tenant-unique id for registry `Name`. It is the backend keyspace prefix.

The elector, the member, and the top-level supervisor must all use the *same* tuid.
The member needs it for its version-key commit and its leader-key fence. So they all
derive it here instead of each rebuilding the literal, which is why it is exported.
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
Initialises the elector state with an empty member map and an undefined leader.

`name` is the **identity** of the member on the same node. `elect_leader/5` turns it
into the local `member_id/0` when it prefers the local member for leadership.
`keyspace` is the registry's durable **prefix**, and the tuid is derived from it. It
defaults to `name`. The two differ only when the registry is started with the
`keyspace` option of `dgen_registry:start_link/3`, which lets several members of one
registry share a VM. See that function's docs.
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
Returns `{noreply, NewState, Actions}` when the handoff must go through the leader.
That is every join, and every member_down that changes the leader. Otherwise it
returns plain `{noreply, NewState}`. The `Actions` run after the transaction commits,
so no distributed lock is needed (§5.7).

Each `{join}` carries a unique token and a freshness flag. The member process creates
the token with `make_ref()` before enqueuing. The flag says whether the member has
ever synced registry state. The elector stores both in `member_info`.

We silently drop a `{member_down, MemberId, Token}` when its token doesn't match the
stored token for that member. A mismatch means the member has rejoined with a new
token since the DOWN was detected, so the message is stale. This prevents a race in
partition recovery, where a `{member_down}` enqueued during the disconnect is
processed after the `{join}` that heals the cluster.
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
    %% Every join goes through the leader, so the joiner gets its snapshot from the same
    %% process that sends later name broadcasts, and receives them in order.
    {noreply, NewState, [?ActionPushLeaderToMembers(MemberId)]};
handle_cast_tx(TxCtx, {member_down, MemberId, Token}, State) ->
    #{name := Name, members := Members, leader := OldLeader, epoch := OldEpoch} = State,
    case maps:get(MemberId, Members, undefined) of
        undefined ->
            {noreply, State};
        #{join_token := StoredToken} when StoredToken =/= Token ->
            %% Stale. The member rejoined with a new token after this DOWN was detected.
            {noreply, State};
        _ ->
            NewMembers = maps:remove(MemberId, Members),
            {NewLeader, NewEpoch} = elect_leader(TxCtx, Name, NewMembers, OldLeader, OldEpoch),
            NewState = State#{members => NewMembers, leader => NewLeader, epoch => NewEpoch},
            case NewLeader =:= OldLeader of
                true ->
                    %% Survivors saw the death through their own monitors, so we
                    %% don't broadcast.
                    {noreply, NewState};
                false ->
                    {noreply, NewState, [?ActionPushLeaderToMembers(undefined)]}
            end
    end;
%% Presence writes (§4.9) are casts for latency, so `ok` means accepted, not committed.
%% Each is an idempotent upsert keyed by the application's id, so the caller can simply
%% repeat a write that was lost before the elector consumed it.
handle_cast_tx(_TxCtx, {subscribe, SubId, Watch, Notify}, State) ->
    Subs = get_subscriptions(State),
    NewState = State#{subscriptions => Subs#{SubId => {Watch, Notify}}},
    {noreply, NewState, [?ActionPushPresenceToLeader({subscribe, SubId, Watch, Notify})]};
handle_cast_tx(_TxCtx, {unsubscribe, SubId}, State) ->
    Subs = get_subscriptions(State),
    NewState = State#{subscriptions => maps:remove(SubId, Subs)},
    {noreply, NewState, [?ActionPushPresenceToLeader({unsubscribe, SubId})]};
handle_cast_tx(_TxCtx, unsubscribe_all, State) ->
    {noreply, State#{subscriptions => #{}}, [?ActionPushPresenceToLeader(unsubscribe_all)]}.

get_subscriptions(State) ->
    maps:get(subscriptions, State, #{}).

%% ---------------------------------------------------------------------------
%% handle_call_tx/4  (all elector calls)
%% ---------------------------------------------------------------------------

%% When the `_tx` variant is exported, dgen_server sends every call to it, priority
%% reads included. So there is deliberately no `handle_call/3`.
-if(?DOCATTRS).
-doc """
Handles the elector's read-only priority calls: `get_leader`, `get_members`, `get_epoch`,
`{get_member_token, _}`, and `get_subscriptions`, which returns the durable
subscription map (§4.9). The presence writes are casts. See handle_cast_tx/3.
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
%% Reads the token from durable state, so a `{member_down}` carries the token the
%% elector actually holds. A stale cached token would cause a real death to be
%% discarded (§5.7).
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

%% Keeps the current leader if it is still a member. Otherwise it picks the local
%% member, since this node won the transaction race, and failing that the lowest
%% member id. The epoch increments only when a new leader is chosen.
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
