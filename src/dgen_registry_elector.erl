-module(dgen_registry_elector).
-behaviour(dgen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
`dgen_server` callback module that tracks registry membership and elects a leader.

The elector's state (member map + current leader) is stored durably in the
backend and is shared across every node that runs the same named registry.
Messages arrive via a durable FIFO queue, so membership changes are processed
one at a time, serialised through the backend.

## Leader election

The leader is determined by `node()` — the Erlang node of the elector
consumer that commits the current backend transaction. This ties leadership
directly to backend consensus: whoever successfully commits is the leader.

If the local node does not yet have a registered member (transient window
during startup), `lists:min/1` over live member IDs is used as a fallback.

## Lock on leadership change

When the elected leader changes, the callback returns `{lock, NewState}`
instead of `{noreply, …}`. This atomically:

1. Commits the new leader and member set to the backend.
2. Sets a distributed lock key that pauses all other elector consumers
   from processing further queue messages.

`handle_locked/3` is then called (synchronously, before the lock clears)
with the committed state. It performs the one-time fan-out:

- Tells every member who the new leader is (`{leader_changed, Leader}`).
- Gives the new member its peer list and the new leader.
- Gives existing members the new member's identity.

The lock clears automatically after `handle_locked` returns, signalling
all waiting consumers to resume.

## Leader key in the backend

```
Key path: {Tuid, <<"leader">>}
Value:    term_to_binary(MemberId | undefined)
```
""".
-endif.

-export([
    init/1,
    handle_cast_tx/3,
    handle_call_tx/4,
    handle_call/3,
    handle_locked/3,
    leader_db_key/2
]).

-export_type([member_id/0, member_info/0, registry_state/0]).

-type member_id() :: {node(), atom()}.
-type member_info() :: #{joined_at := integer()}.
-type registry_state() :: #{
    name := atom(),
    members := #{member_id() => member_info()},
    leader := member_id() | undefined
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
    State = #{name => Name, members => #{}, leader => undefined},
    {ok, Tuid, State}.

-if(?DOCATTRS).
-doc "Delegates read-only priority calls to `handle_call/3`.".
-endif.
-spec handle_call_tx(dgen_server:tx_ctx(), term(), dgen_server:from(), registry_state()) ->
    dgen_server:reply_ret().
handle_call_tx(_TxCtx, Request, From, State) ->
    handle_call(Request, From, State).

-if(?DOCATTRS).
-doc """
Processes membership change messages within a backend transaction.

Handles `{join, MemberId}` and `{member_down, MemberId}`. Returns
`{lock, NewState}` when leadership changes, `{noreply, NewState}` otherwise.
""".
-endif.
-spec handle_cast_tx(dgen_server:tx_ctx(), term(), registry_state()) ->
    dgen_server:noreply_ret() | dgen_server:lock_ret().

handle_cast_tx(TxCtx, {join, MemberId}, State) ->
    #{name := Name, members := Members, leader := OldLeader} = State,
    MemberInfo = #{joined_at => erlang:system_time(millisecond)},
    NewMembers = Members#{MemberId => MemberInfo},
    NewLeader = elect_leader(TxCtx, Name, NewMembers),
    NewState = State#{members => NewMembers, leader => NewLeader},
    if
        NewLeader =/= OldLeader ->
            %% Leadership changed — lock so handle_locked can set up replication paths.
            {lock, NewState};
        true ->
            %% Leader unchanged — notify about the new member via post-commit actions.
            Actions = [
                fun(#{members := All, leader := L}) ->
                    ExistingIds = maps:keys(maps:remove(MemberId, All)),
                    cast_to_member(MemberId, {members, maps:keys(All)}),
                    cast_to_member(MemberId, {leader_changed, L}),
                    lists:foreach(
                        fun(Id) -> cast_to_member(Id, {new_member, MemberId}) end,
                        ExistingIds
                    )
                end
            ],
            {noreply, NewState, Actions}
    end;
handle_cast_tx(TxCtx, {member_down, MemberId}, State) ->
    #{name := Name, members := Members, leader := OldLeader} = State,
    case maps:is_key(MemberId, Members) of
        false ->
            %% Already removed — idempotent.
            {noreply, State};
        true ->
            NewMembers = maps:remove(MemberId, Members),
            NewLeader = elect_leader(TxCtx, Name, NewMembers),
            NewState = State#{members => NewMembers, leader => NewLeader},
            if
                NewLeader =/= OldLeader ->
                    %% Leadership changed — lock to set up new replication paths.
                    {lock, NewState};
                true ->
                    %% Leader unchanged — each surviving member detected the death
                    %% via its own erlang:monitor DOWN signal; no broadcast needed.
                    {noreply, NewState}
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
    {reply, maps:keys(maps:get(members, State, #{})), State}.

-if(?DOCATTRS).
-doc """
Called synchronously after a `{lock, NewState}` commit, before the lock clears.

Sets up replication paths by notifying all members of the new leader.
The lock clears automatically (in dgen_server's `after` block) once this
callback returns, regardless of return value.
""".
-endif.
-spec handle_locked(dgen_server:event_type(), term(), registry_state()) ->
    dgen_server:noreply_ret().

handle_locked(cast, {join, MemberId}, State) ->
    #{members := Members, leader := Leader} = State,
    AllIds = maps:keys(Members),
    ExistingIds = maps:keys(maps:remove(MemberId, Members)),
    %% New member: who all the peers are + who the leader is.
    cast_to_member(MemberId, {members, AllIds}),
    cast_to_member(MemberId, {leader_changed, Leader}),
    %% Existing members: identity of the new peer + updated leader.
    lists:foreach(
        fun(Id) ->
            cast_to_member(Id, {new_member, MemberId}),
            cast_to_member(Id, {leader_changed, Leader})
        end,
        ExistingIds
    ),
    {noreply, State};
handle_locked(cast, {member_down, _MemberId}, State) ->
    #{members := Members, leader := Leader} = State,
    %% Surviving members already detected the death via their own DOWN signals.
    %% Only the new leader identity needs to be broadcast.
    broadcast_to_members(Members, {leader_changed, Leader}),
    {noreply, State}.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

%% Elect the leader as the member on the current node (DB consensus winner).
%% Falls back to lists:min/1 if this node has no registered member yet.
-spec elect_leader(dgen_server:tx_ctx(), atom(), #{member_id() => member_info()}) ->
    member_id() | undefined.
elect_leader(#{td := {Tx, Dir}, tuid := Tuid}, Name, Members) ->
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
    Leader.

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

broadcast_to_members(Members, Msg) ->
    lists:foreach(fun(Id) -> cast_to_member(Id, Msg) end, maps:keys(Members)).

cast_to_member({Node, Name}, Msg) ->
    gen_server:cast({Name, Node}, Msg).
