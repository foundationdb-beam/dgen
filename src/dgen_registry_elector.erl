-module(dgen_registry_elector).
-behaviour(dgen_server).

%% ---------------------------------------------------------------------------
%% Overview
%%
%% The elector is a dgen_server — its state (member map + current leader +
%% registered name table) is stored durably in FDB and is shared across every
%% node that runs the same named registry.  Messages arrive via a durable FIFO
%% queue, so even if the elector process crashes mid-way through a change, the
%% pending message will be replayed once the process restarts.
%%
%% Because dgen_server serialises queue consumption through FDB transactions,
%% the leader election and name registrations are consistent: only one node's
%% elector process can commit a state update at a time.
%%
%% Callback dispatch
%% -----------------
%% Mutating calls use `handle_call_tx/4` and `handle_cast_tx/3` — the `_tx`
%% variants that receive the live FDB transaction context.  This lets us write
%% the leader key atomically alongside the state update.
%%
%% Read-only priority_calls (`get_leader`, `get_members`, `get_names`) fall
%% through to `handle_call/3` via the catch-all clause in `handle_call_tx/4`.
%%
%% Name registration
%% -----------------
%% `{register, LogicalName, Pid}` is delivered via priority_call (bypasses the
%% durable queue) so that the caller gets a synchronous yes/no.  FDB's
%% serialisable isolation provides the race-free guarantee: two nodes that
%% simultaneously try to register the same name will conflict and one will
%% retry with the already-populated state, returning `no`.
%%
%% Leadership algorithm
%% --------------------
%% The current leader is `lists:min/1` over live member ids.  Member ids are
%% `{node(), atom()}` so the order is lexicographic — deterministic and
%% requires no additional coordination.
%%
%% Leader key in FDB
%% -----------------
%% In addition to the normal dgen_server state encoding, the elector writes a
%% standalone FDB key for the current leader whenever membership changes.
%% External processes can watch this key directly via the backend for
%% low-latency leader-change notifications.
%%
%%   Key path: {Tuid, <<"leader">>}
%%   Value:    term_to_binary(MemberId | undefined)
%% ---------------------------------------------------------------------------

-export([
    init/1,
    handle_cast_tx/3,
    handle_call_tx/4,
    handle_call/3,
    leader_fdb_key/2
]).

-export_type([member_id/0, member_info/0, registry_state/0]).

-type member_id() :: {node(), atom()}.
-type member_info() :: #{joined_at := integer()}.
-type registry_state() :: #{
    members := #{member_id() => member_info()},
    leader := member_id() | undefined,
    names := #{term() => pid()}
}.

%% ---------------------------------------------------------------------------
%% dgen_server callbacks
%% ---------------------------------------------------------------------------

-spec init(#{name := atom()}) -> {ok, dgen_server:tuid(), registry_state()}.
init(#{name := Name}) ->
    Tuid = {<<"dgen_registry">>, atom_to_binary(Name)},
    State = #{members => #{}, leader => undefined, names => #{}},
    {ok, Tuid, State}.

%% ---------------------------------------------------------------------------
%% handle_call_tx/4  (preferred over handle_call/3 by dgen_server)
%%
%% Handles mutating calls inside the live FDB transaction.
%% Falls through to handle_call/3 for read-only requests so we don't have to
%% duplicate those clauses here.
%% ---------------------------------------------------------------------------

-spec handle_call_tx(dgen_server:tx_ctx(), term(), dgen_server:from(), registry_state()) ->
    dgen_server:reply_ret().

handle_call_tx(_TxCtx, {register, LogicalName, Pid}, _From, State = #{names := Names}) ->
    case maps:is_key(LogicalName, Names) of
        true ->
            %% Name already taken — return no without modifying state.
            {reply, no, State};
        false ->
            NewState = State#{names => Names#{LogicalName => Pid}},
            Actions = [
                fun(#{members := Members}) ->
                    broadcast_to_members(Members, {name_registered, LogicalName, Pid})
                end
            ],
            {reply, yes, NewState, Actions}
    end;

handle_call_tx(_TxCtx, Request, From, State) ->
    %% Delegate read-only requests to the non-tx handler.
    handle_call(Request, From, State).

%% ---------------------------------------------------------------------------
%% handle_cast_tx/3
%%
%% Mutating casts run inside the FDB transaction so state + leader key are
%% updated atomically.
%% ---------------------------------------------------------------------------

-spec handle_cast_tx(dgen_server:tx_ctx(), term(), registry_state()) ->
    dgen_server:noreply_ret().

handle_cast_tx(TxCtx, {join, MemberId}, State = #{members := Members}) ->
    MemberInfo = #{joined_at => erlang:system_time(millisecond)},
    NewState = elect_leader(TxCtx, State#{members => Members#{MemberId => MemberInfo}}),
    Actions = [
        fun(#{members := All}) ->
            ExistingIds = maps:keys(maps:remove(MemberId, All)),
            notify_join(MemberId, maps:keys(All), ExistingIds)
        end
    ],
    {noreply, NewState, Actions};

handle_cast_tx(TxCtx, {member_down, MemberId}, State = #{members := Members}) ->
    NewState = elect_leader(TxCtx, State#{members => maps:remove(MemberId, Members)}),
    Actions = [
        fun(#{members := Remaining}) ->
            broadcast_to_members(Remaining, {member_down, MemberId})
        end
    ],
    {noreply, NewState, Actions};

handle_cast_tx(_TxCtx, {unregister, LogicalName}, State = #{names := Names}) ->
    NewState = State#{names => maps:remove(LogicalName, Names)},
    Actions = [
        fun(#{members := Members}) ->
            broadcast_to_members(Members, {name_unregistered, LogicalName})
        end
    ],
    {noreply, NewState, Actions}.

%% ---------------------------------------------------------------------------
%% handle_call/3  (read-only, no tx context needed)
%%
%% Used directly for priority_calls that only read state.  Also serves as the
%% fall-through target from handle_call_tx/4.
%% ---------------------------------------------------------------------------

-spec handle_call(term(), dgen_server:from(), registry_state()) ->
    dgen_server:reply_ret().

handle_call(get_leader, _From, State) ->
    {reply, maps:get(leader, State, undefined), State};

handle_call(get_members, _From, State) ->
    {reply, maps:keys(maps:get(members, State, #{})), State};

handle_call(get_names, _From, State) ->
    %% Returns #{LogicalName => Pid} — used by members to seed their ETS
    %% table on startup.
    {reply, maps:get(names, State, #{}), State}.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

-spec elect_leader(dgen_server:tx_ctx(), registry_state()) -> registry_state().
elect_leader(#{td := Td, tuid := Tuid}, State = #{members := Members}) ->
    Leader =
        case maps:keys(Members) of
            [] -> undefined;
            Ids -> lists:min(Ids)
        end,
    write_leader_key(Td, Tuid, Leader),
    State#{leader => Leader}.

-spec write_leader_key(dgen_backend:tenant(), dgen_server:tuid(), member_id() | undefined) -> ok.
write_leader_key({Tx, Dir}, Tuid, Leader) ->
    B = dgen_config:backend(),
    B:set(Tx, B:dir_pack(Dir, leader_key_tuple(Tuid)), term_to_binary(Leader)).

%% Returns the packed FDB key for the leader value.
%% Exported so callers can set up a backend watch without going through the
%% elector process.
-spec leader_fdb_key(dgen_backend:dir(), dgen_server:tuid()) -> dgen_backend:key().
leader_fdb_key(Dir, Tuid) ->
    B = dgen_config:backend(),
    B:dir_pack(Dir, leader_key_tuple(Tuid)).

leader_key_tuple(Tuid) ->
    dgen_key:extend(Tuid, <<"leader">>).

%% Notify helpers — run post-commit as dgen_server actions.

notify_join(NewMemberId, AllMemberIds, ExistingIds) ->
    cast_to_member(NewMemberId, {members, AllMemberIds}),
    lists:foreach(
        fun(Id) -> cast_to_member(Id, {new_member, NewMemberId}) end,
        ExistingIds
    ).

broadcast_to_members(Members, Msg) ->
    lists:foreach(fun(Id) -> cast_to_member(Id, Msg) end, maps:keys(Members)).

cast_to_member({Node, Name}, Msg) ->
    gen_server:cast({Name, Node}, Msg).
