-module(dgen_registry_elector).
-behaviour(dgen_server).

%% ---------------------------------------------------------------------------
%% Overview
%%
%% The elector is a dgen_server — its state (member map + current leader) is
%% stored durably in FDB and is shared across every node that runs the same
%% named registry.
%%
%% Name registration is NO LONGER handled here.  The elected leader member
%% process is the single writer for the name table; it stores names directly
%% in FDB under a well-known key prefix and keeps the other members' ETS
%% tables in sync via gen_server casts.
%%
%% The elector's only jobs are:
%%   1. Maintain the live member set.
%%   2. Elect a leader (lists:min/1 over member IDs — deterministic).
%%   3. Write the leader key to FDB atomically with state changes.
%%   4. Notify all members of the current leader after each membership change.
%%
%% Leadership algorithm
%% --------------------
%% Current leader = lists:min/1 over live member IDs.
%% Member IDs are {node(), atom()} so the ordering is lexicographic.
%%
%% Leader key in FDB
%% -----------------
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
    leader := member_id() | undefined
}.

%% ---------------------------------------------------------------------------
%% dgen_server callbacks
%% ---------------------------------------------------------------------------

-spec init(#{name := atom()}) -> {ok, dgen_server:tuid(), registry_state()}.
init(#{name := Name}) ->
    Tuid = {<<"dgen_registry">>, atom_to_binary(Name)},
    State = #{members => #{}, leader => undefined},
    {ok, Tuid, State}.

%% Delegate read-only priority_calls to handle_call/3.
-spec handle_call_tx(dgen_server:tx_ctx(), term(), dgen_server:from(), registry_state()) ->
    dgen_server:reply_ret().
handle_call_tx(_TxCtx, Request, From, State) ->
    handle_call(Request, From, State).

-spec handle_cast_tx(dgen_server:tx_ctx(), term(), registry_state()) ->
    dgen_server:noreply_ret().

handle_cast_tx(TxCtx, {join, MemberId}, State = #{members := Members}) ->
    MemberInfo = #{joined_at => erlang:system_time(millisecond)},
    NewState = elect_leader(TxCtx, State#{members => Members#{MemberId => MemberInfo}}),
    Leader = maps:get(leader, NewState),
    Actions = [
        fun(#{members := All}) ->
            ExistingIds = maps:keys(maps:remove(MemberId, All)),
            notify_join(MemberId, maps:keys(All), ExistingIds, Leader)
        end
    ],
    {noreply, NewState, Actions};

handle_cast_tx(TxCtx, {member_down, MemberId}, State = #{members := Members}) ->
    NewState = elect_leader(TxCtx, State#{members => maps:remove(MemberId, Members)}),
    Leader = maps:get(leader, NewState),
    Actions = [
        fun(#{members := Remaining}) ->
            broadcast_to_members(Remaining, {leader_changed, Leader})
        end
    ],
    {noreply, NewState, Actions}.

%% ---------------------------------------------------------------------------
%% handle_call/3  (read-only priority_calls)
%% ---------------------------------------------------------------------------

-spec handle_call(term(), dgen_server:from(), registry_state()) ->
    dgen_server:reply_ret().

handle_call(get_leader, _From, State) ->
    {reply, maps:get(leader, State, undefined), State};

handle_call(get_members, _From, State) ->
    {reply, maps:keys(maps:get(members, State, #{})), State}.

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

%% Tell the new member who all peers are and who the leader is.
%% Tell each existing member about the new peer and the (possibly new) leader.
notify_join(NewMemberId, AllMemberIds, ExistingIds, Leader) ->
    cast_to_member(NewMemberId, {members, AllMemberIds}),
    cast_to_member(NewMemberId, {leader_changed, Leader}),
    lists:foreach(
        fun(Id) ->
            cast_to_member(Id, {new_member, NewMemberId}),
            cast_to_member(Id, {leader_changed, Leader})
        end,
        ExistingIds
    ).

broadcast_to_members(Members, Msg) ->
    lists:foreach(fun(Id) -> cast_to_member(Id, Msg) end, maps:keys(Members)).

cast_to_member({Node, Name}, Msg) ->
    gen_server:cast({Name, Node}, Msg).
