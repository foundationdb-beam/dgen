-module(dgen_registry_elector).
-behaviour(dgen_server).

%% ---------------------------------------------------------------------------
%% Overview
%%
%% The elector is a dgen_server — its state (member map + current leader) is
%% stored durably in FDB and is shared across every node that runs the same
%% named registry.  Messages arrive via a durable FIFO queue, so even if the
%% elector process crashes mid-way through a membership change, the pending
%% cast will be replayed once the process restarts.
%%
%% Because dgen_server serialises queue consumption through FDB transactions,
%% the leader election is consistent: only one node's elector process can
%% commit a state update at a time, and the leader key is written atomically
%% with the membership state in the same transaction.
%%
%% Leadership algorithm
%% --------------------
%% The current leader is simply `lists:min/1` over the live member id set.
%% Member ids are `{node(), atom()}` so the comparison is lexicographic over
%% the node name first, then the process name atom.  Any deterministic total
%% order would work; this one requires no additional coordination.
%%
%% Leader key in FDB
%% -----------------
%% In addition to the normal dgen_server state encoding, the elector writes a
%% standalone FDB key for the leader whenever membership changes.  External
%% processes can watch this key directly via the backend to receive low-latency
%% leader-change notifications without polling the elector process.
%%
%%   Key path: {<<"dgen_registry">>, RegistryNameBin, <<"leader">>}
%%   Value:    term_to_binary(MemberId | undefined)
%% ---------------------------------------------------------------------------

-export([
    init/1,
    handle_cast_tx/3,
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
    %% Use a registry-specific Tuid so multiple named registries on the same
    %% tenant are fully isolated from each other.
    Tuid = {<<"dgen_registry">>, atom_to_binary(Name)},
    State = #{members => #{}, leader => undefined},
    {ok, Tuid, State}.

%% handle_cast_tx/3 — preferred over handle_cast/2 by dgen_server when both
%% are present.  Receives the live FDB transaction context so we can write
%% the leader key atomically alongside the state update.

-spec handle_cast_tx(dgen_server:tx_ctx(), term(), registry_state()) ->
    dgen_server:noreply_ret().

handle_cast_tx(TxCtx, {join, MemberId}, State = #{members := Members}) ->
    MemberInfo = #{joined_at => erlang:system_time(millisecond)},
    NewMembers = Members#{MemberId => MemberInfo},
    NewState0 = State#{members => NewMembers},
    NewState = elect_leader(TxCtx, NewState0),
    %% Post-commit actions:
    %%   1. Send the complete member list to the new member's gen_server.
    %%   2. Notify every pre-existing member about the newcomer.
    Actions = [
        fun(#{members := All}) ->
            ExistingIds = maps:keys(maps:remove(MemberId, All)),
            notify_join(MemberId, maps:keys(All), ExistingIds)
        end
    ],
    {noreply, NewState, Actions};

handle_cast_tx(TxCtx, {member_down, MemberId}, State = #{members := Members}) ->
    NewMembers = maps:remove(MemberId, Members),
    NewState0 = State#{members => NewMembers},
    NewState = elect_leader(TxCtx, NewState0),
    %% Notify remaining members that the peer has left so they can clean up
    %% their local monitor tables faster (they will also get the DOWN
    %% themselves via erlang:monitor, but a cast from the elector lets them
    %% remove the member from their logical member map immediately).
    Actions = [
        fun(#{members := Remaining}) ->
            lists:foreach(
                fun({Node, Name}) ->
                    gen_server:cast({Name, Node}, {member_down, MemberId})
                end,
                maps:keys(Remaining)
            )
        end
    ],
    {noreply, NewState, Actions}.

%% handle_call/3 — used for priority_call reads (bypasses the durable queue).
%% These are read-only so they do not need the tx context.

-spec handle_call(term(), dgen_server:from(), registry_state()) ->
    dgen_server:reply_ret().

handle_call(get_leader, _From, State) ->
    {reply, maps:get(leader, State, undefined), State};

handle_call(get_members, _From, State) ->
    {reply, maps:keys(maps:get(members, State, #{})), State}.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

%% Compute the new leader from the current member set and write it to both
%% the in-memory state and the dedicated FDB leader key.
-spec elect_leader(dgen_server:tx_ctx(), registry_state()) -> registry_state().
elect_leader(#{td := Td, tuid := Tuid}, State = #{members := Members}) ->
    Leader =
        case maps:keys(Members) of
            [] -> undefined;
            Ids -> lists:min(Ids)
        end,
    write_leader_key(Td, Tuid, Leader),
    State#{leader => Leader}.

%% Write the leader key inside the current FDB transaction.
%% The key lives outside the normal dgen_server state subspace so that
%% external watchers can observe it without going through the elector process.
-spec write_leader_key(dgen_backend:tenant(), dgen_server:tuid(), member_id() | undefined) -> ok.
write_leader_key({Tx, Dir}, Tuid, Leader) ->
    B = dgen_config:backend(),
    Key = B:dir_pack(Dir, leader_key_tuple(Tuid)),
    B:set(Tx, Key, term_to_binary(Leader)).

%% Returns the packed FDB key for the leader value given a tenant dir + tuid.
%% Exported so callers can set up a watch without going through the elector.
-spec leader_fdb_key(dgen_backend:dir(), dgen_server:tuid()) -> dgen_backend:key().
leader_fdb_key(Dir, Tuid) ->
    B = dgen_config:backend(),
    B:dir_pack(Dir, leader_key_tuple(Tuid)).

leader_key_tuple(Tuid) ->
    dgen_key:extend(Tuid, <<"leader">>).

%% Post-commit notification helpers — these run outside the FDB transaction.

notify_join(NewMemberId, AllMemberIds, ExistingIds) ->
    %% Tell the new member the full current roster.
    cast_to_member(NewMemberId, {members, AllMemberIds}),
    %% Tell every pre-existing member there is a new arrival.
    lists:foreach(
        fun(ExistingId) -> cast_to_member(ExistingId, {new_member, NewMemberId}) end,
        ExistingIds
    ).

cast_to_member({Node, Name}, Msg) ->
    gen_server:cast({Name, Node}, Msg).
