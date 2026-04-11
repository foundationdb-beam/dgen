-module(dgen_registry_member).
-behaviour(gen_server).

%% ---------------------------------------------------------------------------
%% Overview
%%
%% Each node that participates in a named registry runs one member process.
%% The member has two roles depending on whether it is the current leader.
%%
%% Follower role
%% -------------
%% Maintains a peer-member monitor map so it can detect when the leader or
%% other members go down.  Keeps the local ETS name cache up to date by
%% receiving `{name_registered, …}`, `{name_unregistered, …}`, and
%% `{names_snapshot, …}` casts from the leader.  Forwards `{register, …}`
%% calls and `{unregister, …}` casts to the current leader.
%%
%% Leader role
%% -----------
%% Assumed when the elector broadcasts `{leader_changed, Self}`.  On becoming
%% leader, reads the authoritative name table from FDB via a range scan,
%% populates the local `names` map, sets up `erlang:monitor/2` for every
%% registered Pid, updates local ETS, and broadcasts a `{names_snapshot, …}`
%% to all peer members so their ETS caches are consistent.
%%
%% The leader is the SOLE writer for the name table.  It:
%%   - Handles `{register, LogicalName, Pid}` gen_server calls directly,
%%     writing to FDB and broadcasting `{name_registered, …}` to peers.
%%   - Handles `{unregister, LogicalName}` casts directly, deleting from FDB
%%     and broadcasting `{name_unregistered, …}` to peers.
%%   - Monitors every registered Pid.  When one dies, it removes the entry
%%     from FDB and ETS and broadcasts `{name_unregistered, …}`.
%%
%% Leadership relinquishment
%% -------------------------
%% On `{leader_changed, Other}` when currently leader, demonitors all
%% registered Pids and clears the `names` map.  ETS is NOT cleared — it
%% remains the local read cache for `whereis_name/1`.
%%
%% FDB key layout
%% --------------
%%   Names prefix : {Tuid, <<"names">>}
%%   Per-name key : {Tuid, <<"names">>, term_to_binary(LogicalName)}
%%   Value        : term_to_binary(Pid)
%%
%%   Leader key (written by elector, not member):
%%                  {Tuid, <<"leader">>}
%% ---------------------------------------------------------------------------

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    member_id :: dgen_registry_elector:member_id(),
    elector :: atom(),
    ets_table :: atom(),
    tenant :: dgen_backend:tenant(),
    tuid :: dgen_server:tuid(),
    leader :: dgen_registry_elector:member_id() | undefined,
    %% Authoritative name→pid map (leader only; #{} on followers)
    names :: #{term() => pid()},
    %% Peer-member monitors
    members :: #{dgen_registry_elector:member_id() => reference()},
    monitors :: #{reference() => dgen_registry_elector:member_id()},
    %% Registered-process monitors (leader only)
    name_to_ref :: #{term() => reference()},
    ref_to_name :: #{reference() => term()}
}).

%% ---------------------------------------------------------------------------
%% Public API
%% ---------------------------------------------------------------------------

-spec start_link(Name :: atom(), Args :: map()) -> gen_server:start_ret().
start_link(Name, Args) ->
    gen_server:start_link({local, Name}, ?MODULE, Args, []).

%% ---------------------------------------------------------------------------
%% gen_server callbacks
%% ---------------------------------------------------------------------------

init(#{name := RegistryName, elector := Elector, member_name := MemberName, tenant := Tenant}) ->
    MemberId = {node(), MemberName},
    Table = dgen_registry:ets_table_name(RegistryName),
    Tuid = {<<"dgen_registry">>, atom_to_binary(RegistryName)},
    ets:new(Table, [set, named_table, protected, {read_concurrency, true}]),
    %% Announce presence; the elector's post-commit action will send
    %% {members, AllIds} and {leader_changed, Leader} back to us.
    dgen_server:cast(Elector, {join, MemberId}),
    {ok, #state{
        member_id = MemberId,
        elector = Elector,
        ets_table = Table,
        tenant = Tenant,
        tuid = Tuid,
        leader = undefined,
        names = #{},
        members = #{},
        monitors = #{},
        name_to_ref = #{},
        ref_to_name = #{}
    }}.

%% ---------------------------------------------------------------------------
%% handle_call/3
%% ---------------------------------------------------------------------------

%% Leader: handle registration directly.
handle_call({register, LogicalName, Pid}, _From,
            State = #state{leader = Leader, member_id = Leader}) ->
    #state{names = Names, ets_table = Table, tenant = Tenant, tuid = Tuid,
           members = Members, name_to_ref = NTR, ref_to_name = RTN} = State,
    case maps:is_key(LogicalName, Names) of
        true ->
            {reply, no, State};
        false ->
            ok = write_name(Tenant, Tuid, LogicalName, Pid),
            ets:insert(Table, {LogicalName, Pid}),
            Ref = erlang:monitor(process, Pid),
            broadcast_to_peers(Members, {name_registered, LogicalName, Pid}),
            {reply, yes, State#state{
                names = Names#{LogicalName => Pid},
                name_to_ref = NTR#{LogicalName => Ref},
                ref_to_name = RTN#{Ref => LogicalName}
            }}
    end;

%% Follower: forward to leader.
handle_call({register, LogicalName, Pid}, _From,
            State = #state{leader = Leader}) when Leader =/= undefined ->
    {Node, Name} = Leader,
    Reply = gen_server:call({Name, Node}, {register, LogicalName, Pid}),
    {reply, Reply, State};

%% No leader yet.
handle_call({register, _LogicalName, _Pid}, _From, State) ->
    {reply, no, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

%% ---------------------------------------------------------------------------
%% handle_cast/2
%% ---------------------------------------------------------------------------

%% ---- Peer membership (from elector actions) --------------------------------

handle_cast({members, MemberIds}, State) ->
    {noreply, add_member_monitors(MemberIds, State)};

%% New member joined; as leader, send them the current names snapshot.
handle_cast({new_member, NewMemberId}, State = #state{
    member_id = Self, leader = Self, names = Names
}) ->
    cast_to_member(NewMemberId, {names_snapshot, maps:to_list(Names)}),
    {noreply, add_member_monitors([NewMemberId], State)};

handle_cast({new_member, NewMemberId}, State) ->
    {noreply, add_member_monitors([NewMemberId], State)};

%% ---- Leadership changes (from elector actions) ----------------------------

handle_cast({leader_changed, NewLeader}, State = #state{
    member_id = Self, leader = OldLeader
}) ->
    State1 =
        if
            OldLeader =:= Self, NewLeader =/= Self ->
                %% Lost leadership — stop monitoring registered Pids.
                relinquish_leadership(State#state{leader = NewLeader});
            OldLeader =/= Self, NewLeader =:= Self ->
                %% Gained leadership — load names from FDB and seed peers.
                assume_leadership(State#state{leader = NewLeader});
            true ->
                %% No role change (leader unchanged, or unrelated update).
                State#state{leader = NewLeader}
        end,
    {noreply, State1};

%% ---- Registered-name updates (follower receives from leader) ---------------

handle_cast({name_registered, LogicalName, Pid}, State = #state{ets_table = Table}) ->
    ets:insert(Table, {LogicalName, Pid}),
    {noreply, State};

handle_cast({name_unregistered, LogicalName}, State = #state{ets_table = Table}) ->
    ets:delete(Table, LogicalName),
    {noreply, State};

handle_cast({names_snapshot, NamesList}, State = #state{ets_table = Table}) ->
    lists:foreach(fun({LogicalName, Pid}) ->
        ets:insert(Table, {LogicalName, Pid})
    end, NamesList),
    {noreply, State};

%% ---- Unregister -----------------------------------------------------------

%% Leader: handle directly.
handle_cast({unregister, LogicalName}, State = #state{
    leader = Leader, member_id = Leader
}) ->
    #state{tenant = Tenant, tuid = Tuid, ets_table = Table,
           members = Members, name_to_ref = NTR, ref_to_name = RTN} = State,
    delete_name(Tenant, Tuid, LogicalName),
    ets:delete(Table, LogicalName),
    broadcast_to_peers(Members, {name_unregistered, LogicalName}),
    {NTR1, RTN1} = demonitor_name(LogicalName, NTR, RTN),
    {noreply, State#state{
        names = maps:remove(LogicalName, State#state.names),
        name_to_ref = NTR1,
        ref_to_name = RTN1
    }};

%% Follower: forward to leader.
handle_cast({unregister, LogicalName}, State = #state{leader = Leader})
  when Leader =/= undefined ->
    cast_to_member(Leader, {unregister, LogicalName}),
    {noreply, State};

%% No leader — drop.
handle_cast({unregister, _LogicalName}, State) ->
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% ---------------------------------------------------------------------------
%% handle_info/2
%% ---------------------------------------------------------------------------

handle_info({'DOWN', Ref, process, _Pid, _Reason}, State) ->
    #state{
        monitors = Monitors,
        ref_to_name = RefToName,
        elector = Elector,
        member_id = Self,
        ets_table = Table,
        tenant = Tenant,
        tuid = Tuid,
        members = Members
    } = State,
    case maps:get(Ref, Monitors, undefined) of
        undefined ->
            %% Not a peer-member monitor — must be a registered-process monitor
            %% (only set up on the leader).
            case maps:get(Ref, RefToName, undefined) of
                undefined ->
                    {noreply, State};
                LogicalName ->
                    delete_name(Tenant, Tuid, LogicalName),
                    ets:delete(Table, LogicalName),
                    broadcast_to_peers(Members, {name_unregistered, LogicalName}),
                    NTR = maps:remove(LogicalName, State#state.name_to_ref),
                    RTN = maps:remove(Ref, RefToName),
                    {noreply, State#state{
                        names = maps:remove(LogicalName, State#state.names),
                        name_to_ref = NTR,
                        ref_to_name = RTN
                    }}
            end;
        Self ->
            %% Stale self-monitor — should not happen, ignore.
            {noreply, State};
        DeadMemberId ->
            dgen_server:cast(Elector, {member_down, DeadMemberId}),
            {noreply, remove_member(DeadMemberId, State)}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

%% Read the authoritative name table from FDB, populate ETS and name monitors,
%% then broadcast a full snapshot to all peer members so their ETS is fresh.
assume_leadership(State = #state{
    tenant = Tenant, tuid = Tuid, ets_table = Table, members = Members
}) ->
    B = dgen_config:backend(),
    Names = dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        {Start, End} = B:dir_range(Dir, dgen_key:extend(Tuid, <<"names">>)),
        Pairs = B:get_range(Tx, Start, End, []),
        lists:foldl(
            fun({K, V}, Acc) ->
                Unpacked = B:dir_unpack(Dir, K),
                LogicalName = binary_to_term(element(tuple_size(Unpacked), Unpacked)),
                Pid = binary_to_term(V),
                Acc#{LogicalName => Pid}
            end,
            #{},
            Pairs
        )
    end),
    %% Update local ETS (may have stale/missing entries from a crash window).
    maps:foreach(fun(LogicalName, Pid) ->
        ets:insert(Table, {LogicalName, Pid})
    end, Names),
    %% Set up process monitors for every registered Pid.
    {NTR, RTN} = maps:fold(
        fun(LogicalName, Pid, {NTRAcc, RTNAcc}) ->
            Ref = erlang:monitor(process, Pid),
            {NTRAcc#{LogicalName => Ref}, RTNAcc#{Ref => LogicalName}}
        end,
        {#{}, #{}},
        Names
    ),
    %% Push a full snapshot to all peer members to close any ETS lag.
    NamesList = maps:to_list(Names),
    maps:foreach(fun(MemberId, _) ->
        cast_to_member(MemberId, {names_snapshot, NamesList})
    end, Members),
    State#state{names = Names, name_to_ref = NTR, ref_to_name = RTN}.

%% Demonitor all registered Pids and clear leader-only state.
%% ETS is intentionally left intact — it remains the local read cache.
relinquish_leadership(State = #state{name_to_ref = NTR}) ->
    maps:foreach(fun(_LogicalName, Ref) ->
        erlang:demonitor(Ref, [flush])
    end, NTR),
    State#state{names = #{}, name_to_ref = #{}, ref_to_name = #{}}.

write_name(Tenant, Tuid, LogicalName, Pid) ->
    B = dgen_config:backend(),
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        Key = B:dir_pack(Dir, dgen_key:extend(Tuid, <<"names">>, term_to_binary(LogicalName))),
        B:set(Tx, Key, term_to_binary(Pid))
    end).

delete_name(Tenant, Tuid, LogicalName) ->
    B = dgen_config:backend(),
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        Key = B:dir_pack(Dir, dgen_key:extend(Tuid, <<"names">>, term_to_binary(LogicalName))),
        B:clear_range(Tx, Key, B:key_strinc(Key))
    end).

add_member_monitors(MemberIds, State) ->
    #state{member_id = Self, members = Members, monitors = Monitors} = State,
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
    State#state{members = NewMembers, monitors = NewMonitors}.

remove_member(MemberId, State = #state{members = Members, monitors = Monitors}) ->
    case maps:get(MemberId, Members, undefined) of
        undefined ->
            State;
        Ref ->
            erlang:demonitor(Ref, [flush]),
            State#state{
                members = maps:remove(MemberId, Members),
                monitors = maps:remove(Ref, Monitors)
            }
    end.

demonitor_name(LogicalName, NTR, RTN) ->
    case maps:get(LogicalName, NTR, undefined) of
        undefined ->
            {NTR, RTN};
        OldRef ->
            erlang:demonitor(OldRef, [flush]),
            {maps:remove(LogicalName, NTR), maps:remove(OldRef, RTN)}
    end.

broadcast_to_peers(Members, Msg) ->
    maps:foreach(fun(MemberId, _) -> cast_to_member(MemberId, Msg) end, Members).

cast_to_member({Node, Name}, Msg) ->
    gen_server:cast({Name, Node}, Msg).
