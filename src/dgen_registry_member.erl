-module(dgen_registry_member).
-behaviour(gen_server).

%% ---------------------------------------------------------------------------
%% Overview
%%
%% Each node participating in a named registry runs one member process.
%% The member has two roles depending on whether it is the current leader.
%%
%% Storage
%% -------
%% Both leader and follower keep a `names :: #{LogicalName => pid()}` map in
%% their gen_server state.  There is no ETS table and no durable storage for
%% name→pid mappings.  Pids are node-local and process-lifetime-scoped; they
%% have no meaning after a restart and must never be written to FDB.
%%
%% Consistent reads and writes go through the leader.
%% Snapshot reads (`whereis_snapshot`) are served from the local member's map
%% without contacting the leader.
%%
%% Follower role
%% -------------
%% Receives `{leader_changed, Leader}` from the elector when leadership is
%% established or changes.  Keeps its local `names` map in sync by receiving
%% `{name_registered, …}`, `{name_unregistered, …}`, and `{names_snapshot, …}`
%% casts from the leader (one-way replication).
%%
%% Forwards `{register, …}` calls and `{unregister, …}` casts to the leader.
%% For both, the follower also updates its own `names` map immediately so that
%% a subsequent `whereis_snapshot` on this node reflects the change without
%% waiting for the replication cast to arrive over the network.
%%
%% Leader role
%% -----------
%% Assumed when the elector broadcasts `{leader_changed, Self}`.  On assuming
%% leadership the member uses its current in-memory `names` map (which already
%% holds the replicated snapshot from when it was a follower), sets up
%% `erlang:monitor/2` for every entry, and broadcasts a `{names_snapshot, …}`
%% to all followers so their maps are consistent.  Any stale entries (Pids
%% that died while this node was a follower) are removed when their DOWN
%% signals arrive.
%%
%% The leader is the SOLE writer for the name table.  It:
%%   - Handles `{register, LogicalName, Pid}` calls: checks the in-memory map,
%%     updates it, monitors the Pid, and replicates `{name_registered, …}`.
%%   - Handles `{whereis, LogicalName}` calls: consistent read from local map.
%%   - Handles `{unregister, LogicalName}` casts: updates the map, demonitors,
%%     and replicates `{name_unregistered, …}`.
%%   - Monitors every registered Pid.  When one dies, removes from the map
%%     and replicates `{name_unregistered, …}` to followers.
%%
%% On `{leader_changed, Other}` when currently leader, the member relinquishes
%% leadership: demonitors all registered Pids and clears the leader-only
%% state.  The `names` map is kept intact (it still serves snapshot reads).
%% ---------------------------------------------------------------------------

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    member_id :: dgen_registry_elector:member_id(),
    elector :: atom(),
    leader :: dgen_registry_elector:member_id() | undefined,
    %% Name → Pid map.  Authoritative on leader; replicated snapshot on followers.
    %% Never written to durable storage — Pids are ephemeral.
    names :: #{term() => pid()},
    %% Peer-member monitors (all members)
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

init(#{elector := Elector, member_name := MemberName}) ->
    MemberId = {node(), MemberName},
    %% Announce presence; the elector's post-commit action (or handle_locked)
    %% will reply with {members, AllIds} and {leader_changed, Leader}.
    dgen_server:cast(Elector, {join, MemberId}),
    {ok, #state{
        member_id = MemberId,
        elector = Elector,
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

%% ---- Name registration ----------------------------------------------------

%% Leader: handle registration directly.
handle_call({register, LogicalName, Pid}, _From,
            State = #state{leader = Leader, member_id = Leader}) ->
    #state{names = Names, members = Members, name_to_ref = NTR, ref_to_name = RTN} = State,
    case maps:is_key(LogicalName, Names) of
        true ->
            {reply, no, State};
        false ->
            Ref = erlang:monitor(process, Pid),
            broadcast_to_peers(Members, {name_registered, LogicalName, Pid}),
            {reply, yes, State#state{
                names = Names#{LogicalName => Pid},
                name_to_ref = NTR#{LogicalName => Ref},
                ref_to_name = RTN#{Ref => LogicalName}
            }}
    end;

%% Follower: forward to leader, then update local names map optimistically.
%% The optimistic update ensures that a subsequent whereis_snapshot on this
%% node returns the Pid immediately, without waiting for the replication cast
%% from the leader (which may lose the race with the reply over the network).
handle_call({register, LogicalName, Pid}, _From,
            State = #state{leader = Leader}) when Leader =/= undefined ->
    {Node, Name} = Leader,
    try gen_server:call({Name, Node}, {register, LogicalName, Pid}) of
        yes ->
            {reply, yes, State#state{names = (State#state.names)#{LogicalName => Pid}}};
        no ->
            {reply, no, State}
    catch
        exit:_ ->
            {reply, no, State}
    end;

%% No leader yet.
handle_call({register, _LogicalName, _Pid}, _From, State) ->
    {reply, no, State};

%% ---- Consistent read (leader only) ----------------------------------------

handle_call({whereis, LogicalName}, _From,
            State = #state{leader = Leader, member_id = Leader}) ->
    {reply, maps:get(LogicalName, State#state.names, undefined), State};

handle_call({whereis, LogicalName}, _From,
            State = #state{leader = Leader}) when Leader =/= undefined ->
    {Node, Name} = Leader,
    try
        Reply = gen_server:call({Name, Node}, {whereis, LogicalName}),
        {reply, Reply, State}
    catch
        exit:_ ->
            {reply, undefined, State}
    end;

handle_call({whereis, _LogicalName}, _From, State) ->
    {reply, undefined, State};

%% ---- Snapshot read (any member, served from local names map) ---------------

handle_call({whereis_snapshot, LogicalName}, _From, State) ->
    {reply, maps:get(LogicalName, State#state.names, undefined), State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

%% ---------------------------------------------------------------------------
%% handle_cast/2
%% ---------------------------------------------------------------------------

%% ---- Peer membership (from elector) ----------------------------------------

handle_cast({members, MemberIds}, State) ->
    {noreply, add_member_monitors(MemberIds, State)};

%% New member joined; as leader, send them the current names snapshot.
handle_cast({new_member, NewMemberId}, State = #state{member_id = Self, leader = Self, names = Names}) ->
    cast_to_member(NewMemberId, {names_snapshot, maps:to_list(Names)}),
    {noreply, add_member_monitors([NewMemberId], State)};

handle_cast({new_member, NewMemberId}, State) ->
    {noreply, add_member_monitors([NewMemberId], State)};

%% ---- Leadership transitions ------------------------------------------------

handle_cast({leader_changed, NewLeader}, State = #state{member_id = Self, leader = OldLeader}) ->
    State1 =
        if
            OldLeader =:= Self, NewLeader =/= Self ->
                %% Lost leadership — demonitor registered Pids, keep names for snapshot reads.
                relinquish_leadership(State#state{leader = NewLeader});
            OldLeader =/= Self, NewLeader =:= Self ->
                %% Gained leadership — read authoritative snapshot from FDB, seed followers.
                assume_leadership(State#state{leader = NewLeader});
            true ->
                %% No role change.
                State#state{leader = NewLeader}
        end,
    {noreply, State1};

%% ---- One-way replication from leader to followers -------------------------

handle_cast({name_registered, LogicalName, Pid}, State = #state{names = Names}) ->
    {noreply, State#state{names = Names#{LogicalName => Pid}}};

handle_cast({name_unregistered, LogicalName}, State = #state{names = Names}) ->
    {noreply, State#state{names = maps:remove(LogicalName, Names)}};

%% Replace the local names map with the leader's current snapshot.
handle_cast({names_snapshot, NamesList}, State) ->
    {noreply, State#state{names = maps:from_list(NamesList)}};

%% ---- Unregister -----------------------------------------------------------

%% Leader: handle directly.
handle_cast({unregister, LogicalName}, State = #state{leader = Leader, member_id = Leader}) ->
    #state{names = Names, members = Members, name_to_ref = NTR, ref_to_name = RTN} = State,
    {NTR1, RTN1} = demonitor_name(LogicalName, NTR, RTN),
    broadcast_to_peers(Members, {name_unregistered, LogicalName}),
    {noreply, State#state{
        names = maps:remove(LogicalName, Names),
        name_to_ref = NTR1,
        ref_to_name = RTN1
    }};

%% Follower: update local map immediately, then forward to leader.
%% Immediate local update ensures whereis_snapshot returns undefined before
%% the replication cast arrives from the leader.
handle_cast({unregister, LogicalName}, State = #state{leader = Leader, names = Names})
  when Leader =/= undefined ->
    cast_to_member(Leader, {unregister, LogicalName}),
    {noreply, State#state{names = maps:remove(LogicalName, Names)}};

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
        members = Members
    } = State,
    case maps:get(Ref, Monitors, undefined) of
        undefined ->
            %% Not a peer-member monitor — registered-process monitor (leader only).
            case maps:get(Ref, RefToName, undefined) of
                undefined ->
                    {noreply, State};
                LogicalName ->
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

%% Use the current in-memory names map as the starting snapshot, set up
%% process monitors for every entry, and broadcast the snapshot to all peers.
%% Any stale Pid entries (processes that died while this node was a follower)
%% will self-correct when their DOWN signals arrive.
assume_leadership(State = #state{names = Names, members = Members}) ->
    {NTR, RTN} = maps:fold(
        fun(LogicalName, Pid, {NTRAcc, RTNAcc}) ->
            Ref = erlang:monitor(process, Pid),
            {NTRAcc#{LogicalName => Ref}, RTNAcc#{Ref => LogicalName}}
        end,
        {#{}, #{}},
        Names
    ),
    NamesList = maps:to_list(Names),
    maps:foreach(fun(MemberId, _) ->
        cast_to_member(MemberId, {names_snapshot, NamesList})
    end, Members),
    State#state{name_to_ref = NTR, ref_to_name = RTN}.

%% Demonitor all registered Pids; keep the names map for snapshot reads.
relinquish_leadership(State = #state{name_to_ref = NTR}) ->
    maps:foreach(fun(_LogicalName, Ref) ->
        erlang:demonitor(Ref, [flush])
    end, NTR),
    State#state{name_to_ref = #{}, ref_to_name = #{}}.

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
