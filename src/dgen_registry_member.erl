-module(dgen_registry_member).
-behaviour(gen_server).

%% ---------------------------------------------------------------------------
%% Overview
%%
%% Each node that participates in a named registry runs one member process.
%% The member has two distinct responsibilities:
%%
%% 1. Distributed membership tracking
%%    Announces this node's presence to the elector on startup, monitors peer
%%    member processes (across nodes via erlang:monitor), and forwards DOWN
%%    notifications to the elector so it can recompute the leader and update
%%    the FDB leader key atomically.
%%
%% 2. Local process registry cache
%%    Owns an ETS table (`dgen_registry_names_{RegistryName}`) that maps
%%    LogicalName → Pid.  `dgen_registry:whereis_name/1` reads from this
%%    table without any FDB round-trip.
%%
%%    On startup the table is seeded from the elector's current `names` map
%%    via a priority_call.  Thereafter the elector's post-commit actions
%%    broadcast `{name_registered, …}` and `{name_unregistered, …}` casts
%%    to keep all members' tables in sync.
%%
%%    Each registered Pid is monitored.  When the monitored process exits,
%%    the entry is removed from ETS and `{unregister, LogicalName}` is cast
%%    to the elector so all other members are notified.
%%
%% DOWN disambiguation
%% -------------------
%% A single DOWN message can come from either a peer-member monitor or a
%% registered-process monitor.  The two monitor reference spaces are kept
%% separate (`monitors` vs `ref_to_name`) so each DOWN can be dispatched
%% in O(1) without any scanning.
%% ---------------------------------------------------------------------------

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    member_id :: dgen_registry_elector:member_id(),
    elector :: atom(),
    ets_table :: atom(),
    %% Peer-member monitors
    members :: #{dgen_registry_elector:member_id() => reference()},
    monitors :: #{reference() => dgen_registry_elector:member_id()},
    %% Registered-process monitors
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

init(#{name := RegistryName, elector := Elector, member_name := MemberName}) ->
    MemberId = {node(), MemberName},
    Table = dgen_registry:ets_table_name(RegistryName),
    ets:new(Table, [set, named_table, protected, {read_concurrency, true}]),
    %% Seed the ETS table with any names already registered in FDB.
    %% priority_call bypasses the durable queue so this returns quickly even
    %% if there are pending membership changes being processed.
    ExistingNames =
        try
            dgen_server:priority_call(Elector, get_names, 5000)
        catch
            exit:{timeout, _} -> #{};
            exit:{noproc, _} -> #{}
        end,
    {NameToRef, RefToName} = seed_name_monitors(ExistingNames, Table),
    %% Announce presence after seeding so the join action's member list
    %% broadcast doesn't race with our ETS population.
    dgen_server:cast(Elector, {join, MemberId}),
    {ok, #state{
        member_id = MemberId,
        elector = Elector,
        ets_table = Table,
        members = #{},
        monitors = #{},
        name_to_ref = NameToRef,
        ref_to_name = RefToName
    }}.

handle_call(get_members, _From, State = #state{members = Members}) ->
    {reply, maps:keys(Members), State};

handle_call(get_leader, _From, State = #state{elector = Elector}) ->
    {reply, dgen_server:priority_call(Elector, get_leader), State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

%% ---- Peer membership messages (from elector actions) --------------------

handle_cast({members, MemberIds}, State) ->
    {noreply, add_member_monitors(MemberIds, State)};

handle_cast({new_member, MemberId}, State) ->
    {noreply, add_member_monitors([MemberId], State)};

handle_cast({member_down, MemberId}, State) ->
    {noreply, remove_member(MemberId, State)};

%% ---- Registered-name messages (from elector actions) -------------------

handle_cast({name_registered, LogicalName, Pid}, State) ->
    #state{ets_table = Table, name_to_ref = NTR, ref_to_name = RTN} = State,
    %% Idempotent: if we already have a monitor for this name (e.g. from a
    %% concurrent registration race), demonitor the old one first.
    {NTR1, RTN1} = demonitor_name(LogicalName, NTR, RTN),
    ets:insert(Table, {LogicalName, Pid}),
    Ref = erlang:monitor(process, Pid),
    {noreply, State#state{
        name_to_ref = NTR1#{LogicalName => Ref},
        ref_to_name = RTN1#{Ref => LogicalName}
    }};

handle_cast({name_unregistered, LogicalName}, State) ->
    #state{ets_table = Table, name_to_ref = NTR, ref_to_name = RTN} = State,
    ets:delete(Table, LogicalName),
    {NTR1, RTN1} = demonitor_name(LogicalName, NTR, RTN),
    {noreply, State#state{name_to_ref = NTR1, ref_to_name = RTN1}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% ---- Process DOWN messages -----------------------------------------------

handle_info({'DOWN', Ref, process, _Pid, _Reason}, State) ->
    #state{
        monitors = Monitors,
        ref_to_name = RefToName,
        elector = Elector,
        member_id = Self,
        ets_table = Table
    } = State,
    case maps:get(Ref, Monitors, undefined) of
        undefined ->
            %% Not a peer-member monitor — check registered-process monitors.
            case maps:get(Ref, RefToName, undefined) of
                undefined ->
                    {noreply, State};
                LogicalName ->
                    ets:delete(Table, LogicalName),
                    dgen_server:cast(Elector, {unregister, LogicalName}),
                    NTR = maps:remove(LogicalName, State#state.name_to_ref),
                    RTN = maps:remove(Ref, RefToName),
                    {noreply, State#state{name_to_ref = NTR, ref_to_name = RTN}}
            end;
        Self ->
            %% Should not monitor ourselves, but handle gracefully.
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

%% Seed the ETS table and set up process monitors for names that were already
%% registered in FDB before this member started.
seed_name_monitors(Names, Table) ->
    maps:fold(
        fun(LogicalName, Pid, {NTR, RTN}) ->
            ets:insert(Table, {LogicalName, Pid}),
            Ref = erlang:monitor(process, Pid),
            {NTR#{LogicalName => Ref}, RTN#{Ref => LogicalName}}
        end,
        {#{}, #{}},
        Names
    ).

%% Add erlang:monitor/2 for a list of peer member process names.
%% Skips self and already-monitored peers (idempotent).
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

%% Demonitor and remove map entries for a registered LogicalName.
%% Returns updated {NameToRef, RefToName}.
demonitor_name(LogicalName, NTR, RTN) ->
    case maps:get(LogicalName, NTR, undefined) of
        undefined ->
            {NTR, RTN};
        OldRef ->
            erlang:demonitor(OldRef, [flush]),
            {maps:remove(LogicalName, NTR), maps:remove(OldRef, RTN)}
    end.
