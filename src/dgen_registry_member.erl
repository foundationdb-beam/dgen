-module(dgen_registry_member).
-behaviour(gen_server).

%% ---------------------------------------------------------------------------
%% Overview
%%
%% Each node that participates in a named registry runs one member process.
%% The member's responsibilities are:
%%
%%   1. Announce this node's presence to the elector on startup (join).
%%   2. Maintain erlang:monitor/2 references for every known peer process.
%%   3. Forward DOWN notifications to the elector so it can recompute the
%%      leader and update the FDB leader key atomically.
%%
%% Member identity
%% ---------------
%% A member id is `{node(), MemberName}` where `MemberName` is the atom under
%% which this gen_server is locally registered (defaults to
%% `<RegistryName>_member`).  The remote member processes are similarly
%% registered under the same atom on their respective nodes, so inter-node
%% casts use the `{Name, Node}` form.
%%
%% Monitor lifecycle
%% -----------------
%% * `{members, [MemberId]}` — sent by the elector after a join; the full
%%   roster is provided so the new member can monitor all existing peers in
%%   one shot.
%% * `{new_member, MemberId}` — sent by the elector to all existing members
%%   when a new peer joins.
%% * `{'DOWN', Ref, process, _, _}` — fires when a monitored peer process
%%   exits.  The member reports this to the elector and cleans up locally.
%% * `{member_down, MemberId}` — sent by the elector to remaining members
%%   after processing a `member_down` cast, allowing faster local cleanup
%%   for members that don't monitor the dead peer directly (e.g. because the
%%   peer is on a partitioned node where the monitor fires as `noconnection`).
%% ---------------------------------------------------------------------------

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    member_id :: dgen_registry_elector:member_id(),
    elector :: atom(),
    %% MemberId => monitor_ref (one monitor per peer)
    members :: #{dgen_registry_elector:member_id() => reference()},
    %% Reverse map: monitor_ref => MemberId (for O(1) DOWN lookup)
    monitors :: #{reference() => dgen_registry_elector:member_id()}
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
    %% Announce ourselves.  dgen_server:cast enqueues to FDB; if the elector
    %% process is briefly unavailable the message survives in the durable queue.
    dgen_server:cast(Elector, {join, MemberId}),
    State = #state{
        member_id = MemberId,
        elector = Elector,
        members = #{},
        monitors = #{}
    },
    {ok, State}.

handle_call(get_members, _From, State = #state{members = Members}) ->
    {reply, maps:keys(Members), State};

handle_call(get_leader, _From, State = #state{elector = Elector}) ->
    %% Delegate to the elector — this is just a convenience pass-through.
    {reply, dgen_server:priority_call(Elector, get_leader), State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({members, MemberIds}, State) ->
    %% Full roster from the elector — sent once after our join is processed.
    {noreply, add_monitors(MemberIds, State)};

handle_cast({new_member, MemberId}, State) ->
    %% Incremental: a peer joined after us.
    {noreply, add_monitors([MemberId], State)};

handle_cast({member_down, MemberId}, State) ->
    %% The elector confirmed a peer is gone.  Clean up our local state even
    %% if we haven't received (or never receive) the erlang DOWN for it.
    {noreply, remove_member(MemberId, State)};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, _Pid, _Reason}, State) ->
    #state{monitors = Monitors, elector = Elector, member_id = Self} = State,
    case maps:get(Ref, Monitors, undefined) of
        undefined ->
            {noreply, State};
        Self ->
            %% Should not happen (we don't monitor ourselves), but handle it.
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

%% Establish process monitors for all members we haven't seen before.
%% Skips ourselves (no self-monitor) and already-monitored peers (idempotent).
add_monitors(MemberIds, State = #state{member_id = Self, members = Members, monitors = Monitors}) ->
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

%% Remove a member and demonitor it.
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
