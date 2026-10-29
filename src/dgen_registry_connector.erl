-module(dgen_registry_connector).
-behaviour(gen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("../include/dgen_eta.hrl").

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Node-level connectivity for one registry (sibling of the member and elector): meshes
%% distribution to every member node, reaps members stranded on dropped nodes, and probes
%% leader liveness.  Never touches names, commits or leadership; reads the elector
%% off-loop and casts fenced `{member_down}` back.  `§N.N` = docs/design/dgen_registry_design.md.
%%
%% Modes (§8, §4.6): `self_managed` runs the mesh; `provided_externally` opens no
%% connections but still does the registry-scoped duties (reap, leader probe, epoch nudge
%% on its own `epoch_check` timer) and warns if no leader is seen for a sustained window.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([start_link/2]).
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% Backstop; startup and `nodeup` also trigger an immediate pass.
-define(MESH_INTERVAL, 10000).

%% Don't reconnect a node right after its `nodedown`: let the departure settle as a
%% `{member_down}` rather than healing the partition instantly.  `nodeup` clears it early.
-define(MESH_DOWN_COOLDOWN, 10000).

%% Backstop reap for a member whose own `{member_down}` was fenced as stale.  Retried
%% because a resurrecting `{join}` may land after the first pass.  Opens no connection.
-define(REAP_INTERVAL, 1000).
-define(REAP_ATTEMPTS, 6).

%% Reaps a leader on a node we were *never* connected to (e.g. cold restart onto a dead
%% leader: no `nodedown` ever fires).  Gated on `ever_connected` so a distribution-only
%% partition cannot depose the majority's leader via the DB (ping-pong); a lost-but-met
%% leader is the nodedown path's job.  Grace lets the async mesh connect first.
-define(LEADER_PROBE_GRACE, 4000).
-define(LEADER_PROBE_INTERVAL, 4000).

%% `provided_externally` only: epoch-nudge / isolation-check tick (replaces the mesh).
-define(EPOCH_CHECK_INTERVAL, 10000).

%% ~60 s of no leader: long enough to ride out startup/handoff, short enough to surface.
-define(ISOLATION_WARN_TICKS, 6).

-record(state, {
    %% Member's registered name (= registry name); target of `{durable_epoch, _}`.
    name :: atom(),
    mode = self_managed :: self_managed | provided_externally,
    %% `undefined` only until `discover_elector`.
    elector :: pid() | undefined,
    %% Node -> nodedown timestamp; mesh skips these (self_managed only).
    recently_down = #{} :: #{node() => integer()},
    %% Never pruned; gates the leader probe.
    ever_connected = #{} :: #{node() => true},
    %% provided_externally isolation warning: counter and one-shot latch.
    no_leader_ticks = 0 :: non_neg_integer(),
    isolation_warned = false :: boolean()
}).

%% ---------------------------------------------------------------------------
%% Public API
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Starts the connector for registry `Name` (unregistered; held by the supervisor).".
-endif.
-spec start_link(Name :: atom(), Args :: map()) -> gen_server:start_ret().
start_link(Name, Args) ->
    gen_server:start_link(?MODULE, Args#{name => Name}, []).

%% ---------------------------------------------------------------------------
%% gen_server callbacks
%% ---------------------------------------------------------------------------

init(#{name := Name} = Args) ->
    net_kernel:monitor_nodes(true),
    Mode = dgen_config:connectivity(maps:get(config, Args, #{})),
    %% Can't resolve the elector here: the supervisor is blocked in start_child for this
    %% process, so which_children would deadlock.
    {ok,
        #state{
            name = Name,
            mode = Mode,
            elector = undefined,
            recently_down = #{},
            ever_connected = maps:from_keys(nodes(), true)
        },
        {continue, discover_elector}}.

%% The member starts before this connector, so `elector_pid/1` can walk from it.
handle_continue(discover_elector, State = #state{name = Name, mode = Mode}) ->
    Elector = dgen_registry:elector_pid(Name),
    true = is_pid(Elector),
    case Mode of
        self_managed ->
            self() ! mesh_connect;
        provided_externally ->
            arm(?EPOCH_CHECK_INTERVAL, epoch_check)
    end,
    arm(?LEADER_PROBE_GRACE, probe_leader),
    {noreply, State#state{elector = Elector}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% The member handles {nodeup, _} separately (re-announces its {join}).
handle_info({nodeup, Node}, State = #state{mode = provided_externally}) ->
    Ever = maps:put(Node, true, State#state.ever_connected),
    {noreply, State#state{ever_connected = Ever}};
handle_info({nodeup, Node}, State) ->
    RD = maps:remove(Node, State#state.recently_down),
    Ever = maps:put(Node, true, State#state.ever_connected),
    self() ! mesh_connect,
    {noreply, State#state{recently_down = RD, ever_connected = Ever}};
handle_info({nodedown, Node}, State = #state{mode = provided_externally}) ->
    schedule_reap(Node, ?REAP_ATTEMPTS),
    {noreply, State};
handle_info({nodedown, Node}, State = #state{recently_down = RD}) ->
    schedule_reap(Node, ?REAP_ATTEMPTS),
    {noreply, State#state{recently_down = RD#{Node => erlang:system_time(millisecond)}}};
handle_info(epoch_check, State = #state{elector = Elector, name = Name}) ->
    arm(?EPOCH_CHECK_INTERVAL, epoch_check),
    spawn_epoch_check(Elector, Name, self()),
    {noreply, State};
handle_info({connectivity_leader, undefined}, State = #state{no_leader_ticks = N}) ->
    N1 = N + 1,
    Warned1 =
        case N1 >= ?ISOLATION_WARN_TICKS andalso not State#state.isolation_warned of
            true ->
                warn_possible_isolation(State#state.name, N1),
                true;
            false ->
                State#state.isolation_warned
        end,
    {noreply, State#state{no_leader_ticks = N1, isolation_warned = Warned1}};
handle_info({connectivity_leader, _Leader}, State) ->
    {noreply, State#state{no_leader_ticks = 0, isolation_warned = false}};
%% Stops once the node reconnects (a live member's `{join}` wins).
handle_info({reap_down, _Node, 0}, State) ->
    {noreply, State};
handle_info({reap_down, Node, N}, State = #state{elector = Elector}) ->
    case dgen_utils:node_reachable(Node) of
        true ->
            ok;
        false ->
            spawn_reap(Elector, Node),
            schedule_reap(Node, N - 1)
    end,
    {noreply, State};
handle_info(probe_leader, State = #state{elector = Elector, ever_connected = Ever}) ->
    arm(?LEADER_PROBE_INTERVAL, probe_leader),
    spawn_probe_leader(Elector, node(), Ever),
    {noreply, State};
handle_info(mesh_connect, State = #state{elector = Elector, name = Name}) ->
    arm(?MESH_INTERVAL, mesh_connect),
    Now = erlang:system_time(millisecond),
    RD = maps:filter(
        fun(_Node, Ts) -> Ts >= Now - ?MESH_DOWN_COOLDOWN end, State#state.recently_down
    ),
    spawn_mesh_fetch(Elector, self(), Name),
    {noreply, State#state{recently_down = RD}};
%% Suppression is decided here, not in the fetch helper: this message is ordered after
%% any `{nodedown, _}` on our mailbox, so a stale in-flight fetch can't reconnect a node
%% that just dropped (which would heal the partition before `{member_down}` settles).
handle_info({mesh_members, MemberNodes}, State) ->
    Now = erlang:system_time(millisecond),
    RD = maps:filter(
        fun(_Node, Ts) -> Ts >= Now - ?MESH_DOWN_COOLDOWN end, State#state.recently_down
    ),
    SelfNode = node(),
    Targets = [
        Node
     || Node <- MemberNodes, Node =/= SelfNode, not maps:is_key(Node, RD)
    ],
    spawn_mesh_connect(Targets),
    {noreply, State#state{recently_down = RD}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    net_kernel:monitor_nodes(false),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

schedule_reap(Node, N) ->
    arm(?REAP_INTERVAL, {reap_down, Node, N}),
    ok.

%% Fenced with each member's current durable token so a genuine rejoin isn't clobbered.
spawn_reap(Elector, Node) ->
    _ = spawn(fun() ->
        try dgen_server:priority_call(Elector, get_members) of
            Members when is_list(Members) ->
                lists:foreach(
                    fun
                        ({N, _} = MemberId) when N =:= Node ->
                            Token = dgen_registry_elector:member_token(
                                Elector, MemberId, undefined
                            ),
                            dgen_server:cast(Elector, {member_down, MemberId, Token});
                        (_) ->
                            ok
                    end,
                    Members
                );
            _ ->
                ok
        catch
            _:_ -> ok
        end
    end),
    ok.

spawn_probe_leader(Elector, SelfNode, EverConnected) ->
    _ = spawn(fun() ->
        try dgen_server:priority_call(Elector, get_leader) of
            {LeaderNode, _} when LeaderNode =/= SelfNode ->
                case
                    lists:member(LeaderNode, nodes()) orelse
                        maps:is_key(LeaderNode, EverConnected)
                of
                    true -> ok;
                    false -> spawn_reap(Elector, LeaderNode)
                end;
            _ ->
                ok
        catch
            _:_ -> ok
        end
    end),
    ok.

%% Regular `call`, not `priority_call`: it rides the durable queue in order with
%% membership changes, so a just-joined node is seen as soon as its join is processed.
spawn_mesh_fetch(Elector, Connector, MemberName) ->
    _ = spawn(fun() ->
        nudge_durable_epoch(Elector, MemberName),
        try dgen_server:call(Elector, get_members) of
            Members when is_list(Members) ->
                Connector ! {mesh_members, [Node || {Node, _Name} <- Members]};
            _ ->
                ok
        catch
            _:_ -> ok
        end
    end),
    ok.

spawn_epoch_check(Elector, MemberName, Connector) ->
    _ = spawn(fun() ->
        nudge_durable_epoch(Elector, MemberName),
        Leader =
            try dgen_server:priority_call(Elector, get_leader) of
                L -> L
            catch
                %% Counts toward the isolation window.
                _:_ -> undefined
            end,
        Connector ! {connectivity_leader, Leader}
    end),
    ok.

%% Missed-handoff backstop: if the committed epoch is ahead of the member's, it re-joins.
%% The send must be inside `try` (not after `of`): sending to an unregistered name raises.
nudge_durable_epoch(Elector, MemberName) ->
    try
        case dgen_server:priority_call(Elector, get_epoch) of
            DurableEpoch when is_integer(DurableEpoch) ->
                MemberName ! {durable_epoch, DurableEpoch},
                ok;
            _ ->
                ok
        end
    catch
        _:_ -> ok
    end.

warn_possible_isolation(Name, Ticks) ->
    logger:warning(
        "dgen_registry ~p: connectivity=provided_externally but no leader has been "
        "observed for ~b checks (~b ms). Distribution connectivity for this registry is "
        "delegated (§4.6) — verify a self_managed provider registry spans this node, or "
        "this node may be isolated and refusing writes (CP, §5.3).",
        [Name, Ticks, Ticks * ?EPOCH_CHECK_INTERVAL]
    ),
    ok.

%% Off-loop: the handshake can block on an unreachable node.
spawn_mesh_connect(Targets) ->
    _ = spawn(fun() -> lists:foreach(fun net_kernel:connect_node/1, Targets) end),
    ok.

%% See `dgen_registry_member:arm/2`.
arm(Delay, Msg) ->
    _ = ?ETA_LOG({arm, Msg, Delay}),
    erlang:send_after(Delay, self(), Msg).
