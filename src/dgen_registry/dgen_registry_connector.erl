-module(dgen_registry_connector).
-behaviour(gen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("../../include/dgen_eta.hrl").

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Handles node-level connectivity for one registry. It runs alongside the member and
%% the elector. It connects distribution to every member node, reaps members stranded
%% on nodes that dropped, and checks that the leader is still alive. It never touches
%% names, commits or leadership. It reads the elector from spawned processes and casts
%% `{member_down}` back, tagged with the member's token so the elector can ignore a
%% stale one. `§N.N` refers to docs/design/dgen_registry_design.md.
%%
%% There are two modes (§8, §4.6). In `self_managed` mode the connector runs the mesh.
%% In `provided_externally` mode it opens no connections, but it still reaps members,
%% probes the leader, and nudges the member's epoch on its own `epoch_check` timer. It
%% also warns if it sees no leader for a sustained period.
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

%% Safety timer for meshing the network of nodes. Startup and `nodeup` also trigger an
%% immediate pass.
-define(MESH_INTERVAL, 10000).

%% We don't reconnect a node right after its `nodedown`. The departure should settle as
%% a `{member_down}` first, instead of the partition healing instantly. A `nodeup` ends
%% the cooldown early.
-define(MESH_DOWN_COOLDOWN, 10000).

%% Backstop reap for a member whose own `{member_down}` the elector rejected as stale.
%% We retry because a `{join}` that brings the member back may land after the first
%% pass. Reaping opens no connection.
-define(REAP_INTERVAL, 1000).
-define(REAP_ATTEMPTS, 6).

%% Reaps a leader on a node we were *never* connected to. This happens on a cold restart
%% onto a dead leader, where no `nodedown` ever fires. We skip nodes in `ever_connected`.
%% Otherwise a partition in distribution alone could let us depose the majority's leader
%% through the database, and the two sides would keep deposing each other. A leader we
%% did connect to and then lost is handled by the `nodedown` path. The grace period lets
%% the mesh connect first.
-define(LEADER_PROBE_GRACE, 4000).
-define(LEADER_PROBE_INTERVAL, 4000).

%% Only in `provided_externally` mode. This tick nudges the epoch and checks for
%% isolation, in place of the mesh.
-define(EPOCH_CHECK_INTERVAL, 10000).

%% About 60 s without a leader. That is long enough to ride out startup or a handoff,
%% and short enough to surface a real problem.
-define(ISOLATION_WARN_TICKS, 6).

-record(state, {
    %% The member's registered name, which is the registry name. We send
    %% `{durable_epoch, _}` to it.
    name :: atom(),
    mode = self_managed :: self_managed | provided_externally,
    %% `undefined` only until `discover_elector` runs.
    elector :: pid() | undefined,
    %% Maps each node to the time of its `nodedown`. The mesh skips these nodes. Only
    %% used in `self_managed` mode.
    recently_down = #{} :: #{node() => integer()},
    %% Never pruned. The leader probe skips these nodes.
    ever_connected = #{} :: #{node() => true},

    %% `no_leader_ticks` and `isolation_warned` are a safety feature for
    %% `provided_externally` mode. We count the ticks without a connectivity leader
    %% and log a warning once.
    no_leader_ticks = 0 :: non_neg_integer(),
    isolation_warned = false :: boolean()
}).

%% ---------------------------------------------------------------------------
%% Public API
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Starts the connector for registry `Name`. The process is unregistered, and the supervisor holds its pid.".
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
    {ok,
        #state{
            name = Name,
            mode = Mode,
            elector = undefined,
            recently_down = #{},
            ever_connected = maps:from_keys(nodes(), true)
        },
        {continue, discover_elector}}.

%% The member starts before this connector, so `elector_pid/1` can find the elector
%% through it.
handle_continue(discover_elector, State = #state{name = Name, mode = Mode}) ->
    Elector = dgen_registry:elector_pid(Name),
    true = is_pid(Elector),
    case Mode of
        self_managed ->
            self() ! mesh_connect;
        provided_externally ->
            schedule_timer(?EPOCH_CHECK_INTERVAL, epoch_check)
    end,
    schedule_timer(?LEADER_PROBE_GRACE, probe_leader),
    {noreply, State#state{elector = Elector}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% The member handles `{nodeup, _}` separately, by re-announcing its `{join}`.
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
    schedule_timer(?EPOCH_CHECK_INTERVAL, epoch_check),
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
%% Stops once the node reconnects, because a live member's `{join}` takes precedence.
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
    schedule_timer(?LEADER_PROBE_INTERVAL, probe_leader),
    spawn_probe_leader(Elector, node(), Ever),
    {noreply, State};
handle_info(mesh_connect, State = #state{elector = Elector, name = Name}) ->
    schedule_timer(?MESH_INTERVAL, mesh_connect),
    Now = erlang:system_time(millisecond),
    RD = maps:filter(
        fun(_Node, Ts) -> Ts >= Now - ?MESH_DOWN_COOLDOWN end, State#state.recently_down
    ),
    spawn_mesh_fetch(Elector, self(), Name),
    {noreply, State#state{recently_down = RD}};
%% We filter out recently-down nodes here, not in the fetch helper. This message
%% arrives after any `{nodedown, _}` already in our mailbox, so a stale fetch can't
%% reconnect a node that just dropped. That would heal the partition before the
%% `{member_down}` settles.
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
    schedule_timer(?REAP_INTERVAL, {reap_down, Node, N}),
    ok.

%% Sends `member_down` with each member's current durable token. If the member has
%% genuinely rejoined, its token has changed and the elector ignores the stale one.
spawn_reap(Elector, Node) ->
    spawn_catch_all(fun() ->
        case dgen_registry_elector:priority_get_members(Elector) of
            Members when is_list(Members) ->
                lists:foreach(
                    fun
                        ({N, _} = MemberId) when N =:= Node ->
                            Token = dgen_registry_elector:get_member_token(
                                Elector, MemberId, undefined
                            ),
                            dgen_registry_elector:member_down(Elector, MemberId, Token);
                        (_) ->
                            ok
                    end,
                    Members
                );
            _ ->
                ok
        end
    end).

spawn_probe_leader(Elector, SelfNode, EverConnected) ->
    spawn_catch_all(fun() ->
        case dgen_registry_elector:priority_get_leader(Elector) of
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
        end
    end).

%% Uses a regular `call`, not `priority_call`. The call goes through the durable queue
%% in order with membership changes, so we see a newly joined node as soon as the
%% elector processes its join.
spawn_mesh_fetch(Elector, Connector, MemberName) ->
    spawn_catch_all(fun() ->
        nudge_durable_epoch(Elector, MemberName),
        case dgen_registry_elector:get_members(Elector) of
            Members when is_list(Members) ->
                Connector ! {mesh_members, [Node || {Node, _Name} <- Members]};
            _ ->
                ok
        end
    end).

spawn_epoch_check(Elector, MemberName, Connector) ->
    spawn_catch_all(fun() ->
        nudge_durable_epoch(Elector, MemberName),
        Leader =
            try dgen_registry_elector:priority_get_leader(Elector) of
                L -> L
            catch
                %% A failed call counts toward the isolation window.
                _:_ -> undefined
            end,
        Connector ! {connectivity_leader, Leader}
    end).

%% Backstop for a missed handoff. If the committed epoch is ahead of the member's, the
%% member re-joins. The send must be inside the `try` body, not after `of`, because
%% sending to an unregistered name raises.
nudge_durable_epoch(Elector, MemberName) ->
    try
        case dgen_registry_elector:priority_get_epoch(Elector) of
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

%% Runs in a separate process, because the handshake can block on an unreachable node.
spawn_mesh_connect(Targets) ->
    spawn_catch_all(fun() -> lists:foreach(fun net_kernel:connect_node/1, Targets) end).

spawn_catch_all(Fun) ->
    _ = spawn(fun() ->
        try
            Fun()
        catch
            _:_ ->
                ok
        end
    end),
    ok.

%% See `dgen_registry_member:schedule_timer/2`.
schedule_timer(Delay, Msg) ->
    _ = ?ETA_LOG({arm, Msg, Delay}),
    erlang:send_after(Delay, self(), Msg).
