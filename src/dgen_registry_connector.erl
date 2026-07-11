-module(dgen_registry_connector).
-behaviour(gen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Keeps Erlang distribution in step with the registry's database-backed membership,
%% on behalf of `dgen_registry_member`.  One connector runs per registry, a sibling of
%% the member and elector under the registry supervisor.
%%
%% Section references (e.g. `§5.7`) point to `docs/dgen_registry_design.md`.
%%
%% ## Why a separate process
%%
%% This is purely the **node-level connectivity** concern — proactively opening
%% distribution connections to every member node, and reaping nodes that have gone
%% away — which is disjoint from the member's names/commit/leadership state.  Its own
%% state is just `recently_down` (mesh suppression) and `ever_connected` (the
%% leader-probe gate); it never touches the names table, the commit pipeline, or
%% leadership.  Splitting it out keeps the member focused on registration and keeps
%% this plumbing (and its subtle partition-detection ordering) in one small module.
%%
%% It reads the authoritative member set / leader / tokens from the **elector**
%% (off-loop, via short-lived helpers) and casts `{member_down, …}` back to the
%% elector; the resulting membership change flows to the member through the elector's
%% normal post-commit assume/fan-out, exactly as before.  The one message it sends the
%% member directly is `{durable_epoch, E}` (a missed-handoff nudge, piggybacked on the
%% mesh fetch).
%%
%% ## Node-level vs member-level liveness
%%
%% There are two independent liveness mechanisms and this module owns one of them:
%%
%% - **Node-level** (here): `net_kernel:monitor_nodes/1` → `nodeup`/`nodedown`, plus
%%   the periodic mesh, the backstop reap, and the leader-liveness probe.  Drives
%%   connect/disconnect and fenced `{member_down}` reaping.
%% - **Member-level** (in `dgen_registry_member`): an `erlang:monitor` on each peer
%%   *member process*, whose DOWN reports a fenced `{member_down}` and feeds the
%%   conflict trail.  Untouched by this module.
%%
%% Both the member and this connector independently subscribe to
%% `net_kernel:monitor_nodes/1` (multiple subscribers each receive the events): the
%% member reacts to `{nodeup, _}` by re-announcing its own `{join}` (membership
%% identity); this connector reacts by clearing suppression and re-meshing.

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

%% How often the connector proactively connects Erlang distribution to every other
%% member node, so the cluster converges to a full mesh and `nodes()` returns all
%% members (see the "Membership and connectivity" guarantee in the design doc).  A
%% backstop; startup and `nodeup` also trigger an immediate pass.
-define(MESH_INTERVAL, 10000).

%% After observing a node's `nodedown`, the mesh does *not* immediately reconnect it:
%% it waits this long so the departure can be recorded as a `{member_down}` and settle,
%% rather than fighting the partition machinery by healing a drop the instant it
%% happens.  A genuine departure leaves the member set within the window (and so is not
%% reconnected at all); a `{nodeup, _}` clears the suppression early.
-define(MESH_DOWN_COOLDOWN, 10000).

%% After a node drops, reap any registry member still stranded on that (still-
%% disconnected) node from the elector's set — a backstop for the case where a
%% member's own monitor `{member_down}` was fenced away as stale (e.g. a `{join}` the
%% dead member enqueued on a nodeup just before its node vanished re-added it with a
%% token never seen here).  Each pass reads the elector and reports `member_down` for
%% such members; it opens no distribution connection, so it cannot heal the very
%% partition it is reacting to.  Retried a few times because the resurrecting `{join}`
%% may not be processed until after the first pass; it stops early once the node
%% reconnects, leaves the set, or the attempts are exhausted.
-define(REAP_INTERVAL, 1000).
-define(REAP_ATTEMPTS, 6).

%% Leader-liveness probe.  The normal failover trigger is `nodedown`, which only fires
%% for a node this connector was *connected to* and then lost.  A leader recovered from
%% durable elector state whose node was never connected to — e.g. a whole-cluster cold
%% restart where the previously-elected node is gone — produces no `nodedown`, so the
%% dead leader is never reaped and every write forwards to it forever.  This periodic
%% probe closes that gap: if the elected leader's node is unreachable, it reaps that
%% node through the same fenced `member_down` path a `nodedown` uses.
%%
%% Crucially, the probe reaps **only a leader whose node this connector has never been
%% connected to** (`ever_connected`).  A leader we HAVE met and then lost is the
%% nodedown/monitor path's job: reaping it here would let a member on the minority side
%% of a *distribution-only* partition (mesh severed, database still reachable) depose
%% the majority's healthy leader through the shared database every probe interval — a
%% leadership ping-pong.  With the gate, the isolated side settles into the CP refusals
%% (§5.3) instead.  The first pass is delayed a grace window so a leader that is merely
%% *not connected yet* (the mesh connect is async) is given time to become reachable.
-define(LEADER_PROBE_GRACE, 4000).
-define(LEADER_PROBE_INTERVAL, 4000).

-record(state, {
    %% The member's registered name — also this registry's name, and the target of the
    %% `{durable_epoch, _}` nudge.  Self node is always `node()`.
    name :: atom(),
    %% The elector pid, discovered once via the shared supervisor in `discover_elector`
    %% (like the member).  `undefined` only between init/1 and that continuation.
    elector :: pid() | undefined,
    %% Nodes whose `nodedown` we saw within the last ?MESH_DOWN_COOLDOWN ms → timestamp.
    %% The proactive mesh skips these so it does not reconnect a node the instant it
    %% drops (letting the departure settle as a {member_down}); cleared on {nodeup}.
    recently_down = #{} :: #{node() => integer()},
    %% Nodes connected to at least once in this connector's lifetime — seeded from
    %% nodes() at init, extended on every {nodeup, _}, never pruned.  Gates the
    %% leader-liveness probe (see ?LEADER_PROBE_GRACE).
    ever_connected = #{} :: #{node() => true}
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

init(#{name := Name}) ->
    net_kernel:monitor_nodes(true),
    %% The elector pid cannot be resolved from init/1: the supervisor is still
    %% synchronously inside start_child for *this* process, so which_children would
    %% deadlock.  Defer to discover_elector, which also kicks off the first mesh pass.
    {ok,
        #state{
            name = Name,
            elector = undefined,
            recently_down = #{},
            %% A node connected at connector start counts as "ever connected".
            ever_connected = maps:from_keys(nodes(), true)
        },
        {continue, discover_elector}}.

%% Resolve the elector via the shared supervisor (safe here — the supervisor is no
%% longer blocked on this process's init/1) and start the connectivity loops.  The
%% member is started before this connector (see the supervisor child order), so it is
%% already registered and `dgen_registry:elector_pid/1` can walk from it to the elector.
handle_continue(discover_elector, State = #state{name = Name}) ->
    Elector = dgen_registry:elector_pid(Name),
    true = is_pid(Elector),
    %% Proactively connect distribution to every member node, now and periodically, so
    %% the cluster converges to a full mesh (see mesh_connect).
    self() ! mesh_connect,
    %% Backstop the nodedown-driven failover for a leader we never connected to (see
    %% probe_leader).  Delayed so a genuinely-reachable leader is meshed first.
    erlang:send_after(?LEADER_PROBE_GRACE, self(), probe_leader),
    {noreply, State#state{elector = Elector}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% A node became reachable: clear any mesh suppression for it, record it as
%% ever-connected (the probe gate), and re-mesh so we also reconnect to any other
%% members we lost touch with.  The member independently handles {nodeup, _} to
%% re-announce its own {join}; that is its concern, not ours.
handle_info({nodeup, Node}, State) ->
    RD = maps:remove(Node, State#state.recently_down),
    Ever = maps:put(Node, true, State#state.ever_connected),
    self() ! mesh_connect,
    {noreply, State#state{recently_down = RD, ever_connected = Ever}};
%% A node dropped: remember it briefly so the mesh does not immediately reconnect it
%% (let the departure settle as a {member_down}), and start the backstop reap.
handle_info({nodedown, Node}, State = #state{recently_down = RD}) ->
    schedule_reap(Node, ?REAP_ATTEMPTS),
    {noreply, State#state{recently_down = RD#{Node => erlang:system_time(millisecond)}}};
%% Backstop reap: while `Node` is still disconnected, report `member_down` for any
%% member the elector still lists on it, then re-arm for a few attempts.  Stops as soon
%% as the node reconnects (a live member re-announces and its `{join}` wins) or the
%% attempts run out.  Reads the elector and casts only — never opens a connection.
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
%% Periodic leader-liveness probe (see ?LEADER_PROBE_GRACE): if the elector's leader
%% sits on a node that is neither ours, nor connected, nor ever-connected, reap it so a
%% reachable member re-elects.  Off the loop — spawn_probe_leader reads the elector and
%% casts only, opening no connection.
handle_info(probe_leader, State = #state{elector = Elector, ever_connected = Ever}) ->
    erlang:send_after(?LEADER_PROBE_INTERVAL, self(), probe_leader),
    spawn_probe_leader(Elector, node(), Ever),
    {noreply, State};
%% Converge Erlang distribution to a full mesh: read the authoritative member set from
%% the elector (a DB-backed read, identical on every node) and connect to every member
%% node not currently suppressed.  The read runs in a short-lived helper so the
%% (potentially slow, durable-queue) `get_members` call never blocks this loop; the
%% helper reports the set back as `{mesh_members, _}`, and the suppression decision +
%% connect are made *there* — on this connector's mailbox, strictly ordered after any
%% `{nodedown, _}` — so an in-flight fetch cannot reconnect a node that dropped while it
%% was running.  A successful connect fires `nodeup`, driving the member's rejoin.
handle_info(mesh_connect, State = #state{elector = Elector, name = Name}) ->
    erlang:send_after(?MESH_INTERVAL, self(), mesh_connect),
    Now = erlang:system_time(millisecond),
    RD = maps:filter(
        fun(_Node, Ts) -> Ts >= Now - ?MESH_DOWN_COOLDOWN end, State#state.recently_down
    ),
    spawn_mesh_fetch(Elector, self(), Name),
    {noreply, State#state{recently_down = RD}};
%% The mesh fetch helper reports the authoritative member node list.  Deciding
%% suppression *here* (not in the helper, at spawn time) is what makes partition
%% detection reliable: a fetch spawned just before a node dropped carries no knowledge
%% of the drop, and computing suppression from its stale snapshot would reconnect the
%% very node being removed — healing the partition before the `{member_down}` can
%% settle.  Because this message is on the connector's mailbox, it is strictly ordered
%% after the `{nodedown, Node}` the drop enqueued, so `recently_down` already reflects
%% it.  The `connect_node` handshakes run off-process (they can block).
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
    erlang:send_after(?REAP_INTERVAL, self(), {reap_down, Node, N}),
    ok.

%% Report `member_down` for every member the elector currently lists on `Node`, fenced
%% with each member's current durable token (so a member that has genuinely rejoined is
%% not clobbered).  Runs off the loop; reads the elector and casts only.
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

%% Read the elector's current leader; if it is a *different* node that is not connected
%% AND never connected to (the cold-restart recovery case), reap every member stranded
%% there (fenced `member_down`, via spawn_reap), triggering re-election.  A leader that
%% is ourselves, unset, on a connected node, or on a node met before is left alone.
%% Runs off the loop and never opens a connection.
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

%% Read the authoritative member set from the elector and report it back to the
%% connector as `{mesh_members, MemberNodes}`, in a short-lived helper so the
%% (potentially slow, durable-queue) read never blocks the loop.  The member set is
%% read via a regular `call` (not `priority_call`): it rides the elector's durable
%% queue in order with the membership changes, so the mesh sees a just-joined node as
%% soon as its join is processed.  The set is authoritative — the same on every node —
%% including a brand-new member that has only committed its join to the database.
%%
%% Piggybacks a durable-epoch check: read the committed election epoch (a priority
%% read) and send it to the **member** as `{durable_epoch, E}`.  If it is ahead of what
%% the member has heard, a leadership handoff never reached this node — the member
%% re-joins to trigger a fresh assume/fan-out.  The suppression decision is deliberately
%% *not* made here — the connector applies it on its own mailbox (see mesh_members) — so
%% a fetch spawned before a node dropped cannot reconnect it with a stale snapshot.
spawn_mesh_fetch(Elector, Connector, MemberName) ->
    _ = spawn(fun() ->
        try dgen_server:priority_call(Elector, get_epoch) of
            DurableEpoch when is_integer(DurableEpoch) ->
                MemberName ! {durable_epoch, DurableEpoch};
            _ ->
                ok
        catch
            _:_ -> ok
        end,
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

%% Connect Erlang distribution to each target member node, off the loop (the handshake
%% can block on an unreachable node).  `connect_node/1` is idempotent and a node that
%% cannot be reached simply returns `false`; either way the next mesh pass retries.
%% Targets are already filtered against the current recently-down suppression.
spawn_mesh_connect(Targets) ->
    _ = spawn(fun() -> lists:foreach(fun net_kernel:connect_node/1, Targets) end),
    ok.
