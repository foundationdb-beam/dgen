defmodule DGen.Sim.Cluster do
  @moduledoc """
  An N-member `dgen_registry` cluster running inside a single BEAM.

  Normally a member's id is `{node(), RegistryName}` and the registry name also
  fixes the durable keyspace, so one VM can host at most one member of a registry
  and any multi-member test needs real distribution and real nodes. The `keyspace`
  option to `dgen_registry:start_link/3` splits those apart: N differently-named
  members can share one registry's elector queue, leader key, and version key.

  That is what this module exploits. Every member here is a genuine
  `dgen_registry` supervision tree — real elector, real member, real commits
  against the backend — so the protocol under test is the shipped one, not a
  model of it. What changes is only that they are reachable and controllable from
  one process, which makes it cheap (a 3-member cluster starts in ~100ms, against
  ~5s for a `:peer` node) and lets `eta_net` interpose on every message between
  them.

  ## Node faults

  Each member's whole supervision tree sits on a simulated node of its own, so
  `eta_net` can express node-level failure and not only message loss:

  - `partition/3,4` cuts a pair of nodes *and* delivers the `{nodedown, Peer}`
    both ends would see, plus a `noconnection` DOWN at every peer monitor across
    the cut. `isolate/2,3` is the same fault against every peer at once.
  - `kill_node/2,3` is node death — the tree dies atomically and every survivor
    gets the events that death produces.
  - `crash/2` remains member-level loss: the tree dies with no node event, which
    is what a member exiting on its own looks like.

  What is still not simulated is `net_kernel`. A simulated node is a name in a
  table: `nodes()` does not list it and `dgen_utils:node_reachable/1` therefore
  answers `true` for every member id, since every member id's node component is
  the one real `node()`. So the *member*-level reactions to a node event are
  exercised here (`handle_info({nodeup, _})`'s rejoin and unregister re-drive,
  the peer-monitor DOWN that drives `{member_down}`), and the connector's
  reachability-keyed backstops — the `{nodedown, Node}` reap and the leader
  probe — are not: they filter member ids by node and no member id names a
  simulated node. Those belong in `dgen_registry_cluster_test.exs` with real
  peers.
  """

  defstruct [:keyspace, :tenant, :net, :opts, members: %{}]

  @type t :: %__MODULE__{}

  # ---------------------------------------------------------------------------
  # Lifecycle
  # ---------------------------------------------------------------------------

  @doc """
  Starts a cluster of `n` members on `tenant`.

  Options:

  - `:seed` — fault-schedule seed, used only when this call starts the network.
  - `:drop_p`, `:delay_p`, `:max_delay` — the network fault policy. Scoped to
    member-to-member traffic; see `apply_policy/1`, which is the part of this
    module most worth reading before changing a fault.
  - `:registry_opts` — per-registry options (`strict_replication`,
    `register_replicas`, …) applied to every member.
  - `:ready_timeout` — how long to wait for each member to sync (default 10s).
  - `:simulate_peer_monitors` — hold each member and place its tree as it starts,
    rather than placing once the cluster is up. **Requires a scheduler**; see below.

  The policy is applied only once every member is ready, or a member can be
  partitioned away before it has ever synced and the run measures startup rather
  than the protocol.

  Under `eta_run` a network already exists, seeded from the run's seed; this call
  leaves its lifetime, seed and policy alone and only declares the topology.

  ## `:simulate_peer_monitors`, and why it is not the default

  `dgen_registry_member`'s failure detection is an `erlang:monitor` on each peer
  (`add_member_monitors/2`), which `eta_transform` points at `eta_net:monitor/2`.
  That monitor is *simulated* — the only kind a partition can fire `noconnection`
  at — when both ends are already on different simulated nodes at the moment it
  is created. Peers are monitored while the cluster forms, so placing after
  `await_ready/2` leaves every one of them a plain `erlang:monitor` and makes
  `partition/3,4` message loss with a signal attached and nothing more.

  Placing each tree as `start_link/3` returns is necessary and **not sufficient**,
  and the gap is the interesting part: a member learns of its peers when the
  elector distributes the member set back, which is an ordinary message arriving
  some unbounded time later. So which peer monitors ended up simulated came down
  to real-time luck, and the run stopped being a function of its seed — about one
  seed in five. This option therefore *holds* each member from the moment its tree
  starts until every tree is placed, so no monitor anywhere can be created before
  the topology is complete. Ordering the two beats hoping one wins.

  It is opt-in because a simulated monitor learns of an ordinary exit from
  `eta_sched`'s exit trace — so under `eta_run` it fires on a crash exactly as a
  real one would, and with no scheduler it never fires at all. Turning it on for
  the real-clock suite would silently disable `crash/2` detection, which is the
  failure mode `eta_net`'s own docs warn about.
  """
  def start(tenant, n, opts \\ []) do
    keyspace = :"sim_#{:erlang.unique_integer([:positive])}"
    # A run started by `eta_run` already has a network, seeded from the run's seed
    # and configured by the harness; restarting it here would reset both. Outside a
    # run — the older real-clock suites — this is the only thing that would.
    owns_net? = not :eta_net.running()
    if owns_net?, do: :ok = :eta_net.start(%{seed: Keyword.get(opts, :seed, 0)})

    registry_opts =
      opts
      |> Keyword.get(:registry_opts, %{})
      |> Map.put(:keyspace, keyspace)

    ready_timeout = Keyword.get(opts, :ready_timeout, 10_000)
    eager? = Keyword.get(opts, :simulate_peer_monitors, false)

    members =
      for i <- 1..n, into: %{} do
        name = :"#{keyspace}_m#{i}"
        {:ok, sup} = :dgen_registry.start_link(name, tenant, registry_opts)

        # Hold the member before it can act on anything the elector sends back, so
        # no peer monitor can be created until every tree is placed.
        if eager? do
          :ok = hold_member(name)
          :ok = place_tree(%{name: name, sup: sup, index: i})
        end

        # Captured here, while the supervisor is still answerable — see `tree_pids/1`.
        {name, %{name: name, sup: sup, index: i, children: capture_children(sup)}}
      end

    # Every tree is placed, so every peer monitor created from here on crosses a
    # declared link. Released in start order, the order they were placed in.
    if eager?, do: for(i <- 1..n, do: :ok = release_member(:"#{keyspace}_m#{i}"))

    cluster = %__MODULE__{
      keyspace: keyspace,
      tenant: tenant,
      net: if(owns_net?, do: :owned, else: :borrowed),
      opts: opts,
      members: members
    }

    for {name, _} <- members do
      case :dgen_registry.await_ready(name, ready_timeout) do
        :ok -> :ok
        other -> raise "member #{name} never became ready: #{inspect(other)}"
      end

      :ok
    end

    # Only now is it safe to turn faults on: a member partitioned away before it
    # has ever synced makes the run measure startup rather than the protocol.
    #
    # Only when this call started the network. Under `eta_run` the policy belongs
    # to the harness, which sets it at the end of `init/2` for the same reason.
    # Placement always: it describes the system, and it is the thing that decides
    # which sends are faultable at all. The policy only when this call owns the
    # network — see `apply_policy/1`.
    :ok = place_members(cluster)
    if owns_net?, do: :ok = apply_policy(cluster)

    cluster
  end

  @doc """
  Puts each member's whole supervision tree on a simulated node of its own.

  **The placement is the fault model.** `eta_net` faults a send only when both ends
  are faultable and their nodes differ, so which traffic can be lost follows from
  the topology rather than from a predicate this module has to keep correct.

  A node's processes are on it in two different senses, which is the distinction
  `eta_net:place/2` and `eta_net:attach/2` draw:

  - The **member** is `place`d: *located* and *faultable*. It is the only one
    whose messages are on the wire.
  - The **elector** and the **connector** are `attach`ed: located but never
    faultable. They are on the node — they take its link events and die with it
    — and their traffic is never dropped, because it is not network traffic. The
    elector coordinates through the durable store and the connector talks to
    nothing; dropping their sends models a failure the real system cannot have,
    and produces `acked_bindings_present` violations that look exactly like the
    replication defect this suite hunts.

  Attaching rather than leaving them unplaced is what makes a node fault mean
  anything: an unplaced process is on no node at all, so it learns nothing when
  one fails and survives a `kill_node/2,3` that killed its own member.

  **The supervisor is deliberately on no node**, even though it does have to die
  with one. Locating it would hand it every link event, and a supervisor answers
  an unrecognised message by logging one — which under a run is a `logger` call
  from a process the scheduler does not own, loading `Logger.Translator` and
  `Inspect.Tuple` on demand and putting a `code_server` round trip inside the
  schedule. It fails `eta_run:audit/1`, which is how this was found. `kill_node/2`
  takes the supervisor down by hand instead.

  Everything a member spawns — transaction workers, snapshot collectors — inherits
  its node *and its faultability* as `eta_sched` adopts it, so the topology does
  not go stale the first time the system creates a worker.
  """
  def place_members(%__MODULE__{} = c) do
    for m <- Enum.sort_by(Map.values(c.members), & &1.index), do: :ok = place_tree(m)
    :ok
  end

  @doc "The simulated node a member sits on. Stable across a crash and restart."
  def node_of(%__MODULE__{} = c, name), do: node_name(Map.fetch!(c.members, name).index)

  defp node_name(index), do: :"member_#{index}"

  # Tolerant of a tree that is gone or going: `kill_node/2` leaves a dead
  # supervisor behind, and `place_members/1` is called again after every restart.
  defp place_tree(m) do
    node = node_name(m.index)

    case children(m.sup) do
      %{} = kids ->
        for pid <- [kids[:member]], is_pid(pid), do: :ok = :eta_net.place(node, [pid])

        case for id <- [:elector, :connector], is_pid(kids[id]), do: kids[id] do
          [] -> :ok
          pids -> :ok = :eta_net.attach(node, pids)
        end

      :gone ->
        :ok
    end

    :ok
  end

  defp children(sup) do
    Map.new(Supervisor.which_children(sup), fn {id, pid, _type, _mods} -> {id, pid} end)
  catch
    :exit, _ -> :gone
  end

  @doc """
  Every process of every member's tree, **supervisor included**, in a stable
  order — what a harness hands to `eta_harness:processes/1`.

  ## The supervisor belongs in the schedule

  It reads as infrastructure rather than as part of the system, and it is not.
  `dgen_registry:elector_pid/1` finds the elector by reading the member's
  `$ancestors` and asking that supervisor for its children, so every
  `get_leader/1`, `get_epoch/1` and `get_members/1` — from a client, and from the
  connector's reap, mesh-fetch and epoch-check helpers — is a `gen_server:call`
  into the supervisor. Left undeclared, those are scheduled processes blocking on
  one the scheduler does not own, which answers whenever the *real* scheduler
  runs it. It was the largest single source of wall-clock ordering in a node-fault
  run: 561k of the parked-process samples in one sweep were sitting in
  `gen:do_call/4` waiting on a supervisor.

  ## Read from a snapshot, never from the supervisor

  These pids are captured while the tree is starting, and cached. The caller is
  the driver, the driver must not call into a scheduled process, and once the
  supervisor is scheduled `Supervisor.which_children/1` is exactly that call — it
  would block until the run gave up. The cache stays correct because the tree is
  `one_for_all` and nothing restarts it inside a run: `kill_node/2,3` is terminal
  by construction, and `restart/3` recaptures.
  """
  def tree_pids(%__MODULE__{} = c) do
    for m <- Enum.sort_by(Map.values(c.members), & &1.index),
        pid <- [m.sup | m.children],
        is_pid(pid),
        Process.alive?(pid),
        do: pid
  end

  # Hold a member across the placement of the whole cluster, so that placement is
  # ordered *before* peer monitoring rather than racing it. See `start/3`.
  #
  # Suspending the member is enough. Its `discover_elector` continuation runs
  # before any system message and only casts a join to its own elector, which
  # creates no monitors; `add_member_monitors/2` runs when the elector distributes
  # the member set back, and that is an ordinary message a held member cannot
  # touch. So no monitor exists anywhere until `release_member/1`, by which point
  # every tree is on a node.
  defp hold_member(name) do
    :sys.suspend(name)
  catch
    # It has to exist — `start_link/3` has returned — so this is only a guard
    # against a tree that died on the way up, which `await_ready/2` reports better.
    :exit, _ -> :ok
  end

  defp release_member(name) do
    :sys.resume(name)
  catch
    :exit, _ -> :ok
  end

  # Child pids in a fixed role order, captured once while the tree is starting.
  defp capture_children(sup) do
    case children(sup) do
      %{} = kids -> for id <- [:elector, :member, :connector], is_pid(kids[id]), do: kids[id]
      :gone -> []
    end
  end

  @doc """
  `place_members/1` plus this cluster's random fault policy.

  Only for a cluster that started its own network. Under `eta_run` the policy
  belongs to the harness — the topology does not, which is why placement is
  unconditional and this is not.
  """
  def apply_policy(%__MODULE__{} = c) do
    :ok = place_members(c)

    :eta_net.set_policy(%{
      drop_p: Keyword.get(c.opts, :drop_p, 0.0),
      delay_p: Keyword.get(c.opts, :delay_p, 0.0),
      max_delay: Keyword.get(c.opts, :max_delay, 20)
    })
  end

  @doc "Stops every member and the network, and removes the message hook."
  def stop(%__MODULE__{} = c) do
    for {_name, m} <- c.members do
      Process.unlink(m.sup)

      try do
        Supervisor.stop(m.sup, :shutdown)
      catch
        :exit, _ -> :ok
      end
    end

    # Only the network this cluster started. `eta_run` owns its own, and stopping
    # it here would take the counters a result is about to report with it.
    if c.net == :owned, do: :eta_net.stop()

    :ok
  end

  # ---------------------------------------------------------------------------
  # Membership
  # ---------------------------------------------------------------------------

  @doc "Every member's registry name, in start order."
  def names(%__MODULE__{} = c) do
    c.members |> Map.values() |> Enum.sort_by(& &1.index) |> Enum.map(& &1.name)
  end

  @doc "Names of members whose member process is currently alive."
  def alive(%__MODULE__{} = c) do
    Enum.filter(names(c), &(Process.whereis(&1) != nil))
  end

  @doc """
  This member's own view of the registry, read **without asking it**.

  `dgen_registry:status/1` is a `gen_server:call`, which cannot be served while the
  member is suspended — and it fails silently rather than loudly, catching its own
  timeout and answering `undefined`. An invariant computed over that reports "no
  member believes it leads" and passes, having checked nothing.

  `eta_observe` reads the fields the member publishes on every callback return, so
  this works on a frozen system. That is what lets `Invariants` be used unchanged
  from under `eta_sched`. See `eta_observe`.
  """
  def status(name), do: :eta_observe.read(name)

  @doc "The member id every live member's elector currently agrees leads, or nil if they disagree."
  def leader(%__MODULE__{} = c) do
    case c |> alive() |> Enum.map(&:dgen_registry.get_leader/1) |> Enum.uniq() do
      [one] when one != :undefined -> one
      _ -> nil
    end
  end

  @doc """
  The member that *believes* it leads, by its own `status/1` — which is not
  necessarily the committed leader (§5.1's fenced window).
  """
  def self_believed_leaders(%__MODULE__{} = c) do
    for name <- alive(c),
        s = status(name),
        is_map(s) and s.leader == s.member_id,
        do: {name, s.epoch}
  end

  @doc "A live member that is not the current leader, or nil."
  def a_follower(%__MODULE__{} = c) do
    case leader(c) do
      nil -> c |> alive() |> List.first()
      {_node, lname} -> c |> alive() |> Enum.find(&(&1 != lname))
    end
  end

  # ---------------------------------------------------------------------------
  # Replica inspection
  # ---------------------------------------------------------------------------

  @doc """
  A member's local replica as `%{name => {pid, index, data}}`, read straight from
  the `protected` ETS table the member owns — the same rows `whereis_name/1` reads.
  """
  def replica(name) do
    case :ets.whereis(:dgen_registry.names_table(name)) do
      :undefined ->
        %{}

      tab ->
        tab
        |> :ets.tab2list()
        |> Map.new(fn {n, pid, index, data} -> {n, {pid, index, data}} end)
    end
  rescue
    ArgumentError -> %{}
  end

  @doc "Just the `name => pid` bindings of a member's replica."
  def bindings(name) do
    replica(name) |> Map.new(fn {n, {pid, _, _}} -> {n, pid} end)
  end

  @doc "The commit version a member's replica has applied up to (§4.5)."
  def applied_version(name) do
    case status(name) do
      %{applied_version: v} -> v
      _ -> nil
    end
  end

  @doc "Every live member's replica bindings, as `%{member_name => %{name => pid}}`."
  def all_bindings(%__MODULE__{} = c) do
    Map.new(alive(c), fn name -> {name, bindings(name)} end)
  end

  # ---------------------------------------------------------------------------
  # Faults
  # ---------------------------------------------------------------------------

  @doc """
  Severs the link between two members' nodes, with the events a lost link
  produces.

  Both directions are cut, every simulated peer monitor across the cut fires one
  `{'DOWN', _, process, _, noconnection}`, and each side is told the *other* node
  is gone — `{nodedown, member_2}` on side A, `{nodedown, member_1}` on side B.
  That asymmetry is the point: a partition is two ends each learning about the
  other, and one undifferentiated term would tell both sides the same thing,
  which is never what happened.

  `opts` is `eta_net`'s `event_opts`, so `%{learns: :a}` gives the one-sided form
  — A notices, B does not — which is what two independently timing-out ends
  actually do. The cut stays symmetric either way.

  **Delivering the signal is not optional here.** `dgen_registry` hangs recovery
  off it (`handle_info({nodeup, _})` re-announces the join and re-drives stashed
  and forwarded unregisters, Non-goal 5), so cutting without it injects loss no
  real network produces — messages vanishing while both ends still believe the
  link is up — and the unrecovered state that follows is an artefact rather than
  a defect. `eta_net:cut/2` is still the right tool for the narrower fault of a
  channel that swallows traffic while both ends believe the link is up; that is
  what the resync tests use.
  """
  def partition(%__MODULE__{} = c, a, b, opts \\ %{}) do
    :ok = :eta_net.partition(node_of(c, a), node_of(c, b), Map.put_new(opts, :signal, :nodedown))
    c
  end

  @doc """
  Heals a partition, delivering the `{nodeup, Peer}` a reconnect produces.

  **Resurrects nothing.** A peer monitor that fired `noconnection` is gone, as it
  would be in real Erlang; the member re-establishes it when the peer rejoins.
  """
  def heal_partition(%__MODULE__{} = c, a, b, opts \\ %{}) do
    :ok =
      :eta_net.heal_partition(node_of(c, a), node_of(c, b), Map.put_new(opts, :signal, :nodeup))

    c
  end

  @doc "Partitions one member's node away from every other live member's node."
  def isolate(%__MODULE__{} = c, name, opts \\ %{}) do
    for peer <- alive(c), peer != name, do: partition(c, name, peer, opts)
    c
  end

  @doc """
  Node death: the member, its elector, its connector and its supervisor all die
  at once, and every survivor gets the events that death produces.

  This is the fault `crash/2` cannot express. `crash/2` kills a tree and leaves
  the network none the wiser — peers find out only because the processes are
  gone. Here the peers' monitors are retired *before* anything dies and fire
  `noconnection` rather than `killed`, which is the asymmetry real distribution
  has (a remote watcher sees `noconnection`; something on the same machine sees
  the true reason), and the survivors are then told `{nodedown, member_i}`.

  `eta_net`'s part of the sequence is atomic with respect to the schedule, so no
  survivor can observe a half-dead node.

  The node *name* survives, so `restart/3` on the same member is a restart of the
  node. Cuts involving it do not reset — a node that was partitioned and then
  died comes back partitioned, which is `eta_net`'s rule everywhere: an event
  says what just happened, it does not undo what happened before.

  ## The supervisor

  Three things happen to it, in this order, and each is load-bearing:

  1. **Unlinked** — it is linked to whoever called `start/3`, which under
     `eta_run` is the driver.
  2. **Frozen** — this tree is `one_for_all`, so the child deaths below would
     otherwise restart the very node that just died, as new pids on no node. A
     frozen supervisor cannot act on an exit; the signals queue in a mailbox that
     is never read again. This is what makes the kill *terminal*, and it has to
     happen before `eta_net` kills anything rather than after, because "after" is
     a race the supervisor sometimes wins.
  3. **Killed**, once its children are gone.

  Under `eta_run` step 2 is already done: the supervisor is one of the processes
  `tree_pids/1` declares, so the scheduler is holding it suspended for the whole
  of `execute/2`. Asking `sys:suspend/1` for it there would block forever — a
  suspended process cannot answer a system message — so the call is made only when
  no scheduler is running, which is the real-clock suite.

  Killing the supervisor *first* and letting its links do the work would be
  simpler and would model the wrong thing: a link exit is trappable, so a member
  that traps exits would run `terminate/2` and shut down gracefully. A node that
  died did not shut anything down gracefully.
  """
  def kill_node(%__MODULE__{} = c, name, opts \\ %{}) do
    m = Map.fetch!(c.members, name)
    Process.unlink(m.sup)
    freeze(m.sup)
    :ok = :eta_net.kill_node(node_of(c, name), Map.put_new(opts, :signal, :nodedown))
    Process.exit(m.sup, :kill)
    c
  end

  # See `kill_node/3`. A supervisor that is already gone needs no freezing either.
  defp freeze(sup) do
    if :eta_sched.current() == :undefined, do: :sys.suspend(sup), else: :ok
  catch
    :exit, _ -> :ok
  end

  @doc """
  Kills a member's whole supervision tree — elector, member, and connector — which
  is how a member is lost in reality (`one_for_all`, §4.8). Its ETS replica dies
  with it, exactly as the formal model's `Crash` does.

  Member-level loss, with no node event: see `kill_node/2,3` for the other one.
  """
  def crash(%__MODULE__{} = c, name) do
    m = Map.fetch!(c.members, name)
    Process.unlink(m.sup)
    Process.exit(m.sup, :kill)
    wait_gone(name, 2_000)
    c
  end

  @doc """
  Starts a NEW member into the running cluster — the membership-join path,
  which no other fault or lifecycle helper reaches. Returns `{cluster, name}`.

  On a healthy cluster this exercises the continuing-leader **fast path**
  (`onboard_joiner` / `{peer_joined}` in `dgen_registry_member.erl`): the
  leader snapshots only the joiner and tells existing followers to just
  monitor it — no gather, no re-assume, epoch unchanged. That path is
  explicitly OUT of the TLA+ model's scope (its handoff premise is "the gather
  reaches every live member"), so a live test against the real code is the
  only coverage it has; the joining test asserts the epoch did not move, which
  is what distinguishes the fast path from a full leadership change.

  The joiner is placed like any other member, so from the moment the topology
  knows it its traffic is faultable — a join under loss leans on gap
  detection/resync (and the heartbeat) to finish onboarding when the snapshot
  itself is lost, which is precisely what makes it worth testing.
  """
  def join(%__MODULE__{} = c, ready_timeout \\ 30_000) do
    index = (c.members |> Map.values() |> Enum.map(& &1.index) |> Enum.max()) + 1
    name = :"#{c.keyspace}_m#{index}"

    registry_opts =
      c.opts
      |> Keyword.get(:registry_opts, %{})
      |> Map.put(:keyspace, c.keyspace)

    {:ok, sup} = :dgen_registry.start_link(name, c.tenant, registry_opts)

    # Before the tree can be monitored by a peer, for the reason in `start/3`.
    if Keyword.get(c.opts, :simulate_peer_monitors, false),
      do: :ok = place_tree(%{name: name, sup: sup, index: index})

    case :dgen_registry.await_ready(name, ready_timeout) do
      :ok -> :ok
      other -> raise "joining member #{name} never became ready: #{inspect(other)}"
    end

    c = %{
      c
      | members:
          Map.put(c.members, name, %{
            name: name,
            sup: sup,
            index: index,
            children: capture_children(sup)
          })
    }

    # Same shape as `restart/3`: the new tree must be placed before it can be
    # faulted, and the policy re-applied where this cluster owns the network.
    if c.net == :owned, do: :ok = apply_policy(c), else: :ok = place_members(c)
    {c, name}
  end

  @doc """
  Restarts a crashed member under the same name and keyspace. It comes back
  `fresh` — an empty replica, holding nothing — which is what a restarted member
  is (§5.6: a fresh member provably holds no bindings).

  Also how a node killed by `kill_node/2,3` comes back: the node name survived,
  so re-placing on it is the restart. Its new processes take a fresh position in
  the link-event fan-out, and any cut it died under is still in force.
  """
  def restart(%__MODULE__{} = c, name, ready_timeout \\ 10_000) do
    m = Map.fetch!(c.members, name)

    registry_opts =
      c.opts
      |> Keyword.get(:registry_opts, %{})
      |> Map.put(:keyspace, c.keyspace)

    {:ok, sup} = :dgen_registry.start_link(name, c.tenant, registry_opts)

    # Before the tree can be monitored by a peer, for the reason in `start/3`.
    if Keyword.get(c.opts, :simulate_peer_monitors, false),
      do: :ok = place_tree(%{m | sup: sup})

    case :dgen_registry.await_ready(name, ready_timeout) do
      :ok -> :ok
      other -> raise "restarted member #{name} never became ready: #{inspect(other)}"
    end

    c = %{c | members: Map.put(c.members, name, %{m | sup: sup, children: capture_children(sup)})}
    # The restarted member has a new pid, which has to be placed before it can be
    # faulted — a node cut against the old one still holds, because a partition
    # names a place rather than a process. See `apply_policy/1`.
    if c.net == :owned, do: :ok = apply_policy(c), else: :ok = place_members(c)
    c
  end

  defp wait_gone(name, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      if Process.whereis(name) == nil do
        :gone
      else
        Process.sleep(5)
        :alive
      end
    end)
    |> Enum.find(fn r -> r == :gone or System.monotonic_time(:millisecond) > deadline end)
  end

  # ---------------------------------------------------------------------------
  # Convergence
  # ---------------------------------------------------------------------------

  @doc """
  Heals every fault, drains the network, and waits until all live members agree on
  a leader and hold identical replicas.

  Returns `:ok`, or `{:error, :timeout, diagnostics}` — which is a genuine
  finding, not a flake: with a perfect network and traffic flowing, §4.5's gap
  detection and resync exist precisely so a replica that fell behind catches up.

  ## Why this pumps traffic instead of just waiting

  Gap detection is **traffic-triggered**, not periodic. A follower discovers it
  missed a batch only when a *later* broadcast arrives whose `PrevVersion` does not
  match its applied version (`apply_bcast/6`), or when a forwarded register reply
  arrives ahead of its replica (`handle_register_reply/4`). The `resync_timeout`
  handler only clears the once-per-window guard — it does not re-request.

  So a follower that loses the *tail* of the stream — the last broadcast before the
  workload stopped — has nothing left to reveal the gap, and sits diverged for as
  long as the cluster stays quiescent. Simply waiting would therefore hang forever
  on a divergence the protocol will resolve the instant anything is written, which
  would make this a false positive rather than a finding. Pumping a probe
  registration each iteration supplies exactly the traffic a live cluster has, and
  keeps a timeout here meaning "did not converge *despite* traffic".

  (That quiescent-divergence window is itself worth knowing about, and is written
  up in `test/support/sim/README.md`.)
  """
  def converge(%__MODULE__{} = c, timeout \\ 15_000) do
    :ok = heal_everything(c.net)
    signal_heal(c)

    deadline = System.monotonic_time(:millisecond) + timeout
    do_converge(c, deadline, 0)
  end

  # There is nothing to drain: `eta_net` holds no queues, so a message it delayed
  # is already a deadline in the timer wheel and arrives when the clock reaches it.
  #
  # `heal_all/0` removes every cut and pending targeted drop but deliberately emits
  # nothing — it restores a network, it does not announce one. The signal is
  # `signal_heal/1`'s job.
  defp heal_everything(_net) do
    :ok = :eta_net.heal_all()
    :ok = :eta_net.set_policy(%{drop_p: 0.0, delay_p: 0.0})
  end

  @doc """
  Every live member's pid, which is what the fault scope is expressed in terms of.
  """
  def member_pids(%__MODULE__{} = c) do
    for name <- alive(c), pid = Process.whereis(name), is_pid(pid), do: pid
  end

  # Deliver the `{nodeup, _}` a real reconnect would, to every live node pair.
  #
  # This is the harness paying a debt it owes. Dropping a message models an Erlang
  # link failure, but in reality a link failure is never *just* lost messages — it
  # comes with `nodedown`/`nodeup` on both sides, and the registry deliberately
  # hangs recovery off those signals: `handle_info({nodeup, _})` re-announces the
  # member's join and re-drives unregisters that were stashed or forwarded into the
  # dying link (`redrive_unregs`, Non-goal 5).
  #
  # Without this, the harness injects loss that no real network produces — messages
  # vanishing while both ends still believe the link is up — and then reports the
  # unrecovered state as a defect. It is the same trap as reordering within an
  # ordered pair: a fault the system was never designed to see, yielding a finding
  # that cannot happen in production. Concretely it showed up as a follower that
  # had optimistically deleted a row whose `unregister_req` was dropped, diverging
  # from its peers at the same applied_version, forever.
  #
  # Every pair rather than only the cut ones, because random per-message loss has
  # no pair to name and owes the same debt. `heal_partition/3` on an uncut pair
  # heals nothing and still signals, which is exactly what is wanted.
  #
  # This used to be a hand-written `send(pid, {:nodeup, node()})` to each member.
  # Routing it through `eta_net` buys two things a send cannot: each side is told
  # about *the other* node rather than both being handed one undifferentiated
  # term, and the connector — which is on the node but not on the wire — gets it
  # too, because the event goes to everything located rather than to a list this
  # module maintains by hand.
  defp signal_heal(%__MODULE__{} = c) do
    nodes = for name <- alive(c), do: node_of(c, name)

    for {a, i} <- Enum.with_index(nodes), b <- Enum.drop(nodes, i + 1) do
      :ok = :eta_net.heal_partition(a, b, %{signal: :nodeup})
    end

    :ok
  end

  defp do_converge(c, deadline, iteration) do
    live = alive(c)

    converged? =
      live != [] and
        leader(c) != nil and
        live |> Enum.map(&bindings/1) |> Enum.uniq() |> length() == 1

    cond do
      converged? ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, :timeout,
         %{
           leader: leader(c),
           per_member_leader: Map.new(live, &{&1, :dgen_registry.get_leader(&1)}),
           applied_versions: Map.new(live, &{&1, applied_version(&1)}),
           binding_counts: Map.new(live, &{&1, map_size(bindings(&1))}),
           bindings: all_bindings(c)
         }}

      true ->
        pump(c, live, iteration)
        Process.sleep(25)
        do_converge(c, deadline, iteration + 1)
    end
  end

  # One register + unregister of a throwaway name, which broadcasts to every
  # follower and so gives any gapped replica something to notice its gap against.
  # Kept off the workload's namespace so it can never collide with a tracked name.
  defp pump(_c, [], _iteration), do: :ok

  defp pump(c, [origin | _], iteration) do
    name = {:__sim_pump__, iteration}
    pid = self()

    try do
      case :dgen_registry.register_name({origin, name}, pid) do
        :yes -> :dgen_registry.unregister_name({origin, name})
        _ -> :ok
      end
    catch
      # A pump op is best-effort: it exists to create traffic, and its own failure
      # (no leader reachable yet, mid-handoff) is not the thing being measured.
      :exit, _ -> :ok
    end

    _ = c
    :ok
  end
end
