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

  ## What it does not simulate

  All members share a VM, so they also share a fate and a scheduler. A member
  "crash" here kills its supervision tree, which is a faithful model of losing a
  *member*, but not of losing a *node* — the surviving members never see a
  `nodedown`, and the connector's node-level backstops (§4.6) never fire. Tests
  that need those belong in `dgen_registry_cluster_test.exs` with real peers.
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

  The policy is applied only once every member is ready, or a member can be
  partitioned away before it has ever synced and the run measures startup rather
  than the protocol.

  Under `eta_run` a network already exists, seeded from the run's seed; this call
  leaves its lifetime, seed and policy alone and only declares the topology.
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

    members =
      for i <- 1..n, into: %{} do
        name = :"#{keyspace}_m#{i}"
        {:ok, sup} = :dgen_registry.start_link(name, tenant, registry_opts)
        {name, %{name: name, sup: sup, index: i}}
      end

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
  Puts each member on a simulated node of its own.

  **The placement is the fault model.** `eta_net` faults a send only when both ends
  are on nodes and the nodes differ, so which traffic can be lost follows from the
  topology rather than from a predicate this module has to keep correct.

  **Only the member process is placed**, deliberately. A member's elector and
  connector are on no node, so nothing they send or receive is ever faulted: their
  messages stand in for operations against the durable store, and dropping them
  injects a failure the real system cannot have. Faulting them produces
  `acked_bindings_present` violations that look exactly like the replication defect
  this suite hunts and are artefacts of the harness.

  Everything a member spawns — transaction workers, snapshot collectors — inherits
  its node as `eta_sched` adopts it, so the topology does not go stale the first
  time the system creates a worker.
  """
  def place_members(%__MODULE__{} = c) do
    for m <- Map.values(c.members), pid = Process.whereis(m.name), is_pid(pid) do
      :ok = :eta_net.place(:"member_#{m.index}", [pid])
    end

    :ok
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
  Kills a member's whole supervision tree — elector, member, and connector — which
  is how a member is lost in reality (`one_for_all`, §4.8). Its ETS replica dies
  with it, exactly as the formal model's `Crash` does.
  """
  def crash(%__MODULE__{} = c, name) do
    m = Map.fetch!(c.members, name)
    Process.unlink(m.sup)
    Process.exit(m.sup, :kill)
    wait_gone(name, 2_000)
    c
  end

  @doc """
  Restarts a crashed member under the same name and keyspace. It comes back
  `fresh` — an empty replica, holding nothing — which is what a restarted member
  is (§5.6: a fresh member provably holds no bindings).
  """
  def restart(%__MODULE__{} = c, name, ready_timeout \\ 10_000) do
    m = Map.fetch!(c.members, name)

    registry_opts =
      c.opts
      |> Keyword.get(:registry_opts, %{})
      |> Map.put(:keyspace, c.keyspace)

    {:ok, sup} = :dgen_registry.start_link(name, c.tenant, registry_opts)

    case :dgen_registry.await_ready(name, ready_timeout) do
      :ok -> :ok
      other -> raise "restarted member #{name} never became ready: #{inspect(other)}"
    end

    c = %{c | members: Map.put(c.members, name, %{m | sup: sup})}
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

  # Deliver the `{nodeup, _}` a real reconnect would.
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
  # All simulated members share `node()`, so there is no real distribution event to
  # wait for; sending the message directly is what the cluster tests do to drive the
  # same path.
  # There is nothing to drain: `eta_net` holds no queues, so a message it delayed
  # is already a deadline in the timer wheel and arrives when the clock reaches it.
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

  defp signal_heal(%__MODULE__{} = c) do
    for name <- alive(c), pid = Process.whereis(name), is_pid(pid) do
      send(pid, {:nodeup, node()})
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
