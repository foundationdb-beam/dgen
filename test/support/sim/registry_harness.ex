defmodule DGen.Sim.RegistryHarness do
  @moduledoc """
  `dgen_registry` as a `eta_harness` — the adapter `eta_run` drives the cluster
  through.

  The system under test is `dgen_registry` itself, unchanged: a real N-member
  cluster (`DGen.Sim.Cluster`) of the same processes production runs. This module
  is only the adapter around it — it **starts** the cluster, **declares** which of
  its processes to schedule, **drives** it with a workload of registry operations,
  and **judges** it against `DGen.Sim.Invariants`. The framework owns the seed, the
  scheduler, the clock, and the decision of when to inject work versus when to
  leave the system alone.

  ## The fault model

  `config.drop_p` and `config.delay_p` apply to the leader's replication stream
  and nothing else: each `{names_batch, …}` broadcast may be dropped or delayed,
  all other traffic is delivered. `Cluster.apply_policy/1` restricts it further by
  topology — only member-to-member sends can be faulted at all.

  Scoping to that one channel is what lets an invariant be asserted *during* a
  fault rather than only after healing: a follower repairs a lost batch from the
  version discontinuity by itself, with no node event involved.
  """
  @behaviour :eta_harness

  alias DGen.Sim.{Cluster, Invariants}

  # Long enough that a real-clock expiry cannot happen inside a run.
  @register_timeout 3_600_000

  @names for i <- 1..8, do: :"sim_#{i}"

  def run_opts(opts \\ %{}), do: Map.merge(%{preload: [:dgen], net: true}, opts)

  def replay_fixture(path, tenant) do
    %{harness: harness, trace: trace, opts: opts} = :eta_run.load_fixture(path)

    config =
      opts
      |> Map.get(:config, %{})
      |> Map.put(:tenant, tenant)

    :eta_run.replay(harness, trace, Map.put(opts, :config, config))
  end

  @impl true
  def init(seed, config) do
    tenant = Map.fetch!(config, :tenant)
    members = Map.get(config, :members, 3)

    previous = Application.get_env(:dgen, :register_timeout)
    Application.put_env(:dgen, :register_timeout, @register_timeout)

    cluster = Cluster.start(tenant, members, seed: seed)
    :ok = await_quiescent(cluster)

    # After the cluster is up, so no member is partitioned away before it has ever
    # synced (see `Cluster.start/3`).
    :ok =
      :eta_net.set_policy(%{
        drop_p: Map.get(config, :drop_p, 0.0),
        delay_p: Map.get(config, :delay_p, 0.0),
        max_delay: Map.get(config, :max_delay, 5),
        scope: {:tags, [:names_batch]}
      })

    reset_stats()

    {:ok,
     %{
       cluster: cluster,
       clients: [],
       subjects: [],
       next_op: 1,
       labels: build_labels(cluster),
       previous_timeout: previous
     }}
  end

  # Readiness is not quiescence. `eta_harness` requires `init/2` to hand over a
  # system that is not doing anything, because whatever is still running when the
  # driver takes over ran outside the schedule — and `await_ready/2` returns as soon
  # as members report a leader and a sync, which left an elector mid-`$gen_call`
  # about one handover in five and produced two schedules from one seed.
  #
  # Real-time polling is correct here and only here: this runs before the scheduler
  # exists, the one place in a run where wall clock is allowed.
  defp await_quiescent(cluster, attempts \\ 5_000)

  defp await_quiescent(_cluster, 0), do: {:error, :never_quiesced}

  defp await_quiescent(cluster, attempts) do
    children =
      for m <- Map.values(cluster.members),
          {_id, pid, _t, _mods} <- Supervisor.which_children(m.sup),
          is_pid(pid),
          do: pid

    busy =
      for pid <- children,
          {:status, st} = Process.info(pid, :status),
          {:message_queue_len, n} = Process.info(pid, :message_queue_len),
          st != :waiting or n > 0,
          do: pid

    if busy == [] do
      :ok
    else
      Process.sleep(1)
      await_quiescent(cluster, attempts - 1)
    end
  end

  @impl true
  def processes(%{cluster: cluster, clients: clients}) do
    # Ordered by member index, not by map iteration: member names embed a unique
    # integer, so map order is not stable across runs, and `eta_sched` assigns ids
    # in registration order — an unstable order here makes one seed pick different
    # processes.
    tree =
      for m <- Enum.sort_by(Map.values(cluster.members), & &1.index),
          {_id, pid, _type, _mods} <- Supervisor.which_children(m.sup),
          is_pid(pid),
          do: pid

    tree ++ Enum.filter(clients, &Process.alive?/1)
  end

  # Operations name a member by **index**, never by its registered name: member
  # names embed a unique integer and so differ between runs of one seed, which
  # makes a recorded trace incomparable and unreplayable against a fresh cluster.
  #
  # The general rule for an `eta_harness`: an operation must be expressed in terms
  # stable across runs. Pids, refs and generated names are not.
  @impl true
  def generate(%{cluster: cluster}, rand) do
    {roll, rand} = :rand.uniform_s(rand)
    {name_i, rand} = :rand.uniform_s(length(@names), rand)
    {member_i, rand} = :rand.uniform_s(length(Cluster.alive(cluster)), rand)

    name = Enum.at(@names, name_i - 1)

    op =
      cond do
        roll < 0.55 -> {:register, member_i, name}
        roll < 0.75 -> {:unregister, member_i, name}
        true -> {:set_metadata, member_i, name}
      end

    {op, rand}
  end

  # Resolve an index to whichever member holds it now. Out of range (a member has
  # since gone) wraps rather than crashing the run.
  defp member_at(cluster, index) do
    live = Cluster.alive(cluster)
    Enum.at(live, rem(index - 1, length(live)))
  end

  @impl true
  def execute({:register, _member_i, name} = op, sut) do
    subject = spawn(fn -> Process.sleep(:infinity) end)
    client = spawn_client(op, sut, &:dgen_registry.register_name({&1, name}, subject))

    %{
      sut
      | clients: [client | sut.clients],
        subjects: [subject | sut.subjects],
        next_op: sut.next_op + 1
    }
  end

  def execute({:unregister, _member_i, name} = op, sut) do
    client = spawn_client(op, sut, &:dgen_registry.unregister_name({&1, name}))
    %{sut | clients: [client | sut.clients], next_op: sut.next_op + 1}
  end

  def execute({:set_metadata, _member_i, name} = op, sut) do
    client = spawn_client(op, sut, &:dgen_registry.set_metadata({&1, name}, %{index: %{v: 1}}))
    %{sut | clients: [client | sut.clients], next_op: sut.next_op + 1}
  end

  # One operation, in a process of its own. `execute/2` must not block — every
  # scheduled process is suspended, so a synchronous call from the driver into one
  # can never be answered — and `eta_run:spawn_op/1` hands the new process to the
  # scheduler before it runs any real code.
  #
  # Labelled by the operation's ordinal rather than by member or name, for the
  # reason above `generate/2`.
  defp spawn_client({kind, member_i, name}, sut, fun) do
    member = member_at(sut.cluster, member_i)
    n = sut.next_op

    :eta_run.spawn_op(fn ->
      :ok = :eta_log.label({kind, n})
      _ = :eta_log.log({:issued, name})

      # Exits rather than returns when no leader is reachable; either way the
      # client process just ends, and the invariants do not depend on the reply.
      result =
        try do
          fun.(member)
        catch
          :exit, _ -> :timeout
        end

      _ = :eta_log.log({:answered, name, result})
      result
    end)
  end

  @impl true
  def labels(%{labels: labels}), do: labels

  # Every process the cluster starts, by the index and role that are stable across
  # runs of a seed. Built once, while the system is still running.
  defp build_labels(cluster) do
    for m <- Map.values(cluster.members),
        {id, pid, _type, _mods} <- Supervisor.which_children(m.sup),
        is_pid(pid),
        into: %{} do
      {pid, {id, m.index}}
    end
  end

  @impl true
  def check(%{cluster: cluster} = sut) do
    bump(:checks)

    case Invariants.check_always(cluster) do
      :ok -> if quiescent?(sut), do: check_quiescent(cluster), else: :ok
      violation -> violation
    end
  end

  defp check_quiescent(cluster) do
    bump(:quiescent_checks)

    case Invariants.check_quiescent(cluster) do
      :ok ->
        :ok

      {:violation, detail} ->
        {:violation,
         detail
         |> Map.put(:members, member_status(cluster))
         |> Map.put(:net, :eta_net.stats())}
    end
  end

  defp member_status(cluster) do
    Map.new(Cluster.alive(cluster), fn name ->
      {name,
       case Cluster.status(name) do
         %{} = s -> Map.drop(s, [])
         other -> other
       end}
    end)
  end

  @doc """
  Whether the system has stopped moving *and* has nothing outstanding: nothing is
  runnable, and no client operation is still waiting for an answer. This is what
  makes `Invariants.check_quiescent/1` assertable.

  **Both halves are load-bearing.** "Nothing runnable" alone is not enough, because
  this system writes replicas optimistically — `route_unregister/3` deletes a row
  on both the calling member and the leader before anything commits — so replicas
  legitimately differ mid-operation, and a commit parked on a transaction worker
  leaves nothing runnable in the middle of one. The window a speculative write
  lives in is bounded by *its operation*, not by the scheduler going quiet.

  Outside a run there is no scheduler and the answer is `false`, so a
  quiescence-gated property is skipped rather than evaluated against a moving
  system.
  """
  def quiescent?(%{clients: clients}) do
    case :eta_sched.current() do
      :undefined ->
        false

      sched ->
        :eta_sched.runnable(sched) == [] and not Enum.any?(clients, &Process.alive?/1)
    end
  end

  # ---------------------------------------------------------------------------
  # Non-vacuity accounting
  # ---------------------------------------------------------------------------

  @stats :eta_registry_harness_stats

  @doc "Check counts for the most recent run: `%{checks: n, quiescent_checks: n}`."
  def stats do
    case :ets.whereis(@stats) do
      :undefined ->
        %{checks: 0, quiescent_checks: 0, dropped: 0}

      _ ->
        Map.new([:checks, :quiescent_checks, :dropped], &{&1, :ets.lookup_element(@stats, &1, 2)})
    end
  end

  defp reset_stats do
    if :ets.whereis(@stats) == :undefined do
      :ets.new(@stats, [:named_table, :public, :set])
    end

    :ets.insert(@stats, [{:checks, 0}, {:quiescent_checks, 0}, {:dropped, 0}])
  end

  # check/1 runs in a process of eta_run's making, one per check, so the counter
  # cannot live in the harness state — it would be discarded with that process.
  defp bump(key), do: :ets.update_counter(@stats, key, 1)

  defp count_dropped do
    case :eta_net.running() do
      true -> :eta_net.stats().dropped
      false -> 0
    end
  end

  @impl true
  def terminate(sut) do
    Enum.each(sut.clients, &Process.exit(&1, :kill))
    Enum.each(sut.subjects, &Process.exit(&1, :kill))

    # Before `Cluster.stop/1`: read the network's accounting before dismantling
    # what used it.
    :ets.insert(@stats, {:dropped, count_dropped()})

    Cluster.stop(sut.cluster)

    case sut.previous_timeout do
      nil -> Application.delete_env(:dgen, :register_timeout)
      v -> Application.put_env(:dgen, :register_timeout, v)
    end

    :ok
  end
end
