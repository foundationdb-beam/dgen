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

  ## The message fault model

  `config.drop_p` and `config.delay_p` apply to the leader's replication stream
  and nothing else: each `{names_batch, …}` broadcast may be dropped or delayed,
  all other traffic is delivered. `Cluster.place_members/1` restricts it further by
  topology — only member-to-member sends can be faulted at all.

  Scoping to that one channel is what lets an invariant be asserted *during* a
  fault rather than only after healing: a follower repairs a lost batch from the
  version discontinuity by itself, with no node event involved.

  ## The node fault model

  `config.fault_p` (default `0.0`, so **off**) makes a fraction of the generated
  operations node faults rather than registry calls: `partition`, `heal`, and a
  single `kill_node` per run.

  It is opt-in, and separate from `drop_p`, because the two models are not
  interchangeable and mixing them by default would undo something the message
  sweep already paid for. A partition delivers `{nodedown, _}` and severs peer
  monitors, so the system *recovers* — which is the point of injecting it, and
  also exactly what heals the quiescent divergence the `drop_p` sweep exists to
  catch. Keeping them apart lets each sweep assert what it can actually see, and
  keeps the 250-seed replication sweep the fault model it was tuned for.

  ### A node fault run is reproducible, and getting there took seven fixes

  A run under this model is a function of its seed, bit for bit, and
  `dgen_registry_eta_test.exs` asserts it the same way it does for a perfect
  network. That is not what it looked like when node faults were first switched
  on: at `fault_p: 0.3` over 40 seeds, four runs each, roughly a fifth of seeds
  produced more than one schedule.

  None of it showed up in `eta_run:audit/1` — nothing adopted late, no module
  loaded mid-run, no stray timer. That is the thing worth remembering about this
  class of bug: the audit catches a process *running* outside the schedule, and
  every one of these was a process **blocking** on something outside it. What
  found them was diffing two runs of one seed at the step where their runnable
  sets first disagreed, and reading the stack of the process that differed.

  Why node faults and not `drop_p`: `drop_p` here is scoped to `names_batch`,
  while a cut is absolute — `eta_net` does not consult faultability when deciding
  whether a channel is cut. So a partition drops every cross-node message,
  reaching `peer_joined`, `replicate_sync`, `apply_names_snapshot` and the handoff
  gather's replies, and drives recovery paths a scoped policy never touches. The
  leaks were on those paths, and three of the seven are real bugs off the
  simulator too.

  Measure under load. Six of the seven were visible running the suite on an idle
  machine; the last one only showed up with several simulation suites running at
  once, because what it turned on was how much real time a run took.

  1. **`dgen_registry_member:telemetry_available/0`.** `code:ensure_loaded/1`
     short-circuits on a loaded module but not on a missing one, so the first
     telemetry event in a VM was a synchronous call into `code_server` — a process
     the scheduler does not own — from inside a member. Resolved in `init/1` now,
     while the tree is starting. Invisible to the audit, because a *missing*
     module loads nothing.
  2. **`dgen` and `dgen_registry` were not built with the transform.** They run in
     the caller's process, so every `gen_server:call` went into OTP's
     `gen:do_call/4`, taking a wall-clock `receive ... after` and a real monitor
     with it.
  3. **The supervisor was not declared to the scheduler.** `elector_pid/1` asks it
     for its children, so every `get_leader/1`, `get_epoch/1` and `get_members/1`
     blocked a scheduled process on an unscheduled one. See `Cluster.tree_pids/1`.
  4. **`:simulate_peer_monitors` raced the cluster forming.** A monitor is
     simulated only when both ends are already placed as it is created, so which
     peer monitors were simulated came down to real-time luck — and a simulated
     monitor fires `noconnection` where a real one does not fire at all.
     `Cluster.start/3` now holds each member until every tree is placed.
  5. **`eta_sched:do_step/2`** read a process's status and then suspended it as two
     separate acts, and drained the trace only when the process survived the step.
  6. **`eta_net:simulate_monitor/4` asked the VM whether its target was alive.**
     `is_process_alive/1` and `process_info/2` are both signal-based against a live
     target, so they wait for a reply the target produces by *running* — and every
     target there is suspended. `eta_net` reads `eta_sched`'s exit report instead.
  7. **OTP 28 supervisors arm a real timer.** `hibernate_after` arrived with a
     1000ms default, implemented as a `gen_server` timeout, and `supervisor` is OTP
     code no transform reaches — so every supervisor this harness declares dropped
     a `{timeout, _, _}` into a scheduled mailbox on the wall clock. Disabled under
     `DST` only; see `?SUP_SIM_FLAGS` in `dgen_registry`.

  ### One kill per run

  `Guarantee 4` is stated at a single fault (§5.4), and a run is not allowed to
  restart what it killed: `Cluster.restart/3` waits for the new member to sync,
  and `execute/2` may not block. So a kill is terminal for that member and the
  rest of the run proceeds degraded, which is the shape the crash tests in
  `dgen_registry_sim_test.exs` already use.
  """
  @behaviour :eta_harness

  alias DGen.Sim.{Cluster, Invariants}

  # Long enough that a real-clock expiry cannot happen inside a run.
  @register_timeout 3_600_000

  @names for i <- 1..8, do: :"sim_#{i}"

  # `eta_run` preloads `kernel`, `stdlib` and `eta` itself; everything else a
  # scheduled process might reach has to be named. An on-demand code load inside a
  # scheduled process is a synchronous call into `code_server`, which the scheduler
  # does not own, so the interleaving that follows is decided by wall clock.
  #
  # `elixir` and `logger` are here because the *system* reaches them, not the
  # harness: `eta_transform` points dgen's `logger` calls at `eta_logger`, but the
  # OTP code underneath a scheduled process does not go through it — a supervisor
  # report or a crash report loads `Logger.Translator` and the `Inspect` protocol
  # on first use, and those loads happen inside whichever process was stepping.
  @preload [:dgen, :elixir, :logger]

  def run_opts(opts \\ %{}), do: Map.merge(%{preload: @preload, net: true}, opts)

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

    # `simulate_peer_monitors` places each tree as it starts, so the member's peer
    # monitors are created across a declared link and a `partition` can fire
    # `noconnection` at them. It is safe here and only here: a simulated monitor
    # learns of an ordinary exit from `eta_sched`, which this run has.
    cluster = Cluster.start(tenant, members, seed: seed, simulate_peer_monitors: true)
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
       previous_timeout: previous,
       fault_p: Map.get(config, :fault_p, 0.0),
       # Member indices, so a partition is expressed in the same run-stable terms
       # every operation is. Cut pairs are `{lo, hi}`; `killed` never shrinks,
       # since a node kill is terminal for this run.
       cuts: MapSet.new(),
       killed: MapSet.new(),
       # Whether ANY partition ever existed, healed or not. `check_final/2` needs
       # history, not the current cut set: a partition that has since healed can
       # still have cost an acked binding legitimately (a degraded handoff gather
       # is the documented availability tradeoff), and healing does not un-lose it.
       ever_cut: false
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
    # Supervisors included, and read from a snapshot rather than by asking them —
    # both for the reason in `Cluster.tree_pids/1`.
    Cluster.tree_pids(cluster) ++ Enum.filter(clients, &Process.alive?/1)
  end

  # Operations name a member by **index**, never by its registered name: member
  # names embed a unique integer and so differ between runs of one seed, which
  # makes a recorded trace incomparable and unreplayable against a fresh cluster.
  #
  # The general rule for an `eta_harness`: an operation must be expressed in terms
  # stable across runs. Pids, refs and generated names are not.
  @impl true
  def generate(%{cluster: cluster} = sut, rand) do
    {roll, rand} = :rand.uniform_s(rand)
    {name_i, rand} = :rand.uniform_s(length(@names), rand)
    {member_i, rand} = :rand.uniform_s(length(Cluster.alive(cluster)), rand)
    # Drawn unconditionally, so a run with `fault_p` at 0.0 consumes exactly the
    # same entropy as one without the option at all and its schedule is unchanged.
    {fault_roll, rand} = :rand.uniform_s(rand)
    {fault_kind, rand} = :rand.uniform_s(rand)
    {pair_roll, rand} = :rand.uniform_s(length(node_pairs(sut)), rand)

    name = Enum.at(@names, name_i - 1)

    op =
      cond do
        fault_roll < sut.fault_p -> node_fault(sut, fault_kind, pair_roll)
        roll < 0.55 -> {:register, member_i, name}
        roll < 0.75 -> {:unregister, member_i, name}
        true -> {:set_metadata, member_i, name}
      end

    {op, rand}
  end

  # Every unordered pair of member indices, killed nodes included: a pair naming a
  # dead node is still a legal *name*, and refusing it here would make the choice
  # depend on liveness, which differs between a run and its replay of the same
  # trace. `execute/2` is where a fault against a gone node becomes a no-op.
  defp node_pairs(%{cluster: cluster}) do
    indices = cluster.members |> Map.values() |> Enum.map(& &1.index) |> Enum.sort()
    for i <- indices, j <- indices, i < j, do: {i, j}
  end

  # Heal is offered as often as partition so a run explores recovery rather than
  # monotonically shredding the topology, and the single kill sits behind both.
  defp node_fault(sut, kind, pair_roll) do
    {i, j} = Enum.at(node_pairs(sut), pair_roll - 1)

    cond do
      kind < 0.45 -> {:partition, i, j}
      kind < 0.85 -> {:heal, i, j}
      true -> {:kill_node, i}
    end
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

    # The `yes` is recorded from inside the client, at the (scheduled, so
    # deterministic) moment the answer arrives — it is the ack history
    # `check_final/2` folds. Only `yes` is worth recording: a `no` changes no
    # state, and a timeout may or may not have committed, which is exactly why
    # the fold never relies on one.
    client =
      spawn_client(op, sut, fn member ->
        result = :dgen_registry.register_name({member, name}, subject)
        if result == :yes, do: record_event({:yes, name, subject})
        result
      end)

    %{
      sut
      | clients: [client | sut.clients],
        subjects: [subject | sut.subjects],
        next_op: sut.next_op + 1
    }
  end

  def execute({:unregister, _member_i, name} = op, sut) do
    # Recorded at *issue* time, in the driver, deliberately — the earliest point
    # the removal could possibly take effect. An unregister is a zombie: it can
    # commit long after its caller timed out (the removal is stashed and
    # re-driven), so the only sound reading for `check_final/2` is "from here on,
    # this name may legally change hands once more".
    record_event({:unreg_issued, name})
    client = spawn_client(op, sut, &:dgen_registry.unregister_name({&1, name}))
    %{sut | clients: [client | sut.clients], next_op: sut.next_op + 1}
  end

  def execute({:set_metadata, _member_i, name} = op, sut) do
    client = spawn_client(op, sut, &:dgen_registry.set_metadata({&1, name}, %{index: %{v: 1}}))
    %{sut | clients: [client | sut.clients], next_op: sut.next_op + 1}
  end

  # The node faults. Unlike the operations above these are performed *here*, in the
  # driver, rather than issued into a spawned client — they are not something the
  # system does, they are something that happens to it, and `eta_net` runs each as
  # one atomic sequence between steps so no process can observe half of it.
  #
  # Named by node, not by process. A cut names a place: it survives the death of
  # the processes that were behind it, and a member restarted on that node comes
  # back partitioned. That is `eta_net`'s rule, and it is also what a real
  # partition does.
  def execute({:partition, i, j}, sut) do
    Cluster.partition(sut.cluster, member_name(sut, i), member_name(sut, j))
    %{sut | cuts: MapSet.put(sut.cuts, {i, j}), ever_cut: true, next_op: sut.next_op + 1}
  end

  def execute({:heal, i, j}, sut) do
    Cluster.heal_partition(sut.cluster, member_name(sut, i), member_name(sut, j))
    %{sut | cuts: MapSet.delete(sut.cuts, {i, j}), next_op: sut.next_op + 1}
  end

  # One kill per run, and never the last two members. Guarantee 4 is stated at a
  # single fault (§5.4), and the run cannot restart what it killed — `execute/2`
  # may not block, and coming back requires waiting for a sync. A kill that is
  # refused is a no-op rather than an error: `generate/2` must not consult
  # liveness (see `node_pairs/1`), so this is the place that can.
  def execute({:kill_node, i}, sut) do
    sut = %{sut | next_op: sut.next_op + 1}

    if MapSet.size(sut.killed) == 0 and length(Cluster.alive(sut.cluster)) > 2 do
      Cluster.kill_node(sut.cluster, member_name(sut, i))
      %{sut | killed: MapSet.put(sut.killed, i)}
    else
      sut
    end
  end

  defp member_name(%{cluster: cluster}, index) do
    cluster.members |> Map.values() |> Enum.find(&(&1.index == index)) |> Map.fetch!(:name)
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
      :ok -> if quiescent?(sut), do: check_quiescent(sut), else: :ok
      violation -> violation
    end
  end

  defp check_quiescent(%{cluster: cluster} = sut) do
    bump(:quiescent_checks)
    only = reaching_leader(sut)

    # Non-vacuity, counted rather than assumed. `:only` drops every member cut off
    # from the leader, and the property can only see a divergence between two
    # members at the leader's version — so a heavily partitioned run can satisfy it
    # by having nothing left to compare. See `Invariants.compared/2`.
    if Invariants.compared(cluster, only: only) > 1, do: bump(:compared)

    case Invariants.check_quiescent(cluster, only: only) do
      :ok -> :ok
      {:violation, detail} -> {:violation, enrich(detail, sut)}
    end
  end

  # ---------------------------------------------------------------------------
  # Final checks — the properties `check/1` can never assert
  # ---------------------------------------------------------------------------

  # `UniqueBinding` and `acked_bindings_present`, evaluated once, at the end of a
  # normally-ending run. Neither is checkable mid-run (`unique_binding/2` over
  # replicas false-positives on ordinary lag), and before this hook existed
  # neither was evaluated under `eta_run` at all — Guarantee 1 had no
  # deterministic coverage.
  #
  # Both are asserted only on a run with **no kills and no partitions, ever**
  # (`ever_cut`, not the current cut set — healing does not un-lose anything).
  # That scoping is the design's, not caution: under the default degrade-open
  # policy a kill may legitimately drop a singly-held acked binding (Guarantee
  # 4's stated exception), and a partition can force a *degraded* handoff gather
  # — the documented availability tradeoff — after which an acked binding may be
  # gone and its name legally re-issued. Asserting either property there would
  # claim something the design does not.
  #
  # `UniqueBinding` is folded from the recorded **ack history**, not from
  # replicas — the same shape as the spec's cumulative `acked` ghost set, so this
  # is the invariant under its own name rather than a converged-snapshot proxy.
  # The fold is deliberately conservative about concurrency: an issued unregister
  # is a *credit* that excuses one later holder change, because it may commit
  # anywhere in its `[issue, ∞)` window (stash + re-drive). Every real
  # "name freed" consumes one committed unregister, and commits never exceed
  # issues, so observed-changes > credits is a genuine double-`yes`.
  @impl true
  def check_final(_settled, %{cluster: cluster} = sut) do
    bump(:final_checks)
    faulted? = MapSet.size(sut.killed) > 0 or sut.ever_cut

    with false <- faulted?,
         events = recorded_events(),
         :ok <- unique_binding_from_history(events, sut),
         acked = surviving_acked(events),
         _ = if(map_size(acked) > 0, do: bump_by(:final_acked, map_size(acked))),
         :ok <- present_or_violation(cluster, acked, sut) do
      :ok
    else
      true -> :ok
      {:violation, _} = violation -> violation
    end
  end

  defp present_or_violation(cluster, acked, sut) do
    case Invariants.acked_bindings_present(cluster, acked) do
      :ok -> :ok
      {:violation, detail} -> {:violation, enrich(detail, sut)}
    end
  end

  defp unique_binding_from_history(events, sut) do
    initial = {%{}, %{}, []}

    {_, _, violations} =
      Enum.reduce(events, initial, fn
        {:unreg_issued, name}, {holders, credits, v} ->
          {holders, Map.update(credits, name, 1, &(&1 + 1)), v}

        {:yes, name, pid}, {holders, credits, v} ->
          case holders[name] do
            prev when is_pid(prev) and prev != pid ->
              # A holder change. Legal iff some unregister could have freed the
              # name in between — spend a credit; with none left, two live pids
              # were both told `yes` with nothing that could have unbound the
              # first: the spec's UniqueBinding, violated.
              case {Map.get(credits, name, 0), Process.alive?(prev)} do
                {0, true} ->
                  {Map.put(holders, name, pid), credits,
                   [%{name: name, first: prev, second: pid} | v]}

                {0, false} ->
                  # The previous holder is dead by the time this fold runs, so its
                  # death may be what freed the name (`DOWN` cleanup) — not a
                  # double-`yes`. Subjects are immortal in this harness, so a dead
                  # one means something *in the system* killed it; excusing the
                  # change is the sound reading either way.
                  {Map.put(holders, name, pid), credits, v}

                {n, _} ->
                  {Map.put(holders, name, pid), Map.put(credits, name, n - 1), v}
              end

            _ ->
              {Map.put(holders, name, pid), credits, v}
          end
      end)

    case violations do
      [] ->
        :ok

      _ ->
        {:violation,
         enrich(
           %{
             property: :unique_binding,
             detail: "two pids were both acked `yes` for one name with no unregister between",
             conflicts: violations
           },
           sut
         )}
    end
  end

  # The registrations still owed presence at the end of the run: last `yes` per
  # name, minus every name an unregister was *ever issued* for. Issue-time
  # exclusion is the sound reading — a timed-out unregister may still commit
  # (or already have, with its reply lost), so the name is owed nothing.
  defp surviving_acked(events) do
    {acked, unregistered} =
      Enum.reduce(events, {%{}, MapSet.new()}, fn
        {:yes, name, pid}, {acked, unreg} -> {Map.put(acked, name, pid), unreg}
        {:unreg_issued, name}, {acked, unreg} -> {acked, MapSet.put(unreg, name)}
      end)

    Map.drop(acked, MapSet.to_list(unregistered))
  end

  defp enrich(detail, %{cluster: cluster} = sut) do
    detail
    |> Map.put(:members, member_status(cluster))
    |> Map.put(:cuts, MapSet.to_list(sut.cuts))
    |> Map.put(:net, :eta_net.stats())
  end

  @doc """
  The members that can currently reach whichever member believes it leads at the
  newest epoch, as `same_version_same_replica/2`'s `:only`.

  A member cut off from the leader is owed nothing until the cut heals: the batch
  that would reconcile it cannot cross, so it can sit at the leader's version
  holding a write the leader has since superseded, and requiring it to agree
  asserts something the protocol does not claim. A member on the far side of a cut
  that does *not* involve the leader is still being replicated to normally and
  stays in.

  `nil` — meaning every live member — whenever nothing has been cut, so a run
  without node faults gets exactly the comparison it always got.

  Reads `Cluster.status/1`, which is `eta_observe`, not a call: `check/1` runs
  against a frozen system and anything that sends a message and waits either hangs
  or, worse, answers `undefined` and passes while checking nothing.
  """
  def reaching_leader(%{cuts: cuts, cluster: cluster} = sut) do
    if MapSet.size(cuts) == 0 do
      nil
    else
      case leader_index(cluster) do
        nil ->
          nil

        li ->
          for name <- Cluster.alive(cluster),
              i = index_of(sut, name),
              i == li or not MapSet.member?(cuts, {min(i, li), max(i, li)}),
              do: name
      end
    end
  end

  defp leader_index(cluster) do
    cluster
    |> Cluster.alive()
    |> Enum.flat_map(fn name ->
      case Cluster.status(name) do
        %{leader: leader, member_id: id, epoch: e} when leader == id ->
          [{e, Map.fetch!(cluster.members, name).index}]

        _ ->
          []
      end
    end)
    |> case do
      [] -> nil
      believers -> believers |> Enum.max_by(fn {e, _i} -> e end) |> elem(1)
    end
  end

  defp index_of(%{cluster: cluster}, name), do: Map.fetch!(cluster.members, name).index

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

  # The ack history `check_final/2` folds. Written from two places — the driver
  # (unregister issuance, between steps) and register clients (the `yes`, at
  # their own scheduled step) — whose interleaving the scheduler serializes, so
  # the sequence is a function of the seed like everything else. Ordered by an
  # ETS counter rather than by time: virtual time ties, real time lies.
  @events :eta_registry_harness_events

  # `dropped` is the non-vacuity guard for a lossy policy; `noconnection` and
  # `signalled` are the ones for a node fault, and they are not interchangeable. A
  # run can partition a pair and drop nothing (the cut traffic was not
  # `names_batch`), and a run can partition a node no monitor crossed — which
  # exercises no recovery at all and reports the same `ok`.
  #
  # `final_checks`/`final_acked` are `check_final/2`'s: how often it ran, and how
  # many acked registrations it actually demanded presence for. A sweep where
  # `final_acked` stays zero verified UniqueBinding over an empty history and
  # presence of nothing — the sweeps assert it climbs.
  @counters [
    :checks,
    :quiescent_checks,
    :compared,
    :dropped,
    :noconnection,
    :signalled,
    :final_checks,
    :final_acked
  ]

  @doc """
  Counts for the most recent run: how often the invariants were evaluated, and
  what the network actually did.
  """
  def stats do
    case :ets.whereis(@stats) do
      :undefined -> Map.new(@counters, &{&1, 0})
      _ -> Map.new(@counters, &{&1, :ets.lookup_element(@stats, &1, 2)})
    end
  end

  defp reset_stats do
    if :ets.whereis(@stats) == :undefined do
      :ets.new(@stats, [:named_table, :public, :set])
    end

    if :ets.whereis(@events) == :undefined do
      :ets.new(@events, [:named_table, :public, :ordered_set])
    end

    :ets.delete_all_objects(@events)
    :ets.insert(@stats, Enum.map(@counters, &{&1, 0}))
    :ets.insert(@stats, {:event_seq, 0})
  end

  # check/1 runs in a process of eta_run's making, one per check, so the counter
  # cannot live in the harness state — it would be discarded with that process.
  defp bump(key), do: :ets.update_counter(@stats, key, 1)
  defp bump_by(key, n), do: :ets.update_counter(@stats, key, n)

  defp record_event(event) do
    seq = :ets.update_counter(@stats, :event_seq, 1)
    :ets.insert(@events, {seq, event})
    :ok
  end

  defp recorded_events do
    for {_seq, event} <- :ets.tab2list(@events), do: event
  end

  defp net_counters do
    case :eta_net.running() do
      true -> Map.take(:eta_net.stats(), [:dropped, :noconnection, :signalled])
      false -> %{}
    end
  end

  @impl true
  def terminate(sut) do
    Enum.each(sut.clients, &Process.exit(&1, :kill))
    Enum.each(sut.subjects, &Process.exit(&1, :kill))

    # Before `Cluster.stop/1`: read the network's accounting before dismantling
    # what used it.
    :ets.insert(@stats, Enum.map(net_counters(), fn {k, v} -> {k, v} end))

    Cluster.stop(sut.cluster)

    case sut.previous_timeout do
      nil -> Application.delete_env(:dgen, :register_timeout)
      v -> Application.put_env(:dgen, :register_timeout, v)
    end

    :ok
  end
end
