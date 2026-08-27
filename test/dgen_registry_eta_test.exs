defmodule DGen.RegistryEtaTest do
  @moduledoc """
  `dgen_registry` driven by `eta_run`.

  The claim being tested is not "the registry works"; the existing sim harness
  covers that. It is that the registry's simulation is now *an implementation of
  the framework's behaviour*, with the framework — not the harness — owning the
  scheduler, the clock, the workload and the invariant checking.
  """
  use DGen.Case, async: false

  # FoundationDB transactions expire on the real clock, so a member suspended
  # mid-transaction dies of `tooslow`. Running the registry under the scheduler
  # requires the deterministic backend — which is what Phase 2 was for.
  @moduletag :mem_only

  @run :eta_run
  @harness DGen.Sim.RegistryHarness

  defp run(seed, opts \\ %{}, tenant) do
    @run.run(
      @harness,
      @harness.run_opts(
        Map.merge(
          %{seed: seed, max_ops: 12, max_steps: 30_000, config: %{tenant: tenant, members: 3}},
          opts
        )
      )
    )
  end

  # `run/3` takes `opts` whole, so a caller wanting one config key would otherwise
  # have to restate the tenant and the member count and could silently drop either.
  defp run_faulted(seed, tenant, drop_p, max_ops) do
    run(
      seed,
      %{max_ops: max_ops, config: %{tenant: tenant, members: 3, drop_p: drop_p}},
      tenant
    )
  end

  # Every property in this file rests on the run having been deterministic, and
  # `audit/1` is the framework's statement of whether it was: no module loaded
  # mid-run, no process that ran before the scheduler owned it, no scheduler
  # timeout, no stray timer. Each is a piece of the interleaving decided by wall
  # clock rather than by the seed.
  #
  # **Every check is fatal.** `stray_timers` used to be excluded, on the grounds
  # that a leadership handoff spawns a snapshot collector before the scheduler
  # exists and the count was therefore nonzero on about half of these runs. Two
  # things about that turned out to be wrong. The collector's `receive ... after`
  # is not a real-time wait — `eta_transform` rewrites it, like every other timer —
  # and the strays were not inherent either: they came from the leaks listed in
  # `RegistryHarness`'s moduledoc, and disappeared when those were fixed. Measured
  # at zero across 25 seeds on a perfect network and 25 under `drop_p: 0.15`.
  #
  # `allow:` is for the node-fault sweep, which produces strays for a reason that
  # is not a leak; see its own note.
  defp assert_deterministic(result, context, opts \\ []) do
    suspect =
      case @run.audit(result) do
        :ok -> []
        {:suspect, items} -> Keyword.drop(items, Keyword.get(opts, :allow, []))
      end

    assert suspect == [],
           "#{context}: the run was not deterministic — #{inspect(@run.summary(result), pretty: true)}"

    result
  end

  describe "the registry runs under the framework" do
    @tag timeout: 120_000
    test "a seeded run completes with no invariant violation", %{tenant: tenant} do
      r = run(1, tenant) |> assert_deterministic("seed 1")

      assert r.outcome == :ok,
             "the run did not come out clean: #{inspect(@run.summary(r), pretty: true)}"

      # Non-vacuity: the framework must actually have driven the cluster, not
      # merely started it and found nothing to do.
      assert r.ops == 12, "not every operation was injected"
      assert r.steps > 100, "suspiciously few steps: #{r.steps}"
    end

    @tag timeout: 300_000
    test "several seeds, all clean, and each a different schedule", %{tenant: tenant} do
      results =
        for seed <- 1..25, do: run(seed, tenant) |> assert_deterministic("seed #{seed}")

      outcomes = results |> Enum.map(& &1.outcome) |> Enum.uniq()
      assert outcomes == [:ok], "outcomes: #{inspect(outcomes, pretty: true)}"

      traces = Enum.map(results, & &1.trace)

      assert length(Enum.uniq(traces)) == length(traces),
             "different seeds produced the same schedule — nothing is being explored"
    end

    @tag timeout: 300_000
    test "every seed reproduces its own schedule", %{tenant: tenant} do
      # This was a ratio for a long time — 3 of 4, with seed 3 unexplained. It was
      # never about seed 3: the odd one out is always the *first run in a fresh
      # VM*, and reversing the order of these seeds moved the failure with it. The
      # cause was an on-demand code load inside a scheduled process, which blocks
      # it on `code_server` — a process the scheduler does not own.
      #
      # The fix is `preload: [:dgen]`, which `RegistryHarness.run_opts/1` supplies,
      # and `audit/1` above is what would catch it going missing again.
      #
      # Seed 3 stays first in this list deliberately: it is the one that used to
      # fail, and its position is the thing that made it fail.
      seeds = [3, 7, 11, 13]

      not_exact =
        for seed <- seeds,
            traces =
              for(
                _ <- 1..3,
                do:
                  run(seed, tenant) |> assert_deterministic("seed #{seed}") |> Map.fetch!(:trace)
              ),
            length(Enum.uniq(traces)) != 1,
            do: seed

      assert not_exact == [],
             "seeds #{inspect(not_exact)} produced more than one schedule across three runs each"
    end

    @tag timeout: 120_000
    test "instrumentation does not change the schedule", %{tenant: tenant} do
      # `eta_log` is only worth having if a run with collection on is the *same*
      # run as one with it off, or every failure investigated is a different
      # failure from the one being chased. `log: false` suppresses the events while
      # still handing out sequence numbers, so only the recording stops.
      with_log = run(11, tenant)
      without_log = run(11, %{log: false}, tenant)

      assert with_log.trace == without_log.trace,
             "collecting the log changed the schedule, so the log is describing a " <>
               "different run from the one being debugged"
    end
  end

  describe "a wide sweep with a lossy replication stream" do
    # The sweep meant to find something, rather than to prove the plumbing works.
    #
    # Everything above runs on a perfect network, which is the wrong shape for a
    # search: the interesting states are the ones a follower reaches after missing
    # part of the replication stream. `drop_p` is what makes a *partial* batch
    # reachable at all, and the planted `partial_batch` defect needs exactly that.
    # Loss is restricted to `names_batch` because a channel whose recovery needs a
    # node event cannot be asserted against mid-run — see `eta_net:set_policy/1`.
    #
    # `max_ops` matters more than the seed count and is nearly free: a run's length
    # is set by the heartbeats and the settle phase rather than by the workload. It
    # buys reach — the `same_version_same_replica` divergence this sweep exists to
    # catch is unreachable at 12 operations and appears at 1-2% of seeds from 25
    # upwards, so a narrower sweep cannot find it however many seeds it runs.
    #
    # Tagged `:simulation`, so it runs under `mix dst` rather than every `mix test`.
    @sweep 1..250
    @max_ops 40
    @drop_p 0.15

    @tag :simulation
    @tag timeout: 900_000
    test "invariants hold across every seed", %{tenant: tenant} do
      results =
        for seed <- @sweep do
          r = run_faulted(seed, tenant, @drop_p, @max_ops)
          assert_deterministic(r, "seed #{seed} under loss")
          # Sampled per run: both are "most recent run" readings, so they have to
          # be taken before the next run overwrites them.
          {seed, r, @harness.stats()}
        end

      failed =
        for {seed, %{outcome: outcome}, _} <- results, outcome != :ok, do: {seed, outcome}

      assert failed == [],
             "seeds failed under #{@drop_p} loss: #{inspect(failed, pretty: true, limit: 3)}"

      # --- non-vacuity, three ways -------------------------------------------
      #
      # A sweep that runs clean proves nothing on its own. Each of these is a way
      # it could have been green while testing nothing at all.

      # 1. The faults actually fired. `drop_tags` restricts loss to one message
      #    type, so a `drop_p` that never selects one is a perfectly quiet run.
      total_drops = results |> Enum.map(&elem(&1, 2).dropped) |> Enum.sum()
      seeds_with_loss = Enum.count(results, &(elem(&1, 2).dropped > 0))

      assert seeds_with_loss > div(Enum.count(@sweep), 2),
             "only #{seeds_with_loss} of #{Enum.count(@sweep)} seeds dropped anything — " <>
               "the sweep ran on a near-perfect network"

      # 2. The quiescence-gated invariant was reached. `check_quiescent/1` holds
      #    the property that sees a permanent same-version divergence, and it is
      #    only evaluated when nothing is runnable and no client is waiting. A run
      #    that never quiesces passes it by never asking.
      quiescent = results |> Enum.map(&elem(&1, 2).quiescent_checks) |> Enum.sum()

      assert quiescent > 0,
             "the quiescent invariant was never evaluated, so the property that " <>
               "catches a replica divergence was not actually checked"

      # 3. The workload ran to completion rather than stalling under loss.
      assert Enum.all?(results, fn {_, r, _} -> r.ops == @max_ops end),
             "some seeds did not inject every operation"

      late = for {seed, r, _} <- results, r.sched.adopted_late > 0, do: seed

      IO.puts(
        "\n[eta sweep] #{Enum.count(@sweep)} seeds @ drop_p #{@drop_p}: " <>
          "#{total_drops} dropped across #{seeds_with_loss} seeds, " <>
          "#{quiescent} quiescent checks, #{length(late)} seeds adopted late"
      )
    end

    # `adopted_late` counts a process that ran on the real scheduler before
    # `eta_sched` owned it — interleaving decided by wall clock rather than by the
    # seed.
    #
    # Stated on the *lossy* sweep deliberately. The last leak of this kind was a
    # `logger:warning/2` on the degrade-open path, reachable only when replicas
    # cannot ack: it showed on 11 of 100 seeds under loss and none without, so a
    # perfect-network sweep would have passed with the defect present. (`eta` now
    # rewrites logger calls to `eta_logger`, which is what fixed it.)
    @tag :simulation
    @tag timeout: 900_000
    test "nothing is adopted late, which loss is what makes worth asserting", %{tenant: tenant} do
      late =
        for seed <- @sweep,
            r = run_faulted(seed, tenant, @drop_p, @max_ops),
            r.sched.adopted_late > 0,
            do: {seed, r.sched.adopted_late}

      assert late == [],
             "seeds adopted a process late under loss: #{inspect(late)} — something is " <>
               "spawning or blocking outside the schedule again"
    end

    # Where dgen's log output goes under simulation, asserted rather than assumed:
    # `eta_transform` rewrites every `logger` call to `eta_logger`, so a warning
    # becomes an event in the run's log rather than console output, readable next
    # to the scheduler decision that caused it.
    #
    # Non-vacuous in both directions. Degrade-open fires only when replicas cannot
    # ack, so a lossy sweep must produce some and a perfect one must produce none —
    # if the clean side ever produces one, the fault model has stopped meaning what
    # it says.
    @tag :simulation
    @tag timeout: 900_000
    test "a degrade-open warning lands in the run's narrative", %{tenant: tenant} do
      warnings = fn drop_p ->
        for seed <- 1..40 do
          run_faulted(seed, tenant, drop_p, @max_ops)

          for %{what: w} <- :eta_log.profile(),
              is_tuple(w),
              tuple_size(w) == 2,
              elem(w, 0) in [:debug, :info, :notice, :warning, :error],
              do: elem(w, 0)
        end
        |> List.flatten()
      end

      lossy = warnings.(@drop_p)
      clean = warnings.(0.0)

      assert lossy != [],
             "no logger event reached the narrative under loss, so either degrade-open " <>
               "never fired or the logger rewrite is not in this build"

      assert Enum.all?(lossy, &(&1 == :warning)), "unexpected levels: #{inspect(lossy)}"

      assert clean == [],
             "a perfect network logged #{inspect(clean)} — degrade-open should be " <>
               "unreachable without loss"
    end
  end

  describe "node faults" do
    # The message sweep above can lose a replication batch. It cannot lose a
    # *node*, and until `eta_net` grew link events there was no honest way to: a
    # cut channel with nothing announcing it is a state real distribution does not
    # produce, and everything a system fails to recover from afterwards is an
    # artefact that reads exactly like a defect.
    #
    # `fault_p` turns a fraction of the generated operations into node faults —
    # partition, heal, and one kill — each carrying the events the failure
    # produces: `{nodedown, Peer}` to both sides, one `noconnection` DOWN at every
    # peer monitor across the cut, and for a kill the whole tree dying atomically
    # with the survivors told about it.
    #
    # `stray_timers` is allowed here and nowhere else, and it is not a leak. Both
    # node faults leave timers in the wheel that nothing will ever collect: a killed
    # node's periodic timers outlive the processes that armed them, and a call whose
    # callee is behind a cut sits on its (virtual) `register_timeout` — an hour of
    # simulated time the run never reaches. Both are the fault behaving as it should.
    # Every other audit check stays fatal, including here.
    @nodes 1..60
    @fault_p 0.3
    @max_ops 30

    defp run_node_faults(seed, tenant) do
      run(
        seed,
        %{
          max_ops: @max_ops,
          config: %{tenant: tenant, members: 3, fault_p: @fault_p}
        },
        tenant
      )
    end

    @tag :simulation
    @tag timeout: 900_000
    test "invariants hold across every seed", %{tenant: tenant} do
      results =
        for seed <- @nodes do
          r = run_node_faults(seed, tenant)
          assert_deterministic(r, "seed #{seed} under node faults", allow: [:stray_timers])
          {seed, r, @harness.stats()}
        end

      failed = for {seed, %{outcome: o}, _} <- results, o != :ok, do: {seed, o}

      assert failed == [],
             "seeds failed under node faults: #{inspect(failed, pretty: true, limit: 3)}"

      # --- non-vacuity ---------------------------------------------------------
      #
      # `dropped` says nothing here: this sweep's faults are cuts and kills, not a
      # lossy policy, and a cut channel's traffic is counted as dropped only if
      # something tried to use it. The guards that mean something for a node fault
      # are the two counters `eta_net` keeps for link *events*.

      # 1. Something was actually told a node had gone.
      signalled = results |> Enum.map(&elem(&1, 2).signalled) |> Enum.sum()
      seeds_signalled = Enum.count(results, &(elem(&1, 2).signalled > 0))

      assert seeds_signalled > div(Enum.count(@nodes), 2),
             "only #{seeds_signalled} of #{Enum.count(@nodes)} seeds delivered a node " <>
               "signal — `fault_p` is not producing partitions"

      # 2. A monitor was actually severed. This is the one that catches the whole
      #    thing going quietly vacuous: the member's peer monitors are simulated
      #    only if both ends were placed before the monitor was created, so if
      #    `simulate_peer_monitors` ever stops taking effect, every partition here
      #    becomes message loss with a signal attached and this drops to zero —
      #    while every seed still passes.
      noconn = results |> Enum.map(&elem(&1, 2).noconnection) |> Enum.sum()

      assert noconn > 0,
             "no partition severed a peer monitor across #{Enum.count(@nodes)} seeds, so " <>
               "nothing here exercised monitor-driven failure detection"

      # 3. The quiescent invariant was reached, and the workload completed.
      quiescent = results |> Enum.map(&elem(&1, 2).quiescent_checks) |> Enum.sum()
      assert quiescent > 0, "the quiescent invariant was never evaluated"

      # 4. It had something to compare. `reaching_leader/1` drops every member cut
      #    off from the leader, and the property can only see a divergence between
      #    two members standing at the leader's version — so a run partitioned
      #    hard enough satisfies it by having nobody left to disagree with.
      #    Measured at 87% of quiescent checks on this config, so a collapse here
      #    is a real change rather than a threshold set at the edge.
      compared = results |> Enum.map(&elem(&1, 2).compared) |> Enum.sum()

      assert compared > div(quiescent, 2),
             "only #{compared} of #{quiescent} quiescent checks compared two or more " <>
               "members at the leader's version — the property is passing without " <>
               "asserting anything"

      assert Enum.all?(results, fn {_, r, _} -> r.ops == @max_ops end),
             "some seeds did not inject every operation"

      IO.puts(
        "\n[eta node faults] #{Enum.count(@nodes)} seeds @ fault_p #{@fault_p}: " <>
          "#{signalled} signals across #{seeds_signalled} seeds, " <>
          "#{noconn} noconnection DOWNs, #{compared}/#{quiescent} quiescent checks compared"
      )
    end

    @tag :simulation
    @tag timeout: 900_000
    test "every seed reproduces its own schedule", %{tenant: tenant} do
      # The sweep above is worth nothing if the runs producing it were not
      # determined by their seeds, and this is the assertion that says they were.
      #
      # It did not hold when node faults were first switched on — about a fifth of
      # seeds produced more than one schedule — and every cause was a scheduled
      # process *blocking* on something the scheduler does not own, which is the
      # one shape `audit/1` cannot see (it catches a process that ran outside the
      # schedule, not one that waited outside it). `RegistryHarness`'s moduledoc
      # lists all five.
      #
      # Node faults are the right place to assert this rather than the `drop_p`
      # sweep: a cut is absolute where `drop_p` is scoped, so it reaches recovery
      # paths — `peer_joined`, `replicate_sync`, the handoff gather — that a scoped
      # policy never touches, and every one of the five leaks was on one of them.
      not_exact =
        for seed <- 1..20,
            traces =
              for(
                _ <- 1..3,
                do:
                  run_node_faults(seed, tenant)
                  |> assert_deterministic("seed #{seed} under node faults",
                    allow: [:stray_timers]
                  )
                  |> Map.fetch!(:trace)
              ),
            length(Enum.uniq(traces)) != 1,
            do: seed

      assert not_exact == [],
             "seeds #{inspect(not_exact)} produced more than one schedule across three runs " <>
               "each — something in the system is waiting on a process the scheduler does " <>
               "not own, or on the wall clock"
    end

    @tag :simulation
    @tag timeout: 300_000
    test "turning node faults on does not change a run that has none", %{tenant: tenant} do
      # `generate/2` draws the fault entropy unconditionally, so `fault_p` at 0.0
      # has to be the same run as the option not being there — otherwise every
      # seed in the message sweep above silently became a different seed.
      for seed <- [1, 4, 8] do
        without = run(seed, tenant)
        with_zero = run(seed, %{config: %{tenant: tenant, members: 3, fault_p: 0.0}}, tenant)

        assert without.trace == with_zero.trace,
               "seed #{seed}: `fault_p: 0.0` produced a different schedule from no fault_p"
      end
    end
  end

  describe "a recorded run replays" do
    @tag timeout: 300_000
    test "strictly, entry for entry, with nothing skipped", %{tenant: tenant} do
      # Distinct from the ratchet above, which re-runs a *seed*. This follows a
      # recorded *trace*, which is what `eta_shrink` does to every candidate. On a
      # system with timers it only works because letting time pass is itself a
      # trace entry; without those the clock never moves on replay, nothing becomes
      # runnable, and every recorded step is refused.
      r = run(11, tenant)
      assert r.outcome == :ok

      # Non-vacuity: this only tests anything if the run actually waited.
      clocks = Enum.count(r.trace, &match?({:clock, _}, &1))
      assert clocks > 0, "the run never advanced the clock, so nothing here is tested"

      replayed =
        @run.replay(
          @harness,
          r.trace,
          @harness.run_opts(%{seed: 11, config: %{tenant: tenant, members: 3}})
        )

      assert replayed.outcome == :ok,
             "replay did not reproduce the run: #{inspect(@run.summary(replayed), pretty: true)}"

      assert replayed.skipped == 0, "a strict replay must skip nothing"

      assert replayed.trace == r.trace,
             "the replay executed a different schedule from the one it was given"
    end
  end

  describe "virtual time is doing the waiting" do
    @tag timeout: 120_000
    test "the run advances the clock well past its real duration", %{tenant: tenant} do
      t0 = System.monotonic_time(:millisecond)
      r = run(11, tenant)
      real = System.monotonic_time(:millisecond) - t0

      assert r.outcome == :ok

      # The registry's own periodic timers (the replication heartbeat, the prune
      # interval) are on eta_time, so an idle cluster advances the clock rather
      # than sitting there. Simulated time outrunning real time is the phase's
      # whole argument.
      assert r.clock_ms > real,
             "simulated #{r.clock_ms}ms in #{real}ms of real time — the clock is not carrying the wait"
    end
  end
end
