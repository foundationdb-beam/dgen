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
  # **`stray_timers` is excluded, and only it.** This system produces strays: a
  # leadership handoff collects peer snapshots in a bare-spawned process holding a
  # `receive ... after`, and `Cluster.start/3` performs one while the cluster forms,
  # before there is a scheduler to adopt anything. Whether a given startup does so
  # is a real-scheduler race, so the count is nonzero on about half these runs.
  #
  # It is no longer a determinism problem — the driver steps over such a deadline
  # rather than advancing to it, so a schedule is a function of its seed either way
  # — but it is still a true statement about this system, and the exclusion should
  # eventually go. Removing the strays needs `dgen_registry`'s startup handoff to
  # stop spawning a collector before the scheduler exists. Three approaches were
  # measured and none worked: serialising startup, draining the clock in `init/2`,
  # and gating on in-flight gathers.
  #
  # Every other check stays fatal.
  defp assert_deterministic(result, context) do
    suspect =
      case @run.audit(result) do
        :ok -> []
        {:suspect, items} -> Keyword.delete(items, :stray_timers)
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
