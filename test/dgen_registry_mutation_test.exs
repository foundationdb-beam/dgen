defmodule DGen.RegistryMutationTest do
  @moduledoc """
  Acceptance criteria 1–3 for the eta framework, as a test.

  The framework is judged on whether it can find a bug we already understand, in
  code we know contains it, from a cold start and with no test written for the bug
  itself. This file plants the bug and asks; it says nothing about what the defect
  is, and the invariant it trips (`same_version_same_replica`) is the spec's, not
  one written for the occasion.

  ## The bug

  `-DMUTATION_PARTIAL_BATCH` reverts the `names_batch` commit's fix: the leader
  broadcasts one message per op rather than one per batch, all stamped with the
  same `{PrevVersion, Version}`, and `apply_bcast/6` re-admits "another message of
  the batch we are already applying". A member receiving a *strict subset* of a
  batch then reports the full version while holding part of it — permanent, because
  gap detection compares versions and the versions match.

  ## Running it

      DGEN_MUTATION=partial_batch mix compile --force
      DGEN_MUTATION=partial_batch DGEN_BACKEND=dgen_mem mix test --only mutation

  `--force` is not optional; see `erlc_options/1` in mix.exs. Excluded by default,
  because a normal build does not contain the defect and every test here would be a
  false failure.
  """
  use DGen.Case, async: false

  # Driven by eta_sched, so it needs the deterministic backend.
  @moduletag :mem_only
  @moduletag :mutation
  @moduletag timeout: 600_000

  @run :eta_run
  @shrink :eta_shrink
  @harness DGen.Sim.RegistryHarness

  # The workload the criteria are measured against: a 3-member cluster whose
  # replication stream drops one broadcast in five and delays some of the rest.
  #
  # Loss is what makes a *partial* batch reachable at all — with a perfect network
  # every op of a batch arrives and the mutation stays latent. `delay_p` matters
  # for a second reason: a delayed message becomes a deadline the driver chooses
  # when to advance to, which is interleaving surface an immediate delivery does
  # not have. At three operations over seeds 1..200 it takes the defect from 1
  # reachable seed to 4, and costs nothing in wall clock.
  defp opts(seed, tenant, extra \\ %{}) do
    @harness.run_opts(
      Map.merge(
        %{
          seed: seed,
          max_ops: 12,
          max_steps: 60_000,
          config: %{tenant: tenant, members: 3, drop_p: 0.2, delay_p: 0.3, max_delay: 5}
        },
        extra
      )
    )
  end

  setup %{tenant: tenant} do
    defines =
      :dgen_registry_member.module_info(:compile)
      |> Keyword.get(:options, [])
      |> Enum.filter(&match?({:d, _}, &1))

    unless {:d, :MUTATION_PARTIAL_BATCH} in defines do
      flunk("""
      the mutation is not in this build, so nothing here tests anything.

          DGEN_MUTATION=partial_batch mix compile --force

      (`--force` matters: Mix does not reliably rebuild an Erlang module when only
      its compiler options change. Defines found: #{inspect(defines)})
      """)
    end

    {:ok, tenant: tenant}
  end

  describe "criterion 1: rediscover the bug" do
    test "a bounded seed budget finds a same-version replica divergence", %{tenant: tenant} do
      budget = 25

      found =
        Enum.find_value(1..budget, fn seed ->
          case @run.run(@harness, opts(seed, tenant)) do
            %{outcome: {:violation, detail}} -> {seed, detail}
            _ -> nil
          end
        end)

      assert found,
             "#{budget} seeds produced no violation; the framework did not re-find a bug " <>
               "that is definitely present"

      {seed, detail} = found

      assert detail.property == :same_version_same_replica,
             "found a violation, but of #{inspect(detail.property)} — not the divergence " <>
               "the mutation produces (seed #{seed})"

      # The shape of the finding, not just its name: two members at one version
      # holding different maps.
      assert map_size(detail.divergent) >= 1

      for {_version, members} <- detail.divergent do
        assert map_size(members) >= 2
        assert length(Enum.uniq(Map.values(members))) >= 2
      end
    end
  end

  describe "criterion 2: shrink it" do
    test "the failing trace reduces to one a human can read, and it replays", %{tenant: tenant} do
      # A three-operation workload is enough to reach the defect, which is most of
      # what makes the result readable — see the moduledoc note in eta_shrink about
      # positional ids, which is why the *operations* themselves never shrink.
      small = %{max_ops: 3}

      # 1..200 rather than 1..40. The three-operation workload is a narrow window —
      # the defect needs a batch to be partially delivered inside three ops — so the
      # density of reproducing seeds is low, and *which* seeds reproduce is a
      # function of `generate/2`. Adding the node-fault draws to the generator
      # reshuffled that mapping, and the first reproducing seed moved from inside
      # the old range to 119. Nothing about the defect or the framework changed;
      # the range was simply cut too fine to survive a generator change.
      found =
        Enum.find_value(1..200, fn seed ->
          case @run.run(@harness, opts(seed, tenant, small)) do
            %{outcome: {:violation, _}, trace: trace} -> {seed, trace}
            _ -> nil
          end
        end)

      assert found, "no seed under a three-operation workload reproduced the divergence"
      {seed, trace} = found

      result = @shrink.shrink(@harness, trace, opts(seed, tenant, small))

      assert result.verified,
             "the shrunk trace did not survive a strict replay, so it is not a repro"

      assert result.shrunk <= 60,
             "the minimal trace is #{result.shrunk} entries, which is not a schedule anyone " <>
               "will read"

      # Independent of `verified`: replay it here, strictly, and require the same
      # property back with nothing skipped.
      replayed = @run.replay(@harness, result.trace, opts(seed, tenant, small))

      assert {:violation, %{property: :same_version_same_replica}} = replayed.outcome
      assert replayed.skipped == 0, "the minimal trace needed entries skipped to fail"
    end
  end

  describe "the reproduction is pinned to disk" do
    # A seed-pinned regression test is coupled to the workload generator: touch
    # `generate/2` and every pinned seed quietly starts testing something else.
    # This one names a file, and `eta_run:replay/3` never calls `generate/2` — it
    # walks the entries it is given — so the recorded schedule survives generator
    # changes completely. It does still break if the *shape* of an operation
    # changes or if `processes/1` registers in a different order, and it should:
    # those are changes to the contract rather than to the workload.
    #
    # `save_fixture/4` replays strictly before writing anything, so what is on
    # disk has been demonstrated to reproduce rather than believed to.
    #
    # It has been regenerated once, and for exactly the reason above: declaring the
    # supervisors to the scheduler changed the order `processes/1` registers in, so
    # every process id in the recorded trace shifted and the replay diverged at the
    # first step. That is the contract changing, which this fixture is supposed to
    # notice.
    @fixture "test/fixtures/partial_batch.eta"

    test "the recorded divergence still reproduces", %{tenant: tenant} do
      assert File.exists?(@fixture), "the fixture is missing"

      replayed = @harness.replay_fixture(@fixture, tenant)

      assert {:violation, %{property: :same_version_same_replica}} = replayed.outcome,
             "the pinned trace no longer reproduces: " <>
               inspect(@run.summary(replayed), pretty: true)

      assert replayed.skipped == 0,
             "the pinned trace needed entries skipped, so it is not a strict reproduction"
    end

    test "the fixture carries the options the reproduction needs" do
      # `drop_p` is the one that matters. Loss is what makes a *partial* batch
      # reachable at all, so this trace replayed against a perfect network does
      # not reproduce — and a fixture that had dropped the option would fail in a
      # way that looks exactly like the bug having been fixed.
      %{opts: opts, harness: harness} = :eta_run.load_fixture(@fixture)

      assert harness == @harness
      assert opts.config.drop_p == 0.2
      assert opts.config.members == 3

      # Everything a scheduled process might reach has to be preloaded, or an
      # on-demand code load blocks it on `code_server` — see `RegistryHarness`.
      assert opts.preload == [:dgen, :elixir, :logger]
    end
  end

  describe "criterion 3: replay it from the seed alone" do
    test "the same seed produces the same failure and the same schedule", %{tenant: tenant} do
      seed =
        Enum.find(1..25, fn seed ->
          match?(%{outcome: {:violation, _}}, @run.run(@harness, opts(seed, tenant)))
        end)

      assert seed, "no failing seed to replay"

      runs = for _ <- 1..3, do: @run.run(@harness, opts(seed, tenant))

      outcomes = runs |> Enum.map(&elem(&1.outcome, 0)) |> Enum.uniq()

      assert outcomes == [:violation],
             "seed #{seed} did not fail every time: #{inspect(outcomes)}"

      properties = runs |> Enum.map(&elem(&1.outcome, 1).property) |> Enum.uniq()
      assert properties == [:same_version_same_replica]

      traces = Enum.map(runs, & &1.trace)

      assert length(Enum.uniq(traces)) == 1,
             "seed #{seed} produced #{length(Enum.uniq(traces))} different schedules; the seed " <>
               "does not determine the run"
    end
  end

  # The second planted mutation — `-DMUTATION_QUIET_RESYNC`, reverting the
  # replication heartbeat — lives in its own suite,
  # `dgen_registry_mutation_quiet_test.exs` (`--only mutation_quiet`), because its
  # signature is the opposite shape from this one's: the victim follower is
  # *behind* the leader, which is exactly what `same_version_same_replica`
  # excuses, so these sweeping criteria are structurally blind to it and the
  # discrimination has to be targeted and temporal instead. See that module's
  # moduledoc for the reasoning.
end
