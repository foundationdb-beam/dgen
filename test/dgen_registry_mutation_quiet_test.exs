defmodule DGen.RegistryMutationQuietTest do
  @moduledoc """
  The second planted mutation: the quiet-resync gap (sim README, finding 2).

  `-DMUTATION_QUIET_RESYNC` reverts the replication heartbeat — the leader's
  timer fires and advertises nothing — so gap detection is traffic-triggered
  again, and a follower that loses the *tail* of the broadcast stream stays
  diverged for as long as the cluster is quiet.

  This defect's signature is the opposite shape from `partial_batch`'s, and it
  needs the opposite kind of test. The eta sweeps cannot see it structurally:
  `same_version_same_replica` deliberately compares only members standing at the
  leader's version, and this defect's victim is *behind* — behind is exactly what
  the property excuses, because a behind follower is owed a resync. What has
  gone wrong here is that nothing will ever deliver one. So the discrimination is
  targeted and temporal: reproduce finding 2's exact setup and assert the
  divergence PERSISTS through several heartbeat intervals (on a clean build the
  heartbeat heals it within one or two, and this test fails — which is the
  defect-absent half of the discrimination, held up by the regression test in
  `dgen_registry_sim_test.exs` that asserts the same setup converges). A positive
  control then proves the run was measuring the heartbeat's absence and not a
  broken cluster: one fresh write reveals the gap and the follower converges.

  ## Running it

      DGEN_MUTATION=quiet_resync mix compile --force
      DGEN_MUTATION=quiet_resync mix test --only mutation_quiet

  `--force` is not optional; see `erlc_options/1` in mix.exs. Excluded by
  default, because a normal build does not contain the defect and the
  persistence assertion would be a false failure.
  """
  use DGen.Case, async: false

  @moduletag :mutation_quiet
  @moduletag timeout: 120_000

  alias DGen.Sim.{Cluster, Invariants}

  # ?REPLICA_HEARTBEAT_INTERVAL in dgen_registry_member.erl. The persistence
  # window below must span several of these.
  @heartbeat_ms 5_000

  setup %{tenant: tenant} do
    defines =
      :dgen_registry_member.module_info(:compile)
      |> Keyword.get(:options, [])
      |> Enum.filter(&match?({:d, _}, &1))

    unless {:d, :MUTATION_QUIET_RESYNC} in defines do
      flunk("""
      the mutation is not in this build, so nothing here tests anything.

          DGEN_MUTATION=quiet_resync mix compile --force

      (`--force` matters: Mix does not reliably rebuild an Erlang module when only
      its compiler options change. Defines found: #{inspect(defines)})
      """)
    end

    {:ok, tenant: tenant}
  end

  test "without the heartbeat, a tail loss stays diverged until traffic returns", %{
    tenant: tenant
  } do
    seed = 52
    c = Cluster.start(tenant, 3, seed: seed)
    on_exit(fn -> Cluster.stop(c) end)

    {_node, leader} = Cluster.leader(c)
    [follower_a, follower_b] = Enum.reject(Cluster.alive(c), &(&1 == leader))

    pid = spawn(fn -> Process.sleep(:infinity) end)
    :yes = :dgen_registry.register_name({leader, :before_tail_loss}, pid)
    assert eventually(fn -> Map.has_key?(Cluster.bindings(follower_a), :before_tail_loss) end)

    # Lose exactly the next batch to follower_a, then stop writing entirely —
    # finding 2's setup, verbatim.
    :ok = :eta_net.drop_matching(leader, follower_a, :names_batch, 0, 1)
    :ok = :dgen_registry.unregister_name({leader, :before_tail_loss})

    assert eventually(fn ->
             not Map.has_key?(Cluster.bindings(follower_b), :before_tail_loss)
           end),
           "follower_b never saw the unregister — the drop hit the wrong channel"

    # The defect's signature: with the heartbeat reverted, nothing reveals the
    # gap, so the stale binding must SURVIVE several heartbeat intervals of
    # quiet. On a clean build the heartbeat converges this within one or two
    # intervals and the refute below fails — defect absent, test flunks, which
    # is what discrimination means.
    refute eventually(
             fn -> not Map.has_key?(Cluster.bindings(follower_a), :before_tail_loss) end,
             3 * @heartbeat_ms + 1_000
           ),
           """
           follower_a converged with no traffic and no heartbeat — either the
           mutation is not actually in this build's hot code, or something else
           now reveals quiescent gaps and finding 2's fix is redundant.
           """

    # Positive control: the persistence above is only meaningful if resync still
    # works when something DOES reveal the gap. One fresh write is that reveal.
    pid2 = spawn(fn -> Process.sleep(:infinity) end)
    :yes = :dgen_registry.register_name({leader, :fresh_traffic}, pid2)

    assert eventually(
             fn -> not Map.has_key?(Cluster.bindings(follower_a), :before_tail_loss) end,
             15_000
           ),
           "traffic did not heal the gap either — the cluster is broken, not quiet"

    assert Invariants.same_version_same_replica(c) == :ok
  end

  defp eventually(fun, timeout \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(25)
        do_eventually(fun, deadline)
      else
        false
      end
    end
  end
end
