defmodule DGen.RegistrySimTest do
  @moduledoc """
  Deterministic-ish simulation of the replication protocol (§4.5, §5) against the
  real `dgen_registry` code, driven by `DGen.Sim`.

  A run is: start an N-member cluster in this VM, apply a seeded random workload
  (register / unregister / set_metadata / re-register races) while
  `eta_net` injects seeded message loss and delay, check the always-true
  invariants at checkpoints during and after the workload, then heal, converge,
  and check the converged ones. (Truly *continuous* — per-action — checking
  exists only in the `eta_run` suite, `dgen_registry_eta_test.exs`, where the
  scheduler owns every step; here a checkpoint is as fine-grained as it gets.)

  ## What "deterministic" does and does not mean here

  The **fault schedule** is seeded and reproducible: for a given seed, the same
  decision is made about the same message on the same channel. Process scheduling,
  commit timing, and timer expiry are not under the harness's control, so a failing
  run is reproducible at the fault level rather than bit-for-bit. That is enough to
  re-run a seed and usually see the same failure, and enough to read the trace and
  understand it — it is not enough to guarantee it. Getting the rest requires
  virtualising the backend and the commit worker; see `test/support/sim/README.md`.

  Only inter-member protocol traffic is faulted. The elector's durable
  membership/election queue is untouched, so elections still make progress and the
  cluster cannot wedge on a fault the design never claims to survive — which keeps
  the run scoped to the same protocol the formal model covers.
  """
  use DGen.Case, async: false

  alias DGen.Sim.{Cluster, Invariants}

  # Registrations block rather than answering `no` when no leader is reachable
  # (§3), so under injected faults a caller can sit for the full register_timeout.
  # Shorten it so a faulted run spends its time exercising the protocol rather
  # than waiting out the default 5s.
  @register_timeout 1_000

  setup do
    previous = Application.get_env(:dgen, :register_timeout)
    Application.put_env(:dgen, :register_timeout, @register_timeout)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:dgen, :register_timeout)
        v -> Application.put_env(:dgen, :register_timeout, v)
      end

      # Belt and braces: never leave a network running for another test file.
      :eta_net.stop()
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Workload
  # ---------------------------------------------------------------------------

  defp spawn_live do
    pid = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  # Apply `count` seeded operations, round-robining the originating member so both
  # the leader-direct path (§5.5's replicate-before-ack) and the follower-forward
  # path are exercised. Returns the set of registrations definitively acked `yes`.
  defp run_workload(%Cluster{} = c, count, seed) do
    rand = :rand.seed_s(:exsss, {seed, seed * 7 + 1, seed * 13 + 3})
    names = for i <- 1..10, do: :"n#{i}"
    do_workload(c, count, rand, names, %{})
  end

  defp do_workload(_c, 0, _rand, _names, acked), do: acked

  defp do_workload(c, n, rand, names, acked) do
    live = Cluster.alive(c)

    if live == [] do
      acked
    else
      {roll, rand} = :rand.uniform_s(rand)
      {name_i, rand} = :rand.uniform_s(length(names), rand)
      {member_i, rand} = :rand.uniform_s(length(live), rand)

      name = Enum.at(names, name_i - 1)
      member = Enum.at(live, member_i - 1)

      acked =
        cond do
          roll < 0.55 -> op_register(member, name, acked)
          roll < 0.75 -> op_unregister(member, name, acked)
          true -> op_set_metadata(member, name, acked)
        end

      do_workload(c, n - 1, rand, names, acked)
    end
  end

  defp op_register(member, name, acked) do
    pid = spawn_live()

    # A register may exit on its register_timeout when no leader is reachable
    # (§3 — deliberately not converted to `no`), and under injected faults that is
    # an expected outcome, not a failure. It is also explicitly *undecided*: the
    # binding may or may not have committed, so it is not recorded as acked.
    case safe(fn -> :dgen_registry.register_name({member, name}, pid) end) do
      :yes -> Map.put(acked, name, pid)
      :no -> acked
      {:error, _} -> Map.delete(acked, name)
    end
  end

  defp op_unregister(member, name, acked) do
    case safe(fn -> :dgen_registry.unregister_name({member, name}) end) do
      :ok -> Map.delete(acked, name)
      # Undecided: the removal may or may not have landed.
      {:error, _} -> Map.delete(acked, name)
    end
  end

  defp op_set_metadata(member, name, acked) do
    _ = safe(fn -> :dgen_registry.set_metadata({member, name}, %{index: %{v: 1}}) end)
    acked
  end

  defp safe(fun) do
    fun.()
  catch
    :exit, reason -> {:error, reason}
  end

  # Assert the always-true invariants, attaching the seed and the network trace so
  # a failure is actionable rather than just red.
  #
  defp assert_always!(%Cluster{} = c, seed) do
    case Invariants.check_always(c) do
      :ok ->
        :ok

      {:violation, details} ->
        flunk("""
        Invariant violated during the run (seed #{seed}).

        #{inspect(details, pretty: true, limit: :infinity)}

        Network: #{inspect(:eta_net.stats())}
        """)
    end
  end

  defp assert_converged!(%Cluster{} = c, acked, seed, opts \\ []) do
    case Cluster.converge(c) do
      :ok ->
        :ok

      {:error, :timeout, diag} ->
        flunk("""
        Cluster failed to converge after the network was healed (seed #{seed}).

        Replicas must reconcile once traffic flows again — a member that missed a
        batch detects the gap and resyncs (§4.5). Not converging is a finding.

        #{inspect(diag, pretty: true, limit: :infinity)}
        """)
    end

    case Invariants.check_converged(c, acked, opts) do
      :ok ->
        :ok

      {:violation, details} ->
        flunk("""
        Invariant violated after convergence (seed #{seed}).

        #{inspect(details, pretty: true, limit: :infinity)}

        Network: #{inspect(:eta_net.stats())}
        """)
    end
  end

  # ---------------------------------------------------------------------------
  # The control run — a perfect network. Anything failing here is a plain bug,
  # not an interaction with an injected fault.
  # ---------------------------------------------------------------------------

  describe "perfect network" do
    test "a 3-member cluster keeps every invariant under a mixed workload", %{tenant: tenant} do
      seed = 1
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      acked = run_workload(c, 120, seed)

      # Non-vacuity for `acked_bindings_present`: presence of nothing is free.
      assert map_size(acked) > 0, "the workload acked nothing"

      assert_always!(c, seed)
      assert_converged!(c, acked, seed)

      # With no faults nothing should have been lost on the wire at all.
      counts = :eta_net.stats()
      assert counts.dropped == 0

      assert counts.delivered > 0,
             "no inter-member traffic was routed — is the module built with the transform?"
    end

    test "every member's replica agrees once converged", %{tenant: tenant} do
      seed = 2
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      acked = run_workload(c, 60, seed)
      assert_converged!(c, acked, seed)

      replicas = c |> Cluster.alive() |> Enum.map(&Cluster.bindings/1)
      assert length(Enum.uniq(replicas)) == 1, "replicas disagree: #{inspect(replicas)}"

      # And every binding they agree on is one we were actually told `yes` for.
      [converged | _] = replicas

      for {name, pid} <- converged, Process.alive?(pid) do
        assert Map.get(acked, name) == pid or not Map.has_key?(acked, name),
               "converged on a binding for #{inspect(name)} that was never acked"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Lossy network — the DropMsg action of the formal model, against real code.
  # ---------------------------------------------------------------------------

  describe "lossy network" do
    # One test per seed rather than a loop inside one test, so a failure names the
    # seed that produced it and each seed gets its own time budget. Under loss a
    # forwarded op still blocks for its full caller-side `register_timeout`, so a
    # faulted run costs more wall clock than a clean one.
    #
    # Tagged `:simulation`, which test_helper.exs excludes from a plain `mix test` —
    # these run under `mix dst`, the fault-injection entry point. This sweep is what
    # found the partial-batch divergence. Add seeds freely: the value is roughly
    # linear in how many interleavings get visited.
    for seed <- [11, 12, 13, 14, 15] do
      @seed seed
      @tag :simulation
      @tag timeout: 180_000
      test "invariants hold under seeded message loss and delay (seed #{seed})", %{
        tenant: tenant
      } do
        c = Cluster.start(tenant, 3, seed: @seed, drop_p: 0.15, delay_p: 0.15, max_delay: 40)

        try do
          acked = run_workload(c, 50, @seed)
          assert_always!(c, @seed)
          assert_converged!(c, acked, @seed)

          assert :eta_net.stats().dropped > 0,
                 "seed #{@seed} dropped nothing — the fault policy did not engage"
        after
          Cluster.stop(c)
        end
      end
    end

    test "a member that missed batches resyncs and catches up", %{tenant: tenant} do
      seed = 21
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      leader = Cluster.leader(c)
      {_node, leader_name} = leader
      victim = Enum.find(Cluster.alive(c), &(&1 != leader_name))

      # Cut the leader -> victim direction only: the victim keeps sending but stops
      # hearing, so it misses whole batches and must detect the gap (§4.5).
      :ok = :eta_net.cut(leader_name, victim)

      acked = run_workload(c, 40, seed)

      # It genuinely fell behind.
      assert Cluster.applied_version(victim) < Cluster.applied_version(leader_name),
             "the victim did not fall behind — the cut had no effect"

      :ok = :eta_net.heal(leader_name, victim)
      assert_converged!(c, acked, seed)

      assert Cluster.bindings(victim) == Cluster.bindings(leader_name)
    end
  end

  # ---------------------------------------------------------------------------
  # Crashes — the formal model's Crash action. Single-fault only, which is the
  # bound the guarantee is stated at (§5.4).
  # ---------------------------------------------------------------------------

  describe "member crash" do
    test "the cluster survives losing a follower mid-workload", %{tenant: tenant} do
      seed = 31
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      acked = run_workload(c, 40, seed)
      victim = Cluster.a_follower(c)
      c = Cluster.crash(c, victim)

      acked = Map.merge(acked, run_workload(c, 40, seed + 1))

      assert_always!(c, seed)
      # crashed?: true — Guarantee 4 is single-fault and degrade-open by default,
      # so an acked binding held only by the crashed member may legitimately be gone.
      assert_converged!(c, acked, seed, crashed?: true)

      assert length(Cluster.alive(c)) == 2
    end

    test "the cluster elects a new leader when the leader is lost", %{tenant: tenant} do
      seed = 32
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      acked = run_workload(c, 40, seed)

      {_node, leader_name} = Cluster.leader(c)
      c = Cluster.crash(c, leader_name)

      # A new leader must emerge from the surviving members and serve writes again.
      assert eventually(fn ->
               case Cluster.leader(c) do
                 nil -> false
                 {_n, name} -> name != leader_name and name in Cluster.alive(c)
               end
             end),
             "no new leader was elected after the leader was lost"

      acked = Map.merge(acked, run_workload(c, 40, seed + 1))
      assert_always!(c, seed)
      assert_converged!(c, acked, seed, crashed?: true)
    end

    # Guarantee 4's strong half, asserted against the real code for the first
    # time. Under the default degrade-open policy every crash test above must
    # carve durability out (`crashed?: true`) because a singly-held acked binding
    # may legitimately die with its holder — which left the design's central
    # durability claim checked only by TLC, never against the implementation.
    # `strict_replication` removes the excuse: a `yes` requires a version-visible
    # second holder (§5.5), so every acked registration must survive one fault.
    # Losing the *leader* is the hardest case — survival then rests on the §5.7
    # handoff gather preserving every acked row, which is exactly the property
    # the formal model's HandoffRace mutation shows breaking without its fence.
    test "strict replication: every acked registration survives losing the leader", %{
      tenant: tenant
    } do
      seed = 51
      c = Cluster.start(tenant, 3, seed: seed, registry_opts: %{strict_replication: true})
      on_exit(fn -> Cluster.stop(c) end)

      acked = run_workload(c, 60, seed)

      assert map_size(acked) > 0,
             "the workload acked nothing, so the survival assertion below is vacuous"

      {_node, leader_name} = Cluster.leader(c)
      c = Cluster.crash(c, leader_name)

      assert eventually(fn ->
               case Cluster.leader(c) do
                 nil -> false
                 {_n, name} -> name != leader_name and name in Cluster.alive(c)
               end
             end),
             "no new leader was elected after the leader was lost"

      assert_always!(c, seed)
      assert_converged!(c, acked, seed, crashed?: true)

      # The point: the crash carve-out does not apply in strict mode. Every
      # registration acked `yes` before the crash is still bound after it.
      case Invariants.acked_bindings_present(c, acked) do
        :ok ->
          :ok

        {:violation, details} ->
          flunk("""
          A strict-replication `yes` did not survive a single crash (seed #{seed}) —
          Guarantee 4 broken against the real implementation.

          #{inspect(details, pretty: true, limit: :infinity)}
          """)
      end
    end

    test "a crashed member rejoins fresh and re-syncs the full replica", %{tenant: tenant} do
      seed = 33
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      acked = run_workload(c, 40, seed)
      victim = Cluster.a_follower(c)

      c = Cluster.crash(c, victim)
      acked = Map.merge(acked, run_workload(c, 20, seed + 1))
      c = Cluster.restart(c, victim)

      assert_converged!(c, acked, seed, crashed?: true)

      # A restarted member comes back holding nothing and is onboarded by the
      # leader's snapshot, so it must end up with exactly the leader's replica.
      {_node, leader_name} = Cluster.leader(c)
      assert Cluster.bindings(victim) == Cluster.bindings(leader_name)
      assert length(Cluster.alive(c)) == 3
    end
  end

  # ---------------------------------------------------------------------------
  # Node faults — a lost *link*, not a lost message.
  #
  # `cut/2` above is the narrow fault of a channel that swallows traffic while both
  # ends still believe the link is up. `partition/4` is the one real distribution
  # actually produces: the connection goes down, both ends are told, and every
  # monitor across it fires. The registry hangs recovery off exactly those events,
  # so these are the tests of that recovery.
  #
  # One thing is *not* exercised here and is in `dgen_registry_eta_test.exs`
  # instead: this suite runs `eta_net` with no scheduler, and a simulated peer
  # monitor learns of an ordinary exit from `eta_sched`. Placing before the cluster
  # forms — which is what makes a peer monitor severable — would therefore break
  # `crash/2` detection above. So a partition here delivers the node signals and
  # the message loss, and the `noconnection` DOWNs belong to the deterministic
  # suite. See `Cluster.start/3`'s `:simulate_peer_monitors`.
  # ---------------------------------------------------------------------------

  describe "node faults" do
    test "a partitioned member rejoins and reconverges when the link heals", %{tenant: tenant} do
      seed = 81
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      {_node, leader} = Cluster.leader(c)
      victim = Enum.find(Cluster.alive(c), &(&1 != leader))

      acked = run_workload(c, 20, seed)

      before = :eta_net.stats().signalled
      Cluster.partition(c, leader, victim)

      assert :eta_net.stats().signalled > before,
             "the partition told nobody a node had gone — every process on both " <>
               "sides is unplaced, so the topology is not what this test thinks"

      # The victim is cut off in both directions, so the writes below cannot reach
      # it and it must fall behind.
      acked = Map.merge(acked, run_workload(c, 30, seed + 1))

      assert eventually(fn ->
               Cluster.applied_version(victim) < Cluster.applied_version(leader)
             end),
             "the victim did not fall behind — the partition had no effect"

      Cluster.heal_partition(c, leader, victim)
      # crashed?: true although nothing died: a partition can depose the leader and
      # force a handoff gather that cannot reach the freshest member, and the
      # bounded retry then assumes *degraded* — the availability tradeoff §5.7
      # scopes out — after which an acked binding may be legitimately gone. The
      # same scoping gates the harness's `check_final/2` (see `ever_cut` there).
      assert_converged!(c, acked, seed, crashed?: true)

      {_node, converged_leader} = Cluster.leader(c)
      assert Cluster.bindings(victim) == Cluster.bindings(converged_leader)
    end

    test "a fully isolated member rejoins from the minority side", %{tenant: tenant} do
      # The classic shape: one member cut off from every peer at once, while the
      # remaining two carry on. Its elector is `attach`ed rather than `place`d, so
      # it still reaches the durable queue — which is what a real partition looks
      # like when the store is reachable from both sides, and what keeps the run
      # scoped to the protocol rather than wedging on an election that can never
      # complete.
      seed = 85
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      acked = run_workload(c, 20, seed)
      victim = Cluster.a_follower(c)

      Cluster.isolate(c, victim)
      acked = Map.merge(acked, run_workload(c, 30, seed + 1))

      assert_always!(c, seed)
      assert_converged!(c, acked, seed, crashed?: true)
    end

    test "only one side notices, and the cluster still converges", %{tenant: tenant} do
      # `learns: :a` is the asymmetry real distribution produces constantly: the
      # two ends time out independently, so one can find out well before the
      # other. The cut itself stays symmetric — a lost link loses both directions
      # whether or not anyone has realised — which is exactly the state that used
      # to need hand-rolling from two `cut/2` calls.
      seed = 86
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      {_node, leader} = Cluster.leader(c)
      victim = Enum.find(Cluster.alive(c), &(&1 != leader))

      acked = run_workload(c, 20, seed)
      Cluster.partition(c, leader, victim, %{learns: :a})

      acked = Map.merge(acked, run_workload(c, 30, seed + 1))
      assert_always!(c, seed)
      assert_converged!(c, acked, seed, crashed?: true)
    end

    test "the cluster survives losing a whole node", %{tenant: tenant} do
      # `crash/2`'s node-level counterpart. The tree dies atomically, the survivors
      # are told `{nodedown, member_i}`, and the node's name stays behind so it can
      # be restarted onto.
      seed = 82
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      acked = run_workload(c, 30, seed)
      victim = Cluster.a_follower(c)

      before = :eta_net.stats().signalled
      c = Cluster.kill_node(c, victim)

      assert Process.whereis(victim) == nil, "the member survived its node"
      assert :eta_net.stats().signalled > before, "no survivor was told the node had gone"
      assert length(Cluster.alive(c)) == 2

      acked = Map.merge(acked, run_workload(c, 30, seed + 1))
      assert_always!(c, seed)
      # crashed?: true — a node kill is a fault, and Guarantee 4 is degrade-open at
      # one, exactly as for `crash/2`.
      assert_converged!(c, acked, seed, crashed?: true)
    end

    test "a killed node restarts onto the same name and re-syncs", %{tenant: tenant} do
      seed = 83
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      acked = run_workload(c, 30, seed)
      victim = Cluster.a_follower(c)

      c = Cluster.kill_node(c, victim)
      acked = Map.merge(acked, run_workload(c, 20, seed + 1))
      c = Cluster.restart(c, victim)

      assert_converged!(c, acked, seed, crashed?: true)

      {_node, converged_leader} = Cluster.leader(c)
      assert Cluster.bindings(victim) == Cluster.bindings(converged_leader)
      assert length(Cluster.alive(c)) == 3
    end

    test "losing the leader's node elects a new leader", %{tenant: tenant} do
      seed = 84
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      acked = run_workload(c, 30, seed)
      {_node, leader} = Cluster.leader(c)
      c = Cluster.kill_node(c, leader)

      assert eventually(fn ->
               case Cluster.leader(c) do
                 nil -> false
                 {_n, name} -> name != leader and name in Cluster.alive(c)
               end
             end),
             "no new leader was elected after the leader's node was lost"

      acked = Map.merge(acked, run_workload(c, 30, seed + 1))
      assert_always!(c, seed)
      assert_converged!(c, acked, seed, crashed?: true)
    end
  end

  # ---------------------------------------------------------------------------
  # Regression: a batch is atomic on the wire.
  #
  # A batch ships as one `{names_batch, Ops, …}` message, so losing it always leaves
  # a version discontinuity that the existing resync repairs. When it was one
  # message per name a member could hold a strict subset while reporting the
  # batch's version — undetectable, because gap detection compares versions and the
  # versions matched. Finding 1 in README.md has the mechanism; these tests pin the
  # fix from both directions.
  # ---------------------------------------------------------------------------

  describe "batch atomicity" do
    @tag timeout: 120_000
    test "dropping a batch leaves a detectable gap that resyncs, not a silent hole", %{
      tenant: tenant
    } do
      seed = 51
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      {_node, leader} = Cluster.leader(c)
      [follower_a, follower_b] = Enum.reject(Cluster.alive(c), &(&1 == leader))

      # Coalesce N registrations into a single group commit: with the leader
      # suspended every call queues in its mailbox, and on resume the first op
      # starts a commit while the rest accumulate into the next batch.
      names = for i <- 1..10, do: :"batched_#{i}"
      :sys.suspend(leader)

      tasks =
        for name <- names do
          pid = spawn_live()
          Task.async(fn -> :dgen_registry.register_name({leader, name}, pid) end)
        end

      Process.sleep(200)

      # Drop the batch carrying those registrations on the way to follower_a. Skip 1
      # so the leader's own first single-op commit lands and only the *second*,
      # multi-op batch is lost — the exact shape that used to be undetectable.
      :ok = :eta_net.drop_matching(leader, follower_a, :names_batch, 1, 1)

      :sys.resume(leader)
      for t <- tasks, do: Task.await(t, 10_000)

      Process.sleep(200)

      # The core property: a member that lost a batch must NOT be sitting at the same
      # version as one that received it. Either it is genuinely behind (gap refused,
      # awaiting resync) or it has already resynced and agrees. What it must never be
      # is "same version, different content".
      assert Invariants.same_version_same_replica(c) == :ok,
             "a dropped batch left two members at one version with different replicas"

      # And it repairs itself: gap detection fires and the resync snapshot lands.
      assert eventually(
               fn -> Cluster.bindings(follower_a) == Cluster.bindings(follower_b) end,
               15_000
             ),
             """
             follower_a never caught up after losing a batch.
             a=#{inspect(Cluster.applied_version(follower_a))} \
             b=#{inspect(Cluster.applied_version(follower_b))}
             """

      assert map_size(Cluster.bindings(follower_a)) >= length(names)
    end

    @tag timeout: 120_000
    test "a follower that loses the tail of the stream still converges when quiescent", %{
      tenant: tenant
    } do
      # The second finding: gap detection is traffic-triggered, so losing the *last*
      # batch before writes stop left nothing to reveal the gap and the follower sat
      # diverged indefinitely. The leader's replication heartbeat now advertises its
      # applied version periodically, which is itself a gap-detection trigger.
      seed = 52
      c = Cluster.start(tenant, 3, seed: seed)
      on_exit(fn -> Cluster.stop(c) end)

      {_node, leader} = Cluster.leader(c)
      [follower_a, follower_b] = Enum.reject(Cluster.alive(c), &(&1 == leader))

      pid = spawn_live()
      :yes = :dgen_registry.register_name({leader, :before_tail_loss}, pid)
      assert eventually(fn -> Map.has_key?(Cluster.bindings(follower_a), :before_tail_loss) end)

      # Lose exactly the next batch to follower_a, then stop writing entirely.
      :ok = :eta_net.drop_matching(leader, follower_a, :names_batch, 0, 1)
      :ok = :dgen_registry.unregister_name({leader, :before_tail_loss})

      assert eventually(fn ->
               not Map.has_key?(Cluster.bindings(follower_b), :before_tail_loss)
             end),
             "follower_b never saw the unregister — the drop hit the wrong channel"

      # No further writes. Without the heartbeat this is where follower_a would keep
      # the released name forever; convergence here must come from the heartbeat
      # alone, so allow a few of its intervals.
      assert eventually(
               fn -> not Map.has_key?(Cluster.bindings(follower_a), :before_tail_loss) end,
               30_000
             ),
             """
             follower_a still holds a name whose unregister it missed, with no traffic
             to reveal the gap — the replication heartbeat did not drive a resync.
             """

      assert Invariants.same_version_same_replica(c) == :ok
    end

    @tag timeout: 120_000
    test "a forwarded `yes` does not resurrect a binding its own batch removed", %{
      tenant: tenant
    } do
      # Found by `eta_run` on a perfect network — no injected fault, 9 of 200 seeds.
      #
      # A group commit may bind and clear one name in the same batch, which is an
      # ordinary serialisation: the broadcast carries both ops in order and every
      # replica agrees. The *forwarding* follower did not. Its `{register_reply, …}`
      # arrives behind the broadcast (FIFO) and re-inserted the row, version-guarded
      # so it fired precisely once the batch that removed the name had been applied
      # — leaving a binding no one else held, at the same applied_version as
      # everyone else, which is the one shape gap detection cannot see.
      c = Cluster.start(tenant, 3, seed: 71)
      on_exit(fn -> Cluster.stop(c) end)

      {_node, leader} = Cluster.leader(c)
      [forwarder, other] = Enum.reject(Cluster.alive(c), &(&1 == leader))

      pid = spawn_live()
      pacer = spawn_live()

      # The register and the unregister have to ride *one* batch. Suspended, the
      # leader's first queued op takes the commit slot on its own and the rest
      # accumulate into the next batch — so a pacer goes first (the same device
      # Guarantee 13's test uses).
      :sys.suspend(leader)

      pacer_task =
        Task.async(fn -> :dgen_registry.register_name({leader, :resurrect_pacer}, pacer) end)

      Process.sleep(50)
      reg = Task.async(fn -> :dgen_registry.register_name({forwarder, :resurrect_me}, pid) end)

      # After the register, so the leader plans them in that order. Sent from the
      # other follower, which does not hold the name — an unregister of an unbound
      # name still clears it durably and still broadcasts.
      Process.sleep(50)
      unreg = Task.async(fn -> :dgen_registry.unregister_name({other, :resurrect_me}) end)

      Process.sleep(50)
      :sys.resume(leader)

      assert :yes == Task.await(pacer_task, 10_000)
      assert :yes == Task.await(reg, 10_000)
      assert :ok == Task.await(unreg, 10_000)

      Process.sleep(200)

      # Non-vacuity: the two ops must actually have coalesced. If the register
      # committed in a batch of its own, the unregister's batch removes the row on
      # every member and there is nothing here to resurrect.
      versions = Enum.map(Cluster.alive(c), &Cluster.applied_version/1)

      assert length(Enum.uniq(versions)) == 1,
             "members did not settle at one version (#{inspect(versions)}); the batching " <>
               "this test depends on did not happen"

      assert Invariants.same_version_same_replica(c) == :ok,
             """
             a member holds a different replica at the same version.
             bindings: #{inspect(Cluster.all_bindings(c), pretty: true)}
             """

      # And the survivor is the batch's own final word: the name is unbound.
      for m <- Cluster.alive(c) do
        refute Map.has_key?(Cluster.bindings(m), :resurrect_me),
               "#{m} still holds a name its batch unregistered"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Guarantee 13 on the *replicated* path.
  #
  # "query/2 and query_consistent/2 never observe a half-applied group-commit
  # batch." The single-node test in dgen_registry_guarantees_test.exs covers the
  # leader, where a batch is applied inside one handle_info and no query can
  # interleave. A follower was a different story: it received a batch as N separate
  # casts, so it applied it across N handle_cast invocations, and a query message
  # landing between two of them was answered against a half-applied batch. Nothing
  # covered that, and Guarantee 13 does not exempt followers.
  #
  # Batching the broadcast closes it as a side effect: a follower now applies the
  # whole batch in one handle_cast, exactly as the leader does.
  # ---------------------------------------------------------------------------

  describe "batch-consistent queries on a follower (Guarantee 13)" do
    @tag timeout: 120_000
    test "a follower's query never observes a partially-applied batch", %{tenant: tenant} do
      c = Cluster.start(tenant, 3, seed: 61)
      on_exit(fn -> Cluster.stop(c) end)

      {_node, leader} = Cluster.leader(c)
      follower = Cluster.a_follower(c)

      n = 150
      names = for i <- 1..n, do: :"fq_#{i}"

      for name <- names do
        pid = spawn_live()
        :yes = :dgen_registry.register_name({leader, name}, pid, %{index: %{gen: 1}})
      end

      assert eventually(fn -> length(:dgen_registry.query(follower, %{gen: 1})) == n end),
             "the follower never replicated the initial registrations"

      # Coalesce the flip into one batch on the leader, as in the single-node test:
      # suspended, the first op takes the commit slot and the rest ride the next batch.
      pacer = spawn_live()
      :yes = :dgen_registry.register_name({leader, :fq_pacer}, pacer, %{index: %{pace: 1}})

      # Watch what the follower is actually handed. Guarantee 13 is about a batch
      # being applied whole; it says nothing about where the leader puts the batch
      # boundary, and this test cannot control that. Asserting a hardcoded "0 or n"
      # conflated the two — when the pacer did not take the commit slot cleanly and
      # one flip rode its batch, a perfectly whole 1-then-149 split was reported as
      # 2916 torn reads. Deriving the legal counts from the batches the follower
      # received puts the assertion back on the guarantee.
      collector = spawn_link(fn -> collect_batches([], 0) end)
      follower_pid = Process.whereis(follower)
      # No cleanup hook: the cluster is linked to this process, so the traced member
      # dies with the test and takes the trace with it.
      :erlang.trace(follower_pid, true, [:receive, {:tracer, collector}])

      :sys.suspend(leader)

      pacer_task =
        Task.async(fn ->
          :dgen_registry.set_metadata({leader, :fq_pacer}, %{index: %{pace: 2}})
        end)

      flips =
        for name <- names do
          Task.async(fn -> :dgen_registry.set_metadata({leader, name}, %{index: %{gen: 2}}) end)
        end

      Process.sleep(200)
      :sys.resume(leader)

      # Sample the FOLLOWER while the batch replicates to it.
      parent = self()

      spawn_link(fn ->
        samples = sample_follower(follower, n, System.monotonic_time(:millisecond) + 5_000, [])
        send(parent, {:samples, samples})
      end)

      assert :ok == Task.await(pacer_task, 10_000)
      for t <- flips, do: assert(:ok == Task.await(t, 10_000))

      assert_receive {:samples, samples}, 10_000

      {deltas, snapshots} = stop_tracing_and_collect(follower_pid, collector)

      # A resync would re-baseline the replica wholesale, so the batch stream would
      # no longer account for what a query can see. It should not happen here — no
      # faults are injected — and if it did, the counts below would be meaningless.
      assert snapshots == 0,
             "the follower was re-baselined by #{snapshots} snapshot(s); the batch " <>
               "stream no longer explains what its queries could observe"

      # Non-vacuity, in three parts. The sampler must straddle the transition, and
      # the flips must actually have coalesced — 150 batches of one would satisfy
      # the boundary assertion trivially while testing no atomicity at all.
      assert 0 in samples, "the sampler never observed the follower's pre-batch state"
      assert n in samples, "the sampler never observed the follower's post-batch state"

      biggest = Enum.max(deltas, fn -> 0 end)

      assert biggest >= div(n, 2),
             "the leader never coalesced the flips: the largest batch carried " <>
               "#{biggest} of #{n} ops, so nothing here exercised batch atomicity"

      # The counts a whole-batch-consistent follower may show are exactly the
      # prefix sums of the batches it was handed. Anything else is a query answered
      # from inside a batch.
      boundaries = [0 | Enum.scan(deltas, 0, &+/2)] |> Enum.uniq()
      torn = Enum.reject(samples, &(&1 in boundaries))

      assert torn == [],
             "the follower's query observed #{length(torn)} half-applied batch state(s); " <>
               "counts were #{inspect(Enum.uniq(torn))}, but the batches it received " <>
               "(#{inspect(deltas)}) permit only #{inspect(boundaries)}"

      assert eventually(fn -> length(:dgen_registry.query(follower, %{gen: 2})) == n end)
    end
  end

  # Collects, from `:receive` trace events, how many `gen: 2` matches each
  # replication batch handed the follower — i.e. by how much a whole-batch apply
  # moves the count the sampler is reading.
  defp collect_batches(deltas, snapshots) do
    receive do
      {:trace, _pid, :receive, {:"$gen_cast", {:names_batch, ops, _ep, _prev, _v, _leader}}} ->
        collect_batches([batch_delta(ops) | deltas], snapshots)

      {:trace, _pid, :receive, {:"$gen_cast", {:apply_names_snapshot, _, _, _, _, _, _}}} ->
        collect_batches(deltas, snapshots + 1)

      {:trace, _pid, :receive, _msg} ->
        collect_batches(deltas, snapshots)

      {:deltas, from} ->
        send(from, {:deltas, Enum.reverse(deltas), snapshots})
    end
  end

  # The collector is spawn_link'd, so this must not raise on an op shape it did not
  # expect — an unregistered name carries no index at all.
  defp batch_delta(ops) do
    Enum.count(ops, fn
      {:metadata_set, _name, index, _data} -> gen2?(index)
      {:name_registered, _name, _pid, index, _data} -> gen2?(index)
      _ -> false
    end)
  end

  defp gen2?(index), do: is_map(index) and Map.get(index, :gen) == 2

  # `trace_delivered/1` is the only way to know the tracer has every event: trace
  # messages are in flight independently of ordinary sends, so asking the collector
  # without it races the last batch it is supposed to have recorded.
  defp stop_tracing_and_collect(traced, collector) do
    :erlang.trace(traced, false, [:receive])
    ref = :erlang.trace_delivered(traced)

    receive do
      {:trace_delivered, ^traced, ^ref} -> :ok
    after
      5_000 -> flunk("trace events were never delivered")
    end

    send(collector, {:deltas, self()})

    receive do
      {:deltas, deltas, snapshots} -> {deltas, snapshots}
    after
      5_000 -> flunk("the batch collector never answered")
    end
  end

  # Count `gen: 2` matches on `member` until the batch has fully landed or time runs out.
  defp sample_follower(member, n, deadline, acc) do
    if System.monotonic_time(:millisecond) >= deadline do
      Enum.reverse(acc)
    else
      count =
        case :dgen_registry.query(member, %{gen: 2}) do
          matches when is_list(matches) -> length(matches)
          _ -> 0
        end

      if count == n and length(acc) > 2 do
        Enum.reverse([count | acc])
      else
        sample_follower(member, n, deadline, [count | acc])
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Scale — the same properties with more members and a longer workload.
  # ---------------------------------------------------------------------------

  describe "five members" do
    test "invariants hold under loss with a larger member set", %{tenant: tenant} do
      seed = 41
      c = Cluster.start(tenant, 5, seed: seed, drop_p: 0.1, delay_p: 0.1)
      on_exit(fn -> Cluster.stop(c) end)

      acked = run_workload(c, 100, seed)
      assert_always!(c, seed)
      assert_converged!(c, acked, seed)

      assert length(Cluster.alive(c)) == 5

      # The same non-vacuity guard every other lossy test carries: a run whose
      # policy never engaged exercised nothing and must not report `ok`.
      assert :eta_net.stats().dropped > 0,
             "seed #{seed} dropped nothing — the fault policy did not engage"
    end
  end

  # ---------------------------------------------------------------------------

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
