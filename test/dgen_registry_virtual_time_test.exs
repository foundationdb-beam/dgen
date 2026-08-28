defmodule DGen.RegistryVirtualTimeTest do
  @moduledoc """
  The remaining Phase 1 exit criterion: `dgen_registry`'s own timers run on the
  virtual clock (see the `eta` library's docs/design.md).

  `dgen_registry_member`, `_connector`, `_elector`, `dgen_server` and `dgen_queue`
  are compiled with `eta_transform` in the test environment, so their
  `erlang:send_after/3` and clock reads land in `:eta_time`. `dgen_registry` itself
  is deliberately *not* transformed — `await_ready/2` polls a deadline with
  `timer:sleep/1`, and against a frozen virtual clock that would spin forever.
  """
  use DGen.Case, async: false

  alias DGen.Sim.Cluster

  @time :eta_time

  setup do
    on_exit(fn ->
      @time.stop()
      :eta_net.stop()
    end)

    :ok
  end

  defp start_reg!(tenant, opts \\ %{}) do
    reg = :"vt_#{:erlang.unique_integer([:positive])}"
    {:ok, sup} = :dgen_registry.start_link(reg, tenant, opts)
    assert :ok == :dgen_registry.await_ready(reg, 10_000)
    on_exit(fn -> stop_sup(sup) end)
    {reg, sup}
  end

  defp stop_sup(sup) do
    Process.unlink(sup)

    try do
      Supervisor.stop(sup, :shutdown)
    catch
      :exit, _ -> :ok
    end
  end

  # ---------------------------------------------------------------------------

  describe "the registry's timers are on the virtual clock" do
    test "a registry started under a virtual clock arms its timers there", %{tenant: tenant} do
      # Clock first, so the member's init/1 timers land in the wheel rather than
      # on the real one.
      :ok = @time.start(%{start_ms: 0})

      {reg, _sup} = start_reg!(tenant)

      assert @time.pending() > 0,
             "the registry armed no virtual timers — is eta_transform enabled in this build?"

      # Frozen clock: nothing fires, however long we wait in real time.
      assert @time.stats().fired == 0
      Process.sleep(100)
      assert @time.stats().fired == 0, "a timer fired without the clock being advanced"

      # Past the replication heartbeat (5s) and the maintenance prune (30s).
      @time.advance(35_000)
      Process.sleep(200)

      assert @time.stats().fired > 0, "advancing the clock fired nothing"

      assert @time.pending() > 0,
             "timers fired but nothing re-armed — periodic timers should reschedule"

      # And the registry is still a working registry throughout.
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)
      assert :yes == :dgen_registry.register_name({reg, :still_works}, pid)
      assert pid == :dgen_registry.whereis_name({reg, :still_works})
    end

    test "registrations do not depend on the clock advancing", %{tenant: tenant} do
      # A frozen clock must not wedge the write path — the commit pipeline is
      # driven by messages, not timers. If this hangs, something on the write path
      # is waiting for a timer that can no longer fire on its own.
      :ok = @time.start(%{start_ms: 0})
      {reg, _sup} = start_reg!(tenant)

      for n <- 1..25 do
        pid = spawn(fn -> Process.sleep(:infinity) end)
        on_exit(fn -> Process.exit(pid, :kill) end)
        assert :yes == :dgen_registry.register_name({reg, :"frozen_#{n}"}, pid)
      end

      assert @time.now_ms() == 0
      assert length(:dgen_registry.get_members(reg)) == 1
    end
  end

  # ---------------------------------------------------------------------------
  # The payoff: a timer-driven recovery path, at no wall-clock cost
  # ---------------------------------------------------------------------------

  describe "the replication heartbeat under virtual time" do
    @tag timeout: 60_000
    test "a gapped follower recovers when the heartbeat fires, in no real time", %{
      tenant: tenant
    } do
      # This is the recovery added for the traffic-triggered-resync finding: a
      # follower that lost the tail of the broadcast stream learns of the gap only
      # from the leader's periodic heartbeat. Testing it on the real clock means
      # waiting out ?REPLICA_HEARTBEAT_INTERVAL; here it costs nothing, which is
      # the entire argument for the phase.
      :ok = :eta_net.start(%{seed: 1})

      # The clock must start *before* the registries. A timer armed while eta_time
      # is inert goes to the real clock and stays there until it fires and re-arms,
      # so starting the clock afterwards would leave the heartbeat — the very timer
      # under test — running on real time, and the run would sit at one fired timer
      # across minutes of simulated time.
      :ok = @time.start(%{start_ms: 0})

      keyspace = :"vt_ks_#{:erlang.unique_integer([:positive])}"
      {a, _} = start_reg!(tenant, %{keyspace: keyspace})
      {b, _} = start_reg!(tenant, %{keyspace: keyspace})

      assert eventually(fn -> :dgen_registry.get_leader(a) == :dgen_registry.get_leader(b) end),
             "the two members never agreed on a leader"

      {_node, leader} = :dgen_registry.get_leader(a)
      follower = Enum.find([a, b], &(&1 != leader))

      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      # Lose exactly the batch carrying this registration, and nothing else. With
      # no further writes there is no later broadcast to reveal the gap.
      :ok = :eta_net.drop_matching(leader, follower, :names_batch, 0, 1)

      # Registering *on the leader* is the direct path (§5.5), so the leader waits
      # for a follower to confirm a replica before answering `yes`, bounded by
      # `replicate_timeout` — itself a virtual timer. The follower cannot confirm,
      # because the batch it would confirm is the one we dropped, so the clock has
      # to be advanced for the registration to resolve at all. The write path's own
      # deadlines are the simulation's now.
      task = Task.async(fn -> :dgen_registry.register_name({leader, :gapped}, pid) end)
      Process.sleep(100)
      @time.advance(2_000)

      assert :yes == Task.await(task, 10_000),
             "the direct registration did not degrade open once replicate_timeout fired"

      assert eventually(fn -> Cluster.bindings(leader)[:gapped] == pid end)

      refute Map.has_key?(Cluster.bindings(follower), :gapped),
             "the follower received the batch — the drop did not take effect"

      # Nothing on the real clock will fix this.
      Process.sleep(200)
      refute Map.has_key?(Cluster.bindings(follower), :gapped)

      # Advance past the heartbeat and let the resync round-trip complete. The
      # measurement is the point: minutes of simulated time, milliseconds of real.
      {micros, _} =
        :timer.tc(fn ->
          Enum.reduce_while(1..40, :not_yet, fn _, _ ->
            @time.advance(6_000)
            Process.sleep(25)

            if Map.has_key?(Cluster.bindings(follower), :gapped),
              do: {:halt, :recovered},
              else: {:cont, :not_yet}
          end)
        end)

      assert Cluster.bindings(follower)[:gapped] == pid,
             """
             the follower never recovered. Simulated #{@time.now_ms()}ms across
             #{@time.stats().fired} fired timers.
             """

      assert micros < 5_000_000,
             "recovery took #{div(micros, 1000)}ms of real time; virtual time should make it near-free"
    end
  end

  defp eventually(fun, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(20)
        do_eventually(fun, deadline)
      else
        false
      end
    end
  end
end
