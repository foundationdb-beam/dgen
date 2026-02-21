defmodule DGenServer.StopTest do
  use DGen.Case, async: true

  alias DGen.DStopper

  describe "stop without actions" do
    test "handle_info stop", context do
      tenant = context[:tenant]
      {:ok, pid} = DStopper.start_link(tenant, {"stop_info"})
      mref = Process.monitor(pid)

      send(pid, :stop_me)

      assert_receive {:DOWN, ^mref, :process, ^pid, reason}, 5_000
      assert reason == :normal
    end

    test "queued cast stop", context do
      tenant = context[:tenant]
      {:ok, producer} = DStopper.start_link_opts(tenant, {"stop_cast"}, consume: false)
      {:ok, consumer} = DStopper.start_link(tenant, {"stop_cast"})
      mref = Process.monitor(consumer)

      DStopper.stop_via_cast(producer)

      assert_receive {:DOWN, ^mref, :process, ^consumer, reason}, 5_000
      assert reason == :normal

      GenServer.stop(producer)
    end
  end

  describe "stop with actions" do
    test "handle_info stop executes actions", context do
      tenant = context[:tenant]
      {:ok, pid} = DStopper.start_link(tenant, {"stop_info_action"})
      mref = Process.monitor(pid)

      DStopper.stop_with_action_via_info(pid, self())

      assert_receive {:stop_action_executed, 0}, 5_000
      assert_receive {:DOWN, ^mref, :process, ^pid, :normal}, 5_000
    end

    test "queued cast stop executes actions", context do
      tenant = context[:tenant]
      {:ok, producer} = DStopper.start_link_opts(tenant, {"stop_qcast_action"}, consume: false)
      {:ok, consumer} = DStopper.start_link(tenant, {"stop_qcast_action"})
      mref = Process.monitor(consumer)

      DStopper.stop_with_action_via_cast(producer, self())

      assert_receive {:stop_action_executed, 0}, 5_000
      assert_receive {:DOWN, ^mref, :process, ^consumer, :normal}, 5_000

      GenServer.stop(producer)
    end

    test "priority call stop executes actions", context do
      tenant = context[:tenant]
      {:ok, pid} = DStopper.start_link(tenant, {"stop_pcall_action"})
      mref = Process.monitor(pid)

      try do
        DGenServer.priority_call(pid, {:stop_me_with_action, self()}, 10_000)
      catch
        :exit, _ -> :ok
      end

      assert_receive {:stop_action_executed, 0}, 5_000
      assert_receive {:DOWN, ^mref, :process, ^pid, :normal}, 5_000
    end

    test "inline call stop executes actions", context do
      tenant = context[:tenant]
      {:ok, pid} = DStopper.start_link(tenant, {"stop_icall_action"})
      mref = Process.monitor(pid)

      try do
        DStopper.stop_with_action_via_call(pid, self())
      catch
        :exit, _ -> :ok
      end

      assert_receive {:stop_action_executed, 0}, 5_000
      assert_receive {:DOWN, ^mref, :process, ^pid, :normal}, 5_000
    end

    test "state is persisted before actions run on stop", context do
      tenant = context[:tenant]
      {:ok, pid} = DStopper.start_link(tenant, {"stop_persist_action"})

      # Increment state first so we can verify the action receives updated state
      DGenServer.cast(pid, {:incr, 5})
      Process.sleep(200)
      assert 5 = DStopper.get(pid)

      mref = Process.monitor(pid)
      DStopper.stop_with_action_via_info(pid, self())

      # Action should receive the current state (5)
      assert_receive {:stop_action_executed, 5}, 5_000
      assert_receive {:DOWN, ^mref, :process, ^pid, :normal}, 5_000
    end
  end
end
