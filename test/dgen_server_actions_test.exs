defmodule DGenServer.ActionsTest do
  use DGen.Case

  alias DGen.ActionEcho

  defp kill(pid) do
    mref = Process.monitor(pid)
    DGenServer.kill(pid, :normal)

    receive do
      {:DOWN, ^mref, :process, ^pid, :normal} -> :ok
    end
  end

  describe "actions" do
    test "handle_cast executes actions after commit", context do
      tenant = context[:tenant]
      {:ok, pid} = ActionEcho.start_link(tenant, {"act_cast"})

      ActionEcho.incr_then_take_action(pid, self())
      assert_receive {:action_executed, :cast}, 5_000

      assert 1 = ActionEcho.get_then_take_action(pid, self())
      assert_receive {:action_executed, :call}, 5_000

      kill(pid)
    end

    test "handle_call executes actions after commit", context do
      tenant = context[:tenant]
      {:ok, pid} = ActionEcho.start_link(tenant, {"act_call"})

      assert 0 = ActionEcho.get_then_take_action(pid, self())
      assert_receive {:action_executed, :call}, 5_000

      kill(pid)
    end

    test "handle_call executes reply and actions after commit when using reply_with_effects",
         context do
      tenant = context[:tenant]
      {:ok, pid} = ActionEcho.start_link(tenant, {"act_call"})

      assert {0, [:call]} = ActionEcho.get_with_action(pid)

      kill(pid)
    end

    test "handle_info executes actions after commit", context do
      tenant = context[:tenant]
      {:ok, pid} = ActionEcho.start_link(tenant, {"act_info"})

      send(pid, {:info_then_take_action, self()})
      assert_receive {:action_executed, :info}, 5_000

      assert 1 = ActionEcho.get_then_take_action(pid, self())
      assert_receive {:action_executed, :call}, 5_000

      kill(pid)
    end

    test "priority_call executes actions", context do
      tenant = context[:tenant]
      {:ok, pid} = ActionEcho.start_link(tenant, {"act_pcall"})

      assert 0 = ActionEcho.priority_get_then_take_action(pid, self())
      assert_receive {:action_executed, :call}, 5_000

      kill(pid)
    end

    test "priority_cast executes actions", context do
      tenant = context[:tenant]
      {:ok, pid} = ActionEcho.start_link(tenant, {"act_pcast"})

      ActionEcho.priority_incr_then_take_action(pid, self())
      assert_receive {:action_executed, :cast}, 5_000

      assert 1 = ActionEcho.priority_get_then_take_action(pid, self())
      assert_receive {:action_executed, :call}, 5_000

      kill(pid)
    end
  end
end
