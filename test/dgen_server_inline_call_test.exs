defmodule DGenServer.InlineCallTest do
  use DGen.Case

  alias DGen.DCounter
  alias DGen.ActionEcho

  defp kill(pid) do
    mref = Process.monitor(pid)
    DGenServer.kill(pid, :normal)

    receive do
      {:DOWN, ^mref, :process, ^pid, :normal} -> :ok
    end
  end

  describe "inline call optimization" do
    test "call returns correct value when queue is empty", context do
      tenant = context[:tenant]
      {:ok, pid} = DCounter.start_link(tenant, {"inline_get"})

      assert 0 = DCounter.get(pid)
      assert 0 = DCounter.get(pid)

      DCounter.incr(pid)

      assert 1 = DCounter.get(pid)
      assert 1 = DCounter.get(pid)

      kill(pid)
    end

    test "inline call executes actions", context do
      tenant = context[:tenant]
      {:ok, pid} = ActionEcho.start_link(tenant, {"inline_act"})

      assert 0 = ActionEcho.get_then_take_action(pid, self())
      assert_receive {:action_executed, :call}, 5_000

      assert 0 = ActionEcho.get_then_take_action(pid, self())
      assert_receive {:action_executed, :call}, 5_000

      kill(pid)
    end

    # test "inline call with reply_with_effects", context do
    #  tenant = context[:tenant]
    #  {:ok, pid} = ActionEcho.start_link(tenant, {"inline_effects"})

    #  assert {0, [:call]} = ActionEcho.get_with_action(pid)
    #  assert {0, [:call]} = ActionEcho.get_with_action(pid)

    #  kill(pid)
    # end

    test "interleaved casts and calls maintain correct state", context do
      tenant = context[:tenant]
      {:ok, pid} = DCounter.start_link(tenant, {"inline_interleave"})

      assert 0 = DCounter.get(pid)

      for i <- 1..10 do
        DCounter.incr(pid)
        assert ^i = DCounter.get(pid)
      end

      kill(pid)
    end

    test "large reply via inline path", context do
      tenant = context[:tenant]
      {:ok, pid} = DCounter.start_link(tenant, {"inline_blob"})

      blob = DCounter.get_blob(pid, 250_000)
      assert byte_size(blob) == 250_000
      assert blob == :binary.copy(<<0>>, 250_000)

      kill(pid)
    end
  end
end
