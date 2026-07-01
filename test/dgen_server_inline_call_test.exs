defmodule DGen.Server.InlineCallTest do
  use DGen.Case, async: true

  alias DGen.DCounter
  alias DGen.ActionEcho

  defp kill(pid) do
    mref = Process.monitor(pid)
    DGen.Server.kill(pid, :normal)

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

  # With consume_k > 1, inlining is disabled: every call rides the durable queue and
  # the batched consume loop. Calls and casts must still behave correctly via that
  # path (the inline fast path above must not be the only correct route).
  describe "inlining disabled (consume_k > 1)" do
    test "calls and casts are served correctly through the queue", context do
      tenant = context[:tenant]
      {:ok, pid} = DCounter.start_link_opts(tenant, {"noinline_basic"}, consume_k: 4)

      assert 0 = DCounter.get(pid)

      for i <- 1..10 do
        DCounter.incr(pid)
        assert ^i = DCounter.get(pid)
      end

      kill(pid)
    end

    test "a batch of casts coalesces and the final state is correct", context do
      tenant = context[:tenant]
      {:ok, pid} = DCounter.start_link_opts(tenant, {"noinline_batch"}, consume_k: 4)

      for _ <- 1..20, do: DCounter.incr(pid, 1)

      # The trailing get is queued behind the 20 casts (FIFO), so by the time it is
      # served the consume loop has drained them all — in batches of up to 4.
      assert 20 = DCounter.get(pid)

      kill(pid)
    end
  end
end
