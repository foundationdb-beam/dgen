defmodule DGenServer.Test do
  use DGen.Case, async: true
  doctest DGenServer

  alias DGen.DCounter

  defp kill(pid) do
    mref = Process.monitor(pid)
    DGenServer.kill(pid, :normal)

    receive do
      {:DOWN, ^mref, :process, ^pid, :normal} -> :ok
    end
  end

  describe "dcounter" do
    test "count from 0 to 1", context do
      tenant = context[:tenant]
      {:ok, pid1} = DCounter.start_link(tenant, {"a"})
      assert 0 = DCounter.get(pid1)

      {:ok, pid2} = DCounter.start_link(tenant, {"a"})
      assert 0 = DCounter.get(pid2)

      DCounter.incr(pid1)

      assert 1 = DCounter.get(pid1)
      assert 1 = DCounter.get(pid2)

      {:ok, pid3} = DCounter.start_link(tenant, {"a"})
      assert 1 = DCounter.get(pid3)

      kill(pid1)
    end

    test "2 counters", context do
      tenant = context[:tenant]
      {:ok, pidx} = DCounter.start_link(tenant, {"x"})
      {:ok, pidy} = DCounter.start_link(tenant, {"y"})

      assert 0 = DCounter.get(pidx)
      assert 0 = DCounter.get(pidy)

      DCounter.incr(pidx)

      assert 1 = DCounter.get(pidx)
      assert 0 = DCounter.get(pidy)

      kill(pidx)
      kill(pidy)
    end

    test "many casts", context do
      tenant = context[:tenant]
      {:ok, pid} = DCounter.start_link(tenant, {"a"})
      assert 0 = DCounter.get(pid)

      for _ <- 1..100 do
        DCounter.incr(pid)
      end

      assert 100 = DCounter.get(pid)

      kill(pid)
    end

    test "large reply spanning multiple chunks", context do
      tenant = context[:tenant]
      {:ok, pid} = DCounter.start_link(tenant, {"a"})

      # 250KB exceeds the 100KB chunk size, forcing a multi-chunk reply
      blob = DCounter.get_blob(pid, 250_000)
      assert byte_size(blob) == 250_000
      assert blob == :binary.copy(<<0>>, 250_000)

      kill(pid)
    end

    test "kill reset", context do
      tenant = context[:tenant]
      {:ok, pid} = DCounter.start_link(tenant, {"a"})
      DCounter.incr(pid)
      assert 1 = DCounter.get(pid)
      kill(pid)
      {:ok, pid} = DCounter.start_link(tenant, {"a"})
      assert 0 = DCounter.get(pid)
      kill(pid)
    end
  end
end
