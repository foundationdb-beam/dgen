defmodule DGenServer.CacheTest do
  use DGen.Case, async: true

  alias DGen.DCounter

  defp kill(pid) do
    mref = Process.monitor(pid)
    DGenServer.kill(pid, :normal)

    receive do
      {:DOWN, ^mref, :process, ^pid, :normal} -> :ok
    end
  end

  describe "state cache" do
    test "cache enabled by default: sequential casts are correct", context do
      tenant = context[:tenant]
      {:ok, pid} = DCounter.start_link(tenant, {"cache_seq"})

      for i <- 1..50 do
        DCounter.incr(pid)
        assert ^i = DCounter.get(pid)
      end

      kill(pid)
    end

    test "cache populated on init with existing state", context do
      tenant = context[:tenant]

      # Start a counter, increment it, then stop (but don't kill)
      {:ok, pid1} = DCounter.start_link(tenant, {"cache_init"})
      DCounter.incr(pid1)
      DCounter.incr(pid1)
      assert 2 = DCounter.get(pid1)

      # Stop gracefully so state persists
      GenServer.stop(pid1)

      # Start a new process for the same tuid — cache should be populated from
      # the existing backend state on init
      {:ok, pid2} = DCounter.start_link(tenant, {"cache_init"})
      assert 2 = DCounter.get(pid2)

      # Further mutations should work correctly
      DCounter.incr(pid2)
      assert 3 = DCounter.get(pid2)

      kill(pid2)
    end

    test "cache disabled: operations still correct", context do
      tenant = context[:tenant]
      {:ok, pid} = DCounter.start_link_opts(tenant, {"cache_off"}, cache: false)

      assert 0 = DCounter.get(pid)

      for i <- 1..20 do
        DCounter.incr(pid)
        assert ^i = DCounter.get(pid)
      end

      kill(pid)
    end

    test "cache disabled: init with existing state", context do
      tenant = context[:tenant]

      {:ok, pid1} = DCounter.start_link_opts(tenant, {"cache_off_init"}, cache: false)
      DCounter.incr(pid1)
      assert 1 = DCounter.get(pid1)
      GenServer.stop(pid1)

      {:ok, pid2} = DCounter.start_link_opts(tenant, {"cache_off_init"}, cache: false)
      assert 1 = DCounter.get(pid2)
      DCounter.incr(pid2)
      assert 2 = DCounter.get(pid2)

      kill(pid2)
    end

    test "two consumers see each other's writes via version invalidation", context do
      tenant = context[:tenant]

      {:ok, pid1} = DCounter.start_link(tenant, {"cache_2con"})
      {:ok, pid2} = DCounter.start_link(tenant, {"cache_2con"})

      assert 0 = DCounter.get(pid1)
      assert 0 = DCounter.get(pid2)

      DCounter.incr(pid1)
      # pid1 should see its own write via cache
      assert 1 = DCounter.get(pid1)
      # pid2's cache has a stale version, so it should re-read from backend
      assert 1 = DCounter.get(pid2)

      DCounter.incr(pid2)
      assert 2 = DCounter.get(pid2)
      assert 2 = DCounter.get(pid1)

      kill(pid1)
    end

    test "cache survives many no-change reads without version drift", context do
      tenant = context[:tenant]
      {:ok, pid} = DCounter.start_link(tenant, {"cache_nochange"})

      DCounter.incr(pid)

      # Many reads with no intervening writes — cache should be hit every time
      for _ <- 1..20 do
        assert 1 = DCounter.get(pid)
      end

      kill(pid)
    end

    test "state change after no-change reads", context do
      tenant = context[:tenant]
      {:ok, pid} = DCounter.start_link(tenant, {"cache_change_after"})

      DCounter.incr(pid)
      assert 1 = DCounter.get(pid)
      assert 1 = DCounter.get(pid)

      # Now mutate
      DCounter.incr(pid)
      assert 2 = DCounter.get(pid)

      kill(pid)
    end

    test "kill clears cache and state", context do
      tenant = context[:tenant]
      {:ok, pid1} = DCounter.start_link(tenant, {"cache_kill"})
      DCounter.incr(pid1)
      assert 1 = DCounter.get(pid1)
      kill(pid1)

      # After kill, state is gone. New process starts fresh.
      {:ok, pid2} = DCounter.start_link(tenant, {"cache_kill"})
      assert 0 = DCounter.get(pid2)
      kill(pid2)
    end

    test "reset option re-initialises state and cache", context do
      tenant = context[:tenant]

      {:ok, pid1} = DCounter.start_link(tenant, {"cache_reset"})
      DCounter.incr(pid1)
      DCounter.incr(pid1)
      assert 2 = DCounter.get(pid1)
      GenServer.stop(pid1)

      # Re-open with reset: true
      {:ok, pid2} = DCounter.start_link_opts(tenant, {"cache_reset"}, reset: true)
      assert 0 = DCounter.get(pid2)

      # Mutations work after reset
      DCounter.incr(pid2)
      assert 1 = DCounter.get(pid2)

      kill(pid2)
    end
  end
end
