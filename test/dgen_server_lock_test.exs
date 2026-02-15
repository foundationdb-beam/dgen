defmodule DGenServer.LockTest do
  use DGen.Case, async: true

  alias DGen.DLocker

  defp kill(pid) do
    mref = Process.monitor(pid)
    DGenServer.kill(pid, :normal)

    receive do
      {:DOWN, ^mref, :process, ^pid, :normal} -> :ok
    end
  end

  describe "lock via cast" do
    test "lock_incr acquires lock, runs handle_locked, commits result", context do
      tenant = context[:tenant]
      {:ok, pid} = DLocker.start_link(tenant, {"lock_cast"})

      assert 0 = DLocker.get(pid)

      DLocker.lock_incr(pid, self())

      assert_receive {:locked_entered, ^pid}, 5_000

      # Tell handle_locked to proceed
      send(pid, :continue)

      assert_receive {:locked_exiting, ^pid}, 5_000

      # Give the post-lock transaction a moment to commit
      Process.sleep(100)

      # handle_locked added 100
      assert 100 = DLocker.get(pid)

      kill(pid)
    end

    test "casts queue behind a lock and are processed after unlock", context do
      tenant = context[:tenant]
      {:ok, pid} = DLocker.start_link(tenant, {"lock_cast_queue"})

      DLocker.lock_incr(pid, self())
      assert_receive {:locked_entered, ^pid}, 5_000

      # These casts should be queued while locked
      DLocker.incr(pid)
      DLocker.incr(pid)
      DLocker.incr(pid)

      send(pid, :continue)
      assert_receive {:locked_exiting, ^pid}, 5_000

      # Wait for lock to release and queued casts to be consumed
      Process.sleep(500)

      # 100 from handle_locked + 3 from queued incrs
      assert 103 = DLocker.get(pid)

      kill(pid)
    end
  end

  describe "lock via call" do
    test "lock_get acquires lock, runs handle_locked, replies", context do
      tenant = context[:tenant]
      {:ok, pid} = DLocker.start_link(tenant, {"lock_call"})

      assert 0 = DLocker.get(pid)

      # Start the lock_get call in a separate process since it blocks
      caller = self()

      task =
        Task.async(fn ->
          result = DLocker.lock_get(pid, caller)
          send(caller, {:call_result, result})
          result
        end)

      assert_receive {:locked_entered, ^pid}, 5_000

      send(pid, :continue)
      assert_receive {:locked_exiting, ^pid}, 5_000

      # The call should have returned the pre-lock state (0)
      assert_receive {:call_result, 0}, 5_000
      Task.await(task)

      # State is now 100 after handle_locked committed
      assert 100 = DLocker.get(pid)

      kill(pid)
    end
  end

  describe "lock via priority_call" do
    test "priority_lock_get acquires lock and replies", context do
      tenant = context[:tenant]
      {:ok, pid} = DLocker.start_link(tenant, {"lock_pcall"})

      assert 0 = DLocker.get(pid)

      caller = self()

      task =
        Task.async(fn ->
          result = DLocker.priority_lock_get(pid, caller)
          send(caller, {:pcall_result, result})
          result
        end)

      assert_receive {:locked_entered, ^pid}, 5_000
      send(pid, :continue)
      assert_receive {:locked_exiting, ^pid}, 5_000

      assert_receive {:pcall_result, 0}, 5_000
      Task.await(task)

      assert 100 = DLocker.get(pid)

      kill(pid)
    end
  end

  describe "lock blocks other consumers" do
    test "second consumer waits for lock to clear before consuming", context do
      tenant = context[:tenant]

      {:ok, pid1} = DLocker.start_link(tenant, {"lock_multi"})
      {:ok, pid2} = DLocker.start_link(tenant, {"lock_multi"})

      # One of the consumers will acquire the lock via the queued cast.
      # We don't know which consumer dequeues it, so accept either.
      DLocker.lock_incr(pid1, self())
      assert_receive {:locked_entered, locker_pid}, 5_000

      # Queue a regular incr that the other consumer will try to consume
      DLocker.incr(pid2)

      # Give the other consumer time to attempt consume and see the lock
      Process.sleep(200)

      # Release the lock
      send(locker_pid, :continue)
      assert_receive {:locked_exiting, ^locker_pid}, 5_000

      # Wait for lock to clear and queued incr to be consumed
      Process.sleep(500)

      # 100 from handle_locked + 1 from queued incr — check via either process
      assert 101 = DLocker.get(pid1)

      kill(pid1)
    end
  end

  describe "lock with inline call" do
    test "inline call is pushed to queue when locked", context do
      tenant = context[:tenant]
      {:ok, pid} = DLocker.start_link(tenant, {"lock_inline"})

      # Start a lock via cast
      DLocker.lock_incr(pid, self())
      assert_receive {:locked_entered, ^pid}, 5_000

      # Queue a call while locked — it should be pushed to queue since the
      # process is busy in handle_locked and can't inline it
      caller = self()

      task =
        Task.async(fn ->
          result = DLocker.get(pid)
          send(caller, {:get_result, result})
          result
        end)

      # The get call can't complete while locked because the gen_server
      # process is blocked in handle_locked. It will time out at the
      # gen_server:call level. Instead, just release the lock quickly.
      send(pid, :continue)
      assert_receive {:locked_exiting, ^pid}, 5_000

      # The queued get should now complete with the post-lock state
      result = Task.await(task, 10_000)
      assert result == 100

      kill(pid)
    end
  end

  describe "lock state persistence" do
    test "state changes in handle_locked are persisted", context do
      tenant = context[:tenant]
      {:ok, pid1} = DLocker.start_link(tenant, {"lock_persist"})

      DLocker.lock_incr(pid1, self())
      assert_receive {:locked_entered, ^pid1}, 5_000
      send(pid1, :continue)
      assert_receive {:locked_exiting, ^pid1}, 5_000

      Process.sleep(100)
      assert 100 = DLocker.get(pid1)

      # Stop gracefully, start new process — state should persist
      GenServer.stop(pid1)

      {:ok, pid2} = DLocker.start_link(tenant, {"lock_persist"})
      assert 100 = DLocker.get(pid2)

      kill(pid2)
    end
  end

  describe "lock with actions" do
    test "lock_incr followed by normal operations", context do
      tenant = context[:tenant]
      {:ok, pid} = DLocker.start_link(tenant, {"lock_then_normal"})

      # First do a normal operation
      DLocker.incr(pid)
      Process.sleep(100)
      assert 1 = DLocker.get(pid)

      # Now lock
      DLocker.lock_incr(pid, self())
      assert_receive {:locked_entered, ^pid}, 5_000
      send(pid, :continue)
      assert_receive {:locked_exiting, ^pid}, 5_000

      Process.sleep(100)
      # 1 from incr + 100 from handle_locked
      assert 101 = DLocker.get(pid)

      # Normal operations still work after lock released
      DLocker.incr(pid)
      Process.sleep(100)
      assert 102 = DLocker.get(pid)

      kill(pid)
    end
  end
end
