defmodule DGen.Server.LockExpiryTest do
  @moduledoc """
  The `lock_timeout` half of the distributed lock (§4.4).

  `dgen_server_lock_test.exs` covers acquire/hold/clear, but every test there runs
  with the default `lock_timeout: :infinity`, so the `stale` branch of
  `check_lock/2` and the bust at the head of `handle_consume/4` never execute. This
  file drives them.

  Busting is a **time-based guess** that the holder is dead: the busting consumer
  compares its own wall clock against a timestamp written by the holder's. Nothing
  revokes a holder that is merely slow, so the guess can be wrong, and the tests
  here cover both halves of that — the busting mechanism itself, and what happens
  to a holder that was busted while still running.

  The answer to the second half is fencing. A busted holder is refused at commit
  and its message is put back, rather than being allowed to overwrite the state its
  successor wrote. Two tests below pin the values that a missing fence would get
  wrong: the successor's increment surviving, and the successor's lock surviving.
  """
  use DGen.Case, async: true

  alias DGen.DLocker

  defp kill(pid) do
    mref = Process.monitor(pid)
    DGen.Server.kill(pid, :normal)

    receive do
      {:DOWN, ^mref, :process, ^pid, :normal} -> :ok
    end
  end

  # `DGen.Server.kill/2` is a cast, so it cannot reach a process parked inside
  # handle_locked. This is the SIGKILL a stranded holder actually gets.
  defp hard_kill(pid) do
    Process.unlink(pid)
    mref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^mref, :process, ^pid, _} -> :ok
    after
      5_000 -> flunk("#{inspect(pid)} did not die")
    end
  end

  # The lock key `dgen_server:set_lock/2` writes, in the tenant's directory.
  defp lock_key(tuid), do: :dgen_key.extend(tuid, "k")

  defp put_raw_lock(tenant, tuid, value) do
    :dgen_backend.transactional(tenant, fn {tx, dir} ->
      b = :dgen_config.backend()
      b.set(tx, b.dir_pack(dir, lock_key(tuid)), value)
    end)
  end

  defp read_raw_lock(tenant, tuid) do
    :dgen_backend.transactional(tenant, fn {tx, dir} ->
      b = :dgen_config.backend()
      b.wait(b.get(tx, b.dir_pack(dir, lock_key(tuid))))
    end)
  end

  describe "lock_timeout bounds a held lock" do
    test "a second consumer busts a stale lock and makes progress while the holder still runs",
         context do
      tenant = context[:tenant]
      tuid = {"lock_expiry_bust"}

      {:ok, holder} = DLocker.start_link_opts(tenant, tuid, lock_timeout: 200)

      DLocker.lock_incr(holder, self())
      assert_receive {:locked_entered, ^holder}, 5_000

      # The holder is now parked inside handle_locked with the lock set. A second
      # consumer joins; its first consume sees a live lock and arms recheck_lock
      # for the remaining ~200ms rather than busting immediately.
      {:ok, buster} = DLocker.start_link_opts(tenant, tuid, lock_timeout: 200)
      DLocker.incr(buster)

      # Once the lease expires, recheck_lock fires, the lock is seen as stale, and
      # the buster clears it and drains the queue — all while `holder` is still
      # sitting in its receive. Nothing revoked the holder; this is the bust.
      assert eventually(fn -> DLocker.get(buster) == 1 end, 5_000),
             "stale lock was never busted: counter is #{DLocker.get(buster)}, expected 1"

      # And the lock really is gone, not merely ignored.
      assert :not_found = read_raw_lock(tenant, tuid)

      # What happens to the busted holder from here is the fence's business, and
      # is covered separately below; this test is only about the bust itself.
      hard_kill(holder)
      kill(buster)
    end

    test "default lock_timeout of infinity never busts, however long the lock is held",
         context do
      tenant = context[:tenant]
      tuid = {"lock_expiry_infinity"}

      {:ok, holder} = DLocker.start_link(tenant, tuid)

      DLocker.lock_incr(holder, self())
      assert_receive {:locked_entered, ^holder}, 5_000

      {:ok, waiter} = DLocker.start_link(tenant, tuid)
      DLocker.incr(waiter)

      # No timeout means no staleness and no recheck timer: the queued incr waits
      # on the push watch alone, indefinitely.
      Process.sleep(500)
      assert 0 = DLocker.get(waiter)
      assert is_binary(read_raw_lock(tenant, tuid))

      send(holder, :continue)
      assert_receive {:locked_exiting, ^holder}, 5_000

      # clear_lock's notify bump wakes the waiter's push watch.
      assert eventually(fn -> DLocker.get(waiter) == 101 end, 5_000),
             "queued incr never ran after the lock cleared; got #{DLocker.get(waiter)}"

      kill(holder)
    end
  end

  describe "lock values check_lock cannot interpret are never busted" do
    # Both of these degrade to `{live, :infinity}` — deliberately, so a lock this
    # version does not understand is never cleared out from under its holder. The
    # cost is that such a lock is permanently unbustable even when lock_timeout is
    # finite, and no recheck timer is armed for it (`Remaining` is infinity), so
    # recovery depends entirely on the push watch. Untested until now, and the
    # failure mode is a wedge rather than a crash.

    test "a v0.2.0 empty-binary lock is treated as live forever", context do
      tenant = context[:tenant]
      tuid = {"lock_expiry_legacy"}

      put_raw_lock(tenant, tuid, <<>>)

      {:ok, pid} = DLocker.start_link_opts(tenant, tuid, lock_timeout: 50)
      DLocker.incr(pid)

      Process.sleep(400)

      assert 0 = DLocker.get(pid), "an empty-binary lock was busted; it must not be"
      assert <<>> = read_raw_lock(tenant, tuid)

      kill(pid)
    end

    test "an undecodable lock value is treated as live forever", context do
      tenant = context[:tenant]
      tuid = {"lock_expiry_garbage"}

      put_raw_lock(tenant, tuid, <<"not a term">>)

      {:ok, pid} = DLocker.start_link_opts(tenant, tuid, lock_timeout: 50)
      DLocker.incr(pid)

      Process.sleep(400)

      assert 0 = DLocker.get(pid), "an undecodable lock was busted; it must not be"
      assert <<"not a term">> = read_raw_lock(tenant, tuid)

      kill(pid)
    end

    test "a non-integer term lock value is treated as live forever", context do
      tenant = context[:tenant]
      tuid = {"lock_expiry_nonint"}

      # Decodes cleanly but is not a timestamp — the `_ -> {live, infinity}` arm.
      put_raw_lock(tenant, tuid, :erlang.term_to_binary({:not, :a, :timestamp}))

      {:ok, pid} = DLocker.start_link_opts(tenant, tuid, lock_timeout: 50)
      DLocker.incr(pid)

      Process.sleep(400)

      assert 0 = DLocker.get(pid)

      kill(pid)
    end
  end

  describe "recovery on a quiet queue" do
    test "an abandoned lock is busted with no further pushes to wake the watch", context do
      tenant = context[:tenant]
      tuid = {"lock_expiry_quiet"}

      # A lock left behind by a holder that is gone: exactly what a crashed node
      # leaves, since terminate/2 does not clear it. Written in the past so it is
      # already stale by the time a consumer looks.
      stale_ts = System.system_time(:millisecond) - 10_000
      put_raw_lock(tenant, tuid, :erlang.term_to_binary(stale_ts))

      {:ok, pid} = DLocker.start_link_opts(tenant, tuid, lock_timeout: 100)
      DLocker.incr(pid)

      assert eventually(fn -> DLocker.get(pid) == 1 end, 5_000),
             "an already-stale lock was not busted on a quiet queue"

      assert :not_found = read_raw_lock(tenant, tuid)

      kill(pid)
    end

    test "a lock is never cleared before its lease expires, and recovers with no further pushes",
         context do
      tenant = context[:tenant]
      tuid = {"lock_expiry_recheck"}
      timeout = 300

      # Stamped now, so the lock is live when the consumer first looks: the consume
      # takes the `{live, Remaining}` branch and must arm recheck_lock. Exactly one
      # message is ever pushed, and the consume it triggers is the one that parks —
      # so after that point nothing can bump the push watch and only the timer can
      # wake the consumer. If it is not armed, the recovery assertion below fails.
      written_at = System.monotonic_time(:millisecond)
      put_raw_lock(tenant, tuid, :erlang.term_to_binary(System.system_time(:millisecond)))

      {:ok, pid} = DLocker.start_link_opts(tenant, tuid, lock_timeout: timeout)
      DLocker.incr(pid)

      assert eventually(fn -> DLocker.get(pid) == 1 end, 5_000),
             "the lock was never reclaimed: the consumer is parked on a watch nothing will bump"

      # Asserting *when* rather than merely *that*, because the interesting failure
      # is clearing a lease early. Checking elapsed time against the lease rather
      # than sampling state at a fixed offset keeps this honest on a loaded machine:
      # a slow start makes the lock genuinely stale before the first look, which is
      # a legitimate bust, not a test failure.
      elapsed = System.monotonic_time(:millisecond) - written_at

      assert elapsed >= timeout,
             "the lock was cleared #{elapsed}ms after it was written, inside its #{timeout}ms lease"

      kill(pid)
    end
  end

  describe "a busted holder is fenced" do
    test "its commit is refused, and the message it held is retried not lost", context do
      tenant = context[:tenant]
      tuid = {"lock_expiry_fence"}

      Process.flag(:trap_exit, true)

      # Long timeout: once busted, this consumer must not turn around and bust the
      # successor back, which would confuse what is being measured here.
      {:ok, holder} = DLocker.start_link_opts(tenant, tuid, lock_timeout: 30_000)
      DLocker.lock_incr(holder, self())
      assert_receive {:locked_entered, ^holder}, 5_000

      # The successor busts the lease and gets real work done while the holder is
      # still inside handle_locked. Its +1 is the write that used to be destroyed.
      {:ok, successor} = DLocker.start_link_opts(tenant, tuid, lock_timeout: 200)
      DLocker.incr(successor)

      assert eventually(fn -> DLocker.get(successor) == 1 end, 5_000),
             "the successor never busted the lock; nothing to fence"

      # Now let the busted holder finish. Its result was computed from the state it
      # read before taking the lock (0 → 100), so committing it would erase the
      # successor's increment. The fence refuses the commit and the holder exits.
      send(holder, :continue)
      assert_receive {:locked_exiting, ^holder}, 5_000
      assert_receive {:EXIT, ^holder, {{:lock_fenced, :requeued}, _}}, 5_000

      # The refusal costs a retry, not the message: the lock_incr went back on the
      # queue and the current lock holder picks it up.
      assert_receive {:locked_entered, ^successor}, 10_000
      send(successor, :continue)
      assert_receive {:locked_exiting, ^successor}, 5_000

      # 101, not 100: the successor's increment survived, and the locked section
      # ran again on top of it rather than being dropped.
      assert eventually(fn -> DLocker.get(successor) == 101 end, 5_000),
             "expected 101 after the retry; got #{DLocker.get(successor)}"

      kill(successor)
    end
  end

  describe "a busted holder releases only its own lock" do
    test "the successor's lock survives the busted holder's clear", context do
      tenant = context[:tenant]
      tuid = {"lock_expiry_cascade"}

      # The busted holder is fenced at commit and exits; that is expected here.
      Process.flag(:trap_exit, true)

      # `lock_timeout` is per-consumer and governs how *that* consumer judges
      # someone else's lock. The successor's is short so it busts the holder
      # quickly; the holder's is long so that once it is busted it cannot turn
      # around and bust the successor back through the ordinary staleness path —
      # which would be legitimate, and would mask the cascade this test is for.
      {:ok, holder} = DLocker.start_link_opts(tenant, tuid, lock_timeout: 30_000)
      DLocker.lock_incr(holder, self())
      assert_receive {:locked_entered, ^holder}, 5_000

      held = read_raw_lock(tenant, tuid)
      assert is_binary(held)

      # A second consumer busts the lease and takes the lock for its own locked
      # section, so by the time `holder` finishes, the lock in the store is the
      # successor's rather than its own.
      {:ok, successor} = DLocker.start_link_opts(tenant, tuid, lock_timeout: 200)
      DLocker.lock_incr(successor, self())
      assert_receive {:locked_entered, ^successor}, 10_000

      successor_lock = read_raw_lock(tenant, tuid)
      assert is_binary(successor_lock)
      assert successor_lock != held, "the successor should hold a distinct lock"

      # The busted holder now runs its `after` clause. Releasing unconditionally
      # would clear the successor's lock while it is still inside handle_locked,
      # admitting a third consumer with no timeout elapsed — one late holder
      # unlocking an arbitrarily long chain of successors.
      send(holder, :continue)
      assert_receive {:locked_exiting, ^holder}, 5_000
      assert_receive {:EXIT, ^holder, {{:lock_fenced, :requeued}, _}}, 5_000

      refute eventually(fn -> read_raw_lock(tenant, tuid) == :not_found end, 500),
             "the busted holder released the successor's lock"

      assert successor_lock == read_raw_lock(tenant, tuid)

      # And the successor's own release still works: it holds the matching token.
      send(successor, :continue)
      assert_receive {:locked_exiting, ^successor}, 5_000

      # The fenced holder put its message back, so the successor now picks that up
      # and runs the locked section again — on top of the state it already wrote,
      # rather than under an overwrite.
      assert_receive {:locked_entered, ^successor}, 10_000
      send(successor, :continue)
      assert_receive {:locked_exiting, ^successor}, 5_000

      assert eventually(fn -> read_raw_lock(tenant, tuid) == :not_found end, 5_000),
             "the successor never released its own lock"

      kill(successor)
    end
  end

  describe "lock lifetime across process death" do
    test "an orderly stop leaves the lock set for the next consumer to bust", context do
      tenant = context[:tenant]
      tuid = {"lock_expiry_terminate"}

      {:ok, holder} = DLocker.start_link_opts(tenant, tuid, lock_timeout: 200)
      DLocker.lock_incr(holder, self())
      assert_receive {:locked_entered, ^holder}, 5_000

      # Kill the holder while it is inside handle_locked. terminate/2 is a no-op and
      # the `after` clause that would clear the lock never runs, so the lock outlives
      # the process — the lease timeout is the only thing that can recover it.
      Process.unlink(holder)
      Process.exit(holder, :kill)
      assert eventually(fn -> not Process.alive?(holder) end, 5_000)

      assert is_binary(read_raw_lock(tenant, tuid)), "the lock should survive the holder's death"

      {:ok, next} = DLocker.start_link_opts(tenant, tuid, lock_timeout: 200)
      DLocker.incr(next)

      assert eventually(fn -> DLocker.get(next) == 1 end, 5_000),
             "the dead holder's lock was never reclaimed"

      assert :not_found = read_raw_lock(tenant, tuid)

      kill(next)
    end
  end

  defp eventually(fun, timeout) do
    eventually_until(fun, System.monotonic_time(:millisecond) + timeout)
  end

  defp eventually_until(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(20)
        eventually_until(fun, deadline)
    end
  end
end
