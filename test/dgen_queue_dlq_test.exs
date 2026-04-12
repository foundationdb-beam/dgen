defmodule DGen.QueueDLQTest do
  use DGen.Case, async: true
  import ExUnit.CaptureLog

  alias DGen.DCrasher

  # With threshold=1: N=0 < 1 → callback runs → crash → N=1 in queue;
  # N=1 >= 1 → dead-lettered on the next consume.
  @threshold 1

  defp wait_for_down(pid) do
    mref = Process.monitor(pid)

    receive do
      {:DOWN, ^mref, :process, ^pid, _} -> :ok
    after
      5_000 -> flunk("process #{inspect(pid)} did not exit within 5s")
    end
  end

  # Drives one :crash_me cast through @threshold crash cycles and into the DLQ.
  # Returns the quid for the given tuid.
  defp seed_dlq(tenant, tuid) do
    Process.flag(:trap_exit, true)
    quid = :dgen_server.get_quid(tuid)

    # N=0 < @threshold → callback runs → throw → crash → N=1 written to queue
    capture_log(fn ->
      {:ok, pid} = DCrasher.start_link(tenant, tuid, dead_letter_threshold: @threshold)
      DGen.Server.cast(pid, :crash_me)
      wait_for_down(pid)
    end)

    # N=1 >= @threshold → dead-lettered; consumer does NOT crash
    {:ok, pid} = DCrasher.start_link(tenant, tuid, dead_letter_threshold: @threshold)
    # Priority call returns once the consumer is idle (dead-letter has been committed)
    DCrasher.get(pid)
    # Stop without deleting FDB state (DGen.Server.kill would wipe the queue/DLQ)
    GenServer.stop(pid)

    quid
  end

  test "peek_dlq returns dead-lettered entries with envelope and metadata", context do
    tenant = context[:tenant]
    quid = seed_dlq(tenant, {"dlq_peek"})

    assert [{_key, {:cast, :crash_me}, @threshold, ts}] = :dgen_queue.peek_dlq(tenant, quid)
    assert is_integer(ts) and ts > 0
  end

  test "dlq_length returns 0 for an empty DLQ", context do
    tenant = context[:tenant]
    quid = :dgen_server.get_quid({"dlq_length_empty"})

    assert 0 = :dgen_queue.dlq_length(tenant, quid)
  end

  test "dlq_length returns the count of dead-lettered entries", context do
    tenant = context[:tenant]
    quid = seed_dlq(tenant, {"dlq_length"})

    assert 1 = :dgen_queue.dlq_length(tenant, quid)
  end

  test "delete_dlq_entry removes the entry", context do
    tenant = context[:tenant]
    quid = seed_dlq(tenant, {"dlq_delete"})

    [{key, _envelope, _n, _ts}] = :dgen_queue.peek_dlq(tenant, quid)
    :dgen_queue.delete_dlq_entry(tenant, key)

    assert 0 = :dgen_queue.dlq_length(tenant, quid)
  end

  test "purge_dlq removes all entries", context do
    tenant = context[:tenant]
    quid = seed_dlq(tenant, {"dlq_purge"})

    :dgen_queue.purge_dlq(tenant, quid)

    assert 0 = :dgen_queue.dlq_length(tenant, quid)
  end

  test "requeue_dlq_entry moves the message back to the main queue", context do
    tenant = context[:tenant]
    quid = seed_dlq(tenant, {"dlq_requeue"})

    [{key, _envelope, _n, _ts}] = :dgen_queue.peek_dlq(tenant, quid)
    assert :ok = :dgen_queue.requeue_dlq_entry(tenant, quid, key)

    assert 0 = :dgen_queue.dlq_length(tenant, quid)
    assert 1 = :dgen_queue.length(tenant, quid)
  end

  test "requeue_dlq_entry resets the attempt count to 0", context do
    tenant = context[:tenant]
    tuid = {"dlq_requeue_reset"}
    quid = seed_dlq(tenant, tuid)

    [{key, _envelope, _n, _ts}] = :dgen_queue.peek_dlq(tenant, quid)
    :ok = :dgen_queue.requeue_dlq_entry(tenant, quid, key)

    # If the attempt count was correctly reset to 0, a consumer with
    # @threshold=1 will crash on :crash_me (N=0 < 1 → callback runs → throw)
    # rather than immediately dead-lettering it (which would happen if N=1 >= 1).
    capture_log(fn ->
      {:ok, pid} = DCrasher.start_link(tenant, tuid, dead_letter_threshold: @threshold)
      wait_for_down(pid)
    end)

    # Message is still in queue with N=1 (incremented from 0 by the crash)
    assert 1 = :dgen_queue.length(tenant, quid)
    assert 0 = :dgen_queue.dlq_length(tenant, quid)
  end

  test "requeue_dlq_entry returns error when key does not exist", context do
    tenant = context[:tenant]
    quid = seed_dlq(tenant, {"dlq_requeue_not_found"})

    [{key, _envelope, _n, _ts}] = :dgen_queue.peek_dlq(tenant, quid)
    :dgen_queue.delete_dlq_entry(tenant, key)

    assert {:error, :not_found} = :dgen_queue.requeue_dlq_entry(tenant, quid, key)
  end
end
