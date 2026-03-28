defmodule DGenServer.DeadLetterTest do
  use DGen.Case, async: true
  import ExUnit.CaptureLog

  alias DGen.DCrasher

  # Helper: start a DCrasher with the given threshold
  defp start(tenant, tuid, threshold) do
    DCrasher.start_link(tenant, tuid, dead_letter_threshold: threshold)
  end

  defp wait_for_down(pid) do
    mref = Process.monitor(pid)

    receive do
      {:DOWN, ^mref, :process, ^pid, _} -> :ok
    after
      5_000 -> flunk("process #{inspect(pid)} did not exit within 5s")
    end
  end

  describe "cast dead-letter" do
    test "message dead-lettered after threshold failures, consumer survives", context do
      tenant = context[:tenant]
      tuid = {"dead_letter_cast"}
      threshold = 2

      Process.flag(:trap_exit, true)

      # Attempt 1: consumer processes the message (N=0 < 2), crashes, N→1 in queue
      capture_log(fn ->
        {:ok, pid} = start(tenant, tuid, threshold)
        DGenServer.cast(pid, :crash_me)
        wait_for_down(pid)
      end)

      assert 1 = :dgen_queue.length(tenant, :dgen_server.get_quid(tuid))

      # Attempt 2: consumer reads N=1 < 2, crashes, N→2 in queue
      capture_log(fn ->
        {:ok, pid} = start(tenant, tuid, threshold)
        wait_for_down(pid)
      end)

      assert 1 = :dgen_queue.length(tenant, :dgen_server.get_quid(tuid))

      # Attempt 3: N=2 >= threshold=2 → dead-lettered; consumer must NOT crash
      {:ok, pid} = start(tenant, tuid, threshold)

      # Synchronise: a priority call returns once the consumer is idle
      assert 0 = DCrasher.get(pid)

      assert Process.alive?(pid)
      assert 0 = :dgen_queue.length(tenant, :dgen_server.get_quid(tuid))

      DGenServer.kill(pid, :normal)
    end

    test "subsequent messages are processed normally after a dead-letter", context do
      tenant = context[:tenant]
      tuid = {"dead_letter_cast_subsequent"}
      threshold = 1

      Process.flag(:trap_exit, true)

      # One crash → attempt 1 with N=0 < 1, then N=1 in queue
      capture_log(fn ->
        {:ok, pid} = start(tenant, tuid, threshold)
        DGenServer.cast(pid, :crash_me)
        wait_for_down(pid)
      end)

      # Next consumer: N=1 >= threshold=1 → dead-letter, then processes next messages
      {:ok, pid} = start(tenant, tuid, threshold)
      DGenServer.cast(pid, {:incr, 5})

      # Use a queued call so it is processed after the preceding cast
      assert 5 = DCrasher.call_get(pid)
      assert 0 = :dgen_queue.length(tenant, :dgen_server.get_quid(tuid))

      DGenServer.kill(pid, :normal)
    end
  end

  describe "call dead-letter" do
    test "caller receives dead_letter error instead of timing out", context do
      tenant = context[:tenant]
      tuid = {"dead_letter_call"}
      threshold = 2

      Process.flag(:trap_exit, true)

      # Start a non-consuming server so the call goes straight to the durable
      # queue.  If a consuming server handled it inline it would crash before
      # returning the gen_server reply, causing the caller to receive an :exit
      # rather than waiting on the FDB reply key.
      {:ok, push_pid} =
        DGenServer.start_link(DCrasher, [tuid],
          tenant: tenant,
          consume: false,
          dead_letter_threshold: threshold
        )

      caller = Task.async(fn -> DGenServer.call(push_pid, :crash_me, 30_000) end)

      # threshold consumers crash; each failure increments the in-envelope count
      for _ <- 1..threshold do
        capture_log(fn ->
          {:ok, pid} = start(tenant, tuid, threshold)
          wait_for_down(pid)
        end)
      end

      # After threshold crashes the next consumer dead-letters the message and
      # writes {error, {dead_letter, N}} to the reply key in FDB.
      {:ok, pid_final} = start(tenant, tuid, threshold)

      assert {:error, {:dead_letter, ^threshold}} = Task.await(caller, 10_000)
      assert Process.alive?(pid_final)

      DGenServer.kill(pid_final, :normal)
      DGenServer.kill(push_pid, :normal)
    end
  end

  describe "infinity threshold" do
    test "crash loop continues when threshold is infinity", context do
      tenant = context[:tenant]
      tuid = {"dead_letter_infinity"}

      Process.flag(:trap_exit, true)

      # With infinity, every consumer should crash repeatedly
      for _ <- 1..4 do
        capture_log(fn ->
          {:ok, pid} = start(tenant, tuid, :infinity)

          if :dgen_queue.length(tenant, :dgen_server.get_quid(tuid)) == 0 do
            DGenServer.cast(pid, :crash_me)
          end

          wait_for_down(pid)
        end)

        # Message always remains in the queue
        assert 1 = :dgen_queue.length(tenant, :dgen_server.get_quid(tuid))
      end
    end
  end
end
