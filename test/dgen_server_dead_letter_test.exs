defmodule DGen.Server.DeadLetterTest do
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
        DGen.Server.cast(pid, :crash_me)
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

      DGen.Server.kill(pid, :normal)
    end

    test "subsequent messages are processed normally after a dead-letter", context do
      tenant = context[:tenant]
      tuid = {"dead_letter_cast_subsequent"}
      threshold = 1

      Process.flag(:trap_exit, true)

      # One crash → attempt 1 with N=0 < 1, then N=1 in queue
      capture_log(fn ->
        {:ok, pid} = start(tenant, tuid, threshold)
        DGen.Server.cast(pid, :crash_me)
        wait_for_down(pid)
      end)

      # Next consumer: N=1 >= threshold=1 → dead-letter, then processes next messages
      {:ok, pid} = start(tenant, tuid, threshold)
      DGen.Server.cast(pid, {:incr, 5})

      # Use a queued call so it is processed after the preceding cast
      assert 5 = DCrasher.call_get(pid)
      assert 0 = :dgen_queue.length(tenant, :dgen_server.get_quid(tuid))

      DGen.Server.kill(pid, :normal)
    end
  end

  describe "call dead-letter" do
    test "caller raises dead_letter instead of timing out", context do
      tenant = context[:tenant]
      tuid = {"dead_letter_call"}
      threshold = 2

      Process.flag(:trap_exit, true)

      # Non-consuming push server: the call goes straight to the durable queue.
      {:ok, push_pid} =
        DGen.Server.start_link(DCrasher, [tuid],
          tenant: tenant,
          consume: false,
          dead_letter_threshold: threshold
        )

      caller =
        Task.async(fn ->
          try do
            DGen.Server.call(push_pid, :crash_me, 30_000)
          catch
            :error, {:dead_letter, n} -> {:dead_letter, n}
          end
        end)

      # threshold consumers crash; each failure increments the in-envelope count
      for _ <- 1..threshold do
        capture_log(fn ->
          {:ok, pid} = start(tenant, tuid, threshold)
          wait_for_down(pid)
        end)
      end

      # After threshold crashes the next consumer dead-letters the message and
      # writes a raise sentinel to the reply key in FDB; the caller raises.
      {:ok, pid_final} = start(tenant, tuid, threshold)

      assert {:dead_letter, ^threshold} = Task.await(caller, 10_000)
      assert Process.alive?(pid_final)

      DGen.Server.kill(pid_final, :normal)
      DGen.Server.kill(push_pid, :normal)
    end
  end

  describe "default (no dead-lettering)" do
    test "crash loop continues indefinitely with default threshold", context do
      tenant = context[:tenant]
      tuid = {"dead_letter_infinity"}

      Process.flag(:trap_exit, true)

      # No dead_letter_threshold option — infinity is the default
      for _ <- 1..4 do
        capture_log(fn ->
          {:ok, pid} = DCrasher.start_link(tenant, tuid)

          if :dgen_queue.length(tenant, :dgen_server.get_quid(tuid)) == 0 do
            DGen.Server.cast(pid, :crash_me)
          end

          wait_for_down(pid)
        end)

        # Message always remains in the queue
        assert 1 = :dgen_queue.length(tenant, :dgen_server.get_quid(tuid))
      end
    end
  end
end
