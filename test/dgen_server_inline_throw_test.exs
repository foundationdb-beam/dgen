defmodule DGenServer.InlineThrowTest do
  use DGen.Case, async: true
  import ExUnit.CaptureLog

  alias DGen.DStopper

  describe "inline call callback throw" do
    test "throw pushes call to queue and stops server cleanly", context do
      tenant = context[:tenant]
      tuid = {"inline_throw"}

      Process.flag(:trap_exit, true)

      {:ok, pid} = DStopper.start_link(tenant, tuid)
      mref = Process.monitor(pid)

      # Queue should be empty before the call
      assert 0 = :dgen_queue.length(tenant, tuid)

      # The inline call invokes handle_call(:throw_me, …) which throws.
      # The server pushes the call to the durable queue, replies with
      # {noreply, …} so the caller enters await_call_reply, then stops.
      # No consumer is running, so the caller times out.
      capture_log(fn ->
        try do
          DGenServer.call(pid, :throw_me, 200)
        catch
          :error, :timeout -> :ok
        end

        assert_receive {:DOWN, ^mref, :process, ^pid, _reason}, 5_000
      end)

      # The call should now be in the queue
      assert 1 = :dgen_queue.length(tenant, :dgen_server.get_quid(tuid))
    end
  end
end
