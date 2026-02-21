defmodule DGenServer.InlineThrowTest do
  use DGen.Case, async: true

  alias DGen.DStopper

  describe "inline call callback throw" do
    test "throw pushes call to queue and re-throws", context do
      tenant = context[:tenant]
      tuid = {"inline_throw"}

      Process.flag(:trap_exit, true)

      {:ok, pid} = DStopper.start_link(tenant, tuid)
      mref = Process.monitor(pid)

      # Queue should be empty before the call
      assert 0 = :dgen_queue.length(tenant, tuid)

      # The inline call will invoke handle_call(:throw_me, ...) which throws.
      # The try/catch inside the transaction pushes the call to the queue,
      # then re-throws outside the transaction, crashing the gen_server.
      try do
        DGenServer.call(pid, :throw_me, 10_000)
      catch
        :exit, _ -> :ok
      end

      assert_receive {:DOWN, ^mref, :process, ^pid, _reason}, 5_000

      # The call should now be in the queue
      assert 1 = :dgen_queue.length(tenant, tuid)
    end
  end
end
