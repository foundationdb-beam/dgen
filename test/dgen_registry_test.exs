defmodule DGen.RegistryTest do
  use DGen.Case, async: true

  # Minimal GenServer used to exercise the via-tuple contract.
  defmodule PingServer do
    use GenServer
    def start_link(via), do: GenServer.start_link(__MODULE__, :ok, name: via)
    def ping(via), do: GenServer.call(via, :ping)
    def init(:ok), do: {:ok, :ok}
    def handle_call(:ping, _from, state), do: {:reply, :pong, state}
  end

  # ---------------------------------------------------------------------------
  # Setup — unique registry name + tenant per test
  # ---------------------------------------------------------------------------

  setup %{tenant: tenant} do
    reg = :"reg_#{:erlang.unique_integer([:positive])}"
    {:ok, _} = :dgen_registry.start_link(reg, tenant)
    await_leader!(reg)
    on_exit(fn -> stop_registry(reg) end)
    %{reg: reg}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Block until the elector has committed the join and elected a leader.
  defp await_leader!(reg, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_await_leader!(reg, deadline)
  end

  defp do_await_leader!(reg, deadline) do
    case :dgen_registry.get_leader(reg) do
      :undefined ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(20)
          do_await_leader!(reg, deadline)
        else
          flunk("timed out waiting for leader election in #{reg}")
        end

      _leader ->
        :ok
    end
  end

  # Poll `fun.()` until it returns true or the deadline passes.
  defp eventually(fun, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(20)
        do_eventually(fun, deadline)
      else
        false
      end
    end
  end

  # Supervisors exit with :shutdown (not :normal) when their parent process
  # dies, which has already happened by the time on_exit callbacks run.
  # Use :shutdown as the stop reason and catch any race.
  defp stop_registry(reg) do
    case Process.whereis(reg) do
      nil ->
        :ok

      pid ->
        try do
          Supervisor.stop(pid, :shutdown)
        catch
          :exit, _ -> :ok
        end
    end
  end

  defp stop_server(pid) do
    if Process.alive?(pid) do
      try do
        GenServer.stop(pid, :shutdown)
      catch
        :exit, _ -> :ok
      end
    end
  end

  defp via(reg, name), do: {:via, :dgen_registry, {reg, name}}

  # ---------------------------------------------------------------------------
  # Registration
  # ---------------------------------------------------------------------------

  describe "register_name/2" do
    test "returns yes and name resolves via whereis_name/1", %{reg: reg} do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      assert :yes = :dgen_registry.register_name({reg, :foo}, pid)
      assert pid == :dgen_registry.whereis_name({reg, :foo})
    end

    test "returns no when the name is already taken", %{reg: reg} do
      pid1 = spawn(fn -> Process.sleep(:infinity) end)
      pid2 = spawn(fn -> Process.sleep(:infinity) end)

      on_exit(fn ->
        Process.exit(pid1, :kill)
        Process.exit(pid2, :kill)
      end)

      :yes = :dgen_registry.register_name({reg, :dup}, pid1)
      assert :no = :dgen_registry.register_name({reg, :dup}, pid2)
    end

    test "different logical names are independent", %{reg: reg} do
      pid1 = spawn(fn -> Process.sleep(:infinity) end)
      pid2 = spawn(fn -> Process.sleep(:infinity) end)

      on_exit(fn ->
        Process.exit(pid1, :kill)
        Process.exit(pid2, :kill)
      end)

      assert :yes = :dgen_registry.register_name({reg, :a}, pid1)
      assert :yes = :dgen_registry.register_name({reg, :b}, pid2)
      assert pid1 == :dgen_registry.whereis_name({reg, :a})
      assert pid2 == :dgen_registry.whereis_name({reg, :b})
    end
  end

  describe "unregister_name/1" do
    test "removes the registration", %{reg: reg} do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      :yes = :dgen_registry.register_name({reg, :bar}, pid)

      :dgen_registry.unregister_name({reg, :bar})

      # unregister_name is a cast; whereis_name is a call — both go to the
      # same member from the same process, so Erlang per-pair ordering ensures
      # the cast is processed first.
      assert :undefined = :dgen_registry.whereis_name({reg, :bar})
    end

    test "is idempotent — unregistering an unknown name is a no-op", %{reg: reg} do
      assert :ok = :dgen_registry.unregister_name({reg, :no_such})
    end
  end

  describe "whereis_name/1" do
    test "returns undefined for an unknown name", %{reg: reg} do
      assert :undefined = :dgen_registry.whereis_name({reg, :unknown})
    end
  end

  describe "whereis_name_consistent/1" do
    test "returns the authoritative pid from the leader", %{reg: reg} do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      :yes = :dgen_registry.register_name({reg, :consistent}, pid)
      assert pid == :dgen_registry.whereis_name_consistent({reg, :consistent})
    end

    test "returns undefined for an unknown name", %{reg: reg} do
      assert :undefined = :dgen_registry.whereis_name_consistent({reg, :unknown_c})
    end

    test "agrees with whereis_name/1 on the local node", %{reg: reg} do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      :yes = :dgen_registry.register_name({reg, :agree}, pid)

      assert :dgen_registry.whereis_name({reg, :agree}) ==
               :dgen_registry.whereis_name_consistent({reg, :agree})
    end
  end

  # ---------------------------------------------------------------------------
  # OTP via-tuple contract
  # ---------------------------------------------------------------------------

  describe "via-tuple" do
    test "a GenServer can be started and called via a via-tuple name", %{reg: reg} do
      {:ok, pid} = PingServer.start_link(via(reg, :ping))
      on_exit(fn -> stop_server(pid) end)

      assert :pong = PingServer.ping(via(reg, :ping))
    end

    test "whereis_name/1 resolves the pid of a via-registered GenServer", %{reg: reg} do
      {:ok, pid} = PingServer.start_link(via(reg, :ping2))
      on_exit(fn -> stop_server(pid) end)

      assert pid == :dgen_registry.whereis_name({reg, :ping2})
    end

    test "two GenServers can be registered under different names", %{reg: reg} do
      {:ok, pid1} = PingServer.start_link(via(reg, :s1))
      {:ok, pid2} = PingServer.start_link(via(reg, :s2))

      on_exit(fn ->
        stop_server(pid1)
        stop_server(pid2)
      end)

      assert :pong = PingServer.ping(via(reg, :s1))
      assert :pong = PingServer.ping(via(reg, :s2))
      assert pid1 != pid2
    end
  end

  describe "send/2" do
    test "delivers a message to the registered process", %{reg: reg} do
      :yes = :dgen_registry.register_name({reg, :send_target}, self())
      :dgen_registry.send({reg, :send_target}, :hello)
      assert_receive :hello, 1_000
    end

    test "returns the pid of the registered process", %{reg: reg} do
      :yes = :dgen_registry.register_name({reg, :send_pid}, self())
      result = :dgen_registry.send({reg, :send_pid}, :hi)
      assert result == self()
      assert_receive :hi, 1_000
    end

    test "exits with badarg for an unregistered name", %{reg: reg} do
      assert {:badarg, _} = catch_exit(:dgen_registry.send({reg, :no_such}, :hello))
    end
  end

  # ---------------------------------------------------------------------------
  # Auto-unregistration
  # ---------------------------------------------------------------------------

  describe "auto-unregistration" do
    test "process death removes the registration", %{reg: reg} do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      :yes = :dgen_registry.register_name({reg, :dying}, pid)
      assert pid == :dgen_registry.whereis_name({reg, :dying})

      Process.exit(pid, :kill)

      # The leader receives a DOWN signal asynchronously, so poll.
      assert eventually(fn -> :dgen_registry.whereis_name({reg, :dying}) == :undefined end),
             "name was not unregistered after process exit"
    end

    test "name can be re-registered after the original process exits", %{reg: reg} do
      pid1 = spawn(fn -> Process.sleep(:infinity) end)
      :yes = :dgen_registry.register_name({reg, :reuse}, pid1)
      Process.exit(pid1, :kill)

      assert eventually(fn -> :dgen_registry.whereis_name({reg, :reuse}) == :undefined end),
             "name was not released after first process exit"

      pid2 = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid2, :kill) end)

      assert :yes = :dgen_registry.register_name({reg, :reuse}, pid2)
      assert pid2 == :dgen_registry.whereis_name({reg, :reuse})
    end

    test "stopping a via-registered GenServer unregisters its name", %{reg: reg} do
      {:ok, pid} = PingServer.start_link(via(reg, :mortal))
      assert pid == :dgen_registry.whereis_name({reg, :mortal})

      GenServer.stop(pid)

      assert eventually(fn -> :dgen_registry.whereis_name({reg, :mortal}) == :undefined end),
             "name was not unregistered after GenServer.stop"
    end
  end

  # ---------------------------------------------------------------------------
  # Introspection
  # ---------------------------------------------------------------------------

  describe "get_leader/1" do
    test "returns a {node, name} member_id after startup", %{reg: reg} do
      assert {node_name, _member_atom} = :dgen_registry.get_leader(reg)
      assert node_name == node()
    end
  end

  describe "get_members/1" do
    test "returns a non-empty list after startup", %{reg: reg} do
      assert [_ | _] = :dgen_registry.get_members(reg)
    end

    test "includes the local node's member", %{reg: reg} do
      member_name = :dgen_registry.member_name(reg)
      assert {node(), member_name} in :dgen_registry.get_members(reg)
    end
  end

  describe "elector_name/1 and member_name/1" do
    test "elector process is alive after start", %{reg: reg} do
      assert reg |> :dgen_registry.elector_name() |> Process.whereis() |> is_pid()
    end

    test "member process is alive after start", %{reg: reg} do
      assert reg |> :dgen_registry.member_name() |> Process.whereis() |> is_pid()
    end

    test "elector_name/1 appends the _elector suffix" do
      assert :my_reg_elector = :dgen_registry.elector_name(:my_reg)
    end

    test "member_name/1 appends the _member suffix" do
      assert :my_reg_member = :dgen_registry.member_name(:my_reg)
    end
  end
end
