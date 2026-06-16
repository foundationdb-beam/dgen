defmodule DGen.RegistryClusterTest do
  # Cluster tests modify global node/distribution state and start FDB
  # transactions from multiple nodes — run sequentially.
  use DGen.Case, async: false

  import DGen.ClusterHelper, only: [await_leader!: 1, eventually: 1, eventually: 2]

  # ---------------------------------------------------------------------------
  # A minimal via-tuple server usable from both primary and peer.
  # Must be compiled into the ebin so the peer can resolve it.
  # ---------------------------------------------------------------------------

  defmodule PingServer do
    @moduledoc false
    use GenServer

    def start_link(name), do: GenServer.start_link(__MODULE__, [], name: name)
    def ping(name), do: GenServer.call(name, :ping)
    def init(_), do: {:ok, nil}
    def handle_call(:ping, _from, state), do: {:reply, :pong, state}
  end

  # ---------------------------------------------------------------------------
  # Setup — start a peer node for every test and wire it to the same FDB dir
  # ---------------------------------------------------------------------------

  setup %{tenant: {db, case_dir}} do
    # Absolute cluster-file path so the peer can open it regardless of its CWD.
    sandbox_dir = :filename.join([".erlfdb_sandbox", "DGen.Case"])
    cluster_file = :persistent_term.get({:erlfdb, :test_cluster_file, sandbox_dir})
    abs_cluster_file = :filename.absname(cluster_file)

    # The erlfdb directory path is a list of `{:utf8, binary}` tuples —
    # fully serialisable over Erlang distribution.
    dir_path = :erlfdb_directory.get_path(case_dir)

    # Unique name so concurrent test runs don't collide.
    suffix = :erlang.unique_integer([:positive])
    reg = :"cluster_reg_#{suffix}"
    peer_name = :"peer#{suffix}@127.0.0.1"

    # Start the registry on the primary node.
    {:ok, _} = :dgen_registry.start_link(reg, {db, case_dir})

    # Use :standard_io as the control channel so the peer reports boot
    # completion via stdio rather than requiring it to reach back to the
    # primary via distribution (which can fail when distribution was started
    # mid-process via Node.start/2 rather than at BEAM startup).
    {:ok, peer_pid, peer_node} =
      :peer.start_link(%{name: peer_name, connection: :standard_io})

    # Propagate all code paths so compiled BEAM files are reachable on the
    # peer (includes test/support modules and deps).
    :erpc.call(peer_node, :code, :add_paths, [:code.get_path()])

    # Start required applications on the peer.
    {:ok, _} = :erpc.call(peer_node, Application, :ensure_all_started, [:erlfdb])
    {:ok, _} = :erpc.call(peer_node, Application, :ensure_all_started, [:dgen])

    # Start the registry on the peer, connected to the same FDB keyspace.
    :ok =
      :erpc.call(peer_node, DGen.ClusterHelper, :start_registry, [
        reg,
        abs_cluster_file,
        dir_path
      ])

    # Both nodes must have elected a leader before any test assertion.
    await_leader!(reg)
    :erpc.call(peer_node, DGen.ClusterHelper, :await_leader!, [reg])

    # Wait until the peer member's leader field has been set via the async cast.
    :erpc.call(peer_node, DGen.ClusterHelper, :await_registry_ready!, [reg])

    on_exit(fn -> DGen.ClusterHelper.stop_peer(peer_pid) end)

    %{
      reg: reg,
      peer_node: peer_node,
      peer_pid: peer_pid,
      abs_cluster_file: abs_cluster_file,
      dir_path: dir_path,
      db: db,
      case_dir: case_dir
    }
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp via(reg, name), do: {:via, :dgen_registry, {reg, name}}

  # Run whereis_name on the remote node.
  defp remote_whereis(peer_node, reg, name) do
    :erpc.call(peer_node, :dgen_registry, :whereis_name, [{reg, name}])
  end

  # Run whereis_name_consistent on the remote node.
  defp remote_whereis_consistent(peer_node, reg, name) do
    :erpc.call(peer_node, :dgen_registry, :whereis_name_consistent, [{reg, name}])
  end

  # Spawn a long-lived process on a remote node without Elixir dependency.
  # Uses :timer.sleep so no Elixir module is required on the peer.
  defp spawn_remote(node) do
    :erpc.call(node, :erlang, :spawn, [:timer, :sleep, [:infinity]])
  end

  # ---------------------------------------------------------------------------
  # Leadership
  # ---------------------------------------------------------------------------

  describe "leader election" do
    test "both nodes agree on a single leader", %{reg: reg, peer_node: peer_node} do
      local_leader = :dgen_registry.get_leader(reg)
      remote_leader = :erpc.call(peer_node, :dgen_registry, :get_leader, [reg])

      assert local_leader != :undefined
      assert local_leader == remote_leader
    end

    test "each node's member appears in get_members/1", %{reg: reg, peer_node: peer_node} do
      local_member = :dgen_registry.member_name(reg)
      remote_member = :erpc.call(peer_node, :dgen_registry, :member_name, [reg])

      # Members propagate asynchronously after the join is committed.
      assert eventually(fn ->
               members = :dgen_registry.get_members(reg)
               {node(), local_member} in members and {peer_node, remote_member} in members
             end),
             "expected both #{inspect({node(), local_member})} and " <>
               "#{inspect({peer_node, remote_member})} in members"
    end
  end

  # ---------------------------------------------------------------------------
  # Replication — primary → peer
  # ---------------------------------------------------------------------------

  describe "name replication from primary to peer" do
    test "name registered on primary eventually appears on peer (snapshot read)", %{
      reg: reg,
      peer_node: peer_node
    } do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      :yes = :dgen_registry.register_name({reg, :replicated}, pid)

      # Snapshot reads on the peer may lag slightly behind; poll.
      assert eventually(fn -> remote_whereis(peer_node, reg, :replicated) == pid end),
             "name was not replicated to peer"
    end

    test "consistent read on peer routes to leader and returns correct pid", %{
      reg: reg,
      peer_node: peer_node
    } do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      :yes = :dgen_registry.register_name({reg, :consistent}, pid)

      # The peer member may need a brief moment to learn the current leader
      # before it can route the consistent read correctly.
      assert eventually(
               fn -> remote_whereis_consistent(peer_node, reg, :consistent) == pid end,
               3_000
             ),
             "consistent read from peer did not return expected pid"
    end
  end

  # ---------------------------------------------------------------------------
  # Replication — peer → primary
  # ---------------------------------------------------------------------------

  describe "name replication from peer to primary" do
    test "name registered on peer is eventually visible on primary (snapshot read)", %{
      reg: reg,
      peer_node: peer_node
    } do
      remote_pid = spawn_remote(peer_node)

      :yes =
        :erpc.call(peer_node, :dgen_registry, :register_name, [{reg, :peer_name}, remote_pid])

      assert eventually(fn ->
               :dgen_registry.whereis_name({reg, :peer_name}) == remote_pid
             end),
             "peer-registered name was not replicated to primary"
    end

    test "consistent read on primary sees peer-registered name", %{
      reg: reg,
      peer_node: peer_node
    } do
      remote_pid = spawn_remote(peer_node)

      :yes =
        :erpc.call(peer_node, :dgen_registry, :register_name, [
          {reg, :peer_consistent},
          remote_pid
        ])

      # Allow leader routing to stabilise before asserting.
      assert eventually(
               fn ->
                 :dgen_registry.whereis_name_consistent({reg, :peer_consistent}) == remote_pid
               end,
               3_000
             ),
             "consistent read on primary did not return peer-registered pid"
    end
  end

  # ---------------------------------------------------------------------------
  # Snapshot distribution on join
  # ---------------------------------------------------------------------------

  describe "snapshot on join" do
    test "peer receives existing names when it joins after registration", %{
      abs_cluster_file: abs_cluster_file,
      dir_path: dir_path,
      db: db,
      case_dir: case_dir
    } do
      # Use a completely fresh registry just for this test so we can control
      # exactly which names exist before the late peer joins.
      suffix = :erlang.unique_integer([:positive])
      late_reg = :"late_reg_#{suffix}"
      late_peer_name = :"latepeer#{suffix}@127.0.0.1"

      {:ok, _} = :dgen_registry.start_link(late_reg, {db, case_dir})
      await_leader!(late_reg)

      # Register names BEFORE starting the late peer.
      pids =
        for i <- 1..3 do
          pid = spawn(fn -> Process.sleep(:infinity) end)
          on_exit(fn -> Process.exit(pid, :kill) end)
          name = :"snap_#{i}"
          :yes = :dgen_registry.register_name({late_reg, name}, pid)
          {name, pid}
        end

      # Start the late peer now.
      {:ok, late_peer_pid, late_peer_node} =
        :peer.start_link(%{name: late_peer_name, connection: :standard_io})

      on_exit(fn -> DGen.ClusterHelper.stop_peer(late_peer_pid) end)

      :erpc.call(late_peer_node, :code, :add_paths, [:code.get_path()])
      {:ok, _} = :erpc.call(late_peer_node, Application, :ensure_all_started, [:erlfdb])
      {:ok, _} = :erpc.call(late_peer_node, Application, :ensure_all_started, [:dgen])

      :ok =
        :erpc.call(late_peer_node, DGen.ClusterHelper, :start_registry, [
          late_reg,
          abs_cluster_file,
          dir_path
        ])

      :erpc.call(late_peer_node, DGen.ClusterHelper, :await_leader!, [late_reg])

      # After the snapshot is distributed, the late peer must see all pre-registered names.
      for {name, pid} <- pids do
        assert eventually(fn ->
                 :erpc.call(late_peer_node, :dgen_registry, :whereis_name, [
                   {late_reg, name}
                 ]) == pid
               end),
               "pre-join name #{inspect(name)} missing from late peer snapshot"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Auto-unregistration across nodes
  # ---------------------------------------------------------------------------

  describe "cross-node auto-unregistration" do
    test "death of a process on the primary removes its name from the peer", %{
      reg: reg,
      peer_node: peer_node
    } do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      :yes = :dgen_registry.register_name({reg, :dying_primary}, pid)

      assert eventually(fn -> remote_whereis(peer_node, reg, :dying_primary) == pid end),
             "name not yet replicated to peer"

      Process.exit(pid, :kill)

      assert eventually(fn ->
               remote_whereis(peer_node, reg, :dying_primary) == :undefined
             end),
             "peer still holds the unregistered name after process death"
    end

    test "death of a remote process removes its name from the primary", %{
      reg: reg,
      peer_node: peer_node
    } do
      remote_pid = spawn_remote(peer_node)

      :yes =
        :erpc.call(peer_node, :dgen_registry, :register_name, [
          {reg, :dying_remote},
          remote_pid
        ])

      assert eventually(fn ->
               :dgen_registry.whereis_name({reg, :dying_remote}) == remote_pid
             end),
             "peer-registered name not yet visible on primary"

      :erpc.call(peer_node, :erlang, :exit, [remote_pid, :kill])

      assert eventually(fn ->
               :dgen_registry.whereis_name({reg, :dying_remote}) == :undefined
             end),
             "primary still holds name after remote process death"
    end
  end

  # ---------------------------------------------------------------------------
  # Partition recovery
  # ---------------------------------------------------------------------------

  describe "partition recovery via nodeup" do
    test "cluster reconstitutes after Erlang distribution disconnects and reconnects", %{
      reg: reg,
      peer_node: peer_node
    } do
      # Capture before disconnect — erpc won't work across the partition.
      peer_member = :erpc.call(peer_node, :dgen_registry, :member_name, [reg])

      # Sever Erlang distribution between primary and peer.  Both erlang:monitor/2
      # references fire DOWN with :noconnection; each member submits {member_down,
      # other, token} to the elector via FDB, which remains accessible to both nodes.
      :net_kernel.disconnect(peer_node)

      # The primary must detect the peer's departure and remove it from the
      # elector's member set.
      assert eventually(
               fn ->
                 not Enum.any?(:dgen_registry.get_members(reg), fn {n, _} -> n == peer_node end)
               end,
               5_000
             ),
             "primary did not remove peer from member set after disconnect"

      # Reconnect.  Both members receive {nodeup, _} and re-cast {join, Self, FreshToken}
      # to the elector, which reconstitutes the full member set and re-elects a leader.
      # Each fresh token ensures that any stale {member_down, ..., OldToken} still in the
      # FDB queue is discarded by the elector when it is eventually consumed.
      Node.connect(peer_node)

      # Primary: both members and a leader must reappear.
      assert eventually(
               fn ->
                 members = :dgen_registry.get_members(reg)

                 :dgen_registry.get_leader(reg) != :undefined and
                   {peer_node, peer_member} in members
               end,
               5_000
             ),
             "primary did not reconstitute cluster after reconnect"

      # Peer: leader must reappear.  Wrap in try/catch because distribution
      # may briefly be in the process of reconnecting when we first poll.
      assert eventually(
               fn ->
                 try do
                   :erpc.call(peer_node, :dgen_registry, :get_leader, [reg]) != :undefined
                 catch
                   :exit, _ -> false
                 end
               end,
               5_000
             ),
             "peer did not see a leader after reconnect"

      # Registry must be fully functional: register on primary, replicate to peer.
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      assert eventually(
               fn -> :dgen_registry.register_name({reg, :post_partition}, pid) == :yes end,
               5_000
             ),
             "could not register name after partition recovery"

      assert eventually(
               fn -> remote_whereis(peer_node, reg, :post_partition) == pid end,
               5_000
             ),
             "name not replicated to peer after partition recovery"
    end
  end

  # ---------------------------------------------------------------------------
  # Epoch fencing across nodes
  # ---------------------------------------------------------------------------

  describe "epoch fencing" do
    test "stale name_registered broadcast from a fake leader is rejected by the peer", %{
      reg: reg,
      peer_node: peer_node
    } do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      epoch = :dgen_registry.get_epoch(reg)
      assert epoch > 0

      peer_member = :erpc.call(peer_node, :dgen_registry, :member_name, [reg])

      # Inject a broadcast carrying a stale epoch (epoch - 1) directly into
      # the peer member's mailbox, bypassing the real leader.
      :erpc.call(peer_node, :gen_server, :cast, [
        peer_member,
        {:name_registered, :ghost_name, pid, epoch - 1}
      ])

      # A call to the same process serialises after the cast.
      :erpc.call(peer_node, :dgen_registry, :whereis_name, [{reg, :__barrier__}])

      assert :undefined == remote_whereis(peer_node, reg, :ghost_name)
    end

    test "name_registered broadcast with current epoch is accepted by the peer", %{
      reg: reg,
      peer_node: peer_node
    } do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      epoch = :dgen_registry.get_epoch(reg)
      peer_member = :erpc.call(peer_node, :dgen_registry, :member_name, [reg])

      :erpc.call(peer_node, :gen_server, :cast, [
        peer_member,
        {:name_registered, :valid_name, pid, epoch}
      ])

      assert pid == :erpc.call(peer_node, :dgen_registry, :whereis_name, [{reg, :valid_name}])
    end

    test "epoch increments when a new leader is elected after the peer joins", %{
      reg: reg,
      peer_node: peer_node
    } do
      # The initial join of the peer already triggered at least one election,
      # so epoch must be > 0 on both nodes.
      local_epoch = :dgen_registry.get_epoch(reg)
      remote_epoch = :erpc.call(peer_node, :dgen_registry, :get_epoch, [reg])

      assert local_epoch > 0
      assert local_epoch == remote_epoch
    end
  end

  # ---------------------------------------------------------------------------
  # Via-tuple across nodes
  # ---------------------------------------------------------------------------

  describe "via-tuple across nodes" do
    test "a GenServer started on the primary can be called from the peer via name", %{
      reg: reg,
      peer_node: peer_node
    } do
      via_name = via(reg, :cross_ping)
      {:ok, pid} = PingServer.start_link(via_name)

      on_exit(fn ->
        try do
          GenServer.stop(pid, :shutdown)
        catch
          :exit, _ -> :ok
        end
      end)

      # The peer should be able to resolve the name and deliver the call.
      # Use gen_server:call directly to avoid loading an Elixir test module on the peer.
      result = :erpc.call(peer_node, :gen_server, :call, [via_name, :ping])
      assert result == :pong
    end
  end
end
