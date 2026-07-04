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
    _peer_sup =
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

  # The peer member's current applied version (via the same snapshot call the
  # handoff gather uses), so a test can craft version-contiguous broadcasts.
  defp peer_applied_version(peer_node, peer_member) do
    {_records, version, _released} =
      :erpc.call(peer_node, GenServer, :call, [peer_member, :get_names_snapshot])

    version
  end

  # Spawn a long-lived process on a remote node without Elixir dependency.
  # Uses :timer.sleep so no Elixir module is required on the peer.
  defp spawn_remote(node) do
    :erpc.call(node, :erlang, :spawn, [:timer, :sleep, [:infinity]])
  end

  # Boots a 3-member cluster where `leader_node` becomes leader *before* the
  # primary or the second peer join — sticky leadership then keeps it in
  # charge once they do — so a test can kill the leader without having to
  # kill the test process itself (the primary always joins first otherwise,
  # and would always be the leader). Returns `{reg, leader_node, leader_pid,
  # leader_sup, other_node, other_pid}` — `leader_pid`/`other_pid` are the
  # peer nodes' controlling pids (from `boot_peer!/1`, for `stop_peer/1`);
  # `leader_sup` is the leader's registry supervisor pid (from
  # `start_registry/3`, for `stop_registry/1`). Ignores the outer setup's own
  # `reg`/`peer_node`; only the FDB connection details are reused, following
  # the pattern already used by the "snapshot on join" and "lock-free
  # handoff" tests.
  defp start_peer_led_cluster!(%{
         abs_cluster_file: abs_cluster_file,
         dir_path: dir_path,
         db: db,
         case_dir: case_dir
       }) do
    suffix = :erlang.unique_integer([:positive])
    reg = :"failover_reg_#{suffix}"

    {leader_pid, leader_node} = DGen.ClusterHelper.boot_peer!("failoverleader")

    leader_sup =
      :erpc.call(leader_node, DGen.ClusterHelper, :start_registry, [
        reg,
        abs_cluster_file,
        dir_path
      ])

    :erpc.call(leader_node, DGen.ClusterHelper, :await_leader!, [reg])

    # Primary joins second — sticky leadership keeps leader_node in charge.
    {:ok, _} = :dgen_registry.start_link(reg, {db, case_dir})
    await_leader!(reg)

    {other_pid, other_node} = DGen.ClusterHelper.boot_peer!("failoverother")

    _other_sup =
      :erpc.call(other_node, DGen.ClusterHelper, :start_registry, [
        reg,
        abs_cluster_file,
        dir_path
      ])

    :erpc.call(other_node, DGen.ClusterHelper, :await_leader!, [reg])

    # All three must agree leader_node is leader before a test proceeds.
    assert eventually(
             fn ->
               leader = :dgen_registry.get_leader(reg)

               match?({^leader_node, _}, leader) and
                 :erpc.call(other_node, :dgen_registry, :get_leader, [reg]) == leader
             end,
             5_000
           ),
           "expected #{inspect(leader_node)} to be the elected leader"

    on_exit(fn -> DGen.ClusterHelper.stop_peer(leader_pid) end)
    on_exit(fn -> DGen.ClusterHelper.stop_peer(other_pid) end)

    {reg, leader_node, leader_pid, leader_sup, other_node, other_pid}
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

    test "metadata registered on peer is eventually visible on primary", %{
      reg: reg,
      peer_node: peer_node
    } do
      remote_pid = spawn_remote(peer_node)
      spec = %{index: %{role: :worker}, data: :payload}

      :yes =
        :erpc.call(peer_node, :dgen_registry, :register_name, [
          {reg, :peer_meta},
          remote_pid,
          spec
        ])

      assert eventually(fn ->
               :dgen_registry.get_metadata({reg, :peer_meta}) ==
                 {:ok, %{pid: remote_pid, index: %{role: :worker}, data: :payload}}
             end),
             "peer-registered metadata was not replicated to primary"
    end

    test "set_metadata on primary is eventually visible on peer", %{
      reg: reg,
      peer_node: peer_node
    } do
      remote_pid = spawn_remote(peer_node)
      :yes = :dgen_registry.register_name({reg, :meta_to_peer}, remote_pid)

      :ok = :dgen_registry.set_metadata({reg, :meta_to_peer}, %{index: %{x: 1}, data: :d})

      assert eventually(fn ->
               :erpc.call(peer_node, :dgen_registry, :get_metadata, [{reg, :meta_to_peer}]) ==
                 {:ok, %{pid: remote_pid, index: %{x: 1}, data: :d}}
             end),
             "metadata set on primary did not replicate to peer"
    end

    test "a query on the primary finds a peer-registered indexed registration", %{
      reg: reg,
      peer_node: peer_node
    } do
      remote_pid = spawn_remote(peer_node)
      spec = %{index: %{role: :indexer}}

      :yes =
        :erpc.call(peer_node, :dgen_registry, :register_name, [{reg, :peer_q}, remote_pid, spec])

      # The primary rebuilds its inverted index from the replication broadcast, so the
      # local snapshot query eventually finds the peer's registration.
      assert eventually(fn ->
               case :dgen_registry.query(reg, %{role: :indexer}) do
                 [%{name: :peer_q, pid: ^remote_pid}] -> true
                 _ -> false
               end
             end),
             "peer-registered indexed name not found by primary query"
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

      _late_peer_sup =
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
  # Lock-free handoff (§5.7) — the distributed lock that used to serialize the
  # handoff is gone; the leader-key commit fences the old leader instead, so the
  # window is naturally quiescent. This guards the main worry of removing it:
  # back-to-back membership changes the lock used to serialize must still each
  # gather + distribute correctly, with no split-brain.
  # ---------------------------------------------------------------------------

  describe "lock-free handoff (§5.7)" do
    test "back-to-back joins each receive the snapshot and all nodes agree on one leader",
         %{abs_cluster_file: abs_cluster_file, dir_path: dir_path, db: db, case_dir: case_dir} do
      suffix = :erlang.unique_integer([:positive])
      reg = :"lockfree_reg_#{suffix}"

      {:ok, _} = :dgen_registry.start_link(reg, {db, case_dir})
      await_leader!(reg)

      # Pre-register names so a joining member must receive them via the snapshot
      # that the post-commit handoff action distributes.
      pids =
        for i <- 1..3 do
          pid = spawn(fn -> Process.sleep(:infinity) end)
          on_exit(fn -> Process.exit(pid, :kill) end)
          name = :"lf_#{i}"
          :yes = :dgen_registry.register_name({reg, name}, pid)
          {name, pid}
        end

      # Boot two peers, then join them back-to-back so the elector processes two
      # membership changes in quick succession — the case the distributed lock used
      # to serialize. Without the lock each join's post-commit action still gathers
      # and distributes; nothing pauses the other consumers.
      peer_nodes =
        for j <- 1..2 do
          pname = :"lfpeer#{suffix}_#{j}@127.0.0.1"
          {:ok, ppid, pnode} = :peer.start_link(%{name: pname, connection: :standard_io})
          on_exit(fn -> DGen.ClusterHelper.stop_peer(ppid) end)
          :erpc.call(pnode, :code, :add_paths, [:code.get_path()])
          {:ok, _} = :erpc.call(pnode, Application, :ensure_all_started, [:erlfdb])
          {:ok, _} = :erpc.call(pnode, Application, :ensure_all_started, [:dgen])
          pnode
        end

      for pnode <- peer_nodes do
        _sup =
          :erpc.call(pnode, DGen.ClusterHelper, :start_registry, [
            reg,
            abs_cluster_file,
            dir_path
          ])
      end

      for pnode <- peer_nodes do
        :erpc.call(pnode, DGen.ClusterHelper, :await_leader!, [reg])
      end

      # Every joined peer eventually holds the full pre-registration snapshot.
      for pnode <- peer_nodes, {name, pid} <- pids do
        assert eventually(fn ->
                 :erpc.call(pnode, :dgen_registry, :whereis_name, [{reg, name}]) == pid
               end),
               "name #{inspect(name)} missing from #{inspect(pnode)} after back-to-back joins"
      end

      # All nodes agree on a single leader — no split-brain from the unlocked handoff.
      leader = :dgen_registry.get_leader(reg)
      assert leader != :undefined

      for pnode <- peer_nodes do
        assert eventually(fn ->
                 :erpc.call(pnode, :dgen_registry, :get_leader, [reg]) == leader
               end),
               "#{inspect(pnode)} disagrees on the leader after back-to-back joins"
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
  # Leader failure and failover (§5.1, §5.5, §5.7)
  #
  # Every other test in this file disconnects or reconnects the *peer* — the
  # primary always keeps leadership (sticky, and it joins first). None of them
  # exercise the registry's most distinctive claim: that the leader itself can
  # die and the survivors elect a replacement without losing a two-holder
  # binding. Testing that honestly requires a peer, not the primary, to hold
  # leadership in the first place — see start_peer_led_cluster!/1.
  # ---------------------------------------------------------------------------

  describe "leader failure and failover" do
    test "a graceful leader shutdown fails over without losing a registration", context do
      {reg, leader_node, _leader_pid, leader_sup, other_node, _other_pid} =
        start_peer_led_cluster!(context)

      old_leader = :dgen_registry.get_leader(reg)

      # Register from the primary — a *forwarded* registration, so it is
      # two-holder for free (leader + the forwarding primary) the instant it
      # acks `yes` (§5.5) — well before the leader is touched.
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)
      :yes = :dgen_registry.register_name({reg, :survives_graceful}, pid)

      # Stop just the leader's registry supervisor — its node stays up and
      # connected, so peers observe an ordinary process DOWN, not a nodedown.
      :ok = :erpc.call(leader_node, DGen.ClusterHelper, :stop_registry, [leader_sup])

      assert eventually(
               fn ->
                 primary_leader = :dgen_registry.get_leader(reg)
                 other_leader = :erpc.call(other_node, :dgen_registry, :get_leader, [reg])

                 primary_leader not in [:undefined, old_leader] and
                   primary_leader == other_leader
               end,
               20_000
             ),
             "primary and surviving peer did not agree on a new leader " <>
               "after the graceful shutdown"

      # The registration survives — held independently by the primary, which
      # was reachable throughout, regardless of which survivor became leader.
      assert eventually(fn ->
               :dgen_registry.whereis_name_consistent({reg, :survives_graceful}) == pid
             end),
             "registration did not survive the graceful leader shutdown"

      # The registry is writable again post-failover.
      new_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(new_pid, :kill) end)

      assert eventually(fn ->
               :dgen_registry.register_name({reg, :post_graceful_failover}, new_pid) == :yes
             end),
             "could not register a new name after the graceful failover"
    end

    test "abruptly losing the leader node fails over without losing a registration", context do
      {reg, _leader_node, leader_pid, _leader_sup, other_node, _other_pid} =
        start_peer_led_cluster!(context)

      old_leader = :dgen_registry.get_leader(reg)

      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)
      :yes = :dgen_registry.register_name({reg, :survives_hard_kill}, pid)

      # Pull the plug: the whole leader node disappears (unlike the graceful
      # case above, where only its registry supervisor stopped). Survivors
      # see this as a nodedown, not a clean process exit.
      DGen.ClusterHelper.stop_peer(leader_pid)

      assert eventually(
               fn ->
                 primary_leader = :dgen_registry.get_leader(reg)
                 other_leader = :erpc.call(other_node, :dgen_registry, :get_leader, [reg])

                 primary_leader not in [:undefined, old_leader] and
                   primary_leader == other_leader
               end,
               8_000
             ),
             "primary and surviving peer did not agree on a new leader " <>
               "after the leader node died"

      assert eventually(fn ->
               :dgen_registry.whereis_name_consistent({reg, :survives_hard_kill}) == pid
             end),
             "registration did not survive the abrupt leader failure"

      new_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(new_pid, :kill) end)

      assert eventually(fn ->
               :dgen_registry.register_name({reg, :post_hard_failover}, new_pid) == :yes
             end),
             "could not register a new name after the abrupt failover"
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

      leader = :dgen_registry.get_leader(reg)
      peer_member = :erpc.call(peer_node, :dgen_registry, :member_name, [reg])
      v = peer_applied_version(peer_node, peer_member)

      # Inject a broadcast carrying a stale epoch (epoch - 1) directly into
      # the peer member's mailbox, bypassing the real leader.  The version stamps
      # are contiguous, so only the stale epoch causes the discard.
      :erpc.call(peer_node, :gen_server, :cast, [
        peer_member,
        {:name_registered, :ghost_name, pid, %{}, :undefined, epoch - 1, v, v + 1, leader}
      ])

      # whereis_name now reads the member's ETS table in the caller, so it no longer
      # serialises behind the cast — flush the peer member's mailbox with get_state.
      :erpc.call(peer_node, :sys, :get_state, [peer_member])

      assert :undefined == remote_whereis(peer_node, reg, :ghost_name)
    end

    test "name_registered broadcast with current epoch is accepted by the peer", %{
      reg: reg,
      peer_node: peer_node
    } do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      epoch = :dgen_registry.get_epoch(reg)
      leader = :dgen_registry.get_leader(reg)
      peer_member = :erpc.call(peer_node, :dgen_registry, :member_name, [reg])
      v = peer_applied_version(peer_node, peer_member)

      :erpc.call(peer_node, :gen_server, :cast, [
        peer_member,
        {:name_registered, :valid_name, pid, %{}, :undefined, epoch, v, v + 1, leader}
      ])

      # Barrier: ensure the peer member processed the cast before the caller-side read.
      :erpc.call(peer_node, :sys, :get_state, [peer_member])

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
