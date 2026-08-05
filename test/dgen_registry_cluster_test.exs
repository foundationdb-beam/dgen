defmodule DGen.RegistryClusterTest do
  # Cluster tests modify global node/distribution state and start FDB
  # transactions from multiple nodes — run sequentially.
  use DGen.Case, async: false

  # Starts real peer BEAM nodes that open the backend themselves, so this suite
  # needs a genuinely shared database. Excluded when running on `dgen_mem`, which
  # is per-VM ETS.
  @moduletag :cluster

  require Logger

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

  # A replication broadcast in the shape the leader actually sends: one
  # `{:names_batch, Ops, Epoch, PrevV, Version, LeaderId}` message per committed
  # batch, carrying the batch's ops. A batch is delivered whole or not at all —
  # see `broadcast_batch/5` in dgen_registry_member.
  defp names_batch(ops, epoch, prev_v, version, leader) do
    {:names_batch, ops, epoch, prev_v, version, leader}
  end

  # Spawn a long-lived process on a remote node without Elixir dependency.
  # Uses :timer.sleep so no Elixir module is required on the peer.
  defp spawn_remote(node) do
    :erpc.call(node, :erlang, :spawn, [:timer, :sleep, [:infinity]])
  end

  # Run an MFA on one side of a partition: locally on the primary, or on the
  # peer via the :peer stdio control channel — which works while Erlang
  # distribution is severed and, unlike erpc, cannot reconnect (and thereby
  # heal) the partition under test.
  defp side_call(:primary, _peer_pid, m, f, a), do: apply(m, f, a)
  defp side_call(:peer, peer_pid, m, f, a), do: :peer.call(peer_pid, m, f, a, 8_000)

  defp side_spawn(:primary, _peer_pid), do: spawn(fn -> Process.sleep(:infinity) end)

  defp side_spawn(:peer, peer_pid) do
    :peer.call(peer_pid, :erlang, :spawn, [:timer, :sleep, [:infinity]])
  end

  # Raw materials for `cp_refusals_hold?/4` and the diagnostic dump below: issue
  # both probes against `side` and return what actually came back, uncollapsed,
  # so a diagnostic snapshot can show the real register/read results instead of
  # just the pass/fail verdict.
  defp raw_cp_probe(side, peer_pid, reg, held_name) do
    probe_pid = side_spawn(side, peer_pid)
    probe_name = {:cp_probe, :erlang.unique_integer([:positive])}

    register =
      try do
        side_call(side, peer_pid, :dgen_registry, :register_name, [{reg, probe_name}, probe_pid])
      catch
        # Blocked past register_timeout and exited — a refusal.
        kind, reason -> {:caught, kind, reason}
      end

    read =
      try do
        side_call(side, peer_pid, :dgen_registry, :whereis_name_consistent, [{reg, held_name}])
      catch
        kind, reason -> {:caught, kind, reason}
      end

    %{register: register, read: read}
  end

  # Do both CP refusals hold on `side` right now?
  #
  # (1) A write must NOT succeed (`:yes`). Depending on what that side believes,
  #     it either returns an adjudicated `:no` — it still thinks it leads and its
  #     commit aborts on the durable leader-key fence — or, knowing no leader is
  #     reachable, it BLOCKS and its register_timeout exits (register never
  #     returns a false `:no` any more). Both are CP refusals; only `:yes` is a
  #     breach. (The test shrinks register_timeout so the blocking case resolves
  #     quickly.)
  # (2) A consistent read of a name the side's frozen replica still holds answers
  #     `:undefined` (Guarantee 5's denial path) — never the stale pid as
  #     authoritative.
  defp cp_refusals_hold?(side, peer_pid, reg, held_name) do
    %{register: register, read: read} = raw_cp_probe(side, peer_pid, reg, held_name)
    register != :yes and read == :undefined
  end

  # Best-effort forensic snapshot for when the CP-refusal settle poll times out.
  # Logged (not printed) so ExUnit's `capture_log` only surfaces it on a failing
  # test — passing runs never see this. Every call here is independently
  # try/rescued: a diagnostic probe must never itself raise and obscure the
  # original assertion failure. Taken immediately after the settle poll gives
  # up, so it reflects the stuck state, not necessarily the exact instant of
  # the 30s deadline — but the poll ran every 20ms right up to it, so the two
  # are effectively the same moment.
  defp log_cp_settle_diagnostics(peer_node, peer_pid, reg, held_name) do
    primary_view = %{
      leader: safe(fn -> :dgen_registry.get_leader(reg) end),
      epoch: safe(fn -> :dgen_registry.get_epoch(reg) end),
      members: safe(fn -> :dgen_registry.get_members(reg) end),
      connected_nodes: safe(fn -> Node.list() end)
    }

    peer_view = %{
      leader: safe(fn -> :peer.call(peer_pid, :dgen_registry, :get_leader, [reg], 5_000) end),
      epoch: safe(fn -> :peer.call(peer_pid, :dgen_registry, :get_epoch, [reg], 5_000) end),
      members: safe(fn -> :peer.call(peer_pid, :dgen_registry, :get_members, [reg], 5_000) end),
      connected_nodes: safe(fn -> :peer.call(peer_pid, :erlang, :nodes, [], 5_000) end)
    }

    primary_probe = safe(fn -> raw_cp_probe(:primary, peer_pid, reg, held_name) end)
    peer_probe = safe(fn -> raw_cp_probe(:peer, peer_pid, reg, held_name) end)

    Logger.error("""
    CP-refusal settle timed out — post-failure diagnostic snapshot:
      primary (#{inspect(node())}): #{inspect(primary_view)}
      peer    (#{inspect(peer_node)}): #{inspect(peer_view)}
      primary probe (writes/reads issued locally):        #{inspect(primary_probe)}
      peer probe (writes/reads issued via :peer channel):  #{inspect(peer_probe)}
    """)
  end

  defp safe(fun) do
    fun.()
  catch
    kind, reason -> {:caught, kind, reason}
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

    test "await_ready returns ok on both the primary and the joined peer", %{
      reg: reg,
      peer_node: peer_node
    } do
      assert :ok == :dgen_registry.await_ready(reg, 5_000)
      assert :ok == :erpc.call(peer_node, :dgen_registry, :await_ready, [reg, 5_000])
    end

    # The continuing-leader fast path: a second peer joins a cluster that already
    # holds names, handled by the primary as the *continuing* leader. It must
    # onboard the joiner fully, leave the existing follower's replica intact, and
    # keep serving registrations to all three nodes.
    test "a third node joins a populated cluster and onboards fully", %{
      reg: reg,
      peer_node: peer_node,
      abs_cluster_file: abs_cluster_file,
      dir_path: dir_path
    } do
      # Register a handful of names on the primary; the existing peer replicates them.
      names =
        for i <- 1..5 do
          pid = spawn(fn -> Process.sleep(:infinity) end)
          on_exit(fn -> Process.exit(pid, :kill) end)
          assert :yes == :dgen_registry.register_name({reg, {:pre, i}}, pid)
          {{:pre, i}, pid}
        end

      assert eventually(fn -> remote_whereis(peer_node, reg, {:pre, 1}) != :undefined end),
             "existing follower never replicated the pre-existing names"

      # Bring up a THIRD node and start the registry — its join is served by the
      # primary as the continuing leader (the fast path).
      {peer2_pid, peer2_node} = DGen.ClusterHelper.boot_peer!("thirdnode")
      on_exit(fn -> DGen.ClusterHelper.stop_peer(peer2_pid) end)

      :erpc.call(peer2_node, DGen.ClusterHelper, :start_registry, [
        reg,
        abs_cluster_file,
        dir_path
      ])

      assert :ok == :erpc.call(peer2_node, :dgen_registry, :await_ready, [reg, 15_000])

      # The third node onboarded every pre-existing name ...
      for {name, pid} <- names do
        assert eventually(fn -> remote_whereis(peer2_node, reg, name) == pid end),
               "third node did not onboard #{inspect(name)}"
      end

      # ... the existing follower still holds them (its replica was not wiped) ...
      for {name, pid} <- names do
        assert remote_whereis(peer_node, reg, name) == pid,
               "existing follower lost #{inspect(name)} on the third node's join"
      end

      # ... and a new registration propagates to all three nodes.
      np = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(np, :kill) end)
      assert :yes == :dgen_registry.register_name({reg, :after_third}, np)

      assert eventually(fn -> remote_whereis(peer_node, reg, :after_third) == np end),
             "existing follower did not see a registration made after the third node joined"

      assert eventually(fn -> remote_whereis(peer2_node, reg, :after_third) == np end),
             "third node did not see a registration made after it joined"
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
  # Presence — replication and cross-node notification (§4.9)
  # ---------------------------------------------------------------------------

  describe "presence across nodes" do
    test "a subscription is durable elector state, visible from the peer", %{
      reg: reg,
      peer_node: peer_node
    } do
      :ok = :dgen_registry.subscribe(reg, :cross, %{role: :worker}, %{group: :l})

      # Subscriptions live in the elector's durable, cluster-shared state, so the peer's
      # subscriptions/1 sees the same subscription (the elector is one logical entity
      # across nodes).
      assert eventually(fn ->
               subs = :erpc.call(peer_node, :dgen_registry, :subscriptions, [reg])
               Map.has_key?(subs, :cross)
             end),
             "subscription not visible in the peer's durable elector state"
    end

    test "a peer-node registration reaches a primary notify target's presence feed", %{
      reg: reg,
      peer_node: peer_node
    } do
      # `self()` (on the primary) is the notify target.
      :yes =
        :dgen_registry.register_name({reg, {:listener, self()}}, self(), %{index: %{group: :l}})

      # Register the worker on the *peer* first, then subscribe: the subscribe's initial
      # snapshot (computed on the leader once the durable cast lands) sees the already-
      # replicated peer-node registration and delivers it to the primary target. Doing it
      # in this order avoids racing the asynchronous subscribe against the registration —
      # register_name returned :yes, so the leader already holds :peer_worker.
      remote_pid = spawn_remote(peer_node)

      :yes =
        :erpc.call(peer_node, :dgen_registry, :register_name, [
          {reg, :peer_worker},
          remote_pid,
          %{index: %{role: :worker}}
        ])

      :ok = :dgen_registry.subscribe(reg, :peer_change, %{role: :worker}, %{group: :l})

      assert_receive {:dgen_presence, :peer_change, [{:joined, :peer_worker, ^remote_pid}]}, 5_000
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
      #
      # The most generous budget in this file, and the slowest step in it: the
      # disconnect has to produce two `:noconnection` DOWNs, each member has to
      # enqueue `{member_down, Other, Token}` into the elector's **FDB-backed**
      # queue, and the elector has to consume it and rewrite the member set. That
      # is several FoundationDB round trips on a machine that is also hosting the
      # database it is talking to, which is exactly what a CI runner is.
      #
      # It failed there at 5s having never failed locally -- not in isolation, not
      # in the full suite, and not under 3x CPU oversubscription. The property is
      # *eventual* reconstitution rather than a latency bound, so the number is
      # arbitrary and a larger one costs a passing run nothing: `eventually/2`
      # polls every 20ms and returns the moment the condition holds.
      assert eventually(
               fn ->
                 not Enum.any?(:dgen_registry.get_members(reg), fn {n, _} -> n == peer_node end)
               end,
               15_000
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
  # CP refusals during a distribution-only partition (§5.3, Guarantees 5 and 7)
  #
  # The Erlang mesh is severed while the database stays reachable from both
  # sides. The sides briefly contend over the single durable leadership record
  # and then settle (sticky leadership; the leader-liveness probe never reaps a
  # once-connected node, so the losing side cannot keep deposing the winner
  # through the shared database). Whichever side ends up NOT holding leadership
  # must exhibit the CP refusals — and this is asserted through the :peer stdio
  # channel, since an erpc to the disconnected side would heal the partition.
  # ---------------------------------------------------------------------------

  describe "CP refusals during a distribution-only partition" do
    test "the side without leadership refuses writes and fences consistent reads", %{
      reg: reg,
      peer_node: peer_node,
      peer_pid: peer_pid
    } do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      :yes = :dgen_registry.register_name({reg, :cp_name}, pid)

      assert eventually(fn -> remote_whereis(peer_node, reg, :cp_name) == pid end),
             ":cp_name never replicated to the peer"

      # A CP write refusal on the non-leader side now *blocks* (no reachable leader)
      # and leans on register_timeout rather than returning a false :no. Shrink that
      # timeout on both nodes so the settle-poll below iterates quickly; restore after.
      prev = Application.get_env(:dgen, :register_timeout)
      Application.put_env(:dgen, :register_timeout, 800)
      :erpc.call(peer_node, Application, :put_env, [:dgen, :register_timeout, 800])

      on_exit(fn ->
        case prev do
          nil -> Application.delete_env(:dgen, :register_timeout)
          v -> Application.put_env(:dgen, :register_timeout, v)
        end
      end)

      :net_kernel.disconnect(peer_node)

      # Settle + assert in one self-stabilising poll: read the durable-backed
      # leader view, pick the refusing side, and require both refusals to hold
      # there simultaneously. Mid-storm iterations simply return false.
      primary = node()

      settled? =
        eventually(
          fn ->
            # dgen's own mesh actively reconnects a still-committed member roughly
            # MESH_DOWN_COOLDOWN..MESH_DOWN_COOLDOWN + MESH_INTERVAL (10-20s) after a
            # nodedown, by design (dgen_registry_connector.erl) -- this test's peer
            # never leaves the durable member set, so it is exactly the case that
            # self-heal targets. Left unchecked, that reconnect races this 30s
            # observation window: under load (slower leader settle eating into the
            # window) it can silently heal the simulated partition before, or while,
            # we're checking for the refusal -- producing a fully converged, agreeing
            # cluster at diagnostic time instead of a stuck one (see the flake this
            # replaced). Re-assert the disconnect on every tick so the partition is
            # continuously sustained regardless of how long settling takes; probing
            # the peer still works because it goes over the separate :peer stdio
            # channel, not Erlang distribution.
            :net_kernel.disconnect(peer_node)

            case :dgen_registry.get_leader(reg) do
              {^primary, _} -> cp_refusals_hold?(:peer, peer_pid, reg, :cp_name)
              {^peer_node, _} -> cp_refusals_hold?(:primary, peer_pid, reg, :cp_name)
              _ -> false
            end
          end,
          30_000
        )

      unless settled? do
        log_cp_settle_diagnostics(peer_node, peer_pid, reg, :cp_name)
      end

      assert settled?, "the non-leader side never settled into the CP refusals"

      # Heal: the cluster reconstitutes and serves writes again.
      Node.connect(peer_node)

      new_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(new_pid, :kill) end)

      assert eventually(
               fn ->
                 try do
                   :dgen_registry.register_name({reg, :post_cp}, new_pid) == :yes
                 catch
                   _, _ -> false
                 end
               end,
               15_000
             ),
             "could not register a new name after the partition healed"

      assert eventually(fn -> remote_whereis(peer_node, reg, :post_cp) == new_pid end, 10_000),
             "post-heal registration did not replicate to the peer"
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

    # T4: the *direct* registration path (§5.5) — a registration originating on
    # the leader's own node has only the leader as holder at commit time, so the
    # leader must wait for a follower's version-guarded replicate_ack before
    # answering `yes` (replicate-before-ack). This is the half of the two-holder
    # invariant the forwarded-registration failover tests above cannot reach.
    # After the ack, at least one follower provably holds the binding at a
    # version the handoff gather sees — so killing the leader must not lose it.
    test "a direct (leader-origin) registration survives the leader's death", context do
      {reg, leader_node, leader_pid, _leader_sup, other_node, _other_pid} =
        start_peer_led_cluster!(context)

      old_leader = :dgen_registry.get_leader(reg)

      # The pid lives on the primary (it must outlive the leader node), but the
      # registration is made ON the leader node — the direct path.
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      assert :yes ==
               :erpc.call(leader_node, :dgen_registry, :register_name, [
                 {reg, :direct_survivor},
                 pid
               ])

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
             "survivors did not agree on a new leader after the leader node died"

      # The replicate-before-ack machinery guaranteed a follower held the binding
      # when `yes` was answered, so the freshest-wins gather must have carried it.
      assert eventually(fn ->
               :dgen_registry.whereis_name_consistent({reg, :direct_survivor}) == pid
             end),
             "direct registration was lost with the leader despite the replica ack"
    end
  end

  # ---------------------------------------------------------------------------
  # Conflict resolution by termination (§5.6) — end to end
  #
  # The unit suite pins the detect_conflicts/3 predicate; this test drives the
  # whole repair path: a member holding a divergent live binding for a name the
  # rest of the cluster assigns to a different live pid, exposed at a
  # leadership-change gather, must get BOTH processes terminated and the name
  # dropped (kill-both, §5.6). The divergence is created deterministically by
  # injecting a crafted contiguous {name_registered} broadcast into one member —
  # the same technique the epoch-fencing tests use — rather than by racing a real
  # partition, so the test does not depend on partition timing.
  # ---------------------------------------------------------------------------

  describe "conflict resolution by termination (§5.6)" do
    @tag capture_log: true
    test "a leadership-change gather kills both claimants of a diverged name", context do
      {reg, _leader_node, leader_pid, _leader_sup, other_node, _other_pid} =
        start_peer_led_cluster!(context)

      # p1 is the legitimate, cluster-wide owner; p2 the divergent claimant.
      # Both live on the primary so they survive the leader's death and are
      # alive when the new leader's gather adjudicates.
      p1 = spawn(fn -> Process.sleep(:infinity) end)
      p2 = spawn(fn -> Process.sleep(:infinity) end)

      on_exit(fn ->
        Process.exit(p1, :kill)
        Process.exit(p2, :kill)
      end)

      assert :yes == :dgen_registry.register_name({reg, :conflicted}, p1)

      # Every member must hold p1 before the divergence is injected, so the
      # gather sees p1 from the other follower.
      assert eventually(fn ->
               :erpc.call(other_node, :dgen_registry, :whereis_name, [{reg, :conflicted}]) == p1
             end),
             "p1's registration never replicated to the other follower"

      # Divergence: overwrite the PRIMARY's replica row with p2 via a crafted
      # broadcast that is contiguous with its stream (v -> v+1), making the
      # primary both divergent AND the freshest replica by version — exactly the
      # shape a lagging/partitioned member presents at a gather.
      member = :dgen_registry.member_name(reg)
      epoch = :dgen_registry.get_epoch(reg)
      leader = :dgen_registry.get_leader(reg)
      {_records, v, _released} = GenServer.call(member, :get_names_snapshot)

      GenServer.cast(
        member,
        names_batch(
          [{:name_registered, :conflicted, p2, %{}, :undefined}],
          epoch,
          v,
          v + 1,
          leader
        )
      )

      :sys.get_state(member)
      assert p2 == :dgen_registry.whereis_name({reg, :conflicted})

      # Kill the leader: the failover gather collects the primary (freshest,
      # authority = p2) and the other follower (still reporting p1, live and not
      # in the release trail) — a genuine §5.6 conflict. Kill-both must fire.
      DGen.ClusterHelper.stop_peer(leader_pid)

      assert eventually(
               fn -> not Process.alive?(p1) and not Process.alive?(p2) end,
               15_000
             ),
             "kill-both did not terminate both claimants " <>
               "(p1 alive: #{Process.alive?(p1)}, p2 alive: #{Process.alive?(p2)})"

      # The conflicted name was dropped from the reconstructed table everywhere.
      assert eventually(fn ->
               :dgen_registry.whereis_name({reg, :conflicted}) == :undefined and
                 :erpc.call(other_node, :dgen_registry, :whereis_name, [{reg, :conflicted}]) ==
                   :undefined
             end),
             "conflicted name was not cleared after kill-both"

      # Supervised processes would now re-register cleanly; simulate that.
      p3 = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(p3, :kill) end)

      assert eventually(fn ->
               :dgen_registry.register_name({reg, :conflicted}, p3) == :yes
             end),
             "name could not be re-registered after conflict resolution"
    end

    # The other §5.6 trigger point: a previously-synced member REJOINS while the
    # leader stays put (the common partition-heal shape — no leadership change,
    # so no full handoff gather runs). The continuing leader must gather the
    # rejoiner's replica and adjudicate it against its own authoritative table
    # before onboarding, killing both claimants of a diverged name rather than
    # silently overwriting the rejoiner's row and leaving the old claimant
    # running as a phantom singleton. The rejoin is driven deterministically by
    # sending the member the same {:nodeup, _} message a real reconnect would.
    @tag capture_log: true
    test "a rejoin under a continuing leader adjudicates divergent bindings", %{
      reg: reg,
      peer_node: peer_node
    } do
      p1 = spawn(fn -> Process.sleep(:infinity) end)
      p2 = spawn(fn -> Process.sleep(:infinity) end)

      on_exit(fn ->
        Process.exit(p1, :kill)
        Process.exit(p2, :kill)
      end)

      :yes = :dgen_registry.register_name({reg, :fp_conflicted}, p1)

      assert eventually(fn -> remote_whereis(peer_node, reg, :fp_conflicted) == p1 end),
             "registration never replicated to the peer"

      # Diverge the PEER's replica: a crafted contiguous broadcast rebinds the
      # name to p2 there (the same injection technique as the epoch/gap tests).
      peer_member = :erpc.call(peer_node, :dgen_registry, :member_name, [reg])
      epoch = :erpc.call(peer_node, :dgen_registry, :get_epoch, [reg])
      leader = :erpc.call(peer_node, :dgen_registry, :get_leader, [reg])
      v = peer_applied_version(peer_node, peer_member)

      :erpc.call(peer_node, :gen_server, :cast, [
        peer_member,
        names_batch(
          [{:name_registered, :fp_conflicted, p2, %{}, :undefined}],
          epoch,
          v,
          v + 1,
          leader
        )
      ])

      :erpc.call(peer_node, :sys, :get_state, [peer_member])
      assert p2 == remote_whereis(peer_node, reg, :fp_conflicted)

      # Drive the peer's re-announce — the same path a real {:nodeup, _} takes.
      # Leadership does not change, so the continuing leader (primary) handles
      # the join on the fast path and must gather + adjudicate the rejoiner.
      :erpc.call(peer_node, :erlang, :send, [peer_member, {:nodeup, node()}])

      assert eventually(
               fn -> not Process.alive?(p1) and not Process.alive?(p2) end,
               15_000
             ),
             "rejoin adjudication did not terminate both claimants " <>
               "(p1 alive: #{Process.alive?(p1)}, p2 alive: #{Process.alive?(p2)})"

      assert eventually(fn ->
               :dgen_registry.whereis_name({reg, :fp_conflicted}) == :undefined and
                 remote_whereis(peer_node, reg, :fp_conflicted) == :undefined
             end),
             "diverged name was not cleared everywhere after the rejoin adjudication"

      p3 = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(p3, :kill) end)

      assert eventually(fn ->
               :dgen_registry.register_name({reg, :fp_conflicted}, p3) == :yes
             end),
             "name could not be re-registered after the rejoin adjudication"
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
        names_batch(
          [{:name_registered, :ghost_name, pid, %{}, :undefined}],
          epoch - 1,
          v,
          v + 1,
          leader
        )
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

      peer_member = :erpc.call(peer_node, :dgen_registry, :member_name, [reg])
      leader = :erpc.call(peer_node, :dgen_registry, :get_leader, [reg])

      # apply_bcast fences on the *peer member's* current epoch and applied_version,
      # so craft the broadcast from the peer's own view (not the local node's epoch).
      # A transient re-election under CI load bumps the peer's epoch and resets its
      # applied_version (via an apply_names_snapshot) to a fresh versionstamp, which
      # would fence a broadcast stamped from stale reads — retry with fresh stamps
      # until the cluster is quiescent enough for the current-epoch broadcast to land.
      assert eventually(fn ->
               epoch = :erpc.call(peer_node, :dgen_registry, :get_epoch, [reg])
               v = peer_applied_version(peer_node, peer_member)

               :erpc.call(peer_node, :gen_server, :cast, [
                 peer_member,
                 names_batch(
                   [{:name_registered, :valid_name, pid, %{}, :undefined}],
                   epoch,
                   v,
                   v + 1,
                   leader
                 )
               ])

               # Barrier: ensure the peer member processed the cast before the read.
               :erpc.call(peer_node, :sys, :get_state, [peer_member])

               pid == :erpc.call(peer_node, :dgen_registry, :whereis_name, [{reg, :valid_name}])
             end),
             "peer never accepted a current-epoch name_registered broadcast"
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
  # Gap detection → resync recovery (§4.5)
  #
  # The unit suite asserts that a gapped broadcast is *refused*; this drives the
  # recovery half: the refusing member requests a resync snapshot from the
  # leader, re-baselines from it, and rejoins the contiguous broadcast stream.
  # ---------------------------------------------------------------------------

  describe "resync after a broadcast gap" do
    test "a gapped member re-baselines from the leader's snapshot and catches up", %{
      reg: reg,
      peer_node: peer_node
    } do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      # A real registration so the leader (primary) holds state to resync from.
      :yes = :dgen_registry.register_name({reg, :pre_gap}, pid)

      assert eventually(fn -> remote_whereis(peer_node, reg, :pre_gap) == pid end),
             "registration never replicated to the peer"

      peer_member = :erpc.call(peer_node, :dgen_registry, :member_name, [reg])
      epoch = :erpc.call(peer_node, :dgen_registry, :get_epoch, [reg])
      leader = :erpc.call(peer_node, :dgen_registry, :get_leader, [reg])
      v = peer_applied_version(peer_node, peer_member)

      # Inject a broadcast whose PrevVersion is far ahead of the peer's applied
      # version: the peer must refuse it (no hole in the replica) and ask the
      # leader for a resync snapshot instead.
      :erpc.call(peer_node, :gen_server, :cast, [
        peer_member,
        names_batch(
          [{:name_registered, :gap_ghost, pid, %{}, :undefined}],
          epoch,
          v + 10,
          v + 11,
          leader
        )
      ])

      :erpc.call(peer_node, :sys, :get_state, [peer_member])
      assert :undefined == remote_whereis(peer_node, reg, :gap_ghost)

      # The resync snapshot re-baselines the peer to the leader's version...
      {_records, leader_v, _released} =
        GenServer.call(:dgen_registry.member_name(reg), :get_names_snapshot)

      assert eventually(fn ->
               peer_applied_version(peer_node, peer_member) >= leader_v
             end),
             "peer never re-baselined from the resync snapshot"

      # ... the ghost never appears, the real state is intact, and the peer is
      # back on the contiguous stream: a fresh registration replicates normally.
      assert :undefined == remote_whereis(peer_node, reg, :gap_ghost)
      assert pid == remote_whereis(peer_node, reg, :pre_gap)

      pid2 = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid2, :kill) end)
      :yes = :dgen_registry.register_name({reg, :post_gap}, pid2)

      assert eventually(fn -> remote_whereis(peer_node, reg, :post_gap) == pid2 end),
             "peer did not resume applying broadcasts after the resync"
    end
  end

  # ---------------------------------------------------------------------------
  # Uniqueness under contention (Guarantee 1/3)
  #
  # Two nodes race to register the same name; exactly one caller may hear `yes`
  # per name, and every node must converge on the winner. The core singleton
  # claim, exercised as an actual race rather than sequentially.
  # ---------------------------------------------------------------------------

  describe "concurrent registration race" do
    test "when both nodes race for one name, exactly one wins and all agree", %{
      reg: reg,
      peer_node: peer_node
    } do
      for round <- 1..10 do
        name = {:race, round}
        local_pid = spawn(fn -> Process.sleep(:infinity) end)
        remote_pid = spawn_remote(peer_node)
        on_exit(fn -> Process.exit(local_pid, :kill) end)

        local_task =
          Task.async(fn -> :dgen_registry.register_name({reg, name}, local_pid) end)

        remote_task =
          Task.async(fn ->
            :erpc.call(peer_node, :dgen_registry, :register_name, [{reg, name}, remote_pid])
          end)

        local_result = Task.await(local_task, 10_000)
        remote_result = Task.await(remote_task, 10_000)

        assert Enum.sort([local_result, remote_result]) == [:no, :yes],
               "round #{round}: expected exactly one :yes, got " <>
                 "local=#{inspect(local_result)} remote=#{inspect(remote_result)}"

        winner = if local_result == :yes, do: local_pid, else: remote_pid

        assert :dgen_registry.whereis_name_consistent({reg, name}) == winner,
               "round #{round}: authoritative read disagrees with the race verdict"

        assert eventually(fn -> remote_whereis(peer_node, reg, name) == winner end),
               "round #{round}: peer snapshot never converged on the winner"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Unregister re-drive across a partition (Non-goal 5)
  #
  # An explicit unregister accepted while the leader is unreachable must not be
  # silently lost: the member stashes it and re-drives it as a pid-guarded
  # retract once the partition heals. Uses :peer.call (the stdio control
  # channel) to reach the disconnected peer without re-opening distribution —
  # an erpc would heal the very partition under test.
  # ---------------------------------------------------------------------------

  describe "unregister re-drive after a partition" do
    test "an unregister accepted while the leader is unreachable is applied on heal", %{
      reg: reg,
      peer_node: peer_node,
      peer_pid: peer_pid
    } do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      :yes = :dgen_registry.register_name({reg, :redrive_me}, pid)

      assert eventually(fn -> remote_whereis(peer_node, reg, :redrive_me) == pid end),
             "registration never replicated to the peer"

      # Sever distribution; the peer's member now sees the leader (primary) as
      # unreachable, so its unregister takes the stash-and-ok path. (If the
      # disconnect is still propagating on the peer, the forward is instead sent
      # into the dying connection and stashed under its Ref — either way the
      # removal is retained and re-driven, and the call answers :ok. The outer
      # :peer.call timeout is kept well above unregister_name's internal 5s call
      # timeout so the inner path always resolves first.)
      :net_kernel.disconnect(peer_node)

      assert :ok ==
               :peer.call(
                 peer_pid,
                 :dgen_registry,
                 :unregister_name,
                 [{reg, :redrive_me}],
                 15_000
               )

      # Read-your-delete holds locally on the disconnected peer...
      assert :undefined ==
               :peer.call(peer_pid, :dgen_registry, :whereis_name, [{reg, :redrive_me}], 5_000)

      # ... while the primary (which could not have heard about it) still serves it.
      assert pid == :dgen_registry.whereis_name({reg, :redrive_me})

      # Heal. The peer rejoins, receives the leader's snapshot (which briefly
      # resurrects the row on the peer), and its stashed removal is re-driven to
      # the leader as a pid-guarded retract — the unregister must win everywhere.
      Node.connect(peer_node)

      assert eventually(
               fn -> :dgen_registry.whereis_name({reg, :redrive_me}) == :undefined end,
               20_000
             ),
             "the stashed unregister was never re-driven to the leader"

      assert eventually(
               fn -> remote_whereis(peer_node, reg, :redrive_me) == :undefined end,
               20_000
             ),
             "the peer's replica did not converge on the removal"

      # The name is genuinely free again.
      pid2 = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid2, :kill) end)

      assert eventually(fn ->
               :dgen_registry.register_name({reg, :redrive_me}, pid2) == :yes
             end),
             "name was not re-registrable after the re-driven unregister"
    end
  end

  # ---------------------------------------------------------------------------
  # Recovery from durable leadership naming a node that is gone
  # ---------------------------------------------------------------------------

  describe "leadership recovery after a cold restart" do
    # A leader recorded in the durable elector state, whose node was killed while
    # leader and never comes back (a whole-cluster restart under new node names),
    # produces no `nodedown` for a freshly-started member that was never connected
    # to it. The leader-liveness probe must reap it so the fresh node self-elects,
    # rather than forwarding every registration to a dead leader forever.
    test "a fresh node self-elects past a durable leader whose node is gone", %{
      abs_cluster_file: abs_cluster_file,
      dir_path: dir_path,
      db: db,
      case_dir: case_dir
    } do
      self_node = node()
      reg = :"deadleader_reg_#{:erlang.unique_integer([:positive])}"

      # Boot a peer, make it the sole member (and therefore leader), so it writes
      # durable elector state naming itself leader.
      {peer_pid, peer_node} = DGen.ClusterHelper.boot_peer!("deadleader")

      _peer_sup =
        :erpc.call(peer_node, DGen.ClusterHelper, :start_registry, [
          reg,
          abs_cluster_file,
          dir_path
        ])

      assert match?(
               {^peer_node, _},
               :erpc.call(peer_node, DGen.ClusterHelper, :await_leader!, [reg])
             )

      # Kill the peer node abruptly. Durable state still names it leader; this local
      # node never ran `reg`, so it was never a connected member of that cluster and
      # will get no `nodedown` for the now-dead peer.
      DGen.ClusterHelper.stop_peer(peer_pid)

      # Start `reg` here for the first time. Its elector recovers leader = peer_node.
      {:ok, sup} = :dgen_registry.start_link(reg, {db, case_dir})
      on_exit(fn -> DGen.ClusterHelper.stop_registry(sup) end)

      # It really did inherit the dead leader (the bug precondition) ...
      assert eventually(
               fn -> match?({^peer_node, _}, :dgen_registry.get_leader(reg)) end,
               2_000
             ),
             "expected the fresh node to recover the dead peer as leader"

      # ... and the leader-liveness probe reaps it so this node takes over.
      assert eventually(
               fn -> match?({^self_node, _}, :dgen_registry.get_leader(reg)) end,
               15_000
             ),
             "fresh node never self-elected past the dead recovered leader"

      # Leadership actually works now: a registration succeeds.
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      assert eventually(fn ->
               :dgen_registry.register_name({reg, :after_heal}, pid) == :yes
             end),
             "registration still failed after the fresh node took over"
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
