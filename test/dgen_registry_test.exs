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
    {:ok, sup} = :dgen_registry.start_link(reg, tenant)
    await_leader!(reg)
    on_exit(fn -> stop_registry(sup) end)
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

  # Spawn a process that lives until the test ends (auto-killed on exit).
  defp spawn_live do
    pid = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  # Register processes with the given index maps, returning %{name => pid}.
  defp register_indexed(reg, entries) do
    Map.new(entries, fn {name, index} ->
      pid = spawn_live()
      :yes = :dgen_registry.register_name({reg, name}, pid, %{index: index})
      {name, pid}
    end)
  end

  # Sort query matches by name for order-independent comparison.
  defp names(matches), do: matches |> Enum.map(& &1.name) |> Enum.sort()

  # Supervisors exit with :shutdown (not :normal) when their parent process
  # dies, which has already happened by the time on_exit callbacks run.
  # Use :shutdown as the stop reason and catch any race. Takes the supervisor's
  # own pid directly (start_link/2,3 returns it) — the registry name now
  # resolves to the *member*, not the supervisor, so a by-name lookup would
  # stop the wrong process.
  defp stop_registry(sup) do
    try do
      Supervisor.stop(sup, :shutdown)
    catch
      :exit, _ -> :ok
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

  describe "readiness (ready/1, await_ready/2)" do
    test "a leader-elected registry reports ready", %{reg: reg} do
      # setup already awaited the leader, so this node has assumed leadership and synced.
      assert :dgen_registry.ready(reg)
      assert :ok == :dgen_registry.await_ready(reg, 5_000)
    end

    test "await_ready returns ok fast once ready, then registrations succeed", %{reg: reg} do
      assert :ok == :dgen_registry.await_ready(reg, 5_000)

      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)
      assert :yes == :dgen_registry.register_name({reg, :after_ready}, pid)
    end

    test "ready/1 is false and await_ready times out for a registry that does not exist" do
      absent = :"never_started_#{:erlang.unique_integer([:positive])}"
      refute :dgen_registry.ready(absent)
      assert {:error, :timeout} == :dgen_registry.await_ready(absent, 200)
    end
  end

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

      # unregister_name is a cast; whereis_name now reads the member's ETS table in
      # the caller (no member round-trip), so it no longer serialises behind the cast.
      # Flush the member's mailbox with get_state to ensure the local delete is applied
      # before the read.
      :sys.get_state(:dgen_registry.member_name(reg))
      assert :undefined = :dgen_registry.whereis_name({reg, :bar})
    end

    test "is idempotent — unregistering an unknown name is a no-op", %{reg: reg} do
      assert :ok = :dgen_registry.unregister_name({reg, :no_such})
    end

    # Guards the *durable* side: since §4.4 the registry stores no per-name keys, only
    # a per-registry version counter bumped once per fenced commit.  Both register and
    # unregister must drive a real fenced commit (advancing the counter) — not just an
    # in-memory map edit.  If unregister only touched memory, whereis_name would look
    # correct while no commit fenced the change against a concurrent leadership swap.
    test "register and unregister each drive a fenced version-key commit", %{
      reg: reg,
      tenant: tenant
    } do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      tuid = {"dgen_registry.", Atom.to_string(reg)}
      v0 = :dgen_registry_names.read_version(tenant, tuid)

      :yes = :dgen_registry.register_name({reg, :durable_bar}, pid)

      assert eventually(fn -> :dgen_registry_names.read_version(tenant, tuid) > v0 end),
             "register did not bump the durable version key"

      v1 = :dgen_registry_names.read_version(tenant, tuid)

      :dgen_registry.unregister_name({reg, :durable_bar})

      assert eventually(fn -> :dgen_registry_names.read_version(tenant, tuid) > v1 end),
             "unregister did not bump the durable version key"
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
  # Registration metadata (single-node)
  # ---------------------------------------------------------------------------

  describe "metadata" do
    test "a plain registration has empty default metadata", %{reg: reg} do
      pid = spawn_live()
      :yes = :dgen_registry.register_name({reg, :m_plain}, pid)

      assert {:ok, %{pid: ^pid, index: %{}, data: :undefined}} =
               :dgen_registry.get_metadata({reg, :m_plain})
    end

    test "get_metadata returns undefined for an unknown name", %{reg: reg} do
      assert :undefined = :dgen_registry.get_metadata({reg, :m_unknown})
    end

    test "register_name/3 attaches index and data atomically", %{reg: reg} do
      pid = spawn_live()
      spec = %{index: %{role: :worker, shard: 7}, data: %{note: "hi"}}
      :yes = :dgen_registry.register_name({reg, :m_reg3}, pid, spec)

      assert {:ok, %{pid: ^pid, index: %{role: :worker, shard: 7}, data: %{note: "hi"}}} =
               :dgen_registry.get_metadata({reg, :m_reg3})
    end

    test "set_metadata replaces the metadata of a registration", %{reg: reg} do
      pid = spawn_live()
      :yes = :dgen_registry.register_name({reg, :m_set}, pid, %{index: %{a: 1}, data: :first})

      assert :ok = :dgen_registry.set_metadata({reg, :m_set}, %{index: %{b: 2}, data: :second})

      assert eventually(fn ->
               :dgen_registry.get_metadata({reg, :m_set}) ==
                 {:ok, %{pid: pid, index: %{b: 2}, data: :second}}
             end),
             "metadata was not replaced"
    end

    test "set_metadata omitting a field resets it to the empty default", %{reg: reg} do
      pid = spawn_live()
      :yes = :dgen_registry.register_name({reg, :m_reset}, pid, %{index: %{a: 1}, data: :keep})

      # Replace (not merge): omitting :data clears it back to :undefined.
      assert :ok = :dgen_registry.set_metadata({reg, :m_reset}, %{index: %{a: 2}})

      assert eventually(fn ->
               :dgen_registry.get_metadata({reg, :m_reset}) ==
                 {:ok, %{pid: pid, index: %{a: 2}, data: :undefined}}
             end),
             "omitted field was not reset"
    end

    test "set_metadata on an unknown name returns not_registered", %{reg: reg} do
      assert {:error, :not_registered} =
               :dgen_registry.set_metadata({reg, :m_absent}, %{index: %{a: 1}})
    end

    test "get_metadata_consistent agrees with the snapshot read", %{reg: reg} do
      pid = spawn_live()
      :yes = :dgen_registry.register_name({reg, :m_consistent}, pid, %{index: %{k: :v}})

      assert {:ok, %{pid: ^pid, index: %{k: :v}}} =
               :dgen_registry.get_metadata_consistent({reg, :m_consistent})
    end

    test "metadata is removed when the process exits", %{reg: reg} do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      :yes = :dgen_registry.register_name({reg, :m_dies}, pid, %{index: %{a: 1}})
      assert {:ok, _} = :dgen_registry.get_metadata({reg, :m_dies})

      Process.exit(pid, :kill)

      assert eventually(fn -> :dgen_registry.get_metadata({reg, :m_dies}) == :undefined end),
             "metadata outlived the process"
    end

    test "metadata is removed on unregister", %{reg: reg} do
      pid = spawn_live()
      :yes = :dgen_registry.register_name({reg, :m_unreg}, pid, %{index: %{a: 1}})

      :dgen_registry.unregister_name({reg, :m_unreg})
      :sys.get_state(:dgen_registry.member_name(reg))

      assert :undefined = :dgen_registry.get_metadata({reg, :m_unreg})
    end
  end

  # ---------------------------------------------------------------------------
  # Indexed AND-equal queries (single-node)
  # ---------------------------------------------------------------------------

  describe "query" do
    test "single-clause query returns all matching registrations", %{reg: reg} do
      pids =
        register_indexed(reg, %{
          q_a: %{role: :worker, shard: 1},
          q_b: %{role: :worker, shard: 2},
          q_c: %{role: :admin, shard: 1}
        })

      matches = :dgen_registry.query(reg, %{role: :worker})
      assert names(matches) == [:q_a, :q_b]

      # Each match carries the full record.
      a = Enum.find(matches, &(&1.name == :q_a))
      assert a.pid == pids[:q_a]
      assert a.index == %{role: :worker, shard: 1}
    end

    test "multi-clause query is an AND of exact equalities", %{reg: reg} do
      register_indexed(reg, %{
        q_a: %{role: :worker, shard: 1},
        q_b: %{role: :worker, shard: 2},
        q_c: %{role: :admin, shard: 1}
      })

      assert names(:dgen_registry.query(reg, %{role: :worker, shard: 1})) == [:q_a]
      assert :dgen_registry.query(reg, %{role: :worker, shard: 9}) == []
    end

    test "query reflects set_metadata changes", %{reg: reg} do
      register_indexed(reg, %{q_x: %{role: :worker}})

      assert names(:dgen_registry.query(reg, %{role: :worker})) == [:q_x]

      :ok = :dgen_registry.set_metadata({reg, :q_x}, %{index: %{role: :admin}})
      :sys.get_state(:dgen_registry.member_name(reg))

      # No longer a worker; now an admin.
      assert :dgen_registry.query(reg, %{role: :worker}) == []
      assert names(:dgen_registry.query(reg, %{role: :admin})) == [:q_x]
    end

    test "query drops entries whose process has died", %{reg: reg} do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      :yes = :dgen_registry.register_name({reg, :q_dead}, pid, %{index: %{role: :worker}})
      assert names(:dgen_registry.query(reg, %{role: :worker})) == [:q_dead]

      Process.exit(pid, :kill)

      assert eventually(fn -> :dgen_registry.query(reg, %{role: :worker}) == [] end),
             "dead process still returned by query"
    end

    test "an empty constraints map is rejected", %{reg: reg} do
      assert {:error, :empty_query} = :dgen_registry.query(reg, %{})
      assert {:error, :empty_query} = :dgen_registry.query_consistent(reg, %{})
    end

    test "query_consistent agrees with the snapshot query", %{reg: reg} do
      register_indexed(reg, %{q_a: %{role: :worker}, q_b: %{role: :worker}})

      assert names(:dgen_registry.query_consistent(reg, %{role: :worker})) == [:q_a, :q_b]
    end
  end

  # ---------------------------------------------------------------------------
  # §5.6 conflict-detection predicate (pure, unit-tested in isolation)
  #
  # The full partition-heal scenario that produces a genuine conflict needs a
  # multi-node, leadership-orchestrated setup (a follower holds a binding the leader
  # drops on reconstruction and re-issues); that lives in the cluster suite. Here we
  # pin the safety-critical predicate itself: which gathered states count as a
  # conflict, and — crucially — which do not (so we never false-kill a live process).
  # ---------------------------------------------------------------------------

  describe "detect_conflicts/3" do
    test "two different live pids for one name is a conflict" do
      p1 = spawn_live()
      p2 = spawn_live()

      assert [{:n, ^p1, [^p2]}] =
               :dgen_registry_member.detect_conflicts(%{n: p1}, [%{n: p1}, %{n: p2}], %{})
    end

    test "a recently-released divergent pid is suppressed (lag, not conflict)" do
      p1 = spawn_live()
      p2 = spawn_live()
      released = %{p2 => System.system_time(:millisecond)}

      assert [] =
               :dgen_registry_member.detect_conflicts(%{n: p1}, [%{n: p1}, %{n: p2}], released)
    end

    test "a dead divergent pid is not a conflict" do
      p1 = spawn_live()
      p2 = spawn(fn -> :ok end)
      Process.exit(p2, :kill)
      eventually(fn -> not Process.alive?(p2) end)

      assert [] = :dgen_registry_member.detect_conflicts(%{n: p1}, [%{n: p1}, %{n: p2}], %{})
    end

    test "agreement across all maps is not a conflict" do
      p1 = spawn_live()

      assert [] = :dgen_registry_member.detect_conflicts(%{n: p1}, [%{n: p1}, %{n: p1}], %{})
    end

    test "a name with no authority (absent from the freshest map) is not a conflict" do
      p1 = spawn_live()

      assert [] = :dgen_registry_member.detect_conflicts(%{}, [%{n: p1}], %{})
    end
  end

  # ---------------------------------------------------------------------------
  # plan_batch/4 batch-local overlay (pure, unit-tested in isolation)
  #
  # A group commit's plan is built *before* anything lands in the member's ETS
  # table, so an op must still see an earlier op's not-yet-committed decision if
  # both touch the same name within one batch. plan_batch resolves this lazily
  # (seed_lookup/3: the batch's own overlay first, a point ETS lookup otherwise)
  # rather than needing a full pre-seeded map of the registry. These tests pin
  # that overlay directly — in particular the `removed` marker it uses to mean
  # "an earlier op in this batch explicitly cleared this name", which matters
  # because `retract`/`down` (unlike `unregister`) never touch ETS until the
  # whole batch commits, so bare absence from the overlay cannot be made to mean
  # "cleared" without also meaning "untouched".
  # ---------------------------------------------------------------------------

  describe "plan_batch/4 (batch-local overlay)" do
    setup do
      # No on_exit cleanup needed: the table is owned by (and dies with) this
      # test process; on_exit callbacks run in a separate runner process after
      # the test process has already exited, so the table would already be
      # gone by the time an on_exit tried to delete it.
      %{tab: :ets.new(:plan_batch_test, [:set, :protected])}
    end

    test "a down followed by a re-add for the same name in one batch resurrects it", %{
      tab: tab
    } do
      old_pid = spawn_live()
      new_pid = spawn_live()
      ref = make_ref()
      origin = {:local, {self(), make_ref()}}

      # Pre-batch ETS state: old_pid is still registered there. A `down` only
      # takes effect when its batch commits (unlike unregister's optimistic
      # delete), so ETS does not yet know old_pid's binding is gone.
      :ets.insert(tab, {:n, old_pid, %{}, nil})

      ops = [{:down, :n, ref}, {:add, :n, new_pid, {%{}, nil}, origin}]
      plan = :dgen_registry_member.plan_batch(ops, tab, %{n: ref}, 1)

      # Without the `removed` marker, the add's ETS fallback would still see
      # old_pid bound (ETS hasn't been told about the down yet) and wrongly
      # reject new_pid's registration as "already taken".
      assert %{dbop: %{n: {:set, _node}}, replies: replies} = plan
      assert {origin, :yes} in replies
    end

    test "a plain add then remove for the same name in one batch nets to a durable clear", %{
      tab: tab
    } do
      pid = spawn_live()
      add_origin = {:local, {self(), make_ref()}}

      # The name was never registered before this batch (ETS is empty), so the
      # remove's ReleasedPid is :undefined — only the overlay (from the earlier
      # add, still in this same batch) tells the remove there is something to
      # durably clear.
      ops = [{:add, :n, pid, {%{}, nil}, add_origin}, {:remove, :n, :undefined}]
      plan = :dgen_registry_member.plan_batch(ops, tab, %{}, 1)

      assert %{dbop: %{n: :clear}, replies: replies} = plan
      assert {add_origin, :yes} in replies
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

    # Exercises the group-commit removal path under load: a burst of deaths (as
    # when a node is killed) must all be reaped, coalesced into few commits.
    test "a burst of simultaneous process deaths releases every name", %{reg: reg} do
      n = 100

      pids =
        for i <- 1..n do
          pid = spawn(fn -> Process.sleep(:infinity) end)
          assert :yes = :dgen_registry.register_name({reg, {:burst, i}}, pid)
          {i, pid}
        end

      # Kill them all at once, then expect every name to be released.
      Enum.each(pids, fn {_i, pid} -> Process.exit(pid, :kill) end)

      assert eventually(
               fn ->
                 Enum.all?(1..n, fn i ->
                   :dgen_registry.whereis_name({reg, {:burst, i}}) == :undefined
                 end)
               end,
               5_000
             ),
             "not all names were released after a burst of deaths"
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

  describe "get_epoch/1" do
    test "epoch is positive after initial election", %{reg: reg} do
      assert :dgen_registry.get_epoch(reg) > 0
    end
  end

  # ---------------------------------------------------------------------------
  # Epoch fencing and gap detection — stale-leader broadcasts are discarded, and a
  # broadcast that is not contiguous with the member's replica (a missed batch)
  # is not applied.
  #
  # Broadcast shape: {name_registered, Name, Pid, Index, Data, Epoch, PrevVersion,
  # Version, LeaderId} (and the analogous {name_unregistered, Name, ReleasedPid,
  # Epoch, PrevVersion, Version, LeaderId}). A broadcast is applied only when
  # PrevVersion equals the member's applied version (the next batch in sequence)
  # or Version equals it (the same batch continuing).
  # ---------------------------------------------------------------------------

  # The member's current applied version, read through the same snapshot call the
  # handoff gather uses, so tests can craft contiguous (or gapped) broadcasts.
  defp applied_version(member) do
    {_records, version, _released} = GenServer.call(member, :get_names_snapshot)
    version
  end

  describe "epoch fencing" do
    test "name_registered with stale epoch is discarded", %{reg: reg} do
      member = :dgen_registry.member_name(reg)
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      epoch = :dgen_registry.get_epoch(reg)
      leader = :dgen_registry.get_leader(reg)
      v = applied_version(member)
      # Simulate a broadcast from a stale leader carrying a smaller epoch. The
      # version stamps are contiguous, so only the epoch causes the discard.
      GenServer.cast(
        member,
        {:name_registered, :stale_name, pid, %{}, :undefined, epoch - 1, v, v + 1, leader}
      )

      # whereis_name now reads ETS in the caller (no member round-trip), so it can no
      # longer act as a mailbox barrier — flush the member's mailbox with get_state.
      :sys.get_state(member)

      assert :undefined == :dgen_registry.whereis_name({reg, :stale_name})
    end

    test "name_registered with current epoch and contiguous version is applied", %{reg: reg} do
      member = :dgen_registry.member_name(reg)
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      epoch = :dgen_registry.get_epoch(reg)
      leader = :dgen_registry.get_leader(reg)
      v = applied_version(member)

      GenServer.cast(
        member,
        {:name_registered, :current_name, pid, %{}, :undefined, epoch, v, v + 1, leader}
      )

      # Barrier: ensure the member has processed the cast before the caller-side read.
      :sys.get_state(member)

      assert pid == :dgen_registry.whereis_name({reg, :current_name})
    end

    test "name_registered with a version gap is not applied", %{reg: reg} do
      member = :dgen_registry.member_name(reg)
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      epoch = :dgen_registry.get_epoch(reg)
      leader = :dgen_registry.get_leader(reg)
      v = applied_version(member)

      # PrevVersion is ahead of the member's applied version: the member missed a
      # batch, so this broadcast must not be applied (the member asks for a resync
      # snapshot instead of advancing with a hole in its replica).
      GenServer.cast(
        member,
        {:name_registered, :gapped_name, pid, %{}, :undefined, epoch, v + 10, v + 11, leader}
      )

      :sys.get_state(member)

      assert :undefined == :dgen_registry.whereis_name({reg, :gapped_name})
    end

    test "name_unregistered with stale epoch is discarded", %{reg: reg} do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      :yes = :dgen_registry.register_name({reg, :persisted_name}, pid)
      epoch = :dgen_registry.get_epoch(reg)
      leader = :dgen_registry.get_leader(reg)
      member = :dgen_registry.member_name(reg)
      v = applied_version(member)

      GenServer.cast(
        member,
        {:name_unregistered, :persisted_name, :undefined, epoch - 1, v, v + 1, leader}
      )

      # Barrier: ensure the member has processed (and discarded) the stale cast.
      :sys.get_state(member)

      assert pid == :dgen_registry.whereis_name({reg, :persisted_name})
    end

    test "name_unregistered with current epoch and contiguous version is applied", %{reg: reg} do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      :yes = :dgen_registry.register_name({reg, :to_remove}, pid)
      epoch = :dgen_registry.get_epoch(reg)
      leader = :dgen_registry.get_leader(reg)
      member = :dgen_registry.member_name(reg)
      v = applied_version(member)

      GenServer.cast(
        member,
        {:name_unregistered, :to_remove, :undefined, epoch, v, v + 1, leader}
      )

      # Barrier: ensure the member has processed the cast before the caller-side read.
      :sys.get_state(member)

      assert :undefined == :dgen_registry.whereis_name({reg, :to_remove})
    end
  end

  # ---------------------------------------------------------------------------

  describe "member_name/1, names_table/1, and elector_pid/1" do
    test "member process is alive after start", %{reg: reg} do
      assert reg |> :dgen_registry.member_name() |> Process.whereis() |> is_pid()
    end

    test "member_name/1 is the identity — the member is registered as the registry name" do
      assert :my_reg = :dgen_registry.member_name(:my_reg)
    end

    test "names_table/1 is the identity — the ETS table is named after the registry", %{
      reg: reg
    } do
      assert reg == :dgen_registry.names_table(reg)
    end

    test "elector process is alive after start, discovered with no registered name", %{
      reg: reg
    } do
      elector = :dgen_registry.elector_pid(reg)
      assert is_pid(elector)
      assert Process.alive?(elector)
      # The elector itself has no atom — Process.whereis/1 must not find it.
      refute elector == Process.whereis(reg)
    end

    test "elector_pid/1 returns undefined for a registry that was never started" do
      assert :undefined = :dgen_registry.elector_pid(:no_such_registry_at_all)
    end
  end
end
