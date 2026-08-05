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
    # Wait for the *member* to be ready to serve, not merely for the elector to have
    # elected a leader (await_leader!). Leadership is assumed asynchronously (the handoff
    # gather runs off the member loop), so there is a window where get_leader/1 already
    # reports a leader but the member has not finished assuming — leader still undefined,
    # not yet synced. Tests that then call ready/1, set_metadata/2, etc. would race that
    # window. await_ready/2 blocks until the member has actually assumed and synced.
    assert :ok == :dgen_registry.await_ready(reg, 5_000)
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

  # Register the calling (test) process as a presence notify target under `index`, so
  # {:dgen_presence, ...} messages for a subscription whose notify query matches `index`
  # arrive in the test mailbox.
  defp register_listener(reg, index) do
    :yes = :dgen_registry.register_name({reg, {:listener, self()}}, self(), %{index: index})
    :ok
  end

  # subscribe/4 commits on the (durable) elector, which then *asynchronously* pushes the
  # subscription to the leader. These block until the leader has (or no longer has)
  # applied it — the presence sync point for a race-free assertion. Detection is
  # layout-agnostic: it scans the member's #state{} for any map keyed by `sub_id` (the
  # `subs` / `sub_matches` fields), so it does not depend on record field order.
  defp await_sub(reg, sub_id), do: assert(eventually(fn -> leader_has_sub?(reg, sub_id) end))

  defp await_no_sub(reg, sub_id),
    do: assert(eventually(fn -> not leader_has_sub?(reg, sub_id) end))

  defp leader_has_sub?(reg, sub_id) do
    reg
    |> :dgen_registry.member_name()
    |> :sys.get_state()
    |> Tuple.to_list()
    |> Enum.any?(fn v -> is_map(v) and Map.has_key?(v, sub_id) end)
  end

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

    test "returns no when the name is already taken by a different pid", %{reg: reg} do
      pid1 = spawn(fn -> Process.sleep(:infinity) end)
      pid2 = spawn(fn -> Process.sleep(:infinity) end)

      on_exit(fn ->
        Process.exit(pid1, :kill)
        Process.exit(pid2, :kill)
      end)

      :yes = :dgen_registry.register_name({reg, :dup}, pid1)
      assert :no = :dgen_registry.register_name({reg, :dup}, pid2)
    end

    # Re-registering the SAME pid under the SAME name is an idempotent success, not
    # a conflict — so a caller whose register timed out (but whose registration in
    # fact committed) can redrive it and get a decisive :yes for its own binding.
    test "re-registering the same pid under the same name is an idempotent yes", %{reg: reg} do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      assert :yes = :dgen_registry.register_name({reg, :idem}, pid)
      assert :yes = :dgen_registry.register_name({reg, :idem}, pid)
      assert :yes = :dgen_registry.register_name({reg, :idem}, pid)
      assert pid == :dgen_registry.whereis_name({reg, :idem})
      # A different pid is still rejected after the idempotent re-registers.
      other = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(other, :kill) end)
      assert :no = :dgen_registry.register_name({reg, :idem}, other)
      assert pid == :dgen_registry.whereis_name({reg, :idem})
    end

    # register_name/3 re-applies the call's metadata on an idempotent re-register,
    # so a redrive that carries the same metadata leaves the row consistent.
    test "an idempotent re-register with register_name/3 keeps the binding", %{reg: reg} do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      spec = %{index: %{role: :worker}, data: :d}
      assert :yes = :dgen_registry.register_name({reg, :idem3}, pid, spec)
      assert :yes = :dgen_registry.register_name({reg, :idem3}, pid, spec)

      assert {:ok, %{pid: ^pid, index: %{role: :worker}, data: :d}} =
               :dgen_registry.get_metadata({reg, :idem3})
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

    # Note: the "no reachable leader → the register blocks (never a false :no)"
    # contract is exercised deterministically by the cluster suite's
    # "CP refusals during a distribution-only partition" test — on the non-leader
    # side a write blocks and its register_timeout exits rather than returning :no.
    # A single-node unit test for it would have to poke the member's async-managed
    # `leader` field, which races the post-sync re-announce, so it lives there.
  end

  describe "unregister_name/1" do
    test "removes the registration", %{reg: reg} do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      :yes = :dgen_registry.register_name({reg, :bar}, pid)

      # unregister_name is now a tracked call (Non-goal 5): `ok` comes back after
      # the leader has committed the removal, and the member's optimistic delete
      # happened before that — so the caller-side snapshot read sees it at once.
      assert :ok = :dgen_registry.unregister_name({reg, :bar})
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

      # unregister_name is a tracked call; the optimistic delete precedes its reply.
      assert :ok = :dgen_registry.unregister_name({reg, :m_unreg})
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
  # Presence (watch/notify subscriptions, §4.9)
  # ---------------------------------------------------------------------------

  describe "presence (subscribe/unsubscribe)" do
    test "delivers joined/left as processes enter and leave the watch set", %{reg: reg} do
      register_listener(reg, %{group: :l})
      :ok = :dgen_registry.subscribe(reg, :s1, %{role: :worker}, %{group: :l})
      await_sub(reg, :s1)

      # A worker entering the watch set notifies the listener.
      w1 = spawn_live()
      :yes = :dgen_registry.register_name({reg, :w1}, w1, %{index: %{role: :worker}})
      assert_receive {:dgen_presence, :s1, [{:joined, :w1, ^w1}]}, 1_000

      # A registration outside the watch set does not.
      o = spawn_live()
      :yes = :dgen_registry.register_name({reg, :o1}, o, %{index: %{role: :other}})
      refute_receive {:dgen_presence, :s1, _}, 200

      # The worker dying leaves the set (auto-unregister via the leader monitor).
      Process.exit(w1, :kill)
      assert_receive {:dgen_presence, :s1, [{:left, :w1, ^w1}]}, 1_000

      # An explicit unregister of a worker also leaves the set.
      w2 = spawn_live()
      :yes = :dgen_registry.register_name({reg, :w2}, w2, %{index: %{role: :worker}})
      assert_receive {:dgen_presence, :s1, [{:joined, :w2, ^w2}]}, 1_000
      :ok = :dgen_registry.unregister_name({reg, :w2})
      assert_receive {:dgen_presence, :s1, [{:left, :w2, ^w2}]}, 1_000
    end

    test "delivers an initial snapshot of the current watch set on subscribe", %{reg: reg} do
      a = spawn_live()
      b = spawn_live()
      :yes = :dgen_registry.register_name({reg, :a}, a, %{index: %{role: :worker}})
      :yes = :dgen_registry.register_name({reg, :b}, b, %{index: %{role: :worker}})
      register_listener(reg, %{group: :l})

      # No await_sub here — the initial snapshot message is itself the signal that the
      # (asynchronous) subscribe reached the leader, so allow for the cast to propagate.
      :ok = :dgen_registry.subscribe(reg, :s2, %{role: :worker}, %{group: :l})
      assert_receive {:dgen_presence, :s2, events}, 2_000
      assert Enum.sort(events) == Enum.sort([{:joined, :a, a}, {:joined, :b, b}])
    end

    test "a notify process registered after subscribe still gets the initial snapshot",
         %{reg: reg} do
      # A watch member and a subscription exist, but no notify process yet.
      w1 = spawn_live()
      :yes = :dgen_registry.register_name({reg, :w1}, w1, %{index: %{role: :worker}})
      :ok = :dgen_registry.subscribe(reg, :sN, %{role: :worker}, %{group: :l})
      await_sub(reg, :sN)

      # The notify process (self) registers *after* the subscription — it must still learn
      # who is already present, i.e. receive the current watch set as an initial snapshot.
      register_listener(reg, %{group: :l})
      assert_receive {:dgen_presence, :sN, [{:joined, :w1, ^w1}]}, 1_000

      # And thereafter it receives deltas as a continuing notify target.
      w2 = spawn_live()
      :yes = :dgen_registry.register_name({reg, :w2}, w2, %{index: %{role: :worker}})
      assert_receive {:dgen_presence, :sN, [{:joined, :w2, ^w2}]}, 1_000
    end

    test "set_metadata moving a name in and out of the watch set notifies", %{reg: reg} do
      register_listener(reg, %{group: :l})
      :ok = :dgen_registry.subscribe(reg, :s3, %{role: :worker}, %{group: :l})
      await_sub(reg, :s3)

      x = spawn_live()
      :yes = :dgen_registry.register_name({reg, :x}, x, %{index: %{role: :idle}})
      refute_receive {:dgen_presence, :s3, _}, 200

      :ok = :dgen_registry.set_metadata({reg, :x}, %{index: %{role: :worker}})
      assert_receive {:dgen_presence, :s3, [{:joined, :x, ^x}]}, 1_000

      :ok = :dgen_registry.set_metadata({reg, :x}, %{index: %{role: :idle}})
      assert_receive {:dgen_presence, :s3, [{:left, :x, ^x}]}, 1_000
    end

    test "unsubscribe stops notifications", %{reg: reg} do
      register_listener(reg, %{group: :l})
      :ok = :dgen_registry.subscribe(reg, :s4, %{role: :worker}, %{group: :l})
      await_sub(reg, :s4)
      :ok = :dgen_registry.unsubscribe(reg, :s4)
      await_no_sub(reg, :s4)

      w = spawn_live()
      :yes = :dgen_registry.register_name({reg, :w}, w, %{index: %{role: :worker}})
      refute_receive {:dgen_presence, :s4, _}, 300
    end

    test "subscriptions/1 lists durable subscriptions and subscribe upserts", %{reg: reg} do
      # subscribe/unsubscribe are durable *casts* (applied asynchronously), so read-back
      # is eventual.
      :ok = :dgen_registry.subscribe(reg, :s5, %{role: :worker}, %{group: :l})

      assert eventually(fn ->
               :dgen_registry.subscriptions(reg)[:s5] ==
                 {{:all, %{role: :worker}}, {:all, %{group: :l}}}
             end)

      # Same SubId re-subscribed replaces the queries (idempotent upsert).
      :ok = :dgen_registry.subscribe(reg, :s5, %{role: :admin}, %{group: :l})

      assert eventually(fn ->
               :dgen_registry.subscriptions(reg)[:s5] ==
                 {{:all, %{role: :admin}}, {:all, %{group: :l}}}
             end)

      :ok = :dgen_registry.unsubscribe(reg, :s5)
      assert eventually(fn -> not Map.has_key?(:dgen_registry.subscriptions(reg), :s5) end)
    end

    test "subscriptions are durable across a full registry restart", %{tenant: tenant} do
      dreg = :"dreg_#{:erlang.unique_integer([:positive])}"
      {:ok, sup1} = :dgen_registry.start_link(dreg, tenant)
      await_leader!(dreg)

      :ok = :dgen_registry.subscribe(dreg, :d1, %{role: :worker}, %{group: :l})
      assert eventually(fn -> Map.has_key?(:dgen_registry.subscriptions(dreg), :d1) end)

      # Tear the registry all the way down, then bring it back on the same tenant — the
      # elector's durable backend state (including the subscription) must survive.
      # Unlink first: start_link linked sup1 to this test process, so stopping it with
      # reason :shutdown would otherwise propagate through the link and kill the test.
      Process.unlink(sup1)
      stop_registry(sup1)
      assert eventually(fn -> Process.whereis(dreg) == nil end)

      {:ok, sup2} = :dgen_registry.start_link(dreg, tenant)
      on_exit(fn -> stop_registry(sup2) end)
      assert :ok == :dgen_registry.await_ready(dreg, 5_000)

      # The subscription is back, and the re-elected leader actively serves it.
      assert Map.has_key?(:dgen_registry.subscriptions(dreg), :d1)
      await_sub(dreg, :d1)

      register_listener(dreg, %{group: :l})
      w = spawn_live()
      :yes = :dgen_registry.register_name({dreg, :w}, w, %{index: %{role: :worker}})
      assert_receive {:dgen_presence, :d1, [{:joined, :w, ^w}]}, 2_000
    end

    test "an empty watch or notify query is rejected", %{reg: reg} do
      assert {:error, :empty_query} = :dgen_registry.subscribe(reg, :s6, %{}, %{group: :l})
      assert {:error, :empty_query} = :dgen_registry.subscribe(reg, :s6, %{role: :worker}, %{})
    end

    test "a bare map query is accepted and equals the tagged {all, map} form", %{reg: reg} do
      register_listener(reg, %{group: :l})
      :ok = :dgen_registry.subscribe(reg, :s7, {:all, %{role: :worker}}, {:all, %{group: :l}})
      await_sub(reg, :s7)

      w = spawn_live()
      :yes = :dgen_registry.register_name({reg, :w}, w, %{index: %{role: :worker}})
      assert_receive {:dgen_presence, :s7, [{:joined, :w, ^w}]}, 1_000
    end
  end

  # ---------------------------------------------------------------------------
  # Teardown / durable-state cleanup (unsubscribe_all/1, delete/2)
  # ---------------------------------------------------------------------------

  describe "cleanup" do
    test "unsubscribe_all clears every subscription and stops notifications", %{reg: reg} do
      :ok = :dgen_registry.subscribe(reg, :a, %{role: :worker}, %{group: :l})
      :ok = :dgen_registry.subscribe(reg, :b, %{role: :admin}, %{group: :l})
      await_sub(reg, :a)
      await_sub(reg, :b)
      assert eventually(fn -> map_size(:dgen_registry.subscriptions(reg)) == 2 end)

      :ok = :dgen_registry.unsubscribe_all(reg)
      assert eventually(fn -> :dgen_registry.subscriptions(reg) == %{} end)
      await_no_sub(reg, :a)
      await_no_sub(reg, :b)

      # The leader no longer notifies for any watch.
      register_listener(reg, %{group: :l})
      w = spawn_live()
      :yes = :dgen_registry.register_name({reg, :w}, w, %{index: %{role: :worker}})
      refute_receive {:dgen_presence, _, _}, 300
    end

    test "delete/2 wipes durable state — a fresh registry on the same tenant starts clean",
         %{tenant: tenant} do
      dreg = :"dreg_#{:erlang.unique_integer([:positive])}"
      {:ok, sup1} = :dgen_registry.start_link(dreg, tenant)
      assert :ok == :dgen_registry.await_ready(dreg, 5_000)

      :ok = :dgen_registry.subscribe(dreg, :d1, %{role: :worker}, %{group: :l})
      assert eventually(fn -> Map.has_key?(:dgen_registry.subscriptions(dreg), :d1) end)

      # Stop, then delete the durable footprint (the correct order — a running elector
      # would re-create keys from its cache).
      Process.unlink(sup1)
      stop_registry(sup1)
      assert eventually(fn -> Process.whereis(dreg) == nil end)
      :ok = :dgen_registry.delete(dreg, tenant)

      # A fresh registry on the same name+tenant no longer sees the subscription — unlike
      # the "durable across a restart" test, the durable state is gone.
      {:ok, sup2} = :dgen_registry.start_link(dreg, tenant)
      on_exit(fn -> stop_registry(sup2) end)
      assert :ok == :dgen_registry.await_ready(dreg, 5_000)
      assert :dgen_registry.subscriptions(dreg) == %{}
    end
  end

  # ---------------------------------------------------------------------------
  # §5.6 conflict-detection predicate (pure, unit-tested in isolation)
  #
  # The full end-to-end scenario that produces a genuine conflict needs a
  # multi-node, leadership-orchestrated setup (a member holds a divergent live
  # binding that a leadership-change gather exposes); that lives in the cluster
  # suite ("conflict resolution by termination" in dgen_registry_cluster_test.exs).
  # Here we pin the safety-critical predicate itself: which gathered states count
  # as a conflict, and — crucially — which do not (so we never false-kill a live
  # process).
  # ---------------------------------------------------------------------------

  describe "detect_conflicts/3" do
    test "two different live pids for one name is a conflict" do
      p1 = spawn_live()
      p2 = spawn_live()

      assert [{:n, ^p1, [^p2]}] =
               :dgen_registry_member.detect_conflicts(%{n: p1}, [%{n: p1}, %{n: p2}], %{})
    end

    test "a recently-released name/pid pair is suppressed (lag, not conflict)" do
      p1 = spawn_live()
      p2 = spawn_live()
      # Trail entries are keyed {name, pid} (§5.6).
      released = %{{:n, p2} => System.system_time(:millisecond)}

      assert [] =
               :dgen_registry_member.detect_conflicts(%{n: p1}, [%{n: p1}, %{n: p2}], released)
    end

    test "a release under one name does not mask a conflict on another name" do
      p1 = spawn_live()
      p2 = spawn_live()
      # p2 was explicitly released from :other — that must not suppress the
      # genuine divergence on :n, which p2 also (still) claims.
      released = %{{:other, p2} => System.system_time(:millisecond)}

      assert [{:n, ^p1, [^p2]}] =
               :dgen_registry_member.detect_conflicts(%{n: p1}, [%{n: p1}, %{n: p2}], released)
    end

    test "a legacy bare-pid trail entry (rolling upgrade) still suppresses" do
      p1 = spawn_live()
      p2 = spawn_live()
      # A pre-upgrade member's gathered trail keys by pid alone; the detector
      # honours that coarser shape until the mixed entries age out.
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
      ops = [{:add, :n, pid, {%{}, nil}, add_origin}, {:remove, :n, :undefined, :undefined}]
      plan = :dgen_registry_member.plan_batch(ops, tab, %{}, 1)

      assert %{dbop: %{n: :clear}, replies: replies} = plan
      assert {add_origin, :yes} in replies
    end

    test "an add for a name already bound to the same pid is an idempotent yes", %{tab: tab} do
      pid = spawn_live()
      origin = {:local, {self(), make_ref()}}
      # Pre-batch ETS: :n is already bound to pid, with an existing monitor ref.
      :ets.insert(tab, {:n, pid, %{}, nil})

      ops = [{:add, :n, pid, {%{}, nil}, origin}]
      plan = :dgen_registry_member.plan_batch(ops, tab, %{n: make_ref()}, 1)

      # Same pid re-registers: yes, re-writing the (identical) binding via {:set}.
      assert %{dbop: %{n: {:set, _node}}, replies: replies} = plan
      assert {origin, :yes} in replies
    end

    test "an add for a name bound to a DIFFERENT pid is rejected no", %{tab: tab} do
      old_pid = spawn_live()
      new_pid = spawn_live()
      origin = {:local, {self(), make_ref()}}
      :ets.insert(tab, {:n, old_pid, %{}, nil})

      ops = [{:add, :n, new_pid, {%{}, nil}, origin}]
      plan = :dgen_registry_member.plan_batch(ops, tab, %{n: make_ref()}, 1)

      # Different pid: rejected, and no durable change for the name.
      assert %{dbop: dbop, replies: replies} = plan
      refute Map.has_key?(dbop, :n)
      assert {origin, :no} in replies
    end

    test "a tracked remove is answered ok whether or not anything was bound", %{tab: tab} do
      pid = spawn_live()
      unreg_origin = {:unreg, {self(), make_ref()}}
      noop_origin = {:unreg, {self(), make_ref()}}
      :ets.insert(tab, {:bound, pid, %{}, nil})

      ops = [
        {:remove, :bound, pid, unreg_origin},
        # Unbound name: clearing it is an idempotent no-op, still answered ok.
        {:remove, :unbound, :undefined, noop_origin}
      ]

      plan = :dgen_registry_member.plan_batch(ops, tab, %{}, 1)

      assert %{dbop: dbop, replies: replies, released: released} = plan
      assert dbop == %{bound: :clear}
      assert {unreg_origin, :ok} in replies
      assert {noop_origin, :ok} in replies
      # The trail entry is the {name, pid} pair (§5.6).
      assert released == [{:bound, pid}]
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

  # A replication broadcast in the shape the leader actually sends: one
  # `{:names_batch, Ops, Epoch, PrevV, Version, LeaderId}` message per committed
  # batch, with the batch's ops carried inside it. A batch is delivered whole or
  # not at all — see `broadcast_batch/5` in dgen_registry_member.
  defp names_batch(ops, epoch, prev_v, version, leader) do
    {:names_batch, ops, epoch, prev_v, version, leader}
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
        names_batch(
          [{:name_registered, :stale_name, pid, %{}, :undefined}],
          epoch - 1,
          v,
          v + 1,
          leader
        )
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
        names_batch(
          [{:name_registered, :current_name, pid, %{}, :undefined}],
          epoch,
          v,
          v + 1,
          leader
        )
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
        names_batch(
          [{:name_registered, :gapped_name, pid, %{}, :undefined}],
          epoch,
          v + 10,
          v + 11,
          leader
        )
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
        names_batch(
          [{:name_unregistered, :persisted_name, :undefined}],
          epoch - 1,
          v,
          v + 1,
          leader
        )
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
        names_batch([{:name_unregistered, :to_remove, :undefined}], epoch, v, v + 1, leader)
      )

      # Barrier: ensure the member has processed the cast before the caller-side read.
      :sys.get_state(member)

      assert :undefined == :dgen_registry.whereis_name({reg, :to_remove})
    end
  end

  # ---------------------------------------------------------------------------
  # apply_names_snapshot version-monotonicity — regression for the handoff-gather
  # race proven in formal/DgenRegistryReplication.tla. A snapshot re-baselines the
  # whole replica; the pre-fix code guarded only on epoch, so a STALE snapshot (an
  # old assume/resync snapshot delivered late, after the member applied a newer
  # broadcast) would overwrite the replica *backward* and silently drop an already
  # -acked binding. The fix requires the snapshot's version to be >= the member's
  # applied version. See formal/README.md "Discovered finding and the fix".
  # ---------------------------------------------------------------------------

  describe "apply_names_snapshot version-monotonicity (handoff-gather race)" do
    test "a stale-version snapshot does not overwrite a fresher replica", %{reg: reg} do
      member = :dgen_registry.member_name(reg)
      pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)

      # A live, acked binding held at the member's current applied version.
      :yes = :dgen_registry.register_name({reg, :race_name}, pid)
      epoch = :dgen_registry.get_epoch(reg)
      leader = :dgen_registry.get_leader(reg)
      v = applied_version(member)
      assert v > 0

      # A newly-assumed leader whose handoff gather raced an in-flight broadcast and
      # reconstructed a STALE (empty, lower-version) map — the exact shape TLC found.
      # The higher epoch would let the pre-fix (epoch-only) guard apply it and wipe
      # the binding; the version-monotonic guard rejects it because its version is
      # behind what the member has already applied.
      GenServer.cast(
        member,
        {:apply_names_snapshot, %{}, leader, [], %{}, epoch + 1, v - 1}
      )

      # Barrier: ensure the member has processed (and rejected) the stale snapshot.
      :sys.get_state(member)

      # The acked binding survived — the stale snapshot was ignored.
      assert pid == :dgen_registry.whereis_name({reg, :race_name})
    end

    test "a fresh-version snapshot still re-baselines the replica", %{reg: reg} do
      member = :dgen_registry.member_name(reg)
      old_pid = spawn(fn -> Process.sleep(:infinity) end)
      new_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(old_pid, :kill) end)
      on_exit(fn -> Process.exit(new_pid, :kill) end)

      :yes = :dgen_registry.register_name({reg, :old_name}, old_pid)
      epoch = :dgen_registry.get_epoch(reg)
      leader = :dgen_registry.get_leader(reg)
      v = applied_version(member)

      # A legitimate re-baseline at a >= version replaces the whole map (records are
      # #{Name => {Pid, Index, Data}}, Index the queryable-attr map, Data the metadata).
      # It must be applied: :old_name is gone, :new_name is present — the monotonic
      # guard does not block a forward snapshot.
      GenServer.cast(
        member,
        {:apply_names_snapshot, %{new_name: {new_pid, %{}, %{}}}, leader, [], %{}, epoch, v + 1}
      )

      :sys.get_state(member)

      assert new_pid == :dgen_registry.whereis_name({reg, :new_name})
      assert :undefined == :dgen_registry.whereis_name({reg, :old_name})
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
