defmodule DGen.RegistryGuaranteesTest do
  @moduledoc """
  Normative claims from `docs/design/dgen_registry_design.md` that the rest of the
  suite does not pin down.

  Every test here names the guarantee (§6) or non-goal (§7) it defends. The
  design doc says behaviour contradicting a guarantee is a defect and behaviour
  matching a non-goal is intentional — so a non-goal test asserts the *absence*
  of a property just as deliberately as a guarantee test asserts its presence.

  Single-node only. The cluster-scoped guarantees live in
  `dgen_registry_cluster_test.exs`.
  """
  use DGen.Case, async: true

  # ---------------------------------------------------------------------------
  # Helpers (mirroring dgen_registry_test.exs)
  # ---------------------------------------------------------------------------

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

  defp spawn_live do
    pid = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp stop_registry(sup) do
    try do
      Supervisor.stop(sup, :shutdown)
    catch
      :exit, _ -> :ok
    end
  end

  # Start a registry on `tenant` and block until its member has assumed and synced.
  defp start_reg!(tenant, prefix \\ "greg") do
    reg = :"#{prefix}_#{:erlang.unique_integer([:positive])}"
    {:ok, sup} = :dgen_registry.start_link(reg, tenant)
    assert :ok == :dgen_registry.await_ready(reg, 5_000)
    on_exit(fn -> stop_registry(sup) end)
    {reg, sup}
  end

  defp tuid(reg), do: {"dgen_registry.", Atom.to_string(reg)}

  # The member's current applied version, read through the same snapshot call the
  # handoff gather uses, so tests can craft contiguous (or gapped) broadcasts.
  defp applied_version(member) do
    {_records, version, _released} = GenServer.call(member, :get_names_snapshot)
    version
  end

  # A replication broadcast in the shape the leader actually sends: one
  # `{:names_batch, Ops, Epoch, PrevV, Version, LeaderId}` message per committed
  # batch, carrying the batch's ops. Delivered whole or not at all.
  defp names_batch(ops, epoch, prev_v, version, leader) do
    {:names_batch, ops, epoch, prev_v, version, leader}
  end

  defp names(matches), do: matches |> Enum.map(& &1.name) |> Enum.sort()

  # Size of the batch flipped as one group commit in the Guarantee 13 test.
  @batch_names 200

  # Block until a `set_metadata` call for every one of `names` is sitting in the
  # suspended member's mailbox.
  #
  # Reading another process's mailbox is what makes the batch boundary this test
  # needs an established fact rather than something a sleep hopes for.
  defp await_queued(member, names, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 5_000
    {:messages, queued} = member |> Process.whereis() |> Process.info(:messages)

    pending =
      for {:"$gen_call", _from, {:set_metadata, name, _index, _data}} <- queued, do: name

    cond do
      Enum.all?(names, &(&1 in pending)) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("only #{inspect(pending)} reached the member's mailbox, wanted #{inspect(names)}")

      true ->
        Process.sleep(5)
        await_queued(member, names, deadline)
    end
  end

  # Collect `gen: 2` match counts until the deadline, keeping every observation.
  defp sample_until(reg, deadline, acc) do
    if System.monotonic_time(:millisecond) >= deadline do
      Enum.reverse(acc)
    else
      n = length(:dgen_registry.query(reg, %{gen: 2}))

      # Stop early once the batch has fully landed — we have what we need and
      # every further sample is just the post-batch value.
      if n == @batch_names and length(acc) > 2 do
        Enum.reverse([n | acc])
      else
        sample_until(reg, deadline, [n | acc])
      end
    end
  end

  setup %{tenant: tenant} do
    {reg, _sup} = start_reg!(tenant)
    %{reg: reg}
  end

  # ---------------------------------------------------------------------------
  # Guarantee 8 — "Distinct registries do not share leadership, membership, name
  # tables, or database keys."
  #
  # Both registries below run on the *same* tenant, which is the case that
  # matters: isolation must come from the per-registry tuid (§4.4) and ETS table
  # (§4.8), not from keyspace separation the caller arranged.
  # ---------------------------------------------------------------------------

  describe "registry isolation (Guarantee 8)" do
    setup %{tenant: tenant} do
      {a, _} = start_reg!(tenant, "iso_a")
      {b, _} = start_reg!(tenant, "iso_b")
      %{a: a, b: b}
    end

    test "the same logical name binds independently in each registry", %{a: a, b: b} do
      pid_a = spawn_live()
      pid_b = spawn_live()

      # The *same* logical name in both registries: neither adjudicates the other's
      # binding, so both are `yes` — the definition of an independent namespace.
      assert :yes == :dgen_registry.register_name({a, :shared_name}, pid_a)
      assert :yes == :dgen_registry.register_name({b, :shared_name}, pid_b)

      assert pid_a == :dgen_registry.whereis_name({a, :shared_name})
      assert pid_b == :dgen_registry.whereis_name({b, :shared_name})

      assert pid_a == :dgen_registry.whereis_name_consistent({a, :shared_name})
      assert pid_b == :dgen_registry.whereis_name_consistent({b, :shared_name})

      # Unregistering in one leaves the other untouched.
      :ok = :dgen_registry.unregister_name({a, :shared_name})
      assert :undefined == :dgen_registry.whereis_name({a, :shared_name})
      assert pid_b == :dgen_registry.whereis_name({b, :shared_name})
    end

    test "leadership and membership are per registry", %{a: a, b: b} do
      # Each registry elects its own leader over its own member set. On one node
      # both leaders are that node, but they are *different members* — the member
      # id is {node, RegistryName} (§4.8), so the ids must differ.
      assert {node(), a} == :dgen_registry.get_leader(a)
      assert {node(), b} == :dgen_registry.get_leader(b)

      assert :dgen_registry.get_members(a) == [{node(), a}]
      assert :dgen_registry.get_members(b) == [{node(), b}]

      # Independent elections, so independent epoch counters.
      assert :dgen_registry.get_epoch(a) > 0
      assert :dgen_registry.get_epoch(b) > 0
    end

    test "name tables are distinct ETS tables holding only their own rows", %{a: a, b: b} do
      pid_a = spawn_live()
      pid_b = spawn_live()
      :yes = :dgen_registry.register_name({a, :only_in_a}, pid_a)
      :yes = :dgen_registry.register_name({b, :only_in_b}, pid_b)

      tab_a = :dgen_registry.names_table(a)
      tab_b = :dgen_registry.names_table(b)
      assert tab_a != tab_b

      rows = fn tab -> tab |> :ets.tab2list() |> Enum.map(&elem(&1, 0)) end

      assert :only_in_a in rows.(tab_a)
      refute :only_in_b in rows.(tab_a)

      assert :only_in_b in rows.(tab_b)
      refute :only_in_a in rows.(tab_b)

      # And the cross-registry lookup finds nothing.
      assert :undefined == :dgen_registry.whereis_name({a, :only_in_b})
      assert :undefined == :dgen_registry.whereis_name({b, :only_in_a})
    end

    test "a commit in one registry does not touch the other's durable keys", %{
      a: a,
      b: b,
      tenant: tenant
    } do
      v_a0 = :dgen_registry_names.read_version(tenant, tuid(a))
      v_b0 = :dgen_registry_names.read_version(tenant, tuid(b))

      pid = spawn_live()
      :yes = :dgen_registry.register_name({a, :bumps_a_only}, pid)

      assert eventually(fn ->
               :dgen_registry_names.read_version(tenant, tuid(a)) > v_a0
             end),
             "registering in #{a} did not bump its own version key"

      # B's version key is untouched: the two registries write disjoint keyspaces
      # even though they share a tenant.
      assert :dgen_registry_names.read_version(tenant, tuid(b)) == v_b0
    end

    test "subscriptions are per registry", %{a: a, b: b} do
      :ok = :dgen_registry.subscribe(a, :sub_in_a, %{role: :worker}, %{group: :l})
      assert eventually(fn -> Map.has_key?(:dgen_registry.subscriptions(a), :sub_in_a) end)

      assert :dgen_registry.subscriptions(b) == %{}
    end

    test "delete/2 on one registry leaves the other's durable state intact", %{
      tenant: tenant
    } do
      # Two fresh registries so this test owns their lifecycle (delete/2 requires
      # the registry be stopped first).
      {a, sup_a} = start_reg!(tenant, "del_a")
      {b, _sup_b} = start_reg!(tenant, "del_b")

      :ok = :dgen_registry.subscribe(a, :s, %{role: :worker}, %{group: :l})
      :ok = :dgen_registry.subscribe(b, :s, %{role: :worker}, %{group: :l})
      assert eventually(fn -> Map.has_key?(:dgen_registry.subscriptions(a), :s) end)
      assert eventually(fn -> Map.has_key?(:dgen_registry.subscriptions(b), :s) end)

      Process.unlink(sup_a)
      stop_registry(sup_a)
      assert eventually(fn -> Process.whereis(a) == nil end)
      :ok = :dgen_registry.delete(a, tenant)

      # B is still running and still holds its own durable subscription — the
      # range clear was scoped to A's tuid prefix.
      assert Map.has_key?(:dgen_registry.subscriptions(b), :s)

      # And B keeps working.
      pid = spawn_live()
      assert :yes == :dgen_registry.register_name({b, :still_alive}, pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Guarantee 13 — "query/2 and query_consistent/2 never observe a half-applied
  # group-commit batch — the result reflects one committed batch fully, or not at
  # all."
  #
  # This is the justification for routing queries through the member's mailbox
  # rather than serving them lock-free from the caller like whereis_name/1 (§4.7).
  #
  # The test forces a known batch boundary. Suspending the member queues a burst of
  # ops in its mailbox; on resume it drains them in order, the first starting a
  # commit and the rest accumulating into the *next* batch. So the `gen: 1 -> gen: 2`
  # flip of all N names lands as one batch, and a query may only see N or 0, never
  # a count between.
  #
  # **The pacer must be first in that mailbox**, and `Task.async` guarantees no such
  # ordering — so it is waited for rather than assumed. If a flip gets there first
  # it consumes the commit slot alone, the query legitimately observes exactly one
  # changed name, and the test reports a torn batch that never happened. That is
  # this test asserting a schedule instead of a property, and it fails only under
  # load: seen in CI, never locally.
  # ---------------------------------------------------------------------------

  describe "batch-consistent queries (Guarantee 13)" do
    test "a query never observes a partially-applied group commit", %{reg: reg} do
      names = for i <- 1..@batch_names, do: :"batched_#{i}"

      for name <- names do
        pid = spawn_live()
        :yes = :dgen_registry.register_name({reg, name}, pid, %{index: %{gen: 1}})
      end

      # A name outside the queried set, used purely to consume the first commit
      # slot so the N real flips coalesce into a single following batch.
      pacer = spawn_live()
      :yes = :dgen_registry.register_name({reg, :pacer}, pacer, %{index: %{pace: 1}})

      assert length(:dgen_registry.query(reg, %{gen: 1})) == @batch_names

      member = :dgen_registry.member_name(reg)
      :sys.suspend(member)

      # Queued first — this is the op that starts the commit.
      pacer_task =
        Task.async(fn -> :dgen_registry.set_metadata({reg, :pacer}, %{index: %{pace: 2}}) end)

      # Wait for the pacer's call to be queued *before* enqueuing the flips, so the
      # order the batch boundary depends on is established rather than raced for.
      await_queued(member, [:pacer])

      # Queued behind it — these all ride the one following batch.
      flips =
        for name <- names do
          Task.async(fn -> :dgen_registry.set_metadata({reg, name}, %{index: %{gen: 2}}) end)
        end

      await_queued(member, names)
      :sys.resume(member)

      # Sample the query as hard as we can while the batch commits and applies.
      parent = self()

      spawn_link(fn ->
        samples = sample_until(reg, System.monotonic_time(:millisecond) + 2_000, [])
        send(parent, {:samples, samples})
      end)

      assert :ok == Task.await(pacer_task, 5_000)
      for t <- flips, do: assert(:ok == Task.await(t, 5_000))

      assert_receive {:samples, samples}, 5_000

      # Non-vacuity: the sampler must actually straddle the transition, or the
      # invariant below would hold trivially. It does so structurally — its first
      # query queues behind the N already-enqueued ops but ahead of the commit
      # reply, so it is answered pre-batch — and it runs until the batch lands.
      assert 0 in samples, "sampler never observed the pre-batch state"
      assert @batch_names in samples, "sampler never observed the post-batch state"

      # The invariant. Any sample strictly between 0 and N would be a batch
      # observed half-applied — the exact failure mailbox-routing exists to
      # prevent. (A correct member applies the whole batch inside one
      # handle_info, so no query message can interleave with it at all.)
      torn = Enum.reject(samples, &(&1 in [0, @batch_names]))

      assert torn == [],
             "query observed #{length(torn)} half-applied batch state(s); " <>
               "matching-name counts were #{inspect(Enum.uniq(torn))}, " <>
               "expected only 0 or #{@batch_names}"

      # And the flip did land in full.
      assert eventually(fn -> length(:dgen_registry.query(reg, %{gen: 2})) == @batch_names end)
      assert :dgen_registry.query(reg, %{gen: 1}) == []
    end
  end

  # ---------------------------------------------------------------------------
  # Guarantee 11 — "Metadata writes are fenced and linearizable. set_metadata/2
  # rides the same fenced, single-leader commit pipeline as register_name/2,3; a
  # leader that has lost leadership cannot apply or broadcast a metadata change."
  #
  # `metadata_set` is the third broadcast on the pipeline that fences
  # `name_registered` and `name_unregistered`. One escaping the epoch and
  # contiguity checks would let a deposed leader mutate live rows and the inverted
  # index, so the same three cases are checked here.
  # ---------------------------------------------------------------------------

  describe "metadata broadcasts are epoch- and gap-fenced (Guarantee 11)" do
    setup %{reg: reg} do
      pid = spawn_live()

      :yes =
        :dgen_registry.register_name({reg, :fenced_meta}, pid, %{
          index: %{tier: :original},
          data: :original
        })

      %{member: :dgen_registry.member_name(reg), pid: pid}
    end

    test "a metadata_set from a stale epoch is discarded", %{
      reg: reg,
      member: member,
      pid: pid
    } do
      epoch = :dgen_registry.get_epoch(reg)
      leader = :dgen_registry.get_leader(reg)
      v = applied_version(member)

      # Contiguous version stamps, so only the stale epoch causes the discard.
      GenServer.cast(
        member,
        names_batch(
          [{:metadata_set, :fenced_meta, %{tier: :hijacked}, :hijacked}],
          epoch - 1,
          v,
          v + 1,
          leader
        )
      )

      :sys.get_state(member)

      assert {:ok, %{pid: ^pid, index: %{tier: :original}, data: :original}} =
               :dgen_registry.get_metadata({reg, :fenced_meta})

      # The inverted index is untouched too — a deposed leader must not be able to
      # move a row in or out of a query.
      assert names(:dgen_registry.query(reg, %{tier: :original})) == [:fenced_meta]
      assert :dgen_registry.query(reg, %{tier: :hijacked}) == []
    end

    test "a metadata_set with a version gap is not applied", %{
      reg: reg,
      member: member,
      pid: pid
    } do
      epoch = :dgen_registry.get_epoch(reg)
      leader = :dgen_registry.get_leader(reg)
      v = applied_version(member)

      # PrevVersion ahead of the member's applied version: a batch was missed, so
      # applying this would punch a hole in the replica's prefix (§4.5).
      GenServer.cast(
        member,
        names_batch(
          [{:metadata_set, :fenced_meta, %{tier: :gapped}, :gapped}],
          epoch,
          v + 10,
          v + 11,
          leader
        )
      )

      :sys.get_state(member)

      assert {:ok, %{pid: ^pid, index: %{tier: :original}, data: :original}} =
               :dgen_registry.get_metadata({reg, :fenced_meta})

      assert :dgen_registry.query(reg, %{tier: :gapped}) == []
    end

    test "a metadata_set with the current epoch and a contiguous version is applied", %{
      reg: reg,
      member: member,
      pid: pid
    } do
      epoch = :dgen_registry.get_epoch(reg)
      leader = :dgen_registry.get_leader(reg)
      v = applied_version(member)

      GenServer.cast(
        member,
        names_batch(
          [{:metadata_set, :fenced_meta, %{tier: :updated}, :updated}],
          epoch,
          v,
          v + 1,
          leader
        )
      )

      :sys.get_state(member)

      # The positive control for the two tests above: the same message shape, only
      # correctly stamped, does land — row and index together.
      assert {:ok, %{pid: ^pid, index: %{tier: :updated}, data: :updated}} =
               :dgen_registry.get_metadata({reg, :fenced_meta})

      assert names(:dgen_registry.query(reg, %{tier: :updated})) == [:fenced_meta]
      assert :dgen_registry.query(reg, %{tier: :original}) == []
    end
  end

  # ---------------------------------------------------------------------------
  # Non-goal 1 / Non-goal 10 — "Durability of names across a full restart. Pids
  # are never persisted. If every member of a registry is down at once, all of
  # its names are gone... A full cluster restart starts with an empty registry."
  # (and the same for metadata, Non-goal 10).
  #
  # The pairing is the point: the durable elector state survives a full restart,
  # and the name table deliberately does not.
  # ---------------------------------------------------------------------------

  describe "names are not durable across a full restart (Non-goals 1, 10)" do
    test "names and metadata are gone after a restart, while subscriptions survive", %{
      tenant: tenant
    } do
      reg = :"restart_#{:erlang.unique_integer([:positive])}"
      {:ok, sup1} = :dgen_registry.start_link(reg, tenant)
      assert :ok == :dgen_registry.await_ready(reg, 5_000)

      pid = spawn_live()
      :yes = :dgen_registry.register_name({reg, :ephemeral}, pid, %{index: %{role: :worker}})
      :ok = :dgen_registry.subscribe(reg, :durable_sub, %{role: :worker}, %{group: :l})
      assert eventually(fn -> Map.has_key?(:dgen_registry.subscriptions(reg), :durable_sub) end)

      assert pid == :dgen_registry.whereis_name({reg, :ephemeral})
      assert {:ok, _} = :dgen_registry.get_metadata({reg, :ephemeral})

      # Take the whole registry down and bring it back on the same tenant. The
      # registered process is deliberately still alive — its survival must not
      # rescue the binding, because the binding never existed anywhere but in the
      # member's ETS table.
      Process.unlink(sup1)
      stop_registry(sup1)
      assert eventually(fn -> Process.whereis(reg) == nil end)
      assert Process.alive?(pid)

      {:ok, sup2} = :dgen_registry.start_link(reg, tenant)
      on_exit(fn -> stop_registry(sup2) end)
      assert :ok == :dgen_registry.await_ready(reg, 5_000)

      # Non-goal 1: the name is gone, not restored.
      assert :undefined == :dgen_registry.whereis_name({reg, :ephemeral})
      assert :undefined == :dgen_registry.whereis_name_consistent({reg, :ephemeral})

      # Non-goal 10: so is its metadata, and its index entry.
      assert :undefined == :dgen_registry.get_metadata({reg, :ephemeral})
      assert :dgen_registry.query(reg, %{role: :worker}) == []

      # The contrast: durable elector state (§4.9) did survive.
      assert Map.has_key?(:dgen_registry.subscriptions(reg), :durable_sub)

      # And the name is free — re-registration is the application's job
      # (Non-goal 7), and it succeeds.
      assert :yes == :dgen_registry.register_name({reg, :ephemeral}, pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Non-goals 8, 9, 11, 12 — the deliberate limits of the query layer (§4.7).
  #
  # These assert absences. Each one is a property somebody could plausibly assume
  # holds, and the design says it does not.
  # ---------------------------------------------------------------------------

  describe "query-layer non-goals (Non-goals 8, 9, 11, 12)" do
    test "non-indexed `data` is never matched by a query (Non-goal 9)", %{reg: reg} do
      pid = spawn_live()

      # The very same attribute/value pair, but in `data` rather than `index`.
      :yes =
        :dgen_registry.register_name({reg, :opaque}, pid, %{
          index: %{},
          data: %{role: :worker}
        })

      # It round-trips verbatim through get_metadata...
      assert {:ok, %{data: %{role: :worker}}} = :dgen_registry.get_metadata({reg, :opaque})

      # ...but it is invisible to a query, and there is no post-filtering on it.
      assert :dgen_registry.query(reg, %{role: :worker}) == []
      assert :dgen_registry.query_consistent(reg, %{role: :worker}) == []
    end

    test "the same attribute in `index` and `data` matches only via `index`", %{reg: reg} do
      indexed = spawn_live()
      opaque = spawn_live()

      :yes =
        :dgen_registry.register_name({reg, :ix}, indexed, %{index: %{tier: :gold}})

      :yes =
        :dgen_registry.register_name({reg, :dx}, opaque, %{data: %{tier: :gold}})

      assert names(:dgen_registry.query(reg, %{tier: :gold})) == [:ix]
    end

    test "the index is a multimap — no uniqueness constraint on values (Non-goal 11)", %{
      reg: reg
    } do
      # Many registrations sharing one attribute/value pair is not an error and
      # all of them match.
      for i <- 1..5 do
        pid = spawn_live()
        :yes = :dgen_registry.register_name({reg, :"dup_#{i}"}, pid, %{index: %{shard: 1}})
      end

      assert names(:dgen_registry.query(reg, %{shard: 1})) ==
               [:dup_1, :dup_2, :dup_3, :dup_4, :dup_5]
    end

    test "metadata is per registration, not per pid (Non-goal 12)", %{reg: reg} do
      pid = spawn_live()

      # One process under two names, with independent metadata for each.
      :yes = :dgen_registry.register_name({reg, :name_one}, pid, %{index: %{role: :primary}})
      :yes = :dgen_registry.register_name({reg, :name_two}, pid, %{index: %{role: :standby}})

      assert {:ok, %{index: %{role: :primary}}} = :dgen_registry.get_metadata({reg, :name_one})
      assert {:ok, %{index: %{role: :standby}}} = :dgen_registry.get_metadata({reg, :name_two})

      assert names(:dgen_registry.query(reg, %{role: :primary})) == [:name_one]
      assert names(:dgen_registry.query(reg, %{role: :standby})) == [:name_two]

      # Changing one name's metadata leaves the other name's alone, even though
      # both name the same pid.
      :ok = :dgen_registry.set_metadata({reg, :name_one}, %{index: %{role: :retired}})

      assert {:ok, %{index: %{role: :retired}}} = :dgen_registry.get_metadata({reg, :name_one})
      assert {:ok, %{index: %{role: :standby}}} = :dgen_registry.get_metadata({reg, :name_two})
    end

    test "an unmatched attribute yields an empty result, not a schema error (Non-goal 8)", %{
      reg: reg
    } do
      pid = spawn_live()
      :yes = :dgen_registry.register_name({reg, :q1}, pid, %{index: %{region: :us_east}})

      # No declared schema of indexed attributes to check a clause against, so a
      # clause naming an attribute nothing carries simply matches nothing (§4.7).
      assert :dgen_registry.query(reg, %{no_such_attribute: :whatever}) == []

      # Adding an unsatisfiable clause to a satisfiable one ANDs to empty.
      assert :dgen_registry.query(reg, %{region: :us_east}) |> names() == [:q1]
      assert :dgen_registry.query(reg, %{region: :us_east, no_such_attribute: 1}) == []
    end

    test "matching is exact equality — no prefix, range, or coercion (Non-goal 8)", %{reg: reg} do
      pid = spawn_live()

      :yes =
        :dgen_registry.register_name({reg, :exact}, pid, %{
          index: %{region: :us_east, shard: 10}
        })

      assert names(:dgen_registry.query(reg, %{region: :us_east})) == [:exact]

      # A prefix of the value does not match.
      assert :dgen_registry.query(reg, %{region: :us}) == []
      # A different type carrying the "same" text does not match.
      assert :dgen_registry.query(reg, %{region: "us_east"}) == []
      # Ranges are not a thing — an integer clause is equality only.
      assert names(:dgen_registry.query(reg, %{shard: 10})) == [:exact]
      assert :dgen_registry.query(reg, %{shard: 9}) == []
    end

    test "an empty constraints map is rejected rather than meaning `all`", %{reg: reg} do
      pid = spawn_live()
      :yes = :dgen_registry.register_name({reg, :some_name}, pid, %{index: %{a: 1}})

      assert {:error, :empty_query} == :dgen_registry.query(reg, %{})
      assert {:error, :empty_query} == :dgen_registry.query_consistent(reg, %{})
    end
  end
end
