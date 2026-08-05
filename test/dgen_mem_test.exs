defmodule DgenMemTest do
  @moduledoc """
  Phase 2 of the simulation testing work (see the `eta` library's docs/design.md).

  Two kinds of test here, and the second is the one that matters.

  **Semantics** — the FDB behaviours the registry depends on: MVCC read versions,
  read-conflict detection, versionstamps, watches.

  **Differential** — the same operations run against `:dgen_erlfdb` and `:dgen_mem`,
  asserting identical results. A reimplementation is only worth trusting to the
  extent it has been compared against the thing it reimplements; testing it against
  its own author's understanding proves nothing about fidelity.
  """
  use ExUnit.Case, async: false

  @mem :dgen_mem
  @fdb :dgen_erlfdb

  setup do
    name = :"memtest_#{:erlang.unique_integer([:positive])}"
    db = @mem.open(%{name: name})
    on_exit(fn -> @mem.close(db) end)
    %{db: db, name: name}
  end

  defp dir(db), do: @mem.dir_create(db, :erlfdb_subspace.create({}, ""), "t")

  # ---------------------------------------------------------------------------
  # Core key/value semantics
  # ---------------------------------------------------------------------------

  describe "reads and writes" do
    test "a value round-trips", %{db: db} do
      @mem.transactional(db, fn tx -> @mem.set(tx, "k", "v") end)
      assert @mem.transactional(db, fn tx -> @mem.wait(@mem.get(tx, "k")) end) == "v"
    end

    test "a missing key reads not_found", %{db: db} do
      assert @mem.transactional(db, fn tx -> @mem.wait(@mem.get(tx, "nope")) end) == :not_found
    end

    test "a transaction sees its own writes before commit", %{db: db} do
      result =
        @mem.transactional(db, fn tx ->
          @mem.set(tx, "rw", "staged")
          @mem.wait(@mem.get(tx, "rw"))
        end)

      assert result == "staged"
    end

    test "clear_range removes a span, including staged writes", %{db: db} do
      @mem.transactional(db, fn tx ->
        for k <- ["a1", "a2", "a3", "b1"], do: @mem.set(tx, k, "v")
      end)

      @mem.transactional(db, fn tx ->
        @mem.set(tx, "a9", "staged")
        @mem.clear_range(tx, "a", "b")
      end)

      rows = @mem.transactional(db, fn tx -> @mem.get_range(tx, "", "z", []) end)
      assert rows == [{"b1", "v"}]
    end

    test "get_range honours limit and reverse", %{db: db} do
      @mem.transactional(db, fn tx ->
        for n <- 1..5, do: @mem.set(tx, "k#{n}", "v#{n}")
      end)

      all = @mem.transactional(db, fn tx -> @mem.get_range(tx, "k", "l", []) end)
      assert Enum.map(all, &elem(&1, 0)) == ["k1", "k2", "k3", "k4", "k5"]

      limited = @mem.transactional(db, fn tx -> @mem.get_range(tx, "k", "l", limit: 2) end)
      assert Enum.map(limited, &elem(&1, 0)) == ["k1", "k2"]

      reversed = @mem.transactional(db, fn tx -> @mem.get_range(tx, "k", "l", reverse: true) end)
      assert Enum.map(reversed, &elem(&1, 0)) == ["k5", "k4", "k3", "k2", "k1"]
    end

    test "add performs a little-endian integer add", %{db: db} do
      @mem.transactional(db, fn tx -> @mem.add(tx, "ctr", 5) end)
      @mem.transactional(db, fn tx -> @mem.add(tx, "ctr", 3) end)

      raw = @mem.transactional(db, fn tx -> @mem.wait(@mem.get(tx, "ctr")) end)
      <<n::64-little-unsigned>> = raw
      assert n == 8
    end
  end

  # ---------------------------------------------------------------------------
  # MVCC — the property the registry's fence rests on
  # ---------------------------------------------------------------------------

  describe "read versions" do
    test "a pinned read version reads the past", %{db: db} do
      @mem.transactional(db, fn tx -> @mem.set(tx, "mv", "first") end)

      v1 =
        @mem.transactional(db, fn tx ->
          @mem.wait(@mem.get_read_version(tx))
        end)

      @mem.transactional(db, fn tx -> @mem.set(tx, "mv", "second") end)

      # Current read sees the new value; a transaction pinned to v1 must not.
      assert @mem.transactional(db, fn tx -> @mem.wait(@mem.get(tx, "mv")) end) == "second"

      pinned =
        @mem.transactional(db, fn tx ->
          @mem.set_read_version(tx, v1)
          @mem.wait(@mem.get(tx, "mv"))
        end)

      assert pinned == "first",
             "a pinned read version read the present — MVCC is not implemented"
    end

    test "get_committed_version reports the version a commit landed at", %{db: db} do
      tx = @mem.create_transaction(db)
      @mem.set(tx, "cv", "x")
      @mem.wait(@mem.commit(tx))

      v = @mem.get_committed_version(tx)
      assert is_integer(v) and v > 0
    end
  end

  describe "read conflicts" do
    test "a commit conflicts when a read-conflict key was written after its read version",
         %{db: db} do
      @mem.transactional(db, fn tx -> @mem.set(tx, "fence", "leader_a") end)

      # Open a transaction pinned to now, then let someone else move the key.
      tx = @mem.create_transaction(db)
      @mem.add_read_conflict_key(tx, "fence")
      _ = @mem.wait(@mem.get_read_version(tx))
      @mem.set(tx, "other", "work")

      @mem.transactional(db, fn other -> @mem.set(other, "fence", "leader_b") end)

      # This is exactly the registry's leader fence: the commit must be rejected.
      assert catch_error(@mem.wait(@mem.commit(tx))) == {:erlfdb_error, 1020}
    end

    test "a commit succeeds when the conflict key was untouched", %{db: db} do
      @mem.transactional(db, fn tx -> @mem.set(tx, "fence", "leader_a") end)

      tx = @mem.create_transaction(db)
      @mem.add_read_conflict_key(tx, "fence")
      _ = @mem.wait(@mem.get_read_version(tx))
      @mem.set(tx, "other", "work")

      @mem.transactional(db, fn other -> @mem.set(other, "unrelated", "x") end)

      assert @mem.wait(@mem.commit(tx)) == :ok
    end

    test "transactional/2 retries a conflicted body", %{db: db} do
      # A conflicting commit must be retried by the closure runner, not surfaced.
      counter = :counters.new(1, [])

      @mem.transactional(db, fn tx -> @mem.set(tx, "seed", "0") end)

      result =
        @mem.transactional(db, fn tx ->
          :counters.add(counter, 1, 1)
          @mem.add_read_conflict_key(tx, "seed")
          _ = @mem.wait(@mem.get_read_version(tx))

          # Only on the first attempt, race ourselves.
          if :counters.get(counter, 1) == 1 do
            @mem.transactional(db, fn other -> @mem.set(other, "seed", "1") end)
          end

          @mem.set(tx, "out", "done")
          :finished
        end)

      assert result == :finished
      assert :counters.get(counter, 1) == 2, "the body was not retried exactly once"
    end
  end

  # ---------------------------------------------------------------------------
  # Versionstamps — the registry's version key depends on the exact layout
  # ---------------------------------------------------------------------------

  describe "versionstamps" do
    test "a versionstamped value decodes as a monotonically increasing integer", %{db: db} do
      placeholder = <<0::size(14)-unit(8)>>

      stamps =
        for _ <- 1..5 do
          @mem.transactional(db, fn tx ->
            @mem.set_versionstamped_value(tx, "vs", placeholder)
          end)

          raw = @mem.transactional(db, fn tx -> @mem.wait(@mem.get(tx, "vs")) end)
          :binary.decode_unsigned(raw, :big)
        end

      assert length(stamps) == 5
      assert stamps == Enum.sort(stamps), "versionstamps are not monotonic: #{inspect(stamps)}"
      assert length(Enum.uniq(stamps)) == 5, "versionstamps repeated"
    end

    test "the stamp is 10 bytes and its top 8 are the commit version", %{db: db} do
      placeholder = <<0::size(14)-unit(8)>>

      tx = @mem.create_transaction(db)
      @mem.set_versionstamped_value(tx, "vs2", placeholder)
      @mem.wait(@mem.commit(tx))
      committed = @mem.get_committed_version(tx)

      raw = @mem.transactional(db, fn t -> @mem.wait(@mem.get(t, "vs2")) end)
      assert byte_size(raw) == 10

      # This is what dgen_registry_names:read_committed_frontier/2 computes.
      assert Bitwise.bsr(:binary.decode_unsigned(raw, :big), 16) == committed
    end
  end

  # ---------------------------------------------------------------------------
  # Watches
  # ---------------------------------------------------------------------------

  describe "watches" do
    test "a watch fires when its key is written", %{db: db} do
      ref =
        @mem.transactional(db, fn tx ->
          {:dgen_future, ref, _} = @mem.watch(tx, "watched")
          ref
        end)

      refute_receive {^ref, :ready}, 50

      @mem.transactional(db, fn tx -> @mem.set(tx, "watched", "changed") end)
      assert_receive {^ref, :ready}, 500
    end

    test "a watch can be directed at another process", %{db: db} do
      parent = self()
      target = spawn(fn -> receive do: (m -> send(parent, {:got, m})) end)

      ref =
        @mem.transactional(db, fn tx ->
          {:dgen_future, ref, _} = @mem.watch(tx, "w2", to: target)
          ref
        end)

      @mem.transactional(db, fn tx -> @mem.set(tx, "w2", "x") end)
      assert_receive {:got, {^ref, :ready}}, 500
    end

    # A watch is anchored to the value the *creating transaction* would read, not
    # to the moment it is registered. Both tests below fall out of that one rule,
    # and both are load-bearing:
    #
    #   - `dgen_server:consume_queued/5` reads an empty queue and then watches the
    #     push key. A push that commits in between is exactly the first case; miss
    #     it and the consumer sleeps on a queue that already has work, so the
    #     caller's `dgen:call/4` times out with nothing wrong anywhere else.
    #   - `dgen:push_call/7` writes the reply sentinel and watches it in one
    #     transaction. Firing on its own write, the second case, costs a wasted
    #     read-and-rewatch round trip on *every* call — and that round trip is the
    #     window the first case then loses a reply in.
    test "a write after the read version fires a watch registered later", %{db: db} do
      tx = @mem.create_transaction(db)
      assert @mem.wait(@mem.get(tx, "late")) == :not_found

      # Someone else moves the key while this transaction is still open.
      @mem.transactional(db, fn other -> @mem.set(other, "late", "moved") end)

      {:dgen_future, ref, _} = @mem.watch(tx, "late")
      @mem.wait(@mem.commit(tx))

      assert_receive {^ref, :ready}, 500
    end

    test "a transaction's own write does not fire its watch", %{db: db} do
      ref =
        @mem.transactional(db, fn tx ->
          @mem.set(tx, "own", "mine")
          {:dgen_future, ref, _} = @mem.watch(tx, "own")
          ref
        end)

      refute_receive {^ref, :ready}, 50

      @mem.transactional(db, fn tx -> @mem.set(tx, "own", "theirs") end)
      assert_receive {^ref, :ready}, 500
    end
  end

  # ---------------------------------------------------------------------------
  # Commit faults
  # ---------------------------------------------------------------------------

  describe "fault injection" do
    test "conflict_p produces retryable conflicts that transactional/2 absorbs", %{db: db} do
      :ok = @mem.set_faults(db, %{conflict_p: 0.5})

      # Every one of these must still complete: not_committed is retryable.
      for n <- 1..30 do
        @mem.transactional(db, fn tx -> @mem.set(tx, "f#{n}", "v") end)
      end

      :ok = @mem.set_faults(db, %{})

      assert @mem.stats(db).conflicts > 0, "conflict_p injected nothing at p=0.5 over 30 commits"

      rows = @mem.transactional(db, fn tx -> @mem.get_range(tx, "f", "g", []) end)
      assert length(rows) == 30, "a retryable conflict lost a write"
    end

    test "commit_fail_p surfaces a non-retryable error", %{db: db} do
      :ok = @mem.set_faults(db, %{commit_fail_p: 1.0})

      assert catch_error(@mem.transactional(db, fn tx -> @mem.set(tx, "x", "y") end)) ==
               {:erlfdb_error, 1510}
    end

    test "the same seed injects the same faults", %{name: name} do
      runs =
        for _ <- 1..3 do
          db = @mem.open(%{name: :"#{name}_seeded", seed: 7, faults: %{conflict_p: 0.4}})
          for n <- 1..20, do: @mem.transactional(db, fn tx -> @mem.set(tx, "k#{n}", "v") end)
          c = @mem.stats(db).conflicts
          @mem.close(db)
          c
        end

      assert length(Enum.uniq(runs)) == 1,
             "the same seed produced different fault counts: #{inspect(runs)}"

      assert hd(runs) > 0
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrency
  #
  # FoundationDB serialises commits. Without the same guarantee here a second
  # transaction slips between a commit's conflict check and its writes, and three
  # things break quietly: a conflict is missed (so a transaction that should have
  # been fenced commits — the registry's leader fence, §5.1), a key's version list
  # loses an entry to a lost read-modify-write, and versions land out of order so a
  # read returns an older value than it should.
  #
  # The rest of the suite exercises this constantly and would only show it as
  # intermittent failures elsewhere, so it is asserted directly here.
  # ---------------------------------------------------------------------------

  describe "concurrent commits" do
    test "concurrent writers do not lose writes", %{db: db} do
      writers = 16
      per_writer = 25

      tasks =
        for w <- 1..writers do
          Task.async(fn ->
            for n <- 1..per_writer do
              @mem.transactional(db, fn tx -> @mem.set(tx, "c_#{w}_#{n}", "v") end)
            end
          end)
        end

      for t <- tasks, do: Task.await(t, 30_000)

      rows = @mem.transactional(db, fn tx -> @mem.get_range(tx, "c_", "c`", []) end)

      assert length(rows) == writers * per_writer,
             "lost #{writers * per_writer - length(rows)} of #{writers * per_writer} writes"
    end

    test "concurrent increments of one key are all applied", %{db: db} do
      # Every writer touches the *same* key, so each commit is a read-modify-write
      # on one version list — the case a lost update actually corrupts.
      writers = 12
      each = 20

      tasks =
        for _ <- 1..writers do
          Task.async(fn ->
            for _ <- 1..each do
              @mem.transactional(db, fn tx -> @mem.add(tx, "shared_ctr", 1) end)
            end
          end)
        end

      for t <- tasks, do: Task.await(t, 30_000)

      raw = @mem.transactional(db, fn tx -> @mem.wait(@mem.get(tx, "shared_ctr")) end)
      <<total::64-little-unsigned>> = raw

      assert total == writers * each,
             "counter reached #{total}, expected #{writers * each} — a commit was lost"
    end

    test "a fenced writer cannot commit under concurrency", %{db: db} do
      # The registry's leader fence, run concurrently: many writers each pin a read
      # version, take a read-conflict on the shared fence key, and try to bump it.
      # Exactly one may win per generation — a missed conflict means two "leaders"
      # committed, which is the split-brain the fence exists to prevent.
      @mem.transactional(db, fn tx -> @mem.set(tx, "leader", "none") end)

      results =
        for _ <- 1..20 do
          Task.async(fn ->
            tx = @mem.create_transaction(db)
            @mem.add_read_conflict_key(tx, "leader")
            _ = @mem.wait(@mem.get_read_version(tx))
            # Give every contender the same read version before any of them commits.
            Process.sleep(5)
            @mem.set(tx, "leader", "me")

            try do
              @mem.wait(@mem.commit(tx))
              :won
            catch
              :error, {:erlfdb_error, 1020} -> :fenced
            end
          end)
        end
        |> Enum.map(&Task.await(&1, 30_000))

      won = Enum.count(results, &(&1 == :won))

      assert won == 1,
             "#{won} writers committed against one fence key; exactly one may win"
    end
  end

  # ---------------------------------------------------------------------------
  # Differential: identical behaviour to the real backend
  # ---------------------------------------------------------------------------

  describe "differential against dgen_erlfdb" do
    # Every `dgen_mem` defect found so far came from running the same operations
    # against both backends and diffing, rather than from reading the code. This is
    # where to add a case when one is suspected.
    #
    # These open the FoundationDB sandbox directly, so they are the one part of
    # this file that genuinely needs FDB installed — which is exactly what
    # `DGEN_BACKEND=dgen_mem mix test` exists to do without. Excluded there (see
    # test/test_helper.exs); they run on the default backend, where the comparison
    # is the point.
    @describetag :differential

    setup do
      {fdb_db, fdb_dir} = @fdb.sandbox_open("dgen_mem_diff", "dgen_mem_diff")
      id = Base.encode16(:crypto.strong_rand_bytes(8))
      fdb_case = @fdb.dir_create(fdb_db, fdb_dir, id)
      on_exit(fn -> @fdb.dir_remove(fdb_db, fdb_dir, id) end)
      %{fdb: {fdb_db, fdb_case}}
    end

    test "key packing is byte-identical", %{db: db, fdb: {_, fdb_dir}} do
      mem_dir = dir(db)

      # The tuple shapes dgen actually packs: registry tuids, queue keys, and the
      # timestamped reply keys from dgen:get_from/2.
      tuples = [
        {"dgen_registry.", "orders"},
        {"dgen_registry.", "orders", "version"},
        {"dgen_registry.", "orders", "leader"},
        {"q", 1, "push"},
        {"q", 0},
        {"state", 12_345, "x"}
      ]

      for t <- tuples do
        mem = @mem.dir_pack(mem_dir, t)
        fdb = @fdb.dir_pack(fdb_dir, t)

        # Prefixes differ (different directory allocation), so compare the packed
        # tuple portion — which is what has to be order-preserving and identical.
        assert strip_prefix(mem, @mem.dir_pack(mem_dir, {})) ==
                 strip_prefix(fdb, @fdb.dir_pack(fdb_dir, {})),
               "packing diverged for #{inspect(t)}"
      end
    end

    test "key ordering matches", %{db: db, fdb: {_, fdb_dir}} do
      mem_dir = dir(db)

      tuples = [
        {"a"},
        {"a", 1},
        {"a", 2},
        {"a", 10},
        {"b"},
        {"a", "z"},
        {"a", 1, "x"}
      ]

      mem_order = tuples |> Enum.sort_by(&@mem.dir_pack(mem_dir, &1)) |> Enum.map(& &1)
      fdb_order = tuples |> Enum.sort_by(&@fdb.dir_pack(fdb_dir, &1)) |> Enum.map(& &1)

      assert mem_order == fdb_order,
             "packed key ordering differs, so range scans would diverge"
    end

    test "unpack inverts pack", %{db: db} do
      mem_dir = dir(db)

      for t <- [{"x"}, {"x", 1}, {"x", "y", 2}] do
        assert @mem.dir_unpack(mem_dir, @mem.dir_pack(mem_dir, t)) == t
      end
    end

    test "the same workload leaves the same visible state in both backends", %{
      db: db,
      fdb: {fdb_db, fdb_dir}
    } do
      mem_dir = dir(db)

      ops = [
        {:set, {"k", 1}, "one"},
        {:set, {"k", 2}, "two"},
        {:set, {"k", 3}, "three"},
        {:clear, {"k", 2}},
        {:set, {"k", 10}, "ten"},
        {:set, {"j", 1}, "other"}
      ]

      run = fn backend, dir_handle, handle ->
        for op <- ops do
          backend.transactional(handle, fn tx ->
            case op do
              {:set, t, v} ->
                backend.set(tx, backend.dir_pack(dir_handle, t), v)

              {:clear, t} ->
                k = backend.dir_pack(dir_handle, t)
                backend.clear_range(tx, k, backend.key_strinc(k))
            end
          end)
        end

        {s, e} = backend.dir_range(dir_handle, {"k"})

        backend.transactional(handle, fn tx ->
          backend.get_range(tx, s, e, wait: true)
          |> Enum.map(fn {k, v} -> {backend.dir_unpack(dir_handle, k), v} end)
        end)
      end

      assert run.(@mem, mem_dir, db) == run.(@fdb, fdb_dir, fdb_db)
    end

    test "a watch is anchored to the creating transaction, not to when it registers",
         %{db: db, fdb: {fdb_db, fdb_dir}} do
      mem_dir = dir(db)

      # The key the watch is on is written *after* the transaction takes its read
      # version but *before* it registers the watch. A backend that registers
      # watches against the present rather than against the transaction sees no
      # subsequent write and never fires.
      raced = fn backend, dir_handle, handle ->
        k = backend.dir_pack(dir_handle, {"race"})

        tx = backend.create_transaction(handle)
        _ = backend.wait(backend.get(tx, k))
        backend.transactional(handle, fn other -> backend.set(other, k, "moved") end)
        {:dgen_future, ref, _} = backend.watch(tx, k)
        backend.wait(backend.commit(tx))

        receive do
          {^ref, :ready} -> :fired
        after
          500 -> :silent
        end
      end

      # And the mirror image: a watch is relative to the value its own transaction
      # would read, so that transaction's own write is not a change.
      own = fn backend, dir_handle, handle ->
        k = backend.dir_pack(dir_handle, {"own"})

        ref =
          backend.transactional(handle, fn tx ->
            backend.set(tx, k, "mine")
            {:dgen_future, ref, _} = backend.watch(tx, k)
            ref
          end)

        receive do
          {^ref, :ready} -> :fired
        after
          200 -> :silent
        end
      end

      assert raced.(@mem, mem_dir, db) == raced.(@fdb, fdb_dir, fdb_db)
      assert own.(@mem, mem_dir, db) == own.(@fdb, fdb_dir, fdb_db)
    end
  end

  defp strip_prefix(full, prefix) do
    size = byte_size(prefix)
    binary_part(full, size, byte_size(full) - size)
  end
end
