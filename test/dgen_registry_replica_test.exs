defmodule DGen.RegistryReplicaTest do
  @moduledoc """
  The replica's admission rules, in isolation — no member process, no backend.

  Each `describe` is one of the TLA+ model's receive actions
  (formal/DgenRegistryReplication.tla) that `dgen_registry_replica` implements:
  `offer_batch/5` is RecvBcastApply / RecvBcastDrop / RecvBcastGap, and
  `offer_snapshot/4` is RecvSnap with the SafeAssumeSnap guard. The invariant they
  protect is that two replicas at the same version hold the same content.
  """
  use ExUnit.Case, async: true

  alias :dgen_registry_replica, as: Replica

  # A fresh replica at {epoch, version}, holding `records`. Each test gets its own
  # uniquely named table (the replica's table is a named ETS table).
  defp replica(epoch, version, records \\ %{}) do
    name = :"replica_test_#{System.unique_integer([:positive])}"
    Replica.adopt(epoch, version, records, Replica.new(name))
  end

  defp reg(name, pid, index \\ %{}), do: {:name_registered, name, pid, index, nil}
  defp unreg(name, pid), do: {:name_unregistered, name, pid}

  defp pids(r), do: Map.new(Replica.records(r), fn {n, {p, _, _}} -> {n, p} end)

  setup do
    {:ok,
     p1: spawn(fn -> Process.sleep(:infinity) end), p2: spawn(fn -> Process.sleep(:infinity) end)}
  end

  describe "offer_batch/5 — RecvBcastApply" do
    test "a contiguous batch applies whole and advances the version", %{p1: p1, p2: p2} do
      r = replica(1, 10)

      assert {:applied, r} = Replica.offer_batch(1, 10, 20, [reg(:a, p1), reg(:b, p2)], r)
      assert Replica.version(r) == 20
      assert pids(r) == %{a: p1, b: p2}
    end

    test "a batch that binds and clears one name ends without it", %{p1: p1} do
      r = replica(1, 10)

      assert {:applied, r} = Replica.offer_batch(1, 10, 20, [reg(:a, p1), unreg(:a, p1)], r)
      assert pids(r) == %{}
    end

    test "a newer epoch's contiguous batch applies but does not raise the epoch", %{p1: p1} do
      r = replica(1, 10)

      assert {:applied, r} = Replica.offer_batch(2, 10, 20, [reg(:a, p1)], r)
      assert Replica.epoch(r) == 1
      assert Replica.version(r) == 20
    end

    test "the heartbeat (an empty batch at {V, V}) is a no-op on a caught-up replica" do
      r = replica(1, 10)

      assert {:applied, r} = Replica.offer_batch(1, 10, 10, [], r)
      assert Replica.version(r) == 10
    end

    test "metadata changes keep the inverted index in step", %{p1: p1} do
      r = replica(1, 10)
      {:applied, r} = Replica.offer_batch(1, 10, 20, [reg(:a, p1, %{role: :w})], r)
      assert [%{name: :a}] = Replica.query(r, {:all, %{role: :w}})

      {:applied, r} = Replica.offer_batch(1, 20, 30, [{:metadata_set, :a, %{role: :x}, nil}], r)
      assert [] = Replica.query(r, {:all, %{role: :w}})
      assert [%{name: :a}] = Replica.query(r, {:all, %{role: :x}})
    end
  end

  describe "offer_batch/5 — RecvBcastDrop" do
    test "a deposed leader's batch is refused", %{p1: p1} do
      r = replica(2, 10)

      assert :stale_epoch = Replica.offer_batch(1, 10, 20, [reg(:a, p1)], r)
    end

    test "a batch at or behind our version is a duplicate", %{p1: p1} do
      r = replica(1, 20)

      assert :duplicate = Replica.offer_batch(1, 10, 20, [reg(:a, p1)], r)
      assert :duplicate = Replica.offer_batch(1, 5, 15, [reg(:a, p1)], r)
    end
  end

  describe "offer_batch/5 — RecvBcastGap" do
    test "a batch past a hole applies nothing", %{p1: p1} do
      r = replica(1, 10, %{})

      assert :gap = Replica.offer_batch(1, 15, 20, [reg(:a, p1)], r)
      assert pids(r) == %{}
      assert Replica.version(r) == 10
    end

    test "a heartbeat ahead of us reveals a lost tail" do
      r = replica(1, 10)

      assert :gap = Replica.offer_batch(1, 20, 20, [], r)
    end
  end

  describe "offer_snapshot/4 — RecvSnap" do
    test "a current-epoch snapshot at or past our version re-baselines", %{p1: p1, p2: p2} do
      r = replica(1, 10, %{a: {p1, %{}, nil}})

      assert {:accepted, r} = Replica.offer_snapshot(1, 20, %{b: {p2, %{}, nil}}, r)
      assert pids(r) == %{b: p2}
      assert Replica.version(r) == 20
    end

    test "a newer epoch's snapshot sets the epoch", %{p1: p1} do
      r = replica(1, 10)

      assert {:accepted, r} = Replica.offer_snapshot(3, 10, %{a: {p1, %{}, nil}}, r)
      assert Replica.epoch(r) == 3
    end

    test "a snapshot behind our version is refused — no rollback", %{p1: p1, p2: p2} do
      r = replica(1, 20, %{a: {p1, %{}, nil}})

      assert :stale = Replica.offer_snapshot(2, 10, %{b: {p2, %{}, nil}}, r)
      assert pids(r) == %{a: p1}
    end

    test "an older epoch's snapshot is refused", %{p1: p1} do
      r = replica(2, 10)

      assert :stale = Replica.offer_snapshot(1, 30, %{a: {p1, %{}, nil}}, r)
    end
  end

  describe "adopt/4 — a wholesale re-baseline" do
    test "replaces the content in place: kept names updated, absent names removed", %{
      p1: p1,
      p2: p2
    } do
      r = replica(1, 10, %{a: {p1, %{k: 1}, nil}, b: {p1, %{}, nil}})

      r = Replica.adopt(2, 20, %{a: {p2, %{k: 2}, nil}, c: {p2, %{}, nil}}, r)
      assert pids(r) == %{a: p2, c: p2}
      assert [] = Replica.query(r, {:all, %{k: 1}})
      assert [%{name: :a}] = Replica.query(r, {:all, %{k: 2}})
      assert {Replica.epoch(r), Replica.version(r)} == {2, 20}
    end
  end

  describe "commit/5 — the leader's own batch" do
    test "applies only the delta's names and advances the version", %{p1: p1, p2: p2} do
      r = replica(1, 10, %{a: {p1, %{}, nil}, b: {p2, %{}, nil}})

      r = Replica.commit(20, %{a: :clear}, %{}, %{}, r)
      assert pids(r) == %{b: p2}
      assert Replica.version(r) == 20
    end

    test "a metadata-only op never resurrects an optimistically deleted row", %{p1: p1} do
      r = replica(1, 10, %{a: {p1, %{}, nil}})
      r = Replica.delete(r, :a)

      r = Replica.commit(20, %{a: {:meta, node()}}, %{}, %{a: {%{k: 1}, nil}}, r)
      assert pids(r) == %{}
      assert [] = Replica.query(r, {:all, %{k: 1}})
    end
  end
end
