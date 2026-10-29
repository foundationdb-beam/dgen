defmodule DGen.RegistryPipelineTest do
  @moduledoc """
  The leader's write pipeline, in isolation — no member process, no backend.

  Pins the phases (queued → committing → awaiting holders) and the one rule for an
  op the leader cannot finish, `surrender/2`: by kind, the same idempotence rule as
  the follower's intents. A registration is never answered `no` because the leader
  could not finish it.
  """
  use ExUnit.Case, async: true

  alias :dgen_registry_pipeline, as: Pipeline

  @meta {%{}, :undefined}
  @follower {:n2@host, :reg}

  setup do
    {:ok, pid: spawn(fn -> Process.sleep(:infinity) end), from: {self(), make_ref()}}
  end

  describe "queued and committing" do
    test "batches are taken oldest first, bounded, and one commit runs at a time", %{pid: pid} do
      p = Enum.reduce(1..3, Pipeline.new(), &Pipeline.enqueue({:retract, &1, pid}, &2))

      {batch, p} = Pipeline.take_batch(2, p)
      assert batch == [{:retract, 1, pid}, {:retract, 2, pid}]
      assert Pipeline.num_pending(p) == 1

      ref = make_ref()
      mref = make_ref()
      p = Pipeline.start(ref, mref, %{ops: batch}, p)
      assert Pipeline.in_flight(p)

      assert :stale = Pipeline.finish(make_ref(), p)
      assert {:ok, %{ops: ^batch}, ^mref, p} = Pipeline.finish(ref, p)
      refute Pipeline.in_flight(p)
    end

    test "a dead worker releases the slot too", %{pid: pid} do
      mref = make_ref()
      p = Pipeline.start(make_ref(), mref, %{ops: [{:retract, :n, pid}]}, Pipeline.new())

      assert :stale = Pipeline.worker_down(make_ref(), p)
      assert {:ok, %{ops: [_]}, p} = Pipeline.worker_down(mref, p)
      refute Pipeline.in_flight(p)
    end

    test "the cached read version is dropped at the end of a term" do
      p = Pipeline.committed(42, Pipeline.new())
      assert Pipeline.read_version(p) == 42
      assert Pipeline.read_version(Pipeline.new_term(p)) == :undefined
    end
  end

  describe "awaiting holders" do
    test "distinct followers count once each, up to the target", %{pid: pid, from: from} do
      direct = [{{:local, from}, :n, pid}]
      batch = make_ref()
      timer = make_ref()
      p = Pipeline.await_acks(batch, direct, timer, 2, Pipeline.new())

      assert {:waiting, p} = Pipeline.ack(batch, @follower, p)
      assert {:waiting, p} = Pipeline.ack(batch, @follower, p)
      assert {:done, ^direct, ^timer, p} = Pipeline.ack(batch, {:n3@host, :reg}, p)
      assert :none = Pipeline.ack_timeout(batch, p)
    end

    test "a timeout reports how many confirmed", %{pid: pid, from: from} do
      batch = make_ref()
      p = Pipeline.await_acks(batch, [{{:local, from}, :n, pid}], make_ref(), 2, Pipeline.new())
      {:waiting, p} = Pipeline.ack(batch, @follower, p)

      assert {:expired, [_], 2, 1, _p} = Pipeline.ack_timeout(batch, p)
    end

    test "losing leadership takes every batch, in arrival order", %{pid: pid, from: from} do
      p =
        Enum.reduce(1..3, Pipeline.new(), fn n, acc ->
          Pipeline.await_acks(make_ref(), [{{:local, from}, n, pid}], make_ref(), 1, acc)
        end)

      {batches, p} = Pipeline.take_acks(p)
      assert [1, 2, 3] == for({[{_, n, _}], _, _, _} <- batches, do: n)
      assert {[], _} = Pipeline.take_acks(p)
    end
  end

  describe "surrender(:deposed, ops)" do
    test "a local registration becomes a follower intent, keeping its caller", %{
      pid: pid,
      from: from
    } do
      assert [{:intent, {:register, :n, ^pid, @meta}, ^from}] =
               Pipeline.surrender(:deposed, [{:add, :n, pid, @meta, {:local, from}}])
    end

    test "a forwarded registration is left to its follower — no `no`", %{pid: pid} do
      assert [] =
               Pipeline.surrender(:deposed, [
                 {:add, :n, pid, @meta, {:forward, @follower, make_ref()}}
               ])
    end

    test "a set_meta fails: a replace is not safe to re-send", %{from: from} do
      assert [{:reply, {:meta, ^from}, {:error, :no_leader}}] =
               Pipeline.surrender(:deposed, [{:set_meta, :n, %{}, nil, {:meta, from}}])
    end

    test "an unregister is answered ok and held as a pid-guarded retract", %{
      pid: pid,
      from: from
    } do
      assert [
               {:reply, {:unreg, ^from}, :ok},
               {:intent, {:unregister, :n, ^pid}, :answered}
             ] = Pipeline.surrender(:deposed, [{:remove, :n, pid, {:unreg, from}}])

      assert [{:intent, {:unregister, :m, ^pid}, :answered}] =
               Pipeline.surrender(:deposed, [{:retract, :m, pid}])

      # Nothing was bound when it was asked: nothing to retract.
      assert [{:reply, {:unreg, ^from}, :ok}] =
               Pipeline.surrender(:deposed, [{:remove, :n, :undefined, {:unreg, from}}])
    end

    test "a down is dropped: the next leader monitors the pid itself" do
      assert [] = Pipeline.surrender(:deposed, [{:down, :n, make_ref()}])
    end
  end

  describe "surrender(:failed, ops)" do
    test "everything idempotent is retried, in order; only set_meta fails", %{
      pid: pid,
      from: from
    } do
      add = {:add, :a, pid, @meta, {:local, from}}
      fwd = {:add, :b, pid, @meta, {:forward, @follower, make_ref()}}
      down = {:down, :c, make_ref()}
      retract = {:retract, :d, pid}

      effects =
        Pipeline.surrender(:failed, [
          add,
          {:set_meta, :x, %{}, nil, {:meta, from}},
          {:remove, :e, pid, {:unreg, from}},
          fwd,
          down,
          retract
        ])

      assert effects == [
               {:reply, {:meta, from}, {:error, :no_leader}},
               # The unregister's caller is answered now; its removal is retried
               # without an origin so it is not answered twice.
               {:reply, {:unreg, from}, :ok},
               {:retry, [add, {:remove, :e, pid, :undefined}, fwd, down, retract]}
             ]
    end

    test "nothing to retry, no retry effect", %{from: from} do
      assert [{:reply, _, {:error, :no_leader}}] =
               Pipeline.surrender(:failed, [{:set_meta, :x, %{}, nil, {:meta, from}}])
    end
  end
end
