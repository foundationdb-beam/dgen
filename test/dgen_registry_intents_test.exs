defmodule DGen.RegistryIntentsTest do
  @moduledoc """
  A member's unfinished client requests, in isolation — no member process.

  `dgen_registry_intents` is pure: every call returns the new intents and the
  effects for the member to carry out. These tests drive it through its three
  events (`on_reply/4`, `on_version/2`, `redrive/5`) and pin the re-drive rule:
  re-drive what is idempotent (register, unregister-as-retract), and answer
  `{:error, :no_leader}` for what is not (set_meta).
  """
  use ExUnit.Case, async: true

  alias :dgen_registry_intents, as: Intents

  @l1 {:n1@host, :reg}
  @l2 {:n2@host, :reg}
  @ttl 5_000

  setup do
    {:ok, pid: spawn(fn -> Process.sleep(:infinity) end), from: {self(), make_ref()}}
  end

  defp register(pid), do: {:register, :name, pid, {%{}, :undefined}}

  # The reference of the single :forward effect in `effects`.
  defp forwarded_ref(effects) do
    [ref] = for {:forward, _leader, ref, _kind} <- effects, do: ref
    ref
  end

  describe "submit/5" do
    test "a register with a reachable leader is forwarded, tracked", %{pid: pid, from: from} do
      {_t, effects} = Intents.submit(register(pid), from, 0, {:remote, @l1}, Intents.new())

      assert [{:forward, @l1, ref, {:register, :name, ^pid, _}}] = effects
      assert is_reference(ref)
    end

    test "a register with no reachable leader is held, unanswered", %{pid: pid, from: from} do
      assert {_t, []} = Intents.submit(register(pid), from, 0, :none, Intents.new())
    end

    test "an unregister with no reachable leader is answered ok now and held", %{
      pid: pid,
      from: from
    } do
      {t, effects} = Intents.submit({:unregister, :name, pid}, from, 0, :none, Intents.new())
      assert effects == [{:reply, from, :ok}]

      # ...and its effect is still owed: the next leader gets a pid-guarded retract.
      assert {_t, [{:retract, @l1, :name, ^pid}]} =
               Intents.redrive({:remote, @l1}, :moved, 0, @ttl, t)
    end

    test "an unregister of a locally unbound name has nothing to retract", %{from: from} do
      assert {_t, [{:reply, ^from, :ok}]} =
               Intents.submit({:unregister, :name, :undefined}, from, 0, :none, Intents.new())
    end

    test "a set_meta with no reachable leader fails fast", %{from: from} do
      assert {_t, [{:reply, ^from, {:error, :no_leader}}]} =
               Intents.submit({:set_meta, :name, %{}, nil}, from, 0, :none, Intents.new())
    end
  end

  describe "on_reply/4 — a register's verdict" do
    setup %{pid: pid, from: from} do
      {t, effects} = Intents.submit(register(pid), from, 0, {:remote, @l1}, Intents.new())
      {:ok, t: t, ref: forwarded_ref(effects)}
    end

    test "yes at or behind our version is answered", %{t: t, ref: ref, from: from} do
      assert {_t, [{:reply, ^from, :yes}]} = Intents.on_reply(ref, {:register, :yes, 10}, 10, t)
    end

    test "yes ahead of our version waits for the replica and asks for a resync", %{
      t: t,
      ref: ref,
      from: from
    } do
      {t, effects} = Intents.on_reply(ref, {:register, :yes, 20}, 10, t)
      assert effects == [{:resync, @l1}]

      assert {_t, []} = Intents.on_version(15, t)
      assert {_t, [{:reply, ^from, :yes}]} = Intents.on_version(20, t)
    end

    test "no is answered no", %{t: t, ref: ref, from: from} do
      assert {_t, [{:reply, ^from, :no}]} = Intents.on_reply(ref, {:register, :no, 10}, 10, t)
    end

    test "a legacy yes binds the row itself", %{t: t, ref: ref, from: from, pid: pid} do
      assert {_t, [{:reply, ^from, :yes}, {:insert_row, :name, ^pid, _}]} =
               Intents.on_reply(ref, {:register, :yes, :legacy}, 0, t)
    end

    test "a reply for an unknown reference is ignored", %{t: t} do
      assert {^t, []} = Intents.on_reply(make_ref(), {:register, :yes, 10}, 10, t)
    end
  end

  describe "redrive/5 — registers are re-sent, never answered no" do
    test "a forward to a deposed leader goes to the new one, under a fresh reference", %{
      pid: pid,
      from: from
    } do
      {t, effects} = Intents.submit(register(pid), from, 0, {:remote, @l1}, Intents.new())
      old_ref = forwarded_ref(effects)

      {t, effects} = Intents.redrive({:remote, @l2}, :moved, 0, @ttl, t)
      assert [{:forward, @l2, new_ref, {:register, :name, ^pid, _}}] = effects
      assert new_ref != old_ref

      # The deposed leader's late answer no longer finds anyone to answer.
      assert {_t, []} = Intents.on_reply(old_ref, {:register, :no, 10}, 10, t)
    end

    test "a forward to the leader that is still current is kept on a new view", %{
      pid: pid,
      from: from
    } do
      {t, effects} = Intents.submit(register(pid), from, 0, {:remote, @l1}, Intents.new())
      ref = forwarded_ref(effects)

      # A snapshot from the same leader: it received the forward (or postponed it
      # through its assume window), so re-sending would only commit it twice.
      assert {t, []} = Intents.redrive({:remote, @l1}, :moved, 0, @ttl, t)
      assert {_t, [{:reply, ^from, :yes}]} = Intents.on_reply(ref, {:register, :yes, 1}, 1, t)
    end

    test "a restored link re-sends a forward to the same leader (the cast may be lost)", %{
      pid: pid,
      from: from
    } do
      {t, _} = Intents.submit(register(pid), from, 0, {:remote, @l1}, Intents.new())

      assert {_t, [{:forward, @l1, _ref, _kind}]} =
               Intents.redrive({:remote, @l1}, :all, 0, @ttl, t)
    end

    test "a gap-deferred yes is re-driven if its leader no longer leads", %{
      pid: pid,
      from: from
    } do
      {t, effects} = Intents.submit(register(pid), from, 0, {:remote, @l1}, Intents.new())
      {t, _} = Intents.on_reply(forwarded_ref(effects), {:register, :yes, 20}, 10, t)

      # Same leader: still waiting on our replica, nothing to send.
      assert {_t, []} = Intents.redrive({:remote, @l1}, :moved, 0, @ttl, t)
      # New leader: re-sent, where it is decided idempotently.
      assert {_t, [{:forward, @l2, _ref, _kind}]} =
               Intents.redrive({:remote, @l2}, :moved, 0, @ttl, t)
    end

    test "when we lead, a register goes to our own commit pipeline", %{pid: pid, from: from} do
      {t, _} = Intents.submit(register(pid), from, 0, :none, Intents.new())

      assert {_t, [{:enqueue, {:register, :name, ^pid, _}, ^from}]} =
               Intents.redrive(:self, :moved, 0, @ttl, t)
    end

    test "a register whose caller has timed out is dropped without a reply", %{
      pid: pid,
      from: from
    } do
      {t, _} = Intents.submit(register(pid), from, 0, :none, Intents.new())

      assert {_t, []} = Intents.redrive({:remote, @l1}, :moved, @ttl, @ttl, t)
    end
  end

  describe "redrive/5 — unregisters and set_meta" do
    test "a forwarded unregister becomes a pid-guarded retract, caller answered ok", %{
      pid: pid,
      from: from
    } do
      {t, effects} =
        Intents.submit({:unregister, :name, pid}, from, 0, {:remote, @l1}, Intents.new())

      assert [{:forward, @l1, _ref, {:unregister, :name, ^pid}}] = effects

      assert {_t, [{:reply, ^from, :ok}, {:retract, @l2, :name, ^pid}]} =
               Intents.redrive({:remote, @l2}, :moved, 0, @ttl, t)
    end

    test "an unregister is never dropped for age", %{pid: pid, from: from} do
      {t, _} = Intents.submit({:unregister, :name, pid}, from, 0, :none, Intents.new())
      t = Intents.prune(10 * @ttl, @ttl, t)

      assert {_t, [{:enqueue, {:unregister, :name, ^pid}, :answered}]} =
               Intents.redrive(:self, :moved, 10 * @ttl, @ttl, t)
    end

    test "a set_meta keeps waiting on the same leader, fails on a new one", %{from: from} do
      {t, _} =
        Intents.submit({:set_meta, :name, %{}, nil}, from, 0, {:remote, @l1}, Intents.new())

      assert {_t, []} = Intents.redrive({:remote, @l1}, :moved, 0, @ttl, t)

      assert {_t, [{:reply, ^from, {:error, :no_leader}}]} =
               Intents.redrive({:remote, @l2}, :moved, 0, @ttl, t)
    end
  end

  describe "prune/3" do
    test "drops timed-out registers and set_metas only", %{pid: pid, from: from} do
      {t, _} = Intents.submit(register(pid), from, 0, :none, Intents.new())

      {t, _} =
        Intents.submit({:set_meta, :n2, %{}, nil}, from, 0, {:remote, @l1}, t)

      {t, _} = Intents.submit({:unregister, :n3, pid}, from, 0, :none, t)

      t = Intents.prune(@ttl, @ttl, t)

      # Only the unregister is left to re-drive.
      assert {_t, [{:retract, @l1, :n3, ^pid}]} =
               Intents.redrive({:remote, @l1}, :moved, @ttl, @ttl, t)
    end
  end
end
