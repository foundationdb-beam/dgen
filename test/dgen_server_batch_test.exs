defmodule DGen.Server.BatchTest do
  @moduledoc """
  `consume_batch/9`'s exit points, and the atomicity they are responsible for.

  A batch peeks up to `consume_k` messages, invokes each callback carrying mod
  state in memory, and then — at whichever of five exit points it reaches — writes
  mod state once and commits that batch's dequeues, both inside the *same*
  transaction. The invariant is not "the callbacks ran"; it is **a message is never
  dequeued without its effect being durable, and never applied twice**.

  The five exits are:

  | exit | reached when |
  |---|---|
  | batch exhausted | every peeked message succeeded |
  | dead letter | a message's attempt count hit the threshold |
  | `{lock, ...}` | a callback asked for the lock mid-batch |
  | `{stop, ...}` | a callback returned `{stop, ...}` mid-batch |
  | `catch` | a callback raised mid-batch |

  Each writes mod state itself, by convention rather than by construction, so each
  needs a test that reaches it with a *non-empty* prefix of already-applied
  messages — a batch of one exercises none of the interesting behaviour. Existing
  `consume_k > 1` coverage (`dgen_server_inline_call_test.exs`) is happy-path only.
  """
  use DGen.Case, async: true
  import ExUnit.CaptureLog, only: [capture_log: 1, with_log: 1]

  alias DGen.DBatcher

  @k 5

  defp start(tenant, tuid, opts \\ []) do
    DBatcher.start_link_opts(tenant, tuid, Keyword.merge([consume_k: @k, consume: true], opts))
  end

  defp kill(pid) do
    mref = Process.monitor(pid)
    DGen.Server.kill(pid, :normal)

    receive do
      {:DOWN, ^mref, :process, ^pid, :normal} -> :ok
    after
      5_000 -> :timeout
    end
  end

  # Enqueue without a consumer attached, so the whole workload is sitting in the
  # queue as one batch before anything drains it. Without this the consumer wins
  # the race and processes messages one at a time, which is a different test.
  defp with_queued(tenant, tuid, enqueue) do
    {:ok, loader} = start(tenant, tuid, consume: false)
    enqueue.(loader)
    loader
  end

  # A non-consuming attachment used to read durable state. The consumer under test
  # cannot answer even a priority_call while it is inside handle_locked — that
  # callback runs on the gen_server loop — so state has to be read from elsewhere.
  defp observer(tenant, tuid) do
    {:ok, pid} = start(tenant, tuid, consume: false)
    pid
  end

  describe "exit: batch exhausted" do
    test "every message in one batch is applied exactly once, in order", context do
      tenant = context[:tenant]
      tuid = {"batch_exhausted"}

      loader =
        with_queued(tenant, tuid, fn pid ->
          for tag <- [:a, :b, :c, :d, :e], do: DBatcher.incr(pid, tag)
        end)

      {:ok, pid} = start(tenant, tuid)

      assert eventually(fn -> DBatcher.get(pid).n == 5 end, 5_000),
             "batch did not drain: #{inspect(DBatcher.get(pid))}"

      assert %{n: 5, seen: [:a, :b, :c, :d, :e]} = DBatcher.get(pid)

      kill(pid)
      GenServer.stop(loader)
    end
  end

  describe "exit: {lock, ...} mid-batch" do
    test "the prefix commits and its actions run before handle_locked", context do
      tenant = context[:tenant]
      tuid = {"batch_lock"}
      me = self()

      loader =
        with_queued(tenant, tuid, fn pid ->
          DBatcher.incr_action(pid, :a, me)
          DBatcher.incr_action(pid, :b, me)
          DBatcher.lock(pid, :L, me)
          DBatcher.incr(pid, :d)
        end)

      obs = observer(tenant, tuid)
      {:ok, pid} = start(tenant, tuid)

      # The batch splits at :L. Everything before it is committed and its actions
      # are drained (`lock_batch` → handle_actions) *before* handle_locked runs,
      # which is the ordering the split exists to guarantee.
      assert_receive {:action, :a, _}, 5_000
      assert_receive {:action, :b, _}, 5_000
      assert_receive {:locked_entered, :L, ^pid}, 5_000

      # The prefix is durable while the locked section is still running.
      assert %{n: 2, seen: [:a, :b]} = DBatcher.get(obs)

      send(pid, :continue)

      assert eventually(fn -> DBatcher.get(obs).n == 4 end, 5_000),
             "post-lock messages never drained: #{inspect(DBatcher.get(obs))}"

      # :L's own effect lands via handle_locked's own transaction, and :d — which
      # was never part of the split batch — is consumed afterwards. Nothing is
      # dropped and nothing repeats.
      assert %{n: 4, seen: [:a, :b, :L, :d]} = DBatcher.get(obs)

      kill(pid)
      GenServer.stop(obs)
      GenServer.stop(loader)
    end

    test "actions from the prefix see the end-of-prefix state, not their own", context do
      tenant = context[:tenant]
      tuid = {"batch_lock_action_state"}
      me = self()

      loader =
        with_queued(tenant, tuid, fn pid ->
          DBatcher.incr_action(pid, :a, me)
          DBatcher.incr_action(pid, :b, me)
          DBatcher.lock(pid, :L, me)
        end)

      {:ok, pid} = start(tenant, tuid)

      # Documented aliasing: both actions are handed the mod state as of the end of
      # the committed prefix (n == 2), not the state at the message that produced
      # them (n == 1 for :a). Pinned because it is surprising and load-bearing.
      assert_receive {:action, :a, %{n: 2}}, 5_000
      assert_receive {:action, :b, %{n: 2}}, 5_000

      assert_receive {:locked_entered, :L, ^pid}, 5_000
      send(pid, :continue)

      kill(pid)
      GenServer.stop(loader)
    end
  end

  describe "exit: {stop, ...} mid-batch" do
    test "the prefix and the stopping message are durable; the rest survives", context do
      tenant = context[:tenant]
      tuid = {"batch_stop"}

      loader =
        with_queued(tenant, tuid, fn pid ->
          DBatcher.incr(pid, :a)
          DBatcher.incr(pid, :b)
          DBatcher.stop(pid, :S)
          DBatcher.incr(pid, :d)
          DBatcher.incr(pid, :e)
        end)

      {:ok, pid} = start(tenant, tuid)
      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

      # The stopping batch committed :a, :b and :S together. :d and :e were peeked
      # in the same batch but not dequeued, so they must still be in the queue —
      # a stop must not swallow the tail of its own batch.
      {:ok, resumed} = start(tenant, tuid)

      assert eventually(fn -> DBatcher.get(resumed).n == 5 end, 5_000),
             "messages after the stop were lost: #{inspect(DBatcher.get(resumed))}"

      assert %{n: 5, seen: [:a, :b, :S, :d, :e]} = DBatcher.get(resumed)

      kill(resumed)
      GenServer.stop(loader)
    end
  end

  describe "exit: catch (a callback raises mid-batch)" do
    test "the prefix commits once, the failing message is retried, the tail survives",
         context do
      tenant = context[:tenant]
      tuid = {"batch_raise"}

      loader =
        with_queued(tenant, tuid, fn pid ->
          DBatcher.incr(pid, :a)
          DBatcher.incr(pid, :b)
          DBatcher.boom(pid, :X)
          DBatcher.incr(pid, :d)
        end)

      # The consumer crashes on :X and is restarted by hand as many times as the
      # threshold allows; :X then dead-letters and the queue drains. The property
      # under test is that :a and :b are applied exactly once across *all* those
      # attempts — the catch clause commits them, so a retry must not re-apply them.
      Process.flag(:trap_exit, true)
      final = drain_with_restarts(tenant, tuid, dead_letter_threshold: 3)

      assert %{n: 3, seen: [:a, :b, :d]} = final,
             "prefix was re-applied or the tail was lost: #{inspect(final)}"

      GenServer.stop(loader)
    end

    test "a raise on the first message of a batch commits nothing", context do
      tenant = context[:tenant]
      tuid = {"batch_raise_first"}

      loader =
        with_queued(tenant, tuid, fn pid ->
          DBatcher.boom(pid, :X)
          DBatcher.incr(pid, :b)
        end)

      # AccKVs is empty, so the catch clause must skip set_mod_state entirely
      # rather than write an unchanged state — and :b must not be dequeued.
      Process.flag(:trap_exit, true)
      final = drain_with_restarts(tenant, tuid, dead_letter_threshold: 2)

      assert %{n: 1, seen: [:b]} = final

      GenServer.stop(loader)
    end
  end

  describe "exit: dead letter mid-batch" do
    test "the prefix commits with the dead-lettered message in the same transaction",
         context do
      tenant = context[:tenant]
      tuid = {"batch_dlq"}

      loader =
        with_queued(tenant, tuid, fn pid ->
          DBatcher.incr(pid, :a)
          DBatcher.boom(pid, :X)
          DBatcher.incr(pid, :c)
        end)

      Process.flag(:trap_exit, true)
      final = drain_with_restarts(tenant, tuid, dead_letter_threshold: 2)

      # :X reached the threshold and left the queue via the DLQ; :a and :c each
      # applied exactly once despite the crash-restart cycles in between.
      assert %{n: 2, seen: [:a, :c]} = final

      GenServer.stop(loader)
    end
  end

  describe "state after a mid-batch crash" do
    test "a restarted consumer reads the committed prefix, not a stale cache", context do
      tenant = context[:tenant]
      tuid = {"batch_crash_cache"}

      loader =
        with_queued(tenant, tuid, fn pid ->
          DBatcher.incr(pid, :a)
          DBatcher.incr(pid, :b)
          DBatcher.boom(pid, :X)
        end)

      Process.flag(:trap_exit, true)

      capture_log(fn ->
        {:ok, pid} = start(tenant, tuid, dead_letter_threshold: 100)
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
      end)

      # A fresh consumer must see the prefix the crashing batch committed. The
      # cache is per-process, so this also pins that init reads through to the
      # backend rather than starting from the module's initial state.
      {:ok, observer} = start(tenant, tuid, consume: false)
      assert %{n: 2, seen: [:a, :b]} = DBatcher.get(observer)

      GenServer.stop(observer)
      GenServer.stop(loader)
    end
  end

  # Restart the consumer each time a raising message takes it down, until the queue
  # stops making the process crash. Returns the final mod state.
  defp drain_with_restarts(tenant, tuid, opts, attempts \\ 12)

  defp drain_with_restarts(_tenant, tuid, _opts, 0) do
    flunk("consumer for #{inspect(tuid)} never stopped crashing")
  end

  defp drain_with_restarts(tenant, tuid, opts, attempts) do
    {result, _log} =
      with_log(fn ->
        {:ok, pid} = start(tenant, tuid, opts)
        ref = Process.monitor(pid)

        receive do
          {:DOWN, ^ref, :process, ^pid, _reason} ->
            :crashed
        after
          1_000 ->
            # Survived a full second without crashing: the queue is drained.
            Process.demonitor(ref, [:flush])
            state = DBatcher.get(pid)
            kill(pid)
            {:done, state}
        end
      end)

    case result do
      :crashed -> drain_with_restarts(tenant, tuid, opts, attempts - 1)
      {:done, state} -> state
    end
  end

  defp eventually(fun, timeout) do
    eventually_until(fun, System.monotonic_time(:millisecond) + timeout)
  end

  defp eventually_until(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(20)
        eventually_until(fun, deadline)
    end
  end
end
