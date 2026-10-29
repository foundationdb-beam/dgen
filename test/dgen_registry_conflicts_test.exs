defmodule DGen.RegistryConflictsTest do
  @moduledoc """
  Conflict repair (§5.6), in isolation — no member process, no backend.

  `dgen_registry_conflicts` is pure: liveness is passed in, and `resolve/5`
  returns the kills and alarms for the member to carry out. These tests pin
  which gathered states are a conflict, what the remedy does with one (and when
  it holds back), when the release trail may expire, and the version-defined
  `degraded/5`.
  """
  use ExUnit.Case, async: true

  alias :dgen_registry_conflicts, as: Conflicts

  defp spawn_live do
    pid = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp eventually(fun, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    cond do
      fun.() -> true
      System.monotonic_time(:millisecond) > deadline -> false
      true -> Process.sleep(10) && do_eventually(fun, deadline)
    end
  end

  # The detector with real local liveness, as the old 3-arity form probed it.
  defp detect(authority, witnesses, trail),
    do: Conflicts.detect(authority, witnesses, trail, &Process.alive?/1)

  defp detect(authority, witnesses, trail, alive),
    do: Conflicts.detect(authority, witnesses, trail, alive)

  defp rec(pid), do: {pid, %{}, nil}

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

  describe "detect/4 — which gathered states are a conflict" do
    # The detector takes RECORDS maps (`%{name => {pid, index, data}}`) and
    # compares entries against the authority in place — the shape change that made
    # the all-agree handoff allocation-free. `rec/1` builds the record wrapper;
    # the semantics pinned below are unchanged from the pid-map era.

    test "two different live pids for one name is a conflict" do
      p1 = spawn_live()
      p2 = spawn_live()

      assert [{:n, ^p1, [^p2]}] =
               detect(
                 %{n: rec(p1)},
                 [%{n: rec(p1)}, %{n: rec(p2)}],
                 %{}
               )
    end

    test "a recently-released name/pid pair is suppressed (lag, not conflict)" do
      p1 = spawn_live()
      p2 = spawn_live()
      # Trail entries are keyed {name, pid} (§5.6).
      released = %{{:n, p2} => System.system_time(:millisecond)}

      assert [] =
               detect(
                 %{n: rec(p1)},
                 [%{n: rec(p1)}, %{n: rec(p2)}],
                 released
               )
    end

    test "a release under one name does not mask a conflict on another name" do
      p1 = spawn_live()
      p2 = spawn_live()
      # p2 was explicitly released from :other — that must not suppress the
      # genuine divergence on :n, which p2 also (still) claims.
      released = %{{:other, p2} => System.system_time(:millisecond)}

      assert [{:n, ^p1, [^p2]}] =
               detect(
                 %{n: rec(p1)},
                 [%{n: rec(p1)}, %{n: rec(p2)}],
                 released
               )
    end

    test "a legacy bare-pid trail entry (rolling upgrade) still suppresses" do
      p1 = spawn_live()
      p2 = spawn_live()
      # A pre-upgrade member's gathered trail keys by pid alone; the detector
      # honours that coarser shape until the mixed entries age out.
      released = %{p2 => System.system_time(:millisecond)}

      assert [] =
               detect(
                 %{n: rec(p1)},
                 [%{n: rec(p1)}, %{n: rec(p2)}],
                 released
               )
    end

    test "a dead divergent pid is not a conflict" do
      p1 = spawn_live()
      p2 = spawn(fn -> :ok end)
      Process.exit(p2, :kill)
      eventually(fn -> not Process.alive?(p2) end)

      assert [] =
               detect(
                 %{n: rec(p1)},
                 [%{n: rec(p1)}, %{n: rec(p2)}],
                 %{}
               )
    end

    test "a dead authority is not a conflict (nothing to defend)" do
      p1 = spawn(fn -> :ok end)
      Process.exit(p1, :kill)
      eventually(fn -> not Process.alive?(p1) end)
      p2 = spawn_live()

      assert [] =
               detect(
                 %{n: rec(p1)},
                 [%{n: rec(p1)}, %{n: rec(p2)}],
                 %{}
               )
    end

    test "agreement across all maps is not a conflict" do
      p1 = spawn_live()

      assert [] =
               detect(
                 %{n: rec(p1)},
                 [%{n: rec(p1)}, %{n: rec(p1)}],
                 %{}
               )
    end

    test "agreement with different metadata is still agreement (pids decide)" do
      p1 = spawn_live()

      assert [] =
               detect(
                 %{n: {p1, %{v: 1}, nil}},
                 [%{n: {p1, %{v: 2}, :other}}],
                 %{}
               )
    end

    test "a name with no authority (absent from the freshest map) is not a conflict" do
      p1 = spawn_live()

      assert [] = detect(%{}, [%{n: rec(p1)}], %{})
    end

    # The leader adjudicates on its own loop, so liveness is probed ahead of time in
    # the gather helpers (conflict_pids/3 says which pids) and handed in as data
    # (detect/4). No probe — no RPC — runs during adjudication.
    test "conflict_pids/3 names exactly the pids adjudication would probe" do
      [p1, p2, p3, p4] = for _ <- 1..4, do: spawn_live()

      fresh = %{n: rec(p1), agreed: rec(p3)}
      witnesses = [%{n: rec(p2), agreed: rec(p3), trailed: rec(p4)}]
      released = %{{:trailed, p4} => System.system_time(:millisecond)}

      assert Enum.sort([p1, p2]) ==
               :dgen_registry_conflicts.conflict_pids(fresh, witnesses, released)
    end

    test "detect/4 decides on the liveness it is given, not by probing" do
      p1 = spawn_live()
      p2 = spawn_live()
      fresh = %{n: rec(p1)}
      witnesses = [%{n: rec(p2)}]

      assert [{:n, ^p1, [^p2]}] =
               detect(fresh, witnesses, %{}, fn _ -> true end)

      # Both are alive, but the evidence says the divergent one is not: no conflict.
      assert [] =
               detect(fresh, witnesses, %{}, &(&1 == p1))
    end
  end

  describe "resolve/5 — the remedy" do
    @config %{terminate_on_conflict: true, conflict_kill_budget: {2, 60_000}}

    test "kills the authority and every divergent pid, drops the name, alarms" do
      [a, d1, d2] = for _ <- 1..3, do: spawn_live()
      records = %{n: rec(a), kept: rec(a)}
      conflict = {:n, a, [d1, d2]}

      assert {%{kept: _} = left, _c, effects} =
               Conflicts.resolve([conflict], records, @config, 1_000, Conflicts.new())

      refute Map.has_key?(left, :n)
      assert effects == [{:kill, a}, {:kill, d1}, {:kill, d2}, {:alarm, conflict, :terminated}]
    end

    test "an exhausted budget alarms and keeps the name served" do
      [a, d] = for _ <- 1..2, do: spawn_live()
      records = %{n: rec(a)}
      conflict = {:n, a, [d]}

      {_, c, _} = Conflicts.resolve([conflict], records, @config, 1_000, Conflicts.new())
      {_, c, _} = Conflicts.resolve([conflict], records, @config, 2_000, c)

      assert {^records, _c, [{:alarm, ^conflict, :budget_exhausted}]} =
               Conflicts.resolve([conflict], records, @config, 3_000, c)

      # Outside the window the budget is available again.
      assert {left, _c, [_ | _]} = Conflicts.resolve([conflict], records, @config, 70_000, c)
      refute Map.has_key?(left, :n)
    end

    test "detect-only mode alarms and kills nothing" do
      [a, d] = for _ <- 1..2, do: spawn_live()
      records = %{n: rec(a)}
      conflict = {:n, a, [d]}
      config = %{@config | terminate_on_conflict: false}

      assert {^records, _c, [{:alarm, ^conflict, :detect_only}]} =
               Conflicts.resolve([conflict], records, config, 1_000, Conflicts.new())
    end

    test "no conflicts pass the reconstruction through untouched" do
      records = %{n: rec(spawn_live())}
      assert {^records, _c, []} = Conflicts.resolve([], records, @config, 0, Conflicts.new())
    end
  end

  describe "the release trail" do
    @ttl_config %{conflict_release_ttl: 1_000, conflict_kill_budget: {3, 60_000}}

    test "merging keeps each entry's latest release time" do
      p = spawn_live()
      c = Conflicts.released([{:n, p}], 10, Conflicts.new())
      c = Conflicts.merge([%{{:n, p} => 5}, %{{:m, p} => 7}], c)

      assert Conflicts.trail(c) == %{{:n, p} => 10, {:m, p} => 7}
    end

    test "entries expire after the TTL only while every member is connected" do
      p = spawn_live()
      c = Conflicts.released([{:n, p}], 0, Conflicts.new())

      assert Conflicts.trail(Conflicts.prune(5_000, false, @ttl_config, c)) != %{}
      assert Conflicts.trail(Conflicts.prune(5_000, true, @ttl_config, c)) == %{}
    end

    test "entries outlive a member departure by a TTL" do
      p = spawn_live()

      c =
        Conflicts.released([{:n, p}], 0, Conflicts.new()) |> then(&Conflicts.departed(4_500, &1))

      assert Conflicts.trail(Conflicts.prune(5_000, true, @ttl_config, c)) != %{}
      assert Conflicts.trail(Conflicts.prune(6_000, true, @ttl_config, c)) == %{}
    end
  end

  describe "degraded/5" do
    test "is exactly a reconstruction below the durable version key" do
      assert Conflicts.degraded(9, 10, [:a], [], %{})
      refute Conflicts.degraded(10, 10, [:a], [], %{})
    end

    test "ignores who answered the gather when the key is known" do
      # :b never answered and was never synced — still not degraded.
      refute Conflicts.degraded(10, 10, [:a, :b], [], %{a: :snapshot})
    end

    test "falls back to reachability when the key could not be read" do
      assert Conflicts.degraded(10, :unknown, [:a, :b], [], %{a: :snapshot})
      # A member marked fresh (never synced) holds nothing to miss.
      refute Conflicts.degraded(10, :unknown, [:a, :b], [:b], %{a: :snapshot})
    end
  end
end
