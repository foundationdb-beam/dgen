# dgen_registry — client-visible registration latency ACROSS a leadership election,
# as a function of registry size.
#
# The design predicts an O(names) component in the handoff: a genuine leadership
# change gathers every peer's full names map (binary-encoded), rebuilds the new
# leader's ETS table, re-encodes once, and snapshots every follower — all before
# stashed/blocked registrations are re-driven. This script measures what a client
# actually experiences around that window.
#
# One BEAM, three members sharing one registry via the `keyspace` option (the same
# trick the simulation harness uses), so the election is real — durable elector
# queue, member_down from real monitors, full assume-and-distribute — while the
# sweep stays scriptable.
#
# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
#
#   mix run bench/registry_election_latency.exs
#
#   DGEN_SIZES=1000,10000,100000 mix run bench/registry_election_latency.exs
#
#   DGEN_SIZES            comma-separated registry sizes         (1000,10000,50000,100000)
#   DGEN_BACKEND          dgen_mem | dgen_erlfdb                 (dgen_mem)
#   DGEN_PROBE_EVERY_MS   probe registration cadence             (5)
#   DGEN_SETTLE_MS        post-recovery observation window       (3000)
#   DGEN_STRICT           "1" = strict_replication               (0 — the shipped default)
#
# On dgen_mem the commit path is in-VM, so absolute numbers are a floor; the
# O(names) handoff work (gather decode, ETS rebuild, snapshot encode/apply) is
# BEAM-side either way, so the SCALING is the transferable result. Run with
# DGEN_BACKEND=dgen_erlfdb against a live FDB for absolute numbers.

defmodule ElectionBench do
  @probe_timeout 60_000

  def main do
    sizes =
      (System.get_env("DGEN_SIZES") || "1000,10000,50000,100000")
      |> String.split(",")
      |> Enum.map(&String.to_integer(String.trim(&1)))

    backend = String.to_atom(System.get_env("DGEN_BACKEND") || "dgen_mem")
    probe_every = String.to_integer(System.get_env("DGEN_PROBE_EVERY_MS") || "5")
    settle_ms = String.to_integer(System.get_env("DGEN_SETTLE_MS") || "3000")
    strict? = System.get_env("DGEN_STRICT") == "1"

    Application.put_env(:dgen, :backend, backend)
    # Probes must BLOCK through the outage rather than time out, or the
    # measurement truncates at the timeout.
    Application.put_env(:dgen, :register_timeout, @probe_timeout)
    {:ok, _} = Application.ensure_all_started(:dgen)

    IO.puts(
      "backend=#{backend} strict_replication=#{strict?} probe_every=#{probe_every}ms " <>
        "members=3\n"
    )

    header = [
      "size",
      "preload_s",
      "steady p50/p99 (ms)",
      "elect (ms)",
      "assumed (ms)",
      "first_yes (ms)",
      "outage max op (ms)",
      "stalled"
    ]

    rows = for size <- sizes, do: run_size(size, backend, probe_every, settle_ms, strict?)

    IO.puts("\n== summary ==")
    print_table([header | rows])
  end

  # ---------------------------------------------------------------------------
  # One size: cluster up, preload, probe, kill the leader, phase-time recovery.
  # ---------------------------------------------------------------------------

  defp run_size(size, backend, probe_every, settle_ms, strict?) do
    ks = :"bench_#{System.unique_integer([:positive])}"
    tenant = backend.sandbox_open(:"#{ks}_db", :"#{ks}_dir")

    registry_opts =
      if strict?,
        do: %{keyspace: ks, strict_replication: true},
        else: %{keyspace: ks}

    members =
      for i <- 1..3 do
        name = :"#{ks}_m#{i}"
        {:ok, sup} = :dgen_registry.start_link(name, tenant, registry_opts)
        # The bench process must survive the leader kill below.
        Process.unlink(sup)
        :ok = :dgen_registry.await_ready(name, 30_000)
        {name, sup}
      end

    names = Enum.map(members, &elem(&1, 0))

    {preload_us, :ok} = :timer.tc(fn -> preload(names, size) end)
    :ok = await_replicated(names, size)

    {_ldr_node, leader} = leader_of(names)
    survivors = names -- [leader]

    # -- probes -------------------------------------------------------------
    # Fired on a fixed cadence, each in its own process with a fresh name and a
    # fresh subject pid, timed from before the call to its answer. Independent
    # probes (not a serial loop) so the outage is SAMPLED, not measured once.
    stats = :ets.new(:probes, [:public, :ordered_set])
    prober = spawn_prober(survivors, stats, probe_every)

    Process.sleep(1_500)
    baseline = latencies(stats, 0, now_ms())

    # -- the election -------------------------------------------------------
    {^leader, leader_sup} = List.keyfind(members, leader, 0)
    t_kill = now_ms()
    Process.exit(leader_sup, :kill)

    t_elect = await(fn -> committed_leader_changed?(survivors, leader) end, 30_000)
    t_assumed = await(fn -> new_leader_ready?(survivors) end, 60_000)
    t_yes = await(fn -> first_yes_after(stats, t_kill) end, 60_000)

    Process.sleep(settle_ms)
    send(prober, :stop)

    outage_ops = latencies(stats, t_kill, t_kill + (t_yes - t_kill) + settle_ms)
    stalled = Enum.count(outage_ops, fn {_t, _lat, res} -> res != :yes end)
    max_op = (outage_ops |> Enum.map(&elem(&1, 1)) |> Enum.max(fn -> 0 end)) / 1000

    row = [
      to_string(size),
      fmt_s(preload_us / 1_000_000),
      "#{fmt(pctl(baseline, 50) / 1000)}/#{fmt(pctl(baseline, 99) / 1000)}",
      fmt(t_elect - t_kill),
      fmt(t_assumed - t_kill),
      fmt(t_yes - t_kill),
      fmt(max_op),
      to_string(stalled)
    ]

    IO.puts(
      "size=#{size}: preload #{fmt_s(preload_us / 1_000_000)}s, " <>
        "steady p50 #{fmt(pctl(baseline, 50) / 1000)}ms — kill→elect #{fmt(t_elect - t_kill)}ms, " <>
        "kill→assumed #{fmt(t_assumed - t_kill)}ms, kill→first-yes #{fmt(t_yes - t_kill)}ms, " <>
        "max in-flight op #{fmt(max_op)}ms"
    )

    for {_name, sup} <- members, Process.alive?(sup) do
      try do
        Supervisor.stop(sup, :shutdown)
      catch
        :exit, _ -> :ok
      end
    end

    :ets.delete(stats)
    row
  end

  # ---------------------------------------------------------------------------
  # Preload: `size` bindings through the normal write path, concurrently, so the
  # group commit coalesces. A small pool of immortal subjects keeps the process
  # count flat regardless of size.
  # ---------------------------------------------------------------------------

  defp preload(names, size) do
    subjects = for _ <- 1..64, do: spawn(fn -> Process.sleep(:infinity) end)
    member_count = length(names)

    1..size
    |> Task.async_stream(
      fn i ->
        member = Enum.at(names, rem(i, member_count))
        subject = Enum.at(subjects, rem(i, 64))
        :yes = :dgen_registry.register_name({member, {:bench, i}}, subject)
      end,
      max_concurrency: 400,
      ordered: false,
      timeout: 120_000
    )
    |> Stream.run()

    :ok
  end

  # Every member holds every preloaded row before the election — otherwise the
  # gather measures partial replicas.
  defp await_replicated(names, size) do
    await(
      fn ->
        counts =
          for m <- names do
            case :ets.whereis(:dgen_registry.names_table(m)) do
              :undefined -> 0
              tab -> :ets.info(tab, :size)
            end
          end

        if Enum.all?(counts, &(&1 >= size)), do: true, else: nil
      end,
      120_000
    )

    :ok
  end

  # ---------------------------------------------------------------------------
  # Probes
  # ---------------------------------------------------------------------------

  defp spawn_prober(targets, stats, every_ms) do
    spawn(fn -> probe_loop(targets, stats, every_ms, 0) end)
  end

  defp probe_loop(targets, stats, every_ms, n) do
    receive do
      :stop -> :ok
    after
      every_ms ->
        target = Enum.at(targets, rem(n, length(targets)))
        start = now_ms()
        start_us = now_us()

        spawn(fn ->
          subject = spawn(fn -> Process.sleep(:infinity) end)

          result =
            try do
              :dgen_registry.register_name({target, {:probe, start, n}}, subject)
            catch
              :exit, _ -> :timeout
            end

          :ets.insert(stats, {{start, n}, now_us() - start_us, result})
        end)

        probe_loop(targets, stats, every_ms, n + 1)
    end
  end

  # {start_ms, latency_ms, result} for probes STARTED in [from, to).
  defp latencies(stats, from, to) do
    for {{start, _n}, lat, res} <- :ets.tab2list(stats),
        start >= from and start < to,
        do: {start, lat, res}
  end

  defp first_yes_after(stats, t_kill) do
    :ets.tab2list(stats)
    |> Enum.filter(fn {{start, _}, _lat, res} -> start > t_kill and res == :yes end)
    |> Enum.map(fn {{start, _}, lat, _} -> start + lat / 1000 end)
    |> Enum.min(fn -> nil end)
  end

  # ---------------------------------------------------------------------------
  # Phase detection
  # ---------------------------------------------------------------------------

  # The elector's committed answer moved off the dead leader.
  defp committed_leader_changed?(survivors, dead) do
    Enum.find_value(survivors, fn m ->
      case safe(fn -> :dgen_registry.get_leader(m) end) do
        {_node, ^dead} -> nil
        {_node, _new} -> true
        _ -> nil
      end
    end)
  end

  # A survivor believes it leads AND is synced — the assume completed.
  defp new_leader_ready?(survivors) do
    Enum.find_value(survivors, fn m ->
      case safe(fn -> :dgen_registry.status(m) end) do
        %{leader: l, member_id: id, synced: true} when l == id -> true
        _ -> nil
      end
    end)
  end

  # Polls `fun` until non-nil; returns the ms timestamp when it first held.
  defp await(fun, timeout) do
    deadline = now_ms() + timeout

    Stream.repeatedly(fn ->
      case fun.() do
        nil ->
          if now_ms() > deadline, do: raise("await timed out")
          Process.sleep(2)
          nil

        _truthy ->
          now_ms()
      end
    end)
    |> Enum.find(& &1)
  end

  defp leader_of(names) do
    await(
      fn ->
        case safe(fn -> :dgen_registry.get_leader(hd(names)) end) do
          {_node, _name} = l -> l
          _ -> nil
        end
      end,
      30_000
    )

    safe(fn -> :dgen_registry.get_leader(hd(names)) end)
  end

  defp safe(fun) do
    try do
      fun.()
    catch
      _, _ -> :error
    end
  end

  # ---------------------------------------------------------------------------
  # Small helpers
  # ---------------------------------------------------------------------------

  defp now_ms, do: System.monotonic_time(:millisecond)
  defp now_us, do: System.monotonic_time(:microsecond)

  defp pctl([], _p), do: 0

  defp pctl(entries, p) do
    lats = entries |> Enum.map(&elem(&1, 1)) |> Enum.sort()
    Enum.at(lats, min(length(lats) - 1, div(length(lats) * p, 100)))
  end

  defp fmt(ms) when is_number(ms), do: :erlang.float_to_binary(ms / 1, decimals: 2)
  defp fmt_s(s), do: :erlang.float_to_binary(s / 1, decimals: 2)

  defp print_table(rows) do
    widths =
      rows
      |> Enum.zip_with(& &1)
      |> Enum.map(fn col -> col |> Enum.map(&String.length/1) |> Enum.max() end)

    for row <- rows do
      row
      |> Enum.zip(widths)
      |> Enum.map_join("  ", fn {cell, w} -> String.pad_leading(cell, w) end)
      |> IO.puts()
    end
  end
end

ElectionBench.main()
