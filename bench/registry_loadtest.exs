# dgen_registry large-scale load test / soak driver.
#
# Each invocation is ONE cluster node. Start it several times (in separate
# terminals, or several hosts) pointed at the same FDB cluster file and they
# will discover each other through the shared FoundationDB elector queue and
# form a single registry cluster. Kill an instance (Ctrl-C / SIGTERM) to make
# that node leave; start another to make it join. The registry's mesh handles
# node connect/disconnect automatically — there is no :peer and no manual node
# wiring.
#
# The caller is responsible for starting FoundationDB and providing the path to
# its cluster file.
#
# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
#
#   # from the project root, with FDB already running:
#   elixir -S mix run bench/registry_loadtest.exs /usr/local/etc/foundationdb/fdb.cluster
#
#   # or pass the cluster file via the environment:
#   DGEN_FDB_CLUSTER_FILE=/path/fdb.cluster elixir -S mix run bench/registry_loadtest.exs
#
# Add more nodes in other terminals (same cluster file) — give each a distinct,
# STABLE instance id so it keeps its identity across restarts:
#
#   DGEN_INSTANCE=2 elixir -S mix run bench/registry_loadtest.exs /path/fdb.cluster
#   DGEN_INSTANCE=3 elixir -S mix run bench/registry_loadtest.exs /path/fdb.cluster
#
# ---------------------------------------------------------------------------
# Configuration (environment variables; all optional except the cluster file)
# ---------------------------------------------------------------------------
#
#   DGEN_FDB_CLUSTER_FILE  path to the FDB cluster file (or pass as argv[0])
#   DGEN_COUNT             persistent processes to register on THIS node   (10000)
#   DGEN_REG               registry name (shared by all nodes)             ("dgen_load")
#   DGEN_DIR               FDB directory the registry lives under          ("dgen_loadtest")
#   DGEN_COOKIE            Erlang distribution cookie (shared)             ("dgen_loadtest")
#   DGEN_HOST             host part of this node's name                    ("127.0.0.1")
#   DGEN_RAMP_CONCURRENCY  in-flight registrations during the initial ramp (200)
#   DGEN_REPORT_MS         stats reporting interval, ms                    (5000)
#   DGEN_CHURN_PER_SEC     ambient new registrations/sec after the ramp    (200)
#   DGEN_LIFETIME_MIN_MS   min lifetime of an ambient process, ms          (1000)
#   DGEN_LIFETIME_MAX_MS   max lifetime of an ambient process, ms          (30000)
#   DGEN_DURATION_MS       if set, leave the cluster and exit after this   (run forever)
#   DGEN_PROBE_SAMPLES     raw FDB commit-floor probe at startup (0=skip)  (40)
#   DGEN_INSTANCE          stable node identity; distinct per node (1,2,3)  ("1")
#   DGEN_RESET             "1" wipes this registry's FDB state at startup   ("0")

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

defmodule LoadTest.Env do
  def str(key, default), do: System.get_env(key) || default
  def int(key, default), do: (System.get_env(key) || "") |> parse_int(default)
  def int_or_nil(key), do: (System.get_env(key) || "") |> parse_int(nil)

  defp parse_int("", default), do: default

  defp parse_int(s, default) do
    case Integer.parse(s) do
      {n, _} -> n
      :error -> default
    end
  end
end

# ---------------------------------------------------------------------------
# Registration helpers — every worker names itself {node(), unique_int}, which
# is unique across the whole cluster, and registers ITSELF so that when it dies
# the registry's monitor auto-unregisters it. `timed/4` measures end-to-end
# register latency (local member -> leader -> ack) and streams a sample to the
# collector.
# ---------------------------------------------------------------------------

defmodule LoadTest.Reg do
  def fresh_name, do: {node(), :erlang.unique_integer([:positive])}

  # Register `pid` under `name`, retrying transient `:no` verdicts (no leader
  # yet / name churn). Returns the final verdict.
  def register(_reg, _name, _pid, retries) when retries < 0, do: :no

  def register(reg, name, pid, retries) do
    case :dgen_registry.register_name({reg, name}, pid) do
      :yes ->
        :yes

      :no when retries > 0 ->
        Process.sleep(25)
        register(reg, name, pid, retries - 1)

      other ->
        other
    end
  end

  # Time a (possibly retried) registration and emit one sample.
  def timed(reg, name, pid, collector, retries \\ 3) do
    t0 = System.monotonic_time(:microsecond)
    res = register(reg, name, pid, retries)
    dt = System.monotonic_time(:microsecond) - t0
    send(collector, {:sample, res, dt})
    res
  end
end

# ---------------------------------------------------------------------------
# Commit-floor probe. A registration's latency is fundamentally one FDB commit
# (register routes through the leader, whose only durable write is a fenced
# versionstamp bump). This probes that floor directly — same shape the registry
# uses: a pinned read version (no GRV), a write-conflict key (no read), one
# versionstamp write — so every run self-documents the commit latency of the FDB
# it is pointed at. If registrations run near this number, dgen is not the cost.
# ---------------------------------------------------------------------------

defmodule LoadTest.Probe do
  def run(db, dir, samples) when samples > 0 do
    vkey = :erlfdb_directory.pack(dir, {"__loadtest_probe_version__"})
    ckey = :erlfdb_directory.pack(dir, {"__loadtest_probe_conflict__"})
    :erlfdb.transactional(db, fn tx -> :erlfdb.set(tx, ckey, <<0>>) end)

    rv0 = :erlfdb.wait(:erlfdb.get_read_version(:erlfdb.create_transaction(db)))
    warmup = 5

    {_rv, times} =
      Enum.reduce(1..(samples + warmup), {rv0, []}, fn i, {rv, acc} ->
        t0 = System.monotonic_time(:microsecond)
        tx = :erlfdb.create_transaction(db)
        # Pin the previous commit's version so there is no GRV, and fence with a
        # write-conflict on a key rather than reading it — exactly dgen's path.
        :erlfdb.set_read_version(tx, rv)
        :erlfdb.add_read_conflict_key(tx, ckey)
        :erlfdb.set_versionstamped_value(tx, vkey, <<0::112>>)
        :erlfdb.wait(:erlfdb.commit(tx))
        cv = :erlfdb.get_committed_version(tx)
        dt = System.monotonic_time(:microsecond) - t0
        {cv, if(i > warmup, do: [dt | acc], else: acc)}
      end)

    :erlfdb.transactional(db, fn tx ->
      :erlfdb.clear(tx, vkey)
      :erlfdb.clear(tx, ckey)
    end)

    report(times)
  end

  def run(_db, _dir, _samples), do: :ok

  defp report(times) do
    sorted = Enum.sort(times)
    n = length(sorted)
    ms = fn us -> Float.round(us / 1000, 2) end
    pct = fn q -> Enum.at(sorted, min(n - 1, round(q / 100 * (n - 1)))) end

    IO.puts(
      "fdb commit floor (probe n=#{n}): " <>
        "p50=#{ms.(pct.(50))} p90=#{ms.(pct.(90))} p99=#{ms.(pct.(99))} " <>
        "max=#{ms.(List.last(sorted))} avg=#{ms.(round(Enum.sum(times) / n))} ms " <>
        "<- registration latency cannot beat one commit"
    )
  end
end

# ---------------------------------------------------------------------------
# Stats collector — a plain receive loop. Accumulates per-window latency
# samples and prints a percentile summary every DGEN_REPORT_MS. A separate
# ticker process drives the :report messages so a busy sample stream can't
# starve the timer.
# ---------------------------------------------------------------------------

defmodule LoadTest.Stats do
  def start(reg, report_ms) do
    collector = spawn_link(fn -> loop(init(reg)) end)
    spawn_link(fn -> ticker(collector, report_ms) end)
    collector
  end

  defp init(reg) do
    %{
      reg: reg,
      win_lat: [],
      win_ok: 0,
      win_no: 0,
      tot_ok: 0,
      tot_no: 0,
      win_start: System.monotonic_time(:millisecond)
    }
  end

  defp ticker(collector, report_ms) do
    Process.sleep(report_ms)
    send(collector, :report)
    ticker(collector, report_ms)
  end

  defp loop(s) do
    receive do
      {:sample, :yes, dt} ->
        loop(%{s | win_lat: [dt | s.win_lat], win_ok: s.win_ok + 1, tot_ok: s.tot_ok + 1})

      {:sample, _no, _dt} ->
        loop(%{s | win_no: s.win_no + 1, tot_no: s.tot_no + 1})

      :report ->
        loop(report(s))

      {:final, from} ->
        _ = report(s)
        IO.puts("  totals: ok=#{s.tot_ok} no=#{s.tot_no}")
        send(from, :done)
    end
  end

  defp report(s) do
    now = System.monotonic_time(:millisecond)
    secs = max(now - s.win_start, 1) / 1000
    lat = Enum.sort(s.win_lat)
    n = length(lat)
    rate = Float.round(s.win_ok / secs, 1)

    names = table_size(s.reg)
    nodes = length(Node.list()) + 1

    lat_part =
      if n == 0 do
        "lat n/a"
      else
        "lat ms p50=#{ms(pct(lat, n, 50))} p90=#{ms(pct(lat, n, 90))} " <>
          "p99=#{ms(pct(lat, n, 99))} max=#{ms(List.last(lat))} avg=#{ms(avg(lat, n))}"
      end

    IO.puts(
      "[#{ts()} #{node()}] win #{Float.round(secs, 1)}s | " <>
        "reg ok=#{s.win_ok} no=#{s.win_no} (#{rate}/s) | " <>
        "#{lat_part} | names=#{names} nodes=#{nodes}"
    )

    %{s | win_lat: [], win_ok: 0, win_no: 0, win_start: now}
  end

  # Percentile of an ascending list of length n (nearest-rank).
  defp pct(sorted, n, p) do
    idx = min(n - 1, round(p / 100 * (n - 1)))
    Enum.at(sorted, idx)
  end

  defp avg(lat, n), do: Enum.sum(lat) / n

  defp ms(micros), do: Float.round(micros / 1000, 2)

  defp table_size(reg) do
    case :ets.info(:dgen_registry.names_table(reg), :size) do
      :undefined -> 0
      n -> n
    end
  end

  defp ts do
    {_, {h, m, sec}} = :calendar.local_time()
    :io_lib.format("~2..0b:~2..0b:~2..0b", [h, m, sec]) |> to_string()
  end
end

# ---------------------------------------------------------------------------
# Ramp — registers `count` PERSISTENT processes on this node, holding at most
# `concurrency` registrations in flight at once. Each worker registers itself
# and then idles (staying registered) so it counts toward the steady-state
# population. Returns once all `count` are registered.
# ---------------------------------------------------------------------------

defmodule LoadTest.Ramp do
  # Returns the number of registrations that actually succeeded (`:yes`).
  def run(reg, count, concurrency, collector) when count > 0 do
    controller = self()
    first = min(concurrency, count)
    Enum.each(1..first, fn _ -> worker(reg, collector, controller) end)
    wait(reg, collector, controller, count - first, 0, count, 0)
  end

  def run(_reg, _count, _concurrency, _collector), do: 0

  defp wait(_reg, _c, _ctrl, _remaining, done, total, ok) when done >= total, do: ok

  defp wait(reg, collector, ctrl, remaining, done, total, ok) do
    receive do
      {:ramped, res} ->
        done = done + 1
        ok = ok + if res == :yes, do: 1, else: 0
        step = max(div(total, 10), 1)
        if rem(done, step) == 0, do: IO.puts("  ramp #{done}/#{total} (#{ok} ok)")

        remaining =
          if remaining > 0 do
            worker(reg, collector, ctrl)
            remaining - 1
          else
            remaining
          end

        wait(reg, collector, ctrl, remaining, done, total, ok)
    end
  end

  defp worker(reg, collector, ctrl) do
    spawn(fn ->
      res = LoadTest.Reg.timed(reg, LoadTest.Reg.fresh_name(), self(), collector, 10)
      send(ctrl, {:ramped, res})
      # Only a successful registration stays alive to hold its name; a failed
      # worker exits immediately (nothing to hold).
      if res == :yes do
        receive do
          :stop -> :ok
        end
      end
    end)
  end
end

# ---------------------------------------------------------------------------
# Churn — the ambient workload. Every ~100ms it spawns enough short-lived
# workers to hit the target registrations/sec. Each registers itself, lives a
# random lifetime, then exits naturally — the registry auto-unregisters it.
# ---------------------------------------------------------------------------

defmodule LoadTest.Churn do
  @tick_ms 100

  def start(reg, per_sec, {lmin, lmax}, collector) do
    per_tick = max(round(per_sec * @tick_ms / 1000), 0)
    spawn_link(fn -> loop(reg, per_tick, lmin, lmax, collector) end)
  end

  defp loop(reg, per_tick, lmin, lmax, collector) do
    Enum.each(1..per_tick//1, fn _ -> worker(reg, lmin, lmax, collector) end)
    Process.sleep(@tick_ms)
    loop(reg, per_tick, lmin, lmax, collector)
  end

  defp worker(reg, lmin, lmax, collector) do
    spawn(fn ->
      res = LoadTest.Reg.timed(reg, LoadTest.Reg.fresh_name(), self(), collector, 3)

      if res == :yes do
        span = max(lmax - lmin, 1)
        Process.sleep(lmin + :rand.uniform(span) - 1)
      end

      # Falls off the end -> process exits -> registry cleans up the name.
    end)
  end
end

# ===========================================================================
# Main
# ===========================================================================

alias LoadTest.Env

cluster_file =
  case System.argv() do
    [f | _] -> f
    [] -> System.get_env("DGEN_FDB_CLUSTER_FILE")
  end

if cluster_file in [nil, ""] do
  IO.puts(:stderr, "error: FDB cluster file required (argv[0] or DGEN_FDB_CLUSTER_FILE)")
  System.halt(1)
end

count = Env.int("DGEN_COUNT", 10_000)
reg = String.to_atom(Env.str("DGEN_REG", "dgen_load"))
dir_name = Env.str("DGEN_DIR", "dgen_loadtest")
cookie = String.to_atom(Env.str("DGEN_COOKIE", "dgen_loadtest"))
host = Env.str("DGEN_HOST", "127.0.0.1")
ramp_conc = Env.int("DGEN_RAMP_CONCURRENCY", 200)
report_ms = Env.int("DGEN_REPORT_MS", 5_000)
churn_per_sec = Env.int("DGEN_CHURN_PER_SEC", 200)
life_min = Env.int("DGEN_LIFETIME_MIN_MS", 1_000)
life_max = Env.int("DGEN_LIFETIME_MAX_MS", 30_000)
duration_ms = Env.int_or_nil("DGEN_DURATION_MS")
probe_samples = Env.int("DGEN_PROBE_SAMPLES", 40)
# Stable per-instance identity: the node name must be stable across restarts so a
# restarted node reclaims its own durable leadership. Run multiple nodes by giving
# each a distinct DGEN_INSTANCE (1, 2, 3, …). A random name per run would strand
# the previous run's node as a dead-but-durable leader.
instance = Env.str("DGEN_INSTANCE", "1")
reset? = Env.str("DGEN_RESET", "0") in ["1", "true", "yes"]

# --- Distribution: stable per-instance node name + shared cookie -------------
node_name = :"dgen_load_i#{instance}@#{host}"

unless Node.alive?() do
  {:ok, _} = Node.start(node_name, :longnames)
end

Node.set_cookie(cookie)

# The registry supervisor links to this (the starting) process; trap exits so a
# graceful Supervisor.stop on leave doesn't print a spurious crash.
Process.flag(:trap_exit, true)

# --- Start the dgen backend and open the shared FDB keyspace -----------------
{:ok, _} = Application.ensure_all_started(:erlfdb)
{:ok, _} = Application.ensure_all_started(:dgen)

db = :erlfdb.open(cluster_file)
root = :erlfdb_directory.root(node_prefix: <<0xFE>>, content_prefix: <<>>)

if reset? do
  try do
    :erlfdb_directory.remove(db, root, dir_name)
    IO.puts("reset: wiped FDB directory #{inspect(dir_name)}")
  catch
    _, _ -> :ok
  end
end

dir = :erlfdb_directory.create_or_open(db, root, dir_name)

{:ok, sup} = :dgen_registry.start_link(reg, {db, dir})

IO.puts("""
dgen_registry load test
  node          #{node()}
  cluster file  #{cluster_file}
  registry      #{inspect(reg)}  dir=#{dir_name}
  population     #{count} (ramp concurrency #{ramp_conc})
  churn          #{churn_per_sec}/s, lifetime #{life_min}..#{life_max} ms
  report every   #{report_ms} ms
""")

# --- Commit-floor probe: the latency a single registration cannot beat -------
LoadTest.Probe.run(db, dir, probe_samples)

# --- Wait until this node is ready to serve registrations --------------------
# await_ready/2 blocks through leader election and the join handoff (the leader
# gathers + delivers the names snapshot) and tolerates a transiently-blocked
# member/elector internally — so, unlike polling get_leader ourselves, it does not
# crash when joining a busy cluster. A generous deadline: a large-population /
# high-churn cluster hands off slowly.
IO.write("waiting for registry to be ready... ")

case :dgen_registry.await_ready(reg, 120_000) do
  :ok ->
    IO.puts("ready")

  {:error, :timeout} ->
    IO.puts(:stderr, "timed out waiting for the registry to become ready")
    System.halt(1)
end

# The leader is known now that we are ready. Read it for display + the guard below;
# still guard the call since a re-election could momentarily blank it.
leader =
  try do
    :dgen_registry.get_leader(reg)
  catch
    :exit, _ -> :undefined
  end

IO.puts("leader=#{inspect(leader)}")

# --- Guard: warn if the elected leader is not reachable ----------------------
# With await_ready this is now belt-and-suspenders: a node cannot become "ready"
# without syncing from a reachable leader, and the member-side leader-liveness probe
# reaps a leader recovered-but-dead from durable state. But if a re-election is in
# flight the leader may briefly resolve to a node we cannot see; surface that rather
# than silently churning.
{leader_node, _member} =
  case leader do
    {_n, _m} = l -> l
    _ -> {node(), reg}
  end

unless leader_node == node() or leader_node in Node.list() do
  IO.puts(:stderr, """

  ERROR: elected leader #{inspect(leader)} is on node #{inspect(leader_node)},
  which is NOT reachable from #{inspect(node())}
  (connected nodes: #{inspect(Node.list())}).

  This registry's durable leadership names a node that is not in the current
  cluster — almost always a previous run that was killed abruptly while leader.
  Every registration will fail. Fix it with one of:

    * restart with the SAME DGEN_INSTANCE=#{instance} so this node reclaims its
      identity and leadership, or
    * DGEN_RESET=1 to wipe this registry's FDB state and start clean, or
    * pick a fresh DGEN_DIR / DGEN_REG.
  """)

  Supervisor.stop(sup, :shutdown)
  System.halt(1)
end

# --- Optional graceful leave on SIGTERM --------------------------------------
_ =
  try do
    System.trap_signal(:sigterm, fn ->
      IO.puts("SIGTERM: leaving cluster")
      Supervisor.stop(sup, :shutdown)
      System.halt(0)
    end)
  rescue
    _ -> :ok
  end

collector = LoadTest.Stats.start(reg, report_ms)

# --- Ramp: register the persistent population --------------------------------
IO.puts("ramping #{count} persistent registrations...")
ramp_started = System.monotonic_time(:millisecond)
ramp_ok = LoadTest.Ramp.run(reg, count, ramp_conc, collector)
ramp_secs = Float.round((System.monotonic_time(:millisecond) - ramp_started) / 1000, 1)
IO.puts("ramp complete: #{ramp_ok}/#{count} succeeded in #{ramp_secs}s")

if ramp_ok == 0 and count > 0 do
  IO.puts(:stderr, "WARNING: no registrations succeeded — check the leader diagnostics above")
end

# --- Ambient churn -----------------------------------------------------------
LoadTest.Churn.start(reg, churn_per_sec, {life_min, life_max}, collector)
IO.puts("ambient churn started; reporting every #{report_ms}ms. Ctrl-C to leave.\n")

# --- Run until duration elapses (or forever) ---------------------------------
case duration_ms do
  nil ->
    Process.sleep(:infinity)

  ms ->
    Process.sleep(ms)
    IO.puts("\nduration reached; leaving cluster")
    send(collector, {:final, self()})

    receive do
      :done -> :ok
    after
      2_000 -> :ok
    end

    Supervisor.stop(sup, :shutdown)
    System.halt(0)
end
