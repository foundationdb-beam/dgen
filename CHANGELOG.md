# Changelog

## v0.4.1 (TBD)

### Bug fixes

- **`dgen_registry` — an unregister could destroy a binding it never targeted.**
  A register parked behind an in-flight commit planned against the leader's
  optimistically-emptied table and was acked `yes` while the previous holder's
  registration still stood; the unregister's `remove` op then cleared the name
  unconditionally, deleting the new holder. One unregister freed the name twice
  (Guarantees 1 and 4 both violated), with no fault required — ordinary
  register/unregister concurrency sufficed. The remove is now pid-guarded against
  the current holder. Found by the simulation harness's new end-of-run
  ack-history invariant (`test/support/sim/README.md`, finding 7).

- **DST harness — `dgen_registry:await_ready/2` wedged the deterministic
  suite.** Its poll loop's transformed `timer:sleep` ran in the eta driver
  process, arming a virtual deadline nothing could reach; the readiness poll
  now runs on the real clock via transform-free `dgen_utils` helpers.

### Enhancements

- **`dgen_registry` — leadership-handoff map allocations cut (~22% faster
  client-visible election window).** The assume path materialized the replica
  as fresh n-entry Erlang maps at least eight times; profiling
  (`bench/registry_election_latency.exs`) attributed ~52% of the O(names)
  handoff cost to that churn, with ETS writes under 5%. Now:
  `detect_conflicts/3` compares records maps in place and allocates only for
  divergent names (the all-agree handoff allocates nothing);
  `resolve_conflicts` operates on the freshest records map directly, so the
  no-conflict reconstruction passes it through by reference (the
  `maps:keys` + `maps:with` identity projection is gone); and
  `assume_leadership` folds the ETS table straight into its monitor-ref maps.
  Measured kill→first-`yes` at 200k names: ~940ms → ~740ms (~3.1µs/name,
  from ~4.2).

- **Formal layers hardened and aligned.** The TLA+ model now covers the
  replication heartbeat (bounded to one in flight per leader) and splits the
  §5.7 fix into its two guards, each proven independently necessary by its own
  expected-fail config; new configs demonstrate the handoff race's
  UniqueBinding half, machine-check ack reachability (the vacuity canary), and
  characterize the *default* degrade-open mode (keeps everything but
  two-holder durability). The DST harness gains end-of-run ack-history
  invariants under `eta_run` (UniqueBinding in the spec's cumulative form,
  acked-presence), a strict-replication leader-crash durability test, a
  join-mid-workload-under-loss scenario (`Cluster.join/2`) pinning the
  continuing-leader fast path, and a second planted mutation
  (`DGEN_MUTATION=quiet_resync`) with its own targeted suite. Guard rails:
  a plain test run now refuses a build cache still holding a planted
  mutation, `formal/check.sh` is concurrency-safe (per-run metadir), and CI
  runs every TLC config, both mutations, and the previously-uncovered
  deterministic-suite ratchets.

- **`dgen_registry` — per-registry `connectivity` option.** `provided_externally`
  disables a registry's proactive distribution mesh so it can free-ride on the links
  maintained by a `self_managed` registry on the same node (e.g. one system registry,
  many tenant registries). Registry-scoped backstops stay active in both modes. The
  provider must span a node-superset of the consumer's nodes; a sustained no-leader
  window logs a warning. Unknown values fail safe to `self_managed` (the default).
  See `docs/dgen_registry_design.md` §4.6 and §8.

## v0.4.0 (2026-07-12)

### Enhancements

- **`dgen_registry` — a defined consistency model (still experimental).** Full
  contract in `docs/dgen_registry_design.md`. Highlights:

  - **CP and fenced** — the leader is fenced on a backend key, so a deposed leader
    cannot commit; no split-brain, minority partitions refuse writes.
  - **Singleton uniqueness** across any single member failure; pids never persisted
    (~2 durable keys per registry).
  - **Two-holder addressability** — an acked registration is held by ≥2 members;
    tunable via `register_replicas` / `replicate_timeout` / `strict_replication`.
  - **Dynamic membership with an automatic mesh** — nodes join/leave by
    starting/stopping the registry; no external discovery.
  - **Lock-free snapshot reads** — `whereis_name/1` is an `ets:lookup/2` in the
    calling process (eventually-consistent snapshot; empty briefly after member
    restart).
  - **Per-registration metadata** — an `index` map + opaque `data`, set via
    `register_name/3` / `set_metadata/2`, read via `get_metadata/1` or
    `get_metadata_consistent/1`; lifetime is exactly the registration's (§4.7).
  - **Indexed queries** — `query/2` / `query_consistent/2` match AND-equal
    constraints over indexed attributes against a batch-consistent snapshot.
  - **Durable presence** — subscriptions (watch + notify queries under a `SubId`)
    push `{dgen_presence, SubId, Events}` on membership changes; stored durably, so
    they survive full cluster restarts.
  - **Formally verified replication** — core safety properties checked against a
    TLA+ model (`formal/`).

- **`dgen_transaction` (new behaviour)** — owns a single backend transaction in its
  own process with caller-controlled retry; substrate for the registry's group commit.

- **`dgen_server`** — `consume_k` documented; when `consume_k > 1`, inline `call`
  handling is disabled so batching stays in effect. `priority_call`/`priority_cast`
  still bypass the queue.

### Behavioral notes

- **The registry may forcibly kill a registered process to enforce uniqueness.**
  Register only restart-safe processes. Configurable via `terminate_on_conflict`
  (default `true`); see §5.6.

### Breaking changes

- **`dgen_registry:start_link/3` (`SupName` variant) removed** — the registry
  supervisor is nameless by design; hold the returned pid instead.
- **`elector_name/1` removed** in favor of `elector_pid/1`; `member_name/1` and
  `names_table/1` are now identity functions.
- **`register_name/2,3` exits on call timeout instead of returning `no`** — `no` is
  now strictly an adjudicated verdict. Timeout configurable via `register_timeout`
  (default 5000 ms).

## v0.3.0 (2026-06-17)

### Enhancements

- **Transactional callbacks** — optional `handle_cast_tx/3`, `handle_call_tx/4`,
  `handle_info_tx/3` receive a `tx_ctx()` (open transaction + directory) so callbacks
  can read/write keys atomically with the state commit.
- **`lock_timeout` option for `dgen_server`** — backstop timer clears a stale
  distributed lock left by a killed consumer. Default `infinity` (old behavior);
  `dgen_registry` uses 6000 ms.
- **`dgen_registry`** (experimental) — OTP-compatible process registry over the
  storage backend, addressed via `{via, dgen_registry, {Name, LogicalName}}`. Writes
  and consistent reads go through an elected leader; `whereis_name/1` is a local
  snapshot read. Partition recovery is token-fenced so stale `member_down` messages
  can't undo a rejoin.

### Breaking changes

- **`DGenServer` → `DGen.Server`** (`lib/dgen/server.ex`).
- **`handle_locked/3` → `handle_locked/4`** — a `db_ctx()` map is prepended as the
  first argument.

## v0.2.0 (2026-04-05)

### Enhancements

- **Dead-letter queue** — opt-in via `dead_letter_threshold`; messages crashing the
  consumer that many times move to a DLQ, callers raise `{dead_letter, N}`, and the
  optional `handle_dead_letter/2` callback fires.
- **`dgen_server:outbox_cast/1,2`** — returns a closure for enqueuing a cast
  atomically inside the caller's own backend transaction.

## v0.1.0 (2026-02-22)

Initial release.
