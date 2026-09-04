# Changelog

## v0.4.1 (2026-09-04)

### Bug fixes

- Fixed a race where an unregister could delete a binding it didn't own,
  freeing a name twice under ordinary concurrency.
- Fixed the DST harness's readiness poll hanging on a virtual clock deadline.
- Fixed a lock holder that was busted mid-run releasing its successor's lock.
- Fixed a lock holder that was busted mid-run overwriting its successor's
  writes.

### Enhancements

- **`dgen_registry`** is now stable, and no longer experimental.
- The `dgen_server` lock is now fenced. A busted holder is refused at commit and
  its message is retried, so `lock_timeout` can be tuned for recovery speed.
  `handle_locked/4` must be safe to run twice. See
  `docs/design/dgen_server_design.md` §4.4.
- Added tests for `lock_timeout` and the `consume_batch` exit points.
- `dgen_registry_member` rewritten as a `gen_statem` with explicit
  searching/assuming/leader/follower states. `sys:get_state/1` now returns
  `{State, Data}`.
- Leadership handoff sped up (~2x faster election window at 200k names) by
  cutting map allocations, shrinking the snapshot encoding, and deferring
  monitor setup off the critical path.
- Expanded TLA+ model and DST invariants covering replication and leader
  handoff.
- Added a per-registry `connectivity` option (`provided_externally`) to let a
  registry share another registry's distribution mesh instead of running its
  own. See `docs/dgen_registry_design.md` §4.6 and §8.

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
