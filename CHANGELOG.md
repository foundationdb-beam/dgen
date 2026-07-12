# Changelog

## v0.4.0 (2026-07-12)

### Enhancements

- **`dgen_registry` — a defined consistency model (Still considered experimental).** The
  registry now has a precise, documented contract; see `docs/dgen_registry_design.md`
  for the full design, guarantees, and non-goals. In brief:

  - **CP and fenced.** Leadership is an emergent property of the backend: the elected
    leader is fenced on a leader key, so a node that has lost leadership physically
    cannot commit a registration — there is no split-brain dual-accept, and the
    minority side of a partition refuses writes rather than inventing a second owner.
  - **Single-fault singleton uniqueness.** At most one live process holds a name,
    preserved across the failure of any single member. Pids are never persisted; the
    durable footprint is ~2 keys per registry (a leader key and a version
    counter), independent of how many names are registered.
  - **Two-holder addressability.** A registration acknowledged `yes` is held by at
    least two members (a forwarded registration by the leader and the forwarding
    follower; a direct one by replicate-before-ack), so it survives any single node
    loss. Tunable per registry via `register_replicas` / `replicate_timeout` /
    `strict_replication` (default: one replica, degrade-open on timeout with
    telemetry).
  - **Dynamic membership with an automatic mesh.** Nodes join by starting the
    registry and leave by stopping it — no node list to maintain. The registry keeps
    Erlang distribution in step with membership: given nodes that *can* connect
    (shared cookie, reachability), `nodes()` converges to every member without an
    external discovery library.
  - **Lock-free snapshot reads.** Each member keeps its local name replica in a
    `protected` ETS table, so `whereis_name/1` resolves a name with an `ets:lookup/2`
    directly in the *calling* process — no round-trip through the member's mailbox.
    Many processes read concurrently without queuing behind the member, and the
    semantics are unchanged (a still-snapshot, eventually-consistent read). The table
    is recreated empty whenever a member (re)starts, so a read in that brief window
    returns `undefined`.
  - **Per-registration metadata.** A registration can carry metadata — an `index` map
    and an opaque `data` payload — attached at register time (`register_name/3`) or
    replaced later (`set_metadata/2`), and read back lock-free in the caller
    (`get_metadata/1`) or authoritatively through the leader
    (`get_metadata_consistent/1`). Metadata rides the same fenced group-commit pipeline
    as names, replicates to followers, survives a leadership handoff (freshest wins),
    and is removed automatically when the process exits or is unregistered — its
    lifetime is exactly the registration's. See §4.7 of the design doc.
  - **Indexed metadata queries.** Each member maintains an inverted index over the
    registrations' `index` maps, so `query/2` (local snapshot) and `query_consistent/2`
    (leader-authoritative) find every registration matching a conjunction of exact
    equalities (AND-equal) over indexed attributes, returning `#{name, pid, index, data}`
    matches. Queries run on the member's mailbox, so they observe a batch-consistent
    snapshot (never a half-applied commit) at the cost of serialising — a deliberate
    trade, since single-key reads stay lock-free. An empty constraint map is rejected;
    `data` is not queryable.
  - **Durable presence.** A caller registers a *watch*
    query and a *notify* query under an application-chosen `SubId`; whenever a committed
    batch changes which processes satisfy the watch query, the registry pushes
    `{dgen_presence, SubId, Events}` (events `{joined | left, Name, Pid}`) to every process
    matching the notify query — a live feed built on the one-shot `query/2` (§4.7).
    Subscribing delivers an initial snapshot of the current watch set, and the feed is
    correct for every way the set can move: register, unregister, process death, or a
    `set_metadata` that shifts a row in or out of the query. **Subscriptions are durable**
    — stored in the elector's backend-backed `dgen_server` state (keyed by `SubId`, an
    idempotent upsert), so they outlive the whole cluster: an application can tie a
    subscription to a database entity and have presence come back intact after a
    scale-to-zero.
  - **Formally verified replication.** The replication protocol's core safety
    properties — prefix consistency, unique binding, two-holder durability across
    a single node failure, and leader-epoch fencing — are checked exhaustively
    against a TLA+ model of the protocol (`formal/`), not just tested. See
    `formal/README.md` for how to run the checker and what it covers.

- **`dgen_transaction` (new behaviour).** Owns a single backend transaction in its
  own process (create → body → commit → retry, with caller-controlled retry
  semantics), delivering the outcome to an owner. It is the substrate for the
  registry's non-blocking, cached-GRV group commit.

- **`dgen_server` — `consume_k` fully documented, and inlining disabled when
  `consume_k > 1`.** The `consume_k` batch-size option is now documented (batch
  semantics, the throughput/transaction-size trade-off, the single-consumer
  property). When `consume_k > 1`, opportunistic inline handling of `call`s is
  disabled so every call rides the durable queue and the batched consume loop —
  keeping `consume_k` always in effect (and, for the registry, a stable consumer and
  leader). `priority_call`/`priority_cast` still bypass the queue, as before.

### Behavioral notes

- **In rare cases, the registry may forcibly terminate a registered process to enforce uniqueness.**
  Register only processes that can withstand being forcibly killed — supervised and
  restart-safe, or transient by design. This is intentional (a singleton registry)
  and configurable (`terminate_on_conflict`, default `true`). See §5.6 of the design
  doc.

### Breaking changes

- **`dgen_registry:start_link/3` (the `SupName`-embedding variant) is removed.** It
  was unused and is superseded by the "zero atoms created per registry" change above:
  the registry's own supervisor is never registered under a name (embedding it under
  a caller-chosen name doesn't fit a nameless-by-design supervisor). `start_link/2`
  and the options-map `start_link/3` (`Name, Tenant, Opts`) are unaffected. Callers
  should hold onto the returned supervisor pid instead of a name.
- **`dgen_registry:elector_name/1` is removed** in favor of `elector_pid/1`, which
  returns the elector's current pid via supervisor lookup rather than a registered
  name (the elector no longer has one). `member_name/1` and `names_table/1` are kept
  for compatibility but are now identity functions (`member_name(Name) =:= Name`).
- **`dgen_registry:register_name/2,3` exits on a call timeout instead of answering
  `no`.** `no` is now strictly an adjudicated verdict (name taken / no leader /
  leader unreachable); a timeout — where the registration may or may not have
  committed — propagates as a call exit, matching `global`/`syn`/`gproc`
  conventions, so a supervisor restarts the caller rather than acting on a wrong
  "already taken" answer. The bound is configurable via the new `register_timeout`
  application-environment knob (default `5000` ms; §8 of the design doc).

## v0.3.0 (2026-06-17)

### Enhancements

- **Transactional callback variants** — `handle_cast_tx/3`, `handle_call_tx/4`, and
  `handle_info_tx/3` are optional callback variants that receive a `tx_ctx()` map as
  their first argument (`#{td := tenant(), tuid := tuid()}`).  When exported, the `_tx`
  variant is preferred over the plain variant.  The `td` value is the open backend
  transaction+directory pair for the current consume cycle, allowing callbacks to read
  or write arbitrary keys atomically alongside the module-state commit.  The new
  `tx_ctx/0` type is exported from `dgen_server`.

- **`lock_timeout` option for `dgen_server`** — Sets the maximum milliseconds a
  distributed lock may be held before other consumers treat it as stale and
  clear it.  Previously a consumer killed (SIGKILL / VM abort) while holding
  the lock would block all other consumers permanently if no new messages
  arrived to trigger a re-check.  With `lock_timeout` set, a backstop timer is
  scheduled whenever a live lock is observed: after the remaining timeout the
  consumer re-evaluates staleness and clears the lock if the holder has not
  done so.  `infinity` (the default) preserves the previous behaviour.
  `dgen_registry` uses `lock_timeout: 6_000`.

- **`dgen_registry`** — Experimental. An OTP-compatible process registry backed by the
  configured storage backend.  Implements the four-function
  `{via, dgen_registry, {Name, LogicalName}}` contract so standard OTP
  processes (`gen_server`, `gen_statem`, etc.) can be started and addressed by
  name across an Erlang cluster.  Writes and consistent reads go through an
  elected leader; `whereis_name/1` (used by OTP via-tuple routing) is a
  snapshot read from the local member's in-memory map with no backend
  round-trip.  The leader monitors registered pids and propagates
  `{name_unregistered}` to followers on process exit.  Start with
  `dgen_registry:start_link(Name, Tenant)` and supply a supervisor name as the
  first argument to `start_link/3` to embed it in an existing supervision tree.

  Partition recovery is reliable: each join carries a unique token so stale
  `member_down` messages from before a reconnect are discarded rather than
  undoing the rejoin.  Leader transitions during a partition no longer
  trigger automatic distribution reconnect.

### Breaking changes

- **`DGenServer` renamed to `DGen.Server`** — `use DGenServer` becomes `use DGen.Server`;
  all `DGenServer.*` call sites become `DGen.Server.*`. The module now lives at
  `lib/dgen/server.ex`.

- **`handle_locked/3` → `handle_locked/4`** — a `db_ctx()` map is now prepended as
  the first argument, matching the convention of `handle_call_tx/4` and friends.
  `db_ctx()` carries `#{db := tenant(), tuid := tuid()}` where `db` is the DB-level
  tenant (not a transaction); use `dgen_backend:transactional/2` inside the callback
  to open explicit transactions.  Update all `handle_locked` implementations to accept
  the new first argument.

## v0.2.0 (2026-04-05)

### Enhancements

- **Dead-letter queue** — opt-in poison-message handling via the new
  `dead_letter_threshold` start option (default `infinity`, i.e. disabled).
  When set to a positive integer, messages that crash the consumer that many
  times are moved to a dead-letter queue (DLQ) stored in FoundationDB instead
  of being retried indefinitely. For `call` messages the blocked caller raises
  `{dead_letter, N}`. The optional `handle_dead_letter/2` callback is invoked
  when a message is dead-lettered.

- `dgen_server:outbox_cast/1,2` — returns a `Cast = fun((Tx, Message) -> ok)`
  closure for enqueuing a cast message atomically within the caller's own FDB
  transaction. Call it before opening the transaction as a preparatory step;
  the closure captures the queue directory and identifier internally. Intended
  for callers already operating directly with a backend transaction who need
  to compose the enqueue with other writes atomically.

## v0.1.0 (2026-02-22)

Initial release.
