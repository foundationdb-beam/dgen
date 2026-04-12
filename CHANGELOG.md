# Changelog

## v0.3.0 (TBD)

### Enhancements

- **Transactional callback variants** — `handle_cast_tx/3`, `handle_call_tx/4`, and
  `handle_info_tx/3` are optional callback variants that receive a `tx_ctx()` map as
  their first argument (`#{td := tenant(), tuid := tuid()}`).  When exported, the `_tx`
  variant is preferred over the plain variant.  The `td` value is the open backend
  transaction+directory pair for the current consume cycle, allowing callbacks to read
  or write arbitrary keys atomically alongside the module-state commit.  The new
  `tx_ctx/0` type is exported from `dgen_server`.

- **`dgen_registry`** — an OTP-compatible process registry backed by the configured
  storage backend.  Implements the four-function `{via, dgen_registry, {Name, LogicalName}}`
  contract so standard OTP processes (`gen_server`, `gen_statem`, etc.) can be started
  and addressed by name across an Erlang cluster.  Writes and consistent reads go through
  an elected leader; `whereis_name/1` (used by OTP via-tuple routing) is a snapshot read
  from the local member's in-memory map with no backend round-trip.  The leader monitors
  registered pids and propagates `{name_unregistered}` to followers on process exit.
  Start with `dgen_registry:start_link(Name, Tenant)` and supply a supervisor name as
  the first argument to `start_link/3` to embed it in an existing supervision tree.

### Breaking changes

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
