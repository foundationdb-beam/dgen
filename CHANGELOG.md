# Changelog

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
