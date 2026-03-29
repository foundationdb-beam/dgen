# Changelog

## v0.1.1 (TBD)

### Enhancements

- **Dead-letter queue** — `call` and `cast` messages that crash the consumer
  are automatically retried, but only up to `dead_letter_threshold` attempts
  (default `3`). Once the threshold is reached the message is moved to a
  dead-letter queue (DLQ) stored in FoundationDB. For `call` messages the
  blocked caller receives `{error, {dead_letter, N}}`. The optional
  `handle_dead_letter/2` callback is invoked when a message is dead-lettered.
  Set `dead_letter_threshold: infinity` to disable the limit and restore
  the previous unbounded retry behavior.

- `dgen_server:outbox_cast/1,2` — returns a `Cast = fun((Tx, Message) -> ok)`
  closure for enqueuing a cast message atomically within the caller's own FDB
  transaction. Call it before opening the transaction as a preparatory step;
  the closure captures the queue directory and identifier internally. Intended
  for callers already operating directly with a backend transaction who need
  to compose the enqueue with other writes atomically.

## v0.1.0 (2026-02-22)

Initial release.
