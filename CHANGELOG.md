# Changelog

## v0.1.1 (TBD)

### Enhancements

- `dgen_server:outbox_cast/1,2` — returns a `Cast = fun((Tx, Message) -> ok)`
  closure for enqueuing a cast message atomically within the caller's own FDB
  transaction. Call it before opening the transaction as a preparatory step;
  the closure captures the queue directory and identifier internally. Intended
  for callers already operating directly with a backend transaction who need
  to compose the enqueue with other writes atomically.

## v0.1.0 (2026-02-22)

Initial release.
