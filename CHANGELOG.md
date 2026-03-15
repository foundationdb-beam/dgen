# Changelog

## Unreleased

### Added

- `dgen_server:feed_cast/1,2` — returns a `fun((Tx, Message) -> ok)` closure for
  enqueuing a cast message atomically within the caller's own FDB transaction.
  Call it before opening the transaction as a preparatory step; the closure
  captures the queue directory and identifier internally.

## v0.1.0 (2026-02-22)

Initial release.
