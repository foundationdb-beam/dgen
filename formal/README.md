# dgen_registry Formal Model

## Why this exists

The replication in [`src/dgen_registry_member.erl`](../src/dgen_registry_member.erl)
(design: [`docs/design/dgen_registry_design.md`](../docs/design/dgen_registry_design.md)
§4.5, §5) is a bespoke message-passing protocol whose correctness rests on
interleaving-sensitive arguments: FIFO ordering between broadcasts and replies,
gap detection keeping every replica a prefix of the leader's stream,
version-guarded acks, epoch fencing, and a freshest-wins handoff gather. That
is exactly the class of system **TLA+** was built for (it has the same
heritage in Raft, Paxos, and FoundationDB itself), and its model checker
**TLC** explores every interleaving of a bounded instance exhaustively — the
kind of proof a test suite, which only samples interleavings, cannot give.

This model exists to check the protocol's safety properties against every
reachable interleaving of a small cluster, not just the ones a test happens to
schedule. It found a genuine race during development (a leadership-handoff
gather could race an in-flight broadcast and silently lose an acknowledged
registration) — see `docs/design/dgen_registry_design.md` §5.7 for how the
shipped protocol closes it, and "Mutation and pre-fix configs" below for how
this model keeps the counterexample reproducible.

## What it models

One module, `DgenRegistryReplication.tla`, at the abstraction level of the
`dgen_registry_member` moduledoc:

- **Fenced commit** — the leader is sole writer; a commit succeeds only if
  the durable leader key + epoch still match.
- **Version-stamped broadcasts** — each carries `{Epoch, PrevVersion,
  Version}`; followers apply only *contiguous* broadcasts.
- **Resync** — a gapped follower requests a snapshot from its stream's
  sender.
- **Forwarded registrations, version-guarded acks** — a follower answers its
  caller only once its replica has applied the batch (`deferred_yes` when
  gapped).
- **Direct registrations, replicate-before-ack** — the leader waits for a
  follower to confirm before answering `yes` (with a configurable
  degrade-open timeout).
- **Leadership handoff** — the new leader gathers the freshest live replica
  and fans out a snapshot; fenced against the durable version key and
  version-monotonic on apply (§5.7).
- **Failures** — member crash (single-fault, bounded by `MaxCrashes`) and
  message loss with Erlang's signal-ordering semantics (per-pair FIFO of
  delivered messages, arbitrary loss).

Deliberately out of scope: unregister/retract/`DOWN` cleanup and the §5.6
conflict trail + kill budget; metadata/queries/presence (they ride the same
pipeline and add no new replication mechanism); membership joins and partial
gathers (the model assumes the handoff gather reaches every live member); and
Erlang-distribution-only partitions that remove live nodes from the member
set. See the module header in `DgenRegistryReplication.tla` for the full
scope note.

## Checked properties

| Invariant | Meaning | Design doc |
|---|---|---|
| `TypeOK` | state stays within declared shapes | — |
| `PrefixConsistency` | every live member's replica equals the committed history at its applied version | §4.5, §5.7 |
| `LeaderEpochUnique` | two members may both believe they lead only under different epochs | §4.2, §5.1 |
| `UniqueBinding` | at most one pid is ever acked `yes` per name | Guarantee 1 |
| `DurableAcked` | every acked registration has enough version-visible holders to survive `MaxCrashes` | Guarantee 4, §5.5 |

All five hold exhaustively in the main config (`DgenRegistryReplication.cfg`,
`MaxChanLen = 2`, ~10.7M distinct states).

## How to run locally

```sh
curl -fsSL -o formal/tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
formal/check.sh DgenRegistryReplication                    # must pass — all invariants
formal/check.sh DgenRegistryReplicationHandoffRace fail     # pre-fix protocol — must fail
formal/check.sh DgenRegistryReplicationNoGuard fail         # mutation — must fail
formal/check.sh DgenRegistryReplicationDegradeOpen fail     # mutation — must fail
```

Requires Java 11+. `tla2tools.jar` and `formal/states/` (TLC's scratch
directory) are gitignored. The main config takes a couple of minutes; the
three expected-fail configs finish in seconds. CI (`.github/workflows/formal.yml`)
runs all four on every push/PR.

## Mutation and pre-fix configs

Three configs deliberately weaken the model to confirm TLC actually catches
what a guard is meant to prevent — all expected to fail:

- **`DgenRegistryReplicationNoGuard.cfg`** (`VersionGuardedAck = FALSE`): the
  pre-`deferred_yes` shape — a follower acks its client optimistically before
  its replica has applied the row, so a crash right after can leave zero
  version-visible holders.
- **`DgenRegistryReplicationDegradeOpen.cfg`** (`DegradeOpen = TRUE`): the
  documented `strict_replication = false` caveat — a direct registration's
  `yes` can fire off the leader-only degrade-open timeout before any
  follower confirms, leaving one holder with no crash spent.
- **`DgenRegistryReplicationHandoffRace.cfg`** (`SafeAssume = FALSE`): the
  pre-fix handoff path — every other constant is the shipped default. This
  reproduces the race described in design doc §5.7: without the durable
  -version-key fence on the assuming leader's gather and the
  version-monotonic snapshot apply, TLC finds a counterexample where a single
  node crash silently loses an already-acked registration.

## Spec ↔ code map

| Spec | Code |
|---|---|
| `Elect` | `dgen_registry_elector` committing a membership/leadership change (abstracted to one durable write) |
| `AssumeGather` (incl. the `SafeAssume` version-key fence) | `{elector_assume_and_distribute}` genuine-change clause → `spawn_assume_gather` → `gather_caught_up/6` → `{assume_gathered}` continuation |
| `CanCommit`'s `dbLeader/dbEpoch` conjuncts | the fenced version-key bump in `dgen_registry_names:start_commit/4` (§5.1) |
| `RegisterForward` | follower `route_register` forward → leader `{register_req}` → group commit → broadcast + `{register_reply, Ref, yes, Version}` |
| `RegisterDirect` + `RecvSync`/`RecvAck` | leader-local `route_register` → `pending_acks` / `{replicate_sync}` / `{replicate_ack}` |
| `DegradeTimeout` | `handle_info({replicate_timeout, _})` with `strict_replication = false` |
| `RecvBcastApply/Drop/Gap` | `apply_bcast/6` (the four-way case split) |
| `RecvReply` | `handle_register_reply/4` incl. `deferred_yes`; flushes are `bump_applied/2` → `flush_deferred/1` |
| `RecvSnap` (incl. the `SafeAssume` version-monotonic guard) | `handle_cast({apply_names_snapshot, ...})` |
| `ServeResync` / `resyncReq` | `{resync_req}` handler / `request_resync/2` |
| `Crash` | member/node death; the ETS replica dies with the process |
| `DropMsg` | casts lost on an Erlang-distribution disconnect (signal-ordering semantics) |

Keep this in sync when either side changes.

## Files

```
formal/
  DgenRegistryReplication.tla             the spec (the fix lives behind SafeAssume)
  DgenRegistryReplication.cfg             main model — must pass (all invariants)
  DgenRegistryReplicationHandoffRace.cfg  pre-fix protocol (SafeAssume=FALSE) — must fail
  DgenRegistryReplicationNoGuard.cfg      mutation — must fail
  DgenRegistryReplicationDegradeOpen.cfg  mutation — must fail
  check.sh                                runner, local + CI
  README.md                               this file
.github/workflows/formal.yml              CI workflow
```
