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
- **Version-stamped broadcasts** — one per commit version, carrying `{Epoch,
  PrevVersion, Version}`; followers apply only *contiguous* broadcasts.  The code
  matches this: a group commit ships as a single `{names_batch, Ops, …}` message
  (see "A closed abstraction gap" below).
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
  version-monotonic on apply (§5.7). The two guards are separate constants
  (`SafeAssumeGather`, `SafeAssumeSnap`) so each can be mutated alone — and
  each is independently necessary (both half-mutation configs fail).
- **The replication heartbeat** — the leader's periodic empty batch stamped
  `{Applied, Applied}`, the traffic-independent gap reveal (sim README,
  finding 2). Modeled with at most one in flight per leader (the 5s interval
  dwarfs delivery; unconstrained bursts are only a state-space multiplier —
  see `NoHbInFlight`). A deposed-but-uninformed leader still passes the
  `leader = Self` guard, so the stale-heartbeat case is reachable and its
  harmlessness is checked rather than assumed. Verified with every invariant
  in its own config (`DgenRegistryReplicationHeartbeat.cfg`, `MaxVersion = 2`
  — full-depth-with-heartbeats exceeds 60M states, so the main config keeps
  full depth with `Heartbeat = FALSE` and this one keeps the mechanism).
- **Failures** — member crash (single-fault, bounded by `MaxCrashes`) and
  message loss with Erlang's signal-ordering semantics (per-pair FIFO of
  delivered messages, arbitrary loss).

Deliberately out of scope: unregister/retract/`DOWN` cleanup and the §5.6
conflict trail + kill budget; metadata/queries/presence (they ride the same
pipeline and add no new replication mechanism); membership joins and partial
gathers (the model assumes the handoff gather reaches every live member),
including the continuing-leader **join fast path** (`onboard_joiner` /
`{peer_joined}` in `dgen_registry_member.erl`), whose soundness leans on
`PrefixConsistency` proved here for handoffs but *assumed* for joins;
`register_replicas > 1` (the model's `RecvAck` resolves a direct registration
on the first distinct follower ack, i.e. `register_replicas = 1`; the code's
distinct-follower counting beyond that is unmodeled); and
Erlang-distribution-only partitions that remove live nodes from the member
set. See the module header in `DgenRegistryReplication.tla` for the full
scope note.

The scope boundary is not hypothetical: the register/unregister concurrency
this model excludes is exactly where the simulation harness's ack-history
invariant found a real double-free (sim README, finding 7 — one unregister
freeing a name twice, Guarantees 1 and 4 both violated, no fault required).
That is the two methods covering for each other as designed; an unregister
extension to this model (histMap carrying deletions) is the natural v2 if
that machinery keeps changing.

## Checked properties

| Invariant | Meaning | Design doc |
|---|---|---|
| `TypeOK` | state stays within declared shapes | — |
| `PrefixConsistency` | every live member's replica equals the committed history at its applied version | §4.5, §5.7 |
| `LeaderEpochUnique` | two members may both believe they lead only under different epochs | §4.2, §5.1 |
| `UniqueBinding` | at most one pid is ever acked `yes` per name | Guarantee 1 |
| `DurableAcked` | every acked registration has enough version-visible holders to survive `MaxCrashes` | Guarantee 4, §5.5 |

All five hold exhaustively in the main config (`DgenRegistryReplication.cfg`,
`MaxChanLen = 2`, ~10.7M distinct states, ~2.5 min on 10 cores) and in the
heartbeat config (`DgenRegistryReplicationHeartbeat.cfg`, `MaxVersion = 2`
with `Heartbeat = TRUE`, ~17.2M distinct states, ~5 min).

Two configs qualify what a green main run means:

- **`DgenRegistryReplicationAckReachable.cfg`** (expected FAIL) is the
  vacuity canary: every invariant above is trivially satisfied by a protocol
  that never acks anyone, so this config asserts `NoAcks` (`acked = {}`) as an
  invariant under the main constants and TLC's counterexample is machine proof
  that acks are reachable through the unmutated guards. If it ever passes, the
  model has gone quiet and green runs mean much less.
- **`DgenRegistryReplicationDegradeOpenSafety.cfg`** (expected PASS)
  characterizes the DEFAULT deployment. `strict_replication` defaults to
  `false` (`dgen_config.erl`), so production runs degrade-open — the very
  configuration the DegradeOpen mutation proves loses `DurableAcked`. This
  config checks the other four invariants under `DegradeOpen = TRUE`: the
  default mode keeps prefix consistency, uniqueness and epoch fencing, and
  gives up exactly two-holder durability (verified: ~21.9M distinct states,
  no violation). "All five hold" above is a claim about the fail-closed
  setting only — but "does degrade-open also break uniqueness?" now has a
  machine-checked answer: within these bounds, no.

## How to run locally

```sh
formal/check.sh DgenRegistryReplication                          # must pass — all invariants, full depth
formal/check.sh DgenRegistryReplicationHeartbeat                 # must pass — all invariants with the heartbeat on
formal/check.sh DgenRegistryReplicationDegradeOpenSafety         # must pass — the default mode's residual guarantees
formal/check.sh DgenRegistryReplicationHandoffRace fail          # pre-fix protocol — must fail
formal/check.sh DgenRegistryReplicationHandoffRaceUnique fail    # its UniqueBinding half — must fail
formal/check.sh DgenRegistryReplicationHandoffGatherOnly fail    # gather fence alone off — must fail
formal/check.sh DgenRegistryReplicationSnapMonotonicOnly fail    # snapshot guard alone off — must fail
formal/check.sh DgenRegistryReplicationNoGuard fail              # mutation — must fail
formal/check.sh DgenRegistryReplicationDegradeOpen fail          # mutation — must fail
formal/check.sh DgenRegistryReplicationAckReachable fail         # vacuity canary — must fail
```

Requires Java 11+ and nothing else: the TLC jar is **checked in** at
`formal/vendor/tla2tools-v1.8.0.jar` and `check.sh` uses it automatically. (It
used to be downloaded from the tlaplus GitHub release and pinned by sha256 —
which broke twice, because the project republishes assets in place on the same
tag; a vendored jar is immutable in git and hermetic.) To use a different TLC
locally, drop it at `formal/tla2tools.jar` (gitignored, takes precedence) or
set `TLA2TOOLS_JAR` — results here were cross-checked on a 2026 build, which
additionally writes `*_TTrace_*` counterexample files beside the spec; those
and `formal/states/` (TLC scratch) stay gitignored, and `check.sh` gives each
run a private temp metadir so concurrent invocations are safe. The three
passing configs take 2.5-5.5 minutes each on a 10-core machine (timings
below); every expected-fail config finishes in seconds. CI
(`.github/workflows/formal.yml`) runs all ten on every push/PR against the
vendored jar.

## Mutation and pre-fix configs

Six configs deliberately weaken the model to confirm TLC actually catches
what a guard is meant to prevent — all expected to fail (the seventh
expected-fail config, AckReachable, is the vacuity canary described above,
not a mutation):

- **`DgenRegistryReplicationNoGuard.cfg`** (`VersionGuardedAck = FALSE`): the
  pre-`deferred_yes` shape — a follower acks its client optimistically before
  its replica has applied the row, so a crash right after can leave zero
  version-visible holders. This exact optimistic-insert-and-ack shape is
  still live code as the rolling-upgrade legacy-reply clause in
  `handle_register_reply/4`, so during a mixed-version rollout the verified
  guard is off for replies from a pre-version leader — a residual this model
  proves matters.
- **`DgenRegistryReplicationDegradeOpen.cfg`** (`DegradeOpen = TRUE`): the
  documented `strict_replication = false` caveat — a direct registration's
  `yes` can fire off the leader-only degrade-open timeout before any
  follower confirms, leaving one holder with no crash spent. Remember this is
  the *default* configuration; DegradeOpenSafety above records what it keeps.
- **`DgenRegistryReplicationHandoffRace.cfg`** (both `SafeAssume*` FALSE): the
  pre-fix handoff path — every other constant is the shipped default. This
  reproduces the race described in design doc §5.7: without the
  durable-version-key fence on the assuming leader's gather and the
  version-monotonic snapshot apply, TLC finds a counterexample where a single
  node crash silently loses an already-acked registration (`DurableAcked`).
  One caveat the code carries: `gather_caught_up` falls back to
  `DurableVersion = 0` if the frontier read errors, so in a
  backend-failure-during-handoff window the shipped code briefly *is* this
  mutation — the fence degrades to pre-fix behavior, never worse.
- **`DgenRegistryReplicationHandoffRaceUnique.cfg`** (both FALSE,
  `UniqueBinding` alone): TLC stops at the first violation, and in the config
  above BFS reaches `DurableAcked` first — so the race's second consequence,
  the freed name re-issued to a second pid, was long claimed but never
  demonstrated. This config demonstrates it.
- **`DgenRegistryReplicationHandoffGatherOnly.cfg`** /
  **`DgenRegistryReplicationSnapMonotonicOnly.cfg`**: each half of the §5.7
  fix flipped by itself. Both fail, so neither guard shadows the other: a
  stale gather loses acked rows even with monotonic snapshot applies (the
  assuming leader rewinds *itself*), and a late-delivered legitimate snapshot
  rewinds a follower even with every gather fenced.

## A closed abstraction gap: one broadcast per commit version

`RegisterForward`/`RegisterDirect` each commit **exactly one** name at a version, so
in this model a version is always a single broadcast — and `RecvBcastApply`'s
`m.ver = appliedVer[f]` disjunct is reachable only for a duplicate.

The code did not match that. The leader group-commits up to `commit_batch_size`
(default 5000) ops into one version and used to broadcast **one message per changed
name**, all sharing it. There, `m.ver = appliedVer[f]` was the clause applying the
2nd..Nth message of a batch — so `applied_version` advanced on a batch's *first*
message, and a member receiving a strict subset reported the full version while
holding only part of it. `PrefixConsistency`, and the "version tie ⇒ identical
content" property `AssumeGather`'s freshest-wins relies on, were being verified here
under an assumption the implementation did not satisfy.

The simulation harness in [`test/support/sim/`](../test/support/sim/README.md) found
this against the real code (finding 1 there). It was closed **on the code side**: a
batch now ships as a single `{names_batch, Ops, …}` message, so one commit version is
one broadcast, exactly as modelled, and the surviving `V =< Applied` clause is purely
the duplicate guard this spec always assumed it was.

Worth recording as the general lesson: the gap was not in either artefact taken
alone. TLC verified the spec exhaustively and the tests passed; what neither could
see was that they were describing different protocols. That seam is what the
`Spec ↔ code map` below exists to hold shut, and it is worth re-deriving rather than
trusting whenever either side changes.

A coda to that closure: the spec long kept the `m.ver = appliedVer[f]` apply
disjunct that only the pre-fix multi-message batches could reach. It has now been
removed (a re-delivered duplicate falls to the `V <= Applied` drop, exactly as in
the code) — and the main config's distinct-state count is **bit-identical**
before and after (10,656,697), which is TLC's way of confirming the disjunct was
dead in every reachable state rather than someone's way of hoping it was.

## Spec ↔ code map

| Spec | Code |
|---|---|
| `Elect` | `dgen_registry_elector` committing a membership/leadership change (abstracted to one durable write) |
| `AssumeGather` (incl. the `SafeAssumeGather` version-key fence) | the `assuming` statem state: `assume_genuine/6` → `spawn_assume_gather` → `gather_caught_up/6` → the `{assume_gathered}` continuation, valid only in `assuming` for its arming ref. The continuing-leader join fast path (`assume_fast_path/6` / `onboard_joiner` / `{peer_joined}`) dispatches in the `leader` state and is OUT OF SCOPE here (see above) |
| `HeartbeatBcast` / `RecvHeartbeat` | `handle_info(replica_heartbeat, …)` leader clause → `broadcast_heartbeat/1` (`{names_batch, [], E, V, V, Self}`) / the empty batch through `apply_bcast/6`'s three-way split |
| `CanCommit`'s `dbLeader/dbEpoch` conjuncts | the fenced version-key bump in `dgen_registry_names:start_commit/4` (§5.1) |
| `RegisterForward` | follower `route_register` forward → leader `{register_req}` → group commit → `broadcast_batch/5` + `{register_reply, Ref, yes, Version}` |
| `RegisterDirect` + `RecvSync`/`RecvAck` | leader-local `route_register` → `pending_acks` / `{replicate_sync}` / `{replicate_ack}` |
| `DegradeTimeout` | `handle_info({replicate_timeout, _})` with `strict_replication = false` |
| `RecvBcastApply/Drop/Gap` | `apply_bcast/6` (the three-way case split; one `{names_batch, …}` per commit version) |
| `RecvReply` | `handle_register_reply/4` incl. `deferred_yes`; flushes are `bump_applied/2` → `flush_deferred/1` |
| `RecvSnap` (incl. the `SafeAssumeSnap` version-monotonic guard) | `handle_cast({apply_names_snapshot, ...})` — the `Epoch >= CurrentEpoch andalso Version >= CurrentVersion` guard |
| `ServeResync` / `resyncReq` | `{resync_req}` handler / `request_resync/2` |
| `Crash` | member/node death; the ETS replica dies with the process |
| `DropMsg` | casts lost on an Erlang-distribution disconnect (signal-ordering semantics) |

Keep this in sync when either side changes.

Since the member became a `gen_statem`, its states realize this spec's implicit
modes directly — `searching`/`assuming`/`leader`/`follower` are what
`leaderView[m]`/`epoch[m]`/mid-assume encode here — so the map above can name
states rather than ref-guarded continuations. The spec itself needed zero
changes for that port; dimensions the member deliberately keeps as data rather
than state (gap/resync, leader reachability, the commit pipeline) are exactly
the ones this model treats as per-member variables rather than modes.

## Files

```
formal/
  DgenRegistryReplication.tla                       the spec (the §5.7 fix lives behind SafeAssumeGather/SafeAssumeSnap)
  DgenRegistryReplication.cfg                       main model, full depth — must pass (all invariants)
  DgenRegistryReplicationHeartbeat.cfg              the heartbeat, all invariants at MaxVersion 2 — must pass
  DgenRegistryReplicationDegradeOpenSafety.cfg      the DEFAULT mode's residual guarantees — must pass
  DgenRegistryReplicationHandoffRace.cfg            pre-fix protocol (both guards off) — must fail
  DgenRegistryReplicationHandoffRaceUnique.cfg      its UniqueBinding half — must fail
  DgenRegistryReplicationHandoffGatherOnly.cfg      gather fence alone off — must fail
  DgenRegistryReplicationSnapMonotonicOnly.cfg      snapshot guard alone off — must fail
  DgenRegistryReplicationNoGuard.cfg                mutation (pre-deferred_yes) — must fail
  DgenRegistryReplicationDegradeOpen.cfg            mutation (degrade-open loses DurableAcked) — must fail
  DgenRegistryReplicationAckReachable.cfg           vacuity canary (NoAcks) — must fail
  vendor/tla2tools-v1.8.0.jar                       the checked-in TLC (see "How to run locally")
  check.sh                                          runner, local + CI (per-run metadir; concurrency-safe)
  README.md                                         this file
.github/workflows/formal.yml                        CI workflow (all ten configs + the DST jobs)
```
