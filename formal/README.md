# Formal model: `dgen_registry_member` replication

A TLA+ model of the bespoke replication protocol in
[`src/dgen_registry_member.erl`](../src/dgen_registry_member.erl), checked
exhaustively with TLC over a small bounded instance. See
[`docs/design/dgen_registry_design.md`](../docs/design/dgen_registry_design.md)
(§4.5, §5) for the prose design this model checks against, and `PLAN.md` in
this directory for the original implementation plan.

## How to run locally

```sh
curl -fsSL -o formal/tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
formal/check.sh DgenRegistryReplication              # must pass
formal/check.sh DgenRegistryReplicationHandoffRace fail   # must fail — see Finding below
formal/check.sh DgenRegistryReplicationNoGuard fail
formal/check.sh DgenRegistryReplicationDegradeOpen fail
```

Requires Java 11+. `tla2tools.jar` and `formal/states/` (TLC's scratch
directory) are gitignored.

## Discovered finding: a leadership-handoff race silently loses acked registrations

Running the originally-planned main config (`MaxChanLen = 2`, current-code
constants) turned up a genuine counterexample against `DurableAcked` and
`UniqueBinding` — not a modeling artifact. It is preserved as
[`DgenRegistryReplicationHandoffRace.cfg`](DgenRegistryReplicationHandoffRace.cfg),
which expects TLC to fail (`formal/check.sh DgenRegistryReplicationHandoffRace fail`).

**Unlike** `DgenRegistryReplicationNoGuard.cfg` / `DgenRegistryReplicationDegradeOpen.cfg`,
this config does **not** mutate the model — every constant is the shipped
default (`VersionGuardedAck = TRUE`, `DegradeOpen = FALSE`). The trace shows
that `VersionGuardedAck` (the `deferred_yes` guard, §5.5) does not fully close
the silent-loss window it is documented to close: a second, independent
pathway can lose an already-acked registration after a single node crash.

### The trace, step by step

1. `m1` is elected leader (epoch 1) with no other members synced yet.
2. `m1`'s `AssumeGather` fans out an (empty) snapshot to `m2`/`m3`; `m2`
   applies it and now believes `m1` leads.
3. `m2` forwards a registration for `n1 -> p1`. `m1` commits it (`dbVersion = 1`)
   and — same channel, FIFO, broadcast before reply, exactly matching
   `RegisterForward`'s `<<b, r>>` — enqueues `[bcast(ver=1), reply(ver=1)]` to
   `m2`. **Neither message has been processed by `m2` yet.**
4. `m1` crashes. Its outgoing channel to `m2` (still holding `[bcast, reply]`)
   survives the crash — Erlang signal semantics: a message already sent is
   still deliverable even though the sender is now dead.
5. `m3` is elected (epoch 2) and runs the handoff gather (`AssumeGather`).
   The gather reads `m2`'s **current** `applied_version`, which is still `0`
   — `m2` has not yet drained the `[bcast, reply]` sitting in its mailbox
   from step 3. So the freshest-wins reconstruction sees nothing for `n1`,
   and `m3` fans out an empty snapshot (`ver = 0`) to `m2`.
6. Only now does `m2` process the leftover `bcast` (applies `n1 -> p1`,
   `applied_version = 1`) and then the `reply` — the version guard is
   satisfied (`applied_version(1) >= ver(1)`, no gap, `deferred_yes` never
   triggers) — so `m2` legitimately acks its client `yes`.
7. `m2` then receives `m3`'s stale snapshot from step 5. The snapshot-apply
   guard is epoch-only (`Epoch >= CurrentEpoch`, no version comparison), so
   it unconditionally overwrites `m2`'s replica with the empty map. The
   binding the client was just told `yes` about is now gone cluster-wide.
   `DurableAcked` fails immediately (zero version-visible holders for an
   acked registration); continuing the search a few steps further, TLC also
   finds `p2` legitimately re-registered for the now-"free" `n1`, violating
   `UniqueBinding`.

### Why this is real, not a modeling gap

The model's `AssumeGather` reads each live member's state atomically, which
looks like it might be over-strong — but cross-checking the actual code shows
the race is real:

- The gather is a plain synchronous RPC per peer:
  [`member_names/1`](../src/dgen_registry_member.erl#L2059) calls
  `gen_server:call({Name, Node}, get_names_snapshot, ?GATHER_TIMEOUT)`
  ([line 2069](../src/dgen_registry_member.erl#L2069)).
- Erlang gives **no ordering guarantee between messages sent by different
  processes** to the same mailbox. `m1`'s `bcast`/`reply` casts and `m3`'s
  `get_names_snapshot` call are independent senders — `m2`'s gen_server
  processes whichever arrives first, which the network is free to reorder.
- Nothing tells `m2` mid-race that its pending forwarded registration is
  stale: `do_leader_changed` / `reject_forwards`
  ([line 1717](../src/dgen_registry_member.erl#L1717)) — the mechanism that
  invalidates `pending_forwards` on a leadership change — only runs when
  the snapshot cast itself is handled, not proactively.
- The snapshot-apply handler,
  `handle_cast({apply_names_snapshot, ...})`
  ([line ~1223](../src/dgen_registry_member.erl#L1223)), guards only on
  `Epoch >= CurrentEpoch` (line 1233) and then unconditionally replaces the
  local replica — there is no check that the incoming snapshot's version is
  at least as fresh as what the follower already applied locally.

So this contradicts the "central guarantee" in
[design doc §5.4](../docs/design/dgen_registry_design.md) — that losing any
single member never loses an acknowledged binding — via a pathway the
`deferred_yes` guard (§5.5) was never designed to cover (it guards against a
follower's own gap in the broadcast stream, not a stale, already-in-flight
message racing an unrelated handoff gather).

### Reachability note

The race needs two messages (`bcast`, `reply`) queued simultaneously on the
same channel, so it requires `MaxChanLen >= 2`. At `MaxChanLen = 1`,
`DgenRegistryReplicationHandoffRace.cfg`'s invariants hold vacuously (verified
separately) — not because the bug is fixed, but because the model can't
represent the in-flight state that triggers it. This is why the **main**
config (`DgenRegistryReplication.cfg`) uses `MaxChanLen = 1` (see below) and
omits `DurableAcked`/`UniqueBinding` — lowering `MaxChanLen` is the
state-explosion remedy this plan's own troubleshooting section prescribes
(§11), applied here for a config that otherwise would not finish in CI time,
not a way of hiding the finding, which has its own dedicated, fast-failing
config.

## Spec ↔ code map

| Spec | Code |
|---|---|
| `Elect` | `dgen_registry_elector` committing a membership/leadership change (abstracted to one durable write) |
| `AssumeGather` | `{elector_assume_and_distribute}` genuine-change clause → `spawn_assume_gather` → `gather_maps/3` → `{assume_gathered}` continuation (modeled atomically; gather assumed to reach every live member) |
| `CanCommit`'s `dbLeader/dbEpoch` conjuncts | the fenced version-key bump in `dgen_registry_names:start_commit/4` (§5.1) |
| `RegisterForward` | follower `route_register` forward → leader `{register_req}` → group commit → broadcast + `{register_reply, Ref, yes, Version}` |
| `RegisterDirect` + `RecvSync`/`RecvAck` | leader-local `route_register` → `pending_acks` / `{replicate_sync}` / `{replicate_ack}` |
| `DegradeTimeout` | `handle_info({replicate_timeout, _})` with `strict_replication = false` |
| `RecvBcastApply/Drop/Gap` | `apply_bcast/6` (the four-way case split) |
| `RecvReply` | `handle_register_reply/4` incl. `deferred_yes`; flushes are `bump_applied/2` → `flush_deferred/1` |
| `RecvSnap` | `handle_cast({apply_names_snapshot, ...})` |
| `ServeResync` / `resyncReq` | `{resync_req}` handler / `request_resync/2` |
| `Crash` | member/node death; the ETS replica dies with the process |
| `DropMsg` | casts lost on an Erlang-distribution disconnect (signal-ordering semantics) |

## Scope

**Modeled** (see the module header in `DgenRegistryReplication.tla` and
`PLAN.md` §2 for the full rationale): fenced single-leader commits;
version-stamped broadcasts with gap detection + resync; version-guarded
forwarded acks (`deferred_yes`); replicate-before-ack for direct
registrations; freshest-wins handoff gather; member crashes (single-fault,
bounded by `MaxCrashes`); message loss with Erlang signal-ordering semantics.

**Out of scope (v1)**: unregister/retract/`DOWN` cleanup and the §5.6
conflict trail + kill budget; metadata/queries/presence; membership joins
and partial gathers (the model assumes the handoff gather reaches every live
member); Erlang-distribution-only partitions that remove live nodes from the
member set. See `PLAN.md` §13 for candidate v2 extensions.

## Checked properties

| Invariant | Meaning | Design doc | Status at `MaxChanLen = 1` (main config) |
|---|---|---|---|
| `TypeOK` | state stays within declared shapes | — | holds |
| `PrefixConsistency` | every live member's replica equals the committed history at its applied version | §4.5, §5.7 | holds |
| `LeaderEpochUnique` | two members may both believe they lead only under different epochs | §4.2, §5.1 | holds |
| `UniqueBinding` | at most one pid is ever acked `yes` per name | Guarantee 1 | **violated** at `MaxChanLen = 2` — see Finding above; not checked in the main config |
| `DurableAcked` | every acked registration has enough version-visible holders to survive `MaxCrashes` | Guarantee 4, §5.5 | **violated** at `MaxChanLen = 2` — see Finding above; not checked in the main config |

The two mutation configs (`DgenRegistryReplicationNoGuard.cfg`,
`DgenRegistryReplicationDegradeOpen.cfg`) deliberately weaken the model to
confirm TLC actually catches what those guards are meant to prevent — both
expected to fail, and both do:

- **`DgenRegistryReplicationNoGuard.cfg`** (`VersionGuardedAck = FALSE`):
  the pre-guard bug — a follower acks its client optimistically before its
  replica has applied the row, so a crash right after can leave zero
  version-visible holders. Confirms `deferred_yes` is load-bearing.
- **`DgenRegistryReplicationDegradeOpen.cfg`** (`DegradeOpen = TRUE`): the
  documented `strict_replication = false` caveat — a direct registration's
  `yes` can fire off the leader-only degrade-open timeout before any
  follower confirms, leaving one holder with no crash spent.

## Files

```
formal/
  DgenRegistryReplication.tla             the spec
  DgenRegistryReplication.cfg             main model — must pass (structural invariants only)
  DgenRegistryReplicationHandoffRace.cfg  genuine finding — must fail (see above)
  DgenRegistryReplicationNoGuard.cfg      mutation — must fail
  DgenRegistryReplicationDegradeOpen.cfg  mutation — must fail
  check.sh                                runner, local + CI
  README.md                               this file
.github/workflows/formal.yml              CI workflow
```
