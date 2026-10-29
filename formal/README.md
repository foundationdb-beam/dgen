# dgen_registry Formal Model

A TLA+ model of the replication protocol in
[`src/dgen_registry_member.erl`](../src/dgen_registry_member.erl) (design:
[`docs/design/dgen_registry_design.md`](../docs/design/dgen_registry_design.md)
§4.5, §5). TLC checks its safety properties against every interleaving of a
small bounded cluster. Tests can only sample interleavings; the
[simulation harness](../test/support/sim/README.md) covers the other side: it checks
that the real code implements this protocol.

## What it models

One module, `DgenRegistryReplication.tla`:

- **Fenced commit**: the leader is the only writer. A commit succeeds only if
  the durable leader key and epoch still match.
- **Version-stamped broadcasts**: one per commit version, carrying `{Epoch,
  PrevVersion, Version}`. Followers apply only contiguous broadcasts.
- **Resync**: a follower with a gap requests a snapshot from its stream's
  sender.
- **Forwarded registrations, version-guarded acks**: a follower answers its
  caller only after its replica has applied the batch.
- **Direct registrations, replicate-before-ack**: the leader waits for a
  follower to confirm before answering `yes`. The degrade-open timeout is a
  constant.
- **Leadership handoff** (§5.7): the new leader gathers the freshest live
  replica and fans out a snapshot. Two separate constants guard it: the
  durable-version-key fence on the gather (`SafeAssumeGather`) and the
  version-monotonic snapshot apply (`SafeAssumeSnap`).
- **Replication heartbeat**: the leader's periodic empty batch stamped
  `{Applied, Applied}`, which reveals a gap even when there is no traffic. At
  most one is in flight per leader. A deposed leader that hasn't been told yet
  can still send one, and that case is checked.
- **Failures**: member crash (bounded by `MaxCrashes`) and message loss with
  Erlang's signal ordering (per-pair FIFO, arbitrary loss).

**Deliberately out of scope**: unregister, retract and `DOWN` cleanup; the
§5.6 conflict trail and kill budget; metadata, queries and presence; membership
joins and partial gathers, including the continuing-leader join fast path
(`assume_fast_path/6` / `onboard_joiner` / `{peer_joined}`); `register_replicas
> 1` (`RecvAck` resolves on the first follower ack); Erlang-distribution-only
partitions. The simulation harness covers register/unregister concurrency. The
module header in the `.tla` has the full scope note.

## Checked properties

| Invariant | Meaning | Design doc |
|---|---|---|
| `TypeOK` | state stays within declared shapes | — |
| `PrefixConsistency` | every live member's replica equals the committed history at its applied version | §4.5, §5.7 |
| `LeaderEpochUnique` | two members may both believe they lead only under different epochs | §4.2, §5.1 |
| `UniqueBinding` | at most one pid is ever acked `yes` per name | Guarantee 1 |
| `DurableAcked` | every acked registration has enough version-visible holders to survive `MaxCrashes` | Guarantee 4, §5.5 |
| `DegradedFlagSound` | an assume that did not flag itself `degraded` (gathered max version reached the durable version key) adopted exactly the committed map | §5.6, §8 `reject_when_degraded` |

"All invariants hold" is a claim about the fail-closed setting
(`strict_replication = true`). The default is degrade-open. See
DegradeOpenSafety below for what it still guarantees.

## Configs

Each config is `formal/<Name>.cfg`. Passing configs take minutes. Failing ones
take seconds.

| Config | Expect | What it shows |
|---|---|---|
| `DgenRegistryReplication` | pass | all six invariants, full depth, heartbeat off |
| `DgenRegistryReplicationHeartbeat` | pass | the first five with the heartbeat on (`MaxVersion = 2`) |
| `DgenRegistryReplicationDegradeOpenSafety` | pass | the default degrade-open mode keeps everything except `DurableAcked` |
| `DgenRegistryReplicationDegradedAssume` | pass | with the gather deadline modeled, `DegradedFlagSound` holds: the flag never misses an incomplete reconstruction |
| `DgenRegistryReplicationHandoffRace` | fail | the pre-§5.7 handoff (both `SafeAssume*` off): one crash loses an acked registration (`DurableAcked`) |
| `DgenRegistryReplicationHandoffRaceUnique` | fail | the same race checked against `UniqueBinding` alone: the freed name is re-issued to a second pid |
| `DgenRegistryReplicationHandoffGatherOnly` | fail | the gather fence alone off: it is needed on its own |
| `DgenRegistryReplicationSnapMonotonicOnly` | fail | the snapshot guard alone off: it is needed on its own |
| `DgenRegistryReplicationNoGuard` | fail | optimistic follower acks (`VersionGuardedAck = FALSE`) lose `DurableAcked`. The code's versionless `{register_reply, Ref, Result}` rolling-upgrade clause has this shape |
| `DgenRegistryReplicationDegradeOpen` | fail | degrade-open (`strict_replication = false`) loses `DurableAcked` |
| `DgenRegistryReplicationAckReachable` | fail | vacuity canary: `NoAcks` fails, so acks are actually reachable |
| `DgenRegistryReplicationDegradedReachable` | fail | vacuity canary: `NoDegradedAssume` fails, so degraded assumes are actually reachable |

If a vacuity canary ever passes, the model has stopped exercising its
behaviour, and a green run on the other configs means much less.

## Spec ↔ code map

| Spec | Code |
|---|---|
| `Elect` | `dgen_registry_elector` committing a membership/leadership change (abstracted to one durable write) |
| `AssumeGather` (incl. the `SafeAssumeGather` version-key fence) | the `assuming` statem state: `assume_genuine/6` → `spawn_assume_gather` → `gather_caught_up/6` → the `{assume_gathered}` continuation, valid only in `assuming` for its arming ref. The continuing-leader join fast path (`assume_fast_path/6` / `onboard_joiner` / `{peer_joined}`) dispatches in the `leader` state and is out of scope |
| `HeartbeatBcast` / `RecvHeartbeat` | `handle_event(info, replica_heartbeat, leader, …)` → `broadcast_heartbeat/1` (`{names_batch, [], E, V, V, Self}`) / the empty batch through `dgen_registry_replica:offer_batch/5` |
| `CanCommit`'s `dbLeader/dbEpoch` conjuncts | the fenced version-key bump in `dgen_registry_names:start_commit/4` (§5.1) |
| `RegisterForward` | follower `route_register` forward → leader `{register_req}` → group commit → `broadcast_batch/5` + `{register_reply, Ref, yes, Version}` |
| `RegisterDirect` + `RecvSync`/`RecvAck` | leader-local `route_register` → `dgen_registry_pipeline:await_acks/5` / `{replicate_sync}` / `{replicate_ack}` → `dgen_registry_pipeline:ack/3` |
| `DegradeTimeout` | `handle_info({replicate_timeout, _})` with `strict_replication = false` |
| `RecvBcastApply/Drop/Gap` | `dgen_registry_replica:offer_batch/5` → `applied` / `stale_epoch`+`duplicate` / `gap` (one `{names_batch, …}` per commit version). The member's `{names_batch}` handler reacts to the verdict |
| `RecvReply` | `dgen_registry_intents:on_reply/4` incl. the `awaiting_version` phase. Flushes are `dgen_registry_intents:on_version/2`, called from the `{names_batch}` handler's `applied` branch and on an accepted snapshot. On a leadership change the code re-drives a deferred ack as a fresh forward (`redrive/5`) where the spec drops it: a fresh `RegisterForward` in spec terms |
| `RecvSnap` (incl. the `SafeAssumeSnap` version-monotonic guard) | `dgen_registry_replica:offer_snapshot/4` (the `Epoch >= CurEpoch andalso V >= Applied` guard), called from `handle_cast({apply_names_snapshot, ...})` |
| `ServeResync` / `resyncReq` | `{resync_req}` handler / `request_resync/2` |
| `Crash` | member/node death; the ETS replica dies with the process |
| `DropMsg` | casts lost on an Erlang-distribution disconnect (signal-ordering semantics) |

The member's `gen_statem` states (`searching`/`assuming`/`leader`/`follower`)
correspond to what `leaderView[m]`/`epoch[m]`/mid-assume encode in the spec.
Gap/resync, leader reachability and the commit pipeline are per-member
variables on both sides.

Keep this table in sync whenever either side changes. If the two drift, TLC
keeps passing and the tests keep passing, but they no longer describe the same
protocol.

## Running

```sh
formal/check.sh DgenRegistryReplication            # expect pass
formal/check.sh DgenRegistryReplicationNoGuard fail  # expect fail
```

`check.sh <Config> [pass|fail]` runs TLC and checks the result against the
expectation. It needs Java 11+ and nothing else. It uses the TLC jar checked in
at `formal/vendor/tla2tools-v1.8.0.jar`. To use a different TLC, set
`TLA2TOOLS_JAR` or put the jar at `formal/tla2tools.jar` (gitignored). Each run
gets its own temporary metadir, so concurrent runs are safe.

CI (`.github/workflows/formal.yml`) runs every config in the table above with
its expected result on every push and PR.
