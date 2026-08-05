# dgen_registry Simulation Harness

Fault-injecting simulation of the replication protocol (§4.5, §5 of
[the design doc](../../../docs/design/dgen_registry_design.md)) against the **real**
`dgen_registry` code, in a single BEAM.

## Why this exists alongside the TLA+ model

[`formal/`](../../../formal/README.md) proves the *protocol* correct by exploring
every interleaving of an abstraction of it. That is a strong result and a narrow
one: it says nothing about whether `dgen_registry_member.erl` actually implements
that protocol. The two failure modes are disjoint —

| | catches a wrong protocol | catches a wrong implementation |
|---|---|---|
| TLA+ / TLC | yes, exhaustively | no |
| this harness | only if sampled | yes |

So the properties in `DGen.Sim.Invariants` are deliberately named after the
invariants in `DgenRegistryReplication.tla`. A violation found here should be
reproducible there, and vice versa; if it is not, one of the two is wrong about
the system.

## How it works

Three things make it possible:

1. **`keyspace`** (`dgen_registry:start_link/3`). A member's id is `{node(),
   Name}`, and `Name` also fixes the durable keyspace — so one VM normally hosts
   at most one member of a registry. The `keyspace` option splits identity from
   keyspace, letting N differently-named members share one registry's elector
   queue, leader key, and version key. They are real, complete supervision trees
   committing against the real backend.

2. **`eta_net`.** Every inter-member protocol message goes through
   `dgen_registry_member:cast_to_member/2` — broadcasts, register and
   set_metadata replies, `replicate_sync`/`replicate_ack`, resync requests,
   snapshots, retracts — and `eta_transform` rewrites the `gen_server:cast/2`
   underneath it to `eta_net:cast/2`, which owns delivery.

3. **`dgen_registry:status/1`.** Each member's own belief about leadership,
   epoch, sync state, and applied version — which is what the invariants compare
   across members.

A 3-member cluster starts in ~100ms, against ~5s for a `:peer` node, which is what
makes running hundreds of faulted operations per test practical.

## The fault model is narrow on purpose

Only faults real Erlang distribution can produce are injected. Erlang guarantees
that messages delivered between one ordered pair of processes arrive in send
order; it does not guarantee delivery. So, per ordered `{from, to}` pair, the
network may **deliver**, **drop**, **delay**, or **cut** (partition until healed).

It never reorders two messages on the same ordered pair, because distribution
never does — this is the same model as the spec's `DropMsg` action. Injecting
reordering there would manufacture counterexamples the real system cannot produce,
and every "bug" found would be a false positive.

**Loss comes with a signal, and the harness must supply it.** Independent
per-message drop is still a mild over-approximation: in reality messages are lost
because a *link* failed, and a failed link delivers `nodedown`/`nodeup` to both
ends. The registry deliberately hangs recovery off those signals —
`handle_info({nodeup, _})` re-announces the member's join and re-drives unregisters
that were stashed or forwarded into the dying link (Non-goal 5). So
`DGen.Sim.Cluster.converge/2` sends `{nodeup, node()}` to every member as part of
healing.

This was learned the hard way: without it the harness reported a follower that had
optimistically deleted a row whose `unregister_req` was dropped, diverging from its
peers at the same applied_version and never recovering. That looked exactly like a
product bug and was not one — it was the harness modelling lost messages without
the disconnect that must accompany them. Worth internalising: the fault model has
to be faithful in what it *implies*, not only in what it injects.

Only inter-member traffic is faulted. The elector's durable membership/election
queue is untouched, so elections still make progress and a run cannot wedge on a
fault the design never claims to survive.

That scoping is stated as a **topology**, not as a predicate. `Cluster.apply_policy/1`
places each member process on a simulated node of its own, and `eta_net` faults a
send only when both ends are on nodes and the nodes differ — so member-to-member
traffic is faultable and nothing else is. Anything a member spawns inherits its
node, so a transaction worker's traffic to a peer is faulted too.

The elector and the connector are deliberately left **unplaced**. Their messages
are not network traffic: they stand in for operations against the durable store,
and dropping them injects a failure the real system cannot have. Faulting them
produces `acked_bindings_present` violations that look exactly like the
replication defect this suite hunts and are artefacts of the harness — the tell
being that widening the fault model, rather than any change to the system,
produced the failure.

## Determinism: what is and is not guaranteed

The **fault schedule** is seeded and reproducible — for a given seed, the same
decision is made about the same message on the same channel. Process scheduling,
commit timing, and timer expiry are *not* controlled, so a failing run is
reproducible at the fault level rather than bit-for-bit. In practice re-running a
seed usually reproduces a failure, and `eta_net:stats/0` says what the network
did; it is not a guarantee.

**Full determinism is available, and lives elsewhere.**
`test/dgen_registry_eta_test.exs` drives the same cluster through `eta_run`, with
the scheduler, the clock and the network all seeded from one value, against the
in-memory `dgen_mem` backend. That is the suite to reach for when a failure has to
replay exactly.

This suite keeps its own value alongside it: it runs the same invariants against
the same code without suspending anything, so it exercises the real BEAM scheduler
rather than a serialization of it. It was also enough to find the three issues
below.

## Running

```sh
mix dst
```

## Findings

All three were found by this harness and are **fixed**. Each is kept here because
the reasoning is the useful part — the mechanism, why it was invisible to the
existing tests, and what now pins it down.

### 1. A partially-delivered group commit diverged replicas permanently — FIXED

A group commit used to broadcast **one message per changed name**, every one stamped
with the same `{PrevVersion, Version}`. `apply_bcast/6` applied the batch's first
message via `PrevV =:= Applied` and bumped `applied_version` to `V`; each later
message then matched a `V =:= Applied` clause and applied at `V`.

So `applied_version` advanced on a batch's **first** message, not its last. A member
receiving a strict subset of a batch reported version `V` while holding only part of
it. Two consequences, the second being the serious one:

1. It broke the "version tie ⇒ identical content" property that §5.7's freshest-wins
   reconstruction rests on. `gather_maps` picks by version alone, so a handoff could
   pick the member that was *missing* rows — dropping committed, acked bindings and
   fanning the deficient snapshot out to everyone.

2. **It was unrecoverable.** Gap detection compares only `{PrevV, V}` and there was no
   per-batch completeness marker, so a member that lost part of a batch it had already
   advanced onto presented no discontinuity for any later broadcast to catch. Every
   subsequent batch applied cleanly over the hole. The soak saw all three members
   agreeing on a leader, all at the same `applied_version`, one holding fewer names,
   staying that way indefinitely under continued traffic.

Reachable in production via a mid-batch disconnect: the link drops after *k* of *N*
messages. `commit_batch_size` defaults to 5000 and the design explicitly expects large
batches ("a departing node's flood of `DOWN`s"), so multi-message batches were the
normal case.

**Fix:** a batch ships as one `{names_batch, Ops, Epoch, PrevV, Version, LeaderId}`
message (`broadcast_batch/5`). Delivered whole or not at all, a lost batch always
leaves a version discontinuity, which the existing resync already repairs. The
`V =:= Applied` clause collapses into the duplicate guard, so `apply_bcast/6` now
matches the spec's `RecvBcastApply` exactly.

**This also closed the model's abstraction gap.** `RegisterForward` commits exactly one
name per version, so in the spec a version was always a single broadcast and that
clause was only ever reachable for a duplicate — the model could not express the bug.
It now describes the shipped protocol rather than an idealisation of it.

Regression: `"dropping a batch leaves a detectable gap that resyncs, not a silent
hole"`.

### 2. Gap detection was traffic-triggered, so a quiescent cluster stayed diverged — FIXED

`request_resync/2` had exactly two callers — `apply_bcast/6` (a later batch reveals the
gap) and `handle_register_reply/4` — and `resync_timeout` only cleared the
once-per-window guard:

```erlang
%% The resync request we sent went unanswered (dropped cast, deposed target) —
%% clear the guard so the next gap-observing broadcast requests again.
handle_info(resync_timeout, State) ->
    {noreply, State#state{resync_timer = undefined}};
```

So a follower that lost the **tail** of the stream — the last batch before writes
stopped — had nothing left to reveal the gap and held a stale replica for as long as
the cluster stayed quiescent. A lost `resync_req` (or the snapshot answering it) had
the same shape. Safety was unaffected (the replica was still a valid prefix), but the
divergence was unbounded in time rather than "a short replication interval" (Non-goal
4), and for a missed *unregister* it sat badly with Non-goal 5's "eventual propagation
is actively driven at every hop".

**Fix:** the leader broadcasts an empty batch stamped at its current applied version
every `?REPLICA_HEARTBEAT_INTERVAL`. It needs no new handling — a caught-up follower
matches `PrevV =:= applied_version` and applies nothing, a behind follower matches
neither that nor `V =< Applied` and takes the existing gap branch. Cost is one small
cast per follower per interval, independent of the name count.

Regression: `"a follower that loses the tail of the stream still converges when
quiescent"`.

### 3. `set_metadata/2` did not honour a configurable timeout — FIXED

`register_name/2,3` bounded its wait with `register_timeout` (§8), but `set_metadata/2`
and `unregister_name/1` called `gen_server:call/2` with its hidden 5s default, so
tuning the knob silently governed only one of the three writes on the pipeline. Under
injected loss this was the dominant cost of a soak run.

**Fix:** all three leader-routed writes resolve `register_timeout`. The default is
unchanged, so only callers who had already set it see a difference. What a timeout
*means* still differs per call, deliberately: registration exits, unregister answers
`ok` (the removal is stashed and re-driven), `set_metadata` answers
`{error, no_leader}`.

## A note on invariants that are not invariants

`same_version_same_replica` was first written as a continuously-checked property and
had to be moved to converged-only — not because of a bug, but because members write
their replica **optimistically, outside the replicated stream**, without advancing
`applied_version`: `route_unregister/3` deletes the row the moment the caller asks, and
a follower's `handle_register_reply/4` inserts it when the leader's `yes` arrives.
Both leave a member differing from its peers at the same version until the batch comes
back around.

That mid-flight difference is ordinary speculation. What must not survive is a
difference still present after the network is healed and the cluster has quiesced,
which is exactly the shape finding 1 had. An invariant that is only *usually* true
manufactures false findings and trains you to ignore it.
