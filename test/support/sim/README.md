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
`DGen.Sim.Cluster.converge/2` heals through `eta_net:heal_partition/3` with a
`nodeup` signal rather than merely removing the cuts.

This was learned the hard way: without it the harness reported a follower that had
optimistically deleted a row whose `unregister_req` was dropped, diverging from its
peers at the same applied_version and never recovering. That looked exactly like a
product bug and was not one — it was the harness modelling lost messages without
the disconnect that must accompany them. Worth internalising: the fault model has
to be faithful in what it *implies*, not only in what it injects.

Only inter-member traffic is faulted. The elector's durable membership/election
queue is untouched, so elections still make progress and a run cannot wedge on a
fault the design never claims to survive.

That scoping is stated as a **topology**, not as a predicate.
`Cluster.place_members/1` puts each member's tree on a simulated node of its own,
and `eta_net` faults a send only when both ends are faultable and their nodes
differ — so member-to-member traffic is faultable and nothing else is. Anything a
member spawns inherits its node *and its faultability*, so a transaction worker's
traffic to a peer is faulted too.

The elector and the connector are `attach`ed rather than `place`d: on the node,
never on the wire. Their messages are not network traffic — they stand in for
operations against the durable store — and dropping them injects a failure the
real system cannot have. Faulting them produces `acked_bindings_present`
violations that look exactly like the replication defect this suite hunts and are
artefacts of the harness; the tell is that widening the fault model, rather than
any change to the system, produced the failure. But leaving them *unplaced* is
wrong too, and for a reason that only shows up once node faults exist: an
unplaced process is on no node at all, so it learns nothing when one fails and
survives a node kill that took its own member. `attach/2` is the distinction.

## Node faults

A cut channel is not a lost node. `partition/4`, `heal_partition/4`, `isolate/3`
and `kill_node/3` on `DGen.Sim.Cluster` inject the link-level failure instead of
the message-level one, and each carries the events the failure produces:

| | messages | `{nodedown, Peer}` | `noconnection` DOWNs | processes die |
|---|---|---|---|---|
| `set_policy` loss | dropped at random | — | — | — |
| `cut/2` | dropped on one channel | — | — | — |
| `partition/4` | both directions cut | both sides | across the cut | — |
| `kill_node/3` | in flight cancelled | survivors | across the node | the whole tree |
| `crash/2` | — | — | — | the whole tree |

The signal is **derived per side**: A is told `{nodedown, member_2}` and B is told
`{nodedown, member_1}`, because that is what a partition actually says. One
undifferentiated term would tell both sides the same thing, which is never what
happened. `%{learns: :a}` gives the one-sided form, which is what two
independently timing-out ends produce.

`kill_node/3` and `crash/2` are both "the tree dies", and the difference is the
whole reason node faults exist. Under `crash/2` the peers find out because the
processes are gone, and their monitors report `killed`. Under `kill_node/3` the
peers' monitors are retired *before* anything dies and report `noconnection` —
the asymmetry real distribution has, where a remote watcher and a local one see
different reasons for the same death — and the survivors are then told the node
went. The supervisor is frozen before the kill and killed after it, so
`one_for_all` cannot restart the node that just died — under `eta_run` it is
already frozen, being one of the processes the scheduler owns.

### What is still not simulated: `net_kernel`

`eta_net` delivers the *events* a node failure produces and nothing else. A
simulated node is a name in a table: `nodes()` does not list it, and every member
id's node component is the one real `node()`, so `dgen_utils:node_reachable/1`
answers `true` for all of them.

The consequence is worth stating precisely, because it decides which reactions
these tests cover:

- **Covered.** `dgen_registry_member`'s reactions, which do not read the node
  name: `handle_info({nodeup, _})`'s rejoin and unregister re-drive, and the
  peer-monitor `DOWN` that drives `{member_down}` into the elector.
- **Not covered.** `dgen_registry_connector`'s reachability-keyed backstops — the
  `{nodedown, Node}` reap and the leader-liveness probe. Both filter member ids
  by node, and no member id names a simulated node, so both are no-ops here.
  They belong in `dgen_registry_cluster_test.exs` with real peers.

### Where the `noconnection` DOWNs come from, and the race that decides it

`dgen_registry_member`'s failure detection is an `erlang:monitor` on each peer,
which `eta_transform` points at `eta_net:monitor/2`. That monitor is *simulated*
— the only kind a partition can sever — when both ends are already on different
simulated nodes at the moment it is created. Peers are monitored while the cluster
forms, so placement has to happen before that, which is `Cluster.start/3`'s
`:simulate_peer_monitors`.

Placing each tree as it starts is necessary and **not sufficient**, and the gap is
instructive: a member learns of its peers when the elector distributes the member
set back, which is an ordinary message arriving some unbounded time later. So which
peer monitors ended up simulated came down to real-time luck, and a run stopped
being a function of its seed — about a fifth of seeds. `Cluster.start/3` therefore
*holds* each member (`sys:suspend/1`) from the moment its tree starts until every
tree is placed, so no monitor anywhere can be created before the topology is
complete. Ordering the two, rather than hoping one wins, is what made it
deterministic.

It stays off for the real-clock suite in this directory, deliberately. A simulated
monitor learns of an ordinary exit from `eta_sched`'s exit trace: under `eta_run` it
fires on a crash exactly as a real one would, and with no scheduler it never fires
at all. Turning it on here would silently disable the `crash/2` detection the tests
above rely on.

`eta_net:stats/0` counts `signalled` and `noconnection` for exactly this reason.
They are the non-vacuity guard for a node fault the way `dropped` is for a lossy
policy: a run that partitioned a node nothing was on, or severed no monitor,
exercised no recovery and reports the same `ok`. Both sweeps in
`dgen_registry_eta_test.exs` assert on them.

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
replay exactly — under every fault model it offers, message and node alike. It
asserts it, too: `"every seed reproduces its own schedule"` runs each seed three
times and requires one trace, and `eta_run:audit/1` is fatal on every check.

### What node faults cost to get there

A cut is absolute where `drop_p` is scoped, so a partition drops every cross-node
message and drives recovery paths — `peer_joined`, `replicate_sync`,
`apply_names_snapshot`, the §5.7 handoff gather — that a scoped policy never
reaches. Switching node faults on therefore found seven real-time dependencies
that message loss alone never touched. About a fifth of seeds produced more than
one schedule; all five are fixed and it is now nil.

None showed up in `audit/1`, and the reason is worth internalising: the audit
catches a process **running** outside the schedule, and every one of these was a
process **blocking** on something outside it — `code_server`, an unscheduled
supervisor, OTP's own `gen:do_call/4`. What found them was diffing two runs of one
seed at the step where their runnable sets first disagreed, and reading the stack
of the process that differed. `RegistryHarness`'s moduledoc lists all seven;
findings 3, 4 and 5 below are the ones that are also bugs off the simulator.

And measure under load. Six of the seven were visible on an idle machine; the last
— an OTP 28 supervisor's `hibernate_after` timer, which is real — only appeared
with several suites running at once, because what it turned on was how much real
time a run took. `mix dst` looked clean serially and failed one run in three when
six ran together.

Two things are *not* leaks and are allowed. A node fault leaves stray timers — a
killed node's periodic timers outlive it, and a call whose callee is behind a cut
waits out an hour of virtual `register_timeout` the run never reaches. Both are
the fault behaving correctly, and `stray_timers` is allowed on that sweep alone.

This suite keeps its own value alongside it: it runs the same invariants against
the same code without suspending anything, so it exercises the real BEAM scheduler
rather than a serialization of it. It was also enough to find the three issues
below.

## Running

```sh
mix dst
```

`mix dst` runs only the `:simulation`-tagged sweeps (the multi-seed soaks in
both suites). The rest of the two suites — the targeted regressions here and
the untagged determinism/plumbing tests in `dgen_registry_eta_test.exs` — ride
an ordinary `mix test` (the deterministic ones need `DGEN_BACKEND=dgen_mem`;
see `test_helper.exs` for what each backend excludes). The `:mutation` suite is
opt-in and needs the defect compiled in — see `test_helper.exs`.

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

### 3. A `try ... of` left the mesh nudge's send unprotected — FIXED

Found by node-fault injection, and only by it. `spawn_mesh_fetch/3`'s helper calls
`nudge_durable_epoch/2`, which read the committed epoch and sent it to the member:

```erlang
try dgen_server:priority_call(Elector, get_epoch) of
    DurableEpoch when is_integer(DurableEpoch) ->
        MemberName ! {durable_epoch, DurableEpoch};   %% not protected
    _ -> ok
catch
    _:_ -> ok
end,
```

An `of` body is **outside** the protected expression, so the `catch _:_ -> ok`
that every other failure on this best-effort path routes through did not cover the
send. The member is addressed by *registered name*, and sending to a name with no
process raises `badarg` — so a nudge arriving in the window where the member is
gone crashed the helper it ran in.

Reachable in production wherever that window opens: the tree restarting under
`one_for_all`, or the node lost. It was invisible to every existing test because
nothing closed a member while a mesh fetch was in flight; a `kill_node/3` does it
on roughly one sweep in ten.

**Fix:** `try ... catch` around a `case`, so the send is inside the protected
expression. Behaviour is otherwise unchanged — the epoch is re-read on the next
`?MESH_INTERVAL` either way.

### 4. Two client-facing modules were not built with the transform — FIXED

`dgen` and `dgen_registry` had no `-include("dgen_eta.hrl")`. They read as API and
supervisor code, so it looked like they had nothing to instrument — but *where they
run* is the caller's process, which under simulation is a scheduled one. Every
`gen_server:call/2,3` in them therefore went into OTP's `gen:do_call/4`, which no
transform reaches, taking a wall-clock `receive ... after` and a real
`erlang:monitor` with it. Both put a scheduled process's progress outside the
schedule.

Not a production bug — it costs nothing at run time — but a real hole in the
simulation, and the rule it teaches generalises: **a module needs the transform if
it runs inside a scheduled process, not if it owns one.**

**Fix:** the include, in both. Neither uses anything on `eta_transform`'s
`?NET_UNSUPPORTED` list, so the rewrite is total.

### 5. `telemetry_available/0` called `code_server` from inside a member — FIXED

Finding 4 of the previous round cached the telemetry lookup in `persistent_term`
to stop `code:ensure_loaded/1` making a `code_server` round trip per event. The
cache is per VM, so exactly one call still had to happen — and it happened lazily,
inside whichever member emitted the first event. That is a scheduled process making
a synchronous call into one the scheduler does not own.

It is invisible to `eta_run:audit/1`, which is the part worth remembering:
`code:ensure_loaded/1` on a *missing* module loads nothing, so the "no module
loaded mid-run" check stays clean while the leak is wide open. It was found by
reading the stack of a member that was parked in `code_server:call/1` at the step
where two runs of one seed first disagreed.

**Fix:** resolve it in `dgen_registry_member:init/1`, while the tree is starting —
which under `eta_run` is before a scheduler exists at all.

### 6. `set_metadata/2` did not honour a configurable timeout — FIXED

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
