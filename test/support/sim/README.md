# dgen_registry Simulation Harness

Fault-injecting simulation of the replication protocol (§4.5, §5 of
[the design doc](../../../docs/design/dgen_registry_design.md)), run against the
real `dgen_registry` code in a single BEAM.

[`formal/`](../../../formal/README.md) proves an abstraction of the *protocol*
correct by checking every interleaving. This harness checks that the
*implementation* matches the protocol, by sampling interleavings of the real
code. The properties in `DGen.Sim.Invariants` have the same names as the
invariants in `DgenRegistryReplication.tla`. A violation found here should be
reproducible in the model, and the other way round. If it isn't, one of the two
is wrong about the system.

## Running

```sh
mix dst                 # every :simulation-tagged sweep, on the dgen_mem backend
mix dst --seed 0        # extra arguments pass through to mix test
```

There are two suites:

- **`test/dgen_registry_eta_test.exs`** is fully deterministic. It runs the
  cluster under `eta_run` on `dgen_mem`, and one seed controls the scheduler,
  the clock and the network. Use it when a failure has to replay exactly. It
  checks that it is deterministic: each seed runs three times and must produce
  one trace, and every `eta_run:audit/1` check is fatal. The sweeps are the
  lossy replication stream (`drop_p`) and node faults (`fault_p`).
- **`test/dgen_registry_sim_test.exs`** runs in real time on the real scheduler.
  The fault schedule is seeded, but process scheduling and timers are not, so a
  failure can be reproduced at the fault level but not bit-for-bit.

`mix dst` runs only the `:simulation` sweeps. The other tests in both suites run
under an ordinary `mix test`. The deterministic ones need
`DGEN_BACKEND=dgen_mem`, and `test_helper.exs` lists what each backend excludes.

A failure message includes its seed. Re-run that seed to reproduce it. A
recorded `eta` trace replays with `DGen.Sim.RegistryHarness.replay_fixture/2`.
`test/fixtures/partial_batch.eta` is the pinned example.

**Mutation suites.** Two defects can be compiled in on purpose to show the
suite catches them. Both are opt-in, and the build is test-only:

```sh
DGEN_MUTATION=partial_batch DGEN_BACKEND=dgen_mem mix test --only mutation
DGEN_MUTATION=quiet_resync mix test --only mutation_quiet
```

## Invariants

From `DGen.Sim.Invariants`. Each one returns `:ok` or `{:violation, details}`
and must not raise on a cluster that is partly torn down.

| When | Property |
|---|---|
| always | `leader_epoch_unique/1` |
| at quiescence | `same_version_same_replica/1` (`check_quiescent/1`) |
| after convergence | `unique_binding/2`, `acked_bindings_present/2` |
| end of an `eta_run` | `RegistryHarness.check_final/2`: `acked_bindings_present/2` plus a UniqueBinding check over the history of `yes` acks (the spec's `acked` set) |

`same_version_same_replica` is checked only once the cluster is converged.
Members write their replica optimistically: `route_unregister/3` deletes a row
straight away, and a follower inserts a row when the leader's `yes` arrives. In
both cases `applied_version` doesn't advance. So two members at the same version
can legitimately differ until the batch arrives. A difference that is still
there after healing and quiescence is a bug.

## How it works

- **`keyspace`** (`dgen_registry:start_link/3`) separates a member's identity
  from its durable keyspace. That lets N members with different names share one
  registry's elector queue, leader key and version key in one VM. Each member is
  a complete, real supervision tree.
- **`eta_net`**: every message between members goes through
  `dgen_registry_member:cast_to_member/2`. `eta_transform` rewrites the
  `gen_server:cast/2` under it to `eta_net:cast/2`, so `eta_net` controls
  delivery. A module needs `-include("dgen_eta.hrl")` if it *runs inside* a
  scheduled process, even if it doesn't own one. The API modules `dgen` and
  `dgen_registry` are examples.
- **`dgen_registry:status/1`** reports each member's own view of leader, epoch,
  sync state and applied version. The invariants compare these across members.
- **`DGen.Sim.Cluster`** starts, faults and heals the cluster.
  **`DGen.Sim.RegistryHarness`** adapts it to `eta_harness`: it starts the
  cluster, picks which processes to schedule, runs the workload and checks the
  result. Its moduledoc lists the real-time dependencies that had to be removed
  before runs became deterministic.

## Fault model

Only faults that Erlang distribution can actually produce are injected. For each
ordered `{from, to}` pair, the network can **deliver**, **drop**, **delay** or
**cut** a message. It never reorders messages within a pair, which matches the
spec's `DropMsg`. Injecting reordering would only produce false positives.

**A lost message comes with a link signal.** In real distribution, messages are
lost because a link failed, and the failure sends `nodedown`/`nodeup` to both
ends. The registry relies on those signals: `handle_info({nodeup, _})` re-joins
and re-drives stashed unregisters. So `Cluster.converge/2` heals through
`eta_net:heal_partition/3` with a `nodeup` signal. A fault model that leaves out
these signals produces failures that look like product bugs but aren't.

**Topology decides what is faultable.** `Cluster.place_members/1` puts each
member's tree on its own simulated node. `eta_net` faults a send only when both
ends are faultable and on different nodes. Anything a member spawns inherits its
node and whether it can be faulted. The elector and connector are `attach`ed:
they are on the node but never on the wire, because their messages stand in for
operations on the durable store. If they were faultable, the run would produce
failures the real system can't have. If they weren't on any node, they would
survive a node kill that took down their own member.

### Node faults

`partition/4`, `heal_partition/4`, `isolate/3` and `kill_node/3` on
`DGen.Sim.Cluster` inject link-level failures, along with the events each one
causes:

| | messages | `{nodedown, Peer}` | `noconnection` DOWNs | processes die |
|---|---|---|---|---|
| `set_policy` loss | dropped at random | — | — | — |
| `cut/2` | dropped on one channel | — | — | — |
| `partition/4` | both directions cut | both sides | across the cut | — |
| `kill_node/3` | in flight cancelled | survivors | across the node | the whole tree |
| `crash/2` | — | — | — | the whole tree |

Each side gets its own signal: A is told `{nodedown, member_2}` and B is told
`{nodedown, member_1}`. `%{learns: :a}` makes the signal one-sided. With
`kill_node/3`, peer monitors report `noconnection`. With `crash/2`, they report
`killed`. The supervisor is frozen before the kill so `one_for_all` can't
restart the node.

Peer monitors are simulated (and so can be severed) only if both ends are already
on different simulated nodes when the monitor is created. `Cluster.start/3`'s
`:simulate_peer_monitors` holds each member with `sys:suspend/1` until every
tree has been placed. Without that, which monitors were simulated would depend
on real-time luck. It stays off in the real-time suite, because a simulated
monitor never fires on a crash when no scheduler is running.

`eta_net:stats/0` counts `dropped`, `signalled` and `noconnection`. The sweeps
assert on these counts so that a run that injected nothing can't count as a
pass. `stray_timers` is allowed on the node-fault sweep only: a killed node's
timers outlive it, and a call blocked behind a cut waits on a virtual timeout
that the run never reaches.

**Not simulated: `net_kernel`.** A simulated node is only a name. `nodes()`
doesn't list it, and `dgen_utils:node_reachable/1` returns `true` for every
member. So `dgen_registry_member`'s signal-driven reactions are covered, but
`dgen_registry_connector`'s node-keyed backstops (the `{nodedown, Node}` reap
and the leader-liveness probe) are not. Those belong in
`dgen_registry_cluster_test.exs`, which uses real peers.

## Findings

Defects this harness found. All are fixed. Tests and the spec refer to them by
number.

1. **Partially delivered group commit.** A batch used to go out as one message
   per name, all with the same version. A member that received only some of them
   reported the full version, and gap detection couldn't see the hole. The fix
   ships a batch as a single `{names_batch, …}` message, which also matches the
   spec's one broadcast per version. Regression: `"dropping a batch leaves a
   detectable gap that resyncs, not a silent hole"`.
2. **Gap detection needed traffic.** A follower that lost the tail of the stream
   stayed behind for as long as the cluster was idle. The fix is the leader's
   periodic empty-batch heartbeat. Regression: `"a follower that loses the tail
   of the stream still converges when quiescent"`.
3. **Unprotected send in the mesh nudge.** A send inside `try ... of` wasn't
   covered by the `catch`, so sending to a member that had exited crashed the
   helper.
4. **`dgen` and `dgen_registry` weren't built with the eta transform.** Their
   `gen_server:call`s escaped the scheduler. This was a simulation-only gap.
5. **`telemetry_available/0` called `code_server` from inside a member.** It is
   now resolved in `init/1`.
6. **`set_metadata/2` and `unregister_name/1` ignored `register_timeout`.**
7. **One unregister freed a name twice.** An optimistic delete plus an
   unconditional remove let a parked register get `yes` while an acked binding
   still stood, and then deleted it. `plan_op/3` now clears a name only if the
   current holder is the pid the unregister targeted. Found by the ack-history
   check in `check_final/2`. Replica comparisons can't catch it, because every
   member agrees on the same wrong history.
