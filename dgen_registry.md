# dgen_registry

An OTP-compatible process registry backed by FoundationDB, implementing the
`{via, dgen_registry, {RegistryName, LogicalName}}` contract so that standard
OTP processes (`gen_server`, `gen_statem`, `gen_event`, etc.) can be registered
and addressed by name across an Erlang cluster.

## Quick start

```erlang
{ok, _} = dgen_registry:start_link(my_registry, Tenant),

%% Register a gen_server under a logical name
gen_server:start_link({via, dgen_registry, {my_registry, user_service}},
                      my_server, [], []),

%% Call it from any node in the cluster
gen_server:call({via, dgen_registry, {my_registry, user_service}}, ping).
```

## Architecture

Each node that participates in the registry starts two processes under a local
supervisor:

| Process | Module | Role |
|---------|--------|------|
| `<name>_elector` | `dgen_registry_elector` | `dgen_server` callback — tracks membership, elects a leader via FDB consensus |
| `<name>_member` | `dgen_registry_member` | `gen_server` — local name cache, consistent read/write proxy |

### Leader election

The leader is determined by FDB's transaction commit order: whichever node's
elector consumer commits the current FDB transaction becomes the leader
(`node()` inside the callback).  This means leadership is an emergent property
of FDB's serialisability — no external lease, heartbeat, or manual tiebreaker
is required.

When the leader changes, the elector returns `{lock, NewState}` from its
callback, which atomically commits the new leader to FDB and pauses all other
elector consumers via a distributed lock.  `handle_locked/3` uses this window
to fan out `{leader_changed, Leader}` casts to every member before queue
processing resumes.

### Name storage

Name→Pid mappings are **never written to FDB**.  Pids are node-local and
process-lifetime-scoped; they have no meaning after a restart.  The
authoritative names map lives in the leader member's `gen_server` state and
is replicated in-memory to all follower members.

### Consistency model

| Operation | Path | Guarantee |
|-----------|------|-----------|
| `register_name/2` | local member → leader (if follower) | linearisable; leader's mailbox is the serialisation point |
| `unregister_name/1` | cast to local member → leader | fire-and-forget; eventual consistency |
| `whereis_name/1` | local member's `names` map | snapshot; may lag by one replication round-trip on remote nodes |
| `whereis_name_consistent/1` | local member → leader | linearisable; always authoritative |

### Replication

After every write the leader broadcasts `{name_registered, …}` or
`{name_unregistered, …}` to all follower members.  Followers apply these
immediately to their local `names` map.

Because distributed Erlang does not guarantee that a replication cast arrives
before the call reply that triggered it, followers also apply an **optimistic
local update** when they forward a write to the leader:

- `{register, Name, Pid}` forwarded → follower updates its local map on `yes`.
- `{unregister, Name}` forwarded → follower removes from its local map first.

Both are idempotent when the replication cast arrives shortly after.

### Auto-unregistration

The leader monitors every registered Pid with `erlang:monitor/2`.  On
`{'DOWN', …}` the leader removes the entry from its names map and broadcasts
`{name_unregistered, …}` to all followers.  No explicit `unregister_name` call
is needed when a registered process exits.

---

## Comparison with `global`

Erlang's built-in `global` module also provides a cluster-wide process
registry.  `dgen_registry` takes a different set of trade-offs in every
dimension.

### Consensus and split-brain

`global` uses a two-phase lock protocol for registrations and resolves network
partitions with a custom merge algorithm.  During a netsplit each partition
continues to operate, and on reconnect `global` invokes a user-supplied
"resolve" function to decide which registration wins.  In practice this is
fragile: the resolve function is rarely written correctly, and the merge can
produce inconsistent state.

`dgen_registry` delegates consensus to FoundationDB, which is a CP system.
During a partition the minority side cannot commit to FDB and therefore cannot
register names.  There is no merge step because there is never more than one
consistent view of the name table.

### Durability of leadership

`global` has no notion of a persistent leader.  Any node can handle any
registration at any time, subject to the two-phase lock.

`dgen_registry` elects a durable leader via FDB.  The leader identity is
written to FDB on every membership change, so any node can discover the current
leader without gossip.  When the leader node goes down, a new one is elected
the next time any surviving node's elector consumer commits a FDB transaction.

### Name storage and lookup

`global` stores names in an ETS table on every node, updated via a global
broadcast.  `whereis_global/1` is a local ETS lookup — fast, but with the same
eventual-consistency caveat as `whereis_name/1`.

`dgen_registry` stores names purely in `gen_server` state (Erlang maps).
Snapshot reads (`whereis_name/1`) are served from the local member's map.
Consistent reads (`whereis_name_consistent/1`) route to the leader — a
`gen_server:call` with no FDB round-trip.  Pids never touch durable storage.

### Consistency of writes

`global` serialises writes with a two-phase lock over Erlang message passing.
There is no distributed transaction — the lock can be held across arbitrary
message latency and the protocol breaks under concurrent registration attempts
to the same name from multiple nodes.

`dgen_registry` serialises writes through the leader's `gen_server` mailbox.
A single process handles all registrations sequentially; no distributed lock
protocol is needed beyond the leader election itself.

### Dead-process cleanup

`global` detects dead processes via `nodedown` signals and re-registrations.
The cleanup path is interleaved with the merge protocol and has historically
been a source of subtle bugs.

`dgen_registry` uses `erlang:monitor/2` on every registered Pid.  The `DOWN`
signal is immediate and local; cleanup is a simple map removal followed by a
broadcast cast.  There is no merge and no race with reconnection logic.

### Multiple registries

`global` is a single system-wide namespace.  All registered names share the
same ETS table and the same lock domain.

`dgen_registry` supports multiple independent registries, each with its own
`RegistryName` and FDB subspace.  Registries are isolated: leadership, members,
and name tables are completely separate.

### Summary

| Property | `global` | `dgen_registry` |
|----------|----------|-----------------|
| Consensus | Erlang 2-phase lock | FoundationDB (CP) |
| Split-brain | Partition tolerant, merge on reconnect | Minority side blocks |
| Name storage | ETS (every node) | `gen_server` map (replicated) |
| Pid in durable storage | No | No |
| Consistent write path | Distributed lock | Leader mailbox |
| Consistent read | N/A (ETS only) | `whereis_name_consistent/1` |
| Dead-process cleanup | `nodedown` + merge | `erlang:monitor` + broadcast |
| Multiple namespaces | No | Yes |
