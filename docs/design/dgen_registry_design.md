# dgen_registry Design

`dgen_registry` is an OTP-compatible **process registry**: give a running process a
logical name, then find or message it by name from anywhere in an Erlang cluster. It
implements the standard `{via, dgen_registry, {RegistryName, LogicalName}}` contract,
so `gen_server`, `gen_statem`, `gen_event` and friends can be registered and addressed
by name with no special API.

It coordinates through a **strongly-consistent database** (FoundationDB is the default
and reference backend). That choice shapes the rest of the design:

- **It is CP.** In CAP terms it picks consistency over availability. The side of a
  partition that cannot reach the database stops accepting registrations rather than
  risk giving one name to two processes (§5.3).
- **It is a *singleton* registry.** A name maps to at most one live process
  cluster-wide. The registry will **terminate** processes to keep that true, so
  register only processes that can withstand being killed (§5.6).
- **It stores almost nothing durably.** Pids are never written to the database, so the
  durable footprint does not grow with the number of registered names (§4.4).

This document is the source of truth for what the registry does. The
[Guarantees](#6-guarantees) and
[Non-goals](#7-non-goals-and-explicit-non-guarantees) sections are normative:
behaviour that contradicts a guarantee is a bug, and behaviour listed as a non-goal
is intentional.

[dgen_registry Message Flows](dgen_registry_message_flows.md) shows the messages
behind each operation as swimlane diagrams.

**This document is LLM generated.**

## 1. The one-minute model

- Each node runs a **member** process holding a local copy of the `name → pid` table.
- One member is the **leader**. Every change goes through it: register, unregister,
  and cleanup after a process dies. The leader applies changes in order and tells the
  others.
- The leader is chosen through the database, so every node agrees who leads, and a
  stale leader cannot make changes.
- A lookup is usually a local, lock-free ETS read. When you need an authoritative
  answer, you can ask the leader instead.

## 2. Quick start

```erlang
%% Start a registry named `my_registry` against a backend tenant.
{ok, _} = dgen_registry:start_link(my_registry, Tenant),

%% Register a gen_server under a logical name using the via-tuple.
{ok, _} = gen_server:start_link(
    {via, dgen_registry, {my_registry, user_service}},
    my_server, [], []),

%% Call it by name from any node in the cluster.
gen_server:call({via, dgen_registry, {my_registry, user_service}}, ping).
```

Run `dgen_registry:start_link/2` on **every** participating node, all pointing at the
same `Tenant` (the backend database and keyspace).

The direct API mirrors the via-tuple:

```erlang
dgen_registry:register_name({my_registry, user_service}, Pid),   %% -> yes | no
dgen_registry:whereis_name({my_registry, user_service}),         %% -> pid() | undefined
dgen_registry:unregister_name({my_registry, user_service}).      %% -> ok
```

A registration can carry searchable **metadata** that lives exactly as long as the
registration ([§4.7](#47-metadata-and-queries)):

```erlang
dgen_registry:register_name({my_registry, user_service}, Pid,
                             #{index => #{region => us_east}, data => #{version => 3}}),
dgen_registry:get_metadata({my_registry, user_service}),   %% -> {ok, #{pid, index, data}}
dgen_registry:query(my_registry, #{region => us_east}).    %% -> [#{name, pid, index, data}, ...]
```

## 3. Core concepts

**Logical names.** Any Erlang term, scoped to a registry (`{orders, 42}`, `"worker-7"`,
`user_service`). Names live only in memory and do not survive a full cluster restart
(Non-goal 1).

**Registries are independent namespaces.** Each registry (`orders`, `sessions`, …) has
its own leader, membership, name table and database keys. They share nothing.

**Two kinds of read.**

| Function | Reads from | Cost | Freshness |
|---|---|---|---|
| `whereis_name/1` | the local member's `protected` ETS table, read in the calling process | no network hop, no mailbox | *snapshot* — may briefly lag a change made on another node |
| `whereis_name_consistent/1` | the current leader, fence-verified against the database (§5.1) | one hop plus a database read | *authoritative* |

OTP's via routing uses `whereis_name/1`, so a `gen_server:call` through a via-tuple
takes the fast path and never queues behind the member's mailbox.

**Registration blocks until a leader decides it, and `no` is always a real answer.**
`register_name/2` answers `yes` if the name is now yours. It answers `no` **only** when
a different live process holds the name, or when an opt-in §8 policy refuses it
(`reject_when_degraded`, or `strict_replication` failing closed). When no leader is
reachable, the call **blocks**. That happens before the first election, during a
handoff, or across a partition. We re-drive the call once a leader is established. If
the leader was deposed or the commit failed, we retry or re-send the registration to
the next leader. We never answer `no` in these cases, because OTP's via machinery would
read it as "already started" for a name nobody holds.

If nothing decides in time, the call **exits** on `register_timeout` (§8, default 5 s),
as `global`, `syn` and distributed `gproc` do. The name *may* then be bound to the pid
you tried to register. A retry answers `yes` (§5.2), and if you abandon the pid, its
exit clears the binding.

**Automatic cleanup.** The leader monitors every registered process. When one exits,
the leader unregisters its name.

**A returned pid is a candidate, not a liveness guarantee.** Any pid the registry hands
back may have died by the time you use it. That applies to lookups, queries and
presence notifications alike.

## 4. How it works

### 4.1 Three processes per node

Starting a registry on a node creates three processes under a local `one_for_all`
supervisor:

| Process | Role |
|---|---|
| **member** | Holds the local `name → pid` replica in a `protected` ETS table that it alone writes and anyone reads. Serves reads. When leader, it is the sole writer of the name table. |
| **elector** | Tracks membership and decides the leader, coordinating through the database. |
| **connector** | Keeps Erlang distribution in step with membership (§4.6). It connects to member nodes, reaps stranded members, and probes whether the leader is alive. |

Application code only talks to the member (directly or via the via-tuple).

### 4.2 Leadership comes from the database

There is no heartbeat or bespoke election protocol. Each join or leave is an entry on a
durable queue that the database serialises. Every entry is applied exactly once, in one
global order, no matter which node consumes it. The elector commits the resulting member
set and leader to the database, so every node reads the same answer.

Leadership is **sticky**. The leader keeps the role for as long as it remains a member.
If it departs, the next membership change deterministically elects a replacement.

Each leadership term carries an increasing **epoch**, which is also written into the
durable leadership record. Members ignore messages from an older epoch. Because the
epoch only grows, it also serves as the fencing token of §5.1.

### 4.3 The leader is the sole writer

Every change to the name table — register, unregister, and auto-unregister on process
death — is applied by the leader through its own mailbox, one at a time, so changes are
totally ordered without locks. A follower **forwards** registrations to the leader and
passes the answer back to the caller. No member ever blocks its message loop waiting on
another member, which rules out circular waits.

Writes reach the database through a group-commit pipeline. A worker outside the
leader's loop runs the commits, with at most one in flight. There is no batching
window, so an idle leader commits the first op immediately. Ops that arrive while a
commit is in flight queue up. When the worker reports its result, the leader starts the
next commit with everything queued, up to `commit_batch_size` (§8). Batches therefore
grow only under load, and an unloaded leader adds no batching delay.

### 4.4 What the database actually stores

The database is for **coordination, not storage**. A pid read back later could name an
unrelated process that reused it, so we never persist pids. The name table lives only
in the members' ETS replicas, and a member starts with an empty one every time it
starts or restarts.

The name table costs **two keys**, regardless of how many names are registered:

- a **leader key**, which holds the current leader and its epoch. This is the fencing
  token of §5.1.
- a **version key**, which the leader rewrites once per batch of committed changes
  with a monotonically increasing commit version. Writing it turns every leader commit
  into a real transaction, so the fence takes effect. It also gives every batch a
  global order.

The elector also keeps durable state: the membership queue (§4.2) and the subscriptions
(§4.9). Its size depends on cluster size, churn and subscription count. Nothing scales
with the number of registered names.

### 4.5 Replication

After committing a batch, the leader broadcasts it to the other members as one message
carrying its version and the version of the batch before it. A member applies a batch
only if it is **contiguous** with what it holds. If there is a gap, for example from a
dropped message, the member stops and asks the leader for a fresh snapshot. Every
replica is therefore a *prefix* of the leader's ordered stream. The handoff
reconstruction (§5.7) relies on this.

### 4.6 Joining, leaving, and connectivity

A node joins by starting the registry, and leaves by stopping it or by failing. There
is no node list and no cluster-formation step. Membership lives in the database and is
read transactionally, so every node sees the same membership.

The registry also builds the Erlang distribution mesh. Each member reads the member set
at startup, periodically, and whenever a node becomes reachable. It then connects to
any member node it is not yet connected to. You do not need a discovery library or
`net_adm:ping`. A new node connects outward to the members it finds in the database,
and that triggers its first state sync. Nodes must still be *able* to connect. They
need a shared cookie and must be resolvable and reachable on the network.

#### Delegating connectivity (`connectivity => provided_externally`)

Distribution links belong to the whole node, but each registry runs its own mesh. The
mesh periodically reads the member set and sweeps through it connecting nodes. On a
node that hosts many registries, that work grows with the number of registries. A
registry started with `connectivity => provided_externally` turns its mesh off and
relies on the links another registry maintains. The intended setup is one **system
registry** in the default `self_managed` mode on **every** node, plus many
`provided_externally` tenant registries.

**Contract.** The provider must run on every node that any consumer runs on. Otherwise
a tenant member on an uncovered node is **silently isolated**. It cannot reach a
leader, so its writes block and time out (§3). Meeting this contract is the deployer's
job. As a backstop, a `provided_externally` registry that has had no elected leader for
about 60 s logs a warning that names the contract.

Only the mesh is delegated. The stranded-member reap, the leader-liveness probe (§5.3)
and the periodic epoch check (§5.7) stay active. Any value other than
`provided_externally`, including a typo, resolves to `self_managed`.

### 4.7 Metadata and queries

A registration can carry metadata that lives exactly as long as the registration:

- **`index`** — a map of attributes the registry indexes for **queries**.
- **`data`** — an opaque payload, stored and returned verbatim, never searched.

```erlang
%% Register with metadata in one fenced step (both fields optional; default #{} / undefined).
dgen_registry:register_name({my_registry, worker_1}, Pid,
                             #{index => #{role => worker, shard => 3}, data => #{node => n1}}),

%% Replace an existing registration's metadata (a replace, not a merge).
dgen_registry:set_metadata({my_registry, worker_1}, #{index => #{role => worker, shard => 4}}),

%% Read it back — lock-free (snapshot) or leader-routed (authoritative).
dgen_registry:get_metadata({my_registry, worker_1}),             %% -> {ok, #{pid, index, data}}
dgen_registry:get_metadata_consistent({my_registry, worker_1}),

%% Find every registration whose indexed metadata matches (AND of exact equalities).
dgen_registry:query(my_registry, #{role => worker}),             %% local snapshot
dgen_registry:query_consistent(my_registry, #{role => worker}).  %% leader-authoritative
```

**Reads.** Metadata lives in the same replica row that `whereis_name/1` reads.
`get_metadata/1` is a lock-free read in the calling process, like `whereis_name/1`.
`get_metadata_consistent/1` is its authoritative counterpart. Queries use an inverted
index on each member and run **through the member's mailbox**. For
`query_consistent/2` that is the leader's mailbox. We route queries this way because
the member applies a batch row by row. A lock-free read across several keys could see a
batch half-applied, but a query that waits its turn in the mailbox only sees whole
batches.

**Query semantics.** `Constraints :: #{attr() => value()}` matches every registration
whose `index` satisfies *every* clause. Each clause is an exact equality, and there are
no ranges, `OR`, negation or ordering. An empty map is rejected with
`{error, empty_query}`. There is no schema, so an attribute that no registration
carries simply matches nothing. `data` is never queryable.

**Writes.** `register_name/3` and `set_metadata/2` go through the same fenced,
single-writer commit pipeline as `register_name/2` (§4.3). Metadata adds no durable
state. `set_metadata/2` on an unregistered name returns `{error, not_registered}`. If
the leader loses leadership before it answers, the call returns `{error, no_leader}` so
the caller can retry. A follower that forwarded a `set_metadata/2` answers only after
its own row is updated, so a later local `get_metadata/1` on that node sees the write.

**Lifetime and handoff.** When an unregister or a `DOWN` clears a name, it also clears
the name's metadata and index entries. On handoff, metadata travels with the binding
through the reconstruction of §5.7. Conflict detection (§5.6) compares pids only.

### 4.8 Process identity

Starting a registry creates **zero atoms**, so an application can start many registries
dynamically, such as one per tenant, without leaking atoms. We use only the
caller-supplied `Name`. The member is registered as `Name`, and its ETS table is named
`Name`. The supervisor and the elector have no registered name. Callers find the
elector through the member's supervisor. The supervisor is `one_for_all`, so the
elector pid that the member caches can never be stale.

### 4.9 Presence: watch/notify subscriptions

A **presence subscription** turns a query (§4.7) into a live feed. A caller picks an id
and registers a **watch query** and a **notify query** under it. Whenever the set of
processes matching the watch query changes, the registry sends a message to every
process matching the notify query.

```erlang
%% Keep every process matching `notify` up to date with the set matching `watch`.
ok = dgen_registry:subscribe(my_registry,
                             {presence, RoomId},              %% SubId (application term)
                             #{role => member, room => RoomId}, %% watch
                             #{role => room_view, room => RoomId}), %% notify
%% Each matching room-view process now receives, per committed change:
%%   {dgen_presence, {presence, RoomId}, [{joined, Name, Pid} | {left, Name, Pid}, ...]}
dgen_registry:unsubscribe(my_registry, {presence, RoomId}).
```

A `query/0` is either `{all, #{attr => value}}`, with the same meaning as in `query/2`,
or a bare map, which is read as `{all, Map}`. The type is tagged so it can grow without
breaking existing subscriptions.

**Durable, keyed by an application id.** Subscriptions live in the elector's durable
state (§4.2). They are not tied to any process, and they survive a full cluster
restart. Calling `subscribe` with an existing `SubId` is an idempotent **upsert**. That
lets applications model subscriptions as database entities, and scale to zero and back
with the subscriptions intact.

**API.** `subscribe/4`, `unsubscribe/2`, `unsubscribe_all/1` and `subscriptions/1` go
to the elector. Writes are asynchronous. An `ok` means the elector accepted the change
for durable processing. The change commits shortly after, and then shows up in
`subscriptions/1`. If a write is lost before the elector enqueues it, subscribing again
recovers it.

**Teardown.** A registry's durable keys share one keyspace prefix. After you stop the
supervisor, `delete/2(Name, Tenant)` removes all of them in one range clear.
`unsubscribe_all/1` clears only the subscriptions of a running registry.

**Change detection.** As the leader applies each committed batch, it tests every
changed row against each watch query. A name that starts matching is `joined`, and one
that stops matching is `left`. A name rebound to a new pid produces both. This covers
register, unregister, process death and `set_metadata`.

**Initial snapshot.** Whenever a process *enters* the notify set, it first receives the
current watch set as `joined` events, and then the changes after that. This holds both
at `subscribe` time and later. A subscription can therefore exist before any of its
viewers do.

**Leader state.** The elector pushes committed subscription changes to the current
leader. A newly elected leader pulls the full durable set and recomputes every watch
set, which also catches up on any change it missed. Followers hold no presence state.

**Consistency.** Delivery is asynchronous and in commit order. Each viewer sees an
initial snapshot, then a prefix of the true sequence of watch-set changes. Delivered
pids are candidates (§3).

## 5. Consistency and failure model

### 5.1 Fencing: a stale leader cannot write — or serve consistent reads

When leadership moves, the elector commits the new leader's identity and epoch to the
database. From then on the old leader is **fenced**. Its commits conflict with the
leadership record, and the database rejects them. Because the record carries the epoch,
a member that was deposed and later re-elected cannot land a write from its earlier
term either. Transaction conflict detection, not timing, ensures that at most one
leader can write, so there is no split brain.

Consistent **reads** are fenced too. A deposed leader may not know it yet, for example
on the minority side of a partition. So before the member releases any `*_consistent`
answer, it re-checks its leader id and epoch against the durable record. If the check
fails or cannot reach the database, the member answers not-registered or empty. The
cost is one database read per consistent read.

### 5.2 Linearizable registration

All registrations flow through a single fenced leader that processes them one at a
time, so registration is **linearizable**: there is one order of decisions, and once
`register_name/2` returns `yes`, every later consistent read observes it until it
changes.

Registration is **idempotent for the same holder**. Registering a name to the pid that
already holds it returns `yes`. That makes re-driving safe. A caller that timed out
(§3) after its registration committed gets `yes` when it retries.

### 5.3 Partition behaviour: CP

Only the side that reaches the database and holds leadership can commit. On the other
side, registrations block and then time out (§3). An unregister answers `ok` and is
held until a leader can take it (Non-goal 5). Consistent reads answer not-registered
(§5.1). Local snapshot reads keep working, but they may be stale. The registry gives up
write availability to stay consistent.

Sometimes only Erlang distribution is partitioned. The mesh is cut, but both sides
can still reach the database. The two sides then briefly compete for the single leadership
record. One side wins and leads, and the other blocks as above until the mesh heals.
The leader-liveness probe only reaps a leader on a node the prober has *never*
connected to. That case exists to recover from a cold restart, and it means an isolated
member cannot depose a healthy leader. When the partition heals, §5.6 repairs any name
that was reissued across the split.

### 5.4 Uniqueness is single-fault tolerant

**Uniqueness** means at most one live process per name. It holds across the failure of
**any one member at a time**. Pids are not durable, so the only record that a name is
taken is in the members' in-memory replicas. Every acknowledged registration is held by
at least two members (§5.5), so losing one member never loses that record. Losing
*both* holders at once can lose it, and then the name could be reissued. The registry
detects and repairs that case (§5.6).

### 5.5 Addressability and the two-holder rule

**Addressability** asks whether a registered name can still be *found* after a failure.
When `register_name/2` returns `yes`, the binding is normally held by **two members**:

- A registration **forwarded** from a follower is held by the leader and that follower.
  The follower answers `yes` only once its replica has applied the commit version of
  that batch. If the follower is in the middle of a resync, it holds the answer until
  the resync finishes. This way the second copy is always visible to the handoff reconstruction
  (§5.7).
  The commit version only counts in the epoch that committed it. A later leader may
  have taken over without that commit, so the follower never releases the answer on a
  later leader's versions. If its replica moves to a later epoch first, it sends the
  registration again. For the same reason a follower treats a batch from a later epoch
  as a gap, so only a snapshot moves it into a new epoch.
- A registration made **on the leader's node** waits for `register_replicas` follower
  confirmations (§8) before answering `yes`. If none arrive within
  `replicate_timeout`, the default policy **degrades open**. It answers `yes` with a
  single holder and emits a warning and a telemetry event. With `strict_replication`
  the registration fails closed instead.

The rule protects the *routing entry* against losing a node that does not host the
process, which is typically the leader's node. If the process's own node dies, the
process is gone and its name is released.

### 5.6 Conflict resolution by termination

Suppose a process holds a name on a node that stays unreachable long enough to be
dropped, and the name is reissued elsewhere. When the node returns, two live processes
claim the name. When the registry sees two *different, live* pids for one name, it
**terminates both**, clears the name, and raises an alarm. The alarm is a log line,
plus a `[dgen_registry, conflict]` event if `telemetry` is present. Supervised
processes then restart and re-register through the single leader. Killing both, rather
than picking a winner, is deliberate.

Detection runs in two places. At every **leadership handoff**, the new leader checks
the replicas it gathered (§5.7). When a previously synced member **rejoins a continuing
leader**, which is how a partition usually heals, the leader checks the rejoining
member's replica. We skip a member that never synced. Repair needs a *witness*. If no
surviving replica reports a binding, we cannot detect the conflict (Non-goal 2).

**Staleness is not conflict.** A lagging member may still report a binding that was
legitimately unregistered while its process kept running. So every member keeps a
**release trail**. It records each explicit unregister of a live process, keyed by name
and pid, for `conflict_release_ttl` (§8). The trail is merged across handoffs. Entries
expire only while the cluster is fully connected and no member has departed within the
last TTL. When a member has been gone longer than the TTL, we judge its bindings
against whatever trail remains.

**Kill budget.** `conflict_kill_budget` (§8) limits how many times we terminate
processes for one name before escalating to an operator. The current leader tracks the
budget, and it resets on handoff, so the limit is per name *per leader term*.

This is the registry's explicit contract:

> The registry may forcibly terminate a registered process to enforce uniqueness.
> Register only processes that can withstand being forcibly killed — for example,
> supervised and restart-safe, or transient by design.

You can turn termination off with `terminate_on_conflict`, which leaves detection and
the alarm in place. You can also prevent the reissue in the first place with
`reject_when_degraded`, at some cost in availability. See
[Configuration](#8-configuration).

### 5.7 Leadership handoff

A new leader rebuilds its name table. It gathers the replica of every reachable member
in parallel and takes the freshest one. The old leader is already fenced, so no global
pause is needed. Until the gather completes, writes block (§3). We re-drive them once
the new leader takes over.

Replicas are gap-checked prefixes of one ordered stream (§4.5). So the freshest replica
holds the current binding for every name that any reachable member knows, and a
two-holder binding survives any single unreachable member. Sometimes a binding is held
*only* by an unreachable member. That happens with a degrade-open single holder, or
after a multi-fault loss. Such a binding is missing from the rebuilt table, and we do
not restore it when that member returns. If the name was reissued in the meantime, §5.6
resolves the conflict.

The deposed leader may have committed a batch whose broadcast is still on its way to a
member during the gather. Two guards keep the handoff sound in that case:

- The gather is **fenced against the durable version key** (§4.4). No live member can
  be ahead of that key, so the new leader gathers again, a bounded number of times,
  until its freshest replica reaches the key. Past that bound it proceeds anyway, and
  the handoff is **degraded**. A degraded handoff stopped short of the version key and
  may be missing committed bindings. `reject_when_degraded` (§8) acts on this.

  Waiting only helps while a broadcast can still arrive. If the deposed leader died
  after a commit but before broadcasting it, no live member will ever reach the key.
  Erlang delivers a process's messages before its DOWN, so each member reports whether
  it has handled the DOWN for the leader it followed. Once the new leader and every
  peer have, nothing more can arrive, and the new leader proceeds degraded without
  waiting out the bound.
- Every wholesale replacement of a replica, whether a handoff snapshot or a resync, is
  **version-monotonic**. A member rejects one older than what it holds, so a handoff
  never rolls a replica back.

The TLA+ model in [`formal/README.md`](../../formal/README.md) checks this argument.

A handoff always reaches its new leader eventually. Every member periodically compares
the epoch it has heard with the committed one, and announces itself again if they
differ. If leadership is lost while direct registrations are still waiting for
replica confirmations, the replication timeout policy (§8) decides their outcome.

## 6. Guarantees

These are normative. Behaviour that violates one of these is a defect.

1. **Singleton uniqueness (single-fault).** At most one live process is registered
   under a given name at a time, preserved across the failure of any single member.
   A simultaneous loss of both holders of a binding is outside this bound and is
   repaired reactively (§5.6), never silently tolerated.
2. **No split-brain writes.** At most one leader can commit changes. A leader that has
   lost leadership cannot register, unregister, or otherwise mutate the name table.
   There is no merge step and no dual-accept window.
3. **Linearizable registration.** Successful registrations have a single global order.
   After `register_name/2` returns `yes`, every later `whereis_name_consistent/1`
   reflects it until it changes.
4. **Two-holder durability of acknowledged registrations.** A registration acknowledged
   `yes` is held by at least two members and survives any single node failure —
   *except* under the default degrade-open policy after a replication timeout, when it
   may have one holder. `strict_replication` makes the property unconditional
   ([Configuration](#8-configuration)).
5. **Authoritative consistent reads.** `whereis_name_consistent/1` reflects the current
   leader's committed view of the name, even when a leader was deposed without knowing
   it. The leader checks every answer against the durable leadership record (§5.1), so
   a deposed leader answers not-registered rather than a stale value.
6. **Automatic unregistration.** When a registered process exits, its name is
   unregistered without an explicit call.
7. **CP under partition.** The side of a partition that cannot reach the database, or
   that does not hold leadership, does not accept registrations. It never invents a
   second owner for a name.
8. **Registry isolation.** Distinct registries do not share leadership, membership,
   name tables, or database keys.
9. **Consistent membership and convergent connectivity.** Every node observes the same
   committed member set. Assume the nodes are able to connect, with a shared cookie and
   network reachability. Then after a node's join commits, the cluster converges until
   `nodes()` on each member returns every other reachable member, with no external
   discovery. In `connectivity => provided_externally` mode (§4.6, §8) the registry does
   not connect nodes itself, and convergence depends on the provider registry.
10. **Metadata lifetime is tied to the registration.** A registration's `index`, `data`
    and index entries exist exactly while the registration does and are removed on
    unregister or process exit, by the same path (§4.7).
11. **Metadata writes are fenced and linearizable.** `set_metadata/2` goes through the
    same fenced, single-leader commit pipeline as `register_name/2,3`. A leader that has
    lost leadership cannot apply or broadcast a metadata change.
12. **Metadata reads mirror name reads.** `get_metadata/1` is a lock-free snapshot read
    with the freshness of `whereis_name/1`. `get_metadata_consistent/1` is answered by
    the leader, like `whereis_name_consistent/1`.
13. **Queries are batch-consistent.** `query/2` and `query_consistent/2` never observe a
    half-applied commit batch (§4.7). Both are still snapshots, and a returned pid may
    already have died.

## 7. Non-goals and explicit non-guarantees

These are intentional. Relying on the opposite is relying on something the registry
does not promise.

1. **Durability of names across a full restart.** Pids are never persisted. If every
   member of a registry is down at once, its names are gone and applications must
   re-register on startup.
2. **Multi-fault uniqueness.** The simultaneous loss of both holders of a binding may
   lose the registration and can briefly allow the name to be reissued. If a surviving
   replica still reports the old binding, the registry detects and repairs this (§5.6).
   With no surviving witness, the displaced process simply loses its registration and
   is not terminated. Nothing prevents it unless `reject_when_degraded` is enabled,
   which gives up some availability to do so.
3. **Availability during a partition.** The disconnected or minority side cannot
   register names (§5.3).
4. **Strong freshness from `whereis_name/1`.** The snapshot read may briefly lag a change
   made elsewhere. Use `whereis_name_consistent/1` for the authoritative answer.
5. **Synchronous cluster-wide unregistration.** `unregister_name/1` normally returns `ok`
   after the removal has committed on the leader. It does not wait for every node to
   see it, because replication is asynchronous. The removal still always propagates.
   We re-drive an unregister to the current leader instead of dropping it when its
   commit fails, its leader is deposed, its forward is lost, or no leader is reachable.
6. **Preservation of a process the registry must terminate.** The registry may terminate
   a registered process to enforce uniqueness (§5.6). A process that is neither
   restart-safe nor transient by design is not a good fit. Terminating it is intended
   behaviour, not a bug.
7. **Automatic re-registration after failure.** After a multi-fault loss, detecting the
   loss and re-registering (typically via a supervisor) is the application's job.
8. **Rich queries.** Queries support only a conjunction of exact-equality clauses over
   indexed attributes. There are no ranges, `OR`, negation, prefix or substring
   matching, ordering, pagination, or joins (§4.7).
9. **Querying non-indexed metadata.** Only `get_metadata/1` and
   `get_metadata_consistent/1` return `data`. A query never matches or filters on it.
10. **Durability of metadata across a full restart.** Like names (item 1), metadata
    lives only in memory. Applications set it again when they re-register.
11. **Uniqueness or constraints on indexed values.** Many registrations may share an
    indexed attribute and value. There is no unique-attribute constraint.
12. **Per-pid (cross-name) metadata.** Metadata belongs to a registration, not a pid.
    A process registered under several names has separate metadata for each.

## 8. Configuration

The defaults make the registry single-fault tolerant. The settings below trade
availability against strictness. Configuration is **per registry**, and you pass it at
start:

```erlang
dgen_registry:start_link(orders, Tenant, #{
    register_replicas => 2,
    strict_replication => true
}).
```

An unset key falls back to the `dgen` application environment, which acts as a global
default, and then to the built-in default below.

| Setting | Default | Effect |
|---|---|---|
| `register_timeout` | `5000` ms | How long a leader-routed write (`register_name/2,3`, `unregister_name/1`, `set_metadata/2`) waits before the caller exits (§3). |
| `register_replicas` | `1` | How many follower copies a *direct* registration waits for before answering `yes` (§5.5), capped at the number of followers. A higher value makes registrations more durable but slower. |
| `commit_batch_size` | `5000` | The most write ops coalesced into one group commit. The rest go in the next commit. This bounds the work per commit, so the leader stays responsive during a burst such as the flood of `DOWN`s from a departing node. It affects only latency and throughput, never correctness. |
| `replicate_timeout` | `1000` ms | How long the leader waits for those confirmations before applying the timeout policy. |
| `strict_replication` | `false` | The timeout policy. With `false`, the leader **degrades open**. It answers `yes` with whatever holders exist, possibly just one, and emits a warning and a telemetry event. With `true`, it **fails closed**. It answers `no` and retracts the binding. If this leader is deposed first, the next leader takes over the retraction, so the binding cannot survive. |
| `terminate_on_conflict` | `true` | Whether a detected conflict (§5.6) terminates both claimants, or only raises the alarm. |
| `conflict_kill_budget` | `{3, 60000}` | At most *N* terminations per name per window (ms) before escalating to an operator. Per name *per leader term* (§5.6). |
| `conflict_release_ttl` | `600000` ms | How long an explicitly released live name/pid pair stays in the release trail (§5.6). Expiry pauses while any member is disconnected and for one TTL after any member departs. Entries are tiny. |
| `reject_when_degraded` | `false` | Prevents reissued names instead of repairing them (§5.6). When `true`, a leader whose last handoff was **degraded** answers `no` to registering a name it does not already hold. A handoff is degraded when its reconstruction stopped short of the durable version key (§5.7). Members that are merely behind or never synced do not cause this, so normal scaling up and down does not trip it. Losing every holder of a binding does. This gives up availability in exchange for prevention. |
| `connectivity` | `self_managed` | Who maintains the distribution mesh (§4.6). With `self_managed`, this registry runs its own mesh. With `provided_externally`, the mesh is off, and the registry relies on a `self_managed` registry that runs on every node this one runs on. Otherwise the node is silently isolated, and a long stretch with no leader logs a warning. The other per-registry backstops stay active. Any other value resolves to `self_managed`. |

## 9. Comparison with `global`, `gproc`, and Elixir's `Registry`

Elixir's `Registry` solves the single-node problem. `global`, `gproc` in global mode,
and `dgen_registry` each solve the cluster-wide one in a different way. Each is a reasonable
trade-off.

### Elixir's `Registry` (standard library, single node)

A fast, partitioned, ETS-backed **single-node** registry with unique and duplicate keys,
via-tuple support, values and dispatch. If you only need names on one node, it is the
natural choice and needs no dependency. It does not provide one owner of a name across a
cluster, which is what `dgen_registry` is for.

### `global` (OTP, built in)

Built into OTP with no dependency. It keeps one cluster-wide namespace, replicated on
every node and serialised by distributed locks. It leans toward availability. Both
sides of a partition keep accepting registrations, and when the partition heals, a
resolver function settles any clashes. Choose it for a zero-dependency, always-available
registry when you can define how to merge. Choose `dgen_registry` to guarantee a single
owner through a partition, if you can run a consistent database.

### `gproc` (third-party)

A feature-rich, fully in-memory library. It offers names, properties, counters,
aggregated counters and `ets:select`-style queries. It has a fast local mode and a
global mode that elects a leader among the Erlang nodes. Much of what it offers is
outside the scope of `dgen_registry`, and it needs no external database. In return for
depending on a database, `dgen_registry` gives strict consistency and dynamic
membership. There is no node list, and stale leaders are fenced automatically.

### At a glance

| Property | Elixir `Registry` | `global` | `gproc` (global) | `dgen_registry` |
|---|---|---|---|---|
| Cluster-wide | No (single node) | Yes | Yes | Yes |
| Ships with | Elixir stdlib | OTP | No (library) | No (library) |
| External dependency | None | None | None | A CP database backend |
| Coordination source | N/A (local) | Distributed locks | Leader among nodes | Strongly-consistent database |
| Partition stance | N/A (local) | Stays available, merges on heal | Available, leader-coordinated | Refuses minority writes (CP) |
| Membership model | Single node | Dynamic (node connections) | Leader election among nodes | Dynamic join/leave via the DB; no node list |
| Scope | Names (+ duplicate keys, dispatch) | Names | Names, properties, counters, rich queries | Names + lifetime-tied metadata |
| Metadata / secondary index | Values (no query) | No | Properties, counters, `ets:select`-style queries | `index`/`data` per registration; AND-equal query over `index` |
| Authoritative read | Local (single node) | — | — | `whereis_name_consistent/1` (`get_metadata_consistent/1`, `query_consistent/2`) |
| Enforces single owner via termination | No | No | No | Yes (configurable) |
| Isolated namespaces | Yes (each `Registry`) | No (one namespace) | Via classes/scopes | Yes (independent registries) |

## 10. When `dgen_registry` is (and isn't) a good fit

**Good fit** when all of these hold:

- You need a strict cluster-wide singleton, and you would rather a name be briefly
  unavailable than briefly owned twice.
- Your registered processes can withstand being killed.
- You can operate a consistent database such as FoundationDB.
- Any lookup beyond name → pid is a simple AND of equalities (§4.7).

**Reconsider** when writes must stay available on every side of a partition, you cannot
run an external database, a registered process must never be killed by the registry, or
you need counters or a general selection API.
