# dgen_registry Design

`dgen_registry` is an OTP-compatible **process registry**: give a running process a
logical name, then find or message it by name from anywhere in an Erlang cluster. It
implements the standard `{via, dgen_registry, {RegistryName, LogicalName}}` contract,
so `gen_server`, `gen_statem`, `gen_event` and friends can be registered and addressed
by name with no special API.

It coordinates through a **strongly-consistent database** (FoundationDB is the default
and reference backend). That choice defines it:

- **It is CP.** The side of a partition that cannot reach the database stops accepting
  registrations rather than risk giving one name to two processes (§5.3).
- **It is a *singleton* registry.** A name maps to at most one live process
  cluster-wide, and the registry will **terminate** processes to keep that true, so
  register only processes that can withstand being killed (§5.6).
- **It stores almost nothing durably.** Pids are never written to the database; the
  durable footprint does not grow with the number of registered names (§4.4).

This document is the source of truth for what the registry does. The
[Guarantees](#6-guarantees) and
[Non-goals](#7-non-goals-and-explicit-non-guarantees) sections are normative:
behaviour that contradicts a guarantee is a bug, and behaviour listed as a non-goal
is intentional.

**This document is LLM generated.**

## 1. The one-minute model

- Each node runs a **member** process holding a local copy of the `name → pid` table.
- One member is the **leader**. Every change — register, unregister, cleanup after a
  process dies — goes through it; it applies changes in order and tells the others.
- The leader is chosen through the database, so every node agrees who leads, and a
  stale leader cannot make changes.
- Lookups are usually a local, lock-free ETS read; an authoritative lookup is available
  when you need it.

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

**Registration blocks until decided; `no` is always adjudicated.** `register_name/2`
answers `yes` if the name is now yours, and `no` **only** when the name is held by a
different live process or under an opt-in §8 policy (`reject_when_degraded`, or
`strict_replication` failing closed). With no reachable leader (none elected yet, a
handoff window, a partition) the call **blocks** and is re-driven once one is
established; a registration whose leader was deposed, or whose commit failed, is
retried or re-sent to the next leader. A `no` there would tell OTP's via machinery
"already started" for a name that is not taken.

If nothing decides in time the call **exits** on `register_timeout` (§8, default 5 s),
as `global`, `syn` and distributed `gproc` do. The name *may* then be bound to the pid
you tried to register: a retry answers `yes` (§5.2), and if the pid is abandoned its
exit clears the binding.

**Automatic cleanup.** The leader monitors every registered process; when one exits,
its name is unregistered automatically.

**A returned pid is a candidate, not a liveness guarantee.** Any pid the registry hands
back — from a lookup, a query, or a presence notification — may have died by the time
you use it.

## 4. How it works

### 4.1 Three processes per node

Starting a registry on a node creates three processes under a local `one_for_all`
supervisor:

| Process | Role |
|---|---|
| **member** | Holds the local `name → pid` replica in a `protected` ETS table that it alone writes and anyone reads. Serves reads. When leader, it is the sole writer of the name table. |
| **elector** | Tracks membership and decides the leader, coordinating through the database. |
| **connector** | Keeps Erlang distribution in step with membership (§4.6): connects to member nodes, reaps stranded members, probes leader liveness. |

Application code only talks to the member (directly or via the via-tuple).

### 4.2 Leadership comes from the database

There is no heartbeat or bespoke election protocol. Membership changes (join, leave)
are entries on a durable queue that the database serialises: each is applied exactly
once, in one global order, whichever node consumes it. The resulting member set and
leader are committed to the database, so every node reads the same answer.

Leadership is **sticky**: the leader keeps the role while it remains a member. If it
departs, the next membership change deterministically elects a replacement.

Each leadership term carries an increasing **epoch**, also written into the durable
leadership record. Messages from an older epoch are ignored, and the epoch makes the
fence of §5.1 a monotonic fencing token.

### 4.3 The leader is the sole writer

Every change to the name table — register, unregister, and auto-unregister on process
death — is applied by the leader through its own mailbox, one at a time, so changes are
totally ordered without locks. A follower **forwards** registrations to the leader and
relays the verdict. No member ever blocks its message loop waiting on another member,
which rules out circular waits.

### 4.4 What the database actually stores

The database is for **coordination, not storage**. A pid read back later could name an
unrelated, reused process, so pids are never persisted; the name table lives only in
the members' ETS replicas, recreated empty whenever a member (re)starts.

The name table costs **two keys**, regardless of how many names are registered:

- a **leader key** — the current leader and its epoch (the fencing token, §5.1);
- a **version key** — rewritten once per batch of committed changes with a
  monotonically increasing commit version. It makes each leader write a real
  transaction (so the fence engages) and gives every batch a global order.

The elector's state (membership queue, §4.2; subscriptions, §4.9) is bounded by cluster
size, churn and subscription count. Nothing scales with the number of registered names.

### 4.5 Replication

After committing a batch, the leader broadcasts it to the other members as one message
carrying its version and the previous batch's version. A member applies a batch only
if it is **contiguous** with what it holds; on a gap (e.g. a dropped message) it stops
and asks the leader for a fresh snapshot. Every replica is therefore a *prefix* of the
leader's ordered stream — the property the handoff reconstruction (§5.7) relies on.

### 4.6 Joining, leaving, and connectivity

A node joins by starting the registry and leaves by stopping it (or failing). There is
no node list and no cluster-formation step: membership lives in the database and is
read transactionally, so every node sees the same membership.

The registry also builds the Erlang distribution mesh. At startup, periodically, and
whenever a node becomes reachable, each member reads the member set and connects to any
member node it is not connected to. You do not need a discovery library or
`net_adm:ping`; a new node connects outward to the members it finds in the database,
which triggers its first state sync. Nodes must still be *able* to connect: shared
cookie, resolvable and reachable on the network.

#### Delegating connectivity (`connectivity => provided_externally`)

Distribution links are node-global, but each registry runs its own mesh (a periodic
member-set read and connect sweep), so on a node hosting many registries that work
scales with the number of registries. A registry started with
`connectivity => provided_externally` turns its mesh off and free-rides on links
another registry maintains. The intended shape is one **system registry** in the
default `self_managed` mode on **every** node, plus many `provided_externally` tenant
registries.

**Contract.** The provider must run on a **node-superset** of every consumer's nodes.
Otherwise a tenant member on an uncovered node is **silently isolated**: it cannot
reach a leader, so its writes block and time out (§3). This is the deployer's
responsibility; as a backstop, a `provided_externally` registry with no elected leader
for ~60 s logs a warning naming the contract.

Only the mesh is delegated: the stranded-member reap, the leader-liveness probe (§5.3)
and the durable-epoch nudge (§5.7) stay active. Any value other than
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

**Reads.** Metadata lives in the same replica row `whereis_name/1` reads.
`get_metadata/1` is a lock-free caller-side read like `whereis_name/1`;
`get_metadata_consistent/1` is its authoritative counterpart. Queries use a per-member
inverted index and run **through the member's mailbox** (on the leader for
`query_consistent/2`): the member applies a batch row by row, so a lock-free multi-key
read could see it half-applied, whereas a mailbox-serialised query sees whole batches.

**Query semantics.** `Constraints :: #{attr() => value()}` matches every registration
whose `index` satisfies *every* clause (AND of exact equalities; no ranges, `OR`,
negation or ordering). An empty map is rejected with `{error, empty_query}`. An
attribute no registration carries simply matches nothing — there is no schema. `data`
is never queryable.

**Writes.** `register_name/3` and `set_metadata/2` ride the same fenced, sole-writer
commit pipeline as `register_name/2` (§4.3); metadata adds no durable state.
`set_metadata/2` on an unregistered name returns `{error, not_registered}`; if the leader
loses leadership before answering it, the call returns `{error, no_leader}` so the
caller can retry. A follower that forwarded a `set_metadata/2` answers only after its own
row is updated, so a following local `get_metadata/1` on that node sees the write.

**Lifetime and handoff.** The unregister/`DOWN` path that clears a name clears its
metadata and index entries. On handoff, metadata travels with the binding through the
reconstruction of §5.7; conflict detection (§5.6) adjudicates pids only.

### 4.8 Process identity

Starting a registry creates **zero atoms**, so an application can start many registries
dynamically (one per tenant, say) without leaking atoms. Only the caller-supplied `Name`
is used: the member is registered as `Name` and its ETS table is named `Name`; the
supervisor and elector are unregistered (the elector is found through the member's
supervisor). The supervisor is `one_for_all`, so the member's cached elector pid is
never stale.

### 4.9 Presence: watch/notify subscriptions

A **presence subscription** turns a query (§4.7) into a live feed: under an
application-chosen id, a caller registers a **watch query** and a **notify query**, and
the registry pushes a message to the notify targets whenever the set of processes
matching the watch query changes.

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

A `query/0` is `{all, #{attr => value}}` (the `query/2` semantics) or a bare map read as
`{all, Map}`. The type is tagged so it can grow without breaking existing subscriptions.

**Durable, keyed by an application id.** Subscriptions live in the elector's durable
state (§4.2), are tied to no process, and survive a full cluster restart. `subscribe`
with an existing `SubId` is an idempotent **upsert**, so applications can model
subscriptions as database entities and scale to zero and back with them intact.

**API.** `subscribe/4`, `unsubscribe/2`, `unsubscribe_all/1` and `subscriptions/1` go
to the elector. Writes are asynchronous: `ok` means accepted for durable processing; the
change commits (and shows in `subscriptions/1`) shortly after. Re-subscribing recovers a
write lost before it was enqueued.

**Teardown.** A registry's durable keys share one keyspace prefix; `delete/2(Name,
Tenant)`, called after stopping the supervisor, removes them all in one range clear.
`unsubscribe_all/1` clears only the subscriptions of a running registry.

**Change detection.** As the leader applies each committed batch it tests every changed
row against each watch query: a name that starts matching is `joined`, one that stops is
`left`, and a rebind to a new pid is both. This covers register, unregister, process
death and `set_metadata`.

**Initial snapshot.** Whenever a process *enters* the notify set — at `subscribe` time
or later — it first receives the current watch set as `joined` events, then deltas. A
subscription can therefore predate its viewers.

**Leader state.** Committed subscription changes are pushed to the current leader; a
newly-assuming leader pulls the full durable set and recomputes every watch set, which
also reconciles any change it missed. Followers hold no presence state.

**Consistency.** Delivery is asynchronous and in commit order: each viewer sees an
initial snapshot, then a prefix of the true sequence of watch-set transitions. Delivered
pids are candidates (§3).

## 5. Consistency and failure model

### 5.1 Fencing: a stale leader cannot write — or serve consistent reads

When leadership moves, the new leader's identity and epoch are committed to the
database. From then on the old leader is **fenced**: its commits conflict with the
leadership record and the database rejects them. Because the record carries the epoch,
a member deposed and later re-elected cannot land a write from its earlier term either.
At most one leader can write, enforced by transaction conflict detection rather than
timing — so there is no split brain.

Consistent **reads** are fenced too. A deposed leader may not yet know it (e.g. on the
minority side of a partition), so every `*_consistent` answer is released only after
the member re-verifies its leadership (id and epoch) against the durable record. If
verification fails or cannot reach the database, it answers not-registered/empty. The
cost is one database read per consistent read.

### 5.2 Linearizable registration

All registrations flow through a single fenced leader that processes them one at a
time, so registration is **linearizable**: there is one order of decisions, and once
`register_name/2` returns `yes`, every later consistent read observes it until it
changes.

Registration is **idempotent for the same holder**: registering a name to the pid that
already holds it returns `yes`. This makes redriving safe — a caller that timed out
(§3) but whose registration committed gets `yes` on retry.

### 5.3 Partition behaviour: CP

Only the side that reaches the database and holds leadership can commit. On the other
side, registrations block and time out (§3), an unregister answers `ok` and is held
until a leader can take it (Non-goal 5), and consistent reads answer not-registered
(§5.1); local snapshot reads keep working but may be stale. Availability of writes is
traded for consistency.

In an **Erlang-distribution-only** partition (mesh severed, database reachable from
both sides) the sides briefly contend over the single leadership record; then one side
leads and the other blocks as above until the mesh heals. The leader-liveness probe
only reaps a leader on a node the prober has *never* connected to (cold-restart
recovery), so an isolated member cannot depose a healthy leader. On heal, §5.6 repairs
any name reissued across the split.

### 5.4 Uniqueness is single-fault tolerant

**Uniqueness** — at most one live process per name — holds across the failure of **any
one member at a time**. Pids are not durable, so the only record that a name is taken
is the members' in-memory replicas. Every acknowledged registration is held by at least
two members (§5.5), so losing one member never loses that knowledge; losing *both*
holders at once can, after which the name could be reissued. The registry detects and
repairs that case (§5.6).

### 5.5 Addressability and the two-holder rule

**Addressability** asks whether a registered name can still be *found* after a failure.
When `register_name/2` returns `yes`, the binding is normally held by **two members**:

- A registration **forwarded** from a follower is held by the leader and that follower.
  The follower answers `yes` only once its replica has applied the batch's commit
  version (deferring if it is mid-resync), so the second copy is always visible to the
  handoff reconstruction (§5.7).
- A registration made **on the leader's node** waits for `register_replicas` follower
  confirmations (§8) before `yes`. If none arrive within `replicate_timeout`, the
  default policy **degrades open** and answers `yes` with a single holder (emitting a
  warning and telemetry event); `strict_replication` fails it closed instead.

The rule protects the *routing entry* against losing a node that does not host the
process (typically the leader). If the process's own node dies, the process is gone and
its name is released.

### 5.6 Conflict resolution by termination

If a process held a name on a node unreachable long enough to be dropped, and the name
was reissued elsewhere, two live processes can claim the name when the node returns.
On observing two *different, live* pids for one name, the registry **terminates both**,
clears the name, and raises an alarm (a log line and, if `telemetry` is present, a
`[dgen_registry, conflict]` event). Supervised processes restart and re-register under
the single leader. Killing both, rather than picking a winner, is deliberate.

Detection runs at every **leadership handoff**, against the gathered replicas (§5.7),
and when a previously-synced member **rejoins a continuing leader** (the usual partition
heal), against the rejoiner's replica; a never-synced member is skipped. Repair needs a
*witness*: a binding no surviving replica reports cannot be detected (Non-goal 2).

**Staleness is not conflict.** A lagging member may still report a binding that was
legitimately unregistered while its process lives on. Every member therefore keeps a
**release trail** of explicit unregisters of live processes, keyed by name-and-pid, for
`conflict_release_ttl` (§8); it is merged across handoffs. Entries expire only while
the cluster is fully connected and no member has departed within the last TTL. A member
gone longer than the TTL is adjudicated with whatever trail remains.

**Kill budget.** `conflict_kill_budget` (§8) bounds terminations per name before
escalating to an operator. It is tracked by the current leader and reset on handoff, so
the bound is per name *per leader term*.

This is the registry's explicit contract:

> The registry may forcibly terminate a registered process to enforce uniqueness.
> Register only processes that can withstand being forcibly killed — for example,
> supervised and restart-safe, or transient by design.

Termination can be turned off (`terminate_on_conflict`, leaving detect-and-alarm), and
`reject_when_degraded` prevents the reissue in the first place at some cost in
availability — see [Configuration](#8-configuration).

### 5.7 Leadership handoff

A new leader rebuilds its name table by gathering every reachable member's replica in
parallel and taking the freshest. The old leader is already fenced, so no global pause
is needed. Until the gather completes, writes block (§3) and are re-driven once the new
leader assumes.

Because replicas are gap-checked prefixes of one ordered stream (§4.5), the freshest
replica holds the current binding for every name any reachable member knows, so a
two-holder binding survives any single unreachable member. A binding held *only* by an
unreachable member (a degrade-open single holder, or a multi-fault loss) is absent from
the rebuilt table and is not restored when that member returns; if the name was reissued
meanwhile, §5.6 resolves the conflict.

Two guards keep this sound when a committed broadcast from the deposed leader is still
in flight to a member during the gather:

- The gather is **fenced against the durable version key** (§4.4): no live member can be
  ahead of it, so the new leader re-gathers (bounded retries) until its freshest replica
  reaches the key. Past the bound it proceeds, and the handoff is **degraded** — it
  stopped short of the version key and may be missing committed bindings
  (`reject_when_degraded`, §8, acts on this).
- Every wholesale re-baseline (handoff snapshot or resync) is **version-monotonic**: a
  member rejects one older than what it holds, so a handoff never rolls a replica back.

The TLA+ model in [`formal/README.md`](../../formal/README.md) checks this argument.

A handoff always reaches its new leader eventually: every member periodically compares
the epoch it has heard with the committed one and re-announces itself on a mismatch.
Direct registrations awaiting replica confirmations when leadership is lost are resolved
under the replication timeout policy (§8).

## 6. Guarantees

These are normative. Behaviour that violates one of these is a defect.

1. **Singleton uniqueness (single-fault).** At most one live process is registered
   under a given name at a time, preserved across the failure of any single member.
   A simultaneous loss of both holders of a binding is outside this bound and is
   repaired reactively (§5.6), never silently tolerated.
2. **No split-brain writes.** At most one leader can commit changes. A leader that has
   lost leadership cannot register, unregister, or otherwise mutate the name table.
   There is no merge step and no dual-accept window.
3. **Linearizable registration.** Successful registrations have a single global order;
   after `register_name/2` returns `yes`, every later `whereis_name_consistent/1`
   reflects it until it changes.
4. **Two-holder durability of acknowledged registrations.** A registration acknowledged
   `yes` is held by at least two members and survives any single node failure —
   *except* under the default degrade-open policy after a replication timeout, when it
   may have one holder. `strict_replication` makes the property unconditional
   ([Configuration](#8-configuration)).
5. **Authoritative consistent reads.** `whereis_name_consistent/1` reflects the current
   leader's committed view of the name, even across an unnoticed deposition: every
   answer is fence-verified against the durable leadership record (§5.1), so a deposed
   leader answers not-registered rather than a stale value.
6. **Automatic unregistration.** When a registered process exits, its name is
   unregistered without an explicit call.
7. **CP under partition.** The side of a partition that cannot reach the database (or
   does not hold leadership) does not accept registrations; it never invents a second
   owner for a name.
8. **Registry isolation.** Distinct registries do not share leadership, membership,
   name tables, or database keys.
9. **Consistent membership and convergent connectivity.** Every node observes the same
   committed member set. Given nodes able to connect (shared cookie, network
   reachability), after a node's join commits the cluster converges so that `nodes()` on
   each member returns every other reachable member, with no external discovery. In
   `connectivity => provided_externally` mode (§4.6, §8) the registry does not connect
   nodes itself, and convergence rests on the provider registry.
10. **Metadata lifetime is tied to the registration.** A registration's `index`, `data`
    and index entries exist exactly while the registration does and are removed on
    unregister or process exit, by the same path (§4.7).
11. **Metadata writes are fenced and linearizable.** `set_metadata/2` rides the same
    fenced, single-leader commit pipeline as `register_name/2,3`; a leader that has
    lost leadership cannot apply or broadcast a metadata change.
12. **Metadata reads mirror name reads.** `get_metadata/1` is a lock-free snapshot read
    with the freshness of `whereis_name/1`; `get_metadata_consistent/1` is
    leader-authoritative, mirroring `whereis_name_consistent/1`.
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
   lose the registration and can transiently allow the name to be reissued. This is
   detected and repaired (§5.6) when a surviving replica still reports the old binding;
   with no surviving witness the displaced process simply loses its registration and is
   not terminated. It is not prevented unless `reject_when_degraded` is enabled, which
   trades availability for it.
3. **Availability during a partition.** The disconnected/minority side cannot register
   names (§5.3).
4. **Strong freshness from `whereis_name/1`.** The snapshot read may briefly lag a change
   made elsewhere. Use `whereis_name_consistent/1` for the authoritative answer.
5. **Synchronous cluster-wide unregistration.** `unregister_name/1` normally returns `ok`
   after the removal has committed on the leader, but does not wait for every node to
   observe it; replication is asynchronous. Propagation is still guaranteed: an
   unregister whose commit fails, whose leader is deposed, whose forward is dropped, or
   that arrives with no reachable leader is re-driven to the current leader rather than
   dropped.
6. **Preservation of a process the registry must terminate.** The registry may terminate
   a registered process to enforce uniqueness (§5.6). A process that is neither
   restart-safe nor transient by design is not a good fit, and its termination is by
   design.
7. **Automatic re-registration after failure.** After a multi-fault loss, detecting the
   loss and re-registering (typically via a supervisor) is the application's job.
8. **Rich queries.** Queries support only a conjunction of exact-equality clauses over
   indexed attributes — no ranges, `OR`, negation, prefix/substring matching, ordering,
   pagination, or joins (§4.7).
9. **Querying non-indexed metadata.** `data` is returned only by `get_metadata/1` /
   `get_metadata_consistent/1`; it is never matched by or used to filter a query.
10. **Durability of metadata across a full restart.** Metadata is in-memory only, like
    names (item 1); applications re-set it when they re-register.
11. **Uniqueness or constraints on indexed values.** Many registrations may share an
    indexed attribute/value pair; there is no unique-attribute constraint.
12. **Per-pid (cross-name) metadata.** Metadata is per registration: a process
    registered under several names has independent metadata for each.

## 8. Configuration

Defaults make the registry single-fault tolerant; the knobs move it along the
availability/strictness spectrum. Configuration is **per registry**, passed at start:

```erlang
dgen_registry:start_link(orders, Tenant, #{
    register_replicas => 2,
    strict_replication => true
}).
```

An unset key falls back to the `dgen` application environment (a global default), then
to the built-in default below.

| Setting | Default | Effect |
|---|---|---|
| `register_timeout` | `5000` ms | How long a leader-routed write (`register_name/2,3`, `unregister_name/1`, `set_metadata/2`) waits before the caller exits (§3). |
| `register_replicas` | `1` | Follower copies a *direct* registration waits for before `yes` (§5.5), bounded by the number of followers. Higher widens durability at the cost of latency. |
| `commit_batch_size` | `5000` | Max write ops coalesced into one group commit; the rest ride the next. Bounds per-commit work so a burst (e.g. a departing node's flood of `DOWN`s) keeps the leader responsive. Affects latency/throughput only, never correctness. |
| `replicate_timeout` | `1000` ms | How long the leader waits for those confirmations before applying the timeout policy. |
| `strict_replication` | `false` | Timeout policy. `false`: **degrade open** — answer `yes` with whatever holders exist (possibly one), with a warning and telemetry event. `true`: **fail closed** — answer `no` and retract the binding; the retraction is handed to the next leader if this one is deposed first, so the binding cannot survive. |
| `terminate_on_conflict` | `true` | Whether a detected conflict (§5.6) terminates the claimants, or only detects and alarms. |
| `conflict_kill_budget` | `{3, 60000}` | At most *N* terminations per name per window (ms) before escalating to an operator. Per name *per leader term* (§5.6). |
| `conflict_release_ttl` | `600000` ms | How long an explicitly released live name/pid pair stays in the release trail (§5.6). Expiry pauses while any member is disconnected and for one TTL after any member departs. Entries are tiny. |
| `reject_when_degraded` | `false` | Optional prevention (§5.6). When `true`, a leader whose last handoff was **degraded** (reconstruction stopped short of the durable version key, §5.7) answers `no` to registering a name it does not already hold. Merely-behind or never-synced members do not cause degradation, so normal scale-up/down does not trip it; losing a binding's only holders does. Trades availability for prevention. |
| `connectivity` | `self_managed` | Who maintains the distribution mesh (§4.6). `self_managed`: this registry runs its own. `provided_externally`: mesh off; relies on a `self_managed` registry spanning a node-superset of this one's nodes, else the node is silently isolated (a sustained no-leader window logs a warning). Registry-scoped backstops stay active. Any other value resolves to `self_managed`. |

## 9. Comparison with `global`, `gproc`, and Elixir's `Registry`

Elixir's `Registry` solves the single-node problem; `global`, `gproc` (global mode) and
`dgen_registry` solve the cluster-wide one in different ways. Each is a reasonable
trade-off.

### Elixir's `Registry` (standard library, single node)

A fast, partitioned, ETS-backed **single-node** registry with unique and duplicate keys,
via-tuple support, values and dispatch. If your naming need is local to a node it is the
natural, dependency-free choice. It does not provide one owner of a name across a
cluster, which is what `dgen_registry` is for.

### `global` (OTP, built in)

Built into OTP with no dependency: one cluster-wide namespace replicated on every node,
serialised by distributed locks. It is **AP-leaning** — both sides of a partition keep
accepting registrations, and clashes are reconciled on heal by a resolver function.
Choose it for a zero-dependency, always-available registry when you can define the
merge; choose `dgen_registry` to guarantee a single owner through a partition, if you
can run a consistent database.

### `gproc` (third-party)

A feature-rich, fully in-memory library — names, properties, counters, aggregated
counters, `ets:select`-style queries — with a fast local mode and a global mode that
elects a leader among the Erlang nodes. Its breadth is outside `dgen_registry`'s scope,
and it needs no external database. `dgen_registry` pays for that dependency with strict
consistency and dynamic membership: no node list, stale leaders fenced automatically.

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

**Good fit** when you need a strict cluster-wide singleton and prefer a name briefly
unavailable to briefly owned twice; your registered processes can withstand being
killed; you can operate a consistent database such as FoundationDB; and lookups beyond
name → pid are simple AND-equal searches (§4.7).

**Reconsider** when writes must stay available on every side of a partition, you cannot
run an external database, a registered process must never be killed by the registry, or
you need counters or a general selection API.
