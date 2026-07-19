# dgen_registry Design

`dgen_registry` is an OTP-compatible **process registry**: it lets you give a
running process a logical name and then find or message that process by name from
anywhere in an Erlang cluster. It implements the standard
`{via, dgen_registry, {RegistryName, LogicalName}}` contract, so ordinary OTP
behaviours (`gen_server`, `gen_statem`, `gen_event`, …) can be registered and
addressed by name with no special API.

Its defining choice is that it leans on a **strongly-consistent database** for
coordination. The default and reference backend is FoundationDB. That single
decision is what gives `dgen_registry` its particular personality, summarised up
front so you can decide quickly whether it fits:

- **It is a CP system.** When the network splits, the side that cannot reach the
  database stops accepting registrations rather than risk handing the same name to
  two processes. It chooses consistency over availability.
- **It is a *singleton* registry.** A logical name maps to at most one live process
  cluster-wide, and the registry will, if it must, **terminate** a process to keep
  that true. Register only processes that can withstand being forcibly killed — for
  example, supervised and restart-safe, or transient by design.
- **It stores almost nothing in the database.** Process identifiers (pids) are never
  written to durable storage. A registry of a million processes keeps roughly two
  keys for its name table — independent of how many names are registered. (The
  membership/leader-election machinery underneath keeps its own small set of keys,
  also independent of the name count; see §4.4.)

This document is the source of truth for what the registry does. The
[Guarantees](#6-guarantees) and
[Non-goals](#7-non-goals-and-explicit-non-guarantees) sections are normative:
behaviour that contradicts a guarantee is a bug, and behaviour listed as a non-goal
is intentional.

It is written to be read top to bottom. The early sections assume only that you
know what an Erlang process and a `gen_server` are; the later sections get more
precise about distributed-systems behaviour.

---

## 1. The one-minute model

Picture a cluster of Erlang nodes that all want to agree on a shared phone book:
"the name `user_service` belongs to *this* process." `dgen_registry` keeps that
phone book consistent.

- On each node there is a small **member** process that holds a local copy of the
  phone book (a `name → pid` map).
- One member is the **leader**. All changes — registering a name, unregistering it,
  cleaning up after a process dies — go through the leader, which applies them in
  order and then tells the other members.
- The leader is chosen using the database, so every node agrees on who the leader
  is, and a stale leader is automatically prevented from making changes.
- Looking up a name is usually a local, lock-free ETS read that runs in the calling
  process — fast, with no network hop and no round-trip to the member — and there is
  also a slower, always-authoritative lookup when you need it.

Everything below is an elaboration of that picture.

---

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

Run `dgen_registry:start_link/2` on **every** node that should participate. Each
call starts the local machinery and joins the node to the named registry. The
`Tenant` identifies the backend database and keyspace the registry coordinates
through; every node must point at the same one.

The direct API mirrors the via-tuple:

```erlang
dgen_registry:register_name({my_registry, user_service}, Pid),   %% -> yes | no
dgen_registry:whereis_name({my_registry, user_service}),         %% -> pid() | undefined
dgen_registry:unregister_name({my_registry, user_service}).      %% -> ok
```

A registration can also carry **metadata** — attributes that live exactly as long as
the registration and can be searched:

```erlang
dgen_registry:register_name({my_registry, user_service}, Pid,
                             #{index => #{region => us_east}, data => #{version => 3}}),
dgen_registry:get_metadata({my_registry, user_service}),   %% -> {ok, #{pid, index, data}}
dgen_registry:query(my_registry, #{region => us_east}).    %% -> [#{name, pid, index, data}, ...]
```

See [§4.7 Metadata and queries](#47-metadata-and-queries).

---

## 3. Core concepts

**Logical names.** A name is any Erlang term, scoped to a registry. `{orders, 42}`,
`"worker-7"`, and `user_service` are all valid names. Names live only in memory and
mean nothing across a full cluster restart (see [Non-goals](#7-non-goals-and-explicit-non-guarantees)).

**Registries are independent namespaces.** You can run many registries at once
(`orders`, `sessions`, …). Each has its own leader, its own membership, its own
name table, and its own slice of the database. They share nothing.

**Two kinds of read.** Looking a name up comes in two flavours, and choosing
between them is the main thing an application author decides:

| Function | Where it reads from | Speed | Freshness |
|---|---|---|---|
| `whereis_name/1` | the local member's ETS table, read in the calling process | fast, no network hop, no member round-trip | *snapshot* — may briefly lag a recent change made on another node |
| `whereis_name_consistent/1` | the current leader, with the answer **fence-verified** against the database | one network hop plus a database check | *authoritative* — reflects the leader's committed view, and a deposed leader cannot serve it (§5.1) |

`whereis_name/1` is what OTP's routing uses internally, so a `gen_server:call`
through a via-tuple takes the fast path: it does a lock-free `ets:lookup/2` against
the local member's `protected` names table directly in the caller, so many processes
can resolve names concurrently without ever queuing behind the member's mailbox.

**Registration blocks until decided; `no` is always adjudicated.**
`register_name/2` answers `yes` if the name is now yours, or `no` **only** for an
*adjudicated* refusal — the name is held by a different live process (or the opt-in
`reject_when_degraded` prevention mode, §8). It never returns `no` merely because the
registry is not ready: when there is no reachable leader (none elected yet at startup,
a brief no-leader window during a handoff, or the leader is unreachable in a
partition) the call **blocks** and is re-driven the moment a leader is established, so
it succeeds if one appears within `register_timeout`. Returning `no` there would tell
OTP's via machinery "already started" for a name that is not taken — a wrong decisive
answer.

If no leader appears in time, the call **exits** on its `register_timeout` (§8,
default 5 s) rather than inventing a verdict — the same posture `global`, `syn`, and
`gproc`'s distributed mode take; the supervisor restarts the caller with fresh state.
One consequence worth knowing: after such a timeout (or a `no` raced with a leader
failover) the name *may* be durably bound to the pid you tried to register — a retry
then answers `yes` (re-registering the same pid under the same name is idempotent,
§5.2), and if the caller instead abandons the pid, its exit clears the binding
automatically.

**Automatic cleanup.** The leader watches every registered process with
`erlang:monitor/2`. When a registered process exits, its name is unregistered
automatically — you do not need to call `unregister_name/1` yourself.

**Metadata rides along with a registration.** Beyond a bare pid, a registration can
carry an opaque data payload and a set of indexed attributes searchable with a query.
Both live exactly as long as the registration — see
[§4.7 Metadata and queries](#47-metadata-and-queries).

---

## 4. How it works

### 4.1 Three processes per node

Starting a registry on a node creates three processes under a local supervisor:

| Process | Role |
|---|---|
| **member** | Holds the local `name → pid` replica in a `protected` ETS table that it alone writes and any process reads lock-free. Serves reads. When it is the leader, it is the sole writer for the name table. |
| **elector** | Tracks which nodes are members and decides which member is the leader, coordinating through the database. |
| **connector** | Keeps Erlang distribution in step with that membership: proactively connects to every member node, reaps stranded ones, and runs the leader-liveness probe. Pure node-level connectivity plumbing, with no names/leadership state of its own. Its proactive-mesh half can be turned off per registry (`connectivity => provided_externally`, §4.6, §8) so a registry free-rides on connections another registry maintains. |

Application code only ever talks to the member (directly or through the
via-tuple). The elector and connector work behind the scenes.

### 4.2 Leadership comes from the database

The elector does not run a heartbeat or a bespoke election protocol. Membership
changes (a node joining, a node leaving) are submitted as entries on a durable
queue. Each entry is consumed and applied exactly once — the database serialises the
queue so the entries are processed one at a time, in a single global order, no matter
which node happens to consume a given one. The resulting decision (the member set and
who leads) is committed to the database, where every node reads the same answer. There
is no log for each node to replay independently and no opportunity for two nodes to
reach different conclusions.

Leadership is **sticky**: the current leader keeps the role as long as it remains a
member, which avoids needless churn when nodes come and go. If the leader departs,
the next membership change elects a replacement deterministically.

Each leadership term is stamped with an increasing **epoch** number. Messages from
an older epoch are ignored, so a message from a deposed leader cannot disturb the
current one. The epoch is also written into the durable leadership record itself,
which makes the fence (§5.1) a true monotonic fencing token rather than an identity
check: even a leader that is deposed and later *re-elected* cannot complete a write
it started in its earlier term.

### 4.3 The leader is the sole writer

Every change to the name table — `register`, `unregister`, and the automatic
unregister triggered by a monitored process dying — is funnelled to the leader.
The leader applies changes through its own `gen_server` mailbox, one at a time, so
they are totally ordered without any additional locking. A follower that receives a
registration **forwards** it to the leader and is told the verdict.

Crucially, no member ever blocks its own message loop waiting on another member. A
follower forwarding a registration does not sit and wait; it hands the request off
and replies to the caller when the answer comes back. This keeps the system free of
the circular waits that would otherwise be possible when the leader, in turn, needs
something from a follower.

### 4.4 What the database actually stores

The database's job here is **coordination, not storage**. Pids are local to a node
and to a process lifetime; a pid written down and read back later could point at an
unrelated, reused process, so pids are never persisted. The name → pid mapping lives
entirely in the members' memory — each member keeps its replica in a local ETS table
that it recreates, empty, whenever it (re)starts.

As a result the **name table** costs only about **two keys** in the database,
regardless of how many names are registered:

- a **leader key**, recording the current leader *and its epoch* (the fencing
  token, §5.1), and
- a **version key**, rewritten once per batch of committed changes with a
  monotonically increasing commit version.

The version key exists to make the leader's write a real database transaction
(so the consistency machinery in the next section engages) and to give each batch a
globally-ordered version number. Neither key grows with the number of registered
names.

Those two are the registry's *own* keys. Separately, the membership and
leader-election layer (the elector) coordinates through a durable, database-serialised
queue, and that queue keeps its own keys: a small amount of consumer/state bookkeeping
plus one entry per *in-flight* membership change (a node joining or leaving). Those
keys are bounded by the size of the cluster and its churn — they are consumed and
cleared as membership settles — and, like the name-table keys, **do not grow with the
number of registered names**. So the "two keys" figure is the name table's footprint
specifically; the whole registry's durable footprint is "two, plus the election
queue's small, name-count-independent overhead." The property that matters either way:
nothing the registry writes to the database scales with how many processes you
register.

### 4.5 Replication

After the leader applies a batch of changes it broadcasts them to the other members,
which update their local maps. Combined with the forwarding path, this is what keeps
every node's phone book current.

Each broadcast carries its batch's version *and the version of the batch before it*,
so a member applies broadcasts only when they are **contiguous** with what it already
holds. A member that missed a batch — a message dropped while it was briefly
disconnected — detects the gap, stops applying, and asks the leader for a fresh
snapshot instead of silently continuing with a hole in its replica. Every member's
replica is therefore always a *prefix* of the leader's ordered stream, which is what
makes the handoff reconstruction in §5.7 sound. The details of *how current* a
replica is, and what survives a failure, are the subject of the next section.

### 4.6 Joining, leaving, and connectivity

Membership is **dynamic and self-describing**. A node joins simply by starting the
registry; it leaves by stopping it (or by failing). There is no node list to maintain
and no separate cluster-formation step — the membership lives in the database, and
because that record is read transactionally, **every node sees the same membership**,
no matter which node happened to process a given join or leave.

The registry also keeps Erlang's distribution mesh in step with that membership. Each
member periodically — and at startup, and whenever a node becomes reachable — reads the
authoritative member set and opens an Erlang connection to every member node it is not
already connected to. So you do **not** have to wire the cluster together yourself with
a discovery library or manual `net_adm:ping`: once a node's join is committed, the
other members connect to it, and a brand-new node (which at first exists only as a row
in the database) connects outward to the members it finds there. That outbound
connection is what draws it into the cluster and triggers its first state sync.

The one thing the registry cannot do for you is make two nodes *capable* of connecting:
they must share an Erlang cookie and be able to resolve and reach each other on the
network. Given that, the registry guarantees the connections themselves — see
[Guarantees](#6-guarantees).

#### Delegating connectivity (`connectivity => provided_externally`)

The proactive mesh just described is a **node-level** concern — Erlang distribution is a
single mesh of TCP links shared by everything on the node — but each registry runs its
own copy of it. That is exactly what you want for one registry. It becomes wasteful when
a single node hosts **many** registries at once, which is a first-class use case here:
§4.8 goes to some length so that an application can start many independent registries
dynamically (one per tenant, say) without leaking atoms. On such a node, every registry's
connector independently subscribes to node events, keeps its own (identical) node-state,
runs its own mesh timer, and — most expensively — performs its own periodic **durable
`get_members` read** and its own `connect_node` sweep. Those reads and sweeps scale with
the *number of registries on the node*, not with the size of the cluster, even though the
connections they produce are one shared mesh.

`connectivity => provided_externally` lets you collapse that duplication by hand. A
registry started in this mode runs its connector with the **proactive mesh switched
off**: it opens no distribution connections and does no periodic member-set read. It
relies on the fact that distribution links are node-global — once *any* registry on the
node has connected node A to node B, every process on A can reach B, including a member of
a registry that never dialled the connection itself.

The intended shape is a **provider/consumer split**:

- One **system registry** runs in the default `self_managed` mode and is started on
  **every** node in the cluster. Its mesh converges the full distribution graph.
- Many **tenant registries** run in `provided_externally` mode and free-ride on the
  links the system registry maintains. They pay none of the mesh cost.

This is, in effect, a manually-designated node-level connectivity singleton: rather than
introduce a dedicated per-node process (which would have to hold a set of elector pids and
track their liveness — reintroducing the stale-pid problem §4.8's `one_for_all` design
eliminates), you elect an existing, always-present registry to play that role.

**The contract, and its footgun.** Delegation is only safe if the provider's member set
spans a **node-superset** of every consumer's member set — i.e. the system registry is
running on every node any tenant registry runs on. If that is violated — a tenant member
lands on a node the system registry does not cover — nothing connects that node, and it is
**silently isolated**: it cannot reach a leader, so it answers the CP write refusals of
§5.3 with no obvious cause. This is the price of the flexibility, and it is the deployer's
responsibility, not the registry's. As a cheap backstop, a `provided_externally` connector
that observes **no elected leader for a sustained window** (~60 s) logs a warning naming
this contract, so a misconfiguration surfaces in the logs rather than as a mute refusal.

**What is *not* delegated.** Only the mesh is node-level and therefore shareable. The
connector's other duties are **registry-scoped** — they read *this* registry's elector and
act on *this* registry's leader and members — and stay active in both modes:

- the **stranded-member reap** (a `nodedown` backstop that reaps a member the elector
  still lists on a departed node),
- the **leader-liveness probe** (reaps a leader recovered from durable state onto a node
  this node has never connected — the cold-restart failover, §5.3), and
- the **durable-epoch nudge** (prompts the member to re-join on a missed handoff).

These cannot be offloaded to the provider registry, which holds no handle on a tenant
registry's elector. Disabling *them* would not save shared work; it would just delete a
registry's own liveness backstops — so `connectivity` deliberately governs the mesh only.
(Because the epoch nudge normally piggybacks on the mesh's member-set read, a
`provided_externally` connector re-homes it onto a small dedicated timer.)

Any value other than `provided_externally` — including an unrecognised one — resolves to
`self_managed`, so a typo fails *safe*, toward a self-sufficient registry rather than a
silently isolated one.

### 4.7 Metadata and queries

A registration is more than a bare pid: it can carry **metadata** that lives for
exactly the registration's lifetime, in two flavours —

- **Indexed (`index`)** — a map of attributes the registry indexes, so a **query**
  can find every registration matching a set of exact-equality clauses.
- **Non-indexed (`data`)** — an opaque payload, stored and returned verbatim. It is
  never interpreted or searched; think of it as a small key/value store whose entries
  live as long as the process does.

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

**Storage.** Metadata is not a side table: each member's local replica row is
`{Name, Pid, Index, Data}` — the same ETS row `whereis_name/1` reads, widened. There is
no separate lookup for a plain `whereis_name/1`; it is exactly as cheap as before. Each
member additionally keeps an **inverted index** (`attribute → value → set of names`) in
its own process state, derived from the rows and rebuilt on a handoff — it exists purely
to make a query a few map lookups and a set intersection instead of a table scan.

**Three kinds of read, not two.** `get_metadata/1` is a third lock-free, caller-side
read alongside `whereis_name/1` — a single `ets:lookup/2` of the full row, with no
member round-trip. `get_metadata_consistent/1` is its leader-routed, authoritative
counterpart, mirroring `whereis_name_consistent/1`.

**Queries are different: they run *on* the member.** Reading many keys at once (a
query's whole point) makes a caller-side ETS read the wrong tool: the member applies a
committed batch row by row, so a lock-free reader could observe that batch
*half-applied* — some names in the batch updated, others not yet. Routing the query
through the member's own mailbox avoids this: because the member is single-threaded, it
answers a query message strictly *between* batch applications, so the result is always
a **whole-batch-consistent** snapshot — reflecting one committed batch fully, or not at
all, never a mix. This costs a message to the member (unlike the lock-free single-key
reads), which is the deliberate trade: queries are far rarer than routing lookups, and
the extra consistency is worth a mailbox hop. `query_consistent/2` applies the same
mailbox-serialised resolution, but on the leader, for an authoritative answer.

**Query semantics.** `query/2` and `query_consistent/2` take `Constraints ::
#{attr() => value()}` and return every registration whose `index` map satisfies *every*
clause (AND of exact equalities — no ranges, `OR`, negation, or ordering). An empty
`Constraints` map is rejected (`{error, empty_query}`) rather than silently meaning "all
registrations." A clause naming an attribute that no current registration's `index`
carries simply matches nothing — there is no declared schema of "indexed attributes" to
check a clause against, so an unmatched attribute yields an empty result via the same
AND-equal logic as any other non-matching clause, not a schema error. `data` is never
queryable — only `index` is; a query cannot reference it. A returned pid may already
have died by the time it is used, the same eventual-liveness caveat as any registry
lookup — treat matches as candidates, not liveness guarantees.

**How writes ride the pipeline.** Metadata introduces no new durable state and no new
fence: `register_name/3` and `set_metadata/2` are batch ops on the same
leader-as-sole-writer, fenced group-commit pipeline as `register_name/2` (§4.3–§4.4).
`set_metadata/2` on a name that is not currently registered is rejected
(`{error, not_registered}`); a leader that has lost leadership before answering a
buffered `set_metadata/2` rejects it with `{error, no_leader}` rather than silently
dropping it, so the caller knows to retry. Replication and read-after-write follow the
same shape as registration: a follower that forwarded a `set_metadata/2` is answered
only after the leader's replication broadcast has updated its own row, so a subsequent
local `get_metadata/1` on that node reflects the write.

**Lifetime and handoff.** Metadata's lifetime is exactly the registration's — the same
`DOWN`/unregister path that clears a name's row clears its metadata and index entries
with it, no separate cleanup mechanism. On a leadership handoff, metadata travels with
the binding through the same freshest-wins gather (§5.7) that reconstructs names: the
new leader's reconstructed rows carry whichever holder's metadata came with the
freshest binding, and every member rebuilds its inverted index from its reconstructed
rows. The pid-uniqueness conflict detector (§5.6) is unaffected — it adjudicates pids,
and metadata is simply carried along on the winning binding.

### 4.8 Process identity

Starting a registry creates **zero atoms**. `Name` is supplied by the caller to
`start_link/2,3` — it is application-controlled, not something `dgen_registry`
generates — and the module never derives any further atom from it: nothing else is
registered under a name. This matters for an application that starts many independent
registries dynamically — one per tenant, say — since Erlang atoms are never
garbage-collected: if `dgen_registry` derived even one additional atom per registry (an
`{elector, Name}`-style name, for instance), that would mean an unbounded,
permanently-leaked atom for every tenant the application ever creates. Whatever atom
budget an application spends on `Name` itself is its own to manage.

Concretely:

- **The supervisor** returned by `start_link/2,3` is not registered under any name;
  callers hold its pid directly (e.g. to `Supervisor.stop/2` it later).
- **The member** is registered as `Name` itself — the same atom the caller passed in,
  reused rather than combined into a derived name.
- **The member's ETS names-table** is also named `Name`. ETS `named_table` names and
  Erlang process-registration names (`erlang:register/2`) are separate namespaces, so
  the same atom names both the process and its table with no collision.
- **The elector** has no registered name at all. It is found, when needed, by walking
  from the member: reading the member's `$ancestors` process-dictionary entry (set by
  `proc_lib` at spawn time — there is no public accessor for it, so this is read
  straight out of the dictionary, the same technique tools like `recon` use) yields
  its supervisor, and `supervisor:which_children/1` on that supervisor yields the
  elector's current pid (`dgen_registry:elector_pid/1`). This is a local, in-memory
  lookup — no network hop, no registry of its own to maintain. `get_leader/1`,
  `get_epoch/1`, and `get_members/1` use it to reach the elector directly, the same
  lock-free-ish bypass-the-member's-mailbox path they always have.

**Why this lookup is deferred.** The member cannot resolve the elector inside its own
`init/1`: the supervisor starts its children synchronously, one at a time, and is still
blocked inside `supervisor:start_child/2` waiting for the member's `init/1` to return
when the member is starting — calling `supervisor:which_children/1` on it from there
would deadlock. Instead `init/1` returns `{ok, State, {continue, discover_elector}}`,
which lets the supervisor unblock immediately; `handle_continue/2` then performs the
lookup and the elector-dependent startup steps (joining, connecting the mesh) right
after, before any other message is processed.

**Why `one_for_all`.** The member resolves the elector's pid once, at startup, and
never re-resolves it. Under `one_for_one`, an elector crash restarts only the elector,
leaving every member holding a stale pid pointing at the dead process — recoverable
only by adding a monitor and a re-discovery path. The supervisor's strategy is
`one_for_all` instead: elector and member are one unit, and a crash of either restarts
both together, so the member's cached elector pid is never stale by construction. There
is no case where a live member can be holding a pid for an elector that is no longer
the one paired with it.

### 4.9 Presence: watch/notify subscriptions

Metadata queries (§4.7) answer *"who matches this right now?"* as a one-shot read. A
**presence subscription** turns that into a live feed: a caller registers a **watch
query** and a **notify query** under an application-chosen id, and the registry pushes a
message to the notify targets whenever the set of processes satisfying the watch query
changes.

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

A `query/0` is `{all, #{attr => value}}` (a conjunction of exact equalities — the
`query/2` semantics), or a bare map read as `{all, Map}`. The type is **tagged so it can
grow** (ranges, disjunction, predicates) without breaking existing subscriptions.

**Durable, keyed by an application id.** A subscription is **not** tied to any Erlang
process or to the cluster's lifetime. It is stored in the **elector's durable
`dgen_server` state** (§4.2) — the same backend-backed, cluster-shared state that holds
the member set and the leader — keyed by an application-supplied `SubId`. Because that
state survives a full cluster restart (a new elector loads the existing state rather than
re-initialising it), an application can model its presence stakeholders as database
entities: `subscribe` when an entity is created, `unsubscribe` when it is deleted, and a
`subscribe` with an existing `SubId` is an idempotent **upsert**. A system can scale to
zero and come back with exactly the subscriptions it had — the notify targets simply
resume receiving updates as they re-register. This is why subscriptions live on the
elector and not, say, as ordinary registry rows (which are pid-scoped and evaporate with
the VM): durability is the whole point.

**Manipulated only through the elector.** The public API (`subscribe/4`,
`unsubscribe/2`, `unsubscribe_all/1`, `subscriptions/1`) talks to the elector, not the
member. A write is a durable `dgen_server` **cast** (`handle_cast_tx`) — fire-and-forget
for latency, like the membership changes — so `ok` means "accepted for durable
processing", and the change commits (and `subscriptions/1`, a priority read, reflects it)
shortly after. Durability holds regardless, and because a subscription is an idempotent
upsert keyed by its id, a write lost in the narrow pre-enqueue window is recoverable by
re-subscribing. There is no way for an external caller to poke the leader's presence
state directly.

**Teardown.** Because every durable key a registry writes — the elector's state and
queue, the leader fence, the version key — lives under a single tenant keyspace prefix
derived from the registry name, the whole footprint is reclaimed by one range clear:
`delete/2(Name, Tenant)` (call it after stopping the supervisor; a live elector caches
its state in memory and would re-create the keys on its next commit). This matters for an
application that creates many short-lived, per-entity registries in a shared tenant —
without it, each deleted registry would leak its keys. `unsubscribe_all/1` is the
narrower operation that clears just the subscriptions while the registry keeps running.

**How a change is detected.** Notifications are computed on the **leader**, at the same
point a committed batch is applied to its replica (the group commit of §4.5). The batch's
durable delta already names every registration that changed; each changed name's
*post-batch* state is tested against every subscription's watch query and compared to
that subscription's previously-matching set. A name that starts matching is a `joined`,
one that stops is a `left`, and a rebind of a matching name to a different pid is a `left`
of the old plus a `joined` of the new. This makes the feed correct for **every** way the
watch set can move — a register, an unregister, an auto-unregister on process death, or a
`set_metadata` that shifts a row in or out of the query.

**Initial snapshot for every viewer, whenever it appears.** A notify target learns who is
already present via an **initial snapshot** — the current watch set delivered as a batch
of `joined` events. Crucially this fires not only at `subscribe` time but whenever a
process *enters* the notify set: the notify side is tracked symmetrically to the watch
side (`notify_matches` mirrors `sub_matches`), so a process that registers into the
notify query *after* the subscription was created still receives the full current watch
set, then deltas thereafter. This matters because a durable subscription routinely
outlives — or predates — its viewers: the subscription can be created (tied to a database
entity) before any process that will consume it exists. A viewer that is already a notify
target when the watch set changes instead gets the ordinary `joined`/`left` delta.

**How the leader gets the subscriptions.** The elector is the source of truth; the leader
holds a working copy:

- *Live change* — a `subscribe`/`unsubscribe` cast commits in the elector's consume
  transaction, whose post-commit action pushes the delta (`{presence_update, …}`,
  epoch-stamped) to the current leader, which applies it and, for a new subscription,
  seeds its watch membership and fires the initial snapshot.
- *Handoff* — a newly-assuming leader **pulls** the full durable set from its co-located
  elector (a local priority read — the elector shares the member's supervisor) at the
  moment it becomes leader, and computes each watch set against the reconstructed replica.
  Pulling at assume time (rather than trusting a set captured earlier) closes the race
  where a subscribe commits during a handoff window: anything committed before the pull is
  read, and anything after pushes a delta the now-established leader applies. This pull is
  also the backstop that reconciles any live delta a transiently unreachable leader missed
  — every leadership change re-reads the truth. A follower holds no presence state at all.

The leader's working state is three maps: `subs` (`SubId → {watch, notify}`, seeded on
assume and advanced by deltas) plus a leader-only current **watch** membership and
current **notify** membership per subscription (both recomputed on assume, advanced per
commit — the watch one drives the `joined`/`left` deltas, the notify one detects a new
viewer to send the initial snapshot to).

**Consistency.** The feed inherits the model of §5: changes are pushed in commit order,
each subscriber sees a prefix of the true sequence of watch-set transitions, and delivery
is asynchronous (a notify target on another node sees a change slightly after the leader
commits it). A viewer's first message is its initial snapshot (delivered when it enters
the notify set), and every message after that is a delta — so a viewer, from the moment
it appears, always has a complete and then continuously-maintained view of the watch set,
regardless of whether it registered before or after the subscription. As everywhere else,
a delivered `Pid` is a candidate, not a liveness guarantee: it may already have died when
the message arrives.

---

## 5. Consistency and failure model

This section states precisely what holds and what does not. It is the heart of the
document.

### 5.1 Fencing: a stale leader cannot write — or serve consistent reads

When leadership moves, the new leader's identity **and epoch** are committed to the
database. From that instant, the old leader is **fenced**: its attempts to commit a
change conflict with the leadership record and are rejected by the database. It
physically cannot register or unregister a name any more. Because the record carries
the epoch, the fence is a monotonic fencing token: a member that is deposed and later
re-elected holds a *new* epoch, so even its own in-flight writes from the earlier
term are rejected — there is no ABA window in which an old term's write can land in a
new term.

This is what rules out the classic split-brain failure where two nodes each believe
they are in charge and both hand out the same name. There is at most one leader that
can successfully write, enforced by the database's transaction conflict detection
rather than by hope or timing.

Consistent **reads** are fenced too. A deposed leader that has not yet heard about
the handoff (the minority side of a partition believes it still leads until told
otherwise) must not serve its frozen replica as authoritative — so every
`*_consistent` answer is released only after the answering member re-verifies its
leadership (id and epoch) against the durable record. A verification that fails, or
cannot reach the database, answers the not-registered/empty value instead: the same
CP refusal the write path makes. This is what lets Guarantee 5 say "authoritative"
without a timing asterisk; the cost is one database read per consistent read, which
is the deliberate price of the word.

### 5.2 Linearizable registration

Because all registrations flow through a single leader that is fenced against
impostors, and that leader processes them one at a time, registration is
**linearizable**: there is a single, well-defined order of accept/reject decisions,
and once `register_name/2` returns `yes`, every subsequent consistent read observes
that registration (until it is changed).

Registration is also **idempotent for the same holder**: registering a name to the
pid that already holds it returns `yes`, not `no`. Only a *different* live pid is
rejected. This is what makes a registration safe to redrive — a caller whose
`register_name` call timed out (§3) or raced a failover, but whose registration
actually committed, retries and gets a decisive `yes` for its own binding rather than
a `no` it would have to disambiguate.

### 5.3 Partition behaviour: CP

If the network splits, only the side that can still reach the database and holds the
leadership can commit changes. The other side cannot register or unregister names —
its writes are fenced or simply cannot reach the database.

This is the deliberate CP trade-off: during a partition the disconnected side
sacrifices **availability** (of writes) to preserve **consistency** (no two
processes share a name). Reads of the local snapshot still work on both sides, but
may be stale on the disconnected side.

One partition shape deserves its own note: an **Erlang-distribution-only**
partition, where the mesh between nodes is severed but the database stays reachable
from both sides. Both sides then observe each other's departure and briefly contend
over the single durable leadership record; the contention settles — leadership is
sticky and the record is one — after which exactly one side holds leadership and
commits, while the other side's members answer the CP refusals (`no` for writes,
not-registered for consistent reads, §5.1) and periodically re-announce themselves
until the mesh heals. The leader-liveness probe deliberately reaps only a leader on
a node the probing member has *never* been connected to (the cold-restart recovery
case), so an isolated member cannot depose a healthy leader through the shared
database. On heal, the rejoin adjudication of §5.6 repairs any name that was
reissued across the split.

### 5.4 Uniqueness is single-fault tolerant

The central guarantee is **uniqueness**: at most one live process is registered
under a given name at a time. The honest, precise statement is that this holds
across the failure of **any one member at a time**.

Why "one"? Because pids are not durable, the only record that a name is taken is the
in-memory ETS replicas of the members that know about it. The registry's replication keeps
every acknowledged registration on **at least two members** (see below), so losing
any single member never loses the knowledge that a name is taken. Losing *both*
holders simultaneously — a genuinely concurrent multi-node failure — can lose that
knowledge, after which the name could be handed out again.

The registry does not pretend this cannot happen. Instead it **detects and repairs**
it: see [Conflict resolution](#56-conflict-resolution-by-termination).

### 5.5 Addressability and the two-holder rule

"Uniqueness" is about not giving a name to two processes. **Addressability** is the
separate question of whether a name you registered can still be *found* after a
failure.

When `register_name/2` returns `yes`, the binding is held by **two members**, so it
survives any single node going down:

- A registration **forwarded** from a follower is held by both the leader and that
  follower as soon as the follower records it — two holders for free. The
  follower's ack is **version-guarded**: the leader's reply carries the batch's
  commit version, and the follower answers `yes` only once its replica has applied
  up to that version (normally already true — the replication broadcast precedes
  the reply). A follower whose replica is momentarily *gapped* (it missed a batch
  and is awaiting a resync) defers the ack until the resync lands, so the second
  holder is always visible to the handoff reconstruction in §5.7 — without the
  guard, a leader crash in that window could silently drop an acked binding whose
  only surviving copy sat at a version the freshest-wins gather ignores.
- A registration made **directly on the leader's node** is, at first, held only by
  the leader. The leader therefore waits for a follower to confirm it has a copy
  before answering `yes`.

Two honest caveats:

- **Under load the direct path can degrade.** If no follower confirms within a
  timeout, the leader by default answers `yes` anyway with a single holder, to stay
  responsive. This is configurable (see [Configuration](#8-configuration)); you can
  require more confirmations, or require the registry to fail the registration
  instead of proceeding with one holder.
- **A pid cannot outlive its own node.** If the node *hosting the process* dies, the
  process is gone regardless, and its name is released. The two-holder rule protects
  the *routing entry* against the loss of a node that does **not** host the process
  (typically the leader) — it does not resurrect a dead process.

### 5.6 Conflict resolution by termination

Because uniqueness is single-fault tolerant rather than absolute, the registry needs
a way to recover if reality ever diverges — for example, a process that held a name
on a node that was unreachable long enough to be dropped, and the name was reissued
elsewhere. When such a node returns, two different live processes could briefly claim
the same name.

When the registry observes this — two *different, live* processes for one name — it
resolves it decisively: it **terminates both** processes, clears the name, and raises
an alarm (a log line and, when the optional `telemetry` library is present, a
`[dgen_registry, conflict]` event). Supervised processes then restart and re-register
cleanly under the single leader. A per-name budget bounds how often this can fire
before the situation is escalated to an operator instead of looping.

Detection runs at both moments a divergent replica can surface. At every
**leadership-change handoff**, the gather collects every reachable member's replica
and adjudicates divergences before the new leader fans out its snapshot (§5.7). And
when a previously-synced member **rejoins a continuing leader** — the common shape
of a partition heal, where the surviving side kept its leader and no handoff
happens — the leader first gathers the rejoiner's replica (one bounded round-trip,
off its message loop) and adjudicates it against its own authoritative table before
onboarding. A fresh member (never synced) provably holds no bindings and skips the
check entirely, so a healthy scale-up pays nothing. One honest limitation either
way: repair needs a *witness* — some replica still reporting the stale binding. A
binding no surviving replica reports (a multi-fault loss, Non-goal 2) leaves nothing
to detect; its process has simply lost its registration.

Detection must not misfire on mere staleness: a member that lagged behind (or was
briefly disconnected) may still report a binding that was *legitimately* unregistered
while its process lives on — that is lag, not a conflict, and killing for it would be
wrong. The registry therefore keeps a **release trail**: every explicit unregister of
a live process is remembered — keyed by the **name-and-pid pair**, so releasing a pid
under one of its names never masks a genuine conflict on *another* name the same
process also holds — for a configurable window (`conflict_release_ttl`, §8) on
*every* member. The leader records it at commit, followers record it from the
replication broadcast, and a leadership handoff merges every reachable member's
trail into the new leader's, so the discriminator survives leadership changes. Trail
entries are only expired while the cluster is fully connected **and** no member has
departed within the last `conflict_release_ttl`: a member dropped from the set
during a partition is no longer "a disconnected member", but its stale rows can
still surface when it rejoins, and the trail must outlive that window too. (A member
gone longer than the TTL is outside the trail's protection horizon; its rejoin is
adjudicated with whatever trail remains, under the kill budget.)

One scope note on the kill budget below: it is tracked by the **current leader**
and, unlike the trail, is not merged across a handoff — a leadership change starts
the new leader with a fresh budget for every name. The `conflict_kill_budget` bound
is therefore per name *per leader term*; a conflict that regenerates across
failovers is bounded per term, not globally.

This is why the registry is described as a **singleton** registry, and why it carries
an explicit contract:

> The registry may forcibly terminate a registered process to enforce uniqueness.
> Register only processes that can withstand being forcibly killed — for example,
> supervised and restart-safe, or transient by design.

This behaviour is intentional and is **not** a bug. It can be turned off (reducing
the registry to detect-and-alarm), and there is an optional, conservative mode that
prevents the reissue in the first place at the cost of refusing some registrations
during a partition — see [Configuration](#8-configuration).

### 5.7 Leadership handoff

When the leader changes, the new leader rebuilds its name table by asking every
reachable member for its current map — the requests fan out in parallel, bounded by a
single overall timeout — and taking the most up-to-date answer. It does not read
names from the database (there are none to read). Because the previous leader is
already fenced, this handoff needs no global pause: the moment leadership commits,
the old leader can no longer interfere.

That gather is a **network** fan-out, so the new leader runs it **off its message
loop**: it answers the elector immediately and finishes assuming when the gather
returns. This is what keeps a handoff from freezing the new leader — an inline
multi-second gather would stall every request and every further membership change
queued behind it, which under heavy churn (many nodes joining and leaving) compounds
into a cluster that stops responding. During the brief window before the
reconstruction completes the new leader is not yet serving as leader, so writes on it
*block* (the no-leader path, §3) and are re-driven once it assumes — never served
against a half-built table. A newer membership change that arrives mid-gather simply
supersedes the in-flight one.

Reconstruction takes the freshest reachable member's map as the answer. Because
broadcasts are totally ordered *and every member's replica is a gap-checked prefix of
that order* (§4.5 — a member that missed a batch resyncs rather than advancing past
the hole), the freshest map already holds the current binding for every name a
*reachable* member knows, and two members reporting the same version necessarily hold
the same map — so a name that is two-holder survives any single member being
unreachable (its other holder is in the gather). A binding that
was held *only* by an unreachable member (a degrade-open single holder, or a
multi-fault loss) is absent from the rebuilt table and is **not** restored when that
member later returns — its name is treated as free, and re-registration is the
application's responsibility (§5.4). The one thing that *is* repaired automatically is
a resulting **conflict**: if such a name was reissued while the holder was away, the
two live claimants are reconciled by termination (§5.6) when the member returns.

Two subtleties make that freshest-wins claim watertight rather than merely
usual-case. A reachable member's report can be *transiently* behind an
already-committed batch still in flight to it from the deposed leader — a
broadcast cast queued but not yet processed, since Erlang gives no ordering
guarantee between messages sent by different processes (the deposed leader's
broadcast and the gather's own request are independent senders, and the
network is free to deliver either first). Left unhandled, a gather that races
ahead of that in-flight broadcast would reconstruct a map missing a binding
its second holder was about to record — and then, worse, its resulting
snapshot would overwrite that follower once the broadcast finally landed,
silently erasing the very copy that made the registration two-holder in the
first place. Two guards close this:

- The gather is **fenced against the durable version key** (§4.4): a live
  member can never be ahead of it, so the assuming leader compares its
  gathered maximum version against the version key and re-gathers — bounded,
  short retries — until it catches up, rather than finalizing on a report it
  knows is stale. Past the bound it proceeds anyway (a genuinely unreachable
  holder, not a race, is the ordinary degraded case and is backstopped by
  §5.6).
- Any wholesale re-baseline a member applies — a handoff snapshot or a resync
  answer — is **version-monotonic**: a member accepts it only if its version
  is at least as fresh as what the member already holds. A snapshot computed
  before a batch was gathered but delivered after the member independently
  applied that batch is therefore rejected rather than obeyed, so a handoff
  can never roll a member's replica backward.

Together these mean the handoff reconstruction is sound not just in the
common case where every reachable member has already drained its mailbox, but
under the interleaving where it hasn't. This is exactly the class of
correctness argument formal methods are suited to; see
[`formal/README.md`](../../formal/README.md) for the TLA+ model that checks it
(and the counterexample it produces if either guard above is disabled).

Two smaller handoff properties are worth stating. First, a handoff always reaches its
new leader eventually: the membership change and the new leadership record commit
atomically, and if the notification that follows is lost (the notifying process
crashed, or could not reach the new leader), every member periodically compares the
epoch it has heard against the committed one and re-announces itself on a mismatch —
which re-drives the handoff. Second, direct registrations that were awaiting their
replica confirmations when leadership was lost are resolved immediately under the
same policy as a replication timeout (§8), rather than being left to dangle.

---

## 6. Guarantees

These are normative. Behaviour that violates one of these is a defect.

1. **Singleton uniqueness (single-fault).** At most one live process is registered
   under a given name at a time, preserved across the failure of any single member.
   A genuine simultaneous loss of both holders of a binding is outside this bound and
   is repaired reactively (§5.6), never silently tolerated.
2. **No split-brain writes.** At most one leader can commit changes. A leader that
   has lost leadership cannot register, unregister, or otherwise mutate the name
   table. There is no merge step and no dual-accept window.
3. **Linearizable registration.** Successful registrations have a single global
   order; after `register_name/2` returns `yes`, every later
   `whereis_name_consistent/1` reflects it until it changes.
4. **Two-holder durability of acknowledged registrations.** A registration
   acknowledged `yes` is held by at least two members and survives any single node
   failure — *except* under the degrade-open replication policy (which is the
   **default**) when a replication timeout occurred, in which case it may have one
   holder. Operators who want the two-holder property unconditionally must opt into
   `strict_replication`; the policy spectrum is documented in
   [Configuration](#8-configuration).
5. **Authoritative consistent reads.** `whereis_name_consistent/1` reflects the
   current leader's committed view of the name. This holds even across an
   unnoticed deposition: every consistent answer is fence-verified against the
   durable leadership record before release (§5.1), so a deposed leader that has
   not yet heard about the handoff answers the CP refusal (not-registered) rather
   than a stale value presented as authoritative.
6. **Automatic unregistration.** When a registered process exits, its name is
   unregistered without an explicit call.
7. **CP under partition.** The side of a partition that cannot reach the database (or
   does not hold leadership) does not accept registrations; it never invents a second
   owner for a name.
8. **Registry isolation.** Distinct registries do not share leadership, membership,
   name tables, or database keys.
9. **Consistent membership and convergent connectivity.** Every node observes the same
   committed member set (the membership lives in the database and is read
   transactionally). Given nodes that are *able* to connect (shared cookie, network
   reachability), the registry establishes the Erlang connections itself: after a
   node's join is committed, the cluster converges so that `nodes()` on each member
   returns every other reachable member — without an external discovery mechanism. (It
   cannot connect nodes that are misconfigured or unreachable, and an unreachable
   member is, by definition, not in `nodes()` until it becomes reachable.) This is the
   default (`self_managed`) behaviour; a registry configured
   `connectivity => provided_externally` (§4.6, §8) deliberately delegates the
   connecting to another registry on the node and does not establish connections
   itself — the convergence guarantee then rests on that provider.
10. **Metadata lifetime is tied to the registration.** A registration's metadata (both
    `index` and `data`) and its index entries are present exactly while the
    registration is, and are removed automatically on unregister or process exit —
    the same path, no separate cleanup step (§4.7).
11. **Metadata writes are fenced and linearizable.** `set_metadata/2` rides the same
    fenced, single-leader commit pipeline as `register_name/2,3`; a leader that has
    lost leadership cannot apply or broadcast a metadata change.
12. **Metadata reads mirror name reads.** `get_metadata/1` is a lock-free, caller-side
    snapshot read with the same freshness characteristics as `whereis_name/1`;
    `get_metadata_consistent/1` is leader-authoritative, mirroring
    `whereis_name_consistent/1`.
13. **Queries are batch-consistent.** `query/2` and `query_consistent/2` never observe
    a half-applied group-commit batch — the result reflects one committed batch fully,
    or not at all (§4.7). Both are still *snapshots* (eventually consistent locally, or
    authoritative on the leader), and a returned pid may already have died.

## 7. Non-goals and explicit non-guarantees

These are intentional. Relying on the opposite is relying on something the registry
does not promise.

1. **Durability of names across a full restart.** Pids are never persisted. If every
   member of a registry is down at once, all of its names are gone; applications must
   re-register on startup. A full cluster restart starts with an empty registry.
2. **Multi-fault uniqueness.** The simultaneous loss of both holders of a binding may
   lose the registration and can, transiently, allow the name to be reissued. This is
   detected and repaired (§5.6) *when a surviving replica still reports the old
   binding*; a loss with no surviving witness leaves nothing to detect — the
   displaced process has simply lost its registration and is not terminated. It is
   not prevented — unless the optional prevention mode is enabled, which trades
   availability for it.
3. **Availability during a partition.** The disconnected/minority side cannot
   register names. This is a consequence of the CP choice, not a defect.
4. **Strong freshness from `whereis_name/1`.** The snapshot read may lag a recent
   change made elsewhere by a short replication interval. Use
   `whereis_name_consistent/1` when you need the authoritative answer.
5. **Synchronous cluster-wide unregistration.** `unregister_name/1` routes to the
   leader as a tracked call and normally returns `ok` after the removal has
   committed — but it does **not** block until every node has observed it;
   cluster-wide visibility remains eventually consistent (replication is
   asynchronous). Eventual propagation is actively driven at every hop: an
   unregister whose commit fails, whose leader is deposed mid-flight, whose forward
   was dropped, or that was accepted while no leader was reachable is stashed and
   re-driven — re-enqueued on the leader, or handed to the current leader as a
   pid-guarded clear on the next snapshot or leadership event — rather than
   silently dropped. Explicit unregisters are rare, so the tracked round-trip
   costs nothing that matters.
6. **Preservation of a process the registry must terminate.** The registry may
   terminate a registered process to enforce uniqueness (§5.6). A process that cannot
   withstand being forcibly killed — one that is neither restart-safe nor transient by
   design — is not a good fit, and its termination is by design, not a bug.
7. **Automatic re-registration after failure.** The registry does not re-register a
   process on the application's behalf after a multi-fault loss; detecting the loss
   and re-registering (typically via a supervisor) is the application's
   responsibility.
8. **Rich queries.** `query/2` and `query_consistent/2` support only a conjunction of
   exact-equality clauses over indexed attributes — no ranges, `OR`, negation,
   prefix/substring matching, ordering, pagination, or joins (§4.7).
9. **Querying non-indexed metadata.** `data` is opaque and returned only via
   `get_metadata/1` / `get_metadata_consistent/1`; it is never matched by a query, and
   there is no post-filtering of query results on it.
10. **Durability of metadata across a full restart.** Like names, metadata is
    in-memory only; a full cluster restart loses it, and applications re-set metadata
    when they re-register (same as item 1, above).
11. **Uniqueness or constraints on indexed values.** Many registrations may share the
    same indexed attribute/value pair; the index is a multimap, and the registry does
    not enforce a "unique attribute" constraint.
12. **Per-pid (cross-name) metadata.** Metadata is attached per-registration, not
    per-pid: a process registered under several names has independent metadata for
    each name.

---

## 8. Configuration

Sensible defaults make the registry single-fault tolerant out of the box; the knobs
exist to move along the availability/strictness spectrum.

Configuration is **per registry**. Each registry is started independently, and its
tuning is supplied as an options map at start time:

```erlang
dgen_registry:start_link(orders, Tenant, #{
    register_replicas => 2,
    strict_replication => true
}).
```

Because the settings are scoped to a single registry, one registry can be strict
while another degrades open — they do not share configuration. Any key left unset
falls back to the `dgen` application environment (a convenient way to set a global
default for every registry), and then to the built-in default below.

| Setting | Default | Effect |
|---|---|---|
| `register_timeout` | `5000` ms | Caller-side bound on how long `register_name/2,3` waits for its verdict before **exiting** with a call timeout (see §3 — a timeout is deliberately not converted to `no`). Unlike the other knobs this is resolved in the *calling* process, which has no handle on the per-registry options map, so it is set through the `dgen` application environment only (no per-registry override). |
| `register_replicas` | `1` | How many follower copies a *direct* registration waits for before `yes`. Bounded by the number of followers. Higher values widen durability at the cost of registration latency. |
| `commit_batch_size` | `5000` | Max write ops the leader coalesces into one group commit; the rest ride the next commit. Bounds the *inline* per-commit work (plan, replica apply, broadcast fan-out are all O(batch)), so a burst — e.g. a departing node's flood of `DOWN`s — is split across bounded commits and the leader loop stays responsive between them. Latency/throughput only, never correctness. Lower to cap the burst; raise to coalesce more aggressively. |
| `replicate_timeout` | `1000` ms | How long the leader waits for those confirmations before applying the timeout policy. |
| `strict_replication` | `false` | Timeout policy. `false`: **degrade open** — acknowledge `yes` with whatever holders exist (possibly one) to stay responsive. `true`: **fail closed** — reject the registration and retract the binding so a caller never sees an unreplicated `yes`. The retraction survives a leadership change: if the rejecting leader is deposed before its retraction commits, the pid-guarded retract is handed to the new leader, so a `no` cannot silently leave the binding alive. |
| `terminate_on_conflict` | `true` | Whether a detected uniqueness conflict (§5.6) terminates the conflicting processes, or only detects and alarms. |
| `conflict_kill_budget` | `{3, 60000}` | At most *N* terminations per name per window (ms) before escalating to an operator instead of looping. Tracked by the current leader only and not merged across a handoff, so the bound is per name *per leader term* (§5.6). |
| `conflict_release_ttl` | `600000` ms | How long an explicitly-released live name/pid pair stays in the conflict-detector trail (§5.6) — the discriminator that keeps a lagging replica from being mistaken for a conflict. Expiry is suspended while any member is disconnected *and* for one TTL after any member departs, so neither a partition nor a dropped member's rejoin within the window can outlive the trail. Entries are tiny; a generous window is cheap. |
| `reject_when_degraded` | `false` | Optional prevention (§5.6). When `true`, a leader whose last handoff could not reach every member that *could be holding bindings* refuses to register a name it does not already hold — preventing a reissue rather than repairing it. A brand-new joiner that has not yet connected (it exists only as a row in the database, §4.6) holds nothing and does not count, so a healthy scale-up does not trip this. Deliberately blunt otherwise: it cannot distinguish a partition from a deliberate scale-down, so it is off by default. |
| `connectivity` | `self_managed` | Who maintains the Erlang-distribution mesh for this registry (§4.6). `self_managed` (default): the registry runs its own proactive mesh — self-sufficient. `provided_externally`: the mesh is **off** (no `connect_node`, no periodic member-set read); the registry free-rides on links another registry maintains, for a node hosting many registries. Requires that a `self_managed` provider registry span a node-superset of this registry's nodes, or the node is silently isolated (§4.6) — a sustained no-leader window logs a warning. Governs the mesh only; the registry-scoped backstops (leader-liveness probe, stranded-member reap, durable-epoch nudge) stay active. Any value other than `provided_externally` resolves to `self_managed` (typos fail safe). |

Observability: when a direct registration degrades open, the registry emits an event
through the optional `telemetry` library (if present) and logs a warning, so the
weakening of the two-holder rule under load is visible.

---

## 9. Comparison with `global`, `gproc`, and Elixir's `Registry`

Erlang ships with a global registry, `gproc` is a widely-used third-party one, and
Elixir's `Registry` is the standard local one. They make different, reasonable
trade-offs; `dgen_registry` is not a replacement so much as a different point in the
design space. The summaries below aim to be accurate and even-handed — the right
choice depends on what you are optimising for.

A quick orientation: Elixir's `Registry` solves the *single-node* problem, while
`global`, `gproc` (global mode), and `dgen_registry` all solve the *cluster-wide*
problem in different ways.

### Elixir's `Registry` (standard library, single node)

Elixir's `Registry` is a fast, ETS-backed, **single-node** process registry. It is
not a distributed registry and is not Erlang's `global` — it does not span an Erlang
cluster; each node has its own independent `Registry`. Within that one node it is
excellent: it supports unique and duplicate keys, is partitioned internally for high
concurrency, integrates with the `{:via, Registry, …}` tuple, and adds value-style
metadata and dispatch.

The contrast with `dgen_registry` is simply **scope**. If your naming need is
local to a node, `Registry` is the natural, dependency-free choice and there is no
reason to reach for anything heavier. When you need *one* owner of a name across a
*cluster* — the problem `dgen_registry` exists for — `Registry` does not address it on
its own; cluster-wide registration is typically layered on top of a separate
distribution mechanism. `dgen_registry` provides that cluster-wide guarantee directly.

### `global` (OTP, built in)

`global` is part of the Erlang/OTP distribution and needs no external dependency. It
maintains a single cluster-wide namespace, replicated in ETS on every node, and
serialises registrations with a distributed locking protocol. During a network
partition both sides remain **available** for registration, and when the partition
heals `global` reconciles name clashes using a resolver function you can supply
(with a sensible default).

The essential contrast is the partition model. `global` is **AP-leaning**: it stays
available on both sides of a split and reconciles afterward, which is excellent when
availability matters more than never having two owners momentarily.
`dgen_registry` is **CP**: it refuses writes on the disconnected side so that two
owners never arise, accepting reduced availability during a split. If you want a
zero-dependency, always-available registry and can define how to merge after a split,
`global` is a natural fit. If you want the system to guarantee a single owner even
through a partition, and you already run (or can run) a consistent database,
`dgen_registry` is aimed at that.

### `gproc` (third-party)

`gproc` is a popular, feature-rich library. Beyond name → pid registration it offers
properties, counters, aggregated counters, and an extended `ets:select`-style
selection API, in both a very fast single-node local mode and a distributed global
mode. Its global mode elects a leader among the Erlang nodes themselves to coordinate
updates.

The contrast here is **scope, coordination source, and membership model**. `gproc`
covers far more than naming — counters, aggregated counters, and a general selection
API (match specs, guards, ranges) are outside `dgen_registry`'s scope entirely.
`dgen_registry` does have per-registration metadata and an AND-equal query over it
(§4.7), but that is a narrow, lifetime-tied convenience layered on the name table, not
a general secondary-index or counter system — if you need `gproc`'s breadth, especially
within a single node, it is hard to beat, and it runs entirely in memory with **no
external dependency or on-disk state**, a real operational advantage. Its distributed
mode coordinates by electing a leader among the participating Erlang nodes.

`dgen_registry` is narrowly a cluster name registry, and it externalises coordination
to a database rather than electing a coordinator purely within the Erlang cluster.
That externalisation is the whole point of `dgen_registry`'s consistency story, and
also its main cost: it requires a backend database to run, which `gproc` does not.
What the externalisation buys, beyond consistency, is a **dynamic membership model**:
nodes join and leave simply by starting or stopping the registry, the database
serialises the resulting membership changes, and a stale leader is fenced
automatically — so growing, shrinking, or rolling a cluster needs no pre-declared node
list and causes minimal disruption. If your priority is a self-contained, in-memory
system with the richest feature set, `gproc` is compelling; if it is a strict
cluster-wide singleton with low-friction, dynamic membership, `dgen_registry` is aimed
at that.

### At a glance

| Property | Elixir `Registry` | `global` | `gproc` (global) | `dgen_registry` |
|---|---|---|---|---|
| Cluster-wide | No (single node) | Yes | Yes | Yes |
| Ships with | Elixir stdlib | OTP | No (library) | No (library) |
| External dependency | None | None | None | A CP database backend |
| On-disk state on the BEAM nodes | None | None | None | None (durable state lives in the DB) |
| Coordination source | N/A (local) | Distributed locks | Leader among nodes | Strongly-consistent database |
| Partition stance | N/A (local) | Stays available, merges on heal | Available, leader-coordinated | Refuses minority writes (CP) |
| Membership model | Single node | Dynamic (node connections) | Leader election among nodes | Dynamic join/leave via the DB; no node list |
| Scope | Names (+ duplicate keys, dispatch) | Names | Names, properties, counters, rich queries | Names + lifetime-tied metadata |
| Metadata / secondary index | Values (no query) | No | Properties, counters, `ets:select`-style queries | `index`/`data` per registration; AND-equal query over `index` |
| Pids in durable storage | No | No | No | No |
| Authoritative read | Local (single node) | — | — | `whereis_name_consistent/1` (`get_metadata_consistent/1`, `query_consistent/2`) |
| Enforces single owner via termination | No | No | No | Yes (configurable) |
| Isolated namespaces | Yes (each `Registry`) | No (one namespace) | Via classes/scopes | Yes (independent registries) |

---

## 10. When `dgen_registry` is (and isn't) a good fit

**Good fit when** you need a strict cluster-wide singleton — exactly one live process
per name — and you would rather a name be briefly unavailable than briefly owned
twice; when your registered processes can withstand being forcibly killed (restart-safe
or transient by design); when you already operate, or are willing to operate, a
consistent database such as FoundationDB; and when any per-registration lookup beyond
plain name → pid is a simple AND-equal search over a handful of attributes (§4.7), not
a rich query workload.

**Reconsider when** you need writes to remain available on every side of a network
partition; when you cannot run an external database; when a registered process must
never be killed by the registry; or when you need counters, aggregated counters, or a
general selection API (ranges, match specs) rather than plain name → pid lookup with an
AND-equal metadata query, for which a purpose-built library is a better tool.
