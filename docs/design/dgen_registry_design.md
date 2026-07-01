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
| `whereis_name_consistent/1` | the current leader | one network hop | *authoritative* — reflects the leader's committed view |

`whereis_name/1` is what OTP's routing uses internally, so a `gen_server:call`
through a via-tuple takes the fast path: it does a lock-free `ets:lookup/2` against
the local member's `protected` names table directly in the caller, so many processes
can resolve names concurrently without ever queuing behind the member's mailbox.

**Registration returns a verdict.** `register_name/2` answers `yes` if the name is
now yours, or `no` if it is already taken (or if the registry currently has no
leader to adjudicate). It never blocks indefinitely on the result.

**Automatic cleanup.** The leader watches every registered process with
`erlang:monitor/2`. When a registered process exits, its name is unregistered
automatically — you do not need to call `unregister_name/1` yourself.

**Metadata rides along with a registration.** Beyond a bare pid, a registration can
carry an opaque data payload and a set of indexed attributes searchable with a query.
Both live exactly as long as the registration — see
[§4.7 Metadata and queries](#47-metadata-and-queries).

---

## 4. How it works

### 4.1 Two processes per node

Starting a registry on a node creates two processes under a local supervisor:

| Process | Role |
|---|---|
| **member** | Holds the local `name → pid` replica in a `protected` ETS table that it alone writes and any process reads lock-free. Serves reads. When it is the leader, it is the sole writer for the name table. |
| **elector** | Tracks which nodes are members and decides which member is the leader, coordinating through the database. |

Application code only ever talks to the member (directly or through the
via-tuple). The elector works behind the scenes.

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
current one.

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

- a **leader key**, recording the current leader, and
- a **version counter**, bumped once per batch of committed changes.

The version counter exists to make the leader's write a real database transaction
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
every node's phone book current. The details of *how current*, and what survives a
failure, are the subject of the next section.

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

---

## 5. Consistency and failure model

This section states precisely what holds and what does not. It is the heart of the
document.

### 5.1 Fencing: a stale leader cannot write

When leadership moves, the new leader's identity is committed to the database. From
that instant, the old leader is **fenced**: its attempts to commit a change conflict
with the leadership record and are rejected by the database. It physically cannot
register or unregister a name any more.

This is what rules out the classic split-brain failure where two nodes each believe
they are in charge and both hand out the same name. There is at most one leader that
can successfully write, enforced by the database's transaction conflict detection
rather than by hope or timing.

### 5.2 Linearizable registration

Because all registrations flow through a single leader that is fenced against
impostors, and that leader processes them one at a time, registration is
**linearizable**: there is a single, well-defined order of accept/reject decisions,
and once `register_name/2` returns `yes`, every subsequent consistent read observes
that registration (until it is changed).

### 5.3 Partition behaviour: CP

If the network splits, only the side that can still reach the database and holds the
leadership can commit changes. The other side cannot register or unregister names —
its writes are fenced or simply cannot reach the database.

This is the deliberate CP trade-off: during a partition the disconnected side
sacrifices **availability** (of writes) to preserve **consistency** (no two
processes share a name). Reads of the local snapshot still work on both sides, but
may be stale on the disconnected side.

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
  follower as soon as the follower records it — two holders for free.
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
an alarm. Supervised processes then restart and re-register cleanly under the single
leader. A per-name budget bounds how often this can fire before the situation is
escalated to an operator instead of looping.

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
reachable member for its current map and taking the most up-to-date answer. It does
not read names from the database (there are none to read). Because the previous
leader is already fenced, this handoff needs no global pause: the moment leadership
commits, the old leader can no longer interfere.

Reconstruction takes the freshest reachable member's map as the answer. Because
broadcasts are totally ordered, that one map already holds the current binding for
every name a *reachable* member knows — so a name that is two-holder survives any
single member being unreachable (its other holder is in the gather). A binding that
was held *only* by an unreachable member (a degrade-open single holder, or a
multi-fault loss) is absent from the rebuilt table and is **not** restored when that
member later returns — its name is treated as free, and re-registration is the
application's responsibility (§5.4). The one thing that *is* repaired automatically is
a resulting **conflict**: if such a name was reissued while the holder was away, the
two live claimants are reconciled by termination (§5.6) when the member returns.

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
   failure — *except* when the operator has allowed degrade-open and a replication
   timeout occurred, in which case it may have one holder. The default policy and the
   stricter alternatives are documented in [Configuration](#8-configuration).
5. **Authoritative consistent reads.** `whereis_name_consistent/1` reflects the
   current leader's committed view of the name.
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
   member is, by definition, not in `nodes()` until it becomes reachable.)
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
   detected and repaired (§5.6), not prevented — unless the optional prevention mode
   is enabled, which trades availability for it.
3. **Availability during a partition.** The disconnected/minority side cannot
   register names. This is a consequence of the CP choice, not a defect.
4. **Strong freshness from `whereis_name/1`.** The snapshot read may lag a recent
   change made elsewhere by a short replication interval. Use
   `whereis_name_consistent/1` when you need the authoritative answer.
5. **Synchronous unregistration semantics.** `unregister_name/1` is best-effort and
   eventually consistent; it does not block until every node has observed the removal.
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
| `register_replicas` | `1` | How many follower copies a *direct* registration waits for before `yes`. Bounded by the number of followers. Higher values widen durability at the cost of registration latency. |
| `replicate_timeout` | `1000` ms | How long the leader waits for those confirmations before applying the timeout policy. |
| `strict_replication` | `false` | Timeout policy. `false`: **degrade open** — acknowledge `yes` with whatever holders exist (possibly one) to stay responsive. `true`: **fail closed** — reject the registration and retract the binding so a caller never sees an unreplicated `yes`. |
| `terminate_on_conflict` | `true` | Whether a detected uniqueness conflict (§5.6) terminates the conflicting processes, or only detects and alarms. |
| `conflict_kill_budget` | `{3, 60000}` | At most *N* terminations per name per window (ms) before escalating to an operator instead of looping. |
| `reject_when_degraded` | `false` | Optional prevention (§5.6). When `true`, a leader whose last handoff could not reach every member refuses to register a name it does not already hold — preventing a reissue rather than repairing it. Deliberately blunt: it cannot distinguish a partition from a deliberate scale-down, so it is off by default. |

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
