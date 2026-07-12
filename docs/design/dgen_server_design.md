# dgen_server Design

`dgen_server` brings the `gen_server` programming model to a distributed system.
It uses the same callback interface — `init/1`, `handle_call/3`, `handle_cast/2`
— but unlike a `gen_server`, it is not a single Erlang process. A `dgen_server`
is a virtual entity that exists as long as its state does in the configured
backend (default: FoundationDB), independent of any running process, node, or
cluster.

Its defining choice is that state and message queue both live in a
strongly-consistent database instead of process memory. That single decision is
what gives `dgen_server` its particular personality, summarised up front so you
can decide quickly whether it fits:

- **It behaves like an ordinary `gen_server` to write.** Same callbacks, same
  return shapes, same mental model. No new programming model to learn.
- **It is durable and highly available.** State and queued messages survive the
  process, the node, and even a full cluster restart. Zero or more Erlang
  processes may act on a given `dgen_server` at any time — any number of
  consumers, on any number of nodes, can consume from the same message queue
  concurrently, with serialization and exactly-once delivery (under normal
  operation) guaranteed by the backend.
- **Callbacks run inside a database transaction and must be pure.**
  `handle_call/3`, `handle_cast/2`, and `handle_info/2` execute within a backend
  transaction (subject to its size/time limits) and must not perform side
  effects — no I/O, no messaging, nothing that isn't safely repeatable if the
  transaction retries. Side effects are pushed to a post-commit **Actions**
  list instead (§4.3).

This document is the source of truth for what `dgen_server` does. The
[Guarantees](#6-guarantees) section is normative: behaviour that contradicts a
guarantee is a bug.

It is written to be read top to bottom. The early sections assume only that you
know what a `gen_server` is; the later sections get more precise about the
transaction model and failure behaviour.

---

## 1. The one-minute model

I love `gen_server`. There are only two things stopping most of us from writing
an entire app with them:

1. **Durability** — the state is lost when the process goes down.
2. **High availability** — the functionality is unavailable when the process
   goes down.

`dgen_server` solves both by moving state and the message queue into the
backend database and decoupling them from any single process:

- A `dgen_server` has no single Erlang-process representation. It is durable
  state plus a durable, ordered message queue, both living in the database.
- Zero or more **consumer** processes — on any node — pull messages off that
  queue, run your callback, and commit the result in one database transaction:
  the state is read once, the callback runs, and the new state (plus the
  reply, if any) is written back, all atomically.
- **Standard** messages (`call`, `cast`) go through the durable queue and are
  processed with strict serializability. **Priority** messages
  (`priority_call`, `priority_cast`, `handle_info`) skip the queue and run
  immediately — an escape hatch for urgent work, at the cost of ordering.
- If a consumer crashes mid-transaction, nothing commits: the message stays in
  the queue (or is lost, for the non-durable priority paths — see
  [§5](#5-crash-and-failure-behaviour)) and is retried by the next consumer
  that picks it up.

Everything below is an elaboration of that picture.

---

## 2. Quick start

<!-- tabs-open -->

### Erlang

The simplest distributed server is just a regular `gen_server` with the
`dgen_server` behaviour:

```erlang
-module(counter).
-behavior(dgen_server).

-export([start/1, increment/1, value/1]).
-export([init/1, handle_call/3]).

start(Tenant) ->
    dgen_server:start(?MODULE, [], [{tenant, Tenant}]).

increment(Pid) ->
    dgen_server:call(Pid, increment).

value(Pid) ->
    dgen_server:call(Pid, value).

init([]) ->
    {ok, 0}.

handle_call(increment, _From, State) ->
    {reply, ok, State + 1};
handle_call(value, _From, State) ->
    {reply, State, State}.
```

Start it inside a FoundationDB directory, and the state persists across
restarts:

```erlang
Tenant = dgen_erlfdb:sandbox_open(<<"demo">>, <<"counter">>),
{ok, Pid} = counter:start(Tenant),
counter:increment(Pid),
counter:increment(Pid),
2 = counter:value(Pid),

%% Restart the process
dgen_server:stop(Pid),
{ok, Pid2} = counter:start(Tenant),
2 = counter:value(Pid2).  %% State persisted!
```

### Elixir

The simplest distributed server is just a regular `GenServer` with
`use DGen.Server`:

```elixir
defmodule Counter do
  use DGen.Server

  def start(tenant), do: DGen.Server.start(__MODULE__, [], tenant: tenant)

  def increment(pid), do: DGen.Server.cast(pid, :increment)
  def value(pid), do: DGen.Server.call(pid, :value)

  @impl true
  def init([]), do: {:ok, 0}

  @impl true
  def handle_call(:value, _from, state), do: {:reply, state, state}

  @impl true
  def handle_cast(:increment, state), do: {:noreply, state + 1}
end
```

Start it inside a FoundationDB directory, and the state persists across
restarts:

```elixir
tenant = :dgen_erlfdb.sandbox_open("demo", "counter")
{:ok, pid} = Counter.start(tenant)
Counter.increment(pid)
Counter.increment(pid)
2 = Counter.value(pid)

# Restart the process
GenServer.stop(pid)
{:ok, pid2} = Counter.start(tenant)
2 = Counter.value(pid2)  # State persisted!
```

<!-- tabs-close -->

---

## 3. Core concepts

**backend.** A `dgen` backend implementation interfaces with a database. The
provided backend is for FoundationDB using `erlfdb`. Other compatible backends
can, in principle, be contributed.

**tenant.** A pairing of a database handle and a directory handle, as provided
by the backend, that defines a subspace of the keyset partitioned for some
purpose as defined by the developer. Every `dgen_server` is started against a
tenant.

**key-tuple.** A tuple that is to be encoded into a binary for storage as a key
in a key-value pair inside a tenant subspace. Any key-tuple may be further
extended by inserting a new item at the end of the tuple; in that case the
original key-tuple becomes a prefix key-tuple, a container for other
key-values via range operations.

**tuid.** Short for tenant-unique identifier — a key-tuple that uniquely
identifies some resource in a tenant. `init/1` may return `{ok, Tuid, State}`
to choose it explicitly.

**message-queue.** Each `dgen_server` has a durable queue of messages from
which it processes requests, held entirely in the backend.

**call-request** / **cast-request.** A call-request is an operation put on the
message-queue that expects a response; a cast-request does not.

**priority-request.** A call or cast request that ignores the message-queue
and is handled immediately by a `dgen_server` process. Use with caution — this
breaks ordering guarantees with respect to the queued stream. Useful for
urgent, usually read-only work ("snapshot reads").

**dgen_server, dgen_server process, dgen_server consumer.** The `dgen_server`
itself is the abstract entity — its state is one or more key-values in the
tenant, and its functionality is defined by the callback module. It has no
singular representation in the Erlang VM. A **dgen_server process** is any
Erlang process capable of pushing requests onto the message-queue. A
**dgen_server consumer** is a dgen_server process that is *also* capable of
consuming items from the message-queue and performing the operations they
describe. There can be zero, one, or many consumers for a given `dgen_server`
at once.

---

## 4. How it works

### 4.1 Message processing paths

`dgen_server` provides different message paths with different guarantees:

| | Standard (`call`, `cast`) | Priority (`priority_call`, `priority_cast`, `handle_info`) |
|---|---|---|
| Durable queue | goes through it | skips it entirely |
| Ordering | strictly serialized with the rest of the queued stream | none — races ahead of queued messages |
| Transaction | executes within a database transaction (subject to backend transaction limits) | executes within a database transaction (subject to backend transaction limits) |
| Side effects in the callback | not permitted — callbacks must be pure with respect to external systems | not permitted — same rule |
| The lock (§4.4) | respected — paused while locked | ignored — always executes, even while locked |

Priority messages are an explicit escape hatch for urgent work that can't wait
behind the queue; standard messages are the default, ordered, durable path.

### 4.2 `consume_k` and inlining

`consume_k` (default `1`) is the maximum number of queued messages a consumer
processes per transaction — the batch size. Each consume cycle peeks up to
`consume_k` messages, processes them one at a time while carrying the durable
state in memory, and commits the whole batch in a **single** transaction (the
state is read once and written once). It then immediately re-arms itself, so a
consumer that has work keeps draining batches.

Raising `consume_k` amortises the fixed per-transaction cost (read version,
state read/write, commit) across more messages, increasing throughput when the
per-message callback work is small. The cost is larger transactions and a
coarser unit of progress — a batch is all-or-nothing, so a conflict retries
the *whole* batch. Because a busy consumer keeps draining, a larger `consume_k`
also tends to keep a **single** node as the active consumer, which is useful
when the callback's identity matters — for example an election-style callback
where you want to minimise churn in who processes successive messages.

A message can reach a callback by two routes:

1. **Through the durable queue.** `cast/2` and (normally) `call/2` enqueue the
   message; a consumer later peeks it as part of a `consume_k`-sized batch and
   processes it inside the batch's single transaction. This is the ordered,
   durable path, and the *only* path when `consume_k > 1`.
2. **Inline.** As a latency optimisation, when `consume_k =:= 1` a `call/2`
   whose queue is currently empty and unlocked is processed *immediately*, in
   the caller's own request transaction, instead of being enqueued and waited
   on. The result is identical to processing it through the queue — it just
   skips the enqueue/await round-trip.

When **`consume_k > 1`, inlining is disabled**: every `call/2` goes through
the queue and the batched consume loop, so `consume_k` is always in effect.
Use this when you want all processing funnelled through the batched,
single-consumer loop. With the default `consume_k =:= 1`, inlining is enabled
and a contention-free `call/2` is served on the fast path.

Independently of `consume_k`, `priority_call/2` and `priority_cast/2`
**always** bypass the queue (and locks) and are handled immediately, per §4.1.

### 4.3 Actions

Callbacks may return `{reply, Reply, State, Actions}` or
`{noreply, State, Actions}`, where `Actions` is a list of 1-arity functions.
These functions:

- Execute after the transaction commits.
- Receive the committed `State` as their argument, but cannot modify it.
- Are the correct place for side effects: logging, telemetry, publishing to
  external systems.
- Can return `halt` to stop processing actions, or any other value to
  continue.

Under batched consumption (`consume_k > 1`), all of a batch's actions run
after the batch's single commit, and **each** receives the state as of the
**end of the batch** — not the state at the specific message that produced
the action.

### 4.4 Locking

A callback may return `{lock, State}` instead of `{noreply, State}` or
`{reply, ...}` to enter locked mode. This is useful when a state change
requires synchronous post-commit coordination before the next queue message
can be safely processed — long-running operations that would exceed
transaction time limits, such as calling external APIs or performing extended
computations.

When locked:

- Standard `call` and `cast` messages are queued but not processed.
- Priority messages and `handle_info` continue to execute (§4.1).
- The `handle_locked/4` callback is invoked **outside** a transaction:
  - Not subject to backend transaction limits.
  - Side effects are permitted.
  - Can modify state, which is written back to the database.

`lock_timeout` (default `infinity`) bounds how long a lock may be held before
other consumers treat it as stale. When a callback returns `{lock, State}`,
`dgen_server` sets a timestamped lock key in the backend that pauses other
consumers while `handle_locked/4` runs. Under normal operation the lock holder
clears the key itself before `handle_locked` returns. If the holder is killed
(SIGKILL, VM abort) the lock persists until another consumer detects
staleness: it re-checks `lock_timeout` ms after the lock was set and clears it
if that deadline has passed. `infinity` (the default) disables stale-lock
detection entirely — a dead holder will permanently block all consumers. Set
this to a value safely larger than the worst-case `handle_locked` duration for
your callback.

See §4.7 for the exact commit/coordinate/release sequence.

### 4.5 Persisted state

State is persisted to the key-value store using a structured encoding scheme
that optimises for partial updates. Three encoding types are supported:

1. **Assigns map**: maps with all atom keys are split across separate keys,
   one per entry. No ordering guarantees.

        #{
            mykey => <<"my value">>,
            otherkey => 42
        }

2. **Components list**: lists where every item is a map with an atom `id` key
   containing a binary value. Each item is stored separately with ordering
   maintained via fractional indexing in the storage key.

        [
            #{id => "item1", value => 1},
            #{id => "item2", value => 2}
        ]

3. **Term**: all other terms use `term_to_binary` and are chunked into 100KB
   values.

        {this, is <<"some">>, term, 4.5, %{3 => 2}}

4. **Nesting**: the encoder handles nested structures recursively. For
   example, an assigns map containing a components list nests both encodings
   in the key path.

        #{
            mykey => <<"my value">>,
            mylist => [
                #{id => "item1", value => 1},
                #{id => "item2", value => 2}
            ]
        }

When writing updates, diffs are generated by comparing old and new state:

- **Assigns map**: only changed entries are written; removed entries are
  cleared.
- **Components list**: only changed items are written; ordering changes
  update fractional indices.
- **Term**: full rewrite (no diffing).

If the encoding type changes between updates, the old keys are cleared and the
new encoding is written in full.

### 4.6 Caching

Each consumer process can maintain an in-memory cache of the state paired with
its versionstamp. On subsequent messages, if the cached versionstamp matches
the current database version, the state is reused without a read operation.
This eliminates redundant reads when processing multiple messages in
sequence.

The cache is invalidated when the process detects that another consumer has
modified the state.

### 4.7 Request flows

**Call request flow:**

1. Some Erlang process calls `dgen_server:call/3` (the calling entity).
2. A call-request is pushed onto the message-queue, along with the from-key.
3. The from-key and watch are returned to the calling entity.
4. One of the dgen_server consumers consumes the call-request.
5. The consumer retrieves the state.
6. The consumer calls the `handle_call/3` function on the module that
   implements the dgen_server behaviour.
7. The callback returns a new state and an optional list of side-effects.
8. The consumer updates the state.
9. The consumer checks whether the reply-sentinel-key still exists (the
   caller may have timed out and cleared it). If present, the consumer writes
   `{reply, Reply}` as a chunked term under the from-key; if absent, the write
   is skipped.
10. The consumer commits the transaction.
11. (concurrent with 12) The consumer executes the side-effects.
12. (concurrent with 11) The calling entity receives the watch notification,
    reads the chunked reply via `get_range`, and clears the reply keys.

On timeout, the calling entity clears the reply keys to prevent durable key
leaks. The callback still runs and state still mutates (just like
`gen_server` — a timed-out call still executes), only the orphan reply key is
eliminated.

**Cast request flow:**

1. A cast-request is pushed onto the message-queue.
2. One of the dgen_server consumers consumes the cast-request.
3. The consumer retrieves the state.
4. The consumer calls the `handle_cast/2` function on the module that
   implements the dgen_server behaviour.
5. The callback returns a new state and an optional list of side-effects.
6. The consumer updates the state.
7. The consumer commits the transaction.
8. The consumer executes the side-effects.

**Lock flow:**

1. The callback returns `{lock, NewState}`.
2. The consumer commits `NewState` to the DB **and** writes a lock key
   (`{Tuid, <<"k">>}`) in the same transaction.
3. Any other consumer that tries to consume from the queue reads
   `is_locked = true` and parks itself on a DB watch instead of processing.
4. The consumer that set the lock calls `handle_locked/4` with the **same**
   event type and message that triggered the lock, plus the committed
   `NewState`. This is the synchronous coordination window.
5. After `handle_locked/4` returns (regardless of its return value), the lock
   key is cleared and the queue watch is notified in an `after` block —
   always, even on exception.
6. Parked consumers wake up, see `is_locked = false`, and resume normal queue
   consumption.

The lock therefore guarantees that between committing a state change and
resuming queue consumption, one designated node performs a synchronous
coordination step with no risk of another node racing ahead.

### 4.8 Internal terms

Reference material for reading the implementation:

- **key-bin**: a key-tuple encoded into a binary using the tenant subspace.
- **waiting-key**: a prefix key-tuple that contains all entities waiting on
  the result of some call-request.
- **from-key**: a key-tuple that uniquely identifies a single entity waiting
  on the result of some call-request. Includes a system-time timestamp
  (seconds) so abandoned call keys can be garbage-collected using a
  time-based heuristic. Structure: `{WaitingKey..., Timestamp,
  term_to_binary(Ref)}`.
- **reply-sentinel-key**: the first chunk key of the reply term under the
  from-key. The reply is stored using chunked term encoding
  (`{From, <<"t">>, 0}`, `{From, <<"t">>, 1}`, ...) so replies can exceed the
  backend's single-value size limit. The backend watch is placed on the
  reply-sentinel-key (chunk 0). The client reads the reply via `get_range` and
  clears it via `clear_range`.
- **quid**: unique tuple identifier for the queue. It is a prefix key-tuple
  that contains all key-values for the message-queue.
- **item-key**: a key-tuple that identifies an item in the queue (a
  call-request or cast-request).
- **push-key** / **pop-key**: key-tuples tracking the number of pushes onto,
  and pops from, the queue.

---

## 5. Crash and failure behaviour

**Key guarantee:** standard `call` and `cast` messages are processed
**exactly-once** under normal operation. In the event of a crash before the
transaction commits, the message is retried — so in crash scenarios, delivery
is **at-least-once**. When `dead_letter_threshold` is set, retries are
bounded by that limit. Design your callbacks to be idempotent when possible.

**During `init/1`:**

- If the first `init/1` crashes, the gen_server process exits before any
  state is persisted.
- When restarted, `init/1` runs again from scratch.
- No durable state exists yet, so there's nothing to recover.

**During transactional callbacks (`handle_call`, `handle_cast`,
`handle_info`):**

- The database transaction is automatically aborted — no state changes are
  committed.
- For `call` and `cast`: the message remains in the durable queue and will be
  retried by the next consumer. If `dead_letter_threshold` is set, each
  failed attempt increments a counter embedded in the message envelope; once
  the counter reaches the threshold, the message is moved to the dead-letter
  queue instead of being retried (see below).
- For `priority_call` and `priority_cast`: the message is lost (it never
  entered the queue).
- For `handle_info`: the Erlang message is lost (info messages are not
  durable).
- State remains unchanged from before the callback was invoked.

**Dead-letter queue:**

A *poison message* is a queue message that consistently crashes consumers. To
enable bounded retries, set `dead_letter_threshold` to a positive integer.
Each message envelope carries an attempt counter; when the counter reaches
the threshold, the message is moved to the dead-letter queue (DLQ) in
FoundationDB rather than being retried:

- For `call` messages, the blocked caller raises `{dead_letter, N}` where `N`
  is the attempt count.
- The optional `handle_dead_letter/2` callback is invoked with the original
  message and attempt count, if defined.
- A warning is logged.

Dead-lettering is **disabled by default** (`dead_letter_threshold: infinity`).
Enable it with the start option:

<!-- tabs-open -->

### Erlang

```erlang
dgen_server:start(MyMod, [], [{tenant, Tenant}, {dead_letter_threshold, 3}])
```

### Elixir

```elixir
DGen.Server.start_link(MyMod, [], tenant: tenant, dead_letter_threshold: 3)
```

<!-- tabs-close -->

**Coordinating with the supervisor's restart intensity:**

Each consumer crash counts as one restart from the supervisor's perspective.
OTP supervisors enforce a *restart intensity* — a maximum number of restarts
allowed within a sliding time window (`max_restarts` / `max_seconds`,
defaulting to `3` restarts in `5` seconds). If the supervisor reaches this
limit before `dead_letter_threshold` is hit, the supervisor itself terminates
rather than the message being dead-lettered.

To ensure dead-lettering takes effect, configure the supervisor so that it
tolerates at least `dead_letter_threshold` restarts within the window. A
practical approach is to set `max_restarts >= dead_letter_threshold` with a
`max_seconds` value long enough to cover the expected crash-restart cycle
time:

<!-- tabs-open -->

### Erlang

```erlang
%% Allow up to 5 restarts in 60 seconds — enough headroom for a threshold of 3.
{ok, _} = supervisor:start_link({local, my_sup}, my_sup, []),

%% In the supervisor's init/1:
SupFlags = #{strategy => one_for_one, intensity => 5, period => 60},
```

### Elixir

```elixir
# Allow up to 5 restarts in 60 seconds — enough headroom for a threshold of 3.
Supervisor.start_link(children,
  strategy: :one_for_one,
  max_restarts: 5,
  max_seconds: 60
)
```

<!-- tabs-close -->

With `dead_letter_threshold: infinity` (the default), poison messages produce
an unbounded crash loop. The supervisor will eventually exhaust its restart
intensity and terminate, which is standard OTP crash-loop behavior. Set a
finite threshold to bound the loop and keep the supervisor alive.

**Inspecting and managing the dead-letter queue:**

Dead-lettered messages accumulate in FoundationDB and are not automatically
cleared. An operator can inspect and manage them from a remote shell using
functions in `dgen_queue`. The `Quid` for a server is obtained with
`dgen_server:get_quid/1` (Erlang) or `:dgen_server.get_quid/1` (Elixir),
passing the `tuid` the server was started with.

<!-- tabs-open -->

### Erlang

```erlang
Quid = dgen_server:get_quid(Tuid),

%% Inspect — returns [{Key, Envelope, AttemptCount, TimestampMs}]
Entries = dgen_queue:peek_dlq(Tenant, Quid),

%% Count without decoding
dgen_queue:dlq_length(Tenant, Quid),

%% Requeue one entry (resets attempt count to 0, atomic with DLQ delete)
{Key, _Envelope, _N, _Ts} = hd(Entries),
dgen_queue:requeue_dlq_entry(Tenant, Quid, Key),  %% ok | {error, not_found}

%% Discard one entry permanently
dgen_queue:delete_dlq_entry(Tenant, Key),

%% Discard all entries for the queue
dgen_queue:purge_dlq(Tenant, Quid).
```

### Elixir

```elixir
quid = :dgen_server.get_quid(tuid)

# Inspect — returns [{key, envelope, attempt_count, timestamp_ms}]
entries = :dgen_queue.peek_dlq(tenant, quid)

# Count without decoding
:dgen_queue.dlq_length(tenant, quid)

# Requeue one entry (resets attempt count to 0, atomic with DLQ delete)
{key, _envelope, _n, _ts} = hd(entries)
:dgen_queue.requeue_dlq_entry(tenant, quid, key)  # :ok | {:error, :not_found}

# Discard one entry permanently
:dgen_queue.delete_dlq_entry(tenant, key)

# Discard all entries for the queue
:dgen_queue.purge_dlq(tenant, quid)
```

<!-- tabs-close -->

`requeue_dlq_entry/3` is atomic: it pushes the envelope back onto the main
queue with the attempt count reset to zero and deletes the DLQ entry in a
single FDB transaction. If the root cause of the crashes has been fixed and
the server has been redeployed, requeueing allows the message to be retried
from a clean state.

**During `handle_locked`:**

- `handle_locked` executes outside a transaction, so previous state changes
  have already been persisted.
- If the crash is an Erlang/Elixir throw, then the lock is cleared before the
  process exits.
- If the crash is a system disruption such as SegFault, OOM, or sudden power
  loss, the lock is not cleared and the dgen_server is deadlocked. Manual
  intervention is required to clear the lock.
- In either case, the triggering message has been consumed from the queue, so
  it will not be retried.

**During action execution:**

- Actions run after the transaction commits, so state changes are already
  persisted.
- If an action crashes, the state update succeeds but remaining actions are
  not executed.
- The message has been consumed from the queue and will not be retried.

**Supervisor restart:**

- When a dgen_server is restarted by a supervisor, it reads existing state
  from the database.
- If state exists, `init/1` is called, but the initial state is ignored. The
  server resumes with the persisted state.
- The process immediately begins consuming any queued messages, if it's
  configured to do so.
- Multiple processes can safely consume from the same queue; they coordinate
  via database transactions.

---

## 6. Guarantees

These are normative. Behaviour that violates one of these is a defect.

1. **Durability.** State and queued messages persist in the backend,
   independent of the lifetime of any Erlang process, node, or cluster.
2. **Delivery.** Standard `call`/`cast` messages are processed exactly-once
   under normal operation. If a consumer crashes before its transaction
   commits, the message is retried by another consumer — at-least-once
   delivery in that scenario, bounded by `dead_letter_threshold` when
   configured (§5).
3. **Serializability.** Standard messages are committed with strict
   serializability via the durable queue: the state read and write for a
   given batch happen inside one transaction.
4. **Priority messages trade ordering for immediacy.** `priority_call/2`,
   `priority_cast/2`, and `handle_info/2` always bypass the queue and the
   lock, so they are not ordered with respect to the queued stream.
5. **Actions run exactly once per committed batch,** strictly after that
   batch's commit, and only once the state they reference is durable.
6. **Lock exclusivity.** While a `dgen_server` is locked, only the lock
   holder's `handle_locked/4`, priority messages, and `handle_info/2`
   execute; other standard `call`/`cast` messages wait until the lock clears.
7. **Callback purity.** `handle_call/3`, `handle_cast/2`, and
   `handle_info/2` execute inside a database transaction and must not
   perform side effects — those belong in the `Actions` list (§4.3) or in
   `handle_locked/4` (§4.4), the two paths that run outside a transaction.

---

## 7. Configuration

Options passed via the `Opts` proplist to `start/3`, `start/4`, `start_link/3`,
or `start_link/4`:

| Option | Default | Meaning |
|---|---|---|
| `tenant` | — (required) | `{DbHandle, Dir}` pair identifying the backend subspace. |
| `consume` | `true` | Whether this process consumes messages from the queue, or only publishes them. |
| `reset` | `false` | When `true`, re-initialise the durable state even if it already exists. |
| `dead_letter_threshold` | `infinity` | Number of consecutive processing failures before a message is moved to the dead-letter queue instead of retried (§5). |
| `consume_k` | `1` | Maximum number of queued messages processed per transaction (§4.2). |
| `lock_timeout` | `infinity` | Maximum milliseconds a distributed lock may be held before other consumers treat it as stale (§4.4). |

---

## 8. When `dgen_server` is (and isn't) a good fit

**Good fit when** you want the plain `gen_server` programming model but need
the state — and any in-flight messages — to survive process, node, or cluster
death; when a database round-trip per message is an acceptable cost for that
durability and multi-node availability; and when your side effects can be
deferred to the post-commit `Actions` list, or performed inside the atomic
`handle_locked/4` window, rather than issued inline during a callback.

**Reconsider when** you need sub-millisecond, purely in-memory latency with no
network or database hop — a plain `gen_server` is faster for state that
doesn't need to survive a restart; when your callback needs to perform
ordinary, non-deferrable side effects directly inside `handle_call`/
`handle_cast` (`dgen_server` requires those to go through `Actions` or
`handle_locked/4`, since callbacks themselves run inside a database
transaction); or when you cannot run, or do not want to operate, a
strongly-consistent external database.
