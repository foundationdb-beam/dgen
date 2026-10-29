# dgen_server Design

`dgen_server` brings the `gen_server` programming model (`init/1`,
`handle_call/3`, `handle_cast/2`, ...) to a distributed system. A `dgen_server`
is not an Erlang process: it is durable state plus a durable message queue in a
strongly-consistent backend (default: FoundationDB), and it exists as long as
that state does, independent of any process, node, or cluster.

The [Guarantees](#6-guarantees) section is normative: behaviour that contradicts
it is a bug.

**This document is LLM generated.**

---

## 1. The one-minute model

- Zero or more **consumer** processes, on any node, pull messages off the queue,
  run your callback, and commit the new state (and reply, if any) in one
  database transaction.
- **Standard** messages (`call`, `cast`) go through the queue and are processed
  with strict serializability. **Priority** messages (`priority_call`,
  `priority_cast`, `handle_info`) skip the queue and run immediately, at the
  cost of ordering.
- Callbacks run inside that transaction and must be pure; side effects go in
  post-commit **Actions** (§4.3) or `handle_locked/4` (§4.4).
- If a consumer crashes mid-transaction nothing commits; a queued message is
  retried by the next consumer (priority messages are lost, §5).

---

## 2. Quick start

<!-- tabs-open -->

### Erlang

```erlang
-module(counter).
-behavior(dgen_server).

-export([start/1, increment/1, value/1]).
-export([init/1, handle_call/3]).

start(Tenant) -> dgen_server:start(?MODULE, [], [{tenant, Tenant}]).
increment(Pid) -> dgen_server:call(Pid, increment).
value(Pid) -> dgen_server:call(Pid, value).

init([]) -> {ok, 0}.

handle_call(increment, _From, State) -> {reply, ok, State + 1};
handle_call(value, _From, State) -> {reply, State, State}.
```

```erlang
Tenant = dgen_erlfdb:sandbox_open(<<"demo">>, <<"counter">>),
{ok, Pid} = counter:start(Tenant),
counter:increment(Pid), counter:increment(Pid),
dgen_server:stop(Pid),
{ok, Pid2} = counter:start(Tenant),
2 = counter:value(Pid2).  %% State persisted
```

### Elixir

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

```elixir
tenant = :dgen_erlfdb.sandbox_open("demo", "counter")
{:ok, pid} = Counter.start(tenant)
Counter.increment(pid); Counter.increment(pid)
GenServer.stop(pid)
{:ok, pid2} = Counter.start(tenant)
2 = Counter.value(pid2)  # State persisted
```

<!-- tabs-close -->

---

## 3. Core concepts

- **backend**: the database interface; the provided one is FoundationDB via
  `erlfdb`.
- **tenant**: a database handle plus a directory handle defining a key
  subspace. Every `dgen_server` is started against one.
- **key-tuple**: a tuple encoded as a key in a tenant; extending it makes the
  original a prefix containing other keys.
- **tuid**: the key-tuple identifying a `dgen_server` in its tenant. `init/1`
  may return `{ok, Tuid, State}` to choose it.
- **message-queue**: the durable queue a `dgen_server` processes requests from.
- **call-request / cast-request**: a queued operation that does / does not
  expect a response.
- **priority-request**: a call or cast handled immediately, ignoring the queue
  and its ordering; for urgent, usually read-only work.
- **dgen_server process / consumer**: any process that can push requests onto
  the queue / one that also processes them. A `dgen_server` may have zero, one,
  or many consumers.

---

## 4. How it works

### 4.1 Message processing paths

| | Standard (`call`, `cast`) | Priority (`priority_call`, `priority_cast`, `handle_info`) |
|---|---|---|
| Durable queue | goes through it | skips it |
| Ordering | strictly serialized with the queued stream | none; races ahead of queued messages |
| Transaction | runs inside one (backend limits apply) | runs inside one (backend limits apply) |
| Side effects in the callback | not permitted | not permitted |
| The lock (§4.4) | respected; paused while locked | ignored; always executes |

### 4.2 `consume_k` and inlining

`consume_k` (default `1`) is the maximum number of queued messages a consumer
processes per transaction. A consume cycle peeks up to `consume_k` messages,
processes them in order with the state carried in memory, commits the batch in
**one** transaction (state read once, written once), and immediately re-arms.

A larger `consume_k` amortises per-transaction cost and raises throughput when
callbacks are cheap, at the price of larger transactions and all-or-nothing
batches (a conflict retries the whole batch). A busy consumer keeps draining,
so it also tends to keep a **single** node as the active consumer, which helps
when consumer identity matters (e.g. election-style callbacks).

**Inlining.** When `consume_k =:= 1`, a `call/2` whose queue is empty and
unlocked is processed immediately in the caller's own transaction, with the
same result as going through the queue. When **`consume_k > 1`, inlining is
disabled** and every `call/2` goes through the queue.

`priority_call/2` and `priority_cast/2` always bypass the queue and the lock
(§4.1).

### 4.3 Actions

Callbacks may return `{reply, Reply, State, Actions}` or
`{noreply, State, Actions}`, where `Actions` is a list of 1-arity functions
that:

- run after the transaction commits;
- receive the committed `State` but cannot modify it;
- are the place for side effects (logging, telemetry, external publishing);
- may return `halt` to stop running the remaining actions.

With `consume_k > 1`, a batch's actions all run after its single commit and
each receives the state as of the **end of the batch**.

### 4.4 Locking

A callback may return `{lock, State}` to enter locked mode, for work that must
finish before the next queued message is processed but cannot run inside a
transaction (external API calls, long computations).

While locked:

- Standard `call` and `cast` messages are queued but not processed.
- Priority messages and `handle_info` still execute (§4.1).
- `handle_locked/4` runs **outside** a transaction: no transaction limits, side
  effects permitted, and the state it returns is written back.

`lock_timeout` (default `infinity`) bounds how long a lock may be held. The
holder normally clears the lock when `handle_locked/4` returns. If the holder
is killed (SIGKILL, VM abort), another consumer clears the lock once
`lock_timeout` ms have passed since it was set. With `infinity`, a dead holder
blocks all consumers permanently. Set it safely above the worst-case
`handle_locked/4` duration.

Clearing a stale lock is a guess that the holder is dead, so the lock is
fenced (§4.7): a busted holder's result is not committed, its message is
requeued, and it exits with `{lock_fenced, requeued}`. (A priority or inline
lock has no queued message: it exits with `{lock_fenced, not_requeued}`, and the
caller receives the exit.) Its side effects have already happened, so
**`handle_locked/4` must be safe to run twice**.

### 4.5 Persisted state

State is encoded to optimise partial updates:

1. **Assigns map**: a map with all atom keys; one key per entry, unordered.

        #{mykey => <<"my value">>, otherkey => 42}

2. **Components list**: a list of maps each with an atom `id` key holding a
   binary; one key per item, ordered by fractional indexing.

        [#{id => "item1", value => 1}, #{id => "item2", value => 2}]

3. **Term**: anything else, via `term_to_binary`, chunked into 100KB values.

4. **Nesting**: encodings nest recursively, e.g. a components list inside an
   assigns map.

On update, assigns maps and components lists write only changed entries
(clearing removed ones; reordering updates fractional indices); terms are
rewritten in full. If the encoding type changes, the old keys are cleared and
the new encoding written in full.

### 4.6 Caching

A consumer may cache the state with its versionstamp and skip the read when the
versionstamp still matches the database. The cache is invalidated when another
consumer modifies the state.

### 4.7 Request flows

**Call:** the caller enqueues the request with its from-key and watches the
reply-sentinel-key. A consumer runs `handle_call/3`, writes the new state and,
if the sentinel still exists, the reply; then commits. The consumer then runs
the actions while the caller reads and clears the reply. A timed-out caller
clears its reply keys so none leak; as with `gen_server`, the callback still
runs and the state still changes.

**Cast:** enqueue; a consumer runs `handle_cast/2`, commits, then runs the
actions.

**Lock:**

1. The callback returns `{lock, NewState}`. In one transaction the consumer
   commits `NewState`, dequeues the message, and writes a lock key holding
   `{Timestamp, Token}` (`Token` unique to this acquisition).
2. Other consumers see the lock and wait on a DB watch.
3. The holder calls `handle_locked/4` with the triggering event, message, and
   committed `NewState`.
4. The holder commits the result in a second transaction, only if the lock key
   still carries its `Token`. Otherwise nothing is written, the message is
   requeued in that transaction, and the holder exits with
   `{lock_fenced, requeued}` (`not_requeued` for a priority or inline lock).
5. Whatever `handle_locked/4` returns, even on exception, the lock key is
   cleared (only if it still carries this holder's `Token`) and the queue watch
   notified, so waiting consumers resume.

### 4.8 Internal terms

- **from-key**: `{WaitingKey..., Timestamp, term_to_binary(Ref)}` identifies
  one waiting caller under the **waiting-key** prefix; the timestamp lets
  abandoned keys be garbage-collected.
- **reply-sentinel-key**: chunk 0 of the reply, stored as `{From, <<"t">>, N}`
  so replies may exceed the backend value-size limit; the caller watches it.
- **quid**: prefix of all queue keys: **item-keys** (queued requests) and the
  **push-key / pop-key** counters.

---

## 5. Crash and failure behaviour

Standard `call` and `cast` messages are processed **exactly-once** under normal
operation and **at-least-once** if a consumer crashes before commit (bounded by
`dead_letter_threshold` when set). Make callbacks idempotent where possible.

**During `init/1`:** nothing is persisted yet; on restart `init/1` runs again
from scratch.

**During `handle_call`, `handle_cast`, `handle_info`:** the transaction aborts
and state is unchanged. A `call`/`cast` stays in the queue and is retried by
the next consumer (subject to `dead_letter_threshold`). A `priority_call`,
`priority_cast`, or `handle_info` message is lost.

**Dead-letter queue.** A *poison message* consistently crashes consumers. With
`dead_letter_threshold` set to a positive integer, each failed attempt
increments a counter in the message envelope; at the threshold the message is
moved to the dead-letter queue (DLQ) instead of retried, and:

- a blocked `call` caller raises `{dead_letter, N}` (`N` = attempt count);
- the optional `handle_dead_letter/2` callback receives the message and count;
- a warning is logged.

Dead-lettering is **disabled by default** (`infinity`):

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

**Supervisor restart intensity.** Each consumer crash is a supervisor restart.
If the supervisor's intensity limit (default 3 restarts in 5 seconds) is hit
before `dead_letter_threshold`, the supervisor terminates instead. Set
`intensity`/`max_restarts >= dead_letter_threshold` with a period long enough
to cover the crash-restart cycle, e.g. 5 restarts in 60 seconds for a threshold
of 3. With `infinity`, a poison message crash-loops until the supervisor gives
up.

**Managing the DLQ.** Dead-lettered messages are never cleared automatically.
Operators manage them with `dgen_queue`, using the `Quid` from
`dgen_server:get_quid(Tuid)`:

<!-- tabs-open -->

### Erlang

```erlang
Quid = dgen_server:get_quid(Tuid),
Entries = dgen_queue:peek_dlq(Tenant, Quid),  %% [{Key, Envelope, Attempts, TsMs}]
dgen_queue:dlq_length(Tenant, Quid),
dgen_queue:requeue_dlq_entry(Tenant, Quid, Key),  %% ok | {error, not_found}
dgen_queue:delete_dlq_entry(Tenant, Key),
dgen_queue:purge_dlq(Tenant, Quid).
```

### Elixir

```elixir
quid = :dgen_server.get_quid(tuid)
entries = :dgen_queue.peek_dlq(tenant, quid)  # [{key, envelope, attempts, ts_ms}]
:dgen_queue.dlq_length(tenant, quid)
:dgen_queue.requeue_dlq_entry(tenant, quid, key)  # :ok | {:error, :not_found}
:dgen_queue.delete_dlq_entry(tenant, key)
:dgen_queue.purge_dlq(tenant, quid)
```

<!-- tabs-close -->

`requeue_dlq_entry/3` atomically moves the envelope back to the main queue with
its attempt count reset to zero, for retry after the cause is fixed.

**During `handle_locked`:** the state committed with the lock is already
persisted and the triggering message already dequeued. On an exception the lock
is cleared before the process exits and the message is not retried. If the
process dies without clearing it (SIGKILL, OOM, power loss), the lock blocks
the queue until `lock_timeout` expires (forever with `infinity`, requiring
manual intervention). If the holder is fenced, its message is requeued and
retried (§4.4).

**During actions:** state is already committed and the message consumed. A
crashing action stops the remaining actions; nothing is retried.

**Supervisor restart:** if durable state exists, `init/1` is called but its
state is ignored; the process resumes from the persisted state and consumes
(if configured to) alongside any other consumers.

---

## 6. Guarantees

These are normative. Behaviour that violates one of these is a defect.

1. **Durability.** State and queued messages persist in the backend,
   independent of any Erlang process, node, or cluster.
2. **Delivery.** Standard `call`/`cast` messages are processed exactly-once
   under normal operation. If a consumer crashes before its transaction
   commits, the message is retried by another consumer: at-least-once in that
   case, bounded by `dead_letter_threshold` when configured (§5).
3. **Serializability.** Standard messages are committed with strict
   serializability via the durable queue: a batch's state read and write happen
   in one transaction.
4. **Priority messages trade ordering for immediacy.** `priority_call/2`,
   `priority_cast/2`, and `handle_info/2` always bypass the queue and the lock,
   so they are not ordered with respect to the queued stream.
5. **Actions run exactly once per committed batch,** strictly after that
   batch's commit, once the state they reference is durable.
6. **Lock exclusivity.** While a `dgen_server` is locked, only the lock
   holder's `handle_locked/4`, priority messages, and `handle_info/2` execute;
   other standard `call`/`cast` messages wait until the lock clears.
7. **Callback purity.** `handle_call/3`, `handle_cast/2`, and `handle_info/2`
   run inside a database transaction and must not perform side effects; those
   belong in `Actions` (§4.3) or `handle_locked/4` (§4.4), which run outside a
   transaction.

---

## 7. Configuration

Options in the `Opts` proplist of `start/3,4` and `start_link/3,4`:

| Option | Default | Meaning |
|---|---|---|
| `tenant` | — (required) | `{DbHandle, Dir}` pair identifying the backend subspace. |
| `consume` | `true` | Whether this process consumes from the queue, or only publishes. |
| `reset` | `false` | When `true`, re-initialise the durable state even if it exists. |
| `dead_letter_threshold` | `infinity` | Consecutive processing failures before a message is moved to the dead-letter queue instead of retried (§5). |
| `consume_k` | `1` | Maximum queued messages processed per transaction (§4.2). |
| `lock_timeout` | `infinity` | Maximum milliseconds a lock may be held before other consumers treat it as stale (§4.4). |

---

## 8. When `dgen_server` is (and isn't) a good fit

**Good fit when** you want the `gen_server` model but need state and in-flight
messages to survive process, node, or cluster death; a database round-trip per
message is acceptable; and side effects can be deferred to `Actions` or done in
`handle_locked/4`.

**Reconsider when** you need purely in-memory latency (a plain `gen_server` is
faster for state that need not survive a restart); your callbacks need
non-deferrable side effects inline; or you cannot or do not want to operate a
strongly-consistent external database.
