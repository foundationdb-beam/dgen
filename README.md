# DGen

dgen provides a distributed gen_server.

## Motivation

I love gen_server. There are only 2 things stopping me from writing my entire app with them:

1. Durability: The state is lost when the process goes down.
2. High availability: The functionality is unavailable when the process goes down.

Let's try to solve this with a distributed system, and find out if an app actually can be
written with only gen_servers.

## Getting Started

<!-- tabs-open -->

### Erlang

The simplest distributed server is just a regular gen_server with `dgen_server` behaviour:

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

Start it inside a FoundationDB directory, and the state persists across restarts:

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

The simplest distributed server is just a regular GenServer with `use DGenServer`:

```elixir
defmodule Counter do
  use DGenServer

  def start(tenant), do: DGenServer.start(__MODULE__, [], tenant: tenant)

  def increment(pid), do: DGenServer.cast(pid, :increment)
  def value(pid), do: DGenServer.call(pid, :value)

  @impl true
  def init([]), do: {:ok, 0}

  @impl true
  def handle_call(:value, _from, state), do: {:reply, state, state}

  @impl true
  def handle_cast(:increment, state), do: {:noreply, state + 1}
end
```

Start it inside a FoundationDB directory, and the state persists across restarts:

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

## Installation

DGen can be installed by adding `dgen` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:dgen, "~> 0.1.0"}
  ]
end
```

The docs can be found at <https://hexdocs.pm/dgen>.

## API Contract

### Message Processing

`dgen_server` provides different message paths with different guarantees:

**Standard messages** (`call`, `cast`):
- Processed with strict serializability via the durable queue
- Execute within a database transaction (subject to FDB transaction limits)
- Must not include side effects. Callbacks must be pure with respect to external systems
- Respect the lock (see Locking below)

**Priority messages** (`priority_call`, `priority_cast`, `handle_info`):
- Skip the durable queue and execute immediately
- Still execute within a database transaction (subject to FDB transaction limits)
- Must not include side effects
- Do not respect the lock. Always execute even when locked

### Actions

Callbacks may return `{reply, Reply, State, Actions}` or `{noreply, State, Actions}` where `Actions` is a list of 1-arity functions. These functions:
- Execute after the transaction commits
- Receive the committed `State` as their argument, but cannot modify it
- Are the correct place for side effects: logging, telemetry, publishing to external systems
- Can return `halt` to stop processing actions, or any other value to continue

### Locking

A callback may return `{lock, State}` to enter locked mode. When locked:

- Standard `call` and `cast` messages are queued but not processed
- Priority messages and `handle_info` continue to execute
- The `handle_locked/3` callback is invoked outside of a transaction
  - Not subject to FDB transaction limits
  - Side effects are permitted
  - Can modify state, which is written back to the database

Use locking for long-running operations that would exceed transaction time limits, such as calling external APIs or performing extended computations.

## Persisted State

### Encoder/Decoder

State is persisted to the key-value store using a structured encoding scheme that optimizes for partial updates. Three encoding types are supported:

1. **Assigns map**: Maps with all atom keys are split across separate keys, one per entry. No ordering guarantees.

        #{
            mykey => <<"my value">>,
            otherkey => 42
        }

2. **Components list**: Lists where every item is a map with an atom `id` key containing a binary value. Each item is stored separately with ordering maintained via fractional indexing in the storage key.

        [
            #{id => "item1", value => 1},
            #{id => "item2", value => 2}
        ]

3. **Term**: All other terms use `term_to_binary` and are chunked into 100KB values.

        {this, is <<"some">>, term, 4.5, %{3 => 2}}

4. **Nesting**: The encoder handles nested structures recursively. For example, an assigns map containing a components list will nest both encodings in the key path.

        #{
            mykey => <<"my value">>,
            mylist => [
                #{id => "item1", value => 1},
                #{id => "item2", value => 2}
            ]
        }

When writing updates, diffs are generated by comparing old and new state:
- **Assigns map**: Only changed entries are written; removed entries are cleared
- **Components list**: Only changed items are written; ordering changes update fractional indices
- **Term**: Full rewrite (no diffing)

If the encoding type changes between updates, the old keys are cleared and the new encoding is written in full.

### Caching

Each consumer process can maintain an in-memory cache of the state paired with its versionstamp. On subsequent messages, if the cached versionstamp matches the current database version, the state is reused without a read operation. This eliminates redundant reads when processing multiple messages in sequence.

The cache is invalidated when the process detects that another consumer has modified the state.

### Crashing

DGenServer has well-defined behavior during crashes.

**Key guarantee:** Standard `call` and `cast` messages are processed **at-least-once**. If a crash occurs before the transaction commits, the message will be retried. Design your callbacks to be idempotent when possible.

**During `init/1`:**
- If the first `init/1` crashes, the gen_server process exits before any state is persisted
- When restarted `init/1` runs again from scratch
- No durable state exists yet, so there's nothing to recover

**During transactional callbacks (`handle_call`, `handle_cast`, `handle_info`):**
- The database transaction is automatically aborted — no state changes are committed
- For `call` and `cast`: the message remains in the durable queue and will be retried by the next consumer
- For `priority_call` and `priority_cast`: the message is lost (it never entered the queue)
- For `handle_info`: the Erlang message is lost (info messages are not durable)
- State remains unchanged from before the callback was invoked

**During `handle_locked`:**
- `handle_locked` executes outside a transaction, so previous state changes have already been persisted
- If the crash is an Erlang/Elixir throw, then the lock is cleared before the process exits
- If the crash is a system disruption such as SegFault, OOM, or sudden power loss, the lock is not cleared and the dgen_server is deadlocked. Manual intervention is required to clear the lock.
- In either case, the triggering message has been consumed from the queue, so it will not be retried

**During action execution:**
- Actions run after the transaction commits, so state changes are already persisted
- If an action crashes, the state update succeeds but remaining actions are not executed
- The message has been consumed from the queue and will not be retried

**Supervisor restart:**
- When a dgen_server is restarted by a supervisor, it reads existing state from the database
- If state exists, `init/1` is called, but the initial state is ignored. The server resumes with the persisted state
- The process immediately begins consuming any queued messages, if it's configured to do so
- Multiple processes can safely consume from the same queue; they coordinate via database transactions
