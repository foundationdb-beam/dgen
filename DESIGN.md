# Design Details

A dgen_server is an abstract entity that is composed of (i) state and (ii) operations on state. The state is
stored in a durable fashion in a distributed key-value store, such as FoundationDB. The operations are defined
in code by a module that implements the dgen_server behaviour. With this recipe, dgen_server
provides the same programming model as a gen_server, but can live beyond the lifetime of any Erlang process, node, or cluster.

As such, the dgen_server itself does not have a singular representation on the Erlang VM. Instead, zero or more
"dgen_server processes" may exist at a given moment; these are Erlang processes that are responsible for executing
the operations on the state, according to your whims as the developer. The state itself is always changed with
strict serializability to guarantee that your operations yield consistent results.

## API Terms

- **tenant**: A pairing of the database object (`erlfdb_database`) and a directory (`erlfdb_directory`) that
  defines a subspace of the keyset that is partitioned for some purpose as defined by the developer.
- **key-tuple**: A tuple that is to be encoded into a binary for storage as a key in a key-value pair
  inside a tenant subspace. Any key-tuple may be further extended by inserting a new item at the end
  of the tuple. In such a case, the original key-tuple becomes a prefix key-tuple, and can be thought
  of as a container for other key-values, via range operations.
- **tuid**: Short for tenant-unique identifier. This is a key-tuple that uniquely identifies some
  resource in a tenant.
- **message-queue**: Each dgen_server has a queue of messages from which it processes requests.
- **call-request**: An operation put on the message-queue that expects a response.
- **from-key**: A key-tuple that uniquely identifies a single entity waiting on the result of some call-request.
  The key includes a system-time timestamp (seconds) so that abandoned call keys can be garbage-collected
  using a time-based heuristic. Structure: `{WaitingKey..., Timestamp, term_to_binary(Ref)}`.
- **cast-request**: An operation put on the message-queue that does not expect a response.
- **priority-request**: A call or cast request that ignores the message-queue and is handled immediately
  by the dgen_server process. Use with caution, as this breaks ordering guarantees. Can be useful for "snapshot reads".
- **dgen_server**: The distributed gen_server, whose state is represented by one or more key-values in the
  tenant and whose functionality is defined by the module that implements the dgen_server behaviour. This is
  an abstract entity without a singular representation in the Erlang VM.
- **dgen_server process**: An Erlang process that is capable of pushing requests onto the message-queue.
- **dgen_server consumer**: A dgen_server process that is also capable of consuming items from the message-queue and
  performing the operations defined by the item. There can be zero, one, or many consumers for each dgen_server.

## Internal Terms

- **key-bin**: A key-tuple encoded into a binary using the tenant subspace.
- **waiting-key**: A prefix key-tuple that contains all entities waiting on the result of some call-request.
- **reply-sentinel-key**: The first chunk key of the reply term under the from-key. The reply is stored
  using chunked term encoding (`{From, <<"t">>, 0}`, `{From, <<"t">>, 1}`, ...) so that replies
  can exceed the FDB single-value size limit. The FDB watch is placed on the reply-sentinel-key
  (chunk 0). The client reads the reply via `get_range` and clears it via `clear_range`.
- **quid**: Unique tuple identifier for the queue. It is a prefix key-tuple that contains all key-values for the message-queue.
- **item-key**: A key-tuple that identifies an item in the queue (i.e. a call-request or cast-request).
- **push-key**: A key-tuple that tracks the number of pushes onto the queue.
- **pop-key**: A key-tuple that tracks the number of pops from the queue.

## Call Request Flow

1. Some Erlang process calls `dgen_server:call/3` (the calling entity).
2. A call-request is pushed onto the message-queue, along with the from-key.
3. The from-key and watch are returned to the calling entity.
4. One of the dgen_server consumers consumes the call-request.
5. The consumer retrieves the state.
6. The consumer calls the `handle_call/3` function on the module that implements the dgen_server behaviour.
7. The callback returns a new state and an optional list of side-effects.
8. The consumer updates the state.
9. The consumer checks whether the reply-sentinel-key still exists (the caller may have timed out and cleared it).
   If present, the consumer writes `{reply, Reply}` as a chunked term under the from-key; if absent, the write is skipped.
10. The consumer commits the transaction.
11. (concurrent with 12) The consumer executes the side-effects.
12. (concurrent with 11) The calling entity receives the watch notification, reads the chunked reply via `get_range`, and clears the reply keys.

On timeout, the calling entity clears the reply keys to prevent durable key leaks. The callback still
runs and state still mutates (just like `gen_server` — a timed-out call still executes), only the orphan
reply key is eliminated.

## Cast Request Flow

1. A cast-request is pushed onto the message-queue.
2. One of the dgen_server consumers consumes the cast-request.
3. The consumer retrieves the state.
4. The consumer calls the `handle_cast/2` function on the module that implements the dgen_server behaviour.
5. The callback returns a new state and an optional list of side-effects.
6. The consumer updates the state.
7. The consumer commits the transaction.
8. The consumer executes the side-effects.

## Lock Flow

A callback may return `{lock, NewState}` instead of `{noreply, NewState}` or `{reply, …}`.
This is useful when a state change requires synchronous post-commit coordination before
the next queue message can be safely processed.

1. The callback returns `{lock, NewState}`.
2. The consumer commits `NewState` to FDB **and** writes a lock key (`{Tuid, <<"k">>}`) in
   the same transaction.
3. Any other elector consumer that tries to consume from the queue reads `is_locked = true`
   and parks itself on a FDB watch instead of processing.
4. The consumer that set the lock calls `handle_locked/3` with the **same** event type and
   message that triggered the lock, plus the committed `NewState`.  This is the synchronous
   coordination window.
5. After `handle_locked/3` returns (regardless of its return value), the lock key is cleared
   and the queue watch is notified in an `after` block — always, even on exception.
6. Parked consumers wake up, see `is_locked = false`, and resume normal queue consumption.

The lock therefore guarantees that between committing a state change and resuming queue
consumption, one designated node performs a synchronous coordination step with no risk of
another node racing ahead.

---

## Process Registry (`dgen_registry`)

`dgen_registry` is an OTP-compatible process registry built on top of `dgen_server`.
It implements the four-function `{via, dgen_registry, {RegistryName, LogicalName}}` contract
so that standard OTP processes (`gen_server`, `gen_statem`, etc.) can be registered and
addressed by name across a cluster.

### Components

| Module | Role |
|--------|------|
| `dgen_registry` | Supervisor + OTP `via`-tuple contract entry points |
| `dgen_registry_elector` | `dgen_server` callback — tracks membership and elects leaders |
| `dgen_registry_member` | `gen_server` — local name cache, consistent read/write proxy |

Each node that calls `dgen_registry:start_link/2` starts both an elector consumer and a
member process under a local supervisor.

### Leader election via FDB consensus

The leader is the member process on whichever Erlang node commits the current elector FDB
transaction — i.e. `node()` inside the callback.  Because FDB serialises transaction commits,
there is always exactly one leader at any point in time.  No external lease, heartbeat, or
tiebreaker ordering (`lists:min`) is needed, except as a one-time fallback when the local
node's member has not yet sent its `{join}` message (a transient startup window).

This means leadership is an emergent property of FDB consensus: whoever wins the write wins
the leader role.

### Leadership transitions and the lock

When the committed leader changes, `handle_cast_tx` returns `{lock, NewState}` instead of
`{noreply, …, Actions}`.  This triggers the lock flow described above.  `handle_locked/3`
uses the coordination window to fan out replication-topology casts to every member:

- New member receives `{members, AllIds}` and `{leader_changed, Leader}`.
- Existing members receive `{new_member, Id}` and `{leader_changed, Leader}`.

The new leader member receives `{leader_changed, Self}`, which triggers `assume_leadership`:
it uses its current in-memory `names` map (already populated from replication while it was
a follower), sets up process monitors for every entry, and broadcasts a `{names_snapshot, …}`
to all followers.  Any stale Pid entries self-correct when their DOWN signals arrive.
This happens asynchronously in the member's gen_server, after the elector lock has cleared.

When leadership does not change (e.g. a new member joins on the same node), no lock is
taken; a plain `{noreply, NewState, Actions}` handles the notifications.

### Single-writer consistency for names

The leader member is the **sole writer** for the name table.  All consistent writes
(`register_name`, `unregister_name`) and consistent reads (`whereis_name_consistent`) route
through the leader, either directly (if the caller's node is the leader) or by forwarding
via `gen_server:call/cast`.

The leader's gen_server mailbox provides process-level serialisation: registrations are
linearisable without any additional FDB conflict detection on the names sub-space.

### One-way replication to followers

After every write the leader broadcasts `{name_registered, …}` or `{name_unregistered, …}`
to all follower members.  Followers apply these to their local `names` map.  This is a
fire-and-forget push; followers never pull from the leader except at leadership assumption
time (the snapshot).

### Snapshot reads

`whereis_name/1` (used by OTP routing internally) is a snapshot read served from the local
member's in-memory `names` map — a plain `maps:get` inside a `gen_server:call`, no network
hop.  This map may lag behind the leader by one replication round-trip after a remote
registration.

`whereis_name_consistent/1` routes to the leader and always returns the authoritative value.

### Follower optimistic updates

Because Erlang distribution does not guarantee that a replication cast arrives before the
call reply that triggered it, a follower that forwards `{register, …}` to the leader also
updates its own `names` map immediately on receiving `yes`.  Similarly, `{unregister, …}`
removes the entry from the local map before forwarding.  Both are idempotent when the
replication cast arrives shortly after.

### FDB key layout

```
{<<"dgen_registry">>, RegistryName, <<"leader">>} → term_to_binary(MemberId | undefined)
```

where `RegistryName = atom_to_binary(Name)`.  The leader key is the only registry data
written to FDB; it is updated atomically with every membership state change by the elector.

Name→Pid mappings are **never written to FDB**.  Pids are node-local and process-lifetime-
scoped; they have no meaning after a restart.  The authoritative names map lives in the
leader member's gen_server state and is replicated in-memory to followers.

### Auto-unregistration

The leader monitors every registered Pid with `erlang:monitor/2`.  On `{'DOWN', …}` the
leader deletes the name from FDB and broadcasts `{name_unregistered, …}` to all followers.
No explicit `unregister_name` call is needed when a registered process exits.
