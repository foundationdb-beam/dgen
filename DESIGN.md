# dgen_server Design

A dgen_server is an abstract entity that is composed of (i) state and (ii) operations on state. The state is
stored in a durable fashion in a distributed key-value store, such as FoundationDB. The operations are defined
in code by a module that implements the dgen_server behaviour. With this recipe, dgen_server
acts as if it were an Erlang gen_server, but can live beyond the lifetime of any Erlang process, node, or cluster.

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
- **queue-key**: A prefix key-tuple that contains all key-values for the message-queue.
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
