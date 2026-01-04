-module(dgen).

-moduledoc """
## dgen_server

A dgen_server is an abstract entity that is composed of (i) state and (ii) operations on state. The state is
stored in a durable fashion in a distributed key-value store, such as FoundationDB. The operations are defined
in code by a module that implements the dgen_server behaviour. With this recipe, dgen_server
acts as if it were an Erlang gen_server, but can live beyond the lifetime of any Erlang process, node, or cluster.

As such, the dgen_server itself does not have a singular representation on the Erlang VM. Instead, zero or more
"dgen_server processes" may exist at a given moment; these are Erlang process that are responsible for executing
the operations on the state, according to your whims as the developer. The state itself is always changed with
strict serializability to guarantee that your operations yield consistent results.

### API Terms

- `tenant`: A pairing of the database object (erlfdb_database) and a directory (erlfdb_directory) that
  defines a subspace of the keyset that is partitioned for some purpose as defined by the developer.
- `key-tuple`: A tuple that is to be encoded into a binary for storage as a key in a key-value pair
  inside a tenant subspace. Any key-tuple may be futher extended by inserting a new item at the end
  of the tuple. In such a case, the original key-tuple becomes a prefix key-tuple, and can be thought
  of as a container for other key-values, via range operations.
- `tuid`: Short for tenant-unique identifier. This is a key-tuple that uniquely identifies some
  resource in a tenant.
- `message-queue`: Each dgen_server has a queue of messages from which it processes requests.
- `call-request`: An operation put on the message-queue that expects a response.
- `from-key`: A key-tuple that uniquely defines a single entity waiting on the result of some call-request
- `cast-request`: An operation put on the message-queue that does not expect a response.
- `priority-request`: A call or cast request that ignores the message-queue and is handled immediately
  by the dgen_server process. Use with caution, as this breaks ordering guarantees. Can be useful for "snapshot reads".
- `dgen_server`: The distributed gen_server, whose state is represented by one or more key-values in the
  tenant and whose functionality is defined by the module that implements the dgen_server beahviour. This is
  an abstract entity without a singular representation in the Erlang VM.
- `dgen_server process`: An Erlang process that is capable of pushing requests onto the message-queue.
- `dgen_server consumer`: A dgen_server process that is also capable of consuming items from the message-queue and
  performing the operations defined by the item. There can be zero, one, or many consumers for each dgen_server.

### Internal terms

- `key-bin`: A key-tuple encoded into a binary using the tenant subspace
- `waiting-key`: A prefix key-tuple that contains all entities waiting on the result of some call-request
- `call-key-bin`: The encoded from-key. This is where the result of the call-request is stored.
- `queue-key`: A perfix key-tuple that contains all key-values for the message-queue
- `item-key`: A key-tuple that identifiers an item in the queue (i.e. a call-request or cast-request)
- `push-key`: A key-tuple that tracks the number of pushes onto the queue
- `pop-key`: A key-tuple that tracks the number of pops from the queue

### Call Request Flow

1. Some Erlang process calls dgen_server:call/3 (the calling entity)
2. A call-request is pushed onto the message-queue, along with the from-key[^1]
3. The call-key-bin and watch are returned to the calling entity
4. One of the dgen_server consumers consumes the call-request
5. The consumer retrieves the state
6. The consumer calls the `handle_call/3` function on the module that implements the dgen_server behaviour
7. The callback returns a new state and an optional list of side-effects.
8. The consumer updates the state
9. The consumer updates the call-key-bin
10. The consumer commits the transaction
11. (concurrent with 12) The consumer executes the side-effects
12. (concurrent with 11) The calling entity receives the watch notification and then retrieves the result

### Cast Request Flow

1. A cast-request is pushed onto the message-queue
2. One of the dgen_server consumers consumes the cast-request
3. The consumer retrieves the state
4. The consumer calls the `handle_cast/2` function on the module that implements the dgen_server behaviour
5. The callback returns a new state and an optional list of side-effects.
6. The consumer updates the state
7. The consumer commits the transaction
8. The consumer executes the side-effects
""".

-export([get_waiting_key/1, get_from/2, call/4, push_call/4]).

-include("../include/dgen.hrl").

push_call(?IS_DB(Db, Dir), Tuid, Request, WatchTo) ->
    erlfdb:transactional(Db, fun(Tx) -> push_call({Tx, Dir}, Tuid, Request, WatchTo) end);
push_call(Td = ?IS_TX(Tx, Dir), Tuid, Request, WatchTo) ->
    WaitingKey = get_waiting_key(Tuid),
    From = get_from(WaitingKey, make_ref()),
    CallKeyBin = erlfdb_directory:pack(Dir, From),
    erlfdb:set(Tx, CallKeyBin, term_to_binary(noreply)),
    Future = erlfdb:watch(Tx, CallKeyBin, [{to, WatchTo}]),
    dgen_queue:push_k(Td, Tuid, [{call, Request, From}]),
    {CallKeyBin, Future}.

get_waiting_key(Tuple) ->
    erlang:insert_element(1 + tuple_size(Tuple), Tuple, <<"c">>).

get_from(WaitingKey, Ref) ->
    Bin = term_to_binary(Ref),
    erlang:insert_element(1 + tuple_size(WaitingKey), WaitingKey, Bin).

call(Module, Server, Request, Timeout) ->
    T1 = erlang:monotonic_time(millisecond),

    CallResult =
        try
            Module:call(Server, Request, Timeout)
        catch
            error:{timeout, _} ->
                % @todo: If CallKeyBin was created, it will be abandoned
                {noreply, {undefined, undefined, undefined}}
        end,

    case CallResult of
        {reply, Reply} ->
            Reply;
        {noreply, {Tenant, CallKeyBin, Watch}} ->
            T2 = erlang:monotonic_time(millisecond),

            case await_call_reply(Tenant, CallKeyBin, Watch, Timeout - (T2 - T1)) of
                {error, timeout} ->
                    erlang:error(timeout);
                Reply ->
                    Reply
            end
    end.

await_call_reply(undefined, _CallKeyBin, _Watch, _Timeout) ->
    {error, timeout};
await_call_reply(_Tenant, _CallKeyBin, _Watch, Timeout) when Timeout =< 0 -> {error, timeout};
await_call_reply(Tenant, CallKeyBin, ?FUTURE(WatchRef), Timeout) ->
    T1 = erlang:monotonic_time(millisecond),

    Result =
        receive
            {WatchRef, ready} ->
                handle_call_ready(Tenant, CallKeyBin)
        after Timeout ->
            {error, timeout}
        end,

    T2 = erlang:monotonic_time(millisecond),

    case Result of
        {reply, Reply} ->
            Reply;
        {watch, Watch} ->
            await_call_reply(Tenant, CallKeyBin, Watch, Timeout - (T2 - T1));
        {error, timeout} ->
            {error, timeout}
    end.

handle_call_ready(?IS_DB(Db, Dir), CallKeyBin) ->
    erlfdb:transactional(Db, fun(Tx) -> handle_call_ready({Tx, Dir}, CallKeyBin) end);
handle_call_ready(?IS_TX(Tx, _Dir), CallKeyBin) ->
    Future = erlfdb:get(Tx, CallKeyBin),
    case erlfdb:wait(Future) of
        not_found ->
            {watch, erlfdb:watch(Tx, CallKeyBin)};
        Value ->
            CallState = binary_to_term(Value),
            case CallState of
                {reply, Reply} ->
                    erlfdb:clear(Tx, CallKeyBin),
                    {reply, Reply};
                noreply ->
                    {watch, erlfdb:watch(Tx, CallKeyBin)};
                _ ->
                    {error, timeout}
            end
    end.
