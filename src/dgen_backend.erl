-module(dgen_backend).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Behaviour defining the storage backend for dgen.

A backend must implement all required callbacks to provide:

- **Transactions** — serializable read-write transactions over the keyspace.
- **Key-value operations** — get, set, clear, range reads, atomic adds.
- **Watches** — notifications when a key changes.
- **Versionstamps** — globally-ordered, unique write stamps for FIFO queuing.
- **Directory layer** — tuple-key packing, range calculation, namespace isolation.

The default backend is `dgen_erlfdb`, which delegates to FoundationDB via the
`erlfdb` library.

## Configuring the backend

Set the `backend` application env before starting dgen:

```erlang
application:set_env(dgen, backend, my_backend).
```

Or in `config.exs`:

```elixir
config :dgen, backend: MyBackend
```

## Implementing a backend

A backend module must implement this behaviour. All watch and future operations
must return `{dgen_future, Ref, BackendData}` tuples where `Ref` is a reference
that will appear in a `{Ref, ready}` message sent to the watching process.

## Types

Backends define opaque types for database handles, transaction handles,
directory handles, and futures. The dgen core treats these as opaque terms
and only passes them back into backend callbacks.
""".
-endif.

-export([transactional/2]).

%% Transaction management
-callback transactional(Db :: term(), Fun :: fun((Tx :: term()) -> Result)) ->
    Result
when
    Result :: term().

-callback is_transaction(Handle :: term()) -> boolean().

%% Key-value operations
-callback get(Tx :: term(), Key :: binary()) -> Future :: term().
-callback set(Tx :: term(), Key :: binary(), Value :: binary()) -> ok.
-callback clear_range(Tx :: term(), StartKey :: binary(), EndKey :: binary()) -> ok.
-callback get_range(Tx :: term(), StartKey :: binary(), EndKey :: binary(), Opts :: list()) ->
    [{binary(), binary()}].
-callback add(Tx :: term(), Key :: binary(), Value :: integer()) -> ok.

%% Versionstamp operations
-callback get_next_tx_id(Tx :: term()) -> non_neg_integer().
-callback set_versionstamped_key(Tx :: term(), Key :: binary(), Value :: binary()) -> ok.

%% Futures
-callback wait(Future :: term()) -> term().
-callback wait_for_all(Futures :: [term()]) -> [term()].

%% Watches — must return {dgen_future, Ref, BackendData}
-callback watch(Tx :: term(), Key :: binary()) -> Future :: term().
-callback watch(Tx :: term(), Key :: binary(), Opts :: list()) -> Future :: term().

%% Directory / keyspace operations
-callback dir_range(Dir :: term(), TupleKey :: tuple()) -> {binary(), binary()}.
-callback dir_pack(Dir :: term(), TupleKey :: tuple()) -> binary().
-callback dir_pack_vs(Dir :: term(), TupleKey :: tuple()) -> binary().
-callback dir_unpack(Dir :: term(), PackedKey :: binary()) -> tuple().
-callback key_strinc(Key :: binary()) -> binary().

%% Directory management
-callback dir_root(Opts :: list()) -> Dir :: term().
-callback dir_create_or_open(Db :: term(), Dir :: term(), Name :: term()) -> Dir :: term().
-callback dir_create(Db :: term(), Dir :: term(), Name :: term()) -> Dir :: term().
-callback dir_remove(Db :: term(), Dir :: term(), Name :: term()) -> ok.

%% Sandbox / test support
-callback sandbox_open(Name :: term()) -> Db :: term().

-optional_callbacks([sandbox_open/1]).

-if(?DOCATTRS).
-doc """
Runs `Fun` inside a transaction if `Handle` is a database handle,
or calls `Fun` directly if `Handle` is already a transaction.
""".
-endif.
-spec transactional({term(), term()}, fun(({term(), term()}) -> Result)) -> Result when
    Result :: term().
transactional({Handle, Dir}, Fun) ->
    B = dgen_config:backend(),
    case B:is_transaction(Handle) of
        true -> Fun({Handle, Dir});
        false -> B:transactional(Handle, fun(Tx) -> Fun({Tx, Dir}) end)
    end.
