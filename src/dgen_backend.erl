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

-type db() :: term().
-type tx() :: term().
-type dir() :: term().
-type tenant() :: {db() | tx(), dir()}.
-type future() :: term().
-type versionstamp() :: term().
-type key() :: binary().
-type tuple_key() :: tuple().

-export_type([
    db/0,
    tx/0,
    dir/0,
    tenant/0,
    future/0,
    versionstamp/0,
    key/0,
    tuple_key/0
]).

%% Transaction management
-callback transactional(Db :: db(), Fun :: fun((Tx :: tx()) -> Result)) ->
    Result
when
    Result :: term().

-callback is_transaction(Handle :: db() | tx()) -> boolean().

%% Key-value operations
-callback get(Tx :: tx(), Key :: key()) -> future().
-callback set(Tx :: tx(), Key :: key(), Value :: binary()) -> ok.
-callback clear_range(Tx :: tx(), StartKey :: key(), EndKey :: key()) -> ok.
-callback get_range(Tx :: tx(), StartKey :: key(), EndKey :: key(), Opts :: list()) ->
    [{key(), binary()}].
-callback add(Tx :: tx(), Key :: key(), Value :: integer()) -> ok.

%% Versionstamp operations
-callback get_next_tx_id(Tx :: tx()) -> non_neg_integer().
-callback set_versionstamped_key(Tx :: tx(), Key :: key(), Value :: binary()) -> ok.
-callback set_versionstamped_value(Tx :: tx(), Key :: key(), Value :: binary()) -> ok.
-callback get_versionstamp(Tx :: tx()) -> future().

%% Futures
-callback wait(Future :: future()) -> term().
-callback wait_for_all(Futures :: [future()]) -> [term()].

%% Watches — must return {dgen_future, Ref, BackendData}
-callback watch(Tx :: tx(), Key :: key()) -> future().
-callback watch(Tx :: tx(), Key :: key(), Opts :: list()) -> future().

%% Directory / keyspace operations
-callback dir_range(Dir :: dir(), TupleKey :: tuple_key()) -> {key(), key()}.
-callback dir_pack(Dir :: dir(), TupleKey :: tuple_key()) -> key().
-callback dir_pack_vs(Dir :: dir(), TupleKey :: tuple_key()) -> key().
-callback dir_unpack(Dir :: dir(), PackedKey :: key()) -> tuple_key().
-callback key_strinc(Key :: key()) -> key().

%% Directory management
-callback dir_create(Db :: db(), Dir :: dir(), Name :: term()) -> dir().
-callback dir_remove(Db :: db(), Dir :: dir(), Name :: term()) -> ok.

%% Sandbox / test support
-callback sandbox_open(Name :: term(), DirName :: term()) -> tenant().

-optional_callbacks([sandbox_open/2]).

-if(?DOCATTRS).
-doc """
Runs `Fun` inside a transaction if `Handle` is a database handle,
or calls `Fun` directly if `Handle` is already a transaction.
""".
-endif.
-spec transactional(tenant(), fun((tenant()) -> Result)) -> Result when
    Result :: term().
transactional({Handle, Dir}, Fun) ->
    B = dgen_config:backend(),
    case B:is_transaction(Handle) of
        true -> Fun({Handle, Dir});
        false -> B:transactional(Handle, fun(Tx) -> Fun({Tx, Dir}) end)
    end.
