-module(dgen_backend).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Behaviour defining the storage backend for dgen.

A backend must implement all required callbacks to provide:

- **Transactions.** Serializable read-write transactions over the keyspace.
- **Key-value operations.** Get, set, clear, range reads, and atomic adds.
- **Watches.** Notifications when a key changes.
- **Versionstamps.** Unique, globally ordered write stamps, used for FIFO queuing.
- **Directory layer.** Tuple-key packing, range calculation, and namespace isolation.

The default backend is `dgen_erlfdb`, which delegates to FoundationDB via the
`erlfdb` library.

We look forward to changing the backend interface to support other systems.

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

A backend module must implement this behaviour. Every watch and future operation
must return a `{dgen_future, Ref, BackendData}` tuple. `Ref` is a reference, and
the backend later sends `{Ref, ready}` to the watching process.

## Types

Backends define opaque types for database handles, transaction handles,
directory handles, and futures. The dgen core treats these as opaque terms
and only passes them back into backend callbacks.

*This documentation is LLM-generated. See the AI disclosure in `README.md`.*
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

%% Conflict ranges and read-version control.
%%
%% `add_read_conflict_key/2` records a read conflict on `Key` without reading it.
%% The commit then fails if another transaction wrote `Key` between this
%% transaction's read version and its commit version. The registry uses this to
%% fence a registration commit against a concurrent leadership change, without
%% paying for a storage read of the leader key.
%%
%% `set_read_version/2` pins the read version, for example to an earlier commit
%% version, so a fenced write-only batch doesn't have to fetch a new read version.
%% `get_committed_version/1` returns a transaction's commit version after it
%% commits, which the caller can pin on the next batch.
-callback add_read_conflict_key(Tx :: tx(), Key :: key()) -> ok.
-callback get_read_version(Tx :: tx()) -> future().
-callback set_read_version(Tx :: tx(), Version :: integer()) -> ok.
-callback get_committed_version(Tx :: tx()) -> integer().

%% Explicit transaction lifecycle.
%%
%% `transactional/2` runs a closure and retries it automatically. These callbacks
%% expose the individual steps instead, so a caller such as `dgen_transaction` can
%% keep one transaction open across messages, decide when to commit, and control
%% retries itself.
%%
%% `create_transaction/1` opens a fresh transaction against a database handle.
%% `commit/1` returns a future. Waiting on it returns `ok` or raises the backend's
%% transaction error. `on_error/2` takes the FDB error code and returns a future.
%% If the error is retryable, waiting on that future backs off, resets the
%% transaction for a retry, and succeeds. Otherwise it raises. `error_code/1` maps
%% a caught exception reason to the backend's integer error code, or to `error` if
%% it isn't a backend error. This way callers never match the backend-specific
%% error tuple directly.
-callback create_transaction(Db :: db()) -> tx().
-callback commit(Tx :: tx()) -> future().
-callback on_error(Tx :: tx(), ErrorCode :: integer()) -> future().
-callback error_code(Reason :: term()) -> integer() | error.

%% Versionstamp operations
-callback get_next_tx_id(Tx :: tx()) -> non_neg_integer().
-callback set_versionstamped_key(Tx :: tx(), Key :: key(), Value :: binary()) -> ok.
-callback set_versionstamped_value(Tx :: tx(), Key :: key(), Value :: binary()) -> ok.
-callback get_versionstamp(Tx :: tx()) -> future().

%% Futures
-callback wait(Future :: future()) -> term().
-callback wait_for_all(Futures :: [future()]) -> [term()].

%% Watches. These must return `{dgen_future, Ref, BackendData}`.
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
