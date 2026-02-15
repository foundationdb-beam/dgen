-module(dgen_erlfdb).
-behaviour(dgen_backend).

-export([
    transactional/2,
    is_transaction/1,
    get/2,
    set/3,
    clear_range/3,
    get_range/4,
    add/3,
    get_next_tx_id/1,
    set_versionstamped_key/3,
    set_versionstamped_value/3,
    get_versionstamp/1,
    wait/1,
    wait_for_all/1,
    watch/2,
    watch/3,
    dir_range/2,
    dir_pack/2,
    dir_pack_vs/2,
    dir_unpack/2,
    key_strinc/1,
    dir_create/3,
    dir_remove/3,
    sandbox_open/2
]).

%% Transaction management

transactional(Db, Fun) ->
    erlfdb:transactional(Db, Fun).

is_transaction({erlfdb_transaction, _}) -> true;
is_transaction(_) -> false.

%% Key-value operations

get(Tx, Key) ->
    erlfdb:get(Tx, Key).

set(Tx, Key, Value) ->
    erlfdb:set(Tx, Key, Value).

clear_range(Tx, StartKey, EndKey) ->
    erlfdb:clear_range(Tx, StartKey, EndKey).

get_range(Tx, StartKey, EndKey, Opts) ->
    erlfdb:get_range(Tx, StartKey, EndKey, Opts).

add(Tx, Key, Value) ->
    erlfdb:add(Tx, Key, Value).

%% Versionstamp operations

get_next_tx_id(Tx) ->
    erlfdb:get_next_tx_id(Tx).

set_versionstamped_key(Tx, Key, Value) ->
    erlfdb:set_versionstamped_key(Tx, Key, Value).

set_versionstamped_value(Tx, Key, Value) ->
    erlfdb:set_versionstamped_value(Tx, Key, Value).

get_versionstamp(Tx) ->
    erlfdb:get_versionstamp(Tx).

%% Futures

wait(Future) ->
    erlfdb:wait(Future).

wait_for_all(Futures) ->
    erlfdb:wait_for_all(Futures).

%% Watches — wrap erlfdb futures in {dgen_future, Ref, ErlFdbFuture}

watch(Tx, Key) ->
    Future = erlfdb:watch(Tx, Key),
    wrap_future(Future).

watch(Tx, Key, Opts) ->
    Future = erlfdb:watch(Tx, Key, Opts),
    wrap_future(Future).

wrap_future({erlfdb_future, Ref, _} = Future) ->
    {dgen_future, Ref, Future}.

%% Directory / keyspace operations

dir_range(Dir, TupleKey) ->
    erlfdb_directory:range(Dir, TupleKey).

dir_pack(Dir, TupleKey) ->
    erlfdb_directory:pack(Dir, TupleKey).

dir_pack_vs(Dir, TupleKey) ->
    erlfdb_directory:pack_vs(Dir, TupleKey).

dir_unpack(Dir, PackedKey) ->
    erlfdb_directory:unpack(Dir, PackedKey).

key_strinc(Key) ->
    erlfdb_key:strinc(Key).

%% Directory management

dir_create(Db, Dir, Name) ->
    erlfdb_directory:create(Db, Dir, Name).

dir_remove(Db, Dir, Name) ->
    erlfdb_directory:remove(Db, Dir, Name).

%% Sandbox / test support

sandbox_open(Name, DirName) ->
    Db = erlfdb_sandbox:open(Name),
    Root = erlfdb_directory:root([{node_prefix, <<16#FE>>}, {content_prefix, <<>>}]),
    Dir = erlfdb_directory:create_or_open(Db, Root, DirName),
    {Db, Dir}.
