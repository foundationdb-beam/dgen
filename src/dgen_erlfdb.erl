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

-spec transactional(dgen_backend:db(), fun((dgen_backend:tx()) -> Result)) -> Result when
    Result :: term().
transactional(Db, Fun) ->
    erlfdb:transactional(Db, Fun).

-spec is_transaction(dgen_backend:db() | dgen_backend:tx()) -> boolean().
is_transaction({erlfdb_transaction, _}) -> true;
is_transaction(_) -> false.

%% Key-value operations

-spec get(dgen_backend:tx(), dgen_backend:key()) -> dgen_backend:future().
get(Tx, Key) ->
    erlfdb:get(Tx, Key).

-spec set(dgen_backend:tx(), dgen_backend:key(), binary()) -> ok.
set(Tx, Key, Value) ->
    erlfdb:set(Tx, Key, Value).

-spec clear_range(dgen_backend:tx(), dgen_backend:key(), dgen_backend:key()) -> ok.
clear_range(Tx, StartKey, EndKey) ->
    erlfdb:clear_range(Tx, StartKey, EndKey).

-spec get_range(dgen_backend:tx(), dgen_backend:key(), dgen_backend:key(), list()) ->
    [{dgen_backend:key(), binary()}].
get_range(Tx, StartKey, EndKey, Opts) ->
    erlfdb:get_range(Tx, StartKey, EndKey, Opts).

-spec add(dgen_backend:tx(), dgen_backend:key(), integer()) -> ok.
add(Tx, Key, Value) ->
    erlfdb:add(Tx, Key, Value).

%% Versionstamp operations

-spec get_next_tx_id(dgen_backend:tx()) -> non_neg_integer().
get_next_tx_id(Tx) ->
    erlfdb:get_next_tx_id(Tx).

-spec set_versionstamped_key(dgen_backend:tx(), dgen_backend:key(), binary()) -> ok.
set_versionstamped_key(Tx, Key, Value) ->
    erlfdb:set_versionstamped_key(Tx, Key, Value).

-spec set_versionstamped_value(dgen_backend:tx(), dgen_backend:key(), binary()) -> ok.
set_versionstamped_value(Tx, Key, Value) ->
    erlfdb:set_versionstamped_value(Tx, Key, Value).

-spec get_versionstamp(dgen_backend:tx()) -> dgen_backend:future().
get_versionstamp(Tx) ->
    erlfdb:get_versionstamp(Tx).

%% Futures

-spec wait(dgen_backend:future()) -> term().
wait(Future) ->
    erlfdb:wait(Future).

-spec wait_for_all([dgen_backend:future()]) -> [term()].
wait_for_all(Futures) ->
    erlfdb:wait_for_all(Futures).

%% Watches — wrap erlfdb futures in {dgen_future, Ref, ErlFdbFuture}

-spec watch(dgen_backend:tx(), dgen_backend:key()) -> dgen_backend:future().
watch(Tx, Key) ->
    Future = erlfdb:watch(Tx, Key),
    wrap_future(Future).

-spec watch(dgen_backend:tx(), dgen_backend:key(), list()) -> dgen_backend:future().
watch(Tx, Key, Opts) ->
    Future = erlfdb:watch(Tx, Key, Opts),
    wrap_future(Future).

-spec wrap_future(term()) -> dgen_backend:future().
wrap_future({erlfdb_future, Ref, _} = Future) ->
    {dgen_future, Ref, Future}.

%% Directory / keyspace operations

-spec dir_range(dgen_backend:dir(), dgen_backend:tuple_key()) ->
    {dgen_backend:key(), dgen_backend:key()}.
dir_range(Dir, TupleKey) ->
    erlfdb_directory:range(Dir, TupleKey).

-spec dir_pack(dgen_backend:dir(), dgen_backend:tuple_key()) -> dgen_backend:key().
dir_pack(Dir, TupleKey) ->
    erlfdb_directory:pack(Dir, TupleKey).

-spec dir_pack_vs(dgen_backend:dir(), dgen_backend:tuple_key()) -> dgen_backend:key().
dir_pack_vs(Dir, TupleKey) ->
    erlfdb_directory:pack_vs(Dir, TupleKey).

-spec dir_unpack(dgen_backend:dir(), dgen_backend:key()) -> dgen_backend:tuple_key().
dir_unpack(Dir, PackedKey) ->
    erlfdb_directory:unpack(Dir, PackedKey).

-spec key_strinc(dgen_backend:key()) -> dgen_backend:key().
key_strinc(Key) ->
    erlfdb_key:strinc(Key).

%% Directory management

-spec dir_create(dgen_backend:db(), dgen_backend:dir(), term()) -> dgen_backend:dir().
dir_create(Db, Dir, Name) ->
    erlfdb_directory:create(Db, Dir, Name).

-spec dir_remove(dgen_backend:db(), dgen_backend:dir(), term()) -> ok.
dir_remove(Db, Dir, Name) ->
    erlfdb_directory:remove(Db, Dir, Name).

%% Sandbox / test support

-spec sandbox_open(term(), term()) -> dgen_backend:tenant().
sandbox_open(Name, DirName) ->
    Db = erlfdb_sandbox:open(Name),
    Root = erlfdb_directory:root([{node_prefix, <<16#FE>>}, {content_prefix, <<>>}]),
    Dir = erlfdb_directory:create_or_open(Db, Root, DirName),
    {Db, Dir}.
