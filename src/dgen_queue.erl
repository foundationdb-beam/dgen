-module(dgen_queue).

-export([push_k/3, consume_k/3, delete/2, length/2]).

-include("../include/dgen.hrl").

-define(VS(Tx), {
    versionstamp,
    16#ffffffffffffffff,
    16#ffff,
    erlfdb:get_next_tx_id(Tx)
}).

push_k(?IS_DB(Db, Dir), Tuid, Items) ->
    erlfdb:transactional(Db, fun(Tx) -> push_k({Tx, Dir}, Tuid, Items) end);
push_k(?IS_TX(Tx, Dir), Tuid, Items) ->
    QueueKey = get_queue_key(Tuid),
    ItemKey = get_item_key(QueueKey),
    ItemKey2 = erlang:insert_element(1 + tuple_size(ItemKey), ItemKey, undefined),
    [
        erlfdb:set_versionstamped_key(
            Tx,
            erlfdb_directory:pack_vs(
                Dir,
                erlang:setelement(
                    tuple_size(ItemKey2),
                    ItemKey2,
                    ?VS(Tx)
                )
            ),
            term_to_binary(Item)
        )
        || Item <- Items
        ],

        PushKey = get_push_key(QueueKey),
        erlfdb:add(Tx, erlfdb_directory:pack(Dir, PushKey), length(Items)).

consume_k(?IS_DB(Db, Dir), K, Tuid) ->
    erlfdb:transactional(Db, fun(Tx) -> consume_k({Tx, Dir}, K, Tuid) end);
consume_k(?IS_TD = Td, K, Tuid) ->
    case pop_k(Td, K, Tuid) of
        {ok, Vals} ->
            {Vals, undefined};
        {{error, empty}, Vals} ->
            {Vals, watch_push(Td, Tuid)}
    end.

delete(?IS_DB(Db, Dir), Tuid) ->
    erlfdb:transactional(Db, fun(Tx) -> delete({Tx, Dir}, Tuid) end);
delete(?IS_TX(Tx, Dir), Tuid) ->
    QueueKey = get_queue_key(Tuid),
    {SK, EK} = erlfdb_directory:range(Dir, QueueKey),
    erlfdb:clear_range(Tx, SK, EK).

length(?IS_DB(Db, Dir), Tuid) ->
    erlfdb:transactional(Db, fun(Tx) -> length({Tx, Dir}, Tuid) end);
length(?IS_TX(Tx, Dir), Tuid) ->
    QueueKey = get_queue_key(Tuid),
    PushKey = get_push_key(QueueKey),
    PopKey = get_pop_key(QueueKey),
    F = [erlfdb:get(Tx, erlfdb_directory:pack(Dir, PushKey)), erlfdb:get(Tx, erlfdb_directory:pack(Dir, PopKey))],
    [Push, Pop] = erlfdb:wait_for_all(F),
    decode_as_int(Push, 0) - decode_as_int(Pop, 0).

watch_push(?IS_DB(Db, Dir), Tuid) ->
    erlfdb:transactional(Db, fun(Tx) -> watch_push({Tx, Dir}, Tuid) end);
watch_push(?IS_TX(Tx, Dir), Tuid) ->
    QueueKey = get_queue_key(Tuid),
    PushKey = get_push_key(QueueKey),
    erlfdb:watch(Tx, erlfdb_directory:pack(Dir, PushKey)).

pop_k(?IS_DB(Db, Dir), K, Tuid) ->
    erlfdb:transactional(Db, fun(Tx) -> pop_k({Tx, Dir}, K, Tuid) end);
pop_k(?IS_TX(Tx, Dir), K, Tuid) ->
    QueueKey = get_queue_key(Tuid),
    ItemKey = get_item_key(QueueKey),
    {QS, QE} = erlfdb_directory:range(Dir, ItemKey),
    case erlfdb:get_range(Tx, QS, QE, [{limit, K}, {wait, true}]) of
        [] ->
            {{error, empty}, []};
        KVs = [{S, _} | _] ->
            N = length(KVs),
            {E, _} = lists:last(KVs),
            erlfdb:clear_range(Tx, S, erlfdb_key:strinc(E)),
            PopKey = get_pop_key(QueueKey),
            erlfdb:add(Tx, erlfdb_directory:pack(Dir, PopKey), N),

            Status =
                if
                    N == K -> ok;
                    true -> {error, empty}
                end,
            {Status, [binary_to_term(V) || {_, V} <- KVs]}
    end.

get_queue_key(Tuple) ->
    erlang:insert_element(1 + tuple_size(Tuple), Tuple, <<"q">>).

get_item_key(QueueKey) ->
    erlang:insert_element(1 + tuple_size(QueueKey), QueueKey, <<"i">>).

get_push_key(QueueKey) ->
    erlang:insert_element(1 + tuple_size(QueueKey), QueueKey, <<"n">>).

get_pop_key(QueueKey) ->
    erlang:insert_element(1 + tuple_size(QueueKey), QueueKey, <<"p">>).

decode_as_int(not_found, Default) -> Default;
decode_as_int(Val, _Default) -> binary:decode_unsigned(Val, little).
