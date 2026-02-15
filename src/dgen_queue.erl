-module(dgen_queue).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Durable FIFO queue backed by the configured dgen backend.

Each dgen_server has its own queue keyed under `{Tuid, <<"q">>}`. Items are
ordered by versionstamps, guaranteeing strict FIFO across transactions.
Push and pop counts are tracked with atomic `add` operations for O(1) length.
""".
-endif.

-export([push_k/3, consume_k/3, delete/2, length/2, watch_push/2, notify/2]).

-include("../include/dgen.hrl").

-define(VS(Tx, B), {
    versionstamp,
    16#ffffffffffffffff,
    16#ffff,
    B:get_next_tx_id(Tx)
}).

-if(?DOCATTRS).
-doc """
Pushes a list of items onto the queue atomically.

Each item is stored with a versionstamped key to ensure FIFO ordering.
""".
-endif.
-spec push_k(dgen_backend:tenant(), dgen:tuid(), [term()]) -> ok.
push_k(Tenant, Tuid, Items) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        QueueKey = get_queue_key(Tuid),
        ItemKey = get_item_key(QueueKey),
        ItemKey2 = dgen_key:extend(ItemKey, undefined),
        [
            B:set_versionstamped_key(
                Tx,
                B:dir_pack_vs(
                    Dir,
                    erlang:setelement(
                        tuple_size(ItemKey2),
                        ItemKey2,
                        ?VS(Tx, B)
                    )
                ),
                term_to_binary(Item)
            )
         || Item <- Items
        ],

        PushKey = get_push_key(QueueKey),
        B:add(Tx, B:dir_pack(Dir, PushKey), length(Items))
    end).

notify({Tx, Dir}, Tuid) ->
    B = dgen_config:backend(),
    QueueKey = get_queue_key(Tuid),
    PushKey = get_push_key(QueueKey),
    PopKey = get_pop_key(QueueKey),
    B:add(Tx, B:dir_pack(Dir, PushKey), 1),
    B:add(Tx, B:dir_pack(Dir, PopKey), 1).

-if(?DOCATTRS).
-doc """
Pops up to `K` items from the queue.

Returns `{Items, Watch}` where `Watch` is `undefined` if the queue still has
items, or a future that fires when new items are pushed.
""".
-endif.
-spec consume_k(dgen_backend:tenant(), pos_integer(), dgen:tuid()) ->
    {[term()], undefined | dgen_backend:future()}.
consume_k(Tenant, K, Tuid) ->
    dgen_backend:transactional(Tenant, fun(Td) ->
        case pop_k(Td, K, Tuid) of
            {ok, Vals} ->
                {Vals, undefined};
            {{error, empty}, Vals} ->
                {Vals, watch_push(Td, Tuid)}
        end
    end).

-if(?DOCATTRS).
-doc "Deletes the entire queue for the given `Tuid`, including all items and counters.".
-endif.
-spec delete(dgen_backend:tenant(), dgen:tuid()) -> ok.
delete(Tenant, Tuid) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        QueueKey = get_queue_key(Tuid),
        {SK, EK} = B:dir_range(Dir, QueueKey),
        B:clear_range(Tx, SK, EK)
    end).

-if(?DOCATTRS).
-doc "Returns the number of items currently in the queue (pushes minus pops).".
-endif.
-spec length(dgen_backend:tenant(), dgen:tuid()) -> non_neg_integer().
length(Tenant, Tuid) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        QueueKey = get_queue_key(Tuid),
        PushKey = get_push_key(QueueKey),
        PopKey = get_pop_key(QueueKey),
        F = [
            B:get(Tx, B:dir_pack(Dir, PushKey)),
            B:get(Tx, B:dir_pack(Dir, PopKey))
        ],
        [Push, Pop] = B:wait_for_all(F),
        decode_as_int(Push, 0) - decode_as_int(Pop, 0)
    end).

watch_push({Tx, Dir}, Tuid) ->
    B = dgen_config:backend(),
    QueueKey = get_queue_key(Tuid),
    PushKey = get_push_key(QueueKey),
    B:watch(Tx, B:dir_pack(Dir, PushKey)).

pop_k({Tx, Dir}, K, Tuid) ->
    B = dgen_config:backend(),
    QueueKey = get_queue_key(Tuid),
    ItemKey = get_item_key(QueueKey),
    {QS, QE} = B:dir_range(Dir, ItemKey),
    case B:get_range(Tx, QS, QE, [{limit, K}, {wait, true}]) of
        [] ->
            {{error, empty}, []};
        KVs = [{S, _} | _] ->
            N = length(KVs),
            {E, _} = lists:last(KVs),
            B:clear_range(Tx, S, B:key_strinc(E)),
            PopKey = get_pop_key(QueueKey),
            B:add(Tx, B:dir_pack(Dir, PopKey), N),

            Status =
                if
                    N == K -> ok;
                    true -> {error, empty}
                end,
            {Status, [binary_to_term(V) || {_, V} <- KVs]}
    end.

get_queue_key(Tuple) ->
    dgen_key:extend(Tuple, <<"q">>).

get_item_key(QueueKey) ->
    dgen_key:extend(QueueKey, <<"i">>).

get_push_key(QueueKey) ->
    dgen_key:extend(QueueKey, <<"n">>).

get_pop_key(QueueKey) ->
    dgen_key:extend(QueueKey, <<"p">>).

decode_as_int(not_found, Default) -> Default;
decode_as_int(Val, _Default) -> binary:decode_unsigned(Val, little).
