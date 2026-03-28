-module(dgen_queue).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Durable FIFO queue backed by the configured dgen backend.

Each dgen_server has its own queue keyed under `Quid`. Items are
ordered by versionstamps, guaranteeing strict FIFO across transactions.
Push and pop counts are tracked with atomic `add` operations for O(1) length.
""".
-endif.

-export([push_k/3, consume_k/3, consume_k_ext/3, push_dlq/4, update_message/3, delete/2, length/2, watch_push/2, notify/2]).

-include("../include/dgen.hrl").

% the queue data structure is internally versioned
-define(QueueVersion, 0).

-define(VS(Tx, B), {
    versionstamp,
    16#ffffffffffffffff,
    16#ffff,
    B:get_next_tx_id(Tx)
}).

-type quid() :: tuple().

-export_type([quid/0]).

-if(?DOCATTRS).
-doc """
Pushes a list of items onto the queue atomically.

Each item is stored with a versionstamped key to ensure FIFO ordering.
""".
-endif.
-spec push_k(dgen_backend:tenant(), quid(), [term()]) -> ok.
push_k(Tenant, Quid, Items) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        ItemKey = get_item_key(Quid),
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

        PushKey = get_push_key(Quid),
        B:add(Tx, B:dir_pack(Dir, PushKey), length(Items))
    end).

notify({Tx, Dir}, Quid) ->
    B = dgen_config:backend(),
    PushKey = get_push_key(Quid),
    PopKey = get_pop_key(Quid),
    B:add(Tx, B:dir_pack(Dir, PushKey), 1),
    B:add(Tx, B:dir_pack(Dir, PopKey), 1).

-if(?DOCATTRS).
-doc """
Pops up to `K` items from the queue.

Returns `{Items, Watch}` where `Watch` is `undefined` if the queue still has
items, or a future that fires when new items are pushed.
""".
-endif.
-spec consume_k(dgen_backend:tenant(), pos_integer(), quid()) ->
    {[term()], undefined | dgen_backend:future()}.
consume_k(Tenant, K, Quid) ->
    dgen_backend:transactional(Tenant, fun(Td) ->
        case pop_k(Td, K, Quid) of
            {ok, KVs} ->
                {[V || {_, V} <- KVs], undefined};
            {{error, empty}, KVs} ->
                {[V || {_, V} <- KVs], watch_push(Td, Quid)}
        end
    end).

-if(?DOCATTRS).
-doc """
Like `consume_k/3` but also returns the raw FDB key for each item.

Returns `{[{RawKey, Value}], Watch}` where `RawKey` is the packed FDB key of
the message. The key can be used to update the message in-place via
`update_message/3`.
""".
-endif.
-spec consume_k_ext(dgen_backend:tenant(), pos_integer(), quid()) ->
    {[{dgen_backend:key(), term()}], undefined | dgen_backend:future()}.
consume_k_ext(Tenant, K, Quid) ->
    dgen_backend:transactional(Tenant, fun(Td) ->
        case pop_k(Td, K, Quid) of
            {ok, KVs} ->
                {KVs, undefined};
            {{error, empty}, KVs} ->
                {KVs, watch_push(Td, Quid)}
        end
    end).

-if(?DOCATTRS).
-doc """
Writes an updated value to an existing message's FDB key in-place.

Used to increment the attempt count embedded in a message envelope after a
failed processing attempt. No-ops if the key no longer exists (meaning
the message was successfully consumed by another consumer).
""".
-endif.
-spec update_message(dgen_backend:tenant(), dgen_backend:key(), term()) -> ok.
update_message(Tenant, RawKey, NewEnvelope) ->
    dgen_backend:transactional(Tenant, fun({Tx, _Dir}) ->
        B = dgen_config:backend(),
        case B:wait(B:get(Tx, RawKey)) of
            not_found -> ok;
            _ -> B:set(Tx, RawKey, term_to_binary(NewEnvelope))
        end
    end).

-if(?DOCATTRS).
-doc """
Appends an entry to the dead-letter queue for `Quid`.

Called when a message exceeds its dead-letter threshold. Stores the original
envelope, attempt count, and a millisecond timestamp as a versionstamped entry
under the DLQ subspace for the queue.
""".
-endif.
-spec push_dlq(dgen_backend:tenant(), quid(), term(), non_neg_integer()) -> ok.
push_dlq({Tx, Dir}, Quid, Envelope, AttemptCount) ->
    B = dgen_config:backend(),
    DlqKey = get_dlq_key(Quid),
    DlqKey2 = dgen_key:extend(DlqKey, undefined),
    Key = B:dir_pack_vs(
        Dir,
        erlang:setelement(tuple_size(DlqKey2), DlqKey2, ?VS(Tx, B))
    ),
    Payload = term_to_binary({Envelope, AttemptCount, erlang:system_time(millisecond)}),
    B:set_versionstamped_key(Tx, Key, Payload).

-if(?DOCATTRS).
-doc "Deletes the entire queue for the given `Quid`, including all items and counters.".
-endif.
-spec delete(dgen_backend:tenant(), quid()) -> ok.
delete(Tenant, Quid) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        {SK, EK} = B:dir_range(Dir, Quid),
        B:clear_range(Tx, SK, EK)
    end).

-if(?DOCATTRS).
-doc "Returns the number of items currently in the queue (pushes minus pops).".
-endif.
-spec length(dgen_backend:tenant(), quid()) -> non_neg_integer().
length(Tenant, Quid) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        PushKey = get_push_key(Quid),
        PopKey = get_pop_key(Quid),
        F = [
            B:get(Tx, B:dir_pack(Dir, PushKey)),
            B:get(Tx, B:dir_pack(Dir, PopKey))
        ],
        [Push, Pop] = B:wait_for_all(F),
        decode_as_int(Push, 0) - decode_as_int(Pop, 0)
    end).

watch_push({Tx, Dir}, Quid) ->
    B = dgen_config:backend(),
    PushKey = get_push_key(Quid),
    B:watch(Tx, B:dir_pack(Dir, PushKey)).

pop_k({Tx, Dir}, K, Quid) ->
    B = dgen_config:backend(),
    ItemKey = get_item_key(Quid),
    {QS, QE} = B:dir_range(Dir, ItemKey),
    case B:get_range(Tx, QS, QE, [{limit, K}, {wait, true}]) of
        [] ->
            {{error, empty}, []};
        KVs = [{S, _} | _] ->
            N = length(KVs),
            {E, _} = lists:last(KVs),
            B:clear_range(Tx, S, B:key_strinc(E)),
            PopKey = get_pop_key(Quid),
            B:add(Tx, B:dir_pack(Dir, PopKey), N),

            Status =
                if
                    N == K -> ok;
                    true -> {error, empty}
                end,
            {Status, [{S, binary_to_term(V)} || {S, V} <- KVs]}
    end.

get_item_key(Quid) ->
    dgen_key:extend(Quid, ?QueueVersion, <<"i">>).

get_dlq_key(Quid) ->
    dgen_key:extend(Quid, ?QueueVersion, <<"d">>).

get_push_key(Quid) ->
    dgen_key:extend(Quid, ?QueueVersion, <<"n">>).

get_pop_key(Quid) ->
    dgen_key:extend(Quid, ?QueueVersion, <<"p">>).

decode_as_int(not_found, Default) -> Default;
decode_as_int(Val, _Default) -> binary:decode_unsigned(Val, little).
