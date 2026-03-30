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

-export([
    push_k/3,
    peek_k/3,
    consume_peeked/3,
    update_peeked/3,
    push_dlq/4,
    peek_dlq/2,
    dlq_length/2,
    delete_dlq_entry/2,
    purge_dlq/2,
    requeue_dlq_entry/3,
    delete/2,
    length/2,
    watch_push/2,
    notify/2
]).

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
Reads up to `K` items from the queue without removing them.

Returns `{ok, [{RawKey, RawBin}]}` where `RawBin` is the raw encoded value,
or `{error, empty}` when the queue is empty. The items remain in the queue
until `consume_peeked/3` is called. On failure `update_peeked/3` can overwrite a key
in-place within the same transaction to update the embedded attempt counter.

All three operations must be called within the same transaction so that the
read, the callback invocation, and the delete-or-update are atomic.
""".
-endif.
-spec peek_k(dgen_backend:tenant(), pos_integer(), quid()) ->
    {ok, [{dgen_backend:key(), binary()}]} | {error, empty}.
peek_k({Tx, Dir}, K, Quid) ->
    B = dgen_config:backend(),
    ItemKey = get_item_key(Quid),
    {QS, QE} = B:dir_range(Dir, ItemKey),
    case B:get_range(Tx, QS, QE, [{limit, K}, {wait, true}]) of
        [] -> {error, empty};
        KVs -> {ok, KVs}
    end.

-if(?DOCATTRS).
-doc """
Deletes the items returned by `peek_k/3` and increments the pop counter.

Call this within the same transaction as `peek_k/3` after the callback
succeeds to commit the consume.
""".
-endif.
-spec consume_peeked(dgen_backend:tenant(), [{dgen_backend:key(), binary()}], quid()) -> ok.
consume_peeked({Tx, Dir}, KVs, Quid) ->
    B = dgen_config:backend(),
    N = length(KVs),
    [{FirstKey, _} | _] = KVs,
    {LastKey, _} = lists:last(KVs),
    B:clear_range(Tx, FirstKey, B:key_strinc(LastKey)),
    PopKey = get_pop_key(Quid),
    B:add(Tx, B:dir_pack(Dir, PopKey), N).

-if(?DOCATTRS).
-doc """
Overwrites the value at `Key` within an existing transaction.

Call this within the same transaction as `peek_k/3` when the callback fails,
to update the embedded attempt counter before the transaction commits. The
updated message will be visible to the next consumer.
""".
-endif.
-spec update_peeked(dgen_backend:tenant(), dgen_backend:key(), term()) -> ok.
update_peeked({Tx, _Dir}, Key, Envelope) ->
    B = dgen_config:backend(),
    B:set(Tx, Key, term_to_binary(Envelope)).

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
-doc """
Returns all entries in the dead-letter queue for `Quid`.

Each entry is `{Key, Envelope, AttemptCount, TimestampMs}` where `Key` can be
passed to `delete_dlq_entry/2` or `requeue_dlq_entry/3`.
""".
-endif.
-spec peek_dlq(dgen_backend:tenant(), quid()) ->
    [{dgen_backend:key(), term(), non_neg_integer(), integer()}].
peek_dlq(Tenant, Quid) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        DlqKey = get_dlq_key(Quid),
        {DS, DE} = B:dir_range(Dir, DlqKey),
        KVs = B:get_range(Tx, DS, DE, [{wait, true}]),
        [
            begin
                {Envelope, AttemptCount, TimestampMs} = binary_to_term(V),
                {K, Envelope, AttemptCount, TimestampMs}
            end
         || {K, V} <- KVs
        ]
    end).

-if(?DOCATTRS).
-doc "Returns the number of entries currently in the dead-letter queue for `Quid`.".
-endif.
-spec dlq_length(dgen_backend:tenant(), quid()) -> non_neg_integer().
dlq_length(Tenant, Quid) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        DlqKey = get_dlq_key(Quid),
        {DS, DE} = B:dir_range(Dir, DlqKey),
        erlang:length(B:get_range(Tx, DS, DE, [{wait, true}]))
    end).

-if(?DOCATTRS).
-doc """
Deletes a single dead-letter entry by its key.

`Key` is the first element of a tuple returned by `peek_dlq/2`.
""".
-endif.
-spec delete_dlq_entry(dgen_backend:tenant(), dgen_backend:key()) -> ok.
delete_dlq_entry(Tenant, Key) ->
    dgen_backend:transactional(Tenant, fun({Tx, _Dir}) ->
        B = dgen_config:backend(),
        B:clear_range(Tx, Key, B:key_strinc(Key))
    end).

-if(?DOCATTRS).
-doc "Deletes all entries in the dead-letter queue for `Quid`.".
-endif.
-spec purge_dlq(dgen_backend:tenant(), quid()) -> ok.
purge_dlq(Tenant, Quid) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        DlqKey = get_dlq_key(Quid),
        {DS, DE} = B:dir_range(Dir, DlqKey),
        B:clear_range(Tx, DS, DE)
    end).

-if(?DOCATTRS).
-doc """
Moves a dead-letter entry back onto the main queue, resetting the attempt count to 0.

Atomically reads the entry at `Key`, pushes its envelope back onto the main
queue with attempt count 0, and deletes the DLQ entry. Returns
`{error, not_found}` if the key no longer exists.

`Key` is the first element of a tuple returned by `peek_dlq/2`.
""".
-endif.
-spec requeue_dlq_entry(dgen_backend:tenant(), quid(), dgen_backend:key()) ->
    ok | {error, not_found}.
requeue_dlq_entry(Tenant, Quid, DlqKey) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        case B:wait(B:get(Tx, DlqKey)) of
            not_found ->
                {error, not_found};
            Bin ->
                {Envelope, _AttemptCount, _Ts} = binary_to_term(Bin),
                ItemKey = get_item_key(Quid),
                ItemKey2 = dgen_key:extend(ItemKey, undefined),
                NewKey = B:dir_pack_vs(
                    Dir,
                    erlang:setelement(tuple_size(ItemKey2), ItemKey2, ?VS(Tx, B))
                ),
                B:set_versionstamped_key(
                    Tx, NewKey, term_to_binary(reset_envelope(Envelope))
                ),
                PushKey = get_push_key(Quid),
                B:add(Tx, B:dir_pack(Dir, PushKey), 1),
                B:clear_range(Tx, DlqKey, B:key_strinc(DlqKey))
        end
    end).

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

reset_envelope({cast, R}) -> {cast, R, 0};
reset_envelope({call, R, F}) -> {call, R, F, [], 0}.
