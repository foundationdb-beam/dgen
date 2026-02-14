-module(dgen_mod_state_codec).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Encoder/decoder for dgen_server mod_state.

Stores structured Erlang terms across FDB key-value pairs using a recursive
encoding scheme. Three encoding types are supported:

- **term** (fallback) - `term_to_binary`, chunked into 100KB values.
- **assigns map** - map with all atom keys; each entry stored at a
  separate FDB key using `atom_to_binary(Key)` in the path.
- **component list** - list of maps where every item has an atom `id`
  key with a binary value; each item stored separately, ordered by a
  fractional index embedded in the FDB key.

The encoding is applied recursively. For example, an assigns map whose value
is a component list will nest both encodings in the key path.

### Key structure

```
term:  {BaseKey, <<"t">>, ChunkIndex}
map:   {BaseKey, <<"m">>}                       (marker)
       {BaseKey, <<"m">>, KeyBin, ...}           (recursive)
list:  {BaseKey, <<"l">>}                        (marker, holds Id => FracIndex map)
       {BaseKey, <<"l">>, FracIndex, Id, ...}    (recursive)
```
""".
-endif.

-export([get/2, set/3, set/4, clear/2, write_term/3, read_term/2, clear_term/2, term_first_key/2]).

-type tenant() :: {term(), term()}.
-type base_key() :: tuple().
-type mod_state() :: term().

-define(CHUNK_SIZE, 100000).

%% Type tags used as the first sub-key element after BaseKey
-define(TAG_TERM, <<"t">>).
-define(TAG_MAP, <<"m">>).
-define(TAG_LIST, <<"l">>).

%%
%% Public API
%%

-if(?DOCATTRS).
-doc "Reads mod_state from the backend at `BaseKey`.".
-endif.
-spec get(tenant(), base_key()) -> {ok, mod_state()} | {error, not_found}.
get(Tenant, BaseKey) ->
    dgen_backend:transactional(Tenant, fun(Td) -> get_tx(Td, BaseKey) end).

get_tx({Tx, Dir}, BaseKey) ->
    B = dgen_config:backend(),
    {SK, EK} = B:dir_range(Dir, BaseKey),
    case B:get_range(Tx, SK, EK, [{wait, true}]) of
        [] ->
            {error, not_found};
        KVs ->
            decode_kvs(Dir, BaseKey, KVs)
    end.

-if(?DOCATTRS).
-doc "Full write of `ModState` at `BaseKey`. Clears existing data first.".
-endif.
-spec set(tenant(), base_key(), mod_state()) -> ok.
set(Tenant, BaseKey, ModState) ->
    dgen_backend:transactional(Tenant, fun(Td) ->
        clear_tx(Td, BaseKey),
        write(Td, BaseKey, ModState)
    end).

-if(?DOCATTRS).
-doc """
Diff-based partial write. Compares `OldModState` and `NewModState` and
only writes changed keys. Falls back to a full rewrite when the encoding
type changes or both values are plain terms.
""".
-endif.
-spec set(tenant(), base_key(), mod_state(), mod_state()) -> ok.
set(_Td, _BaseKey, Same, Same) ->
    ok;
set(Tenant, BaseKey, OldModState, NewModState) ->
    dgen_backend:transactional(Tenant, fun(Td) ->
        OldType = classify(OldModState),
        NewType = classify(NewModState),
        case OldType =:= NewType of
            false ->
                clear_tx(Td, BaseKey),
                write(Td, BaseKey, NewModState);
            true ->
                case OldType of
                    term ->
                        clear_tx(Td, BaseKey),
                        write(Td, BaseKey, NewModState);
                    map ->
                        diff_map(Td, BaseKey, OldModState, NewModState);
                    list ->
                        diff_list(Td, BaseKey, OldModState, NewModState)
                end
        end
    end).

-if(?DOCATTRS).
-doc "Clears all state keys under `BaseKey`.".
-endif.
-spec clear(tenant(), base_key()) -> ok.
clear(Tenant, BaseKey) ->
    dgen_backend:transactional(Tenant, fun(Td) -> clear_tx(Td, BaseKey) end).

clear_tx({Tx, Dir}, BaseKey) ->
    B = dgen_config:backend(),
    {SK, EK} = B:dir_range(Dir, BaseKey),
    B:clear_range(Tx, SK, EK).

-if(?DOCATTRS).
-doc """
Reads a term-encoded value at `BaseKey`.

Unlike `get/2`, this only reads the term encoding (`{BaseKey, <<"t">>, N}` keys)
and does not attempt to decode map or list encodings. Used by the call/reply
protocol to read chunked reply payloads.
""".
-endif.
-spec read_term(tenant(), base_key()) -> {ok, term()} | {error, not_found}.
read_term(Tenant, BaseKey) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        TermBaseKey = extend_key(BaseKey, ?TAG_TERM),
        {SK, EK} = B:dir_range(Dir, TermBaseKey),
        case B:get_range(Tx, SK, EK, [{wait, true}]) of
            [] -> {error, not_found};
            KVs -> decode_term(KVs)
        end
    end).

-if(?DOCATTRS).
-doc """
Clears the term-encoded keys under `BaseKey`.

Removes all `{BaseKey, <<"t">>, N}` chunk keys. Used by the call/reply
protocol to clear chunked reply payloads.
""".
-endif.
-spec clear_term(tenant(), base_key()) -> ok.
clear_term(Tenant, BaseKey) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        TermBaseKey = extend_key(BaseKey, ?TAG_TERM),
        {SK, EK} = B:dir_range(Dir, TermBaseKey),
        B:clear_range(Tx, SK, EK)
    end).

-if(?DOCATTRS).
-doc """
Returns the packed key for chunk 0 of a term-encoded payload at `BaseKey`.

This is the key that is written first by `write_term/3` and is suitable for
placing a watch on a term-encoded value.
""".
-endif.
-spec term_first_key(term(), base_key()) -> binary().
term_first_key(Dir, BaseKey) ->
    B = dgen_config:backend(),
    B:dir_pack(Dir, extend_key(BaseKey, ?TAG_TERM, 0)).

%%
%% Classification
%%

-type encoding_type() :: term | map | list.

%% Classify a term into its encoding type.
%% - map  : map with all atom keys (assigns map)
%% - list : list of maps each with a binary id key (component list)
%% - term : everything else (fallback)
-spec classify(term()) -> encoding_type().
classify(ModState) when is_map(ModState) ->
    case maps:size(ModState) of
        0 ->
            map;
        _ ->
            AllAtoms = maps:fold(
                fun
                    (K, _V, true) when is_atom(K) -> true;
                    (_K, _V, _Acc) -> false
                end,
                true,
                ModState
            ),
            case AllAtoms of
                true -> map;
                false -> term
            end
    end;
classify(ModState) when is_list(ModState) ->
    case ModState of
        [] ->
            list;
        _ ->
            case is_component_list(ModState) of
                true -> list;
                false -> term
            end
    end;
classify(_ModState) ->
    term.

-spec is_component_list(list()) -> boolean().
is_component_list(List) ->
    lists:all(
        fun
            (Item) when is_map(Item) ->
                case maps:find(id, Item) of
                    {ok, Id} when is_binary(Id) -> true;
                    _ -> false
                end;
            (_) ->
                false
        end,
        List
    ).

%%
%% Write (internal, does not clear)
%%

-spec write(tenant(), base_key(), mod_state()) -> ok.
write(Td, BaseKey, ModState) ->
    case classify(ModState) of
        term -> write_term(Td, BaseKey, ModState);
        map -> write_map(Td, BaseKey, ModState);
        list -> write_list(Td, BaseKey, ModState)
    end.

write_term({Tx, Dir}, BaseKey, ModState) ->
    B = dgen_config:backend(),
    Bin = term_to_binary(ModState),
    Chunks = binary_chunk_every(Bin, ?CHUNK_SIZE, []),
    TermBaseKey = extend_key(BaseKey, ?TAG_TERM),
    write_chunks(B, Tx, Dir, TermBaseKey, Chunks, 0),
    ok.

write_chunks(_B, _Tx, _Dir, _BaseKey, [], _Idx) ->
    ok;
write_chunks(B, Tx, Dir, BaseKey, [Chunk | Rest], Idx) ->
    Key = extend_key(BaseKey, Idx),
    B:set(Tx, B:dir_pack(Dir, Key), Chunk),
    write_chunks(B, Tx, Dir, BaseKey, Rest, Idx + 1).

write_map({Tx, Dir} = Td, BaseKey, ModState) ->
    B = dgen_config:backend(),
    MarkerKey = extend_key(BaseKey, ?TAG_MAP),
    B:set(Tx, B:dir_pack(Dir, MarkerKey), <<>>),
    maps:foreach(
        fun(AtomKey, Value) ->
            SubBaseKey = extend_key(BaseKey, ?TAG_MAP, atom_to_binary(AtomKey)),
            write(Td, SubBaseKey, Value)
        end,
        ModState
    ),
    ok.

write_list({Tx, Dir} = Td, BaseKey, ModState) ->
    B = dgen_config:backend(),
    MarkerKey = extend_key(BaseKey, ?TAG_LIST),
    case ModState of
        [] ->
            B:set(Tx, B:dir_pack(Dir, MarkerKey), term_to_binary(#{})),
            ok;
        _ ->
            N = length(ModState),
            FracIndices = dgen_frac_index:n_first(N),
            ItemsWithIndex = lists:zip(ModState, FracIndices),
            OrderMap = maps:from_list([{maps:get(id, Item), FI} || {Item, FI} <- ItemsWithIndex]),
            B:set(Tx, B:dir_pack(Dir, MarkerKey), term_to_binary(OrderMap)),
            lists:foreach(
                fun({#{id := Id} = Item, FI}) ->
                    SubBaseKey = extend_key(BaseKey, ?TAG_LIST, FI, Id),
                    write(Td, SubBaseKey, Item)
                end,
                ItemsWithIndex
            ),
            ok
    end.

%%
%% Decode
%%

-spec decode_kvs(term(), base_key(), [{binary(), binary()}]) ->
    {ok, mod_state()}.
decode_kvs(Dir, BaseKey, KVs) ->
    B = dgen_config:backend(),
    BaseSize = tuple_size(BaseKey),
    {FirstPackedKey, _FirstVal} = hd(KVs),
    FirstTuple = B:dir_unpack(Dir, FirstPackedKey),
    Tag = element(BaseSize + 1, FirstTuple),
    case Tag of
        ?TAG_TERM -> decode_term(KVs);
        ?TAG_MAP -> decode_map(B, Dir, BaseKey, KVs);
        ?TAG_LIST -> decode_list(B, Dir, BaseKey, KVs)
    end.

decode_term(KVs) ->
    {_, Vs} = lists:unzip(KVs),
    {ok, binary_to_term(iolist_to_binary(Vs))}.

decode_map(B, Dir, BaseKey, KVs) ->
    BaseSize = tuple_size(BaseKey),
    MarkerSize = BaseSize + 1,
    Unpacked = [{B:dir_unpack(Dir, PK), PK, V} || {PK, V} <- KVs],
    DataKVs = [{T, PK, V} || {T, PK, V} <- Unpacked, tuple_size(T) > MarkerSize],
    case DataKVs of
        [] ->
            {ok, #{}};
        _ ->
            Groups = group_by_pos(BaseSize + 2, DataKVs),
            Map = maps:from_list([
                begin
                    AtomKey = binary_to_atom(GroupKey),
                    SubBaseKey = extend_key(BaseKey, ?TAG_MAP, GroupKey),
                    SubKVs = [{PK, V} || {_T, PK, V} <- GroupKVs],
                    {ok, Value} = decode_kvs(Dir, SubBaseKey, SubKVs),
                    {AtomKey, Value}
                end
             || {GroupKey, GroupKVs} <- Groups
            ]),
            {ok, Map}
    end.

decode_list(B, Dir, BaseKey, KVs) ->
    BaseSize = tuple_size(BaseKey),
    MarkerSize = BaseSize + 1,
    Unpacked = [{B:dir_unpack(Dir, PK), PK, V} || {PK, V} <- KVs],
    DataKVs = [{T, PK, V} || {T, PK, V} <- Unpacked, tuple_size(T) > MarkerSize],
    case DataKVs of
        [] ->
            {ok, []};
        _ ->
            %% Group by FracIndex (BaseSize+2). Each frac index maps to one item.
            %% FDB returns keys sorted by (FracIndex, Id, ...), so groups are
            %% already in the correct list order -- no sort needed.
            Groups = group_by_pos(BaseSize + 2, DataKVs),
            Items = [
                begin
                    {FirstTuple, _FPK, _FV} = hd(GroupKVs),
                    GroupId = element(BaseSize + 3, FirstTuple),
                    FracIndex = element(BaseSize + 2, FirstTuple),
                    SubBaseKey = extend_key(BaseKey, ?TAG_LIST, FracIndex, GroupId),
                    SubKVs = [{PK, V} || {_T, PK, V} <- GroupKVs],
                    {ok, Item} = decode_kvs(Dir, SubBaseKey, SubKVs),
                    Item
                end
             || {_GroupFI, GroupKVs} <- Groups
            ],
            {ok, Items}
    end.

%%
%% Diff-based writes
%%

-spec diff_map(tenant(), base_key(), map(), map()) -> ok.
diff_map(Td, BaseKey, OldMap, NewMap) ->
    OldKeys = maps:keys(OldMap),
    NewKeys = maps:keys(NewMap),
    Removed = OldKeys -- NewKeys,
    Added = NewKeys -- OldKeys,
    Common = NewKeys -- Added,
    lists:foreach(
        fun(AtomKey) ->
            SubBaseKey = extend_key(BaseKey, ?TAG_MAP, atom_to_binary(AtomKey)),
            clear_tx(Td, SubBaseKey)
        end,
        Removed
    ),
    lists:foreach(
        fun(AtomKey) ->
            SubBaseKey = extend_key(BaseKey, ?TAG_MAP, atom_to_binary(AtomKey)),
            write(Td, SubBaseKey, maps:get(AtomKey, NewMap))
        end,
        Added
    ),
    lists:foreach(
        fun(AtomKey) ->
            OldVal = maps:get(AtomKey, OldMap),
            NewVal = maps:get(AtomKey, NewMap),
            SubBaseKey = extend_key(BaseKey, ?TAG_MAP, atom_to_binary(AtomKey)),
            set_tx(Td, SubBaseKey, OldVal, NewVal)
        end,
        Common
    ),
    ok.

%% Internal diff-set that operates on an already-transactional tenant
set_tx(_Td, _BaseKey, Same, Same) ->
    ok;
set_tx(Td, BaseKey, OldModState, NewModState) ->
    OldType = classify(OldModState),
    NewType = classify(NewModState),
    case OldType =:= NewType of
        false ->
            clear_tx(Td, BaseKey),
            write(Td, BaseKey, NewModState);
        true ->
            case OldType of
                term ->
                    clear_tx(Td, BaseKey),
                    write(Td, BaseKey, NewModState);
                map ->
                    diff_map(Td, BaseKey, OldModState, NewModState);
                list ->
                    diff_list(Td, BaseKey, OldModState, NewModState)
            end
    end.

-spec diff_list(tenant(), base_key(), list(), list()) -> ok.
diff_list({Tx, Dir} = Td, BaseKey, OldList, NewList) ->
    B = dgen_config:backend(),
    OldById = maps:from_list([{maps:get(id, Item), Item} || Item <- OldList]),
    NewById = maps:from_list([{maps:get(id, Item), Item} || Item <- NewList]),
    OldIds = maps:keys(OldById),
    NewIds = [maps:get(id, Item) || Item <- NewList],
    RemovedIds = OldIds -- NewIds,
    AddedIds = NewIds -- OldIds,
    CommonIds = NewIds -- AddedIds,

    %% Read current order map (Id => FracIndex) from the marker key
    MarkerKey = extend_key(BaseKey, ?TAG_LIST),
    PackedMarker = B:dir_pack(Dir, MarkerKey),
    OldOrderMap =
        case B:wait(B:get(Tx, PackedMarker)) of
            not_found -> #{};
            OrderBin -> binary_to_term(OrderBin)
        end,

    %% Remove deleted items from order map
    OrderMap1 = maps:without(RemovedIds, OldOrderMap),

    %% Build new order map with fractional indices for new items
    OrderMap2 = assign_new_indices(NewList, AddedIds, OrderMap1),

    %% Write updated order map
    B:set(Tx, PackedMarker, term_to_binary(OrderMap2)),

    %% Clear removed items (using old frac index in key path)
    lists:foreach(
        fun(Id) ->
            OldFI = maps:get(Id, OldOrderMap),
            SubBaseKey = extend_key(BaseKey, ?TAG_LIST, OldFI, Id),
            clear_tx(Td, SubBaseKey)
        end,
        RemovedIds
    ),

    %% Write added items (using new frac index in key path)
    lists:foreach(
        fun(Id) ->
            NewFI = maps:get(Id, OrderMap2),
            SubBaseKey = extend_key(BaseKey, ?TAG_LIST, NewFI, Id),
            write(Td, SubBaseKey, maps:get(Id, NewById))
        end,
        AddedIds
    ),

    %% Diff common items (using existing frac index in key path)
    lists:foreach(
        fun(Id) ->
            OldItem = maps:get(Id, OldById),
            NewItem = maps:get(Id, NewById),
            FI = maps:get(Id, OldOrderMap),
            SubBaseKey = extend_key(BaseKey, ?TAG_LIST, FI, Id),
            set_tx(Td, SubBaseKey, OldItem, NewItem)
        end,
        CommonIds
    ),

    ok.

%% Walk the new list in order and assign fractional indices to newly added
%% items, placing each between its neighbours' existing indices.
-spec assign_new_indices(list(), [binary()], map()) -> map().
assign_new_indices(NewList, AddedIds, OrderMap) ->
    AddedSet = sets:from_list(AddedIds),
    Ordered = [
        {
            maps:get(id, Item),
            case sets:is_element(maps:get(id, Item), AddedSet) of
                true -> undefined;
                false -> maps:get(maps:get(id, Item), OrderMap)
            end
        }
     || Item <- NewList
    ],
    assign_gaps(Ordered, OrderMap).

assign_gaps(Ordered, OrderMap) ->
    assign_gaps(Ordered, OrderMap, undefined).

assign_gaps([], OrderMap, _PrevFI) ->
    OrderMap;
assign_gaps([{Id, undefined} | Rest], OrderMap, PrevFI) ->
    NextFI = find_next_defined(Rest),
    NewFI =
        case {PrevFI, NextFI} of
            {undefined, undefined} -> dgen_frac_index:first();
            {undefined, Next} -> dgen_frac_index:before(Next);
            {Prev, undefined} -> dgen_frac_index:after_(Prev);
            {Prev, Next} -> dgen_frac_index:between(Prev, Next)
        end,
    assign_gaps(Rest, OrderMap#{Id => NewFI}, NewFI);
assign_gaps([{_Id, FI} | Rest], OrderMap, _PrevFI) ->
    assign_gaps(Rest, OrderMap, FI).

find_next_defined([]) ->
    undefined;
find_next_defined([{_Id, undefined} | Rest]) ->
    find_next_defined(Rest);
find_next_defined([{_Id, FI} | _Rest]) ->
    FI.

%%
%% Helpers
%%

-spec extend_key(tuple(), term()) -> tuple().
extend_key(BaseKey, Element) ->
    erlang:insert_element(1 + tuple_size(BaseKey), BaseKey, Element).

-spec extend_key(tuple(), term(), term()) -> tuple().
extend_key(BaseKey, E1, E2) ->
    extend_key(extend_key(BaseKey, E1), E2).

-spec extend_key(tuple(), term(), term(), term()) -> tuple().
extend_key(BaseKey, E1, E2, E3) ->
    extend_key(extend_key(BaseKey, E1, E2), E3).

%% Group a sorted list of pre-unpacked KVs by the element at Pos.
%% Relies on FDB key ordering to keep groups contiguous.
-spec group_by_pos(pos_integer(), [{tuple(), binary(), binary()}]) ->
    [{term(), [{tuple(), binary(), binary()}]}].
group_by_pos(Pos, UnpackedKVs) ->
    group_by_pos(Pos, UnpackedKVs, []).

group_by_pos(_Pos, [], Acc) ->
    [{G, lists:reverse(Items)} || {G, Items} <- lists:reverse(Acc)];
group_by_pos(Pos, [{Tuple, PK, V} | Rest], [{GroupKey, Items} | AccRest]) when
    element(Pos, Tuple) =:= GroupKey
->
    group_by_pos(Pos, Rest, [{GroupKey, [{Tuple, PK, V} | Items]} | AccRest]);
group_by_pos(Pos, [{Tuple, PK, V} | Rest], Acc) ->
    GroupKey = element(Pos, Tuple),
    group_by_pos(Pos, Rest, [{GroupKey, [{Tuple, PK, V}]} | Acc]).

-spec binary_chunk_every(binary(), pos_integer(), [binary()]) -> [binary()].
binary_chunk_every(<<>>, _Size, Acc) ->
    lists:reverse(Acc);
binary_chunk_every(Bin, Size, Acc) ->
    case Bin of
        <<Chunk:Size/binary, Rest/binary>> ->
            binary_chunk_every(Rest, Size, [Chunk | Acc]);
        Chunk ->
            lists:reverse([Chunk | Acc])
    end.
