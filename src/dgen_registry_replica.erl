-module(dgen_registry_replica).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% A member's copy of the registry's name table, and the rules for advancing it.
%% `§N.N` refers to docs/design/dgen_registry_design.md.
%%
%% A replica is a position in the leader's totally ordered stream of committed batches
%% plus the content it implies: `tab` (rows `{Name, Pid, Index, Data}` in a protected
%% ETS table, read lock-free by any process), `index` (attribute -> value -> names,
%% derived from the rows), `version` (commit version applied up to) and `epoch` (the
%% leadership term followed). Commit versions are monotonic across terms, so replicas
%% compare by version alone.
%%
%% Invariant: two replicas at the same version hold the same content. Freshest-wins
%% handoff reconstruction (§5.7) depends on it; every admission rule protects it.
%%
%% Admission mirrors formal/DgenRegistryReplication.tla action for action:
%%   offer_batch/5     RecvBcastApply / RecvBcastDrop / RecvBcastGap
%%   offer_snapshot/4  RecvSnap (with the SafeAssumeSnap guard)
%% An older-epoch batch is dropped; a newer-epoch batch is applied but does not raise
%% `epoch`, while a current-or-newer snapshot does. The asymmetry is the model's and is
%% unreachable: a new leader snapshots each follower before broadcasting to it (FIFO).
%%
%% Pure: no sends, timers or replies; results tell the member what happened.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([new/1, tab/1, version/1, epoch/1]).
-export([offer_batch/5, offer_snapshot/4]).
-export([adopt/4, commit/5, insert/5, delete/2]).
-export([lookup_pid/2, lookup_metadata/2, exists/2, records/1, encode/1]).
-export([query/2, query_constraints/2, query_match_names/2, query_satisfies/2]).
-export([encode_records/1, decode_records/1]).

-export_type([t/0, records/0, bcast_op/0, query/0]).

-record(replica, {
    tab :: ets:tid() | atom(),
    index = #{} :: #{term() => #{term() => #{term() => []}}},
    version = 0 :: non_neg_integer(),
    epoch = 0 :: non_neg_integer()
}).

-opaque t() :: #replica{}.

%% Replica content as a map, as carried by the handoff gather and snapshot fan-out.
-type records() :: #{term() => {pid(), map(), term()}}.

%% One op of a `{names_batch, …}` broadcast, epoch already stripped to the envelope.
-type bcast_op() ::
    {name_registered, term(), pid(), map(), term()}
    | {metadata_set, term(), map(), term()}
    | {name_unregistered, term(), pid() | undefined}.

%% A tagged query (§4.9).  Only `{all, Constraints}` (AND-equal) exists today.
-type query() :: {all, #{term() => term()}}.

%% ---------------------------------------------------------------------------
%% Construction and accessors
%% ---------------------------------------------------------------------------

%% Created empty on every member (re)start, with no heir: a crashed member's replica
%% is rebuilt from the leader's snapshot.
-spec new(atom()) -> t().
new(TabName) ->
    Tab = ets:new(TabName, [named_table, set, protected, {read_concurrency, true}]),
    #replica{tab = Tab}.

-spec tab(t()) -> ets:tid() | atom().
tab(#replica{tab = Tab}) -> Tab.

-spec version(t()) -> non_neg_integer().
version(#replica{version = V}) -> V.

-spec epoch(t()) -> non_neg_integer().
epoch(#replica{epoch = E}) -> E.

%% ---------------------------------------------------------------------------
%% Stream admission
%% ---------------------------------------------------------------------------

%% Offer one broadcast batch. Applied only if contiguous (`PrevV` is our version), so
%% the replica stays a prefix of the leader's stream; on `gap` nothing is applied and
%% the caller requests a snapshot. A batch is one message, so a lost batch always
%% leaves a gap. The leader's heartbeat is an empty batch at `{V, V}`.
-spec offer_batch(non_neg_integer(), non_neg_integer(), non_neg_integer(), [bcast_op()], t()) ->
    {applied, t()} | stale_epoch | duplicate | gap.
%% MUTATION_PARTIAL_BATCH (test-only, never in a release): re-admits per-op messages
%% sharing a version, reintroducing undetectable partially delivered batches.
-ifdef(MUTATION_PARTIAL_BATCH).
offer_batch(Epoch, PrevV, V, Ops, R = #replica{epoch = CurEpoch, version = Applied}) ->
    if
        Epoch < CurEpoch -> stale_epoch;
        PrevV =:= Applied -> {applied, apply_ops(Ops, R#replica{version = max(Applied, V)})};
        V =:= Applied -> {applied, apply_ops(Ops, R)};
        V =< Applied -> duplicate;
        true -> gap
    end.
-else.
offer_batch(Epoch, PrevV, V, Ops, R = #replica{epoch = CurEpoch, version = Applied}) ->
    if
        Epoch < CurEpoch -> stale_epoch;
        PrevV =:= Applied -> {applied, apply_ops(Ops, R#replica{version = max(Applied, V)})};
        V =< Applied -> duplicate;
        true -> gap
    end.
-endif.

%% Offer a wholesale snapshot (handoff fan-out, joiner onboarding, resync answer). The
%% version guard stops a snapshot gathered before a batch but delivered after it from
%% rolling the replica back (§5.7). Equal versions are accepted: equal content.
-spec offer_snapshot(non_neg_integer(), non_neg_integer(), records(), t()) ->
    {accepted, t()} | stale.
offer_snapshot(Epoch, V, Records, R = #replica{epoch = CurEpoch, version = Applied}) ->
    case Epoch >= CurEpoch andalso V >= Applied of
        true -> {accepted, adopt(Epoch, V, Records, R)};
        false -> stale
    end.

%% ---------------------------------------------------------------------------
%% Authoring (the leader's own replica)
%% ---------------------------------------------------------------------------

%% Re-baseline unconditionally. The table is read lock-free meanwhile, so new rows are
%% written first and only then are absent names deleted: a reader never sees a bound
%% name go missing.
-spec adopt(non_neg_integer(), non_neg_integer(), records(), t()) -> t().
adopt(Epoch, V, Records, R = #replica{tab = Tab}) ->
    ets:insert(
        Tab, [{Name, Pid, Index, Data} || {Name, {Pid, Index, Data}} <- maps:to_list(Records)]
    ),
    Absent = ets:foldl(
        fun({Name, _Pid, _Index, _Data}, Acc) ->
            case is_map_key(Name, Records) of
                true -> Acc;
                false -> [Name | Acc]
            end
        end,
        [],
        Tab
    ),
    lists:foreach(fun(Name) -> ets:delete(Tab, Name) end, Absent),
    R#replica{index = index_rebuild(Records), version = V, epoch = Epoch}.

%% Apply the leader's own committed batch at `V`. Only names in `DBOp` are touched, so
%% a concurrent optimistic delete is not clobbered, and a metadata-only op never
%% resurrects a row removed between plan and apply.
-spec commit(non_neg_integer(), map(), map(), map(), t()) -> t().
commit(V, DBOp, WNames, WMeta, R) ->
    R1 = maps:fold(
        fun
            (Name, clear, Acc) ->
                delete(Acc, Name);
            (Name, {set, _PidNode}, Acc) ->
                {Index, Data} = maps:get(Name, WMeta),
                insert(Acc, Name, maps:get(Name, WNames), Index, Data);
            (Name, {meta, _PidNode}, Acc) ->
                {Index, Data} = maps:get(Name, WMeta),
                update_meta(Acc, Name, Index, Data)
        end,
        R,
        DBOp
    ),
    R1#replica{version = V}.

%% Row writers outside the stream (the optimistic local delete on unregister, the
%% legacy pre-version reply path's insert). Neither moves the version.
-spec insert(t(), term(), pid(), map(), term()) -> t().
insert(R = #replica{tab = Tab, index = Inv}, Name, Pid, Index, Data) ->
    Old = row_index(Tab, Name),
    ets:insert(Tab, {Name, Pid, Index, Data}),
    R#replica{index = index_add(index_remove(Inv, Name, Old), Name, Index)}.

-spec delete(t(), term()) -> t().
delete(R = #replica{tab = Tab, index = Inv}, Name) ->
    Old = row_index(Tab, Name),
    ets:delete(Tab, Name),
    R#replica{index = index_remove(Inv, Name, Old)}.

%% ---------------------------------------------------------------------------
%% Reads
%% ---------------------------------------------------------------------------

-spec lookup_pid(t(), term()) -> pid() | undefined.
lookup_pid(#replica{tab = Tab}, Name) ->
    case ets:lookup(Tab, Name) of
        [{_Name, Pid, _Index, _Data} | _] -> Pid;
        [] -> undefined
    end.

%% The public `get_metadata` shape, `{ok, #{pid, index, data}}` or `undefined`.
-spec lookup_metadata(t(), term()) -> {ok, map()} | undefined.
lookup_metadata(#replica{tab = Tab}, Name) ->
    case ets:lookup(Tab, Name) of
        [{_Name, Pid, Index, Data} | _] -> {ok, #{pid => Pid, index => Index, data => Data}};
        [] -> undefined
    end.

-spec exists(t(), term()) -> boolean().
exists(#replica{tab = Tab}, Name) ->
    ets:member(Tab, Name).

%% Materialise the content as a records map.
-spec records(t()) -> records().
records(#replica{tab = Tab}) ->
    ets:foldl(
        fun({Name, Pid, Index, Data}, Acc) -> Acc#{Name => {Pid, Index, Data}} end, #{}, Tab
    ).

%% ---------------------------------------------------------------------------
%% Snapshot codec
%% ---------------------------------------------------------------------------

%% Snapshots ship as one compressed binary: off-heap, never GC-scanned, and shared by
%% every follower in a fan-out. `encode/1` folds ETS straight into a pair list (cheaper
%% than building a map); `encode_records/1` takes a map already in hand. Level 1
%% because encode time sits on client-visible handoff/resync paths. `decode_records/1`
%% also accepts the bare list/map terms older senders ship.
-define(SNAPSHOT_ENCODE_OPTS, [{compressed, 1}]).

-spec encode(t()) -> binary().
encode(#replica{tab = Tab}) ->
    Pairs = ets:foldl(
        fun({Name, Pid, Index, Data}, Acc) -> [{Name, {Pid, Index, Data}} | Acc] end, [], Tab
    ),
    term_to_binary(Pairs, ?SNAPSHOT_ENCODE_OPTS).

-spec encode_records(records()) -> binary().
encode_records(Records) when is_map(Records) ->
    term_to_binary(Records, ?SNAPSHOT_ENCODE_OPTS).

-spec decode_records(binary() | list() | map()) -> records().
decode_records(Bin) when is_binary(Bin) -> decode_records(binary_to_term(Bin));
decode_records(List) when is_list(List) -> maps:from_list(List);
decode_records(Map) when is_map(Map) -> Map.

%% ---------------------------------------------------------------------------
%% Queries (§4.7, §4.9) against the inverted index
%% ---------------------------------------------------------------------------

%% Run a tagged query, returning `[#{name, pid, index, data}]`.
-spec query(t(), query()) -> [map()].
query(R, {all, Constraints}) ->
    query_constraints(R, Constraints).

%% AND-equal query: intersect posting sets smallest-first, then re-check survivors
%% against their rows so a stale posting cannot yield a false positive.
-spec query_constraints(t(), #{term() => term()}) -> [map()].
query_constraints(_R, Constraints) when map_size(Constraints) =:= 0 ->
    [];
query_constraints(#replica{tab = Tab, index = Inv}, Constraints) ->
    Postings = [posting(Inv, Attr, Value) || {Attr, Value} <- maps:to_list(Constraints)],
    case lists:any(fun(P) -> map_size(P) =:= 0 end, Postings) of
        true ->
            [];
        false ->
            [Smallest | Rest] = lists:sort(
                fun(A, B) -> map_size(A) =< map_size(B) end, Postings
            ),
            Names = [N || N <- maps:keys(Smallest), in_all(N, Rest)],
            materialize(Names, Constraints, Tab)
    end.

%% The membership `#{Name => Pid}` a query currently resolves to.
-spec query_match_names(t(), query()) -> #{term() => pid()}.
query_match_names(R, Query) ->
    maps:from_list([{N, P} || #{name := N, pid := P} <- query(R, Query)]).

%% Does `Index` (a row's indexed-attribute map) satisfy the query?
-spec query_satisfies(query(), map()) -> boolean().
query_satisfies({all, Constraints}, Index) ->
    map_size(Constraints) > 0 andalso satisfies(Constraints, Index).

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

apply_ops(Ops, R) ->
    lists:foldl(fun apply_op/2, R, Ops).

apply_op({name_registered, Name, Pid, Index, Data}, R) ->
    insert(R, Name, Pid, Index, Data);
apply_op({metadata_set, Name, Index, Data}, R) ->
    update_meta(R, Name, Index, Data);
%% The released pid matters only to the member's conflict trail (§5.6).
apply_op({name_unregistered, Name, _ReleasedPid}, R) ->
    delete(R, Name).

%% Keeps the pid. A no-op if the row is absent: never invent postings.
update_meta(R = #replica{tab = Tab, index = Inv}, Name, Index, Data) ->
    Old = row_index(Tab, Name),
    case ets:update_element(Tab, Name, [{3, Index}, {4, Data}]) of
        true -> R#replica{index = index_add(index_remove(Inv, Name, Old), Name, Index)};
        false -> R
    end.

%% `#{}` if no row, so a writer can retract old postings first.
row_index(Tab, Name) ->
    case ets:lookup(Tab, Name) of
        [{_N, _Pid, Index, _Data} | _] -> Index;
        [] -> #{}
    end.

%% Removal prunes empty value- and attribute-maps.
index_add(Inv, Name, Index) ->
    maps:fold(
        fun(Attr, Value, Acc) ->
            Vs = maps:get(Attr, Acc, #{}),
            Names = maps:get(Value, Vs, #{}),
            Acc#{Attr => Vs#{Value => Names#{Name => []}}}
        end,
        Inv,
        Index
    ).

index_remove(Inv, Name, Index) ->
    maps:fold(
        fun(Attr, Value, Acc) ->
            case Acc of
                #{Attr := #{Value := Names} = Vs} ->
                    Names1 = maps:remove(Name, Names),
                    Vs1 =
                        case map_size(Names1) of
                            0 -> maps:remove(Value, Vs);
                            _ -> Vs#{Value => Names1}
                        end,
                    case map_size(Vs1) of
                        0 -> maps:remove(Attr, Acc);
                        _ -> Acc#{Attr => Vs1}
                    end;
                _ ->
                    Acc
            end
        end,
        Inv,
        Index
    ).

index_rebuild(Records) ->
    maps:fold(
        fun(Name, {_Pid, Index, _Data}, Acc) -> index_add(Acc, Name, Index) end,
        #{},
        Records
    ).

posting(Inv, Attr, Value) ->
    case Inv of
        #{Attr := #{Value := Names}} -> Names;
        _ -> #{}
    end.

in_all(Name, Postings) ->
    lists:all(fun(P) -> maps:is_key(Name, P) end, Postings).

materialize(Names, Constraints, Tab) ->
    lists:foldl(
        fun(Name, Acc) ->
            case ets:lookup(Tab, Name) of
                [{_N, Pid, Index, Data} | _] ->
                    case satisfies(Constraints, Index) of
                        true -> [#{name => Name, pid => Pid, index => Index, data => Data} | Acc];
                        false -> Acc
                    end;
                [] ->
                    Acc
            end
        end,
        [],
        Names
    ).

satisfies(Constraints, Index) ->
    maps:fold(
        fun(Attr, Value, Ok) -> Ok andalso maps:get(Attr, Index, '$absent') =:= Value end,
        true,
        Constraints
    ).
