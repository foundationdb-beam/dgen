-module(dgen_registry_replica).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("dgen_registry_member.hrl").

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% A member's copy of the registry's name table, and the rules for advancing it.
%% `§N.N` refers to docs/design/dgen_registry_design.md.
%%
%% A replica is a position in the leader's stream of committed batches, which is
%% totally ordered, together with the content that position implies. It has four
%% fields:
%%
%%   `tab`      The rows `{Name, Pid, Index, Data}`, in a protected ETS table that any
%%              process can read without locking.
%%   `index`    An inverted index from attribute to value to names, derived from the
%%              rows.
%%   `version`  The commit version we have applied up to.
%%   `epoch`    The leadership term we are following.
%%
%% Commit versions keep increasing across terms, so we compare replicas by version
%% alone.
%%
%% Invariant: two replicas at the same version hold the same content. The handoff
%% rebuilds the table from the freshest replica (§5.7), which only works if this
%% holds. Every rule for accepting a batch or snapshot exists to protect it.
%%
%% These rules mirror formal/DgenRegistryReplication.tla action for action:
%%
%%   offer_batch/5     RecvBcastApply / RecvBcastDrop / RecvBcastGap
%%   offer_snapshot/4  RecvSnap (with the SafeAssumeSnap guard)
%%
%% We drop a batch from an older epoch. We apply a batch from a newer epoch but don't
%% raise `epoch`, while a snapshot from the current or a newer epoch does raise it. The
%% model has the same asymmetry, and it can't actually happen. A new leader sends each
%% follower a snapshot before broadcasting to it, and messages arrive in order.
%%
%% Nothing here sends messages, sets timers or replies. The return values tell the
%% member what happened.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([new/1, get_tab/1, get_version/1, get_epoch/1]).
-export([offer_batch/5, offer_snapshot/4]).
-export([adopt/4, commit/5, insert/5, delete/2]).
-export([lookup_pid/2, lookup_metadata/2, exists/2, get_records/1, encode/1]).
-export([query/2, query_constraints/2, query_match_names/2, query_satisfies/2]).
-export([encode_records/1, decode_records/1]).

-export_type([t/0, records/0, bcast_op/0, query/0]).

-type t() :: #replica{}.

%% Replica content as a map. The handoff gather and the snapshot fan-out carry it in
%% this shape.
-type records() :: #{term() => {pid(), map(), term()}}.

%% One op of a `{names_batch, …}` broadcast. The epoch isn't repeated here, because it
%% travels once in the batch envelope.
-type bcast_op() ::
    {name_registered, term(), pid(), map(), term()}
    | {metadata_set, term(), map(), term()}
    | {name_unregistered, term(), pid() | undefined}.

%% A tagged query (§4.9). Only `{all, Constraints}` exists today. It matches rows
%% where every attribute equals the given value.
-type query() :: {all, #{term() => term()}}.

%% ---------------------------------------------------------------------------
%% Construction and accessors
%% ---------------------------------------------------------------------------

%% Creates an empty replica. This happens on every member start or restart. The table
%% has no heir, because a crashed member rebuilds its replica from the leader's
%% snapshot.
-spec new(atom()) -> t().
new(TabName) ->
    Tab = ets:new(TabName, [named_table, set, protected, {read_concurrency, true}]),
    #replica{tab = Tab}.

-spec get_tab(t()) -> ets:tid() | atom().
get_tab(#replica{tab = Tab}) -> Tab.

-spec get_version(t()) -> non_neg_integer().
get_version(#replica{version = V}) -> V.

-spec get_epoch(t()) -> non_neg_integer().
get_epoch(#replica{epoch = E}) -> E.

%% ---------------------------------------------------------------------------
%% Stream admission
%% ---------------------------------------------------------------------------

%% Offers one broadcast batch. We apply it only when `PrevV` equals our version, so the
%% replica stays a prefix of the leader's stream. On `gap` we apply nothing, and the
%% caller requests a snapshot. Each batch is one message, so a lost batch always leaves
%% a gap. The leader's heartbeat is an empty batch at `{V, V}`.
-spec offer_batch(non_neg_integer(), non_neg_integer(), non_neg_integer(), [bcast_op()], t()) ->
    {applied, t()} | stale_epoch | duplicate | gap.
%% MUTATION_PARTIAL_BATCH is for tests only and never in a release. It accepts
%% separate per-op messages that share a version. That brings back the bug where a
%% partially delivered batch goes undetected.
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

%% Offers a full snapshot. Snapshots come from the handoff fan-out, from onboarding a
%% joiner, and in answer to a resync. The version check stops a snapshot that was
%% gathered before a batch, but delivered after it, from rolling the replica back
%% (§5.7). We accept an equal version, because equal versions mean equal content.
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

%% Replaces the replica content unconditionally. Other processes keep reading the
%% table while this runs, so we write the new rows first and only then delete the
%% names that are gone. That way a reader never sees a bound name go missing.
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
    R#replica{index = rebuild_index(Records), version = V, epoch = Epoch}.

%% Applies the leader's own committed batch at `V`. We only touch names in `DBOp`, so
%% we don't undo an optimistic delete that happened in the meantime. A metadata-only
%% op never brings back a row that was removed between planning and applying.
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

%% These write rows outside the stream, and neither changes the version. The member
%% uses delete/2 for its optimistic local delete on unregister. Tests use insert/5 to
%% build replicas.
-spec insert(t(), term(), pid(), map(), term()) -> t().
insert(R = #replica{tab = Tab, index = Inv}, Name, Pid, Index, Data) ->
    Old = get_row_index(Tab, Name),
    ets:insert(Tab, {Name, Pid, Index, Data}),
    R#replica{index = add_to_index(remove_from_index(Inv, Name, Old), Name, Index)}.

-spec delete(t(), term()) -> t().
delete(R = #replica{tab = Tab, index = Inv}, Name) ->
    Old = get_row_index(Tab, Name),
    ets:delete(Tab, Name),
    R#replica{index = remove_from_index(Inv, Name, Old)}.

%% ---------------------------------------------------------------------------
%% Reads
%% ---------------------------------------------------------------------------

-spec lookup_pid(t(), term()) -> pid() | undefined.
lookup_pid(#replica{tab = Tab}, Name) ->
    case ets:lookup(Tab, Name) of
        [{_Name, Pid, _Index, _Data} | _] -> Pid;
        [] -> undefined
    end.

%% Returns the public `get_metadata` shape, `{ok, #{pid, index, data}}` or `undefined`.
-spec lookup_metadata(t(), term()) -> {ok, map()} | undefined.
lookup_metadata(#replica{tab = Tab}, Name) ->
    case ets:lookup(Tab, Name) of
        [{_Name, Pid, Index, Data} | _] -> {ok, #{pid => Pid, index => Index, data => Data}};
        [] -> undefined
    end.

-spec exists(t(), term()) -> boolean().
exists(#replica{tab = Tab}, Name) ->
    ets:member(Tab, Name).

%% Returns the content as a records map.
-spec get_records(t()) -> records().
get_records(#replica{tab = Tab}) ->
    ets:foldl(
        fun({Name, Pid, Index, Data}, Acc) -> Acc#{Name => {Pid, Index, Data}} end, #{}, Tab
    ).

%% ---------------------------------------------------------------------------
%% Snapshot codec
%% ---------------------------------------------------------------------------

%% Snapshots travel as one compressed binary. The binary lives off-heap, so the GC
%% never scans it, and every follower in a fan-out shares the same copy. `encode/1`
%% folds the ETS table straight into a list of pairs, which is cheaper than building a
%% map. `encode_records/1` takes a map the caller already has. We use compression
%% level 1 because encoding time adds to handoff and resync latency, which clients
%% see. `decode_records/1` reads either kind of binary.
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

-spec decode_records(binary()) -> records().
decode_records(Bin) when is_binary(Bin) ->
    case binary_to_term(Bin) of
        Pairs when is_list(Pairs) -> maps:from_list(Pairs);
        Records when is_map(Records) -> Records
    end.

%% ---------------------------------------------------------------------------
%% Queries (§4.7, §4.9) against the inverted index
%% ---------------------------------------------------------------------------

%% Runs a tagged query and returns `[#{name, pid, index, data}]`.
-spec query(t(), query()) -> [map()].
query(R, {all, Constraints}) ->
    query_constraints(R, Constraints).

%% Returns the rows where every attribute in `Constraints` equals its value. We
%% intersect the posting sets, smallest first. Then we re-check each remaining name
%% against its row, so a stale posting can't produce a false match.
-spec query_constraints(t(), #{term() => term()}) -> [map()].
query_constraints(_R, Constraints) when map_size(Constraints) =:= 0 ->
    [];
query_constraints(#replica{tab = Tab, index = Inv}, Constraints) ->
    Postings = [get_posting(Inv, Attr, Value) || {Attr, Value} <- maps:to_list(Constraints)],
    case lists:any(fun(P) -> map_size(P) =:= 0 end, Postings) of
        true ->
            [];
        false ->
            [Smallest | Rest] = lists:sort(
                fun(A, B) -> map_size(A) =< map_size(B) end, Postings
            ),
            Names = [
                N
             || N <- maps:keys(Smallest), lists:all(fun(P) -> maps:is_key(N, P) end, Rest)
            ],
            materialize(Names, Constraints, Tab)
    end.

%% Returns the names and pids, as `#{Name => Pid}`, that a query currently matches.
-spec query_match_names(t(), query()) -> #{term() => pid()}.
query_match_names(R, Query) ->
    maps:from_list([{N, P} || #{name := N, pid := P} <- query(R, Query)]).

%% Returns whether `Index`, the indexed attributes of a row, satisfies the query.
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
%% Only the member's conflict trail uses the released pid (§5.6).
apply_op({name_unregistered, Name, _ReleasedPid}, R) ->
    delete(R, Name).

%% Updates the metadata and keeps the pid. If the row is absent we do nothing, so we
%% never add postings for a name that isn't bound.
update_meta(R = #replica{tab = Tab, index = Inv}, Name, Index, Data) ->
    Old = get_row_index(Tab, Name),
    case ets:update_element(Tab, Name, [{3, Index}, {4, Data}]) of
        true -> R#replica{index = add_to_index(remove_from_index(Inv, Name, Old), Name, Index)};
        false -> R
    end.

%% Returns the row's index, or `#{}` if there is no row. Writers use it to remove the
%% old postings first.
get_row_index(Tab, Name) ->
    case ets:lookup(Tab, Name) of
        [{_N, _Pid, Index, _Data} | _] -> Index;
        [] -> #{}
    end.

add_to_index(Inv, Name, Index) ->
    maps:fold(
        fun(Attr, Value, Acc) ->
            Vs = maps:get(Attr, Acc, #{}),
            Names = maps:get(Value, Vs, #{}),
            Acc#{Attr => Vs#{Value => Names#{Name => []}}}
        end,
        Inv,
        Index
    ).

%% Removes the postings for `Name`, and prunes any value or attribute map left
%% empty so the index doesn't keep entries for names that are gone.
remove_from_index(Inv, Name, Index) ->
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

rebuild_index(Records) ->
    maps:fold(
        fun(Name, {_Pid, Index, _Data}, Acc) -> add_to_index(Acc, Name, Index) end,
        #{},
        Records
    ).

get_posting(Inv, Attr, Value) ->
    case Inv of
        #{Attr := #{Value := Names}} -> Names;
        _ -> #{}
    end.

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
