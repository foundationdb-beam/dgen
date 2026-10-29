-module(dgen_registry_replica).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% A member's copy of the registry's name table, and the rules for advancing it.
%%
%% Section references (e.g. `§5.7`) point to `docs/design/dgen_registry_design.md`.
%%
%% ## What a replica is
%%
%% The leader authors a totally ordered stream of committed batches, each stamped
%% with the FDB commit version it landed at.  A replica is a position in that
%% stream plus the content it implies:
%%
%%   - `tab`      the rows, `{Name, Pid, Index, Data}`, in a named `protected` ETS
%%                table that the owning member alone writes and any process reads
%%                lock-free (`dgen_registry:whereis_name/1`).
%%   - `index`    the inverted index over the rows' `Index` metadata (attribute ->
%%                value -> set of names), derived from the rows and kept in step by
%%                every writer here.
%%   - `version`  the commit version this replica has applied up to.
%%   - `epoch`    the leadership term of the stream this replica follows.
%%
%% Because commit versions are globally monotonic across leadership terms, two
%% replicas compare by version alone, with no knowledge of terms.
%%
%% ## The one invariant
%%
%%   Two replicas at the same version hold the same content.
%%
%% Freshest-wins handoff reconstruction (§5.7) is sound only because of it, and
%% every admission rule below exists to protect it.  The functions are the TLA+
%% model's receive actions (formal/DgenRegistryReplication.tla), named for what a
%% member is offered:
%%
%%   offer_batch/5     RecvBcastApply / RecvBcastDrop / RecvBcastGap
%%   offer_snapshot/4  RecvSnap (with the SafeAssumeSnap guard)
%%
%% Nothing here sends a message, arms a timer, or answers a caller.  Each function
%% says what happened and the member reacts: a `gap` becomes a resync request, an
%% `applied` batch may release deferred acks, and so on.
%%
%% ## Epoch
%%
%% The epoch belongs to the stream, so it lives here.  The two admission rules treat
%% it differently, deliberately and exactly as the model does:
%%
%%   - A batch from an older epoch is dropped.  A batch from a newer epoch is
%%     applied (if contiguous) but does **not** raise `epoch`.
%%   - A snapshot from a current-or-newer epoch is accepted (if not behind) and
%%     **does** set `epoch`.
%%
%% The asymmetry is unreachable in practice: a new leader snapshots every follower
%% before it broadcasts to it, from the same process, so per-pair FIFO delivers the
%% epoch-raising snapshot first.  Following the model rather than "fixing" it keeps
%% this module checkable against the spec action for action.
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

%% The replica's content as a transient map, the shape the handoff gather and the
%% snapshot fan-out carry.
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

%% Create an empty replica whose table is named `TabName` and owned by the caller.
%% Recreated empty on every member (re)start, with no heir: a crashed member's
%% replica is rebuilt from the leader's snapshot.
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

%% Offer one broadcast batch.  Applied only when it is contiguous with this replica
%% (`PrevV` is our version), so the replica always remains a prefix of the leader's
%% stream:
%%
%%   - `stale_epoch`  a deposed leader's batch: dropped.
%%   - `{applied, R}` the next batch in sequence: applied whole, version advanced.
%%   - `duplicate`    at or behind our version: already held, dropped.
%%   - `gap`          we missed at least one batch: nothing applied, nothing
%%                    advanced.  The caller requests a snapshot.
%%
%% A batch arrives as one message (see dgen_registry_member's broadcast_batch/5), so
%% a lost batch always leaves a version discontinuity for `gap` to catch.  The
%% leader's heartbeat is an empty batch at `{V, V}`: `applied` (as a no-op) on a
%% caught-up replica, `gap` on one that lost the tail of the stream.
-spec offer_batch(non_neg_integer(), non_neg_integer(), non_neg_integer(), [bcast_op()], t()) ->
    {applied, t()} | stale_epoch | duplicate | gap.
%%
%% `V =:= Applied` is purely the duplicate case.  It used to also mean "another
%% message of the batch we are already applying", which is what made a partially
%% delivered batch undetectable: the receiver had already advanced to V on the
%% batch's first message, so the loss of any later one left no version
%% discontinuity to notice.
%%
%% ## The mutation
%%
%% `-DMUTATION_PARTIAL_BATCH` puts both halves of that back: dgen_registry_member's
%% `broadcast_batch/5` sends one message per op sharing a version, and the clause
%% below re-admits "another message of the batch we are at".  It exists so the eta
%% framework can be required to *rediscover* a divergence we already understand,
%% from a cold start and with no test written for it — acceptance criterion 1 in
%% the `eta` library's docs/design.md.  Test builds only, off unless the
%% `DGEN_MUTATION` environment variable asks for it, and never in a release: see
%% `erlc_options/1` in mix.exs.
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

%% Offer a wholesale snapshot (a handoff fan-out, a joiner onboarding, or a resync
%% answer).  Accepted only if it is from a current-or-newer epoch **and** at least
%% as fresh as what we hold.  The version guard is what stops a snapshot computed
%% before a batch was gathered, but delivered after this replica applied that batch,
%% from rolling the replica backward (the handoff-gather race, §5.7).  A legitimate
%% re-baseline never moves backward: a new leader is caught up to the durable
%% version key before it fans out, and a resync carries the leader's current
%% version.  Equal versions are accepted: by the invariant they hold equal content.
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

%% Re-baseline unconditionally: a newly assuming leader adopts the reconstruction its
%% handoff gather produced (already fenced against the durable version key), and an
%% accepted snapshot lands here too.
-spec adopt(non_neg_integer(), non_neg_integer(), records(), t()) -> t().
adopt(Epoch, V, Records, R = #replica{tab = Tab}) ->
    ets:delete_all_objects(Tab),
    ets:insert(
        Tab, [{Name, Pid, Index, Data} || {Name, {Pid, Index, Data}} <- maps:to_list(Records)]
    ),
    R#replica{index = index_rebuild(Records), version = V, epoch = Epoch}.

%% Apply the leader's own committed batch at commit version `V`: its durable delta
%% `DBOp` (`Name => clear | {set, _} | {meta, _}`), with pids from the plan's working
%% name map and metadata from its working meta map.  Only names in `DBOp` are
%% touched, so a concurrent optimistic delete of an untouched name is never
%% clobbered, and a metadata-only op updates an existing row in place (never
%% resurrecting one removed between plan and apply).
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

%% Row writers outside the stream: the optimistic local delete an unregister makes
%% the moment it is accepted (so a caller-side whereis_name/1 on this node sees it at
%% once), and the legacy pre-version reply path's insert.  Neither moves the version.
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

%% Materialise the content as a records map (the gather's self snapshot, conflict
%% adjudication).
-spec records(t()) -> records().
records(#replica{tab = Tab}) ->
    ets:foldl(
        fun({Name, Pid, Index, Data}, Acc) -> Acc#{Name => {Pid, Index, Data}} end, #{}, Tab
    ).

%% ---------------------------------------------------------------------------
%% Snapshot codec
%% ---------------------------------------------------------------------------

%% A snapshot ships as a single `term_to_binary` blob rather than an on-heap term.
%% A large binary is a refc binary — stored off-heap and never scanned by GC — so a
%% 100k+-name snapshot no longer bloats the sender's or receiver's process heap, nor
%% thrashes their collectors while it sits in a mailbox.  Encoding once and casting
%% the *same* binary to every follower makes the fan-out share one blob instead of
%% copying the whole term per follower.
%%
%% Two encoders, by what the sender holds:
%%
%% - `encode/1` — the "serve my replica" sites (the gather reply, the resync serve,
%%   the joiner snapshot).  There is no native ETS→binary dump (`ets:tab2file` is a
%%   slower, disk-only walk of the same terms), and a records MAP built only to be
%%   encoded costs ~96ms at 200k names.  One `ets:foldl` straight into a
%%   `[{Name, Record}]` pair list is ~3x cheaper, and the list inside the blob
%%   decodes through `decode_records/1`'s list normalization.
%% - `encode_records/1` — the assume fan-out, where the reconstructed records map is
%%   already in hand (list vs map inside the blob is a wash there).
%%
%% Compression is zlib level 1, deliberately: measured at 200k names, the default
%% `[compressed]` (level 6) spent 60ms for a 0.76MB blob where level 1 spends 37ms
%% for 1.07MB — decode time unchanged.  Snapshot encodes sit on the client-visible
%% handoff/resync paths, so encode time buys more than the ~40% wire bytes it
%% costs; going uncompressed (12ms) is off the table because the blob is ~10MB
%% plain and crosses distribution once per follower.
%%
%% `decode_records/1` normalises every shape to the map all callers want: a map
%% (this build's fan-out, and any pre-list-era sender), a `[{Name, Record}]` list
%% (`encode/1`, and the pre-binary-era apply_names_snapshot wire), or a bare map
%% term (pre-binary-era get_names_snapshot).
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

%% Resolve an AND-equal query: every registration whose `Index` satisfies *all*
%% clauses.  Empty constraints yield `[]` (the public API rejects them before
%% reaching here).  Takes each clause's posting set, short-circuits if any is empty,
%% intersects smallest-first, then re-checks each survivor against its current row
%% (so a stale posting can never produce a false positive).
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
%% A no-op if the row is absent (this replica missed the registration; the next
%% handoff reconciles it).
apply_op({metadata_set, Name, Index, Data}, R) ->
    update_meta(R, Name, Index, Data);
%% The released pid is the conflict-detector trail's concern (§5.6), recorded by the
%% member from the applied ops; the replica only drops the row.
apply_op({name_unregistered, Name, _ReleasedPid}, R) ->
    delete(R, Name).

%% Replace a row's metadata in place, keeping its pid.  A no-op if the row is absent:
%% do not invent index postings for a name this replica does not hold.
update_meta(R = #replica{tab = Tab, index = Inv}, Name, Index, Data) ->
    Old = row_index(Tab, Name),
    case ets:update_element(Tab, Name, [{3, Index}, {4, Data}]) of
        true -> R#replica{index = index_add(index_remove(Inv, Name, Old), Name, Index)};
        false -> R
    end.

%% A name's current indexed-attribute map (`#{}` if it has no row), so a writer can
%% retract its old postings before applying new ones.
row_index(Tab, Name) ->
    case ets:lookup(Tab, Name) of
        [{_N, _Pid, Index, _Data} | _] -> Index;
        [] -> #{}
    end.

%% Add/remove a name's postings for each {attr, value} pair in `Index`.  Removal prunes
%% empty value- and attribute-maps so the index does not accumulate empty shells.
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
