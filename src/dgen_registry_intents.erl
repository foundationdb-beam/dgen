-module(dgen_registry_intents).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% A non-leader member's unfinished client requests: writes only the leader can commit.
%% `§N.N` refers to docs/design/dgen_registry_design.md.
%%
%% An intent owes a reply (a blocked caller's `From`), an effect (a change that must
%% reach the leader), or both. It is a kind in a phase, keyed by its forward's reference;
%% a reply under an unknown reference (superseded or answered) is ignored.
%%
%%   kinds                              phases
%%   {register, Name, Pid, Meta}        awaiting_leader          no reachable leader
%%   {unregister, Name, ReleasedPid}    {forwarded, Leader}      sent; awaiting a reply
%%   {set_meta, Name, Index, Data}      {awaiting_version, V, L} L committed it at V;
%%                                                               awaiting our replica
%%
%% Three events move it: on_reply/4 (the leader answered), on_version/2 (our replica
%% advanced), redrive/5 (a leader may have been established).
%%
%% Re-drive follows idempotence, by kind alone:
%%   - register: re-routed until its outcome is known. Same pid, same name is idempotent
%%     (§5.2), so `no` is always adjudicated, never caused by leadership moving.
%%   - unregister: re-driven as a pid-guarded retract, safe against re-registration.
%%   - set_meta: a replace, unsafe to re-send under reordering; if its leader no longer
%%     leads, the caller gets `{error, no_leader}`.
%%
%% Pure: functions return effects for the member to carry out.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([new/0, submit/5, on_reply/4, on_version/2, redrive/5, prune/3]).

-export_type([t/0, kind/0, route/0, scope/0, effect/0]).

-type member_id() :: dgen_registry_elector:member_id().
-type from() :: gen_server:from().

-type kind() ::
    {register, Name :: term(), pid(), Meta :: {map(), term()}}
    | {unregister, Name :: term(), ReleasedPid :: pid() | undefined}
    | {set_meta, Name :: term(), Index :: map(), Data :: term()}.

-type phase() ::
    awaiting_leader
    | {forwarded, member_id()}
    | {awaiting_version, non_neg_integer(), member_id()}.

-record(intent, {
    kind :: kind(),
    phase :: phase(),
    %% `answered`: the reply was given but the effect is still owed.
    from :: from() | answered,
    %% Wall-clock ms of the request; compared against the caller timeout.
    at :: integer(),
    %% Arrival order. Effects are emitted in this order, never map (reference) order,
    %% so the message schedule is reproducible under deterministic simulation.
    seq :: non_neg_integer()
}).

-record(intents, {
    all = #{} :: #{reference() => #intent{}},
    %% Ref => V of the `awaiting_version` subset: a replica advance scans only these.
    deferred = #{} :: #{reference() => non_neg_integer()},
    next_seq = 0 :: non_neg_integer()
}).

-opaque t() :: #intents{}.

%% Where a request can go right now, as the member sees it.
-type route() :: self | {remote, member_id()} | none.

-type scope() :: moved | all.

-type effect() ::
    {reply, from(), term()}
    %% Tracked forward to the leader.
    | {forward, member_id(), reference(), kind()}
    %% Untracked pid-guarded clear.
    | {retract, member_id(), Name :: term(), pid()}
    %% We lead: our own commit pipeline.
    | {enqueue, kind(), from() | answered}
    %% A committed `yes` is ahead of our replica: ask the leader for a snapshot.
    | {resync, member_id()}
    %% A pre-version leader's `yes` (rolling upgrade): the reply binds the row.
    | {insert_row, Name :: term(), pid(), Meta :: {map(), term()}}.

-spec new() -> t().
new() -> #intents{}.

%% ---------------------------------------------------------------------------
%% Submission
%% ---------------------------------------------------------------------------

%% Accept a request on a non-leader. A forwardable unregister/set_meta is tracked and
%% its caller waits for the commit; everything else, including an `answered`
%% unregister the pipeline surrendered, takes the re-drive path.
-spec submit(kind(), from() | answered, integer(), route(), t()) -> {t(), [effect()]}.
submit({set_meta, _, _, _}, From, _Now, Route, T) when Route =:= none; Route =:= self ->
    {T, [{reply, From, {error, no_leader}}]};
submit(Kind, From, Now, Route, T = #intents{next_seq = Seq}) ->
    I = #intent{kind = Kind, phase = awaiting_leader, from = From, at = Now, seq = Seq},
    T1 = T#intents{next_seq = Seq + 1},
    case Route of
        %% An `answered` unregister re-drives as a pid-guarded retract instead.
        {remote, L} when element(1, Kind) =/= register, From =/= answered ->
            Ref = make_ref(),
            {put(Ref, I#intent{phase = {forwarded, L}}, T1), [{forward, L, Ref, Kind}]};
        _ ->
            dispatch(make_ref(), I, Route, T1)
    end.

%% ---------------------------------------------------------------------------
%% The three events
%% ---------------------------------------------------------------------------

%% The leader answered the forward under `Ref`; `Applied` is our replica's version.
%% A register `yes` is version-guarded (§5.5): answered only once our replica holds the
%% committing batch, so the handoff gather can see this member as a second holder.
%% Never write the row here: the same batch may have cleared the name too.
-spec on_reply(reference(), term(), non_neg_integer(), t()) -> {t(), [effect()]}.
on_reply(Ref, Reply, Applied, T = #intents{all = All}) ->
    case All of
        #{Ref := I = #intent{phase = {forwarded, Leader}}} ->
            resolve(Ref, I, Leader, Reply, Applied, delete(Ref, T));
        _ ->
            {T, []}
    end.

resolve(_Ref, #intent{kind = {register, Name, Pid, Meta}, from = From}, _L, Reply, _A, T) when
    Reply =:= {register, yes, legacy}
->
    %% Pre-version leader: no version to guard on; the reply alone binds the row.
    {T, [{reply, From, yes}, {insert_row, Name, Pid, Meta}]};
resolve(Ref, I = #intent{kind = {register, _, _, _}, from = From}, L, {register, yes, V}, A, T) ->
    case V =< A of
        true -> {T, [{reply, From, yes}]};
        false -> {put(Ref, I#intent{phase = {awaiting_version, V, L}}, T), [{resync, L}]}
    end;
resolve(_Ref, #intent{kind = {register, _, _, _}, from = From}, _L, {register, no, _}, _A, T) ->
    {T, [{reply, From, no}]};
resolve(_Ref, #intent{kind = {set_meta, _, _, _}, from = From}, _L, {set_meta, Result}, _A, T) ->
    {T, [{reply, From, Result}]};
resolve(_Ref, #intent{kind = {unregister, _, _}, from = From}, _L, unregister, _A, T) ->
    {T, reply_if_owed(From, ok)};
resolve(Ref, I, _Leader, _Mismatched, _Applied, T) ->
    %% A reply whose shape does not match the kind it is keyed under: keep waiting.
    {put(Ref, I, T), []}.

%% Release every deferred `yes` our replica now covers.
-spec on_version(non_neg_integer(), t()) -> {t(), [effect()]}.
on_version(_Applied, T = #intents{deferred = D}) when map_size(D) =:= 0 ->
    {T, []};
on_version(Applied, T = #intents{all = All, deferred = D}) ->
    Due = in_order([{Ref, maps:get(Ref, All)} || {Ref, V} <- maps:to_list(D), V =< Applied]),
    lists:foldl(
        fun({Ref, #intent{from = From}}, {Acc, Effs}) ->
            {delete(Ref, Acc), Effs ++ [{reply, From, yes}]}
        end,
        {T, []},
        Due
    ).

%% Re-drive unresolved intents by kind (see header); register/set_meta past `TTL` (the
%% caller timeout) are dropped unreplied. A register awaiting a version from the current
%% leader is kept; one forwarded to it is kept only when `Scope = moved`. With `all` (a
%% node came back) that forward may have died with the link, so it is re-sent; a
%% duplicate is decided idempotently but costs a second commit.
-spec redrive(route(), scope(), integer(), non_neg_integer(), t()) -> {t(), [effect()]}.
redrive(Route, Scope, Now, TTL, T = #intents{all = All}) ->
    {T1, EffsRev} = lists:foldl(
        fun({Ref, I}, {Acc, Effs}) ->
            {Acc1, New} = redrive_one(Ref, I, Route, Scope, Now, TTL, delete(Ref, Acc)),
            {Acc1, lists:reverse(New, Effs)}
        end,
        {T, []},
        in_order(maps:to_list(All))
    ),
    {T1, lists:reverse(EffsRev)}.

redrive_one(Ref, I = #intent{kind = {register, _, _, _}, at = At}, Route, Scope, Now, TTL, T) ->
    case {Now - At >= TTL, I#intent.phase, Route} of
        {true, _, _} -> {T, []};
        {false, {awaiting_version, _V, L}, {remote, L}} -> {put(Ref, I, T), []};
        {false, {forwarded, L}, {remote, L}} when Scope =:= moved -> {put(Ref, I, T), []};
        {false, _, _} -> dispatch(make_ref(), I#intent{phase = awaiting_leader}, Route, T)
    end;
redrive_one(Ref, I = #intent{kind = {unregister, _, _}}, Route, _Scope, _Now, _TTL, T) ->
    dispatch(Ref, I, Route, T);
redrive_one(Ref, I = #intent{kind = {set_meta, _, _, _}}, Route, _Scope, Now, TTL, T) ->
    #intent{from = From, at = At} = I,
    case {Now - At >= TTL, I#intent.phase, Route} of
        {true, _, _} -> {T, []};
        {false, {forwarded, L}, {remote, L}} -> {put(Ref, I, T), []};
        {false, _, _} -> {T, [{reply, From, {error, no_leader}}]}
    end.

%% Drop timed-out register/set_meta intents so a long leaderless window cannot grow
%% the map. Unregisters are kept: their effect is owed whether or not anyone waits.
-spec prune(integer(), non_neg_integer(), t()) -> t().
prune(Now, TTL, T = #intents{all = All}) ->
    maps:fold(
        fun
            (_Ref, #intent{kind = {unregister, _, _}}, Acc) -> Acc;
            (Ref, #intent{at = At}, Acc) when Now - At >= TTL -> delete(Ref, Acc);
            (_Ref, _I, Acc) -> Acc
        end,
        T,
        All
    ).

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

%% Send a register/unregister where `Route` allows, keeping it while it owes something.
dispatch(_Ref, #intent{kind = Kind = {register, _, _, _}, from = From}, self, T) ->
    {T, [{enqueue, Kind, From}]};
dispatch(Ref, I = #intent{kind = Kind = {register, _, _, _}}, {remote, L}, T) ->
    {put(Ref, I#intent{phase = {forwarded, L}}, T), [{forward, L, Ref, Kind}]};
dispatch(Ref, I = #intent{kind = {register, _, _, _}}, none, T) ->
    {put(Ref, I#intent{phase = awaiting_leader}, T), []};
%% An unregister's caller is answered `ok` now; the removal follows as a retract.
dispatch(_Ref, #intent{kind = {unregister, _Name, undefined}, from = From}, _Route, T) ->
    %% Nothing was bound locally when it was asked: nothing to retract.
    {T, reply_if_owed(From, ok)};
dispatch(_Ref, #intent{kind = Kind = {unregister, _, _}, from = From}, self, T) ->
    {T, reply_if_owed(From, ok) ++ [{enqueue, Kind, answered}]};
dispatch(_Ref, #intent{kind = {unregister, Name, Pid}, from = From}, {remote, L}, T) ->
    {T, reply_if_owed(From, ok) ++ [{retract, L, Name, Pid}]};
dispatch(Ref, I = #intent{kind = {unregister, _, _}, from = From}, none, T) ->
    %% CP: answer now, keep the intent until a leader can take it.
    {put(Ref, I#intent{phase = awaiting_leader, from = answered}, T), reply_if_owed(From, ok)}.

reply_if_owed(answered, _Value) -> [];
reply_if_owed(From, Value) -> [{reply, From, Value}].

put(Ref, I = #intent{phase = {awaiting_version, V, _L}}, T = #intents{all = A, deferred = D}) ->
    T#intents{all = A#{Ref => I}, deferred = D#{Ref => V}};
put(Ref, I, T = #intents{all = A, deferred = D}) ->
    T#intents{all = A#{Ref => I}, deferred = maps:remove(Ref, D)}.

%% `{Ref, Intent}` pairs in arrival order.
in_order(Pairs) ->
    lists:sort(fun({_, A}, {_, B}) -> A#intent.seq =< B#intent.seq end, Pairs).

delete(Ref, T = #intents{all = A, deferred = D}) ->
    T#intents{all = maps:remove(Ref, A), deferred = maps:remove(Ref, D)}.
