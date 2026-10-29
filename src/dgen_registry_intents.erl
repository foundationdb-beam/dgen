-module(dgen_registry_intents).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% A member's unfinished client requests that it cannot decide itself.
%%
%% Section references (e.g. `§5.5`) point to `docs/design/dgen_registry_design.md`.
%%
%% ## What an intent is
%%
%% A non-leader member accepts writes it cannot commit: only the leader can.  Each
%% one becomes an **intent** that carries up to two obligations:
%%
%%   - a reply owed: a caller blocked in a call, holding a `From`;
%%   - an effect owed: a change that must still reach the leader.
%%
%% An intent is a **kind** (what was asked) in a **phase** (how far it got):
%%
%%   kinds                              phases
%%   {register, Name, Pid, Meta}        awaiting_leader          no reachable leader
%%   {unregister, Name, ReleasedPid}    {forwarded, Leader}      sent; awaiting a reply
%%   {set_meta, Name, Index, Data}      {awaiting_version, V, L} L committed it at V;
%%                                                               awaiting our replica
%%
%% Every intent is keyed by the reference its forward carries, so a reply finds it,
%% and a reply for a reference that is no longer here (superseded by a re-drive, or
%% already answered) is ignored.
%%
%% ## The three events
%%
%% Only three things move an intent, one function each:
%%
%%   on_reply/4    the leader answered a forward.
%%   on_version/2  our replica advanced (a deferred `yes` may now be safe).
%%   redrive/5     a leader may have been established (a snapshot, assuming
%%                 leadership, a node coming back).
%%
%% ## Re-drive follows idempotence
%%
%% What `redrive/5` does with an unresolved intent depends on its kind alone:
%%
%%   - register: re-routed, in every phase where the outcome is not yet known to
%%     this member.  Registering the same pid under the same name is idempotent
%%     (§5.2), so sending it again can only produce the right answer.  This is
%%     what keeps `no` *adjudicated*: a register is never answered `no` because
%%     leadership moved under it, only because the name is held by another pid.
%%   - unregister: re-driven as a pid-guarded retract.  The pid guard makes it
%%     idempotent and safe against a name since re-registered by someone else.
%%   - set_meta: a replace, not idempotent under reordering (re-sending could
%%     overwrite a newer write), so it is never re-sent.  If the leader it was
%%     sent to is no longer the leader, the caller gets `{error, no_leader}`.
%%
%% Nothing here sends a message or answers a caller.  Each function returns the
%% effects for the member to carry out.
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
    %% The caller to answer, or `answered` once the reply has been given while the
    %% effect is still owed (an unregister accepted with no reachable leader).
    from :: from() | answered,
    %% Wall-clock ms the caller asked.  A register or set_meta older than the
    %% caller's timeout has no one left to answer and is dropped.
    at :: integer(),
    %% Arrival order.  Every multi-intent event emits its effects in this order, never
    %% in map order: references differ from run to run, and an order that followed
    %% them would make the member's message schedule irreproducible under
    %% deterministic simulation.
    seq :: non_neg_integer()
}).

-record(intents, {
    %% Every unresolved intent, by the reference its forward carries.
    all = #{} :: #{reference() => #intent{}},
    %% The subset in `{awaiting_version, V, _}`, as Ref => V, so a replica advance
    %% (every applied batch) looks only at the deferred acks, not every forward.
    deferred = #{} :: #{reference() => non_neg_integer()},
    %% The next intent's `seq`.
    next_seq = 0 :: non_neg_integer()
}).

-opaque t() :: #intents{}.

%% Where a request can go right now, as the member sees it.
-type route() :: self | {remote, member_id()} | none.

%% Why a re-drive is happening (see redrive/5).
-type scope() :: moved | all.

-type effect() ::
    {reply, from(), term()}
    %% Send the kind's wire request to the leader, tracked under the reference.
    | {forward, member_id(), reference(), kind()}
    %% Send an untracked, pid-guarded clear to the leader.
    | {retract, member_id(), Name :: term(), pid()}
    %% We lead: hand the kind to our own commit pipeline (answering From, if any).
    | {enqueue, kind(), from() | answered}
    %% A committed `yes` is ahead of our replica: ask the leader for a snapshot.
    | {resync, member_id()}
    %% A legacy (pre-version) leader's `yes`: the reply itself must bind the row.
    | {insert_row, Name :: term(), pid(), Meta :: {map(), term()}}.

-spec new() -> t().
new() -> #intents{}.

%% ---------------------------------------------------------------------------
%% Submission
%% ---------------------------------------------------------------------------

%% Accept a new request on a member that does not lead (`Route` is never `self` here:
%% the member commits its own writes directly).
%%
%% An unregister or set_meta with a reachable leader is forwarded tracked: its caller
%% waits for the commit.  Everything else takes the same path a re-drive would —
%% including an `answered` unregister, which the leader pipeline hands over when it
%% cannot commit one (dgen_registry_pipeline:surrender/2).
-spec submit(kind(), from() | answered, integer(), route(), t()) -> {t(), [effect()]}.
submit({set_meta, _, _, _}, From, _Now, Route, T) when Route =:= none; Route =:= self ->
    {T, [{reply, From, {error, no_leader}}]};
submit(Kind, From, Now, Route, T = #intents{next_seq = Seq}) ->
    I = #intent{kind = Kind, phase = awaiting_leader, from = From, at = Now, seq = Seq},
    T1 = T#intents{next_seq = Seq + 1},
    case Route of
        %% An already-answered unregister (one the leader could not commit) is not a
        %% caller's request by name: it re-drives as a pid-guarded retract instead.
        {remote, L} when element(1, Kind) =/= register, From =/= answered ->
            Ref = make_ref(),
            {put(Ref, I#intent{phase = {forwarded, L}}, T1), [{forward, L, Ref, Kind}]};
        _ ->
            dispatch(make_ref(), I, Route, T1)
    end.

%% ---------------------------------------------------------------------------
%% The three events
%% ---------------------------------------------------------------------------

%% The leader answered the forward tracked under `Ref`.  `Applied` is our replica's
%% version now.  The reply shapes are the member's wire replies, minus the reference:
%%
%%   {register, yes | no, Version | legacy}
%%   {set_meta, Result}
%%   unregister
%%
%% A register `yes` is version-guarded (§5.5): answered only once our replica has
%% applied the batch that committed it, so this member is a second holder the
%% freshest-wins handoff gather can see.  Ahead of it (our replica is gapped), the
%% ack waits in `{awaiting_version, V, Leader}` and a resync is requested.
%%
%% The at-or-behind branch must not write the row: a batch may bind *and* clear one
%% name, and its broadcast (already applied, by the guard) carries both.  Writing
%% the binding here would resurrect what the batch removed.
-spec on_reply(reference(), term(), non_neg_integer(), t()) -> {t(), [effect()]}.
on_reply(Ref, Reply, Applied, T = #intents{all = All}) ->
    case All of
        #{Ref := I = #intent{phase = {forwarded, Leader}}} ->
            resolve(Ref, I, Leader, Reply, Applied, delete(Ref, T));
        _ ->
            %% Unknown, superseded by a re-drive, or not awaiting a reply.
            {T, []}
    end.

resolve(_Ref, #intent{kind = {register, Name, Pid, Meta}, from = From}, _L, Reply, _A, T) when
    Reply =:= {register, yes, legacy}
->
    %% A pre-version leader (rolling upgrade): no version to guard on, and its
    %% per-name broadcasts are a wire format this member no longer applies, so
    %% the reply is the only thing that binds the row.
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

%% Our replica advanced to `Applied`: release every deferred `yes` it now covers.
%% Reaching the version means the batch's own broadcast was applied (or a snapshot
%% at or past it), so the binding is there.
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

%% A leader may have been established: re-drive what is unresolved, by kind (see the
%% module doc).  `TTL` is the caller timeout; a register or set_meta older than it is
%% dropped without a reply (its caller has given up).
%%
%%   - register in `awaiting_leader`: routed.
%%   - register in `{forwarded, L}`: re-sent under a fresh reference when L no longer
%%     leads — or, with `Scope = all`, even when it does (see below).  Otherwise it
%%     is kept: L will answer it.
%%   - register in `{awaiting_version, V, L}`: kept while L still leads (it is
%%     waiting on our replica, not the leader); re-routed otherwise.
%%   - unregister in any phase: re-driven as a pid-guarded retract (answering a
%%     still-waiting caller `ok`), or kept, answered, if there is no reachable leader.
%%   - set_meta forwarded to anyone but the current leader: `{error, no_leader}`.
%%
%% `Scope` says why the re-drive is happening.  `moved` (a snapshot, assuming
%% leadership): the leadership view changed, and a forward to the leader that is
%% still current was delivered — a member in its assume window postpones it until
%% it leads — so only forwards to someone else need re-sending.  `all` (a node came
%% back): a forward cast into the link that died may have been lost, so forwards to
%% the current leader are re-sent too.  A duplicate is decided idempotently, but it
%% is a second commit, so it is sent only when a loss is possible.
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

%% Drop register and set_meta intents whose callers have timed out, so a prolonged
%% leaderless window (a partition), or a forward whose reply was lost, cannot grow
%% the map without bound.  No reply is sent.  An unregister is never dropped: its
%% effect is owed whether or not anyone is waiting.
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

%% Send a register or unregister wherever `Route` allows, keeping it under `Ref` if it
%% still owes something.  (A set_meta is never re-sent; see redrive/5.)
dispatch(_Ref, #intent{kind = Kind = {register, _, _, _}, from = From}, self, T) ->
    {T, [{enqueue, Kind, From}]};
dispatch(Ref, I = #intent{kind = Kind = {register, _, _, _}}, {remote, L}, T) ->
    {put(Ref, I#intent{phase = {forwarded, L}}, T), [{forward, L, Ref, Kind}]};
dispatch(Ref, I = #intent{kind = {register, _, _, _}}, none, T) ->
    {put(Ref, I#intent{phase = awaiting_leader}, T), []};
%% An unregister's caller is answered `ok` now, and the removal travels as a
%% pid-guarded retract, or waits for a leader.
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
