-module(dgen_registry_intents).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("dgen_registry_member.hrl").

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Tracks the client requests a non-leader member is holding until the leader
%% commits them. `§N.N` refers to docs/design/dgen_registry_design.md.
%%
%% Each intent is one request. It may owe the caller a reply, owe the leader a
%% change, or both. We store it under the reference we sent with the forward, so
%% the leader's reply can find it. If a reply arrives under a reference we no
%% longer hold, the intent was already answered or re-sent, and we drop the reply.
%%
%% An intent moves through these phases:
%%
%%   awaiting_leader           No leader is reachable yet.
%%   {forwarded, L}            We sent it to leader L and are waiting for a reply.
%%   {awaiting_version, V, L}  L committed it at version V. We are waiting for our
%%                             replica to reach V.
%%
%% Three events move it forward: on_reply/4 (the leader answered), on_version/2
%% (our replica advanced), and redrive/5 (leadership may have changed).
%%
%% When leadership changes, each kind is re-sent only as far as re-sending is safe:
%%   - register: re-sent until we know the outcome. Registering the same pid under
%%     the same name twice is harmless (§5.2), so a `no` always means another pid
%%     holds the name, never that leadership moved.
%%   - unregister: re-sent as a retract that only removes the name if the same pid
%%     still holds it, so it can't undo a later registration.
%%   - set_meta: replaces the value, so a late duplicate could overwrite a newer
%%     one. If its leader has stepped down, the caller gets `{error, no_leader}`.
%%
%% The functions here have no side effects. They return effects for the member
%% to carry out.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([new/0, submit/5, on_reply/4, on_version/2, redrive/5, prune/3]).

-export_type([t/0, kind/0, phase/0, route/0, scope/0, effect/0]).

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

-type t() :: #intents{}.

%% Where a request can go right now, as the member sees it.
-type route() :: self | {remote, member_id()} | none.

-type scope() :: moved | all.

-type effect() ::
    {reply, from(), term()}
    %% Forward to the leader and wait for its reply.
    | {forward, member_id(), reference(), kind()}
    %% Ask the leader to clear the name if this pid still holds it. No reply.
    | {retract, member_id(), Name :: term(), pid()}
    %% We are the leader, so commit it ourselves.
    | {enqueue, kind(), from() | answered}
    %% Our replica is behind a committed `yes`, so ask the leader for a snapshot.
    | {resync, member_id()}.

-spec new() -> t().
new() -> #intents{}.

%% ---------------------------------------------------------------------------
%% Submission
%% ---------------------------------------------------------------------------

%% Accepts a request on a non-leader. When a leader is reachable, an unregister or
%% set_meta is forwarded and its caller waits for the commit. Everything else goes
%% through dispatch/4, the same path a re-drive takes. That includes an unregister
%% the pipeline handed back already answered.
-spec submit(kind(), from() | answered, integer(), route(), t()) -> {t(), [effect()]}.
submit({set_meta, _, _, _}, From, _Now, Route, T) when Route =:= none; Route =:= self ->
    {T, [{reply, From, {error, no_leader}}]};
submit(Kind, From, Now, Route, T = #intents{next_seq = Seq}) ->
    I = #intent{kind = Kind, phase = awaiting_leader, from = From, at = Now, seq = Seq},
    T1 = T#intents{next_seq = Seq + 1},
    case Route of
        %% An unregister that was already answered goes to dispatch/4 as a retract.
        {remote, L} when element(1, Kind) =/= register, From =/= answered ->
            Ref = make_ref(),
            {put_intent(Ref, I#intent{phase = {forwarded, L}}, T1), [{forward, L, Ref, Kind}]};
        _ ->
            dispatch(make_ref(), I, Route, T1)
    end.

%% ---------------------------------------------------------------------------
%% The three events
%% ---------------------------------------------------------------------------

%% Handles the leader's reply to the forward sent under `Ref`. `Applied` is our
%% replica's version. We hold a register `yes` until our replica reaches the
%% version that committed it (§5.5). Otherwise a leadership handoff could gather
%% our replica without the name and lose it. We don't write the row here, because
%% the same batch may also have cleared the name.
-spec on_reply(reference(), term(), non_neg_integer(), t()) -> {t(), [effect()]}.
on_reply(Ref, Reply, Applied, T = #intents{all = All}) ->
    case All of
        #{Ref := I = #intent{phase = {forwarded, Leader}}} ->
            resolve_reply(Ref, I, Leader, Reply, Applied, delete_intent(Ref, T));
        _ ->
            {T, []}
    end.

resolve_reply(
    Ref, I = #intent{kind = {register, _, _, _}, from = From}, L, {register, yes, V}, A, T
) ->
    case V =< A of
        true -> {T, [{reply, From, yes}]};
        false -> {put_intent(Ref, I#intent{phase = {awaiting_version, V, L}}, T), [{resync, L}]}
    end;
resolve_reply(_Ref, #intent{kind = {register, _, _, _}, from = From}, _L, {register, no, _}, _A, T) ->
    {T, [{reply, From, no}]};
resolve_reply(
    _Ref, #intent{kind = {set_meta, _, _, _}, from = From}, _L, {set_meta, Result}, _A, T
) ->
    {T, [{reply, From, Result}]};
resolve_reply(_Ref, #intent{kind = {unregister, _, _}, from = From}, _L, unregister, _A, T) ->
    {T, maybe_reply(From, ok)};
resolve_reply(Ref, I, _Leader, _Mismatched, _Applied, T) ->
    %% The reply doesn't match the request's kind, so keep waiting.
    {put_intent(Ref, I, T), []}.

%% Answers every held `yes` whose version our replica has now reached.
-spec on_version(non_neg_integer(), t()) -> {t(), [effect()]}.
on_version(_Applied, T = #intents{deferred = D}) when map_size(D) =:= 0 ->
    {T, []};
on_version(Applied, T = #intents{all = All, deferred = D}) ->
    Due = sort_by_arrival([{Ref, maps:get(Ref, All)} || {Ref, V} <- maps:to_list(D), V =< Applied]),
    lists:foldl(
        fun({Ref, #intent{from = From}}, {Acc, Effs}) ->
            {delete_intent(Ref, Acc), Effs ++ [{reply, From, yes}]}
        end,
        {T, []},
        Due
    ).

%% Re-sends unresolved intents, following the rules in the module header.
%% Registers and set_metas older than `TTL` (the caller's timeout) are dropped
%% without a reply, since nobody is waiting. A register already waiting on the
%% current leader is left alone. With `Scope = all`, a node has reconnected and
%% a forward to it may have been lost with the old link, so we send it again. A
%% duplicate is harmless but costs an extra commit.
-spec redrive(route(), scope(), integer(), non_neg_integer(), t()) -> {t(), [effect()]}.
redrive(Route, Scope, Now, TTL, T = #intents{all = All}) ->
    {T1, EffsRev} = lists:foldl(
        fun({Ref, I}, {Acc, Effs}) ->
            {Acc1, New} = redrive_one(Ref, I, Route, Scope, Now, TTL, delete_intent(Ref, Acc)),
            {Acc1, lists:reverse(New, Effs)}
        end,
        {T, []},
        sort_by_arrival(maps:to_list(All))
    ),
    {T1, lists:reverse(EffsRev)}.

redrive_one(Ref, I = #intent{kind = {register, _, _, _}, at = At}, Route, Scope, Now, TTL, T) ->
    case {Now - At >= TTL, I#intent.phase, Route} of
        {true, _, _} -> {T, []};
        {false, {awaiting_version, _V, L}, {remote, L}} -> {put_intent(Ref, I, T), []};
        {false, {forwarded, L}, {remote, L}} when Scope =:= moved -> {put_intent(Ref, I, T), []};
        {false, _, _} -> dispatch(make_ref(), I#intent{phase = awaiting_leader}, Route, T)
    end;
redrive_one(Ref, I = #intent{kind = {unregister, _, _}}, Route, _Scope, _Now, _TTL, T) ->
    dispatch(Ref, I, Route, T);
redrive_one(Ref, I = #intent{kind = {set_meta, _, _, _}}, Route, _Scope, Now, TTL, T) ->
    #intent{from = From, at = At} = I,
    case {Now - At >= TTL, I#intent.phase, Route} of
        {true, _, _} -> {T, []};
        {false, {forwarded, L}, {remote, L}} -> {put_intent(Ref, I, T), []};
        {false, _, _} -> {T, [{reply, From, {error, no_leader}}]}
    end.

%% Drops timed-out registers and set_metas so the map can't grow during a long
%% stretch without a leader. Unregisters are kept, because the name must still be
%% removed even when nobody is waiting.
-spec prune(integer(), non_neg_integer(), t()) -> t().
prune(Now, TTL, T = #intents{all = All}) ->
    maps:fold(
        fun
            (_Ref, #intent{kind = {unregister, _, _}}, Acc) -> Acc;
            (Ref, #intent{at = At}, Acc) when Now - At >= TTL -> delete_intent(Ref, Acc);
            (_Ref, _I, Acc) -> Acc
        end,
        T,
        All
    ).

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

%% Sends a register or unregister along `Route`, and keeps the intent while it
%% still owes something.
dispatch(_Ref, #intent{kind = Kind = {register, _, _, _}, from = From}, self, T) ->
    {T, [{enqueue, Kind, From}]};
dispatch(Ref, I = #intent{kind = Kind = {register, _, _, _}}, {remote, L}, T) ->
    {put_intent(Ref, I#intent{phase = {forwarded, L}}, T), [{forward, L, Ref, Kind}]};
dispatch(Ref, I = #intent{kind = {register, _, _, _}}, none, T) ->
    {put_intent(Ref, I#intent{phase = awaiting_leader}, T), []};
%% An unregister's caller gets `ok` right away. The removal follows as a retract.
dispatch(_Ref, #intent{kind = {unregister, _Name, undefined}, from = From}, _Route, T) ->
    %% The name wasn't bound locally when the caller asked, so there's nothing to retract.
    {T, maybe_reply(From, ok)};
dispatch(_Ref, #intent{kind = Kind = {unregister, _, _}, from = From}, self, T) ->
    {T, maybe_reply(From, ok) ++ [{enqueue, Kind, answered}]};
dispatch(_Ref, #intent{kind = {unregister, Name, Pid}, from = From}, {remote, L}, T) ->
    {T, maybe_reply(From, ok) ++ [{retract, L, Name, Pid}]};
dispatch(Ref, I = #intent{kind = {unregister, _, _}, from = From}, none, T) ->
    %% Answer now, and keep the intent until a leader can take it.
    {put_intent(Ref, I#intent{phase = awaiting_leader, from = answered}, T), maybe_reply(From, ok)}.

maybe_reply(answered, _Value) -> [];
maybe_reply(From, Value) -> [{reply, From, Value}].

put_intent(
    Ref, I = #intent{phase = {awaiting_version, V, _L}}, T = #intents{all = A, deferred = D}
) ->
    T#intents{all = A#{Ref => I}, deferred = D#{Ref => V}};
put_intent(Ref, I, T = #intents{all = A, deferred = D}) ->
    T#intents{all = A#{Ref => I}, deferred = maps:remove(Ref, D)}.

%% Sorts `{Ref, Intent}` pairs into arrival order.
sort_by_arrival(Pairs) ->
    lists:sort(fun({_, A}, {_, B}) -> A#intent.seq =< B#intent.seq end, Pairs).

delete_intent(Ref, T = #intents{all = A, deferred = D}) ->
    T#intents{all = maps:remove(Ref, A), deferred = maps:remove(Ref, D)}.
