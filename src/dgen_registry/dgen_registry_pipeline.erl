-module(dgen_registry_pipeline).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% The leader's write pipeline: every op it has accepted and not yet answered.
%% `§N.N` refers to docs/design/dgen_registry_design.md.
%%
%% Phases, one field each:
%%   queued      `pending`     waiting for the next group commit.
%%   committing  `committing`  the single in-flight commit; arrivals ride the next one.
%%   awaiting    `acks`        a committed direct registration waiting for a follower
%%   holders                   copy before its `yes` (§5.5); the follower-side twin is
%%                             an intent's `awaiting_version` (dgen_registry_intents).
%% The member plans and applies the ops (plan_batch/4, apply_committed_plan/3).
%%
%% One surrender rule, surrender/2, for ops the leader cannot finish. `failed` (the
%% commit did not land, or landed under a moved view) retries them; `deposed` hands
%% them off by kind, by the same idempotence rule as the follower's intents:
%%   add, local       becomes a follower intent, re-driven to the next leader.
%%   add, forwarded   no reply: the forwarding follower re-drives its own intent.
%%   set_meta         `{error, no_leader}`, also on failed: a replace is unsafe to re-send.
%%   remove/retract   a waiting caller gets `ok`, also on failed; then an answered,
%%                    pid-guarded unregister intent.
%%   down             dropped: the next leader monitors the pid itself.
%% So `no` stays an adjudicated refusal (§3), never "the leader could not finish".
%%
%% Pure: surrender/2 returns effects for the member to carry out.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([new/0, enqueue/2, num_pending/1, in_flight/1, take_batch/2, take_pending/1]).
-export([start/4, finish/2, worker_down/2, committed/2, read_version/1, new_term/1]).
-export([await_acks/5, ack/3, ack_timeout/2, take_acks/1]).
-export([surrender/2]).

-export_type([t/0, op/0, origin/0, effect/0]).

-type member_id() :: dgen_registry_elector:member_id().
-type from() :: gen_server:from().

%% Where an op came from, so how to answer it: plain variants are direct calls answered
%% via `From`; `forward*` ones get a `{register_reply | set_meta_reply |
%% unregister_reply, Ref, ...}` cast to the forwarding member.
-type origin() ::
    {local, from()}
    | {forward, member_id(), reference()}
    | {meta, from()}
    | {forward_meta, member_id(), reference()}
    | {unreg, from()}
    | {forward_unreg, member_id(), reference()}.

%% A leader write op. A batch rides one fenced commit and applies in arrival order, so
%% it equals one-at-a-time processing. `remove`'s ReleasedPid is captured before the
%% optimistic local delete; when set it drives the durable clear and feeds the
%% conflict trail (§5.6). `down` is Ref-guarded against a stale DOWN.
-type op() ::
    {add, Name :: term(), pid(), Meta :: {map(), term()}, origin()}
    | {set_meta, Name :: term(), Index :: map(), Data :: term(), origin()}
    | {remove, Name :: term(), ReleasedPid :: pid() | undefined, origin() | undefined}
    | {retract, Name :: term(), pid()}
    | {down, Name :: term(), reference()}.

%% Awaiting holders: origin plus binding, so a fail-closed timeout can retract it.
-type direct() :: {origin(), Name :: term(), pid()}.

-record(pipeline, {
    pending = queue:new() :: queue:queue(op()),
    num_pending = 0 :: non_neg_integer(),
    %% Monitored so a worker dying without a result cannot wedge the pipeline.
    committing = undefined :: undefined | #{ref := reference(), mref := reference(), plan := map()},
    %% Pinned as the next read version to skip a GRV; if stale, the worker retries.
    last_version = undefined :: undefined | integer(),
    %% BatchRef => {Seq, Direct, TimerRef, Needed, Acked}. Answered in Seq order, never
    %% reference order, so simulated schedules are reproducible.
    acks = #{} :: #{
        reference() =>
            {non_neg_integer(), [direct()], reference(), non_neg_integer(), #{
                member_id() => true
            }}
    },
    next_ack_seq = 0 :: non_neg_integer()
}).

-opaque t() :: #pipeline{}.

-type effect() ::
    {reply, origin(), term()}
    %% Hand the kind to the follower-side intents (dgen_registry_intents:submit/5).
    | {intent, dgen_registry_intents:kind(), from() | answered}
    | {retry, [op()]}.

-spec new() -> t().
new() -> #pipeline{}.

%% ---------------------------------------------------------------------------
%% Queued
%% ---------------------------------------------------------------------------

-spec enqueue(op(), t()) -> t().
enqueue(Op, P = #pipeline{pending = Q, num_pending = N}) ->
    P#pipeline{pending = queue:in(Op, Q), num_pending = N + 1}.

-spec num_pending(t()) -> non_neg_integer().
num_pending(#pipeline{num_pending = N}) -> N.

-spec in_flight(t()) -> boolean().
in_flight(#pipeline{committing = C}) -> C =/= undefined.

%% Take up to `Max` oldest ops. The bound keeps a burst (a DOWN flood) from becoming
%% one huge commit whose plan/apply/broadcast would freeze the loop.
-spec take_batch(pos_integer(), t()) -> {[op()], t()}.
take_batch(Max, P = #pipeline{pending = Q, num_pending = N}) ->
    Size = min(N, Max),
    {Ops, Rest} = take_n(Q, Size, []),
    {Ops, P#pipeline{pending = Rest, num_pending = N - Size}}.

-spec take_pending(t()) -> {[op()], t()}.
take_pending(P = #pipeline{pending = Q}) ->
    {queue:to_list(Q), P#pipeline{pending = queue:new(), num_pending = 0}}.

take_n(Q, 0, Acc) ->
    {lists:reverse(Acc), Q};
take_n(Q, N, Acc) ->
    {{value, Op}, Q1} = queue:out(Q),
    take_n(Q1, N - 1, [Op | Acc]).

%% ---------------------------------------------------------------------------
%% Committing
%% ---------------------------------------------------------------------------

-spec start(reference(), reference(), map(), t()) -> t().
start(Ref, MRef, Plan, P = #pipeline{committing = undefined}) ->
    P#pipeline{committing = #{ref => Ref, mref => MRef, plan => Plan}}.

-spec finish(reference(), t()) -> {ok, map(), reference(), t()} | stale.
finish(Ref, P = #pipeline{committing = #{ref := Ref, mref := MRef, plan := Plan}}) ->
    {ok, Plan, MRef, P#pipeline{committing = undefined}};
finish(_Ref, _P) ->
    stale.

%% A monitored process died: if it is the commit worker, release the slot.
-spec worker_down(reference(), t()) -> {ok, map(), t()} | stale.
worker_down(MRef, P = #pipeline{committing = #{mref := MRef, plan := Plan}}) ->
    {ok, Plan, P#pipeline{committing = undefined}};
worker_down(_MRef, _P) ->
    stale.

-spec committed(integer(), t()) -> t().
committed(Version, P) -> P#pipeline{last_version = Version}.

-spec read_version(t()) -> undefined | integer().
read_version(#pipeline{last_version = V}) -> V.

%% The cached version belongs to the ended term: never pin a later term's read to it.
-spec new_term(t()) -> t().
new_term(P) -> P#pipeline{last_version = undefined}.

%% ---------------------------------------------------------------------------
%% Awaiting holders
%% ---------------------------------------------------------------------------

-spec await_acks(reference(), [direct()], reference(), non_neg_integer(), t()) -> t().
await_acks(BatchRef, Direct, TimerRef, Needed, P = #pipeline{acks = A, next_ack_seq = Seq}) ->
    P#pipeline{
        acks = A#{BatchRef => {Seq, Direct, TimerRef, Needed, #{}}}, next_ack_seq = Seq + 1
    }.

%% Distinct followers count once; at `Needed` the batch's direct registrations are done.
-spec ack(reference(), member_id(), t()) ->
    {done, [direct()], reference(), t()} | {waiting, t()}.
ack(BatchRef, Follower, P = #pipeline{acks = A}) ->
    case A of
        #{BatchRef := {Seq, Direct, TimerRef, Needed, Acked}} ->
            Acked1 = Acked#{Follower => true},
            case map_size(Acked1) >= Needed of
                true ->
                    {done, Direct, TimerRef, P#pipeline{acks = maps:remove(BatchRef, A)}};
                false ->
                    Entry = {Seq, Direct, TimerRef, Needed, Acked1},
                    {waiting, P#pipeline{acks = A#{BatchRef => Entry}}}
            end;
        _ ->
            {waiting, P}
    end.

-spec ack_timeout(reference(), t()) ->
    {expired, [direct()], non_neg_integer(), non_neg_integer(), t()} | none.
ack_timeout(BatchRef, P = #pipeline{acks = A}) ->
    case maps:take(BatchRef, A) of
        {{_Seq, Direct, _TimerRef, Needed, Acked}, A1} ->
            {expired, Direct, Needed, map_size(Acked), P#pipeline{acks = A1}};
        error ->
            none
    end.

%% Every batch still awaiting holders, in arrival order.
-spec take_acks(t()) ->
    {[{[direct()], reference(), non_neg_integer(), non_neg_integer()}], t()}.
take_acks(P = #pipeline{acks = A}) ->
    Batches = [
        {Direct, TimerRef, Needed, map_size(Acked)}
     || {_Seq, Direct, TimerRef, Needed, Acked} <- lists:sort(maps:values(A))
    ],
    {Batches, P#pipeline{acks = #{}}}.

%% ---------------------------------------------------------------------------
%% Exits
%% ---------------------------------------------------------------------------

%% Effects come out in op order.
-spec surrender(failed | deposed, [op()]) -> [effect()].
surrender(failed, Ops) ->
    {Replies, Retry} = lists:foldl(
        fun
            ({set_meta, _, _, _, Origin}, {Rs, Rt}) ->
                {[{reply, Origin, {error, no_leader}} | Rs], Rt};
            ({remove, Name, Pid, Origin}, {Rs, Rt}) when Origin =/= undefined ->
                {[{reply, Origin, ok} | Rs], [{remove, Name, Pid, undefined} | Rt]};
            (Op, {Rs, Rt}) ->
                {Rs, [Op | Rt]}
        end,
        {[], []},
        Ops
    ),
    lists:reverse(Replies) ++
        case Retry of
            [] -> [];
            _ -> [{retry, lists:reverse(Retry)}]
        end;
surrender(deposed, Ops) ->
    lists:flatmap(fun deposed/1, Ops).

deposed({add, Name, Pid, Meta, {local, From}}) ->
    [{intent, {register, Name, Pid, Meta}, From}];
deposed({add, _Name, _Pid, _Meta, _Forwarded}) ->
    [];
deposed({set_meta, _Name, _Index, _Data, Origin}) ->
    [{reply, Origin, {error, no_leader}}];
deposed({remove, Name, Pid, Origin}) ->
    Reply =
        case Origin of
            undefined -> [];
            _ -> [{reply, Origin, ok}]
        end,
    Reply ++ retract(Name, Pid);
deposed({retract, Name, Pid}) ->
    retract(Name, Pid);
deposed({down, _Name, _Ref}) ->
    [].

%% Nothing was bound when an unregister was asked: nothing to retract.
retract(_Name, undefined) -> [];
retract(Name, Pid) -> [{intent, {unregister, Name, Pid}, answered}].
