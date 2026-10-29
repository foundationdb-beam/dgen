-module(dgen_registry_pipeline).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% The leader's write pipeline: every op it has accepted and not yet answered.
%%
%% Section references (e.g. `§5.5`) point to `docs/design/dgen_registry_design.md`.
%%
%% ## Phases
%%
%% A leader op moves through three phases, one field each:
%%
%%   queued        `pending`     waiting for the next group commit.
%%   committing    `committing`  in the single in-flight commit (one at a time, so
%%                               ops arriving meanwhile accumulate and ride the
%%                               next batch).
%%   awaiting      `acks`        a direct registration that committed and waits for
%%                 holders       a follower to confirm a copy before its `yes`
%%                               (§5.5).  The follower-side twin is an intent's
%%                               `awaiting_version` phase (dgen_registry_intents).
%%
%% The ops themselves are planned and applied by the member (plan_batch/4,
%% apply_committed_plan/3), which owns the replica and the monitors they touch.
%%
%% ## Exits
%%
%% An op leaves the pipeline the normal way (committed and answered) or because the
%% leader cannot finish it.  The second way has exactly one rule, `surrender/2`,
%% and two reasons:
%%
%%   failed   the commit did not land (fenced, a backend error, the worker died) or
%%            landed under a leadership view that has since moved.  The op is
%%            retried after a delay — on this leader if it still leads, otherwise
%%            surrendered as `deposed`.
%%   deposed  this member no longer leads.  The op leaves the pipeline.
%%
%% What happens to an op depends on its kind, by the same idempotence rule as the
%% follower's intents:
%%
%%   add, local origin       deposed: becomes a follower intent, re-driven to the
%%                           next leader.  failed: retried.
%%   add, forwarded origin   deposed: no reply — the forwarding follower holds the
%%                           intent and re-drives it on its own leadership event.
%%                           failed: retried.
%%   set_meta                `{error, no_leader}` either way: a replace is not safe
%%                           to re-send (a newer write could be overtaken).
%%   remove / retract        a waiting caller is answered `ok` (the removal is
%%                           owed, not lost); deposed: becomes an answered
%%                           unregister intent — a pid-guarded retract, held until
%%                           a leader can take it.  failed: retried.
%%   down                    deposed: dropped — the next leader monitors the pid
%%                           itself.  failed: retried.
%%
%% So a registration is never answered `no` because the leader could not finish
%% it: `no` stays an adjudicated refusal (§3).
%%
%% Nothing here sends a message or answers a caller; `surrender/2` returns the
%% effects for the member to carry out.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([new/0, enqueue/2, num_pending/1, in_flight/1, take_batch/2, take_pending/1]).
-export([start/4, finish/2, worker_down/2, committed/2, read_version/1, new_term/1]).
-export([await_acks/5, ack/3, ack_timeout/2, take_acks/1]).
-export([surrender/2]).

-export_type([t/0, op/0, origin/0, effect/0]).

-type member_id() :: dgen_registry_elector:member_id().
-type from() :: gen_server:from().

%% Where an op came from, and so how to answer it once committed:
%%   - {local, From}:           a direct register/2,3 call to this leader.
%%   - {forward, MemberId, Ref}: a registration forwarded by a follower; answered
%%                              with a {register_reply, Ref, _} cast to that member.
%%   - {meta, From}:            a set_metadata call made directly on this leader.
%%   - {forward_meta, MemberId, Ref}: a forwarded set_metadata; answered with a
%%                              {set_meta_reply, Ref, _} cast.
%%   - {unreg, From}:           an unregister_name call made directly on this leader.
%%   - {forward_unreg, MemberId, Ref}: a forwarded unregister; answered with an
%%                              {unregister_reply, Ref} cast.
-type origin() ::
    {local, from()}
    | {forward, member_id(), reference()}
    | {meta, from()}
    | {forward_meta, member_id(), reference()}
    | {unreg, from()}
    | {forward_unreg, member_id(), reference()}.

%% A leader write op.  All ops in a batch ride one fenced commit (a single version-key
%% bump) and are applied to the replica in arrival order, so the batched outcome
%% equals sequential one-at-a-time processing:
%%   - add:      a registration (with its metadata); the origin gets the yes/no.
%%   - set_meta: replace an existing registration's metadata; the origin gets `ok`,
%%               or `{error, not_registered}` if the name is not bound.
%%   - remove:   an unregister (by name).  ReleasedPid is the pid that was bound when
%%               the unregister was enqueued (or `undefined` if the name was unbound),
%%               captured before the leader removes it from the replica optimistically.
%%               A non-`undefined` ReleasedPid both drives the durable clear and is
%%               recorded in `recently_released` (the conflict-detector trail, §5.6).
%%               Origin (or `undefined` for a legacy cast / a retried op) is answered
%%               `ok` once the batch commits.
%%   - retract:  a pid-guarded clear, with no caller.
%%   - down:     an auto-unregister from a monitored process exit; Ref-guarded so a
%%               stale DOWN for an already-re-registered name is ignored.
-type op() ::
    {add, Name :: term(), pid(), Meta :: {map(), term()}, origin()}
    | {set_meta, Name :: term(), Index :: map(), Data :: term(), origin()}
    | {remove, Name :: term(), ReleasedPid :: pid() | undefined, origin() | undefined}
    | {retract, Name :: term(), pid()}
    | {down, Name :: term(), reference()}.

%% A direct registration awaiting holders: the origin to answer and the binding, so a
%% fail-closed timeout can retract it.
-type direct() :: {origin(), Name :: term(), pid()}.

-record(pipeline, {
    pending = queue:new() :: queue:queue(op()),
    %% Kept alongside `pending` so the batch size is known in O(1).
    num_pending = 0 :: non_neg_integer(),
    %% The in-flight commit: the worker's correlation ref, its monitor (so a worker
    %% that dies without a result does not wedge the pipeline), and the plan to apply.
    committing = undefined :: undefined | #{ref := reference(), mref := reference(), plan := map()},
    %% The last committed version, pinned as the next commit's read version to skip a
    %% GRV.  A stale value is handled by the worker retrying with a fresh GRV.
    last_version = undefined :: undefined | integer(),
    %% BatchRef => {Seq, Direct, TimerRef, Needed, Acked}: Needed is the
    %% `register_replicas` target, Acked the distinct followers that confirmed, and
    %% Seq the arrival order (take_acks/1 answers in it, never in reference order,
    %% which differs from run to run and would make simulated schedules
    %% irreproducible).
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
    %% Put these back in the pipeline after a delay.
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

%% Take up to `Max` of the oldest queued ops as the next batch, in commit order.
%% Bounding the batch keeps a burst (a departing node's DOWN flood) from coalescing
%% into one enormous commit whose O(batch) plan/apply/broadcast would freeze the loop.
-spec take_batch(pos_integer(), t()) -> {[op()], t()}.
take_batch(Max, P = #pipeline{pending = Q, num_pending = N}) ->
    Size = min(N, Max),
    {Ops, Rest} = take_n(Q, Size, []),
    {Ops, P#pipeline{pending = Rest, num_pending = N - Size}}.

%% Take every queued op (to surrender them).
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

%% The worker tracked under `Ref` reported back: release the commit slot.
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

%% Leadership ended: the cached commit version belongs to that term and must not pin
%% a later term's read version.
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

%% A follower confirmed it holds `BatchRef`'s bindings.  Distinct followers count
%% once each; at `Needed` the batch's direct registrations are done.
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
            %% Already answered (enough acks, or the timeout).
            {waiting, P}
    end.

%% `BatchRef`'s replicate timeout fired.
-spec ack_timeout(reference(), t()) ->
    {expired, [direct()], non_neg_integer(), non_neg_integer(), t()} | none.
ack_timeout(BatchRef, P = #pipeline{acks = A}) ->
    case maps:take(BatchRef, A) of
        {{_Seq, Direct, _TimerRef, Needed, Acked}, A1} ->
            {expired, Direct, Needed, map_size(Acked), P#pipeline{acks = A1}};
        error ->
            none
    end.

%% Every batch still awaiting holders, as {Direct, TimerRef, Needed, Got}, in
%% arrival order.
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

%% The ops the leader cannot finish, and why (see the module doc for the rule).
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
