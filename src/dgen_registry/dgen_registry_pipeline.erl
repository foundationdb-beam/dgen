-module(dgen_registry_pipeline).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("dgen_registry_member.hrl").

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Tracks the leader's writes, meaning every op it has accepted and not yet answered.
%% `§N.N` refers to docs/design/dgen_registry_design.md.
%%
%% An op moves through these phases. Each phase has its own field:
%%
%%   queued      `pending`     Waiting for the next group commit.
%%   committing  `committing`  The one commit in flight. New ops wait for the next one.
%%   awaiting    `acks`        A committed direct registration. Before replying `yes`,
%%   holders                   we wait until a follower holds a copy (§5.5). On the
%%                             follower side, the matching phase is `awaiting_version`
%%                             in dgen_registry_intents.
%%
%% The member plans and applies the ops itself, in plan_batch/4 and
%% apply_committed_plan/3.
%%
%% surrender/2 decides what happens to ops the leader can't finish. With `failed`, the
%% commit didn't land, or it landed under an older epoch, so we retry the ops.
%% With `deposed`, we hand each op off according to its kind. The rules match the ones
%% the follower uses for its intents:
%%
%%   add, local       Becomes a follower intent and is re-sent to the next leader.
%%   add, forwarded   No reply. The follower that forwarded it re-sends its own intent.
%%   set_meta         Replies `{error, no_leader}`, on `failed` too. It replaces the
%%                    value, so re-sending it is unsafe.
%%   remove/retract   A waiting caller gets `ok`, on `failed` too. The removal then
%%                    continues as an already-answered unregister intent that only
%%                    removes the name if the same pid still holds it.
%%   down             Dropped, because the next leader monitors the pid itself.
%%
%% So `no` always means the name is taken (§3). It never means the leader couldn't
%% finish.
%%
%% surrender/2 has no side effects. It returns effects for the member to carry out.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([new/0, enqueue/2, get_num_pending/1, is_in_flight/1, take_batch/2, take_pending/1]).
-export([
    start_commit/4,
    finish_commit/2,
    handle_worker_down/2,
    set_last_version/2,
    get_read_version/1,
    start_new_term/1
]).
-export([await_acks/5, record_ack/3, handle_ack_timeout/2, take_acks/1]).
-export([surrender/2]).

-export_type([t/0, op/0, origin/0, direct/0, effect/0]).

-type member_id() :: dgen_registry_elector:member_id().
-type from() :: gen_server:from().

%% Where an op came from, which tells us how to answer it. The plain variants are
%% direct calls, answered through `From`. For the `forward*` variants we cast a
%% `{register_reply | set_meta_reply | unregister_reply, Ref, ...}` to the forwarding
%% member.
-type origin() ::
    {local, from()}
    | {forward, member_id(), reference()}
    | {meta, from()}
    | {forward_meta, member_id(), reference()}
    | {unreg, from()}
    | {forward_unreg, member_id(), reference()}.

%% A write op on the leader. A batch goes through one fenced commit and is applied in
%% arrival order, so the result is the same as processing ops one at a time. For
%% `remove`, we capture `ReleasedPid` before the optimistic local delete. When it is
%% set, it drives the durable clear and is added to the conflict trail (§5.6). `down`
%% carries the monitor reference, so we can ignore a stale DOWN.
-type op() ::
    {add, Name :: term(), pid(), Meta :: {map(), term()}, origin()}
    | {set_meta, Name :: term(), Index :: map(), Data :: term(), origin()}
    | {remove, Name :: term(), ReleasedPid :: pid() | undefined, origin() | undefined}
    | {retract, Name :: term(), pid()}
    | {down, Name :: term(), reference()}.

%% A registration awaiting holders. We keep the binding along with the origin, so that
%% under strict replication a timed-out wait can retract the binding and refuse the
%% call.
-type direct() :: {origin(), Name :: term(), pid()}.

-type t() :: #pipeline{}.

-type effect() ::
    {reply, origin(), term()}
    %% Hand the request to the follower-side intents (dgen_registry_intents:submit/5).
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

-spec get_num_pending(t()) -> non_neg_integer().
get_num_pending(#pipeline{num_pending = N}) -> N.

-spec is_in_flight(t()) -> boolean().
is_in_flight(#pipeline{committing = C}) -> C =/= undefined.

%% Takes up to `Max` of the oldest ops. The limit keeps a burst, such as a flood of
%% DOWNs, from turning into one huge commit whose planning, applying and broadcasting
%% would freeze the loop.
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

-spec start_commit(reference(), reference(), map(), t()) -> t().
start_commit(Ref, MRef, Plan, P = #pipeline{committing = undefined}) ->
    P#pipeline{committing = #{ref => Ref, mref => MRef, plan => Plan}}.

-spec finish_commit(reference(), t()) -> {ok, map(), reference(), t()} | stale.
finish_commit(Ref, P = #pipeline{committing = #{ref := Ref, mref := MRef, plan := Plan}}) ->
    {ok, Plan, MRef, P#pipeline{committing = undefined}};
finish_commit(_Ref, _P) ->
    stale.

%% Handles the death of a monitored process. If it was the commit worker, we free the
%% commit slot.
-spec handle_worker_down(reference(), t()) -> {ok, map(), t()} | stale.
handle_worker_down(MRef, P = #pipeline{committing = #{mref := MRef, plan := Plan}}) ->
    {ok, Plan, P#pipeline{committing = undefined}};
handle_worker_down(_MRef, _P) ->
    stale.

-spec set_last_version(integer(), t()) -> t().
set_last_version(Version, P) -> P#pipeline{last_version = Version}.

-spec get_read_version(t()) -> undefined | integer().
get_read_version(#pipeline{last_version = V}) -> V.

%% Clears the cached version, because it belongs to the term that just ended. A later
%% term must never pin its read version to it.
-spec start_new_term(t()) -> t().
start_new_term(P) -> P#pipeline{last_version = undefined}.

%% ---------------------------------------------------------------------------
%% Awaiting holders
%% ---------------------------------------------------------------------------

-spec await_acks(reference(), [direct()], reference(), non_neg_integer(), t()) -> t().
await_acks(BatchRef, Direct, TimerRef, Needed, P = #pipeline{acks = A, next_ack_seq = Seq}) ->
    P#pipeline{
        acks = A#{BatchRef => {Seq, Direct, TimerRef, Needed, #{}}}, next_ack_seq = Seq + 1
    }.

%% Records an ack from `Follower`. Each follower counts once. When `Needed` followers
%% have acked, the batch's direct registrations are done.
-spec record_ack(reference(), member_id(), t()) ->
    {done, [direct()], reference(), t()} | {waiting, t()}.
record_ack(BatchRef, Follower, P = #pipeline{acks = A}) ->
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

-spec handle_ack_timeout(reference(), t()) ->
    {expired, [direct()], non_neg_integer(), non_neg_integer(), t()} | none.
handle_ack_timeout(BatchRef, P = #pipeline{acks = A}) ->
    case maps:take(BatchRef, A) of
        {{_Seq, Direct, _TimerRef, Needed, Acked}, A1} ->
            {expired, Direct, Needed, map_size(Acked), P#pipeline{acks = A1}};
        error ->
            none
    end.

%% Takes every batch still awaiting holders, in arrival order.
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

%% Returns the effects in op order.
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
    lists:flatmap(fun get_deposed_effects/1, Ops).

get_deposed_effects({add, Name, Pid, Meta, {local, From}}) ->
    [{intent, {register, Name, Pid, Meta}, From}];
get_deposed_effects({add, _Name, _Pid, _Meta, _Forwarded}) ->
    [];
get_deposed_effects({set_meta, _Name, _Index, _Data, Origin}) ->
    [{reply, Origin, {error, no_leader}}];
get_deposed_effects({remove, Name, Pid, Origin}) ->
    Reply =
        case Origin of
            undefined -> [];
            _ -> [{reply, Origin, ok}]
        end,
    Reply ++ get_retract_effects(Name, Pid);
get_deposed_effects({retract, Name, Pid}) ->
    get_retract_effects(Name, Pid);
get_deposed_effects({down, _Name, _Ref}) ->
    [].

%% The name wasn't bound when the caller asked to unregister, so there's nothing to
%% retract.
get_retract_effects(_Name, undefined) -> [];
get_retract_effects(Name, Pid) -> [{intent, {unregister, Name, Pid}, answered}].
