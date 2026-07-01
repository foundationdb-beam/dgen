-module(dgen_config).

-export([
    backend/0,
    terminate_on_conflict/1,
    conflict_kill_budget/1,
    register_replicas/1,
    replicate_timeout/1,
    strict_replication/1,
    reject_when_degraded/1
]).

-type config() :: #{atom() => term()}.
-export_type([config/0]).

-define(DOCATTRS, ?OTP_RELEASE >= 27).
-if(?DOCATTRS).
-moduledoc false.
-endif.

backend() ->
    application:get_env(dgen, backend, dgen_erlfdb).

%% The registry tuning knobs below are **per-registry**: each is resolved from the
%% options map passed to `dgen_registry:start_link/3` for that registry, falling back
%% to the `dgen` application environment (a global default), then to a built-in
%% default. So one registry can be strict while another degrades open, with no global
%% coupling. They are documented for end users in `docs/dgen_registry_design.md`
%% (§8 Configuration); section references (e.g. `§5.6`) point there.

%% Resolve a knob: per-registry option > application env > built-in default.
-spec get(config(), atom(), term()) -> term().
get(Config, Key, Default) ->
    case maps:find(Key, Config) of
        {ok, Value} -> Value;
        error -> application:get_env(dgen, Key, Default)
    end.

%% Replicate-before-ack target for a *direct* registration (§5.5, §8): the number
%% of distinct follower replicas the leader waits to confirm before acking `yes`.
%% Bounded by the number of followers (with none, the leader acks immediately —
%% nothing survives the sole node anyway).  Default 1 (the two-holder invariant).
-spec register_replicas(config()) -> non_neg_integer().
register_replicas(Config) ->
    get(Config, register_replicas, 1).

%% How long a direct registration waits for its replica acks before the timeout
%% policy (`strict_replication`) fires.  Milliseconds.
-spec replicate_timeout(config()) -> pos_integer().
replicate_timeout(Config) ->
    get(Config, replicate_timeout, 1000).

%% Timeout policy for a direct registration that did not gather `register_replicas`
%% acks in time (§8).  `false` (default) → **degrade open**: ack `yes` leader-only
%% (uniqueness is already committed; the async broadcast keeps propagating) and emit
%% telemetry.  `true` → **fail closed**: reject the registration (`no`) and retract
%% the just-applied binding, so the caller never sees a `yes` that was not replicated.
-spec strict_replication(config()) -> boolean().
strict_replication(Config) ->
    get(Config, strict_replication, false).

%% Partition-case *prevention* (§5.6).  When `true`, a leader whose most recent
%% handoff gather was **incomplete** (a member in the committed member set did not
%% respond — it is unreachable) refuses to register a name it does not currently hold,
%% since that name might be held by the unreachable member and registering it would be
%% a re-issue. `false` (default) keeps the reactive posture: re-issues during a
%% partition are caught at heal by §5.6 termination instead of prevented.
%%
%% This is deliberately blunt: with no durable taken-set the leader cannot tell a
%% genuine re-issue from a fresh name, so it rejects *all* new names while degraded —
%% and it cannot tell a partition from a legitimate scale-down, so a member that
%% leaves and never returns keeps the leader degraded until the cluster is whole
%% again. Hence default `false`; enable it only where fail-closed-during-partition is
%% preferred over availability.
-spec reject_when_degraded(config()) -> boolean().
reject_when_degraded(Config) ->
    get(Config, reject_when_degraded, false).

%% Registry conflict termination (§5.6).  When `true` (default), a confirmed
%% divergence — two different live pids holding one name, observed at a handoff
%% gather — forcibly terminates the conflicting processes (kill-both), broadcasts an
%% unregister, and alarms.  Set `false` to detect + alarm only (no kill), leaving the
%% uniqueness hole open until an operator intervenes.  Termination is the load-bearing
%% uniqueness backstop now that the per-name occupancy set is gone (§4.4), so it
%% defaults on.
-spec terminate_on_conflict(config()) -> boolean().
terminate_on_conflict(Config) ->
    get(Config, terminate_on_conflict, true).

%% Kill budget for conflict termination: at most `Count` kills per name per
%% `WindowMs`, after which the registry stops killing that name and escalates to an
%% operator (alarm only), so a bug that regenerates a conflict cannot loop.
-spec conflict_kill_budget(config()) -> {pos_integer(), pos_integer()}.
conflict_kill_budget(Config) ->
    get(Config, conflict_kill_budget, {3, 60000}).
