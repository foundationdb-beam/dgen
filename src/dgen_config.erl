-module(dgen_config).

-export([
    backend/0,
    terminate_on_conflict/1,
    conflict_kill_budget/1,
    conflict_release_ttl/1,
    register_replicas/1,
    register_timeout/1,
    replicate_timeout/1,
    strict_replication/1,
    reject_when_degraded/1,
    commit_batch_size/1,
    connectivity/1
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
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

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

%% Maximum number of write ops (registrations, unregisters, auto-unregisters) the
%% leader coalesces into a single group commit.  Ops beyond this ride the following
%% commit.  This bounds the *inline* per-commit work — plan, replica apply, and the
%% broadcast fan-out are all O(batch) — so a burst (a node with many names leaving
%% floods the leader with DOWNs) is split across several bounded commits and the
%% leader's message loop stays responsive *between* them, instead of freezing under
%% one enormous batch.  Lower it to cap the burst; raise it to coalesce more
%% aggressively (fewer commits, larger inline bursts).  Milliseconds of latency, not
%% correctness: the batched outcome equals sequential processing at any size.
-spec commit_batch_size(config()) -> pos_integer().
commit_batch_size(Config) ->
    get(Config, commit_batch_size, 5000).

%% Caller-side bound (ms) on how long `dgen_registry:register_name/2,3` waits for
%% its verdict before **exiting** with a call timeout (§3 of the design doc): a
%% timeout is not converted to `no` — `no` means the leader adjudicated (taken /
%% no leader / unreachable), a timeout means nothing was adjudicated, and masking
%% it as `no` would hand OTP's via machinery a wrong `already_started` error.
%% Unlike the other knobs this one is resolved in the *calling* process, which has
%% no handle on the per-registry options map — so only the `dgen` application
%% environment and the built-in default apply (no per-registry override).
-spec register_timeout(config()) -> pos_integer().
register_timeout(Config) ->
    get(Config, register_timeout, 5000).

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
%%
%% Scope caveat: the budget is tracked in the *current leader's* process state and
%% is not merged across a handoff (unlike the release trail, §5.6) — a leadership
%% change starts the new leader with a fresh budget for every name.  The bound is
%% therefore per-name *per leader term*, not global; a conflict that regenerates
%% across failovers can be killed up to `Count` times per term.
-spec conflict_kill_budget(config()) -> {pos_integer(), pos_integer()}.
conflict_kill_budget(Config) ->
    get(Config, conflict_kill_budget, {3, 60000}).

%% How long (ms) a pid explicitly released while alive (an unregister of a live
%% process) stays in the conflict-detector trail (§5.6).  The trail is what stops
%% the detector from kill-both-ing a legitimately unregistered-then-re-registered
%% name when a lagging member reports the old binding at a handoff gather, so it
%% must comfortably outlast realistic disconnect windows — pruning is additionally
%% suspended while any member is disconnected.  Entries are tiny (pid + timestamp)
%% and only explicit live releases create them, so a generous default is cheap.
-spec conflict_release_ttl(config()) -> pos_integer().
conflict_release_ttl(Config) ->
    get(Config, conflict_release_ttl, 600000).

%% Who is responsible for keeping the Erlang-distribution mesh in step with this
%% registry's membership (§4.6, §8).
%%
%%   `self_managed` (default) — the registry's connector runs the proactive mesh: it
%%     periodically reads the authoritative member set and opens a distribution
%%     connection to every member node, so `nodes()` converges without any external
%%     discovery.  Every registry is self-sufficient.
%%
%%   `provided_externally` — the connector does **not** mesh: it opens no distribution
%%     connections and does not read the member set on a timer.  The registry assumes
%%     the connections it needs are established by *something else on the node* —
%%     typically another `self_managed` "system" registry whose member set spans a
%%     superset of this registry's nodes (distribution links are node-global, so a
%%     tenant registry free-rides on them).  The per-registry liveness backstops (the
%%     leader-liveness probe, the stranded-member reap, the durable-epoch nudge) stay
%%     active — they are registry-scoped and cannot be delegated.
%%
%% Only `provided_externally` disables meshing; any other value (including an
%% unrecognised one) resolves to `self_managed`, so a typo fails *safe* — toward doing
%% the connectivity work rather than silently isolating the node.  See §4.6 for the
%% contract this mode places on the deployer and the isolation it risks if that
%% contract is broken.
-spec connectivity(config()) -> self_managed | provided_externally.
connectivity(Config) ->
    case get(Config, connectivity, self_managed) of
        provided_externally -> provided_externally;
        _ -> self_managed
    end.
