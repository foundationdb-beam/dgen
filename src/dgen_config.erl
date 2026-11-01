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

%% The registry tuning knobs below are set per registry. Each one is read from the
%% options map passed to `dgen_registry:start_link/3` for that registry. If it isn't
%% there, we fall back to the `dgen` application environment, and then to a built-in
%% default. This lets one registry be strict while another degrades open. The knobs
%% are documented for users in docs/design/dgen_registry_design.md (§8), and section
%% references such as `§5.6` point there.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

%% Resolves a knob from the per-registry option, then the application env, then
%% the built-in default.
-spec get(config(), atom(), term()) -> term().
get(Config, Key, Default) ->
    case maps:find(Key, Config) of
        {ok, Value} -> Value;
        error -> application:get_env(dgen, Key, Default)
    end.

%% The number of distinct follower replicas that must confirm a direct registration
%% before the leader answers `yes` (§5.5, §8). It is capped at the number of
%% followers. With no followers the leader answers immediately, since nothing would
%% survive the loss of the only node anyway. The default of 1 keeps every name on
%% at least two nodes.
-spec register_replicas(config()) -> non_neg_integer().
register_replicas(Config) ->
    get(Config, register_replicas, 1).

%% The most write operations (registrations, unregisters and auto-unregisters) the
%% leader groups into a single commit. Any extra operations go in the next commit.
%% Planning, applying to the replica, and broadcasting all cost O(batch), and they
%% run inside the leader's message loop. When a node with many names leaves, the
%% leader is flooded with DOWNs. The cap splits that burst into several bounded
%% commits, so the leader stays responsive between them instead of freezing on one
%% huge batch. Lower it to cap the burst. Raise it to make fewer, larger commits.
%% This only affects latency. The outcome of a batch is the same as processing its
%% operations one at a time, at any size.
-spec commit_batch_size(config()) -> pos_integer().
commit_batch_size(Config) ->
    get(Config, commit_batch_size, 5000).

%% How long, in ms, `dgen_registry:register_name/2,3` waits for an answer before
%% it exits with a call timeout (§3). We don't turn a timeout into `no`. A `no`
%% means the leader decided against the registration. A timeout means nothing
%% was decided, and reporting it as `no` would make OTP's `via` machinery return
%% a wrong `already_started`.
%% Unlike the other knobs, the caller resolves this one in its own process, which
%% has no access to the per-registry options map. So on the caller side only the
%% `dgen` application environment and the built-in default apply.
-spec register_timeout(config()) -> pos_integer().
register_timeout(Config) ->
    get(Config, register_timeout, 5000).

%% How long, in ms, a direct registration waits for its replica acks before the
%% `strict_replication` policy decides the outcome.
-spec replicate_timeout(config()) -> pos_integer().
replicate_timeout(Config) ->
    get(Config, replicate_timeout, 1000).

%% What to do when a direct registration doesn't get `register_replicas` acks in
%% time (§8). With `false`, the default, we degrade open. The leader answers `yes`
%% on its own and emits telemetry. Uniqueness is already committed, and the async
%% broadcast keeps spreading the name. With `true`, we fail closed. The leader
%% answers `no` and removes the binding it just applied, so the caller never sees
%% a `yes` that wasn't replicated.
-spec strict_replication(config()) -> boolean().
strict_replication(Config) ->
    get(Config, strict_replication, false).

%% Prevents duplicate names during a partition instead of cleaning them up later
%% (§5.6). Sometimes a member in the committed member set doesn't respond to the
%% leader's handoff gather, because it is unreachable. When this is `true` and the
%% last gather was incomplete like that, the leader refuses to register any name it
%% doesn't already hold. The unreachable member might hold that name, and
%% registering it again would create a duplicate. With `false`, the default, we
%% allow those registrations and rely on §5.6 termination to resolve duplicates
%% when the partition heals.
%%
%% This is deliberately blunt. The leader has no durable record of taken names, so
%% it can't tell a real duplicate from a fresh name, and it rejects every new name
%% while degraded. It also can't tell a partition from a planned scale-down. A
%% member that leaves and never returns keeps the leader degraded until the cluster
%% is whole again. That is why the default is `false`. Enable it only where failing
%% closed during a partition matters more than availability.
-spec reject_when_degraded(config()) -> boolean().
reject_when_degraded(Config) ->
    get(Config, reject_when_degraded, false).

%% Registry conflict termination (§5.6). A conflict is two different live pids
%% holding one name, found during a handoff gather. When this is `true`, the
%% default, the leader kills both processes, broadcasts an unregister, and raises
%% an alarm. With `false` it only raises the alarm, and the duplicate stays until
%% an operator steps in. The registry no longer keeps a per-name occupancy set
%% (§4.4), so termination is what ultimately guarantees uniqueness. That is why it
%% is on by default.
-spec terminate_on_conflict(config()) -> boolean().
terminate_on_conflict(Config) ->
    get(Config, terminate_on_conflict, true).

%% Limits conflict termination to `Count` kills per name in each `WindowMs`. Past
%% that, the registry stops killing for that name and only raises an alarm for an
%% operator. This stops a bug that keeps recreating a conflict from looping forever.
%%
%% The budget lives in the current leader's process state. Unlike the release trail
%% (§5.6), it isn't carried over in a handoff, so a new leader starts with a fresh
%% budget for every name. The limit is therefore per name and per leader term, not
%% global. A conflict that keeps coming back across failovers can be killed up to
%% `Count` times in each term.
-spec conflict_kill_budget(config()) -> {pos_integer(), pos_integer()}.
conflict_kill_budget(Config) ->
    get(Config, conflict_kill_budget, {3, 60000}).

%% How long, in ms, the conflict detector remembers a live pid that was explicitly
%% unregistered (§5.6). Suppose a name is unregistered and then registered to a new
%% pid, and a lagging member still reports the old binding at a handoff gather. The
%% trail is what tells the detector this isn't a conflict, so it doesn't kill both
%% pids. The TTL must comfortably outlast realistic disconnects, and we also pause
%% pruning while any member is disconnected. Entries are tiny (a pid and a
%% timestamp), and only explicit unregisters of live pids create them, so a
%% generous default is cheap.
-spec conflict_release_ttl(config()) -> pos_integer().
conflict_release_ttl(Config) ->
    get(Config, conflict_release_ttl, 600000).

%% Decides who keeps Erlang distribution connected to every node in this
%% registry's membership (§4.6, §8).
%%
%%   `self_managed` (default). The registry's connector does it. It periodically
%%     reads the authoritative member set and connects to every member node, so
%%     `nodes()` converges without any external discovery. Each registry looks
%%     after itself.
%%
%%   `provided_externally`. The connector opens no distribution connections and
%%     doesn't poll the member set. The registry assumes something else on the
%%     node sets up the connections it needs. Usually that is another
%%     `self_managed` "system" registry whose members cover all of this registry's
%%     nodes. Distribution links are shared by the whole node, so a tenant registry
%%     can rely on them. The per-registry liveness checks stay active: the
%%     leader-liveness probe, the stranded-member reap, and the durable-epoch nudge.
%%     Each belongs to one registry and can't be handed off to another.
%%
%% Only `provided_externally` turns the connections off. Any other value, including
%% a typo, resolves to `self_managed`. That way a mistake errs toward connecting
%% rather than silently isolating the node. See §4.6 for what this mode requires of
%% the deployer and how the node can end up isolated if that isn't met.
-spec connectivity(config()) -> self_managed | provided_externally.
connectivity(Config) ->
    case get(Config, connectivity, self_managed) of
        provided_externally -> provided_externally;
        _ -> self_managed
    end.
