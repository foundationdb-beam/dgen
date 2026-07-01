-module(dgen_registry_member).
-behaviour(gen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Local name cache and consistent read/write proxy for `dgen_registry`.
%%
%% Section references (e.g. `§5.6`) point to `docs/dgen_registry_design.md`, the
%% registry's design-and-guarantees document.
%%
%% Each node participating in a named registry runs one member process.
%% The member has two roles depending on whether it is the current leader.
%%
%% ## Storage
%%
%% Both leader and follower keep their local replica in a **named, `protected` ETS
%% table** (`dgen_registry:names_table/1`), owned and written **only** by this member
%% process — it is the single writer, so per-row updates are atomic and a row is never
%% observed torn. Each row is `{Name, Pid, Index, Data}`: the bound pid plus its
%% metadata (`Index`, the queryable map; `Data`, the opaque payload — both `#{}` /
%% `undefined` for a plain registration; see §4.7 of the design doc). The
%% table is the source of truth for the local replica; there is no parallel in-process
%% map. It is **not** durable: pids are node-local and process-lifetime-scoped, have no
%% meaning after a restart, and are never written to the backend. The member recreates
%% an empty table on (re)start — there is no heir — so a snapshot read in the gap before
%% the member is up (or while it is restarting after a crash) returns `undefined`.
%%
%% Consistent reads and writes go through the leader.  Single-key snapshot reads
%% (`dgen_registry:whereis_name/1`, `get_metadata/1`) are served **lock-free from the
%% calling process** by reading the row directly (`ets:lookup/2`), without contacting the
%% member at all — many callers read concurrently and never queue behind the member's
%% mailbox.  Multi-key indexed queries (`query/2`, `query_consistent/2`) instead run
%% **on the member's mailbox** against an in-state inverted index (`inv_index`): being
%% single-threaded, the member answers a query *between* batch applies, so it sees a
%% whole-batch-consistent snapshot rather than risking a half-applied group commit (§6).
%%
%% ## Follower role
%%
%% Keeps its local names replica (ETS) in sync by receiving `{name_registered, …}`,
%% `{name_unregistered, …}`, and `{apply_names_snapshot, …}` casts from the
%% leader (one-way replication).  All follower messages come from the leader
%% process, so Erlang's per-pair FIFO guarantee ensures followers always see a
%% snapshot before any `{name_registered}` broadcast that post-dates it.
%%
%% ## Membership and connectivity
%%
%% The member keeps the Erlang-distribution mesh in step with registry membership.
%% Periodically (and at startup, and on every `{nodeup, _}`) it reads the authoritative
%% member set from the elector — a DB-backed read, identical on every node regardless of
%% which one consumed the joins — and `net_kernel:connect_node/1`s to each member node
%% (`mesh_connect`). So once a node's join is committed, every member connects to it and
%% `nodes()` converges to include all reachable members; the application only has to make
%% the nodes *able* to connect (shared cookie, reachable hostnames), not wire up the mesh
%% itself. This is what lets a brand-new node, which so far exists only as a row in the
%% database, be drawn into the cluster: it connects to the existing members, the resulting
%% `{nodeup, _}` drives the rejoin below, and it is brought fully up to date.
%%
%% The member subscribes to `nodeup`/`nodedown` events via
%% `net_kernel:monitor_nodes/1`.  On `{nodeup, Node}`, the member re-announces
%% itself to the elector (`{join, Self}`).  This handles the case where an
%% Erlang-level network partition caused both sides to remove each other from
%% the member set via `{member_down}` while the DB remained healthy: once the
%% partition heals and distribution reconnects, both sides re-join and the
%% elector reconstitutes the cluster without requiring a restart.
%%
%% Forwards `{register, …}` and `{unregister, …}` to the leader. The register
%% forward is **asynchronous**: the follower stashes the caller's `From` (with the
%% registration's metadata) under a `Ref`, casts `{register_req, Ref, Self, Name, Pid,
%% Meta}` to the leader, and replies `{noreply}` — it never blocks on the leader. When
%% the leader's `{register_reply, Ref, yes|no}` arrives, the follower (on `yes`) writes
%% the registration's row (pid + metadata) into its own table and then answers the
%% caller; on `no` the table is left unchanged.
%%
%% A `set_metadata` forward is also asynchronous: the follower stashes `From` under a
%% `Ref`, casts `{set_meta_req, Ref, Self, Name, Index, Data}`, and answers the caller
%% when the leader's `{set_meta_reply, Ref, _}` arrives. No optimistic update is needed —
%% routing the reply back through this member means the leader's `{metadata_set, …}`
%% broadcast (FIFO, ahead of the reply) has updated the follower's row before the caller
%% is answered, preserving read-after-write for a subsequent local `get_metadata`.
%%
%% This async forward is required for deadlock-freedom: no member ever blocks its
%% loop on a call to another member on the registration path. (A blocking forward
%% would deadlock once the leader awaits a follower's replication ack for a direct
%% registration — every follower could be blocked forwarding while the leader is
%% blocked awaiting one of them.) The follower's `yes`-time `names` update also makes
%% it the binding's **confirmed second holder**: by the time the caller sees `yes`,
%% both the leader and the forwarding follower hold the binding, so it survives
%% either node failing — at no extra round-trip.
%%
%% Forwards still in flight when leadership changes are rejected (`no`); the caller
%% retries against the new leader.
%%
%% A **consistent read** (`whereis_name_consistent`) on a follower forwards the same
%% way: the follower casts `{whereis_req, Name, From}` to the leader and replies
%% `{noreply}`, and the leader answers the caller's `From` directly from its
%% authoritative map. The follower never blocks its loop on the read, so a consistent
%% read cannot head-of-line block its other messages. If the leader's node is
%% unreachable, the follower answers `undefined` immediately rather than hang the
%% caller.
%%
%% ## Leader role
%%
%% Assumed when the elector calls `{elector_assume_and_distribute, …}`.  On
%% assuming leadership the member reconstructs its names map by gathering the
%% freshest of every reachable member's replica (`gather_maps/3`) — the freshest
%% map *is* the reconstructed state (there is no durable taken-set to reconcile
%% against, §4.4), sets up `erlang:monitor/2` for every entry, and distributes
%% `{apply_names_snapshot}` casts to all followers from its own process (same
%% sender as future `{name_registered}` broadcasts — see elector moduledoc for the
%% FIFO ordering guarantee).  Any stale Pid entries are removed when their DOWN
%% signals arrive.
%%
%% The leader is the sole writer for the name table. It:
%%
%% - Serves all writes through one **group-commit buffer** — registrations
%%   (`{register}` calls), unregisters (`{unregister}` casts), and auto-unregisters
%%   (monitored-process `DOWN`s). Each is parked in the pending buffer; at most one
%%   commit runs at a time, so ops that arrive while a commit is in flight accumulate
%%   and ride the next batch (`dgen_registry_names:start_commit/4`, whose only write is
%%   a fenced bump of the per-registry version key — the name delta is applied in
%%   memory). The commit runs in a `dgen_transaction` worker so this
%%   process keeps serving snapshot reads while it is in flight, and the read version
%%   is pinned to the last committed version to skip a GRV (a stale pin falls back to
%%   a fresh GRV in the worker). Ops are applied in arrival order, so a burst of
%%   `DOWN`s (a peer node dying) or registrations (a node booting) coalesces into few
%%   commits while preserving the one-at-a-time outcome.
%%   When the worker reports back, each registration is answered through its origin
%%   (a direct call via `gen_server:reply/2`, a forwarded one via a `{register_reply}`
%%   cast to the forwarding follower), pids are monitored, and `{name_registered, …}` /
%%   `{name_unregistered, …}` are replicated. In-batch duplicates and already-taken
%%   names are rejected (`no`); a `DOWN` whose ref no longer matches the current
%%   binding is ignored; a fenced commit (leadership lost) rejects the whole batch.
%% - Handles `{whereis, LogicalName}` calls: consistent read from local map.
%% - Handles `{unregister, LogicalName}` casts: updates the map, demonitors,
%%   and replicates `{name_unregistered, …}`.
%% - Monitors every registered Pid. When one dies, removes from the map
%%   and replicates `{name_unregistered, …}` to followers.
%%
%% On relinquishing leadership the member demonitors all registered Pids and
%% clears the leader-only state.  The names replica (ETS) is kept intact (it still
%% serves caller-side snapshot reads).
%%
%% ## Replication and the two-holder invariant
%%
%% A registration is acked `yes` only once **two members** hold the binding, so it
%% survives any single node loss. The cost is asymmetric and maps onto the topology:
%%
%% - A **forwarded** registration (the common case — every follower forwards) is held
%%   by the leader *and* the forwarding follower as soon as the follower applies its
%%   optimistic update, so it is two-holder for free, with no extra round-trip.
%% - A **direct** registration (one originating on the leader's own node) has only the
%%   leader as a holder, so the leader does replicate-before-ack: it broadcasts the
%%   binding, then waits for ≥1 follower to confirm a copy (`replicate_sync` →
%%   `replicate_ack`) before replying `yes`. Only the leader — 1 of m nodes — pays this,
%%   which matches the forward asymmetry: the other m-1 nodes forward and get the second
%%   holder for free.
%%
%% ### Caveats — this guarantee is deliberately modest, and under review
%%
%% The two-holder invariant is a pragmatic middle ground, not a strong durability
%% guarantee. It deliberately avoids both persisting pids (which cannot be done
%% meaningfully — a pid is node- and lifetime-scoped, and a stale pid could name a
%% reused process) and forcing every caller to re-register on failover (onerous for an
%% arbitrary process). The trade-offs:
%%
%% - **Single-fault only.** Losing both holders (e.g. the leader and the one follower
%%   that acked) loses the binding. With the per-name occupancy set dropped (§4.4), the
%%   DB no longer marks the name taken, so this now costs **both** addressability *and*
%%   uniqueness: a lost name may be re-issued. Uniqueness is thus single-fault, matching
%%   addressability; the kill-both-on-conflict termination backstop (§5.6) is what
%%   re-converges if a re-issue ever races a survivor.
%% - **The replica is ephemeral and arbitrarily placed.** The "second holder" is just an
%%   in-memory copy in *some* follower's map — whichever acked first, unrelated to where
%%   the pid lives. Survival depends on that particular node staying up.
%% - **Degrade-open weakens it under load.** If fewer than `register_replicas` acks
%%   arrive within `replicate_timeout`, the leader (by default) acks `yes` leader-only
%%   (n=1) to preserve liveness — emitting telemetry. So the invariant is really "N
%%   holders, or one under timeout." Set `strict_replication = true` to fail-closed
%%   instead (reject + retract the binding); set `register_replicas > 1` for more holders
%%   (bounded by the follower count). See §8 and `dgen_config`.
%% - **Pids cannot outlive their node.** If the pid's *own* node dies the pid is dead and
%%   the name should be released anyway (its monitor fires). So the two-holder invariant
%%   really protects the *routing entry* against loss of a node that does **not** host
%%   the pid (typically the leader) — narrower than "registrations are replicated".
%% - **Whether n=2 is the right point is open.** A different point on the spectrum (a
%%   re-registration contract, durable routing hints, or higher replication) may prove
%%   better; the current choice is not considered final.
%%
%% ## Failure model
%%
%% Name-to-pid mappings are intentionally not stored in the backend — and, since
%% §4.4, neither is the taken-set: the only durable keys are the leader key (fence)
%% and a per-registry version counter.  As a result:
%%
%% - **Leader crash**: the new leader reconstructs its map from the freshest of
%%   every reachable member's replica (`gather_maps/3`).  A binding the dead
%%   leader committed to its map but had not yet replicated to any survivor is
%%   silently lost — and, with no durable taken-set, its name may be re-issued
%%   (single-fault uniqueness).  A caller that received `yes` from `register_name/2`
%%   may therefore find the name absent, or reassigned, after a leader failover;
%%   re-registration after detecting the loss is the caller's responsibility, and
%%   the termination backstop (§5.6) re-converges a re-issue that races a survivor.
%%
%% - **Full cluster restart**: all registered names are lost.  Applications
%%   must re-register on startup.

%% Maximum number of write ops coalesced into a single commit.  A larger pending
%% buffer is split across successive commits, bounding transaction size.
-define(MAX_BATCH, 10000).

%% (The replicate-before-ack target and timeout are configurable per registry — see
%% dgen_config:register_replicas/1, replicate_timeout/1, strict_replication/1.)

%% Per-member bound on the handoff gather: a member that does not return its names
%% map within this is skipped (its bindings are unavailable for this gather; a
%% binding only that member held is then lost — single-fault uniqueness).
-define(GATHER_TIMEOUT, 2000).

%% How often each member proactively connects Erlang distribution to every other
%% member node, so the cluster converges to a full mesh and `nodes()` returns all
%% members (see the "Membership and connectivity" guarantee in the design doc).  This
%% is a backstop; startup and `nodeup` also trigger an immediate pass.
-define(MESH_INTERVAL, 10000).

%% After observing a node's `nodedown`, the mesh does *not* immediately reconnect it:
%% it waits this long so the departure can be recorded as a `{member_down}` and settle,
%% rather than fighting the partition machinery by healing a drop the instant it
%% happens.  A genuine departure leaves the member set within the window (and so is not
%% reconnected at all); a `{nodeup, _}` clears the suppression early.
-define(MESH_DOWN_COOLDOWN, 10000).

%% How long a pid the leader *explicitly* released (an unregister of a live process)
%% is remembered in `recently_released`.  This is the discriminator that keeps the
%% conflict detector (§5.6) from false-killing a process that was legitimately
%% unregistered-while-alive but is still reported by a lagging member: a real
%% unregister leaves a trail here, a reconstruction-drop (the genuine conflict) does
%% not.  It only needs to outlast normal broadcast lag, not a partition.
-define(RELEASED_TTL, 30000).

%% Bound on the per-peer aliveness probe used when adjudicating a suspected conflict.
-define(ALIVE_PROBE_TIMEOUT, 1000).

-export([start_link/2]).
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).
%% Exported for unit testing the §5.6 conflict-detection predicate in isolation.
-export([detect_conflicts/3]).
%% Exported for unit testing the group-commit batch planner in isolation — in
%% particular, the batch-local overlay (`seed_lookup/3`, the `removed` marker) that lets
%% a later op in the same batch see an earlier op's not-yet-committed decision without a
%% full-registry seed map.  Takes a real ETS table (the same `{Name, Pid, Index, Data}`
%% row shape as the member's names table) so a test can pre-populate the pre-batch state.
-export([plan_batch/4]).

%% A registration's metadata: an indexed map (queryable, a later increment) and an
%% opaque data payload (returned verbatim).  A plain register_name/2 uses the empty
%% default `{#{}, undefined}`.  See §4.7 of the design doc.
-type meta() :: {Index :: map(), Data :: term()}.

%% Where a write op came from, and how to answer it once committed:
%%   - {local, From}:           a direct register/2,3 call to this leader; answered
%%                              with gen_server:reply/2.
%%   - {forward, MemberId, Ref}: a registration forwarded by a follower; answered
%%                              with a {register_reply, Ref, _} cast to that member,
%%                              which then answers its own caller.
%%   - {meta, From}:            a set_metadata call made directly on this leader;
%%                              answered with gen_server:reply/2.
%%   - {forward_meta, MemberId, Ref}: a set_metadata forwarded by a follower; answered
%%                              with a {set_meta_reply, Ref, _} cast to that member,
%%                              which then answers its own caller (after the FIFO-ordered
%%                              {metadata_set} broadcast has updated the follower's row).
-type origin() ::
    {local, gen_server:from()}
    | {forward, dgen_registry_elector:member_id(), reference()}
    | {meta, gen_server:from()}
    | {forward_meta, dgen_registry_elector:member_id(), reference()}.

%% A leader write op awaiting the next group commit.  All ride one fenced commit (a
%% single version-key bump) and are applied to the local replica in arrival order, so
%% the batched outcome equals sequential one-at-a-time processing:
%%   - add:      a registration (with its metadata); the origin gets the deferred yes/no.
%%   - set_meta: replace an existing registration's metadata; the origin ({meta, From})
%%               gets `ok`, or `{error, not_registered}` if the name is not bound.
%%   - remove:   an unregister (by name).  ReleasedPid is the pid that was bound when
%%               the unregister was enqueued (or `undefined` if the name was unbound),
%%               captured before the leader removes it from the replica optimistically.
%%               A non-`undefined` ReleasedPid both drives the durable clear and is
%%               recorded in `recently_released` (the conflict-detector trail, §5.6).
%%   - down:     an auto-unregister from a monitored process exit; Ref-guarded so a
%%               stale DOWN for an already-re-registered name is ignored.  The pid is
%%               dead, so it is *not* recorded in `recently_released`.
-type batch_op() ::
    {add, LogicalName :: term(), Pid :: pid(), Meta :: meta(), Origin :: origin()}
    | {set_meta, LogicalName :: term(), Index :: map(), Data :: term(), Origin :: origin()}
    | {remove, LogicalName :: term(), ReleasedPid :: pid() | undefined}
    | {retract, LogicalName :: term(), Pid :: pid()}
    | {down, LogicalName :: term(), Ref :: reference()}.

-record(state, {
    member_id :: dgen_registry_elector:member_id(),
    %% The elector has no registered name (see dgen_registry's "Process identity"
    %% moduledoc note); this is its pid, discovered once via the shared supervisor in
    %% the `discover_elector` handle_continue right after init, and never re-resolved
    %% afterward — the supervisor is `one_for_all`, so if the elector ever dies this
    %% member is torn down and restarted alongside it (a fresh discovery on the next
    %% start), never left holding a stale reference. `undefined` only for the brief
    %% window between init/1 returning and discover_elector running.
    elector :: pid() | undefined,
    %% Backend handle and tuid, used by the leader to drive the fenced version-key
    %% commit (a single key per registry — no per-name state) and to fence on the
    %% leader key. See dgen_registry_names.
    tenant :: dgen_backend:tenant(),
    tuid :: dgen_server:tuid(),
    leader :: dgen_registry_elector:member_id() | undefined,
    %% Named, `protected` ETS table holding the local replica as `{Name, Pid, Index,
    %% Data}` rows (pid + metadata).  The member is its sole writer; any process reads it
    %% lock-free (`dgen_registry:whereis_name/1`, `get_metadata/1`).  Authoritative on the
    %% leader; replicated snapshot on followers.  Never written to durable storage — Pids
    %% are ephemeral.  On a handoff the freshest member's table is the sole reconstruction
    %% source (no durable taken-set).
    names_tab :: ets:tid() | atom(),
    %% Inverted index over the rows' `Index` (queryable) metadata, kept in this member's
    %% process state on *every* member (so `query/2` is a local read): `#{attr() =>
    %% #{value() => #{Name => []}}}` — attribute → value → set of names.  Derived state,
    %% maintained incrementally alongside every row write (see the `row_*` helpers) and
    %% rebuilt wholesale on a handoff / snapshot apply.  AND-equal queries intersect the
    %% per-clause posting sets (§4.7 of the design doc).
    inv_index = #{} :: #{term() => #{term() => #{term() => []}}},
    %% Peer-member monitors (all members)
    members :: #{dgen_registry_elector:member_id() => reference()},
    monitors :: #{reference() => dgen_registry_elector:member_id()},
    %% Registered-process monitors (leader only)
    name_to_ref :: #{term() => reference()},
    ref_to_name :: #{reference() => term()},
    %% Follower-only: registrations forwarded to the leader and awaiting their
    %% {register_reply}.  Ref => {LogicalName, Pid, Meta, From}; on the reply we apply
    %% the optimistic row update (yes), writing the full record (pid + metadata), and
    %% answer From.  Cleared on a leadership change (the reply from the old leader will
    %% never come — callers retry).
    forwards = #{} :: #{reference() => {term(), pid(), meta(), gen_server:from()}},
    %% Follower-only: set_metadata calls forwarded to the leader and awaiting their
    %% {set_meta_reply}.  Ref => From.  The reply is routed back *through* this member
    %% (not direct from the leader) so the leader's {metadata_set} broadcast — FIFO
    %% ahead of the reply — has updated our row before we answer the caller, preserving
    %% read-after-write.  Cleared on a leadership change (callers retry).
    meta_forwards = #{} :: #{reference() => gen_server:from()},
    %% Leader-only: direct (local-origin) registrations that have committed but are
    %% awaiting follower replica confirmation before being acked `yes` (the two-holder
    %% invariant, §5.5/§8).  BatchRef => {Direct, TimerRef, Needed, Acked}, where
    %% Direct is `[{Origin, Name, Pid}]`, Needed is the `register_replicas` target
    %% (bounded by follower count), and Acked is the set of distinct followers that
    %% confirmed.  Answered `yes` once `map_size(Acked) >= Needed`, or on timeout per
    %% the `strict_replication` policy (degrade-open `yes`, or fail-closed `no` +
    %% retract).
    pending_acks = #{} ::
        #{
            reference() =>
                {[{origin(), term(), pid()}], reference(), non_neg_integer(), #{
                    dgen_registry_elector:member_id() => true
                }}
        },
    %% Token used in our own {join, Self, Token} announcement.  Refreshed on
    %% each nodeup so any stale {member_down, Self, OldToken} in the queue is
    %% discarded by the elector.
    join_token :: reference(),
    %% Tokens received from the elector via snapshots, keyed by peer MemberId.
    %% Echoed back in {member_down, PeerId, Token} so the elector can distinguish
    %% a stale DOWN (from before a re-join) from a fresh one.
    peer_tokens :: #{dgen_registry_elector:member_id() => reference()},
    %% Leader-only group-commit buffer: write ops (adds + removes) awaiting their
    %% batched commit, as a FIFO queue (oldest at the front, the commit order).  An
    %% add's From is the deferred caller (a local direct register or a forwarding
    %% follower's call), answered via gen_server:reply/2 once the batch commits.
    pending = queue:new() :: queue:queue(batch_op()),
    %% Number of ops in `pending`, maintained incrementally so the batch size is
    %% known in O(1) (no length/1 scan on the commit path).
    num_pending = 0 :: non_neg_integer(),
    %% Leader-only: the in-flight commit, or undefined when none is running.  Only
    %% one commit runs at a time (group commit): ops arriving while it is in flight
    %% accumulate in `pending` and are committed in the next batch once it completes.
    %% Holds the worker correlation ref, the monitor ref (so a worker that dies
    %% without delivering a result does not wedge the commit lock), and the plan to
    %% apply on success.
    committing = undefined ::
        undefined | #{ref := reference(), mref := reference(), plan := map()},
    %% Leader-only: the last committed version, pinned as the next commit's read
    %% version to skip a GRV (cached GRV).  A stale value is handled by the worker
    %% retrying with a fresh GRV.  Reset on losing leadership.
    last_version = undefined :: undefined | integer(),
    %% The highest commit version this member has applied to `names` — bumped by
    %% every name broadcast (which carries its batch's commit version) and set from
    %% the snapshot on a leadership change.  Globally monotonic (FDB commit versions),
    %% so on a handoff the new leader picks the member with the highest applied_version
    %% as the freshest source for the gather.  All members track it.
    applied_version = 0 :: non_neg_integer(),
    %% Monotonically increasing leader term counter set by the elector.
    %% Broadcasts from a prior leader carry a smaller epoch and are discarded.
    epoch :: non_neg_integer(),
    %% Leader-only conflict-detection trail (§5.6): pids the leader *explicitly*
    %% released (an unregister of a live process) → the wall-clock ms it happened.
    %% A handoff gather suppresses a divergence on a pid still in here (it was
    %% legitimately unregistered, a lagging member just has not caught up), so only a
    %% reconstruction-drop divergence — which leaves no trail — is killed.  TTL-pruned
    %% (?RELEASED_TTL) by a periodic self-message.
    recently_released = #{} :: #{pid() => integer()},
    %% Leader-only kill budget (§5.6): name → recent kill timestamps (ms).  Bounds
    %% kills to `conflict_kill_budget` per name per window so a regenerating conflict
    %% escalates to an operator instead of looping.
    kill_budget = #{} :: #{term() => [integer()]},
    %% Leader-only: `true` when this leader's most recent handoff gather was
    %% incomplete (a committed member did not respond — unreachable), so it may be
    %% missing that member's bindings.  Under `reject_when_degraded` the leader then
    %% refuses to register a name it does not already hold (a potential re-issue,
    %% §5.6).  Recomputed on every handoff; default off otherwise.
    degraded = false :: boolean(),
    %% Per-registry tuning options (from dgen_registry:start_link/3), resolved through
    %% dgen_config with application-env and built-in-default fallbacks.  Scopes the
    %% replication, termination, and degraded-reject knobs to this registry.
    config = #{} :: dgen_config:config(),
    %% Nodes whose `nodedown` we saw within the last ?MESH_DOWN_COOLDOWN ms → timestamp.
    %% The proactive mesh skips these so it does not reconnect a node the instant it
    %% drops (letting the departure settle as a {member_down}); cleared on {nodeup}.
    recently_down = #{} :: #{node() => integer()}
}).

%% ---------------------------------------------------------------------------
%% Public API
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Starts the member process registered as `Name`.".
-endif.
-spec start_link(Name :: atom(), Args :: map()) -> gen_server:start_ret().
start_link(Name, Args) ->
    gen_server:start_link({local, Name}, ?MODULE, Args, []).

%% ---------------------------------------------------------------------------
%% gen_server callbacks
%% ---------------------------------------------------------------------------

init(#{name := Name, tenant := Tenant, tuid := Tuid} = Args) ->
    Config = maps:get(config, Args, #{}),
    MemberId = {node(), Name},
    %% Create the local names replica as a named, `protected` ETS table owned by this
    %% process: this member is the sole writer, callers read it lock-free from their
    %% own process (`dgen_registry:whereis_name/1`).  `read_concurrency` suits the
    %% read-mostly access pattern.  Recreated empty on every (re)start — no heir — so a
    %% crash drops the local replica and it is rebuilt from the leader's snapshot.
    NamesTab = ets:new(dgen_registry:names_table(Name), [
        named_table, set, protected, {read_concurrency, true}
    ]),
    net_kernel:monitor_nodes(true),
    %% Periodically expire the conflict-detection trail (§5.6).
    erlang:send_after(?RELEASED_TTL, self(), prune_released),
    %% Everything that needs the elector's pid (announcing presence, kicking off the
    %% first mesh pass) is deferred to `discover_elector` below: the supervisor is
    %% still synchronously waiting on *this* init/1 to return when it runs (this
    %% process is the second of its two children to start), so `supervisor:which_children/1`
    %% would deadlock if called from here directly. Returning via `{continue, ...}`
    %% lets the supervisor consider this child started, unblocking it, before the
    %% lookup runs.
    {ok,
        #state{
            member_id = MemberId,
            elector = undefined,
            tenant = Tenant,
            tuid = Tuid,
            leader = undefined,
            names_tab = NamesTab,
            members = #{},
            monitors = #{},
            name_to_ref = #{},
            ref_to_name = #{},
            forwards = #{},
            meta_forwards = #{},
            pending_acks = #{},
            join_token = make_ref(),
            peer_tokens = #{},
            pending = queue:new(),
            num_pending = 0,
            committing = undefined,
            last_version = undefined,
            applied_version = 0,
            epoch = 0,
            recently_released = #{},
            kill_budget = #{},
            degraded = false,
            config = Config,
            recently_down = #{}
        },
        {continue, discover_elector}}.

%% ---------------------------------------------------------------------------
%% handle_continue/2
%% ---------------------------------------------------------------------------

%% Finds the elector via the shared supervisor (see the moduledoc's "Process
%% identity" note in dgen_registry) — safe here because the supervisor is no
%% longer blocked on this process's own init/1 by the time this runs — then
%% performs the elector-dependent startup steps init/1 could not: announcing
%% presence and kicking off the first mesh-connect pass.
handle_continue(
    discover_elector, State = #state{member_id = {_Node, Name} = MemberId, join_token = Token}
) ->
    Elector = dgen_registry:elector_pid(Name),
    true = is_pid(Elector),
    %% Announce presence; the elector will call {elector_assume_and_distribute}
    %% on the new leader, which then sends {apply_names_snapshot} to this member.
    dgen_server:cast(Elector, {join, MemberId, Token}),
    %% Proactively connect Erlang distribution to every member node, now and
    %% periodically, so the cluster converges to a full mesh (see mesh_connect).  The
    %% application only has to make the nodes *able* to connect (cookie, networking);
    %% dgen does the connecting.
    self() ! mesh_connect,
    {noreply, State#state{elector = Elector}}.

%% ---------------------------------------------------------------------------
%% handle_call/3
%% ---------------------------------------------------------------------------

%% ---- Name registration ----------------------------------------------------

%% Leader: park the registration in the group-commit buffer and answer later.
%% The reply is deferred ({noreply}) until the batch commit completes and
%% apply_committed_plan/3 answers this caller.  The origin is {local, From} — a
%% direct registration, answered with gen_server:reply/2.  See enqueue_op/2.
handle_call(
    {register, LogicalName, Pid, Meta},
    From,
    State = #state{leader = Leader, member_id = Leader}
) ->
    case reject_new_when_degraded(LogicalName, State) of
        true -> {reply, no, State};
        false -> {noreply, enqueue_op({add, LogicalName, Pid, Meta, {local, From}}, State)}
    end;
%% Follower: forward to the leader **asynchronously** (cast, never a blocking
%% call), stashing From so the eventual {register_reply} answers this caller.  A
%% blocking call here would deadlock once the leader awaits a follower ack on the
%% same path (see the moduledoc); casts keep every member's loop free.  The
%% optimistic local row update happens when the yes reply arrives (which is also
%% what makes the forwarding follower the binding's confirmed second holder).
handle_call(
    {register, LogicalName, Pid, Meta},
    From,
    State = #state{leader = Leader, member_id = Self, forwards = Forwards}
) when Leader =/= undefined, Leader =/= Self ->
    Ref = make_ref(),
    cast_to_member(Leader, {register_req, Ref, Self, LogicalName, Pid, Meta}),
    {noreply, State#state{forwards = Forwards#{Ref => {LogicalName, Pid, Meta, From}}}};
%% No leader yet.
handle_call({register, _LogicalName, _Pid, _Meta}, _From, State) ->
    {reply, no, State};
%% ---- Metadata write (set_metadata) ----------------------------------------

%% Leader: park the metadata replacement in the group-commit buffer; the origin
%% {meta, From} gets `ok` (or {error, not_registered}) once the batch commits.
handle_call(
    {set_metadata, LogicalName, Index, Data},
    From,
    State = #state{leader = Leader, member_id = Leader}
) ->
    {noreply, enqueue_op({set_meta, LogicalName, Index, Data, {meta, From}}, State)};
%% Follower: forward to the leader asynchronously, stashing From under a Ref.  The
%% leader answers via a {set_meta_reply} cast routed back through this member, so the
%% leader's {metadata_set} broadcast (FIFO, ahead of the reply) has updated our row
%% before we answer the caller — read-after-write on this node's snapshot read.
handle_call(
    {set_metadata, LogicalName, Index, Data},
    From,
    State = #state{leader = Leader, member_id = Self, meta_forwards = MetaForwards}
) when Leader =/= undefined, Leader =/= Self ->
    Ref = make_ref(),
    cast_to_member(Leader, {set_meta_req, Ref, Self, LogicalName, Index, Data}),
    {noreply, State#state{meta_forwards = MetaForwards#{Ref => From}}};
%% No leader yet.
handle_call({set_metadata, _LogicalName, _Index, _Data}, _From, State) ->
    {reply, {error, no_leader}, State};
%% ---- Consistent metadata read (leader only) --------------------------------

handle_call(
    {get_metadata, LogicalName},
    _From,
    State = #state{leader = Leader, member_id = Leader, names_tab = Tab}
) ->
    {reply, lookup_metadata(Tab, LogicalName), State};
handle_call(
    {get_metadata, LogicalName},
    From,
    State = #state{leader = Leader}
) when Leader =/= undefined ->
    %% Forward to the leader, which answers `From` directly from its authoritative row
    %% (same non-blocking forward as {whereis}).  Unreachable leader → answer now.
    {Node, _Name} = Leader,
    case Node =:= node() orelse lists:member(Node, nodes()) of
        true ->
            cast_to_member(Leader, {get_metadata_req, LogicalName, From}),
            {noreply, State};
        false ->
            {reply, undefined, State}
    end;
handle_call({get_metadata, _LogicalName}, _From, State) ->
    {reply, undefined, State};
%% ---- Indexed query (runs on the member's mailbox for batch-consistency, §6) -

%% Snapshot query: run against this member's own index + table.  Running it in the
%% handler (on the mailbox) means it observes the local replica at a batch boundary,
%% never a half-applied group commit — a batch-consistent snapshot.
handle_call(
    {query, Constraints}, _From, State = #state{names_tab = Tab, inv_index = Inv}
) ->
    {reply, run_query(Constraints, Tab, Inv), State};
%% Consistent query: must run on the leader.  If we are the leader, run it here;
%% otherwise forward (non-blocking) and let the leader answer the caller directly.
handle_call(
    {query_consistent, Constraints},
    _From,
    State = #state{leader = Leader, member_id = Leader, names_tab = Tab, inv_index = Inv}
) ->
    {reply, run_query(Constraints, Tab, Inv), State};
handle_call(
    {query_consistent, Constraints}, From, State = #state{leader = Leader}
) when Leader =/= undefined ->
    {Node, _Name} = Leader,
    case Node =:= node() orelse lists:member(Node, nodes()) of
        true ->
            cast_to_member(Leader, {query_req, Constraints, From}),
            {noreply, State};
        false ->
            {reply, [], State}
    end;
handle_call({query_consistent, _Constraints}, _From, State) ->
    {reply, [], State};
%% ---- Consistent read (leader only) ----------------------------------------

handle_call(
    {whereis, LogicalName},
    _From,
    State = #state{leader = Leader, member_id = Leader}
) ->
    {reply, lookup_name(State#state.names_tab, LogicalName), State};
handle_call(
    {whereis, LogicalName},
    From,
    State = #state{leader = Leader}
) when Leader =/= undefined ->
    %% Forward the caller's `From` to the leader, which replies to it directly — the
    %% follower never blocks its loop on a `gen_server:call` to the leader, so a
    %% consistent read can no longer head-of-line block the follower's other messages.
    %% If the leader's node is unreachable, answer now rather than hang the caller.
    {Node, _Name} = Leader,
    case Node =:= node() orelse lists:member(Node, nodes()) of
        true ->
            cast_to_member(Leader, {whereis_req, LogicalName, From}),
            {noreply, State};
        false ->
            {reply, undefined, State}
    end;
handle_call({whereis, _LogicalName}, _From, State) ->
    {reply, undefined, State};
%% ---- Snapshot read ---------------------------------------------------------
%% Served lock-free in the calling process via `ets:lookup` on the member's
%% `protected` names table (`dgen_registry:whereis_name/1`); no handler here.

%% ---- Handoff gather (any member) -------------------------------------------

%% Return our current records map (`#{Name => {Pid, Index, Data}}`) and the version we
%% have applied up to, so a newly-assuming leader can reconstruct the bindings *and
%% their metadata* from the freshest of every reachable member (the cross-member
%% gather, §5.7 / §4.4).
handle_call(
    get_names_snapshot, _From, State = #state{names_tab = Tab, applied_version = Version}
) ->
    {reply, {current_records(Tab), Version}, State};
%% ---- Elector calls (during lock period) ------------------------------------

%% Called by the elector to atomically assume leadership and fan out the
%% names snapshot to all followers.
%%
%% The bindings are reconstructed entirely from the freshest of every reachable
%% member's names map (`gather_maps/3`) — there is no durable name state to read
%% (the DB holds only a fence/version key, §4.4). The old leader, if alive, is one of
%% those members and relinquishes when it later receives `{apply_names_snapshot}`;
%% the handoff window is fenced (its commits conflict on the leader key), so no
%% synchronous relinquish-and-transfer step is needed.
%%
%% `MemberId` is the newly joining member that triggered this transition, or
%% `undefined` for a `member_down` event.
%%
%% `AllIds` is the full current member list.
%%
%% After updating own state the leader sends `{apply_names_snapshot}` casts
%% to every follower from its own process (maintaining FIFO ordering with
%% subsequent `{name_registered}` broadcasts).
handle_call(
    {elector_assume_and_distribute, MemberId, AllIds, Tokens, Epoch},
    _From,
    State = #state{member_id = Self}
) ->
    %% Reconstruct the pid bindings from the freshest of every reachable member's
    %% names map (our own replica + an RPC to each peer).  The freshest map *is* the
    %% reconstructed state — its highest-version owner holds the freshest binding for
    %% every name (broadcasts are totally ordered + FIFO).  A binding that reached no
    %% surviving member (a crash gap on the lost holder) is simply absent and may be
    %% re-issued: single-fault uniqueness, backstopped by termination (§5.6), not by a
    %% durable taken-set.  MaxVersion (the freshest applied version) becomes our
    %% applied_version and re-baselines the followers.
    Tab = State#state.names_tab,
    SelfRecords = current_records(Tab),
    OtherIds = lists:delete(Self, AllIds),
    {FreshestRecords, MaxVersion, PeerRecordMaps} = gather_maps(
        SelfRecords, State#state.applied_version, OtherIds
    ),
    %% The gather is *incomplete* if a committed member did not respond (unreachable):
    %% we may be missing its bindings, so the leader is `degraded` (§5.6).  Under
    %% reject_when_degraded it then refuses new names that could be that member's.
    Degraded = length(PeerRecordMaps) =/= length(OtherIds),
    %% Resolve genuine uniqueness conflicts the gather exposes (§5.6): two different
    %% live pids for one name, which a partition can produce now that there is no
    %% durable taken-set.  Conflict detection is about *pid* uniqueness, so it runs over
    %% pid-only views derived from the gathered records.  Kill-both + alarm + bounded
    %% budget; conflicted names are dropped (the fan-out below propagates the drop).  In
    %% the common conflict-free handoff this is a no-op.
    FreshestNames = record_pids(FreshestRecords),
    {CleanNames, State0} = resolve_conflicts(
        detect_conflicts(
            FreshestNames,
            [record_pids(SelfRecords) | [record_pids(M) || M <- PeerRecordMaps]],
            State#state.recently_released
        ),
        FreshestNames,
        State
    ),
    %% Carry metadata on the surviving bindings: resolve_conflicts only ever *drops*
    %% names, so the reconstructed records are the freshest records restricted to the
    %% names that survived.
    Records = maps:with(maps:keys(CleanNames), FreshestRecords),
    %% Reconstruct the local replica (pid + metadata) and the inverted index from the
    %% freshest gathered records (wholesale), then become (or remain) leader, monitoring
    %% the reconstructed names (read back from the table by assume_leadership).
    State0a = records_replace(State0, Records),
    State1 = relinquish_leadership(State0a),
    State2 = assume_leadership(State1#state{
        leader = Self,
        epoch = Epoch,
        applied_version = MaxVersion,
        degraded = Degraded
    }),
    State3 = add_member_monitors(extra_member_ids(MemberId, AllIds, Self), State2),
    State4 = merge_peer_tokens(Tokens, State3),
    RecordsOut = maps:to_list(Records),
    lists:foreach(
        fun(Id) ->
            cast_to_member(
                Id,
                {apply_names_snapshot, RecordsOut, Self, extra_member_ids(MemberId, AllIds, Id),
                    Tokens, Epoch, MaxVersion}
            )
        end,
        lists:delete(Self, AllIds)
    ),
    %% We are the leader now: any registrations we forwarded as a follower will
    %% never be answered by the old leader — reject them so callers retry against
    %% us.  Then commit anything buffered while we were assuming leadership.
    {reply, ok, maybe_start_commit(reject_forwards(State4))};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

%% ---------------------------------------------------------------------------
%% handle_cast/2
%% ---------------------------------------------------------------------------

%% Leader: a registration forwarded by a follower.  Park it as an add whose origin
%% is the forwarding member, answered via {register_reply} once committed.
handle_cast(
    {register_req, Ref, FollowerId, LogicalName, Pid, Meta},
    State = #state{leader = Leader, member_id = Leader}
) ->
    case reject_new_when_degraded(LogicalName, State) of
        true ->
            cast_to_member(FollowerId, {register_reply, Ref, no}),
            {noreply, State};
        false ->
            {noreply, enqueue_op({add, LogicalName, Pid, Meta, {forward, FollowerId, Ref}}, State)}
    end;
%% Not the leader (race: the follower's leader belief was stale) — reject so the
%% follower's caller retries against the current leader.
handle_cast({register_req, Ref, FollowerId, _LogicalName, _Pid, _Meta}, State) ->
    cast_to_member(FollowerId, {register_reply, Ref, no}),
    {noreply, State};
%% Leader: a set_metadata forwarded by a follower.  Park it as a set_meta op whose
%% origin is the forwarding member, answered via {set_meta_reply} once committed.
handle_cast(
    {set_meta_req, Ref, FollowerId, LogicalName, Index, Data},
    State = #state{leader = Leader, member_id = Leader}
) ->
    {noreply,
        enqueue_op({set_meta, LogicalName, Index, Data, {forward_meta, FollowerId, Ref}}, State)};
%% Not the leader (stale belief) — reject so the follower's caller retries.
handle_cast({set_meta_req, Ref, FollowerId, _LogicalName, _Index, _Data}, State) ->
    cast_to_member(FollowerId, {set_meta_reply, Ref, {error, no_leader}}),
    {noreply, State};
%% Follower: the leader's answer to a forwarded set_metadata.  By now the matching
%% {metadata_set} broadcast (FIFO ahead of this) has updated our row, so answering the
%% caller here preserves read-after-write for a subsequent local get_metadata.
handle_cast({set_meta_reply, Ref, Result}, State = #state{meta_forwards = MetaForwards}) ->
    case maps:take(Ref, MetaForwards) of
        {From, MetaForwards1} ->
            gen_server:reply(From, Result),
            {noreply, State#state{meta_forwards = MetaForwards1}};
        error ->
            %% Unknown Ref — already answered (e.g. rejected on a leadership change).
            {noreply, State}
    end;
%% Leader: a consistent metadata read forwarded by a follower.  Reply to the original
%% caller's `From` directly from the authoritative row.
handle_cast(
    {get_metadata_req, LogicalName, From},
    State = #state{leader = Leader, member_id = Leader, names_tab = Tab}
) ->
    gen_server:reply(From, lookup_metadata(Tab, LogicalName)),
    {noreply, State};
handle_cast({get_metadata_req, _LogicalName, From}, State) ->
    gen_server:reply(From, undefined),
    {noreply, State};
%% Leader: a consistent query forwarded by a follower.  Run it on our mailbox (between
%% batches → batch-consistent) and answer the original caller's `From` directly.
handle_cast(
    {query_req, Constraints, From},
    State = #state{leader = Leader, member_id = Leader, names_tab = Tab, inv_index = Inv}
) ->
    gen_server:reply(From, run_query(Constraints, Tab, Inv)),
    {noreply, State};
handle_cast({query_req, _Constraints, From}, State) ->
    gen_server:reply(From, []),
    {noreply, State};
%% Leader: a consistent read forwarded by a follower (whereis_name_consistent).  Reply
%% to the original caller's `From` directly from the authoritative map.
handle_cast(
    {whereis_req, LogicalName, From},
    State = #state{leader = Leader, member_id = Leader, names_tab = Tab}
) ->
    gen_server:reply(From, lookup_name(Tab, LogicalName)),
    {noreply, State};
%% Not the leader (stale belief, raced a leadership change) — answer `undefined`; the
%% caller can retry and reach the current leader.
handle_cast({whereis_req, _LogicalName, From}, State) ->
    gen_server:reply(From, undefined),
    {noreply, State};
%% Follower: the leader's answer to a forwarded registration.  Apply the optimistic
%% names update on yes (making this member the binding's confirmed second holder),
%% then answer the original caller.
handle_cast({register_reply, Ref, Result}, State = #state{forwards = Forwards}) ->
    case maps:take(Ref, Forwards) of
        {{LogicalName, Pid, {Index, Data}, From}, Forwards1} ->
            State1 =
                case Result of
                    yes -> row_insert(State, LogicalName, Pid, Index, Data);
                    no -> State
                end,
            gen_server:reply(From, Result),
            {noreply, State1#state{forwards = Forwards1}};
        error ->
            %% Unknown Ref — already answered (e.g. rejected on a leadership change).
            {noreply, State}
    end;
%% Follower: the leader asks us to confirm we hold a batch's bindings.  This cast
%% arrives (FIFO) after the batch's {name_registered} casts, so by now we have
%% applied them and hold the bindings — ack back to the leader.
handle_cast({replicate_sync, BatchRef, LeaderId}, State = #state{member_id = Self}) ->
    cast_to_member(LeaderId, {replicate_ack, BatchRef, Self}),
    {noreply, State};
%% Leader: a follower confirms it holds a batch's bindings.  Count *distinct*
%% followers (a duplicate ack from the same one does not count); once
%% `register_replicas` of them have confirmed, the two-holder (or n-holder) invariant
%% is met for that batch's direct registrations — answer them `yes`.
handle_cast({replicate_ack, BatchRef, FollowerId}, State = #state{pending_acks = PA}) ->
    case maps:get(BatchRef, PA, undefined) of
        {Direct, TimerRef, Needed, Acked} ->
            Acked1 = Acked#{FollowerId => true},
            case map_size(Acked1) >= Needed of
                true ->
                    erlang:cancel_timer(TimerRef),
                    deliver_direct(Direct),
                    {noreply, State#state{pending_acks = maps:remove(BatchRef, PA)}};
                false ->
                    {noreply, State#state{
                        pending_acks = PA#{BatchRef => {Direct, TimerRef, Needed, Acked1}}
                    }}
            end;
        undefined ->
            %% Already answered (enough acks or the timeout) — ignore.
            {noreply, State}
    end;
handle_cast(
    {name_registered, LogicalName, Pid, Index, Data, Epoch, Version},
    State = #state{epoch = CurrentEpoch}
) ->
    case Epoch >= CurrentEpoch of
        true ->
            {noreply, bump_applied(Version, row_insert(State, LogicalName, Pid, Index, Data))};
        false ->
            {noreply, State}
    end;
handle_cast(
    {name_unregistered, LogicalName, Epoch, Version},
    State = #state{epoch = CurrentEpoch}
) ->
    case Epoch >= CurrentEpoch of
        true ->
            {noreply, bump_applied(Version, row_delete(State, LogicalName))};
        false ->
            {noreply, State}
    end;
%% Follower: the leader replaced a registration's metadata.  Update the row's Index
%% and Data, keeping its pid (a no-op if the row is absent — e.g. this member missed
%% the registration; the next handoff reconciles it).  Epoch/version stamped like
%% {name_registered}, so a stale leader's broadcast is dropped.
handle_cast(
    {metadata_set, LogicalName, Index, Data, Epoch, Version},
    State = #state{epoch = CurrentEpoch}
) ->
    case Epoch >= CurrentEpoch of
        true ->
            {noreply, bump_applied(Version, row_update_meta(State, LogicalName, Index, Data))};
        false ->
            {noreply, State}
    end;
%% Leadership transition snapshot sent by the new leader to all followers.
%% Applies the leader transition, the record update (pid + metadata per name), and
%% extra member monitors atomically within a single cast — no other message can
%% interleave.  The snapshot carries the leader's applied_version, which re-baselines
%% this member.  `RecordsList` is `[{Name, {Pid, Index, Data}}]`.
handle_cast(
    {apply_names_snapshot, RecordsList, NewLeader, ExtraMembers, Tokens, Epoch, Version},
    State = #state{member_id = Self, leader = OldLeader, epoch = CurrentEpoch}
) ->
    case Epoch >= CurrentEpoch of
        true ->
            %% Overwrite the local replica and inverted index with the leader's snapshot
            %% before the leader transition: do_leader_changed -> assume_leadership (if we
            %% are becoming leader) reads the names back from the table to set up monitors.
            State0 = records_replace(State, maps:from_list(RecordsList)),
            State1 = do_leader_changed(NewLeader, OldLeader, Self, State0),
            State2 = State1#state{epoch = Epoch, applied_version = Version},
            State3 = add_member_monitors(ExtraMembers, State2),
            State4 = merge_peer_tokens(Tokens, State3),
            %% Handle any buffered ops under the new leadership view: reject them
            %% if we are now a follower, or commit them if we remain leader.
            {noreply, maybe_start_commit(State4)};
        false ->
            {noreply, State}
    end;
%% Leader: optimistically remove from the local replica (ETS) so that whereis_name
%% returns undefined immediately (a caller-side lookup sees the delete at once), then
%% park the durable removal in the group-commit buffer.
handle_cast(
    {unregister, LogicalName},
    State = #state{leader = Leader, member_id = Leader, names_tab = Tab}
) ->
    %% Capture the bound pid (if any) *before* the optimistic removal: the plan seeds
    %% its working map from the (now mutated) table, so without this plan_op/3 could
    %% not tell a genuine unregister from a no-op and would skip the durable clear +
    %% broadcast.  A bound pid is also recorded in `recently_released` on commit (the
    %% conflict-detector trail, §5.6).
    ReleasedPid = lookup_name(Tab, LogicalName),
    {noreply, enqueue_op({remove, LogicalName, ReleasedPid}, row_delete(State, LogicalName))};
%% Follower: delete from the local replica immediately, then forward to leader.
%% The immediate table delete ensures a caller-side `whereis_name/1` on this node
%% returns undefined before the replication cast arrives from the leader.
handle_cast({unregister, LogicalName}, State = #state{leader = Leader}) when
    Leader =/= undefined
->
    cast_to_member(Leader, {unregister, LogicalName}),
    {noreply, row_delete(State, LogicalName)};
handle_cast(_, State) ->
    {noreply, State}.

%% ---------------------------------------------------------------------------
%% handle_info/2
%% ---------------------------------------------------------------------------

%% Not enough followers confirmed a replica for these direct registrations within the
%% timeout.  The policy is `strict_replication` (§8):
%%   - false (default): **degrade open** — ack `yes` leader-only (the binding is
%%     applied and the async broadcast keeps propagating) and emit telemetry. The
%%     residual (a 1-holder reg whose holder dies) is a multi-fault case deferred to
%%     §5.6 termination.
%%   - true: **fail closed** — reject (`no`) and retract the just-applied binding via
%%     an unregister, so the caller never sees a `yes` that was not replicated.
handle_info({replicate_timeout, BatchRef}, State = #state{pending_acks = PA, config = Config}) ->
    case maps:take(BatchRef, PA) of
        {{Direct, _TimerRef, Needed, Acked}, PA1} ->
            State1 = State#state{pending_acks = PA1},
            case dgen_config:strict_replication(Config) of
                false ->
                    notify_degrade_open(Needed, map_size(Acked), length(Direct)),
                    deliver_direct(Direct),
                    {noreply, State1};
                true ->
                    {noreply, reject_and_retract_direct(Direct, State1)}
            end;
        error ->
            %% Already answered by an ack — ignore.
            {noreply, State}
    end;
%% Result of an in-flight group commit (from the dgen_transaction worker).
handle_info(
    {dgen_transaction, Ref, Reply},
    State = #state{committing = #{ref := Ref, mref := MRef, plan := Plan}}
) ->
    %% Drop the worker monitor (and any DOWN already queued behind this result —
    %% signal ordering guarantees the result arrives before the DOWN).
    erlang:demonitor(MRef, [flush]),
    State1 =
        case Reply of
            {committed, Version} ->
                apply_committed_plan(Plan, Version, State);
            _Fenced_or_error ->
                %% {aborted, fenced} (lost leadership) or {error, _} (backend
                %% failure / retry exhausted): the batch did not commit.  Reject
                %% every registration in it; callers retry.
                reject_plan(Plan, State)
        end,
    %% Start the next batch if more ops have accumulated.
    {noreply, maybe_start_commit(State1#state{committing = undefined})};
handle_info({dgen_transaction, _StaleRef, _Reply}, State) ->
    %% Result for a commit we are no longer tracking — ignore.
    {noreply, State};
%% The commit worker died without delivering a result (e.g. it was killed).  Fail
%% the batch so its registrations are answered and the commit lock is released.
handle_info(
    {'DOWN', MRef, process, _Pid, _Reason},
    State = #state{committing = #{mref := MRef, plan := Plan}}
) ->
    State1 = reject_plan(Plan, State),
    {noreply, maybe_start_commit(State1#state{committing = undefined})};
handle_info({'DOWN', Ref, process, _Pid, _Reason}, State) ->
    #state{
        monitors = Monitors,
        ref_to_name = RefToName,
        elector = Elector,
        member_id = Self
    } = State,
    case maps:get(Ref, Monitors, undefined) of
        undefined ->
            %% Not a peer-member monitor — registered-process monitor (leader only).
            case maps:get(Ref, RefToName, undefined) of
                undefined ->
                    {noreply, State};
                LogicalName ->
                    %% Park the auto-unregister in the group-commit buffer, carrying
                    %% Ref so the flush can apply the ref-match guard (a stale DOWN
                    %% for a name already re-registered must not evict the new
                    %% binding).  If we have since lost leadership the flush drops it
                    %% and the new leader, which monitors this pid itself, drives the
                    %% removal.
                    {noreply, enqueue_op({down, LogicalName, Ref}, State)}
            end;
        Self ->
            %% Stale self-monitor — should not happen, ignore.
            {noreply, State};
        DeadMemberId ->
            %% Include the token we last received for this peer so the elector can
            %% reject the message if the peer has already rejoined with a new token.
            Token = maps:get(DeadMemberId, State#state.peer_tokens, undefined),
            dgen_server:cast(Elector, {member_down, DeadMemberId, Token}),
            {noreply, remove_member(DeadMemberId, State)}
    end;
handle_info({nodeup, Node}, State = #state{elector = Elector, member_id = Self}) ->
    %% Re-announce to the elector — this member may have been removed from the
    %% member set while the node was unreachable (partition).  A fresh token is
    %% generated so any stale {member_down, Self, OldToken} already in the queue
    %% is discarded by the elector when it is eventually processed.
    NewToken = make_ref(),
    dgen_server:cast(Elector, {join, Self, NewToken}),
    %% The node is reachable again — clear any mesh suppression for it and re-run the
    %% mesh pass so we also reconnect to any other members we may have lost touch with.
    RD = maps:remove(Node, State#state.recently_down),
    self() ! mesh_connect,
    {noreply, State#state{join_token = NewToken, recently_down = RD}};
%% A node dropped: remember it briefly so the mesh does not immediately reconnect it
%% (let the departure settle as a {member_down}).  Departure itself is driven by the
%% per-member erlang:monitor DOWN, not here.
handle_info({nodedown, Node}, State = #state{recently_down = RD}) ->
    {noreply, State#state{recently_down = RD#{Node => erlang:system_time(millisecond)}}};
%% Converge Erlang distribution to a full mesh: read the authoritative member set
%% from the elector (a DB-backed read, so it is the same on every node regardless of
%% which one consumed the joins) and connect to every member node not currently
%% suppressed (recently down).  Done off this process in a short-lived helper so the
%% connect handshakes never block the member's loop.  A connect that succeeds fires
%% `nodeup`, which drives the rejoin + snapshot so a freshly-meshed node is brought
%% fully up to date.
handle_info(mesh_connect, State = #state{elector = Elector, member_id = {SelfNode, _}}) ->
    erlang:send_after(?MESH_INTERVAL, self(), mesh_connect),
    Now = erlang:system_time(millisecond),
    RD = maps:filter(
        fun(_Node, Ts) -> Ts >= Now - ?MESH_DOWN_COOLDOWN end, State#state.recently_down
    ),
    spawn_mesh_connect(Elector, SelfNode, maps:keys(RD)),
    {noreply, State#state{recently_down = RD}};
%% Expire stale entries from the conflict-detection trail (§5.6) and re-arm.
handle_info(prune_released, State = #state{recently_released = Rel}) ->
    erlang:send_after(?RELEASED_TTL, self(), prune_released),
    Cutoff = erlang:system_time(millisecond) - ?RELEASED_TTL,
    Rel1 = maps:filter(fun(_Pid, Ts) -> Ts >= Cutoff end, Rel),
    {noreply, State#state{recently_released = Rel1}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    net_kernel:monitor_nodes(false),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

do_leader_changed(NewLeader, OldLeader, Self, State0) ->
    %% On any leadership change, reject registrations forwarded to the old leader —
    %% its {register_reply} will never arrive; callers retry against the new leader.
    State =
        case NewLeader =/= OldLeader of
            true -> reject_forwards(State0);
            false -> State0
        end,
    if
        OldLeader =:= Self, NewLeader =/= Self ->
            %% Lost leadership — demonitor registered Pids, keep names for snapshot reads.
            relinquish_leadership(State#state{leader = NewLeader});
        OldLeader =/= Self, NewLeader =:= Self ->
            %% Gained leadership — set up monitors for all currently known names.
            assume_leadership(State#state{leader = NewLeader});
        true ->
            State#state{leader = NewLeader}
    end.

%% Set up process monitors for every entry in the current names map.
%% Any stale Pid entries (processes that died while this node was a follower)
%% will self-correct when their DOWN signals arrive.  This is an O(n) table scan, but
%% it runs once per leadership transition — a rare event that already does other O(n)
%% work in the same call chain (the handoff gather, records_replace, index_rebuild) — so
%% it is not the hot path the commit-plan seed is (see plan_batch/maybe_start_commit).
assume_leadership(State = #state{names_tab = Tab}) ->
    Names = record_pids(current_records(Tab)),
    {NTR, RTN} = maps:fold(
        fun(LogicalName, Pid, {NTRAcc, RTNAcc}) ->
            Ref = erlang:monitor(process, Pid),
            {NTRAcc#{LogicalName => Ref}, RTNAcc#{Ref => LogicalName}}
        end,
        {#{}, #{}},
        Names
    ),
    State#state{name_to_ref = NTR, ref_to_name = RTN}.

%% Demonitor all registered Pids; keep the names map for snapshot reads.  The
%% cached commit version is dropped too — it is this member's term and must not pin a
%% later term's read version.
relinquish_leadership(State = #state{name_to_ref = NTR}) ->
    maps:foreach(
        fun(_LogicalName, Ref) ->
            erlang:demonitor(Ref, [flush])
        end,
        NTR
    ),
    State#state{name_to_ref = #{}, ref_to_name = #{}, last_version = undefined}.

%% Gather every reachable member's names map across this (assuming) leader's own
%% replica and every other reachable member.  Because the leader's broadcasts are
%% totally ordered (commit versions are globally monotonic) and FIFO, the member with
%% the highest applied_version holds the freshest binding for *every* name, so the
%% single freshest map is the reconstruction source.  The other members' maps are
%% returned too, for the conflict detector (§5.6) — a partition can leave a member
%% holding a divergent live binding the freshest map never saw.  Returns
%% `{FreshestNames, MaxVersion, PeerMaps}` where PeerMaps is each *other* reachable
%% member's names map.
gather_maps(SelfNames, SelfVersion, OtherIds) ->
    % TODO - parallelize rpcs to get the names maps
    PeerResults = [R || MemberId <- OtherIds, {ok, R} <- [member_names(MemberId)]],
    {FreshestNames, MaxVersion} = lists:foldl(
        fun({Names, Version}, {_BestNames, BestVersion} = Best) ->
            case Version > BestVersion of
                true -> {Names, Version};
                false -> Best
            end
        end,
        {SelfNames, SelfVersion},
        PeerResults
    ),
    {FreshestNames, MaxVersion, [Names || {Names, _V} <- PeerResults]}.

%% Detect genuine uniqueness conflicts among the gathered maps (§5.6).  A conflict is
%% a name held by *two different live pids* — the authority (the freshest map's pid)
%% and some other member's pid that is (a) different, (b) not in `recently_released`
%% (so a legitimately-unregistered-but-lagging binding is suppressed — that is the
%% discriminator that makes this safe), and (c) actually alive.  Single-fault
%% uniqueness (no durable taken-set) lets a partition produce this: a follower holds a
%% binding the leader dropped on reconstruction and re-issued elsewhere.  A name the
%% freshest map lacks entirely (authority `undefined`) is *not* a conflict — there is
%% no second live claimant — so it is left to the freshest-wins reconstruction.
%% Aliveness is probed only when a candidate divergence exists, so the common
%% all-agree handoff costs no probes.  Returns `[{Name, AuthorityPid, [DivergentPid]}]`.
detect_conflicts(FreshestNames, AllMaps, Released) ->
    ByName = lists:foldl(
        fun(Map, Acc0) ->
            maps:fold(
                fun(Name, Pid, Acc) ->
                    Pids = maps:get(Name, Acc, #{}),
                    Acc#{Name => Pids#{Pid => true}}
                end,
                Acc0,
                Map
            )
        end,
        #{},
        AllMaps
    ),
    maps:fold(
        fun(Name, PidSet, Acc) ->
            Authority = maps:get(Name, FreshestNames, undefined),
            Others = [
                P
             || P <- maps:keys(PidSet), P =/= Authority, not maps:is_key(P, Released)
            ],
            case Others =:= [] orelse not (is_pid(Authority) andalso is_pid_alive(Authority)) of
                true ->
                    Acc;
                false ->
                    case [P || P <- Others, is_pid_alive(P)] of
                        [] -> Acc;
                        Divergent -> [{Name, Authority, Divergent} | Acc]
                    end
            end
        end,
        [],
        ByName
    ).

%% Resolve detected conflicts (§5.6).  Kill-both (terminate the authority pid and
%% every divergent pid), drop the name from the reconstructed map (the snapshot
%% fan-out propagates the drop, so supervised processes restart and re-register
%% cleanly under the single fenced leader), and alarm — subject to a per-name kill
%% budget and the `terminate_on_conflict` config.  Returns `{CleanNames, State}`.
resolve_conflicts([], Names, State) ->
    {Names, State};
resolve_conflicts(Conflicts, Names, State = #state{config = Config}) ->
    Terminate = dgen_config:terminate_on_conflict(Config),
    lists:foldl(
        fun({Name, Authority, Divergent}, {NAcc, S}) ->
            resolve_one_conflict(Name, Authority, Divergent, Terminate, NAcc, S)
        end,
        {Names, State},
        Conflicts
    ).

%% Detect-only mode: alarm + keep the authority binding (best-effort), no kill.
resolve_one_conflict(Name, Authority, Divergent, false, Names, State) ->
    alarm_conflict(Name, Authority, Divergent, detect_only),
    {Names, State};
resolve_one_conflict(
    Name, Authority, Divergent, true, Names, State = #state{kill_budget = KB, config = Config}
) ->
    {Count, Window} = dgen_config:conflict_kill_budget(Config),
    Now = erlang:system_time(millisecond),
    Recent = [Ts || Ts <- maps:get(Name, KB, []), Ts >= Now - Window],
    case length(Recent) < Count of
        false ->
            %% Budget exhausted — escalate to an operator and stop killing this name;
            %% keep the authority binding so the name stays served.
            alarm_conflict(Name, Authority, Divergent, budget_exhausted),
            {Names, State};
        true ->
            Kill = [Authority || is_pid_alive(Authority)] ++ Divergent,
            lists:foreach(fun(P) -> exit(P, kill) end, Kill),
            alarm_conflict(Name, Authority, Divergent, terminated),
            {maps:remove(Name, Names), State#state{kill_budget = KB#{Name => [Now | Recent]}}}
    end.

alarm_conflict(Name, Authority, Divergent, Action) ->
    logger:error(
        "dgen_registry: name conflict — name=~p authority=~p divergent=~p action=~p. "
        "Two different live pids held one name (single-fault uniqueness breach, §5.6).",
        [Name, Authority, Divergent, Action]
    ).

%% Is a (possibly remote) pid alive?  An unreachable node is treated as not-alive so a
%% conflict is never escalated on a binding we cannot verify (the next gather re-checks
%% once the node is reachable).
is_pid_alive(Pid) when node(Pid) =:= node() ->
    is_process_alive(Pid);
is_pid_alive(Pid) ->
    Node = node(Pid),
    case lists:member(Node, nodes()) of
        false ->
            false;
        true ->
            try erpc:call(Node, erlang, is_process_alive, [Pid], ?ALIVE_PROBE_TIMEOUT) of
                Alive when is_boolean(Alive) -> Alive;
                _ -> false
            catch
                _:_ -> false
            end
    end.

%% Bounded, fault-tolerant snapshot read of a peer member's names map + version.  A
%% member on a disconnected node, or one that does not answer within ?GATHER_TIMEOUT,
%% is skipped — its bindings are unavailable for this gather.  With no durable
%% taken-set, a binding only that member held is lost (single-fault uniqueness); if it
%% was replicated (two-holder), the freshest surviving holder still carries it.
member_names({Node, Name}) ->
    case lists:member(Node, nodes()) of
        false ->
            error;
        true ->
            try gen_server:call({Name, Node}, get_names_snapshot, ?GATHER_TIMEOUT) of
                {Names, Version} when is_map(Names), is_integer(Version) -> {ok, {Names, Version}}
            catch
                _:_ -> error
            end
    end.

%% §5.6 prevention: while the leader is degraded (its last handoff gather was
%% incomplete, so a committed member is unreachable) and `reject_when_degraded` is
%% enabled, refuse to register a name the leader does not already hold — it could be
%% the unreachable member's binding, and registering it would be a re-issue.  A name
%% already in the map flows to the normal dup path.  Default off (reactive posture).
reject_new_when_degraded(Name, #state{degraded = true, names_tab = Tab, config = Config}) ->
    dgen_config:reject_when_degraded(Config) andalso not name_exists(Tab, Name);
reject_new_when_degraded(_Name, _State) ->
    false.

%% Append a write op to the pending queue (FIFO), then try to start a commit.  If
%% one is already in flight, the op just accumulates and rides the next batch
%% (started when the in-flight commit completes).
enqueue_op(Op, State = #state{pending = Pending, num_pending = NumPending}) ->
    maybe_start_commit(State#state{
        pending = queue:in(Op, Pending), num_pending = NumPending + 1
    }).

%% Start a group commit for the pending buffer if appropriate.  At most one commit
%% runs at a time; ops that arrive while it is in flight accumulate and ride the
%% next batch.  Plans the batch (in arrival order), spawns a dgen_transaction
%% worker to commit it off this process's loop, and stashes the plan to apply when
%% the worker reports back.
maybe_start_commit(State = #state{committing = C}) when C =/= undefined ->
    %% A commit is in flight; this batch waits for it.
    State;
maybe_start_commit(State = #state{num_pending = 0}) ->
    State;
maybe_start_commit(State = #state{member_id = Self, leader = Leader, pending = Pending}) when
    Leader =/= Self
->
    %% Not (or no longer) the leader — reject every buffered registration and
    %% set_metadata (caller retries against the new leader); drop removes (the new
    %% leader owns those names and drives their DOWNs itself).
    lists:foreach(
        fun
            ({add, _N, _P, _Meta, Origin}) -> deliver_reply(Origin, reject_value(Origin));
            ({set_meta, _N, _I, _D, Origin}) -> deliver_reply(Origin, reject_value(Origin));
            (_) -> ok
        end,
        queue:to_list(Pending)
    ),
    State#state{pending = queue:new(), num_pending = 0};
maybe_start_commit(State) ->
    #state{
        member_id = Self,
        epoch = Epoch,
        tenant = Tenant,
        tuid = Tuid,
        names_tab = Tab,
        name_to_ref = NTR,
        pending = Pending,
        num_pending = NumPending,
        last_version = LastVersion
    } = State,
    %% Take up to ?MAX_BATCH oldest ops (front of the queue); the rest ride the
    %% following batch.  num_pending keeps the size O(1), so no length/1 scan.
    BatchSize = min(NumPending, ?MAX_BATCH),
    {ThisBatch, Rest} = take_n(Pending, BatchSize),
    State1 = State#state{pending = Rest, num_pending = NumPending - BatchSize},
    %% plan_batch resolves each op's binding lazily (its own batch-local overlay, falling
    %% back to a point ets:lookup) rather than needing a pre-seeded map of the whole
    %% registry — see plan_batch's doc.  Planning cost is O(batch), not O(registry size).
    Plan = plan_batch(ThisBatch, Tab, NTR, Epoch),
    #{dbop := DBOp, replies := RepliesRev} = Plan,
    case map_size(DBOp) =:= 0 of
        true ->
            %% Nothing durable changed — only rejected registrations.  No commit is
            %% needed (the batch neither advances the version nor needs fencing).
            %% Answer them and continue with whatever did not fit in this batch.
            send_replies(lists:reverse(RepliesRev)),
            maybe_start_commit(State1);
        false ->
            Ref = make_ref(),
            %% Pin last_version as the read version to skip the GRV (cached GRV).
            %% Safe: if it is stale, the worker's leader-key read raises too-old and
            %% dgen_transaction retries with a fresh GRV (a body-level retry); a
            %% fresh start (last_version = undefined) uses a fresh GRV.  The worker is
            %% monitored so its death without a result still resolves the commit.  The
            %% commit's only write is the fenced version bump; the name delta is
            %% applied in memory (and broadcast) when the worker reports back.
            {ok, {_Pid, MRef}} = dgen_registry_names:start_commit(
                Tenant,
                Tuid,
                Self,
                #{owner => self(), ref => Ref, read_version => LastVersion}
            ),
            State1#state{committing = #{ref => Ref, mref => MRef, plan => Plan}}
    end.

%% Apply a committed batch: bind/clear the touched names in the local replica (a
%% delta over the table — only DBOp names are touched, so a concurrent optimistic
%% unregister of an untouched name is not clobbered), (de)monitor pids, answer the
%% registration callers, replicate, and record the committed version (for the
%% read-version-pinning optimization).
apply_committed_plan(Plan, Version, State) ->
    #{
        dbop := DBOp,
        names := WNames,
        meta := WMeta,
        replies := RepliesRev,
        bcasts := BcastsRev,
        released := Released,
        direct_meta := DirectMeta
    } = Plan,
    #state{
        name_to_ref = NTR0,
        ref_to_name = RTN0,
        members = Members,
        recently_released = Rel0
    } = State,
    %% Apply the durable delta to the local replica (and the inverted index).  Only the
    %% names in DBOp are touched, so a concurrent optimistic unregister of an untouched
    %% name (already removed from the table) is never clobbered.
    State0 = apply_dbop(State, DBOp, WNames, WMeta),
    {NTR1, RTN1} = apply_monitor_ops(DBOp, WNames, NTR0, RTN0),
    %% Replicate to followers first, so the replicate_sync below (FIFO behind these)
    %% is seen by a follower only after it already holds the batch's bindings.  Each
    %% broadcast carries this batch's commit Version, which advances followers'
    %% applied_version (used to pick the freshest source on a handoff gather).
    lists:foreach(
        fun(Msg) -> broadcast_to_peers(Members, stamp_bcast(Msg, Version)) end,
        lists:reverse(BcastsRev)
    ),
    Now = erlang:system_time(millisecond),
    Rel1 = lists:foldl(fun(P, Acc) -> Acc#{P => Now} end, Rel0, Released),
    State1 = State0#state{
        name_to_ref = NTR1,
        ref_to_name = RTN1,
        last_version = Version,
        applied_version = Version,
        recently_released = Rel1
    },
    %% Answer callers.  Forwarded registrations and all rejections are answered
    %% immediately (a forwarded `yes` is already two-holder — leader + forwarding
    %% follower).  A *direct* `yes` (origin on the leader's own node) has only the
    %% leader as a holder, so it waits for a follower to confirm a replica before
    %% acking — the two-holder invariant.  See the moduledoc for the caveats.
    {DirectYes, Immediate} = lists:partition(fun is_direct_yes/1, lists:reverse(RepliesRev)),
    lists:foreach(fun({Origin, Result}) -> deliver_reply(Origin, Result) end, Immediate),
    %% Enrich each direct `yes` with its {Name, Pid} (from the plan) so the replicate
    %% path can roll the binding back under strict_replication.
    EnrichedDirect = [
        {Origin, Name, Pid}
     || {Origin, yes} <- DirectYes, {Name, Pid} <- [maps:get(Origin, DirectMeta)]
    ],
    confirm_direct(EnrichedDirect, State1).

is_direct_yes({{local, _From}, yes}) -> true;
is_direct_yes(_Other) -> false.

%% Replicate-before-ack for direct registrations (§5.5, §8).  Waits for
%% `register_replicas` *distinct* follower acks (bounded by the follower count) before
%% acking `yes`.  With no follower to replicate to (or replicas disabled) there is no
%% second holder to wait for — nothing survives the sole node anyway — so ack now.
%% Otherwise broadcast a replicate_sync and resolve on the acks or, on timeout, the
%% `strict_replication` policy (degrade-open or fail-closed).  `Direct` entries are
%% `{Origin, Name, Pid}`.
confirm_direct([], State) ->
    State;
confirm_direct(
    Direct, State = #state{members = Members, member_id = Self, pending_acks = PA, config = Config}
) ->
    Needed = min(dgen_config:register_replicas(Config), map_size(Members)),
    case Needed =< 0 of
        true ->
            deliver_direct(Direct),
            State;
        false ->
            BatchRef = make_ref(),
            broadcast_to_peers(Members, {replicate_sync, BatchRef, Self}),
            TimerRef = erlang:send_after(
                dgen_config:replicate_timeout(Config), self(), {replicate_timeout, BatchRef}
            ),
            State#state{
                pending_acks = PA#{BatchRef => {Direct, TimerRef, Needed, #{}}}
            }
    end.

deliver_direct(Direct) ->
    lists:foreach(fun({Origin, _Name, _Pid}) -> deliver_reply(Origin, yes) end, Direct).

%% strict_replication fail-closed: reject each unreplicated direct registration and
%% retract its just-applied binding (a pid-guarded clear that rides the commit
%% pipeline, so it is durably fenced and broadcast to followers).  The caller never
%% sees a `yes` that was not replicated.
reject_and_retract_direct(Direct, State) ->
    lists:foldl(
        fun({Origin, Name, Pid}, S) ->
            deliver_reply(Origin, no),
            enqueue_op({retract, Name, Pid}, S)
        end,
        State,
        Direct
    ).

%% Surface a degrade-open event (§8): a direct registration acked `yes` with fewer
%% than `register_replicas` replica acks.  Emitted as telemetry (if the optional
%% `telemetry` application is present) and logged.
notify_degrade_open(Needed, Got, Count) ->
    logger:warning(
        "dgen_registry: degrade-open — acked ~b direct registration(s) leader-only "
        "(needed ~b replica ack(s), got ~b) on replicate timeout (§8).",
        [Count, Needed, Got]
    ),
    emit_telemetry(
        [dgen_registry, register, degrade_open],
        #{count => Count},
        #{needed => Needed, got => Got}
    ).

%% Optional telemetry: emit only if the `telemetry` library is available, so it stays
%% a soft dependency (the registry does not list it in `deps`).
emit_telemetry(Event, Measurements, Metadata) ->
    case code:ensure_loaded(telemetry) of
        {module, telemetry} -> apply(telemetry, execute, [Event, Measurements, Metadata]);
        _ -> ok
    end.

%% Reject a batch that did not commit (fenced or errored): answer every op with its
%% rejection verdict (`no` for a registration, `{error, no_leader}` for a set_metadata
%% — the leader changed mid-flight, so the caller should retry).  No durable or monitor
%% state changed.
reject_plan(#{replies := RepliesRev}, State) ->
    lists:foreach(fun({Origin, _}) -> deliver_reply(Origin, reject_value(Origin)) end, RepliesRev),
    State.

reject_value({meta, _From}) -> {error, no_leader};
reject_value({forward_meta, _MemberId, _Ref}) -> {error, no_leader};
reject_value(_Origin) -> no.

%% Answer a write op's origin.  A direct (local) registration or set_metadata is
%% answered with gen_server:reply/2; a forwarded registration with a {register_reply}
%% cast, and a forwarded set_metadata with a {set_meta_reply} cast, to the forwarding
%% member, which then answers its own caller.
deliver_reply({local, From}, Result) ->
    gen_server:reply(From, Result);
deliver_reply({meta, From}, Result) ->
    gen_server:reply(From, Result);
deliver_reply({forward, MemberId, Ref}, Result) ->
    cast_to_member(MemberId, {register_reply, Ref, Result});
deliver_reply({forward_meta, MemberId, Ref}, Result) ->
    cast_to_member(MemberId, {set_meta_reply, Ref, Result}).

%% Follower: reject every forwarded registration and set_metadata still awaiting a reply
%% (the leader they were sent to is no longer current), so callers retry against the new
%% leader.  Registrations get `no`; set_metadata gets `{error, no_leader}`.
reject_forwards(State = #state{forwards = Forwards, meta_forwards = MetaForwards}) ->
    maps:foreach(
        fun(_Ref, {_Name, _Pid, _Meta, From}) -> gen_server:reply(From, no) end,
        Forwards
    ),
    maps:foreach(
        fun(_Ref, From) -> gen_server:reply(From, {error, no_leader}) end,
        MetaForwards
    ),
    State#state{forwards = #{}, meta_forwards = #{}}.

%% Dequeue exactly N ops from the front of the queue (oldest first), returning
%% {OpsInOrder, RestQueue}.  The caller guarantees N =< the queue's length
%% (tracked in num_pending), so the queue never empties early.
take_n(Queue, N) ->
    take_n(Queue, N, []).

take_n(Queue, 0, Acc) ->
    {lists:reverse(Acc), Queue};
take_n(Queue, N, Acc) ->
    {{value, Op}, Queue1} = queue:out(Queue),
    take_n(Queue1, N - 1, [Op | Acc]).

%% Fold the ordered ops into a plan: the working name overlay, the net durable delta
%% (`dbop`: Name => {set, PidNode} | {meta, PidNode} | clear, last write wins), the
%% deferred replies, and the follower broadcasts (both newest-first).  `wntr` tracks each
%% name's current monitor ref so a `down` honours the ref-match guard; a name (re)bound
%% within this batch carries a `{pending, Pid}` placeholder that never equals a real
%% ref, so a stale DOWN for it is skipped.
%%
%% `names` is seeded **empty**, not with the whole registry: every plan_op clause only
%% ever needs the binding for the one name it touches, resolved via `seed_lookup/3`,
%% which checks this batch's own overlay first and falls back to a point `ets:lookup`
%% for a name nothing earlier in the batch has touched yet.  This keeps planning cost
%% proportional to the batch, not the registry.  `tab` threads the table through for
%% that fallback.
plan_batch(Ops, Tab, NTR, Epoch) ->
    Init = #{
        tab => Tab,
        names => #{},
        meta => #{},
        wntr => NTR,
        dbop => #{},
        replies => [],
        bcasts => [],
        released => [],
        direct_meta => #{}
    },
    lists:foldl(fun(Op, Acc) -> plan_op(Op, Epoch, Acc) end, Init, Ops).

%% Resolve a name's pid "as of partway through this batch": this batch's own overlay
%% (`WN`) for a name an earlier op in the batch already touched, falling back to a point
%% `ets:lookup` for the committed pre-batch truth otherwise.  The overlay uses the atom
%% `removed` to mark a name an earlier op in *this* batch explicitly cleared — needed
%% because only `unregister`'s optimistic delete updates ETS immediately (see the
%% moduledoc's "Storage" section); a `retract` or `down` processed earlier in the same
%% batch does not touch ETS until the batch commits, so without this marker a later op
%% in the same batch would see the stale, still-bound row.
seed_lookup(WN, Tab, Name) ->
    case WN of
        #{Name := removed} -> undefined;
        #{Name := Pid} -> Pid;
        _ -> lookup_name(Tab, Name)
    end.

plan_op({add, Name, Pid, {Index, Data} = Meta, Origin}, Epoch, Acc) ->
    #{
        tab := Tab,
        names := WN,
        meta := WMeta,
        wntr := WNTR,
        dbop := DB,
        replies := Rs,
        bcasts := Bs,
        direct_meta := DM
    } = Acc,
    case seed_lookup(WN, Tab, Name) of
        undefined ->
            %% Record {Name, Pid} for a direct-origin reg so the replicate-before-ack
            %% path (confirm_direct) can roll the binding back under strict_replication.
            DM1 =
                case Origin of
                    {local, _From} -> DM#{Origin => {Name, Pid}};
                    _ -> DM
                end,
            Acc#{
                names := WN#{Name => Pid},
                meta := WMeta#{Name => Meta},
                wntr := WNTR#{Name => {pending, Pid}},
                dbop := DB#{Name => {set, node(Pid)}},
                replies := [{Origin, yes} | Rs],
                bcasts := [{name_registered, Name, Pid, Index, Data, Epoch} | Bs],
                direct_meta := DM1
            };
        _TakenPid ->
            Acc#{replies := [{Origin, no} | Rs]}
    end;
plan_op({set_meta, Name, Index, Data, Origin}, Epoch, Acc) ->
    #{tab := Tab, names := WN, meta := WMeta, dbop := DB, replies := Rs, bcasts := Bs} = Acc,
    case seed_lookup(WN, Tab, Name) of
        undefined ->
            %% No binding to attach metadata to.
            Acc#{replies := [{Origin, {error, not_registered}} | Rs]};
        Pid ->
            %% Rewrite the row's metadata.  Keep a {set,…} dbop (a registration earlier
            %% in this same batch) so the new pid is still monitored; otherwise use a
            %% {meta,…} dbop that writes the row without disturbing the existing monitor.
            DBVal =
                case maps:get(Name, DB, undefined) of
                    {set, _} = SetOp -> SetOp;
                    _ -> {meta, node(Pid)}
                end,
            Acc#{
                meta := WMeta#{Name => {Index, Data}},
                dbop := DB#{Name => DBVal},
                replies := [{Origin, ok} | Rs],
                bcasts := [{metadata_set, Name, Index, Data, Epoch} | Bs]
            }
    end;
plan_op({remove, Name, ReleasedPid}, Epoch, Acc) ->
    #{tab := Tab, names := WN, wntr := WNTR, dbop := DB, bcasts := Bs, released := Rel} = Acc,
    %% Clear durably if the name was bound at enqueue (ReleasedPid is a pid) or is bound
    %% now, as of this point in the batch (seed_lookup — overlay first, ETS fallback).
    %% ReleasedPid covers the leader's optimistic removal (already gone from ETS); the
    %% seed_lookup covers a name added earlier in this same batch (ETS would not show
    %% that yet — only unregister's own optimistic delete updates ETS immediately).
    %% Record the released pid in the conflict-detector trail so a lagging member
    %% reporting it is not later mistaken for a divergence.
    WasBound = is_pid(ReleasedPid),
    case WasBound orelse (seed_lookup(WN, Tab, Name) =/= undefined) of
        true ->
            Rel1 =
                case WasBound of
                    true -> [ReleasedPid | Rel];
                    false -> Rel
                end,
            Acc#{
                names := WN#{Name => removed},
                wntr := maps:remove(Name, WNTR),
                dbop := DB#{Name => clear},
                bcasts := [{name_unregistered, Name, Epoch} | Bs],
                released := Rel1
            };
        false ->
            Acc
    end;
plan_op({retract, Name, Pid}, Epoch, Acc) ->
    #{tab := Tab, names := WN, wntr := WNTR, dbop := DB, bcasts := Bs, released := Rel} = Acc,
    %% Pid-guarded retract (strict_replication fail-closed): clear the binding only if
    %% it is *still* this pid's, so a name re-registered to someone else since the
    %% failed reg is not clobbered.  Trail the retracted pid so a lagging follower that
    %% still holds the transient binding is not later mistaken for a conflict (§5.6).
    case seed_lookup(WN, Tab, Name) of
        Pid ->
            Acc#{
                names := WN#{Name => removed},
                wntr := maps:remove(Name, WNTR),
                dbop := DB#{Name => clear},
                bcasts := [{name_unregistered, Name, Epoch} | Bs],
                released := [Pid | Rel]
            };
        _ ->
            %% Binding changed since the failed registration — nothing to retract.
            Acc
    end;
plan_op({down, Name, Ref}, Epoch, Acc) ->
    #{names := WN, wntr := WNTR, dbop := DB, bcasts := Bs} = Acc,
    case maps:get(Name, WNTR, undefined) of
        Ref ->
            %% Ref still matches the live binding — honour the auto-unregister.
            Acc#{
                names := WN#{Name => removed},
                wntr := maps:remove(Name, WNTR),
                dbop := DB#{Name => clear},
                bcasts := [{name_unregistered, Name, Epoch} | Bs]
            };
        _ ->
            %% Stale DOWN (name re-registered, or already removed) — ignore.
            Acc
    end.

%% Apply the durable delta's monitor side effects: for a clear, demonitor the name's
%% prior ref; for a set (new/changed binding), demonitor the old ref and monitor the
%% new pid; for a meta (metadata-only update), leave the existing monitor untouched (the
%% pid is unchanged).  Returns the updated name_to_ref / ref_to_name maps.
apply_monitor_ops(DBOp, WNames, NTR, RTN) ->
    maps:fold(
        fun
            (_Name, {meta, _PidNode}, Acc) ->
                Acc;
            (Name, clear, {NTRacc, RTNacc}) ->
                demonitor_name(Name, NTRacc, RTNacc);
            (Name, {set, _PidNode}, {NTRacc, RTNacc}) ->
                {NTR1, RTN1} = demonitor_name(Name, NTRacc, RTNacc),
                Pid = maps:get(Name, WNames),
                Ref = erlang:monitor(process, Pid),
                {NTR1#{Name => Ref}, RTN1#{Ref => Name}}
        end,
        {NTR, RTN},
        DBOp
    ).

send_replies(Replies) ->
    lists:foreach(fun({Origin, Reply}) -> deliver_reply(Origin, Reply) end, Replies).

%% Stamp a plan's broadcast message with the batch's commit version, which followers
%% use to advance their applied_version.
stamp_bcast({name_registered, Name, Pid, Index, Data, Epoch}, Version) ->
    {name_registered, Name, Pid, Index, Data, Epoch, Version};
stamp_bcast({metadata_set, Name, Index, Data, Epoch}, Version) ->
    {metadata_set, Name, Index, Data, Epoch, Version};
stamp_bcast({name_unregistered, Name, Epoch}, Version) ->
    {name_unregistered, Name, Epoch, Version}.

%% Advance applied_version to the highest version applied (broadcasts can only carry
%% non-decreasing versions within an epoch, but max/2 is robust to any reordering).
bump_applied(Version, State = #state{applied_version = Applied}) ->
    State#state{applied_version = max(Applied, Version)}.

%% ---------------------------------------------------------------------------
%% Local names replica (ETS) — the member is this table's sole writer.
%% ---------------------------------------------------------------------------

%% Rows are `{Name, Pid, Index, Data}`: Pid is the bound process, Index the queryable
%% metadata map, Data the opaque payload.  A plain registration uses `#{}` / `undefined`.

%% Per-name pid read (member-side; callers read pid via ets:lookup directly).
lookup_name(Tab, Name) ->
    case ets:lookup(Tab, Name) of
        [{_Name, Pid, _Index, _Data} | _] -> Pid;
        [] -> undefined
    end.

%% Per-name metadata read for the consistent get_metadata path → the public shape
%% `{ok, #{pid, index, data}}` or `undefined` (mirrors dgen_registry:get_metadata/1).
lookup_metadata(Tab, Name) ->
    case ets:lookup(Tab, Name) of
        [{_Name, Pid, Index, Data} | _] -> {ok, #{pid => Pid, index => Index, data => Data}};
        [] -> undefined
    end.

name_exists(Tab, Name) ->
    ets:member(Tab, Name).

insert_record(Tab, Name, Pid, Index, Data) ->
    ets:insert(Tab, {Name, Pid, Index, Data}),
    ok.

%% Replace a row's metadata (Index + Data) in place, keeping its pid.  A no-op if the
%% row is absent (update_element returns false), which is what a metadata broadcast for
%% a name this member has not registered should be.
update_metadata(Tab, Name, Index, Data) ->
    ets:update_element(Tab, Name, [{3, Index}, {4, Data}]),
    ok.

delete_name(Tab, Name) ->
    ets:delete(Tab, Name),
    ok.

%% Materialise the whole replica as a transient records map `#{Name => {Pid, Index,
%% Data}}` — used by the handoff gather/fan-out, which must carry metadata.
current_records(Tab) ->
    ets:foldl(
        fun({Name, Pid, Index, Data}, Acc) -> Acc#{Name => {Pid, Index, Data}} end, #{}, Tab
    ).

%% Project a records map down to its pids for the pid-uniqueness conflict detector.
record_pids(Records) ->
    maps:map(fun(_Name, {Pid, _Index, _Data}) -> Pid end, Records).

%% Wholesale replace the replica with a records map `#{Name => {Pid, Index, Data}}` (a
%% leadership-snapshot apply / handoff reconstruction): clear the table, insert the new
%% rows, and rebuild the inverted index from them.  The member is the sole writer, so no
%% concurrent write can interleave between the clear and the insert.  Returns the state
%% with the rebuilt index.
records_replace(State = #state{names_tab = Tab}, Records) ->
    ets:delete_all_objects(Tab),
    ets:insert(
        Tab, [{Name, Pid, Index, Data} || {Name, {Pid, Index, Data}} <- maps:to_list(Records)]
    ),
    State#state{inv_index = index_rebuild(Records)}.

%% Apply a committed batch's durable delta to the replica, keeping the inverted index in
%% step.  Only names in `DBOp` are touched, so a concurrent optimistic unregister of an
%% untouched name is never clobbered (the ETS analogue of the old delta-over-the-map fold):
%%   - clear: delete the row.
%%   - set:   write the full row (a new/changed binding — pid from the plan's working name
%%            map, metadata from its working meta map).
%%   - meta:  update only the metadata of the *existing* row (an `update_element`, a no-op
%%            if the row was optimistically unregistered between plan and apply — so a
%%            metadata-only op can never resurrect a removed binding).
apply_dbop(State, DBOp, WNames, WMeta) ->
    maps:fold(
        fun
            (Name, clear, S) ->
                row_delete(S, Name);
            (Name, {set, _PidNode}, S) ->
                {Index, Data} = maps:get(Name, WMeta),
                row_insert(S, Name, maps:get(Name, WNames), Index, Data);
            (Name, {meta, _PidNode}, S) ->
                {Index, Data} = maps:get(Name, WMeta),
                row_update_meta(S, Name, Index, Data)
        end,
        State,
        DBOp
    ).

%% ---------------------------------------------------------------------------
%% Inverted index over the rows' queryable `Index` metadata (on every member; §6).
%% ---------------------------------------------------------------------------

%% State-threading row writers — each keeps the ETS row and the inverted index in step,
%% retracting the name's old index postings (read from the table before the write)
%% before applying its new ones.  Use these (not the bare insert/delete/update helpers)
%% at every site that mutates a row, so the index never drifts.
row_insert(S = #state{names_tab = Tab, inv_index = Inv}, Name, Pid, Index, Data) ->
    Old = row_index(Tab, Name),
    insert_record(Tab, Name, Pid, Index, Data),
    S#state{inv_index = index_add(index_remove(Inv, Name, Old), Name, Index)}.

row_update_meta(S = #state{names_tab = Tab, inv_index = Inv}, Name, Index, Data) ->
    Old = row_index(Tab, Name),
    update_metadata(Tab, Name, Index, Data),
    %% update_metadata is a no-op when the row is absent; do not invent index postings
    %% for a name this member does not hold.
    case name_exists(Tab, Name) of
        true -> S#state{inv_index = index_add(index_remove(Inv, Name, Old), Name, Index)};
        false -> S
    end.

row_delete(S = #state{names_tab = Tab, inv_index = Inv}, Name) ->
    Old = row_index(Tab, Name),
    delete_name(Tab, Name),
    S#state{inv_index = index_remove(Inv, Name, Old)}.

%% A name's current indexed-attribute map (`#{}` if it has no row), so a mutation can
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
                #{Attr := Vs} ->
                    case Vs of
                        #{Value := Names} ->
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
                    end;
                _ ->
                    Acc
            end
        end,
        Inv,
        Index
    ).

%% Rebuild the whole inverted index from a records map (handoff / snapshot apply).
index_rebuild(Records) ->
    maps:fold(
        fun(Name, {_Pid, Index, _Data}, Acc) -> index_add(Acc, Name, Index) end,
        #{},
        Records
    ).

%% Resolve an AND-equal query against the inverted index + table (§6).  `Constraints` is
%% `#{attr() => value()}`; the result is `[#{name, pid, index, data}]`, every registration
%% whose `Index` satisfies *all* clauses.  Empty constraints yield `[]` (the public API
%% rejects them before reaching here).  Steps: take each clause's posting set, short-
%% circuit if any is empty, intersect smallest-first, then materialise + re-check each
%% surviving name against its current row (so a stale index posting can never produce a
%% false positive).
run_query(Constraints, _Tab, _Inv) when map_size(Constraints) =:= 0 ->
    [];
run_query(Constraints, Tab, Inv) ->
    Clauses = maps:to_list(Constraints),
    Postings = [posting(Inv, Attr, Value) || {Attr, Value} <- Clauses],
    case lists:any(fun(P) -> map_size(P) =:= 0 end, Postings) of
        true ->
            %% Some clause matches nothing — the conjunction is empty.
            [];
        false ->
            [Smallest | Rest] = lists:sort(
                fun(A, B) -> map_size(A) =< map_size(B) end, Postings
            ),
            Names = [N || N <- maps:keys(Smallest), in_all(N, Rest)],
            materialize(Names, Constraints, Tab)
    end.

%% Posting set (`#{Name => []}`) for one clause, or `#{}` if the attr/value is absent.
posting(Inv, Attr, Value) ->
    case Inv of
        #{Attr := #{Value := Names}} -> Names;
        _ -> #{}
    end.

in_all(Name, Postings) ->
    lists:all(fun(P) -> maps:is_key(Name, P) end, Postings).

%% Read each candidate's row and keep it only if the row still satisfies every clause
%% (defends against any index drift), shaping it into the public `#{name, pid, index,
%% data}` match.
materialize(Names, Constraints, Tab) ->
    lists:foldl(
        fun(Name, Acc) ->
            case ets:lookup(Tab, Name) of
                [{_N, Pid, Index, Data} | _] ->
                    case satisfies(Constraints, Index) of
                        true ->
                            [#{name => Name, pid => Pid, index => Index, data => Data} | Acc];
                        false ->
                            Acc
                    end;
                [] ->
                    Acc
            end
        end,
        [],
        Names
    ).

%% Does `Index` (a row's indexed-attribute map) satisfy every clause exactly?
satisfies(Constraints, Index) ->
    maps:fold(
        fun(Attr, Value, Ok) -> Ok andalso maps:get(Attr, Index, '$absent') =:= Value end,
        true,
        Constraints
    ).

add_member_monitors(MemberIds, State) ->
    #state{member_id = Self, members = Members, monitors = Monitors} = State,
    {NewMembers, NewMonitors} = lists:foldl(
        fun(MemberId, {MA, MonA}) ->
            case MemberId =:= Self orelse maps:is_key(MemberId, MA) of
                true ->
                    {MA, MonA};
                false ->
                    {Node, Name} = MemberId,
                    Ref = erlang:monitor(process, {Name, Node}),
                    {MA#{MemberId => Ref}, MonA#{Ref => MemberId}}
            end
        end,
        {Members, Monitors},
        MemberIds
    ),
    State#state{members = NewMembers, monitors = NewMonitors}.

remove_member(MemberId, State = #state{members = Members, monitors = Monitors}) ->
    case maps:get(MemberId, Members, undefined) of
        undefined ->
            State;
        Ref ->
            erlang:demonitor(Ref, [flush]),
            %% Drop the peer's token too, so `peer_tokens` does not accumulate stale
            %% entries across node churn (it is repopulated from the snapshot on
            %% rejoin).  The caller reads the token for its {member_down} cast before
            %% calling this, so pruning here is safe.
            State#state{
                members = maps:remove(MemberId, Members),
                monitors = maps:remove(Ref, Monitors),
                peer_tokens = maps:remove(MemberId, State#state.peer_tokens)
            }
    end.

demonitor_name(LogicalName, NTR, RTN) ->
    case maps:get(LogicalName, NTR, undefined) of
        undefined ->
            {NTR, RTN};
        OldRef ->
            erlang:demonitor(OldRef, [flush]),
            {maps:remove(LogicalName, NTR), maps:remove(OldRef, RTN)}
    end.

%% Returns the list of member IDs to add as monitors for a given member during
%% a join/member_down leadership transition.
%%   - undefined MemberId (member_down):    no extra monitors for anyone.
%%   - Id = MemberId (new joining member):  add all current peers.
%%   - Id ≠ MemberId (existing member):     add only the new member.
extra_member_ids(undefined, _AllIds, _Id) -> [];
extra_member_ids(MemberId, AllIds, MemberId) -> AllIds;
extra_member_ids(MemberId, _AllIds, _Id) -> [MemberId].

%% Merge incoming token map into local peer_tokens, taking the newer token when
%% both sides know about the same member (higher value = more recent make_ref
%% within a single BEAM session, but refs are opaque so we always overwrite —
%% the elector is the authoritative source and always sends current tokens).
merge_peer_tokens(Tokens, State = #state{peer_tokens = PeerTokens}) ->
    State#state{peer_tokens = maps:merge(PeerTokens, Tokens)}.

broadcast_to_peers(Members, Msg) ->
    maps:foreach(fun(MemberId, _) -> cast_to_member(MemberId, Msg) end, Members).

%% Connect Erlang distribution to every member node (except those recently down), in a
%% short-lived helper so neither the queued read nor the (potentially slow) connection
%% handshakes block the member's loop.  The member set is read from the elector via a
%% regular `call` (not `priority_call`): it rides the elector's durable queue in order
%% with the membership changes, rather than bypassing it on the urgent lane, so the
%% mesh sees a just-joined node as soon as its join is processed and does not jump ahead
%% of or add bypass load to the membership work.  The set is authoritative — the same on
%% every node — including a brand-new member that has only committed its join to the
%% database and is not yet reachable any other way.  `connect_node/1` is idempotent (an
%% already-connected node is a no-op) and a node that cannot be reached simply returns
%% `false`; either way the next pass retries.
spawn_mesh_connect(Elector, SelfNode, Suppressed) ->
    _ = spawn(fun() ->
        try dgen_server:call(Elector, get_members) of
            Members when is_list(Members) ->
                lists:foreach(
                    fun
                        ({Node, _Name}) when Node =/= SelfNode ->
                            case lists:member(Node, Suppressed) of
                                true -> ok;
                                false -> net_kernel:connect_node(Node)
                            end;
                        (_) ->
                            ok
                    end,
                    Members
                );
            _ ->
                ok
        catch
            _:_ -> ok
        end
    end),
    ok.

%% Cast to a peer member.  A fire-and-forget cast to `{Name, Node}` on a
%% currently-disconnected node would trigger an automatic distribution reconnect
%% (the same hazard `dgen_registry_elector:call_to_member/2` guards against): if
%% it fires during a partition, both sides see `{nodeup, _}`, re-join with fresh
%% tokens, and the partition is healed before either side ever removed the other
%% — so the departure is never observed.  Drop the cast when the target node is
%% not connected; the peer re-syncs via `{apply_names_snapshot}` when it rejoins
%% on the next `{nodeup, _}`.
cast_to_member({Node, Name}, Msg) when Node =:= node() ->
    gen_server:cast({Name, Node}, Msg);
cast_to_member({Node, Name}, Msg) ->
    case lists:member(Node, nodes()) of
        true -> gen_server:cast({Name, Node}, Msg);
        false -> ok
    end.
