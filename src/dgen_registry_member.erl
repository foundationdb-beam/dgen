-module(dgen_registry_member).
-behaviour(gen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("../include/dgen_eta.hrl").
-eta_observe(
    {state, [
        member_id,
        leader,
        epoch,
        synced,
        applied_version,
        pending_forwards,
        pending_unregs,
        deferred_yes,
        committing,
        num_pending
    ]}
).

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
%% Keeps its local names replica (ETS) in sync by receiving `{names_batch, …}` and
%% `{apply_names_snapshot, …}` casts from the leader (one-way replication).  A
%% `{names_batch, Ops, …}` carries one committed group commit whole — its
%% registrations, unregisters and metadata changes together — so a batch is applied
%% in full or not at all; see broadcast_batch/5 for why that atomicity is required
%% rather than merely tidy.  All follower messages come from the leader process, so
%% Erlang's per-pair FIFO guarantee ensures followers always see a snapshot before
%% any batch broadcast that post-dates it.
%%
%% ## Membership and connectivity
%%
%% Keeping the Erlang-distribution mesh in step with registry membership — the
%% proactive `net_kernel:connect_node/1` pass, the backstop reap of stranded members,
%% and the leader-liveness probe — lives in the sibling `dgen_registry_connector`, not
%% here.  So once a node's join is committed the connector connects to it and `nodes()`
%% converges to every reachable member; the application only has to make the nodes
%% *able* to connect (shared cookie, reachable hostnames), not wire up the mesh itself.
%%
%% The member still subscribes to `nodeup`/`nodedown` via `net_kernel:monitor_nodes/1`.
%% On `{nodeup, Node}` it does two things: it re-announces itself to the elector
%% (`{join, Self}`), and it promptly re-drives any unregisters it stashed while the
%% leader was unreachable (`redrive_pending_unregs`, Non-goal 5).  The re-announce
%% handles an Erlang-level partition that caused both sides to remove each other from the
%% member set via `{member_down}` while the DB stayed healthy: once distribution
%% reconnects, both sides re-join and the elector reconstitutes the cluster without a
%% restart.  The stashed-unregister re-drive means the removal reaches the (now-reachable)
%% leader immediately, rather than waiting for the re-onboarding snapshot.  (The connector subscribes to node
%% events independently for its mesh bookkeeping.)  The connector also nudges this
%% member with `{durable_epoch, E}` when the committed election epoch has moved ahead of
%% what the member has heard — a missed handoff — prompting a re-join.
%%
%% ## Registration blocks until a leader exists
%%
%% A registration is only ever answered `no` for an *adjudicated* refusal — the name is
%% held by a different live pid (or the opt-in `reject_when_degraded` prevention mode).
%% When there is simply **no reachable leader** (none elected yet at startup, a brief
%% no-leader window during a handoff, or the elected leader is unreachable in a
%% partition), the registration is **not** answered `no` — that would be a false
%% `already_started` to OTP's via machinery.  Instead the caller is *blocked*: the
%% request is stashed in `pending_registers` and re-driven (`redrive_registers/1`) the
%% moment a leader is established (assuming leadership, or applying a leader's snapshot),
%% so it succeeds if a leader appears within the caller's `register_timeout` and
%% otherwise the caller's own `gen_server:call` times out (§3 of the design doc — a
%% timeout exits rather than returning a verdict).  The stash is pruned by wall-clock
%% age so a prolonged no-leader window (a partition) cannot grow it without bound.  All
%% register routing is in `route_register/5`.
%%
%% Forwards `{register, …}` and `{unregister, …}` to the leader. The register
%% forward is **asynchronous**: the follower stashes the caller's `From` (with the
%% registration's metadata) under a `Ref`, casts `{register_req, Ref, Self, Name, Pid,
%% Meta}` to the leader, and replies `{noreply}` — it never blocks on the leader. When
%% the leader's `{register_reply, Ref, yes|no, Version}` arrives, the follower (on
%% `yes`) writes the registration's row (pid + metadata) into its own table and then
%% answers the caller; on `no` the table is left unchanged.  The `yes` ack is
%% **version-guarded**: it is answered only once the follower's replica has applied up
%% to the batch's commit `Version` — normally already true, since the batch's
%% `{names_batch}` broadcast precedes the reply (FIFO).  If that broadcast was
%% gap-refused (the follower missed an earlier batch and awaits a resync), the ack is
%% *deferred* until the resync lands (`deferred_yes` / `flush_deferred`), so the
%% "second holder" is always version-visible to the freshest-wins handoff gather —
%% otherwise a single leader crash could silently drop an acked binding whose only
%% surviving copy sat in a gapped replica the gather's version comparison ignores.
%%
%% An `{unregister, …}` is likewise a tracked, call-shaped forward (`unregister_req` /
%% `unregister_reply`): the follower deletes its row optimistically, stashes the
%% caller under a `Ref`, and — if the reply never comes (dropped cast, deposed
%% leader) or no leader was reachable at all — re-drives the removal as a
%% pid-guarded retract on the next snapshot/leadership event (`redrive_unregs`), so
%% an explicit unregister is never silently lost.  Explicit unregisters are rare, so
%% the extra round-trip costs nothing that matters.
%%
%% A `set_metadata` forward is also asynchronous: the follower stashes `From` under a
%% `Ref`, casts `{set_meta_req, Ref, Self, Name, Index, Data}`, and answers the caller
%% when the leader's `{set_meta_reply, Ref, _}` arrives. No optimistic update is needed —
%% routing the reply back through this member means the leader's `{names_batch, …}`
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
%% Assumed when the elector calls `{elector_assume_and_distribute, …}`.  On a genuine
%% leadership change the member reconstructs its names map by gathering the freshest of
%% every reachable member's replica — the freshest map *is* the reconstructed state
%% (there is no durable taken-set to reconcile against, §4.4), then sets up
%% `erlang:monitor/2` for every entry and distributes `{apply_names_snapshot}` casts to
%% all followers from its own process (same sender as future `{names_batch}`
%% broadcasts — see elector moduledoc for the FIFO ordering guarantee).  Any stale Pid
%% entries are removed when their DOWN signals arrive.
%%
%% The gather is a **network** fan-out (an RPC per peer, bounded by ?GATHER_TIMEOUT), so
%% it runs **off the member's loop**: the elector's assume call is answered `ok`
%% immediately, and the `{assume_gathered, …}` continuation finishes assuming when the
%% gather returns (`spawn_assume_gather` → the assume_gathered handler, correlated by
%% `assume_ref`).  This keeps the member responsive during a handoff — under churn an
%% inline multi-second gather would freeze the loop, time out the elector's call, and
%% stall every membership change queued behind it.  During the (sub-?GATHER_TIMEOUT)
%% window the member is not yet leader/synced, so writes block (no reachable leader) and
%% are re-driven by the continuation, never served against a half-built map.
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
%%   cast to the forwarding follower), pids are monitored, and the batch is replicated as one
%%   `{names_batch, …}` broadcast. A name already held by a *different* live
%%   pid is rejected (`no`); re-registering the *same* pid under the same name is an
%%   idempotent `yes` (see plan_op/3); a `DOWN` whose ref no longer matches the current
%%   binding is ignored; a fenced commit (leadership lost) rejects the whole batch.
%% - Handles `{whereis, LogicalName}` calls: consistent read from local map.
%% - Handles `{unregister, LogicalName}` calls (and legacy casts): updates the map,
%%   demonitors, replicates the batch, and answers the tracked
%%   caller `ok` once the removal has committed.
%% - Monitors every registered Pid. When one dies, removes from the map
%%   and replicates the batch to followers.
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

%% (The group-commit batch size, the replicate-before-ack target, and the timeout
%% are configurable per registry — see dgen_config:commit_batch_size/1,
%% register_replicas/1, replicate_timeout/1, strict_replication/1.)

%% Per-member bound on the handoff gather: a member that does not return its names
%% map within this is skipped (its bindings are unavailable for this gather; a
%% binding only that member held is then lost — single-fault uniqueness).
-define(GATHER_TIMEOUT, 2000).
%% How long the off-loop assume gather keeps re-gathering while its reconstruction is
%% behind the durable version key (the committed frontier), and how long it pauses
%% between attempts.  This closes the handoff-gather race proven in
%% formal/DgenRegistryReplication.tla (the SafeAssume fence): a gather can momentarily
%% race an in-flight broadcast still queued in a peer's mailbox and reconstruct a stale
%% map; the peer applies that broadcast within milliseconds, so a brief re-gather
%% catches up.  Bounded overall by ?GATHER_TIMEOUT (past it the missing version's holder
%% is genuinely unreachable — a real degrade — so we proceed with what we have).
-define(ASSUME_CATCHUP_INTERVAL, 25).
%% Upper bound on the off-loop pull of durable subscriptions from the co-located elector
%% during a handoff (§4.9).  Short and best-effort: a busy elector falls back to the
%% empty set, reconciled by a later delta / the next handoff.
-define(SUBS_PULL_TIMEOUT, 1000).

%% How often the periodic maintenance pass runs: it prunes `recently_released`
%% (the conflict-detector trail, §5.6 — TTL from `dgen_config:conflict_release_ttl/1`)
%% and the per-name `kill_budget` timestamps.  Trail pruning is *suspended* while any
%% current member is disconnected: a disconnected member misses unregister broadcasts
%% entirely (they are dropped, not delayed), so its stale rows can surface at a rejoin
%% gather arbitrarily later than any broadcast lag — the trail must outlive the
%% partition, not just the lag.  The trail itself is also replicated (unregister
%% broadcasts carry the released pid) and merged across members at a handoff gather,
%% so it survives leadership changes.
-define(PRUNE_INTERVAL, 30000).

%% How long a follower waits after requesting a resync (it observed a gap in the
%% leader's broadcast stream) before it may request again.
-define(RESYNC_RETRY, 2000).

%% How often the leader broadcasts an **empty** batch stamped at its current applied
%% version — a replication heartbeat.
%%
%% Gap detection is otherwise entirely traffic-driven: a follower learns it missed a
%% batch only when a *later* batch arrives whose PrevVersion does not match its
%% replica (`apply_bcast/6`), or when a forwarded register reply arrives ahead of it.
%% So a follower that loses the *tail* of the stream — the last batch before writes
%% stop — has nothing left to reveal the gap, and holds a stale replica for as long
%% as the cluster stays quiescent.  Losing the `resync_req` (or the snapshot
%% answering it) has the same shape: `resync_timeout` clears the once-per-window
%% guard, but nothing re-requests without new traffic.
%%
%% The heartbeat closes both.  It is an ordinary `{names_batch, [], …}` with
%% `PrevVersion = Version = the leader's applied_version`, so it needs no new
%% handling: a caught-up follower matches `PrevV =:= applied_version` and applies
%% nothing, while a follower that is behind matches neither that nor `V =< Applied`
%% and so takes the existing gap branch and resyncs.  Cost is one small cast per
%% follower per interval, independent of the number of registered names.
-define(REPLICA_HEARTBEAT_INTERVAL, 5000).

%% Delay before re-driving the destructive ops (removes/retracts/downs) of a batch
%% that failed to commit — a backend error, a fence, or a worker death.  Registrations
%% are simply rejected (callers retry), but a lost unregister/retract would leave a
%% permanent replica divergence, so those are re-enqueued (still leader) or forwarded
%% to the current leader (deposed) after this delay.
-define(REQUEUE_DELAY, 1000).

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

%% A presence subscription's watch/notify queries, in the internal tagged form (see
%% dgen_registry:query/0).  Extensible: only `{all, Constraints}` (AND-equal) exists today.
-type query() :: {'all', #{term() => term()}}.

%% An application-supplied presence subscription id (§4.9).  The durable subscriptions
%% live in the elector's dgen_server state; the leader is fed the set and computes the
%% notifications.  Keyed by this id so an application can tie a subscription to a
%% database entity and re-address it (unsubscribe, re-subscribe) by the same stable key
%% across a full cluster restart.
-type sub_id() :: term().

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
%%                              {names_batch} broadcast has updated the follower's row).
%%   - {unreg, From}:           an unregister_name call made directly on this leader;
%%                              answered `ok` with gen_server:reply/2 once committed.
%%   - {forward_unreg, MemberId, Ref}: an unregister forwarded by a follower; answered
%%                              with an {unregister_reply, Ref} cast to that member,
%%                              which then answers its own caller `ok` (after the
%%                              FIFO-ordered {names_batch} broadcast).
-type origin() ::
    {local, gen_server:from()}
    | {forward, dgen_registry_elector:member_id(), reference()}
    | {meta, gen_server:from()}
    | {forward_meta, dgen_registry_elector:member_id(), reference()}
    | {unreg, gen_server:from()}
    | {forward_unreg, dgen_registry_elector:member_id(), reference()}.

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
%%               Origin (or `undefined` for a legacy cast / salvaged re-drive) is
%%               answered `ok` once the batch commits.
%%   - down:     an auto-unregister from a monitored process exit; Ref-guarded so a
%%               stale DOWN for an already-re-registered name is ignored.  The pid is
%%               dead, so it is *not* recorded in `recently_released`.
-type batch_op() ::
    {add, LogicalName :: term(), Pid :: pid(), Meta :: meta(), Origin :: origin()}
    | {set_meta, LogicalName :: term(), Index :: map(), Data :: term(), Origin :: origin()}
    | {remove, LogicalName :: term(), ReleasedPid :: pid() | undefined,
        Origin :: origin() | undefined}
    | {retract, LogicalName :: term(), Pid :: pid()}
    | {down, LogicalName :: term(), Ref :: reference()}.

%% A follower's in-flight forwarded call, keyed in `pending_forwards` by the `Ref` the
%% leader echoes in its reply.  One map holds all three kinds so there is a single home
%% for "calls I forwarded and am awaiting a verdict on", and a single resolution when
%% leadership moves.  The kinds still differ in how they resolve — a register applies
%% (or gap-defers) the optimistic row on `yes`; a set_meta just answers; an unregister
%% is re-driven rather than rejected on a leadership change (see reject_forwards /
%% redrive_unregs):
%%   - {register, Name, Pid, Meta, From}: awaiting {register_reply}; on `yes` the row is
%%                              written (making this member the second holder) and From
%%                              answered — version-guarded, see handle_register_reply.
%%   - {set_meta, From}:        awaiting {set_meta_reply}; From gets the leader's result.
%%   - {unregister, Name, Pid, From}: awaiting {unregister_reply}; From gets `ok`.  Pid is
%%                              the released pid (or `undefined` if unbound locally) so a
%%                              re-drive can pid-guard the retract.
-type pending_forward() ::
    {register, term(), pid(), meta(), gen_server:from()}
    | {set_meta, gen_server:from()}
    | {unregister, term(), pid() | undefined, gen_server:from()}.

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
    %% Leader-only presence state (§4.9): the durable subscription set the elector feeds
    %% this member — `#{SubId => {Watch, Notify}}`.  Seeded wholesale from the
    %% `{elector_assume_and_distribute}` payload when this member assumes leadership, and
    %% advanced by `{presence_update, …}` deltas the elector pushes as subscriptions are
    %% created/removed.  A follower does not track it (only the leader fires
    %% notifications); it is reseeded from the elector on the next assume.
    subs = #{} :: #{sub_id() => {query(), query()}},
    %% Leader-only: the current watch-set membership of each subscription —
    %% `#{SubId => #{Name => Pid}}` — i.e. the names that satisfy the subscription's
    %% watch query right now.  Recomputed wholesale from the replica on assuming
    %% leadership (recompute_sub_matches) and advanced incrementally per commit
    %% (apply_committed_plan): comparing a batch's post-state matches against this
    %% pre-state is what yields the `joined`/`left` deltas.
    sub_matches = #{} :: #{sub_id() => #{term() => pid()}},
    %% Leader-only: the current *notify*-set membership of each subscription, the mirror
    %% of `sub_matches` for the notify query — `#{SubId => #{Name => Pid}}`.  Its purpose
    %% is the initial snapshot: a process that only starts matching the notify query later
    %% (registered after the subscription was created) must still learn who is already
    %% present, so when a pid **enters** this set it is sent the full current watch
    %% snapshot — symmetric to a watch member entering triggering a `joined` delta.
    %% Recomputed on assume and advanced per commit alongside sub_matches.
    notify_matches = #{} :: #{sub_id() => #{term() => pid()}},
    %% Peer-member monitors (all members)
    members :: #{dgen_registry_elector:member_id() => reference()},
    monitors :: #{reference() => dgen_registry_elector:member_id()},
    %% Registered-process monitors (leader only)
    name_to_ref :: #{term() => reference()},
    ref_to_name :: #{reference() => term()},
    %% Follower-only: calls forwarded to the leader and awaiting their reply, keyed by
    %% the Ref the leader echoes back (`register` / `set_meta` / `unregister` kinds —
    %% see the pending_forward() type).  One map for all three, so there is a single
    %% place forwarded-call state lives and a single resolution when leadership moves:
    %% register and set_meta are rejected so their callers retry (reject_forwards);
    %% unregister is instead re-driven, since its intent is idempotent and must not be
    %% lost (redrive_unregs).
    pending_forwards = #{} :: #{reference() => pending_forward()},
    %% Follower-only: forwarded registrations whose `yes` reply arrived while this
    %% member's replica was **gapped** (the batch's {names_batch} broadcast was
    %% refused pending a resync, so applied_version is behind the reply's commit
    %% version).  Acking `yes` at that point would make the "second holder"
    %% invisible to the freshest-wins handoff gather (§5.5/§5.7) — a leader crash
    %% before the resync landed could then silently drop an acked binding.  The ack
    %% is deferred as {Version, From} and released by flush_deferred/1 once
    %% applied_version reaches Version (the resync snapshot both delivers the
    %% binding and advances the version, making the second holder version-visible).
    %% Rejected (`no`) on a leadership change, like the register forwards it came from.
    deferred_yes = [] :: [{non_neg_integer(), gen_server:from()}],
    %% Unregisters accepted while no leader was reachable (answered `ok` under the
    %% CP contract — the commit must wait), kept as {Name, ReleasedPid} until
    %% redrive_unregs/1 can hand them to a leader as pid-guarded retracts.  (Distinct
    %% from an `unregister` entry in `pending_forwards`, which was successfully
    %% forwarded and is awaiting a reply; these never reached a leader at all.)
    pending_unregs = [] :: [{term(), pid()}],
    %% Registrations received while there was **no reachable leader** (none elected
    %% yet, or the elected one is unreachable).  Rather than answer a false `no` — which
    %% OTP's via machinery reads as `already_started` — the register is *blocked*: its
    %% caller is stashed here as {EnqueuedAtMs, Name, Pid, Meta, From} and re-driven
    %% (redrive_registers/1) the moment a leader is established, so it succeeds if the
    %% leader appears within the caller's `register_timeout` and otherwise the caller's
    %% own call times out (§3 / §5.2).  Pruned by wall-clock age on the periodic pass so
    %% the stash cannot grow unbounded while no leader ever appears (a partition).
    pending_registers = [] :: [{integer(), term(), pid(), meta(), gen_server:from()}],
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
    %% The highest commit version this member has applied to `names` — advanced by
    %% every name broadcast (which carries its batch's commit version) and set from
    %% the snapshot on a leadership change.  Globally monotonic (FDB commit versions),
    %% so on a handoff the new leader picks the member with the highest applied_version
    %% as the freshest source for the gather.  All members track it.
    %%
    %% Crucially, a broadcast advances it only when it is **contiguous**: each
    %% broadcast carries its batch's predecessor version, and a follower that observes
    %% a gap (it missed a batch — a dropped cast during a disconnect, a lost in-flight
    %% message) does *not* apply past the gap; it requests a resync snapshot instead.
    %% This is what makes "highest applied_version holds the freshest binding for
    %% every name" actually true: every replica is a prefix of the leader's stream,
    %% so freshest-wins reconstruction can never silently lose a binding a gathered
    %% member holds, nor resurrect one it unregistered (§5.7 / Guarantee 4).
    applied_version = 0 :: non_neg_integer(),
    %% Timer for an outstanding resync request (a gap was observed and a snapshot was
    %% requested from the broadcast's leader); `undefined` when none is outstanding.
    %% Cleared when any snapshot arrives; on expiry the next gap re-requests.
    resync_timer = undefined :: undefined | reference(),
    %% Whether this member has ever synced registry state (applied a snapshot or
    %% assumed leadership).  Joins announce `fresh = not synced`; a fresh member holds
    %% no bindings, so the leader's `degraded` computation ignores it when it is
    %% unreachable at a gather (see dgen_registry_elector's member_info doc).  On the
    %% first sync the member re-announces itself with `fresh = false`.
    synced = false :: boolean(),
    %% Correlates the in-flight asynchronous handoff gather with the continuation that
    %% finishes assuming leadership (§5.7).  When the elector asks this member to assume
    %% (a genuine leadership change), the peer-snapshot gather runs off the loop; this
    %% ref tags that gather, and the `{assume_gathered, …}` continuation applies its
    %% reconstruction *only* if this ref still matches — so a newer assume, or a snapshot
    %% that made us a follower meanwhile, harmlessly supersedes a stale gather.
    %% `undefined` when no assume is in flight.
    assume_ref = undefined :: undefined | reference(),
    %% Monotonically increasing leader term counter set by the elector.
    %% Broadcasts from a prior leader carry a smaller epoch and are discarded.
    epoch :: non_neg_integer(),
    %% Conflict-detection trail (§5.6), kept on *every* member: name/pid pairs
    %% explicitly released (an unregister of a live process) → the wall-clock ms it
    %% happened.  Keyed by `{Name, Pid}` — not bare pid — so releasing a pid under
    %% one of its names never masks a genuine conflict on *another* name the same
    %% pid also holds (a pid may be registered under several names).  Bare-pid keys
    %% from a pre-upgrade member's gathered trail are still honoured by the
    %% detector during a rolling upgrade (see released_entry/3).
    %% The leader records entries at commit; followers record them from the released
    %% pid carried on {names_batch} unregister ops; and at a handoff gather every
    %% reachable member's trail is merged into the new leader's, so the trail
    %% survives leadership changes.  A gather suppresses a divergence on an entry
    %% still in here (it was legitimately unregistered, a lagging member just has not
    %% caught up), so only a reconstruction-drop divergence — which leaves no trail —
    %% is killed.  TTL-pruned (dgen_config:conflict_release_ttl/1) by a periodic
    %% self-message, with pruning suspended while any member is disconnected (a
    %% partition must not outlive the trail that protects against its lagging rows).
    recently_released = #{} :: #{{term(), pid()} | pid() => integer()},
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
    %% Wall-clock ms of the most recent member departure this member observed
    %% (remove_member/2).  Trail pruning (§5.6) is suspended not only while a
    %% *current* member is disconnected but also within `conflict_release_ttl` of
    %% any departure: a member dropped from the set during a partition is no
    %% longer "a disconnected member", yet its stale rows can surface at a rejoin
    %% gather — the trail that discriminates lag from divergence must outlive
    %% that window too.  (A member that stays gone longer than the TTL is outside
    %% the trail's protection horizon either way.)
    last_departure = 0 :: integer()
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
    %% Subscribe to node up/down: the member reacts to `{nodeup, _}` by re-announcing
    %% its own `{join}` (partition-heal, membership identity).  The node-level mesh /
    %% reap / probe machinery lives in the sibling `dgen_registry_connector`, which
    %% subscribes independently — this member no longer meshes or reaps.
    net_kernel:monitor_nodes(true),
    %% Resolve the telemetry lookup here rather than lazily on the first event.  It
    %% is cached per VM either way (see `telemetry_available/0`), so this only moves
    %% *when* the one uncached call happens -- and where it happens is the point.
    %% Uncached, it is a synchronous call into `code_server`; lazily, it lands inside
    %% whichever member emits the first event, which under simulation is a scheduled
    %% process blocking on a process the scheduler does not own.  Everything after
    %% that is ordered by wall clock, and nothing reports it: `code:ensure_loaded/1`
    %% on a *missing* module loads nothing, so the "no module loaded mid-run" audit
    %% stays clean while the leak is wide open.  Here it runs while the tree is
    %% starting, which under `eta_run` is before a scheduler exists at all.
    _ = telemetry_available(),
    %% Periodic maintenance: expire the conflict-detection trail (§5.6) and the
    %% per-name kill-budget timestamps.
    arm(?PRUNE_INTERVAL, prune_released),
    %% Periodic replication heartbeat (leader only, see the define): lets a follower
    %% that lost the tail of the broadcast stream discover it and resync, rather than
    %% sitting diverged until the next write.
    arm(?REPLICA_HEARTBEAT_INTERVAL, replica_heartbeat),
    %% Announcing presence needs the elector's pid, which is deferred to
    %% `discover_elector` below: the supervisor is still synchronously waiting on *this*
    %% init/1 to return when it runs, so `supervisor:which_children/1` would deadlock if
    %% called from here directly. Returning via `{continue, ...}` lets the supervisor
    %% consider this child started, unblocking it, before the lookup runs.
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
            pending_forwards = #{},
            deferred_yes = [],
            pending_unregs = [],
            pending_registers = [],
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
            last_departure = 0
        },
        {continue, discover_elector}}.

%% ---------------------------------------------------------------------------
%% handle_continue/2
%% ---------------------------------------------------------------------------

%% Finds the elector via the shared supervisor (see the moduledoc's "Process
%% identity" note in dgen_registry) — safe here because the supervisor is no
%% longer blocked on this process's own init/1 by the time this runs — then
%% announces presence.  The mesh/probe startup lives in the connector.
handle_continue(
    discover_elector, State = #state{member_id = {_Node, Name} = MemberId, join_token = Token}
) ->
    Elector = dgen_registry:elector_pid(Name),
    true = is_pid(Elector),
    %% Announce presence; the elector will call {elector_assume_and_distribute}
    %% on the new leader, which then sends {apply_names_snapshot} to this member.
    %% `fresh = true`: this member has never synced (empty replica), so an
    %% unreachable-at-gather window for it cannot be hiding any bindings.
    dgen_server:cast(Elector, {join, MemberId, Token, true}),
    {noreply, State#state{elector = Elector}}.

%% ---------------------------------------------------------------------------
%% handle_call/3
%% ---------------------------------------------------------------------------

%% Readiness probe (see dgen_registry:await_ready/2).  This member can serve
%% registrations once it knows a leader *and* has synced registry state — it has
%% applied a snapshot (follower) or assumed leadership itself.  Before that a
%% register would be fast-rejected (`no`, no leader) or read an empty replica.
%% Side-effect free: a plain state read, no name is touched.
handle_call(ready, _From, State = #state{leader = Leader, synced = Synced}) ->
    {reply, Leader =/= undefined andalso Synced, State};
%% Observability probe (see dgen_registry:status/1).  Reports this member's own
%% *belief* about leadership — which is distinct from the elector's committed view
%% (`get_leader/1`), and the gap between them is exactly the handoff window a
%% deposed leader has not yet heard about (§5.1).  Side-effect free, and returns a
%% map so callers never depend on the #state{} record's field order.
handle_call(status, _From, State) ->
    #state{
        member_id = Self,
        leader = Leader,
        epoch = Epoch,
        synced = Synced,
        applied_version = AppliedVersion
    } = State,
    {reply,
        #{
            member_id => Self,
            leader => Leader,
            is_leader => Leader =:= Self,
            epoch => Epoch,
            synced => Synced,
            applied_version => AppliedVersion
        },
        State};
%% ---- Name registration ----------------------------------------------------

%% All routing (leader/follower/no-leader) is handled by route_register/5, which
%% either defers the reply (leader parks it in the commit buffer; a follower forwards
%% it; a not-ready member stashes it to await a leader) or answers immediately for an
%% adjudicated verdict.  The only immediate reply is `no`, and only when the name is
%% genuinely refused — see route_register/5.  See the moduledoc "blocks until a leader
%% exists" note.
handle_call({register, LogicalName, Pid, Meta}, From, State) ->
    {noreply, route_register(LogicalName, Pid, Meta, From, State)};
%% ---- Name unregistration (leader-routed call, mirroring the register path) --

handle_call({unregister, LogicalName}, From, State) ->
    route_unregister(LogicalName, From, State);
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
%% leader's {names_batch} broadcast (FIFO, ahead of the reply) has updated our row
%% before we answer the caller — read-after-write on this node's snapshot read.
handle_call(
    {set_metadata, LogicalName, Index, Data},
    From,
    State = #state{leader = Leader, member_id = Self, pending_forwards = PF}
) when Leader =/= undefined, Leader =/= Self ->
    %% Same unreachable-leader fast-fail as the register forward above.
    case member_reachable(Leader) of
        true ->
            Ref = make_ref(),
            cast_to_member(Leader, {set_meta_req, Ref, Self, LogicalName, Index, Data}),
            {noreply, State#state{pending_forwards = PF#{Ref => {set_meta, From}}}};
        false ->
            {reply, {error, no_leader}, State}
    end;
%% No leader yet.
handle_call({set_metadata, _LogicalName, _Index, _Data}, _From, State) ->
    {reply, {error, no_leader}, State};
%% ---- Consistent metadata read (leader only) --------------------------------

handle_call(
    {get_metadata, LogicalName},
    From,
    State = #state{leader = Leader, member_id = Leader, names_tab = Tab}
) ->
    %% Consistent reads are fenced (Guarantee 5): the row is captured here (at the
    %% mailbox point) and the reply is sent only after a spawned helper verifies our
    %% leadership against the durable leader key — a deposed leader that has not yet
    %% heard about the handoff cannot serve a stale answer as authoritative.
    reply_fenced(From, lookup_metadata(Tab, LogicalName), undefined, State),
    {noreply, State};
handle_call(
    {get_metadata, LogicalName},
    From,
    State = #state{leader = Leader}
) when Leader =/= undefined ->
    %% Forward to the leader, which answers `From` directly from its authoritative row
    %% (same non-blocking forward as {whereis}).  Unreachable leader → answer now.
    case member_reachable(Leader) of
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
%% Consistent query: must run on the leader.  If we are the leader, run it here
%% (on the mailbox — batch-consistent) and fence the reply like the other
%% consistent reads; otherwise forward (non-blocking) and let the leader answer
%% the caller directly.
handle_call(
    {query_consistent, Constraints},
    From,
    State = #state{leader = Leader, member_id = Leader, names_tab = Tab, inv_index = Inv}
) ->
    reply_fenced(From, run_query(Constraints, Tab, Inv), [], State),
    {noreply, State};
handle_call(
    {query_consistent, Constraints}, From, State = #state{leader = Leader}
) when Leader =/= undefined ->
    case member_reachable(Leader) of
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
    From,
    State = #state{leader = Leader, member_id = Leader}
) ->
    %% Fenced consistent read — see the {get_metadata} leader clause.
    reply_fenced(From, lookup_name(State#state.names_tab, LogicalName), undefined, State),
    {noreply, State};
handle_call(
    {whereis, LogicalName},
    From,
    State = #state{leader = Leader}
) when Leader =/= undefined ->
    %% Forward the caller's `From` to the leader, which replies to it directly — the
    %% follower never blocks its loop on a `gen_server:call` to the leader, so a
    %% consistent read can no longer head-of-line block the follower's other messages.
    %% If the leader's node is unreachable, answer now rather than hang the caller.
    case member_reachable(Leader) of
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

%% Return our current records map (`#{Name => {Pid, Index, Data}}`), the version we
%% have applied up to, and our copy of the released-pid trail, so a newly-assuming
%% leader can reconstruct the bindings *and their metadata* from the freshest of
%% every reachable member (the cross-member gather, §5.7 / §4.4) and merge the
%% conflict-detector trails (§5.6) so a legitimate unregister is not mistaken for a
%% divergence just because leadership moved.
handle_call(
    get_names_snapshot,
    _From,
    State = #state{names_tab = Tab, applied_version = Version, recently_released = Released}
) ->
    %% Ship the (potentially huge) names map as an off-heap binary, same as the
    %% distribute path — see encode_records/1.  The gathering leader decodes it.
    {reply, {encode_records(current_records(Tab)), Version, Released}, State};
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
%% subsequent `{names_batch}` broadcasts).
%% Fast path — a *continuing* leader (already leader for this exact epoch, holding
%% synced state) handling a member's join.  Its own replica is authoritative: every
%% follower is a prefix of its broadcast stream, so no member can hold a fresher
%% binding and there is nothing to gather.  So skip the O(members) gather, the
%% wholesale replica rebuild, and the leadership re-assumption (which would demonitor
%% and re-monitor every registered pid) — and, crucially, do NOT re-snapshot the
%% already-synced followers.  Just monitor the joiner, ship *it* the current snapshot
%% (inline, so it precedes any later {names_batch} broadcast — FIFO ordering),
%% and tell the existing followers to monitor the joiner too via a small token-only
%% message.  The full gather+distribute below is reserved for a genuine leadership
%% change, where a new leader must reconstruct from the freshest surviving member.
handle_call(
    {elector_assume_and_distribute, MemberId, AllIds, Tokens, FreshIds, Epoch},
    _From,
    State = #state{member_id = Self, leader = Self, epoch = Epoch, synced = true}
) when MemberId =/= undefined ->
    State1 = merge_peer_tokens(
        Tokens, add_member_monitors(extra_member_ids(MemberId, AllIds, Self), State)
    ),
    %% A joiner that has never synced (fresh — a brand-new member, zero cost to
    %% skip), or our own no-op re-join, holds no bindings: onboard immediately.
    %% A **rejoining** previously-synced member, however, may hold divergent live
    %% bindings from before it dropped out of the member set — the §5.6
    %% partition-heal case, which under a *continuing* leader never passes through
    %% the full handoff gather.  Gather its replica first (off this loop, bounded
    %% by ?GATHER_TIMEOUT) and adjudicate it against our authoritative table in
    %% the {joiner_gathered} continuation before onboarding, so a name reissued
    %% while the member was away is repaired by termination rather than silently
    %% overwritten with the old claimant left running.  Detection is best-effort:
    %% an unanswerable joiner is onboarded anyway (same posture as the full
    %% gather skipping an unreachable member).
    case MemberId =:= Self orelse lists:member(MemberId, FreshIds) of
        true ->
            {reply, ok, onboard_joiner(MemberId, AllIds, Tokens, State1)};
        false ->
            spawn_joiner_gather(MemberId, AllIds, Tokens, Epoch, self()),
            {reply, ok, State1}
    end;
handle_call(
    {elector_assume_and_distribute, MemberId, AllIds, Tokens, FreshIds, Epoch},
    _From,
    State = #state{member_id = Self}
) ->
    %% Genuine leadership change (a fresh leader, or a member_down that moved
    %% leadership).  The reconstruction gathers the freshest of every reachable member's
    %% replica — a **network** fan-out bounded by ?GATHER_TIMEOUT — so it runs **off this
    %% loop**: the member keeps draining messages during the wait, and the elector's call
    %% is answered `ok` immediately (an inline multi-second gather would time the
    %% elector's call out and stall any membership change queued behind it, the churn
    %% collapse).  A *consistent* snapshot of our own replica and applied_version is
    %% captured now (so a broadcast landing during the gather cannot skew freshest-wins),
    %% and the `{assume_gathered, …}` continuation finishes assuming when the gather
    %% returns — iff `assume_ref` still matches (a newer assume, or a snapshot making us a
    %% follower, harmlessly supersedes it).  Until then we are not yet leader/synced, so
    %% writes block (no reachable leader) and are re-driven by the continuation, never
    %% served against a half-built map.
    Tab = State#state.names_tab,
    SelfRecords = current_records(Tab),
    OtherIds = lists:delete(Self, AllIds),
    Ref = make_ref(),
    spawn_assume_gather(
        {MemberId, AllIds, Tokens, FreshIds, Epoch, Ref},
        SelfRecords,
        State#state.applied_version,
        OtherIds,
        State#state.elector,
        self(),
        State#state.tenant,
        State#state.tuid
    ),
    %% Clear the leader for the window: we are mid-handoff with no stable leader, so
    %% writes block (no reachable leader) and are re-driven by the continuation — rather
    %% than forwarding to the old, possibly-dead leader.  The continuation sets
    %% leader=Self.  `synced` is left as-is; the continuation's mark_synced handles it.
    {reply, ok, State#state{assume_ref = Ref, leader = undefined}};
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
%% Leader: an unregister forwarded by a follower.  Same shape as the direct
%% unregister call: capture our (authoritative) bound pid, optimistic delete, and
%% park the removal; the origin is answered via {unregister_reply} once committed.
handle_cast(
    {unregister_req, Ref, FollowerId, LogicalName},
    State = #state{leader = Leader, member_id = Leader, names_tab = Tab}
) ->
    ReleasedPid = lookup_name(Tab, LogicalName),
    {noreply,
        enqueue_op(
            {remove, LogicalName, ReleasedPid, {forward_unreg, FollowerId, Ref}},
            row_delete(State, LogicalName)
        )};
handle_cast({unregister_req, _Ref, _FollowerId, _LogicalName}, State) ->
    %% Not the leader (the follower's leader belief was stale).  Deliberately no
    %% reply: the follower keeps the removal stashed, and redrive_unregs/1 hands it
    %% (pid-guarded) to the leader it learns from the snapshot/leadership change.
    {noreply, State};
%% Follower: the leader committed a forwarded unregister — answer the caller `ok`.
%% FIFO puts this behind the batch's {names_batch} broadcast, so this member's
%% row state reflects the removal by the time the caller sees the reply.
handle_cast({unregister_reply, Ref}, State = #state{pending_forwards = PF}) ->
    case maps:take(Ref, PF) of
        {{unregister, _Name, _ReleasedPid, From}, PF1} ->
            gen_server:reply(From, ok),
            {noreply, State#state{pending_forwards = PF1}};
        _ ->
            %% Unknown Ref — already answered (e.g. re-driven on a leadership change).
            {noreply, State}
    end;
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
%% {names_batch} broadcast (FIFO ahead of this) has updated our row, so answering the
%% caller here preserves read-after-write for a subsequent local get_metadata.
handle_cast({set_meta_reply, Ref, Result}, State = #state{pending_forwards = PF}) ->
    case maps:take(Ref, PF) of
        {{set_meta, From}, PF1} ->
            gen_server:reply(From, Result),
            {noreply, State#state{pending_forwards = PF1}};
        _ ->
            %% Unknown Ref — already answered (e.g. rejected on a leadership change).
            {noreply, State}
    end;
%% Leader: a consistent metadata read forwarded by a follower.  Reply to the original
%% caller's `From` directly from the authoritative row — fenced, like every
%% consistent read (see reply_fenced).
handle_cast(
    {get_metadata_req, LogicalName, From},
    State = #state{leader = Leader, member_id = Leader, names_tab = Tab}
) ->
    reply_fenced(From, lookup_metadata(Tab, LogicalName), undefined, State),
    {noreply, State};
handle_cast({get_metadata_req, _LogicalName, From}, State) ->
    gen_server:reply(From, undefined),
    {noreply, State};
%% Leader: a consistent query forwarded by a follower.  Run it on our mailbox (between
%% batches → batch-consistent) and answer the original caller's `From` directly —
%% fenced, like every consistent read.
handle_cast(
    {query_req, Constraints, From},
    State = #state{leader = Leader, member_id = Leader, names_tab = Tab, inv_index = Inv}
) ->
    reply_fenced(From, run_query(Constraints, Tab, Inv), [], State),
    {noreply, State};
handle_cast({query_req, _Constraints, From}, State) ->
    gen_server:reply(From, []),
    {noreply, State};
%% Leader: a consistent read forwarded by a follower (whereis_name_consistent).  Reply
%% to the original caller's `From` directly from the authoritative map — fenced,
%% like every consistent read.
handle_cast(
    {whereis_req, LogicalName, From},
    State = #state{leader = Leader, member_id = Leader, names_tab = Tab}
) ->
    reply_fenced(From, lookup_name(Tab, LogicalName), undefined, State),
    {noreply, State};
%% Not the leader (stale belief, raced a leadership change) — answer `undefined`; the
%% caller can retry and reach the current leader.
handle_cast({whereis_req, _LogicalName, From}, State) ->
    gen_server:reply(From, undefined),
    {noreply, State};
%% Follower: the leader's answer to a forwarded registration.  The committed form
%% carries the batch's commit Version so the ack can be **version-guarded** (§5.5):
%% a `yes` is answered only when this member's replica has genuinely applied up to
%% that version — normally already true, because the batch's {names_batch}
%% broadcast precedes the reply (FIFO) — making the forwarding follower a
%% version-visible second holder the handoff gather (§5.7) is guaranteed to see.
%% If the broadcast was gap-refused (this member missed an earlier batch and is
%% awaiting a resync), the ack is deferred until the resync lands rather than
%% answered against a replica whose extra row freshest-wins reconstruction would
%% ignore — the fix for the silent-loss window a single leader crash could open.
handle_cast({register_reply, Ref, Result, Version}, State) ->
    {noreply, handle_register_reply(Ref, Result, Version, State)};
%% Legacy 3-tuple reply (a pre-version leader during a rolling upgrade): no version
%% to guard on — apply the old immediate behaviour.  Marked `legacy` rather than
%% version 0, because "0 is at-or-behind everything" put it on the same branch as a
%% guarded reply, and those two must now do different things (see
%% handle_register_reply/4).
handle_cast({register_reply, Ref, Result}, State) ->
    {noreply, handle_register_reply(Ref, Result, legacy, State)};
%% Follower: the leader asks us to confirm we hold a batch's bindings.  This cast
%% arrives (FIFO) after the batch's {names_batch} cast, so normally we have
%% applied them and hold the bindings — ack back to the leader.  The ack is
%% version-guarded: if we have *not* applied up to the batch's version (we observed
%% a gap and are awaiting a resync, or the broadcasts were stale-epoch-dropped), we
%% do not hold the bindings and must not claim to — the leader falls back to other
%% followers' acks or its timeout policy.
handle_cast(
    {replicate_sync, BatchRef, LeaderId, Version},
    State = #state{member_id = Self, applied_version = Applied}
) ->
    case Applied >= Version of
        true -> cast_to_member(LeaderId, {replicate_ack, BatchRef, Self});
        false -> ok
    end,
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
%% Replication broadcast.  One message per committed batch, carrying `{Ops, Epoch,
%% PrevVersion, Version, LeaderId}`: the batch's ops in commit order, the leader's
%% epoch, the commit version of the batch *before* this one (the leader's
%% applied_version when the batch was applied), this batch's commit version, and the
%% sender.  `apply_bcast/6` applies the batch only when it is contiguous with our
%% replica (PrevVersion matches ours); a gap means we missed a batch — we stop
%% applying and request a resync snapshot instead, so our replica always remains a
%% *prefix* of the leader's stream (see the applied_version field doc).
%%
%% The batch applies as a unit, which is what keeps "same applied_version implies
%% same content" true — see broadcast_batch/5 for why per-name messages could not.
handle_cast({names_batch, Ops, Epoch, PrevV, Version, LeaderId}, State) ->
    Apply = fun(S) -> lists:foldl(fun apply_bcast_op/2, S, Ops) end,
    {noreply, apply_bcast(Epoch, PrevV, Version, LeaderId, Apply, State)};
%% Follower: a member that observed a gap asks the leader for a fresh snapshot.
%% Answer with the same {apply_names_snapshot} shape the handoff fan-out uses —
%% empty extra-member and token payloads (they merge as no-ops) — carrying our
%% current epoch and applied version, which re-baselines the requester.
handle_cast(
    {resync_req, FollowerId},
    State = #state{
        leader = Leader,
        member_id = Leader,
        names_tab = Tab,
        epoch = Epoch,
        applied_version = Version
    }
) ->
    RecordsBin = encode_records(current_records(Tab)),
    cast_to_member(
        FollowerId, {apply_names_snapshot, RecordsBin, Leader, [], #{}, Epoch, Version}
    ),
    {noreply, State};
handle_cast({resync_req, _FollowerId}, State) ->
    %% Not the leader (the requester's stream came from someone else, or leadership
    %% has since moved) — ignore; the requester's retry/rejoin will find the leader.
    {noreply, State};
%% A pid-guarded remote retract (strict_replication fail-closed, §8): a member that
%% failed to durably retract a binding — it lost leadership with the retract still
%% buffered, or its retract batch failed — forwards it here so the binding whose
%% caller was answered `no` cannot silently live on.  Guarded by pid, so a name
%% since re-registered to someone else is never clobbered.
handle_cast(
    {retract_req, LogicalName, Pid}, State = #state{leader = Leader, member_id = Leader}
) ->
    {noreply, enqueue_op({retract, LogicalName, Pid}, State)};
handle_cast({retract_req, LogicalName, Pid}, State = #state{leader = Leader}) when
    Leader =/= undefined
->
    cast_to_member(Leader, {retract_req, LogicalName, Pid}),
    {noreply, State};
handle_cast({retract_req, LogicalName, Pid}, State) ->
    %% No leader right now — most likely this member is mid-handoff (the assume
    %% window, where leader is transiently `undefined`).  Stash the pid-guarded clear
    %% in `pending_unregs` rather than dropping it, so the assume continuation
    %% (redrive_unregs) re-drives it once a leader is established — otherwise a
    %% re-driven unregister that lands during a handoff would be silently lost.
    {noreply, stash_pending_unreg(LogicalName, Pid, State)};
%% Leadership transition snapshot sent by the new leader to all followers.
%% Applies the leader transition, the record update (pid + metadata per name), and
%% extra member monitors atomically within a single cast — no other message can
%% interleave.  The snapshot carries the leader's applied_version, which re-baselines
%% this member.  `RecordsPayload` is the `term_to_binary`-encoded records map (a legacy
%% `[{Name, {Pid, Index, Data}}]` list is also accepted — see decode_records/1).
handle_cast(
    {apply_names_snapshot, RecordsPayload, NewLeader, ExtraMembers, Tokens, Epoch, Version},
    State = #state{
        member_id = Self, leader = OldLeader, epoch = CurrentEpoch, applied_version = CurrentVersion
    }
) ->
    %% The re-baseline must be version-MONOTONIC, not merely epoch-guarded.  A snapshot
    %% carries the sending leader's applied_version; applying one whose version is BEHIND
    %% what we have already applied would wholesale-overwrite our replica *backward*,
    %% silently dropping a row we (or a peer) may have already acked.  That is exactly the
    %% handoff-gather race proven in formal/DgenRegistryReplication.tla: an old assume /
    %% resync snapshot delivered late — after we applied a newer broadcast — must be
    %% ignored, not obeyed.  A legitimate re-baseline never moves us backward: a genuine
    %% new leader is caught up to the durable version key before it fans out (see
    %% gather_caught_up/6), and a resync only ever carries the leader's current (>=)
    %% version.  So `Version >= CurrentVersion` rejects only genuinely stale snapshots;
    %% a rejected one is harmless — the normal gap/resync machinery re-baselines us from
    %% the current leader.
    case Epoch >= CurrentEpoch andalso Version >= CurrentVersion of
        true ->
            %% Overwrite the local replica and inverted index with the leader's snapshot
            %% before the leader transition: do_leader_changed -> assume_leadership (if we
            %% are becoming leader) reads the names back from the table to set up monitors.
            %% The snapshot satisfies any outstanding resync request (cancel it) and makes
            %% this member synced (its first snapshot re-announces `fresh = false`).
            State0 = records_replace(cancel_resync(State), decode_records(RecordsPayload)),
            State1 = do_leader_changed(NewLeader, OldLeader, Self, State0),
            %% A leader's snapshot supersedes any assume gather we had in flight (we are
            %% a follower of NewLeader now): drop its ref so the stale {assume_gathered}
            %% continuation is ignored when it lands.
            State2 = State1#state{epoch = Epoch, applied_version = Version, assume_ref = undefined},
            State3 = add_member_monitors(ExtraMembers, State2),
            State4 = merge_peer_tokens(Tokens, State3),
            %% Handle any buffered ops under the new leadership view: reject them
            %% if we are now a follower, or commit them if we remain leader.  The
            %% snapshot advanced applied_version, so release any forwarded-`yes`
            %% acks that were deferred on a gap (flush_deferred; a leadership
            %% change already rejected them in do_leader_changed), and re-drive any
            %% stashed unregisters and blocked registrations against the now-known
            %% leader (redrive_unregs / redrive_registers).
            {noreply,
                maybe_start_commit(
                    redrive_registers(redrive_unregs(flush_deferred(mark_synced(State4))))
                )};
        false ->
            {noreply, State}
    end;
%% Legacy fire-and-forget unregister casts (from a pre-call node during a rolling
%% upgrade).  The public API now routes unregisters as tracked calls (see the
%% {unregister} handle_call clauses); these keep the old wire shape working with
%% the old (silent-drop) semantics.
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
    {noreply,
        enqueue_op({remove, LogicalName, ReleasedPid, undefined}, row_delete(State, LogicalName))};
handle_cast({unregister, LogicalName}, State = #state{leader = Leader}) when
    Leader =/= undefined
->
    cast_to_member(Leader, {unregister, LogicalName}),
    {noreply, row_delete(State, LogicalName)};
%% A new member joined and the (continuing) leader's fast path is onboarding it
%% directly.  We are an existing follower whose replica is already up to date, so we
%% must not touch it — we only start monitoring the joiner and record its token, so a
%% future DOWN we observe for it is fenced with the token the elector holds.  (On a
%% genuine leadership change the new leader still re-snapshots everyone the full way.)
handle_cast({peer_joined, MemberId, Tokens}, State) ->
    {noreply, merge_peer_tokens(Tokens, add_member_monitors([MemberId], State))};
%% A durable presence subscription changed (§4.9): the elector pushes the delta to the
%% current leader.  Apply it only if we are still the leader for a current-or-newer
%% epoch — a stale push (we were deposed) is dropped and the survivor gets the full set
%% reseeded on its assume.  A `subscribe` seeds the subscription's watch membership and
%% fires its initial snapshot; an `unsubscribe` drops it.
handle_cast(
    {presence_update, Update, Epoch},
    State = #state{member_id = Self, leader = Self, epoch = Cur}
) when Epoch >= Cur ->
    {noreply, apply_presence_update(Update, State)};
handle_cast({presence_update, _Update, _Epoch}, State) ->
    {noreply, State};
handle_cast(_, State) ->
    {noreply, State}.

%% Apply a pushed subscription delta to the leader's live presence state.
apply_presence_update({subscribe, SubId, Watch, Notify}, State = #state{subs = Subs}) ->
    seed_subscription(SubId, State#state{subs = Subs#{SubId => {Watch, Notify}}});
apply_presence_update(
    {unsubscribe, SubId}, State = #state{subs = Subs, sub_matches = SM, notify_matches = NM}
) ->
    State#state{
        subs = maps:remove(SubId, Subs),
        sub_matches = maps:remove(SubId, SM),
        notify_matches = maps:remove(SubId, NM)
    };
apply_presence_update(unsubscribe_all, State) ->
    State#state{subs = #{}, sub_matches = #{}, notify_matches = #{}}.

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
    State = #state{
        committing = #{ref := Ref, mref := MRef, plan := Plan},
        member_id = Self,
        leader = Leader,
        epoch = Epoch
    }
) ->
    %% Drop the worker monitor (and any DOWN already queued behind this result —
    %% signal ordering guarantees the result arrives before the DOWN).
    erlang:demonitor(MRef, [flush]),
    PlanEpoch = maps:get(epoch, Plan),
    State1 =
        case Reply of
            {committed, Version} when Leader =:= Self, PlanEpoch =:= Epoch ->
                apply_committed_plan(Plan, Version, State);
            {committed, _Version} ->
                %% The commit landed, but this member's leadership view moved while
                %% the worker was in flight (deposed, or a new epoch).  Applying an
                %% old-term plan over new-term state — or broadcasting it stamped
                %% with the old epoch — would diverge the replicas, so the plan is
                %% discarded: nothing was applied or broadcast (the durable effect
                %% of a commit is only the version bump), callers are answered as
                %% rejected and retry against the current leader, and the batch's
                %% destructive ops are salvaged below so no unregister is lost.
                salvage_failed_plan(reject_plan(Plan, State), Plan);
            _Fenced_or_error ->
                %% {aborted, fenced} (lost leadership) or {error, _} (backend
                %% failure / retry exhausted): the batch did not commit.  Reject
                %% every registration in it (callers retry); re-drive the
                %% destructive ops so a lost unregister/retract cannot leave a
                %% permanent replica divergence.
                salvage_failed_plan(reject_plan(Plan, State), Plan)
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
    State1 = salvage_failed_plan(reject_plan(Plan, State), Plan),
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
            %% Fence the {member_down} with the token the elector *currently* holds for
            %% this peer, read fresh from the durable elector state — not our locally
            %% cached peer_tokens entry, which a dropped snapshot cast (e.g. the peer
            %% re-announced a fresh token on a nodeup while our node was transiently
            %% disconnected) may have left stale.  A stale token would make the elector
            %% discard this DOWN as if the peer had rejoined, stranding a genuinely dead
            %% member in the set and preventing failover (§5.7).  The fresh read still
            %% correctly discards a true stale DOWN: if the peer really did rejoin, the
            %% durable token has already advanced past what we send.
            %%
            %% The read + cast run in a short-lived helper, never inline: a synchronous
            %% call from this loop to the elector could deadlock against the elector's
            %% own post-commit `{elector_assume_and_distribute}` call back into this
            %% member.  The helper falls back to the cached token if the elector read
            %% fails (e.g. mid-shutdown).
            Cached = maps:get(DeadMemberId, State#state.peer_tokens, undefined),
            spawn_member_down(Elector, DeadMemberId, Cached),
            {noreply, remove_member(DeadMemberId, State)}
    end;
handle_info({nodeup, _Node}, State = #state{elector = Elector, member_id = Self, leader = Leader}) ->
    %% Re-announce to the elector — this member may have been removed from the
    %% member set while the node was unreachable (partition).  A fresh token is
    %% generated so any stale {member_down, Self, OldToken} already in the queue
    %% is discarded by the elector when it is eventually processed.  (The mesh
    %% re-connect and probe-gate bookkeeping for this nodeup live in the connector,
    %% which subscribes to node events independently.)
    NewToken = make_ref(),
    dgen_server:cast(Elector, {join, Self, NewToken, not State#state.synced}),
    %% A nodeup is a partition heal: if the leader we already know is now reachable
    %% again, re-drive any unregisters retained while it was unreachable *immediately*,
    %% rather than waiting for the re-onboarding snapshot that also triggers
    %% redrive_unregs/1.  This covers both ways an unregister survives a partition — a
    %% removal *stashed* because the leader was already seen unreachable (`pending_unregs`)
    %% and one *forwarded into the dying link* before the disconnect propagated
    %% (`pending_forwards`) — so the Non-goal-5 guarantee ("an explicit unregister is
    %% never silently lost") does not hinge on the snapshot arriving.  Pid-guarded and
    %% stash-clearing, so it composes harmlessly with the snapshot path.
    %%
    %% Guarded on the leader being reachable (or us): redrive_unregs/1 forwards and clears
    %% the stash unconditionally (safe on the snapshot path, where the snapshot's sender
    %% is by definition reachable), so calling it here while the leader is still
    %% unreachable would drop the retract into a dropped cast *and* clear the stash —
    %% losing it.  A different node's nodeup with our leader still unreachable is left for
    %% the snapshot / a later heal.
    State1 = State#state{join_token = NewToken},
    LeaderReachable =
        Leader =:= Self orelse (Leader =/= undefined andalso member_reachable(Leader)),
    {noreply,
        case LeaderReachable of
            true -> redrive_unregs(State1);
            false -> State1
        end};
%% Continuation of the continuing-leader fast path for a rejoining (non-fresh)
%% member: its replica snapshot arrived from the gather helper.  Adjudicate it
%% against our authoritative table (§5.6) — a name a rejoiner still binds to a
%% *different, live, un-trailed* pid than ours is a genuine conflict, resolved by
%% kill-both under the usual budget/alarm/config — then onboard the joiner with
%% our snapshot.  The killed authority's monitor DOWN drives the normal
%% unregister/broadcast cleanup, so no bespoke removal path is needed.  The
%% joiner's release trail is merged either way (the same merge the full-path
%% gather does), so a legitimately-unregistered-but-lagging row is suppressed,
%% not killed.  Guarded on leadership, epoch, and current membership: a handoff
%% or re-join that superseded this gather owns the onboarding instead.
handle_info(
    {joiner_gathered, MemberId, AllIds, Tokens, Epoch, Result},
    State = #state{member_id = Self, leader = Self, epoch = Epoch, members = Members}
) when is_map_key(MemberId, Members) ->
    State1 =
        case Result of
            {ok, {Records, _Version, Released}} ->
                MergedReleased = maps:merge_with(
                    fun(_K, A, B) -> max(A, B) end,
                    State#state.recently_released,
                    Released
                ),
                OwnPids = record_pids(current_records(State#state.names_tab)),
                Conflicts = detect_conflicts(OwnPids, [record_pids(Records)], MergedReleased),
                {_Names, S1} = resolve_conflicts(
                    Conflicts, OwnPids, State#state{recently_released = MergedReleased}
                ),
                S1;
            error ->
                %% The joiner did not answer in time — onboard it regardless; a
                %% divergence it still holds will surface at the next gather.
                State
        end,
    {noreply, onboard_joiner(MemberId, AllIds, Tokens, State1)};
handle_info({joiner_gathered, _MemberId, _AllIds, _Tokens, _Epoch, _Result}, State) ->
    %% Superseded (leadership/epoch moved, or the joiner left the set) while the
    %% gather was in flight — whatever superseded it re-snapshots the member.
    {noreply, State};
%% The asynchronous handoff gather (spawn_assume_gather) has returned: finish assuming
%% leadership with the freshest reconstruction, off the critical path the elector's call
%% was on.  Applied only if `assume_ref` still matches — a newer assume, or a snapshot
%% that made us a follower, has since replaced it and owns reconstruction instead.
%%
%% This is exactly the reconstruction the inline handler used to do, just deferred to
%% here: the freshest map *is* the reconstructed state (its highest-version owner holds
%% the freshest binding for every name — broadcasts are totally ordered + FIFO, and gap
%% detection keeps every replica a prefix of that stream; a binding no surviving member
%% held is absent and may be re-issued, single-fault uniqueness backstopped by §5.6, not
%% a durable taken-set).  MaxVersion becomes our applied_version and re-baselines the
%% followers.
handle_info(
    {assume_gathered, {MemberId, AllIds, Tokens, FreshIds, Epoch, Ref}, SelfRecords,
        FreshestRecords, MaxVersion, PeerResults, Subs},
    State = #state{member_id = Self, assume_ref = Ref}
) ->
    OtherIds = lists:delete(Self, AllIds),
    PeerRecordMaps = [Recs || {Recs, _V, _Rel} <- maps:values(PeerResults)],
    %% Incomplete gather (a committed, non-fresh member did not respond → unreachable):
    %% we may be missing its bindings, so the leader is `degraded` (§5.6).  A member the
    %% elector marked *fresh* (never synced) holds nothing, so its absence is not a
    %% degrade — a healthy scale-up does not flag one.
    Unreachable = OtherIds -- maps:keys(PeerResults),
    Degraded = lists:any(fun(Id) -> not lists:member(Id, FreshIds) end, Unreachable),
    %% Merge every reachable member's released-pid trail into our own (§5.6): the trail
    %% distinguishes "legitimately unregistered, reported by a lagging member" from a
    %% genuine divergence, and must survive leadership changes.
    MergedReleased = lists:foldl(
        fun({_Recs, _V, Rel}, Acc) -> maps:merge_with(fun(_P, A, B) -> max(A, B) end, Acc, Rel) end,
        State#state.recently_released,
        maps:values(PeerResults)
    ),
    %% Resolve genuine uniqueness conflicts the gather exposes (§5.6) — kill-both +
    %% alarm + bounded budget; conflicted names are dropped (the fan-out propagates the
    %% drop).  A no-op in the common conflict-free handoff.
    FreshestNames = record_pids(FreshestRecords),
    {CleanNames, State0} = resolve_conflicts(
        detect_conflicts(
            FreshestNames,
            [record_pids(SelfRecords) | [record_pids(M) || M <- PeerRecordMaps]],
            MergedReleased
        ),
        FreshestNames,
        State#state{recently_released = MergedReleased}
    ),
    %% Metadata rides the surviving bindings (resolve_conflicts only ever *drops* names).
    Records = maps:with(maps:keys(CleanNames), FreshestRecords),
    %% Reconstruct the local replica (pid + metadata) + inverted index wholesale, then
    %% become leader, monitoring the reconstructed names.  Clearing assume_ref marks the
    %% assume complete.
    State0a = records_replace(State0, Records),
    State1 = relinquish_leadership(State0a),
    %% Seed the durable presence subscriptions (§4.9) with the set the gather helper
    %% pulled from the co-located elector off this loop (`Subs`) — *before* assuming, so
    %% assume_leadership -> recompute_sub_matches computes each watch set against the
    %% reconstructed replica.  A silent reseed — it does not re-fire initial snapshots;
    %% subscribers keep their view, and a subscribe racing the last moment of the handoff
    %% pushes a {presence_update} delta we apply once established as leader below.
    State2 = assume_leadership(State1#state{
        leader = Self,
        epoch = Epoch,
        applied_version = MaxVersion,
        degraded = Degraded,
        assume_ref = undefined,
        subs = Subs
    }),
    State3 = add_member_monitors(extra_member_ids(MemberId, AllIds, Self), State2),
    State4 = merge_peer_tokens(Tokens, State3),
    %% Encode the snapshot once; every follower's cast shares the one refc binary.
    RecordsBin = encode_records(Records),
    lists:foreach(
        fun(Id) ->
            cast_to_member(
                Id,
                {apply_names_snapshot, RecordsBin, Self, extra_member_ids(MemberId, AllIds, Id),
                    Tokens, Epoch, MaxVersion}
            )
        end,
        OtherIds
    ),
    %% We are the leader now: reject any registrations we forwarded as a follower (the
    %% old leader will never answer them), re-drive stashed unregisters and blocked
    %% registrations into our own commit buffer, then commit anything buffered while we
    %% assumed.  The gather made us synced; any outstanding resync is moot.
    {noreply,
        maybe_start_commit(
            redrive_registers(
                redrive_unregs(reject_forwards(mark_synced(cancel_resync(State4))))
            )
        )};
handle_info({assume_gathered, _Ctx, _SelfRecords, _Freshest, _MaxV, _PeerResults, _Subs}, State) ->
    %% Superseded (a newer assume, or a snapshot made us a follower) while the gather
    %% was in flight — the current assume, or the leader, owns reconstruction now.
    {noreply, State};
%% Periodic maintenance: expire stale entries from the conflict-detection trail
%% (§5.6) and the per-name kill-budget timestamps, then re-arm.  Trail pruning is
%% suspended while any current member is disconnected: a disconnected member misses
%% unregister broadcasts outright, so its stale rows can show up at a rejoin gather
%% long after any broadcast lag — the trail must outlive the partition (see the
%% ?PRUNE_INTERVAL note).
handle_info(
    prune_released,
    State = #state{recently_released = Rel, kill_budget = KB, members = Members, config = Config}
) ->
    arm(?PRUNE_INTERVAL, prune_released),
    Now = erlang:system_time(millisecond),
    AllConnected = lists:all(
        fun({Node, _Name}) -> dgen_utils:node_reachable(Node) end,
        maps:keys(Members)
    ),
    TTL = dgen_config:conflict_release_ttl(Config),
    %% Pruning is also suspended within a TTL of the most recent member departure:
    %% a member dropped from the set (so no longer "a disconnected member") may
    %% rejoin with stale rows, and the trail must still discriminate lag from
    %% divergence at that rejoin's gather (§5.6).  A member gone longer than the
    %% TTL is outside the trail's protection horizon either way.
    RecentDeparture = Now - State#state.last_departure < TTL,
    Rel1 =
        case AllConnected andalso not RecentDeparture of
            true ->
                Cutoff = Now - TTL,
                maps:filter(fun(_Key, Ts) -> Ts >= Cutoff end, Rel);
            false ->
                Rel
        end,
    {_Count, Window} = dgen_config:conflict_kill_budget(Config),
    KB1 = maps:filter(
        fun(_Name, Tss) -> Tss =/= [] end,
        maps:map(fun(_Name, Tss) -> [Ts || Ts <- Tss, Ts >= Now - Window] end, KB)
    ),
    %% Also drop stashed registrations whose callers have timed out (bounds
    %% pending_registers while no leader appears — see the field doc).
    {noreply, prune_pending_registers(State#state{recently_released = Rel1, kill_budget = KB1})};
%% The resync request we sent went unanswered (dropped cast, deposed target) —
%% clear the guard so the next gap-observing broadcast (or the leader's replication
%% heartbeat, which is one) requests again.
handle_info(resync_timeout, State) ->
    {noreply, State#state{resync_timer = undefined}};
%% Replication heartbeat (see ?REPLICA_HEARTBEAT_INTERVAL).  Only the leader sends,
%% and only an *empty* batch stamped at its current applied version: a follower that
%% is caught up applies nothing, one that is behind takes apply_bcast/6's gap branch
%% and asks for a resync.  This is what makes replication converge without new
%% writes; every other gap-detection trigger needs traffic that a quiescent cluster
%% does not have.
handle_info(replica_heartbeat, State = #state{member_id = Self, leader = Self}) ->
    broadcast_heartbeat(State),
    arm(?REPLICA_HEARTBEAT_INTERVAL, replica_heartbeat),
    {noreply, State};
handle_info(replica_heartbeat, State) ->
    %% Not the leader — nothing to advertise, but keep the timer running so this
    %% member heartbeats immediately if it is later elected.
    arm(?REPLICA_HEARTBEAT_INTERVAL, replica_heartbeat),
    {noreply, State};
%% (broadcast_heartbeat/1, the leader clause's advertisement above, is defined
%% beside broadcast_batch/5's MUTATION ifdef — it is itself mutation-planted.)
%% Re-drive the destructive ops of a batch that failed to commit (see
%% salvage_failed_plan): re-enqueue them if we are (still) the leader, or forward
%% the pid-guarded clears to the current leader if we were deposed — so an
%% unregister or strict-mode retract is never silently lost, which would otherwise
%% leave replicas divergent (or a `no`-answered binding alive) until an unrelated
%% handoff.
handle_info({requeue_ops, Ops}, State = #state{leader = Leader, member_id = Self}) when
    Leader =:= Self
->
    {noreply, lists:foldl(fun enqueue_op/2, State, Ops)};
handle_info({requeue_ops, Ops}, State = #state{leader = Leader}) when Leader =/= undefined ->
    lists:foreach(
        fun
            ({remove, Name, Pid, _Origin}) when is_pid(Pid) -> forward_retract(Leader, Name, Pid);
            ({retract, Name, Pid}) -> forward_retract(Leader, Name, Pid);
            %% A remove that never had a bound pid nets to nothing; a down is the
            %% new leader's to observe via its own monitor.
            (_) -> ok
        end,
        Ops
    ),
    {noreply, State};
handle_info({requeue_ops, Ops}, State) ->
    %% No leader right now (likely mid-handoff — the assume window, leader =
    %% undefined).  Stash the pid-guarded clears in `pending_unregs` so the assume
    %% continuation (or the next snapshot) re-drives them, rather than dropping them
    %% and leaving a divergence until an unrelated handoff.  Downs are dropped — the
    %% new leader monitors those pids itself.
    Stashed = lists:foldl(
        fun
            ({remove, Name, Pid, _Origin}, S) when is_pid(Pid) -> stash_pending_unreg(Name, Pid, S);
            ({retract, Name, Pid}, S) -> stash_pending_unreg(Name, Pid, S);
            (_, S) -> S
        end,
        State,
        Ops
    ),
    {noreply, Stashed};
%% Periodic durable-epoch check (piggybacked on the mesh pass): the election record
%% in the database is ahead of what this member has heard.  That means a leadership
%% handoff happened that never reached us — the elector's post-commit assume action
%% failed (consumer crash, unreachable leader), or we missed the snapshot fan-out.
%% Re-announcing produces a fresh membership event whose assume/fan-out brings us —
%% and, if we are the durable leader, our leadership — up to date.
handle_info({durable_epoch, DurableEpoch}, State = #state{epoch = Epoch}) when
    is_integer(DurableEpoch), DurableEpoch > Epoch
->
    #state{elector = Elector, member_id = Self, join_token = Token, synced = Synced} = State,
    dgen_server:cast(Elector, {join, Self, Token, not Synced}),
    {noreply, State};
handle_info({durable_epoch, _DurableEpoch}, State) ->
    {noreply, State};
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
            %% Lost leadership — resolve any direct registrations still awaiting
            %% replica acks (their timers must not fire into a follower), then
            %% demonitor registered Pids, keeping names for snapshot reads.
            relinquish_leadership(fail_pending_acks(State#state{leader = NewLeader}));
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
    %% Seed each subscription's watch-set membership from the reconstructed replica, so
    %% the first commit under this leadership computes its deltas against the true
    %% current state (§4.9).  Leader-only derived state — a follower never reads it.
    recompute_sub_matches(State#state{name_to_ref = NTR, ref_to_name = RTN}).

%% Recompute every subscription's watch-set and notify-set membership
%% (`#{SubId => #{Name => Pid}}` each) by running its watch/notify queries against the
%% current replica.  Called on assuming leadership (the only time the maps must be rebuilt
%% from scratch); thereafter apply_committed_plan keeps them current incrementally.  A
%% silent reseed — it does not re-fire snapshots; the new leader just advances deltas
%% correctly from here (subscribers keep the view they already had).
recompute_sub_matches(State = #state{subs = Subs, names_tab = Tab, inv_index = Inv}) ->
    SM = maps:map(
        fun(_SubId, {Watch, _Notify}) -> query_match_names(Watch, Tab, Inv) end,
        Subs
    ),
    NM = maps:map(
        fun(_SubId, {_Watch, Notify}) -> query_match_names(Notify, Tab, Inv) end,
        Subs
    ),
    State#state{sub_matches = SM, notify_matches = NM}.

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
%% totally ordered (commit versions are globally monotonic) and FIFO, and gap
%% detection keeps every replica a *prefix* of that stream (see applied_version),
%% the member with the highest applied_version holds the freshest binding for
%% *every* name, so the single freshest map is the reconstruction source — and a
%% version tie implies identical content, so tie-breaking (self-first) is safe.
%% The other members' snapshots are returned too, for the conflict detector (§5.6)
%% — a partition can leave a member holding a divergent live binding the freshest
%% map never saw — and for the released-trail merge.  Returns
%% `{FreshestNames, MaxVersion, PeerResults}` where PeerResults is
%% `#{MemberId => {Records, Version, Released}}` for each *other* member that
%% answered.  The per-peer snapshot calls run in parallel (each in a short-lived
%% helper process), so the assuming leader's loop is blocked for at most about one
%% ?GATHER_TIMEOUT overall rather than one per unreachable peer.
gather_maps(SelfNames, SelfVersion, OtherIds) ->
    PeerResults = gather_peer_snapshots(OtherIds),
    {FreshestNames, MaxVersion} = maps:fold(
        fun(_Id, {Names, Version, _Released}, {_BestNames, BestVersion} = Best) ->
            case Version > BestVersion of
                true -> {Names, Version};
                false -> Best
            end
        end,
        {SelfNames, SelfVersion},
        PeerResults
    ),
    {FreshestNames, MaxVersion, PeerResults}.

%% Onboard a joining member on the continuing-leader fast path: ship it our full
%% snapshot (unavoidable O(names) for one member — exactly what the full path would
%% send it), carrying the member list so it monitors every peer, and tell the
%% existing followers to monitor the joiner via a small token-only {peer_joined}
%% (their prefix-consistent replicas are untouched — no records travel there).
onboard_joiner(MemberId, AllIds, Tokens, State = #state{member_id = Self, epoch = Epoch}) ->
    RecordsBin = encode_records(current_records(State#state.names_tab)),
    cast_to_member(
        MemberId,
        {apply_names_snapshot, RecordsBin, Self, AllIds, Tokens, Epoch, State#state.applied_version}
    ),
    lists:foreach(
        fun(Id) -> cast_to_member(Id, {peer_joined, MemberId, Tokens}) end,
        [Id || Id <- AllIds, Id =/= Self, Id =/= MemberId]
    ),
    State.

%% Fetch a rejoining member's replica snapshot off the leader's loop (bounded by
%% member_names/1's ?GATHER_TIMEOUT; an unreachable joiner fast-fails), reporting
%% back as {joiner_gathered, …} for the fast-path §5.6 adjudication.  AllIds/Tokens
%% ride along so the continuation onboards with exactly the member list the elector
%% committed for this join event.
spawn_joiner_gather(MemberId, AllIds, Tokens, Epoch, Owner) ->
    _ = spawn(fun() ->
        Owner ! {joiner_gathered, MemberId, AllIds, Tokens, Epoch, member_names(MemberId)}
    end),
    ok.

%% Run the full handoff gather (the peer-snapshot RPCs, bounded overall by
%% ?GATHER_TIMEOUT) off the member's loop, reporting the freshest reconstruction back as
%% {assume_gathered, …} for the continuation to finish assuming leadership (§5.7).
%% `SelfRecords` / `SelfVersion` are captured by the caller — a consistent snapshot of
%% our own replica — so a broadcast that lands during the gather cannot skew the
%% freshest-wins comparison.  `Ctx` carries the elector's assume payload plus the
%% correlation ref the continuation matches against `assume_ref`.
%%
%% The durable presence subscriptions (§4.9) are pulled from the co-located `Elector`
%% here too — **off the member's loop, deliberately**: doing it in the continuation
%% (on the loop) would be a synchronous member→elector call that can dead-lock against
%% the elector's own synchronous assume call back into this member (both block until a
%% timeout fires).  Pulling in this helper keeps the member loop responsive, and reading
%% after the gather means any subscribe committed during the (up to ~?GATHER_TIMEOUT)
%% gather is captured; a subscribe racing the final millisecond pushes a
%% {presence_update} delta the now-established leader applies.
spawn_assume_gather(Ctx, SelfRecords, SelfVersion, OtherIds, Elector, Owner, Tenant, Tuid) ->
    _ = spawn(fun() ->
        %% Fall back to a self-only reconstruction if the gather ever throws, so the
        %% continuation still fires and the member is never stranded leaderless (it
        %% assumes with its own replica, degraded — a later handoff reconciles).
        {FreshestRecords, MaxVersion, PeerResults} =
            gather_caught_up(SelfRecords, SelfVersion, OtherIds, Tenant, Tuid),
        Subs = fetch_subscriptions(Elector),
        Owner ! {assume_gathered, Ctx, SelfRecords, FreshestRecords, MaxVersion, PeerResults, Subs}
    end),
    ok.

%% Gather, then ensure the reconstruction is at least as fresh as the durable version
%% key before returning — the handoff-gather race fix proven in
%% formal/DgenRegistryReplication.tla (the SafeAssume fence).  The committed frontier is
%% the durable version key (dgen_registry_names:read_committed_frontier/2); a live member
%% can never be ahead of it, so a gathered MaxVersion below it means the gather is
%% incomplete — most
%% often because it raced an in-flight (committed but not-yet-applied) broadcast still
%% queued in a reachable peer's mailbox.  Reconstructing from that stale map and fanning
%% it out would overwrite the very follower about to hold the missing binding, silently
%% dropping an already-acked registration (the finding).  The lagging peer applies its
%% queued broadcast within milliseconds, so we briefly re-gather until MaxVersion catches
%% up to the frontier or ?GATHER_TIMEOUT elapses.  Past the deadline the missing version's
%% holder is genuinely unreachable (a real degrade / multi-fault, not a race), so we
%% proceed with what we have — the continuation flags `degraded` and §5.6 backstops any
%% conflict.  Runs entirely in the off-loop gather helper, so the member loop stays
%% responsive and healthy handoffs (where MaxVersion already equals the frontier on the
%% first pass) pay nothing beyond one durable read.
gather_caught_up(SelfRecords, SelfVersion, OtherIds, Tenant, Tuid) ->
    Deadline = erlang:monotonic_time(millisecond) + ?GATHER_TIMEOUT,
    gather_caught_up(SelfRecords, SelfVersion, OtherIds, Tenant, Tuid, Deadline).

gather_caught_up(SelfRecords, SelfVersion, OtherIds, Tenant, Tuid, Deadline) ->
    Result =
        {_FreshestRecords, MaxVersion, _PeerResults} =
        try
            gather_maps(SelfRecords, SelfVersion, OtherIds)
        catch
            _:_ -> {SelfRecords, SelfVersion, #{}}
        end,
    %% Best-effort read of the committed frontier — normalised to the same scale as a
    %% member's applied_version (read_committed_frontier strips the versionstamp's batch
    %% bytes).  A backend hiccup (or a test double without a real tenant) falls back to 0,
    %% which disables the fence rather than wedging the handoff — the pre-fix behaviour,
    %% never worse.
    DurableVersion =
        try
            dgen_registry_names:read_committed_frontier(Tenant, Tuid)
        catch
            _:_ -> 0
        end,
    case MaxVersion >= DurableVersion orelse erlang:monotonic_time(millisecond) >= Deadline of
        true ->
            Result;
        false ->
            timer:sleep(?ASSUME_CATCHUP_INTERVAL),
            gather_caught_up(SelfRecords, SelfVersion, OtherIds, Tenant, Tuid, Deadline)
    end.

%% Read the durable subscription set from the (co-located) elector — best-effort, under a
%% short bound so a momentarily-busy elector cannot stall the gather helper: on timeout
%% the leader assumes with the empty set and a later {presence_update} delta / the next
%% handoff reconciles.  Called only from off-loop helpers (never the member loop).
fetch_subscriptions(Elector) when is_pid(Elector) ->
    try dgen_server:priority_call(Elector, get_subscriptions, ?SUBS_PULL_TIMEOUT) of
        Subs when is_map(Subs) -> Subs;
        _ -> #{}
    catch
        _:_ -> #{}
    end;
fetch_subscriptions(_Elector) ->
    #{}.

%% Fan out one member_names/1 helper per peer and collect the answers under a
%% single overall deadline.  Late answers (a peer that responds after the deadline)
%% arrive as ordinary messages and are dropped by the catch-all handle_info clause.
gather_peer_snapshots([]) ->
    #{};
gather_peer_snapshots(OtherIds) ->
    Ref = make_ref(),
    Parent = self(),
    lists:foreach(
        fun(MemberId) ->
            spawn(fun() -> Parent ! {Ref, MemberId, member_names(MemberId)} end)
        end,
        OtherIds
    ),
    Deadline = erlang:monotonic_time(millisecond) + ?GATHER_TIMEOUT + 500,
    collect_gather(Ref, OtherIds, Deadline, #{}).

collect_gather(_Ref, [], _Deadline, Acc) ->
    Acc;
collect_gather(Ref, Waiting, Deadline, Acc) ->
    Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
    _ = ?ETA_LOG({gather_wait, length(Waiting), Timeout}),
    receive
        {Ref, MemberId, {ok, Snapshot}} ->
            collect_gather(Ref, lists:delete(MemberId, Waiting), Deadline, Acc#{
                MemberId => Snapshot
            });
        {Ref, MemberId, error} ->
            collect_gather(Ref, lists:delete(MemberId, Waiting), Deadline, Acc)
    after Timeout ->
        _ = ?ETA_LOG({gather_timed_out, length(Waiting)}),
        Acc
    end.

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
             || P <- maps:keys(PidSet), P =/= Authority, not released_entry(Name, P, Released)
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

%% Was this pid explicitly released *from this name* recently?  Trail keys are
%% {Name, Pid} pairs, so releasing a pid under one of its names never suppresses a
%% genuine conflict on another name the same pid also holds.  Bare-pid keys — the
%% legacy trail shape, seen in a pre-upgrade member's gathered trail during a
%% rolling upgrade — are honoured too (the coarser, name-agnostic suppression the
%% old code applied), until the mixed entries age out of the TTL.
released_entry(Name, Pid, Released) ->
    maps:is_key({Name, Pid}, Released) orelse maps:is_key(Pid, Released).

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
    ),
    %% Also surface through telemetry (when available), like degrade-open events —
    %% `action` distinguishes terminated / detect_only / budget_exhausted, the last
    %% being the one an operator must act on.
    emit_telemetry(
        [dgen_registry, conflict],
        #{divergent_count => length(Divergent)},
        #{name => Name, authority => Authority, action => Action}
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

%% Bounded, fault-tolerant snapshot read of a peer member's names map + version +
%% released trail.  A member on a disconnected node, or one that does not answer
%% within ?GATHER_TIMEOUT, is skipped — its bindings are unavailable for this
%% gather.  With no durable taken-set, a binding only that member held is lost
%% (single-fault uniqueness); if it was replicated (two-holder), the freshest
%% surviving holder still carries it.
member_names({Node, Name}) ->
    case lists:member(Node, nodes()) of
        false ->
            error;
        true ->
            %% Everything — the call, the decode, and the shape check — runs inside the
            %% try: a `try ... of` body is *not* covered by the catch, and decode_records
            %% can fault on a corrupt payload, so keep it protected and fall to `error`.
            try
                {Payload, Version, Released} =
                    gen_server:call({Name, Node}, get_names_snapshot, ?GATHER_TIMEOUT),
                Names = decode_records(Payload),
                true = is_map(Names) andalso is_integer(Version) andalso is_map(Released),
                {ok, {Names, Version, Released}}
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

%% Route a registration to wherever it must go, deferring the caller's reply in every
%% case except an adjudicated refusal.  `no` is answered *only* when the name is
%% genuinely refused — held by a different pid is decided downstream by the leader; the
%% one immediate `no` here is the `reject_when_degraded` prevention mode (§5.6), an
%% explicit opt-in.  Crucially, **no leader (or an unreachable one) does not answer
%% `no`** — that would be a false `already_started` to OTP.  Instead the caller is
%% *blocked*: the registration is stashed to await a leader (redrive_registers/1), so
%% it succeeds if one appears within the caller's `register_timeout` and otherwise the
%% caller's own call times out.  Returns the new state; the reply is either already
%% sent (immediate `no`) or deferred.
route_register(Name, Pid, Meta, From, State = #state{leader = Leader, member_id = Self}) ->
    if
        Leader =:= Self ->
            %% Leader: park in the group-commit buffer, answered on commit — unless the
            %% degraded-prevention mode refuses it outright.
            case reject_new_when_degraded(Name, State) of
                true ->
                    gen_server:reply(From, no),
                    State;
                false ->
                    enqueue_op({add, Name, Pid, Meta, {local, From}}, State)
            end;
        Leader =:= undefined ->
            %% No leader elected yet (startup, or a brief handoff window) — block.
            stash_pending_register(Name, Pid, Meta, From, State);
        true ->
            %% A different member leads.  Forward if reachable (the caller blocks on the
            %% eventual {register_reply}); if unreachable (partition), block and stash
            %% rather than forward into a dropped cast — the removal-style re-drive picks
            %% it up when a leader is reachable again.
            case member_reachable(Leader) of
                true ->
                    Ref = make_ref(),
                    cast_to_member(Leader, {register_req, Ref, Self, Name, Pid, Meta}),
                    State#state{
                        pending_forwards =
                            (State#state.pending_forwards)#{
                                Ref => {register, Name, Pid, Meta, From}
                            }
                    };
                false ->
                    stash_pending_register(Name, Pid, Meta, From, State)
            end
    end.

%% Route an unregister (or unsubscribe — the reserved subscription name) to wherever it
%% must go, mirroring route_register.  Returns a handle_call reply tuple.  The bound pid
%% is captured *before* any optimistic row_delete so the leader can seed the durable
%% clear and the conflict trail (§5.6):
%%   - Leader: optimistically remove the local row (a caller-side whereis_name/1 on this
%%     node sees the delete at once) and park the durable removal in the group-commit
%%     buffer with the caller as origin — answered `ok` on commit.  A batch that fails
%%     salvages its removes, so `ok` means "accepted and re-driven", never "dropped".
%%   - Follower, reachable leader: delete the local row and forward as a *tracked*
%%     request stashed under Ref (a dropped forward or deposed leader is recovered by
%%     redrive_unregs/1 — a pid-guarded retract to the then-current leader).
%%   - Follower, unreachable leader / no leader: answer `ok` now and stash the intent;
%%     it is re-driven once a leader is reachable (CP — the commit waits, the intent is
%%     never lost).
route_unregister(
    LogicalName, From, State = #state{leader = Leader, member_id = Self, names_tab = Tab}
) ->
    ReleasedPid = lookup_name(Tab, LogicalName),
    if
        Leader =:= Self ->
            {noreply,
                enqueue_op(
                    {remove, LogicalName, ReleasedPid, {unreg, From}},
                    row_delete(State, LogicalName)
                )};
        Leader =/= undefined ->
            State1 = row_delete(State, LogicalName),
            case member_reachable(Leader) of
                true ->
                    Ref = make_ref(),
                    cast_to_member(Leader, {unregister_req, Ref, Self, LogicalName}),
                    {noreply, State1#state{
                        pending_forwards = (State1#state.pending_forwards)#{
                            Ref => {unregister, LogicalName, ReleasedPid, From}
                        }
                    }};
                false ->
                    {reply, ok, stash_pending_unreg(LogicalName, ReleasedPid, State1)}
            end;
        true ->
            {reply, ok,
                stash_pending_unreg(LogicalName, ReleasedPid, row_delete(State, LogicalName))}
    end.

%% Stash a registration that could not be routed (no reachable leader), tagged with
%% the wall-clock ms it arrived so the periodic prune can drop it once its caller has
%% (about) timed out.
stash_pending_register(Name, Pid, Meta, From, State = #state{pending_registers = PR}) ->
    Now = erlang:system_time(millisecond),
    State#state{pending_registers = [{Now, Name, Pid, Meta, From} | PR]}.

%% Re-drive every stashed registration now that a leader may be known — called on the
%% leadership-establishing events (assuming leadership, applying a leader's snapshot).
%% Each entry is re-routed: it commits (we are leader), forwards (a follower with a
%% reachable leader), or re-stashes (still no reachable leader).  An entry whose caller
%% has already timed out (older than `register_timeout`) is dropped without a reply.
redrive_registers(State = #state{pending_registers = []}) ->
    State;
redrive_registers(State = #state{pending_registers = PR, config = Config}) ->
    TTL = dgen_config:register_timeout(Config),
    Now = erlang:system_time(millisecond),
    %% Clear first; route_register re-stashes any that still cannot proceed.  Oldest
    %% first (the list is newest-first) for rough FIFO fairness among waiters.
    lists:foldl(
        fun({At, Name, Pid, Meta, From}, S) ->
            case Now - At >= TTL of
                true -> S;
                false -> route_register(Name, Pid, Meta, From, S)
            end
        end,
        State#state{pending_registers = []},
        lists:reverse(PR)
    ).

%% Drop stashed registrations whose callers have (about) timed out — the periodic
%% bound on `pending_registers` so a prolonged no-leader window (a partition) cannot
%% grow it without limit.  No reply is sent (the caller has moved on).
prune_pending_registers(State = #state{pending_registers = []}) ->
    State;
prune_pending_registers(State = #state{pending_registers = PR, config = Config}) ->
    TTL = dgen_config:register_timeout(Config),
    Now = erlang:system_time(millisecond),
    State#state{pending_registers = [E || {At, _, _, _, _} = E <- PR, Now - At < TTL]}.

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
    %% set_metadata (caller retries against the new leader); forward buffered
    %% removes/retracts to the new leader as pid-guarded retracts so an unregister
    %% (or a strict-mode retraction whose caller already heard `no`) is not silently
    %% lost with the deposition.  Downs are dropped: the new leader monitors the
    %% pids itself and observes their exits directly.
    lists:foreach(
        fun
            ({add, _N, _P, _Meta, Origin}) ->
                deliver_reply(Origin, reject_value(Origin));
            ({set_meta, _N, _I, _D, Origin}) ->
                deliver_reply(Origin, reject_value(Origin));
            ({remove, N, P, Origin}) ->
                %% Forward the pid-guarded clear, and answer the tracked caller `ok`
                %% (the forwarded retract carries the removal onward).
                case is_pid(P) of
                    true -> forward_retract(Leader, N, P);
                    false -> ok
                end,
                case Origin of
                    undefined -> ok;
                    _ -> deliver_reply(Origin, reject_value(Origin))
                end;
            ({retract, N, P}) ->
                forward_retract(Leader, N, P);
            (_) ->
                ok
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
        last_version = LastVersion,
        config = Config
    } = State,
    %% Take up to `commit_batch_size` oldest ops (front of the queue); the rest ride
    %% the following batch.  num_pending keeps the size O(1), so no length/1 scan.
    %% Bounding the batch keeps a burst (e.g. a departing node's DOWN flood) from
    %% coalescing into one enormous commit whose O(batch) plan/apply/broadcast would
    %% freeze the loop — see dgen_config:commit_batch_size/1.
    BatchSize = min(NumPending, dgen_config:commit_batch_size(Config)),
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
                #{owner => self(), ref => Ref, epoch => Epoch, read_version => LastVersion}
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
        member_id = Self,
        name_to_ref = NTR0,
        ref_to_name = RTN0,
        members = Members,
        recently_released = Rel0,
        applied_version = PrevVersion
    } = State,
    %% Apply the durable delta to the local replica (and the inverted index).  Only the
    %% names in DBOp are touched, so a concurrent optimistic unregister of an untouched
    %% name (already removed from the table) is never clobbered.
    State0 = apply_dbop(State, DBOp, WNames, WMeta),
    {NTR1, RTN1} = apply_monitor_ops(DBOp, WNames, NTR0, RTN0),
    %% Replicate to followers first, so the replicate_sync below (FIFO behind it)
    %% is seen by a follower only after it already holds the batch's bindings.
    %%
    %% The whole batch travels as **one** `{names_batch, …}` message carrying this
    %% batch's commit Version, the predecessor version (our applied_version before
    %% this batch) and our id, so followers apply only contiguous batches and a
    %% member that missed one detects the gap and resyncs (see apply_bcast/6).
    %%
    %% One message per batch, not one per changed name: a batch has to be atomic on
    %% the wire.  When it was N messages sharing a version, the first to arrive
    %% advanced the receiver's applied_version to that version and the rest matched
    %% the "already mid-batch" clause — so a member that received a *strict subset*
    %% (a link dropping mid-batch) ended up reporting the full version while holding
    %% only part of it.  Nothing could detect that afterwards: gap detection compares
    %% versions, and the versions matched.  It also broke the "same version implies
    %% same content" property the freshest-wins handoff gather relies on (§5.7), so a
    %% handoff could adopt the deficient replica and fan it out.  Delivered whole or
    %% not at all, a lost batch always leaves a version discontinuity, which is
    %% exactly what apply_bcast/6 already knows how to repair.
    broadcast_batch(Members, lists:reverse(BcastsRev), PrevVersion, Version, Self),
    Now = erlang:system_time(millisecond),
    %% Trail entries are {Name, Pid} pairs (§5.6 — see the recently_released doc).
    Rel1 = lists:foldl(fun(NamePid, Acc) -> Acc#{NamePid => Now} end, Rel0, Released),
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
    lists:foreach(
        fun({Origin, Result}) -> deliver_committed_reply(Origin, Result, Version) end, Immediate
    ),
    %% Enrich each direct `yes` with its {Name, Pid} (from the plan) so the replicate
    %% path can roll the binding back under strict_replication.
    EnrichedDirect = [
        {Origin, Name, Pid}
     || {Origin, yes} <- DirectYes, {Name, Pid} <- [maps:get(Origin, DirectMeta)]
    ],
    %% Fire presence notifications for this batch (§4.9): the replica now reflects the
    %% committed batch, so the watch-set deltas are computed against it and delivered to
    %% each subscription's notify targets.  Advances sub_matches.
    State2 = fire_presence(DBOp, State1),
    confirm_direct(EnrichedDirect, Version, State2).

%% Deliver presence notifications for a just-applied committed batch (leader only, §4.9)
%% and advance the leader-only `sub_matches` / `notify_matches`.  The batch's durable
%% delta `DBOp` names every registration that changed, and `State` already reflects the
%% post-batch replica (apply_dbop ran first).  For each subscription two things happen,
%% both driven by diffing the changed names:
%%   - watch side: a name entering/leaving the watch set yields a `joined`/`left` delta,
%%     delivered to the subscription's *continuing* notify targets;
%%   - notify side: a pid entering the notify set is a **new** viewer and is sent the
%%     full current watch snapshot, so a notifier that registered after the subscription
%%     still learns who is already present.
fire_presence(_DBOp, State = #state{subs = Subs}) when map_size(Subs) =:= 0 ->
    State;
fire_presence(DBOp, State = #state{names_tab = Tab, subs = Subs}) ->
    RealNames = maps:keys(DBOp),
    maps:fold(
        fun(SubId, {Watch, Notify}, S) ->
            diff_subscription(SubId, Watch, Notify, RealNames, Tab, S)
        end,
        State,
        Subs
    ).

%% Seed a subscription (on the elector's push): compute its full watch and notify sets
%% from the current replica, record them, and deliver the watch set as an initial
%% `joined` snapshot to the current notify targets.  A re-subscribe (upsert with changed
%% queries) reseeds and re-delivers.  (Notify targets that appear *later* are handled by
%% diff_subscription, not here.)
seed_subscription(
    SubId,
    State = #state{
        subs = Subs, names_tab = Tab, inv_index = Inv, sub_matches = SM, notify_matches = NM
    }
) ->
    case Subs of
        #{SubId := {Watch, Notify}} ->
            WatchMatches = query_match_names(Watch, Tab, Inv),
            NotifyMatches = query_match_names(Notify, Tab, Inv),
            send_presence(SubId, snapshot_events(WatchMatches), pidset(NotifyMatches)),
            State#state{
                sub_matches = SM#{SubId => WatchMatches},
                notify_matches = NM#{SubId => NotifyMatches}
            };
        _ ->
            State
    end.

%% Diff one subscription against the batch's row changes and fire its notifications,
%% advancing both tracked sets.  The watch diff yields `joined`/`left` deltas; the notify
%% diff yields the current notify-target set.  A pid that newly *entered* the notify set
%% (a viewer that just appeared) gets the full current watch snapshot; every *continuing*
%% notify target gets the watch delta.  Computed on pid-sets, so a pid already a target
%% under another name is not treated as new (no duplicate snapshot).
diff_subscription(SubId, Watch, Notify, RealNames, Tab, State) ->
    #state{sub_matches = SM, notify_matches = NM} = State,
    {WatchEvents, WatchAfter} = diff_query_matches(Watch, RealNames, maps:get(SubId, SM, #{}), Tab),
    NotifyBefore = maps:get(SubId, NM, #{}),
    {_NotifyEvents, NotifyAfter} = diff_query_matches(Notify, RealNames, NotifyBefore, Tab),
    AfterPids = pidset(NotifyAfter),
    NewTargets = AfterPids -- pidset(NotifyBefore),
    ContinuingTargets = AfterPids -- NewTargets,
    send_presence(SubId, WatchEvents, ContinuingTargets),
    send_presence(SubId, snapshot_events(WatchAfter), NewTargets),
    State#state{sub_matches = SM#{SubId => WatchAfter}, notify_matches = NM#{SubId => NotifyAfter}}.

%% Diff a query's membership over the batch's changed names: fold each changed name's
%% post-batch match (present in the replica *and* satisfying the query) against whether it
%% was a member before, yielding `joined`/`left` events and the updated `#{Name => Pid}`
%% membership.  Used for both the watch and notify queries.
diff_query_matches(Query, RealNames, Before, Tab) ->
    lists:foldl(
        fun(Name, {EvAcc, MatchAcc}) ->
            WasPid = maps:get(Name, Before, undefined),
            NowMatch =
                case ets:lookup(Tab, Name) of
                    [{_N, RowPid, Index, _Data} | _] ->
                        case query_satisfies(Query, Index) of
                            true -> {true, RowPid};
                            false -> false
                        end;
                    [] ->
                        false
                end,
            case {WasPid, NowMatch} of
                {undefined, {true, Pid}} ->
                    {[{joined, Name, Pid} | EvAcc], MatchAcc#{Name => Pid}};
                {undefined, false} ->
                    {EvAcc, MatchAcc};
                {WasPid, {true, WasPid}} ->
                    {EvAcc, MatchAcc};
                {OldPid, {true, NewPid}} ->
                    {[{joined, Name, NewPid}, {left, Name, OldPid} | EvAcc], MatchAcc#{
                        Name => NewPid
                    }};
                {OldPid, false} ->
                    {[{left, Name, OldPid} | EvAcc], maps:remove(Name, MatchAcc)}
            end
        end,
        {[], Before},
        RealNames
    ).

%% The current watch set (`#{Name => Pid}`) as a list of `joined` events — the initial
%% snapshot payload.
snapshot_events(Matches) ->
    [{joined, Name, Pid} || {Name, Pid} <- maps:to_list(Matches)].

%% The distinct pids of a `#{Name => Pid}` membership map (a pid registered under several
%% matching names is one recipient).
pidset(Matches) ->
    lists:usort(maps:values(Matches)).

%% Send one `{dgen_presence, SubId, Events}` message to each recipient pid.  Keyed by the
%% subscription's application-supplied id, so a durable subscriber re-addresses its feed
%% by the same stable key across restarts.  A no-op with no events or no recipients.
send_presence(_SubId, [], _Pids) ->
    ok;
send_presence(_SubId, _Events, []) ->
    ok;
send_presence(SubId, Events, Pids) ->
    Msg = {dgen_presence, SubId, Events},
    lists:foreach(fun(Pid) -> Pid ! Msg end, Pids).

is_direct_yes({{local, _From}, yes}) -> true;
is_direct_yes(_Other) -> false.

%% Replicate-before-ack for direct registrations (§5.5, §8).  Waits for
%% `register_replicas` *distinct* follower acks (bounded by the follower count) before
%% acking `yes`.  With no follower to replicate to (or replicas disabled) there is no
%% second holder to wait for — nothing survives the sole node anyway — so ack now.
%% Otherwise broadcast a replicate_sync and resolve on the acks or, on timeout, the
%% `strict_replication` policy (degrade-open or fail-closed).  `Direct` entries are
%% `{Origin, Name, Pid}`.
confirm_direct([], _Version, State) ->
    State;
confirm_direct(
    Direct,
    Version,
    State = #state{members = Members, member_id = Self, pending_acks = PA, config = Config}
) ->
    Needed = min(dgen_config:register_replicas(Config), map_size(Members)),
    case Needed =< 0 of
        true ->
            deliver_direct(Direct),
            State;
        false ->
            BatchRef = make_ref(),
            %% The sync carries the batch's commit version so a follower only acks
            %% once it has actually applied up to this batch — a follower that
            %% gap-skipped the broadcasts (awaiting resync) does not hold the
            %% bindings and must not be counted as a holder.
            broadcast_to_peers(Members, {replicate_sync, BatchRef, Self, Version}),
            TimerRef = arm(
                dgen_config:replicate_timeout(Config), {replicate_timeout, BatchRef}
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
%%
%% Whether it is available is resolved **once** and cached.  `code:ensure_loaded/1`
%% short-circuits on an already-loaded module, but not on a missing one — it falls
%% through to a synchronous call to `code_server`.  So the configuration that pays
%% for this check is exactly the common one (no telemetry), on every event, forever.
%%
%% Found by the eta framework rather than by profiling, and the *scheduling* cost is
%% what made it visible: `code_server` is a process the simulation does not control,
%% so a member emitting an event blocked on something outside the schedule and the
%% run stopped being reproducible.  Degrade-open only fires when replicas cannot ack,
%% which is why it appeared under injected message loss and nowhere else.
%%
%% Resolved once per VM, in the same spirit as `dgen_config`'s backend lookup: a
%% `telemetry` loaded *after* the first event is not picked up.  It is an application
%% dependency, present or absent for the life of the node.
-define(TELEMETRY_KEY, {?MODULE, telemetry_available}).

emit_telemetry(Event, Measurements, Metadata) ->
    case telemetry_available() of
        true -> apply(telemetry, execute, [Event, Measurements, Metadata]);
        false -> ok
    end.

telemetry_available() ->
    case persistent_term:get(?TELEMETRY_KEY, undefined) of
        undefined ->
            Available = code:ensure_loaded(telemetry) =:= {module, telemetry},
            persistent_term:put(?TELEMETRY_KEY, Available),
            Available;
        Available ->
            Available
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
%% A rejected batch's removes are salvaged and re-driven (salvage_failed_plan), so
%% `ok` remains the truthful answer for an unregister even when its batch failed.
reject_value({unreg, _From}) -> ok;
reject_value({forward_unreg, _MemberId, _Ref}) -> ok;
reject_value(_Origin) -> no.

%% Answer a write op's origin.  A direct (local) registration, set_metadata, or
%% unregister is answered with gen_server:reply/2; a forwarded one with the matching
%% reply cast ({register_reply} / {set_meta_reply} / {unregister_reply}) to the
%% forwarding member, which then answers its own caller.
deliver_reply({local, From}, Result) ->
    gen_server:reply(From, Result);
deliver_reply({meta, From}, Result) ->
    gen_server:reply(From, Result);
deliver_reply({unreg, From}, Result) ->
    gen_server:reply(From, Result);
deliver_reply({forward, MemberId, Ref}, Result) ->
    cast_to_member(MemberId, {register_reply, Ref, Result});
deliver_reply({forward_meta, MemberId, Ref}, Result) ->
    cast_to_member(MemberId, {set_meta_reply, Ref, Result});
deliver_reply({forward_unreg, MemberId, Ref}, _Result) ->
    cast_to_member(MemberId, {unregister_reply, Ref}).

%% Post-commit reply delivery: a forwarded registration's reply is stamped with the
%% batch's commit version so the forwarding follower can version-guard its `yes` ack
%% (see the {register_reply, _, _, _} handler).  Every other origin (and every
%% rejection path, which has no commit version) uses the plain deliver_reply/2.
deliver_committed_reply({forward, MemberId, Ref}, Result, Version) ->
    cast_to_member(MemberId, {register_reply, Ref, Result, Version});
deliver_committed_reply(Origin, Result, _Version) ->
    deliver_reply(Origin, Result).

%% Follower: the leader these were forwarded to is no longer current.  Reject the
%% register and set_metadata forwards so their callers retry against the new leader
%% (registrations `no`, including the gap-deferred `yes` acks whose resync will never
%% arrive from the deposed leader; set_metadata `{error, no_leader}`).  Unregister
%% forwards are **kept** — their intent is idempotent and must not be lost, so
%% redrive_unregs re-drives them to the new leader rather than failing the caller.
reject_forwards(State = #state{pending_forwards = PF, deferred_yes = Deferred}) ->
    Kept = maps:fold(
        fun
            (_Ref, {register, _Name, _Pid, _Meta, From}, Acc) ->
                gen_server:reply(From, no),
                Acc;
            (_Ref, {set_meta, From}, Acc) ->
                gen_server:reply(From, {error, no_leader}),
                Acc;
            (Ref, {unregister, _, _, _} = Unreg, Acc) ->
                Acc#{Ref => Unreg}
        end,
        #{},
        PF
    ),
    lists:foreach(fun({_Version, From}) -> gen_server:reply(From, no) end, Deferred),
    State#state{pending_forwards = Kept, deferred_yes = []}.

%% Resolve the leader's answer to a forwarded registration (see the
%% {register_reply, _, _, _} handler doc).  `yes` at-or-behind our applied version:
%% the FIFO-ordered broadcast already applied the binding, so just answer — the
%% normal path.  `yes` ahead of it: our replica is gapped (the broadcast was refused
%% pending a resync), so defer the ack until applied_version reaches the batch's
%% version — answering now would create a second holder the freshest-wins gather
%% cannot see (§5.5/§5.7).  The gap-refused broadcast already requested the resync;
%% re-request here as a belt-and-braces (request_resync no-ops while one is
%% outstanding).
%%
%% **The at-or-behind branch must not write the row.**  It used to `row_insert` the
%% binding, on the reasoning that the insert is idempotent over a broadcast that has
%% already applied it.  It is not: a group commit may bind *and clear* one name — a
%% register and an unregister of it landing in the same batch — and the broadcast
%% carries both ops in order, so the replica correctly ends up without the binding.
%% Re-inserting it here resurrects exactly what the batch removed, and the version
%% guard makes that fire precisely when the batch has been applied.  The member is
%% then holding a binding no other member has, at the same applied_version as all of
%% them, which is the one divergence gap detection cannot see (§4.5) and the one the
%% freshest-wins gather may fan out (§5.7).
%%
%% Passing the guard means the replica has applied this batch (or a snapshot at or
%% past its version), and that content is authoritative — including a later op in the
%% same batch.  `flush_deferred/1` already answers on exactly that reasoning, and this
%% branch is now consistent with it.  Found by `eta_run` on an unfaulted 3-member
%% cluster; see the regression test in dgen_registry_sim_test.exs.
handle_register_reply(Ref, Result, Version, State = #state{pending_forwards = PF}) ->
    case maps:take(Ref, PF) of
        {{register, LogicalName, Pid, {Index, Data}, From}, PF1} ->
            State1 = State#state{pending_forwards = PF1},
            case Result of
                yes when Version =:= legacy ->
                    %% A pre-version leader (rolling upgrade): there is no version to
                    %% guard on, and its per-name broadcasts are a wire format this
                    %% member no longer applies, so this insert is the only thing that
                    %% would bind the row.  Left exactly as it was.
                    gen_server:reply(From, yes),
                    row_insert(State1, LogicalName, Pid, Index, Data);
                yes when Version =< State1#state.applied_version ->
                    gen_server:reply(From, yes),
                    State1;
                yes ->
                    State2 =
                        case State1#state.leader of
                            undefined -> State1;
                            Leader -> request_resync(Leader, State1)
                        end,
                    State2#state{
                        deferred_yes = [{Version, From} | State2#state.deferred_yes]
                    };
                no ->
                    gen_server:reply(From, no),
                    State1
            end;
        _ ->
            %% Unknown Ref — already answered (e.g. rejected on a leadership change).
            State
    end.

%% Release gap-deferred forwarded-`yes` acks whose batch version the replica has now
%% genuinely applied (the resync snapshot, or a contiguous broadcast run, advanced
%% applied_version past them).  The binding itself is guaranteed present: reaching
%% the version contiguously means the batch's own broadcast was applied, and a
%% snapshot at-or-past it carries the committed binding.
flush_deferred(State = #state{deferred_yes = []}) ->
    State;
flush_deferred(State = #state{deferred_yes = Deferred, applied_version = Applied}) ->
    {Due, Rest} = lists:partition(fun({Version, _From}) -> Version =< Applied end, Deferred),
    lists:foreach(fun({_Version, From}) -> gen_server:reply(From, yes) end, Due),
    State#state{deferred_yes = Rest}.

%% Stash an unregister accepted while no leader was reachable (nothing to retract if
%% the name was locally unbound — matching the old nets-to-nothing semantics); it is
%% re-driven by redrive_unregs/1 once a leader is known.
stash_pending_unreg(_Name, undefined, State) ->
    State;
stash_pending_unreg(Name, Pid, State = #state{pending_unregs = Pending}) ->
    State#state{pending_unregs = [{Name, Pid} | Pending]}.

%% Re-drive every stashed unregister — the follower-side counterpart of the leader's
%% salvage_failed_plan: forwarded removals whose reply never came (dropped cast,
%% deposed leader) and removals accepted while no leader was reachable are handed to
%% the current leader as **pid-guarded retracts** (or enqueued directly if this
%% member is now the leader itself), and any still-waiting caller is answered `ok`.
%% Pid-guarding makes the re-drive idempotent and reissue-safe: a name since
%% re-registered to a different pid is never clobbered, and a removal the old leader
%% did commit after all nets to nothing.  Called on every snapshot apply and on
%% assuming leadership — the events that establish who the leader is.
redrive_unregs(State = #state{pending_forwards = PF, pending_unregs = Pending}) ->
    {Unregs, RestPF} = take_unreg_forwards(PF),
    case Unregs =:= [] andalso Pending =:= [] of
        true ->
            State;
        false ->
            redrive_unregs(State, Unregs, RestPF)
    end.

%% No leader to hand these to yet — leave everything stashed (RestPF/Pending
%% untouched) for the next snapshot/leadership event.
redrive_unregs(State = #state{leader = undefined}, _Unregs, _RestPF) ->
    State;
redrive_unregs(State = #state{leader = Leader, member_id = Self}, Unregs, RestPF) ->
    Entries =
        Unregs ++ [{Name, Pid, undefined} || {Name, Pid} <- State#state.pending_unregs],
    State1 = State#state{pending_forwards = RestPF, pending_unregs = []},
    lists:foldl(
        fun({Name, Pid, From}, S) ->
            From =/= undefined andalso gen_server:reply(From, ok),
            case is_pid(Pid) of
                false ->
                    %% Nothing was bound locally at call time — nothing to retract.
                    S;
                true when Leader =:= Self ->
                    enqueue_op({retract, Name, Pid}, S);
                true ->
                    forward_retract(Leader, Name, Pid),
                    S
            end
        end,
        State1,
        Entries
    ).

%% Split `pending_forwards` into its unregister entries (as {Name, Pid, From} for the
%% re-drive) and the remaining register/set_meta forwards (kept in flight — a
%% snapshot refresh that did not change leadership must not disturb them).
take_unreg_forwards(PF) ->
    maps:fold(
        fun
            (_Ref, {unregister, Name, Pid, From}, {Acc, Rest}) ->
                {[{Name, Pid, From} | Acc], Rest};
            (Ref, Other, {Acc, Rest}) ->
                {Acc, Rest#{Ref => Other}}
        end,
        {[], #{}},
        PF
    ).

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
        direct_meta => #{},
        %% The epoch the plan was built under: the commit-result handler refuses to
        %% apply a plan whose epoch is no longer current (leadership moved while the
        %% worker was in flight), and the broadcasts are stamped with it.
        epoch => Epoch,
        %% The raw ops, kept so a failed batch's destructive ops (removes/retracts/
        %% downs) can be salvaged (salvage_failed_plan) instead of silently dropped.
        ops => Ops
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
        Taken when is_pid(Taken), Taken =/= Pid ->
            %% Held by a *different* live pid — reject (the singleton contract).
            Acc#{replies := [{Origin, no} | Rs]};
        _Free_or_same ->
            %% Free (`undefined`) or already this same pid: bind it and answer `yes`.
            %% Re-registering the *same* pid under the *same* name is an idempotent
            %% success, not a conflict — so a caller whose register call timed out (or
            %% raced a failover) but whose registration actually committed can simply
            %% redrive and get a decisive `yes` for its own binding, rather than a
            %% confusing `no`.  It rides the normal bind path (a same-pid re-register
            %% re-writes the identical row, refreshes the monitor, and re-applies the
            %% call's metadata — use set_metadata/2 to change metadata independently).
            %%
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
            }
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
plan_op({remove, Name, ReleasedPid, Origin}, Epoch, Acc) ->
    #{
        tab := Tab,
        names := WN,
        wntr := WNTR,
        dbop := DB,
        replies := Rs,
        bcasts := Bs,
        released := Rel
    } = Acc,
    %% Clear durably if the name was bound at enqueue (ReleasedPid is a pid) or is bound
    %% now, as of this point in the batch (seed_lookup — overlay first, ETS fallback).
    %% ReleasedPid covers the leader's optimistic removal (already gone from ETS); the
    %% seed_lookup covers a name added earlier in this same batch (ETS would not show
    %% that yet — only unregister's own optimistic delete updates ETS immediately).
    %% Record the released {Name, Pid} in the conflict-detector trail so a lagging
    %% member reporting it is not later mistaken for a divergence.  The origin (a
    %% tracked unregister call, if any) is answered `ok` either way — clearing an
    %% already-unbound name is an idempotent no-op, not a failure.
    Rs1 = unreg_reply(Origin, Rs),
    WasBound = is_pid(ReleasedPid),
    Current = seed_lookup(WN, Tab, Name),
    %% Guard the clear against the *current* holder.  ReleasedPid is captured at
    %% arrival, but this plan runs later — and the arrival-time optimistic
    %% row_delete frees the name in ETS immediately, so a register that was already
    %% enqueued (parked behind an in-flight commit) plans against the freed table,
    %% answers `yes`, and binds a new pid before this remove is planned.  Clearing
    %% unconditionally then deletes that new holder: one unregister frees the name
    %% twice — once optimistically for the parked register, once durably against
    %% the pid it never targeted — and three `yes` acks for one name with a single
    %% unregister between them has no legal serialization (Guarantee 1).  Found by
    %% the DST harness's end-of-run ack-history fold (`check_final`, seed 5 under
    %% loss); invisible to every replica comparison because all members apply the
    %% same wrong batch and agree.
    %%
    %% So: a removal whose captured target is a pid only clears while the name's
    %% current holder (batch overlay first, ETS fallback) is that pid or nobody
    %% (nobody = the optimistic delete, which is this removal's own footprint).  A
    %% different current pid means the target binding is already gone — answer the
    %% idempotent `ok` and keep the new holder.  A capture of `undefined` keeps its
    %% existing meaning: the unregister serializes after whatever bound the name
    %% (in-batch add included), which is a legal linearization of "unregister by
    %% name", and clears it.
    Rebound = WasBound andalso is_pid(Current) andalso Current =/= ReleasedPid,
    case (WasBound orelse Current =/= undefined) andalso not Rebound of
        true ->
            Rel1 =
                case WasBound of
                    true -> [{Name, ReleasedPid} | Rel];
                    false -> Rel
                end,
            %% The broadcast carries the released pid so every member records it in
            %% its own copy of the conflict-detector trail (§5.6).
            ReleasedOut =
                case WasBound of
                    true -> ReleasedPid;
                    false -> undefined
                end,
            Acc#{
                names := WN#{Name => removed},
                wntr := maps:remove(Name, WNTR),
                dbop := DB#{Name => clear},
                replies := Rs1,
                bcasts := [{name_unregistered, Name, ReleasedOut, Epoch} | Bs],
                released := Rel1
            };
        false ->
            Acc#{replies := Rs1}
    end;
plan_op({retract, Name, Pid}, Epoch, Acc) ->
    #{tab := Tab, names := WN, wntr := WNTR, dbop := DB, bcasts := Bs, released := Rel} = Acc,
    %% Pid-guarded retract (strict_replication fail-closed): clear the binding only if
    %% it is *still* this pid's, so a name re-registered to someone else since the
    %% failed reg is not clobbered.  Trail the retracted {Name, Pid} so a lagging
    %% follower that still holds the transient binding is not later mistaken for a
    %% conflict (§5.6).
    case seed_lookup(WN, Tab, Name) of
        Pid ->
            Acc#{
                names := WN#{Name => removed},
                wntr := maps:remove(Name, WNTR),
                dbop := DB#{Name => clear},
                bcasts := [{name_unregistered, Name, Pid, Epoch} | Bs],
                released := [{Name, Pid} | Rel]
            };
        _ ->
            %% Binding changed since the failed registration — nothing to retract.
            Acc
    end;
plan_op({down, Name, Ref}, Epoch, Acc) ->
    #{names := WN, wntr := WNTR, dbop := DB, bcasts := Bs} = Acc,
    case maps:get(Name, WNTR, undefined) of
        Ref ->
            %% Ref still matches the live binding — honour the auto-unregister.  The
            %% pid is dead, so no released pid rides the broadcast (the trail is for
            %% *live* releases only).
            Acc#{
                names := WN#{Name => removed},
                wntr := maps:remove(Name, WNTR),
                dbop := DB#{Name => clear},
                bcasts := [{name_unregistered, Name, undefined, Epoch} | Bs]
            };
        _ ->
            %% Stale DOWN (name re-registered, or already removed) — ignore.
            Acc
    end.

%% Append an `ok` reply for a tracked unregister's origin; a legacy/salvaged remove
%% (origin `undefined`) has no caller to answer.
unreg_reply(undefined, Rs) -> Rs;
unreg_reply(Origin, Rs) -> [{Origin, ok} | Rs].

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

%% Apply one op of a replicated batch to the local replica.  Called only from the
%% batch handler, under apply_bcast/6's contiguity guard, so the whole batch lands
%% or none of it does.
apply_bcast_op({name_registered, Name, Pid, Index, Data}, S) ->
    row_insert(S, Name, Pid, Index, Data);
%% Update the row's Index and Data, keeping its pid (a no-op if the row is absent —
%% e.g. this member missed the registration; the next handoff reconciles it).
apply_bcast_op({metadata_set, Name, Index, Data}, S) ->
    row_update_meta(S, Name, Index, Data);
%% ReleasedPid is the live pid this unregister explicitly released (`undefined` for
%% a death-driven cleanup).  Recording it — keyed by {Name, Pid}, so a release under
%% one name never masks a conflict on another name the same pid holds — keeps every
%% member's copy of the conflict-detector trail (§5.6) in step, so the trail
%% survives leadership changes via the handoff gather merge.
apply_bcast_op({name_unregistered, Name, ReleasedPid}, S0) ->
    S1 = row_delete(S0, Name),
    case is_pid(ReleasedPid) of
        true ->
            Rel = S1#state.recently_released,
            S1#state{
                recently_released = Rel#{
                    {Name, ReleasedPid} => erlang:system_time(millisecond)
                }
            };
        false ->
            S1
    end.

%% Broadcast a committed batch to every peer as a single `{names_batch, Ops, Epoch,
%% PrevV, Version, LeaderId}` message: the batch's ops in commit order, the epoch it
%% was planned under, the predecessor version, this batch's commit version, and the
%% sending leader's id.  Followers use the version pair to apply only *contiguous*
%% batches (gap detection — see apply_bcast/6) and the leader id as the resync
%% target when a gap is observed.
%%
%% Every op in a batch is planned under one epoch, so it is carried once on the
%% envelope rather than repeated on each op — which matters for a batch of
%% `commit_batch_size` (5000 by default) ops.  An empty batch sends nothing.
-ifdef(MUTATION_PARTIAL_BATCH).
%% MUTATION — see the note above apply_bcast/6.  One message per op, every one
%% stamped with the same {PrevV, Version}: the pre-fix wire format, reintroduced so
%% the eta framework can be asked to rediscover the divergence it produces.
broadcast_batch(_Members, [], _PrevV, _Version, _Self) ->
    ok;
broadcast_batch(Members, Ops, PrevV, Version, Self) ->
    lists:foreach(
        fun(Op) ->
            broadcast_to_peers(
                Members,
                {names_batch, [strip_bcast_epoch(Op)], bcast_epoch(Op), PrevV, Version, Self}
            )
        end,
        Ops
    ).
-else.
broadcast_batch(_Members, [], _PrevV, _Version, _Self) ->
    ok;
broadcast_batch(Members, [First | _] = Ops, PrevV, Version, Self) ->
    Epoch = bcast_epoch(First),
    Stripped = [strip_bcast_epoch(Op) || Op <- Ops],
    broadcast_to_peers(Members, {names_batch, Stripped, Epoch, PrevV, Version, Self}).
-endif.

-ifdef(MUTATION_QUIET_RESYNC).
%% MUTATION (`DGEN_MUTATION=quiet_resync`, test builds only) — the heartbeat
%% reverted to the pre-fix shape: the leader's timer fires and advertises
%% nothing, so gap detection is traffic-triggered again and a follower that
%% loses the *tail* of the stream stays diverged for as long as the cluster is
%% quiet (sim README, finding 2).  Planted so the mutation suite can be asked to
%% rediscover it; see test/dgen_registry_mutation_quiet_test.exs.
broadcast_heartbeat(_State) ->
    ok.
-else.
%% The leader's periodic empty batch, stamped {Applied, Applied}: a caught-up
%% follower applies nothing, a behind one takes apply_bcast/6's gap branch and
%% requests a resync.  The traffic-independent half of gap detection.
broadcast_heartbeat(#state{
    members = Members, epoch = Epoch, applied_version = Applied, member_id = Self
}) ->
    broadcast_to_peers(Members, {names_batch, [], Epoch, Applied, Applied, Self}),
    ok.
-endif.

bcast_epoch({name_registered, _Name, _Pid, _Index, _Data, Epoch}) -> Epoch;
bcast_epoch({metadata_set, _Name, _Index, _Data, Epoch}) -> Epoch;
bcast_epoch({name_unregistered, _Name, _ReleasedPid, Epoch}) -> Epoch.

strip_bcast_epoch({name_registered, Name, Pid, Index, Data, _Epoch}) ->
    {name_registered, Name, Pid, Index, Data};
strip_bcast_epoch({metadata_set, Name, Index, Data, _Epoch}) ->
    {metadata_set, Name, Index, Data};
strip_bcast_epoch({name_unregistered, Name, ReleasedPid, _Epoch}) ->
    {name_unregistered, Name, ReleasedPid}.

%% Apply a replication broadcast if — and only if — it is contiguous with this
%% member's replica, so the replica always remains a prefix of the leader's totally
%% ordered stream (what makes freshest-wins reconstruction sound, §5.7):
%%
%%   - An older epoch's broadcast (a deposed leader's) is dropped.
%%   - `PrevV =:= applied_version`: the next batch in sequence — apply it whole.
%%   - `V =< applied_version`: at or behind our baseline — a duplicate batch, or one
%%     already superseded by a snapshot; drop.
%%   - Otherwise there is a **gap**: we missed at least one batch (a cast dropped
%%     while we were briefly disconnected, a message lost with a dying connection).
%%     Do not apply and do not advance; request a full snapshot from the sender
%%     instead.  Guarded by resync_timer so a burst of gapped batches asks once.
%%
%% A batch arrives as one message (broadcast_batch/5), so `V =:= applied_version` is
%% now purely the duplicate case and folds into `V =< applied_version`.  It used to
%% also mean "another message of the batch we are already applying", which is what
%% made a partially-delivered batch undetectable — the receiver had already advanced
%% to V on the batch's first message, so the loss of any later one left no version
%% discontinuity to notice.
%%
%% ## The mutation
%%
%% `-DMUTATION_PARTIAL_BATCH` puts both halves of that back: `broadcast_batch/5`
%% sends one message per op sharing a version, and the clause below re-admits
%% "another message of the batch we are at".  It exists so the eta framework can be
%% required to *rediscover* a divergence we already understand, from a cold start and
%% with no test written for it — acceptance criterion 1 in
%% the `eta` library's docs/design.md.  Test builds only, off unless the
%% `DGEN_MUTATION` environment variable asks for it, and never in a release: see
%% `erlc_options/1` in mix.exs.
-ifdef(MUTATION_PARTIAL_BATCH).
apply_bcast(Epoch, PrevV, V, LeaderId, ApplyFun, State) ->
    #state{epoch = CurrentEpoch, applied_version = Applied} = State,
    if
        Epoch < CurrentEpoch -> State;
        PrevV =:= Applied -> bump_applied(V, ApplyFun(cancel_resync(State)));
        %% MUTATION: "another message of the batch we are already applying" — apply
        %% it without advancing, because we are already at V.
        V =:= Applied -> ApplyFun(State);
        V =< Applied -> State;
        true -> request_resync(LeaderId, State)
    end.
-else.
apply_bcast(Epoch, PrevV, V, LeaderId, ApplyFun, State) ->
    #state{epoch = CurrentEpoch, applied_version = Applied} = State,
    if
        Epoch < CurrentEpoch -> State;
        PrevV =:= Applied -> bump_applied(V, ApplyFun(cancel_resync(State)));
        V =< Applied -> State;
        true -> request_resync(LeaderId, State)
    end.
-endif.

%% Ask `LeaderId` for a full snapshot (we observed a gap in its stream), at most
%% once per ?RESYNC_RETRY window.  The reply is a regular {apply_names_snapshot}.
request_resync(LeaderId, State = #state{resync_timer = undefined, member_id = Self}) ->
    cast_to_member(LeaderId, {resync_req, Self}),
    Ref = arm(?RESYNC_RETRY, resync_timeout),
    State#state{resync_timer = Ref};
request_resync(_LeaderId, State) ->
    State.

cancel_resync(State = #state{resync_timer = undefined}) ->
    State;
cancel_resync(State = #state{resync_timer = Ref}) ->
    erlang:cancel_timer(Ref),
    State#state{resync_timer = undefined}.

%% Advance applied_version to the highest version applied (within an epoch a leader's
%% broadcasts carry non-decreasing versions, and apply_bcast only lets contiguous
%% ones through, but max/2 keeps this robust regardless).  Advancing may release
%% gap-deferred forwarded-`yes` acks (flush_deferred) — a no-op when none are held.
bump_applied(Version, State = #state{applied_version = Applied}) ->
    flush_deferred(State#state{applied_version = max(Applied, Version)}).

%% Record that this member now holds synced registry state, announcing the
%% transition to the elector once (`fresh = false`) so the membership record's
%% freshness converges — see the `synced` field doc and the elector's member_info.
mark_synced(State = #state{synced = true}) ->
    State;
mark_synced(State = #state{elector = Elector, member_id = Self, join_token = Token}) ->
    dgen_server:cast(Elector, {join, Self, Token, false}),
    State#state{synced = true}.

%% Is a peer member's node currently connected (so a cast to it will not be
%% dropped and a forward will actually be answered)?
member_reachable({Node, _Name}) ->
    dgen_utils:node_reachable(Node).

%% Report a peer's death to the elector, fencing the `{member_down}` with the token the
%% elector currently holds for that member (read fresh from durable state via a priority
%% read that bypasses the queue), falling back to `Cached` if that read fails.  Runs in a
%% short-lived helper so the member loop never blocks on a call to the elector — see the
%% DOWN handler for the deadlock this avoids and why the fresh read matters.
spawn_member_down(Elector, MemberId, Cached) ->
    _ = spawn(fun() ->
        Token = dgen_registry_elector:member_token(Elector, MemberId, Cached),
        dgen_server:cast(Elector, {member_down, MemberId, Token})
    end),
    ok.

%% Answer a consistent read only after verifying, against the durable leader key,
%% that this member really is the fenced leader for its current epoch (Guarantee 5:
%% consistent reads are authoritative, never stale).  Without this, a deposed
%% leader that has not yet heard about the handoff — the minority side of a
%% partition believes `leader = self` until a snapshot it may not receive — would
%% serve its frozen replica as authoritative.  Fencing writes alone cannot prevent
%% that; reads need their own fence.
%%
%% `Value` is captured in the handler, at the member's mailbox point; the verify
%% runs in a short-lived helper so the DB round-trip never blocks the member's
%% loop.  This is linearizable because leadership is continuous within an epoch:
%% if `{Self, Epoch}` still holds at verify time, no other leader existed between
%% the capture and the verify, so the captured value was (and still is at the
%% verification point) the authoritative answer.  Any verification failure — a
%% mismatch, or an unreachable backend — answers `Denied` (the CP refusal), the
%% same value an unreachable-leader forward answers.
reply_fenced(From, Value, Denied, #state{
    tenant = Tenant, tuid = Tuid, member_id = Self, epoch = Epoch
}) ->
    _ = spawn(fun() ->
        Reply =
            case dgen_registry_names:verify_leader(Tenant, Tuid, Self, Epoch) of
                true -> Value;
                false -> Denied
            end,
        gen_server:reply(From, Reply)
    end),
    ok.

%% Forward a pid-guarded retract to the (believed) current leader — used when a
%% durable clear this member owed (an unregister, or a strict-mode retraction)
%% cannot ride its own commit pipeline anymore.  Best-effort by necessity: with no
%% leader there is nowhere to send it, and a later handoff gather reconciles.
forward_retract(undefined, _Name, _Pid) ->
    ok;
forward_retract(Leader, Name, Pid) ->
    cast_to_member(Leader, {retract_req, Name, Pid}).

%% A batch failed to commit (fenced / backend error / worker death) or committed
%% but its plan is no longer applicable (leadership moved in flight).  Its
%% registrations were rejected — callers retry — but its destructive ops (removes,
%% retracts, downs) must be re-driven: dropping an unregister would leave this
%% member's optimistic delete diverged from every other replica until an unrelated
%% handoff, and dropping a retract would leave a `no`-answered binding alive.
%% Scheduled after a short delay so a persistently failing backend does not spin.
salvage_failed_plan(State, #{ops := Ops}) ->
    Destructive = [
        strip_remove_origin(Op)
     || Op <- Ops,
        element(1, Op) =:= remove orelse element(1, Op) =:= retract orelse
            element(1, Op) =:= down
    ],
    case Destructive of
        [] -> ok;
        _ -> arm(?REQUEUE_DELAY, {requeue_ops, Destructive})
    end,
    State.

%% Arm a timer, and say so.
%%
%% Under simulation `eta_transform` points the `erlang:send_after/3` below at
%% `eta_time`, so every deadline here lands in the virtual clock's wheel and the
%% driver records reaching it as a `{clock, Ms}` trace entry. Those entries are
%% otherwise anonymous: a run that advances to 6500 tells you a timer was due and
%% nothing about whose it was. Logging the arm is what makes one attributable.
%%
%% `?ETA_LOG` is a no-op in a production build, where this is a plain
%% `send_after/3` with an extra function call around it.
arm(Delay, Msg) ->
    _ = ?ETA_LOG({arm, Msg, Delay}),
    erlang:send_after(Delay, self(), Msg).

%% A salvaged remove's caller was already answered by reject_plan (`ok` — see
%% reject_value); strip the origin so the re-driven op does not answer it twice.
strip_remove_origin({remove, Name, Pid, _Origin}) -> {remove, Name, Pid, undefined};
strip_remove_origin(Op) -> Op.

%% Losing leadership with direct registrations still awaiting replica acks: resolve
%% them now, by the same policy as a replicate timeout (§8).  Degrade-open answers
%% `yes` (the binding committed under our term and we hold it, so the handoff gather
%% carries it); fail-closed answers `no` and forwards the pid-guarded retract to the
%% new leader, so the caller's `no` cannot leave the binding alive.
fail_pending_acks(State = #state{pending_acks = PA}) when map_size(PA) =:= 0 ->
    State;
fail_pending_acks(State = #state{pending_acks = PA, leader = NewLeader, config = Config}) ->
    Strict = dgen_config:strict_replication(Config),
    maps:foreach(
        fun(_BatchRef, {Direct, TimerRef, Needed, Acked}) ->
            erlang:cancel_timer(TimerRef),
            case Strict of
                false ->
                    notify_degrade_open(Needed, map_size(Acked), length(Direct)),
                    deliver_direct(Direct);
                true ->
                    lists:foreach(
                        fun({Origin, Name, Pid}) ->
                            deliver_reply(Origin, no),
                            forward_retract(NewLeader, Name, Pid)
                        end,
                        Direct
                    )
            end
        end,
        PA
    ),
    State#state{pending_acks = #{}}.

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

%% Snapshot payload codec (see the "distribute" note above).  The records map is
%% shipped as a single `term_to_binary` blob rather than an on-heap list of tuples.
%% A large binary is a refc binary — stored off-heap and never scanned by GC — so a
%% 100k+-name snapshot no longer bloats the sender's or receiver's process heap, nor
%% thrashes their collectors while it sits in a mailbox; `[compressed]` also shrinks
%% the wire bytes (names/pids compress well).  Encoding once and casting the *same*
%% binary to every follower makes the fan-out share one blob instead of copying the
%% whole list per follower.  `decode_records/1` also accepts the legacy list form, so
%% a node running the old wire format during a rolling upgrade still applies.
-define(SNAPSHOT_ENCODE_OPTS, [compressed]).

encode_records(Records) when is_map(Records) ->
    term_to_binary(Records, ?SNAPSHOT_ENCODE_OPTS).

decode_records(Bin) when is_binary(Bin) -> binary_to_term(Bin);
%% Legacy wire forms during a rolling upgrade: apply_names_snapshot shipped a
%% `[{Name, Record}]` list, get_names_snapshot a bare records map.  Both normalise to
%% the map every caller wants.
decode_records(List) when is_list(List) -> maps:from_list(List);
decode_records(Map) when is_map(Map) -> Map.

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

%% ---------------------------------------------------------------------------
%% Tagged queries (§4.9) — the extensible query() form used by presence
%% subscriptions.  Only `{all, Constraints}` (AND-equal, the run_query semantics)
%% exists today; new kinds slot in here without touching call sites.
%% ---------------------------------------------------------------------------

%% Run a query against the replica, returning the full `#{name, pid, index, data}`
%% matches (the run_query shape).
query_run({all, Constraints}, Tab, Inv) ->
    run_query(Constraints, Tab, Inv).

%% Does `Index` (a row's indexed-attribute map) satisfy the query?
query_satisfies({all, Constraints}, Index) ->
    map_size(Constraints) > 0 andalso satisfies(Constraints, Index).

%% The watch-set membership `#{Name => Pid}` a query currently resolves to.
query_match_names(Query, Tab, Inv) ->
    maps:from_list([{N, P} || #{name := N, pid := P} <- query_run(Query, Tab, Inv)]).

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
            %% calling this, so pruning here is safe.  The departure timestamp keeps
            %% trail pruning suspended for a TTL window (§5.6): the departed member
            %% is no longer "a disconnected member", but its stale rows can still
            %% surface at a rejoin gather within that window.
            State#state{
                members = maps:remove(MemberId, Members),
                monitors = maps:remove(Ref, Monitors),
                peer_tokens = maps:remove(MemberId, State#state.peer_tokens),
                last_departure = erlang:system_time(millisecond)
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

%% Cast to a peer member.  A fire-and-forget cast to `{Name, Node}` on a
%% currently-disconnected node would trigger an automatic distribution reconnect
%% (the same hazard `dgen_registry_elector:call_to_member/2` guards against): if
%% it fires during a partition, both sides see `{nodeup, _}`, re-join with fresh
%% tokens, and the partition is healed before either side ever removed the other
%% — so the departure is never observed.  Drop the cast when the target node is
%% not already connected.
%%
%% Every inter-member protocol message goes through here: broadcasts, register and
%% set_metadata replies, replicate_sync/ack, resync requests, snapshots, retracts.
%%
%% That used to matter for a second reason.  A `persistent_term` hook sat on this
%% function so a simulation could own delivery, and this was the one place it
%% could — a seam this module maintained on the test harness's behalf.  It is
%% gone.  `eta_transform` rewrites the `gen_server:cast/2` below to
%% `eta_net:cast/2` under `-ifdef(DST)`, so a simulation interposes at the send
%% itself, and `eta_net` is inert unless a run installs a network — so a release
%% build is a plain cast with nothing in front of it, rather than a
%% persistent_term read of a key nobody ever sets.  See `test/support/sim/`.
cast_to_member(To, Msg) ->
    do_cast_to_member(To, Msg).

do_cast_to_member({Node, Name}, Msg) when Node =:= node() ->
    gen_server:cast({Name, Node}, Msg);
do_cast_to_member({Node, Name}, Msg) ->
    case lists:member(Node, nodes()) of
        true -> gen_server:cast({Name, Node}, Msg);
        false -> ok
    end.
