-module(dgen_registry_names).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Durable fence for `dgen_registry`'s leader-driven commit pipeline.
%%
%% Section references (e.g. `§4.4`) point to `docs/dgen_registry_design.md`, the
%% registry's design-and-guarantees document.
%%
%% Historically this module stored one **occupancy key per registered name**
%% (`{Tuid, <<"names">>, Name} -> {Epoch, PidNode}`) as the authoritative taken-set.
%% That has been collapsed to a **single key per registry**: a monotonic counter at
%% `{Tuid, <<"version">>}`, bumped once per committed batch with a conflict-free
%% atomic add. See §4.4 of `docs/dgen_registry_design.md`.
%%
%% The bump exists *solely* to turn the fenced commit into a write transaction so the
%% leader-key read-conflict is actually resolved by the FDB resolver (a transaction
%% with conflict ranges but no mutations commits trivially without consulting the
%% resolver). The registry's name→pid state lives entirely in the members' in-memory
%% maps, reconstructed on a handoff by the cross-member gather
%% (`dgen_registry_member:gather_maps/3`); the DB no longer stores name state.
%%
%% ## Key layout
%%
%% ```
%% {Tuid, <<"version">>} -> atomic counter (little-endian)   %% the only data key
%% {Tuid, <<"leader">>}  -> term_to_binary(MemberId)         %% the fence (elector-owned)
%% ```
%%
%% So a registry's *name table* holds ~2 DB keys for N processes, independent of N.
%% (The elector's membership/leader-election queue keeps its own small set of keys,
%% bounded by cluster size and churn — also independent of N. See §4.4.)
%%
%% ## Guarantee change: unconditional → single-fault uniqueness
%%
%% With a per-name occupancy set, the DB was the authoritative taken-set: a name could
%% never be re-issued while its key existed, regardless of how many members were lost
%% simultaneously. Collapsing to a single version key removes that backstop. Uniqueness
%% now rests on the in-memory maps and the replication invariants that keep every live
%% binding on at least two holders — the leader plus one follower (forwarded
%% registrations are two-holder by construction; a direct registration waits for a
%% replica ack before its caller is told `yes`; see `dgen_registry_member`).
%%
%% The registry therefore tolerates the loss of **any single member** without
%% re-issuing a live name, but **not** the simultaneous loss of both holders of a
%% binding (nor a degrade-open residual, where a direct ack proceeded on a replication
%% timeout). Closing that remaining gap is the job of the kill-both-on-conflict
%% termination backstop (§5.6 of the design doc) — not the DB.
%%
%% ## Fencing
%%
%% A commit is fenced against a concurrent leadership change on the leader key. Two
%% equivalent mechanisms are used, both safe:
%%
%% - **Read** (the first commit after assuming leadership; every retry): read the
%%   leader key inside the transaction and proceed only if it still names `Self`. The
%%   read adds a read-conflict, so a concurrent leadership change conflicts the commit;
%%   the retry re-reads, sees the new leader, and aborts with `fenced`.
%% - **Write-conflict only** (steady state, when the read version is pinned to a prior
%%   committed version): add a read-conflict on the leader key *without* reading it. A
%%   pinned version is necessarily a leader-era version (we only commit while leader),
%%   so any later leader-key write conflicts the commit — fencing with no storage read,
%%   a single-round-trip commit. The version bump makes this a write transaction, so
%%   the conflict is actually resolved. A retry falls back to a read, because its fresh
%%   GRV may already post-date the change, which a conflict range would miss.
%%
%% A stale leader therefore can never commit.

-behaviour(dgen_transaction).

-export([read_version/2, start_commit/4]).
%% dgen_transaction callbacks (the async, non-blocking, cached-GRV commit path).
-export([init/1, handle_begin/2, handle_retry/2, handle_committed/2]).

-if(?DOCATTRS).
-doc """
Reads the registry's durable version counter (the number of committed batches).

Returns a non-negative integer (`0` if no batch has committed yet). Used for
diagnostics and tests; the registry itself does not read this key — the commit
version returned by the transaction is what stamps the in-memory maps.
""".
-endif.
-spec read_version(dgen_backend:tenant(), dgen_server:tuid()) -> non_neg_integer().
read_version(Tenant = {_Handle, Dir}, Tuid) ->
    B = dgen_config:backend(),
    Key = version_db_key(Dir, Tuid),
    dgen_backend:transactional(Tenant, fun({Tx, _Dir}) ->
        case B:wait(B:get(Tx, Key)) of
            not_found -> 0;
            Bin -> binary:decode_unsigned(Bin, big)
        end
    end).

-if(?DOCATTRS).
-doc """
Starts an asynchronous, fenced batch commit in a `dgen_transaction` worker.

The commit's only write is a conflict-free atomic bump of the registry's version
key (see the module doc); its purpose is to fence the leader (a stale leader's
commit aborts) and to assign the batch a globally-monotonic commit version that the
caller uses to stamp its in-memory maps. It runs in its own process so the caller
(the registry leader) is not blocked while it is in flight, and the read version may
be pinned (`read_version`) to skip a GRV. The worker delivers
`{dgen_transaction, Ref, Reply}` to `owner` where `Reply` is `{committed, Version}`
on success, `{aborted, fenced}` if a newer leader has been elected, or
`{error, Reason}` on a non-retryable backend failure.

The worker is **monitored** (returns `{ok, {Pid, MRef}}`): the caller must handle
its `DOWN` as a commit failure, so a worker that dies without delivering a result
(e.g. it is killed) still resolves the in-flight commit rather than wedging it.

`Self` is the committing member's id (the fence subject). `Opts` is a map: `owner`
(pid, required), `ref` (correlation token, required), `read_version` (a prior
committed version to pin, or omitted/`undefined` for a fresh GRV). On a retryable
conflict the worker resets to a fresh GRV automatically.
""".
-endif.
-spec start_commit(
    dgen_backend:tenant(),
    dgen_server:tuid(),
    dgen_registry_elector:member_id(),
    #{owner := pid(), ref := term(), read_version => undefined | integer()}
) -> {ok, {pid(), reference()}} | {error, term()}.
start_commit({Db, Dir}, Tuid, Self, Opts) ->
    LeaderKey = dgen_registry_elector:leader_db_key(Dir, Tuid),
    VersionKey = version_db_key(Dir, Tuid),
    ReadVersion = maps:get(read_version, Opts, undefined),
    Args = #{
        self => Self,
        leader_key => LeaderKey,
        version_key => VersionKey,
        %% How to fence the first attempt (see the Fencing section): a pinned read
        %% version is leader-era, so a leader-key write-conflict fully fences with
        %% no read; a fresh GRV must read the leader key.
        fence => fence_mode(ReadVersion)
    },
    TxOpts =
        [
            {db, Db},
            {owner, maps:get(owner, Opts)},
            {ref, maps:get(ref, Opts)}
            | read_version_opt(ReadVersion)
        ],
    dgen_transaction:start_monitor(?MODULE, Args, TxOpts).

read_version_opt(undefined) -> [];
read_version_opt(Version) -> [{read_version, Version}].

fence_mode(undefined) -> read;
fence_mode(_Version) -> conflict.

%% ---------------------------------------------------------------------------
%% dgen_transaction callbacks
%% ---------------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

%% First attempt.  When the read version is pinned (leader-era), fence with a
%% write-conflict on the leader key *without* reading it — no storage read, so the
%% happy-path commit is a single round-trip: a leadership change wrote the leader
%% key after our read version, conflicting the commit (which then retries and
%% reads, below).  With a fresh GRV there is no such guarantee — the version may
%% already post-date an unheard-of leadership change a conflict range would miss —
%% so read the leader key instead.
handle_begin(Tx, State = #{fence := conflict, leader_key := LeaderKey}) ->
    B = dgen_config:backend(),
    B:add_read_conflict_key(Tx, LeaderKey),
    bump_version(B, Tx, State),
    {commit, State};
handle_begin(Tx, State = #{fence := read}) ->
    fenced_by_read(Tx, State).

%% A retry runs after on_error reset the read version to a fresh GRV, which may
%% post-date a leadership change — so always read the leader key to detect a new
%% leader and abort, rather than a conflict range that would miss it.
handle_retry(Tx, State) ->
    fenced_by_read(Tx, State).

handle_committed(Version, _State) ->
    {ok, Version}.

%% Read the leader key (the read adds the read-conflict that fences against a
%% concurrent leadership change) and bump the version only while still the leader.
fenced_by_read(Tx, State = #{self := Self, leader_key := LeaderKey}) ->
    B = dgen_config:backend(),
    case current_leader(B, Tx, LeaderKey) of
        Self ->
            bump_version(B, Tx, State),
            {commit, State};
        _Other ->
            {stop, fenced, State}
    end.

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

%% The single write of a fenced commit: a versionstamp on the version key so the
%% value monotonically advances with FDB's commit-version ordering.  This turns
%% the transaction into a write transaction (so the leader-key read-conflict is
%% resolved by the resolver) and advances the durable batch counter.
%% `set_versionstamped_value` is conflict-free on the value, so the version key
%% itself never causes a (retryable) conflict between successive leader commits.
bump_version(B, Tx, #{version_key := VersionKey}) ->
    B:set_versionstamped_value(Tx, VersionKey, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>).

current_leader(B, Tx, LeaderKey) ->
    case B:wait(B:get(Tx, LeaderKey)) of
        not_found -> undefined;
        Bin -> binary_to_term(Bin)
    end.

version_db_key(Dir, Tuid) ->
    B = dgen_config:backend(),
    B:dir_pack(Dir, dgen_key:extend(Tuid, <<"version">>)).
