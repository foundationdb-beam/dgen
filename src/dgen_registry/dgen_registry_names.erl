-module(dgen_registry_names).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Durable fence for the leader-driven commit pipeline (§4.4 of
%% docs/design/dgen_registry_design.md).  Name state lives only in members' memory; the
%% DB holds one version key per registry, versionstamp-bumped once per batch.  The bump
%% exists solely to make the commit a write transaction, so the resolver actually checks
%% the leader-key read-conflict.
%%
%% ```
%% {Tuid, <<"version">>} -> 10-byte versionstamp (big-endian commit version)
%% {Tuid, <<"leader">>}  -> term_to_binary({MemberId, Epoch})   (elector-owned fence)
%% ```
%%
%% Uniqueness therefore survives any single member loss but not loss of both holders of a
%% binding; that residual is covered by kill-both-on-conflict (§5.6), not the DB.
%%
%% Fencing on `{Self, Epoch}` (epoch prevents ABA).  Pinned read version (leader-era):
%% read-conflict on the leader key without reading it.  Fresh GRV or retry: read it,
%% since the GRV may post-date a leadership change a conflict range would miss.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-behaviour(dgen_transaction).

-export([read_version/2, read_committed_frontier/2, start_commit/4, verify_leader/4]).
-export([init/1, handle_begin/2, handle_retry/2, handle_committed/2]).

-if(?DOCATTRS).
-doc """
Reads the registry's durable version key — the versionstamp of the most recently
committed batch, decoded as a big-endian integer.

Returns a non-negative integer (`0` if no batch has committed yet). It is a
globally-monotonic FDB commit version, **not** a count of batches. Used for
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
Reads the durable version key as the **committed frontier on the same scale as a
member's `applied_version`** (`get_committed_version`).

The version key holds a 10-byte FDB versionstamp — an 8-byte commit version
followed by a 2-byte in-transaction batch order — so `read_version/2` returns the
commit version left-shifted 16 bits plus the batch. A member stamps its maps with
the bare 8-byte commit version, so the two are not directly comparable; this
shifts the batch bytes off to recover the commit version. Each registry commit is
its own transaction (a single versionstamp write), so the batch order is always 0,
but the shift is correct regardless.

Used by the leadership-handoff fence (`dgen_registry_member:gather_caught_up/6`):
a new leader whose gathered `MaxVersion` is behind this frontier has an incomplete
reconstruction and re-gathers before assuming — the fix for the handoff-gather
race (formal/DgenRegistryReplication.tla). Returns `0` if no batch has committed.
""".
-endif.
-spec read_committed_frontier(dgen_backend:tenant(), dgen_server:tuid()) -> non_neg_integer().
read_committed_frontier(Tenant, Tuid) ->
    read_version(Tenant, Tuid) bsr 16.

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

`Self` is the committing member's id and `epoch` its current leadership epoch —
together the fence subject `{Self, Epoch}`. `Opts` is a map: `owner` (pid,
required), `ref` (correlation token, required), `epoch` (the leader's epoch,
required), `read_version` (a prior committed version to pin, or
omitted/`undefined` for a fresh GRV). On a retryable conflict the worker resets
to a fresh GRV automatically.
""".
-endif.
-spec start_commit(
    dgen_backend:tenant(),
    dgen_server:tuid(),
    dgen_registry_elector:member_id(),
    #{
        owner := pid(),
        ref := term(),
        epoch := non_neg_integer(),
        read_version => undefined | integer()
    }
) -> {ok, {pid(), reference()}} | {error, term()}.
start_commit({Db, Dir}, Tuid, Self, Opts) ->
    LeaderKey = dgen_registry_elector:leader_db_key(Dir, Tuid),
    VersionKey = version_db_key(Dir, Tuid),
    ReadVersion = maps:get(read_version, Opts, undefined),
    Args = #{
        self => Self,
        epoch => maps:get(epoch, Opts),
        leader_key => LeaderKey,
        version_key => VersionKey,
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

-if(?DOCATTRS).
-doc """
Verifies, against the durable leader key, that `Self` is the fenced leader for
`Epoch` right now.

Used by the consistent-read path (`whereis_name_consistent/1` and friends): a
member that believes it is leader confirms that belief against the database
before answering, so a deposed leader that has not yet heard about the handoff
(e.g. it is on the minority side of a partition) cannot serve a stale answer as
authoritative (Guarantee 5 of the design doc). Any failure to verify — a
mismatch, a missing key, or an unreachable backend — returns `false` (the CP
refusal).
""".
-endif.
-spec verify_leader(
    dgen_backend:tenant(),
    dgen_server:tuid(),
    dgen_registry_elector:member_id(),
    non_neg_integer()
) -> boolean().
verify_leader(Tenant = {_Db, Dir}, Tuid, Self, Epoch) ->
    B = dgen_config:backend(),
    Key = dgen_registry_elector:leader_db_key(Dir, Tuid),
    try
        dgen_backend:transactional(Tenant, fun({Tx, _Dir}) ->
            case B:wait(B:get(Tx, Key)) of
                not_found -> false;
                Bin -> leader_matches(binary_to_term(Bin), Self, Epoch)
            end
        end)
    catch
        _:_ -> false
    end.

%% Rolling upgrade: a bare `{Node, Name}` (pre-epoch) is accepted on id alone.  Both
%% formats are 2-tuples; the second element's type (integer vs atom) tells them apart.
leader_matches({{_, _} = Leader, LeaderEpoch}, Self, Epoch) when is_integer(LeaderEpoch) ->
    Leader =:= Self andalso LeaderEpoch =:= Epoch;
leader_matches({undefined, LeaderEpoch}, _Self, _Epoch) when is_integer(LeaderEpoch) ->
    false;
leader_matches({_, _} = Leader, Self, _Epoch) ->
    Leader =:= Self;
leader_matches(_Other, _Self, _Epoch) ->
    false.

%% ---------------------------------------------------------------------------
%% dgen_transaction callbacks
%% ---------------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

%% Pinned (leader-era) read version: any later leader-key write conflicts us, so no
%% storage read is needed (single round-trip).
handle_begin(Tx, State = #{fence := conflict, leader_key := LeaderKey}) ->
    B = dgen_config:backend(),
    B:add_read_conflict_key(Tx, LeaderKey),
    bump_version(B, Tx, State),
    {commit, State};
handle_begin(Tx, State = #{fence := read}) ->
    fenced_by_read(Tx, State).

%% Retry runs on a fresh GRV that may post-date a leadership change: must read.
handle_retry(Tx, State) ->
    fenced_by_read(Tx, State).

handle_committed(Version, _State) ->
    {ok, Version}.

fenced_by_read(Tx, State = #{self := Self, epoch := Epoch, leader_key := LeaderKey}) ->
    B = dgen_config:backend(),
    case leader_matches(current_leader(B, Tx, LeaderKey), Self, Epoch) of
        true ->
            bump_version(B, Tx, State),
            {commit, State};
        false ->
            {stop, fenced, State}
    end.

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

%% Conflict-free on the value, so successive leader commits never conflict on it.
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
