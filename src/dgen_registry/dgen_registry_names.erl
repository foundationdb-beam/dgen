-module(dgen_registry_names).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Provides the durable fence for the commit pipeline that the leader drives (§4.4 of
%% docs/design/dgen_registry_design.md). Name state lives only in the members' memory.
%% The database holds one version key per registry, and each batch bumps it with a
%% versionstamp. The bump exists only to make the commit a write transaction. Without a
%% write, the resolver would never check the read conflict on the leader key.
%%
%% ```
%% {Tuid, <<"version">>} -> 10-byte versionstamp (big-endian commit version)
%% {Tuid, <<"leader">>}  -> term_to_binary({MemberId, Epoch})   (elector-owned fence)
%% ```
%%
%% So uniqueness survives the loss of any single member, but not the loss of both
%% members holding a binding. That remaining case is handled by killing both pids on
%% conflict (§5.6), not by the database.
%%
%% We fence on `{Self, Epoch}`. The epoch makes sure a re-elected member doesn't match
%% its own earlier term. When the read version is pinned to one taken during this
%% leader's term, we add a read conflict on the leader key without reading it. With a
%% fresh read version (GRV), or on a retry, we must read the key. The fresh version may
%% come after a leadership change, and a conflict range would not catch that.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-behaviour(dgen_transaction).

-export([read_version/2, read_committed_frontier/2, start_commit/4, verify_leader/4]).
-export([init/1, handle_begin/2, handle_retry/2, handle_committed/2]).

-if(?DOCATTRS).
-doc """
Reads the registry's durable version key. It holds the versionstamp of the most
recently committed batch, decoded as a big-endian integer.

Returns a non-negative integer, or `0` if no batch has committed yet. The value is a
globally monotonic FDB commit version, **not** a count of batches. It is used for
diagnostics and tests. The registry itself does not read this key. It stamps the
in-memory maps with the commit version that the transaction returns.
""".
-endif.
-spec read_version(dgen_backend:tenant(), dgen_server:tuid()) -> non_neg_integer().
read_version(Tenant = {_Handle, Dir}, Tuid) ->
    B = dgen_config:backend(),
    Key = get_version_db_key(Dir, Tuid),
    dgen_backend:transactional(Tenant, fun({Tx, _Dir}) ->
        case B:wait(B:get(Tx, Key)) of
            not_found -> 0;
            Bin -> binary:decode_unsigned(Bin, big)
        end
    end).

-if(?DOCATTRS).
-doc """
Reads the durable version key as the **committed frontier, on the same scale as a
member's `applied_version`** (`get_committed_version`).

The version key holds a 10-byte FDB versionstamp. That is an 8-byte commit version
followed by a 2-byte batch order within the transaction. So `read_version/2` returns
the commit version shifted left 16 bits, plus the batch order. A member stamps its
maps with the bare 8-byte commit version, so the two can't be compared directly. This
function shifts off the batch bytes to recover the commit version. Each registry
commit is its own transaction with a single versionstamp write, so the batch order is
always 0, but the shift is correct either way.

The leadership-handoff fence uses this in `dgen_registry_member:gather_until_caught_up/6`.
If a new leader's gathered `MaxVersion` is behind this frontier, its reconstruction is
incomplete, and it gathers again before assuming leadership. That is the fix for the
handoff-gather race found in formal/DgenRegistryReplication.tla. Returns `0` if no
batch has committed.
""".
-endif.
-spec read_committed_frontier(dgen_backend:tenant(), dgen_server:tuid()) -> non_neg_integer().
read_committed_frontier(Tenant, Tuid) ->
    read_version(Tenant, Tuid) bsr 16.

-if(?DOCATTRS).
-doc """
Starts an asynchronous, fenced batch commit in a `dgen_transaction` worker.

The commit's only write is a conflict-free atomic bump of the registry's version key
(see the module doc). It has two purposes. It fences the leader, so a stale leader's
commit aborts. And it gives the batch a globally monotonic commit version, which the
caller uses to stamp its in-memory maps. The commit runs in its own process so the
registry leader isn't blocked while it is in flight. The caller may pin the read
version with `read_version` to skip fetching a fresh one (a GRV). The worker sends
`{dgen_transaction, Ref, Reply}` to `owner`. `Reply` is `{committed, Version}` on
success, `{aborted, fenced}` if a newer leader has been elected, or `{error, Reason}`
on a backend failure that can't be retried.

The worker is **monitored**, and the function returns `{ok, {Pid, MRef}}`. The caller
must treat the worker's `DOWN` as a commit failure. That way, a worker that dies
without delivering a result, for example because it was killed, still resolves the
in-flight commit instead of leaving it stuck.

`Self` is the committing member's id and `epoch` is its current leadership epoch.
Together they form `{Self, Epoch}`, the value the fence checks. `Opts` is a map with
these keys:

- `owner`, required. The pid that receives the result.
- `ref`, required. A correlation token.
- `epoch`, required. The leader's epoch.
- `read_version`, optional. A previously committed version to pin. Omit it or pass
  `undefined` to get a fresh GRV.

On a retryable conflict the worker switches to a fresh GRV automatically.
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
    LeaderKey = dgen_registry_elector:get_leader_db_key(Dir, Tuid),
    VersionKey = get_version_db_key(Dir, Tuid),
    ReadVersion = maps:get(read_version, Opts, undefined),
    Args = #{
        self => Self,
        epoch => maps:get(epoch, Opts),
        leader_key => LeaderKey,
        version_key => VersionKey,
        fence => get_fence_mode(ReadVersion)
    },
    TxOpts =
        [
            {db, Db},
            {owner, maps:get(owner, Opts)},
            {ref, maps:get(ref, Opts)}
            | get_read_version_opt(ReadVersion)
        ],
    dgen_transaction:start_monitor(?MODULE, Args, TxOpts).

get_read_version_opt(undefined) -> [];
get_read_version_opt(Version) -> [{read_version, Version}].

get_fence_mode(undefined) -> read;
get_fence_mode(_Version) -> conflict.

-if(?DOCATTRS).
-doc """
Checks the durable leader key to verify that `Self` is the leader for `Epoch` right
now.

The consistent-read path uses this, in `whereis_name_consistent/1` and related
functions. A member that believes it is the leader confirms that against the database
before answering. Otherwise a deposed leader that hasn't heard about the handoff yet,
for example one on the minority side of a partition, could serve a stale answer as
authoritative (Guarantee 5 of the design doc). Any failure to verify returns `false`,
whether it is a mismatch, a missing key, or an unreachable backend. The registry
prefers refusing to answer over answering wrongly.
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
    Key = dgen_registry_elector:get_leader_db_key(Dir, Tuid),
    try
        dgen_backend:transactional(Tenant, fun({Tx, _Dir}) ->
            case B:wait(B:get(Tx, Key)) of
                not_found -> false;
                Bin -> is_fenced_leader(binary_to_term(Bin), Self, Epoch)
            end
        end)
    catch
        _:_ -> false
    end.

is_fenced_leader({{_, _} = Leader, LeaderEpoch}, Self, Epoch) when is_integer(LeaderEpoch) ->
    Leader =:= Self andalso LeaderEpoch =:= Epoch;
is_fenced_leader({undefined, LeaderEpoch}, _Self, _Epoch) when is_integer(LeaderEpoch) ->
    false;
is_fenced_leader(_Other, _Self, _Epoch) ->
    false.

%% ---------------------------------------------------------------------------
%% dgen_transaction callbacks
%% ---------------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

%% The read version is pinned to one from this leader's term. Any later write to the
%% leader key conflicts with us, so we don't need to read it, which saves a round trip.
handle_begin(Tx, State = #{fence := conflict, leader_key := LeaderKey}) ->
    B = dgen_config:backend(),
    B:add_read_conflict_key(Tx, LeaderKey),
    bump_version(B, Tx, State),
    {commit, State};
handle_begin(Tx, State = #{fence := read}) ->
    fence_by_read(Tx, State).

%% A retry runs on a fresh read version that may come after a leadership change, so we
%% must read the leader key.
handle_retry(Tx, State) ->
    fence_by_read(Tx, State).

handle_committed(Version, _State) ->
    {ok, Version}.

fence_by_read(Tx, State = #{self := Self, epoch := Epoch, leader_key := LeaderKey}) ->
    B = dgen_config:backend(),
    case is_fenced_leader(get_current_leader(B, Tx, LeaderKey), Self, Epoch) of
        true ->
            bump_version(B, Tx, State),
            {commit, State};
        false ->
            {stop, fenced, State}
    end.

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

%% Writes a versionstamped value, which never causes a conflict. So successive commits
%% from the leader never conflict on this key.
bump_version(B, Tx, #{version_key := VersionKey}) ->
    B:set_versionstamped_value(Tx, VersionKey, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>).

get_current_leader(B, Tx, LeaderKey) ->
    case B:wait(B:get(Tx, LeaderKey)) of
        not_found -> undefined;
        Bin -> binary_to_term(Bin)
    end.

get_version_db_key(Dir, Tuid) ->
    B = dgen_config:backend(),
    B:dir_pack(Dir, dgen_key:extend(Tuid, <<"version">>)).
