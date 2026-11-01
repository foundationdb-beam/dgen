-module(dgen_mem).
-behaviour(dgen_backend).

%% In a simulation build, this rewrites the `erlang:monotonic_time/1` calls below to
%% use `eta_time`, so commit versions advance with the virtual clock when one is
%% running. Outside a simulation build this module doesn't depend on `eta` at all.
-include("../include/dgen_eta.hrl").

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
A pure-Erlang, in-memory `dgen_backend`.

It implements the FoundationDB semantics the registry actually depends on: MVCC
read versions, read-conflict detection, versionstamps, and watches. It needs no
NIF, no server, and no I/O. That gives us two things.

- **The suite runs without FoundationDB installed.** That is worth having on its own.
- **Commit timing is no longer a source of nondeterminism.** It was the last one
  outside `eta_sched` and `eta_time`. Commits happen synchronously, in the calling
  process, in a deterministic order.

## Fidelity, and where it stops

Key layout is exactly the same as FDB's. `erlfdb_subspace` and `erlfdb_tuple` are
pure Erlang, so we reuse them instead of reimplementing order-preserving tuple
encoding. A subtle error there would show up as ordering bugs in `dgen_queue`.

What is modelled faithfully:

- **Read versions.** A read sees the newest value written at or before the
  transaction's read version. Pinning an older read version with
  `set_read_version/2` really does read the past. That is what makes the registry's
  fencing test meaningful.
- **Read conflicts.** A commit aborts with `not_committed` (1020) if any key in a
  read-conflict range was written after the transaction's read version. The leader
  fence depends on this (§5.1), so it has to be real.
- **Versionstamps.** `set_versionstamped_value/3` fills in a 10-byte stamp, made of
  an 8-byte commit version and a 2-byte batch order within the transaction. The
  registry's version key then decodes and sorts exactly as it does against FDB.
- **Watches.** A watch fires when its key is written by a later commit.

What is not:

- **No storage-level partitioning, no coordinators, no real distribution.** A
  simulation runs in one VM, so the database is a single point where every commit
  is serialised. It is not a cluster that can partition underneath the system
  under test.
- **Write conflicts are not tracked.** FDB fails a commit whose write set overlaps
  another transaction's read set. Here commits are serialised, so the losing
  transaction is simply the one that commits second, and it sees the other's
  writes. Nothing in `dgen` depends on the difference.
- **No transaction size limit** (`transaction_too_large`). Nothing in `dgen` comes
  near FoundationDB's 10MB transaction cap.
- **No retry backoff.** FoundationDB backs off exponentially with jitter. Here
  `transactional/2` retries at once, up to 100 attempts. A `timer:sleep/1` would be
  wrong under a virtual clock. A sleeping process is neither runnable nor blocked
  on a receive, so `eta_sched` could never step it.

**The MVCC window is modelled.** Versions advance at FoundationDB's rate
(`VERSIONS_PER_SECOND` = 1e6), and the window is the same version distance
(`MAX_READ_TRANSACTION_LIFE_VERSIONS`, 5 seconds' worth). A read older than the
window raises `transaction_too_old`, as it would against the real thing. Under a
virtual clock, those five seconds pass instantly.

The version rate follows whatever clock `eta_time` exposes. Start the clock before
opening the database. Starting it later shifts the origin we measure the rate from.

## Fault injection

`set_faults/2` injects the failures the registry is built to survive, in a way
that can be reproduced. `conflict_p` causes spurious `not_committed` errors, which a
correct caller retries. `commit_fail_p` causes a non-retryable error. Both draw from
a seeded RNG, so a run can be replayed.

## Determinism

State lives in ETS, keyed by the database handle, and nothing runs in a process of
its own. So no call into this module can look like quiescence to `eta_sched`.
Commit versions come from the clock that `eta_time` exposes, so they are
deterministic under a virtual clock.

*This documentation is LLM-generated. See the AI disclosure in `README.md`.*
""".
-endif.

-export([
    transactional/2,
    is_transaction/1,
    get/2,
    set/3,
    clear_range/3,
    get_range/4,
    add/3,
    add_read_conflict_key/2,
    get_read_version/1,
    set_read_version/2,
    get_committed_version/1,
    create_transaction/1,
    commit/1,
    on_error/2,
    error_code/1,
    get_next_tx_id/1,
    set_versionstamped_key/3,
    set_versionstamped_value/3,
    get_versionstamp/1,
    wait/1,
    wait_for_all/1,
    watch/2,
    watch/3,
    dir_range/2,
    dir_pack/2,
    dir_pack_vs/2,
    dir_unpack/2,
    key_strinc/1,
    dir_create/3,
    dir_remove/3,
    sandbox_open/2
]).

%% Simulation control, outside the behaviour.
-export([open/1, close/1, set_faults/2, stats/1, reset/1]).

-export_type([db/0, fault_opts/0]).

%% FDB error codes. `on_error/2` resets the transaction for the retryable set, and
%% that set has to match FoundationDB's. A missing code turns a recoverable error
%% into a fatal one, and that kind of mismatch makes a simulation backend worse
%% than useless.
-define(TRANSACTION_TOO_OLD, 1007).
%% Raised when a transaction reads a key it has a pending versionstamped write to.
%% That value isn't known until commit.
-define(ACCESSED_UNREADABLE, 1036).

%% FoundationDB advances the read version at a fixed rate rather than once per
%% commit, and it defines its MVCC window as a distance in versions.
%% `VERSIONS_PER_SECOND` is 1e6, and `MAX_READ_TRANSACTION_LIFE_VERSIONS` is 5
%% seconds' worth. Reading at a version older than that raises
%% `transaction_too_old`.
%%
%% Modelling it the same way costs nothing and gets us the same behaviour. Under a
%% virtual clock it also becomes controllable. Five seconds of MVCC window are five
%% simulated seconds, which pass instantly instead of needing a six-second sleep.
-define(VERSIONS_PER_MS, 1000).
-define(MAX_READ_TRANSACTION_LIFE_VERSIONS, 5000000).
-define(NOT_COMMITTED, 1020).
-define(COMMIT_UNKNOWN, 1021).
-define(TRANSACTION_TIMED_OUT, 1031).
-define(PROCESS_BEHIND, 1037).
-define(RETRYABLE, [
    ?TRANSACTION_TOO_OLD, ?NOT_COMMITTED, ?COMMIT_UNKNOWN, ?TRANSACTION_TIMED_OUT, ?PROCESS_BEHIND
]).

-opaque db() :: {dgen_mem_db, atom()}.
-type tx() :: {dgen_mem_tx, atom(), reference()}.

-type fault_opts() :: #{
    conflict_p => float(),
    commit_fail_p => float()
}.

%% ---------------------------------------------------------------------------
%% Lifecycle
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Opens (or resets) a named in-memory database.

`Opts` may carry `seed` for the fault RNG and `faults` (see `set_faults/2`).
""".
-endif.
-spec open(atom() | #{name := atom(), seed => integer(), faults => fault_opts()}) -> db().
open(Name) when is_atom(Name) ->
    open(#{name => Name});
open(Opts = #{name := Name}) ->
    Db = {dgen_mem_db, Name},
    close(Db),
    _ = ets:new(data_tab(Name), [named_table, public, ordered_set]),
    _ = ets:new(meta_tab(Name), [named_table, public, set]),
    _ = ets:new(tx_tab(Name), [named_table, public, set]),
    _ = ets:new(watch_tab(Name), [named_table, public, bag]),
    Seed = maps:get(seed, Opts, 0),
    ets:insert(meta_tab(Name), [
        %% Starts above zero so that a pinned read version of 0 really is "before
        %% everything". That matches how the registry treats an unwritten version key.
        {version, 1},
        %% Versions advance with the clock (see ?VERSIONS_PER_MS), so the database
        %% needs an origin to measure from. Under the transform this reads the
        %% virtual clock, and `eta_time` falls back to the real monotonic clock
        %% when none is running, so it behaves correctly either way.
        {start_ms, erlang:monotonic_time(millisecond)},
        {commits, 0},
        {conflicts, 0},
        {faults, maps:get(faults, Opts, #{})},
        {rand, rand:seed_s(exsss, {Seed, Seed + 3, Seed + 5})}
    ]),
    Db.

-if(?DOCATTRS).
-doc "Deletes a database and everything in it. Safe when it does not exist.".
-endif.
-spec close(db()) -> ok.
close({dgen_mem_db, Name}) ->
    lists:foreach(
        fun(Tab) ->
            %% The process that opened the database owns the tables, so the VM
            %% destroys them when that process exits. Checking first and then
            %% deleting would race with that, because the table can vanish in
            %% between. An ExUnit `on_exit` hits exactly this, since it runs after
            %% the test process is gone. So we just delete, and ignore a missing table.
            try
                ets:delete(Tab)
            catch
                error:badarg -> ok
            end
        end,
        [data_tab(Name), meta_tab(Name), tx_tab(Name), watch_tab(Name)]
    ),
    ok.

-if(?DOCATTRS).
-doc "Clears all data, keeping the database open. Versions keep advancing.".
-endif.
-spec reset(db()) -> ok.
reset({dgen_mem_db, Name}) ->
    ets:delete_all_objects(data_tab(Name)),
    ok.

-if(?DOCATTRS).
-doc """
Sets the commit fault policy.

- `conflict_p` is the probability that a commit is rejected with `not_committed`
  (1020) even though nothing conflicted. The error is retryable, so a correct caller
  recovers. The point is to check that it does.
- `commit_fail_p` is the probability that a commit fails with a non-retryable
  error. That error must reach the caller rather than being retried away.
""".
-endif.
-spec set_faults(db(), fault_opts()) -> ok.
set_faults({dgen_mem_db, Name}, Faults) ->
    ets:insert(meta_tab(Name), {faults, Faults}),
    ok.

-if(?DOCATTRS).
-doc "Returns the commit count, conflict count, current version, and key count.".
-endif.
-spec stats(db()) -> #{atom() => non_neg_integer()}.
stats({dgen_mem_db, Name}) ->
    #{
        version => meta(Name, version),
        commits => meta(Name, commits),
        conflicts => meta(Name, conflicts),
        keys => ets:info(data_tab(Name), size)
    }.

data_tab(Name) -> list_to_atom("dgen_mem_data_" ++ atom_to_list(Name)).
meta_tab(Name) -> list_to_atom("dgen_mem_meta_" ++ atom_to_list(Name)).
tx_tab(Name) -> list_to_atom("dgen_mem_tx_" ++ atom_to_list(Name)).
watch_tab(Name) -> list_to_atom("dgen_mem_watch_" ++ atom_to_list(Name)).

meta(Name, Key) -> ets:lookup_element(meta_tab(Name), Key, 2).

%% Returns the current read version, which is the last commit version or the clock
%% version, whichever is further ahead. So versions never go backwards, and an idle
%% database still advances. That is what makes the MVCC window measure elapsed time
%% rather than a number of commits.
current_version(Name) ->
    max(meta(Name, version), clock_version(Name)).

clock_version(Name) ->
    Elapsed = erlang:monotonic_time(millisecond) - meta(Name, start_ms),
    max(0, Elapsed) * ?VERSIONS_PER_MS.

%% Raises FoundationDB's `transaction_too_old` for a read at a version the storage
%% servers would no longer keep.
check_not_too_old(Name, RV) ->
    case current_version(Name) - RV > ?MAX_READ_TRANSACTION_LIFE_VERSIONS of
        true -> erlang:error({erlfdb_error, ?TRANSACTION_TOO_OLD});
        false -> ok
    end.

%% ---------------------------------------------------------------------------
%% Transactions
%% ---------------------------------------------------------------------------

-spec is_transaction(term()) -> boolean().
is_transaction({dgen_mem_tx, _, _}) -> true;
is_transaction(_) -> false.

-spec create_transaction(db()) -> tx().
create_transaction({dgen_mem_db, Name}) ->
    Ref = make_ref(),
    Tx = {dgen_mem_tx, Name, Ref},
    ets:insert(tx_tab(Name), {Ref, new_tx_state()}),
    Tx.

new_tx_state() ->
    #{
        %% `undefined` means we assign it on the first read or at commit. FDB works
        %% the same way, so a transaction that only writes never fetches a read
        %% version.
        read_version => undefined,
        %% Mutations, newest first. Sets and clear_ranges interleave, and their order
        %% decides the outcome, so a map keyed by key would lose information.
        muts => [],
        read_conflicts => [],
        committed_version => undefined,
        versionstamp => undefined,
        next_tx_id => 0,
        %% Watches this transaction registered, so a reset can cancel them.
        watches => []
    }.

tx_state({dgen_mem_tx, Name, Ref}) ->
    ets:lookup_element(tx_tab(Name), Ref, 2).

put_tx_state({dgen_mem_tx, Name, Ref}, State) ->
    ets:insert(tx_tab(Name), {Ref, State}),
    ok.

update_tx(Tx, Fun) ->
    put_tx_state(Tx, Fun(tx_state(Tx))).

%% The read version, assigned on demand.
read_version(Tx = {dgen_mem_tx, Name, _}) ->
    case maps:get(read_version, tx_state(Tx)) of
        undefined ->
            V = current_version(Name),
            update_tx(Tx, fun(S) -> S#{read_version := V} end),
            V;
        V ->
            V
    end.

-spec get_read_version(tx()) -> dgen_backend:future().
get_read_version(Tx) ->
    ready(read_version(Tx)).

-spec set_read_version(tx(), integer()) -> ok.
set_read_version(Tx, Version) ->
    update_tx(Tx, fun(S) -> S#{read_version := Version} end).

-spec get_committed_version(tx()) -> integer().
get_committed_version(Tx) ->
    case maps:get(committed_version, tx_state(Tx)) of
        undefined -> -1;
        V -> V
    end.

-if(?DOCATTRS).
-doc """
Runs `Fun` in a transaction and retries it on retryable errors. This is the same
contract as `erlfdb:transactional/2`. If you pass in a transaction handle, it is
used directly, so nested calls work.
""".
-endif.
-spec transactional(db() | tx(), fun((tx()) -> Result)) -> Result when Result :: term().
transactional(Tx = {dgen_mem_tx, _, _}, Fun) ->
    Fun(Tx);
transactional(Db = {dgen_mem_db, _}, Fun) ->
    do_transactional(Db, Fun, 0).

do_transactional(Db, Fun, Attempt) when Attempt < 100 ->
    Tx = create_transaction(Db),
    try
        Result = Fun(Tx),
        wait(commit(Tx)),
        Result
    catch
        error:{erlfdb_error, Code} when Code =:= ?NOT_COMMITTED ->
            discard_tx(Tx),
            do_transactional(Db, Fun, Attempt + 1)
    end;
do_transactional(_Db, _Fun, _Attempt) ->
    erlang:error({erlfdb_error, ?NOT_COMMITTED}).

%% ---------------------------------------------------------------------------
%% Reads
%% ---------------------------------------------------------------------------

-spec get(tx(), dgen_backend:key()) -> dgen_backend:future().
get(Tx = {dgen_mem_tx, Name, _}, Key) ->
    RV = read_version(Tx),
    check_not_too_old(Name, RV),
    add_read_conflict_range(Tx, Key, key_strinc(Key)),
    %% The transaction reads its own writes. Its uncommitted mutations take
    %% precedence over the store, as they do in FDB.
    case pending_value(tx_state(Tx), Key) of
        {ok, Value} -> ready(Value);
        cleared -> ready(not_found);
        unreadable -> erlang:error({erlfdb_error, ?ACCESSED_UNREADABLE});
        none -> ready(stored_value(Name, Key, RV))
    end.

-spec get_range(tx(), dgen_backend:key(), dgen_backend:key(), list()) ->
    [{dgen_backend:key(), binary()}].
get_range(Tx = {dgen_mem_tx, Name, _}, StartKey, EndKey, Opts) ->
    RV = read_version(Tx),
    check_not_too_old(Name, RV),
    add_read_conflict_range(Tx, StartKey, EndKey),
    S = tx_state(Tx),
    Rows = range_rows(Name, StartKey, EndKey, RV, S),
    Ordered =
        case proplists:get_value(reverse, Opts, false) of
            true -> lists:reverse(Rows);
            false -> Rows
        end,
    case proplists:get_value(limit, Opts, 0) of
        0 -> Ordered;
        Limit -> lists:sublist(Ordered, Limit)
    end.

%% Returns every live key in [StartKey, EndKey) as of `RV`, with the transaction's
%% own mutations applied on top.
range_rows(Name, StartKey, EndKey, RV, S) ->
    Stored = [
        {K, V}
     || K <- keys_in_range(Name, StartKey, EndKey),
        (V = stored_value(Name, K, RV)) =/= not_found
    ],
    Pending = [
        {K, V}
     || {K, V} <- pending_in_range(S, StartKey, EndKey)
    ],
    merge_rows(Stored, Pending).

merge_rows(Stored, Pending) ->
    Merged = lists:foldl(
        fun({K, V}, Acc) -> lists:keystore(K, 1, Acc, {K, V}) end,
        Stored,
        [{K, V} || {K, V} <- Pending, V =/= cleared]
    ),
    Cleared = [K || {K, cleared} <- Pending],
    lists:keysort(1, [{K, V} || {K, V} <- Merged, not lists:member(K, Cleared)]).

%% Returns every stored key in [StartKey, EndKey). `ets:next/2` returns the first
%% key strictly greater than its argument, so we check the start key separately.
%% Without that check the start key would silently drop out of every range. That
%% breaks single-key clear_range and, less visibly, read-conflict detection on a
%% key that exists.
keys_in_range(Name, StartKey, EndKey) ->
    Tab = data_tab(Name),
    First =
        case ets:member(Tab, StartKey) of
            true -> StartKey;
            false -> ets:next(Tab, StartKey)
        end,
    keys_from(Tab, First, EndKey, []).

keys_from(_Tab, '$end_of_table', _EndKey, Acc) ->
    lists:reverse(Acc);
keys_from(Tab, Key, EndKey, Acc) when Key < EndKey ->
    keys_from(Tab, ets:next(Tab, Key), EndKey, [Key | Acc]);
keys_from(_Tab, _Key, _EndKey, Acc) ->
    lists:reverse(Acc).

%% Returns the newest value at or before `RV`. Returns `not_found` if the key didn't
%% exist then or had been cleared.
stored_value(Name, Key, RV) ->
    case ets:lookup(data_tab(Name), Key) of
        [] ->
            not_found;
        [{Key, Versions}] ->
            case [V || {Ver, V} <- Versions, Ver =< RV] of
                [] -> not_found;
                [Newest | _] -> value_or_not_found(Newest)
            end
    end.

value_or_not_found(cleared) -> not_found;
value_or_not_found(Value) -> Value.

%% Returns what this transaction's own mutations say about a key.
pending_value(#{muts := Muts}, Key) ->
    lists:foldl(
        fun
            ({set, K, V}, _Acc) when K =:= Key -> {ok, V};
            %% A pending versionstamped write makes the key unreadable for the
            %% rest of the transaction, because its value isn't decided until the
            %% commit assigns the stamp. FoundationDB raises accessed_unreadable
            %% (1036) here, and matching that matters. If we fell through to the
            %% last committed value instead, we would silently return a stale
            %% answer. A caller comparing a cached version against this key would
            %% then get a false match and serve stale state.
            ({vs_value, K, _}, _Acc) when K =:= Key -> unreadable;
            ({clear_range, S, E}, _Acc) when Key >= S, Key < E -> cleared;
            (_, Acc) -> Acc
        end,
        none,
        lists:reverse(Muts)
    ).

pending_in_range(#{muts := Muts}, StartKey, EndKey) ->
    lists:foldl(
        fun
            ({set, K, V}, Acc) when K >= StartKey, K < EndKey ->
                lists:keystore(K, 1, Acc, {K, V});
            ({clear_range, S, E}, Acc) ->
                %% Anything already staged inside the cleared span goes away, and
                %% the span itself is recorded so stored rows are masked too.
                Kept = [{K, V} || {K, V} <- Acc, K < S orelse K >= E],
                Kept ++ [{K, cleared} || {K, _} <- Acc, K >= S, K < E];
            (_, Acc) ->
                Acc
        end,
        [],
        lists:reverse(Muts)
    ).

%% ---------------------------------------------------------------------------
%% Writes
%% ---------------------------------------------------------------------------

-spec set(tx(), dgen_backend:key(), binary()) -> ok.
set(Tx, Key, Value) ->
    stage(Tx, {set, Key, Value}).

-spec clear_range(tx(), dgen_backend:key(), dgen_backend:key()) -> ok.
clear_range(Tx, StartKey, EndKey) ->
    stage(Tx, {clear_range, StartKey, EndKey}).

-spec add(tx(), dgen_backend:key(), integer()) -> ok.
add(Tx, Key, Value) ->
    stage(Tx, {add, Key, Value}).

-spec set_versionstamped_value(tx(), dgen_backend:key(), binary()) -> ok.
set_versionstamped_value(Tx, Key, Value) ->
    stage(Tx, {vs_value, Key, Value}).

-spec set_versionstamped_key(tx(), dgen_backend:key(), binary()) -> ok.
set_versionstamped_key(Tx, Key, Value) ->
    stage(Tx, {vs_key, Key, Value}).

stage(Tx, Mut) ->
    update_tx(Tx, fun(S = #{muts := Muts}) -> S#{muts := [Mut | Muts]} end).

-spec add_read_conflict_key(tx(), dgen_backend:key()) -> ok.
add_read_conflict_key(Tx, Key) ->
    add_read_conflict_range(Tx, Key, key_strinc(Key)).

%% Every non-snapshot read records a read conflict, because FoundationDB does the
%% same. Far more code depends on this than the few explicit
%% `add_read_conflict_key/2` calls suggest.
%%
%% Across all of `dgen` there is exactly one explicit conflict, the registry's
%% leader fence. Everything else relies on the implicit ones. The most important is
%% `dgen_queue:peek_k/3`. Two consumers can read the same pending items, and the
%% conflict is what stops both of them from consuming them. Without it, no
%% read-then-write would be serializable. The resulting failures, such as a message
%% consumed twice or a lost increment, look nothing like a missing conflict range.
-spec add_read_conflict_range(tx(), dgen_backend:key(), dgen_backend:key()) -> ok.
add_read_conflict_range(Tx, StartKey, EndKey) ->
    update_tx(Tx, fun(S = #{read_conflicts := Rs}) ->
        S#{read_conflicts := [{StartKey, EndKey} | Rs]}
    end).

-spec get_next_tx_id(tx()) -> non_neg_integer().
get_next_tx_id(Tx) ->
    S = #{next_tx_id := Id} = tx_state(Tx),
    put_tx_state(Tx, S#{next_tx_id := Id + 1}),
    Id.

%% ---------------------------------------------------------------------------
%% Commit
%% ---------------------------------------------------------------------------

-spec commit(tx()) -> dgen_backend:future().
commit(Tx = {dgen_mem_tx, Name, _}) ->
    S = tx_state(Tx),
    RV =
        case maps:get(read_version, S) of
            undefined -> current_version(Name);
            V -> V
        end,
    Outcome = with_commit_lock(Name, fun() -> try_commit(Tx, Name, S, RV) end),
    case Outcome of
        ok -> ready(ok);
        {error, Code} -> erlang:error({erlfdb_error, Code})
    end.

%% Runs under the commit lock. In some orderings, raising here would escape before
%% the lock's `after` clause has run. So we return failures as a value, and the
%% caller raises them once the lock is released.
try_commit(_Tx, _Name, #{muts := []}, _RV) ->
    %% A read-only transaction always commits and can never conflict. FDB treats
    %% it as a no-op. Every read records a conflict range, so checking them here
    %% would fail read-only transactions for no reason.
    ok;
try_commit(Tx, Name, S, RV) ->
    case commit_fault(Name) of
        {fail, Code} ->
            bump(Name, conflicts, 1),
            {error, Code};
        ok ->
            case has_conflict(Name, maps:get(read_conflicts, S), RV) of
                true ->
                    bump(Name, conflicts, 1),
                    {error, ?NOT_COMMITTED};
                false ->
                    do_commit(Tx, Name, S),
                    ok
            end
    end.

do_commit(Tx = {dgen_mem_tx, _, TxRef}, Name, S) ->
    %% The commit version must be strictly ahead of both the last commit and the
    %% clock. In other words, it must be ahead of `current_version/1`, which every
    %% read version handed out so far came from. Landing exactly on a read version
    %% is a real bug, not a rounding detail. Both `has_conflict/3` and `watch/3` ask
    %% whether a key was written after a read version. A commit at that same
    %% version would be a conflict we miss and a watch that never fires. An earlier
    %% `max(Version + 1, Clock)` did exactly that whenever the clock was ahead of
    %% the commit counter, which is most of the time. This runs under the commit
    %% lock, so the read-modify-write is safe.
    Version = current_version(Name) + 1,
    ets:insert(meta_tab(Name), {version, Version}),
    Stamp = versionstamp(Version, 0),
    Written = apply_muts(Name, lists:reverse(maps:get(muts, S)), Version, Stamp, []),
    bump(Name, commits, 1),
    put_tx_state(Tx, S#{committed_version := Version, versionstamp := Stamp}),
    fire_watches(Name, Written, TxRef),
    ok.

%% Runs `Fun` while holding the commit lock, so commits happen one at a time.
%%
%% Checking the conflict ranges, assigning a version, and applying the mutations
%% have to be one indivisible step. FoundationDB serialises commits. Without the
%% same guarantee here, a second transaction can slip into the gap, and three
%% things break quietly.
%%
%%   - A conflict is missed, so a transaction that should have been fenced
%%     commits. The registry's leader fence depends on exactly this (§5.1).
%%   - The read-modify-write of a key's version list in `write/4` loses an entry.
%%   - Versions land out of order in that list. `stored_value/3` takes the first
%%     matching entry, so a read returns an older value than it should.
%%
%% We use an ETS lock rather than a process that serialises commits. A blocking
%% call into a process looks like quiescence to `eta_sched`, so every commit would
%% end a scheduling step. The critical section is pure in-memory work, so spinning
%% is cheap.
with_commit_lock(Name, Fun) ->
    Tab = meta_tab(Name),
    case ets:insert_new(Tab, {commit_lock, self()}) of
        true ->
            try
                Fun()
            after
                ets:delete(Tab, commit_lock)
            end;
        false ->
            reclaim_or_wait(Tab),
            with_commit_lock(Name, Fun)
    end.

%% A lock holder can die without releasing the lock. `try ... after` doesn't run
%% when a process is killed with an untrappable exit, and that happens routinely
%% here, since a simulated member crash is exactly that. If we did nothing, one such
%% death would block every later commit in the VM. It would look like the whole
%% suite hanging somewhere unrelated.
%%
%% We use `delete_object/2` rather than `delete/2` so that we can only remove the
%% dead holder's lock, never one a live process has just taken.
reclaim_or_wait(Tab) ->
    case ets:lookup(Tab, commit_lock) of
        [{commit_lock, Holder}] ->
            case is_process_alive(Holder) of
                true -> erlang:yield();
                false -> ets:delete_object(Tab, {commit_lock, Holder})
            end;
        [] ->
            ok
    end.

%% Returns whether any key in a read-conflict range was written after our read
%% version. The registry's leader fence relies on this check (§5.1).
has_conflict(_Name, [], _RV) ->
    false;
has_conflict(Name, Ranges, RV) ->
    lists:any(
        fun({S, E}) ->
            lists:any(
                fun(Key) -> newest_version(Name, Key) > RV end,
                keys_in_range(Name, S, E)
            )
        end,
        Ranges
    ).

newest_version(Name, Key) ->
    case ets:lookup(data_tab(Name), Key) of
        [] -> 0;
        [{Key, [{Ver, _} | _]}] -> Ver
    end.

apply_muts(_Name, [], _Version, _Stamp, Written) ->
    lists:reverse(Written);
apply_muts(Name, [Mut | Rest], Version, Stamp, Written) ->
    W =
        case Mut of
            {set, Key, Value} ->
                write(Name, Key, Value, Version),
                [Key];
            {vs_value, Key, Value} ->
                write(Name, Key, substitute_stamp(Value, Stamp), Version),
                [Key];
            {vs_key, Key, Value} ->
                write(Name, substitute_stamp(Key, Stamp), Value, Version),
                [Key];
            {add, Key, N} ->
                write(Name, Key, add_to(current_raw(Name, Key), N), Version),
                [Key];
            {clear_range, S, E} ->
                Keys = keys_in_range(Name, S, E),
                [write(Name, K, cleared, Version) || K <- Keys],
                Keys
        end,
    apply_muts(Name, Rest, Version, Stamp, lists:reverse(W) ++ Written).

write(Name, Key, Value, Version) ->
    Versions =
        case ets:lookup(data_tab(Name), Key) of
            [] -> [];
            [{Key, Vs}] -> Vs
        end,
    ets:insert(data_tab(Name), {Key, [{Version, Value} | Versions]}),
    ok.

current_raw(Name, Key) ->
    case ets:lookup(data_tab(Name), Key) of
        [{Key, [{_, V} | _]}] when is_binary(V) -> V;
        _ -> <<>>
    end.

%% FDB's `add` is a little-endian, zero-extended integer add.
add_to(Current, N) ->
    Size = max(byte_size(Current), 8),
    Padded = binary:part(<<Current/binary, 0:(Size * 8)>>, 0, Size),
    <<Cur:(Size * 8)/little-unsigned>> = Padded,
    <<((Cur + N) band ((1 bsl (Size * 8)) - 1)):(Size * 8)/little-unsigned>>.

%% Builds a 10-byte FDB versionstamp from an 8-byte big-endian commit version and a
%% 2-byte batch order. The registry decodes it as a big-endian integer and shifts
%% off the batch bytes to get the commit version, so the layout has to match.
versionstamp(Version, Batch) ->
    <<Version:64/big-unsigned, Batch:16/big-unsigned>>.

%% A versionstamped value carries a 10-byte placeholder, followed by a 4-byte
%% little-endian offset that says where the placeholder is.
substitute_stamp(Value, Stamp) when byte_size(Value) >= 14 ->
    Body = binary:part(Value, 0, byte_size(Value) - 4),
    <<Offset:32/little-unsigned>> = binary:part(Value, byte_size(Value) - 4, 4),
    Before = binary:part(Body, 0, Offset),
    After = binary:part(Body, Offset + 10, byte_size(Body) - Offset - 10),
    <<Before/binary, Stamp/binary, After/binary>>;
substitute_stamp(Value, _Stamp) ->
    Value.

-if(?DOCATTRS).
-doc """
Returns the transaction's versionstamp as a future that resolves **after** the
transaction commits.

Resolving late isn't an optimisation here. It is the contract. Callers take this
future before committing and wait on it afterwards. `set_mod_state/4` in
`dgen_server` does exactly that to version its state cache. If we resolved it
early, we would return `undefined`, because no commit version exists yet. The cache
would then key itself on a value that never matches, or worse, one that matches by
accident.
""".
-endif.
-spec get_versionstamp(tx()) -> dgen_backend:future().
get_versionstamp(Tx) ->
    {dgen_mem_vs_future, Tx}.

bump(Name, Key, N) ->
    ets:update_counter(meta_tab(Name), Key, N).

%% ---------------------------------------------------------------------------
%% Errors and retries
%% ---------------------------------------------------------------------------

-spec error_code(term()) -> integer() | error.
error_code({erlfdb_error, Code}) -> Code;
error_code(_) -> error.

-if(?DOCATTRS).
-doc """
Resets a transaction for a retry when `Code` is retryable, like
`erlfdb:on_error/2`. For a retryable error it returns a future that succeeds.
Otherwise it raises. A reset drops staged mutations and conflict ranges, cancels
the transaction's watches, and takes a fresh read version.
""".
-endif.
-spec on_error(tx(), integer()) -> dgen_backend:future().
on_error(Tx, Code) ->
    case lists:member(Code, ?RETRYABLE) of
        true ->
            cancel_watches(Tx),
            put_tx_state(Tx, new_tx_state()),
            ready(ok);
        false ->
            erlang:error({erlfdb_error, Code})
    end.

commit_fault(Name) ->
    Faults = meta(Name, faults),
    case roll(Name) < maps:get(commit_fail_p, Faults, 0.0) of
        true ->
            %% Not retryable: a caller must surface this rather than loop.
            {fail, 1510};
        false ->
            case roll(Name) < maps:get(conflict_p, Faults, 0.0) of
                true -> {fail, ?NOT_COMMITTED};
                false -> ok
            end
    end.

roll(Name) ->
    {V, Rand} = rand:uniform_s(meta(Name, rand)),
    ets:insert(meta_tab(Name), {rand, Rand}),
    V.

%% ---------------------------------------------------------------------------
%% Futures and watches
%% ---------------------------------------------------------------------------

%% Every read here is already resolved, so a future just wraps the value. Watches
%% are the exception and carry a real ref.
ready(Value) -> {dgen_mem_future, Value}.

-spec wait(dgen_backend:future()) -> term().
wait({dgen_mem_vs_future, Tx}) -> maps:get(versionstamp, tx_state(Tx));
wait({dgen_future, _Ref, {dgen_mem_vs_future, Tx}}) -> maps:get(versionstamp, tx_state(Tx));
wait({dgen_mem_future, Value}) -> Value;
wait({dgen_future, _Ref, {dgen_mem_future, Value}}) -> Value;
wait(Value) -> Value.

-spec wait_for_all([dgen_backend:future()]) -> [term()].
wait_for_all(Futures) -> [wait(F) || F <- Futures].

-if(?DOCATTRS).
-doc """
Watches `Key` and sends `{Ref, ready}` to the watcher when its value changes.

**A watch is anchored to the transaction that creates it, not to the moment it is
registered.** FoundationDB reports a change relative to the value the creating
transaction would read. Getting that wrong silently loses a wakeup.

- A write that lands after the transaction's read version, but before the watch is
  registered, still fires the watch. `consume_queued/5` in `dgen_server` reads an
  empty queue and then watches the push key. Without this rule, a push committed in
  between leaves the consumer asleep on a queue that already has work. The caller's
  `call/4` in `dgen` then times out with nothing wrong anywhere else. The check and
  the registration run under the commit lock, so no write can slip between them.

- The creating transaction's own writes don't fire the watch, because they are part
  of the value it would read. `push_call/7` in `dgen` writes the reply sentinel and
  watches it in one transaction. Firing there would cost a read-and-rewatch round
  trip on every call. That round trip is exactly the window in which the first case
  loses a reply.

We fire at registration rather than at commit. This is a deliberate
simplification. Firing early is safe here, because every consumer re-reads the
watched state before acting. Firing late is not.
""".
-endif.
-spec watch(tx(), dgen_backend:key()) -> dgen_backend:future().
watch(Tx, Key) -> watch(Tx, Key, []).

-spec watch(tx(), dgen_backend:key(), list()) -> dgen_backend:future().
watch(Tx = {dgen_mem_tx, Name, TxRef}, Key, Opts) ->
    Ref = make_ref(),
    To = proplists:get_value(to, Opts, self()),
    RV = read_version(Tx),
    Entry = {Key, Ref, To, TxRef},
    with_commit_lock(Name, fun() ->
        case newest_version(Name, Key) > RV of
            true ->
                %% The key already changed after the transaction's read version.
                %% Nothing later will fire this watch, so we fire it now.
                catch To ! {Ref, ready};
            false ->
                ets:insert(watch_tab(Name), Entry),
                update_tx(Tx, fun(S = #{watches := Ws}) -> S#{watches := [Entry | Ws]} end)
        end
    end),
    {dgen_future, Ref, {dgen_mem_future, ok}}.

%% Runs under the commit lock, as part of `do_commit/3`. `TxRef` is the committing
%% transaction. Its writes don't fire its own watches, which stay armed.
fire_watches(Name, Keys, TxRef) ->
    lists:foreach(
        fun(Key) ->
            lists:foreach(
                fun
                    ({_, _, _, Owner}) when Owner =:= TxRef ->
                        ok;
                    (Entry = {_, Ref, To, _}) ->
                        catch To ! {Ref, ready},
                        ets:delete_object(watch_tab(Name), Entry)
                end,
                ets:lookup(watch_tab(Name), Key)
            )
        end,
        lists:usort(Keys)
    ).

%% Cancels a transaction's watches, as FoundationDB does when a transaction is
%% reset. Without this, a retried transaction leaves an armed watch behind for every
%% attempt. Each one later fires and delivers to a caller that has moved on.
cancel_watches(Tx = {dgen_mem_tx, Name, _}) ->
    lists:foreach(
        fun(Entry) -> ets:delete_object(watch_tab(Name), Entry) end,
        maps:get(watches, tx_state(Tx))
    ),
    update_tx(Tx, fun(S) -> S#{watches := []} end).

%% Drops a transaction that will never be used again. This is only safe for a
%% transaction that `do_transactional/3` abandons and replaces with a new one.
%% `on_error/2` works on a transaction the caller still holds, so it resets the
%% state instead of removing it.
discard_tx(Tx = {dgen_mem_tx, Name, Ref}) ->
    cancel_watches(Tx),
    ets:delete(tx_tab(Name), Ref),
    ok.

%% ---------------------------------------------------------------------------
%% Directory layer
%% ---------------------------------------------------------------------------
%%
%% A directory is an `erlfdb_subspace` over a prefix this module chooses. We reuse
%% erlfdb's subspace and tuple modules, which need no NIF. That makes the key
%% layout byte-for-byte the same as the FoundationDB backend's, so key order is
%% identical, not just similar. The FIFO in `dgen_queue` depends on that order.
%%
%% Prefixes come from the directory name rather than from a counter, so the same
%% directory name always gives the same keyspace. That is simpler than FDB's
%% allocator, and it makes runs easier to replay.

-spec dir_range(dgen_backend:dir(), dgen_backend:tuple_key()) ->
    {dgen_backend:key(), dgen_backend:key()}.
dir_range(Dir, TupleKey) -> erlfdb_subspace:range(Dir, TupleKey).

-spec dir_pack(dgen_backend:dir(), dgen_backend:tuple_key()) -> dgen_backend:key().
dir_pack(Dir, TupleKey) -> erlfdb_subspace:pack(Dir, TupleKey).

-spec dir_pack_vs(dgen_backend:dir(), dgen_backend:tuple_key()) -> dgen_backend:key().
dir_pack_vs(Dir, TupleKey) -> erlfdb_subspace:pack_vs(Dir, TupleKey).

-spec dir_unpack(dgen_backend:dir(), dgen_backend:key()) -> dgen_backend:tuple_key().
dir_unpack(Dir, PackedKey) -> erlfdb_subspace:unpack(Dir, PackedKey).

-spec key_strinc(dgen_backend:key()) -> dgen_backend:key().
key_strinc(Key) -> erlfdb_key:strinc(Key).

-spec dir_create(db(), dgen_backend:dir(), term()) -> dgen_backend:dir().
dir_create(_Db, Dir, Name) ->
    erlfdb_subspace:create({to_bin(Name)}, erlfdb_subspace:key(Dir)).

-spec dir_remove(db(), dgen_backend:dir(), term()) -> ok.
dir_remove(Db = {dgen_mem_db, _}, Dir, Name) ->
    Child = dir_create(Db, Dir, Name),
    {S, E} = erlfdb_subspace:range(Child),
    transactional(Db, fun(Tx) -> clear_range(Tx, S, E) end),
    ok.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).

-if(?DOCATTRS).
-doc """
Opens a database and a root directory, like `dgen_erlfdb:sandbox_open/2`. This
lets `DGen.Case` swap backends without knowing which one it has.
""".
-endif.
-spec sandbox_open(term(), term()) -> dgen_backend:tenant().
sandbox_open(Name, DirName) ->
    Db = open(sandbox_name(Name)),
    Root = erlfdb_subspace:create({}, <<>>),
    {Db, dir_create(Db, Root, to_bin(DirName))}.

sandbox_name(Name) when is_atom(Name) -> Name;
sandbox_name(Name) -> list_to_atom("dgen_mem_" ++ lists:flatten(io_lib:format("~s", [Name]))).
