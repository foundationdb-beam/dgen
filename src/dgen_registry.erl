-module(dgen_registry).
-behaviour(supervisor).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("../include/dgen_eta.hrl").

-if(?DOCATTRS).
-moduledoc """
OTP-compatible process registry implementing the `{via, dgen_registry, {RegistryName, LogicalName}}` contract.

Standard OTP processes (`gen_server`, `gen_statem`, `gen_event`, etc.) can be registered
and addressed by name across an Erlang cluster.

See `docs/dgen_registry_design.md` for the full design, guarantees, and non-goals;
section references (e.g. `§5.7`) in the registry modules point there.

This module implements the four-function contract required for OTP `via` tuples:

- `register_name/2` — consistent write through the leader
- `unregister_name/1` — consistent delete through the leader
- `whereis_name/1` — lock-free snapshot read from the local member's ETS table
- `send/2` — routes a message to a named process

## Name terms

Every `via` name is a two-tuple `{RegistryName, LogicalName}` where
`RegistryName` identifies which registry to use (the one started with
`start_link/2`), and `LogicalName` is any term meaningful to the
application (atom, binary, tuple, …).

## Example

```erlang
{ok, _} = dgen_registry:start_link(my_registry, Tenant),

%% Start a gen_server registered in the dgen registry
gen_server:start_link({via, dgen_registry, {my_registry, user_service}},
                      my_server, [], []),

%% Call it from anywhere on the cluster
gen_server:call({via, dgen_registry, {my_registry, user_service}}, ping).
```

## Process model

`dgen_registry` is a supervisor that manages a set of children described below. When
your application starts a `dgen_registry`, you provide a `RegistryName`, which must
be an atom. Among your clustered nodes, this atom identifies the grouping of
`dgen_registry` processes into a single clustered entity. When the documentation refers
to "the dgen_registry" it will typically be referring to this cluster of processes,
rather than a singlular Erlang process, unless otherwise specified.

- `dgen_registry_elector`: A `dgen_server` process that decides which `dgen_registry`
  is the leader by using the underlying database for consensus. There can be only
  one leader per named registry.
- `dgen_registry_connector`: A `gen_server` process that automatically connects your nodes
  together in the Erlang distribution so that they can each rpc to each other.
- `dgen_registry_member`: A `gen_statem` process that stores the actual process map
  in its in-memory state, and is responsible for implementation the replication of
  that state among the set of leader/followers.

The `dgen_registry` process is not registered by name in the local process registry.
Instead, you must use the functions in this module or the via contract when using
the registry.

**Implementation detail:** The `dgen_registry_member` **is** registered in the local
process registry. This is an internal implementation detail and should not be used
outside of `dgen`.

## Consistency model

**Strongly consistent operations**: All writes (`register_name`, `unregister_name`) and consistent reads
(`whereis_name_consistent`) go through the elected leader member process.
The leader is the single writer for the name table.

**Eventually consistent operations**: `whereis_name/1` (used by the OTP via-tuple machinery) is a **snapshot read**
served lock-free from the local member's `protected` ETS table, in the calling
process. The table is kept in sync by one-way replication casts from the leader.
There is therefore a short window after registration where a remote node's
`whereis_name/1` may still return `undefined`.

## Leadership

Leadership election takes advantage of the distributed consensus of the underlying
database, FoundationDB, and is thus simple. The leader is determined by the Erlang
node that processes the "elect leader" message on the `dgen_server`. Since the `dgen_server`
message queue is stored in the database, we are guaranteed that one and only one
Erlang node will successfully dequeue that message from the queue. That winning node is
the registry leader.

When leadership changes, the new leader gathers state from itself (previously a follower)
and from other current followers before allowing any new operations on the registry.
There is no distributed lock. Instead, the leader-key in the database fences the
old leader. Via the leader-key all participating members agree on what the cluster
is doing. (§5.7 of the design doc).

## Auto-unregistration

The leader monitors every registered Pid. When a monitored process exits,
the leader removes the entry and propagates `{name_unregistered}` to all
follower members.

In the time period between a process exiting and the registry leader processing
the `'DOWN'`, the registry will still return that process's former mapping.
Once a `'DOWN'` is converted into an unregistry operation, then the process map's
consistency guarantees hold. If you need stronger guarantees on process termination,
you may have the process unregister itself at the opportune time.

## Metadata and queries

Each process registration can optionally provide metadata, decided by your application.
Process metadata can be used as a simple data store whose lifetime is tied to
the registered process itself.

The metadata can be specified as an `index` map, which is queryable and/or an opaque `data`
payload, which is not queryable, but is retrievable.

Refer to the following functions for more detail:

  - `register_name/3`
  - `set_metadata/2`
  - `get_metadata/1`/`get_metadata_consistent/1`
  - `query/2`/`query_consistent/2`

Also see §4.7 of the design doc.

## Presence

`subscribe/4` / `unsubscribe/2` build upon the queryable metadata. A subscription is
a **durable** query, stored in the elector's backend state, that sends Erlang messages
when metadata inside the registry changes.

Put another way, a subscription watches one query and notifies (via a second query) whenever
the set of processes matching the watched query changes.

  - `{dgen_presence, SubId, [{joined | left, Name, Pid}]}`

We expect the typical use case of a subscription to be one that is tied to the lifetime of
some database entry of your application, and thus its existence out-lives any in-memory
process registry map. In other words, if your application scales to zero, it can scale back
up and the subscriptions will survive.

See §4.9 of the design doc.

### Example

For example, consider a chat application where a Room is created in the database, and
active WebSocket connections (each an Erlang process) correspond to who is online/present
at a given moment.

```erlang
% Write a database entity for a Room with primary key `Id`
% ...

% Store the subscription with the same `Id` (unsubscribe later when the Room is deleted)
ok = dgen_registry:subscribe(chat_registry, Id,
  #{in_room => Id},  % <- This query watches for changes
  #{in_room => Id}   % <- This query finds which processes to notify
).

% In the WebSocket process
yes = dgen_registry:register_name({chat_registry, ref}, self()).

dgen_registry:set_metadata(
  {chat_registry, self()},
  #{index => #{in_room => Id}} % <- This metadata satisfies the queries above.
).
```

Now, all WebSocket processes in the Room are notified of all other WebSocket processes
in the same room via the `dgen_presence` message.
""".
-endif.

-export([
    %% Supervisor start
    start_link/2,
    start_link/3,
    %% OTP via-tuple registry contract
    register_name/2,
    unregister_name/1,
    whereis_name/1,
    send/2,
    %% Registration metadata
    register_name/3,
    set_metadata/2,
    get_metadata/1,
    get_metadata_consistent/1,
    %% Indexed metadata queries
    query/2,
    query_consistent/2,
    %% Presence: durable watch/notify subscriptions
    subscribe/4,
    unsubscribe/2,
    unsubscribe_all/1,
    subscriptions/1,
    %% Teardown / durable-state cleanup
    delete/2,
    %% Consistent (leader-routed) name lookup
    whereis_name_consistent/1,
    %% Convenience queries
    get_leader/1,
    get_members/1,
    get_epoch/1,
    %% Readiness and introspection
    ready/1,
    await_ready/1,
    await_ready/2,
    status/1,
    %% Process identity helpers (exported for tests / introspection)
    member_name/1,
    names_table/1,
    elector_pid/1
]).

-export([init/1]).

-export_type([member_id/0, registry_opts/0, query/0, presence_event/0]).

%% A member identifier is the pair of the Erlang node and the registry name.
-type member_id() :: {node(), atom()}.

%% Per-registry tuning options passed to `start_link/3`.
-type registry_opts() :: #{atom() => term()}.

%% A metadata query (`subscribe/4`): AND-equal `{all, Map}`, or a bare Map meaning that.
-type query() :: {'all', #{term() => term()}} | #{term() => term()}.

%% A name entered (`joined`) or left (`left`) a watch query's result set.
-type presence_event() :: {joined | left, Name :: term(), Pid :: pid()}.

%% ---------------------------------------------------------------------------
%% Supervisor start
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Starts the registry `Name` with default options. See `start_link/3` for per-registry tuning.".
-endif.
-spec start_link(Name :: atom(), Tenant :: dgen_backend:tenant() | function) ->
    supervisor:startlink_ret().
start_link(Name, Tenant) ->
    start_link(Name, Tenant, #{}).

-if(?DOCATTRS).
-doc """
Starts the registry `Name` with per-registry options `Opts`.

`Opts` is a map of tuning knobs scoped to *this* registry — `register_replicas`,
`replicate_timeout`, `strict_replication`, `terminate_on_conflict`,
`conflict_kill_budget`, `reject_when_degraded`, `connectivity`. Each unset key falls
back to the `dgen` application environment, then to a built-in default, so different
registries can be tuned independently. `connectivity => provided_externally` turns off
this registry's proactive distribution mesh so it free-rides on connections another
registry maintains (§4.6) — the rest default to a self-sufficient, single-fault-tolerant
registry. See `docs/dgen_registry_design.md` (§8 Configuration).

The supervisor itself is **not** registered under a name. See the
"Process model" note above.

## `keyspace` — separating identity from the coordination keyspace

The `keyspace` option will typically not be used by your application code. It exists
to allow more than one registry of the same name per Erlang node, which is not advised
outside of dgen's own test suites.

`Name` is used in the following ways:

  1. The member's **identity**: the registered process name, the ETS table name,
     and the second element of its `member_id/0`.
  2. The registry's **keyspace**: the tenant-unique-identifier (tuid) prefixing
     every durable key — the elector queue, the leader key, the version key.

The optional `keyspace` option splits the second role out:

```erlang
dgen_registry:start_link(member_a, Tenant, #{keyspace => shared_registry}),
dgen_registry:start_link(member_b, Tenant, #{keyspace => shared_registry}).
```
""".
-endif.
-spec start_link(
    Name :: atom(), Tenant :: dgen_backend:tenant() | function(), Opts :: registry_opts()
) ->
    supervisor:startlink_ret().
start_link(Name, Tenant, Opts) ->
    Tenant2 =
        if
            is_function(Tenant) -> Tenant();
            true -> Tenant
        end,
    supervisor:start_link(?MODULE, {Name, Tenant2, Opts}).

%% ---------------------------------------------------------------------------
%% OTP via-tuple registry contract
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Registers `Pid` under `{RegistryName, LogicalName}`.

Routes through the local member, which forwards to the leader if needed.
Returns `yes` on success, or `no` if a *different* live process holds the name
or an opt-in policy refuses it (§8). If no leader is reachable, the call waits
for one, and exits if nothing is decided within `register_timeout` (§3).

Registering the **same** pid under the **same** name again is an idempotent
`yes` (not a `no`): a caller whose register call timed out — but whose
registration in fact committed — can safely redrive it and get a decisive
`yes` for its own binding. See `register_name/3` for the timeout contract.
""".
-endif.
-spec register_name({atom(), term()}, pid()) -> yes | no.
register_name(Name, Pid) ->
    register_name(Name, Pid, #{}).

-if(?DOCATTRS).
-doc """
Removes the registration for `{RegistryName, LogicalName}`. Always returns `ok`.
""".
-endif.
-spec unregister_name({atom(), term()}) -> ok.
unregister_name({RegistryName, LogicalName}) ->
    try
        gen_statem:call(
            member_name(RegistryName),
            {unregister, LogicalName},
            %% Same caller-side bound as set_metadata/2.
            dgen_config:register_timeout(#{})
        )
    catch
        %% noproc: nothing to unregister; timeout: the member re-drives it. `ok` either way.
        exit:_ -> ok
    end.

-if(?DOCATTRS).
-doc """
Finds a process by its name in the registry via a snapshot read, using the local member's ETS table.
""".
-endif.
-spec whereis_name({atom(), term()}) -> pid() | undefined.
whereis_name({RegistryName, LogicalName}) ->
    try ets:lookup(names_table(RegistryName), LogicalName) of
        [{_Name, Pid, _Index, _Data} | _] -> Pid;
        [] -> undefined
    catch
        %% No table yet (member (re)starting): not registered.
        error:badarg -> undefined
    end.

-if(?DOCATTRS).
-doc """
Sends `Msg` to the process registered as `Name`, returning the Pid.

Exits with reason `{badarg, {Name, Msg}}` if the name is not registered.
Called internally by gen_server/gen_event for `{via, …}` routing.
""".
-endif.
-spec send({atom(), term()}, term()) -> pid().
send(Name, Msg) ->
    case whereis_name(Name) of
        undefined ->
            exit({badarg, {Name, Msg}});
        Pid ->
            Pid ! Msg,
            Pid
    end.

%% ---------------------------------------------------------------------------
%% Registration metadata
%% ---------------------------------------------------------------------------
%% Metadata lives exactly as long as its registration (§4.7).

-if(?DOCATTRS).
-doc """
Registers `Pid` under `{RegistryName, LogicalName}` with metadata.

`MetaSpec` is `#{index => map(), data => term()}` (both optional; defaults `#{}` and
`undefined`). Returns `yes` or `no` with the same meaning as in `register_name/2`.

A call **timeout** (the member, or the leader behind it, did not answer within
`register_timeout`, §8 — default 5000 ms) **exits** rather than answering `no`.
""".
-endif.
-spec register_name({atom(), term()}, pid(), map()) -> yes | no.
register_name({RegistryName, LogicalName}, Pid, MetaSpec) ->
    gen_statem:call(
        member_name(RegistryName),
        {register, LogicalName, Pid, meta_of(MetaSpec)},
        dgen_config:register_timeout(#{})
    ).

-if(?DOCATTRS).
-doc """
Replaces the metadata of the existing registration `{RegistryName, LogicalName}`.

`MetaSpec` is `#{index => map(), data => term()}` (both optional; an omitted field
becomes `#{}` / `undefined` — this is a replace, not a merge).

Returns `ok`, `{error, not_registered}` if the name is not currently bound, or
`{error, no_leader}` if no leader is elected.
""".
-endif.
-spec set_metadata({atom(), term()}, map()) -> ok | {error, not_registered | no_leader}.
set_metadata({RegistryName, LogicalName}, MetaSpec) ->
    {Index, Data} = meta_of(MetaSpec),
    try
        gen_statem:call(
            member_name(RegistryName),
            {set_metadata, LogicalName, Index, Data},
            %% register_name/2,3's knob, not call/2's hidden 5s: same write pipeline.
            dgen_config:register_timeout(#{})
        )
    catch
        exit:_ -> {error, no_leader}
    end.

-if(?DOCATTRS).
-doc """
Retrieves a registered process's metadata via a snapshot read, using the local member's ETS table.

Returns `{ok, #{pid := pid(), index := map(), data := term()}}` for a registered name,
or `undefined` if it is not registered.
""".
-endif.
-spec get_metadata({atom(), term()}) ->
    {ok, #{pid := pid(), index := map(), data := term()}} | undefined.
get_metadata({RegistryName, LogicalName}) ->
    try ets:lookup(names_table(RegistryName), LogicalName) of
        [{_Name, Pid, Index, Data} | _] -> {ok, #{pid => Pid, index => Index, data => Data}};
        [] -> undefined
    catch
        error:badarg -> undefined
    end.

-if(?DOCATTRS).
-doc """
Retrieves a registered process's metadata via a consistent read.

Returns `{ok, #{pid := pid(), index := map(), data := term()}}` for a registered name,
or `undefined` if it is not registered.
""".
-endif.
-spec get_metadata_consistent({atom(), term()}) ->
    {ok, #{pid := pid(), index := map(), data := term()}} | undefined.
get_metadata_consistent({RegistryName, LogicalName}) ->
    try
        gen_statem:call(member_name(RegistryName), {get_metadata, LogicalName})
    catch
        exit:_ -> undefined
    end.

-if(?DOCATTRS).
-doc """
Finds every registration whose indexed metadata satisfies *all* of `Constraints`, via a snapshot read.

`Constraints` is `#{attr() => value()}` — a conjunction of exact equalities (AND-equal)
matched against each registration's `index` map. Returns a point-in-time list of
`#{name, pid, index, data}` matches (possibly empty), or `{error, empty_query}` if
`Constraints` is empty.

This is a **batch-consistent snapshot** read of the local member's replica: it runs on
the member's mailbox (unlike the lock-free single-key reads) so it never observes a
half-applied group commit, but it may still lag a change made on another node. A
returned pid may already have died — treat results as candidates, not liveness
guarantees.
""".
-endif.
-spec query(atom(), map()) ->
    [#{name := term(), pid := pid(), index := map(), data := term()}] | {error, empty_query}.
query(_RegistryName, Constraints) when map_size(Constraints) =:= 0 ->
    {error, empty_query};
query(RegistryName, Constraints) ->
    try
        gen_statem:call(member_name(RegistryName), {query, Constraints})
    catch
        exit:_ -> []
    end.

-if(?DOCATTRS).
-doc """
Finds every registration whose indexed metadata satisfies *all* of `Constraints`, via a consistent read.

Returns a point-in-time list of `#{name, pid, index, data}` matches (possibly empty),
or `{error, empty_query}` if `Constraints` is empty.
""".
-endif.
-spec query_consistent(atom(), map()) ->
    [#{name := term(), pid := pid(), index := map(), data := term()}] | {error, empty_query}.
query_consistent(_RegistryName, Constraints) when map_size(Constraints) =:= 0 ->
    {error, empty_query};
query_consistent(RegistryName, Constraints) ->
    try
        gen_statem:call(member_name(RegistryName), {query_consistent, Constraints})
    catch
        exit:_ -> []
    end.

%% ---------------------------------------------------------------------------
%% Presence: durable watch/notify subscriptions
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Creates (or updates) a **durable** presence subscription `SubId`.

A subscription keeps the processes matching the `Notify` query up to date with the set
of processes matching the `Watch` query.

`Watch` and `Notify` are `query/0` values — a `{all, #{attr => value}}` conjunction of
exact equalities over registration `index` metadata (a bare map is accepted and read as
`{all, Map}`). `SubId` is any application term; a subscribe with an existing `SubId` is
an idempotent **upsert** (replaces its queries).

Whenever a committed batch of registrations changes which processes satisfy `Watch`,
the registry sends the following message to **every process currently matching `Notify`**:

```erlang
{dgen_presence, SubId, Events}
```

where `Events` is a non-empty list of `presence_event/0` — `{joined, Name, Pid}` /
`{left, Name, Pid}`.

When a process is registerd that matches the notify set, it received an **initial snapshot**,
the current `Watch` set as a batch of `joined` events. Thereafter, it receives deltas.
The initial snapshot and the deltas combine to a complete, continuously-maintained view
of the set of "present" processes.

## Durability

The subscription is stored in the elector's **durable** backend state, not tied to any
Erlang process: it outlives the registry cluster. An application can therefore tie a
subscription's lifetime to a database entity (subscribe when the entity is created,
`unsubscribe/2` when it is deleted) and have presence come back intact after a
scale-to-zero — the durable subscription is reloaded and re-fed to the leader on
restart, and processes matching `Notify` resume receiving updates as they re-register.

Returns `ok`, or `{error, empty_query}` if either query is empty (validated locally).
The write is sent to the elector as a durable `dgen_server:cast`.
""".
-endif.
-spec subscribe(atom(), term(), query(), query()) -> ok | {error, empty_query}.
subscribe(RegistryName, SubId, Watch, Notify) ->
    {all, W} = NWatch = normalize_query(Watch),
    {all, N} = NNotify = normalize_query(Notify),
    case map_size(W) =:= 0 orelse map_size(N) =:= 0 of
        true ->
            {error, empty_query};
        false ->
            with_elector(RegistryName, fun(Pid) ->
                dgen_registry_elector:subscribe(Pid, SubId, NWatch, NNotify)
            end)
    end.

-if(?DOCATTRS).
-doc """
Removes the durable presence subscription `SubId` (from `subscribe/4`). Returns `ok`.
Sent as a durable `dgen_server:cast`, applied asynchronously (see `subscribe/4`).
""".
-endif.
-spec unsubscribe(atom(), term()) -> ok.
unsubscribe(RegistryName, SubId) ->
    with_elector(RegistryName, fun(Pid) -> dgen_registry_elector:unsubscribe(Pid, SubId) end).

-if(?DOCATTRS).
-doc """
Removes **all** durable presence subscriptions from the registry. Returns `ok`.
""".
-endif.
-spec unsubscribe_all(atom()) -> ok.
unsubscribe_all(RegistryName) ->
    with_elector(RegistryName, fun(Pid) -> dgen_registry_elector:unsubscribe_all(Pid) end).

-if(?DOCATTRS).
-doc """
Returns the registry's durable presence subscriptions as `#{SubId => {Watch, Notify}}`.
""".
-endif.
-spec subscriptions(atom()) -> #{term() => {query(), query()}}.
subscriptions(RegistryName) ->
    with_elector(RegistryName, #{}, fun(Pid) ->
        dgen_registry_elector:priority_get_subscriptions(Pid)
    end).

with_elector(Name, Default, Fun) ->
    case elector_pid(Name) of
        undefined -> Default;
        Pid -> Fun(Pid)
    end.

with_elector(Name, Fun) ->
    case elector_pid(Name) of
        undefined -> exit({noproc, {?MODULE, elector, Name}});
        Pid -> Fun(Pid)
    end.

%% ---------------------------------------------------------------------------
%% Teardown / durable-state cleanup
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Removes **all** of the registry's durable backend state. Returns `ok`.

**Stop the registry first.** `delete/2` operates directly on the backend and assumes no
live processes: a running elector caches its state in memory and would re-write it (and
so re-create the keys) on its next commit.

This does not delete anything the registered processes themselves own; it only removes
the registry's own bookkeeping.
""".
-endif.
-spec delete(atom(), dgen_backend:tenant()) -> ok.
delete(Name, Tenant) ->
    Tuid = dgen_registry_elector:tuid(Name),
    Backend = dgen_config:backend(),
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        {StartKey, EndKey} = Backend:dir_range(Dir, Tuid),
        Backend:clear_range(Tx, StartKey, EndKey)
    end),
    ok.

%% A bare constraints map means `{all, Map}`.
-spec normalize_query(query()) -> {'all', map()}.
normalize_query({all, C} = Q) when is_map(C) -> Q;
normalize_query(C) when is_map(C) -> {all, C}.

-spec meta_of(map()) -> {map(), term()}.
meta_of(MetaSpec) ->
    {maps:get(index, MetaSpec, #{}), maps:get(data, MetaSpec, undefined)}.

%% ---------------------------------------------------------------------------
%% Consistent name lookup (leader-routed)
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Consistent read routed through the leader.

Returns the authoritative Pid for `LogicalName`, or `undefined` if not
registered. More expensive than `whereis_name/1` but never stale.
""".
-endif.
-spec whereis_name_consistent({atom(), term()}) -> pid() | undefined.
whereis_name_consistent({RegistryName, LogicalName}) ->
    try
        gen_statem:call(member_name(RegistryName), {whereis, LogicalName})
    catch
        exit:_ -> undefined
    end.

%% ---------------------------------------------------------------------------
%% Convenience queries (bypass durable queue, always fresh)
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Returns the current leader member id, or `undefined` if no leader is elected.".
-endif.
-spec get_leader(Name :: atom()) -> member_id() | undefined.
get_leader(Name) ->
    with_elector(Name, undefined, fun(Pid) -> dgen_registry_elector:priority_get_leader(Pid) end).

-if(?DOCATTRS).
-doc "Returns the current leader epoch. Increments each time a new leader is elected.".
-endif.
-spec get_epoch(Name :: atom()) -> non_neg_integer().
get_epoch(Name) ->
    with_elector(Name, 0, fun(Pid) -> dgen_registry_elector:priority_get_epoch(Pid) end).

%% Poll interval and per-probe call bound for await_ready/2.
-define(READY_POLL_INTERVAL, 50).
-define(READY_CALL_TIMEOUT, 1000).

-if(?DOCATTRS).
-doc """
Returns `true` if the registry on this node is ready to serve registrations.

This is a point-in-time check; use `await_ready/2` to block until ready. Note that
readiness means registrations are being *served*, not that any particular
registration will return `yes`.
""".
-endif.
-spec ready(Name :: atom()) -> boolean().
ready(Name) ->
    try gen_statem:call(member_name(Name), ready, ?READY_CALL_TIMEOUT) of
        Ready when is_boolean(Ready) -> Ready;
        _ -> false
    catch
        %% noproc (starting) or timeout (busy): not ready yet.
        exit:_ -> false
    end.

-if(?DOCATTRS).
-doc """
Reports this node's member's own view of the registry, as a map:

- `member_id` — this member's `member_id/0`.
- `leader` — the member id it currently believes leads, or `undefined`.
- `is_leader` — `true` if the member believes itself to be the leader.
- `epoch` — the leadership epoch it's aware of (§4.2).
- `synced` — whether it has applied a snapshot or assumed leadership.
- `applied_version` — the commit version its replica has applied up to (§4.5).

This is a member's **belief**, which is deliberately not the same thing as
`get_leader/1`'s committed answer from the elector: a deposed leader that has not
yet heard about the handoff still reports `is_leader => true` here while
`get_leader/1` already names its successor. That gap is the fenced window of §5.1,
and seeing it is the point of this call.

Side-effect free — a plain state read, useful for operational introspection and for
asserting replication invariants across members. Returns `undefined` if the member
is not running.
""".
-endif.
-spec status(Name :: atom()) -> map() | undefined.
status(Name) ->
    try gen_statem:call(member_name(Name), status, ?READY_CALL_TIMEOUT) of
        Status when is_map(Status) -> Status;
        _ -> undefined
    catch
        exit:_ -> undefined
    end.

-if(?DOCATTRS).
-doc "Blocks until `ready/1` holds, or 5s elapses. See `await_ready/2`.".
-endif.
-spec await_ready(Name :: atom()) -> ok | {error, timeout}.
await_ready(Name) ->
    await_ready(Name, 5000).

-if(?DOCATTRS).
-doc """
Blocks until the registry on this node is ready to serve registrations, or `Timeout`
(milliseconds) elapses.

Returns `ok` once `ready/1` holds, or `{error, timeout}` if the deadline passes first.
Intended for a node that has just started/joined a cluster: it waits out leader
election and the join handoff (the leader gathers and delivers the names snapshot)
before the caller starts registering, so registrations are not fast-rejected against a
not-yet-known leader. Safe to call from many nodes; it only reads local member state.
""".
-endif.
-spec await_ready(Name :: atom(), Timeout :: non_neg_integer()) -> ok | {error, timeout}.
await_ready(Name, Timeout) ->
    Deadline = dgen_utils:real_monotonic_ms() + Timeout,
    await_ready_loop(Name, Deadline).

%% Sleeps and reads the clock via dgen_utils, untouched by the eta transform: under
%% eta_run this loop runs in the driver, where a transformed sleep would never wake.
await_ready_loop(Name, Deadline) ->
    case ready(Name) of
        true ->
            ok;
        false ->
            case dgen_utils:real_monotonic_ms() < Deadline of
                true ->
                    dgen_utils:real_sleep(?READY_POLL_INTERVAL),
                    await_ready_loop(Name, Deadline);
                false ->
                    {error, timeout}
            end
    end.

-if(?DOCATTRS).
-doc "Returns the list of all current member ids in the registry.".
-endif.
-spec get_members(Name :: atom()) -> [member_id()].
get_members(Name) ->
    with_elector(Name, [], fun(Pid) -> dgen_registry_elector:priority_get_members(Pid) end).

%% ---------------------------------------------------------------------------
%% Process identity helpers
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Returns the registered name of the member process for the given registry name.
""".
-endif.
-spec member_name(atom()) -> atom().
member_name(Name) ->
    Name.

-if(?DOCATTRS).
-doc """
Returns the ETS table name holding the registry's local `Name => Pid` replica.
""".
-endif.
-spec names_table(atom()) -> atom().
names_table(Name) ->
    Name.

-if(?DOCATTRS).
-doc """
Returns the current elector `pid()` for registry `Name`, or `undefined` if the
member (and so the elector) cannot currently be found.
""".
-endif.
-spec elector_pid(atom()) -> pid() | undefined.
elector_pid(Name) ->
    case whereis(Name) of
        undefined ->
            undefined;
        MemberPid ->
            Ancestors =
                case erlang:process_info(MemberPid, dictionary) of
                    {dictionary, Dict} -> proplists:get_value('$ancestors', Dict, []);
                    undefined -> []
                end,
            case Ancestors of
                [Sup | _] ->
                    case lists:keyfind(elector, 1, supervisor:which_children(Sup)) of
                        {elector, ElectorPid, _Type, _Modules} when is_pid(ElectorPid) ->
                            ElectorPid;
                        _ ->
                            undefined
                    end;
                [] ->
                    undefined
            end
    end.

%% ---------------------------------------------------------------------------
%% Supervisor callbacks
%% ---------------------------------------------------------------------------

init({Name, Tenant, Opts}) ->
    %% Start the elector first.
    %%
    %% 1. A large consume_k keeps one stable consumer, and therefore a stable leader.
    %% 2. consume_k > 1 disables inlining.
    %% 3. By default, Keyspace is the same as Name. It cna optionally be changed to
    %%    to support dgen test suite.
    Keyspace = maps:get(keyspace, Opts, Name),
    ElectorSpec = #{
        id => elector,
        start =>
            {dgen_server, start_link, [
                dgen_registry_elector,
                #{name => Name, keyspace => Keyspace},
                [{tenant, Tenant}, {consume_k, 50}]
            ]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [dgen_server, dgen_registry_elector]
    },

    %% Then start the member. It will discover the elector via this supervisor.
    Tuid = dgen_registry_elector:tuid(Keyspace),
    MemberSpec = #{
        id => member,
        start =>
            {dgen_registry_member, start_link, [
                Name,
                #{
                    name => Name,
                    tenant => Tenant,
                    tuid => Tuid,
                    config => Opts
                }
            ]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [dgen_registry_member]
    },

    %% Finally start the node connector.
    ConnectorSpec = #{
        id => connector,
        start =>
            {dgen_registry_connector, start_link, [
                Name,
                #{name => Name, config => Opts}
            ]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [dgen_registry_connector]
    },

    %% Supervisor config
    %%
    %%   1. one_for_all: the children discover and store the elector pid, so they must
    %%      all restart in unison.
    %%   2. intensity and period: Sane choices, but they are not tuned.
    %%   3. Simulation flags: defined at compile time for DST
    {ok,
        {maps:merge(#{strategy => one_for_all, intensity => 5, period => 10}, ?SUP_SIM_FLAGS), [
            ElectorSpec, MemberSpec, ConnectorSpec
        ]}}.
