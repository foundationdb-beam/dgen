-module(dgen_registry).
-behaviour(supervisor).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
OTP-compatible process registry implementing the `{via, dgen_registry, {RegistryName, LogicalName}}` contract.

Standard OTP processes (`gen_server`, `gen_statem`, `gen_event`, etc.) can be registered
and addressed by name across an Erlang cluster.

This module implements the four-function contract required for OTP `via` tuples:

- `register_name/2` — consistent write through the leader
- `unregister_name/1` — consistent delete through the leader
- `whereis_name/1` — snapshot read from the local member's map
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

## Consistency model

All writes (`register_name`, `unregister_name`) and consistent reads
(`whereis_name_consistent`) go through the elected leader member process.
The leader is the single writer for the name table.

`whereis_name/1` (used by the OTP via-tuple machinery) is a snapshot read
served from the local member's in-memory map — no network hop, no backend
round-trip. This map is kept in sync by one-way replication casts from
the leader. There is therefore a short window after registration where a
remote node's `whereis_name/1` may still return `undefined`; this is the
same eventual-consistency trade-off as `gproc` global mode.

## Leadership

The elected leader is the member process on the Erlang node that most
recently committed a backend transaction for the elector queue — i.e.,
whoever wins the backend consensus. When leadership changes, the elector
sets a distributed lock so all elector consumers pause while replication
paths are reconfigured. The new leader broadcasts a name snapshot to all
followers on assumption.

## Auto-unregistration

The leader monitors every registered Pid. When a monitored process exits,
the leader removes the entry and propagates `{name_unregistered}` to all
follower members.
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
    %% Consistent (leader-routed) name lookup
    whereis_name_consistent/1,
    %% Convenience queries
    get_leader/1,
    get_members/1,
    get_epoch/1,
    %% Name derivation helpers (exported for tests / introspection)
    elector_name/1,
    member_name/1
]).

-export([init/1]).

%% ---------------------------------------------------------------------------
%% Supervisor start
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Starts the registry supervisor registered as `Name`.".
-endif.
-spec start_link(Name :: atom(), Tenant :: dgen_backend:tenant()) ->
    supervisor:startlink_ret().
start_link(Name, Tenant) ->
    supervisor:start_link({local, Name}, ?MODULE, {Name, Tenant}).

-if(?DOCATTRS).
-doc "Starts the registry supervisor registered as `SupName`, using `Name` as the registry name.".
-endif.
-spec start_link(SupName :: atom(), Name :: atom(), Tenant :: dgen_backend:tenant()) ->
    supervisor:startlink_ret().
start_link(SupName, Name, Tenant) ->
    supervisor:start_link({local, SupName}, ?MODULE, {Name, Tenant}).

%% ---------------------------------------------------------------------------
%% OTP via-tuple registry contract
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Registers `Pid` under `{RegistryName, LogicalName}`.

Routes through the local member, which forwards to the leader if needed.
Returns `yes` on success, `no` if the name is already taken or no leader
is currently elected.
""".
-endif.
-spec register_name({atom(), term()}, pid()) -> yes | no.
register_name({RegistryName, LogicalName}, Pid) ->
    gen_server:call(member_name(RegistryName), {register, LogicalName, Pid}).

-if(?DOCATTRS).
-doc """
Removes the registration for `{RegistryName, LogicalName}`.

Fire-and-forget: routes through the local member, which forwards to the
leader. The local member also removes the entry from its own map
immediately so snapshot reads on this node are consistent right away.
""".
-endif.
-spec unregister_name({atom(), term()}) -> ok.
unregister_name({RegistryName, LogicalName}) ->
    gen_server:cast(member_name(RegistryName), {unregister, LogicalName}).

-if(?DOCATTRS).
-doc """
Snapshot read — served from the local member's in-memory map.

Never blocks on the leader or the backend. May be slightly stale on follower
nodes in the brief window between a remote registration and the replication
cast arriving.
""".
-endif.
-spec whereis_name({atom(), term()}) -> pid() | undefined.
whereis_name({RegistryName, LogicalName}) ->
    try
        gen_server:call(member_name(RegistryName), {whereis_snapshot, LogicalName})
    catch
        exit:_ -> undefined
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
        gen_server:call(member_name(RegistryName), {whereis, LogicalName})
    catch
        exit:_ -> undefined
    end.

%% ---------------------------------------------------------------------------
%% Convenience queries (bypass durable queue, always fresh)
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Returns the current leader member id, or `undefined` if no leader is elected.".
-endif.
-spec get_leader(Name :: atom()) -> dgen_registry_elector:member_id() | undefined.
get_leader(Name) ->
    dgen_server:priority_call(elector_name(Name), get_leader).

-if(?DOCATTRS).
-doc "Returns the current leader epoch. Increments each time a new leader is elected.".
-endif.
-spec get_epoch(Name :: atom()) -> non_neg_integer().
get_epoch(Name) ->
    dgen_server:priority_call(elector_name(Name), get_epoch).

-if(?DOCATTRS).
-doc "Returns the list of all current member ids in the registry.".
-endif.
-spec get_members(Name :: atom()) -> [dgen_registry_elector:member_id()].
get_members(Name) ->
    dgen_server:priority_call(elector_name(Name), get_members).

%% ---------------------------------------------------------------------------
%% Name derivation helpers
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Returns the registered name of the elector process for the given registry name.".
-endif.
-spec elector_name(atom()) -> atom().
elector_name(Name) ->
    list_to_atom(atom_to_list(Name) ++ "_elector").

-if(?DOCATTRS).
-doc "Returns the registered name of the member process for the given registry name.".
-endif.
-spec member_name(atom()) -> atom().
member_name(Name) ->
    list_to_atom(atom_to_list(Name) ++ "_member").

%% ---------------------------------------------------------------------------
%% Supervisor callbacks
%% ---------------------------------------------------------------------------

init({Name, Tenant}) ->
    ElectorName = elector_name(Name),
    MemberName = member_name(Name),

    %% Elector must start first: the member's init casts `{join, MemberId}`
    %% to it immediately after starting. A large consume_k is chosen
    %% because the in-transaction work is very small, and we want to keep
    %% a single node as the sole consumer as much as possible, to avoid
    %% leadership churn. @todo: Also, we need to turn off inlining
    ElectorSpec = #{
        id => elector,
        start =>
            {dgen_server, start_link, [
                {local, ElectorName},
                dgen_registry_elector,
                #{name => Name},
                [{tenant, Tenant}, {consume_k, 50}, {lock_timeout, 6000}]
            ]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [dgen_server, dgen_registry_elector]
    },

    MemberSpec = #{
        id => member,
        start =>
            {dgen_registry_member, start_link, [
                MemberName,
                #{
                    elector => ElectorName,
                    member_name => MemberName
                }
            ]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [dgen_registry_member]
    },

    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, [ElectorSpec, MemberSpec]}}.
