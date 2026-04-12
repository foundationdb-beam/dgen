-module(dgen_registry).
-behaviour(supervisor).

%% ---------------------------------------------------------------------------
%% Process registry — OTP `{via, dgen_registry, {RegistryName, LogicalName}}`
%%
%% This module implements the four-function contract required for OTP `via`
%% tuples:
%%
%%   register_name/2    — consistent write through the leader
%%   unregister_name/1  — consistent delete through the leader
%%   whereis_name/1     — snapshot read from the local member's map
%%   send/2             — routes a message to a named process
%%
%% Name terms
%% ----------
%% Every `via` name is a two-tuple `{RegistryName, LogicalName}` where
%% `RegistryName` identifies which registry to use (the one started with
%% `start_link/2`), and `LogicalName` is any term meaningful to the
%% application (atom, binary, tuple, …).
%%
%% Example
%% -------
%%   {ok, _} = dgen_registry:start_link(my_registry, Tenant),
%%
%%   %% Start a gen_server registered in the dgen registry
%%   gen_server:start_link({via, dgen_registry, {my_registry, user_service}},
%%                         my_server, [], []),
%%
%%   %% Call it from anywhere on the cluster
%%   gen_server:call({via, dgen_registry, {my_registry, user_service}}, ping).
%%
%% Consistency model
%% -----------------
%% All writes (`register_name`, `unregister_name`) and consistent reads
%% (`whereis_name_consistent`) go through the elected leader member process.
%% The leader is the single writer for the name table, backed by FDB.
%%
%% `whereis_name/1` (used by the OTP via-tuple machinery) is a snapshot read
%% served from the local member's in-memory map — no network hop, no FDB
%% round-trip.  This map is kept in sync by one-way replication casts from
%% the leader.  There is therefore a short window after registration where a
%% remote node's `whereis_name/1` may still return `undefined`; this is the
%% same eventual-consistency trade-off as `gproc` global mode.
%%
%% Leadership
%% ----------
%% The elected leader is the member process on the Erlang node that most
%% recently committed a FDB transaction for the elector queue — i.e., whoever
%% wins the FDB consensus.  When leadership changes, the elector sets a
%% distributed lock in FDB so all elector consumers pause while replication
%% paths are reconfigured.  The new leader reads the authoritative name
%% snapshot from FDB on assumption and broadcasts it to all followers.
%%
%% Auto-unregistration
%% -------------------
%% The leader monitors every registered Pid.  When a monitored process exits,
%% the leader removes the entry from FDB and propagates `{name_unregistered}`
%% to all follower members.
%% ---------------------------------------------------------------------------

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
    %% Name derivation helpers (exported for tests / introspection)
    elector_name/1,
    member_name/1
]).

-export([init/1]).

%% ---------------------------------------------------------------------------
%% Supervisor start
%% ---------------------------------------------------------------------------

-spec start_link(Name :: atom(), Tenant :: dgen_backend:tenant()) ->
    supervisor:startlink_ret().
start_link(Name, Tenant) ->
    supervisor:start_link({local, Name}, ?MODULE, {Name, Tenant}).

-spec start_link(SupName :: atom(), Name :: atom(), Tenant :: dgen_backend:tenant()) ->
    supervisor:startlink_ret().
start_link(SupName, Name, Tenant) ->
    supervisor:start_link({local, SupName}, ?MODULE, {Name, Tenant}).

%% ---------------------------------------------------------------------------
%% OTP via-tuple registry contract
%% ---------------------------------------------------------------------------

%% Registers `Pid` under `{RegistryName, LogicalName}`.
%% Routes through the local member, which forwards to the leader if needed.
%% Returns `yes` on success, `no` if the name is already taken or no leader
%% is currently elected.
-spec register_name({atom(), term()}, pid()) -> yes | no.
register_name({RegistryName, LogicalName}, Pid) ->
    gen_server:call(member_name(RegistryName), {register, LogicalName, Pid}).

%% Removes the registration for `{RegistryName, LogicalName}`.
%% Fire-and-forget: routes through the local member, which forwards to the
%% leader.  The local member also removes the entry from its own map
%% immediately so snapshot reads on this node are consistent right away.
-spec unregister_name({atom(), term()}) -> ok.
unregister_name({RegistryName, LogicalName}) ->
    gen_server:cast(member_name(RegistryName), {unregister, LogicalName}).

%% Snapshot read — served from the local member's in-memory map.
%% Never blocks on the leader or FDB.  May be slightly stale on follower nodes
%% in the brief window between a remote registration and the replication cast
%% arriving.
-spec whereis_name({atom(), term()}) -> pid() | undefined.
whereis_name({RegistryName, LogicalName}) ->
    try
        gen_server:call(member_name(RegistryName), {whereis_snapshot, LogicalName})
    catch
        exit:_ -> undefined
    end.

%% Sends `Msg` to the process registered as `Name`, returning the Pid.
%% Exits with reason `{badarg, {Name, Msg}}` if the name is not registered.
%% Called internally by gen_server/gen_event for `{via, …}` routing.
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

%% Consistent read routed through the leader.
%% Returns the authoritative Pid for `LogicalName`, or `undefined` if not
%% registered.  More expensive than `whereis_name/1` but never stale.
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

-spec get_leader(Name :: atom()) -> dgen_registry_elector:member_id() | undefined.
get_leader(Name) ->
    dgen_server:priority_call(elector_name(Name), get_leader).

-spec get_members(Name :: atom()) -> [dgen_registry_elector:member_id()].
get_members(Name) ->
    dgen_server:priority_call(elector_name(Name), get_members).

%% ---------------------------------------------------------------------------
%% Name derivation helpers
%% ---------------------------------------------------------------------------

-spec elector_name(atom()) -> atom().
elector_name(Name) ->
    list_to_atom(atom_to_list(Name) ++ "_elector").

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
    %% to it immediately after starting.
    ElectorSpec = #{
        id => elector,
        start =>
            {dgen_server, start_link, [
                {local, ElectorName},
                dgen_registry_elector,
                #{name => Name},
                [{tenant, Tenant}]
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
