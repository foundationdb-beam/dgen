-module(dgen_registry).
-behaviour(supervisor).

%% ---------------------------------------------------------------------------
%% Process registry — OTP `{via, dgen_registry, {RegistryName, LogicalName}}`
%%
%% This module implements the four-function contract required for OTP `via`
%% tuples:
%%
%%   register_name/2    — register a Pid under a logical name
%%   unregister_name/1  — remove a registration
%%   whereis_name/1     — look up the Pid for a logical name (O(1) ETS read)
%%   send/2             — send a message to a named process
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
%% `whereis_name` reads from a local ETS table — it never touches FDB and
%% never blocks.  `register_name` writes through an FDB priority_call so FDB's
%% serialisable isolation detects simultaneous competing registrations on
%% different nodes: exactly one wins and the other returns `no`.
%%
%% ETS is kept in sync by the `dgen_registry_member` gen_server, which
%% receives broadcast casts from the elector's post-commit actions whenever a
%% name is registered or unregistered anywhere in the cluster.  The table is
%% populated from FDB on member startup.  There is therefore a short window
%% after registration where a remote node's `whereis_name` may still return
%% `undefined`; this is the same trade-off as `gproc` global mode.
%%
%% Auto-unregistration
%% -------------------
%% The member monitors every registered Pid.  When the monitored process
%% exits the member removes the ETS entry and casts `{unregister, Name}` to
%% the elector, which propagates the removal to all peers.
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
    %% Convenience
    get_leader/1,
    get_members/1,
    %% Name derivation helpers (exported for tests / introspection)
    elector_name/1,
    member_name/1,
    ets_table_name/1
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
%% Returns `yes` on success, `no` if the name is already taken.
%%
%% Consistency notes
%% -----------------
%% We route through the durable queue (`dgen_server:call`) rather than
%% `priority_call` for two reasons:
%%
%% 1. Queue ordering vs. zombie registrations.
%%    When a monitored process dies, the member casts `{unregister, Name}` to
%%    the elector queue.  A `priority_call` bypasses the queue and can
%%    therefore see the FDB state *before* that unregister is processed —
%%    returning `no` for a name that belongs to a dead process.  Using the
%%    durable queue ensures the `register` is ordered after any preceding
%%    `unregister` for the same name.
%%
%% 2. ETS lag on the registering node.
%%    The `yes/no` reply arrives from FDB before the elector's post-commit
%%    action (the `name_registered` cast) has been processed by the local
%%    member gen_server.  We therefore update the local ETS table
%%    synchronously here, immediately before returning `yes`, so that a
%%    `whereis_name` call on this node is consistent right away.
%%
%%    Remote nodes still receive the update via the elector's action cast
%%    (eventual consistency), and the ets:insert in the member is idempotent.
-spec register_name({atom(), term()}, pid()) -> yes | no.
register_name({RegistryName, LogicalName}, Pid) ->
    case dgen_server:call(elector_name(RegistryName), {register, LogicalName, Pid}) of
        yes ->
            Table = ets_table_name(RegistryName),
            try ets:insert(Table, {LogicalName, Pid})
            catch error:badarg -> ok  %% member not yet started; action will seed it
            end,
            yes;
        no ->
            no
    end.

%% Removes the registration for `{RegistryName, LogicalName}`.
%% The local ETS entry is deleted immediately (so subsequent `whereis_name`
%% calls on this node return `undefined` right away).  The elector removes
%% the entry from FDB and broadcasts `{name_unregistered, …}` to all peer
%% members asynchronously.
-spec unregister_name({atom(), term()}) -> ok.
unregister_name({RegistryName, LogicalName}) ->
    Table = ets_table_name(RegistryName),
    try ets:delete(Table, LogicalName) catch error:badarg -> ok end,
    dgen_server:cast(elector_name(RegistryName), {unregister, LogicalName}).

%% Looks up the Pid for `{RegistryName, LogicalName}` from the local ETS
%% cache.  Never blocks; returns `undefined` if not registered or if the
%% member has not yet started (ETS table doesn't exist yet).
-spec whereis_name({atom(), term()}) -> pid() | undefined.
whereis_name({RegistryName, LogicalName}) ->
    Table = ets_table_name(RegistryName),
    try
        case ets:lookup(Table, LogicalName) of
            [{_, Pid}] -> Pid;
            [] -> undefined
        end
    catch
        error:badarg -> undefined
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

%% The ETS table owned by the local member process, used as the name lookup
%% cache for `whereis_name/1`.  Schema: `{LogicalName, Pid}`.
-spec ets_table_name(atom()) -> atom().
ets_table_name(Name) ->
    list_to_atom("dgen_registry_names_" ++ atom_to_list(Name)).

%% ---------------------------------------------------------------------------
%% Supervisor callbacks
%% ---------------------------------------------------------------------------

init({Name, Tenant}) ->
    ElectorName = elector_name(Name),
    MemberName = member_name(Name),

    %% Elector must start first: the member's init casts `{join, MemberId}`
    %% to it and calls `priority_call(get_names)` to seed the ETS table.
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
                    name => Name,
                    elector => ElectorName,
                    member_name => MemberName
                }
            ]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [dgen_registry_member]
    },

    {ok, {#{strategy => one_for_one, intensity => 5, period => 10},
          [ElectorSpec, MemberSpec]}}.
