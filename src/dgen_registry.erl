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
%% never blocks.
%%
%% `register_name` routes through the local member gen_server.  The current
%% leader member is the sole writer for the name table: it serialises
%% registrations through its own mailbox, writes to FDB, and broadcasts
%% `{name_registered, …}` to every follower member.  If this node is a
%% follower, its member process forwards the call to the leader.
%%
%% This avoids FDB write conflicts between nodes while still persisting names
%% durably.  There is a short window after registration where a remote node's
%% `whereis_name` may still return `undefined` (eventual consistency via
%% async broadcast); this is the same trade-off as `gproc` global mode.
%%
%% Auto-unregistration
%% -------------------
%% The leader member monitors every registered Pid.  When the monitored
%% process exits the leader removes the entry from FDB and ETS and broadcasts
%% `{name_unregistered, …}` to all peer members.
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
%% The call is routed to the local member gen_server.  If this node is the
%% current leader the member handles it directly (writes to FDB, updates ETS,
%% broadcasts to peers).  If this node is a follower the member forwards the
%% call to the leader's member process.
%%
%% In either case the `yes/no` reply arrives before the leader's
%% `{name_registered, …}` broadcast has been processed by the local member.
%% We therefore update the local ETS table synchronously here, immediately
%% before returning `yes`, so that a `whereis_name` call on this node is
%% consistent right away.  The ets:insert in the member is idempotent.
-spec register_name({atom(), term()}, pid()) -> yes | no.
register_name({RegistryName, LogicalName}, Pid) ->
    case gen_server:call(member_name(RegistryName), {register, LogicalName, Pid}) of
        yes ->
            Table = ets_table_name(RegistryName),
            try ets:insert(Table, {LogicalName, Pid})
            catch error:badarg -> ok  %% member not yet started
            end,
            yes;
        no ->
            no
    end.

%% Removes the registration for `{RegistryName, LogicalName}`.
%% The local ETS entry is deleted immediately (so subsequent `whereis_name`
%% calls on this node return `undefined` right away).  The local member
%% forwards the unregister to the leader, which removes the entry from FDB
%% and broadcasts `{name_unregistered, …}` to all peer members asynchronously.
-spec unregister_name({atom(), term()}) -> ok.
unregister_name({RegistryName, LogicalName}) ->
    Table = ets_table_name(RegistryName),
    try ets:delete(Table, LogicalName) catch error:badarg -> ok end,
    gen_server:cast(member_name(RegistryName), {unregister, LogicalName}).

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
    %% to it immediately after creating the ETS table.
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
                    member_name => MemberName,
                    tenant => Tenant
                }
            ]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [dgen_registry_member]
    },

    {ok, {#{strategy => one_for_one, intensity => 5, period => 10},
          [ElectorSpec, MemberSpec]}}.
