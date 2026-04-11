-module(dgen_registry).
-behaviour(supervisor).

-export([
    start_link/2,
    start_link/3,
    get_leader/1,
    get_members/1,
    elector_name/1,
    member_name/1
]).

-export([init/1]).

%% ---------------------------------------------------------------------------
%% Public API
%% ---------------------------------------------------------------------------

%% Start a registry supervisor registered under `Name`.
%% Each call site (node) that starts the same registry with the same `Tenant`
%% participates in the same distributed membership group.
%%
%% `Tenant` is `{Db, Dir}` — the FoundationDB tenant passed through to both
%% the elector (dgen_server) and member processes.
-spec start_link(Name :: atom(), Tenant :: dgen_backend:tenant()) ->
    supervisor:startlink_ret().
start_link(Name, Tenant) ->
    supervisor:start_link({local, Name}, ?MODULE, {Name, Tenant}).

%% Start with an explicit supervisor registration name separate from the
%% registry logical name.  Useful when the supervisor name needs to differ
%% from the registry identity.
-spec start_link(SupName :: atom(), Name :: atom(), Tenant :: dgen_backend:tenant()) ->
    supervisor:startlink_ret().
start_link(SupName, Name, Tenant) ->
    supervisor:start_link({local, SupName}, ?MODULE, {Name, Tenant}).

%% Returns the current leader member id `{Node, Name}` as last written to FDB.
%% Performs a priority_call (bypasses the durable queue) so the read is always
%% fresh relative to the elector process's latest committed state.
-spec get_leader(Name :: atom()) -> dgen_registry_elector:member_id() | undefined.
get_leader(Name) ->
    dgen_server:priority_call(elector_name(Name), get_leader).

%% Returns the full set of member ids currently tracked by the elector.
-spec get_members(Name :: atom()) -> [dgen_registry_elector:member_id()].
get_members(Name) ->
    dgen_server:priority_call(elector_name(Name), get_members).

%% Derived name helpers — exported so other modules (e.g. tests) can locate
%% the child processes without hard-coding naming conventions.
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

    %% The elector is a dgen_server — its state lives in FDB and survives
    %% process crashes.  It must start before the member because the member
    %% casts `{join, MemberId}` to the elector during its own init.
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

    %% The member is a plain gen_server.  It registers this node's presence
    %% with the elector and maintains local process monitors for all peers.
    MemberSpec = #{
        id => member,
        start =>
            {dgen_registry_member, start_link, [
                MemberName,
                #{
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

    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },

    {ok, {SupFlags, [ElectorSpec, MemberSpec]}}.
