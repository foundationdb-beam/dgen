-module(dgen_registry_conflicts).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("dgen_registry_member.hrl").

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Detects and repairs conflicting name bindings (§5.6). `§N.N` refers to
%% docs/design/dgen_registry_design.md.
%%
%% Pids are never durable. If every member holding a binding is lost, the leader can
%% hand the name out again while the original pid is still alive. When the lost
%% members reconnect, two live pids hold the same name.
%%
%% Evidence. We keep a trail of explicit releases as `{Name, Pid} => ms`. The leader
%% records them at commit, followers record them from the broadcast, and a handoff
%% merges them. If a witness still binds a released pair, that is replication lag, not
%% a conflict. The trail is keyed by name and pid together, so a release of one name
%% never hides a conflict on another. We don't prune it while a member is disconnected
%% or within a TTL of a departure, because that member's stale rows may come back.
%%
%% Verdict. A conflict exists when the authority binds a name to a live pid and a
%% witness binds it to a different live pid that isn't on the trail. The authority is
%% the freshest reconstruction. If the authority doesn't hold the name at all, there is
%% no conflict. The leader probes liveness outside its own loop. get_conflict_pids/3
%% lists the pids to probe, and detect/4 makes the decision.
%%
%% Remedy. resolve/5 kills the authority pid and every divergent pid, and drops the
%% name. There are two exceptions. With `terminate_on_conflict` off, we only raise an
%% alarm. When the `conflict_kill_budget` for that name is spent, we raise an alarm and
%% stop killing, so a recurring conflict escalates instead of looping. This module
%% returns kills and alarms as effects. It never probes, sends or kills anything itself.
%%
%% Prevention. With `reject_when_degraded`, a leader whose reconstruction may be
%% missing a committed binding refuses names it doesn't already hold. See
%% is_reconstruction_degraded/5.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([new/0, get_trail/1, record_releases/3, merge/2, record_departure/2, prune/4]).
-export([get_conflict_pids/3, detect/4, resolve/5]).
-export([is_degraded/1, set_degraded/2, is_reconstruction_degraded/5]).

-export_type([t/0, trail/0, conflict/0, effect/0]).

-type records() :: dgen_registry_replica:records().

-type trail() :: #{{term(), pid()} => integer()}.

-type conflict() :: {Name :: term(), Authority :: pid(), Divergent :: [pid()]}.

-type effect() ::
    {kill, pid()}
    | {alarm, conflict(), terminated | detect_only | budget_exhausted}.

-type t() :: #conflicts{}.

-spec new() -> t().
new() -> #conflicts{}.

%% ---------------------------------------------------------------------------
%% Evidence: the release trail
%% ---------------------------------------------------------------------------

-spec get_trail(t()) -> trail().
get_trail(#conflicts{trail = T}) -> T.

%% Records explicit releases of live pids at time `Now`.
-spec record_releases([{term(), pid()}], integer(), t()) -> t().
record_releases([], _Now, C) ->
    C;
record_releases(Pairs, Now, C = #conflicts{trail = T}) ->
    C#conflicts{trail = lists:foldl(fun(Pair, Acc) -> Acc#{Pair => Now} end, T, Pairs)}.

%% Merges the trails gathered at handoff, keeping the latest release time for each entry.
-spec merge([trail()], t()) -> t().
merge(Trails, C = #conflicts{trail = T}) ->
    Merged = lists:foldl(
        fun(Other, Acc) -> maps:merge_with(fun(_K, A, B) -> max(A, B) end, Acc, Other) end,
        T,
        Trails
    ),
    C#conflicts{trail = Merged}.

-spec record_departure(integer(), t()) -> t().
record_departure(Now, C) -> C#conflicts{last_departure = Now}.

%% Expires old trail entries, but only when every member is connected and none has
%% departed within the TTL. Also drops kill-budget timestamps older than the window.
-spec prune(integer(), boolean(), dgen_config:config(), t()) -> t().
prune(Now, AllConnected, Config, C = #conflicts{trail = T, kill_budget = KB}) ->
    TTL = dgen_config:conflict_release_ttl(Config),
    RecentDeparture = Now - C#conflicts.last_departure < TTL,
    T1 =
        case AllConnected andalso not RecentDeparture of
            true -> maps:filter(fun(_Key, Ts) -> Ts >= Now - TTL end, T);
            false -> T
        end,
    {_Count, Window} = dgen_config:conflict_kill_budget(Config),
    KB1 = maps:filter(
        fun(_Name, Tss) -> Tss =/= [] end,
        maps:map(fun(_Name, Tss) -> [Ts || Ts <- Tss, Ts >= Now - Window] end, KB)
    ),
    C#conflicts{trail = T1, kill_budget = KB1}.

%% ---------------------------------------------------------------------------
%% Verdict
%% ---------------------------------------------------------------------------

%% Returns every pid a conflict could involve. The leader probes these outside its
%% loop before calling detect/4.
-spec get_conflict_pids(records(), [records()], trail()) -> [pid()].
get_conflict_pids(Authority, Witnesses, Trail) ->
    lists:usort(
        maps:fold(
            fun(Name, PidSet, Acc) ->
                [P || P <- [get_authority_pid(Name, Authority) | maps:keys(PidSet)], is_pid(P)] ++
                    Acc
            end,
            [],
            get_divergent_bindings(Authority, Witnesses, Trail)
        )
    ).

%% Returns the conflicts among the divergent bindings, using `Alive` to test pids. The
%% authority's records may also appear among the witnesses, which is fine because an
%% entry always agrees with itself.
-spec detect(records(), [records()], trail(), fun((pid()) -> boolean())) -> [conflict()].
detect(Authority, Witnesses, Trail, Alive) ->
    maps:fold(
        fun(Name, PidSet, Acc) ->
            APid = get_authority_pid(Name, Authority),
            Others = [P || P <- maps:keys(PidSet), P =/= APid],
            case Others =:= [] orelse not (is_pid(APid) andalso Alive(APid)) of
                true ->
                    Acc;
                false ->
                    case [P || P <- Others, Alive(P)] of
                        [] -> Acc;
                        Divergent -> [{Name, APid, Divergent} | Acc]
                    end
            end
        end,
        [],
        get_divergent_bindings(Authority, Witnesses, Trail)
    ).

%% Returns the witness bindings that differ from the authority and aren't on the
%% trail, as `#{Name => #{Pid => true}}`.
get_divergent_bindings(Authority, Witnesses, Trail) ->
    lists:foldl(
        fun(Map, Acc0) ->
            maps:fold(
                fun(Name, {Pid, _Index, _Data}, Acc) ->
                    case maps:get(Name, Authority, undefined) of
                        {Pid, _, _} ->
                            Acc;
                        _AbsentOrDifferent ->
                            case maps:is_key({Name, Pid}, Trail) of
                                true ->
                                    Acc;
                                false ->
                                    Pids = maps:get(Name, Acc, #{}),
                                    Acc#{Name => Pids#{Pid => true}}
                            end
                    end
                end,
                Acc0,
                Map
            )
        end,
        #{},
        Witnesses
    ).

get_authority_pid(Name, Authority) ->
    case maps:get(Name, Authority, undefined) of
        {Pid, _Index, _Data} -> Pid;
        undefined -> undefined
    end.

%% ---------------------------------------------------------------------------
%% Remedy
%% ---------------------------------------------------------------------------

%% Applies the remedy to each conflict. Resolving only ever removes names from
%% `Records`, so with no conflicts we return it unchanged without copying.
-spec resolve([conflict()], records(), dgen_config:config(), integer(), t()) ->
    {records(), t(), [effect()]}.
resolve([], Records, _Config, _Now, C) ->
    {Records, C, []};
resolve(Conflicts, Records, Config, Now, C) ->
    Terminate = dgen_config:terminate_on_conflict(Config),
    Budget = dgen_config:conflict_kill_budget(Config),
    {Records1, C1, EffsRev} = lists:foldl(
        fun(Conflict, Acc) -> resolve_conflict(Conflict, Terminate, Budget, Now, Acc) end,
        {Records, C, []},
        Conflicts
    ),
    {Records1, C1, lists:reverse(EffsRev)}.

resolve_conflict(Conflict, false, _Budget, _Now, {Records, C, Effs}) ->
    %% Detect-only mode. Raise an alarm and keep the authority's binding.
    {Records, C, [{alarm, Conflict, detect_only} | Effs]};
resolve_conflict(
    {Name, Authority, Divergent} = Conflict, true, {Count, Window}, Now, {Records, C, Effs}
) ->
    KB = C#conflicts.kill_budget,
    Recent = [Ts || Ts <- maps:get(Name, KB, []), Ts >= Now - Window],
    case length(Recent) < Count of
        false ->
            %% The budget is spent. Escalate with an alarm, stop killing for this name, and
            %% keep serving it.
            {Records, C, [{alarm, Conflict, budget_exhausted} | Effs]};
        true ->
            %% A pid that died since detection simply ignores the kill.
            Kills = [{kill, P} || P <- [Authority | Divergent]],
            {
                maps:remove(Name, Records),
                C#conflicts{kill_budget = KB#{Name => [Now | Recent]}},
                [{alarm, Conflict, terminated} | lists:reverse(Kills, Effs)]
            }
    end.

%% ---------------------------------------------------------------------------
%% Prevention
%% ---------------------------------------------------------------------------

-spec is_degraded(t()) -> boolean().
is_degraded(#conflicts{degraded = D}) -> D.

-spec set_degraded(boolean(), t()) -> t().
set_degraded(D, C) -> C#conflicts{degraded = D}.

%% Returns true when the reconstruction stopped short of `Frontier`, the durable
%% version key. Every replica is a prefix of the same stream, so a replica that reached
%% the key holds the committed map (TLA+ DegradedFlagSound). If we couldn't read the
%% key, we fall back to reachability. The reconstruction is degraded if any member not
%% marked fresh failed to answer the gather.
-spec is_reconstruction_degraded(
    non_neg_integer(), non_neg_integer() | unknown, [term()], [term()], #{term() => term()}
) -> boolean().
is_reconstruction_degraded(MaxVersion, Frontier, _OtherIds, _FreshIds, _PeerResults) when
    is_integer(Frontier)
->
    MaxVersion < Frontier;
is_reconstruction_degraded(_MaxVersion, unknown, OtherIds, FreshIds, PeerResults) ->
    Unreachable = OtherIds -- maps:keys(PeerResults),
    lists:any(fun(Id) -> not lists:member(Id, FreshIds) end, Unreachable).
