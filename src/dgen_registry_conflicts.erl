-module(dgen_registry_conflicts).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Conflict repair (§5.6; `§N.N` is docs/design/dgen_registry_design.md). Pids are never
%% durable, so if every holder of a binding is lost the name can be re-issued while the
%% original lives on; on reconnection two live pids hold one name.
%%
%% Evidence: a trail of explicit releases, `{Name, Pid} => ms`, recorded at commit or
%% from the broadcast and merged at handoff; a witness binding on it is lag, not a
%% conflict. Keyed by pair, so one release never masks another name. Not pruned while a
%% member is disconnected or within a TTL of a departure: its stale rows may resurface.
%%
%% Verdict: the authority (freshest reconstruction) binds a name to a live pid and a
%% witness to a different live pid off the trail. A name the authority lacks is no
%% conflict. Liveness is probed off the leader's loop: conflict_pids/3 lists what to
%% probe, detect/4 decides.
%%
%% Remedy: resolve/5 kills the authority and all divergent pids and drops the name,
%% unless `terminate_on_conflict` is off (alarm only) or the per-name, per-term
%% `conflict_kill_budget` is spent (alarm and stop: escalate rather than loop). Kills
%% and alarms are returned as effects; nothing here probes, sends or kills.
%%
%% Prevention: with `reject_when_degraded`, a leader whose reconstruction may lack a
%% committed binding refuses names it does not hold (degraded/5).
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([new/0, trail/1, released/3, merge/2, departed/2, prune/4]).
-export([conflict_pids/3, detect/4, resolve/5]).
-export([degraded/1, set_degraded/2, degraded/5]).

-export_type([t/0, trail/0, conflict/0, effect/0]).

-type records() :: dgen_registry_replica:records().

%% Bare-pid keys come from pre-upgrade members during a rolling upgrade.
-type trail() :: #{{term(), pid()} | pid() => integer()}.

-type conflict() :: {Name :: term(), Authority :: pid(), Divergent :: [pid()]}.

-type effect() ::
    {kill, pid()}
    | {alarm, conflict(), terminated | detect_only | budget_exhausted}.

-record(conflicts, {
    trail = #{} :: trail(),
    %% Leader-only: name => recent kill times (ms), for `conflict_kill_budget`.
    kill_budget = #{} :: #{term() => [integer()]},
    %% Last member departure seen; suspends trail pruning for a TTL.
    last_departure = 0 :: integer(),
    %% Leader-only: may the last reconstruction lack a committed binding?
    degraded = false :: boolean()
}).

-opaque t() :: #conflicts{}.

-spec new() -> t().
new() -> #conflicts{}.

%% ---------------------------------------------------------------------------
%% Evidence: the release trail
%% ---------------------------------------------------------------------------

-spec trail(t()) -> trail().
trail(#conflicts{trail = T}) -> T.

%% Record explicit releases of live pids at `Now`.
-spec released([{term(), pid()}], integer(), t()) -> t().
released([], _Now, C) ->
    C;
released(Pairs, Now, C = #conflicts{trail = T}) ->
    C#conflicts{trail = lists:foldl(fun(Pair, Acc) -> Acc#{Pair => Now} end, T, Pairs)}.

%% Merge gathered trails, keeping each entry's latest release time.
-spec merge([trail()], t()) -> t().
merge(Trails, C = #conflicts{trail = T}) ->
    C#conflicts{trail = merge_trails(T, Trails)}.

-spec departed(integer(), t()) -> t().
departed(Now, C) -> C#conflicts{last_departure = Now}.

%% Trail entries expire only if every member is connected and none departed in the TTL.
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

%% Every pid a conflict could turn on, to probe off the leader's loop before detect/4.
-spec conflict_pids(records(), [records()], trail()) -> [pid()].
conflict_pids(Authority, Witnesses, Trail) ->
    lists:usort(
        maps:fold(
            fun(Name, PidSet, Acc) ->
                [P || P <- [authority(Name, Authority) | maps:keys(PidSet)], is_pid(P)] ++ Acc
            end,
            [],
            candidates(Authority, Witnesses, Trail)
        )
    ).

%% The authority's records may be among the witnesses: an entry agrees with itself.
-spec detect(records(), [records()], trail(), fun((pid()) -> boolean())) -> [conflict()].
detect(Authority, Witnesses, Trail, Alive) ->
    maps:fold(
        fun(Name, PidSet, Acc) ->
            APid = authority(Name, Authority),
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
        candidates(Authority, Witnesses, Trail)
    ).

%% Differing witness bindings off the trail: `#{Name => #{Pid => true}}`.
candidates(Authority, Witnesses, Trail) ->
    lists:foldl(
        fun(Map, Acc0) ->
            maps:fold(
                fun(Name, {Pid, _Index, _Data}, Acc) ->
                    case maps:get(Name, Authority, undefined) of
                        {Pid, _, _} ->
                            Acc;
                        _AbsentOrDifferent ->
                            case on_trail(Name, Pid, Trail) of
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

authority(Name, Authority) ->
    case maps:get(Name, Authority, undefined) of
        {Pid, _Index, _Data} -> Pid;
        undefined -> undefined
    end.

on_trail(Name, Pid, Trail) ->
    maps:is_key({Name, Pid}, Trail) orelse maps:is_key(Pid, Trail).

%% ---------------------------------------------------------------------------
%% Remedy
%% ---------------------------------------------------------------------------

%% `Records` only loses names, so with no conflicts it passes through by reference.
-spec resolve([conflict()], records(), dgen_config:config(), integer(), t()) ->
    {records(), t(), [effect()]}.
resolve([], Records, _Config, _Now, C) ->
    {Records, C, []};
resolve(Conflicts, Records, Config, Now, C) ->
    Terminate = dgen_config:terminate_on_conflict(Config),
    Budget = dgen_config:conflict_kill_budget(Config),
    {Records1, C1, EffsRev} = lists:foldl(
        fun(Conflict, Acc) -> resolve_one(Conflict, Terminate, Budget, Now, Acc) end,
        {Records, C, []},
        Conflicts
    ),
    {Records1, C1, lists:reverse(EffsRev)}.

resolve_one(Conflict, false, _Budget, _Now, {Records, C, Effs}) ->
    %% Detect-only: alarm, and keep the authority's binding.
    {Records, C, [{alarm, Conflict, detect_only} | Effs]};
resolve_one(
    {Name, Authority, Divergent} = Conflict, true, {Count, Window}, Now, {Records, C, Effs}
) ->
    KB = C#conflicts.kill_budget,
    Recent = [Ts || Ts <- maps:get(Name, KB, []), Ts >= Now - Window],
    case length(Recent) < Count of
        false ->
            %% Exhausted: escalate, stop killing this name, keep it served.
            {Records, C, [{alarm, Conflict, budget_exhausted} | Effs]};
        true ->
            %% A pid that died since detection ignores the kill.
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

-spec degraded(t()) -> boolean().
degraded(#conflicts{degraded = D}) -> D.

-spec set_degraded(boolean(), t()) -> t().
set_degraded(D, C) -> C#conflicts{degraded = D}.

%% Degraded exactly when the reconstruction stopped short of the durable version key
%% (`Frontier`): replicas are stream prefixes, so the freshest at the key is the
%% committed map (TLA+ DegradedFlagSound). With the key unreadable, fall back to
%% reachability: degraded if a member not marked fresh did not answer the gather.
-spec degraded(
    non_neg_integer(), non_neg_integer() | unknown, [term()], [term()], #{term() => term()}
) -> boolean().
degraded(MaxVersion, Frontier, _OtherIds, _FreshIds, _PeerResults) when is_integer(Frontier) ->
    MaxVersion < Frontier;
degraded(_MaxVersion, unknown, OtherIds, FreshIds, PeerResults) ->
    Unreachable = OtherIds -- maps:keys(PeerResults),
    lists:any(fun(Id) -> not lists:member(Id, FreshIds) end, Unreachable).

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

merge_trails(Trail, Others) ->
    lists:foldl(
        fun(T, Acc) -> maps:merge_with(fun(_K, A, B) -> max(A, B) end, Acc, T) end, Trail, Others
    ).
