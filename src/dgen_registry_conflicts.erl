-module(dgen_registry_conflicts).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Conflict repair (§5.6): the evidence a member keeps, the verdict it reaches, and
%% the remedy it applies.
%%
%% Section references (e.g. `§5.6`) point to `docs/design/dgen_registry_design.md`.
%%
%% ## Why it exists
%%
%% Uniqueness is single-fault: pids are never durable, so if every holder of a
%% binding is lost the name can be re-issued while the original process lives on
%% somewhere a handoff could not reach.  When that member comes back, two live
%% processes believe they hold one name.  The registry repairs it by terminating
%% both (dgen's stance: let supervisors restart them cleanly under one leader,
%% rather than leave an alarm to go unnoticed).
%%
%% ## Evidence: the release trail
%%
%% Repair must not fire on mere lag.  A member that missed an unregister still
%% reports the binding, and its process may be alive — that is not a conflict.  So
%% every member keeps a **trail** of explicit releases, `{Name, Pid} => ms`: the
%% leader records each one at commit, followers from the replication broadcast, and
%% a handoff merges every reachable member's trail.  A witness binding on the trail
%% is lag.  Keyed by the pair, not the pid, so releasing a pid under one name never
%% masks a genuine conflict on another name it holds.
%%
%% The trail is pruned after `conflict_release_ttl` — but never while a member is
%% disconnected, nor within a TTL of a member departing: a disconnected or dropped
%% member misses unregister broadcasts outright, so its stale rows can surface at a
%% rejoin arbitrarily later, and the trail must outlive that.  A member gone longer
%% than the TTL is outside the trail's protection horizon.
%%
%% ## Verdict
%%
%% A conflict is a name the **authority** (the freshest reconstruction) binds to a
%% live pid while some witness binds it to a *different*, live pid that is not on
%% the trail.  A name the authority lacks is not a conflict: there is no second
%% claimant, and reconstruction leaves it free.  Liveness is evidence gathered
%% *before* adjudication — off the leader's loop, since probing a remote pid is an
%% RPC — and passed in: `conflict_pids/3` says which pids to probe, `detect/4`
%% decides with the result.
%%
%% ## Remedy
%%
%% `resolve/5` terminates the authority and every divergent pid and drops the name
%% from the reconstruction, subject to `terminate_on_conflict` (off: alarm only) and
%% a per-name `conflict_kill_budget` (exhausted: alarm and stop killing, so a
%% regenerating conflict escalates instead of looping).  The budget is the current
%% leader's and is not merged across a handoff, so it bounds kills per name per term.
%%
%% ## Prevention
%%
%% `degraded` is the opt-in alternative to repair (`reject_when_degraded`): a leader
%% whose reconstruction may be missing a committed binding refuses names it does not
%% already hold.  See `degraded/5`.
%%
%% Nothing here sends a message, probes a pid, or kills one: `resolve/5` returns the
%% kills and alarms for the member to carry out.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([new/0, trail/1, released/3, merge/2, departed/2, prune/4]).
-export([conflict_pids/3, detect/4, resolve/5]).
-export([degraded/1, set_degraded/2, degraded/5]).

-export_type([t/0, trail/0, conflict/0, effect/0]).

-type records() :: dgen_registry_replica:records().

%% `{Name, Pid} => ms`.  Bare-pid keys are the pre-upgrade shape, still honoured when
%% gathered from an older member during a rolling upgrade.
-type trail() :: #{{term(), pid()} | pid() => integer()}.

-type conflict() :: {Name :: term(), Authority :: pid(), Divergent :: [pid()]}.

-type effect() ::
    {kill, pid()}
    | {alarm, conflict(), terminated | detect_only | budget_exhausted}.

-record(conflicts, {
    trail = #{} :: trail(),
    %% Leader-only: name => recent kill times (ms), for `conflict_kill_budget`.
    kill_budget = #{} :: #{term() => [integer()]},
    %% When this member last saw a member depart; suspends trail pruning for a TTL.
    last_departure = 0 :: integer(),
    %% Leader-only: may the last handoff's reconstruction be missing a committed
    %% binding (degraded/5)?
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

%% Record explicit releases (`{Name, Pid}` pairs of live pids) at `Now`.
-spec released([{term(), pid()}], integer(), t()) -> t().
released([], _Now, C) ->
    C;
released(Pairs, Now, C = #conflicts{trail = T}) ->
    C#conflicts{trail = lists:foldl(fun(Pair, Acc) -> Acc#{Pair => Now} end, T, Pairs)}.

%% Merge other members' trails (a handoff or joiner gather), keeping each entry's
%% latest release time.
-spec merge([trail()], t()) -> t().
merge(Trails, C = #conflicts{trail = T}) ->
    C#conflicts{trail = merge_trails(T, Trails)}.

%% A member left the set at `Now`.
-spec departed(integer(), t()) -> t().
departed(Now, C) -> C#conflicts{last_departure = Now}.

%% Periodic expiry of the trail and the kill budget.  `AllConnected` says whether
%% every current member is reachable; trail entries expire only if so, and only
%% when no member has departed within the TTL (see the module doc).
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

%% Every pid a conflict among these maps could turn on — each candidate name's
%% authority and its divergent claimants — so their liveness can be probed ahead of
%% `detect/4`, off the leader's loop.
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

%% The conflicts among the authority's records and the witnesses', given `Alive`
%% (the probed liveness).  The authority's own records may be among the witnesses:
%% an entry always agrees with itself.
%%
%% The common all-agree handoff allocates nothing but the folds' frames: entries are
%% compared with the authority in place, and only divergent names accumulate.
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

%% Names some witness binds to a pid other than the authority's and that is not on
%% the trail: `#{Name => #{Pid => true}}`.
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

%% Apply the remedy to each conflict: terminate the authority and every divergent
%% pid and drop the name from `Records` (the reconstruction; the snapshot fan-out
%% propagates the drop), within the kill budget.  `Records` only ever loses names,
%% so with no conflicts it passes through by reference.  Returns the records, the
%% new state, and the kills and alarms in order.
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
            %% The authority was alive when the conflict was found; a pid that has
            %% died since ignores the kill.
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

%% Could a handoff's reconstruction be missing a committed binding?  Exactly when it
%% stopped short of the durable version key (`Frontier`): every replica is a prefix
%% of the leader's stream, so the freshest one at the key IS the committed map, and
%% one below it lacks whatever was committed past it.  Checked by the TLA+ model's
%% DegradedFlagSound (formal/DgenRegistryReplication.tla).  Which members answered
%% does not matter: a member that did not answer but was behind held nothing extra
%% (a scale-down is not degraded), and one that did answer cannot hide a version.
%%
%% With the key unreadable (`unknown`, a backend hiccup) the version test has nothing
%% to compare against, so it falls back to the reachability test it replaced:
%% degraded if a member that could hold bindings — any not marked fresh (never
%% synced) by the elector — did not answer the gather.
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
