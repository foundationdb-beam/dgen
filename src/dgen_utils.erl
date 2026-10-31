-module(dgen_utils).

%% Small, dependency-free helpers for the BEAM and distribution, shared across dgen
%% modules. It plays the same role for them that `dgen_key` plays for keys. Keep this
%% module general. Anything here should be useful to more than one caller and hold
%% no dgen-specific state.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-export([node_reachable/1, real_sleep/1, real_monotonic_ms/0]).

-if(?DOCATTRS).
-doc """
Returns whether we can send `Node` a message right now. That is true for the local
node and for any node that is currently connected.

The registry uses this check wherever it must decide whether a cast or call to a
peer will really be delivered. Sending to a disconnected node would instead start an
automatic distribution reconnect, which can heal a partition before the registry
notices it. See `dgen_registry_member:cast_to_member/2` and
`dgen_registry_elector:call_to_member/2`. A remote node is reachable only if it is
in `nodes()`.
""".
-endif.
-spec node_reachable(node()) -> boolean().
node_reachable(Node) ->
    Node =:= node() orelse lists:member(Node, nodes()).

%% A sleep and a monotonic clock read that stay on real time under simulation.
%%
%% This module deliberately doesn't include `dgen_eta.hrl`, so the eta transform
%% leaves calls through here alone. That is the whole point. A startup wait such as
%% `dgen_registry:await_ready/2` runs in whichever process is bringing the system
%% up. Under `eta_run` that is the driver. If the driver's sleep were transformed,
%% it would wait for a virtual deadline that never arrives, by the boundary rule in
%% `eta_time:sleep/1`. Readiness polling is about wall-clock time anyway. It happens
%% either before the scheduler takes over, or on a real node with no scheduler.
%%
%% Do NOT use these in member, elector or connector code that runs during a
%% simulation. There they would leak real time into the simulation, which is
%% exactly what the transform exists to prevent.
-spec real_sleep(non_neg_integer()) -> ok.
real_sleep(Ms) ->
    timer:sleep(Ms).

-spec real_monotonic_ms() -> integer().
real_monotonic_ms() ->
    erlang:monotonic_time(millisecond).
