-module(dgen_utils).

%% Small, dependency-free helpers shared across dgen modules (the BEAM/distribution
%% analogue of dgen_key's key helpers).  Keep this module general: anything here
%% should be useful to more than one caller and carry no dgen-specific state.

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-export([node_reachable/1, real_sleep/1, real_monotonic_ms/0]).

-if(?DOCATTRS).
-doc """
Is `Node` usable for a synchronous message right now — the local node, or a
currently-connected one?

This is the predicate the registry uses everywhere it must decide whether a cast
or call to a peer will actually be delivered (as opposed to silently triggering an
automatic distribution *reconnect*, which would heal a partition before it can be
observed — see `dgen_registry_member:cast_to_member/2` and
`dgen_registry_elector:call_to_member/2`).  The local node is trivially reachable;
a remote node is reachable iff it is in `nodes()`.
""".
-endif.
-spec node_reachable(node()) -> boolean().
node_reachable(Node) ->
    Node =:= node() orelse lists:member(Node, nodes()).

%% Wall-clock sleep and monotonic read that stay real under simulation.
%%
%% This module deliberately carries no `dgen_eta.hrl` include, so calls routed
%% through here are exempt from the eta transform.  That exemption is the entire
%% point: a *startup* wait like `dgen_registry:await_ready/2` runs in whatever
%% process is bringing the system up — under `eta_run` that is the driver, the one
%% process whose transformed sleep arms a virtual deadline nothing will ever reach
%% (`eta_time:sleep/1`'s own boundary rule).  Readiness polling is a wall-clock
%% concern by definition: it happens before the schedule owns the system, or on a
%% real node where there is no schedule at all.
%%
%% Do NOT reach for these inside a member/elector/connector code path that runs
%% during a simulation — there they are a real-time leak of exactly the class the
%% transform exists to remove.
-spec real_sleep(non_neg_integer()) -> ok.
real_sleep(Ms) ->
    timer:sleep(Ms).

-spec real_monotonic_ms() -> integer().
real_monotonic_ms() ->
    erlang:monotonic_time(millisecond).
