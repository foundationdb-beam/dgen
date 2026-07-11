-module(dgen_utils).

%% Small, dependency-free helpers shared across dgen modules (the BEAM/distribution
%% analogue of dgen_key's key helpers).  Keep this module general: anything here
%% should be useful to more than one caller and carry no dgen-specific state.

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-export([node_reachable/1]).

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
