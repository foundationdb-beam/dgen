%% dgen's compile-time contract with the `eta` simulation framework.
%%
%%     -include("../include/dgen_eta.hrl").
%%
%% Everything here hangs on the `DST` define, which the test build sets (see
%% `erlc_options/1` in mix.exs) and a release build does not, so the
%% instrumentation below can be left in the source of a module that ships.
%%
%% ## Why not include `eta/include/eta.hrl` directly
%%
%% `-include_lib` resolves through the code path, so `eta` would have to be
%% findable in *every* compilation — which the library's own walkthrough handles by
%% telling adopters to depend on it with `runtime: false`. dgen is a published
%% package, and test tooling has no business becoming a package requirement for
%% every consumer, so `eta` stays behind `only: :test` (pinned from Hex, ~> 0.1 —
%% see mix.exs).
%%
%% So `eta` stays test-only, the `-include_lib` sits behind `-ifdef(DST)` where the
%% preprocessor never resolves it in a release build, and the `-else` branch below
%% supplies the macros that branch would otherwise leave undefined. Without that
%% branch, `?ETA_LOG` in shipped code is a compile error in the only build that
%% matters.

-ifndef(DGEN_ETA_HRL).
-define(DGEN_ETA_HRL, true).

-ifdef(DST).

%% Brings the parse transform and the real macros: timer and clock calls rewritten
%% to `eta_time`, sends to `eta_net`, spawns to `eta_sched`, and `-eta_observe`
%% fields published on every callback return. Inert unless a run is active.
-include_lib("eta/include/eta.hrl").

-else.

%% Release build. `Event` and `Label` are **not evaluated**, matching `eta.hrl`, so
%% anything logged must be side-effect free and a variable used only inside one of
%% these reads as unused.
-define(ETA_LABEL(Label), ok).
-define(ETA_LOG(Event), 0).

-endif.
-endif.
