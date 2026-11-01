%% dgen's compile-time contract with the `eta` simulation framework.
%%
%%     -include("../include/dgen_eta.hrl").
%%
%% Everything here depends on the `DST` define. The test build sets it, in
%% `erlc_options/1` in mix.exs, and a release build does not. That lets a module
%% that ships keep the instrumentation below in its source.
%%
%% ## Why not include `eta/include/eta.hrl` directly
%%
%% `-include_lib` resolves through the code path, so `eta` would have to be
%% available in every compilation. The eta walkthrough handles this by telling
%% adopters to depend on it with `runtime: false`. But dgen is a published package,
%% and its consumers shouldn't have to pull in our test tooling. So `eta` stays
%% behind `only: :test`, pinned from Hex at ~> 0.1 in mix.exs.
%%
%% The `-include_lib` sits behind `-ifdef(DST)`, so the preprocessor never resolves
%% it in a release build. The `-else` branch below defines the macros that the
%% include would otherwise provide. Without it, `?ETA_LOG` in shipped code would be
%% a compile error in the release build, which is the one that matters.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-ifndef(DGEN_ETA_HRL).
-define(DGEN_ETA_HRL, true).

-ifdef(DST).

%% Brings in the parse transform and the real macros. The transform rewrites timer
%% and clock calls to `eta_time`, sends to `eta_net` and spawns to `eta_sched`, and
%% publishes `-eta_observe` fields on every callback return. None of it does
%% anything unless a simulation run is active.
-include_lib("eta/include/eta.hrl").

%% Turns off the supervisor's `hibernate_after` in DST builds on OTP 28 and later.
%% It is a wall-clock timer, and elector_pid/1 calls into the supervisor, so it
%% would make runs with the same seed diverge.
-if(?OTP_RELEASE >= 28).
-define(SUP_SIM_FLAGS, #{hibernate_after => infinity}).
-else.
-define(SUP_SIM_FLAGS, #{}).
-endif.

-else.

-define(SUP_SIM_FLAGS, #{}).

%% Release build. Like `eta.hrl`, these macros do not evaluate `Event` or `Label`.
%% So anything logged must be free of side effects, and a variable used only inside
%% one of these macros counts as unused.
-define(ETA_LABEL(Label), ok).
-define(ETA_LOG(Event), 0).

-endif.
-endif.
