-module(dgen_config).

-export([backend/0]).

backend() ->
    application:get_env(dgen, backend, dgen_erlfdb).
