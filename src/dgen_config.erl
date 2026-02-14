-module(dgen_config).

-export([backend/0, init/0]).

init() ->
    Mod = application:get_env(dgen, backend, dgen_erlfdb),
    persistent_term:put({dgen, backend}, Mod).

backend() ->
    persistent_term:get({dgen, backend}, dgen_erlfdb).
