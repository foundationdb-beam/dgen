-module(dgen_key).

-export([extend/2, extend/3, extend/4]).

-spec extend(tuple(), term()) -> tuple().
extend(BaseKey, Element) ->
    erlang:insert_element(1 + tuple_size(BaseKey), BaseKey, Element).

-spec extend(tuple(), term(), term()) -> tuple().
extend(BaseKey, E1, E2) ->
    extend(extend(BaseKey, E1), E2).

-spec extend(tuple(), term(), term(), term()) -> tuple().
extend(BaseKey, E1, E2, E3) ->
    extend(extend(BaseKey, E1, E2), E3).
