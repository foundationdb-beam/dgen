-module(dgen).

-export([get_waiting_key/1, get_from/2, call/4, push_call/4]).

-include("../include/dgen.hrl").

push_call(?IS_DB(Db, Dir), Key, Request, WatchTo) ->
    erlfdb:transactional(Db, fun(Tx) -> push_call({Tx, Dir}, Key, Request, WatchTo) end);
push_call(Td = ?IS_TX(Tx, Dir), Key, Request, WatchTo) ->
    WaitingKey = get_waiting_key(Key),
    From = get_from(WaitingKey, make_ref()),
    CallKey = erlfdb_directory:pack(Dir, From),
    erlfdb:set(Tx, CallKey, term_to_binary(noreply)),
    Future = erlfdb:watch(Tx, CallKey, [{to, WatchTo}]),
    dgen_queue:push_k(Td, Key, [{call, Request, From}]),
    {CallKey, Future}.

get_waiting_key(Tuple) ->
    erlang:insert_element(1 + tuple_size(Tuple), Tuple, <<"c">>).

get_from(WaitingKey, Ref) ->
    Bin = term_to_binary(Ref),
    erlang:insert_element(1 + tuple_size(WaitingKey), WaitingKey, Bin).

call(Module, Server, Request, Timeout) ->
    T1 = erlang:monotonic_time(millisecond),

    CallResult =
        try
            Module:call(Server, Request, Timeout)
        catch
            error:{timeout, _} ->
                % @todo: If CallKey was created, it will be abandoned
                {noreply, {undefined, undefined, undefined}}
        end,

    case CallResult of
        {reply, Reply} ->
            Reply;
        {noreply, {Tenant, CallKey, Watch}} ->
            T2 = erlang:monotonic_time(millisecond),

            case await_call_reply(Tenant, CallKey, Watch, Timeout - (T2 - T1)) of
                {error, timeout} ->
                    erlang:error(timeout);
                Reply ->
                    Reply
            end
    end.

await_call_reply(undefined, _CallKey, _Watch, _Timeout) ->
    {error, timeout};
await_call_reply(_Tenant, _CallKey, _Watch, Timeout) when Timeout =< 0 -> {error, timeout};
await_call_reply(Tenant, CallKey, ?FUTURE(WatchRef), Timeout) ->
    T1 = erlang:monotonic_time(millisecond),

    Result =
        receive
            {WatchRef, ready} ->
                handle_call_ready(Tenant, CallKey)
        after Timeout ->
            {error, timeout}
        end,

    T2 = erlang:monotonic_time(millisecond),

    case Result of
        {reply, Reply} ->
            Reply;
        {watch, Watch} ->
            await_call_reply(Tenant, CallKey, Watch, Timeout - (T2 - T1));
        {error, timeout} ->
            {error, timeout}
    end.

handle_call_ready(?IS_DB(Db, Dir), CallKey) ->
    erlfdb:transactional(Db, fun(Tx) -> handle_call_ready({Tx, Dir}, CallKey) end);
handle_call_ready(?IS_TX(Tx, _Dir), CallKey) ->
    Future = erlfdb:get(Tx, CallKey),
    case erlfdb:wait(Future) of
        not_found ->
            {watch, erlfdb:watch(Tx, CallKey)};
        Value ->
            CallState = binary_to_term(Value),
            case CallState of
                {reply, Reply} ->
                    erlfdb:clear(Tx, CallKey),
                    {reply, Reply};
                noreply ->
                    {watch, erlfdb:watch(Tx, CallKey)};
                _ ->
                    {error, timeout}
            end
    end.
