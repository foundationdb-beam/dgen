-module(dgen).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Internal helpers for the dgen_server call/reply protocol.

This module implements the client-side call flow: pushing a call request
onto the durable queue, waiting for the reply via an FDB watch, and
cleaning up the call-key-bin on timeout.

### Call Request Flow

1. The caller invokes `dgen_server:call/3` which delegates to `call/4`.
2. A call request is pushed onto the durable queue via `push_call/5`,
   which creates a `CallKeyBin` (the reply slot) and an FDB watch.
3. The caller blocks in `await_call_reply/4` until the watch fires or
   the timeout expires.
4. A consumer processes the request and writes `{reply, Reply}` into
   `CallKeyBin`.
5. The watch fires, the caller reads and clears `CallKeyBin`.
6. On timeout the caller clears `CallKeyBin` to avoid leaking the key.
""".
-endif.

-export([get_waiting_key/1, get_from/2, call/4, push_call/5]).

-include("../include/dgen.hrl").

-if(?DOCATTRS).
-doc """
Pushes a call request onto the durable queue.

Creates a unique `CallKeyBin` (the reply slot) under the waiting-key prefix,
sets an FDB watch on it directed to `WatchTo`, and enqueues the request.
Returns `{CallKeyBin, Watch}`.
""".
-endif.
push_call(?IS_DB(Db, Dir), Tuid, Request, WatchTo, Options) ->
    erlfdb:transactional(Db, fun(Tx) -> push_call({Tx, Dir}, Tuid, Request, WatchTo, Options) end);
push_call(Td = ?IS_TX(Tx, Dir), Tuid, Request, WatchTo, Options) ->
    WaitingKey = get_waiting_key(Tuid),
    From = get_from(WaitingKey, make_ref()),
    CallKeyBin = erlfdb_directory:pack(Dir, From),
    erlfdb:set(Tx, CallKeyBin, term_to_binary(noreply)),
    Future = erlfdb:watch(Tx, CallKeyBin, [{to, WatchTo}]),
    dgen_queue:push_k(Td, Tuid, [{call, Request, From, Options}]),
    {CallKeyBin, Future}.

-if(?DOCATTRS).
-doc "Appends the `<<\"c\">>` waiting-key tag to a tuid tuple.".
-endif.
get_waiting_key(Tuple) ->
    erlang:insert_element(1 + tuple_size(Tuple), Tuple, <<"c">>).

-if(?DOCATTRS).
-doc """
Builds a from-key tuple from a waiting key and a unique ref.

The key includes a system-time timestamp (seconds) so that abandoned
call keys can be garbage-collected using a time-based heuristic.
Key structure: `{WaitingKey..., Timestamp, term_to_binary(Ref)}`.
""".
-endif.
get_from(WaitingKey, Ref) ->
    Ts = erlang:system_time(second),
    Bin = term_to_binary(Ref),
    K1 = erlang:insert_element(1 + tuple_size(WaitingKey), WaitingKey, Ts),
    erlang:insert_element(1 + tuple_size(K1), K1, Bin).

-if(?DOCATTRS).
-doc """
Sends a synchronous call through the durable queue and waits for the reply.

`Module` is the gen_server-compatible module used to send the initial message
(typically `gen_server`). On timeout, the `CallKeyBin` is cleared to prevent
durable key leaks.
""".
-endif.
call(Module, Server, Request, Timeout) ->
    T1 = erlang:monotonic_time(millisecond),

    CallResult =
        try
            Module:call(Server, Request, Timeout)
        catch
            error:{timeout, _} ->
                % gen_server:call timed out before returning. If push_call
                % committed, CallKeyBin is abandoned (we don't have it).
                % It will be cleaned up when the dgen_server is killed,
                % which clears the entire waiting-key range.
                {noreply, {undefined, undefined, undefined}}
        end,

    case CallResult of
        {reply, Reply} ->
            Reply;
        {noreply, {Tenant, CallKeyBin, Watch}} ->
            T2 = erlang:monotonic_time(millisecond),

            case await_call_reply(Tenant, CallKeyBin, Watch, Timeout - (T2 - T1)) of
                {error, timeout} ->
                    cleanup_call(Tenant, CallKeyBin),
                    erlang:error(timeout);
                Reply ->
                    Reply
            end
    end.

await_call_reply(undefined, _CallKeyBin, _Watch, _Timeout) ->
    {error, timeout};
await_call_reply(_Tenant, _CallKeyBin, _Watch, Timeout) when Timeout =< 0 -> {error, timeout};
await_call_reply(Tenant, CallKeyBin, ?FUTURE(WatchRef), Timeout) ->
    T1 = erlang:monotonic_time(millisecond),

    Result =
        receive
            {WatchRef, ready} ->
                handle_call_ready(Tenant, CallKeyBin)
        after Timeout ->
            {error, timeout}
        end,

    T2 = erlang:monotonic_time(millisecond),

    case Result of
        {reply, Reply} ->
            Reply;
        {watch, Watch} ->
            await_call_reply(Tenant, CallKeyBin, Watch, Timeout - (T2 - T1));
        {error, timeout} ->
            {error, timeout}
    end.

cleanup_call(undefined, _CallKeyBin) ->
    ok;
cleanup_call(?IS_DB(Db, _Dir), CallKeyBin) ->
    erlfdb:transactional(Db, fun(Tx) -> erlfdb:clear(Tx, CallKeyBin) end).

handle_call_ready(?IS_DB(Db, Dir), CallKeyBin) ->
    erlfdb:transactional(Db, fun(Tx) -> handle_call_ready({Tx, Dir}, CallKeyBin) end);
handle_call_ready(?IS_TX(Tx, _Dir), CallKeyBin) ->
    Future = erlfdb:get(Tx, CallKeyBin),
    case erlfdb:wait(Future) of
        not_found ->
            {watch, erlfdb:watch(Tx, CallKeyBin)};
        Value ->
            CallState = binary_to_term(Value),
            case CallState of
                {reply, Reply} ->
                    erlfdb:clear(Tx, CallKeyBin),
                    {reply, Reply};
                noreply ->
                    {watch, erlfdb:watch(Tx, CallKeyBin)};
                _ ->
                    {error, timeout}
            end
    end.
