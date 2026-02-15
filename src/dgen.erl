-module(dgen).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Internal helpers for the dgen_server call/reply protocol.

This module implements the client-side call flow: pushing a call request
onto the durable queue, waiting for the reply via a watch, and
cleaning up reply keys on timeout.

Reply payloads are stored using the codec's chunked term encoding so that
replies can exceed the single-value size limit. The watch is placed on
the first chunk key; the client always reads via `get_range`.

### Call Request Flow

1. The caller invokes `dgen_server:call/3` which delegates to `call/4`.
2. A call request is pushed onto the durable queue via `push_call/5`,
   which writes `noreply` as a chunked term under the from-key and sets
   a watch on the first chunk key.
3. The caller blocks in `await_call_reply/4` until the watch fires or
   the timeout expires.
4. A consumer processes the request and writes `{reply, Reply}` as a
   chunked term under the from-key.
5. The watch fires, the caller reads and clears the chunked reply.
6. On timeout the caller clears the chunked reply keys to avoid leaks.
""".
-endif.

-export([get_waiting_key/1, get_from/2, call/4, push_call/5]).

-include("../include/dgen.hrl").

-type tuid() :: tuple().
-type from() :: tuple().

-export_type([tuid/0, from/0]).

-if(?DOCATTRS).
-doc """
Pushes a call request onto the durable queue.

Writes `noreply` as a chunked term under the from-key prefix, sets a
watch on the first chunk key directed to `WatchTo`, and enqueues the request.
Returns `{From, Watch}` where `From` is the from-key tuple.
""".
-endif.
-spec push_call(dgen_backend:tenant(), tuid(), term(), pid(), list()) ->
    {from(), dgen_backend:future()}.
push_call(Tenant, Tuid, Request, WatchTo, Options) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        WaitingKey = get_waiting_key(Tuid),
        From = get_from(WaitingKey, make_ref()),
        dgen_mod_state_codec:write_term({Tx, Dir}, From, noreply),
        ReplySentinelKey = dgen_mod_state_codec:term_first_key(Dir, From),
        Future = B:watch(Tx, ReplySentinelKey, [{to, WatchTo}]),
        dgen_queue:push_k({Tx, Dir}, Tuid, [{call, Request, From, Options}]),
        {From, Future}
    end).

-if(?DOCATTRS).
-doc "Appends the `<<\"c\">>` waiting-key tag to a tuid tuple.".
-endif.
-spec get_waiting_key(tuid()) -> dgen_backend:tuple_key().
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
-spec get_from(dgen_backend:tuple_key(), reference()) -> from().
get_from(WaitingKey, Ref) ->
    Ts = erlang:system_time(second),
    Bin = term_to_binary(Ref),
    K1 = erlang:insert_element(1 + tuple_size(WaitingKey), WaitingKey, Ts),
    erlang:insert_element(1 + tuple_size(K1), K1, Bin).

-if(?DOCATTRS).
-doc """
Sends a synchronous call through the durable queue and waits for the reply.

`Module` is the gen_server-compatible module used to send the initial message
(typically `gen_server`). On timeout, the reply keys are cleared to prevent
durable key leaks.
""".
-endif.
-spec call(module(), gen_server:server_ref(), term(), timeout()) -> term().
call(Module, Server, Request, Timeout) ->
    T1 = erlang:monotonic_time(millisecond),

    CallResult =
        try
            Module:call(Server, Request, Timeout)
        catch
            error:{timeout, _} ->
                % gen_server:call timed out before returning. If push_call
                % committed, the reply keys are abandoned (we don't have From).
                % They will be cleaned up when the dgen_server is killed,
                % which clears the entire waiting-key range.
                {noreply, {undefined, undefined, undefined}}
        end,

    case CallResult of
        {reply, Reply} ->
            Reply;
        {noreply, {Tenant, From, Watch}} ->
            T2 = erlang:monotonic_time(millisecond),

            case await_call_reply(Tenant, From, Watch, Timeout - (T2 - T1)) of
                {error, timeout} ->
                    cleanup_call(Tenant, From),
                    erlang:error(timeout);
                Reply ->
                    Reply
            end
    end.

await_call_reply(undefined, _From, _Watch, _Timeout) ->
    {error, timeout};
await_call_reply(_Tenant, _From, _Watch, Timeout) when Timeout =< 0 -> {error, timeout};
await_call_reply(Tenant, From, ?FUTURE(WatchRef), Timeout) ->
    T1 = erlang:monotonic_time(millisecond),

    Result =
        receive
            {WatchRef, ready} ->
                handle_call_ready(Tenant, From)
        after Timeout ->
            {error, timeout}
        end,

    T2 = erlang:monotonic_time(millisecond),

    case Result of
        {reply, Reply} ->
            Reply;
        {watch, Watch} ->
            await_call_reply(Tenant, From, Watch, Timeout - (T2 - T1));
        {error, timeout} ->
            {error, timeout}
    end.

cleanup_call(undefined, _From) ->
    ok;
cleanup_call(Tenant, From) ->
    dgen_mod_state_codec:clear_term(Tenant, From).

handle_call_ready(Tenant, From) ->
    dgen_backend:transactional(Tenant, fun({Tx, Dir}) ->
        B = dgen_config:backend(),
        case dgen_mod_state_codec:read_term({Tx, Dir}, From) of
            {error, not_found} ->
                {watch, B:watch(Tx, dgen_mod_state_codec:term_first_key(Dir, From))};
            {ok, noreply} ->
                {watch, B:watch(Tx, dgen_mod_state_codec:term_first_key(Dir, From))};
            {ok, {reply, Reply}} ->
                dgen_mod_state_codec:clear_term({Tx, Dir}, From),
                {reply, Reply}
        end
    end).
