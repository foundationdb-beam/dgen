-module(dgen_server).
-behaviour(gen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
A durable, distributed gen_server backed by a pluggable storage backend.

A dgen_server is an abstract entity composed of durable state and operations on
that state. The state lives in the configured backend (default: FoundationDB)
and the operations are defined by a callback module implementing the
`dgen_server` behaviour. This allows a gen_server to outlive any single Erlang
process, node, or cluster.

Zero or more Erlang processes may act on a dgen_server at any time. Processes
with `consume` enabled consume messages from the durable queue and invoke
callbacks; processes without it only publish messages.

## Options

The following options may be passed via the `Opts` proplist:

- `tenant` (required) - `{DbHandle, Dir}` pair identifying the backend subspace.
- `consume` (default `true`) - whether this process consumes messages from
  the queue.
- `reset` (default `false`) - when `true`, re-initialise the durable state
  even if it already exists.

## Callbacks

- `init/1` - return `{ok, State}` or `{ok, Tuid, State}`.
- `handle_call/3` - return `{reply, Reply, State}` or
  `{reply, Reply, State, Actions}`.
- `handle_cast/2` - return `{noreply, State}` or `{noreply, State, Actions}`.
- `handle_info/2` - return `{noreply, State}` or `{noreply, State, Actions}`.

`Actions` is a list of 1-arity funs executed after the transaction commits. The
argument is the
""".
-endif.

-export([
    start/3, start/4,
    start_link/3, start_link/4,
    cast/2,
    cast_k/2,
    priority_cast/2,
    priority_call/2, priority_call/3,
    call/2, call/3,
    kill/2
]).

-include("../include/dgen.hrl").

-type state() :: term().
-type action() :: fun().
-type tuid() :: tuple().
-type from() :: term().
-type event_type() :: {call, from()} | cast | info.
-type init_ret() :: {ok, state()} | {ok, tuid(), state()} | {error, term()}.
-type lock_ret() :: {lock, state()}.
-type reply_ret() :: {reply, term(), state()} | {reply, term(), state(), [action()]}.
-type noreply_ret() :: {noreply, state()} | {noreply, state(), [action()]}.
-type stop_ret() :: {stop, term(), state()}.

-callback init(Args :: term()) -> init_ret().
-callback handle_cast(Msg :: term(), State :: state()) -> noreply_ret() | lock_ret() | stop_ret().
-callback handle_call(Request :: term(), From :: from(), State :: state()) ->
    reply_ret() | noreply_ret() | lock_ret() | stop_ret().
-callback handle_info(Info :: term(), State :: state()) -> noreply_ret() | stop_ret().
-callback handle_locked(EventType :: event_type(), Msg :: term(), State :: state()) ->
    reply_ret() | noreply_ret() | stop_ret().

-optional_callbacks([handle_cast/2, handle_call/3, handle_info/2, handle_locked/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type server() :: gen_server:server_ref().
-type option() ::
    {tenant, dgen_backend:tenant()}
    | {consume, boolean()}
    | {reset, boolean()}
    | {cache, boolean()}
    | gen_server:start_opt().
-type options() :: [option()].
-type start_ret() :: gen_server:start_ret().

-export_type([server/0, option/0, options/0]).

-define(DefaultTuid(Mod), {<<"dgen_server">>, atom_to_binary(Mod)}).
-define(TxCallbackTimeout, 5000).
-define(DefaultCallTimeout, 5000).

-record(state, {
    tenant :: dgen_backend:tenant(),
    mod :: atom(),
    tuid :: tuple(),
    watch :: undefined | dgen_backend:future(),
    cache :: boolean(),
    mod_state_cache ::
        undefined
        | {dgen_backend:versionstamp() | dgen_backend:future(), {ok, term()} | {error, not_found}},
    cache_misses = 0 :: non_neg_integer()
}).

-if(?DOCATTRS).
-doc """
Starts a dgen_server process without linking.

See `start_link/3` for details on `Mod`, `Arg`, and `Opts`.
""".
-endif.
-spec start(module(), term(), options()) -> start_ret().
start(Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    Cache = proplists:get_value(cache, Opts, true),
    gen_server:start(?MODULE, {get_tenant(Opts), Mod, Arg, Consume, Reset, Cache}, Opts).

-if(?DOCATTRS).
-doc """
Starts a dgen_server process without linking, registered as `Reg`.

See `start_link/3` for details on `Mod`, `Arg`, and `Opts`.
""".
-endif.
-spec start(gen_server:server_name(), module(), term(), options()) -> start_ret().
start(Reg, Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    Cache = proplists:get_value(cache, Opts, true),
    gen_server:start(Reg, ?MODULE, {get_tenant(Opts), Mod, Arg, Consume, Reset, Cache}, Opts).

-if(?DOCATTRS).
-doc """
Starts a dgen_server process linked to the calling process.

- `Mod` is the callback module implementing the `dgen_server` behaviour.
- `Arg` is passed to `Mod:init/1`.
- `Opts` is a proplist that must include `{tenant, {Db, Dir}}` and may
  include `consume` and `reset`.
""".
-endif.
-spec start_link(module(), term(), options()) -> start_ret().
start_link(Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    Cache = proplists:get_value(cache, Opts, true),
    gen_server:start_link(?MODULE, {get_tenant(Opts), Mod, Arg, Consume, Reset, Cache}, Opts).

-if(?DOCATTRS).
-doc """
Starts a dgen_server process linked to the calling process, registered as `Reg`.

See `start_link/3` for details on `Mod`, `Arg`, and `Opts`.
""".
-endif.
-spec start_link(gen_server:server_name(), module(), term(), options()) -> start_ret().
start_link(Reg, Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    Cache = proplists:get_value(cache, Opts, true),
    gen_server:start_link(Reg, ?MODULE, {get_tenant(Opts), Mod, Arg, Consume, Reset, Cache}, Opts).

-if(?DOCATTRS).
-doc "Sends an asynchronous cast request to the dgen_server's durable queue.".
-endif.
-spec cast(server(), term()) -> ok.
cast(Server, Request) ->
    cast_k(Server, [Request]).

-if(?DOCATTRS).
-doc "Sends a batch of cast requests to the dgen_server's durable queue atomically.".
-endif.
-spec cast_k(server(), [term()]) -> ok.
cast_k(Server, Requests) ->
    gen_server:cast(Server, {cast, Requests}).

-if(?DOCATTRS).
-doc "Sends a synchronous call request via the durable queue. Default timeout 5000ms.".
-endif.
-spec call(server(), term()) -> term().
call(Server, Request) ->
    call(Server, Request, ?DefaultCallTimeout).

-if(?DOCATTRS).
-doc """
Sends a synchronous call request via the durable queue.

The request is enqueued durably and the caller blocks until a consumer
processes it and writes the reply, or until `Timeout` milliseconds elapse.

## Options

- `timeout`: Default `5000`. Timeout in milliseconds, or `infinity`.
""".
-endif.
-spec call(server(), term(), timeout() | list()) -> term().
call(Server, Request, Timeout) when Timeout =:= infinity orelse is_integer(Timeout) ->
    call(Server, Request, [{timeout, Timeout}]);
call(Server, Request, Options) when is_list(Options) ->
    {Timeout, CallOptions} =
        case lists:keytake(timeout, 1, Options) of
            false ->
                {?DefaultCallTimeout, Options};
            {value, {timeout, T}, Rest} ->
                {T, Rest}
        end,
    dgen:call(gen_server, Server, {call, Request, self(), CallOptions}, Timeout).

-if(?DOCATTRS).
-doc """
Sends a cast that bypasses the durable queue and is handled immediately.

Use with caution: this breaks ordering guarantees with respect to queued
messages and ignores locks.
""".
-endif.
-spec priority_cast(server(), term()) -> ok.
priority_cast(Server, Request) ->
    gen_server:cast(Server, {priority, Request}).

-if(?DOCATTRS).
-doc """
Sends a call that bypasses the durable queue and is handled immediately.

Use with caution: this breaks ordering guarantees with respect to queued
messages and ignores locks. Can be useful for snapshot reads.
""".
-endif.
-spec priority_call(server(), term()) -> term().
priority_call(Server, Request) ->
    gen_server:call(Server, {priority, Request}).

-if(?DOCATTRS).
-doc "Like `priority_call/2` but with an explicit timeout.".
-endif.
-spec priority_call(server(), term(), timeout()) -> term().
priority_call(Server, Request, Timeout) ->
    gen_server:call(Server, {priority, Request}, Timeout).

-if(?DOCATTRS).
-doc """
Kills the dgen_server, deleting all durable state, queue items, and waiting
call keys. The process exits with `Reason`.
""".
-endif.
-spec kill(server(), term()) -> ok.
kill(Server, Reason) ->
    gen_server:cast(Server, {kill, Reason}).

get_tenant(Opts) ->
    case proplists:get_value(tenant, Opts) of
        undefined ->
            erlang:error({badarg, required, tenant});
        Tenant ->
            Tenant
    end.

-spec init(term()) -> {ok, #state{}} | {error, term()}.
init({Tenant, Mod, Arg, Consume, Reset, Cache}) ->
    case init_tuid(Mod, Arg) of
        {ok, Tuid, InitialState} ->
            State = #state{tenant = Tenant, mod = Mod, tuid = Tuid, cache = Cache},
            {_, State1} = init_mod_state(Tenant, InitialState, Reset, State),
            [gen_server:cast(self(), consume) || Consume],
            {ok, State1};
        Other ->
            Other
    end.

-spec handle_call(term(), gen_server:from(), #state{}) ->
    {reply, term(), #state{}}.
handle_call(
    {call, Request, WatchTo, Options},
    _LocalFrom,
    State = #state{tenant = Tenant, tuid = Tuid, watch = Watch}
) ->
    case Watch of
        undefined ->
            {From, NewWatch} = dgen_backend:transactional(Tenant, fun(Td) ->
                dgen:push_call(Td, Tuid, Request, WatchTo, Options)
            end),
            LocalReply = {noreply, {Tenant, From, NewWatch}},
            {reply, LocalReply, State};
        _ ->
            LocalFrom = make_ref(),

            PushFun = fun(Td, State0) ->
                {From, NewWatch} = dgen:push_call(Td, Tuid, Request, WatchTo, Options),
                {push, From, NewWatch, State0}
            end,

            % @todo if the callback throws, we need to push the call to provide the same behavior as
            % the normal path
            Result = dgen_backend:transactional(Tenant, fun(Td) ->
                case dgen_queue:length(Td, State#state.tuid) of
                    0 ->
                        case is_locked(Td, State) of
                            true ->
                                PushFun(Td, State);
                            false ->
                                consume_call(Td, Request, LocalFrom, State)
                        end;
                    _ ->
                        PushFun(Td, State)
                end
            end),

            case Result of
                {{reply, Reply, Actions}, ModState, State2} ->
                    State3 = resolve_version(State2),
                    _ = handle_actions(Actions, [], ModState),
                    LocalReply = {reply, Reply},
                    {reply, LocalReply, State3};
                {{lock, EventType, Msg}, ModState, State2} ->
                    case consume_locked(EventType, Msg, ModState, true, State2) of
                        {reply, Reply, State3} ->
                            {reply, {reply, Reply}, State3};
                        Other ->
                            Other
                    end;
                {stop, Reason, State2} ->
                    {stop, Reason, State2};
                {push, From, NewWatch, State1} ->
                    LocalReply = {noreply, {Tenant, From, NewWatch}},
                    {reply, LocalReply, State1}
            end
    end;
handle_call({priority, Request}, _From, State = #state{tenant = Tenant}) ->
    LocalFrom = make_ref(),
    Result = dgen_backend:transactional(Tenant, fun(Td) ->
        consume_call(Td, Request, LocalFrom, State)
    end),
    case Result of
        {{reply, Reply, Actions}, ModState, State1} ->
            State2 = resolve_version(State1),
            _ = handle_actions(Actions, [], ModState),
            {reply, Reply, State2};
        {{lock, EventType, Msg}, ModState, State1} ->
            consume_locked(EventType, Msg, ModState, true, State1);
        {stop, Reason, State1} ->
            {stop, Reason, State1}
    end.

-spec handle_cast(term(), #state{}) ->
    {noreply, #state{}} | {stop, term(), #state{}}.
handle_cast(consume, State = #state{tenant = Tenant, tuid = Tuid}) ->
    % 1 at a time because we are limited to 5 seconds per transaction
    K = 1,
    Ret = handle_consume(Tenant, K, Tuid, State),
    case Ret of
        {noreply, #state{watch = undefined, cache_misses = Misses}} when Misses > 0 ->
            Delay = min(1 bsl (Misses - 1), 50),
            erlang:send_after(Delay, self(), consume_after_penalty);
        {noreply, #state{watch = undefined}} ->
            gen_server:cast(self(), consume);
        _ ->
            ok
    end,
    Ret;
handle_cast({cast, Requests}, State = #state{tenant = Tenant, tuid = Tuid}) ->
    dgen_queue:push_k(Tenant, Tuid, [{cast, Request} || Request <- Requests]),
    {noreply, State};
handle_cast({priority, Request}, State = #state{tenant = Tenant}) ->
    Result = dgen_backend:transactional(Tenant, fun(Td) ->
        consume_cast(Td, Request, State)
    end),
    case Result of
        {{noreply, Actions}, ModState, State1} ->
            State2 = resolve_version(State1),
            _ = handle_actions(Actions, [], ModState),
            {noreply, State2};
        {{lock, EventType, Msg}, ModState, State1} ->
            consume_locked(EventType, Msg, ModState, true, State1);
        {stop, Reason, State1} ->
            {stop, Reason, State1}
    end;
handle_cast({kill, Reason}, State = #state{tenant = Tenant, tuid = Tuid}) ->
    delete(Tenant, Tuid),
    {stop, Reason, State#state{mod_state_cache = undefined}}.

-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info({Ref, ready}, State = #state{watch = ?FUTURE(Ref)}) ->
    handle_cast(consume, State#state{watch = undefined});
handle_info(consume_after_penalty, State) ->
    handle_cast(consume, State);
handle_info(Info, State = #state{tenant = Tenant}) ->
    Result = dgen_backend:transactional(Tenant, fun(Td) ->
        consume_info(Td, Info, State)
    end),
    case Result of
        {{noreply, Actions}, ModState, State2} ->
            State3 = resolve_version(State2),
            _ = handle_actions(Actions, [], ModState),
            {noreply, State3}
    end.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

invoke_tx_callback(Td, Callback, Args, State = #state{mod = Mod}) ->
    T1 = erlang:monotonic_time(millisecond),
    Arity = length(Args) + 1,
    Result =
        case erlang:function_exported(Mod, Callback, Arity) of
            true ->
                case get_mod_state(Td, State) of
                    {{ok, ModState}, State1} ->
                        Args2 = modify_args_for_callback(Callback, Args, ModState),
                        CallbackResult = erlang:apply(Mod, Callback, Args2),
                        {ok, ModState, CallbackResult, State1};
                    {{error, not_found}, _State1} ->
                        {error, {mod_state_not_found, Mod}}
                end;
            false ->
                {error, {function_not_exported, {Mod, Callback, Arity}}}
        end,
    T2 = erlang:monotonic_time(millisecond),
    if
        T2 - T1 > ?TxCallbackTimeout ->
            erlang:error(tooslow);
        true ->
            Result
    end.

invoke_handle_locked_callback(EventType, Msg, ModState, State = #state{mod = Mod}) ->
    case erlang:function_exported(Mod, handle_locked, 3) of
        true ->
            CallbackResult = erlang:apply(Mod, handle_locked, [EventType, Msg, ModState]),
            {ok, ModState, CallbackResult, State};
        false ->
            {error, {function_not_exported, {Mod, handle_locked, 3}}}
    end.

modify_args_for_callback(_, Args, ModState) ->
    Args ++ [ModState].

delete(Tenant, Tuid) ->
    dgen_backend:transactional(Tenant, fun(Td = {Tx, Dir}) ->
        B = dgen_config:backend(),
        clear_mod_state(Td, Tuid),
        dgen_queue:delete(Td, Tuid),
        WaitingKey = dgen:get_waiting_key(Tuid),
        {SK, EK} = B:dir_range(Dir, WaitingKey),
        B:clear_range(Tx, SK, EK)
    end).

init_mod_state(Tenant, InitialState, Reset, State) ->
    {Result, State1} = dgen_backend:transactional(Tenant, fun(Td) ->
        case {Reset, get_mod_state(Td, State)} of
            {false, {{ok, _ModState}, State1}} ->
                {ok, State1};
            {true, {_ModState, State1}} ->
                set_mod_state(Td, undefined, InitialState, State1);
            {_, {{error, not_found}, State1}} ->
                set_mod_state(Td, undefined, InitialState, State1)
        end
    end),
    {Result, resolve_version(State1)}.

clear_mod_state(Td, Tuid) ->
    dgen_mod_state_codec:clear(Td, get_state_key(Tuid)).

get_mod_state(Td, State = #state{cache = true}) ->
    #state{tuid = Tuid, mod_state_cache = MSCache} = State,
    {Vsn, ModState} =
        case MSCache of
            {CVsn, {ok, CModState}} ->
                {CVsn, CModState};
            _ ->
                {<<>>, undefined}
        end,
    case dgen_mod_state_codec:get_version(Td, get_state_key(Tuid)) of
        {ok, Vsn} ->
            {{ok, ModState}, State#state{cache_misses = 0}};
        {ok, OtherVsn} ->
            ActualModStateResult = dgen_mod_state_codec:get(Td, get_state_key(Tuid)),
            MSCache1 = {OtherVsn, ActualModStateResult},
            Misses = State#state.cache_misses,
            {ActualModStateResult, State#state{
                mod_state_cache = MSCache1, cache_misses = Misses + 1
            }};
        {error, not_found} ->
            ActualModStateResult = dgen_mod_state_codec:get(Td, get_state_key(Tuid)),
            {ActualModStateResult, State#state{mod_state_cache = undefined}}
    end;
get_mod_state(Td, State = #state{}) ->
    #state{tuid = Tuid} = State,
    {dgen_mod_state_codec:get(Td, get_state_key(Tuid)), State}.

set_mod_state(_Td, ModState, ModState, State) ->
    {ok, State};
set_mod_state(Td = {Tx, _Dir}, OrigModState, ModState, State = #state{cache = Cache}) ->
    B = dgen_config:backend(),
    % Always write a versioned ModState, regardless of cache flag
    Result = dgen_mod_state_codec:set(
        Td, get_state_key(State#state.tuid), OrigModState, ModState, [{versioned, true}]
    ),
    State1 =
        case Cache of
            true ->
                VF = B:get_versionstamp(Tx),
                State#state{mod_state_cache = {VF, {ok, ModState}}};
            false ->
                State
        end,
    {Result, State1}.

handle_callback_result(Td, handle_cast, _EventType, _Msg, {noreply, ModState}, OrigModState, State) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, []}, ModState, State1};
handle_callback_result(
    Td, handle_cast, _EventType, _Msg, {noreply, ModState, Actions}, OrigModState, State
) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, Actions}, ModState, State1};
handle_callback_result(
    Td, handle_call, _EventType, _Msg, {reply, Reply, ModState}, OrigModState, State
) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{reply, Reply, []}, ModState, State1};
handle_callback_result(
    Td, handle_call, _EventType, _Msg, {reply, Reply, ModState, Actions}, OrigModState, State
) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{reply, Reply, Actions}, ModState, State1};
handle_callback_result(Td, handle_info, _EventType, _Msg, {noreply, ModState}, OrigModState, State) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, []}, ModState, State1};
handle_callback_result(
    Td, handle_info, _EventType, _Msg, {noreply, ModState, Actions}, OrigModState, State
) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, Actions}, ModState, State1};
handle_callback_result(
    Td, handle_locked, _EventType, _Msg, {noreply, ModState}, OrigModState, State
) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, []}, ModState, State1};
handle_callback_result(
    Td, handle_locked, _EventType, _Msg, {noreply, ModState, Actions}, OrigModState, State
) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, Actions}, ModState, State1};
handle_callback_result(
    Td, handle_locked, _EventType, _Msg, {reply, Reply, ModState}, OrigModState, State
) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{reply, Reply, []}, ModState, State1};
handle_callback_result(
    Td, handle_locked, _EventType, _Msg, {reply, Reply, ModState, Actions}, OrigModState, State
) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{reply, Reply, Actions}, ModState, State1};
handle_callback_result(Td, _Callback, EventType, Msg, {lock, ModState}, OrigModState, State) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    State2 = set_lock(Td, State1),
    {{lock, EventType, Msg}, ModState, State2};
handle_callback_result(
    Td, _Callback, _EventType, _Msg, {stop, Reason, ModState}, OrigModState, State
) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {stop, Reason, State1}.

handle_actions([], Acc, _ModState) ->
    lists:append(lists:reverse(Acc));
handle_actions([Action | Actions], Acc, ModState) ->
    case Action(ModState) of
        {cont, Items} ->
            handle_actions(Actions, [Items | Acc], ModState);
        halt ->
            ok;
        _ ->
            handle_actions(Actions, Acc, ModState)
    end.

get_state_key(Tuple) ->
    dgen_key:extend(Tuple, <<"s">>).

handle_consume(Tenant, K, Tuid, State) ->
    Result = dgen_backend:transactional(Tenant, fun(Td) ->
        case is_locked(Td, State) of
            true ->
                Watch = dgen_queue:watch_push(Td, Tuid),
                {{noreply, []}, undefined, State#state{watch = Watch}};
            false ->
                case dgen_queue:consume_k(Td, K, Tuid) of
                    {[{call, Request, From, _CallOptions}], Watch} ->
                        case consume_call(Td, Request, From, State#state{watch = Watch}) of
                            {{reply, Reply, Actions}, ModState, State2} ->
                                set_reply(Td, From, Reply),
                                {{noreply, Actions}, ModState, State2};
                            Result ->
                                Result
                        end;
                    {[{cast, Request}], Watch} ->
                        consume_cast(Td, Request, State#state{watch = Watch});
                    {[], Watch} ->
                        {{noreply, []}, undefined, State#state{watch = Watch}}
                end
        end
    end),
    case Result of
        {{noreply, Actions}, ModState, State2} ->
            State3 = resolve_version(State2),
            _ = handle_actions(Actions, [], ModState),
            {noreply, State3};
        {{lock, EventType, Msg}, ModState, State2} ->
            State3 = resolve_version(State2),
            consume_locked(EventType, Msg, ModState, false, State3);
        {{stop, Reason}, _ModState, State2} ->
            {stop, Reason, State2}
    end.

consume_call(Td, Request, From, State) ->
    case invoke_tx_callback(Td, handle_call, [Request, From], State) of
        {error, Reason} ->
            erlang:error(Reason);
        {ok, OrigModState, CallbackResult, State1} ->
            handle_callback_result(
                Td, handle_call, {call, From}, Request, CallbackResult, OrigModState, State1
            )
    end.

consume_cast(Td, Request, State) ->
    case invoke_tx_callback(Td, handle_cast, [Request], State) of
        {error, Reason} ->
            erlang:error(Reason);
        {ok, OrigModState, CallbackResult, State1} ->
            handle_callback_result(
                Td, handle_cast, cast, Request, CallbackResult, OrigModState, State1
            )
    end.

consume_locked(EventType, Msg, ModState, IsLocalReply, State = #state{tenant = Tenant}) ->
    Result =
        try invoke_handle_locked_callback(EventType, Msg, ModState, State) of
            {error, Reason} ->
                erlang:error(Reason);
            {ok, OrigModState, CallbackResult, State1} ->
                dgen_backend:transactional(Tenant, fun(Td) ->
                    case
                        handle_callback_result(
                            Td,
                            handle_locked,
                            EventType,
                            Msg,
                            CallbackResult,
                            OrigModState,
                            State1
                        )
                    of
                        {{reply, Reply, Actions}, LModState, State2} ->
                            {call, From} = EventType,
                            if
                                IsLocalReply ->
                                    {{reply, Reply, Actions}, LModState, State2};
                                true ->
                                    set_reply(Td, From, Reply),
                                    {{noreply, Actions}, LModState, State2}
                            end;
                        {{noreply, Actions}, LModState, State2} ->
                            {{noreply, Actions}, LModState, State2};
                        {stop, Reason, State2} ->
                            {{stop, Reason}, undefined, State2}
                    end
                end)
        after
            dgen_backend:transactional(Tenant, fun(Td) ->
                clear_lock(Td, State)
            end)
        end,

    case Result of
        {{reply, Reply, Actions}, LModState, State3} ->
            State4 = resolve_version(State3),
            _ = handle_actions(Actions, [], LModState),
            {reply, Reply, State4};
        {{noreply, Actions}, LModState, State3} ->
            State4 = resolve_version(State3),
            _ = handle_actions(Actions, [], LModState),
            {noreply, State4};
        {{stop, Reason1}, _LModState, State3} ->
            {stop, Reason1, State3}
    end.

consume_info(Td, Info, State) ->
    case invoke_tx_callback(Td, handle_info, [Info], State) of
        {error, _} ->
            {{noreply, []}, undefined, State};
        {ok, OrigModState, CallbackResult, State1} ->
            handle_callback_result(
                Td, handle_info, info, Info, CallbackResult, OrigModState, State1
            )
    end.

set_reply({Tx, Dir}, From, Reply) ->
    B = dgen_config:backend(),
    ReplySentinelKey = dgen_mod_state_codec:term_first_key(Dir, From),
    % Skip writing the reply if the caller timed out and cleared the reply keys
    case B:wait(B:get(Tx, ReplySentinelKey)) of
        not_found ->
            ok;
        _ ->
            dgen_mod_state_codec:clear_term({Tx, Dir}, From),
            dgen_mod_state_codec:write_term({Tx, Dir}, From, {reply, Reply})
    end.

init_tuid(Mod, Arg) ->
    case Mod:init(Arg) of
        {ok, InitialState} ->
            {ok, ?DefaultTuid(Mod), InitialState};
        Other ->
            Other
    end.

resolve_version(State = #state{mod_state_cache = {VF, ModStateResult}}) ->
    B = dgen_config:backend(),
    State#state{mod_state_cache = {B:wait(VF), ModStateResult}};
resolve_version(State) ->
    State.

set_lock({Tx, Dir}, State = #state{tuid = Tuid}) ->
    B = dgen_config:backend(),
    B:set(Tx, B:dir_pack(Dir, get_lock_key(Tuid)), <<>>),
    State.

clear_lock(Td = {Tx, Dir}, #state{tuid = Tuid}) ->
    B = dgen_config:backend(),
    LockKey = B:dir_pack(Dir, get_lock_key(Tuid)),
    B:clear_range(Tx, LockKey, B:key_strinc(LockKey)),
    dgen_queue:notify(Td, Tuid).

get_lock_key(Tuid) ->
    dgen_key:extend(Tuid, <<"k">>).

is_locked({Tx, Dir}, #state{tuid = Tuid}) ->
    B = dgen_config:backend(),
    case B:wait(B:get(Tx, B:dir_pack(Dir, get_lock_key(Tuid)))) of
        not_found ->
            false;
        _ ->
            true
    end.
