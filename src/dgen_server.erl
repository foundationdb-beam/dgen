-module(dgen_server).
-behaviour(gen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
A durable, distributed gen_server backed by FoundationDB.

A dgen_server is an abstract entity composed of durable state and operations on
that state. The state lives in FoundationDB and the operations are defined by a
callback module implementing the `dgen_server` behaviour. This allows a
gen_server to outlive any single Erlang process, node, or cluster.

Zero or more Erlang processes may act on a dgen_server at any time. Processes
with `consume` enabled consume messages from the durable queue and invoke
callbacks; processes without it only publish messages.

## Options

The following options may be passed via the `Opts` proplist:

- `tenant` (required) - `{erlfdb:database(), erlfdb_directory:dir()}` pair
  identifying the FDB subspace.
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

-callback init(Args :: term()) ->
    {ok, State :: term()} | {ok, Tuid :: tuple(), State :: term()} | {error, Reason :: term()}.
-callback handle_cast(Msg :: term(), State :: term()) -> {noreply, NewState :: term()}.
-callback handle_call(Request :: term(), From :: term(), State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} | {noreply, NewState :: term()}.
-callback handle_info(Info :: term(), State :: term()) -> {noreply, NewState :: term()}.

-optional_callbacks([handle_cast/2, handle_call/3, handle_info/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DefaultTuid(Mod), {<<"dgen_server">>, atom_to_binary(Mod)}).
-define(TxCallbackTimeout, 5000).

-record(state, {tenant, mod, tuid, watch}).

-if(?DOCATTRS).
-doc """
Starts a dgen_server process without linking.

See `start_link/3` for details on `Mod`, `Arg`, and `Opts`.
""".
-endif.
start(Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    gen_server:start(?MODULE, {get_tenant(Opts), Mod, Arg, Consume, Reset}, Opts).

-if(?DOCATTRS).
-doc """
Starts a dgen_server process without linking, registered as `Reg`.

See `start_link/3` for details on `Mod`, `Arg`, and `Opts`.
""".
-endif.
start(Reg, Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    gen_server:start(Reg, ?MODULE, {get_tenant(Opts), Mod, Arg, Consume, Reset}, Opts).

-if(?DOCATTRS).
-doc """
Starts a dgen_server process linked to the calling process.

- `Mod` is the callback module implementing the `dgen_server` behaviour.
- `Arg` is passed to `Mod:init/1`.
- `Opts` is a proplist that must include `{tenant, {Db, Dir}}` and may
  include `consume` and `reset`.
""".
-endif.
start_link(Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    gen_server:start_link(?MODULE, {get_tenant(Opts), Mod, Arg, Consume, Reset}, Opts).

-if(?DOCATTRS).
-doc """
Starts a dgen_server process linked to the calling process, registered as `Reg`.

See `start_link/3` for details on `Mod`, `Arg`, and `Opts`.
""".
-endif.
start_link(Reg, Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    gen_server:start_link(Reg, ?MODULE, {get_tenant(Opts), Mod, Arg, Consume, Reset}, Opts).

-if(?DOCATTRS).
-doc "Sends an asynchronous cast request to the dgen_server's durable queue.".
-endif.
cast(Server, Request) ->
    cast_k(Server, [Request]).

-if(?DOCATTRS).
-doc "Sends a batch of cast requests to the dgen_server's durable queue atomically.".
-endif.
cast_k(Server, Requests) ->
    gen_server:cast(Server, {cast, Requests}).

-if(?DOCATTRS).
-doc "Sends a synchronous call request via the durable queue. Default timeout 5000ms.".
-endif.
call(Server, Request) ->
    call(Server, Request, 5000).

-if(?DOCATTRS).
-doc """
Sends a synchronous call request via the durable queue.

The request is enqueued durably and the caller blocks until a consumer
processes it and writes the reply, or until `Timeout` milliseconds elapse.
""".
-endif.
call(Server, Request, Timeout) ->
    dgen:call(gen_server, Server, {call, Request, self()}, Timeout).

-if(?DOCATTRS).
-doc """
Sends a cast that bypasses the durable queue and is handled immediately.

Use with caution: this breaks ordering guarantees with respect to queued
messages.
""".
-endif.
priority_cast(Server, Request) ->
    gen_server:cast(Server, {priority, Request}).

-if(?DOCATTRS).
-doc """
Sends a call that bypasses the durable queue and is handled immediately.

Use with caution: this breaks ordering guarantees. Useful for snapshot reads.
""".
-endif.
priority_call(Server, Request) ->
    gen_server:call(Server, {priority, Request}).

-if(?DOCATTRS).
-doc "Like `priority_call/2` but with an explicit timeout.".
-endif.
priority_call(Server, Request, Timeout) ->
    gen_server:call(Server, {priority, Request}, Timeout).

-if(?DOCATTRS).
-doc """
Kills the dgen_server, deleting all durable state, queue items, and waiting
call keys. The process exits with `Reason`.
""".
-endif.
kill(Server, Reason) ->
    gen_server:cast(Server, {kill, Reason}).

get_tenant(Opts) ->
    case proplists:get_value(tenant, Opts) of
        undefined ->
            erlang:error({badarg, required, tenant});
        Tenant ->
            Tenant
    end.

init({Tenant, Mod, Arg, Consume, Reset}) ->
    case init_tuid(Mod, Arg) of
        {ok, Tuid, InitialState} ->
            State = #state{tenant = Tenant, mod = Mod, tuid = Tuid},
            init_mod_state(Tenant, InitialState, Reset, State),
            [gen_server:cast(self(), consume) || Consume],
            {ok, State};
        Other ->
            Other
    end.

handle_call({call, Request, WatchTo}, _LocalFrom, State = #state{tenant = Tenant, tuid = Tuid}) ->
    % if there's no watch, then assume the queue is nonempty, and we must push the request
    %
    % @todo: if there's a watch, we can pay for a queue length check, assuming it will be 0 in most cases. If
    % it is zero, then we can handle the call immediately without pushing it onto the queue.
    {CallKeyBin, Watch} = dgen:push_call(Tenant, Tuid, Request, WatchTo),
    {reply, {noreply, {Tenant, CallKeyBin, Watch}}, State};
handle_call({priority, Request}, _From, State = #state{tenant = Tenant}) ->
    From = make_ref(),
    {Actions, ModState, Reply, State2} = handle_new_priority_call(Tenant, Request, From, State),
    _ = handle_actions(Actions, [], ModState),
    {reply, Reply, State2}.

handle_cast(consume, State = #state{tenant = Tenant, tuid = Tuid}) ->
    % 1 at a time because we are limited to 5 seconds per transaction
    K = 1,
    {Watch, Actions, ModState, State2} = handle_consume(Tenant, K, Tuid, State),
    _ = handle_actions(Actions, [], ModState),
    case Watch of
        undefined ->
            gen_server:cast(self(), consume);
        _ ->
            ok
    end,
    {noreply, State2#state{watch = Watch}};
handle_cast({cast, Requests}, State = #state{tenant = Tenant, tuid = Tuid}) ->
    dgen_queue:push_k(Tenant, Tuid, [{cast, Request} || Request <- Requests]),
    {noreply, State};
handle_cast({priority, Request}, State = #state{tenant = Tenant}) ->
    {Actions, ModState, State2} = handle_new_priority_cast(Tenant, Request, State),
    _ = handle_actions(Actions, [], ModState),
    {noreply, State2};
handle_cast({kill, Reason}, State = #state{tenant = Tenant, tuid = Tuid}) ->
    delete(Tenant, Tuid),
    {stop, Reason, State}.

handle_info({Ref, ready}, State = #state{watch = ?FUTURE(Ref)}) ->
    handle_cast(consume, State#state{watch = undefined});
handle_info(Info, State = #state{tenant = Tenant}) ->
    {Actions, ModState, State2} = handle_info(Tenant, Info, State),
    _ = handle_actions(Actions, [], ModState),
    {noreply, State2}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

invoke_tx_callback(?IS_TD = Td, Callback, Args, State = #state{mod = Mod}) ->
    T1 = erlang:monotonic_time(millisecond),
    Arity = length(Args) + 1,
    Result =
        case erlang:function_exported(Mod, Callback, Arity) of
            true ->
                case get_mod_state(Td, State) of
                    {ok, ModState} ->
                        Args2 = modify_args_for_callback(Callback, Args, ModState),
                        CallbackResult = erlang:apply(Mod, Callback, Args2),
                        {ok,
                            handle_callback_result(
                                Td, Callback, CallbackResult, ModState, State
                            ),
                            ModState};
                    {error, not_found} ->
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

modify_args_for_callback(_, Args, ModState) ->
    Args ++ [ModState].

delete(?IS_DB(Db, Dir), Tuid) ->
    erlfdb:transactional(Db, fun(Tx) -> delete({Tx, Dir}, Tuid) end);
delete(Td = ?IS_TD = ?IS_TX(Tx, Dir), Tuid) ->
    clear_mod_state(Td, Tuid),
    dgen_queue:delete(Td, Tuid),
    WaitingKey = dgen:get_waiting_key(Tuid),
    {SK, EK} = erlfdb_directory:range(Dir, WaitingKey),
    erlfdb:clear_range(Tx, SK, EK).

init_mod_state(?IS_DB(Db, Dir), InitialState, Reset, State) ->
    erlfdb:transactional(Db, fun(Tx) -> init_mod_state({Tx, Dir}, InitialState, Reset, State) end);
init_mod_state(?IS_TD = Td, InitialState, Reset, State) ->
    case {Reset, get_mod_state(Td, State)} of
        {false, {ok, _ModState}} ->
            ok;
        {true, _ModState} ->
            set_mod_state(Td, InitialState, State);
        {_, {error, not_found}} ->
            set_mod_state(Td, InitialState, State)
    end.

clear_mod_state(Td, Tuid) ->
    dgen_mod_state_codec:clear(Td, get_state_key(Tuid)).

get_mod_state(Td, _State = #state{tuid = Tuid}) ->
    dgen_mod_state_codec:get(Td, get_state_key(Tuid)).

set_mod_state(_Td, ModState, ModState, _State) ->
    ok;
set_mod_state(Td, OrigModState, ModState, State) ->
    dgen_mod_state_codec:set(Td, get_state_key(State#state.tuid), OrigModState, ModState).

set_mod_state(Td, ModState, _State = #state{tuid = Tuid}) ->
    dgen_mod_state_codec:set(Td, get_state_key(Tuid), ModState).

handle_callback_result(?IS_TD = Td, handle_cast, {noreply, ModState}, OrigModState, State) ->
    set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, State}, []};
handle_callback_result(?IS_TD = Td, handle_cast, {noreply, ModState, Actions}, OrigModState, State) ->
    set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, State}, Actions};
handle_callback_result(?IS_TD = Td, handle_call, {reply, Reply, ModState}, OrigModState, State) ->
    set_mod_state(Td, OrigModState, ModState, State),
    {{reply, Reply, State}, []};
handle_callback_result(
    ?IS_TD = Td, handle_call, {reply, Reply, ModState, Actions}, OrigModState, State
) ->
    set_mod_state(Td, OrigModState, ModState, State),
    {{reply, Reply, State}, Actions};
handle_callback_result(?IS_TD = Td, handle_info, {noreply, ModState}, OrigModState, State) ->
    set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, State}, []};
handle_callback_result(?IS_TD = Td, handle_info, {noreply, ModState, Actions}, OrigModState, State) ->
    set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, State}, Actions}.

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
    erlang:insert_element(1 + tuple_size(Tuple), Tuple, <<"s">>).

handle_new_priority_call(?IS_DB(Db, Dir), Request, From, State) ->
    erlfdb:transactional(Db, fun(Tx) ->
        handle_new_priority_call({Tx, Dir}, Request, From, State)
    end);
handle_new_priority_call(?IS_TD = Td, Request, From, State) ->
    case invoke_tx_callback(Td, handle_call, [Request, From], State) of
        {error, Reason = {function_not_exported, _}} ->
            erlang:error(Reason);
        {ok, {{reply, Reply, State2}, Actions}, ModState} ->
            {Actions, ModState, Reply, State2}
    end.

handle_new_priority_cast(?IS_DB(Db, Dir), Request, State) ->
    erlfdb:transactional(Db, fun(Tx) -> handle_new_priority_cast({Tx, Dir}, Request, State) end);
handle_new_priority_cast(?IS_TD = Td, Request, State) ->
    case invoke_tx_callback(Td, handle_cast, [Request], State) of
        {error, Reason = {function_not_exported, _}} ->
            erlang:error(Reason);
        {ok, {{noreply, State2}, Actions}, ModState} ->
            {Actions, ModState, State2}
    end.

handle_consume(?IS_DB(Db, Dir), K, Tuid, State) ->
    erlfdb:transactional(Db, fun(Tx) -> handle_consume({Tx, Dir}, K, Tuid, State) end);
handle_consume(?IS_TD = Td, K, Tuid, State) ->
    case dgen_queue:consume_k(Td, K, Tuid) of
        {[{call, Request, From}], Watch} ->
            case invoke_tx_callback(Td, handle_call, [Request, From], State) of
                {error, Reason = {function_not_exported, _}} ->
                    erlang:error(Reason);
                {ok, {{reply, Reply, State2}, Actions}, ModState} ->
                    reply(Td, From, Reply),
                    {Watch, Actions, ModState, State2}
            end;
        {[{cast, Request}], Watch} ->
            case invoke_tx_callback(Td, handle_cast, [Request], State) of
                {error, Reason = {function_not_exported, _}} ->
                    erlang:error(Reason);
                {ok, {{noreply, State2}, Actions}, ModState} ->
                    {Watch, Actions, ModState, State2}
            end;
        {[], Watch} ->
            {Watch, [], undefined, State}
    end.

reply(?IS_TD = _Td = ?IS_TX(Tx, Dir), From, Reply) ->
    CallKeyBin = erlfdb_directory:pack(Dir, From),
    % Skip writing the reply if the caller timed out and cleared CallKeyBin
    case erlfdb:wait(erlfdb:get(Tx, CallKeyBin)) of
        not_found -> ok;
        _ -> erlfdb:set(Tx, CallKeyBin, term_to_binary({reply, Reply}))
    end.

handle_info(?IS_DB(Db, Dir), Info, State) ->
    erlfdb:transactional(Db, fun(Tx) -> handle_info({Tx, Dir}, Info, State) end);
handle_info(?IS_TD = Td, Info, State) ->
    case invoke_tx_callback(Td, handle_info, [Info], State) of
        {error, {function_not_exported, _}} ->
            {[], State};
        {ok, {{noreply, State2}, Actions}, ModState} ->
            {Actions, ModState, State2}
    end.

init_tuid(Mod, Arg) ->
    case Mod:init(Arg) of
        {ok, InitialState} ->
            {ok, ?DefaultTuid(Mod), InitialState};
        Other ->
            Other
    end.
