-module(dgen_server).
-behaviour(gen_server).

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

-callback init(Args :: term()) -> {ok, State :: term()} | {error, Reason :: term()}.
-callback key(State :: term()) -> Key :: tuple().
-callback handle_cast(Msg :: term(), State :: term()) -> {noreply, NewState :: term()}.
-callback handle_call(Request :: term(), From :: term(), State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} | {noreply, NewState :: term()}.
-callback handle_info(Info :: term(), State :: term()) -> {noreply, NewState :: term()}.

-optional_callbacks([key/1, handle_cast/2, handle_call/3, handle_info/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DefaultKey(Mod), {<<"dgen_server">>, atom_to_binary(Mod)}).
-define(TxCallbackTimeout, 5000).

-record(state, {tenant, mod, key, watch}).

start(Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    gen_server:start(?MODULE, {get_tenant(Opts), Mod, Arg, Consume, Reset}, Opts).

start(Reg, Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    gen_server:start(Reg, ?MODULE, {get_tenant(Opts), Mod, Arg, Consume, Reset}, Opts).

start_link(Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    gen_server:start_link(?MODULE, {get_tenant(Opts), Mod, Arg, Consume, Reset}, Opts).

start_link(Reg, Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    gen_server:start_link(Reg, ?MODULE, {get_tenant(Opts), Mod, Arg, Consume, Reset}, Opts).

cast(Server, Request) ->
    cast_k(Server, [Request]).

cast_k(Server, Requests) ->
    gen_server:cast(Server, {cast, Requests}).

call(Server, Request) ->
    call(Server, Request, 5000).

call(Server, Request, Timeout) ->
    dgen:call(gen_server, Server, {call, Request, self()}, Timeout).

priority_cast(Server, Request) ->
    gen_server:cast(Server, {priority, Request}).

priority_call(Server, Request) ->
    gen_server:call(Server, {priority, Request}).

priority_call(Server, Request, Timeout) ->
    gen_server:call(Server, {priority, Request}, Timeout).

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
    case Mod:init(Arg) of
        {ok, InitialState} ->
            State0 = #state{tenant = Tenant, mod = Mod},
            State = State0#state{
                key = invoke_pure_callback(key, [InitialState], State0, ?DefaultKey(Mod))
            },
            init_mod_state(Tenant, InitialState, Reset, State),
            [gen_server:cast(self(), consume) || Consume],
            {ok, State};
        Other ->
            Other
    end.

handle_call(
    {call, Request, WatchTo}, _LocalFrom, State = #state{tenant = Tenant, key = Key, watch = undefined}
) ->
    % if there's no watch, then assume the queue is nonempty, and push the request
    {CallKey, Watch} =  dgen_queue:push_call(Tenant, Key, Request, WatchTo),
    {reply, {Tenant, CallKey, Watch}, State};
handle_call(
    {call, Request, WatchTo}, _LocalFrom, State = #state{tenant = Tenant, key = Key, watch = _ConsumeWatch}
) ->
    % if there's a watch, we pay for the queue length check, assuming it will be 0 in most cases. If
    % it is zero, then we can handle the call immediately without pushing it onto the queue.
    {Resp, Actions} = handle_new_call(Tenant, Tenant, Key, Request, WatchTo, State),
    _ = handle_actions(Actions),
    Resp;
handle_call({priority, Request}, _From, State = #state{tenant = Tenant}) ->
    From = make_ref(),
    {Actions, Reply, State2} = handle_new_priority_call(Tenant, Request, From, State),
    _ = handle_actions(Actions),
    {reply, Reply, State2}.

handle_cast(consume, State = #state{tenant = Tenant, key = Key}) ->
    % 1 at a time because we are limited to 5 seconds per transaction
    K = 1,
    {Watch, Actions, State2} = handle_consume(Tenant, K, Key, State),
    _ = handle_actions(Actions),
    case Watch of
        undefined ->
            gen_server:cast(self(), consume);
        _ ->
            ok
    end,
    {noreply, State2#state{watch = Watch}};
handle_cast({cast, Requests}, State = #state{tenant = Tenant, key = Key}) ->
    dgen_queue:push_k(Tenant, Key, [{cast, Request} || Request <- Requests]),
    {noreply, State};
handle_cast({priority, Request}, State = #state{tenant = Tenant}) ->
    {Actions, State2} = handle_new_priority_cast(Tenant, Request, State),
    _ = handle_actions(Actions),
    {noreply, State2};
handle_cast({kill, Reason}, State = #state{tenant = Tenant, key = Key}) ->
    delete(Tenant, Key),
    {stop, Reason, State}.

handle_info({Ref, ready}, State = #state{watch = {erlfdb_future, Ref, _}}) ->
    handle_cast(consume, State#state{watch = undefined});
handle_info(Info, State = #state{tenant = Tenant}) ->
    {Actions, State2} = handle_info(Tenant, Info, State),
    _ = handle_actions(Actions),
    {noreply, State2}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

invoke_tx_callback(?IS_DB(Db, Dir), Callback, Args, State) ->
    erlfdb:transactional(Db, fun(Tx) -> invoke_tx_callback({Tx, Dir}, Callback, Args, State) end);
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
                            )};
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

invoke_pure_callback(Callback, Args, State, Default) ->
    case invoke_pure_callback(Callback, Args, State) of
        {ok, Result} ->
            Result;
        {error, {function_not_exported, _}} ->
            Default
    end.

invoke_pure_callback(Callback, Args, #state{mod = Mod}) ->
    Arity = length(Args),
    case erlang:function_exported(Mod, Callback, Arity) of
        true ->
            {ok, erlang:apply(Mod, Callback, Args)};
        false ->
            {error, {function_not_exported, {Mod, Callback, Arity}}}
    end.

modify_args_for_callback(_, Args, ModState) ->
    Args ++ [ModState].

delete(?IS_DB(Db, Dir), Key) ->
    erlfdb:transactional(Db, fun(Tx) -> delete({Tx, Dir}, Key) end);
delete(?IS_TX(Tx, Dir), Key) ->
    clear_mod_state(Tx, Key),
    dgen_queue:delete(Tx, Key),
    WaitingKey = dgen:get_waiting_key(Key),
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

clear_mod_state(?IS_DB(Db, Dir), Key) ->
    erlfdb:transactional(Db, fun(Tx) -> clear_mod_state({Tx, Dir}, Key) end);
clear_mod_state(?IS_TX(Tx, Dir), Key) ->
    StateKey = get_state_key(Key),
    {SK, EK} = erlfdb_directory:range(Dir, StateKey),
    erlfdb:clear_range(Tx, SK, EK).

get_mod_state(?IS_DB(Db, Dir), State) ->
    erlfdb:transactional(Db, fun(Tx) -> get_mod_state({Tx, Dir}, State) end);
get_mod_state(?IS_TX(Tx, Dir), _State = #state{key = Key}) ->
    StateKey = get_state_key(Key),
    {SK, EK} = erlfdb_directory:range(Dir, StateKey),
    case erlfdb:get_range(Tx, SK, EK, [{wait, true}]) of
        [] ->
            {error, not_found};
        KVs ->
            {_, Vs} = lists:unzip(KVs),
            {ok, binary_to_term(iolist_to_binary(Vs))}
    end.

set_mod_state(_DbOrTx, ModState, ModState, _State) ->
    ok;
set_mod_state(DbOrTx, _OrigModState, ModState, State) ->
    set_mod_state(DbOrTx, ModState, State).

set_mod_state(?IS_DB(Db, Dir), ModState, State) ->
    erlfdb:transactional(Db, fun(Tx) -> set_mod_state({Tx, Dir}, ModState, State) end);
set_mod_state(?IS_TX(Tx, Dir), ModState, _State = #state{key = Key}) ->
    Bin = term_to_binary(ModState),
    Chunks = binary_chunk_every(Bin, 100000, []),
    StateKey = get_state_key(Key),
    {ChunkKeys, {FirstUnused, EK}} = partition_chunked_key(Dir, StateKey, length(Chunks)),
    [erlfdb:set(Tx, erlfdb_directory:pack(Dir, K), Chunk) || {K, Chunk} <- lists:zip(ChunkKeys, Chunks)],
    erlfdb:clear_range(Tx, erlfdb_directory:pack(Dir, FirstUnused), EK),
    ok.

handle_callback_result(?IS_DB(Db, Dir), Origin, Result, OrigModState, State) ->
    erlfdb:transactional(Db, fun(Tx) -> handle_callback_result({Tx, Dir}, Origin, Result, OrigModState, State) end);
handle_callback_result(?IS_TD = Td, handle_cast, {noreply, ModState}, OrigModState, State) ->
    set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, State}, []};
handle_callback_result(?IS_TD = Td, handle_cast, {noreply, ModState, Actions}, OrigModState, State) ->
    set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, State}, Actions};
handle_callback_result(?IS_TD = Td, handle_call, {reply, Reply, ModState}, OrigModState, State) ->
    set_mod_state(Td, OrigModState, ModState, State),
    {{reply, Reply, State}, []};
handle_callback_result(?IS_TD = Td, handle_call, {reply, Reply, ModState, Actions}, OrigModState, State) ->
    set_mod_state(Td, OrigModState, ModState, State),
    {{reply, Reply, State}, Actions};
handle_callback_result(?IS_TD = Td, handle_info, {noreply, ModState}, OrigModState, State) ->
    set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, State}, []};
handle_callback_result(?IS_TD = Td, handle_info, {noreply, ModState, Actions}, OrigModState, State) ->
    set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, State}, Actions}.

binary_chunk_every(<<>>, _Size, Acc) ->
    lists:reverse(Acc);
binary_chunk_every(Bin, Size, Acc) ->
    case Bin of
        <<Chunk:Size/binary, Rest/binary>> ->
            binary_chunk_every(Rest, Size, [Chunk | Acc]);
        Chunk ->
            lists:reverse([Chunk | Acc])
    end.

handle_actions([]) ->
    ok;
handle_actions([Action | Actions]) ->
    Action(),
    handle_actions(Actions).

get_state_key(Tuple) ->
    erlang:insert_element(1 + tuple_size(Tuple), Tuple, <<"s">>).

partition_chunked_key(Dir, BaseKey, N) ->
    {_, EK} = erlfdb_directory:range(Dir, BaseKey),
    ChunkKey = erlang:insert_element(1 + tuple_size(BaseKey), BaseKey, 0),
    ChunkKeys = [erlang:setelement(tuple_size(ChunkKey), ChunkKey, X) || X <- lists:seq(0, N - 1)],
    FirstUnused = erlang:setelement(tuple_size(ChunkKey), ChunkKey, N),
    {ChunkKeys, {FirstUnused, EK}}.

handle_new_call(?IS_DB(Db, Dir), Tenant, Key, Request, WatchTo, State) ->
    erlfdb:transactional(Db, fun(Tx) -> handle_new_call({Tx, Dir}, Tenant, Key, Request, WatchTo, State) end);
handle_new_call(?IS_TD = Td, Tenant, Key, Request, WatchTo, State) ->
    case dgen_queue:length(Td, Key) of
        0 ->
            WaitingKey = dgen:get_waiting_key(Key),
            From = dgen:get_from(WaitingKey, make_ref()),
            case invoke_tx_callback(Td, handle_call, [Request, From], State) of
                {error, Reason = {function_not_exported, _}} ->
                    erlang:error(Reason);
                {ok, {{reply, Reply, State2}, Actions}} ->
                    {{reply, {reply, Reply}, State2}, Actions}
            end;
        _Len ->
            {CallKey, Watch} = dgen:push_call(Td, Key, Request, WatchTo),
            {{reply, {noreply, {Tenant, CallKey, Watch}}, State}, []}
    end.

handle_new_priority_call(?IS_DB(Db, Dir), Request, From, State) ->
    erlfdb:transactional(Db, fun(Tx) -> handle_new_priority_call({Tx, Dir}, Request, From, State) end);
handle_new_priority_call(?IS_TD = Td, Request, From, State) ->
    case invoke_tx_callback(Td, handle_call, [Request, From], State) of
        {error, Reason = {function_not_exported, _}} ->
            erlang:error(Reason);
        {ok, {{reply, Reply, State2}, Actions}} ->
            {Actions, Reply, State2}
    end.

handle_new_priority_cast(?IS_DB(Db, Dir), Request, State) ->
    erlfdb:transactional(Db, fun(Tx) -> handle_new_priority_cast({Tx, Dir}, Request, State) end);
handle_new_priority_cast(?IS_TD = Td, Request, State) ->
    case invoke_tx_callback(Td, handle_cast, [Request], State) of
        {error, Reason = {function_not_exported, _}} ->
            erlang:error(Reason);
        {ok, {{noreply, State2}, Actions}} ->
            {Actions, State2}
    end.

handle_consume(?IS_DB(Db, Dir), K, Key, State) ->
    erlfdb:transactional(Db, fun(Tx) -> handle_consume({Tx, Dir}, K, Key, State) end);
handle_consume(?IS_TD = Td = ?IS_TX(Tx, Dir), K, Key, State) ->
    case dgen_queue:consume_k(Td, K, Key) of
        {[{call, Request, From}], Watch} ->
            case invoke_tx_callback(Td, handle_call, [Request, From], State) of
                {error, Reason = {function_not_exported, _}} ->
                    erlang:error(Reason);
                {ok, {{reply, Reply, State2}, Actions}} ->
                    erlfdb:set(
                        Tx, erlfdb_directory:pack(Dir, From), term_to_binary({reply, Reply})
                    ),
                    {Watch, Actions, State2}
            end;
        {[{cast, Request}], Watch} ->
            case invoke_tx_callback(Td, handle_cast, [Request], State) of
                {error, Reason = {function_not_exported, _}} ->
                    erlang:error(Reason);
                {ok, {{noreply, State2}, Actions}} ->
                    {Watch, Actions, State2}
            end;
        {[], Watch} ->
            {Watch, [], State}
    end.

handle_info(?IS_DB(Db, Dir), Info, State) ->
    erlfdb:transactional(Db, fun(Tx) -> handle_info({Tx, Dir}, Info, State) end);
handle_info(?IS_TD = Td, Info, State) ->
    case invoke_tx_callback(Td, handle_info, [Info], State) of
        {error, {function_not_exported, _}} ->
            {[], State};
        {ok, {{noreply, State2}, Actions}} ->
            {Actions, State2}
    end.
