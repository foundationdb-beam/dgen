-module(dgen_server).
-behaviour(gen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("../include/dgen_eta.hrl").

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
- `dead_letter_threshold` (default `infinity`) - number of consecutive
  processing failures before a message is treated as a dead letter. When a
  message has failed this many times it is moved to the dead-letter queue, the
  caller raises `{dead_letter, N}` (for `call` messages), and the optional
  `handle_dead_letter/2` callback is invoked. `infinity` (the default) disables
  dead-lettering entirely.
- `consume_k` (default `1`) - the maximum number of queued messages a consumer
  processes per transaction (the batch size). Each consume cycle peeks up to
  `consume_k` messages, processes them one at a time while carrying the durable
  state in memory, and commits the whole batch in a **single** transaction (the
  state is read once and written once). It then immediately re-arms itself, so a
  consumer that has work keeps draining batches.

  Raising `consume_k` amortises the fixed per-transaction cost (read version,
  state read/write, commit) across more messages, increasing throughput when the
  per-message callback work is small. The cost is larger transactions and a
  coarser unit of progress (a batch is all-or-nothing; on a conflict the whole
  batch retries). Because a busy consumer keeps draining, a larger `consume_k`
  also tends to keep a **single** node as the active consumer, which is useful
  when the callback's identity matters — for example an election-style callback
  where you want to minimise churn in who processes successive messages.

  See "consume_k and inlining" below for how `consume_k > 1` changes call
  handling.
- `lock_timeout` (default `infinity`) - maximum number of milliseconds a
  distributed lock may be held before other consumers treat it as stale.  When
  a callback returns `{lock, State}`, `dgen_server` sets a timestamped lock key
  in the backend that pauses all other consumers while `handle_locked/4` runs.
  Under normal operation the lock holder clears the key itself before
  `handle_locked` returns.  If the holder is killed (SIGKILL, VM abort) the
  lock persists until another consumer detects staleness: it re-checks
  `lock_timeout` ms after the lock was set and clears it if that deadline has
  passed.  `infinity` (the default) disables stale-lock detection entirely —
  a dead holder will permanently block all consumers.  Set this to a value
  safely larger than the worst-case `handle_locked` duration for your callback.

## consume_k and inlining

A message can reach a callback by two routes:

1. **Through the durable queue.** `cast/2` and (normally) `call/2` enqueue the
   message; a consumer later peeks it as part of a `consume_k`-sized batch and
   processes it inside the batch's single transaction. This is the ordered,
   durable path, and the only path when `consume_k > 1`.
2. **Inline.** As a latency optimisation, when `consume_k =:= 1` a `call/2` whose
   queue is currently empty and unlocked is processed *immediately*, in the
   caller's own request transaction, instead of being enqueued and waited on.
   The result is identical to processing it through the queue; it just skips the
   enqueue/await round-trip.

When **`consume_k > 1`, inlining is disabled**: every `call/2` goes through the
queue and the batched consume loop, so `consume_k` is always in effect. Use this
when you want all processing funnelled through the batched, single-consumer loop —
for example to keep a stable consumer identity (an election-style callback that
would otherwise see successive messages committed by different nodes). With the
default `consume_k =:= 1`, inlining is enabled and a contention-free `call/2` is
served on the fast path.

Independently of `consume_k`, `priority_call/2` and `priority_cast/2` **always**
bypass the queue (and locks) and are handled immediately. They are an explicit
escape hatch for urgent, usually read-only work; they trade away ordering with
respect to queued messages, so prefer `call/2`/`cast/2` for anything that must be
ordered with the rest of the stream.

## Callbacks

- `init/1` - return `{ok, State}` or `{ok, Tuid, State}`.
- `handle_call/3` - return `{reply, Reply, State}` or
  `{reply, Reply, State, Actions}`.
- `handle_cast/2` - return `{noreply, State}` or `{noreply, State, Actions}`.
- `handle_info/2` - return `{noreply, State}` or `{noreply, State, Actions}`.
- `handle_dead_letter/2` (optional) - called after a message is dead-lettered.
  Receives `(Msg, AttemptCount)`. Return value is ignored. Useful for custom
  alerting or metrics.

`Actions` is a list of 1-arity funs executed after the transaction commits. The
argument is the module state as committed by that transaction — note that under
batched consumption (`consume_k > 1`) all of a batch's actions run after the
batch's single commit and each receives the state as of the **end of the batch**,
not the state at the message that produced the action.

## Module State

The module that implements the `dgen_server` behaviour may define any term to
serve as the State. `dgen_server` will encode this state for writing to the
database. We encourage you to structure your state to fit in with this
encoding scheme, which will yield performance benefits.

- **term** (fallback) - `term_to_binary`, chunked into 100KB values.
- **assigns map** - map with all atom keys; each entry stored at a
  separate DB key using `atom_to_binary(Key)` in the path.
- **component list** - list of maps where every item has an atom `id`
  key with a binary value; each item stored separately, ordered by a
  fractional index embedded in the DB key.

The encoding is applied recursively. For example, an assigns map whose value
is a component list will nest both encodings in the key path.

*This documentation is LLM-generated. See the AI disclosure in `README.md`.*
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
    kill/2,
    get_quid/1,
    outbox_cast/1, outbox_cast/2
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
-type stop_ret() :: {stop, term(), state()} | {stop, term(), state(), [action()]}.

%% Passed as the first argument to `_tx` callback variants.
%% `td` is the current backend transaction+directory pair; `tuid` is the server's
%% tenant-unique identifier.  Both may be used to read or write arbitrary keys
%% within the same atomic transaction as the callback.
-type tx_ctx() :: #{td := dgen_backend:tenant(), tuid := tuid()}.

%% Passed as the first argument to `handle_locked/4`.
%% `db` is the DB-level tenant (not a transaction); `tuid` is the server's
%% tenant-unique identifier.  Use `dgen_backend:transactional/2` to open
%% explicit transactions within the locked section.
-type db_ctx() :: #{db := dgen_backend:tenant(), tuid := tuid()}.

-callback init(Args :: term()) -> init_ret().
-callback handle_cast(Msg :: term(), State :: state()) -> noreply_ret() | lock_ret() | stop_ret().
-callback handle_cast_tx(TxCtx :: tx_ctx(), Msg :: term(), State :: state()) ->
    noreply_ret() | lock_ret() | stop_ret().
-callback handle_call(Request :: term(), From :: from(), State :: state()) ->
    reply_ret() | lock_ret() | stop_ret().
-callback handle_call_tx(TxCtx :: tx_ctx(), Request :: term(), From :: from(), State :: state()) ->
    reply_ret() | lock_ret() | stop_ret().
-callback handle_info(Info :: term(), State :: state()) -> noreply_ret() | stop_ret().
-callback handle_info_tx(TxCtx :: tx_ctx(), Info :: term(), State :: state()) ->
    noreply_ret() | stop_ret().
-callback handle_locked(
    DbCtx :: db_ctx(), EventType :: event_type(), Msg :: term(), State :: state()
) ->
    reply_ret() | noreply_ret() | stop_ret().

-callback handle_dead_letter(Msg :: term(), AttemptCount :: non_neg_integer()) -> any().

-optional_callbacks([
    handle_cast/2,
    handle_cast_tx/3,
    handle_call/3,
    handle_call_tx/4,
    handle_info/2,
    handle_info_tx/3,
    handle_locked/4,
    handle_dead_letter/2
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type server() :: gen_server:server_ref().
-type option() ::
    {tenant, dgen_backend:tenant()}
    | {consume, boolean()}
    | {reset, boolean()}
    | {cache, boolean()}
    | {dead_letter_threshold, pos_integer() | infinity}
    | {lock_timeout, pos_integer() | infinity}
    | gen_server:start_opt().
-type options() :: [option()].
-type start_ret() :: gen_server:start_ret().

-export_type([
    server/0,
    option/0,
    options/0,
    tx_ctx/0,
    db_ctx/0,
    tuid/0,
    from/0,
    event_type/0,
    lock_ret/0,
    reply_ret/0,
    noreply_ret/0,
    init_ret/0,
    stop_ret/0
]).

-define(DefaultTuid(Mod), {<<"dgen_server">>, atom_to_binary(Mod)}).
-define(TxCallbackTimeout, 5000).
-define(DefaultCallTimeout, 5000).

% the state data structure is externally versioned
-define(CodecVersion, 0).

-record(state, {
    tenant :: dgen_backend:tenant(),
    mod :: atom(),
    tuid :: tuple(),
    watch :: undefined | dgen_backend:future(),
    cache :: boolean(),
    mod_state_cache ::
        undefined
        | {dgen_backend:versionstamp() | dgen_backend:future(), {ok, term()} | {error, not_found}},
    cache_misses = 0 :: non_neg_integer(),
    dead_letter_threshold = infinity :: pos_integer() | infinity,
    consume_k = 1 :: pos_integer(),
    lock_timeout = infinity :: pos_integer() | infinity
}).

-type internalstate() :: #state{}.

-if(?DOCATTRS).
-doc """
Starts a dgen_server process without linking.

See `start_link/3` for details on `Mod`, `Arg`, and `Opts`.
""".
-endif.
-spec start(module(), term(), options()) -> start_ret().
start(Mod, Arg, Opts) ->
    {Tenant, Consume, Reset, Cache, DLT, ConsumeK, LockTimeout} = parse_opts(Opts),
    gen_server:start(
        ?MODULE, {Tenant, Mod, Arg, Consume, Reset, Cache, DLT, ConsumeK, LockTimeout}, Opts
    ).

-if(?DOCATTRS).
-doc """
Starts a dgen_server process without linking, registered as `Reg`.

See `start_link/3` for details on `Mod`, `Arg`, and `Opts`.
""".
-endif.
-spec start(gen_server:server_name(), module(), term(), options()) -> start_ret().
start(Reg, Mod, Arg, Opts) ->
    {Tenant, Consume, Reset, Cache, DLT, ConsumeK, LockTimeout} = parse_opts(Opts),
    gen_server:start(
        Reg, ?MODULE, {Tenant, Mod, Arg, Consume, Reset, Cache, DLT, ConsumeK, LockTimeout}, Opts
    ).

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
    {Tenant, Consume, Reset, Cache, DLT, ConsumeK, LockTimeout} = parse_opts(Opts),
    gen_server:start_link(
        ?MODULE, {Tenant, Mod, Arg, Consume, Reset, Cache, DLT, ConsumeK, LockTimeout}, Opts
    ).

-if(?DOCATTRS).
-doc """
Starts a dgen_server process linked to the calling process, registered as `Reg`.

See `start_link/3` for details on `Mod`, `Arg`, and `Opts`.
""".
-endif.
-spec start_link(gen_server:server_name(), module(), term(), options()) -> start_ret().
start_link(Reg, Mod, Arg, Opts) ->
    {Tenant, Consume, Reset, Cache, DLT, ConsumeK, LockTimeout} = parse_opts(Opts),
    gen_server:start_link(
        Reg, ?MODULE, {Tenant, Mod, Arg, Consume, Reset, Cache, DLT, ConsumeK, LockTimeout}, Opts
    ).

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

-if(?DOCATTRS).
-doc """
Returns a closure for atomically casting a message from within the caller's
own backend transaction.

Call this before opening the transaction as a preparatory step. Bind the
result to `Cast` and call `Cast(Tx, Message)` inside the transaction to
enqueue the message without going through the dgen_server process. The queue
directory and identifier are captured internally and not exposed to the
caller.

## Backend coupling

This function is intended for callers that are already operating directly
with a backend transaction — for example, when a message must be enqueued
atomically alongside other writes in the same transaction. Using it means
intentionally stepping outside the gen_server abstraction: the caller takes
responsibility for managing the transaction lifetime and is coupled to the
configured backend. If you do not need to compose the enqueue with other
backend writes, prefer `cast/2` instead.
""".
-endif.
-spec outbox_cast(server()) -> fun((dgen_backend:tx(), term()) -> ok).
outbox_cast(Server) ->
    outbox_cast(Server, ?DefaultCallTimeout).

-spec outbox_cast(server(), timeout()) -> fun((dgen_backend:tx(), term()) -> ok).
outbox_cast(Server, Timeout) ->
    gen_server:call(Server, outbox_cast, Timeout).

parse_opts(Opts) ->
    Tenant =
        case proplists:get_value(tenant, Opts) of
            undefined -> erlang:error({badarg, required, tenant});
            T -> T
        end,
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    Cache = proplists:get_value(cache, Opts, true),
    DeadLetterThreshold = proplists:get_value(dead_letter_threshold, Opts, infinity),
    ConsumeK = proplists:get_value(consume_k, Opts, 1),
    LockTimeout = proplists:get_value(lock_timeout, Opts, infinity),
    {Tenant, Consume, Reset, Cache, DeadLetterThreshold, ConsumeK, LockTimeout}.

-spec init(term()) -> {ok, internalstate()} | {error, term()}.
init({Tenant, Mod, Arg, Consume, Reset, Cache, DeadLetterThreshold, ConsumeK, LockTimeout}) ->
    case init_tuid(Mod, Arg) of
        {ok, Tuid, InitialState} ->
            State = #state{
                tenant = Tenant,
                mod = Mod,
                tuid = Tuid,
                cache = Cache,
                dead_letter_threshold = DeadLetterThreshold,
                consume_k = ConsumeK,
                lock_timeout = LockTimeout
            },
            {_, State1} = init_mod_state(Tenant, InitialState, Reset, State),
            [gen_server:cast(self(), consume) || Consume],
            {ok, State1};
        Other ->
            Other
    end.

-spec handle_call(term(), gen_server:from(), internalstate()) -> {reply, term(), internalstate()}.
handle_call({call, Request, WatchTo, Options}, _LocalFrom, State = #state{watch = undefined}) ->
    #state{tenant = Tenant, tuid = Tuid} = State,
    {From, NewWatch} = dgen_backend:transactional(Tenant, fun(Td) ->
        dgen:push_call(Td, Tuid, get_quid(Tuid), Request, WatchTo, Options)
    end),
    LocalReply = {noreply, {Tenant, From, NewWatch}},
    {reply, LocalReply, State};
handle_call({call, Request, WatchTo, Options}, GsFrom, State = #state{}) ->
    #state{tenant = Tenant, tuid = Tuid} = State,
    LocalFrom = make_ref(),
    Result = dgen_backend:transactional(Tenant, fun(Td) ->
        inline_or_push(Td, Request, LocalFrom, WatchTo, Options, Tuid, State)
    end),
    finalize_inline_call(Result, GsFrom, Tenant);
handle_call(outbox_cast, _From, State = #state{tenant = {_Db, Dir}, tuid = Tuid}) ->
    Quid = get_quid(Tuid),
    Closure = fun(Tx, Message) ->
        dgen_queue:push_k({Tx, Dir}, Quid, [{cast, Message}])
    end,
    {reply, Closure, State};
handle_call({priority, Request}, _From, State = #state{tenant = Tenant}) ->
    LocalFrom = make_ref(),
    Result = dgen_backend:transactional(Tenant, fun(Td) ->
        consume_call(Td, Request, LocalFrom, State)
    end),
    case Result of
        {{lock, EventType, Msg}, ModState, State1} ->
            consume_locked(EventType, Msg, ModState, true, State1);
        _ ->
            finalize(Result)
    end.

inline_or_push(Td, Request, LocalFrom, WatchTo, Options, Tuid, State = #state{consume_k = K}) ->
    Push = fun(Attempts) ->
        dgen:push_call(Td, Tuid, get_quid(Tuid), Request, WatchTo, Options, Attempts)
    end,
    %% Inline handling — process the call immediately in this transaction instead of
    %% enqueuing it and waiting for the consume loop — is attempted only when
    %% consume_k =:= 1, and then only if the queue is empty and unlocked.  With
    %% consume_k > 1 the server is configured for batched, single-consumer processing,
    %% so inlining is disabled entirely: every call goes through the durable queue and
    %% the consume loop, keeping consume_k always in effect.  See "consume_k and
    %% inlining" in the moduledoc.  (`priority_call` always bypasses the queue,
    %% independent of consume_k.)
    InlineEligible = K =:= 1 andalso dgen_queue:length(Td, get_quid(Tuid)) =:= 0,
    case InlineEligible andalso not is_locked(Td, State) of
        true ->
            try
                consume_call(Td, Request, LocalFrom, State)
            catch
                Class:Reason:Stack ->
                    {From, NewWatch} = Push(1),
                    {push_after_fail, From, NewWatch, State, Class, Reason, Stack}
            end;
        false ->
            {From, NewWatch} = Push(0),
            {push, From, NewWatch, State}
    end.

finalize_inline_call(Result, GsFrom, Tenant) ->
    case Result of
        {{reply, Reply, Actions}, ModState, State} ->
            finalize({{reply, {reply, Reply}, Actions}, ModState, State});
        {{lock, EventType, Msg}, ModState, State} ->
            case consume_locked(EventType, Msg, ModState, true, State) of
                {reply, Reply, State2} -> {reply, {reply, Reply}, State2};
                Other -> Other
            end;
        {{stop, Reason, Actions}, ModState, State} ->
            finalize({{stop, Reason, Actions}, ModState, State});
        {push_after_fail, From, NewWatch, _State, Class, Reason, Stack} ->
            gen_server:reply(GsFrom, {noreply, {Tenant, From, NewWatch}}),
            erlang:raise(Class, Reason, Stack);
        {push, From, NewWatch, State} ->
            {reply, {noreply, {Tenant, From, NewWatch}}, State}
    end.

-spec handle_cast(term(), internalstate()) ->
    {noreply, internalstate()} | {stop, term(), internalstate()}.
handle_cast(consume, State = #state{tenant = Tenant, tuid = Tuid, consume_k = K}) ->
    Ret = handle_consume(Tenant, K, Tuid, State),
    case Ret of
        {noreply, #state{watch = undefined, cache_misses = Misses}} when Misses > 0 ->
            Delay = min(1 bsl (Misses - 1), 50),
            arm(Delay, consume_after_penalty);
        {noreply, #state{watch = undefined}} ->
            gen_server:cast(self(), consume);
        _ ->
            ok
    end,
    Ret;
handle_cast({cast, Requests}, State = #state{tenant = Tenant, tuid = Tuid}) ->
    dgen_queue:push_k(Tenant, get_quid(Tuid), [{cast, Request} || Request <- Requests]),
    {noreply, State};
handle_cast({priority, Request}, State = #state{tenant = Tenant}) ->
    Result = dgen_backend:transactional(Tenant, fun(Td) ->
        consume_cast(Td, Request, State)
    end),
    case Result of
        {{lock, EventType, Msg}, ModState, State1} ->
            consume_locked(EventType, Msg, ModState, true, State1);
        _ ->
            finalize(Result)
    end;
handle_cast({kill, Reason}, State = #state{tenant = Tenant, tuid = Tuid}) ->
    delete(Tenant, Tuid),
    {stop, Reason, State#state{mod_state_cache = undefined}}.

-spec handle_info(term(), internalstate()) ->
    {noreply, internalstate()} | {stop, term(), internalstate()}.
handle_info({Ref, ready}, State = #state{watch = ?FUTURE(Ref)}) ->
    handle_cast(consume, State#state{watch = undefined});
handle_info(consume_after_penalty, State) ->
    handle_cast(consume, State);
handle_info(recheck_lock, State) ->
    %% Fired by the timer scheduled in handle_consume when the lock was not yet
    %% stale.  Re-enter the consume loop so check_lock is re-evaluated; if
    %% the lock holder died without clearing the lock, it will now be stale.
    handle_cast(consume, State);
handle_info(Info, State = #state{tenant = Tenant}) ->
    Result = dgen_backend:transactional(Tenant, fun(Td) ->
        consume_info(Td, Info, State)
    end),
    finalize(Result).

-spec terminate(term(), internalstate()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), internalstate(), term()) -> {ok, internalstate()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Dispatches a callback, preferring the `_tx` variant (which receives a
%% `tx_ctx()` as its first argument) when the callback module exports it.
%%
%% Resolution order for a callback named `handle_foo/N`:
%%   1. `handle_foo_tx/(N+1)` — receives `#{td, tuid}` prepended to Args
%%   2. `handle_foo/N`        — standard variant, no tx context
invoke_tx_callback(Td, Callback, Args, State = #state{mod = Mod, tuid = Tuid}) ->
    T1 = erlang:monotonic_time(millisecond),
    Arity = length(Args) + 1,
    TxCallback = tx_callback_name(Callback),
    TxCtx = #{td => Td, tuid => Tuid},
    Result =
        case erlang:function_exported(Mod, TxCallback, Arity + 1) of
            true ->
                dispatch_callback(Td, undefined, Mod, TxCallback, [TxCtx | Args], State);
            false ->
                case erlang:function_exported(Mod, Callback, Arity) of
                    true ->
                        dispatch_callback(Td, undefined, Mod, Callback, Args, State);
                    false ->
                        {error, {function_not_exported, {Mod, Callback, Arity}}}
                end
        end,
    T2 = erlang:monotonic_time(millisecond),
    if
        T2 - T1 > ?TxCallbackTimeout ->
            erlang:error(tooslow);
        true ->
            Result
    end.

dispatch_callback(Td, undefined, Mod, Fn, Args, State) ->
    case get_mod_state(Td, State) of
        {{ok, ModState}, State1} when ModState =/= undefined ->
            {ok, _ModState, CallbackResult} = dispatch_callback(Td, ModState, Mod, Fn, Args, State),
            {ok, ModState, CallbackResult, State1};
        {{error, not_found}, _} ->
            {error, {mod_state_not_found, Mod}}
    end;
dispatch_callback(_Td, ModState, Mod, Fn, Args, _State) ->
    CallbackResult = erlang:apply(Mod, Fn, Args ++ [ModState]),
    {ok, ModState, CallbackResult}.

%% Like invoke_tx_callback/4 but uses CurrentModState directly instead of reading from FDB.
invoke_tx_callback_batch(
    Td, Callback, Args, CurrentModState, State = #state{mod = Mod, tuid = Tuid}
) ->
    T1 = erlang:monotonic_time(millisecond),
    Arity = length(Args) + 1,
    TxCallback = tx_callback_name(Callback),
    TxCtx = #{td => Td, tuid => Tuid},
    Result =
        case erlang:function_exported(Mod, TxCallback, Arity + 1) of
            true ->
                dispatch_callback(Td, CurrentModState, Mod, TxCallback, [TxCtx | Args], State);
            false ->
                case erlang:function_exported(Mod, Callback, Arity) of
                    true ->
                        dispatch_callback(Td, CurrentModState, Mod, Callback, Args, State);
                    false ->
                        {error, {function_not_exported, {Mod, Callback, Arity}}}
                end
        end,
    T2 = erlang:monotonic_time(millisecond),
    if
        T2 - T1 > ?TxCallbackTimeout ->
            erlang:error(tooslow);
        true ->
            Result
    end.

tx_callback_name(Callback) ->
    list_to_atom(atom_to_list(Callback) ++ "_tx").

invoke_handle_locked_callback(
    EventType, Msg, ModState, State = #state{mod = Mod, tenant = Tenant, tuid = Tuid}
) ->
    case erlang:function_exported(Mod, handle_locked, 4) of
        true ->
            DbCtx = #{db => Tenant, tuid => Tuid},
            CallbackResult = erlang:apply(Mod, handle_locked, [DbCtx, EventType, Msg, ModState]),
            {ok, ModState, CallbackResult, State};
        false ->
            {error, {function_not_exported, {Mod, handle_locked, 4}}}
    end.

delete(Tenant, Tuid) ->
    dgen_backend:transactional(Tenant, fun(Td = {Tx, Dir}) ->
        B = dgen_config:backend(),
        clear_mod_state(Td, Tuid),
        dgen_queue:delete(Td, get_quid(Tuid)),
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

handle_callback_result(Td, _EventType, _Msg, {noreply, ModState}, OrigModState, State) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, []}, ModState, State1};
handle_callback_result(Td, _EventType, _Msg, {noreply, ModState, Actions}, OrigModState, State) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{noreply, Actions}, ModState, State1};
handle_callback_result(Td, _EventType, _Msg, {reply, Reply, ModState}, OrigModState, State) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{reply, Reply, []}, ModState, State1};
handle_callback_result(
    Td, _EventType, _Msg, {reply, Reply, ModState, Actions}, OrigModState, State
) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{reply, Reply, Actions}, ModState, State1};
handle_callback_result(Td, EventType, Msg, {lock, ModState}, OrigModState, State) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    State2 = set_lock(Td, State1),
    {{lock, EventType, Msg}, ModState, State2};
handle_callback_result(Td, _EventType, _Msg, {stop, Reason, ModState}, OrigModState, State) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{stop, Reason, []}, ModState, State1};
handle_callback_result(
    Td, _EventType, _Msg, {stop, Reason, ModState, Actions}, OrigModState, State
) ->
    {_, State1} = set_mod_state(Td, OrigModState, ModState, State),
    {{stop, Reason, Actions}, ModState, State1}.

%% Batch variant of handle_callback_result: does NOT write mod state to the backend.
%% consume_batch is responsible for a single set_mod_state call at each exit point.
%% The lock case still sets the lock key within the transaction, as it must be
%% atomic with the mod state write and queue consume.
handle_callback_result_batch(_Td, _EventType, _Msg, {noreply, ModState}, _OrigModState, State) ->
    {{noreply, []}, ModState, State};
handle_callback_result_batch(
    _Td, _EventType, _Msg, {noreply, ModState, Actions}, _OrigModState, State
) ->
    {{noreply, Actions}, ModState, State};
handle_callback_result_batch(_Td, _EventType, _Msg, {reply, Reply, ModState}, _OrigModState, State) ->
    {{reply, Reply, []}, ModState, State};
handle_callback_result_batch(
    _Td, _EventType, _Msg, {reply, Reply, ModState, Actions}, _OrigModState, State
) ->
    {{reply, Reply, Actions}, ModState, State};
handle_callback_result_batch(Td, EventType, Msg, {lock, ModState}, _OrigModState, State) ->
    State1 = set_lock(Td, State),
    {{lock, EventType, Msg}, ModState, State1};
handle_callback_result_batch(_Td, _EventType, _Msg, {stop, Reason, ModState}, _OrigModState, State) ->
    {{stop, Reason, []}, ModState, State};
handle_callback_result_batch(
    _Td, _EventType, _Msg, {stop, Reason, ModState, Actions}, _OrigModState, State
) ->
    {{stop, Reason, Actions}, ModState, State}.

finalize({{noreply, Actions}, ModState, State}) ->
    State1 = resolve_version(State),
    _ = handle_actions(Actions, [], ModState),
    {noreply, State1};
finalize({{reply, Reply, Actions}, ModState, State}) ->
    State1 = resolve_version(State),
    _ = handle_actions(Actions, [], ModState),
    {reply, Reply, State1};
finalize({{stop, Reason, Actions}, ModState, State}) ->
    State1 = resolve_version(State),
    _ = handle_actions(Actions, [], ModState),
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
    dgen_key:extend(Tuple, <<"s">>, ?CodecVersion).

handle_consume(Tenant, K, Tuid, State = #state{dead_letter_threshold = Threshold}) ->
    Quid = get_quid(Tuid),
    Result = dgen_backend:transactional(Tenant, fun(Td) ->
        case check_lock(Td, State) of
            not_locked ->
                consume_queued(Td, K, Quid, Threshold, State);
            stale ->
                clear_lock(Td, State),
                consume_queued(Td, K, Quid, Threshold, State);
            {live, Remaining} ->
                %% Lock is held but not yet stale.  Set a queue watch
                %% for the fast path (lock cleared normally fires
                %% notify → push key changes → watch fires).  Also
                %% schedule a timer so that if the lock holder is
                %% killed without clearing the lock and no new messages
                %% arrive, we still re-evaluate staleness after the
                %% remaining timeout — guaranteeing recovery even on a
                %% perfectly quiet queue.
                Watch = dgen_queue:watch_push(Td, Quid),
                Action = fun(_) ->
                    case Remaining of
                        infinity -> ok;
                        Ms -> arm(Ms, recheck_lock)
                    end
                end,
                {{noreply, [Action]}, undefined, State#state{watch = Watch}}
        end
    end),
    case Result of
        {reraise, Class, Reason, Stack} ->
            erlang:raise(Class, Reason, Stack);
        {lock_batch, PriorActions, EventType, Msg, ModState, State2} ->
            State3 = resolve_version(State2),
            _ = handle_actions(PriorActions, [], ModState),
            consume_locked(EventType, Msg, ModState, false, State3);
        {{lock, EventType, Msg}, ModState, State2} ->
            State3 = resolve_version(State2),
            consume_locked(EventType, Msg, ModState, false, State3);
        _ ->
            finalize(Result)
    end.

consume_cast_batch(Td, Request, CurrentModState, State) ->
    case invoke_tx_callback_batch(Td, handle_cast, [Request], CurrentModState, State) of
        {error, Reason} ->
            erlang:error(Reason);
        {ok, OrigModState, CallbackResult} ->
            handle_callback_result_batch(Td, cast, Request, CallbackResult, OrigModState, State)
    end.

consume_call_batch(Td, Request, From, CurrentModState, State) ->
    case invoke_tx_callback_batch(Td, handle_call, [Request, From], CurrentModState, State) of
        {error, Reason} ->
            erlang:error(Reason);
        {ok, OrigModState, CallbackResult} ->
            handle_callback_result_batch(
                Td, {call, From}, Request, CallbackResult, OrigModState, State
            )
    end.

invoke_queued_msg_batch(Td, {cast, Request, _N}, CurrentModState, State) ->
    consume_cast_batch(Td, Request, CurrentModState, State);
invoke_queued_msg_batch(Td, {call, Request, From, _Opts, _N}, CurrentModState, State) ->
    case consume_call_batch(Td, Request, From, CurrentModState, State) of
        {{reply, Reply, Actions}, ModState, State2} ->
            set_reply(Td, From, Reply),
            {{noreply, Actions}, ModState, State2};
        Other ->
            Other
    end.

consume_queued(Td, K, Quid, Threshold, State) ->
    case dgen_queue:peek_k(Td, K, Quid) of
        {error, empty} ->
            Watch = dgen_queue:watch_push(Td, Quid),
            {{noreply, []}, undefined, State#state{watch = Watch}};
        {ok, KVs} ->
            case get_mod_state(Td, State) of
                {{error, not_found}, _} ->
                    #state{mod = Mod} = State,
                    erlang:error({mod_state_not_found, Mod});
                {{ok, InitModState}, State1} ->
                    consume_batch(
                        Td,
                        KVs,
                        Quid,
                        Threshold,
                        State1#state{watch = undefined},
                        [],
                        [],
                        InitModState,
                        InitModState
                    )
            end
    end.

%% Processes peeked KVs one at a time, carrying mod state in memory.
%% Mod state is read once (in consume_queued) and written once at each exit point,
%% avoiding repeated FDB reads/writes due to set_versionstamped_value not being
%% visible in read-your-own-writes within the same transaction.
%%
%% Parameters:
%%   8th — CurrentModState: mod state produced by the last successfully invoked callback
%%   9th — InitModState: mod state at the start of the batch, used for diff-based writes
consume_batch(Td, [], Quid, _Threshold, State, AccActions, AccKVs, CurrentModState, InitModState) ->
    {_, State1} = set_mod_state(Td, InitModState, CurrentModState, State),
    dgen_queue:consume_peeked(Td, lists:reverse(AccKVs), Quid),
    FinalActions = lists:append(lists:reverse(AccActions)),
    {{noreply, FinalActions}, CurrentModState, State1#state{cache_misses = 0}};
consume_batch(
    Td,
    [{RawKey, RawBin} | Rest],
    Quid,
    Threshold,
    State,
    AccActions,
    AccKVs,
    CurrentModState,
    InitModState
) ->
    Envelope = normalize_message(binary_to_term(RawBin)),
    N = envelope_attempts(Envelope),
    case is_dead_letter(N, Threshold) of
        true ->
            AllKVs = lists:reverse([{RawKey, RawBin} | AccKVs]),
            {_, State1} = set_mod_state(Td, InitModState, CurrentModState, State),
            dgen_queue:consume_peeked(Td, AllKVs, Quid),
            handle_dead_letter_internal(Td, to_dl_envelope(Envelope), N, State1);
        false ->
            try invoke_queued_msg_batch(Td, Envelope, CurrentModState, State) of
                {{noreply, Actions}, NewModState, State1} ->
                    consume_batch(
                        Td,
                        Rest,
                        Quid,
                        Threshold,
                        State1,
                        [Actions | AccActions],
                        [{RawKey, RawBin} | AccKVs],
                        NewModState,
                        InitModState
                    );
                {{lock, EventType, Msg}, ModState, State1} ->
                    AllKVs = lists:reverse([{RawKey, RawBin} | AccKVs]),
                    {_, State2} = set_mod_state(Td, InitModState, ModState, State1),
                    dgen_queue:consume_peeked(Td, AllKVs, Quid),
                    PriorActions = lists:append(lists:reverse(AccActions)),
                    State3 = State2#state{cache_misses = 0},
                    case PriorActions of
                        [] -> {{lock, EventType, Msg}, ModState, State3};
                        _ -> {lock_batch, PriorActions, EventType, Msg, ModState, State3}
                    end;
                {{stop, Reason, Actions}, ModState, State1} ->
                    AllKVs = lists:reverse([{RawKey, RawBin} | AccKVs]),
                    {_, State2} = set_mod_state(Td, InitModState, ModState, State1),
                    dgen_queue:consume_peeked(Td, AllKVs, Quid),
                    All = lists:append(lists:reverse([Actions | AccActions])),
                    {{stop, Reason, All}, ModState, State2#state{cache_misses = 0}}
            catch
                Class:Reason:Stack ->
                    case AccKVs of
                        [] ->
                            ok;
                        _ ->
                            % Write mod state for successfully processed prior messages
                            % before committing their dequeues.
                            set_mod_state(Td, InitModState, CurrentModState, State),
                            dgen_queue:consume_peeked(Td, lists:reverse(AccKVs), Quid)
                    end,
                    dgen_queue:update_peeked(Td, RawKey, increment_envelope(Envelope)),
                    {reraise, Class, Reason, Stack}
            end
    end.

envelope_attempts({cast, _R, N}) -> N;
envelope_attempts({call, _R, _F, _O, N}) -> N.

to_dl_envelope({cast, R, _N}) -> {cast, R};
to_dl_envelope({call, R, F, _O, _N}) -> {call, R, F}.

normalize_message({cast, R}) -> {cast, R, 0};
normalize_message({cast, R, N}) -> {cast, R, N};
normalize_message({call, R, F, O}) -> {call, R, F, O, 0};
normalize_message({call, R, F, O, N}) -> {call, R, F, O, N}.

is_dead_letter(_N, infinity) -> false;
is_dead_letter(N, Threshold) -> N >= Threshold.

increment_envelope({cast, R, N}) -> {cast, R, N + 1};
increment_envelope({call, R, F, O, N}) -> {call, R, F, O, N + 1}.

handle_dead_letter_internal(
    Td, MsgEnvelope, AttemptCount, State = #state{mod = Mod, tuid = Tuid}
) ->
    dgen_queue:push_dlq(Td, get_quid(Tuid), MsgEnvelope, AttemptCount),
    case MsgEnvelope of
        {call, _Request, From} ->
            set_raise(Td, From, error, {dead_letter, AttemptCount});
        _ ->
            ok
    end,
    Actions =
        case erlang:function_exported(Mod, handle_dead_letter, 2) of
            true ->
                Msg =
                    case MsgEnvelope of
                        {call, R, _} -> R;
                        {cast, R} -> R
                    end,
                [fun(_) -> Mod:handle_dead_letter(Msg, AttemptCount) end];
            false ->
                []
        end,
    logger:warning("dgen_server: message dead-lettered after ~b attempts: ~p", [
        AttemptCount, MsgEnvelope
    ]),
    {{noreply, Actions}, undefined, State}.

consume_call(Td, Request, From, State) ->
    case invoke_tx_callback(Td, handle_call, [Request, From], State) of
        {error, Reason} ->
            erlang:error(Reason);
        {ok, OrigModState, CallbackResult, State1} ->
            handle_callback_result(
                Td, {call, From}, Request, CallbackResult, OrigModState, State1
            )
    end.

consume_cast(Td, Request, State) ->
    case invoke_tx_callback(Td, handle_cast, [Request], State) of
        {error, Reason} ->
            erlang:error(Reason);
        {ok, OrigModState, CallbackResult, State1} ->
            handle_callback_result(
                Td, cast, Request, CallbackResult, OrigModState, State1
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
                        {{stop, Reason, Actions}, LModState, State2} ->
                            {{stop, Reason, Actions}, LModState, State2}
                    end
                end)
        after
            dgen_backend:transactional(Tenant, fun(Td) ->
                clear_lock(Td, State)
            end)
        end,

    case Result of
        {{reply, _, _}, _, _} ->
            finalize(Result);
        _ ->
            finalize(Result)
    end.

consume_info(Td, Info, State) ->
    case invoke_tx_callback(Td, handle_info, [Info], State) of
        {error, _} ->
            {{noreply, []}, undefined, State};
        {ok, OrigModState, CallbackResult, State1} ->
            handle_callback_result(
                Td, info, Info, CallbackResult, OrigModState, State1
            )
    end.

set_reply(Td, From, Reply) ->
    write_reply_slot(Td, From, {reply, Reply}).

set_raise(Td, From, Class, Reason) ->
    write_reply_slot(Td, From, {raise, Class, Reason}).

% Skip writing if the caller timed out and cleared the reply keys.
write_reply_slot({Tx, Dir}, From, Value) ->
    B = dgen_config:backend(),
    ReplySentinelKey = dgen_mod_state_codec:term_first_key(Dir, From),
    case B:wait(B:get(Tx, ReplySentinelKey)) of
        not_found ->
            ok;
        _ ->
            dgen_mod_state_codec:clear_term({Tx, Dir}, From),
            dgen_mod_state_codec:write_term({Tx, Dir}, From, Value)
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
    B:set(Tx, B:dir_pack(Dir, get_lock_key(Tuid)), term_to_binary(erlang:system_time(millisecond))),
    State.

%% Single read of the lock key — returns not_locked | stale | {live, infinity | Ms}.
%% Used by handle_consume to decide in one pass whether to clear, sleep, or proceed.
check_lock({Tx, Dir}, #state{tuid = Tuid, lock_timeout = Timeout}) ->
    B = dgen_config:backend(),
    case B:wait(B:get(Tx, B:dir_pack(Dir, get_lock_key(Tuid)))) of
        not_found ->
            not_locked;
        <<>> ->
            %% backward compat: v0.2.0 used empty binary (no timestamp) — treat
            %% as live with no timeout so we never falsely clear a legitimate lock.
            {live, infinity};
        Value ->
            %% v0.3.0+ stores the set-time as a millisecond timestamp so we can
            %% compute both staleness and the remaining wait in one decode.
            case (catch binary_to_term(Value)) of
                Ts when is_integer(Ts) ->
                    Elapsed = erlang:system_time(millisecond) - Ts,
                    case Timeout of
                        infinity ->
                            {live, infinity};
                        _ when Elapsed > Timeout ->
                            stale;
                        _ ->
                            {live, Timeout - Elapsed}
                    end;
                _ ->
                    {live, infinity}
            end
    end.

clear_lock(Td = {Tx, Dir}, #state{tuid = Tuid}) ->
    B = dgen_config:backend(),
    LockKey = B:dir_pack(Dir, get_lock_key(Tuid)),
    LockEnd = B:key_strinc(LockKey),
    B:clear_range(Tx, LockKey, LockEnd),
    dgen_queue:notify(Td, get_quid(Tuid)).

get_lock_key(Tuid) ->
    dgen_key:extend(Tuid, <<"k">>).

is_locked(Td, State) ->
    check_lock(Td, State) =/= not_locked.

get_quid(Tuple) ->
    dgen_key:extend(Tuple, <<"q">>).

%% Arm a timer, and say so. See `dgen_registry_member:arm/2`.
arm(Delay, Msg) ->
    _ = ?ETA_LOG({arm, Msg, Delay}),
    erlang:send_after(Delay, self(), Msg).
