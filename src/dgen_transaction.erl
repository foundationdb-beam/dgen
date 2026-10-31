-module(dgen_transaction).
-behaviour(gen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Behaviour that wraps the lifecycle of a single FDB transaction in a process.

`dgen_backend:transactional/2` runs a closure and retries it automatically.
That is simple, but the caller never holds the transaction, and the caller's
process blocks for the whole commit. `dgen_transaction` instead gives the
transaction **its own process**. That process creates the transaction, runs the
body from a callback module against it, decides when to commit, drives the
retry loop, and delivers the outcome asynchronously. This buys us two things:

1. **More control over the transaction lifecycle.** For example, a
   dgen_transaction can reuse the read version (GRV) of a previous transaction
   instead of paying for a fresh one. On retry it falls back to a fresh GRV
   automatically, because the backend's `on_error/2` resets the read version.
2. **Non-blocking commit.** A calling process can hand a write to a
   `dgen_transaction` worker and keep receiving messages from its queue
   while the commit is in flight, learning the result via a message.

## Lifecycle

```
start_link/3 ──▶ init/1 ──▶ create_transaction ──▶ apply read version
                                                         │
                                                   handle_begin/2
                                                    │    │     │
                                            {noreply}  {commit} {stop}
                                                │        │        │
                                       (interactive)  commit    abort
                                       handle_cast/3    │
                                       handle_call/4    ▼
                                                  ┌── ok ──▶ handle_committed/2 ──▶ {committed, Result}
                                                  │
                                                  └ retryable error ─▶ handle_conflict/2
                                                          retry ─▶ on_error ─▶ handle_retry/2 ─▶ (commit again)
                                                          {stop, R} ─▶ {aborted, R}
```

## Result delivery

When the transaction reaches a terminal state the worker sends the owner

```
{dgen_transaction, Ref, Reply}
```

where `Ref` is the correlation token, either supplied by the caller or
generated, and `Reply` is one of:

- `{committed, Result}` — `Result` is whatever `handle_committed/2` returned.
- `{aborted, Reason}` — the module chose not to commit, or not to retry.
- `{error, Reason}` — a non-retryable backend error, retry-limit exhaustion, or
  the worker terminating abnormally.

## Callbacks

- `init/1` — sets up the callback state.
- `handle_begin/2` — issues the transaction body against the live `Tx`. That is
  the reads that guard the commit against conflicts, plus the writes. It is
  replayed on every retry unless the module provides `handle_retry/2`. Returns
  a directive such as `noreply` or `commit`.
- `handle_committed/2` — called with the committed version after a successful
  commit. Returns `{ok, Result}`.

Optional:

- `handle_cast/3`, `handle_call/4` — let the caller drive the live transaction
  directly with messages before it commits.
- `handle_retry/2` — re-issues the body on retry. Defaults to `handle_begin/2`.
- `handle_conflict/2` — allows or vetoes a retry after a retryable error.
  Defaults to `retry`. It sees retryable errors from the commit **and** from
  issuing the body, such as a read at a pinned read version that has grown too
  old. Both go through `handle_conflict/2` → `on_error/2` → replay. So a stale
  pinned read version falls back to a fresh GRV instead of failing the worker.
- `terminate/2` — cleans up. Receives the terminal `Reply`.

*This documentation is LLM-generated. See the AI disclosure in `README.md`.*
""".
-endif.

-include("../include/dgen_eta.hrl").

-export([start_link/3, start_monitor/3, start/3, run/3]).
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% ---------------------------------------------------------------------------
%% Behaviour
%% ---------------------------------------------------------------------------

-type cb_state() :: term().
-type directive() ::
    {noreply, cb_state()}
    | {commit, cb_state()}
    | {stop, term(), cb_state()}.

-export_type([directive/0]).

%% The body callbacks receive the transaction handle. Everything else the body
%% needs, such as the directory, the tuid or the batch being committed, lives in
%% the callback module's own state. dgen_transaction itself knows nothing about
%% directories.
-callback init(Args :: term()) -> {ok, cb_state()} | {stop, Reason :: term()}.
-callback handle_begin(Tx :: dgen_backend:tx(), State :: cb_state()) -> directive().
-callback handle_committed(CommittedVersion :: integer(), State :: cb_state()) ->
    {ok, Result :: term()}.
-callback handle_cast(Msg :: term(), Tx :: dgen_backend:tx(), State :: cb_state()) -> directive().
-callback handle_call(
    Msg :: term(), From :: gen_server:from(), Tx :: dgen_backend:tx(), State :: cb_state()
) ->
    {reply, term(), cb_state()} | directive().
-callback handle_retry(Tx :: dgen_backend:tx(), State :: cb_state()) -> directive().
-callback handle_conflict(ErrorCode :: integer(), State :: cb_state()) ->
    retry | {stop, Reason :: term()}.
-callback terminate(Reply :: term(), State :: cb_state()) -> term().

-optional_callbacks([
    handle_cast/3,
    handle_call/4,
    handle_retry/2,
    handle_conflict/2,
    terminate/2
]).

%% ---------------------------------------------------------------------------
%% Internal state
%% ---------------------------------------------------------------------------

-record(st, {
    b :: module(),
    db :: dgen_backend:db(),
    tx :: dgen_backend:tx(),
    mod :: module(),
    cb :: cb_state(),
    owner :: pid(),
    ref :: term(),
    retries = 0 :: non_neg_integer(),
    max_retries :: non_neg_integer() | infinity,
    %% Set once the owner has the terminal Reply, so that the gen_server
    %% terminate/2 does not send it a second, failure Reply.
    done = false :: boolean()
}).

-type opt() ::
    {db, dgen_backend:db()}
    | {read_version, undefined | integer()}
    | {owner, pid()}
    | {ref, term()}
    | {max_retries, non_neg_integer() | infinity}.
-type opts() :: [opt()].

%% ---------------------------------------------------------------------------
%% Public API
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Starts (and links) a transaction worker for `Module`.

`Opts` must contain `{db, Db}`. The optional keys are:

- `read_version`: `undefined` or a version `V`. Defaults to `undefined`.
- `owner`: defaults to the caller.
- `ref`: the correlation token. Defaults to a fresh `make_ref/0`.
- `max_retries`: defaults to `infinity`.
""".
-endif.
-spec start_link(module(), term(), opts()) -> {ok, pid()} | {error, term()}.
start_link(Module, Args, Opts) ->
    gen_server:start_link(?MODULE, init_arg(Module, Args, Opts), []).

-if(?DOCATTRS).
-doc "Like `start_link/3` but without linking the worker to the caller.".
-endif.
-spec start(module(), term(), opts()) -> {ok, pid()} | {error, term()}.
start(Module, Args, Opts) ->
    gen_server:start(?MODULE, init_arg(Module, Args, Opts), []).

-if(?DOCATTRS).
-doc "Like `start_link/3` but creates a monitor and does not create a link.".
-endif.
-spec start_monitor(module(), term(), opts()) ->
    {ok, {pid(), reference()}} | {error, term()}.
start_monitor(Module, Args, Opts) ->
    gen_server:start_monitor(?MODULE, init_arg(Module, Args, Opts), []).

-if(?DOCATTRS).
-doc """
Runs a transaction worker synchronously and returns its `Reply`.

Starts the worker with `gen_server:start_monitor/3`, which monitors it
atomically without linking. Blocks until the worker delivers its terminal
`Reply`, or returns `{error, {crashed, Reason}}` if the worker dies first. Use
it when you want the lifecycle control, such as a cached GRV or retries decided
by the module, without managing the worker yourself.
""".
-endif.
-spec run(module(), term(), opts()) -> term().
run(Module, Args, Opts0) ->
    Ref = make_ref(),
    Opts = lists:keystore(
        ref, 1, lists:keystore(owner, 1, Opts0, {owner, self()}), {ref, Ref}
    ),
    case start_monitor(Module, Args, Opts) of
        {ok, {Pid, MRef}} ->
            receive
                {dgen_transaction, Ref, Reply} ->
                    erlang:demonitor(MRef, [flush]),
                    Reply;
                {'DOWN', MRef, process, Pid, Reason} ->
                    {error, {crashed, Reason}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

init_arg(Module, Args, Opts) ->
    #{
        module => Module,
        args => Args,
        db => proplists:get_value(db, Opts),
        read_version => proplists:get_value(read_version, Opts),
        owner => proplists:get_value(owner, Opts, self()),
        ref => proplists:get_value(ref, Opts, make_ref()),
        max_retries => proplists:get_value(max_retries, Opts, infinity)
    }.

%% ---------------------------------------------------------------------------
%% gen_server callbacks
%% ---------------------------------------------------------------------------

init(#{module := Module, args := Args} = A) ->
    case Module:init(Args) of
        {ok, Cb} ->
            B = dgen_config:backend(),
            Db = maps:get(db, A),
            Tx = B:create_transaction(Db),
            apply_read_version(B, Tx, maps:get(read_version, A)),
            St = #st{
                b = B,
                db = Db,
                tx = Tx,
                mod = Module,
                cb = Cb,
                owner = maps:get(owner, A),
                ref = maps:get(ref, A),
                max_retries = maps:get(max_retries, A)
            },
            {ok, St, {continue, do_begin}};
        {stop, Reason} ->
            {stop, {init_failed, Reason}}
    end.

handle_continue(do_begin, St) ->
    run_body(St).

handle_call(Msg, From, St = #st{mod = M, cb = Cb}) ->
    case erlang:function_exported(M, handle_call, 4) of
        true -> exec_call(M:handle_call(Msg, From, tx(St), Cb), St);
        false -> {reply, {error, no_handle_call}, St}
    end.

handle_cast(Msg, St = #st{mod = M, cb = Cb}) ->
    case erlang:function_exported(M, handle_cast, 3) of
        true -> exec(M:handle_cast(Msg, tx(St), Cb), St);
        false -> {noreply, St}
    end.

handle_info(_Info, St) ->
    {noreply, St}.

terminate(_Reason, #st{done = true}) ->
    ok;
terminate(Reason, St = #st{done = false}) ->
    %% We are exiting abnormally before the owner got a terminal Reply, so tell it.
    _ = finish(St, {error, {terminated, Reason}}),
    ok.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% ---------------------------------------------------------------------------
%% Directive handling
%% ---------------------------------------------------------------------------

%% Runs the transaction body and acts on its directive. The first attempt runs
%% handle_begin, and a replay runs handle_retry or handle_begin. We catch errors
%% from the body so that a retryable FDB error raised inside it, such as a read at
%% a stale pinned read version, goes through the same retry path as a commit
%% conflict. The callback module decides whether to retry in handle_conflict/2,
%% instead of the worker crashing.
run_body(St = #st{mod = M, retries = N}) ->
    BodyResult =
        try
            {ok, invoke_body(M, body_callback(M, N), tx(St), St#st.cb)}
        catch
            error:Reason -> {fdb_error, Reason}
        end,
    case BodyResult of
        {ok, Directive} -> exec(Directive, St);
        {fdb_error, Reason2} -> handle_fdb_error(Reason2, St)
    end.

%% First attempt runs handle_begin; a replay runs handle_retry if the module
%% defines it, otherwise handle_begin again.
body_callback(_M, 0) ->
    handle_begin;
body_callback(M, _N) ->
    case erlang:function_exported(M, handle_retry, 2) of
        true -> handle_retry;
        false -> handle_begin
    end.

invoke_body(M, handle_begin, Tx, Cb) -> M:handle_begin(Tx, Cb);
invoke_body(M, handle_retry, Tx, Cb) -> M:handle_retry(Tx, Cb).

%% Directives from handle_begin / handle_cast / handle_retry.
exec({noreply, Cb}, St) ->
    {noreply, St#st{cb = Cb}};
exec({commit, Cb}, St) ->
    do_commit(St#st{cb = Cb});
exec({stop, Reason, Cb}, St) ->
    St1 = finish(St#st{cb = Cb}, {aborted, Reason}),
    {stop, normal, St1}.

%% Directives from handle_call (adds {reply, R, Cb}).
exec_call({reply, Reply, Cb}, St) ->
    {reply, Reply, St#st{cb = Cb}};
exec_call(Directive, St) ->
    exec(Directive, St).

do_commit(St = #st{b = B, tx = Tx, mod = M, cb = Cb}) ->
    try B:wait(B:commit(Tx)) of
        _ ->
            CommittedVersion = B:get_committed_version(Tx),
            {ok, Result} = M:handle_committed(CommittedVersion, Cb),
            St1 = finish(St, {committed, Result}),
            {stop, normal, St1}
    catch
        error:Reason -> handle_fdb_error(Reason, St)
    end.

%% Handles a backend error from the body or the commit. A non-FDB error is fatal.
%% For an FDB error code, we let the module decide whether to retry.
handle_fdb_error(Reason, St = #st{b = B}) ->
    case B:error_code(Reason) of
        error ->
            St1 = finish(St, {error, Reason}),
            {stop, normal, St1};
        Code ->
            maybe_retry(Code, St)
    end.

maybe_retry(Code, St = #st{mod = M, cb = Cb}) ->
    Decision =
        case erlang:function_exported(M, handle_conflict, 2) of
            true -> M:handle_conflict(Code, Cb);
            false -> retry
        end,
    case Decision of
        {stop, Reason} ->
            St1 = finish(St, {aborted, Reason}),
            {stop, normal, St1};
        retry ->
            retry_commit(Code, St)
    end.

retry_commit(Code, St = #st{retries = N, max_retries = Max}) when
    Max =/= infinity, N >= Max
->
    St1 = finish(St, {error, {retry_limit, Code}}),
    {stop, normal, St1};
retry_commit(Code, St = #st{b = B, tx = Tx, retries = N}) ->
    try B:wait(B:on_error(Tx, Code)) of
        _ ->
            %% on_error reset the transaction and cleared its read version, so
            %% the retry gets a fresh GRV.
            run_body(St#st{retries = N + 1})
    catch
        error:Reason ->
            St1 = finish(St, {error, Reason}),
            {stop, normal, St1}
    end.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

tx(#st{tx = Tx}) ->
    Tx.

apply_read_version(_B, _Tx, undefined) ->
    ok;
apply_read_version(B, Tx, Version) ->
    B:set_read_version(Tx, Version).

%% Delivers the terminal Reply to the owner exactly once and marks the worker done.
finish(St = #st{done = true}, _Reply) ->
    St;
finish(St = #st{owner = Owner, ref = Ref, mod = M, cb = Cb}, Reply) ->
    case erlang:function_exported(M, terminate, 2) of
        true -> catch M:terminate(Reply, Cb);
        false -> ok
    end,
    Owner ! {dgen_transaction, Ref, Reply},
    St#st{done = true}.
