%% Records shared by the dgen_registry modules, so the member can pattern match on
%% fields of its helpers' state. Each record still belongs to one module, which is the
%% only one that writes its fields:
%%   `#data{}`                  dgen_registry_member
%%   `#pipeline{}`              dgen_registry_pipeline
%%   `#conflicts{}`             dgen_registry_conflicts
%%   `#intent{}`, `#intents{}`  dgen_registry_intents
%%   `#replica{}`               dgen_registry_replica
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-ifndef(DGEN_REGISTRY_MEMBER_HRL).
-define(DGEN_REGISTRY_MEMBER_HRL, true).

-record(data, {
    member_id :: dgen_registry_elector:member_id(),
    %% Looked up once after init. The `one_for_all` supervisor restarts us together
    %% with the elector, so this pid never goes stale.
    elector :: pid() | undefined,
    %% The backend handle the leader uses to commit the version key. See
    %% dgen_registry_names.
    tenant :: dgen_backend:tenant(),
    tuid :: dgen_server:tuid(),
    leader :: dgen_registry_elector:member_id() | undefined,
    %% The local replica: ETS rows, inverted index, applied version and epoch.
    replica :: dgen_registry_replica:t(),
    %% Leader only (§4.9). Maps `SubId` to `{Watch, Notify}`. It is loaded at assume
    %% and updated by `{presence_update, …}`.
    subs = #{} :: #{term() => {dgen_registry_replica:query(), dgen_registry_replica:query()}},
    %% Leader only. The names matching each watch query. Each commit diffs it to find
    %% `joined` and `left` events.
    sub_matches = #{} :: #{term() => #{term() => pid()}},
    %% Leader only. The pids matching each notify query. A pid that newly matches is
    %% sent the current watch snapshot.
    notify_matches = #{} :: #{term() => #{term() => pid()}},
    %% Monitors on peer members. Every member keeps these.
    members :: #{dgen_registry_elector:member_id() => reference()},
    monitors :: #{reference() => dgen_registry_elector:member_id()},
    %% Monitors on registered processes. Only the leader keeps these. We need no
    %% reverse map, because each monitor is tagged `{down, Name}` and its DOWN carries
    %% the name.
    name_to_ref :: #{term() => reference()},
    %% Requests waiting for a leader to decide them. See dgen_registry_intents.
    intents = dgen_registry_intents:new() :: dgen_registry_intents:t(),
    %% Leader only. Write ops not yet answered. See dgen_registry_pipeline.
    pipeline = dgen_registry_pipeline:new() :: dgen_registry_pipeline:t(),
    %% The epoch in which a `fenced` commit last made this member join again. See
    %% maybe_rejoin/1. It lets us join again at most once per epoch.
    fence_rejoined = undefined :: undefined | non_neg_integer(),
    %% The token for our join. We replace it on nodeup, so the elector discards a
    %% stale `{member_down, Self, OldToken}`.
    join_token :: reference(),
    %% The join tokens of our peers. We send them back in
    %% `{member_down, PeerId, Token}`, so the elector can discard a stale DOWN.
    peer_tokens :: #{dgen_registry_elector:member_id() => reference()},
    %% The timer for an unanswered resync request. Any snapshot clears it.
    resync_timer = undefined :: undefined | reference(),
    %% Whether we have ever synced, by applying a snapshot or assuming leadership.
    %% Joins send `fresh = not synced`. The first sync joins again with
    %% `fresh = false`.
    synced = false :: boolean(),
    %% Tags the handoff gather running off the loop (§5.7). We only apply
    %% `{assume_gathered, …}` if its ref still matches, so a replaced gather is
    %% harmless.
    assume_ref = undefined :: undefined | reference(),
    %% Conflict repair state (§5.6). See dgen_registry_conflicts.
    conflicts = dgen_registry_conflicts:new() :: dgen_registry_conflicts:t(),
    %% Per-registry tuning options, resolved through dgen_config.
    config = #{} :: dgen_config:config()
}).

-record(pipeline, {
    pending = queue:new() :: queue:queue(dgen_registry_pipeline:op()),
    num_pending = 0 :: non_neg_integer(),
    %% We monitor the worker, so if it dies without a result the pipeline doesn't get
    %% stuck.
    committing = undefined :: undefined | #{ref := reference(), mref := reference(), plan := map()},
    %% Used as the next read version to skip a GRV. If it is stale, the worker
    %% retries.
    last_version = undefined :: undefined | integer(),
    %% Maps `BatchRef` to `{Seq, Direct, TimerRef, Needed, Acked}`. We answer batches
    %% in `Seq` order, never in reference order, so simulated runs are reproducible.
    acks = #{} :: #{
        reference() =>
            {
                non_neg_integer(),
                [dgen_registry_pipeline:direct()],
                reference(),
                non_neg_integer(),
                #{
                    dgen_registry_elector:member_id() => true
                }
            }
    },
    next_ack_seq = 0 :: non_neg_integer()
}).

-record(conflicts, {
    trail = #{} :: dgen_registry_conflicts:trail(),
    %% Leader only. Maps each name to its recent kill times in ms, for
    %% `conflict_kill_budget`.
    kill_budget = #{} :: #{term() => [integer()]},
    %% When we last saw a member leave. Trail pruning pauses for a TTL after it.
    last_departure = 0 :: integer(),
    %% Leader only. True when the last rebuild may have missed a committed binding.
    degraded = false :: boolean()
}).

-record(intent, {
    kind :: dgen_registry_intents:kind(),
    phase :: dgen_registry_intents:phase(),
    %% `answered` means the caller already has its reply, but the change still has to
    %% be made.
    from :: gen_server:from() | answered,
    %% The wall-clock time of the request in ms. We compare it with the caller
    %% timeout.
    at :: integer(),
    %% The arrival order. We emit effects in this order, never in map order, so the
    %% message schedule is reproducible under deterministic simulation.
    seq :: non_neg_integer()
}).

-record(intents, {
    all = #{} :: #{reference() => #intent{}},
    %% Maps `Ref` to `V` for the intents in `awaiting_version`. When the replica
    %% advances, we only scan these.
    deferred = #{} :: #{reference() => non_neg_integer()},
    next_seq = 0 :: non_neg_integer()
}).

-record(replica, {
    tab :: ets:tid() | atom(),
    index = #{} :: #{term() => #{term() => #{term() => []}}},
    version = 0 :: non_neg_integer(),
    epoch = 0 :: non_neg_integer()
}).

-endif.
