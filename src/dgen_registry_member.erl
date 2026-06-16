-module(dgen_registry_member).
-behaviour(gen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Local name cache and consistent read/write proxy for `dgen_registry`.

Each node participating in a named registry runs one member process.
The member has two roles depending on whether it is the current leader.

## Storage

Both leader and follower keep a `names :: #{LogicalName => pid()}` map in
their gen_server state. There is no ETS table and no durable storage for
name→pid mappings. Pids are node-local and process-lifetime-scoped; they
have no meaning after a restart and must never be written to the backend.

Consistent reads and writes go through the leader.
Snapshot reads (`whereis_snapshot`) are served from the local member's map
without contacting the leader.

## Follower role

Keeps its local `names` map in sync by receiving `{name_registered, …}`,
`{name_unregistered, …}`, and `{apply_names_snapshot, …}` casts from the
leader (one-way replication).  All follower messages come from the leader
process, so Erlang's per-pair FIFO guarantee ensures followers always see a
snapshot before any `{name_registered}` broadcast that post-dates it.

## Partition recovery

The member subscribes to `nodeup`/`nodedown` events via
`net_kernel:monitor_nodes/1`.  On `{nodeup, Node}`, the member re-announces
itself to the elector (`{join, Self}`).  This handles the case where an
Erlang-level network partition caused both sides to remove each other from
the member set via `{member_down}` while the DB remained healthy: once the
partition heals and distribution reconnects, both sides re-join and the
elector reconstitutes the cluster without requiring a restart.

Forwards `{register, …}` calls and `{unregister, …}` casts to the leader.
For `register`, the follower also updates its own `names` map on receiving
`yes` from the leader, so a subsequent `whereis_snapshot` on this node
reflects the change without waiting for the replication cast. On `no` the
local map is left unchanged.

## Leader role

Assumed when the elector calls `{elector_assume_and_distribute, …}`.  On
assuming leadership the member uses the supplied snapshot (or its own names
map if the snapshot is `self_snapshot`), sets up `erlang:monitor/2` for
every entry, and distributes `{apply_names_snapshot}` casts to all followers
from its own process (same sender as future `{name_registered}` broadcasts —
see elector moduledoc for the FIFO ordering guarantee).  Any stale Pid
entries are removed when their DOWN signals arrive.

The leader is the sole writer for the name table. It:

- Handles `{register, LogicalName, Pid}` calls: checks the in-memory map,
  updates it, monitors the Pid, and replicates `{name_registered, …}`.
- Handles `{whereis, LogicalName}` calls: consistent read from local map.
- Handles `{unregister, LogicalName}` casts: updates the map, demonitors,
  and replicates `{name_unregistered, …}`.
- Monitors every registered Pid. When one dies, removes from the map
  and replicates `{name_unregistered, …}` to followers.

On relinquishing leadership the member demonitors all registered Pids and
clears the leader-only state.  The `names` map is kept intact (it still
serves snapshot reads).

## Failure model

Name-to-pid mappings are intentionally not stored in the backend.  As a
result:

- **Leader crash**: the new leader starts with `self_snapshot` — its own
  in-memory follower replica.  Any registrations the dead leader committed
  to its map but had not yet broadcast to followers are silently lost.
  A caller that received `yes` from `register_name/2` may find the name
  absent after a leader failover.  Re-registration after detecting the loss
  is the caller's responsibility.

- **Full cluster restart**: all registered names are lost.  Applications
  must re-register on startup.
""".
-endif.

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    member_id :: dgen_registry_elector:member_id(),
    elector :: atom(),
    leader :: dgen_registry_elector:member_id() | undefined,
    %% Name → Pid map.  Authoritative on leader; replicated snapshot on followers.
    %% Never written to durable storage — Pids are ephemeral.
    names :: #{term() => pid()},
    %% Peer-member monitors (all members)
    members :: #{dgen_registry_elector:member_id() => reference()},
    monitors :: #{reference() => dgen_registry_elector:member_id()},
    %% Registered-process monitors (leader only)
    name_to_ref :: #{term() => reference()},
    ref_to_name :: #{reference() => term()},
    %% Token used in our own {join, Self, Token} announcement.  Refreshed on
    %% each nodeup so any stale {member_down, Self, OldToken} in the queue is
    %% discarded by the elector.
    join_token :: reference(),
    %% Tokens received from the elector via snapshots, keyed by peer MemberId.
    %% Echoed back in {member_down, PeerId, Token} so the elector can distinguish
    %% a stale DOWN (from before a re-join) from a fresh one.
    peer_tokens :: #{dgen_registry_elector:member_id() => reference()},
    %% Monotonically increasing leader term counter set by the elector.
    %% Broadcasts from a prior leader carry a smaller epoch and are discarded.
    epoch :: non_neg_integer()
}).

%% ---------------------------------------------------------------------------
%% Public API
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Starts the member process registered as `Name`.".
-endif.
-spec start_link(Name :: atom(), Args :: map()) -> gen_server:start_ret().
start_link(Name, Args) ->
    gen_server:start_link({local, Name}, ?MODULE, Args, []).

%% ---------------------------------------------------------------------------
%% gen_server callbacks
%% ---------------------------------------------------------------------------

init(#{elector := Elector, member_name := MemberName}) ->
    MemberId = {node(), MemberName},
    net_kernel:monitor_nodes(true),
    Token = make_ref(),
    %% Announce presence; the elector will call {elector_assume_and_distribute}
    %% on the new leader, which then sends {apply_names_snapshot} to this member.
    dgen_server:cast(Elector, {join, MemberId, Token}),
    {ok, #state{
        member_id = MemberId,
        elector = Elector,
        leader = undefined,
        names = #{},
        members = #{},
        monitors = #{},
        name_to_ref = #{},
        ref_to_name = #{},
        join_token = Token,
        peer_tokens = #{},
        epoch = 0
    }}.

%% ---------------------------------------------------------------------------
%% handle_call/3
%% ---------------------------------------------------------------------------

%% ---- Name registration ----------------------------------------------------

%% Leader: handle registration directly.
handle_call(
    {register, LogicalName, Pid},
    _From,
    State = #state{leader = Leader, member_id = Leader, epoch = Epoch}
) ->
    #state{names = Names, members = Members, name_to_ref = NTR, ref_to_name = RTN} = State,
    case maps:is_key(LogicalName, Names) of
        true ->
            {reply, no, State};
        false ->
            Ref = erlang:monitor(process, Pid),
            broadcast_to_peers(Members, {name_registered, LogicalName, Pid, Epoch}),
            {reply, yes, State#state{
                names = Names#{LogicalName => Pid},
                name_to_ref = NTR#{LogicalName => Ref},
                ref_to_name = RTN#{Ref => LogicalName}
            }}
    end;
%% Follower: forward to leader, then update local names map optimistically.
%% The optimistic update ensures that a subsequent whereis_snapshot on this
%% node returns the Pid immediately, without waiting for the replication cast
%% from the leader (which may lose the race with the reply over the network).
handle_call(
    {register, LogicalName, Pid},
    _From,
    State = #state{leader = Leader}
) when Leader =/= undefined ->
    {Node, Name} = Leader,
    try gen_server:call({Name, Node}, {register, LogicalName, Pid}) of
        yes ->
            {reply, yes, State#state{names = (State#state.names)#{LogicalName => Pid}}};
        no ->
            {reply, no, State}
    catch
        exit:_ ->
            {reply, no, State}
    end;
%% No leader yet.
handle_call({register, _LogicalName, _Pid}, _From, State) ->
    {reply, no, State};
%% ---- Consistent read (leader only) ----------------------------------------

handle_call(
    {whereis, LogicalName},
    _From,
    State = #state{leader = Leader, member_id = Leader}
) ->
    {reply, maps:get(LogicalName, State#state.names, undefined), State};
handle_call(
    {whereis, LogicalName},
    _From,
    State = #state{leader = Leader}
) when Leader =/= undefined ->
    {Node, Name} = Leader,
    try
        Reply = gen_server:call({Name, Node}, {whereis, LogicalName}),
        {reply, Reply, State}
    catch
        exit:_ ->
            {reply, undefined, State}
    end;
handle_call({whereis, _LogicalName}, _From, State) ->
    {reply, undefined, State};
%% ---- Snapshot read (any member, served from local names map) ---------------

handle_call({whereis_snapshot, LogicalName}, _From, State) ->
    {reply, maps:get(LogicalName, State#state.names, undefined), State};
%% ---- Elector calls (during lock period) ------------------------------------

%% Atomically relinquish leadership and return the current names snapshot.
%% Called by the elector when a new member becomes the new leader: the old
%% leader hands off its authoritative state and stops accepting writes in a
%% single step.  Any registration already in the mailbox before this call is
%% processed first (FIFO) and included in the returned snapshot; any arriving
%% after this call returns 'no' — leader is undefined until {apply_names_snapshot}
%% arrives from the new leader.
handle_call(
    {transfer_snapshot, _NewLeader},
    _From,
    State = #state{member_id = Self, leader = Self}
) ->
    %% Set leader = undefined (not NewLeader) so this member stays silent on
    %% registrations until {apply_names_snapshot} arrives from the new leader.
    %% If we set leader = NewLeader here, any pending {register} call would be
    %% forwarded to a leader that has not yet assumed leadership and would fail.
    %% The correct leader is communicated atomically via {apply_names_snapshot}.
    State1 = relinquish_leadership(State#state{leader = undefined}),
    {reply, maps:to_list(State1#state.names), State1};
%% Called by the elector to atomically assume leadership and fan out the
%% names snapshot to all followers.
%%
%% `Snapshot` is either `self_snapshot` (use own names — leader was already
%% an existing member) or a `[{Name, Pid}]` list pre-fetched from the old
%% leader via `transfer_snapshot`.
%%
%% `MemberId` is the newly joining member that triggered this transition, or
%% `undefined` for a `member_down` event.
%%
%% `AllIds` is the full current member list.
%%
%% After updating own state the leader sends `{apply_names_snapshot}` casts
%% to every follower from its own process (maintaining FIFO ordering with
%% subsequent `{name_registered}` broadcasts).
handle_call(
    {elector_assume_and_distribute, Snapshot, MemberId, AllIds, Tokens, Epoch},
    _From,
    State = #state{member_id = Self, leader = OldLeader}
) ->
    State1 = do_leader_changed(Self, OldLeader, Self, State),
    State2 =
        case Snapshot of
            self_snapshot -> State1;
            NamesList -> State1#state{names = maps:from_list(NamesList)}
        end,
    State3 = add_member_monitors(extra_member_ids(MemberId, AllIds, Self), State2),
    State4 = merge_peer_tokens(Tokens, State3),
    State5 = State4#state{epoch = Epoch},
    Names = maps:to_list(State5#state.names),
    lists:foreach(
        fun(Id) ->
            cast_to_member(
                Id,
                {apply_names_snapshot, Names, Self, extra_member_ids(MemberId, AllIds, Id), Tokens,
                    Epoch}
            )
        end,
        lists:delete(Self, AllIds)
    ),
    {reply, ok, State5};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

%% ---------------------------------------------------------------------------
%% handle_cast/2
%% ---------------------------------------------------------------------------

handle_cast(
    {name_registered, LogicalName, Pid, Epoch},
    State = #state{names = Names, epoch = CurrentEpoch}
) ->
    case Epoch >= CurrentEpoch of
        true -> {noreply, State#state{names = Names#{LogicalName => Pid}}};
        false -> {noreply, State}
    end;
handle_cast(
    {name_unregistered, LogicalName, Epoch},
    State = #state{names = Names, epoch = CurrentEpoch}
) ->
    case Epoch >= CurrentEpoch of
        true -> {noreply, State#state{names = maps:remove(LogicalName, Names)}};
        false -> {noreply, State}
    end;
%% Leadership transition snapshot sent by the new leader to all followers.
%% Applies the leader transition, the names update, and extra member monitors
%% atomically within a single cast — no other message can interleave.
handle_cast(
    {apply_names_snapshot, NamesList, NewLeader, ExtraMembers, Tokens, Epoch},
    State = #state{member_id = Self, leader = OldLeader, epoch = CurrentEpoch}
) ->
    case Epoch >= CurrentEpoch of
        true ->
            State1 = do_leader_changed(NewLeader, OldLeader, Self, State),
            State2 = State1#state{names = maps:from_list(NamesList), epoch = Epoch},
            State3 = add_member_monitors(ExtraMembers, State2),
            {noreply, merge_peer_tokens(Tokens, State3)};
        false ->
            {noreply, State}
    end;
%% Leader: handle unregister directly.
handle_cast(
    {unregister, LogicalName}, State = #state{leader = Leader, member_id = Leader, epoch = Epoch}
) ->
    #state{names = Names, members = Members, name_to_ref = NTR, ref_to_name = RTN} = State,
    {NTR1, RTN1} = demonitor_name(LogicalName, NTR, RTN),
    broadcast_to_peers(Members, {name_unregistered, LogicalName, Epoch}),
    {noreply, State#state{
        names = maps:remove(LogicalName, Names),
        name_to_ref = NTR1,
        ref_to_name = RTN1
    }};
%% Follower: update local map immediately, then forward to leader.
%% Immediate local update ensures whereis_snapshot returns undefined before
%% the replication cast arrives from the leader.
handle_cast({unregister, LogicalName}, State = #state{leader = Leader, names = Names}) when
    Leader =/= undefined
->
    cast_to_member(Leader, {unregister, LogicalName}),
    {noreply, State#state{names = maps:remove(LogicalName, Names)}};
handle_cast(_, State) ->
    {noreply, State}.

%% ---------------------------------------------------------------------------
%% handle_info/2
%% ---------------------------------------------------------------------------

handle_info({'DOWN', Ref, process, _Pid, _Reason}, State) ->
    #state{
        monitors = Monitors,
        ref_to_name = RefToName,
        elector = Elector,
        member_id = Self,
        members = Members,
        epoch = Epoch
    } = State,
    case maps:get(Ref, Monitors, undefined) of
        undefined ->
            %% Not a peer-member monitor — registered-process monitor (leader only).
            case maps:get(Ref, RefToName, undefined) of
                undefined ->
                    {noreply, State};
                LogicalName ->
                    broadcast_to_peers(Members, {name_unregistered, LogicalName, Epoch}),
                    NTR = maps:remove(LogicalName, State#state.name_to_ref),
                    RTN = maps:remove(Ref, RefToName),
                    {noreply, State#state{
                        names = maps:remove(LogicalName, State#state.names),
                        name_to_ref = NTR,
                        ref_to_name = RTN
                    }}
            end;
        Self ->
            %% Stale self-monitor — should not happen, ignore.
            {noreply, State};
        DeadMemberId ->
            %% Include the token we last received for this peer so the elector can
            %% reject the message if the peer has already rejoined with a new token.
            Token = maps:get(DeadMemberId, State#state.peer_tokens, undefined),
            dgen_server:cast(Elector, {member_down, DeadMemberId, Token}),
            {noreply, remove_member(DeadMemberId, State)}
    end;
handle_info({nodeup, _Node}, State = #state{elector = Elector, member_id = Self}) ->
    %% Re-announce to the elector — this member may have been removed from the
    %% member set while the node was unreachable (partition).  A fresh token is
    %% generated so any stale {member_down, Self, OldToken} already in the queue
    %% is discarded by the elector when it is eventually processed.
    NewToken = make_ref(),
    dgen_server:cast(Elector, {join, Self, NewToken}),
    {noreply, State#state{join_token = NewToken}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    net_kernel:monitor_nodes(false),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------------
%% Internal helpers
%% ---------------------------------------------------------------------------

do_leader_changed(NewLeader, OldLeader, Self, State) ->
    if
        OldLeader =:= Self, NewLeader =/= Self ->
            %% Lost leadership — demonitor registered Pids, keep names for snapshot reads.
            relinquish_leadership(State#state{leader = NewLeader});
        OldLeader =/= Self, NewLeader =:= Self ->
            %% Gained leadership — set up monitors for all currently known names.
            assume_leadership(State#state{leader = NewLeader});
        true ->
            State#state{leader = NewLeader}
    end.

%% Set up process monitors for every entry in the current names map.
%% Any stale Pid entries (processes that died while this node was a follower)
%% will self-correct when their DOWN signals arrive.
assume_leadership(State = #state{names = Names}) ->
    {NTR, RTN} = maps:fold(
        fun(LogicalName, Pid, {NTRAcc, RTNAcc}) ->
            Ref = erlang:monitor(process, Pid),
            {NTRAcc#{LogicalName => Ref}, RTNAcc#{Ref => LogicalName}}
        end,
        {#{}, #{}},
        Names
    ),
    State#state{name_to_ref = NTR, ref_to_name = RTN}.

%% Demonitor all registered Pids; keep the names map for snapshot reads.
relinquish_leadership(State = #state{name_to_ref = NTR}) ->
    maps:foreach(
        fun(_LogicalName, Ref) ->
            erlang:demonitor(Ref, [flush])
        end,
        NTR
    ),
    State#state{name_to_ref = #{}, ref_to_name = #{}}.

add_member_monitors(MemberIds, State) ->
    #state{member_id = Self, members = Members, monitors = Monitors} = State,
    {NewMembers, NewMonitors} = lists:foldl(
        fun(MemberId, {MA, MonA}) ->
            case MemberId =:= Self orelse maps:is_key(MemberId, MA) of
                true ->
                    {MA, MonA};
                false ->
                    {Node, Name} = MemberId,
                    Ref = erlang:monitor(process, {Name, Node}),
                    {MA#{MemberId => Ref}, MonA#{Ref => MemberId}}
            end
        end,
        {Members, Monitors},
        MemberIds
    ),
    State#state{members = NewMembers, monitors = NewMonitors}.

remove_member(MemberId, State = #state{members = Members, monitors = Monitors}) ->
    case maps:get(MemberId, Members, undefined) of
        undefined ->
            State;
        Ref ->
            erlang:demonitor(Ref, [flush]),
            State#state{
                members = maps:remove(MemberId, Members),
                monitors = maps:remove(Ref, Monitors)
            }
    end.

demonitor_name(LogicalName, NTR, RTN) ->
    case maps:get(LogicalName, NTR, undefined) of
        undefined ->
            {NTR, RTN};
        OldRef ->
            erlang:demonitor(OldRef, [flush]),
            {maps:remove(LogicalName, NTR), maps:remove(OldRef, RTN)}
    end.

%% Returns the list of member IDs to add as monitors for a given member during
%% a join/member_down leadership transition.
%%   - undefined MemberId (member_down):    no extra monitors for anyone.
%%   - Id = MemberId (new joining member):  add all current peers.
%%   - Id ≠ MemberId (existing member):     add only the new member.
extra_member_ids(undefined, _AllIds, _Id) -> [];
extra_member_ids(MemberId, AllIds, MemberId) -> AllIds;
extra_member_ids(MemberId, _AllIds, _Id) -> [MemberId].

%% Merge incoming token map into local peer_tokens, taking the newer token when
%% both sides know about the same member (higher value = more recent make_ref
%% within a single BEAM session, but refs are opaque so we always overwrite —
%% the elector is the authoritative source and always sends current tokens).
merge_peer_tokens(Tokens, State = #state{peer_tokens = PeerTokens}) ->
    State#state{peer_tokens = maps:merge(PeerTokens, Tokens)}.

broadcast_to_peers(Members, Msg) ->
    maps:foreach(fun(MemberId, _) -> cast_to_member(MemberId, Msg) end, Members).

cast_to_member({Node, Name}, Msg) ->
    gen_server:cast({Name, Node}, Msg).
