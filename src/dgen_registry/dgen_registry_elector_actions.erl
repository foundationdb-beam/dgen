-module(dgen_registry_elector_actions).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("../../include/dgen_eta.hrl").
-define(SnapshotTimeout, 2000).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Post-commit actions returned by `dgen_registry_elector:handle_cast_tx/3`. Each one is
%% a fun that takes the committed elector state. `dgen_server` runs it in the elector
%% process after the transaction commits.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([
    push_leader_to_members/1,
    push_presence_to_leader/1
]).

%% Tells the leader to take over and distribute the snapshot. An unreachable leader
%% must not fail the action. The change is already committed, and members correct
%% themselves on the next membership event.
push_leader_to_members(TriggerMember) ->
    fun(#{members := All, leader := Leader, epoch := Epoch}) ->
        case Leader of
            undefined ->
                ok;
            _ ->
                try
                    %% The leader reads subscriptions from the elector on its own node.
                    call_to_member(
                        Leader,
                        {elector_assume_and_distribute, TriggerMember, maps:keys(All),
                            get_member_tokens(All), get_fresh_ids(All), Epoch}
                    )
                catch
                    exit:Reason ->
                        maybe_force_leader_down(Leader, Reason, All)
                end
        end
    end.

%% Sends a presence change to the leader without waiting. If the push is dropped, the
%% reseed on the next handoff catches the leader up.
push_presence_to_leader(Update) ->
    fun(#{leader := Leader, epoch := Epoch}) ->
        case Leader of
            undefined -> ok;
            _ -> push_to_leader(Leader, {presence_update, Update, Epoch})
        end
    end.

%% `noproc` means the leader's node answered but its registry is gone. Leaving it as
%% leader would strand the cluster, so we force a re-election by casting `member_down`
%% with its current token. We deliberately don't depose a leader that is merely not
%% connected. That case is ambiguous, and finding out would open a distribution
%% connection and defeat partition detection. A timeout just means the leader is slow,
%% so we leave it alone too. `self()` is the elector, because actions run in its process.
maybe_force_leader_down(Leader, Reason, All) ->
    case is_noproc(Reason) andalso maps:get(Leader, All, undefined) of
        #{join_token := Token} ->
            % We have to avoid calling `dgen_registry_elector:member_down/3` because
            % that would create a circular dependency.
            dgen_server:cast(self(), {member_down, Leader, Token});
        _ ->
            ok
    end.

is_noproc(noproc) -> true;
is_noproc({noproc, _}) -> true;
is_noproc(_) -> false.

%% Uses the same guard against auto-reconnect as call_to_member/2.
push_to_leader({Node, Name}, Msg) ->
    case dgen_utils:node_reachable(Node) of
        true -> gen_statem:cast({Name, Node}, Msg);
        false -> ok
    end.

get_member_tokens(Members) ->
    maps:map(fun(_, #{join_token := T}) -> T end, Members).

get_fresh_ids(Members) ->
    [Id || {Id, Info} <- maps:to_list(Members), maps:get(fresh, Info, false)].

call_to_member({Node, Name}, Msg) ->
    %% Never let the call auto-reconnect a disconnected node. The resulting nodeup
    %% makes the peer re-join with a new token. Its `{member_down}` is then discarded as
    %% stale, and the partition goes undetected (§5.7).
    case dgen_utils:node_reachable(Node) of
        true -> gen_statem:call({Name, Node}, Msg, ?SnapshotTimeout);
        false -> exit({nodedown, Node})
    end.
