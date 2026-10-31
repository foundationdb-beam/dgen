-module(dgen_registry_elector_actions).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-include("../../include/dgen_eta.hrl").
-define(SnapshotTimeout, 2000).

-if(?DOCATTRS).
-moduledoc false.
-endif.

%% Post-commit actions returned by `dgen_registry_elector:handle_cast_tx/3`.  Each is a
%% fun of the committed elector state, run by `dgen_server` in the elector process
%% after the transaction commits.
%%
%% *This documentation is LLM-generated. See the AI disclosure in README.md.*

-export([
    assume_distribute/1,
    presence_push/1
]).

%% Post-commit: an unreachable leader must not fail the action (the change is already
%% committed; members self-correct on the next membership event).
assume_distribute(TriggerMember) ->
    fun(#{members := All, leader := Leader, epoch := Epoch}) ->
        case Leader of
            undefined ->
                ok;
            _ ->
                try
                    %% The leader pulls subscriptions from its co-located elector.
                    call_to_member(
                        Leader,
                        {elector_assume_and_distribute, TriggerMember, maps:keys(All),
                            member_tokens(All), fresh_ids(All), Epoch}
                    )
                catch
                    exit:Reason ->
                        depose_if_dead(Leader, Reason, All)
                end
        end
    end.

%% Fire-and-forget; a dropped push is caught up by the reseed on the next handoff.
presence_push(Update) ->
    fun(#{leader := Leader, epoch := Epoch}) ->
        case Leader of
            undefined -> ok;
            _ -> push_to_leader(Leader, {presence_update, Update, Epoch})
        end
    end.

%% `noproc` means the leader's node answered but its registry is gone; leaving it as
%% leader strands the cluster, so force a re-election (fenced with its current token).
%% Not-connected is ambiguous and deliberately not deposed: disambiguating would open a
%% distribution connection and defeat partition detection.  Timeouts = slow, left alone.
%% `self()` is the elector: actions run in its process.
depose_if_dead(Leader, Reason, All) ->
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

%% Same auto-reconnect guard as call_to_member/2.
push_to_leader({Node, Name}, Msg) ->
    case dgen_utils:node_reachable(Node) of
        true -> gen_statem:cast({Name, Node}, Msg);
        false -> ok
    end.

member_tokens(Members) ->
    maps:map(fun(_, #{join_token := T}) -> T end, Members).

fresh_ids(Members) ->
    [Id || {Id, Info} <- maps:to_list(Members), maps:get(fresh, Info, false)].

call_to_member({Node, Name}, Msg) ->
    %% Never let the call auto-reconnect a disconnected node: the resulting nodeup makes
    %% the peer re-join with a new token, so its `{member_down}` is discarded as stale
    %% and the partition goes undetected (§5.7).
    case dgen_utils:node_reachable(Node) of
        true -> gen_statem:call({Name, Node}, Msg, ?SnapshotTimeout);
        false -> exit({nodedown, Node})
    end.
