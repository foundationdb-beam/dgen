-module(dgen_frac_index).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Fractional indexing for ordered sequences.

Generates lexicographically-sortable binary strings using base-62 digits
(0-9, A-Z, a-z in ASCII order). A new key can always be generated between
any two existing keys, making this suitable for maintaining ordered lists
in key-value stores without reindexing.
""".
-endif.

-export([
    first/0,
    before/1,
    after_/1,
    between/2,
    n_first/1,
    n_between/3
]).

-export_type([key/0]).

-type key() :: binary().

%% Base-62 digits in ASCII sort order: 0-9, A-Z, a-z
-define(DIGITS, <<"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz">>).
-define(BASE, 62).

%% The minimum and maximum digit values (0-indexed)
-define(MIN_DIGIT, 0).
-define(MAX_DIGIT, 61).

-if(?DOCATTRS).
-doc "Returns the midpoint key of the full keyspace.".
-endif.
-spec first() -> key().
first() ->
    midpoint_char(?MIN_DIGIT, ?MAX_DIGIT + 1).

-if(?DOCATTRS).
-doc "Returns a key that sorts before `Key`.".
-endif.
-spec before(key()) -> key().
before(Key) when is_binary(Key), byte_size(Key) > 0 ->
    between_bins(<<>>, Key).

-if(?DOCATTRS).
-doc "Returns a key that sorts after `Key`.".
-endif.
-spec after_(key()) -> key().
after_(Key) when is_binary(Key), byte_size(Key) > 0 ->
    between_bins(Key, <<>>).

-if(?DOCATTRS).
-doc "Returns a key that sorts between `A` and `B`. Requires `A < B`.".
-endif.
-spec between(key(), key()) -> key().
between(A, B) when is_binary(A), is_binary(B), A < B ->
    between_bins(A, B).

-if(?DOCATTRS).
-doc "Returns `N` evenly-spaced keys across the full keyspace.".
-endif.
-spec n_first(pos_integer()) -> [key()].
n_first(N) when is_integer(N), N > 0 ->
    n_between(<<>>, <<>>, N).

-if(?DOCATTRS).
-doc "Returns `N` evenly-spaced keys between `A` and `B`. Use `<<>>` for unbounded start/end.".
-endif.
-spec n_between(binary(), binary(), non_neg_integer()) -> [key()].
n_between(_A, _B, 0) ->
    [];
n_between(A, B, N) when is_binary(A), is_binary(B), is_integer(N), N > 0 ->
    case N of
        1 ->
            [between_bins(A, B)];
        _ ->
            Mid = between_bins(A, B),
            Left = n_between(A, Mid, N div 2),
            Right = n_between(Mid, B, N - N div 2 - 1),
            Left ++ [Mid | Right]
    end.

%% Internal

-spec between_bins(binary(), binary()) -> key().
between_bins(A, B) ->
    between_bins(A, B, 0, []).

between_bins(A, B, Pos, Acc) ->
    DigitA = digit_at(A, Pos),
    DigitB =
        case B of
            <<>> -> ?MAX_DIGIT + 1;
            _ -> digit_at_or_max(B, Pos, byte_size(B))
        end,
    Mid = (DigitA + DigitB) div 2,
    case Mid > DigitA of
        true ->
            list_to_binary(lists:reverse([digit_to_char(Mid) | Acc]));
        false ->
            between_bins(A, B, Pos + 1, [digit_to_char(DigitA) | Acc])
    end.

digit_at(Bin, Pos) when Pos < byte_size(Bin) ->
    char_to_digit(binary:at(Bin, Pos));
digit_at(_Bin, _Pos) ->
    ?MIN_DIGIT.

digit_at_or_max(Bin, Pos, Size) when Pos < Size ->
    char_to_digit(binary:at(Bin, Pos));
digit_at_or_max(_Bin, Pos, Size) when Pos >= Size ->
    ?MAX_DIGIT + 1.

digit_to_char(D) ->
    binary:at(?DIGITS, D).

char_to_digit(C) ->
    char_to_digit(C, ?DIGITS, 0).

char_to_digit(C, Bin, N) ->
    case Bin of
        <<C, _/binary>> -> N;
        <<_, Rest/binary>> -> char_to_digit(C, Rest, N + 1)
    end.

midpoint_char(A, B) ->
    Mid = (A + B) div 2,
    <<(digit_to_char(Mid))>>.
