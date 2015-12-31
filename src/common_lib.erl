-module(common_lib).

-export([
    notice/1,
    to_list/1,
    to_binary/1,
    to_atom/1
]).

notice(Msg) ->
    lager:notice("~p~n", [Msg]).

to_list(B) when is_list(B) ->
    B;
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(B) when is_atom(B) ->
    atom_to_list(B).

to_binary(B) when is_binary(B) ->
    B;
to_binary(B) when is_list(B) ->
    list_to_binary(B);
to_binary(B) when is_atom(B) ->
    atom_to_binary(B, utf8).

to_atom(B) when is_atom(B) ->
    B;
to_atom(B) when is_binary(B) ->
    binary_to_atom(B, utf8);
to_atom(B) when is_list(B) ->
    list_to_binary(B).
