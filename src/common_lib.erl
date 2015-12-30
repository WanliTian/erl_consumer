-module(common_lib).

-export([
    notice/1
]).

notice(Msg) ->
    lager:notice("~p~n", [Msg]).
