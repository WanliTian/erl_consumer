-module(config).

-export([
        check/0,
        get_auto_offset_rest/0
]).

check() ->
    ok.

get_auto_offset_rest() ->
    case application:get_env(erl_consumer, auto_offset_reset, largest) of 
        largest ->
            -1;
        _ ->
            -2
    end.
