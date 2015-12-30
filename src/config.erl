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

-spec get_msg_callback(binary()) -> {atom(), atom()}.
get_msg_callback(Topic) ->
    Props = application:get_env(erl_consumer, msg_callback, []),
    proplists:get_value(Topic, Props, {common_lib, notice}).
