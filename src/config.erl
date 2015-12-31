-module(config).

-export([
        check/0,
        get_auto_offset_rest/0,
        get_msg_callback/1,
        get_kafka_brokers/0
]).

check() ->
    KB = ?MODULE:get_kafka_brokers(),
    KB =/= [].

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

get_kafka_brokers() ->
    Props = application:get_env(erl_consumer, kafka_brokers, []),
    lists:map(fun({Host, Port}) ->
        {common_lib:to_list(Host), Port}
    end, Props).
