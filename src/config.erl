-module(config).
-define(LARGEST, -1).
-define(SMALLEST, -2).

-export([
        check/0,
        get_auto_offset_rest/0,
        get_msg_callback/1,
        get_kafka_brokers/0,
        cluster_info/0
]).

check() ->
    KB = ?MODULE:get_kafka_brokers(),
    case KB of 
        [] ->
            lager:error("Kafka Broker Host/Port Cannot Be Null~n"),
            throw(badarg);
        _ ->
            ok
    end,

    Node = atom_to_list(node()),
    case string:str(Node, "@") of 
        0 ->
            lager:error("Erlang Node Cannot Been Started With Short Name~n"),
            throw(badarg);
        _ ->
            ok
    end,

    ok.

get_auto_offset_rest() ->
    case application:get_env(erl_consumer, auto_offset_reset, largest) of 
        largest ->
            ?LARGEST;
        _ ->
            ?SMALLEST
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

cluster_info() ->
    application:get_env(erl_consumer, cluster_info, []).
