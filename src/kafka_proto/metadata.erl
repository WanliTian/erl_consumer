%%%
%%% Some code of this module is copied from ekaf
%%% 
-module(metadata).

-export([
    new/0,
    new/1,
    encode/1,
    decode/1
]).

-include("protocol.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec new() -> {ok, #metadata_req{}}.
new() ->
    {ok, #metadata_req{}}.

-spec new(binary() | list() | atom()) -> {ok, #metadata_req{}}.
new(TopicList) when is_list(TopicList) ->
    BinList = lists:map(fun(Topic) when is_binary(Topic) -> Topic;
                           (Topic) when is_list(Topic)   -> list_to_binary(Topic);
                           (Topic) when is_atom(Topic)   -> atom_to_binary(Topic, utf8)
              end, TopicList),
    {ok, #metadata_req{topics = BinList}};
new(Topic) when is_binary(Topic) ->
    {ok, #metadata_req{topics = [Topic]}};
new(Topic) when is_atom(Topic) ->
    {ok, #metadata_req{topics = [atom_to_binary(Topic, utf8)]}};
new(_) ->
    throw(bad_topic_type).

-spec encode(#metadata_req{}) -> {ok, binary()}.
encode(#metadata_req{topics = TopicList}) ->
    List = lists:map(fun(Topic) ->
                common_proto:encode_string(Topic)
        end, TopicList),
    RequestMessage = common_proto:encode_array(List),
    Request = common_proto:encode_request(?METADATA_REQUEST, RequestMessage),
    {ok, Request}.

-spec decode(binary()) -> {ok, #metadata_res{}}.
decode(Packet) ->
    case Packet of
        <<_:32, Rest/binary>> ->
            {Brokers, Topics, _ } = decode_to_brokers_and_topics(Rest),
            {ok, #metadata_res{brokers = Brokers, topics = Topics}};
        _ ->
            {ok, #metadata_res{}}
    end.

-spec decode_to_brokers_and_topics(binary()) -> {list(), list(), binary()}.
decode_to_brokers_and_topics(Packet)->
    {Brokers,TopicsPacket} = decode_to_brokers(Packet),
    {Topics,Rest} = decode_to_topics(TopicsPacket),
    {Brokers,Topics,Rest}.

-spec decode_to_brokers(binary()) -> list().
decode_to_brokers(Packet) ->
    case Packet of
        <<Len:32, Rest/binary>>->
            decode_to_brokers(Len,Rest,[]);
        _Else ->
            []  
    end.
-spec decode_to_brokers(integer(), binary(), list()) -> {list(), binary()}.
decode_to_brokers(0, Packet, Previous)->
    {Previous,Packet};
decode_to_brokers(Counter, Packet, Previous)->
    {Next,Rest} = decode_to_broker(Packet),
    NewPrevious = case Next of 
        undefined ->
            Previous;
        _ ->
            [Next | Previous]
    end,
    decode_to_brokers(Counter-1, Rest, NewPrevious).

-spec decode_to_broker(binary()) -> {#broker{}, binary()}.
decode_to_broker(<<NodeId:32, HostLen:16, Host:HostLen/binary,Port:32,Rest/binary>>)->
    {#broker{ id = NodeId, host = Host, port = Port},
     Rest};
decode_to_broker(Rest)->
    {undefined,Rest}.

-spec decode_to_topics(binary()) -> list().
decode_to_topics(Packet) ->
    case Packet of
        <<Len:32,Rest/binary>> ->
            decode_to_topics(Len,Rest,[]);
        _E ->
            []  
    end.

-spec decode_to_topics(integer(), binary(), list()) -> {list(), binary()}.
decode_to_topics(0, Packet, Previous)->
    {Previous, Packet};
decode_to_topics(Counter, Packet, Previous) ->
    {Next,Rest} = decode_to_topic(Packet),
    NewPrevious = case Next of 
        undefined ->
            Previous;
        _ ->
            [Next | Previous]
    end,
    decode_to_topics(Counter-1, Rest, NewPrevious).

-spec decode_to_topic(binary()) -> {#topic{}, binary()}.
decode_to_topic(<<NameLen:32, Name:NameLen/binary,PartitionsBinary/binary>>)->
    {Partitions,Rest} = decode_to_partitions(PartitionsBinary),
    {#topic{ name = Name, partitions = Partitions},
     Rest};
decode_to_topic(Rest)->
    {undefined,Rest}.

-spec decode_to_partitions(binary) -> list().
decode_to_partitions(Packet) ->
    case Packet of
        <<Len:32,Rest/binary>> ->
            decode_to_partitions(Len,Rest,[]);
        _ ->
            []
    end.

-spec decode_to_partitions(integer(), binary(), list()) -> {list(), binary()}.
decode_to_partitions(0, Packet, Previous)->
    {Previous, Packet};
decode_to_partitions(Counter, Packet, Previous) ->
    {Next,Rest} = decode_to_partition(Packet),
    NewPrevious = case Next of 
        undefined ->
            Previous;
        _ ->
            [Next | Previous]
    end,
    decode_to_partitions(Counter-1, Rest, NewPrevious).

-spec decode_to_partition(binary()) -> {#partition{}, binary()}.
decode_to_partition(<<ErrorCode:16, Id:32, Leader:32, ReplicasBinary/binary>>)->
    {Replicas,Isrs,Rest} = decode_to_replicas_and_isrs(ReplicasBinary),
    {#partition{ id = Id, error_code = ErrorCode, leader = Leader, reps = Replicas, isrs = Isrs },
     Rest};
decode_to_partition(Rest)->
    {undefined,Rest}.

-spec decode_to_replicas_and_isrs(binary()) -> {list(), list(), binary()}.
decode_to_replicas_and_isrs(Packet)->
    {Replicas,IsrsPacket} = decode_to_replicas(Packet),
    {Isrs,Rest} = decode_to_isrs(IsrsPacket),
    {Replicas,Isrs,Rest}.

-spec decode_to_replicas(binary()) -> list().
decode_to_replicas(Packet) ->
    case Packet of
        <<Len:32,Rest/binary>> ->
            decode_to_replicas(Len,Rest,[]);
        _ ->
            []
    end.

-spec decode_to_replicas(integer(), binary(), list()) -> {list(), binary()}.
decode_to_replicas(0, Packet, Previous)->
    {Previous, Packet};
decode_to_replicas(Counter, Packet, Previous) ->
    {Next,Rest} = decode_to_replica(Packet),
    NewPrevious = case Next of 
        -1 ->
            Previous;
        _ ->
            [Next | Previous]
    end,
    decode_to_replicas(Counter-1, Rest, NewPrevious).

-spec decode_to_replica(binary()) -> {integer(), binary()}.
decode_to_replica(<<Id:32, Rest/binary>>)->
    {Id, Rest};
decode_to_replica(Rest)->
    {-1,Rest}.

decode_to_isrs(Packet) ->
    case Packet of
        <<Len:32,Rest/binary>> ->
            decode_to_isrs(Len,Rest,[]);
        _ ->
            []
    end.
decode_to_isrs(0, Packet, Previous)->
    {Previous, Packet};
decode_to_isrs(Counter, Packet, Previous) ->
    {Next,Rest} = decode_to_isr(Packet),
    NewPrevious = case Next of 
        -1 ->
            Previous;
        _ ->
            [Next | Previous]
    end,
    decode_to_isrs(Counter-1, Rest, NewPrevious).
decode_to_isr(<<Id:32, Rest/binary>>)->
    {Id, Rest};
decode_to_isr(Rest)->
    {-1,Rest}.

-ifdef(TEST).
metadata_test() ->
    {ok, MetaReq} = metadata:new(<<"blcs-channel-1001">>),
    {ok, ReqPacket} = metadata:encode(MetaReq),
    {ok, Ref} = gen_tcp:connect("localhost", 9092, [binary, {packet, 4}, {active, true}]),
    gen_tcp:send(Ref, ReqPacket),
    ResPacket = receive {tcp, _, P} -> P end,
    {ok, _} = metadata:decode(ResPacket),
    gen_tcp:close(Ref).
-endif.
