-module(msg_fetch).

-export([
    new/0,
    add/4,
    encode/1,
    decode/1
]).

-include("protocol.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec new() -> {ok, #fetch_req{}}.
new() ->
    {ok, #fetch_req{}}.

-spec add(binary(), integer(), integer(), #fetch_req{}) -> {ok, #fetch_req{}}.
add(Topic, Partition, Offset, FetchReq=#fetch_req{topic_anchor_list = TopicAnchorList}) ->
    NewTopicAnchorList = lists:map(fun(TopicAnchor=#topic_anchor{topic = AnchorTopic, partition_anchor_list = PartitionAnchorList}) ->
                case AnchorTopic of 
                    Topic ->
                        TopicAnchor#topic_anchor{
                            partition_anchor_list=[ #req_partition_anchor{partition = Partition, offset = Offset} | PartitionAnchorList]
                        };
                    _ ->
                        TopicAnchor
                end end, TopicAnchorList),
    case NewTopicAnchorList of 
        TopicAnchorList ->
            FinalList = [ #topic_anchor{
                    topic = Topic,
                    partition_anchor_list = [
                        #req_partition_anchor{partition = Partition, offset = Offset}
                    ]
                }| TopicAnchorList],
            {ok, FetchReq#fetch_req{topic_anchor_list = FinalList}};
        _ ->
            {ok, FetchReq#fetch_req{topic_anchor_list = NewTopicAnchorList}}
    end.

-spec encode(#fetch_req{}) -> {ok, binary()}.
encode(#fetch_req{topic_anchor_list = TopicAnchorList}) ->
    Message = <<-1:32/signed, ?MAX_WAIT_TIME:32/signed, ?MIN_BYTES:32/signed,
        (encode_topic_anchor_list(TopicAnchorList))/binary>>,
    Packet = common_proto:encode_request(?FETCH_REQUEST, Message),
    {ok, Packet}.

-spec  decode(binary()) -> {ok, #fetch_res{}}.
decode(Packet) ->
    <<_:32/signed, Rest/binary>> = Packet,
    Topics = decode_topics(Rest),
    Result = #fetch_res{
        topic_anchor_list = Topics
    },
    {ok, Result}.

%%Internal Functions

decode_topics(Packet) ->
    <<Len:32/signed, RestPacket/binary>> = Packet,
    decode_topics(Len, RestPacket, []).
decode_topics(0, _RestPacket, Result) ->
    Result;
decode_topics(Len, Packet, Result) ->
    <<TopicLen:16/signed, Topic:TopicLen/binary, RestPacket/binary>> = Packet,
    {Partitions, RestPacket1} = decode_partitions(RestPacket),
    TopicAnchor = #topic_anchor{
        topic=Topic,
        partition_anchor_list = Partitions
    },
    decode_topics(Len-1, RestPacket1, [TopicAnchor | Result]).

decode_partitions(Packet) ->
    <<Len:32/signed, RestPacket/binary>> = Packet,
    decode_partitions(Len, RestPacket, []).
decode_partitions(0, RestPacket, Result) ->
    {Result, RestPacket};
decode_partitions(Len, Packet, Result)->
    <<Partition:32/signed, ErrorCode:16/signed, HWOffset:64/signed, RestPacket/binary>> = Packet,
    {MessageSet, RestPacket1} = decode_message_sets(RestPacket),
    PartitionAnchor = #res_partition_anchor{
        partition  =Partition,
        error_code =ErrorCode,
        hw_offset  =HWOffset,
        message_set=MessageSet
    },
    decode_partitions(Len-1, RestPacket1, [PartitionAnchor | Result]).

decode_message_sets(Packet) ->
    <<MessageSize:32/signed, MessageSetBin:MessageSize/binary, RestPacket/binary>> = Packet,
    MessageSet = decode_message_set(MessageSetBin, []),
    {MessageSet, RestPacket}.

decode_message_set(<<>>, Result) ->
    Messages = lists:reverse(Result),
    #message_set{
        messages = Messages
    };
decode_message_set(Packet, Result) ->
    <<Offset:64/signed, MessageSize:32/signed, MessageBin:MessageSize/binary, RestPacket/binary>> = Packet,
    case MessageSize > 0 of 
        true ->
            Message = decode_message(MessageBin),
            NewMessage = Message#message{offset = Offset},
            decode_message_set(RestPacket, [NewMessage | Result]);
        false ->
            decode_message_set(RestPacket, Result)
    end.

decode_message(Packet) ->
    <<Crc:32, MagicByte:8/signed, Attributes:8/signed, KeySize:32/signed, Key:KeySize/binary,
        ValueSize:32/signed, Value:ValueSize/binary>> = Packet,
    #message{
        crc       =Crc,
        magic_byte=MagicByte,
        attributes=Attributes,
        key       =Key,
        value     =Value
    }.
encode_topic_anchor_list(List) ->
    EncodedList = lists:map(fun(TA) -> encode_topic_anchor(TA) end, List),
    common_proto:encode_array(EncodedList).

encode_topic_anchor(#topic_anchor{topic=Topic, partition_anchor_list = PartitionAnchorList}) ->
    <<(common_proto:encode_string(Topic))/binary, (encode_partition_anchor_list(PartitionAnchorList))/binary>>.

encode_partition_anchor_list(List) ->
    EncodedList = lists:map(fun(PA) -> encode_partition_anchor(PA) end, List),
    common_proto:encode_array(EncodedList).

encode_partition_anchor(#req_partition_anchor{partition = Partition,
    offset = Offset, max_bytes = MaxBytes}) ->
    <<Partition:32/signed, Offset:64/signed, MaxBytes:32/signed>>.

-ifdef(TEST).
fetch_add_test() ->
    {ok, F} = msg_fetch:new(),
    {ok, F1} = msg_fetch:add(<<"topic">>, 1, 1, F),
    [T] = F1#fetch_req.topic_anchor_list,
    <<"topic">> = T#topic_anchor.topic,
    [P] = T#topic_anchor.partition_anchor_list,
    1 = P#req_partition_anchor.partition,
    1 = P#req_partition_anchor.offset,

    {ok, F2} = msg_fetch:add(<<"topic">>, 2, 2, F1),
    [T2] = F2#fetch_req.topic_anchor_list,
    <<"topic">> = T2#topic_anchor.topic,
    P2 = T2#topic_anchor.partition_anchor_list,
    lists:foreach(fun(#req_partition_anchor{partition=Partition, offset=Offset}) ->
                case Partition of 
                    1 ->
                        Offset = 1;
                    2 ->
                        Offset = 2
                end end, P2),

    {ok, F3} = msg_fetch:add(<<"another_topic">>, 23, 23, F2),
    T3 = F3#fetch_req.topic_anchor_list,
    lists:foreach(fun(#topic_anchor{topic = Topic, partition_anchor_list = PList}) ->
                Len = erlang:length(PList),
                case Topic of 
                    <<"topic">> ->
                        Len = 2;
                    _ ->
                        Len = 1,
                        [P3] = PList,
                        23 =P3#req_partition_anchor.partition,
                        23 = P3#req_partition_anchor.offset
                end
        end, T3).

fetch_encode_test() ->
    {ok, F} = msg_fetch:new(),
    {ok, F1} = msg_fetch:add(<<"topic">>, 1, 1, F),
    {ok, Packet} = msg_fetch:encode(F1),
    {ok, Packet} = encode(<<"topic">>, 1, 1).

fetch_decode_test() ->
    {ok, F} = msg_fetch:new(),
    {ok, F1} = msg_fetch:add(<<"blcs-channel-1001">>, 1,1, F),
    {ok, Packet} = msg_fetch:encode(F1),
    {ok, Ref} = gen_tcp:connect("localhost", 9092, [binary, {active, true}, {packet, 4}]),
    gen_tcp:send(Ref, Packet),
    _ResPacket = receive {tcp, _, P} -> {ok, Result} = msg_fetch:decode(P), Result end, 
    gen_tcp:close(Ref).

encode(Topic, Partition, Offset) ->
    Message = <<-1:32/signed, ?MAX_WAIT_TIME:32/signed, ?MIN_BYTES:32/signed,
        (common_proto:encode_array([<<(common_proto:encode_string(Topic))/binary,
        (common_proto:encode_array([<<Partition:32/signed, Offset:64/signed, ?MAX_BYTES:32/signed>>]))/binary >>]))/binary >>,
    Packet = common_proto:encode_request(?FETCH_REQUEST, Message),
    {ok, Packet}.

-endif.
