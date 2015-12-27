%%%
%%% Some code of this module is copied from ekaf
%%% 
-module(fetch).

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

-spec add(#fetch_req{}, binary(), integer(), integer()) -> {ok, #fetch_req{}}.
add(FetchReq=#fetch_req{topic_anchor_list = TopicAnchorList}, Topic, Partition, Offset) ->
    NewTopicAnchorList = lists:map(fun(TopicAnchor=#topic_anchor{topic = AnchorTopic, partition_anchor_list = PartitionAnchorList}) ->
                case AnchorTopic of 
                    Topic ->
                        TopicAnchor#topic_anchor{
                            partition_anchor_list=[ #partition_anchor{partition = Partition, offset = Offset} | PartitionAnchorList]
                        };
                    _ ->
                        TopicAnchor
                end end, TopicAnchorList),
    case NewTopicAnchorList of 
        TopicAnchorList ->
            FinalList = [ #topic_anchor{
                    topic = Topic,
                    partition_anchor_list = [
                        #partition_anchor{partition = Partition, offset = Offset}
                    ]
                }| TopicAnchorList],
            {ok, FetchReq#fetch_req{topic_anchor_list = FinalList}};
        _ ->
            {ok, FetchReq#fetch_req{topic_anchor_list = NewTopicAnchorList}}
    end.

encode(#fetch_req{topic_anchor_list = TopicAnchorList}) ->
    Message = <<-1:32/signed, ?MAX_WAIT_TIME:32/signed, ?MIN_BYTES:32/signed,
        (encode_topic_anchor_list(TopicAnchorList))/binary>>,
    Packet = common_proto:encode_request(?FETCH_REQUEST, Message),
    {ok, Packet}.

decode(_) ->
    ok.

encode_topic_anchor_list(List) ->
    EncodedList = lists:map(fun(TA) -> encode_topic_anchor(TA) end, List),
    common_proto:encode_array(EncodedList).

encode_topic_anchor(#topic_anchor{topic=Topic, partition_anchor_list = PartitionAnchorList}) ->
    <<(common_proto:encode_string(Topic))/binary, (encode_partition_anchor_list(PartitionAnchorList))/binary>>.

encode_partition_anchor_list(List) ->
    EncodedList = lists:map(fun(PA) -> encode_partition_anchor(PA) end, List),
    common_proto:encode_array(EncodedList).

encode_partition_anchor(#partition_anchor{partition = Partition,
    offset = Offset, max_bytes = MaxBytes}) ->
    <<Partition:32/signed, Offset:64/signed, MaxBytes:32/signed>>.

-ifdef(TEST).
fetch_add_test() ->
    {ok, F} = fetch:new(),
    {ok, F1} = fetch:add(F, <<"topic">>, 1, 1),
    [T] = F1#fetch_req.topic_anchor_list,
    <<"topic">> = T#topic_anchor.topic,
    [P] = T#topic_anchor.partition_anchor_list,
    1 = P#partition_anchor.partition,
    1 = P#partition_anchor.offset,

    {ok, F2} = fetch:add(F1, <<"topic">>, 2, 2),
    [T2] = F2#fetch_req.topic_anchor_list,
    <<"topic">> = T2#topic_anchor.topic,
    P2 = T2#topic_anchor.partition_anchor_list,
    lists:foreach(fun(#partition_anchor{partition=Partition, offset=Offset}) ->
                case Partition of 
                    1 ->
                        Offset = 1;
                    2 ->
                        Offset = 2
                end end, P2),

    {ok, F3} = fetch:add(F2, <<"another_topic">>, 23, 23),
    T3 = F3#fetch_req.topic_anchor_list,
    lists:foreach(fun(#topic_anchor{topic = Topic, partition_anchor_list = PList}) ->
                Len = erlang:length(PList),
                case Topic of 
                    <<"topic">> ->
                        Len = 2;
                    _ ->
                        Len = 1,
                        [P3] = PList,
                        23 =P3#partition_anchor.partition,
                        23 = P3#partition_anchor.offset
                end
        end, T3).

fetch_encode_test() ->
    {ok, F} = fetch:new(),
    {ok, F1} = fetch:add(F, <<"topic">>, 1, 1),
    {ok, Packet} = fetch:encode(F1),
    {ok, Packet} = encode(<<"topic">>, 1, 1).

encode(Topic, Partition, Offset) ->
    Message = <<-1:32/signed, ?MAX_WAIT_TIME:32/signed, ?MIN_BYTES:32/signed,
        (common_proto:encode_array([<<(common_proto:encode_string(Topic))/binary,
        (common_proto:encode_array([<<Partition:32/signed, Offset:64/signed, ?MAX_BYTES:32/signed>>]))/binary >>]))/binary >>,
    Packet = common_proto:encode_request(?FETCH_REQUEST, Message),
    {ok, Packet}.

-endif.
