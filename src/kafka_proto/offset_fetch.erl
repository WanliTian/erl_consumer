-module(offset_fetch).

-export([
    new/1,
    add/3,
    encode/1,
    decode/1
]).

-include("protocol.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec new(binary()) -> {ok, #offset_req{}}.
new(GroupId) ->
    {ok, #offset_req{group_id = GroupId}}.

-spec add(binary(), binary(), #offset_req{}) -> 
    {ok, #offset_req{}}.
add(Topic, Partition, #offset_req{topic_anchor_list = AnchorList}=Req) ->
    {NewAnchorList, Changed} = lists:foldl(
        fun(TopicAnchor = #topic_anchor{topic = AnchorTopic, partition_anchor_list= PAnchorList}, {Result,IsChanged}) ->
                case AnchorTopic of 
                    Topic  ->
                        NewPAnchorList = case lists:member(Partition, PAnchorList) of 
                            true ->
                                PAnchorList;
                            false ->
                                [Partition | PAnchorList]
                        end,
                        {[TopicAnchor#topic_anchor{partition_anchor_list = NewPAnchorList} | Result], true};
                    _ ->
                        {[TopicAnchor | Result], IsChanged}
                end
        end, {[], false}, AnchorList),
    %%% there are no that topic
    NewAnchorList2 = case Changed of 
        false ->
            [#topic_anchor{topic = Topic, partition_anchor_list=[Partition]} | NewAnchorList];
        _ ->
            NewAnchorList
    end,
    {ok, Req#offset_req{topic_anchor_list=NewAnchorList2}}.

-spec encode(#offset_req{}) -> {ok, binary()}.
encode(#offset_req{group_id=GroupId, topic_anchor_list=AnchorList}) ->
    ConsumerGroup = common_proto:encode_string(GroupId),
    AnchorBinList = encode_topic_anchor_list(AnchorList),
    Message = <<ConsumerGroup/binary, AnchorBinList/binary>>,
    Packet = common_proto:encode_request(?OFFSET_FETCH_REQUEST, Message),
    {ok, Packet}.

-spec decode(binary()) -> {ok, #offset_res{}}.
decode(Packet) ->
    <<_:32/signed, RestPacket/binary>> = Packet,
    Res = decode_topics(RestPacket),
    {ok, Res}.

%%% Internal Functions
decode_topics(Packet) ->
    <<Len:32/signed, RestPacket/binary>> = Packet,
    Topics = decode_topics(Len, RestPacket, []),
    #offset_res{
        topic_anchor_list=Topics
    }.

decode_topics(0, <<>>, Result) ->
    Result;
decode_topics(Len, Packet, Result) ->
    {TopicAnchor, RestPacket} = decode_topic(Packet),
    decode_topics(Len-1, RestPacket, [TopicAnchor | Result]).

decode_topic(Packet) ->
    <<Len:16/signed, Name:Len/binary, RestPacket/binary>> = Packet,
    {PartitionRecords, RestPacket1} = decode_partitions(RestPacket),
    TopicAnchor = #topic_anchor{
        topic = Name,
        partition_anchor_list=PartitionRecords
    },
    {TopicAnchor, RestPacket1}.

decode_partitions(Packet) ->
    <<Len:32/signed, RestPacket/binary>> = Packet,
    decode_partitions(Len, RestPacket, []).
decode_partitions(0, RestPacket, Result) ->
    {Result, RestPacket};
decode_partitions(Len, Packet, Result) ->
    {PartitionRecord, RestPacket} = decode_partition(Packet),
    decode_partitions(Len-1, RestPacket, [PartitionRecord | Result]).

decode_partition(Packet) ->
    <<Partition:32/signed, OffSet:64/signed, MLen:16/signed, _:MLen/binary, 
        ErrorCode:16/signed, RestPacket/binary>> = Packet,
    PartitionRecord = #offset_fetch_pa_res{
        partition  = Partition,
        offset     = OffSet,
        error_code = ErrorCode
    },
    {PartitionRecord, RestPacket}.

encode_topic_anchor_list(List) ->
    BinList = lists:map(fun(#topic_anchor{topic = Topic, partition_anchor_list=PList}) ->
        BinTopic = common_proto:encode_string(Topic),
        BinPList = encode_partition_list(PList),
        <<BinTopic/binary, BinPList/binary>>
    end, List),
    common_proto:encode_array(BinList).

encode_partition_list(List) ->
    BinList = [encode_partition(Partition) || Partition <- List],
    common_proto:encode_array(BinList).

encode_partition(Partition) ->
    <<Partition:32/signed>>.

-ifdef(TEST).
offset_fetch_add_test() ->
    {ok, F} = ?MODULE:new(<<"blcs-channel-1001">>),
    {ok, F2} = ?MODULE:add(<<"blcs-channel-1001">>, 1, F),
    <<"blcs-channel-1001">> = F2#offset_req.group_id,
    [T] = F2#offset_req.topic_anchor_list,
    <<"blcs-channel-1001">> = T#topic_anchor.topic,
    [20] = T#topic_anchor.partition_anchor_list,

    {ok, F2} = ?MODULE:add(<<"blcs-channel-1001">>, 1, F2),

    {ok, F3} = ?MODULE:add(<<"blcs-channel-1001">>, 2, F2),
    [T1] = F3#offset_req.topic_anchor_list,
    <<"blcs-channel-1001">> = T1#topic_anchor.topic,
    [30,20] = T1#topic_anchor.partition_anchor_list.

offset_fetch_encode_test() ->
    {ok, F} = ?MODULE:new(<<"blcs-channel-1001">>),
    {ok, F2} = ?MODULE:add(<<"blcs-channel-1001">>, 1, F),
    {ok, _Packet} = ?MODULE:encode(F2).

offset_fetch_decode_test() ->
    {ok, F} = ?MODULE:new(<<"blcs-channel-1001">>),
    {ok, F2} = ?MODULE:add(<<"blcs-channel-1001">>, 2, F),
    {ok, Packet} = ?MODULE:encode(F2),
    {ok, Ref} = gen_tcp:connect("localhost", 9092, [binary, {active, true}, {packet, 4}]),
    gen_tcp:send(Ref, Packet),
    Response = receive {tcp, _, P}-> {ok, Res}= ?MODULE:decode(P), Res end,
    ?debugFmt("~p~n", [Response]),
    gen_tcp:close(Ref).

-endif.
