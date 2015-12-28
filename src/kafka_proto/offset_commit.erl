-module(offset_commit).

-export([
    new/1,
    add/4,
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

-spec add(binary(), integer(), integer(), #offset_req{}) -> 
    {ok, #offset_req{}}.
add(Topic, Partition, OffSet, #offset_req{topic_anchor_list = AnchorList}=Req) ->
    PAnchor = #req_partition_anchor{
        partition = Partition,
        offset    = OffSet
    },
    NewAnchorList = lists:map(
        fun(TopicAnchor = #topic_anchor{topic = AnchorTopic, partition_anchor_list= PAnchorList}) ->
                case AnchorTopic of 
                    Topic  ->
                        NewPAnchorList = case lists:member(PAnchor, PAnchorList) of 
                            true ->
                                PAnchorList;
                            false ->
                                [PAnchor | PAnchorList]
                        end,
                        TopicAnchor#topic_anchor{partition_anchor_list = NewPAnchorList};
                    _ ->
                        TopicAnchor
                end
        end, AnchorList),
    %%% there are no that topic
    NewAnchorList2 = case NewAnchorList of 
        AnchorList ->
            [#topic_anchor{topic = Topic, partition_anchor_list=[PAnchor]}];
        _ ->
            NewAnchorList
    end,
    {ok, Req#offset_req{topic_anchor_list=NewAnchorList2}}.

-spec encode(#offset_req{}) -> {ok, binary()}.
encode(#offset_req{group_id=GroupId, topic_anchor_list=AnchorList}) ->
    ConsumerGroup = common_proto:encode_string(GroupId),
    AnchorBinList = encode_topic_anchor_list(AnchorList),
    Message = <<ConsumerGroup/binary, AnchorBinList/binary>>,
    Packet = common_proto:encode_request(?OFFSET_COMMIT_REQUEST, Message),
    {ok, Packet}.

-spec decode(binary()) -> {ok, #offset_res{}}.
decode(Packet) ->
    <<_:32/signed, RestPacket/binary>> = Packet,
    TopicAnchors = decode_topics(RestPacket),
    {
        ok,
        #offset_res{
            topic_anchor_list = TopicAnchors
        }
    }.

decode_topics(Packet) ->
    <<Len:32/signed, RestPacket/binary>> = Packet,
    decode_topics(Len, RestPacket, []).
decode_topics(0, <<>>, Result) ->
    Result;
decode_topics(Len, Packet, Result) ->
    {TopicAnchor, RestPacket} = decode_topic(Packet),
    decode_topics(Len-1, RestPacket, [TopicAnchor|Result]).

decode_topic(Packet) ->
    <<Len:16/signed, Topic:Len/binary, RestPacket/binary>> = Packet,
    {PartitionAnchors, RestPacket1} = decode_partitions(RestPacket),
    {
        #topic_anchor{
            topic = Topic,
            partition_anchor_list = PartitionAnchors
        },
        RestPacket1
    }.

decode_partitions(Packet) ->
    <<Len:32/signed, RestPacket/binary>> = Packet,
    decode_partitions(Len, RestPacket, []).

decode_partitions(0, RestPacket, Result) ->
    {Result, RestPacket};
decode_partitions(Len, Packet, Result) ->
    {PartitionAnchor, RestPacket} = decode_partition(Packet),
    decode_partitions(Len-1, RestPacket, [PartitionAnchor | Result]).

decode_partition(Packet) ->
    <<Partition:32/signed, ErrorCode:16/signed, RestPacket/binary>> = Packet,
    {
        #offset_fetch_pa_res{
            partition  = Partition,
            error_code = ErrorCode
        },
        RestPacket
    }.

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

encode_partition(#req_partition_anchor{partition=Partition, offset=Offset}) ->
    <<Partition:32/signed, Offset:64/signed,(common_proto:encode_string(<<"erl_consumer">>))/binary>>.

-ifdef(TEST).
offset_commit_add_test() ->
    {ok, F} = ?MODULE:new(<<"blcs-channel-1001">>),
    {ok, F2} = ?MODULE:add(<<"blcs-channel-1001">>, 20, 10000, F),
    <<"blcs-channel-1001">> = F2#offset_req.group_id,
    [T] = F2#offset_req.topic_anchor_list,
    <<"blcs-channel-1001">> = T#topic_anchor.topic,
    [#req_partition_anchor{partition=20, offset=10000}] = T#topic_anchor.partition_anchor_list,

    {ok, F2} = ?MODULE:add(<<"blcs-channel-1001">>, 20, 10000, F2),

    {ok, F3} = ?MODULE:add(<<"blcs-channel-1001">>, 20, 10001, F2),
    [T1] = F3#offset_req.topic_anchor_list,
    <<"blcs-channel-1001">> = T1#topic_anchor.topic,
    [#req_partition_anchor{partition=20, offset=10001},
    #req_partition_anchor{partition=20, offset=10000}] = T1#topic_anchor.partition_anchor_list.

offset_commit_encode_test() ->
    {ok, F} = ?MODULE:new(<<"blcs-channel-1001">>),
    {ok, F2} = ?MODULE:add(<<"blcs-channel-1001">>, 20, 10000, F),
    {ok, _Packet} = ?MODULE:encode(F2).

offset_commit_decode_test() ->
    {ok, F} = ?MODULE:new(<<"blcs-channel-1001">>),
    {ok, F2} = ?MODULE:add(<<"blcs-channel-1001">>, 1, 10, F),
    {ok, Packet} = ?MODULE:encode(F2),
    {ok, Ref} = gen_tcp:connect("localhost", 9092, [binary, {active, true}, {packet, 4}]),
    gen_tcp:send(Ref, Packet),
    _Res = receive {tcp, _, P} -> {ok, Res} = ?MODULE:decode(P), Res end,
    gen_tcp:close(Ref).

-endif.
