-module(offset_range).

-export([
    new/0,
    add/3,
    encode/1,
    decode/1
]).

-include("protocol.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec new() -> {ok, #offset_req{}}.
new() ->
    {ok, #offset_req{}}.

-spec add(binary(), integer(), #offset_req{}) -> {ok, #offset_req{}}.
add(Topic, Partition, Req=#offset_req{topic_anchor_list=TopicAnchors}) ->
    {TopicAnchors1, Changed} = 
    lists:foldl(fun(#topic_anchor{topic=TmpTopic, partition_anchor_list = PAnchors}=TopicAnchor,
                {Result, IsChanged}) ->
                case TmpTopic of 
                    Topic ->
                        NewPAnchors = case lists:member(Partition, PAnchors) of 
                            true ->
                                PAnchors;
                            false ->
                                [Partition | PAnchors]
                        end,
                        {[TopicAnchor#topic_anchor{partition_anchor_list=NewPAnchors} | Result], true};
                    _ ->
                        {[TopicAnchor | Result], IsChanged}
                end end, {[], false}, TopicAnchors),
    TopicAnchors2 = case Changed of 
        false ->
            [#topic_anchor{topic = Topic, partition_anchor_list=[Partition]} | TopicAnchors1];
        _ ->
            TopicAnchors1
    end,
    {ok, Req#offset_req{topic_anchor_list=TopicAnchors2}}.

-spec encode(#offset_req{}) -> {ok, binary()}.
encode(#offset_req{topic_anchor_list = TopicAnchors}) ->
    BinTopics = encode_topics(TopicAnchors),
    Message   = <<-1:32/signed, BinTopics/binary>>,
    Packet = common_proto:encode_request(?OFFSET_REQUEST, Message),
    {ok, Packet}.

-spec decode(binary()) -> {ok, #offset_res{}}.
decode(Packet) ->
    <<_:32/signed, RestPacket/binary>> = Packet,
    TopicAnchors = decode_topics(RestPacket),
    {
        ok,
        #offset_res{
            topic_anchor_list=TopicAnchors
        }
    }.

decode_topics(Packet) ->
    <<Len:32/signed, RestPacket/binary>> = Packet,
    decode_topics(Len, RestPacket, []).
decode_topics(0, <<>>, Result) ->
    Result;
decode_topics(Len, Packet, Result) ->
    {TopicAnchor, RestPacket} = decode_topic(Packet),
    decode_topics(Len-1, RestPacket, [TopicAnchor | Result]).

decode_topic(Packet) ->
    <<Len:16/signed, Topic:Len/binary, RestPacket/binary>> = Packet,
    {PAnchors, RestPacket1} = decode_partitions(RestPacket),
    TopicAnchor=#topic_anchor{
        topic = Topic,
        partition_anchor_list = PAnchors
    },
    {TopicAnchor, RestPacket1}.

decode_partitions(Packet) ->
    <<Len:32/signed, RestPacket/binary>> = Packet,
    decode_partitions(Len, RestPacket, []).

decode_partitions(0, RestPacket, Result) ->
    {Result, RestPacket};
decode_partitions(Len, Packet, Result) ->
    {PAnchor, RestPacket} = decode_partition(Packet),
    decode_partitions(Len-1, RestPacket, [PAnchor | Result]).

decode_partition(Packet) ->
    <<Partition:32/signed, ErrorCode:16/signed, RestPacket/binary>> = Packet,
    <<1:32/signed, Offset:64/signed, RestPacket1/binary>> = RestPacket,
    PAnchor = #offset_fetch_pa_res{
        partition  = Partition,
        offset     = Offset,
        error_code = ErrorCode
    },
    {PAnchor, RestPacket1}.

encode_topics(List) ->
    BinList = lists:map(fun(T) -> encode_topic(T) end, List),
    common_proto:encode_array(BinList).

encode_topic(#topic_anchor{topic=Topic, partition_anchor_list=PList}) ->
    BinTopic = common_proto:encode_string(Topic),
    BinPList = encode_partitions(PList),
    <<BinTopic/binary, BinPList/binary>>.

encode_partitions(List) ->
    BinList = lists:map(fun(P) -> encode_partition(P) end, List),
    common_proto:encode_array(BinList).

encode_partition(Partition) ->
    Time = config:get_auto_offset_rest(),
    <<Partition:32/signed, Time:64/signed, 1:32/signed>>.

-ifdef(TEST).

offset_range_add_test() ->
    {ok, R1} = ?MODULE:new(),
    {ok, R2} = ?MODULE:add(<<"blcs-channel-1001">>, 1, R1),
    {ok, R2} = ?MODULE:add(<<"blcs-channel-1001">>, 1, R2),
    [T] = R2#offset_req.topic_anchor_list,
    <<"blcs-channel-1001">> = T#topic_anchor.topic,
    [1] = T#topic_anchor.partition_anchor_list,

    {ok, R3} = ?MODULE:add(<<"blcs-channel-1001">>, 2, R2),
    [T1] = R3#offset_req.topic_anchor_list,
    <<"blcs-channel-1001">> = T1#topic_anchor.topic,
    [2,1] = T1#topic_anchor.partition_anchor_list,


    {ok, R4} = ?MODULE:add(<<"blcs-push-1001">>, 5, R2),
    List = R4#offset_req.topic_anchor_list,
    lists:foreach(fun(#topic_anchor{topic=Topic, partition_anchor_list=L}) ->
                case Topic of 
                    <<"blcs-channel-1001">> ->
                        L = [1];
                    _ ->
                        L = [5]
                end end, List).

offset_range_encode_test() ->
    {ok, R1} = ?MODULE:new(),
    {ok, R2} = ?MODULE:add(<<"blcs-channel-1001">>, 1, R1),
    {ok, _Packet} = ?MODULE:encode(R2).

offset_range_decode_test() ->
    {ok, R1} = ?MODULE:new(),
    {ok, R2} = ?MODULE:add(<<"blcs-channel-1001">>, 1, R1),
    {ok, Packet} = ?MODULE:encode(R2),
    {ok, Ref} = gen_tcp:connect("localhost", 9092, [binary, {active, true}, {packet, 4}]),
    ok = gen_tcp:send(Ref, Packet),
    Res = receive {tcp, Ref, P} -> P end,
    {ok, Result} = ?MODULE:decode(Res),
    [L] = Result#offset_res.topic_anchor_list,
    [P1] = L#topic_anchor.partition_anchor_list,
    ?debugFmt("Result: ~p~n", [P1#offset_fetch_pa_res.offset]).
-endif.
