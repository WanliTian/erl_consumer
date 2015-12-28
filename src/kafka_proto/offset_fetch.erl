-module(offset_fetch).

-export([
    new/1,
    add/3
]).

-include("protocol.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec new(binary()) -> {ok, #offset_fetch_req{}}.
new(GroupId) ->
    {ok, #offset_fetch_req{group_id = GroupId}}.

-spec add(binary(), binary(), #offset_fetch_req{}) -> 
    {ok, #offset_fetch_req{}}.
add(Topic, Partition, #offset_fetch_req{topic_anchor_list = AnchorList}=Req) ->
    NewAnchorList = lists:map(
        fun(TopicAnchor = #topic_anchor{topic = AnchorTopic, partition_anchor_list= PAnchorList}) ->
                case AnchorTopic of 
                    Topic  ->
                        NewPAnchorList = case lists:member(Partition, PAnchorList) of 
                            true ->
                                PAnchorList;
                            false ->
                                [Partition | PAnchorList]
                        end,
                        TopicAnchor#topic_anchor{partition_anchor_list = NewPAnchorList};
                    _ ->
                        TopicAnchor
                end
        end, AnchorList),
    %%% there are no that topic
    NewAnchorList2 = case NewAnchorList of 
        AnchorList ->
            [#topic_anchor{topic = Topic, partition_anchor_list=[Partition]}];
        _ ->
            NewAnchorList
    end,
    {ok, Req#offset_fetch_req{topic_anchor_list=NewAnchorList2}}.


-ifdef(TEST).
offset_add_test() ->
    {ok, F} = ?MODULE:new(<<"blcs-channel-1001">>),
    {ok, F2} = ?MODULE:add(<<"blcs-channel-1001">>, 20, F),
    <<"blcs-channel-1001">> = F2#offset_fetch_req.group_id,
    [T] = F2#offset_fetch_req.topic_anchor_list,
    <<"blcs-channel-1001">> = T#topic_anchor.topic,
    [20] = T#topic_anchor.partition_anchor_list,

    {ok, F2} = ?MODULE:add(<<"blcs-channel-1001">>, 20, F2),

    {ok, F3} = ?MODULE:add(<<"blcs-channel-1001">>, 30, F2),
    [T1] = F3#offset_fetch_req.topic_anchor_list,
    <<"blcs-channel-1001">> = T1#topic_anchor.topic,
    [30,20] = T1#topic_anchor.partition_anchor_list.
-endif.
