-module(coordinator).

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

-spec new() -> {ok, #coordinator_req{}}.
new() ->
    {ok, #coordinator_req{}}.

-spec new(binary()) -> {ok, #coordinator_req{}}.
new(GroupId) ->
    {ok, #coordinator_req{group_id = GroupId}}.

-spec encode(#coordinator_req{}) -> {ok, binary()}.
encode(#coordinator_req{group_id=GroupId}) -> 
    Message = common_proto:encode_string(GroupId),
    CoorReq = common_proto:encode_request(?COORDINATOR_REQUEST, Message),
    {ok, CoorReq}.

-spec decode(binary()) -> {ok, #coordinator_res{}}.
decode(Packet) ->
    <<_:32/signed, ErrorCode:16/signed, RestPacket/binary>> = Packet,
    <<CoordinatorId:32/signed, HostSize:16/signed, Host:HostSize/binary, 
        Port:32/signed>> = RestPacket,
    CoordinatorRes = #coordinator_res{
        error_code = ErrorCode,
        broker_id  = CoordinatorId,
        host       = Host,
        port       = Port
    },
    {ok, CoordinatorRes}.

-ifdef(TEST).
coordinator_encode_test() ->
    {ok, F}      = coordinator:new(<<"blcs-channel-1001">>),
    {ok, _Packet} = coordinator:encode(F).

coordinator_decode_test() ->
    {ok, F} = coordinator:new(<<"blcs-channel-1001">>),
    {ok, Packet} = coordinator:encode(F),
    {ok, Ref} = gen_tcp:connect("localhost", 9092, [binary, {active, true}, {packet, 4}]),
    gen_tcp:send(Ref, Packet),
    _Res = receive {tcp, _, P} -> {ok, CoorRes} = coordinator:decode(P), CoorRes end,
    gen_tcp:close(Ref).
-endif.
