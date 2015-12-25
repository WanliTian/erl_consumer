-module(common_proto).

-export([
    encode_string/1,
    encode_bytes/1,
    encode_array/1,

    encode_request/2
]).

-include("protocol.hrl").

encode_bytes(undefined) ->
    <<-1:32/signed>>;
encode_bytes(Bytes) ->
    Data = erlang:iolist_to_binary(Bytes),
    Size = byte_size(Data),
    <<Size:32, Data/binary>>.

encode_string(undefined) ->
    <<-1:16/signed>>;
encode_string(String) when is_atom(String) ->
    encode_string(atom_to_list(String));
encode_string(String) ->
    Data = iolist_to_binary(String),
    Size = byte_size(Data),
    <<Size:16/signed, Data/binary>>.

encode_array(List) ->
    Length = erlang:length(List),
    Data   = << <<(iolist_to_binary(B))/binary>> || B <- List>>,
    <<Length:32/signed, Data/binary>>.

encode_request(ApiKey, RequestMessage) ->
    <<ApiKey:16/signed, ?API_VERSION:16/signed, 0:32/signed,
        (encode_string("erl_consumer"))/binary, RequestMessage/binary>>.
