-module(ekaf_protocol).

-export([encode_metadata_request/3]).
-export([decode_metadata_response/1]).

-export([encode_sync/3, encode_async/3,
         encode_produce_request/3]).
-export([decode_produce_response/1]).
-export([encode_fetch_request/3, decode_fetch_response/1]).
-export([encode_offset_request/3, decode_offset_response/1]).

-export([encode_request/4,
         encode_bytes/1, encode_string/1, encode_array/1]).

-include("ekaf_definitions.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").
-endif.

encode_bytes(undefined) ->
    <<-1:32/signed>>;
encode_bytes(Data) ->
    Payload = iolist_to_binary(Data),
    <<(byte_size(Payload)):32, Payload/binary>>.

encode_string(undefined) ->
    <<-1:16/signed>>;
encode_string(Data) ->
    Payload = iolist_to_binary(Data),
    <<(byte_size(Payload)):16, Payload/binary>>.

encode_array(List) ->
    Len = length(List),
    Payload = << <<(iolist_to_binary(B))/binary>> || B <- List>>,
    <<Len:32, Payload/binary>>.

encode_request(ApiKey, CorrelationId, ClientId, RequestMessage) ->
    <<ApiKey:16, ?API_VERSION:16, CorrelationId:32, (encode_string(ClientId))/binary, RequestMessage/binary>>.

encode_sync(CorrelationId, ClientId, Packet)->
    ekaf_protocol_produce:encode_produce_request(CorrelationId, ClientId, Packet).

encode_async(CorrelationId, ClientId, Packet)->
    ekaf_protocol_produce:encode_produce_request(CorrelationId, ClientId, Packet).

encode_produce_request(CorrelationId, ClientId, Packet)->
    ekaf_protocol_produce:encode(CorrelationId, ClientId, Packet).

decode_produce_response(Packet)->
    ekaf_protocol_produce:decode(Packet).

encode_fetch_request(CorrelationId, ClientId, Packet) ->
    ekaf_protocol_fetch:encode(CorrelationId, ClientId, Packet).

decode_fetch_response(Packet) ->
    ekaf_protocol_fetch:decode(Packet).

encode_offset_request(CorrelationId, ClientId, Packet) ->
    ekaf_protocol_offset:encode(CorrelationId, ClientId, Packet).
decode_offset_response(Packet) ->
    ekaf_protocol_offset:decode(Packet).
%%---------------------------------
%% Decode metadata response
%%---------------------------------
encode_metadata_request(CorrelationId, ClientId, Topics) ->
    MetadataRequest = encode_array([encode_string(Topic) || Topic <- Topics]),
    encode_request(?METADATA_REQUEST, CorrelationId, ClientId, MetadataRequest).


decode_metadata_response(Packet)->
    ekaf_protocol_metadata:decode(Packet).
