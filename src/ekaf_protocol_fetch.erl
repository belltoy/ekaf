-module(ekaf_protocol_fetch).

-include("ekaf_definitions.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").
-endif.

%% -export([decode/1,encode/3,encode_sync/3, encode_async/3]).
%% -export([encode_produce_request/3]).
%%
-export([decode/1, encode/3]).

%% encode(CorrelationId, Client, Packet) ->
%%     ekaf_protocol:encode_fetch_request(CorrelationId, ClientId, Packet).

encode(CorrelationId, ClientId, FetchPacket) ->
    FetchRequest = encode_fetch_packet(FetchPacket),
    ekaf_protocol:encode_request(?FETCH_REQUEST, CorrelationId, ClientId, FetchRequest).

encode_fetch_packet(#fetch_request{replica_id = ReplicaId,
                                   max_wait_time = MaxWaitTime,
                                   min_bytes = MinBytes,
                                   topics = Topics } = _Packet) ->
    Encoded = encode_topics_with_partitions(Topics),
    <<ReplicaId:32/signed-integer,
      MaxWaitTime:32,
      MinBytes:32,
      Encoded/binary>>;
encode_fetch_packet(_) ->
    <<>>.

encode_topics_with_partitions(Topics) ->
    encode_topics(Topics).

encode_topics(Topics) when is_list(Topics) ->
    Len = length(Topics),
    EncodedTopics = encode_topics(Topics, <<>>),
    <<Len:32, EncodedTopics/binary>>;
encode_topics(_) ->
    <<>>.

encode_topics([], Bin) ->
    Bin;
encode_topics([Topic|Rest], Bin) ->
    encode_topics(Rest, <<Bin/binary, (encode_topic(Topic))/binary>>).

encode_topic(Topic) when is_binary(Topic)->
    encode_topic(#topic{ name=Topic});
encode_topic(#topic{ partitions = [] }=Topic)->
    encode_topic(Topic#topic{ partitions = [#partition{ id = 0 }] });
encode_topic(Topic) when is_record(Topic, topic) ->
    <<(ekaf_protocol:encode_string(Topic#topic.name))/binary,
      (encode_partitions(Topic#topic.partitions))/binary >>;
encode_topic(_)->
    <<>>.

encode_partitions(Partitions) when is_list(Partitions)->
    ekaf_protocol:encode_array([encode_partition(P) || P <- Partitions]);
encode_partitions(_) ->
    <<>>.

encode_partition(Partition) when is_integer(Partition)->
    encode_partition(#partition{ id = Partition,
                                 fetch_offset = 0,
                                 max_bytes = ?EKAF_MAX_MSG_SIZE });
encode_partition(#partition{ id = Id,
                             fetch_offset = FetchOffset,
                             max_bytes = MaxBytes })->
    <<Id:32, FetchOffset:64, MaxBytes:32>>.

decode(Packet) ->
    case Packet of
        <<CorrelationId:32, Rest/binary>> ->
            {Topics, _ } = decode_to_x(Rest),
            #fetch_response{ cor_id = CorrelationId, topics = Topics};
            %% #metadata_response{ cor_id = CorrelationId, brokers = Brokers, topics = Topics};
        _ ->
            #fetch_response{}
    end.

decode_to_x(Packet) ->
    case Packet of
        <<Len:32, Rest/binary>> ->
            decode_to_topics(Len, Rest, []);
        _ ->
            {[], Packet}
    end.

decode_to_topics(0, Packet, Previous) ->
    {Previous, Packet};
decode_to_topics(Counter, Packet, Previous) ->
    {Next, Rest} = decode_to_topic(Packet),
    decode_to_topics(Counter - 1, Rest, [Next|Previous]).

decode_to_topic(<<NameLen:16, Name:NameLen/binary, PartitionsBinary/binary>>) ->
    {Partitions,Rest} = decode_to_partitions(PartitionsBinary),
    {#topic{ name = Name, partitions = Partitions},
     Rest};
decode_to_topic(Rest)->
    {#topic{}, Rest}.

decode_to_partitions(Packet) ->
    case Packet of
        <<Len:32, Rest/binary>> ->
            decode_to_partitions(Len, Rest, []);
        _E ->
            {[],Packet}
    end.
decode_to_partitions(0, Packet, Previous)->
    {Previous, Packet};
decode_to_partitions(Counter, Packet, Previous) ->
    {Next, Rest} = decode_to_partition(Packet),
    decode_to_partitions(Counter-1, Rest, [Next|Previous]).

decode_to_partition(<<Id:32,
                      ErrorCode:16,
                      HighwaterMarkOffset:64,
                      MessageSetSize:32,
                      Packet/binary>>) ->

    {MessageSets, Rest2} = case Packet of
        <<MessageSetsEncoded:MessageSetSize/binary, Rest/binary>> ->
            {decode_to_message_sets(MessageSetsEncoded, []), Rest};
        _ ->
            {[], Packet}
    end,

    {#partition{ id = Id,
                 error_code = ErrorCode,
                 highwater_mark_offset = HighwaterMarkOffset,
                 message_sets_size = MessageSetSize,
                 message_sets = MessageSets }, Rest2};
decode_to_partition(Rest)->
    {#partition{}, Rest}.

decode_to_message_sets(<<>>, Previous) ->
    lists:reverse(lists:flatten(Previous));
decode_to_message_sets(Packet, Previous) ->
    {MessageSets, Rest} = decode_to_message_set(Packet),
    decode_to_message_sets(Rest, [MessageSets|Previous]).

decode_to_message_set(<<Offset:64,
                        MessageSize:32,
                        Packet/binary>>) ->
    {Messages, Rest2} = case Packet of
        <<Crc:32,
          MagicByte:8,
          Attributes:8,
          _KeyLen:32,
          ValueLen:32,
          Value:ValueLen/binary,
          Rest/binary>> ->
            {[#message{ crc = Crc,
                magicbyte = MagicByte,
                attributes = Attributes,
                value = Value }], Rest};
        <<Crc:32,
          MagicByte:8,
          Attributes:8,
          KeyLen:32,
          Key:KeyLen/binary,
          ValueLen:32,
          Value:ValueLen/binary,
          Rest/binary>> ->
            {[#message{ crc = Crc,
                magicbyte = MagicByte,
                attributes = Attributes,
                key = Key,
                value = Value }], Rest}
    end,
    {[#message_set{ offset = Offset, size = MessageSize, messages = Messages }], Rest2};
decode_to_message_set(Rest) ->
    {[], Rest}.
