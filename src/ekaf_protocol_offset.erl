-module(ekaf_protocol_offset).

-include("ekaf_definitions.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").
-endif.

-export([decode/1, encode/3]).

encode(CorrelationId, ClientId, Packet) ->
    Request = encode_packet(Packet),
    ekaf_protocol:encode_request(?OFFSET_REQUEST, CorrelationId, ClientId, Request).

encode_packet(#offset_request{replica_id = ReplicaId, topics = Topics } = _Packet) ->
    Encoded = encode_topics_with_partitions(Topics),
    <<ReplicaId:32/signed-integer, Encoded/binary>>;
encode_packet(_) ->
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
    Now = get_timestamp(), % TODO
    encode_topic(Topic#topic{ partitions = [#partition{ id = 0, time = Now, max_number_of_offsets = 1 }] });
encode_topic(Topic) when is_record(Topic, topic) ->
    <<(ekaf_protocol:encode_string(Topic#topic.name))/binary, (encode_partitions(Topic#topic.partitions))/binary >>;
encode_topic(_)->
    <<>>.

encode_partitions(Partitions) when is_list(Partitions)->
    ekaf_protocol:encode_array([encode_partition(P) || P <- Partitions]);
encode_partitions(_) ->
    <<>>.

encode_partition(Partition) when is_integer(Partition)->
    encode_partition(#partition{ id = Partition, fetch_offset = 0, max_bytes = ?EKAF_MAX_MSG_SIZE });
encode_partition(#partition{ id = Id, time = Time, max_number_of_offsets = MaxNumberOfOffsets })->
    <<Id:32, Time:64, MaxNumberOfOffsets:32>>.

decode(Packet) ->
    ?INFO_MSG("~n Decode packet: ~p", [Packet]),
    case Packet of
        <<CorrelationId:32, Rest/binary>> ->
            {Topics, _ } = decode_response(Rest),
            #offset_response{ cor_id = CorrelationId, topics = Topics};
            %% #metadata_response{ cor_id = CorrelationId, brokers = Brokers, topics = Topics};
        _ ->
            #offset_response{}
    end.

decode_response(Packet) ->
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
    {Partitions, Rest} = decode_to_partitions(PartitionsBinary),
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

decode_to_partition(<<Id:32, ErrorCode:16, Packet/binary>>) ->
    {Offsets, Rest2} = case Packet of
        <<Len:32, Rest/binary>> ->
          decode_to_offsets(Len, Rest, []);
        _E ->
          {[], Packet}
    end,
    {#partition{ id = Id, error_code = ErrorCode, offsets = Offsets }, Rest2};
decode_to_partition(Rest)->
    {#partition{},Rest}.

%% decode_to_partition_offsets(Packet) ->
%%     case Packet of
%%         <<Len:32, Rest/binary>> ->
%%             decode_to_offsets(Len, Rest, []);
%%         _ ->
%%             {[], Packet}
%%     end.

decode_to_offsets(0, Packet, Previous) ->
    {lists:flatten(Previous), Packet};
decode_to_offsets(Counter, Packet, Previous) ->
    {Next, Rest} = decode_to_offset(Packet),
    decode_to_offsets(Counter - 1, Rest, [Next|Previous]).

decode_to_offset(<<Offset:64, Packet/binary>>) ->
    {Offset, Packet};
decode_to_offset(Rest) ->
    {[], Rest}.

get_timestamp() ->
    {Mega,Sec,Micro} = erlang:now(),
    (Mega*1000000+Sec)*1000000+Micro.
