%%======================================================================
%% Constants
%%======================================================================
-define(EKAF_DEFAULT_MAX_BUFFER_SIZE             , 100).
-define(EKAF_DEFAULT_BOOTSTRAP_BROKER            , {"localhost",9091}).
-define(EKAF_DEFAULT_PER_PARTITION_WORKERS       , 100).
-define(EKAF_DEFAULT_PER_PARTITION_WORKERS_MAX   , 100).
-define(EKAF_DEFAULT_BUFFER_TTL                  , 5000).
-define(EKAF_DEFAULT_PARTITION_STRATEGY          , ordered_round_robin).
-define(EKAF_SYNC_TIMEOUT                        , 1000).
-define(EKAF_MAX_MSG_SIZE                        , 1048576).
-define(EKAF_LATEST_OFFSET_TIME                  , -1).
-define(EKAF_EARLIEST_OFFSET_TIME                , -2).

-define(EKAF_PACKET_IGNORE                       , 0).
-define(EKAF_PACKET_ENCODE_METADATA              , 1).
-define(EKAF_PACKET_DECODE_METADATA              , 2).
-define(EKAF_PACKET_ENCODE_PRODUCE               , 3).
-define(EKAF_PACKET_DECODE_PRODUCE               , 4).

-define(API_VERSION                              , 0).
-define(PRODUCE_REQUEST                          , 0).
-define(FETCH_REQUEST                            , 1).
-define(OFFSET_REQUEST                           , 2).
-define(METADATA_REQUEST                         , 3).
-define(OFFSET_COMMIT_REQUEST                    , 8).
-define(OFFSET_FETCH_REQUEST                     , 9).
-define(CONSUMER_METADATA_REQUEST                , 10).

-define(EKAF_CONSTANT_REFRESH_EVERY_SEC          , <<"refresh_every_second">>).

%%======================================================================
%% Macros
%%======================================================================
-define(INFO_MSG(Format, Args),
    case application:get_env(kafboy,enable_logging) of
         {ok,true} ->
            lager:info
              ("~p:~p ~p:"++Format, [?MODULE, ?LINE, self()]++Args);
        _ ->
            io:format
          ("~n~p:~p ~p:"++Format, [?MODULE, ?LINE, self()]++Args)
    end
       ).

-define(ERROR_MSG(Format, Args),
        case application:get_env(kafboy,enable_logging) of
            {ok,true} ->
                lager:error("~p:~p "++Format++"~n~p", [?MODULE, ?LINE]++Args++[erlang:get_stacktrace()]);
        _ ->
                io:format("~n{~p:~p ~p}:"++Format++"~n~p", [?MODULE, ?LINE, self()]++Args++[erlang:get_stacktrace()])
    end
       ).

-define(DEBUG_MSG(Format, Args),
        case application:get_env(kafboy,enable_logging) of
            {ok,true} ->
                lager:debug(Format,Args);
            _ ->
                ok
        end
       ).

%%======================================================================
%% Specs
%%======================================================================
-define(record_to_list(Rec, Ref), lists:zip(record_info(fields, Rec),tl(tuple_to_list(Ref)))).
-define(BITMASK_REQUIRE_ACK,1).
-type kafka_api_key() :: 0..7.
-type kafka_correlation_id() :: -2147483648..2147483647.
-type kafka_byte() :: <<_:8>>.
-type kafka_word() :: <<_:16>>.
-type kafka_bytes() :: <<_:32, _:_*8>>.
-type kafka_string() :: <<_:16, _:_*8>>.
-type kafka_array() :: <<_:32, _:_*8>>.
-type kafka_request() :: <<_:64, _:_*8>>.
-type kafka_message() :: {iodata(), iodata()} | {iodata(), iodata(), 0..2}.

%%======================================================================
%% Records
%%======================================================================
%% Used by workers
-record(ekaf_fsm, { topic::binary(), broker:: tuple(), partition::integer(), replica::integer(), leader::integer(), socket :: port(), pool::atom(), metadata, cor_id = 0 :: integer(), client_id = "ekaf", reply_to, buffer=[]::list(), max_buffer_size = 1, buffer_ttl = ?EKAF_DEFAULT_BUFFER_TTL, kv, to_buffer = true::boolean(), last_known_size :: integer(), topic_packet, partition_packet, produce_packet, flush_callback }).


%% Requests
-record(produce_request, { required_acks=0::kafka_word(), timeout=0::integer(), topics = []::list()}).
-record(fetch_request, { replica_id=-1::integer(), max_wait_time=1000::integer(), min_bytes=1::integer(), topics = []::list()}).
-record(offset_request, { replica_id=-1::integer(), topics = []::list()}).

%% Responses
-record(produce_response, { cor_id::kafka_word(), timeout=0::integer(), topics = []::list()}).
-record(metadata_response, { cor_id = 0 :: kafka_correlation_id(), brokers = [], topics = [] }).
-record(fetch_response, { cor_id::kafka_word(), timeout=0::integer(), topics = []::list()}).
-record(offset_response, { cor_id::kafka_word(), topics = []::list()}).

%% Other Records
-record(broker, {node_id = 1 :: integer(), host = <<"localhost">> :: binary(), port = 9091 :: integer()}).
-record(topic, {name:: binary(), error_code = 0:: integer(), partitions = []}).
-record(partition, {id                     = 0::integer(),
                    error_code             = 0:: integer(),
                    leader                 = 0:: integer(),
                    replicas               = []::list(),
                    isrs                   = []::list(),
                    highwater_mark_offset::integer(),
                    message_sets_size      = 0::integer(),
                    message_sets           = []::list(),
                    fetch_offset           = 0::integer(),
                    max_bytes              = ?EKAF_MAX_MSG_SIZE::integer(),
                    time::integer(),
                    max_number_of_offsets  = 1::integer(),
                    offsets                = []::list()}).
-record(message_set, {offset=0::integer(), size=0::integer(), messages=[]::list()}).
-record(message, {crc=0::integer(), magicbyte=0::kafka_byte(), attributes=0::kafka_byte(), key = undefined::binary(), value::binary()}).
-record(isr, {id::integer() }).
-record(replica, {id::integer() }).
