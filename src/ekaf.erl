-module(ekaf).

-behaviour(application).

%% includes
-include("ekaf_definitions.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").
-endif.

-export([start/0, start/2]).
-export([stop/0, stop/1]).

-export([prepare/1, pick/1, pick/2,
         publish/2, batch/2,
         produce_sync_batched/2, produce_async_batched/2,
         produce_sync/2, produce_async/2,
         fetch/1, offset/1,
         metadata/1, metadata/2, info/1, info/2]).

start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

start(_Type, _Args) ->
    ekaf_sup:start_link([]).

stop(_State) ->
    ok.

%%--------------------------------------------------------------------
%%% API
%%--------------------------------------------------------------------

batch(Topic,Data)->
    produce_sync_batched(Topic, Data).

publish(Topic,Data)->
    produce_async(Topic, Data).

produce_sync(Topic, Data)->
    ekaf_lib:common_sync(produce_sync, Topic, Data).

produce_async(Topic, Data)->
    ekaf_lib:common_async(produce_async, Topic, Data).

produce_sync_batched(Topic, Data)->
    ekaf_lib:common_sync(produce_sync_batched, Topic, Data).

produce_async_batched(Topic, Data)->
    ekaf_lib:common_async(produce_async_batched, Topic, Data).

metadata(Topic)->
    metadata(Topic,?EKAF_SYNC_TIMEOUT).
metadata(Topic, Timeout)->
    Worker = ?MODULE:pick(Topic),
    case Worker of
        {error,{retry,N}} when N < 10 ->
            metadata(Topic,Timeout);
        {error,_}=E->
            E;
        _ ->
            gen_fsm:sync_send_event(Worker, {metadata, Topic}, Timeout)
    end.

fetch(Topic) ->
    Data = #fetch_request{topics = [Topic]},
    ekaf_lib:common_sync(fetch, Topic, Data).

offset(Topic) ->
    Data = #offset_request{topics = [Topic]},
    ekaf_lib:common_sync(offset, Topic, Data).

info(Topic)->
    info(Topic,?EKAF_SYNC_TIMEOUT).
info(Topic,Timeout)->
    Worker = ?MODULE:pick(Topic),
    case Worker of
        {error,{retry,N}} when N < 10 ->
            info(Topic,Timeout);
        {error,_}=E->
            E;
        _ ->
            gen_fsm:sync_send_event(Worker, info, Timeout)
    end.

prepare(Topic)->
    ekaf_lib:prepare(Topic).

pick(Topic)->
    %% synchronous
    ekaf_picker:pick(Topic).

pick(Topic,Callback)->
    %% asynchronous
    ekaf_picker:pick(Topic, Callback).
