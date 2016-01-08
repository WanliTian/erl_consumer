-module(cons_group).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-define(SECOND, 1000).
-define(MINUTE, (60 * ?SECOND)).

-include("erl_consumer.hrl").
-include("protocol.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Topic) ->
    gen_server:start_link(?MODULE, Topic, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Topic) ->
    State = #group_state{
        topic = Topic, 
        kafka = #kafka_cluster{
            brokers = config:get_kafka_brokers()
        }
    },
    gproc:reg({n, l, common_lib:to_binary(Topic)}),
    erlang:send_after(?SECOND, self(), create_consumer),
    {ok, State}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(node_changed, State=#group_state{topic=Topic, tref=TRef}) ->
    erlang:cancel_timer(TRef),
    Pids = gproc:lookup_pids({p, l, Topic}),
    %% consumer:close using gen_server:cast
    %% to prevent closing many partitions using too much time
    lists:foreach(fun(Pid) ->
            consumer:close(Pid)
        end, Pids),
    %% check all consumer has been terminated
    %% if not, wait
    lists:foreach(fun(Pid) ->
            util_dead(Pid)
        end, Pids),
    %% clear child spec which has been terminated
    common_lib:clear_specs(consumer_sup),
    handle_info(create_consumer, State);
handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(create_consumer, State=#group_state{topic=Topic}) ->
    {Metadata, NewState} = metadata(State),
    Nodes  = sorted_nodes(Topic),
    Index  = string:str(Nodes, [node()]),
    create_consumer(Nodes, Index, Metadata, NewState),
    TRef = erlang:send_after(?MINUTE, self(), create_consumer),
    {noreply, NewState#group_state{tref=TRef}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
sorted_nodes(Topic) ->
    Nodes  = common_lib:nodes_online(Topic),
    lists:sort(Nodes).

metadata(State=#group_state{topic=Topic, kafka=Kafka}) ->
    case gproc:where({n,l,{<<>>, <<>>, -1}}) of 
        undefined ->
            NewState = build_conn(State),
            metadata(NewState);
        Pid ->
            case connection:metadata(Pid, Topic) of 
                retry ->
                    timer:sleep(100),
                    metadata(State);
                down ->
                    connection:close(Pid),
                    metadata(State);
                Metadata ->
                    Brokers = lists:map(fun(#broker{host=Host,port=Port}) ->
                                {common_lib:to_list(Host), Port}
                        end, Metadata#metadata_res.brokers),
                    Index = 
                    case Kafka#kafka_cluster.index < erlang:length(Brokers) of 
                        true ->
                            Kafka#kafka_cluster.index;
                        false ->
                            1
                    end,
                    {
                        Metadata, 
                        State#group_state{kafka=Kafka#kafka_cluster{
                                index = Index,
                                brokers = Brokers
                            }
                        }
                    }
            end
    end.

build_conn(State=#group_state{kafka=#kafka_cluster{
            index=Index, brokers=Brokers}=Kafka}) ->
    {Host, Port} = lists:nth(Index, Brokers),
    ok = connection_sup:start_child({Host, Port, <<>>, <<>>, -1}),
    FreshIndex = case erlang:length(Brokers) of 
        Index ->
            1;
        _ ->
            Index + 1
    end,
    State#group_state{kafka=Kafka#kafka_cluster{index=FreshIndex}}.

create_consumer(Nodes, Index, #metadata_res{brokers=Brokers, topics=[TopicInfo]}, 
    #group_state{topic=Topic}) ->
    BMapper =  %% broker id map to {host, port}
        lists:foldl(fun(#broker{id=Id, host=Host, port=Port}, M) ->
                    M#{Id => {Host, Port}}
            end, #{}, Brokers),
    PMapper =  %% partition id mapper to broker id for leader
        lists:foldl(fun(#partition{id=Id, leader=Leader}, M) ->
                    M#{Id => Leader}
            end, #{}, TopicInfo#topic.partitions),

    Partitions = lists:sort(fun(#partition{id=Id1}, #partition{id=Id2}) ->
                Id1 > Id2
        end, TopicInfo#topic.partitions),

    Piece = common_lib:piece(length(Partitions), length(Nodes)),

    case length(Partitions) > ((Index-1) * Piece + 1) of 
        true ->
            HoldedPartitions = lists:sublist(Partitions, (Index-1) * Piece + 1, Piece),
            lists:foreach(fun(#partition{id=Id}) ->
                {Host, Port} = maps:get(maps:get(Id, PMapper), BMapper),
                consumer_sup:start_child({Host, Port, Topic, Topic, Id})
                end, HoldedPartitions);
        false ->
            nop
    end.

util_dead(Pid) ->
    case erlang:is_process_alive(Pid) of  
        true ->
            timer:sleep(100),
            util_dead(Pid);
        false ->
            nop
    end.
