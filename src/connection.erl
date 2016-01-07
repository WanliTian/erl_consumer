-module(connection).
-behaviour(gen_fsm).
-define(SERVER, ?MODULE).
-define(TCP_OPTS, [binary, {active, true}, {packet, 4}]).

-include("erl_consumer.hrl").
-include("protocol.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    start_link/1,
    fetch/1,
    metadata/2,
    ack/1
]).

%% ------------------------------------------------------------------
%% gen_fsm Function Exports
%% ------------------------------------------------------------------

-export([init/1, connect/2, connect/3, ready/2, ready/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3,
         code_change/4]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_fsm:start_link(?MODULE, Args, []).

fetch(Pid) ->
    gen_fsm:sync_send_event(Pid, fetch, infinity).

ack(Pid) ->
    gen_fsm:sync_send_event(Pid, ack).

metadata(Pid, Topic) ->
    gen_fsm:sync_send_event(Pid, {metadata, Topic}).
%% ------------------------------------------------------------------
%% gen_fsm Function Definitions
%% ------------------------------------------------------------------

init({Host, Port, GroupId, Topic, Partition}) ->
    gproc:reg({n, l, {GroupId, Topic, Partition}}),
    State = #conn_state{
        bro = #location{
            host = common_lib:to_list(Host),
            port = Port
        },
        coor = #location{},
        anchor = #anchor{
            group_id  = GroupId,
            topic     = Topic,
            partition = Partition
        },
        is_down=false
    },
    gen_fsm:send_event(self(), connect_bro),
    {ok, connect, State}.

connect(connect_bro, State=#conn_state{bro=#location{host=Host, port=Port}=Bro}) ->
    case gen_tcp:connect(Host, Port, ?TCP_OPTS) of 
        {ok, Ref} ->
            gen_fsm:send_event(self(), fetch_coor),
            Bro1 = Bro#location{ref=Ref},
            {next_state, connect, State#conn_state{bro=Bro1}};
        {error, Reason} ->
            lager:warning("connection stop, State is : ~p, Reason is : ~p~n",
                [State, Reason]),
            State1 = State#conn_state{is_down=true},
            {next_state, ready, State1}
    end;

connect(fetch_coor, State=#conn_state{
        anchor=#anchor{partition=-1}}) ->
    {next_state, ready, State};
connect(fetch_coor, State=#conn_state{coor=#location{ref =Ref}}) when Ref =/= undefined->
    {next_state, ready, State};
connect(fetch_coor, State=#conn_state{bro=#location{ref=Ref},
    anchor=#anchor{group_id=GroupId}}) ->
    {ok, Req} = coordinator:new(GroupId),
    {ok, Packet} = coordinator:encode(Req),
    case gen_tcp:send(Ref, Packet) of 
        ok ->
            case receive_packet(Ref) of 
                error ->
                    %% handle_info will process these tcp error
                    %% so just keep status unchanged
                    {next_state, connect ,State};
                Response ->
                    {ok, #coordinator_res{host=Host, port=Port}}
                        =coordinator:decode(Response),
                    gen_fsm:send_event(self(), connect_coor),
                    Coor = #location{host=common_lib:to_list(Host), port=Port},
                    {next_state, connect, State#conn_state{coor=Coor}}
            end;
        {error, closed} ->
            {next_state, connect, State}
    end;

connect(connect_coor, State=#conn_state{coor=#location{host=Host,port=Port}=Coor}) ->
    case gen_tcp:connect(Host, Port, ?TCP_OPTS) of 
        {ok, Ref} ->
            State1 = State#conn_state{coor = Coor#location{ref=Ref}},
            case State1#conn_state.offset of 
                -1 ->
                    gen_fsm:send_event(self(), init_offset),
                    {next_state, connect, State1};
                _ ->
                    {next_state, ready, State1}
            end;
        {error,Reason} ->
            lager:warning("connect_coor error, Reason: ~p~n", [Reason]),
            gen_fsm:send_event(self(), fetch_coor),
            State1 = State#conn_state{coor = Coor#location{ref=undefined}},
            {next_state, connect, State1}
    end;

connect(init_offset, State=#conn_state{coor=#location{ref=Ref},
    anchor=#anchor{group_id=GroupId, topic=Topic,partition=Partition}}) ->
    {ok, R}  = offset_fetch:new(GroupId),
    {ok, R2} = offset_fetch:add(Topic, Partition, R),
    {ok, Request} = offset_fetch:encode(R2),
    case gen_tcp:send(Ref, Request) of 
        ok ->
            case receive_packet(Ref) of
                error ->
                    {next_state, connect, State};
                Response ->
                    {ok, ResRecord} = offset_fetch:decode(Response),
                    Offset = get_offset(ResRecord),
                    State1 = State#conn_state{offset=Offset},
                    {next_state, ready, State1}
            end;
        {error, closed} ->
            {next_state, connect, State}
    end;

connect({tcp_closed, _Ref}=Info, State) ->
    handle_info(Info, connect, State);

connect(Event, State) ->
    lager:warning("Some events have not handled: ~p~n", [Event]),
    {next_state, connect, State}.

connect(_, _, State) ->
    {reply, retry, connect, State}.


ready({tcp_closed, _Ref}=Info, State) ->
    handle_info(Info, ready, State).

ready(_, _From, State=#conn_state{is_down=true}) ->
    {reply, down, ready, State};
ready(fetch, _From, State=#conn_state{messages=[]}) ->
    case fetch_msgs(State) of 
        retry ->
            {reply, retry, ready, State};
        NewState=#conn_state{messages=Messages} ->
            case Messages of 
                [] ->
                    {reply, empty, ready, NewState};
                [H|_] ->
                    lists:foreach(fun(Message) ->
                                lager:info("Message: ~p~n", [lager:pr(Message, ?MODULE)])
                        end, Messages),
                    {reply, H, ready, NewState}
            end
    end;
ready(fetch, _From, State=#conn_state{messages=Messages}) ->
    [H|_] = Messages,
    {reply, H#message.value, ready, State};

ready(ack, _From, State=#conn_state{messages=[H | Messages],
        coor=#location{ref=Ref},
        anchor=#anchor{group_id=GroupId, partition=Partition, topic=Topic}}) ->
    Offset = H#message.offset + 1,
    {ok, Req}  = offset_commit:new(GroupId),
    {ok, Req1} = offset_commit:add(Topic, Partition, Offset, Req),
    {ok, Packet} = offset_commit:encode(Req1),
    case gen_tcp:send(Ref, Packet) of 
        ok ->
            case receive_packet(Ref) of 
                error ->
                    {reply, retry, ready, State};
                Response ->
                    {ok,_} = offset_commit:decode(Response),
                    {reply, ok, ready, State#conn_state{messages=Messages}}
            end;
        {error, closed} ->
            {reply, retry, ready, State}
    end;

ready({metadata, Topic}, _From, State=#conn_state{bro=#location{ref=Ref}}) ->
    {ok, Req} = metadata:new(Topic),
    {ok, Packet} = metadata:encode(Req),
    case gen_tcp:send(Ref, Packet) of 
        ok ->
            case receive_packet(Ref) of 
                error ->
                    {reply, retry, ready, State};
                Response ->
                    {ok, Result} = metadata:decode(Response),
                    {reply, Result, ready, State}
            end;
        {error, closed} ->
            {reply, retry, ready, State}
    end;

ready(stop, _From, State) ->
    {stop, normal, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info({tcp_closed, Ref}, _StatusName, State=#conn_state{bro=#location{ref=Ref}}) ->
    gen_tcp:close(Ref),
    gen_fsm:send_event(self(), connect_bro),
    {next_state, connect, State};
handle_info({tcp_closed, Ref}, _StatusName, State=#conn_state{coor=#location{ref=Ref}=Coor}) ->
    gen_tcp:close(Ref),
    gen_fsm:send_event(self(), connect_coor),
    Coor1 = Coor#location{ref=undefined},
    {next_state, connect, State#conn_state{coor=Coor1}};
handle_info(Cmd, Status, State) ->
    lager:error("unhandled cmd: ~p~n", [Cmd]),
    {next_state, Status, State}.

terminate(_Reason, _StateName, #conn_state{coor=#location{ref=CRef},
        bro=#location{ref=BRef}}) ->
    case BRef of
        undefined ->
            ok;
        _ ->
            gen_tcp:close(BRef)
    end,
    case CRef of
        undefined ->
            ok;
        _ ->
            gen_tcp:close(CRef)
    end,
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
receive_packet(Ref) ->
    receive 
        {tcp, Ref, Packet} ->
            Packet;
        {tcp_closed, Ref}=Other ->
            gen_fsm:send_event(self(), Other),
            error
    end.

get_offset(#offset_res{topic_anchor_list=[TopicAnchor]}) ->
    #topic_anchor{partition_anchor_list=[P]} = TopicAnchor,
    Offset = P#offset_fetch_pa_res.offset,
    case Offset of  
        -1 ->
            0;  
        _ ->
            Offset
    end.

fetch_msgs(retry) ->
    retry;
fetch_msgs(State=#conn_state{anchor=#anchor{topic=Topic, partition=Partition}, offset=Offset,
            bro=#location{ref=Ref}}) ->
    {ok, R} = msg_fetch:new(),
    {ok, R2} = msg_fetch:add(Topic, Partition, Offset, R),
    {ok, Request} = msg_fetch:encode(R2),
    case gen_tcp:send(Ref, Request) of 
        ok ->
            case receive_packet(Ref) of 
                error ->
                    retry;
                Response ->
                    {ok, #fetch_res{topic_anchor_list=[TopicAnchor]}} = msg_fetch:decode(Response),
                    #topic_anchor{partition_anchor_list=[PartitionAnchor]} = TopicAnchor,
                    case PartitionAnchor#res_partition_anchor.error_code of 
                        ?OffsetOutOfRange ->
                            fetch_msgs(fetch_outofrange_offset(State));
                        _ ->
                            #message_set{messages=Messages} = PartitionAnchor#res_partition_anchor.message_set,
                            State#conn_state{messages = Messages}
                    end
            end;
        {error, closed} ->
            retry
    end.

fetch_outofrange_offset(State=#conn_state{anchor=#anchor{topic=Topic, partition=Partition},
        coor=#location{ref=Ref}}) ->
    {ok, Req1}   = offset_range:new(),
    {ok, Req2}   = offset_range:add(Topic, Partition, Req1),
    {ok, Packet} = offset_range:encode(Req2),
    case gen_tcp:send(Ref, Packet) of 
        ok ->
            case receive_packet(Ref) of 
                error ->
                    retry;
                Response ->
                    {ok, #offset_res{topic_anchor_list=[TopicAnchor]}} 
                        = offset_range:decode(Response),
                    [PartitionAnchor] = TopicAnchor#topic_anchor.partition_anchor_list,
                    State#conn_state{offset=PartitionAnchor#offset_fetch_pa_res.offset}

            end;
        {error, closed} ->
            retry
    end.
