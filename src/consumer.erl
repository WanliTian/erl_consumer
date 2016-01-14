-module(consumer).
-behaviour(gen_server).

-include("erl_consumer.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    start_link/1,
    skip/2,
    close/1
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

close(Pid) ->
    gen_server:cast(Pid, close).

skip(Pid, N) ->
    gen_server:cast(Pid, {skip, N}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init({Host,Port,GroupId,Topic,Partition}=Args) ->
    gproc:reg({p, l, GroupId}, self()),
    ok = connection_sup:start_child(Args),
    State = #consumer_state{
        location = #location{
            host = Host,
            port = Port
        },
        anchor = #anchor{
            group_id  = GroupId,
            topic     = Topic,
            partition = Partition
        }
    },
    {ok, State, 0}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({skip, SN}, State=#consumer_state{skip_n=N}) ->
    {noreply, State#consumer_state{skip_n = N + SN}};

handle_cast(close, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State=#consumer_state{skip_n=0,
        anchor=#anchor{group_id=GroupId, topic=Topic, partition=Partition}}) ->
    case connection:fetch(gproc:where({n, l, {GroupId, Topic, Partition}})) of 
        down ->
            {stop, normal, State};
        retry ->
            {noreply, State, 0};
        empty ->
            {noreply, State, 0};
        Message ->
            {M, F} = config:get_msg_callback(Topic),
            case erlang:apply(M, F, [Message]) of 
                true ->
                    case ack(State) of 
                        ok ->
                            {noreply, State, 0};
                        down ->
                            {sotp ,normal, State}
                    end;
                %%% false indicates retry more
                false ->
                    {noreply, State, 0}
            end
    end;
handle_info(timeout, State=#consumer_state{skip_n=N,
        anchor=#anchor{group_id=GroupId, topic = Topic, partition=Partition}}) ->
    case connection:fetch(gproc:where({n, l, {GroupId, Topic, Partition}})) of 
        down ->
            {stop, normal, State};
        retry ->
            {noreply, State, 0};
        empty ->
            {noreply, State, 0};
        Message ->
            lager:warning("Pid: ~p, State: ~p, Skip One Message:~p~n", [
                    self(), State, Message]),
            case ack(State) of 
                ok ->
                    {noreply, State#consumer_state{skip_n=N-1}, 0};
                down ->
                    {stop, normal, State}
            end
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State=#consumer_state{anchor=#anchor{
        group_id=GroupId, topic=Topic, partition=Partition}}) ->
    connection:close({GroupId, Topic, Partition}),
    lager:warning("consumer close for reason: ~p, pid: ~p, state:~p~n", [
            Reason, self(), State]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
ack(#consumer_state{anchor=#anchor{group_id=GroupId, topic=Topic, partition=Partition}}=State) ->
    case connection:ack(gproc:where({n, l, {GroupId, Topic, Partition}})) of 
        ok ->
            ok;
        down ->
            down;
        retry ->
            timer:sleep(100),
            ack(State)
    end.
