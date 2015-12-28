-module(connection).
-behaviour(gen_server).
-define(SERVER, ?MODULE).
-define(TCP_OPTS, [binary, {active, true}, {packet, 4}]).

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

start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init({Host, Port, GroupId, Topic, Partition}) ->
    State = #conn_state{
        bro=#location{
            host = Host,
            port = Port
        },
        anchor=#anchor{
            group_id = GroupId,
            topic=Topic,
            partition=Partition
        }
    },
    {ok, State, 0}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    {noreply, State};
handle_info({tcp, _Ref, _Packet}, #conn_state{bro=#location{ref=_Ref}}=State) ->
    {noreply, State};
handle_info({tcp, _Ref, _Packet}, #conn_state{coor=#location{ref=_Ref}}=State) ->
    {noreply, State};
handle_info({tcp_closed, _Ref}, #conn_state{bro=#location{ref=_Ref}}=State) ->
    {noreply, State};
handle_info({tcp_closed, _Ref}, #conn_state{coor=#location{ref=_Ref}}=State) ->
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
connect_to_bro(State=#conn_state{bro=Bro=#localtion{host=Host,port=Port}})->
    {ok, Ref} = gen_tcp:connect(Host, Port, ?TCP_OPTS),
    NewBro=Bro#location{ref=Ref},
    State#conn_state{bro=NewBro}.

connect_to_coor(State) ->
    ok.

prepare_offset(State) ->
    ok.
