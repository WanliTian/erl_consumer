-module(group).
-behaviour(gen_fsm).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).

%% ------------------------------------------------------------------
%% gen_fsm Function Exports
%% ------------------------------------------------------------------

-export([init/1, state_name/2, state_name/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3,
         code_change/4]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Topic) ->
    gen_fsm:start_link(?MODULE, Topic, []).

%% ------------------------------------------------------------------
%% gen_fsm Function Definitions
%% ------------------------------------------------------------------

init(Topic) ->
    gproc:reg({n,l, Topic}),
    State = #group_state{
        topic = Topic,
        kafka = #kafka_cluster{
            brokers = config:get_kafka_brokers()
        }   
    },
    gen_fsm:sync_send_event(self(), 
    {ok, next_state_name, State}.

state_name(_Event, State) ->
    {next_state, state_name, State}.

state_name(_Event, _From, State) ->
    {reply, ok, state_name, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
index_self(Topic) ->
    Nodes  = common_lib:nodes_online(Topic),
    SNodes = lists:sort(Nodes),
    string:str(SNodes, [node()]).
