%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% 1. Monitor node up/down messages
%%% 2. Monitor config file for changing 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(arbiter).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-include("protocol.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    {ok, Args, 0}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    Brokers = config:get_kafka_brokers(),
    Metadata = metadata(Brokers),
    create_controller(Metadata),
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

metadata([]) ->
    lager:error("All kafka brokers is down~n"),
    spawn(fun() -> application:stop(erl_consumer) end),
    ok;
metadata([{Host, Port}|Brokers]=B) ->
    case connection_sup:start_child({Host, Port, <<>>, <<>>, -1}) of 
        ok ->
            case connection:metadata(gproc:where({n, l, {<<>>, <<>>, -1}})) of 
                down ->
                    metadata(Brokers);
                retry ->
                    metadata(B);
                Metadata ->
                    Metadata
            end;
        _Other ->
            io:format("Other: ~p~n", [_Other]),
            metadata(Brokers)
    end.

create_controller(ok) ->
    ok;
create_controller(#metadata_res{topics=Topics}) ->
    lists:foreach(fun(#topic{name = Topic}) ->
                ok = controller_sup:start_child(Topic)
        end, Topics).
