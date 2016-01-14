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
    net_kernel:monitor_nodes(true),
    sub_file_changing(),
    {ok, Args, 0}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, _State) ->
    common_lib:world(),
    Topics = common_lib:topics(),
    lists:foreach(fun(Topic) ->
        Nodes = common_lib:nodes(Topic),
        case lists:member(node(), Nodes) of 
            true ->
                 ok = cons_group_sup:start_child(Topic);
            false ->
                nop
        end
    end, Topics),

    TimeRef = erlang:send_after(60000, self(), world),
    {noreply, TimeRef};

handle_info({nodedown, Node}, State) ->
    handle_info({nodeup, Node}, State);

handle_info({nodeup, Node}, State) ->
    Topics = common_lib:topics(),
    lists:foreach(fun(Topic) ->
        Nodes = common_lib:nodes(Topic),
        case lists:member(Node, Nodes) of 
            true ->
                gen_server:cast(gproc:where({n, l, {<<"erl_consumer">>, common_lib:to_binary(Topic)}}), node_changed);
            false ->
                nop
        end
    end, Topics),
    {noreply, State};

handle_info(world, _State) ->
    TimeRef = erlang:send_after(60000, self(), world),
    common_lib:world(),
    {noreply, TimeRef};

handle_info({erl_consumer, kafka_brokers, _OldValue, _NewValue}, TimeRef) ->
    erlang:cancel_timer(TimeRef),
    lists:foreach(fun({Topic, _, _, _}) ->
                cons_group_sup:close_child(Topic)
        end, supervisor:which_children(cons_group_sup)),
    handle_info(timeout, TimeRef);

handle_info({erl_consumer, cluster_info, _OldValue, _NewValue}, TimeRef) ->
    erlang:cancel_timer(TimeRef),
    Topics = common_lib:topics(),
    lists:foreach(fun({Topic, _, _, _}) ->
                case lists:member(Topic, Topics) of 
                    true ->
                        nop;
                    false ->
                        cons_group_sup:close_child(Topic)
                end end, supervisor:which_children(cons_group_sup)),
     handle_info(timeout, TimeRef);

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
sub_file_changing() ->
    mconfig_server:sub(erl_consumer, kafka_brokers, self()),
    mconfig_server:sub(erl_consumer, cluster_info, self()).
