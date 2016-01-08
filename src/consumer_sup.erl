-module(consumer_sup).
-behaviour(supervisor).

-export([
    start_link/0,
    start_child/1,
    close_child/1,
    init/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Args) ->
    {_, _, GroupId, Topic, Partition} = Args,
    case supervisor:start_child(?MODULE, {{GroupId, Topic, Partition},
                {consumer, start_link, [Args]}, transient, 5000, worker, [consumer]}) of  
        {ok, _} ->
            ok; 
        {error, already_present} ->
            supervisor:delete_child(?MODULE, {GroupId, Topic, Partition}),
            start_child(Args);
        {error, {already_started, _}} ->
            ok; 
        {error, {{already_started, _}, _}} ->
            ok; 
        Other ->
            Other
    end.

close_child({_GroupId, _Topic, _Partition}=Arg) ->
    supervisor:terminate_child(?MODULE, Arg),
    supervisor:delete_child(?MODULE, Arg).

init([]) ->
    {ok, {{one_for_one, 5, 10}, []}}.
