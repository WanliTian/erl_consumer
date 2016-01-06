-module(connection_sup).
-behaviour(supervisor).

-define(CHILD(Name, I, Type, Args), {Name, {I, start_link, [Args]}, transient, 5000, Type, [I]}).

-export([
    start_link/0,
    start_child/1,
    init/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Args) ->
    {_, _, GroupId, Topic, Partition} = Args,
    case supervisor:start_child(?MODULE, {{GroupId, Topic, Partition},
                {connection, start_link, [Args]}, transient, 5000, worker, [connection]}) of 
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

init([]) ->
    {ok, { {one_for_one, 5, 10}, []} }.
