-module(group_sup).
-behaviour(supervisor).

-export([
    start_link/0,
    start_child/1,
    init/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Topic) ->
    case supervisor:start_child(?MODULE, {Topic,
                {group, start_link, [Topic]}, 
                transient, 5000, worker, [group]}) of 
        {ok, _} ->
            ok;
        {error, already_present} ->
            supervisor:delete_child(?MODULE, Topic),
            start_child(Topic);
        {error, {already_started, _}} ->
            ok;
        {error, {{already_started, _}, _}} ->
            ok; 
        Other ->
            Other
    end.

init([]) ->
    {ok, { {one_for_one, 5, 10}, []} }.
