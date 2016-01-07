-module(common_lib).

-export([
    notice/1,
    to_list/1,
    to_binary/1,
    to_atom/1,
    world/0,
    nodes/1,
    piece/2,
    topics/0,
    nodes_online/1,
    nodename_prefix/0
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec nodename_prefix() -> list().
nodename_prefix() ->
    Node   = atom_to_list(node()),
    [Prefix, _] = string:tokens(Node, "@"),
    Prefix.

-spec world() -> ok.
world() ->
    Prefix = ?MODULE:nodename_prefix(),
    ClusterInfo = config:cluster_info(),
    lists:foreach(fun({_, NodeList}) ->
        lists:foreach(fun(Ip) ->
            net_adm:ping(list_to_atom(Prefix ++ "@" ++ 
                    ?MODULE:to_list(Ip)))
        end, NodeList)
    end, ClusterInfo),
    ok.

-spec nodes(binary()) -> list().
nodes(Topic) ->
    Prefix = ?MODULE:nodename_prefix(),
    ClusterInfo = config:cluster_info(),
    Nodes = proplists:get_value(Topic, ClusterInfo, []),
    lists:map(fun(Node) ->
        list_to_atom(Prefix ++ "@" ++ ?MODULE:to_list(Node))
    end, Nodes).

-spec nodes_online(binary()) -> list().
nodes_online(Topic) ->
    Nodes = ?MODULE:nodes(Topic),
    lists:foldl(fun(Node, Pre) ->
        case lists:member(Node, Nodes) of 
            true ->
                [Node|Pre];
            false ->
                Pre
        end
    end, [], [node()|nodes()]).

-spec topics() -> list().
topics() ->
    ClusterInfo = config:cluster_info(),
    lists:map(fun({Topic, _}) ->
        Topic
    end, ClusterInfo).

-spec notice(term()) -> term().
notice(Msg) ->
    lager:notice("~p~n", [Msg]).

piece(PLen, 0) ->
    PLen;
piece(PLen, NLen) ->
    Val = PLen/NLen,
    Round = erlang:round(Val),
    case Val > Round of 
        true ->
            Round + 1;
        false ->
            Round
    end.

to_list(B) when is_list(B) ->
    B;
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(B) when is_atom(B) ->
    atom_to_list(B).

to_binary(B) when is_binary(B) ->
    B;
to_binary(B) when is_list(B) ->
    list_to_binary(B);
to_binary(B) when is_atom(B) ->
    atom_to_binary(B, utf8).

to_atom(B) when is_atom(B) ->
    B;
to_atom(B) when is_binary(B) ->
    binary_to_atom(B, utf8);
to_atom(B) when is_list(B) ->
    list_to_binary(B).

-ifdef(TEST).
-endif.
