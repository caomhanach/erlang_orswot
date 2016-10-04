-module(erlang_orswot).

-export([go/0]).

-export([add_entry/2,
         remove_entry/2,
	 get_data/1,
	 merge/0,
	 merge_nodes/2,
	 reset_node/1]).

-include("../include/erlang_orswot.hrl").

add_entry(Entry, Node) when is_atom(Entry) ->
    erlang_orswot_worker:add_entry(Entry, Node).

remove_entry(Entry, Node) when is_atom(Entry) ->
    erlang_orswot_worker:remove_entry(Entry, Node).

get_data(Node) ->
    erlang_orswot_worker:get_data(Node).

merge() ->
    [Node1, Node2] = ?NODES,
    merge_nodes(Node1, Node2),
    ok.

merge_nodes(Node1, Node2) ->
    erlang_orswot_worker:merge(Node1, Node2),
    ok.

reset_node(Node) ->
    erlang_orswot_worker:reset(Node).

go() ->
    %% ets:new(A,B),
    %% mnesia:create(),
    ok.
