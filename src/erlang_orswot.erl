-module(erlang_orswot).


-export([add_entry/2,
         remove_entry/2,
         get_data/1,
         get_entries/1,
         get_version_vector/1,
         merge/0,
         merge_nodes/2,
         reset_node/1]).

-include("../include/erlang_orswot.hrl").
-include_lib("proper/include/proper.hrl").

add_entry(Entry, Node) when is_atom(Entry) ->
    erlang_orswot_worker:add_entry(Entry, Node).

remove_entry(Entry, Node) when is_atom(Entry) ->
    erlang_orswot_worker:remove_entry(Entry, Node).

get_data(Node) ->
    erlang_orswot_worker:get_data(Node).

get_entries(Node) ->
    erlang_orswot_worker:get_entries(Node).

get_version_vector(Node) ->
    erlang_orswot_worker:get_version_vector(Node).

merge() ->
    [Node1, Node2, _Node3] = ?NODES,
    merge_nodes(Node1, Node2),
    ok.

merge_nodes(Node1, Node2) ->
    erlang_orswot_worker:merge(Node1, Node2),
    ok.

reset_node(Node) ->
    erlang_orswot_worker:reset(Node).

prop_add_entry_adds_entry() ->
    application:start(erlang_orswot),
    ?FORALL({Entry, Node}, {atom(), lists:nth(1, ?NODES)},
            begin
                ok = add_entry(Entry, Node),
                _Val = maps:get(Entry, get_entries(Node)),
                %% We would have gotten a badkey or badmap exception
                %% on the get, so if we're this far return true
                true
            end).

prop_remove_entry_removes_entry() ->
    application:start(erlang_orswot),
    ?FORALL({Entry, Node}, {atom(), lists:nth(1, ?NODES)},
            begin
                ok = add_entry(Entry, Node),
                Val = maps:get(Entry, get_entries(Node)),
                ok = remove_entry(Entry, Node),
                Res = (catch maps:get(Entry, get_entries(Node))),
                Val /= Res
            end).

prop_get_data() ->
    application:start(erlang_orswot),
    ?FORALL({Entry, Node}, {atom(), lists:nth(1, ?NODES)},
            begin
                ok = add_entry(Entry, Node),
                Entries = get_entries(Node),
                VV = get_version_vector(Node),
                #{version_vector := VV, entries := Entries} =
                    get_data(Node),
                true
            end).

prop_get_entries() ->
    application:start(erlang_orswot),
    ?FORALL({Entry, Node}, {atom(), lists:nth(1, ?NODES)},
            begin
                ok = add_entry(Entry, Node),
                Entries = get_entries(Node),
                #{version_vector := _VV, entries := Entries} =
                    get_data(Node),
                true
            end).

prop_get_version_vector() ->
    application:start(erlang_orswot),
    ?FORALL({Entry, Node}, {atom(), lists:nth(1, ?NODES)},
            begin
                ok = add_entry(Entry, Node),
                VV = get_version_vector(Node),
                #{version_vector := VV, entries := _Entries} =
                    get_data(Node),
                true
            end).

prop_reset_node() ->
    application:start(erlang_orswot),
    ?FORALL({Entry, Node}, {atom(), lists:nth(1, ?NODES)},
            begin
                VV = maps:new(),
                Entries = maps:new(),
                ok = add_entry(Entry, Node),
                reset_node(Node),
                #{version_vector := VV, entries := Entries} =
                    get_data(Node),
                true
            end).
