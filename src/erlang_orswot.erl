-module(erlang_orswot).

-export([add_entry/2,
         remove_entry/2,
         get_data/1,
         get_entries/1,
         get_version_vector/1,
         merge/0,
         merge_nodes/2,
         reset_node/1]).

%% for testing
-export([erlang_orswot_test/0,
         erlang_orswot_test/1]).

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
    [Node1, Node2 | _T] = ?NODES,
    merge_nodes(Node1, Node2),
    ok.

merge_nodes(Node1, Node2) ->
    erlang_orswot_worker:merge(Node1, Node2),
    ok.

reset_node(Node) ->
    erlang_orswot_worker:reset(Node).

erlang_orswot_test() ->
    proper:module(erlang_orswot).
erlang_orswot_test(UserOpts) ->
    proper:module(erlang_orswot, UserOpts).

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

prop_reset_node_resets_node() ->
    application:start(erlang_orswot),
    ?FORALL({Entry, Node}, {atom(), lists:nth(1, ?NODES)},
            begin
                VV = maps:new(),
                Entries = maps:new(),
                ok = add_entry(Entry, Node),
                ok = reset_node(Node),
                #{version_vector := VV, entries := Entries} =
                    get_data(Node),
                true
            end).

prop_merge_nodes_no_data() ->
    %% @see erlang_orswot_worker:merge_int
    %% case 1: neither side has add/remove history
    application:stop(erlang_orswot),
    ok = application:start(erlang_orswot),
    ?FORALL({Node1, Node2},
            {lists:nth(1, ?NODES), lists:nth(2, ?NODES)},
            begin
                ok = merge_nodes(Node1, Node2),
                get_data(Node1) =:= get_data(Node2)
            end).

prop_merge_nodes_identical_data() ->
    %% @see erlang_orswot_worker:merge_int
    %% case 1: our data is identical to their data
    application:stop(erlang_orswot),
    ok = application:start(erlang_orswot),
    ?FORALL(
       {Entry, Node1, Node2},
       {atom(), lists:nth(1, ?NODES), lists:nth(2, ?NODES)},
       begin
           ok = add_entry(Entry, Node1),
           ok = add_entry(Entry, Node2),
           ok = merge_nodes(Node1, Node2),
           ok = merge_nodes(Node2, Node1),
           get_data(Node1) =:= get_data(Node2)
       end).

prop_merge_nodes_only_their_node_has_data() ->
    %% @see erlang_orswot_worker:merge_int
    %% case 2: we have no add/remove history, but they do
    application:stop(erlang_orswot),
    ok = application:start(erlang_orswot),
    ?FORALL(
       {Entry, Node1, Node2},
       {atom(), lists:nth(1, ?NODES), lists:nth(2, ?NODES)},
       begin
           ok = reset_node(Node1),
           ok = add_entry(Entry, Node2),
           Data = get_data(Node2),
           ok = merge_nodes(Node1, Node2),
           get_data(Node1) =:= Data
               andalso get_data(Node2) =:= Data
       end).

prop_merge_nodes_only_our_node_has_data() ->
    %% @see erlang_orswot_worker:merge_int
    %% case 3: they have no add/remove history, but we do
    application:stop(erlang_orswot),
    ok = application:start(erlang_orswot),
    ?FORALL(
       {Entry, EmptyMap, Node1, Node2},
       {atom(), #{}, lists:nth(1, ?NODES), lists:nth(2, ?NODES)},
       begin
           ok = reset_node(Node2),
           ok = add_entry(Entry, Node1),
           Data = get_data(Node1),
           ok = merge_nodes(Node1, Node2),
           get_data(Node1) =:= Data andalso
               get_data(Node2) =:=
               #{version_vector => EmptyMap, entries => EmptyMap}
       end).

prop_merge_nodes_different_data() ->
    %% @see erlang_orswot_worker:merge_int
    %% case 4: both sides have different add/remove histories
    application:stop(erlang_orswot),
    ok = application:start(erlang_orswot),
    ?FORALL(
       {Entry1, Entry2, Node1, Node2},
       {atom(), atom(),
        lists:nth(1, ?NODES), lists:nth(2, ?NODES)},
       ?IMPLIES(Entry1 /= Entry2,
                begin
                    ok = add_entry(Entry1, Node1),
                    ok = add_entry(Entry2, Node2),
                    true = merge_and_check_data(Node1, Node2)
                end)).

prop_merge_nodes_we_delete_entries() ->
    %% @see erlang_orswot_worker:merge_int
    %% case 4: both sides have different add/remove histories
    %% subcase:
    %% 1. they add an entry
    %% 2. we merge and pick up their entry
    %% 3. we delete the entry
    %% 4. we merge again - we still shouldn't have the deleted entry
    application:stop(erlang_orswot),
    ok = application:start(erlang_orswot),
    ?FORALL(
       {Entry1, Entry2, Node1, Node2},
       {atom(), atom(),
        lists:nth(1, ?NODES), lists:nth(2, ?NODES)},
       ?IMPLIES(Entry1 /= Entry2,
                begin
                    ok = add_entry(Entry1, Node1),
                    ok = add_entry(Entry2, Node2),

                    ok = merge_nodes(Node1, Node2),

                    ok = remove_entry(Entry2, Node1),

                    true = merge_and_check_data(Node1, Node2)
                end
               )
      ).

prop_merge_nodes_they_delete_entries() ->
    %% @see erlang_orswot_worker:merge_int
    %% case 4: both sides have different add/remove histories
    %% subcase:
    %% 1. they add an entry
    %% 2. we merge and pick up their entry
    %% 3. they delete the entry
    %% 4. we merge again - we should have removed the entry,
    %%    unless we had records for it from one or more other nodes
    application:stop(erlang_orswot),
    ok = application:start(erlang_orswot),
    ?FORALL(
       {Entry1, Entry2, Node1, Node2},
       {atom(), atom(),
        lists:nth(1, ?NODES), lists:nth(2, ?NODES)},
       ?IMPLIES(Entry1 /= Entry2,
                begin
                    ok = add_entry(Entry1, Node1),
                    ok = add_entry(Entry2, Node2),

                    ok = merge_nodes(Node1, Node2),

                    ok = remove_entry(Entry2, Node2),

                    true = merge_and_check_data(Node1, Node2)
                end
               )
      ).

prop_merge_multiple_nodes_different_data() ->
    %% @see erlang_orswot_worker:merge_int
    %% case 4: both sides have different add/remove histories
    application:stop(erlang_orswot),
    ok = application:start(erlang_orswot),
    Pairs = pair(?NODES),
    compare_nodes(Pairs),
    compare_nodes([{B, A} || {A, B} <- Pairs]).

compare_nodes([]) ->
    true;
compare_nodes([H | T]) ->
    proper:quickcheck(proper:numtests(100, compare_node_pair(H))),
    compare_nodes(T).

compare_node_pair({Node1, Node2}) ->
    ?FORALL(
       {Entry1, Entry2},
       {atom(), atom()},
       ?IMPLIES(Entry1 /= Entry2,
                begin
                    %% mix things up a bit
                    ok = add_entry(Entry1, Node1),
                    ok = remove_entry(Entry1, Node1),
                    ok = add_entry(Entry1, Node1),
                    ok = add_entry(Entry1, Node2),
                    ok = add_entry(Entry2, Node2),
                    true = merge_and_check_data(Node1, Node2)
                end)).

merge_and_check_data(Node1, Node2) ->
    #{version_vector := VV1_Before,
      entries := Entries1_Before} =
        get_data(Node1),
    #{version_vector := VV2_Before,
      entries := Entries2_Before} =
        get_data(Node2),

    ok = merge_nodes(Node1, Node2),

    #{version_vector := VV1_After,
      entries := Entries1_After} =
        get_data(Node1),
    #{version_vector := VV2_After,
      entries := Entries2_After} =
        get_data(Node2),

    ok = check_entries(
           VV1_Before, VV2_Before,
           Entries1_Before, Entries2_Before,
           Entries1_After, Entries2_After),

    check_version_vectors(VV1_Before,
                          VV2_Before,
                          VV1_After,
                          VV2_After).

check_entries(OurVV_Before, TheirVV, Entries1_Before, Entries2_Before,
              Entries1_After, Entries2_After) ->

    %% Sanity check: their Entries should be unchanged
    true = Entries2_Before =:= Entries2_After,

    TheirEntryKeys = maps:keys(Entries2_Before),
    OurKeysAfter = maps:keys(Entries1_After),
    OurDiffKeysAfter = OurKeysAfter -- TheirEntryKeys,

    %% First check their entries
    check_their_entries(
      TheirEntryKeys, Entries1_Before, Entries1_After, Entries2_Before, OurVV_Before),

    %% Next check entries we have after the merge that they don't
    check_our_diff_entries(Entries1_Before, Entries1_After, TheirVV, OurDiffKeysAfter).

check_their_entries(TheirEntryKeys, Entries1_Before, Entries1_After, Entries2_Before, OurVV_Before) ->
    lists:foreach(
      fun(TheirEntryKey) ->
              TheirEntryMap =
                  maps:from_list(maps:get(TheirEntryKey, Entries2_Before)),
              OurEntryMapBefore =
                  maps:from_list(maps:get(TheirEntryKey, Entries1_Before, [])),
              OurEntryMapAfter =
                  maps:from_list(maps:get(TheirEntryKey, Entries1_After, [])),

              %% Inner loop
              %% Check their record keys in our current entry map
              %% for each of their records:
              %%  - do we have it after the merge?
              %%    - if no, ensure our version vector version for the node before the merge
              %%        was greater than their node version for their record
              %%    - if yes, did we have it before the merge,
              %%      - if yes, ensure we didn't have a higher version before the merge
              %%      - if no, ensure their node version for the record
              %%          was greater than our version vector version for the node
              check_their_records(TheirEntryMap, OurVV_Before, OurEntryMapAfter, OurEntryMapBefore)
      end,
      TheirEntryKeys).

check_their_records(TheirEntryMap, OurVV_Before, OurEntryMapAfter, OurEntryMapBefore) ->
    lists:foreach(
      fun(TheirNodeRecordKey) ->
              TheirNodeRecordVersion =
                  maps:get(TheirNodeRecordKey, TheirEntryMap),
              OurNodeRecordVersionAfter =
                  maps:get(TheirNodeRecordKey, OurEntryMapAfter, no_entry),
              OurNodeVVVersionBefore =
                  maps:get(TheirNodeRecordKey, OurVV_Before, 0),
              check_their_record(maps:get(TheirNodeRecordKey,
                                          OurEntryMapAfter,
                                          no_entry),
                                 TheirNodeRecordKey,
                                 TheirNodeRecordVersion,
                                 OurNodeRecordVersionAfter,
                                 OurNodeVVVersionBefore,
                                 OurEntryMapBefore)
      end,
      maps:keys(TheirEntryMap)).

%%
%% We don't have this record after the merge
%%
check_their_record(no_entry, _TheirNodeRecordKey, TheirNodeRecordVersion, no_entry, OurNodeVVVersionBefore, _OurEntryMapBefore)
  when TheirNodeRecordVersion > OurNodeVVVersionBefore ->
    %% Record was incorrectly omitted from subset M''
    erlang:error({record_incorrectly_omitted_from_subset_m_prime,
                  {TheirNodeRecordVersion, OurNodeVVVersionBefore}});
check_their_record(no_entry, _TheirNodeRecordKey, _TheirNodeRecordVersion, no_entry, _OurNodeVVVersionBefore, _OurEntryMapBefore) ->
    %% Record was correctly omitted from subset M''
    ok;

%%
%% We do have this record after the merge
%%
check_their_record(_OurNodeRecordVersionAfter, TheirNodeRecordKey, TheirNodeRecordVersion,
                   OurNodeRecordVersionAfter, OurNodeVVVersionBefore, OurEntryMapBefore) ->
    check_their_record_in(
      maps:get(TheirNodeRecordKey, OurEntryMapBefore, no_entry),
      TheirNodeRecordVersion,
      OurNodeRecordVersionAfter,
      OurNodeVVVersionBefore).

%% We have it now but didn't have this entry before;
%% use VV version if present
check_their_record_in(no_entry,
                      TheirNodeRecordVersion,
                      _OurNodeRecordVersionAfter,
                      OurNodeVVVersionBefore)
  when TheirNodeRecordVersion > OurNodeVVVersionBefore ->
    %% Record was correctly added to subset M''
    ok;
check_their_record_in(no_entry,
                      TheirNodeRecordVersion,
                      _OurNodeRecordVersionAfter,
                      OurNodeVVVersionBefore) ->
    %% Record was incorrectly omitted from subset M''
    erlang:error({record_incorrectly_added_to_subset_o,
                  {TheirNodeRecordVersion, OurNodeVVVersionBefore}});

%% We had this record before; compare record versions
check_their_record_in(OurNodeRecordVersionBefore,
                      TheirNodeRecordVersion,
                      TheirNodeRecordVersion,
                      _OurNodeVVVersionBefore)
  when TheirNodeRecordVersion >= OurNodeRecordVersionBefore ->
    %% Record was correctly omitted from subset O (>),
    %% or we had an identical entry (=)
    %% Also validates that we have their node record version after the merge
    ok;
check_their_record_in(OurNodeRecordVersionBefore,
                      TheirNodeRecordVersion,
                      OurNodeRecordVersionAfter,
                      _OurNodeVVVersionBefore)
  when TheirNodeRecordVersion >= OurNodeRecordVersionBefore ->
    %% Record was correctly omitted from subset O (>),
    %% or we had an identical entry (=),
    %% BUT node record versions don't match after the merge
    erlang:error(different_node_record_versions_mismatch,
                 {TheirNodeRecordVersion, OurNodeRecordVersionAfter});
check_their_record_in(_OurNodeRecordVersionBefore,
                      TheirNodeRecordVersion,
                      TheirNodeRecordVersion,
                      _OurNodeVVVersionBefore) ->
    %% Record was incorrectly omitted from subset O
    %% We have it with their version after the merge
    erlang:error(identical_node_record_versions_mismatch,
                 {TheirNodeRecordVersion, TheirNodeRecordVersion});
check_their_record_in(_OurNodeRecordVersionBefore,
                      _TheirNodeRecordVersion,
                      _OurNodeRecordVersionAfter,
                      _OurNodeVVVersionBefore) ->
    %% Their version was less than ours before, and we still have a
    %% different version after the merge
    ok.

check_our_diff_entries(Entries1_Before, Entries1_After, TheirVV, OurDiffKeysAfter) ->
    lists:foreach(
      fun(OurDiffEntryKey) ->
              OurEntryMapBefore =
                  maps:from_list(maps:get(OurDiffEntryKey, Entries1_Before, [])),

              %% Inner loop
              %% Check record entries for entries we have after that they don't
              %% for each record entry:
              %%   - ensure our node version for the record was greater
              %%     than their version vector version for the node;
              %%   - or else that we have other node records for this entry,
              %%     and that's why we still have the entry.
              ok =
                  lists:foreach(
                    fun(OurNodeRecordKey) ->
                            OurNodeRecordVersion =
                                maps:get(OurNodeRecordKey, OurEntryMapBefore),
                            TheirNodeVVVersion =
                                maps:get(OurNodeRecordKey, TheirVV, 0),
                            case OurNodeRecordVersion > TheirNodeVVVersion of
                                true ->
                                    %% correctly added to subset M'
                                    ok;
                                false ->
                                    case
                                        maps:get(OurDiffEntryKey, Entries1_After, [])
                                    of
                                        [] ->
                                            %% incorrectly added to subset M'
                                            erlang:error({bad_record_in_subset_m_prime,
                                                          {OurNodeRecordKey, OurNodeRecordVersion}});
                                        _OurEntryAfter ->
                                            ok
                                    end
                            end
                    end,
                    maps:keys(OurEntryMapBefore))
      end,
      OurDiffKeysAfter).

check_version_vectors(VV1_Before,
                      VV2_Before,
                      VV1_After,
                      VV2_After) ->

    %% Sanity check: their VV should be unchanged
    true = VV2_Before =:= VV2_After,

    %% VV1_After should follow this spec:
    %% v := [max(v[0], B.v[0]), . . . , max(v[n], B.v[n])]

    TheirKeys = maps:keys(VV2_Before),
    ok =
        lists:foreach(
          fun(TheirKey) ->
                  TheirVal = maps:get(TheirKey, VV2_Before),
                  OurValBefore = maps:get(TheirKey, VV1_Before, 0),
                  OurValAfter = maps:get(TheirKey, VV1_After),

                  case TheirVal > OurValBefore of
                      true ->
                          case OurValAfter =:= TheirVal of
                              true ->
                                  ok;
                              false ->
                                  erlang:error({bad_value_in_version_vector,
                                               TheirVal})
                          end;
                      false ->
                          case OurValAfter =:= OurValBefore of
                              true ->
                                  ok;
                              false ->
                                  erlang:error({bad_value_in_version_vector,
                                               TheirVal})
                          end
                  end
          end,
          TheirKeys),


    %% Last, ensure that VV entries we had before that
    %% they didn't have are unchanged after the merge
    OurDiffMapBefore = maps:without(TheirKeys, VV1_Before),
    OurDiffMapAfter = maps:without(TheirKeys, VV1_After),

    OurDiffMapBefore =:= OurDiffMapAfter.


pair(List) ->
    lists:flatten(pair_in(List, [])).

pair_in([], Acc) ->
    Acc;
pair_in([H | T], Acc) ->
    pair_in(T, [pair_node(H, T) | Acc]).

pair_node(Node, List) ->
    pair_node_in(Node, List, []).

pair_node_in(_Node, [], Acc) ->
    Acc;
pair_node_in(Node, [H | T], Acc) ->
    pair_node_in(Node, T, [{Node, H} | Acc]).
