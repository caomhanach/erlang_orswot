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
                    %% ok = add_entry(Entry1, Node1),
                    ok = add_entry(Entry2, Node2),
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

                    true = check_entries(VV1_Before, VV2_Before,
                                         Entries1_Before, Entries2_Before,
                                         Entries1_After, Entries2_After),

                    check_version_vectors(VV1_Before,
                                          VV2_Before,
                                          VV1_After,
                                          VV2_After)
                end)).

check_entries(OurVV_Before, TheirVV, Entries1_Before, Entries2_Before,
              Entries1_After, Entries2_After) ->

    %% Sanity check: their Entries should be unchanged
    true = Entries2_Before =:= Entries2_After,

    TheirEntryKeys = maps:keys(Entries2_Before),
    OurKeysAfter = maps:keys(Entries1_After),
    OurDiffKeysAfter = OurKeysAfter -- TheirEntryKeys,

    %% First check their entries
    CheckTheirAcc =
        lists:foldl(
          fun(TheirEntryKey, OuterAcc) ->
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
                  %%    - if yes, did we have it before the merge
                  %%      - if yes, ensure we didn't have a higher version before the merge
                  %%      - if no, ensure their node version for the record
                  %%          was greater than our version vector version for the node
                  InnerAcc =
                      lists:foldl(
                        fun(TheirNodeRecordKey, Acc) ->
                                TheirNodeRecordVersion =
                                    maps:get(TheirNodeRecordKey, TheirEntryMap),
                                OurNodeVVVersionBefore =
                                    maps:get(TheirNodeRecordKey, OurVV_Before, 0),
                                case maps:get(TheirNodeRecordKey, OurEntryMapAfter, no_entry) of
                                    no_entry ->
                                        case TheirNodeRecordVersion > OurNodeVVVersionBefore of
                                            true ->
                                                %% Record was incorrectly omitted from subset M'
                                                [false | Acc];
                                            false ->
                                                %% Record was correctly omitted from subset M'
                                                Acc
                                        end;
                                    _OurNodeRecordVersionAfter ->
                                        case maps:is_key(TheirNodeRecordKey, OurEntryMapBefore) of
                                            true ->
                                                %% We had this record before; compare record versions
                                                OurNodeRecordVersionBefore =
                                                    maps:get(TheirNodeRecordKey, OurEntryMapBefore),
                                                case TheirNodeRecordVersion >= OurNodeRecordVersionBefore of
                                                    true ->
                                                        %% Record was correctly added to subset O
                                                        Acc;
                                                    false ->
                                                        %% Record was incorrectly added to subset O
                                                        [false | Acc]
                                                end;

                                            false ->
                                                %% We didn't have this entry before; use VV version if present
                                                OurNodeVVVersionBefore =
                                                    maps:get(TheirNodeRecordKey, OurVV_Before, 0),
                                                case TheirNodeRecordVersion > OurNodeVVVersionBefore of
                                                    true ->
                                                        %% Record was correctly added to subset O
                                                        Acc;
                                                    false ->
                                                        %% Record was incorrectly added to subset O
                                                        [false | Acc]
                                                end
                                        end
                                end
                        end,
                        [],
                        maps:keys(TheirEntryMap)),
                  lists:append(InnerAcc, OuterAcc)
          end,
          [],
          TheirEntryKeys),
    false = lists:member(false, CheckTheirAcc),

    %% Next check entries we have after the merge that they don't
    CheckOurAcc =
        lists:foldl(
          fun(OurDiffEntryKey, OuterDiffAcc) ->
                  OurEntryMapBefore =
                      maps:from_list(maps:get(OurDiffEntryKey, Entries1_Before, [])),

                  %% Inner loop
                  %% Check record entries for entries we have after that they don't
                  %% for each record entry:
                  %%   - ensure our node version for the record was greater
                  %%     than their version vector version for the node
                  InnerDiffAcc =
                      lists:foldl(
                        fun(OurNodeRecordKey, InnerAcc) ->
                                OurNodeRecordVersion =
                                    maps:get(OurNodeRecordKey, OurEntryMapBefore),
                                TheirNodeVVVersion =
                                    maps:get(OurNodeRecordKey, TheirVV, 0),
                                case OurNodeRecordVersion > TheirNodeVVVersion of
                                    true ->
                                        %% correctly added to subset M'
                                        InnerAcc;
                                    false ->
                                        %% incorrectly added to subset M'
                                        [false | InnerAcc]
                                end
                        end,
                        [],
                        maps:keys(OurEntryMapBefore)),
                  lists:append(InnerDiffAcc, OuterDiffAcc)
          end,
          [],
          OurDiffKeysAfter),

    false =:= lists:member(false, CheckOurAcc).

check_version_vectors(VV1_Before,
                      VV2_Before,
                      VV1_After,
                      VV2_After) ->

    %% Sanity check: their VV should be unchanged
    true = VV2_Before =:= VV2_After,

    %% VV1_After should follow this spec:
    %% v := [max(v[0], B.v[0]), . . . , max(v[n], B.v[n])]

    TheirKeys = maps:keys(VV2_Before),
    BoolAcc =
        lists:foldl(
          fun(TheirKey, Acc) ->
                  TheirVal = maps:get(TheirKey, VV2_Before),
                  OurValBefore =
                      case maps:is_key(TheirKey, VV1_Before) of
                          true ->
                              maps:get(TheirKey, VV1_Before);
                          false ->
                              0
                      end,
                  OurValAfter = maps:get(TheirKey, VV1_After),

                  case TheirVal > OurValBefore of
                      true ->
                          case OurValAfter =:= TheirVal of
                              true ->
                                  Acc;
                              false ->
                                  [false | Acc]
                          end;
                      false ->
                          case OurValAfter =:= OurValBefore of
                              true ->
                                  Acc;
                              false ->
                                  [false | Acc]
                          end
                  end
          end,
          [],
          TheirKeys),

    case lists:member(false, BoolAcc) of
        true ->
            false;
        false ->
            %% Last, ensure that VV entries we had before that
            %% they didn't have are unchanged after the merge
            OurDiffMapBefore = maps:without(TheirKeys, VV1_Before),
            OurDiffMapAfter = maps:without(TheirKeys, VV1_After),

            OurDiffMapBefore =:= OurDiffMapAfter
    end.
