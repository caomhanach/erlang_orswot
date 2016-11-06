%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(erlang_orswot_worker).

-behaviour(gen_server).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-export([start_link/1,
         add_entry/2,
         remove_entry/2,
         get_data/1,
         get_entries/1,
         get_version_vector/1,
         merge/2,
         reset/1]).

%%--------------------------------------------------------------------
%% Internal exports
%%--------------------------------------------------------------------
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-export([terminate/2, code_change/3]).

%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include_lib("proper/include/proper.hrl").
-include("../include/erlang_orswot.hrl").

%%--------------------------------------------------------------------
%% Definitions
%%--------------------------------------------------------------------
-define(SERVER, ?MODULE).
-define(NAME(Id), list_to_atom(atom_to_list(Id) ++ "_worker")).
-define(VV_START_VERSION, 1).
-define(ENTRY_START_VERSION, 1).
-define(TIMEOUT, infinity). %% milliseconds | infinity
-define(TABLE_NAME(Id), list_to_atom(atom_to_list(Id) ++ "_table").

%% -type version_vector() :: map().
-type version_vector() :: map().
-type entries() :: map().
-type node_data() :: #{version_vector := version_vector(),
                       entries := entries()}.
-type id_tuple() :: {id, atom()}.
-type nodes_tuple() :: {nodes, list()}.
-type init_args() :: [id_tuple() | nodes_tuple()].

%%--------------------------------------------------------------------
%% Records
%%--------------------------------------------------------------------
-record(state, {id :: atom(),
                nodes :: [atom()],
                tid :: ets:tid(),
                version_vector :: version_vector()}).

%%====================================================================
%% API
%%====================================================================
-spec start_link(Args) -> Result when
      Args   :: init_args() | term(),
      Result :: {ok, pid()} | ignore | {error, Reason},
      Reason :: {already_started, pid()} | term().
start_link([{id, Id}, {nodes, _Nodes}] = Args) ->
    gen_server:start_link({local, Id}, ?MODULE, Args, []);
start_link(Other) ->
    {error, {invalid_init_args, Other}}.


-spec add_entry(Entry, Node) -> Result when
      Entry  :: term(),
      Node   :: atom(),
      Result :: ok | {error, Reason},
      Reason :: term().
add_entry(Entry, Node) ->
    call(Node, {add, Entry}).

-spec remove_entry(Entry, Node) -> Result when
      Entry  :: term(),
      Node   :: atom(),
      Result :: ok | {error, Reason},
      Reason :: term().
remove_entry(Entry, Node) ->
    call(Node, {remove, Entry}).

-spec merge(ThisNode, ThatNode) -> Result when
      ThisNode :: atom(),
      ThatNode :: atom(),
      Result   :: ok | {error, Reason},
      Reason   :: term().
merge(ThisNode, ThatNode) ->
    ThatData = call(ThatNode, get_data),
    call(ThisNode, {merge, ThatData}).

-spec get_data(Node) -> Result when
      Node    :: atom(),
      Result  :: node_data() | {error, Reason},
      Reason  :: term().
get_data(Node) ->
    call(Node, get_data).

-spec get_entries(Node) -> Result when
      Node    :: atom(),
      Result  :: entries() | {error, Reason},
      Reason  :: term().
get_entries(Node) ->
    call(Node, get_entries).

-spec get_version_vector(Node) -> Result when
      Node    :: atom(),
      Result  :: entries() | {error, Reason},
      Reason  :: term().
get_version_vector(Node) ->
    call(Node, get_version_vector).

-spec reset(Node) -> Result when
      Node    :: atom(),
      Result  :: ok | {error, Reason},
      Reason  :: term().
reset(Node) ->
    call(Node, reset).


%%====================================================================
%% Server functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc Initialize the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args) -> Result when
      Args    :: init_args() | term(),
      Result  :: {ok, State} | {ok, State, Timeout} |
                 {ok, State, hibernate} | {stop, Reason} | ignore,
      State   :: term(),
      Timeout :: non_neg_integer(),
      Reason  :: term().
init([{id, Id}, {nodes, Nodes}]) when is_atom(Id) ->
    Tid = ets:new(?TABLE_NAME(Id)), [set]),
    VV = maps:new(),
    {ok,
     #state{id=Id, nodes=Nodes, tid=Tid,
            version_vector=VV}};
init(Other) ->
    {stop, {invalid_init_args, Other}}.

%%--------------------------------------------------------------------
%% @doc Handle call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request, From, State) -> Result when
      Request  :: term(),
      From     :: {pid(), Tag :: term()},
      State    :: term(),
      Result   :: {reply, Reply, NewState}            |
                  {reply, Reply, NewState, Timeout}   |
                  {reply, Reply, NewState, hibernate} |
                  {noreply, NewState}                 |
                  {noreply, NewState, Timeout}        |
                  {noreply, NewState, hibernate}      |
                  {stop, Reason, Reply, NewState}     |
                  {stop, Reason, NewState},
      Reply    :: term(),
      NewState :: term(),
      Timeout  :: non_neg_integer() | infinity,
      Reason   :: term().
handle_call({add, Entry}, _From,
            #state{id=Id, tid=Tid, version_vector=VV}=State) ->
    NewVV = add(Entry, Id, Tid, VV, ets:lookup(Tid, Entry)),
    NewState = State#state{version_vector=NewVV},
    {reply, ok, NewState};
handle_call({remove, Entry}, _From,
            #state{tid=Tid}=State) ->
    remove(Entry, Tid, ets:lookup(Tid, Entry)),
    {reply, ok, State};
handle_call({merge, #{version_vector := OtherVV,
                      entries := OtherEntries}},
            _From,
            #state{version_vector=VV, tid=Tid}=State) ->
    Entries = maps:from_list(ets:tab2list(Tid)),
    NewVV = merge_int(OtherVV, OtherEntries, VV, Entries, Tid),
    {reply, ok, State#state{version_vector=NewVV}};
handle_call(get_data, _From,
            #state{tid=Tid, version_vector=VV}=State) ->
    {reply, get_data_int(Tid, VV), State};
handle_call(get_entries, _From,
            #state{tid=Tid}=State) ->
    {reply, get_entries_int(Tid), State};
handle_call(get_version_vector, _From,
            #state{version_vector=VV}=State) ->
    {reply, get_vv_int(VV), State};
handle_call(reset, _From,
            #state{tid=Tid}=State) ->
    NewVV = reset_int(Tid),
    {reply, ok, State#state{version_vector=NewVV}};
handle_call(UnknownRequest, _From, State) ->
    {reply, {error, {bad_request, UnknownRequest}}, State}.

%%--------------------------------------------------------------------
%% @doc Handle cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request, State) -> Result when
      Request :: term(),
      State   :: term(),
      Result  :: {noreply, NewState}            |
                 {noreply, NewState, Timeout}   |
                 {noreply, NewState, hibernate} |
                 {stop, Reason, NewState},
      NewState :: term(),
      Timeout  :: non_neg_integer() | infinity,
      Reason   :: term().
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @doc Handle all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info, State) -> Result when
      Info    :: timeout | term(),
      State   :: term(),
      Result  :: {noreply, NewState}            |
                 {noreply, NewState, Timeout}   |
                 {noreply, NewState, hibernate} |
                 {stop, Reason, NewState},
      NewState :: term(),
      Timeout  :: non_neg_integer() | infinity,
      Reason   :: term().
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @doc Shutdown the server.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason, State) -> no_return() when
      Reason :: normal | shutdown | {shutdown,term()} | term(),
      State  :: term().
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @doc Convert process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn, State, Extra) -> Result when
      OldVsn   :: Vsn | {down, Vsn},
      Vsn      :: term(),
      State    :: term(),
      Extra    :: term(),
      Result   :: {ok, NewState} | {error, Reason},
      NewState :: term(),
      Reason   :: term().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

call(Pid, Msg) ->
    gen_server:call(Pid, Msg, ?TIMEOUT).

add(Entry, Id, Tid, VersionVector, []) when map_size(VersionVector) == 0 ->
    true = ets:insert(Tid, {Entry, [{Id, ?ENTRY_START_VERSION}]}),
    #{Id => ?VV_START_VERSION};
add(Entry, Id, Tid, VersionVector, []) ->
    Version = maps:get(Id, VersionVector, 0),
    true = ets:insert(Tid, {Entry, [{Id, Version+1}]}),
    maps:update_with(Id, fun(X) -> X+1 end, ?VV_START_VERSION, VersionVector);
add(_Entry, _Id, _Tid, VersionVector, _Existing) ->
    VersionVector.

remove(_Entry, _Tid, []) ->
    true;
remove(Entry, Tid, _Existing) ->
    true = ets:delete(Tid, Entry).

%% Merge algotrithm
%% See figure 3 on page 7 here:
%% https://arxiv.org/pdf/1210.3368v1.pdf
%%
%% merge (B)
%% let M = (E ∩ B.E)
%% let M' = {(e, c, i) ∈ E \ B.E|c > B.v[i]}
%% let M'' = {(e, c, i) ∈ B.E \ E|c > v[i]}
%% let U = M ∪ M' ∪ M''
%% let O = {(e, c, i) ∈ U|∃(e, c', i) ∈ U : c < c'}
%% E := U \ O
%% v := [max(v[0], B.v[0]), . . . , max(v[n], B.v[n])]
%%
merge_int(OtherVV, OtherEntries, OtherVV, OtherEntries, _Tid) ->
    %% case 1: our data is identical to their data
    %% This includes the case where neither side
    %% has add/remove history (everything is empty)
    OtherVV;
merge_int(OtherVV, OtherEntries, OurVV, _OurEntries, Tid)
  when map_size(OurVV) == 0 ->
    %% case 2: we have no add/remove history, but they do
    ets:insert(Tid, maps:to_list(OtherEntries)),
    OtherVV;
merge_int(OtherVV, _OtherEntries, VV, _Entries, _Tid)
  when map_size(OtherVV) == 0 ->
    %% case 3: they have no add/remove history, but we do
    VV;
merge_int(OtherVV, OtherEntries, OurVV, OurEntries, Tid) ->
    %% case 4: both sides have different add/remove histories
    OtherKeys = maps:keys(OtherEntries),

    %% First merge using their keys
    merge_their_keys(Tid, OurEntries, OurVV, OtherEntries),

    %% Next merge using keys we have that they don't
    Keys = maps:keys(OurEntries),
    DiffKeys = Keys -- OtherKeys,
    merge_diff_keys(Tid, DiffKeys, OurEntries, OtherVV),

    %% Finally merge version vectors
    merge_version_vectors(OurVV, OtherVV).

merge_their_keys(Tid, OurEntries, OurVV, TheirEntries) ->
    %% Outer loop: each entry in their db
    lists:foreach(
      fun(TheirEntryKey) ->
              handle_their_entry_key(TheirEntryKey,
                                     maps:from_list(maps:get(TheirEntryKey, OurEntries, [])),
                                     maps:from_list(maps:get(TheirEntryKey, TheirEntries)),
                                     OurVV, Tid)
      end,
      maps:keys(TheirEntries)).

handle_their_entry_key(TheirEntryKey, OurEntryMap, TheirEntryMap, OurVV, Tid) ->
    %% Inner loop: each {node, version} record for this entry in TheirEntryMap
    EntryAccMap =
        lists:foldl(
          fun(TheirNodeRecordKey, InnerAccMap) ->
                  check_record_entries(
                    maps:get(TheirNodeRecordKey, OurEntryMap, no_entry),
                    maps:get(TheirNodeRecordKey, TheirEntryMap),
                    TheirNodeRecordKey,
                    OurVV,
                    InnerAccMap)
          end,
          OurEntryMap,
          maps:keys(TheirEntryMap)),

    true = update_db_after_merge(EntryAccMap, TheirEntryKey, Tid).


check_record_entries(no_entry, TheirNodeRecordVersion, TheirNodeRecordKey, OurVV, Map) ->
    update_record_map_we_have_no_entry(TheirNodeRecordVersion,
                                       TheirNodeRecordKey,
                                       maps:get(TheirNodeRecordKey, OurVV, 0),
                                       Map);
check_record_entries(TheirNodeRecordVersion, TheirNodeRecordVersion, _TheirNodeRecordKey, _OurVV, Map) ->
    %% We have an identical entry for this key
    %% These are entries in subset M
    Map;
check_record_entries(OurNodeRecordVersion, TheirNodeRecordVersion, TheirNodeRecordKey, _OurVV, Map) ->
    %% Same node, but different version
    %% We will update if their version is a later one
    update_record_map_we_have_different_node_version(TheirNodeRecordKey,
                                                     TheirNodeRecordVersion,
                                                     OurNodeRecordVersion,
                                                     Map).

update_record_map_we_have_no_entry(TheirNodeRecordVersion, TheirNodeRecordKey, OurNodeVVVersion, Map)
  when TheirNodeRecordVersion > OurNodeVVVersion ->
    %% Either it's new on their side, or
    %% we removed it since we last merged, but they have re-added it
    %%
    %% These are entries in subset M'
    maps:put(TheirNodeRecordKey, TheirNodeRecordVersion, Map);
update_record_map_we_have_no_entry(_TheirNodeRecordVersion, _TheirNodeRecordKey, _OurNodeVVVersion, Map) ->
    %% We removed it since we last merged, and they haven't re-added it
    %%
    %% These are entries on the lesser-than-or-equal-to
    %% side of the inequality test for M'
    Map.

update_record_map_we_have_different_node_version(TheirNodeRecordKey,
                                          TheirNodeRecordVersion,
                                          OurNodeRecordVersion,
                                          Map)
  when TheirNodeRecordVersion > OurNodeRecordVersion ->
    %% added to subset O
    maps:put(TheirNodeRecordKey, TheirNodeRecordVersion, Map);
update_record_map_we_have_different_node_version(_TheirNodeRecordKey, _TheirNodeRecordVersion, _OurNodeRecordVersion, Map) ->
    %% excluded from subset O
    Map.

update_db_after_merge(Map, _Key, _Tid) when map_size(Map) =:= 0 ->
    %% This entry still doesn't exist in our db
    true;
update_db_after_merge(Map, Key, Tid) ->
    %% This is a new, updated, or identical entry in our db
    %% TODO avoid inserting identical entries
    ets:insert(Tid, {Key, maps:to_list(Map)}).

merge_diff_keys(Tid, DiffKeys, OurEntries, TheirVV) ->
    lists:foreach(
      fun(OurEntryKey) ->
              AccMap =
                  build_diff_key_map(
                    maps:from_list(maps:get(OurEntryKey, OurEntries)),
                    TheirVV),
              true = handle_updated_diff_key(OurEntryKey, AccMap, Tid)
      end,
      DiffKeys).

build_diff_key_map(OurEntryMap, TheirVV) ->
    lists:foldl(
      fun(OurNodeRecordKey, AccMap) ->
              compare_diff_versions(
                OurNodeRecordKey,
                maps:get(OurNodeRecordKey, OurEntryMap),
                maps:get(OurNodeRecordKey, TheirVV, 0),
                AccMap)
      end,
      maps:new(),
      maps:keys(OurEntryMap)).

compare_diff_versions(OurNodeRecordKey,
                      OurNodeRecordVersion,
                      TheirNodeVVVersion,
                      Map) when OurNodeRecordVersion > TheirNodeVVVersion ->
    %% belongs to subset M'
    maps:put(OurNodeRecordKey, OurNodeRecordVersion, Map);
compare_diff_versions(_OurKey, _Ours, _Theirs, Map) ->
    %% excluded from subset M'
    Map.

handle_updated_diff_key(Key, Map, Tid) when map_size(Map) =:= 0 ->
    ets:delete(Tid, Key);
handle_updated_diff_key(Key, Map, Tid) ->
    %% TODO avoid inserting identical entries
    ets:insert(Tid, {Key, maps:to_list(Map)}).

merge_version_vectors(VV, OtherVV) ->
    %%
    %% Final stage of the merge algortihm:
    %% v := [max(v[0], B.v[0]), . . . , max(v[n], B.v[n])]
    %%
    Keys = maps:keys(VV),
    OtherKeys = maps:keys(OtherVV),
    NewVV1 = maps:from_list(compare_vv_values(Keys, VV, OtherVV)),
    DiffKeys = OtherKeys -- Keys,
    NewVV2 = maps:with(DiffKeys, OtherVV),

    maps:merge(NewVV1, NewVV2).

compare_vv_values(Keys, VV, OtherVV) ->
    [compare_vv_value(Key,
                      maps:get(Key, VV),
                      maps:get(Key, OtherVV, 0)) ||
     Key <- Keys].

compare_vv_value(Key, Val, OtherVal) ->
    {Key, choose_vv_val(OtherVal, Val)}.

choose_vv_val(OtherVal, Val) when OtherVal > Val ->
    OtherVal;
choose_vv_val(_OtherVal, Val) ->
    Val.

get_data_int(Tid, VV) ->
    #{version_vector => VV, entries => maps:from_list(ets:tab2list(Tid))}.

get_entries_int(Tid) ->
    maps:from_list(ets:tab2list(Tid)).

get_vv_int(VV) ->
    VV.

reset_int(Tid) ->
    ets:delete_all_objects(Tid),
    maps:new().
