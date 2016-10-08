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
-export([stop/0]).

-export([start_link/1,
         add_entry/2,
         remove_entry/2,
         get_data/1,
         merge/2,
         reset/1]).

%% For debugging:
-export([dump/0]).
-export([crash/0]).

%%--------------------------------------------------------------------
%% Internal exports
%%--------------------------------------------------------------------
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-export([terminate/2, code_change/3]).

%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Definitions
%%--------------------------------------------------------------------
-define(SERVER, ?MODULE).
-define(NAME(Id), list_to_atom(atom_to_list(Id) ++ "_worker")).
-define(VV_START_VERSION, 1).
-define(ENTRY_START_VERSION, 1).
-define(TIMEOUT, infinity). %% milliseconds | infinity
-define(TABLE_NAME(Id), list_to_atom(atom_to_list(Id) ++ "_table").
%% -type version_vector() :: [tuple()].
-type version_vector() :: map().
%% -type entry() :: binary().

%%--------------------------------------------------------------------
%% Records
%%--------------------------------------------------------------------
-record(state, {id :: integer(),
                nodes :: [atom()],
                tid :: integer(),
                version_vector :: version_vector()}).

%%====================================================================
%% API
%%====================================================================
-spec start_link(Args) -> Result when
      Args   :: term(),
      Result :: {ok, pid()} | ignore | {error, Reason},
      Reason :: {already_started, pid()} | term().
start_link(Args) ->
    Id = proplists:get_value(id, Args),
    %% put(id, Id),
    gen_server:start_link({local, Id}, ?MODULE, Args, []).

add_entry(Entry, Node) ->
    call(Node, {add, Entry}).

remove_entry(Entry, Node) ->
    call(Node, {remove, Entry}).

merge(ThisNode, ThatNode) ->
    ThatData = call(ThatNode, get_data),
    io:format("ThatData: ~p~n", [ThatData]),
    call(ThisNode, {merge, ThatData}).

get_data(Node) ->
    %% gen_server:call(get(id), get_data).
    call(Node, get_data).

reset(Node) ->
    call(Node, reset).

%%--------------------------------------------------------------------
%% @doc Start the server.
%% @end
%%--------------------------------------------------------------------
%% -spec start_link() -> Result when
%%       Result :: {ok, pid()} | ignore | {error, Reason},
%%       Reason :: {already_started, pid()} | term().
%% start_link() ->
%%     gen_server:start_link({local, ?SERVER}, ?MODULE, {}, []).

%%--------------------------------------------------------------------
%% @doc Stop the server.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
    MRef = erlang:monitor(process, ?SERVER),
    Reply = call(?SERVER, stop),
    receive {'DOWN', MRef, _Type, _Object, _Info} -> Reply end.

%%--------------------------------------------------------------------
%% @doc Dump the server's internal state (for debugging purposes).
%% @end
%%--------------------------------------------------------------------
-spec dump() -> term().
dump() ->
    call(?SERVER, dump).

%%--------------------------------------------------------------------
%% @doc Crash the server (for debugging purposes).
%% @end
%%--------------------------------------------------------------------
-spec crash() -> no_return().
crash() ->
    call(?SERVER, crash).

%%====================================================================
%% Server functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc Initialize the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args) -> Result when
      Args    :: term(),
      Result  :: {ok, State} | {ok, State, Timeout} | {ok, State, hibernate} |
                 {stop, Reason} | ignore,
      State   :: term(),
      Timeout :: non_neg_integer(),
      Reason  :: term().
init([{id, Id}, {nodes, Nodes}]) when is_atom(Id) ->
    %% io:format("Id: ~p~n, self(): ~p~n", [Id, self()]),
    Tid = ets:new(?TABLE_NAME(Id)), [set]),
%% io:format("process_info: ~p~n", [process_info(self())]),
%% io:format("registered: ~p~n", [registered()]),
    %% true = erlang:register(?NAME(Id), self()),
%% NewMap = maps:new(),
VV = maps:new(),
%% maps:put(Id, ?VV_START_VERSION, NewMap),
    {ok,
#state{id=Id, nodes=Nodes, tid=Tid,
 version_vector=VV
%% [{Id, ?VV_START_VERSION}]

}}.

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
    io:format("State: ~p~n", [State]),
    NewVV = add(Entry, Id, Tid, VV, ets:lookup(Tid, Entry)),
    NewState = State#state{version_vector=NewVV},
    io:format("NewState: ~p~n", [NewState]),

    {reply, ok, NewState};
handle_call({remove, Entry}, _From,
            #state{tid=Tid}=State) ->
    remove(Entry, Tid, ets:lookup(Tid, Entry)),
    {reply, ok, State};
handle_call({merge, [{version_vector, OtherVV}, {entries, OtherEntries}]},
            _From,
            #state{version_vector=VV, tid=Tid}=State) ->
    Entries = maps:from_list(ets:tab2list(Tid)),
    io:format("Entries: ~p~n", [Entries]),
    NewVV = merge_int(OtherVV, OtherEntries, VV, Entries, Tid),
    %% {reply, ok, NewState};
    {reply, ok, State#state{version_vector=NewVV}};
handle_call(get_data, _From,
            #state{tid=Tid, version_vector=VV}=State) ->
    {reply, get_data_int(Tid, VV), State};
handle_call(reset, _From,
            #state{tid=Tid}=State) ->
    NewVV = reset_int(Tid),
    {reply, ok, State#state{version_vector=NewVV}};
%% handle_call(stop, _From, State) ->
%%     {stop, normal, ok, State};
%% handle_call(dump, _From, State) ->
%%     io:format("~p:~p: State=~n  ~p~n", [?MODULE, self(), State]),
%%     {reply, State, State};
%% handle_call(crash, From, _State) ->
%%     erlang:error({deliberately_crashed_from,From});
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
    io:format("Info: ~p~n", [_Info]),
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
    io:format("Id : ~p~n", [Id]),
    io:format("VersionVector: ~p~n", [VersionVector]),
    Version = maps:get(Id, VersionVector),
    true = ets:insert(Tid, {Entry, [{Id, Version+1}]}),
    maps:update_with(Id, fun(X) -> X+1 end, ?VV_START_VERSION, VersionVector);
add(_Entry, _Id, _Tid, VersionVector, _Existing) ->
    VersionVector.

remove(_Entry, _Tid, []) ->
    ok;
remove(Entry, Tid, _Existing) ->
    ets:delete(Tid, Entry).

%% Merge algotrithm
%% See figure 3 on page 7
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
    io:format("merge_int, both identical~n"),
    OtherVV;
merge_int(OtherVV, OtherEntries, OurVV, _OurEntries, Tid)
  when map_size(OurVV) == 0 ->
    %% case 2: we have no add/remove history, but they do
    io:format("merge_int, empty us~n"),
    io:format("OtherVV: ~p~n", [OtherVV]),
    io:format("OtherEntries: ~p~n", [OtherEntries]),
    ets:insert(Tid, maps:to_list(OtherEntries)),
    OtherVV;
merge_int(OtherVV, _OtherEntries, VV, Entries, _Tid)
  when map_size(OtherVV) == 0 ->
    %% case 3: they have no add/remove history, but we do
    io:format("merge_int, empty incoming~n"),
    io:format("Entries: ~p~n", [Entries]),
    io:format("ThisVV: ~p~n", [VV]),
    VV;
merge_int(OtherVV, OtherEntries, VV, Entries, Tid) ->
    %% case 4: both sides have different add/remove histories
    io:format("merge_int~n"),
    io:format("VV: ~p~n", [VV]),
    io:format("OtherVV: ~p~n", [OtherVV]),
    io:format("OtherEntries: ~p~n", [OtherEntries]),
    io:format("Entries: ~p~n", [Entries]),
    io:format("ThisVV: ~p~n", [VV]),

    OtherKeys = maps:keys(OtherEntries),

    %% merge using their keys
    process_data(Tid, VV, Entries, OtherKeys, OtherEntries),

    %% merge using keys we have that they don't
    Keys = maps:keys(Entries),
    DiffKeys = Keys -- OtherKeys,
    io:format("DiffKeys: ~p~n", [DiffKeys]),

    %% process_data(Tid, OtherVV, OtherEntries, DiffKeys, Entries),
    process_diff_entries(DiffKeys, Entries, OtherVV, Tid),

    NewVV = merge_version_vectors(VV, OtherVV),
    io:format("NewVV: ~p~n", [NewVV]),
    NewVV.

process_diff_entries(DiffKeys, Entries, OtherVV, Tid) ->
    lists:foreach(
      fun(Key) ->
              OurEntry =
                  [{Node, EntryVersion}] =
                  maps:get(Key, Entries),
              io:format("OurEntry: ~p~n", [OurEntry]),
              case maps:get(Node, OtherVV, nope) of
                  nope ->
                      %% They've never seen this node's data
                      %%
                      %% These are entries in subset M
                      ok;
                  OtherNodeVersion ->
                      %% This case tests for membership in the M' subset
                      %% based on the formula:
                      %% let M' = {(e, c, i) ∈ E \ B.E|c > B.v[i]}
                      io:format("OtherNodeVersion: ~p~n", [OtherNodeVersion]),
                      case EntryVersion > OtherNodeVersion of
                          true ->
                              %% We added it since we last merged
                              %%
                              %% These are entries in subset M'
                              ok;
                          false ->
                              %% They removed it since we last merged
                              %%
                              %% These are entries on the lesser-than
                              %% side of the inequality test for M'
                              ets:delete(Tid, Key)
                      end
              end
      end,
      DiffKeys).


merge_version_vectors(VV, OtherVV) ->
    %%
    %% Final stage of the merge algortihm:
    %% v := [max(v[0], B.v[0]), . . . , max(v[n], B.v[n])]
    %%
    Keys = maps:keys(VV),
    OtherKeys = maps:keys(OtherVV),
    NewVV1 = maps:from_list(compare_vv_values(Keys, VV, OtherVV)),
    io:format("NewVV1: ~p~n", [NewVV1]),

    DiffKeys = OtherKeys -- Keys,
    NewVV2 = maps:with(DiffKeys, OtherVV),
    io:format("NewVV2: ~p~n", [NewVV2]),
    maps:merge(NewVV1, NewVV2).

compare_vv_values(Keys, VV, OtherVV) ->
    lists:foldl(
      fun(Key, Acc) ->
              Val = maps:get(Key, VV),
              OtherVal = maps:get(Key, OtherVV, 0),
              case OtherVal > Val of
                  true ->
                      [{Key, OtherVal} | Acc];
                  false ->
                      [{Key, Val} | Acc]
              end
      end,
      [],
      Keys).


process_data(Tid, VV, Entries, OtherKeys, OtherEntries) ->
    lists:foreach (
      fun(OtherKey) ->
              OtherEntry =
                  [{OtherEntryNode, OtherEntryVersion}] =
                  maps:get(OtherKey, OtherEntries),
              io:format("OtherKey: ~p~n", [OtherKey]),
              io:format("OtherEntry: ~p~n", [OtherEntry]),

              %%
              %% Step 1: have we merged from this entry's node before?
              %%
              case maps:get(OtherEntryNode, VV, nope) of
                  nope ->
                      %% No, so just add the entry
                      %%
                      %% These are entries in subset M
                      %%
                      %% Note that we rely on the behavior of
                      %% the "update add (element e)"
                      %% algorithm to guarantee that the version vector check is
                      %% a reliable shortcut here
                      io:format("inserting new entry: ~p~n", [OtherEntry]),
                      ets:insert(Tid, {OtherKey, OtherEntry});
                  OurOtherNodeVersion ->
                      %%
                      %% Step 2: yes, now compare the entries
                      %%
                      OurEntry = maps:get(OtherKey, Entries, nope),
                      io:format("OurEntry: ~p~n", [OurEntry]),
                      case maps:get(OtherKey, Entries, no_entry) of
                          no_entry ->
                              %% We don't have an entry for this key
                              io:format("OtherEntryVersion: ~p~nOurNodeVersion: ~p~n", [OtherEntryVersion, OurOtherNodeVersion]),
                              case OtherEntryVersion > OurOtherNodeVersion of
                                  true ->
                                      %% Either it's new on their side, or
                                      %% we removed it since we last merged, but they have re-added it
                                      %%
                                      %% These are entries in subset M'
                                      io:format("inserting new or re-added entry: ~p~n", [OtherEntry]),
                                      ets:insert(Tid, [{OtherKey, OtherEntry}]);

                                  false ->
                                      %% We removed it since we last merged, and they haven't re-added it
                                      %%
                                      %% These are entries on the lesser-than
                                      %% side of the inequality test for M'
                                      ok
                              end;
                          OtherEntry ->
                              io:format("identical entry~n"),
                              %% We have an identical entry for this key
                              ok;
                          [{_Node, OurEntryVersion}] ->
                              %% Same node, but different version
                              %% We will update if their version is a later one
                              case OtherEntryVersion > OurEntryVersion of
                                  true ->
                                      io:format("inserting updated entry: ~p~n", [OtherEntry]),
                                      ets:insert(Tid, [{OtherKey, OtherEntry}]);
                                  false ->
                                      ok
                              end
                              %% [{_DifferentNode, _DifferentEntryVersion}] ->
                              %%     io:format("strange days indeed, DifferentNode: ~p~n", [_DifferentNode]),
                              %%     %% ?? we have this key mapped to a different node
                              %%     %% hmmm....
                              %%     %% TODO sort this out, i.e. support more that 2 nodes
                              %%     %% [{DifferentNode, DifferentEntryVersion} | Acc]
                              %%     ok
                      end
              end
      end,
      OtherKeys).

get_data_int(Tid, VV) ->
    io:format("Tid: ~p~ndata: ~p~n", [Tid, maps:from_list(ets:tab2list(Tid))]),
    [{version_vector, VV}, {entries, maps:from_list(ets:tab2list(Tid))}].

reset_int(Tid) ->
    ets:delete_all_objects(Tid),
    maps:new().
