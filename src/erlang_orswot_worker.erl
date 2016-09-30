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
	 merge/2]).

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
-define(VV_START_VERSION, 0).
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
    call(ThisNode, {merge, ThatData}).

get_data(Node) ->
    %% gen_server:call(get(id), get_data).
    call(Node, get_data).


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
NewMap = maps:new(),
VV = maps:put(Id, ?VV_START_VERSION, NewMap),
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
handle_call({add, Entry}, _From, #state{id=Id, tid=Tid, version_vector=VersionVector}=State) ->
    io:format("State: ~p~n", [State]),
    NewVersionVector =
	add(Entry, Id, Tid, VersionVector, ets:lookup(Tid, Entry)),
    NewState = State#state{version_vector=NewVersionVector},
    io:format("NewState: ~p~n", [NewState]),

    {reply, ok, NewState};
handle_call({remove, Entry}, _From, #state{tid=Tid}=State) ->
    true = remove(Entry, Tid, ets:lookup(Tid, Entry)),
    {reply, ok, State};
handle_call({merge, ThatData}, _From,
	    #state{version_vector=VV, tid=Tid}=State) ->
    NewState = merge_int(ThatData, VV, Tid),
    %% {reply, ok, NewState};
    {reply, ok, State};
handle_call(get_data, _From,
	    #state{tid=Tid, version_vector=VV}=State) ->
    {reply, get_data_int(Tid, VV), State};
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

add(Entry, Id, Tid, VersionVector, []) ->
    true = ets:insert(Tid, {Entry, [{Id, ?ENTRY_START_VERSION}]}),
    maps:update(Id, maps:get(Id, VersionVector) +1, VersionVector);
add(_Entry, _Id, _Tid, VersionVector, _Existing) ->
    VersionVector.

remove(_Entry, _Tid, []) ->
    true;
remove(Entry, Tid, _Existing) ->
    ets:delete(Tid, Entry).

merge_int([{version_vector, OtherVV}, {entries, OtherEntries}],
	  VV, Tid) ->
    io:format("merge_int~nOtherVV: ~p~n", [OtherVV]),
    io:format("OtherEntries: ~p~n", [OtherEntries]),
    Entries = maps:from_list(ets:tab2list(Tid)),
    io:format("Entries: ~p~n", [Entries]),
    io:format("ThisVV: ~p~nThisTid: ~p~n", [VV, Tid]),
    ok.

get_data_int(Tid, VV) ->
    io:format("Tid: ~p~ndata: ~p~n", [Tid, ets:tab2list(Tid)]),
    [{version_vector, VV}, {entries, ets:tab2list(Tid)}].
