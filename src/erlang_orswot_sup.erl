%%%-------------------------------------------------------------------
%% @doc erlang_orswot top level supervisor.
%% @end
%%%-------------------------------------------------------------------
-module(erlang_orswot_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-include("../include/erlang_orswot.hrl").


%%====================================================================
%% API functions
%%====================================================================
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================
init([]) ->
    ChildSpec =
        [#{id => Id,
           start => {erlang_orswot_worker,
                     start_link,
                     [[{id, Id}, {nodes, lists:delete(Id, ?NODES)}]]}} ||
            Id <- ?NODES],
    {ok, { #{strategy => one_for_all,
             intensity => 0,
             period => 1},
           ChildSpec
         }}.

%%====================================================================
%% Internal functions
%%====================================================================
