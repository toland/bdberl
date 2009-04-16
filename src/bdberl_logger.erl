%% -------------------------------------------------------------------
%% @doc
%% SASL/OTP logger for BDB. Routes BDB errors/messages into SASL logger.
%%
%% @copyright 2008-9 The Hive.  All rights reserved.
%% @end
%% -------------------------------------------------------------------
-module(bdberl_logger).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Start up the logger -- automatically initializes a port for this
    %% PID.
    bdberl:register_logger(),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {stop, unsupportedOperation, State}.

handle_cast(_Msg, State) ->
    {stop, unsupportedOperation, State}.

handle_info({bdb_error_log, Msg}, State) ->
    error_logger:error_msg("BDB Error: ~s\n", [Msg]),
    {noreply, State};

handle_info({bdb_info_log, Msg}, State) ->
    error_logger:info_msg("BDB Info: ~s\n", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

