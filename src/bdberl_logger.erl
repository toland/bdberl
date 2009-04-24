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

%% Macro for setting a counter
-define(SNMP_SET(Key, Value), (snmp_generic:variable_set({Key, volatile}, Value))).

%% Macro for incrementing a counter
-define(SNMP_INC(Key), (snmp_generic:variable_inc({Key, volatile}, 1))).

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
    ok = bdberl:register_logger(),

    %% If SNMP is available, load our MIBs
    case is_snmp_running() of
        true ->
            load_mibs(['BDBERL-MIB']);
        false ->
            error_logger:warn_msg("SNMP is not running; bdberl Stats will not be published.\n")
    end,
    
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
    {noreply, State};

handle_info({bdb_checkpoint_stats, CheckpointSecs, ArchiveSecs, 0, 0}, State) ->
    case is_snmp_running() of
        true ->
            ?SNMP_INC(bdbCheckpoints),
            ?SNMP_SET(bdbCheckpointRuntimeSecs, CheckpointSecs),
            ?SNMP_SET(bdbArchiveRuntimeSecs, ArchiveSecs);
        false ->
            ok
    end,
    {noreply, State};

handle_info({bdb_checkpoint_stats, _CheckpointSecs, _ArchiveSecs, CheckpointRc, ArchiveRc}, State) ->
    error_logger:error_msg("BDB Checkpoint error: ~w ~w\n", [CheckpointRc, ArchiveRc]),
    {noreply, State};

handle_info({bdb_trickle_stats, ElapsedSecs, Pages, 0}, State) ->
    case is_snmp_running() of
        true ->
            ?SNMP_INC(bdbTrickleWrites),
            ?SNMP_SET(bdbTrickleRuntimeSecs, ElapsedSecs),
            ?SNMP_SET(bdbTricklePages, Pages);
        false ->
            ok
    end,
    {noreply, State};
handle_info({bdb_trickle_stats, _ElapsedSecs, _Pages, Rc}, State) ->
    error_logger:error_msg("BDB Trickle Write error: ~w\n", [Rc]),
    {noreply, State};

handle_info(Msg, State) ->
    io:format("Unexpected message: ~p\n", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ====================================================================
%% Internal functions
%% ====================================================================

is_snmp_running() ->
    whereis(snmpa_local_db) /= undefined.

%%
%% Take a list of MIB atoms and load them from priv/ directory, if they aren't already loaded
%%
load_mibs([]) ->
    ok;
load_mibs([Mib | Rest]) ->
    MibFile = filename:join([code:priv_dir(bdberl), "mibs", lists:concat([Mib, ".bin"])]),
    case snmpa:whereis_mib(Mib) of
        {ok, _} ->
            load_mibs(Rest);
        {error, not_found} ->
            ok = snmpa:load_mibs([MibFile]),
            load_mibs(Rest)
    end.
