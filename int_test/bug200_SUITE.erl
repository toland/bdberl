%% -------------------------------------------------------------------
%%
%% bdberl: Port Driver Bug200 tests
%% Copyright (c) 2008 The Hive.  All rights reserved.
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
%% THE SOFTWARE.
%%
%% -------------------------------------------------------------------
-module(bug200_SUITE).

-compile(export_all).

-define(PROCS, 4).
%-define(DATA_SIZE, 2097152).
-define(DATA_SIZE, 10240).
-define(REQUESTS, 1000).

all() ->
    [bug200].

bug200(_Config) ->
    %% Make sure bdberl is not loaded
    
    %% Set the thread pools
    os:putenv("BDBERL_NUM_GENERAL_THREADS", "10"),
    os:putenv("BDBERL_NUM_TXN_THREADS",     "10"),
    os:putenv("BDBERL_DEADLOCK_CHECK_INTERVAL", "1"),

    %% Copy in the DB_CONFIG
    {ok, _} = file:copy("../../int_test/PROD_DB_CONFIG","DB_CONFIG"),
    
    crypto:start(),
    Data = crypto:rand_bytes(?DATA_SIZE),

    ct:print("Driver running with:~n~p~n", [bdberl:driver_info()]),

    %% Spin up 15 processes (async thread pool is 10)
    start_procs(?PROCS, Data),
    wait_for_finish(?PROCS).

start_procs(0,_) ->
    ok;
start_procs(Count, Data) ->
    spawn_link(?MODULE, bug200_run, [self(), Data]),
    start_procs(Count-1, Data).

wait_for_finish(0) ->
    ok;
wait_for_finish(Count) ->
    receive
        {finished, Pid} ->
            ct:print("~p is done; ~p remaining.\n", [Pid, Count-1]),
            wait_for_finish(Count-1)
    end.

bug200_run(Owner, Data) ->
    %% Seed the RNG
    {A1, A2, A3} = now(),
    random:seed(A1, A2, A3),

    %% Start bug200ing
    File = "bug200_" ++ integer_to_list(random:uniform(16)) ++ ".bdb",
    {ok, DbRef} = bdberl:open(File, btree,  [create, multiversion, read_uncommitted]),
    Reqs = ?REQUESTS div ?PROCS,
    ct:print("~p starting for ~p requests~n", [self(), Reqs]),
    bug200_incr_loop(Owner, DbRef, Data, Reqs).

bug200_incr_loop(Owner, DbRef, _DataBin, 0) ->
    bdberl:close(DbRef),
    Owner ! {finished, self()};
bug200_incr_loop(Owner, DbRef, DataBin, Count) ->
    if 
        (Count rem 10) =:= 0 ->
            ct:print("Pid ~p count ~p~n", [self(), Count]);
        true ->
            ok
    end,
            
    %% Choose random key
    Key = random:uniform(1200),

    %% Do a get on the key with read_uncommitted
    case bdberl:get(DbRef, Key,  [read_uncommitted]) of
        {error, Reason} ->
            ct:print("get error: ~p~n", [Reason]);
        _ ->
            ok
    end,

    %% Data 
    DataSize = random:uniform(?DATA_SIZE),
    <<Data:DataSize/bytes,_/binary>> = DataBin,

    F = fun(_Key, _Value) ->
                Data
        end,
    
    %% Do an update inside a txn snapshot
    case bdberl:update(DbRef, Key, F, undefined, [txn_snapshot]) of
        {error, Reason2} ->
            ct:print("update error: ~p~n", [Reason2]);
        _ ->
            ok
    end,

    bug200_incr_loop(Owner, DbRef, DataBin, Count-1).

