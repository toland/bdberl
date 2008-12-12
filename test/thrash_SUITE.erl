%% -------------------------------------------------------------------
%%
%% bdberl: Port Driver Thrash tests
%% Copyright (c) 2008 The Hive.  All rights reserved.
%%
%% -------------------------------------------------------------------
-module(thrash_SUITE).

-compile(export_all).

all() ->
    [test_thrash].

-define(PROCS, 10).

test_thrash(_Config) ->
    %% Spin up 15 processes (async thread pool is 10)
    start_procs(?PROCS),
    wait_for_finish(?PROCS).

start_procs(0) ->    
    ok;
start_procs(Count) ->
    spawn_link(?MODULE, thrash_run, [self()]),
    start_procs(Count-1).

wait_for_finish(0) ->
    ok;
wait_for_finish(Count) ->
    receive
        {finished, Pid} ->
            io:format("~p is done; ~p remaining.\n", [Pid, Count-1]),
            wait_for_finish(Count-1)
    end.

thrash_run(Owner) ->
    %% Seed the RNG
    {A1, A2, A3} = now(),
    random:seed(A1, A2, A3),

    %% Open up a port and database
    {ok, 0} = bdberl:open("thrash", btree),

    %% Start thrashing
    thrash_incr_loop(Owner, 1000).

thrash_incr_loop(Owner, 0) ->
    Owner ! {finished, self()};
thrash_incr_loop(Owner, Count) ->
    ct:print("~p\n", [Count]),
    %% Choose random key
    Key = random:uniform(1200),
    
    %% Start a txn that will read the current value of the key and increment by 1
    F = fun() ->
                case bdberl:get(0, Key, [rmw]) of
                    not_found ->
                        Value = 0;

                    {ok, Value} ->
                        Value
                end,
                ok = bdberl:put(0, Key, Value)
        end,
    ok = do_txn(F, 0),
    thrash_incr_loop(Owner, Count-1).

do_txn(F, Count) ->
    case bdberl:txn_begin() of
        ok ->
            case catch(F()) of
                {'EXIT', _Reason} ->
                    io:format("Txn attempt ~p failed; retrying", [Count]),
                    do_txn(F, Count+1);
                _Other ->
                    ok = bdberl:txn_commit()
            end;
        {error, _Reason} ->
            do_txn(F, Count+1)
    end.
