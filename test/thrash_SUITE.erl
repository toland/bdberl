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
    {ok, P} = bdberl_port:new(),
    {ok, 0} = bdberl_port:open_database(P, "thrash", btree),

    %% Start thrashing
    thrash_incr_loop(P, Owner, 1000).



thrash_incr_loop(Port, Owner, 0) ->
    Owner ! {finished, self()};
thrash_incr_loop(Port, Owner, Count) ->
    ct:print("~p\n", [Count]),
    %% Choose random key
    Key = random:uniform(1200),
    
    %% Start a txn that will read the current value of the key and increment by 1
    F = fun() ->
                case get_or_die(Port, 0, Key) of
                    not_found ->
                        Value = 0;

                    Value ->
                        Value
                end,
                put_or_die(Port, 0, Key, Value)
        end,
    ok = do_txn(Port, F, 0),
    thrash_incr_loop(Port, Owner, Count-1).



get_or_die(Port, DbRef, Key) ->
    case bdberl_port:get(Port, DbRef, Key, [rmw]) of
        not_found ->
            not_found;
        {ok, Value} ->
            Value
    end.


put_or_die(Port, DbRef, Key, Value) ->
    ok = bdberl_port:put(Port, DbRef, Key, Value).


do_txn(Port, F, Count) ->
    case bdberl_port:txn_begin(Port) of
        ok ->
            case catch(F()) of
                {'EXIT', Reason} ->
                    io:format("Txn attempt ~p failed; retrying", [Count]),
                    do_txn(Port, F, Count+1);
                Other ->
                    ok = bdberl_port:txn_commit(Port)
            end;
        {error, Reason} ->
            do_txn(Port, F, Count+1)
    end.
