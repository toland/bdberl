%% -------------------------------------------------------------------
%%
%% bdberl: Port Driver Bug200 tests
%% Copyright (c) 2008 The Hive.  All rights reserved.
%%
%% -------------------------------------------------------------------
-module(bug200_SUITE).

-compile(export_all).

-define(PROCS, 32).
all() ->
    [bug200].

bug200(_Config) ->
    %% Spin up 15 processes (async thread pool is 10)
    start_procs(?PROCS),
    wait_for_finish(?PROCS).

start_procs(0) ->
    ok;
start_procs(Count) ->
    spawn_link(?MODULE, bug200_run, [self()]),
    start_procs(Count-1).

wait_for_finish(0) ->
    ok;
wait_for_finish(Count) ->
    receive
        {finished, Pid} ->
            ct:print("~p is done; ~p remaining.\n", [Pid, Count-1]),
            wait_for_finish(Count-1)
    end.

bug200_run(Owner) ->
    %% Seed the RNG
    {A1, A2, A3} = now(),
    random:seed(A1, A2, A3),

    %% Start bug200ing
    bug200_incr_loop(Owner, 1000).

bug200_incr_loop(Owner, 0) ->
    Owner ! {finished, self()};
bug200_incr_loop(Owner, Count) ->
    % ct:print("~p", [Count]),
    %% Choose random key
    Key = random:uniform(1200),
    File = "bug200_" ++ integer_to_list(random:uniform(16)) ++ ".bdb",
    %% Start a txn that will read the current value of the key and increment by 1
    F = fun(_Key, Value) ->
            case Value of
                not_found -> 0;
                Value     -> Value + 1
            end
        end,
    %% Open up a port and database
    {ok, DbRef} = bdberl:open(File, btree),
    {ok, _} = bdberl:update(DbRef, Key, F),
    ok = bdberl:close(DbRef),
    bug200_incr_loop(Owner, Count-1).

