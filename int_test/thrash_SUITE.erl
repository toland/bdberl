%% -------------------------------------------------------------------
%%
%% bdberl: Port Driver Thrash tests
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
            ct:print("~p is done; ~p remaining.\n", [Pid, Count-1]),
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
    % ct:print("~p", [Count]),
    %% Choose random key
    Key = random:uniform(1200),

    %% Start a txn that will read the current value of the key and increment by 1
    F = fun(_Key, Value) ->
            case Value of
                not_found -> 0;
                Value     -> Value + 1
            end
        end,
    {ok, _} = bdberl:update(0, Key, F),
    thrash_incr_loop(Owner, Count-1).
