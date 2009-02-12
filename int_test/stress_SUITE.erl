%% -------------------------------------------------------------------
%%
%% bdberl: Port Driver Stress tests
%% Copyright (c) 2008 The Hive.  All rights reserved.
%%
%% -------------------------------------------------------------------
-module(stress_SUITE).
-compile(export_all).
-include_lib("ct.hrl").

%% NOTE: all of the tests are set for a low number of iterations to guarantee
%% that they all pass and run in a reasonable amount of time. That kinda defeats
%% the purpose of the test, tho. Work is ongoing to make this a useful test suite.

all() ->
    [rewrite_array_test,
     rewrite_bytes_test,
     write_array_test,
     write_bytes_test].

init_per_suite(Config) ->
    {ok, Cwd} = file:get_cwd(),
    {ok, _} = file:copy(lists:append([Cwd, "/../../int_test/DB_CONFIG"]),
                        lists:append([Cwd, "/DB_CONFIG"])),
    crypto:start(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Size = 1024 * 1024,
    Chunk = crypto:rand_bytes(Size),
    Name = io_lib:format("~p.db", [TestCase]),
    {ok, Db} = bdberl:open(Name, hash),
    [{size, Size}, {chunk, Chunk}, {db, Db}|Config].

end_per_testcase(_TestCase, Config) ->
    bdberl:close(?config(db, Config)),
    ok.

%%---------------------------------------------------------------------------

rewrite_array_test(Config) ->
    %% If you try to run this one for more than 2K iterations than the Erlang
    %% VM will die with a memory allocation error when creating the binary.
    ct:print("Running rewrite_array test for 2000 iterations..."),
    Chunk = ?config(chunk, Config),
    rewrite_array(?config(db, Config), Chunk, [Chunk], 20).

rewrite_array(_Db, _Block, _Bytes, 0) ->
    ok;
rewrite_array(Db, Block, Bytes, Iter) ->
    bdberl:put(Db, 1, Bytes),
    rewrite_array(Db, Block, [Block|Bytes], Iter - 1).

%%---------------------------------------------------------------------------

rewrite_bytes_test(Config) ->
    ct:print("Running rewrite_bytes test for 2500 iterations..."),
    rewrite_bytes(?config(db, Config), ?config(chunk, Config), 25).

rewrite_bytes(_Db, _Bytes, 0) ->
    ok;
rewrite_bytes(Db, Bytes, Iter) ->
    bdberl:put(Db, 1, Bytes),
    rewrite_bytes(Db, Bytes, Iter - 1).

%%---------------------------------------------------------------------------

write_array_test(Config) ->
    ct:print("Running write_array test for 150 iterations..."),
    Chunk = ?config(chunk, Config),
    write_array(?config(db, Config), Chunk, [Chunk], 15).

write_array(_Db, _Block, _Bytes, 0) ->
    ok;
write_array(Db, Block, Bytes, Iter) ->
    bdberl:put(Db, Iter, Bytes),
    write_array(Db, Block, [Block|Bytes], Iter - 1).

%%---------------------------------------------------------------------------

write_bytes_test(Config) ->
    ct:print("Running write_bytes test for 2500 iterations..."),
    write_bytes(?config(db, Config), ?config(chunk, Config), 25).

write_bytes(_Db, _Bytes, 0) ->
    ok;
write_bytes(Db, Bytes, Iter) ->
    bdberl:put(Db, Iter, Bytes),
    write_bytes(Db, Bytes, Iter - 1).
