%% -------------------------------------------------------------------
%%
%% bdberl: Port Tests
%% Copyright (c) 2008 The Hive.  All rights reserved.
%%
%% -------------------------------------------------------------------
-module(port_SUITE).

-compile(export_all).

all() ->
%    [test_db].
    [test_put, test_txn].

init_per_testcase(TestCase, Config) ->
    Config.

end_per_testcase(TestCase, _Config) ->
    ok.
    

test_db(_Config) ->
    {ok, P} = bdberl_port:new(),
    
    %% Create two databases 
    {ok, 0} = bdberl_port:open_database(P, "test1", hash),
    {ok, 1} = bdberl_port:open_database(P, "test2", btree),
    
    %% Open another port and open the same databases in reverse order. The ref system should
    %% ensure that the databases return the same refs as previously
    {ok, P2} = bdberl_port:new(),
    {ok, 1} = bdberl_port:open_database(P2, "test2", btree),
    {ok, 0} = bdberl_port:open_database(P2, "test1", hash),
    
    %% Close one of the databases
    ok = bdberl_port:close_database(P, 0),
    ok = bdberl_port:close_database(P2, 0),
    
    %% Attempt to close an invalid ref
    {error, invalid_dbref} = bdberl_port:close_database(P, 21000),
    
    %% Open up another db -- should re-use dbref 0 as that's the first available
    {ok, 0} = bdberl_port:open_database(P, "test3", btree),
    
    %% Close both ports
    true = port_close(P),
    true = port_close(P2).
    
    
test_put(_Config) ->
    {ok, P} = bdberl_port:new(),
    {ok, 0} = bdberl_port:open_database(P, "test1", hash),

    ok = bdberl_port:txn_begin(P),
    ok = bdberl_port:put(P, 0, akey, avalue),
    ok = bdberl_port:txn_commit(P),

    ok = bdberl_port:txn_begin(P),    
    {ok, avalue} = bdberl_port:get(P, 0, akey),   
    ok = bdberl_port:txn_commit(P).


test_txn(_Config) ->
    {ok, P} = bdberl_port:new(),
    {ok, 0} = bdberl_port:open_database(P, "test2", btree),
    
    ok = bdberl_port:txn_begin(P),
    ok = bdberl_port:put(P, 0, akey, avalue),
    ok = bdberl_port:txn_abort(P),

    ok = bdberl_port:txn_begin(P),
    not_found = bdberl_port:get(P, 0, akey),
    ok = bdberl_port:txn_commit(P).
    



            
    
