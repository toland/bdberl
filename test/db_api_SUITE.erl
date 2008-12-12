%% -------------------------------------------------------------------
%%
%% bdberl: DB API Tests
%% Copyright (c) 2008 The Hive.  All rights reserved.
%%
%% -------------------------------------------------------------------
-module(db_api_SUITE).

-compile(export_all).

-include_lib("ct.hrl").

all() ->
    [open_should_create_database_if_none_exists,
     get_should_fail_when_getting_a_nonexistant_record,
     get_should_return_a_value_when_getting_a_valid_record,
     transaction_should_commit_on_success,
     transaction_should_abort_on_exception,
     transaction_should_abort_on_user_abort,
     update_should_save_value_if_successful].


init_per_testcase(_TestCase, Config) ->
    {ok, Port} = bdberl_port:new(),
    {ok, Db} = bdberl_db:open(Port, "api_test.db", btree, [create, exclusive]),
    [{port, Port},{db, Db}|Config].

end_per_testcase(_TestCase, Config) ->
    ok = bdberl_db:close(?config(db, Config)),
    true = port_close(?config(port, Config)),
    ok = file:delete("api_test.db").


open_should_create_database_if_none_exists(_Config) ->
    true = filelib:is_file("api_test.db").

get_should_fail_when_getting_a_nonexistant_record(Config) ->
    not_found = bdberl_db:get(?config(db, Config), bad_key).

get_should_return_a_value_when_getting_a_valid_record(Config) ->
    Db = ?config(db, Config),
    ok = bdberl_db:put(Db, mykey, avalue),
    {ok, avalue} = bdberl_db:get(Db, mykey).

transaction_should_commit_on_success(Config) ->
    Db = ?config(db, Config),
    F = fun() -> bdberl_db:put(Db, mykey, avalue) end,
    {ok, ok} = bdberl_db:transaction(Db, F),
    {ok, avalue} = bdberl_db:get(Db, mykey).

transaction_should_abort_on_exception(Config) ->
    Db = ?config(db, Config),

    F = fun() -> 
            bdberl_db:put(Db, mykey, should_not_see_this),
            throw(testing)
        end,

    {error, {transaction_failed, testing}} = bdberl_db:transaction(Db, F),
    not_found = bdberl_db:get(Db, mykey).

transaction_should_abort_on_user_abort(Config) ->
    Db = ?config(db, Config),

    F = fun() -> 
            bdberl_db:put(Db, mykey, should_not_see_this),
            abort
        end,

    {error, transaction_aborted} = bdberl_db:transaction(Db, F),
    not_found = bdberl_db:get(Db, mykey).

update_should_save_value_if_successful(Config) ->
    Db = ?config(db, Config),
    ok = bdberl_db:put(Db, mykey, avalue),

    F = fun(Key, Value) ->
            mykey = Key,
            avalue = Value,
            newvalue
        end,

    {ok, newvalue} = bdberl_db:update(Db, mykey, F),
    {ok, newvalue} = bdberl_db:get(Db, mykey).

