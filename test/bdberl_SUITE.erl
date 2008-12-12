%% -------------------------------------------------------------------
%%
%% bdberl: DB API Tests
%% Copyright (c) 2008 The Hive.  All rights reserved.
%%
%% -------------------------------------------------------------------
-module(bdberl_SUITE).

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


init_per_suite(Config) ->
    ok = bdberl:init(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    {ok, Db} = bdberl:open("api_test.db", btree, [create, exclusive]),
    [{db, Db}|Config].

end_per_testcase(_TestCase, Config) ->
    ok = bdberl:close(?config(db, Config)),
    ok = file:delete("api_test.db").


open_should_create_database_if_none_exists(_Config) ->
    true = filelib:is_file("api_test.db").

get_should_fail_when_getting_a_nonexistant_record(Config) ->
    not_found = bdberl:get(?config(db, Config), bad_key).

get_should_return_a_value_when_getting_a_valid_record(Config) ->
    Db = ?config(db, Config),
    ok = bdberl:put(Db, mykey, avalue),
    {ok, avalue} = bdberl:get(Db, mykey).

transaction_should_commit_on_success(Config) ->
    Db = ?config(db, Config),
    F = fun() -> bdberl:put(Db, mykey, avalue) end,
    {ok, ok} = bdberl:transaction(F),
    {ok, avalue} = bdberl:get(Db, mykey).

transaction_should_abort_on_exception(Config) ->
    Db = ?config(db, Config),

    F = fun() -> 
            bdberl:put(Db, mykey, should_not_see_this),
            throw(testing)
        end,

    {error, {transaction_failed, testing}} = bdberl:transaction(F),
    not_found = bdberl:get(Db, mykey).

transaction_should_abort_on_user_abort(Config) ->
    Db = ?config(db, Config),

    F = fun() -> 
            bdberl:put(Db, mykey, should_not_see_this),
            abort
        end,

    {error, transaction_aborted} = bdberl:transaction(F),
    not_found = bdberl:get(Db, mykey).

update_should_save_value_if_successful(Config) ->
    Db = ?config(db, Config),
    ok = bdberl:put(Db, mykey, avalue),

    F = fun(Key, Value) ->
            mykey = Key,
            avalue = Value,
            newvalue
        end,

    {ok, newvalue} = bdberl:update(Db, mykey, F),
    {ok, newvalue} = bdberl:get(Db, mykey).

