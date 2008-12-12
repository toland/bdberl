%% -------------------------------------------------------------------
%%
%% bdberl: API Interface
%% Copyright (c) 2008 The Hive.  All rights reserved.
%%
%% -------------------------------------------------------------------
-module(bdberl_db).


-export([open/3, open/4,
         close/1, close/2,
         put/3, put/4,
         get/2, get/3,
         transaction/2,
         update/3]).


open(Port, Name, Type) ->
    open(Port, Name, Type, [create]).

open(Port, Name, Type, Opts) ->
    case bdberl_port:open_database(Port, Name, Type, Opts) of
        {ok, DbRef} -> {ok, {db, Port, DbRef}};
        {error, Reason} -> {error, Reason}
    end.

close(Db) ->
    close(Db, []).

close({db, Port, DbRef}, Opts) ->
    bdberl_port:close_database(Port, DbRef, Opts).

put(Db, Key, Value) ->
    put(Db, Key, Value, []).

put({db, Port, DbRef}, Key, Value, Opts) ->
    bdberl_port:put(Port, DbRef, Key, Value, Opts).

get(Db, Key) ->
    get(Db, Key, []).

get({db, Port, DbRef}, Key, Opts) ->
    bdberl_port:get(Port, DbRef, Key, Opts).


transaction({db, Port, _DbRef}, Fun) ->
    bdberl_port:txn_begin(Port),
    try Fun() of
        abort ->
            bdberl_port:txn_abort(Port),
            {error, transaction_aborted};
        Value ->
            bdberl_port:txn_commit(Port),
            {ok, Value}
    catch
        _ : Reason -> 
            bdberl_port:txn_abort(Port),
            {error, {transaction_failed, Reason}}
    end.

update(Db, Key, Fun) ->
    F = fun() ->
            {ok, Value} = get(Db, Key, [rmw]),
            NewValue = Fun(Key, Value),
            ok = put(Db, Key, NewValue),
            NewValue
        end,
    transaction(Db, F).
