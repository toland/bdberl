%% -------------------------------------------------------------------
%%
%% bdberl: Interface to BerkeleyDB
%% Copyright (c) 2008 The Hive.  All rights reserved.
%%
%% -------------------------------------------------------------------
-module(bdberl).

-export([open/2, open/3,
         close/1, close/2,
         txn_begin/0, txn_begin/1, 
         txn_commit/0, txn_commit/1, txn_abort/0,
         get_cache_size/0, set_cache_size/3,
         get_txn_timeout/0, set_txn_timeout/1,
         transaction/1,
         put/3, put/4,
         put_r/3, put_r/4,
         get/2, get/3,
         get_r/2, get_r/3,
         update/3, update/4,
         cursor_open/1, cursor_next/0, cursor_prev/0, cursor_current/0, cursor_close/0]).

-include("bdberl.hrl").


open(Name, Type) ->
    open(Name, Type, [create]).

open(Name, Type, Opts) ->
    %% Map database type into an integer code
    case Type of
        btree -> TypeCode = ?DB_TYPE_BTREE;
        hash  -> TypeCode = ?DB_TYPE_HASH
    end,
    Flags = process_flags(lists:umerge(Opts, [auto_commit, threaded])),
    Cmd = <<Flags:32/unsigned-native-integer, TypeCode:8/native-integer, (list_to_binary(Name))/bytes, 0:8/native-integer>>,
    case erlang:port_control(get_port(), ?CMD_OPEN_DB, Cmd) of
        <<?STATUS_OK:8, Db:32/native>> ->
            {ok, Db};
        <<?STATUS_ERROR:8, Errno:32/native>> ->
            {error, Errno}
    end.

close(Db) ->
    close(Db, []).

close(Db, Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Db:32/native-integer, Flags:32/unsigned-native-integer>>,
    <<Rc:32/native-signed>> = erlang:port_control(get_port(), ?CMD_CLOSE_DB, Cmd),
    case decode_rc(Rc) of
        ok ->
            ok;
        Reason ->
            {error, Reason}
    end.

txn_begin() ->
    txn_begin([]).

txn_begin(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/unsigned-native>>,
    <<Result:32/native>> = erlang:port_control(get_port(), ?CMD_TXN_BEGIN, Cmd),
    case decode_rc(Result) of
        ok -> ok;
        Error -> {error, {txn_begin, Error}}
    end.

txn_commit() ->
    txn_commit([]).

txn_commit(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/unsigned-native>>,
    <<Result:32/native>> = erlang:port_control(get_port(), ?CMD_TXN_COMMIT, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                ok -> ok;
                {error, Reason} -> {error, {txn_commit, decode_rc(Reason)}}
            end;
        Error ->
            {error, {txn_commit, Error}}
    end.

txn_abort() ->
    <<Result:32/native>> = erlang:port_control(get_port(), ?CMD_TXN_ABORT, <<>>),
    case decode_rc(Result) of
        ok ->
            receive
                ok -> ok;
                {error, Reason} -> {error, {txn_abort, decode_rc(Reason)}}
            end;
        Error ->
            {error, {txn_abort, Error}}
    end.
            
transaction(Fun) ->
    txn_begin(),
    try Fun() of
        abort ->
            txn_abort(),
            {error, transaction_aborted};
        Value ->
            txn_commit(),
            {ok, Value}
    catch
        _ : Reason -> 
            txn_abort(),
            {error, {transaction_failed, Reason}}
    end.

put(Db, Key, Value) ->
    put(Db, Key, Value, []).

put(Db, Key, Value, Opts) ->
    {KeyLen, KeyBin} = to_binary(Key),
    {ValLen, ValBin} = to_binary(Value),
    Flags = process_flags(Opts),
    Cmd = <<Db:32/native, Flags:32/unsigned-native, KeyLen:32/native, KeyBin/bytes, ValLen:32/native, ValBin/bytes>>,
    <<Result:32/native>> = erlang:port_control(get_port(), ?CMD_PUT, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                ok -> ok;
                {error, Reason} -> {error, {put, decode_rc(Reason)}}
            end;
        Error ->
            {error, {put, decode_rc(Error)}}
    end.

put_r(Db, Key, Value) ->
    put(Db, Key, Value, []).

put_r(Db, Key, Value, Opts) ->
    case put(Db, Key, Value, Opts) of
        ok -> ok;
        Error -> throw(Error)
    end.

get(Db, Key) ->
    get(Db, Key, []).

get(Db, Key, Opts) ->
    {KeyLen, KeyBin} = to_binary(Key),
    Flags = process_flags(Opts),
    Cmd = <<Db:32/native, Flags:32/unsigned-native, KeyLen:32/native, KeyBin/bytes>>,
    <<Result:32/native>> = erlang:port_control(get_port(), ?CMD_GET, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                {ok, Bin} -> {ok, binary_to_term(Bin)};
                not_found -> not_found;
                {error, Reason} -> {error, {get, decode_rc(Reason)}}
            end;
        Error ->
            {error, {get, decode_rc(Error)}}
    end.

get_r(Db, Key) ->
    get_r(Db, Key, []).

get_r(Db, Key, Opts) ->
    case get(Db, Key, Opts) of
        {ok, Value} -> {ok, Value};
        not_found   -> not_found;
        Error       -> throw(Error)
    end.

update(Db, Key, Fun) ->
    update(Db, Key, Fun, undefined).

update(Db, Key, Fun, Args) ->
    F = fun() ->
            {ok, Value} = get(Db, Key, [rmw]),
            NewValue = case Args of
                           undefined -> Fun(Key, Value);
                           Args      -> Fun(Key, Value, Args)
                       end,
            ok = put(Db, Key, NewValue),
            NewValue
        end,
    transaction(F).

cursor_open(Db) ->
    Cmd = <<Db:32/native, 0:32/native>>,
    <<Rc:32/native-signed>> = erlang:port_control(get_port(), ?CMD_CURSOR_OPEN, Cmd),
    case decode_rc(Rc) of
        ok ->
            ok;
        Reason ->
            {error, Reason}
    end.
            

cursor_next() ->            
    do_cursor_move(?CMD_CURSOR_NEXT).

cursor_prev() ->
    do_cursor_move(?CMD_CURSOR_PREV).

cursor_current() ->
    do_cursor_move(?CMD_CURSOR_CURR).

cursor_close() ->
    <<Rc:32/native-signed>> = erlang:port_control(get_port(), ?CMD_CURSOR_CLOSE, <<>>),
    case decode_rc(Rc) of
        ok ->
            ok;
        Reason ->
            {error, Reason}
    end.
    

get_cache_size() ->    
    Cmd = <<?SYSP_CACHESIZE_GET:32/native>>,
    <<Result:32/signed-native, Gbytes:32/native, Bytes:32/native, Ncaches:32/native>> = 
        erlang:port_control(get_port(), ?CMD_TUNE, Cmd),
    case Result of
        0 ->
            {ok, Gbytes, Bytes, Ncaches};
        _ ->
            {error, Result}
    end.

set_cache_size(Gbytes, Bytes, Ncaches) ->
    Cmd = <<?SYSP_CACHESIZE_SET:32/native, Gbytes:32/native, Bytes:32/native, Ncaches:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_TUNE, Cmd),
    case Result of
        0 ->
            ok;
        _ ->
            {error, Result}
    end.
    

get_txn_timeout() ->    
    Cmd = <<?SYSP_TXN_TIMEOUT_GET:32/native>>,
    <<Result:32/signed-native, Timeout:32/native>> = erlang:port_control(get_port(), ?CMD_TUNE, Cmd),
    case Result of
        0 ->
            {ok, Timeout};
        _ ->
            {error, Result}
    end.

set_txn_timeout(Timeout) ->
    Cmd = <<?SYSP_TXN_TIMEOUT_SET:32/native, Timeout:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_TUNE, Cmd),
    case Result of
        0 ->
            ok;
        _ ->
            {error, Result}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

init() ->
    case erl_ddll:load_driver(code:priv_dir(bdberl), bdberl_drv) of
        ok -> ok;
        {error, permanent} -> ok               % Means that the driver is already active
    end,
    Port = open_port({spawn, bdberl_drv}, [binary]),
    erlang:put(bdb_port, Port),
    Port.

get_port() ->
    case erlang:get(bdb_port) of
        undefined -> init();
        Port      -> Port
    end.

%% 
%% Decode a integer return value into an atom representation
%%
decode_rc(?ERROR_NONE)               -> ok;
decode_rc(?ERROR_ASYNC_PENDING)      -> async_pending;
decode_rc(?ERROR_INVALID_DBREF)      -> invalid_db;
decode_rc(?ERROR_TXN_OPEN)           -> transaction_open;
decode_rc(?ERROR_NO_TXN)             -> no_txn;
decode_rc(?ERROR_CURSOR_OPEN)        -> cursor_open;
decode_rc(?ERROR_NO_CURSOR)          -> no_cursor;
decode_rc(?ERROR_DB_LOCK_NOTGRANTED) -> lock_not_granted;
decode_rc(?ERROR_DB_LOCK_DEADLOCK)   -> deadlock;
decode_rc(Rc)                        -> {unknown, Rc}.
    
%%
%% Convert a term into a binary, returning a tuple with the binary and the length of the binary
%%
to_binary(Term) ->
    Bin = term_to_binary(Term),
    {size(Bin), Bin}.

%%
%% Given an array of options, produce a single integer with the numeric values
%% of the options joined with binary OR
%%
process_flags([]) ->
    0;
process_flags([Flag|Flags]) ->
    flag_value(Flag) bor process_flags(Flags).

%%
%% Given an option as an atom, return the numeric value
%%
flag_value(Flag) ->
    case Flag of
        append           -> ?DB_APPEND;
        auto_commit      -> ?DB_AUTO_COMMIT;
        consume          -> ?DB_CONSUME;
        consume_wait     -> ?DB_CONSUME_WAIT;
        create           -> ?DB_CREATE;
        exclusive        -> ?DB_EXCL;
        get_both         -> ?DB_GET_BOTH;
        ignore_lease     -> ?DB_IGNORE_LEASE;
        multiple         -> ?DB_MULTIPLE;
        multiversion     -> ?DB_MULTIVERSION;
        no_duplicate     -> ?DB_NODUPDATA;
        no_mmap          -> ?DB_NOMMAP;
        no_overwrite     -> ?DB_NOOVERWRITE;
        no_sync          -> ?DB_NOSYNC;
        read_committed   -> ?DB_READ_COMMITTED;
        read_uncommitted -> ?DB_READ_UNCOMMITTED;
        readonly         -> ?DB_RDONLY;
        rmw              -> ?DB_RMW;
        set_recno        -> ?DB_SET_RECNO;
        threaded         -> ?DB_THREAD;
        truncate         -> ?DB_TRUNCATE;
        txn_no_sync      -> ?DB_TXN_NOSYNC;
        txn_no_wait      -> ?DB_TXN_NOWAIT;
        txn_snapshot     -> ?DB_TXN_SNAPSHOT;
        txn_sync         -> ?DB_TXN_SYNC;
        txn_wait         -> ?DB_TXN_WAIT;
        txn_write_nosync -> ?DB_TXN_WRITE_NOSYNC
    end.


%%
%% Move the cursor in a given direction. Invoked by cursor_next/prev/current.
%%
do_cursor_move(Direction) ->            
    <<Rc:32/native-signed>> = erlang:port_control(get_port(), Direction, <<>>),
    case decode_rc(Rc) of
        ok ->
            receive
                {ok, KeyBin, ValueBin} ->
                    {ok, binary_to_term(KeyBin), binary_to_term(ValueBin)};
                not_found ->
                    not_found;
                {error, ReasonCode} ->
                    {error, decode_rc(ReasonCode)}
            end;
        Reason ->
            {error, Reason}
    end.
