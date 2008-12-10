%% -------------------------------------------------------------------
%%
%% bdberl: Port Interface
%% Copyright (c) 2008 The Hive.  All rights reserved.
%%
%% -------------------------------------------------------------------
-module(bdberl_port).

-export([new/0,
         open_database/3, open_database/4,
         close_database/2,
         txn_begin/1, txn_commit/1, txn_abort/1,
         put/4,
         get/3]).

-define(CMD_NONE,       0).
-define(CMD_OPEN_DB,    1).
-define(CMD_CLOSE_DB,   2).
-define(CMD_TXN_BEGIN,  3).
-define(CMD_TXN_COMMIT, 4).
-define(CMD_TXN_ABORT,  5).
-define(CMD_GET,        6).
-define(CMD_PUT,        7).
-define(CMD_PUT_ATOMIC, 8).

-define(DB_TYPE_BTREE, 1).
-define(DB_TYPE_HASH,  2).

-define(DB_AUTO_COMMIT,      16#00000100).
-define(DB_CREATE,           16#00000001).
-define(DB_EXCL,             16#00000400).
-define(DB_MULTIVERSION,     16#00000008).
-define(DB_NOMMAP,           16#00000010).
-define(DB_RDONLY,           16#00000080).
-define(DB_READ_UNCOMMITTED, 16#00000200).
-define(DB_THREAD,           16#00000004).
-define(DB_TRUNCATE,         16#00008000).

-define(STATUS_OK,    0).
-define(STATUS_ERROR, 1).

-define(ERROR_NONE,          0).
-define(ERROR_MAX_DBS,       -29000).           % System can not open any more databases
-define(ERROR_ASYNC_PENDING, -29001).           % Async operation already pending on this port
-define(ERROR_INVALID_DBREF, -29002).           % DbRef not currently opened by this port
-define(ERROR_TXN_OPEN,      -29003).           % Transaction already active on this port
-define(ERROR_NO_TXN,        -29004).           % No transaction open on this port

new() ->
    case erl_ddll:load_driver(code:priv_dir(bdberl), bdberl_drv) of
        ok -> ok;
        {error, permanent} -> ok               % Means that the driver is already active
    end,
    Port = open_port({spawn, bdberl_drv}, [binary]),
    {ok, Port}.

open_database(Port, Name, Type) ->
    open_database(Port, Name, Type, [create, auto_commit, threaded]).

open_database(Port, Name, Type, Opts) ->
    %% Map database type into an integer code
    case Type of
        btree -> TypeCode = ?DB_TYPE_BTREE;
        hash  -> TypeCode = ?DB_TYPE_HASH
    end,
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/unsigned-native-integer, TypeCode:8/native-integer, (list_to_binary(Name))/bytes, 0:8/native-integer>>,
    case erlang:port_control(Port, ?CMD_OPEN_DB, Cmd) of
        <<?STATUS_OK:8, DbRef:32/native>> ->
            {ok, DbRef};
        <<?STATUS_ERROR:8, Errno:32/native>> ->
            {error, Errno}
    end.

close_database(Port, DbRef) ->
    Cmd = <<DbRef:32/native-integer>>,
    case erlang:port_control(Port, ?CMD_CLOSE_DB, Cmd) of
        <<0:32/native-integer>> ->
            {error, invalid_dbref};
        <<1:32/native-integer>> ->
            ok
    end.

txn_begin(Port) ->
    <<Result:32/native>> = erlang:port_control(Port, ?CMD_TXN_BEGIN, <<>>),
    case Result of
        ?ERROR_NONE -> ok;
        ?ERROR_ASYNC_PENDING -> {error, async_pending};
        ?ERROR_TXN_OPEN -> {error, txn_open}
    end.

txn_commit(Port) ->
    <<Result:32/native>> = erlang:port_control(Port, ?CMD_TXN_COMMIT, <<>>),
    case Result of
        ?ERROR_NONE -> 
            receive
                ok -> ok;
                {error, Reason} -> {error, Reason}
            end;
        ?ERROR_ASYNC_PENDING -> {error, async_pending};
        ?ERROR_NO_TXN -> {error, no_txn}
    end.

txn_abort(Port) ->
    <<Result:32/native>> = erlang:port_control(Port, ?CMD_TXN_ABORT, <<>>),
    case Result of
        ?ERROR_NONE -> 
            receive
                ok -> ok;
                {error, Reason} -> {error, Reason}
            end;
        ?ERROR_ASYNC_PENDING -> {error, async_pending};
        ?ERROR_NO_TXN -> {error, no_txn}
    end.
            
            
put(Port, DbRef, Key, Value) ->
    {KeyLen, KeyBin} = to_binary(Key),
    {ValLen, ValBin} = to_binary(Value),
    Cmd = <<DbRef:32/native, KeyLen:32/native, KeyBin/bytes, ValLen:32/native, ValBin/bytes>>,
    <<Result:32/native>> = erlang:port_control(Port, ?CMD_PUT, Cmd),
    case Result of
        ?ERROR_NONE -> 
            receive
                ok -> ok;
                {error, Reason} -> {error, Reason}
            end;
        ?ERROR_ASYNC_PENDING -> {error, async_pending};
        ?ERROR_INVALID_DBREF -> {error, invalid_dbref}
    end.


get(Port, DbRef, Key) ->
    {KeyLen, KeyBin} = to_binary(Key),
    Cmd = <<DbRef:32/native, KeyLen:32/native, KeyBin/bytes>>,
    <<Result:32/native>> = erlang:port_control(Port, ?CMD_GET, Cmd),
    case Result of
        ?ERROR_NONE ->
            receive
                {ok, Bin} -> {ok, binary_to_term(Bin)};
                not_found -> not_found;
                {error, Reason} -> {error, Reason}
            end;
        ?ERROR_ASYNC_PENDING -> {error, async_pending};
        ?ERROR_INVALID_DBREF -> {error, invalid_dbref}
    end.
    
            



to_binary(Term) ->
    Bin = term_to_binary(Term),
    {size(Bin), Bin}.

process_flags([Flag]) ->
    flag_value(Flag);
process_flags([Flag|Flags]) ->
    flag_value(Flag) bor process_flags(Flags).

flag_value(Flag) ->
    case Flag of
        auto_commit  -> ?DB_AUTO_COMMIT;
        create       -> ?DB_CREATE;
        exclusive    -> ?DB_EXCL;
        multiversion -> ?DB_MULTIVERSION;
        no_mmap      -> ?DB_NOMMAP;
        readonly     -> ?DB_RDONLY;
        threaded     -> ?DB_THREAD;
        truncate     -> ?DB_TRUNCATE
    end.
