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
         txn_begin/1, txn_begin/2, 
         txn_commit/1, txn_commit/2, txn_abort/1,
         get_cache_size/1, set_cache_size/4,
         get_txn_timeout/1, set_txn_timeout/2,
         put/4, put/5,
         get/3, get/4]).

-include("bdberl.hrl").

new() ->
    case erl_ddll:load_driver(code:priv_dir(bdberl), bdberl_drv) of
        ok -> ok;
        {error, permanent} -> ok               % Means that the driver is already active
    end,
    Port = open_port({spawn, bdberl_drv}, [binary]),
    {ok, Port}.

open_database(Port, Name, Type) ->
    open_database(Port, Name, Type, [create]).

open_database(Port, Name, Type, Opts) ->
    %% Map database type into an integer code
    case Type of
        btree -> TypeCode = ?DB_TYPE_BTREE;
        hash  -> TypeCode = ?DB_TYPE_HASH
    end,
    Flags = process_flags(lists:umerge(Opts, [auto_commit, threaded])),
    Cmd = <<Flags:32/unsigned-native-integer, TypeCode:8/native-integer, (list_to_binary(Name))/bytes, 0:8/native-integer>>,
    case erlang:port_control(Port, ?CMD_OPEN_DB, Cmd) of
        <<?STATUS_OK:8, DbRef:32/native>> ->
            {ok, DbRef};
        <<?STATUS_ERROR:8, Errno:32/native>> ->
            {error, Errno}
    end.

close_database(Port, DbRef) ->
    close_database(Port, DbRef, []).

close_database(Port, DbRef, Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<DbRef:32/native-integer, Flags:32/unsigned-native-integer>>,
    case erlang:port_control(Port, ?CMD_CLOSE_DB, Cmd) of
        <<0:32/native-integer>> ->
            {error, invalid_dbref};
        <<1:32/native-integer>> ->
            ok
    end.

txn_begin(Port) ->
    txn_begin(Port, []).

txn_begin(Port, Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/unsigned-native>>,
    <<Result:32/native>> = erlang:port_control(Port, ?CMD_TXN_BEGIN, Cmd),
    case decode_rc(Result) of
        ok -> ok;
        Error -> {error, {txn_begin, Error}}
    end.
            
txn_commit(Port) ->
    txn_commit(Port, []).

txn_commit(Port, Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/unsigned-native>>,
    <<Result:32/native>> = erlang:port_control(Port, ?CMD_TXN_COMMIT, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                ok -> ok;
                {error, Reason} -> {error, {txn_commit, decode_rc(Reason)}}
            end;
        Error ->
            {error, {txn_commit, Error}}
    end.

txn_abort(Port) ->
    <<Result:32/native>> = erlang:port_control(Port, ?CMD_TXN_ABORT, <<>>),
    case decode_rc(Result) of
        ok ->
            receive
                ok -> ok;
                {error, Reason} -> {error, {txn_abort, decode_rc(Reason)}}
            end;
        Error ->
            {error, {txn_abort, Error}}
    end.
            
put(Port, DbRef, Key, Value) ->
    put(Port, DbRef, Key, Value, []).

put(Port, DbRef, Key, Value, Opts) ->
    {KeyLen, KeyBin} = to_binary(Key),
    {ValLen, ValBin} = to_binary(Value),
    Flags = process_flags(Opts),
    Cmd = <<DbRef:32/native, Flags:32/unsigned-native, KeyLen:32/native, KeyBin/bytes, ValLen:32/native, ValBin/bytes>>,
    <<Result:32/native>> = erlang:port_control(Port, ?CMD_PUT, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                ok -> ok;
                {error, Reason} -> {error, {put, decode_rc(Reason)}}
            end;
        Error ->
            {error, {put, decode_rc(Error)}}
    end.

get(Port, DbRef, Key) ->
    get(Port, DbRef, Key, []).

get(Port, DbRef, Key, Opts) ->
    {KeyLen, KeyBin} = to_binary(Key),
    Flags = process_flags(Opts),
    Cmd = <<DbRef:32/native, Flags:32/unsigned-native, KeyLen:32/native, KeyBin/bytes>>,
    <<Result:32/native>> = erlang:port_control(Port, ?CMD_GET, Cmd),
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

get_cache_size(Port) ->    
    Cmd = <<?SYSP_CACHESIZE_GET:32/native>>,
    <<Result:32/signed-native, Gbytes:32/native, Bytes:32/native, Ncaches:32/native>> = 
        erlang:port_control(Port, ?CMD_TUNE, Cmd),
    case Result of
        0 ->
            {ok, Gbytes, Bytes, Ncaches};
        _ ->
            {error, Result}
    end.

set_cache_size(Port, Gbytes, Bytes, Ncaches) ->
    Cmd = <<?SYSP_CACHESIZE_SET:32/native, Gbytes:32/native, Bytes:32/native, Ncaches:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(Port, ?CMD_TUNE, Cmd),
    case Result of
        0 ->
            ok;
        _ ->
            {error, Result}
    end.
    

get_txn_timeout(Port) ->    
    Cmd = <<?SYSP_TXN_TIMEOUT_GET:32/native>>,
    <<Result:32/signed-native, Timeout:32/native>> = erlang:port_control(Port, ?CMD_TUNE, Cmd),
    case Result of
        0 ->
            {ok, Timeout};
        _ ->
            {error, Result}
    end.

set_txn_timeout(Port, Timeout) ->
    Cmd = <<?SYSP_TXN_TIMEOUT_SET:32/native, Timeout:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(Port, ?CMD_TUNE, Cmd),
    case Result of
        0 ->
            ok;
        _ ->
            {error, Result}
    end.
    
    
            
%% ====================================================================
%% Internal functions
%% ====================================================================

%% 
%% Decode a integer return value into an atom representation
%%
decode_rc(?ERROR_NONE)               -> ok;
decode_rc(?ERROR_ASYNC_PENDING)      -> async_pending;
decode_rc(?ERROR_INVALID_DBREF)      -> invalid_dbref;
decode_rc(?ERROR_NO_TXN)             -> no_txn;
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

