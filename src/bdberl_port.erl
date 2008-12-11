%% -------------------------------------------------------------------
%%
%% bdberl: Port Interface
%% Copyright (c) 2008 The Hive.  All rights reserved.
%%
%% -------------------------------------------------------------------
-module(bdberl_port).

-export([new/0,
         open_database/3,
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

-define(STATUS_OK,    0).
-define(STATUS_ERROR, 1).

-define(ERROR_NONE,          0).
-define(ERROR_MAX_DBS,       -29000).           % System can not open any more databases
-define(ERROR_ASYNC_PENDING, -29001).           % Async operation already pending on this port
-define(ERROR_INVALID_DBREF, -29002).           % DbRef not currently opened by this port
-define(ERROR_TXN_OPEN,      -29003).           % Transaction already active on this port
-define(ERROR_NO_TXN,        -29004).           % No transaction open on this port

-define(ERROR_DB_LOCK_NOTGRANTED, -30993).      % Lock was busy and not granted
-define(ERROR_DB_LOCK_DEADLOCK,   -30994).      % Deadlock occurred

new() ->
    case erl_ddll:load_driver(code:priv_dir(bdberl), bdberl_drv) of
        ok -> ok;
        {error, permanent} -> ok               % Means that the driver is already active
    end,
    Port = open_port({spawn, bdberl_drv}, [binary]),
    {ok, Port}.


open_database(Port, Name, Type) ->
    %% Map database type into an integer code
    case Type of
        btree -> TypeCode = ?DB_TYPE_BTREE;
        hash  -> TypeCode = ?DB_TYPE_HASH
    end,    
    Cmd = <<TypeCode:8/native-integer, (list_to_binary(Name))/bytes, 0:8/native-integer>>,
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
    case decode_rc(Result) of
        ok -> ok;
        Error -> {error, {txn_begin, Error}}
    end.
            

txn_commit(Port) ->
    <<Result:32/native>> = erlang:port_control(Port, ?CMD_TXN_COMMIT, <<>>),
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
    {KeyLen, KeyBin} = to_binary(Key),
    {ValLen, ValBin} = to_binary(Value),
    Cmd = <<DbRef:32/native, KeyLen:32/native, KeyBin/bytes, ValLen:32/native, ValBin/bytes>>,
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
    {KeyLen, KeyBin} = to_binary(Key),
    Cmd = <<DbRef:32/native, KeyLen:32/native, KeyBin/bytes>>,
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


