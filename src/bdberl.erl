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
         get_cache_size/0,
         get_data_dirs/0,
         get_txn_timeout/0,
         transaction/1, transaction/2,
         put/3, put/4,
         put_r/3, put_r/4,
         put_commit/3, put_commit/4,
         put_commit_r/3, put_commit_r/4,
         get/2, get/3,
         get_r/2, get_r/3,
         update/3, update/4,
         truncate/0, truncate/1,
         delete_database/1,
         cursor_open/1, cursor_next/0, cursor_prev/0, cursor_current/0, cursor_close/0]).

-include("bdberl.hrl").

-define(is_lock_error(Error), (Error =:= deadlock orelse Error =:= lock_not_granted)).

-type db() :: integer().
-type db_name() :: [byte(),...].
-type db_type() :: btree | hash.
-type db_flags() :: [atom()].
-type db_key() :: term().
-type db_value() :: term().
-type db_ret_value() :: not_found | db_value().

-type db_error_reason() :: atom() | {unknown, integer()}.
-type db_error() :: {error, db_error_reason()}.

-type txn_fun() :: fun(() -> term()).
-type txn_retries() :: infinity | non_neg_integer().

-type db_update_fun() :: fun((db_key(), db_value(), any()) -> db_value()).
-type db_update_fun_args() :: undefined | [term()].


-spec open(Name :: db_name(), Type :: db_type()) ->
    {ok, db()} | {error, integer()}.

open(Name, Type) ->
    open(Name, Type, [create]).


-spec open(Name :: db_name(), Type :: db_type(), Opts :: db_flags()) ->
    {ok, db()} | {error, integer()}.

open(Name, Type, Opts) ->
    % Map database type into an integer code
    case Type of
        btree -> TypeCode = ?DB_TYPE_BTREE;
        hash  -> TypeCode = ?DB_TYPE_HASH
    end,
    Flags = process_flags(lists:umerge(Opts, [auto_commit, threaded])),
    Cmd = <<Flags:32/native, TypeCode:8/signed-native, (list_to_binary(Name))/bytes, 0:8/native>>,
    case erlang:port_control(get_port(), ?CMD_OPEN_DB, Cmd) of
        <<?STATUS_OK:8, Db:32/signed-native>> ->
            {ok, Db};
        <<?STATUS_ERROR:8, Errno:32/signed-native>> ->
            {error, Errno}
    end.


-spec close(Db :: db()) -> ok | db_error().

close(Db) ->
    close(Db, []).


-spec close(Db :: db(), Opts :: db_flags()) -> ok | db_error().

close(Db, Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Db:32/signed-native, Flags:32/native>>,
    <<Rc:32/signed-native>> = erlang:port_control(get_port(), ?CMD_CLOSE_DB, Cmd),
    case decode_rc(Rc) of
        ok ->
            ok;
        Reason ->
            {error, Reason}
    end.


-spec txn_begin() -> ok | db_error().

txn_begin() ->
    txn_begin([]).


-spec txn_begin(Opts :: db_flags()) -> ok | db_error().

txn_begin(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_TXN_BEGIN, Cmd),
    case decode_rc(Result) of
        ok -> ok;
        Error -> {error, Error}
    end.


-spec txn_commit() -> ok | db_error().

txn_commit() ->
    txn_commit([]).


-spec txn_commit(Opts :: db_flags()) -> ok | db_error().

txn_commit(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_TXN_COMMIT, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                ok -> ok;
                {error, Reason} -> {error, decode_rc(Reason)}
            end;
        Error ->
            {error, Error}
    end.


-spec txn_abort() -> ok | db_error().

txn_abort() ->
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_TXN_ABORT, <<>>),
    case decode_rc(Result) of
        ok ->
            receive
                ok -> ok;
                {error, Reason} -> {error, decode_rc(Reason)}
            end;

        no_txn ->
            ok;

        Error ->
            {error, Error}
    end.


-spec transaction(Fun :: txn_fun()) -> {ok, db_value()} | db_error().

transaction(Fun) ->
    transaction(Fun, infinity).


-spec transaction(Fun :: txn_fun(), Retries :: txn_retries()) ->
    {ok, db_value()} | {error, db_error_reason() | {transaction_failed, term()}}.

transaction(_Fun, 0) ->
    ok = txn_abort(),
    {error, {transaction_failed, retry_limit_reached}};
transaction(Fun, Retries) ->
    case txn_begin() of
        ok ->
            try Fun() of
                abort ->
                    ok = txn_abort(),
                    {error, transaction_aborted};

                Value ->
                    case txn_commit() of
                        ok -> {ok, Value};
                        Error -> Error
                    end
            catch
                throw : {error, Error} when ?is_lock_error(Error) ->
                    ok = txn_abort(),
                    erlang:yield(),
                    R = case Retries of
                            infinity -> infinity;
                            Retries -> Retries - 1
                        end,
                    transaction(Fun, R);

                _ : Reason ->
                    ok = txn_abort(),
                    {error, {transaction_failed, Reason}}
            end;

        Error ->
            Error
    end.


-spec put(Db :: db(), Key :: db_key(), Value :: db_value()) ->
    ok | db_error().

put(Db, Key, Value) ->
    put(Db, Key, Value, []).


-spec put(Db :: db(), Key :: db_key(), Value :: db_value(), Opts :: db_flags()) ->
    ok | db_error().

put(Db, Key, Value, Opts) ->
    do_put(?CMD_PUT, Db, Key, Value, Opts).


-spec put_r(Db :: db(), Key :: db_key(), Value :: db_value()) -> ok.

put_r(Db, Key, Value) ->
    put_r(Db, Key, Value, []).


-spec put_r(Db :: db(), Key :: db_key(), Value :: db_value(), Opts :: db_flags()) -> ok.

put_r(Db, Key, Value, Opts) ->
    case do_put(?CMD_PUT, Db, Key, Value, Opts) of
        ok -> ok;
        Error -> throw(Error)
    end.


-spec put_commit(Db :: db(), Key :: db_key(), Value :: db_value()) ->
    ok | db_error().

put_commit(Db, Key, Value) ->
    put_commit(Db, Key, Value, []).


-spec put_commit(Db :: db(), Key :: db_key(), Value :: db_value(), Opts :: db_flags()) ->
    ok | db_error().

put_commit(Db, Key, Value, Opts) ->
    do_put(?CMD_PUT_COMMIT, Db, Key, Value, Opts).


-spec put_commit_r(Db :: db(), Key :: db_key(), Value :: db_value()) -> ok.

put_commit_r(Db, Key, Value) ->
    put_commit_r(Db, Key, Value, []).


-spec put_commit_r(Db :: db(), Key :: db_key(), Value :: db_value(), Opts :: db_flags()) -> ok.

put_commit_r(Db, Key, Value, Opts) ->
    case do_put(?CMD_PUT_COMMIT, Db, Key, Value, Opts) of
        ok -> ok;
        Error -> throw(Error)
    end.


-spec get(Db :: db(), Key :: db_key()) ->
    not_found | {ok, db_ret_value()} | db_error().

get(Db, Key) ->
    get(Db, Key, []).


-spec get(Db :: db(), Key :: db_key(), Opts :: db_flags()) ->
    not_found | {ok, db_ret_value()} | db_error().

get(Db, Key, Opts) ->
    {KeyLen, KeyBin} = to_binary(Key),
    Flags = process_flags(Opts),
    Cmd = <<Db:32/signed-native, Flags:32/native, KeyLen:32/native, KeyBin/bytes>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_GET, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                {ok, _, Bin} -> {ok, binary_to_term(Bin)};
                not_found -> not_found;
                {error, Reason} -> {error, decode_rc(Reason)}
            end;
        Error ->
            {error, Error}
    end.


-spec get_r(Db :: db(), Key :: db_key()) ->
    not_found | {ok, db_ret_value()}.

get_r(Db, Key) ->
    get_r(Db, Key, []).


-spec get_r(Db :: db(), Key :: db_key(), Opts :: db_flags()) ->
    not_found | {ok, db_ret_value()}.

get_r(Db, Key, Opts) ->
    case get(Db, Key, Opts) of
        {ok, Value} -> {ok, Value};
        not_found   -> not_found;
        Error       -> throw(Error)
    end.


-spec update(Db :: db(), Key :: db_key(), Fun :: db_update_fun()) ->
    {ok, db_value()} | db_error().

update(Db, Key, Fun) ->
    update(Db, Key, Fun, undefined).


-spec update(Db :: db(), Key :: db_key(), Fun :: db_update_fun(), Args :: db_update_fun_args()) ->
    {ok, db_value()} | db_error().

update(Db, Key, Fun, Args) ->
    F = fun() ->
            Value = case get_r(Db, Key, [rmw]) of
                        not_found -> not_found;
                        {ok, Val} -> Val
                    end,
            NewValue = case Args of
                           undefined -> Fun(Key, Value);
                           Args      -> Fun(Key, Value, Args)
                       end,
            put_r(Db, Key, NewValue),
            NewValue
        end,
    transaction(F).


-spec truncate() -> ok | db_error().

truncate() ->
    truncate(-1).


-spec truncate(Db :: -1 | db()) -> ok | db_error().

truncate(Db) ->
    Cmd = <<Db:32/signed-native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_TRUNCATE, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                ok -> ok;
                {error, Reason} -> {error, decode_rc(Reason)}
            end;

        Error ->
            {error, Error}
    end.


-spec cursor_open(Db :: db()) -> ok | db_error().

cursor_open(Db) ->
    Cmd = <<Db:32/signed-native, 0:32/native>>,
    <<Rc:32/signed-native>> = erlang:port_control(get_port(), ?CMD_CURSOR_OPEN, Cmd),
    case decode_rc(Rc) of
        ok ->
            ok;
        Reason ->
            {error, Reason}
    end.


-spec cursor_next() -> {ok, db_key(), db_value()} | not_found | db_error().

cursor_next() ->
    do_cursor_move(?CMD_CURSOR_NEXT).


-spec cursor_prev() -> {ok, db_key(), db_value()} | not_found | db_error().

cursor_prev() ->
    do_cursor_move(?CMD_CURSOR_PREV).


-spec cursor_current() -> {ok, db_key(), db_value()} | not_found | db_error().

cursor_current() ->
    do_cursor_move(?CMD_CURSOR_CURR).


-spec cursor_close() -> ok | db_error().

cursor_close() ->
    <<Rc:32/signed-native>> = erlang:port_control(get_port(), ?CMD_CURSOR_CLOSE, <<>>),
    case decode_rc(Rc) of
        ok ->
            ok;
        Reason ->
            {error, Reason}
    end.


-spec delete_database(Filename :: db_name()) ->
    ok | db_error().

delete_database(Filename) ->
    Cmd = <<(list_to_binary(Filename))/binary, 0:8>>,
    <<Rc:32/signed-native>> = erlang:port_control(get_port(), ?CMD_REMOVE_DB, Cmd),
    case decode_rc(Rc) of
        ok ->
            ok;
        Reason ->
            {error, Reason}
    end.


-spec get_data_dirs() -> [db_name(),...] | db_error().

get_data_dirs() ->
    % Call into the BDB library and get a list of configured data directories
    Cmd = <<?SYSP_DATA_DIR_GET:32/signed-native>>,
    <<Result:32/signed-native, Rest/bytes>> = erlang:port_control(get_port(), ?CMD_GETINFO, Cmd),
    case decode_rc(Result) of
        ok ->
            Dirs = [binary_to_list(D) || D <- split_bin(0, Rest, <<>>, [])],
            case os:getenv("DB_HOME") of
                false ->
                    Dirs;

                DbHome ->
                    % Make sure DB_HOME is part of the list
                    case lists:member(DbHome, Dirs) of
                        true  -> Dirs;
                        false -> [DbHome | Dirs]
                    end
            end;

        Reason ->
            {error, Reason}
    end.


-spec get_cache_size() ->
    {ok, non_neg_integer(), non_neg_integer(), non_neg_integer()} | db_error().

get_cache_size() ->
    Cmd = <<?SYSP_CACHESIZE_GET:32/signed-native>>,
    <<Result:32/signed-native, Gbytes:32/native, Bytes:32/native, Ncaches:32/native>> =
        erlang:port_control(get_port(), ?CMD_GETINFO, Cmd),
    case Result of
        0 ->
            {ok, Gbytes, Bytes, Ncaches};
        _ ->
            {error, decode_rc(Result)}
    end.


-spec get_txn_timeout() -> {ok, non_neg_integer()} | db_error().

get_txn_timeout() ->
    Cmd = <<?SYSP_TXN_TIMEOUT_GET:32/signed-native>>,
    <<Result:32/signed-native, Timeout:32/native>> = erlang:port_control(get_port(), ?CMD_GETINFO, Cmd),
    case Result of
        0 ->
            {ok, Timeout};
        _ ->
            {error, decode_rc(Result)}
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
decode_rc(Rc) when is_integer(Rc)    -> {unknown, Rc}.

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
%% Execute a PUT, using the provide "Action" to determine if it's a PUT or PUT_COMMIT
%%
do_put(Action, Db, Key, Value, Opts) ->
    {KeyLen, KeyBin} = to_binary(Key),
    {ValLen, ValBin} = to_binary(Value),
    Flags = process_flags(Opts),
    Cmd = <<Db:32/signed-native, Flags:32/native, KeyLen:32/native, KeyBin/bytes, ValLen:32/native, ValBin/bytes>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), Action, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                ok -> ok;
                {error, Reason} -> {error, decode_rc(Reason)}
            end;
        Error ->
            {error, Error}
    end.



%%
%% Move the cursor in a given direction. Invoked by cursor_next/prev/current.
%%
do_cursor_move(Direction) ->
    <<Rc:32/signed-native>> = erlang:port_control(get_port(), Direction, <<>>),
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



%%
%% Split a binary into pieces, using a single character delimiter
%%
split_bin(_Delimiter, <<>>, <<>>, Acc) ->
    lists:reverse(Acc);
split_bin(_Delimiter, <<>>, ItemAcc, Acc) ->
    lists:reverse([ItemAcc | Acc]);
split_bin(Delimiter, <<Delimiter:8, Rest/binary>>, ItemAcc, Acc) ->
    split_bin(Delimiter, Rest, <<>> ,[ItemAcc | Acc]);
split_bin(Delimiter, <<Other:8, Rest/binary>>, ItemAcc, Acc) ->
    split_bin(Delimiter, Rest, <<ItemAcc/binary, Other:8>>, Acc).
