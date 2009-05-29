%% -------------------------------------------------------------------
%% @doc
%% Erlang interface to BerkeleyDB.
%%
%% @copyright 2008-9 The Hive.  All rights reserved.
%% @end
%% -------------------------------------------------------------------
-module(bdberl).

-export([open/2, open/3,
         close/1, close/2,
         txn_begin/0, txn_begin/1,
         txn_commit/0, txn_commit/1, txn_abort/0,
         get_cache_size/0,
         get_data_dirs/0,
         get_data_dirs_info/0,
         get_lg_dir_info/0,
         get_txn_timeout/0,
         stat/1, stat/2,
         stat_print/1, stat_print/2,
         lock_stat/0, lock_stat/1,
         lock_stat_print/0, lock_stat_print/1,
         log_stat/0, log_stat/1,
         log_stat_print/0, log_stat_print/1,
         memp_stat/0, memp_stat/1,
         memp_stat_print/0, memp_stat_print/1,
         mutex_stat/0, mutex_stat/1,
         mutex_stat_print/0, mutex_stat_print/1,
         txn_stat/0, txn_stat/1,
         txn_stat_print/0, txn_stat_print/1,
         env_stat_print/0, env_stat_print/1, 
         transaction/1, transaction/2, transaction/3,
         put/3, put/4,
         put_r/3, put_r/4,
         put_commit/3, put_commit/4,
         put_commit_r/3, put_commit_r/4,
         get/2, get/3,
         get_r/2, get_r/3,
         update/3, update/4, update/5,
         truncate/0, truncate/1,
         delete_database/1,
         cursor_open/1, cursor_next/0, cursor_prev/0, cursor_current/0, cursor_close/0,
         register_logger/0]).

-include("bdberl.hrl").

-define(is_lock_error(Error), (Error =:= deadlock orelse Error =:= lock_not_granted)).

-type db() :: integer().
-type db_name() :: [byte(),...].
-type db_type() :: btree | hash.
-type db_flags() :: [atom()].
-type db_fsid() :: binary().
-type db_key() :: term().
-type db_mbytes() :: non_neg_integer().
-type db_value() :: term().
-type db_ret_value() :: not_found | db_value().
-type db_error_reason() :: atom() | {unknown, integer()}.
-type db_error() :: {error, db_error_reason()}.

-type db_txn_fun() :: fun(() -> term()).
-type db_txn_retries() :: infinity | non_neg_integer().
-type db_txn_error() :: {error, db_error_reason() | {transaction_failed, _}}.

-type db_update_fun() :: fun((db_key(), db_value(), any()) -> db_value()).
-type db_update_fun_args() :: undefined | [term()].


%%--------------------------------------------------------------------
%% @doc
%% Open a database file, creating it if it does not exist.
%%
%% @spec open(Name, Type) -> {ok, Db} | {error, Error}
%% where
%%    Name = string()
%%    Type = btree | hash
%%    Db = integer()
%%
%% @equiv open(Name, Type, [create])
%% @see open/3
%% @end
%%--------------------------------------------------------------------
-spec open(Name :: db_name(), Type :: db_type()) ->
    {ok, db()} | {error, integer()}.

open(Name, Type) ->
    open(Name, Type, [create]).


%%--------------------------------------------------------------------
%% @doc
%% Open a database file.
%%
%% This function opens the database represented by the `Name' parameter
%% for both reading and writing. The `Type' parameter specifies the
%% database file format. The currently supported file formats (or access
%% methods) are `btree' and `hash'. The `btree' format is a
%% representation of a sorted, balanced tree structure. The `hash'
%% format is an extensible, dynamic hashing scheme.
%%
%% Calling open is a relatively expensive operation, and maintaining a
%% set of open databases will normally be preferable to repeatedly
%% opening and closing the database for each new query.
%%
%% This function returns `{ok, Db}' on success. `Db' is a handle that
%% must be used when performing operations on this database. If open
%% fails, the close function is called automatically to discard the `Db'
%% handle.
%%
%% === Options ===
%%
%% The `Opts' parameter takes a list of flags that will modify the
%% behavior of bdberl when accessing this file. The following flags are
%% recognized:
%%
%% <dl>
%%   <dt>create</dt>
%%   <dd>Create the database if it does not exist.</dd>
%%   <dt>exclusive</dt>
%%   <dd>Return an error if the database already exists. Only
%%       meaningful when used with `create'.</dd>
%%   <dt>multiversion</dt>
%%   <dd>Open the database with support for multiversion concurrency
%%       protocol.</dd>
%%   <dt>no_mmap</dt>
%%   <dd>Do not map this database into process memory.</dd>
%%   <dt>readonly</dt>
%%   <dd>Open the database for reading only. Any attempt to modify items
%%       in the database will fail.</dd>
%%   <dt>read_uncommitted</dt>
%%   <dd>Support transactional read operations with degree 1
%%       isolation. Read operations on the database may request the
%%       return of modified but not yet committed data.</dd>
%%   <dt>truncate</dt>
%%   <dd>Physically truncate the underlying file, discarding all
%%       previous databases it might have held.</dd>
%% </dl>
%%
%% Additionally, the driver supports the `auto_commit' and `threaded'
%% flags which are always enabled. Specifying either flag in `Opts' is
%% safe, but does not alter the behavior of bdberl.
%%
%% @spec open(Name, Type, Opts) -> {ok, Db} | {error, Error}
%% where
%%    Name = string()
%%    Type = btree | hash | unknown
%%    Opts = [atom()]
%%    Db = integer()
%%
%% @end
%%--------------------------------------------------------------------
-spec open(Name :: db_name(), Type :: db_type() | unknown, Opts :: db_flags()) ->
    {ok, db()} | {error, integer()}.

open(Name, Type, Opts) ->
    % Map database type into an integer code
    case Type of
        btree -> TypeCode = ?DB_TYPE_BTREE;
        hash  -> TypeCode = ?DB_TYPE_HASH;
        unknown -> TypeCode = ?DB_TYPE_UNKNOWN %% BDB automatically determines if file exists
    end,
    Flags = process_flags(lists:umerge(Opts, [auto_commit, threaded])),
    Cmd = <<Flags:32/native, TypeCode:8/signed-native, (list_to_binary(Name))/bytes, 0:8/native>>,
    <<Rc:32/signed-native>> = erlang:port_control(get_port(), ?CMD_OPEN_DB, Cmd),
    case decode_rc(Rc) of
        ok ->
            receive 
                {ok, DbRef} ->
                    {ok, DbRef};
                {error, Reason} ->
                    {error, Reason}
            end;
        Error ->
            {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Close a database file with default options.
%%
%% @spec close(Db) -> ok | {error, Error}
%% where
%%    Db = integer()
%%
%% @equiv close(Db, [])
%% @see close/2
%% @end
%%--------------------------------------------------------------------
-spec close(Db :: db()) -> ok | db_error().

close(Db) ->
    close(Db, []).


%%--------------------------------------------------------------------
%% @doc
%% Close a database file.
%%
%% This function flushes any cached database information to disk, closes
%% any open cursors, frees any allocated resources, and closes any
%% underlying files.
%%
%% The `Db' handle should not be closed while any other handle that refers
%% to it is not yet closed; for example, database handles must not be
%% closed while cursor handles into the database remain open, or
%% transactions that include operations on the database have not yet
%% been committed or aborted.
%%
%% Because key/data pairs are cached in memory, failing to sync the file
%% with the `close' function may result in inconsistent or lost
%% information.
%%
%% The `Db' handle may not be accessed again after this function is
%% called, regardless of its return.
%%
%% === Options ===
%%
%% <dl>
%%   <dt>no_sync</dt>
%%   <dd>Do not flush cached information to disk.</dd>
%% </dl>
%%
%% @spec close(Db, Opts) -> ok | {error, Error}
%% where
%%    Db = integer()
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec close(Db :: db(), Opts :: db_flags()) -> ok | db_error().

close(Db, Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Db:32/signed-native, Flags:32/native>>,
    <<Rc:32/signed-native>> = erlang:port_control(get_port(), ?CMD_CLOSE_DB, Cmd),
    case decode_rc(Rc) of
        ok ->
            receive 
                ok ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        Error ->
            {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Begin a transaction with default options.
%%
%% @spec txn_begin() -> ok | {error, Error}
%%
%% @equiv txn_begin([])
%% @see txn_begin/1
%% @end
%%--------------------------------------------------------------------
-spec txn_begin() -> ok | db_error().

txn_begin() ->
    txn_begin([txn_snapshot]).


%%--------------------------------------------------------------------
%% @doc
%% Begin a transaction.
%%
%% This function creates a new transaction in the environment. Calling
%% the `abort' or `commit' functions will end the transaction.
%%
%% Transactions may only span threads if they do so serially; that is,
%% each transaction must be active in only a single thread of control at
%% a time. This restriction holds for parents of nested transactions as
%% well; no two children may be concurrently active in more than one
%% thread of control at any one time.
%%
%% Cursors may not span transactions; that is, each cursor must be
%% opened and closed within a single transaction.
%%
%% A parent transaction may not issue any Berkeley DB operations --
%% except for `txn_begin', `abort' and `commit' -- while it has active
%% child transactions (child transactions that have not yet been
%% committed or aborted).
%%
%% === Options ===
%%
%% <dl>
%%   <dt>read_committed</dt>
%%   <dd>This transaction will have degree 2 isolation. This provides
%%       for cursor stability but not repeatable reads. Data items which
%%       have been previously read by this transaction may be deleted or
%%       modified by other transactions before this transaction
%%       completes.</dd>
%%   <dt>read_uncommitted</dt>
%%   <dd>This transaction will have degree 1 isolation. Read operations
%%       performed by the transaction may read modified but not yet
%%       committed data. Silently ignored if the `read_uncommitted'
%%       option was not specified when the underlying database was
%%       opened.</dd>
%%   <dt>txn_no_sync</dt>
%%   <dd>Do not synchronously flush the log when this transaction
%%       commits. This means the transaction will exhibit the ACI
%%       (atomicity, consistency, and isolation) properties, but not D
%%       (durability); that is, database integrity will be maintained
%%       but it is possible that this transaction may be undone during
%%       recovery.</dd>
%%   <dt>txn_no_wait</dt>
%%   <dd>If a lock is unavailable for any Berkeley DB operation
%%       performed in the context of this transaction, cause the
%%       operation to return `deadlock' or `lock_not_granted'.</dd>
%%   <dt>txn_snapshot</dt>
%%   <dd>This transaction will execute with snapshot isolation. For
%%       databases with the `multiversion' flag set, data values will be
%%       read as they are when the transaction begins, without taking
%%       read locks. Silently ignored for operations on databases with
%%       `multiversion' not set on the underlying database (read locks
%%       are acquired). The error `deadlock' will be returned from
%%       update operations if a snapshot transaction attempts to update
%%       data which was modified after the snapshot transaction read it.
%%       </dd>
%%    <dt>txn_sync</dt>
%%    <dd>Synchronously flush the log when this transaction commits.
%%        This means the transaction will exhibit all of the ACID
%%        (atomicity, consistency, isolation, and durability)
%%        properties. This behavior is the default.</dd>
%%    <dt>txn_wait</dt>
%%    <dd>If a lock is unavailable for any operation performed in the
%%        context of this transaction, wait for the lock. This behavior
%%        is the default.</dd>
%%    <dt>txn_write_nosync</dt>
%%    <dd>Write, but do not synchronously flush, the log when this
%%        transaction commits. This means the transaction will exhibit
%%        the ACI (atomicity, consistency, and isolation) properties,
%%        but not D (durability); that is, database integrity will be
%%        maintained, but if the system fails, it is possible some
%%        number of the most recently committed transactions may be
%%        undone during recovery. The number of transactions at risk is
%%        governed by how often the system flushes dirty buffers to disk
%%        and how often the log is flushed or checkpointed.</dd>
%% </dl>
%%
%% @spec txn_begin(Opts) -> ok | {error, Error}
%% where
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec txn_begin(Opts :: db_flags()) -> ok | db_error().

txn_begin(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_TXN_BEGIN, Cmd),
    case decode_rc(Result) of
        ok -> 
            receive
                ok -> ok;
                {error, Reason} -> {error, decode_rc(Reason)}
            end;
        Error -> {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Commit a transaction with default options.
%%
%% @spec txn_commit() -> ok | {error, Error}
%%
%% @equiv txn_commit([])
%% @see txn_commit/1
%% @end
%%--------------------------------------------------------------------
-spec txn_commit() -> ok | db_error().

txn_commit() ->
    txn_commit([]).


%%--------------------------------------------------------------------
%% @doc
%% Commit a transaction.
%%
%% This function ends the transaction. In the case of nested
%% transactions, if the transaction is a parent transaction, committing
%% the parent transaction causes all unresolved children of the parent
%% to be committed. In the case of nested transactions, if the
%% transaction is a child transaction, its locks are not released, but
%% are acquired by its parent. Although the commit of the child
%% transaction will succeed, the actual resolution of the child
%% transaction is postponed until the parent transaction is committed or
%% aborted; that is, if its parent transaction commits, it will be
%% committed; and if its parent transaction aborts, it will be aborted.
%%
%% All cursors opened within the transaction must be closed before the
%% transaction is committed.
%%
%% === Options ===
%%
%% <dl>
%%   <dt>txn_no_sync</dt>
%%   <dd>Do not synchronously flush the log. This means the transaction
%%       will exhibit the ACI (atomicity, consistency, and isolation)
%%       properties, but not D (durability); that is, database integrity
%%       will be maintained but it is possible that this transaction may
%%       be undone during recovery.</dd>
%%    <dt>txn_sync</dt>
%%    <dd>Synchronously flush the log. This means the transaction will
%%        exhibit all of the ACID (atomicity, consistency, isolation,
%%        and durability) properties. This behavior is the default.</dd>
%% </dl>
%%
%% @spec txn_commit(Opts) -> ok | {error, Error}
%% where
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
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


%%--------------------------------------------------------------------
%% @doc
%% Abort a transaction.
%%
%% This function causes an abnormal termination of the transaction. The
%% log is played backward, and any necessary undo operations are
%% performed. Before this function returns, any locks held by the
%% transaction will have been released.
%%
%% In the case of nested transactions, aborting a parent transaction
%% causes all children (unresolved or not) of the parent transaction to
%% be aborted.
%%
%% All cursors opened within the transaction must be closed before the
%% transaction is aborted.
%%
%% @spec txn_abort() -> ok | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec txn_abort() -> ok | db_error().

txn_abort() ->
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_TXN_ABORT, <<>>),
    case decode_rc(Result) of
        ok ->
            receive
                ok -> ok;
                {error, no_txn} -> ok;
                {error, Reason} -> {error, decode_rc(Reason)}
            end;

        no_txn ->
            ok;

        Error ->
            {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Execute a fun inside of a transaction.
%%
%% @spec transaction(Fun) -> {ok, Value} | {error, Error}
%% where
%%    Fun = function()
%%
%% @equiv transaction(Fun, infinity, [])
%% @see transaction/3
%% @end
%%--------------------------------------------------------------------
-spec transaction(Fun :: db_txn_fun()) -> {ok, db_value()} | db_txn_error().

transaction(Fun) ->
    transaction(Fun, infinity).


%%--------------------------------------------------------------------
%% @doc
%% Execute a fun inside of a transaction.
%%
%% @spec transaction(Fun, Retries) -> {ok, Value} | {error, Error}
%% where
%%    Fun = function()
%%    Retries = infinity | integer()
%%
%% @equiv transaction(Fun, Retries, [])
%% @see transaction/3
%% @end
%%--------------------------------------------------------------------
-spec transaction(Fun :: db_txn_fun(), Retries :: db_txn_retries()) ->
    {ok, db_value()} | db_txn_error().

transaction(Fun, Retries) ->
    transaction(Fun, Retries, []).


%%--------------------------------------------------------------------
%% @doc
%% Execute a fun inside of a transaction.
%%
%% This function executes a user-supplied function inside the scope of a
%% transaction. If the function returns normally, then the transaction
%% is committed and the value returned by the function is returned. If
%% the function returns the value `abort' then the transaction will be
%% aborted.
%%
%% If the transaction deadlocks on commit, the function will be executed
%% again until the commit succeeds or the number of retries exceeds the
%% value of the `Retries' parameter.
%%
%% The 'Opts' parameter is used to set flags for the call to txn_begin/1.
%%
%% @spec transaction(Fun, Retries, Opts) -> {ok, Value} | {error, Error}
%% where
%%    Fun = function()
%%    Retries = infinity | integer()
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec transaction(Fun :: db_txn_fun(), Retries :: db_txn_retries(), Opts :: db_flags()) ->
    {ok, db_value()} | db_txn_error().

transaction(_Fun, 0, _Opts) ->
    {error, {transaction_failed, retry_limit_reached}};

transaction(Fun, Retries, Opts) ->
    case txn_begin(Opts) of
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


%%--------------------------------------------------------------------
%% @doc
%% Store a value in a database file.
%%
%% @spec put(Db, Key, Value) -> ok | {error, Error}
%% where
%%    Db = integer()
%%    Key = any()
%%    Value = any()
%%
%% @equiv put(Db, Key, Value, [])
%% @see put/4
%% @end
%%--------------------------------------------------------------------
-spec put(Db :: db(), Key :: db_key(), Value :: db_value()) ->
    ok | db_error().

put(Db, Key, Value) ->
    put(Db, Key, Value, []).


%%--------------------------------------------------------------------
%% @doc
%% Store a value in a database file.
%%
%% This function stores key/data pairs in the database. The default
%% behavior is to enter the new key/data pair, replacing any previously
%% existing key if duplicates are disallowed, or adding a duplicate data
%% item if duplicates are allowed. If the database supports duplicates,
%% this function adds the new data value at the end of the duplicate
%% set. If the database supports sorted duplicates, the new data value
%% is inserted at the correct sorted location.
%%
%% === Options ===
%%
%% <dl>
%%   <dt>no_duplicate</dt>
%%   <dd>enter the new key/data pair only if it does not already appear
%%       in the database. This option may only be specified if the
%%       underlying database has been configured to support sorted
%%       duplicates. The `put' function will return `key_exist' if
%%       this option is set and the key/data pair already appears in the
%%       database.</dd>
%%   <dt>no_overwrite</dt>
%%   <dd>Enter the new key/data pair only if the key does not already
%%       appear in the database. The `put' function call with this
%%       option set will fail if the key already exists in the database,
%%       even if the database supports duplicates. In this case the
%%       `put' function will return `key_exist'.</dd>
%% </dl>
%%
%% @spec put(Db, Key, Value, Opts) -> ok | {error, Error}
%% where
%%    Db = integer()
%%    Key = any()
%%    Value = any()
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec put(Db :: db(), Key :: db_key(), Value :: db_value(), Opts :: db_flags()) ->
    ok | db_error().

put(Db, Key, Value, Opts) ->
    do_put(?CMD_PUT, Db, Key, Value, Opts).


%%--------------------------------------------------------------------
%% @doc
%% Store a value in a database file raising an exception on failure.
%%
%% @spec put_r(Db, Key, Value) -> ok
%% where
%%    Db = integer()
%%    Key = any()
%%    Value = any()
%%
%% @throws {error, Error}
%%
%% @equiv put_r(Db, Key, Value, [])
%% @see put_r/4
%% @end
%%--------------------------------------------------------------------
-spec put_r(Db :: db(), Key :: db_key(), Value :: db_value()) -> ok.

put_r(Db, Key, Value) ->
    put_r(Db, Key, Value, []).


%%--------------------------------------------------------------------
%% @doc
%% Store a value in a database file raising an exception on failure.
%%
%% This function is the same as put/4 except that it raises an exception
%% on failure instead of returning an error tuple.
%%
%% @see put/4
%%
%% @spec put_r(Db, Key, Value, Opts) -> ok
%% where
%%    Db = integer()
%%    Key = any()
%%    Value = any()
%%    Opts = [atom()]
%%
%% @throws {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec put_r(Db :: db(), Key :: db_key(), Value :: db_value(), Opts :: db_flags()) -> ok.

put_r(Db, Key, Value, Opts) ->
    case do_put(?CMD_PUT, Db, Key, Value, Opts) of
        ok -> ok;
        Error -> throw(Error)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Store a value in a database file and commit the transaction.
%%
%% @spec put_commit(Db, Key, Value) -> ok | {error, Error}
%% where
%%    Db = integer()
%%    Key = any()
%%    Value = any()
%%
%% @equiv put_commit(Db, Key, Value, [])
%% @see put_commit/4
%% @end
%%--------------------------------------------------------------------
-spec put_commit(Db :: db(), Key :: db_key(), Value :: db_value()) ->
    ok | db_error().

put_commit(Db, Key, Value) ->
    put_commit(Db, Key, Value, []).


%%--------------------------------------------------------------------
%% @doc
%% Store a value in a database file and commit the transaction.
%%
%% This function is logically the equicalent of calling `put' followed
%% by `txn_commit' except that it is implemented with the driver and is
%% therefore more efficient.
%%
%% @spec put_commit(Db, Key, Value, Opts) -> ok | {error, Error}
%% where
%%    Db = integer()
%%    Key = any()
%%    Value = any()
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec put_commit(Db :: db(), Key :: db_key(), Value :: db_value(), Opts :: db_flags()) ->
    ok | db_error().

put_commit(Db, Key, Value, Opts) ->
    do_put(?CMD_PUT_COMMIT, Db, Key, Value, Opts).


%%--------------------------------------------------------------------
%% @doc
%% Store a value in a database file and commit the transaction.
%%
%% @spec put_commit_r(Db, Key, Value) -> ok
%% where
%%    Db = integer()
%%    Key = any()
%%    Value = any()
%%
%% @throws {error, Error}
%%
%% @equiv put_commit_r(Db, Key, Value, [])
%% @see put_commit_r/4
%% @end
%%--------------------------------------------------------------------
-spec put_commit_r(Db :: db(), Key :: db_key(), Value :: db_value()) -> ok.

put_commit_r(Db, Key, Value) ->
    put_commit_r(Db, Key, Value, []).


%%--------------------------------------------------------------------
%% @doc
%% Store a value in a database file and commit the transaction.
%%
%% This function is the same as put_commit/4 except that it raises an
%% exception on failure instead of returning an error tuple.
%%
%% @see put_commit/4
%%
%% @spec put_commit_r(Db, Key, Value, Opts) -> ok
%% where
%%    Db = integer()
%%    Key = any()
%%    Value = any()
%%    Opts = [atom()]
%%
%% @throws {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec put_commit_r(Db :: db(), Key :: db_key(), Value :: db_value(), Opts :: db_flags()) -> ok.

put_commit_r(Db, Key, Value, Opts) ->
    case do_put(?CMD_PUT_COMMIT, Db, Key, Value, Opts) of
        ok -> ok;
        Error -> throw(Error)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieve a value based on key.
%%
%% @spec get(Db, Key) -> not_found | {ok, Value} | {error, Error}
%% where
%%    Db = integer()
%%    Key = term()
%%
%% @equiv get(Db, Key, [])
%% @see get/3
%% @end
%%--------------------------------------------------------------------
-spec get(Db :: db(), Key :: db_key()) ->
    not_found | {ok, db_ret_value()} | db_error().

get(Db, Key) ->
    get(Db, Key, []).


%%--------------------------------------------------------------------
%% @doc
%% Retrieve a value based on key.
%%
%% This function retrieves key/data pairs from the database. In the
%% presence of duplicate key values, this function will return the first
%% data item for the designated key. Duplicates are sorted by insert
%% order, except where this order has been overridden by cursor
%% operations. Retrieval of duplicates requires the use of cursor
%% operations.
%%
%% This function will return `not_found' if the specified key is not in
%% the database.
%%
%% === Options ===
%%
%% <dl>
%%   <dt>read_committed</dt>
%%   <dd>Configure a transactional get operation to have degree 2
%%       isolation (the read is not repeatable).</dd>
%%   <dt>read_uncommitted</dt>
%%   <dd>Configure a transactional get operation to have degree 1
%%       isolation, reading modified but not yet committed data.
%%       Silently ignored if the `read_uncommitted' flag was not
%%       specified when the underlying database was opened.</dd>
%%   <dt>rmw</dt>
%%   <dd>Acquire write locks instead of read locks when doing the read,
%%       if locking is configured. Setting this flag can eliminate
%%       deadlock during a read-modify-write cycle by acquiring the
%%       write lock during the read part of the cycle so that another
%%       thread of control acquiring a read lock for the same item, in
%%       its own read-modify-write cycle, will not result in deadlock.
%%       This option is meaningful only in the presence of transactions.
%%       </dd>
%% </dl>
%%
%% @spec get(Db, Key, Opts) -> not_found | {ok, Value} | {error, Error}
%% where
%%    Db = integer()
%%    Key = term()
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
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


%%--------------------------------------------------------------------
%% @doc
%% Retrieve a value based on key.
%%
%% @spec get_r(Db, Key) -> not_found | {ok, Value}
%% where
%%    Db = integer()
%%    Key = term()
%%
%% @throws {error, Error}
%%
%% @equiv get_r(Db, Key, [])
%% @see get_r/3
%% @end
%%--------------------------------------------------------------------
-spec get_r(Db :: db(), Key :: db_key()) ->
    not_found | {ok, db_ret_value()}.

get_r(Db, Key) ->
    get_r(Db, Key, []).


%%--------------------------------------------------------------------
%% @doc
%% Retrieve a value based on key.
%%
%% This function is the same as get/3 except that it raises an
%% exception on failure instead of returning an error tuple.
%%
%% @see get/3
%%
%% @spec get_r(Db, Key, Opts) -> not_found | {ok, Value}
%% where
%%    Db = integer()
%%    Key = term()
%%    Opts = [atom()]
%%
%% @throws {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec get_r(Db :: db(), Key :: db_key(), Opts :: db_flags()) ->
    not_found | {ok, db_ret_value()}.

get_r(Db, Key, Opts) ->
    case get(Db, Key, Opts) of
        {ok, Value} -> {ok, Value};
        not_found   -> not_found;
        Error       -> throw(Error)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Updates the value of a key by executing a fun.
%%
%% @spec update(Db, Key, Fun) -> {ok, Value} | {error, Error}
%% where
%%    Db = integer()
%%    Key = term()
%%    Fun = function()
%%
%% @equiv update(Db, Key, Fun, undefined, [])
%% @see update/5
%% @end
%%--------------------------------------------------------------------
-spec update(Db :: db(), Key :: db_key(), Fun :: db_update_fun()) ->
    {ok, db_value()} | db_txn_error().

update(Db, Key, Fun) ->
    update(Db, Key, Fun, undefined, []).


%%--------------------------------------------------------------------
%% @doc
%% Updates the value of a key by executing a fun.
%%
%% @spec update(Db, Key, Fun, Args) -> {ok, Value} | {error, Error}
%% where
%%    Db = integer()
%%    Key = term()
%%    Fun = function()
%%    Args = any()
%%
%% @equiv update(Db, Key, Fun, undefined, [])
%% @see update/5
%% @end
%%--------------------------------------------------------------------
-spec update(Db :: db(), Key :: db_key(), Fun :: db_update_fun(), Args :: db_update_fun_args()) ->
    {ok, db_value()} | db_txn_error().

update(Db, Key, Fun, Args) ->
    update(Db, Key, Fun, Args, []).


%%--------------------------------------------------------------------
%% @doc
%% Updates the value of a key by executing a fun.
%%
%% This function updates a key/value pair by calling the provided
%% function and passing in the key and value as they are
%% currently. Additional arguments can be provided to the function by
%% passing them in the `Args' parameter. The entire operation is
%% executed within the scope of a new transaction.
%%
%% The 'Opts' parameter is used to pass flags to transaction/3.
%%
%% @spec update(Db, Key, Fun, Args, Opts) -> {ok, Value} | {error, Error}
%% where
%%    Db = integer()
%%    Key = term()
%%    Fun = function()
%%    Args = any()
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec update(Db :: db(), Key :: db_key(), Fun :: db_update_fun(), Args :: db_update_fun_args(), Opts :: db_flags()) ->
    {ok, db_value()} | db_txn_error().

update(Db, Key, Fun, Args, Opts) ->
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
    transaction(F, infinity, Opts).


%%--------------------------------------------------------------------
%% @doc
%% Truncates all of the open databases.
%%
%% This function is equivalent to calling truncate/1 for every open
%% database.
%%
%% @spec truncate() -> ok | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec truncate() -> ok | db_error().

truncate() ->
    truncate(-1).


%%--------------------------------------------------------------------
%% @doc
%% Truncates a database.
%%
%% This function empties the database, discarding all records it
%% contains.
%%
%% It is an error to call this function on a database with open cursors.
%%
%% @spec truncate(Db) -> ok | {error, Error}
%% where
%%    Db = integer()
%%
%% @end
%%--------------------------------------------------------------------
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


%%--------------------------------------------------------------------
%% @doc
%% Opens a cursor on a database.
%%
%% This function creates a database cursor. Cursors may span threads,
%% but only serially, that is, the application must serialize access to
%% the cursor handle.
%%
%% @spec cursor_open(Db) -> ok | {error, Error}
%% where
%%    Db = integer()
%%
%% @end
%%--------------------------------------------------------------------
-spec cursor_open(Db :: db()) -> ok | db_error().

cursor_open(Db) ->
    Cmd = <<Db:32/signed-native, 0:32/native>>,
    <<Rc:32/signed-native>> = erlang:port_control(get_port(), ?CMD_CURSOR_OPEN, Cmd),
    case decode_rc(Rc) of
        ok ->
            receive
                ok -> ok;
                {error, Reason} -> {error, Reason}
            end;

        Reason ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves key/data pairs from the database.
%%
%% The cursor is moved to the next key/data pair of the database, and
%% that pair is returned. In the presence of duplicate key values, the
%% value of the key may not change.
%%
%% Modifications to the database during a sequential scan will be
%% reflected in the scan; that is, records inserted behind a cursor will
%% not be returned while records inserted in front of a cursor will be
%% returned.
%%
%% If this function fails for any reason, the state of the cursor will
%% be unchanged.
%%
%% @spec cursor_next() -> not_found | {ok, Key, Value} | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec cursor_next() -> {ok, db_key(), db_value()} | not_found | db_error().

cursor_next() ->
    do_cursor_move(?CMD_CURSOR_NEXT).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves key/data pairs from the database.
%%
%% The cursor is moved to the previous key/data pair of the database,
%% and that pair is returned. In the presence of duplicate key values,
%% the value of the key may not change.
%%
%% Modifications to the database during a sequential scan will be
%% reflected in the scan; that is, records inserted behind a cursor will
%% not be returned while records inserted in front of a cursor will be
%% returned.
%%
%% If this function fails for any reason, the state of the cursor will
%% be unchanged.
%%
%% @spec cursor_prev() -> not_found | {ok, Key, Value} | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec cursor_prev() -> {ok, db_key(), db_value()} | not_found | db_error().

cursor_prev() ->
    do_cursor_move(?CMD_CURSOR_PREV).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves key/data pairs from the database.
%%
%% Returns the key/data pair to which the cursor refers.
%%
%% Modifications to the database during a sequential scan will be
%% reflected in the scan; that is, records inserted behind a cursor will
%% not be returned while records inserted in front of a cursor will be
%% returned.
%%
%% If this function fails for any reason, the state of the cursor will
%% be unchanged.
%%
%% @spec cursor_current() -> not_found | {ok, Key, Value} | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec cursor_current() -> {ok, db_key(), db_value()} | not_found | db_error().

cursor_current() ->
    do_cursor_move(?CMD_CURSOR_CURR).


%%--------------------------------------------------------------------
%% @doc
%% Closes the cursor.
%%
%% This function discards the cursor.
%%
%% It is possible for this function to return `deadlock', signaling that
%% any enclosing transaction should be aborted. If the application is
%% already intending to abort the transaction, this error should be
%% ignored, and the application should proceed.
%%
%% After this function has been called, regardless of its return, the
%% cursor handle may not be used again.
%%
%% @spec cursor_close() -> ok | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec cursor_close() -> ok | db_error().

cursor_close() ->
    <<Rc:32/signed-native>> = erlang:port_control(get_port(), ?CMD_CURSOR_CLOSE, <<>>),
    case decode_rc(Rc) of
        ok ->
            receive
                ok -> ok;
                {error, Reason} -> {error, Reason}
            end;
        Reason ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Delete a database file.
%%
%% This function removes the database specified by the filename
%% parameter.
%%
%% Applications should never remove databases with open handles. For
%% example, some architectures do not permit the removal of files with
%% open system handles. On these architectures, attempts to remove
%% databases currently in use by any thread of control in the system may
%% fail.
%%
%% @spec delete_database(Filename) -> ok | {error, Error}
%% where
%%    Filename = string()
%%
%% @end
%%--------------------------------------------------------------------
-spec delete_database(Filename :: db_name()) ->
    ok | db_error().

delete_database(Filename) ->
    Cmd = <<(list_to_binary(Filename))/binary, 0:8>>,
    <<Rc:32/signed-native>> = erlang:port_control(get_port(), ?CMD_REMOVE_DB, Cmd),
    case decode_rc(Rc) of
        ok ->
            receive
                ok -> ok;
                {error, Reason} -> {error, decode_rc(Reason)}
            end;
        Reason ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns the list of directories that bdberl searches to find databases.
%%
%% @spec get_data_dirs() -> [DirName] | {error, Error}
%% where
%%    DirName = string()
%%
%% @end
%%--------------------------------------------------------------------
-spec get_data_dirs() -> [string()] | db_error().

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


%%--------------------------------------------------------------------
%% @doc
%% Returns the list of directories that bdberl searches to find databases
%% with the number of megabytes available for each dir
%%
%% @spec get_data_dirs_info() -> {ok, [DirName, Fsid, MbytesAvail]} | {error, Error}
%% where
%%    DirName = string()
%%    Fsid = binary()
%%    MbytesAvail = integer()
%%
%% @end
%%--------------------------------------------------------------------
-spec get_data_dirs_info() -> {ok, [{string(), db_fsid(), db_mbytes()}]} | db_error().

get_data_dirs_info() ->
    % Call into the BDB library and get a list of configured data directories
    Cmd = <<>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(),?CMD_DATA_DIRS_INFO, Cmd),
    case decode_rc(Result) of
        ok ->
            recv_dirs_info([]);
        Reason ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns the log directory info (name and megabytes available)
%%
%% @spec get_lg_dir_info() -> {ok, DirName, Fsid, MbytesAvail} | {error, Error}
%% where
%%    DirName = string()
%%    Fsid = binary()
%%    MbytesAvail = integer()
%%
%% @end
%%--------------------------------------------------------------------
-spec get_lg_dir_info() -> {ok, string(), db_fsid(), db_mbytes()} | db_error().
get_lg_dir_info() ->
    % Call into the BDB library and get the log dir and filesystem info
    Cmd = <<>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_LOG_DIR_INFO, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                {dirinfo, Path, FsId, MbytesAvail} ->
                    {ok, Path, FsId, MbytesAvail};
                {error, Reason} ->
                    {error, Reason}
            end;
        Reason ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns the size of the in-memory cache.
%%
%% The cache size is returned as a three element tuple representing the
%% number of gigabytes and megabytes and the number of caches.
%%
%% @spec get_cache_size() -> {ok, Gbytes, Mbytes, NumCaches} | {error, Error}
%% where
%%    Gbytes = integer()
%%    Mbytes = integer()
%%    NumCaches = integer()
%%
%% @end
%%--------------------------------------------------------------------
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


%%--------------------------------------------------------------------
%% @doc
%% Returns the transaction timeout value.
%%
%% @spec get_txn_timeout() -> {ok, Timeout} | {error, Error}
%% where
%%    Timeout = integer()
%%
%% @end
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% @doc
%% Retrieve database stats
%%
%% This function retrieves database statistics 
%%
%% === Options ===
%%
%% <dl>
%%   <dt>fast_stat</dt>
%%   <dd>Return only the values which do not require traversal of the database. 
%%       Among  other things, this flag makes it possible for applications to
%%       request key and record counts without incurring the performance 
%%       penalty of traversing the entire database.</dd>
%%   <dt>read_committed</dt>
%%   <dd>Database items read during a transactional call will have degree 2 
%%       isolation. This ensures the stability of the data items read during
%%       the stat operation but permits that data to be modified or deleted by
%%       other transactions prior to the commit of the specified 
%%       transaction.</dd>
%%   <dt>read_uncommitted</dt>
%%   <dd>Database items read during a transactional call will have degree 1 
%%       isolation, including modified but not yet committed data. Silently 
%%       ignored if the read_committed flag was not specified when the 
%%       underlying database was opened.</dd>
%% </dl>
%%
%% @spec stat(Db, Opts) -> {ok, [{atom(), number()}]} | {error, Error}
%% where
%%    Db = integer()
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec stat(Db :: db(), Opts :: db_flags()) ->
    {ok, [{atom(), number()}]} | db_error().

stat(Db, Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Db:32/signed-native, Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_DB_STAT, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                {error, Reason} ->
                    {error, decode_rc(Reason)};
                {ok, Stats} ->
                    {ok, Stats}
            end;
        Error ->
            {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieve database stats with empty flags
%%
%% @spec stat(Db) -> {ok, [{atom(), number()}]} | {error, Error}
%% where
%%    Db = integer()
%%
%% @end
%%--------------------------------------------------------------------
-spec stat(Opts :: db_flags()) ->
    {ok, [{atom(), number()}]} | db_error().
stat(Db) ->
    stat(Db, []).


%%--------------------------------------------------------------------
%% @doc
%% Print database stats
%%
%% This function prints statistics from the database to wherever
%% BDB messages are being sent
%%
%% === Options ===
%%
%% <dl>
%%   <dt>fast_stat</dt>
%%   <dd>Return only the values which do not require traversal of the database. 
%%       Among  other things, this flag makes it possible for applications to
%%       request key and record counts without incurring the performance 
%%       penalty of traversing the entire database.</dd>
%%   <dt>read_committed</dt>
%%   <dd>Database items read during a transactional call will have degree 2 
%%       isolation. This ensures the stability of the data items read during
%%       the stat operation but permits that data to be modified or deleted by
%%       other transactions prior to the commit of the specified transaction.
%%       </dd>
%%   <dt>read_uncommitted</dt>
%%   <dd>Database items read during a transactional call will have degree 1 
%%       isolation, including modified but not yet committed data. Silently 
%%       ignored if the read_committed flag was not specified when the 
%%       underlying database was opened.</dd>
%% </dl>
%%
%% @spec stat_print(Db, Opts) -> ok | {error, Error}
%% where
%%    Db = integer()
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec stat_print(Db :: db(), Opts :: db_flags()) ->
    ok | db_error().
stat_print(Db, Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Db:32/signed-native, Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_DB_STAT_PRINT, Cmd),
    case decode_rc(Result) of
        ok -> ok;
        Error -> {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Print database stats with empty flags
%%
%% @spec stat_print(Db) -> ok | {error, Error}
%% where
%%    Db = integer()
%%
%% @end
%%--------------------------------------------------------------------
-spec stat_print(Db :: db()) ->
    ok | db_error().
stat_print(Db) ->
    stat_print(Db, []).

%%--------------------------------------------------------------------
%% @doc
%% Retrieve lock stats
%%
%% This function retrieves lock statistics from the database.
%%
%% === Options ===
%%
%% <dl>
%%   <dt>stat_clear</dt>
%%   <dd>Reset statistics after returning their values</dd>
%% </dl>
%%
%% @spec lock_stat(Opts) -> {ok, [{atom(), number()}]} | {error, Error}
%% where
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec lock_stat(Opts :: db_flags()) ->
    {ok, [{atom(), number()}]} | db_error().

lock_stat(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_LOCK_STAT, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                {error, Reason} ->
                    {error, decode_rc(Reason)};
                {ok, Stats} ->
                    {ok, Stats}
            end;
        Error ->
            {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieve lock stats with no flags
%%
%% @spec lock_stat() -> {ok, [{atom(), number()}]} | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec lock_stat() ->
    {ok, [{atom(), number()}]} | db_error().
lock_stat() ->
    lock_stat([]).

%%--------------------------------------------------------------------
%% @doc
%% Print lock stats
%%
%% This function prints lock statistics to wherever
%% BDB messages are being sent
%%
%% === Options ===
%%
%% <dl>
%%   <dt>stat_all</dt>
%%   <dd>Display all available information.</dd>
%%   <dt>stat_clear</dt>
%%   <dd>Reset statistics after displaying their values.</dd>
%%   <dt>stat_lock_conf</dt>
%%   <dd>Display the lock conflict matrix.</dd>
%%   <dt>stat_lock_lockers</dt>
%%   <dd>Display the lockers within hash chains.</dd>
%%   <dt>stat_lock_objects</dt>
%%   <dd>Display the lock objects within hash chains.</dd>
%%   <dt>stat_lock_params</dt>
%%   <dd>Display the locking subsystem parameters.</dd>
%% </dl>
%%
%% @spec lock_stat_print(Opts) -> ok | {error, Error}
%% where
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec lock_stat_print(Opts :: db_flags()) ->
    ok | db_error().
lock_stat_print(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_LOCK_STAT_PRINT, Cmd),
    case decode_rc(Result) of
        ok -> ok;
        Error -> {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Print lock stats with empty flags
%%
%% @spec lock_stat_print() -> ok | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec lock_stat_print() ->
    ok | db_error().
lock_stat_print() ->
    lock_stat_print([]).


%%--------------------------------------------------------------------
%% @doc
%% Retrieve log stats
%%
%% This function retrieves bdb log statistics
%%
%% === Options ===
%%
%% <dl>
%%   <dt>stat_clear</dt>
%%   <dd>Reset statistics after returning their values</dd>
%% </dl>
%%
%% @spec log_stat(Opts) -> {ok, [{atom(), number()}]} | {error, Error}
%% where
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec log_stat(Opts :: db_flags()) ->
    {ok, [{atom(), number()}]} | db_error().

log_stat(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_LOG_STAT, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                {error, Reason} ->
                    {error, decode_rc(Reason)};
                {ok, Stats} ->
                    {ok, Stats}
            end;
        Error ->
            {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieve log stats with empty flags
%%
%% @spec log_stat() -> {ok, [{atom(), number()}]} | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec log_stat() ->
    {ok, [{atom(), number()}]} | db_error().
log_stat() ->
    log_stat([]).

%%--------------------------------------------------------------------
%% @doc
%% Print log stats
%%
%% This function prints bdb log statistics to wherever
%% BDB messages are being sent
%%
%% === Options ===
%%
%% <dl>
%%   <dt>stat_all</dt>
%%   <dd>Display all available information.</dd>
%%   <dt>stat_clear</dt>
%%   <dd>Reset statistics after displaying their values.</dd>
%% </dl>
%%
%% @spec log_stat_print(Opts) -> ok | {error, Error}
%% where
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec log_stat_print(Opts :: db_flags()) ->
    ok | db_error().
log_stat_print(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_LOG_STAT_PRINT, Cmd),
    case decode_rc(Result) of
        ok -> ok;
        Error -> {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Print log stats with empty flags
%%
%% @spec log_stat_print() -> ok | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec log_stat_print() ->
    ok | db_error().
log_stat_print() ->
    log_stat_print([]).

%%--------------------------------------------------------------------
%% @doc
%% Retrieve memory pool stats
%%
%% This function retrieves bdb mpool statistics
%%
%% === Options ===
%%
%% <dl>
%%   <dt>stat_clear</dt>
%%   <dd>Reset statistics after returning their values</dd>
%% </dl>
%%
%% @spec memp_stat(Opts) -> {ok, [{atom(), number()}]} | {error, Error}
%% where
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec memp_stat(Opts :: db_flags()) ->
    {ok, [{atom(), number()}]} | db_error().

memp_stat(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_MEMP_STAT, Cmd),
    case decode_rc(Result) of
        ok ->
            recv_memp_stat([]);
        Error ->
            {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieve memory pool stats with empty flags
%%
%% @spec memp_stat() -> {ok, [{atom(), number()}]} | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec memp_stat() ->
    {ok, [{atom(), number()}]} | db_error().
memp_stat() ->
    memp_stat([]).

%%--------------------------------------------------------------------
%% @doc
%% Print memory pool stats
%%
%% This function prints bdb mpool statistics to wherever
%% BDB messages are being sent
%%
%% === Options ===
%%
%% <dl>
%%   <dt>stat_all</dt>
%%   <dd>Display all available information.</dd>
%%   <dt>stat_clear</dt>
%%   <dd>Reset statistics after displaying their values.</dd>
%%   <dt>stat_memp_hash</dt>
%%   <dd>Display the buffers with hash chains.</dd>
%% </dl>
%%
%% @spec memp_stat_print(Opts) -> ok | {error, Error}
%% where
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec memp_stat_print(Opts :: db_flags()) ->
    ok | db_error().
memp_stat_print(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_MEMP_STAT_PRINT, Cmd),
    case decode_rc(Result) of
        ok -> ok;
        Error -> {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Print memory pool stats with empty flags
%%
%% @spec memp_stat_print() -> ok | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec memp_stat_print() ->
    ok | db_error().
memp_stat_print() ->
    memp_stat_print([]).

%%--------------------------------------------------------------------
%% @doc
%% Retrieve mutex stats
%%
%% This function retrieves mutex statistics
%%
%% === Options ===
%%
%% <dl>
%%   <dt>stat_clear</dt>
%%   <dd>Reset statistics after returning their values</dd>
%% </dl>
%%
%% @spec mutex_stat(Opts) -> {ok, [{atom(), number()}]} | {error, Error}
%% where
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec mutex_stat(Opts :: db_flags()) ->
    {ok, [{atom(), number()}]} | db_error().

mutex_stat(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_MUTEX_STAT, Cmd),
    case decode_rc(Result) of
        ok ->
            receive
                {error, Reason} ->
                    {error, decode_rc(Reason)};
                {ok, Stats} ->
                    {ok, Stats}
            end;
        Error ->
            {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieve mutex stats with empty flags
%%
%% @spec mutex_stat() -> {ok, [{atom(), number()}]} | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec mutex_stat() ->
    {ok, [{atom(), number()}]} | db_error().
mutex_stat() ->
    mutex_stat([]).

%%--------------------------------------------------------------------
%% @doc
%% Print mutex stats
%%
%% This function prints mutex statistics to wherever
%% BDB messages are being sent
%%
%% === Options ===
%%
%% <dl>
%%   <dt>stat_all</dt>
%%   <dd>Display all available information.</dd>
%%   <dt>stat_clear</dt>
%%   <dd>Reset statistics after displaying their values.</dd>
%% </dl>
%%
%% @spec mutex_stat_print(Opts) -> ok | {error, Error}
%% where
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec mutex_stat_print(Opts :: db_flags()) ->
    ok | db_error().
mutex_stat_print(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_MUTEX_STAT_PRINT, Cmd),
    case decode_rc(Result) of
        ok -> ok;
        Error -> {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Print mutex stats with empty flags
%%
%% @spec mutex_stat_print() -> ok | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec mutex_stat_print() ->
    ok | db_error().
mutex_stat_print() ->
    mutex_stat_print([]).

%%--------------------------------------------------------------------
%% @doc
%% Retrieve transaction stats
%%
%% This function retrieves transaction statistics
%%
%% === Options ===
%%
%% <dl>
%%   <dt>stat_clear</dt>
%%   <dd>Reset statistics after returning their values</dd>
%% </dl>
%%
%% @spec txn_stat(Opts) -> {ok, [{atom(), number()}], [[{atom(), number()}]]} | {error, Error}
%% where
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec txn_stat(Opts :: db_flags()) ->
    {ok, [{atom(), number()}], [[{atom(), number()}]]} | db_error().

txn_stat(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_TXN_STAT, Cmd),
    case decode_rc(Result) of
        ok ->
            recv_txn_stat([]);
        Error ->
            {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieve transaction stats with empty flags
%%
%% @spec txn_stat() -> {ok, [{atom(), number()}], [[{atom(), number()}]]} | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec txn_stat() ->
    {ok, [{atom(), number()}], [[{atom(), number()}]]} | db_error().
txn_stat() ->
    txn_stat([]).

%%--------------------------------------------------------------------
%% @doc
%% Print transaction stats
%%
%% This function prints transaction statistics to wherever
%% BDB messages are being sent
%%
%% === Options ===
%%
%% <dl>
%%   <dt>stat_all</dt>
%%   <dd>Display all available information.</dd>
%%   <dt>stat_clear</dt>
%%   <dd>Reset statistics after displaying their values.</dd>
%% </dl>
%%
%% @spec txn_stat_print(Opts) -> ok | {error, Error}
%% where
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec txn_stat_print(Opts :: db_flags()) ->
    ok | db_error().
txn_stat_print(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_TXN_STAT_PRINT, Cmd),
    case decode_rc(Result) of
        ok -> ok;
        Error -> {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Print transaction stats with empty flags
%%
%% @spec txn_stat_print() -> ok | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec txn_stat_print() ->
    ok | db_error().
txn_stat_print() ->
    txn_stat_print([]).

%%--------------------------------------------------------------------
%% @doc
%% Print environment stats
%%
%% This function prints environment statistics to wherever
%% BDB messages are being sent.  There is no documented way
%% to get this programatically
%%
%% === Options ===
%%
%% <dl>
%%   <dt>stat_all</dt>
%%   <dd>Display all available information.</dd>
%%   <dt>stat_clear</dt>
%%   <dd>Reset statistics after displaying their values.</dd>
%%   <dt>stat_subsystem</dt>
%%   <dd>Display information for all configured subsystems.</dd>
%% </dl>
%%
%% @spec env_stat_print(Opts) -> ok | {error, Error}
%% where
%%    Opts = [atom()]
%%
%% @end
%%--------------------------------------------------------------------
-spec env_stat_print(Opts :: db_flags()) ->
    ok | db_error().
env_stat_print(Opts) ->
    Flags = process_flags(Opts),
    Cmd = <<Flags:32/native>>,
    <<Result:32/signed-native>> = erlang:port_control(get_port(), ?CMD_ENV_STAT_PRINT, Cmd),
    case decode_rc(Result) of
        ok -> ok;
        Error -> {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Print environment stats with empty flags
%%
%% @spec env_stat_print() -> ok | {error, Error}
%%
%% @end
%%--------------------------------------------------------------------
-spec env_stat_print() ->
    ok | db_error().
env_stat_print() ->
    env_stat_print([]).

%%--------------------------------------------------------------------
%% @doc
%% Registers the port owner pid to receive any BDB err/msg events. Note
%% that this is global registration -- ALL BDB err/msg events for this
%% VM instance will be routed to the pid.
%%
%% @spec register_logger() -> ok
%%
%% @end
%%--------------------------------------------------------------------
-spec register_logger() -> ok.

register_logger() ->
    [] = erlang:port_control(get_port(), ?CMD_REGISTER_LOGGER, <<>>),
    ok.


%% ====================================================================
%% Internal functions
%% ====================================================================

init() ->
    case erl_ddll:load_driver(code:priv_dir(bdberl), bdberl_drv) of
        ok -> ok;
        {error, permanent} -> ok               % Means that the driver is already active
    end,

    %% Look for logging process -- make sure it's running and/or registered
    case whereis(bdberl_logger) of
        undefined ->
            C = {bdberl_logger, {bdberl_logger, start_link, []}, permanent, 1000,
                 worker, [bdberl_logger]},
            supervisor:start_child(kernel_safe_sup, C);
        _ ->
            ok
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
decode_rc(Rc) when is_atom(Rc)       -> Rc;
decode_rc(?ERROR_NONE)               -> ok;
decode_rc(?ERROR_ASYNC_PENDING)      -> async_pending;
decode_rc(?ERROR_INVALID_DBREF)      -> invalid_db;
decode_rc(?ERROR_TXN_OPEN)           -> transaction_open;
decode_rc(?ERROR_NO_TXN)             -> no_txn;
decode_rc(?ERROR_CURSOR_OPEN)        -> cursor_open;
decode_rc(?ERROR_NO_CURSOR)          -> no_cursor;
decode_rc(?DB_BUFFER_SMALL)          -> buffer_small;
decode_rc(?DB_KEYEMPTY)              -> key_empty;
decode_rc(?DB_KEYEXIST)              -> key_exist;
decode_rc(?DB_LOCK_DEADLOCK)         -> deadlock;
decode_rc(?DB_LOCK_NOTGRANTED)       -> lock_not_granted;
decode_rc(?DB_LOG_BUFFER_FULL)       -> log_buffer_full;
decode_rc(?DB_NOTFOUND)              -> not_found;
decode_rc(?DB_OLD_VERSION)           -> old_version;
decode_rc(?DB_PAGE_NOTFOUND)         -> page_not_found;
decode_rc(?DB_RUNRECOVERY)           -> run_recovery;
decode_rc(?DB_VERIFY_BAD)            -> verify_bad;
decode_rc(?DB_VERSION_MISMATCH)      -> version_mismatch;
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
        fast_stat        -> ?DB_FAST_STAT;
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
        stat_all         -> ?DB_STAT_ALL;
        stat_clear       -> ?DB_STAT_CLEAR;
        stat_lock_conf   -> ?DB_STAT_LOCK_CONF;
        stat_lock_lockers -> ?DB_STAT_LOCK_LOCKERS;
        stat_lock_objects -> ?DB_STAT_LOCK_OBJECTS;
        stat_lock_params  -> ?DB_STAT_LOCK_PARAMS;
        stat_memp_hash    -> ?DB_STAT_MEMP_HASH;
        stat_subsystem   -> ?DB_STAT_SUBSYSTEM;
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


%%
%% Receive memory pool stats
%%
recv_memp_stat(Fstats) ->
    receive
        {error, Reason} ->
            {error, decode_rc(Reason)};
        {fstat, Fstat} ->
            recv_memp_stat([Fstat|Fstats]);
        {ok, Stats} ->
            {ok, Stats, Fstats}
    end.

%%
%% Receive transaction stats
%%
recv_txn_stat(Tstats) ->
    receive
        {error, Reason} ->
            {error, decode_rc(Reason)};
        {txn, Tstat} ->
            recv_txn_stat([Tstat|Tstats]);
        {ok, Stats} ->
            {ok, Stats, Tstats}
    end.

%%
%% Receive directory info messages until ok
%%
recv_dirs_info(DirInfos) ->
    receive
        {dirinfo, Path, FsId, MbytesAvail} ->
            recv_dirs_info([{Path, FsId, MbytesAvail} | DirInfos]);
        {error, Reason} ->
            {error, Reason};
        ok ->
            {ok, DirInfos}
    end.
    
