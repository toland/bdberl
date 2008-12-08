%% -------------------------------------------------------------------
%%
%% bdberl: Port Interface
%% Copyright (c) 2008 The Hive.  All rights reserved.
%%
%% -------------------------------------------------------------------
-module(bdberl_port).

-export([new/0,
         open_database/3,
         close_database/2]).

-define(CMD_OPEN_DB, 0).
-define(CMD_CLOSE_DB, 1).

-define(DB_TYPE_BTREE, 1).
-define(DB_TYPE_HASH, 2).

-define(STATUS_OK, 0).
-define(STATUS_ERROR, 1).

new() ->
    ok = erl_ddll:load_driver(code:priv_dir(bdberl), bdberl_drv),
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

