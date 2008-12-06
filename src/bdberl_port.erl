%% -------------------------------------------------------------------
%%
%% bdberl: Port Interface
%% Copyright (c) 2008 The Hive.  All rights reserved.
%%
%% -------------------------------------------------------------------
-module(bdberl_port).

-export([open/0]).

open() ->
    ok = erl_ddll:load_driver(code:priv_dir(bdberl), bdberl_drv),
    Port = open_port({spawn, bdberl_drv}, [binary]),
    {ok, Port}.
