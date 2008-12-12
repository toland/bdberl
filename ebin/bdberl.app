{application, bdberl,
 [{description, "Berkeley DB Erlang Driver"},
  {vsn, "1"},
  {modules, [ bdberl_port, bdberl_db ]},
  {registered, []},
  {applications, [kernel, 
                  stdlib]},
%  {mod, {sparker, []}},
  {env, []}
]}.
