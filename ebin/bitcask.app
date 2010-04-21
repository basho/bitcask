{application, bitcask,
 [
  {description, ""},
  {vsn, "1"},
  {modules, [
             bitcask,
             bitcask_app,
             bitcask_merge_worker,
             bitcask_nifs,
             bitcask_lockops,
             bitcask_fileops,
             bitcask_sup
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {mod, {bitcask_app, []}},
  {env, []}
 ]}.
