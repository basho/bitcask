{application, bitcask,
 [
  {description, ""},
  {vsn, "1"},
  {modules, [
             bitcask_nifs,
             bitcask,
             bitcask_lockops,
             bitcask_fileops
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {env, []}
 ]}.
