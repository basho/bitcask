{application, bitcask,
 [
  {description, ""},
  {vsn, "1"},
  {modules, [
             bitcask_nifs,
             bitcask_server,
             bitcask,
             bitcask_fileops
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {env, []}
 ]}.
