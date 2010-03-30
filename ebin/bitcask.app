{application, bitcask,
 [
  {description, "bitcask: Eric Brewer-inspired key/value store"},
  {vsn, "1"},
  {modules, [
             bitcask,
             bitcask_app,
             bitcask_client,
             bitcask_client_mgr,
             bitcask_client_sup,
             bitcask_fileops,
             bitcask_lockops,
             bitcask_merge_queue,
             bitcask_merge_sup,
             bitcask_merge_worker,
             bitcask_nifs,
             bitcask_sup
            ]},
  {registered, []},
  {mod, {bitcask_app, []}},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {env, []}
 ]}.
