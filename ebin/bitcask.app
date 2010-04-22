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
  {env, [
         {sync_interval, 30}, % Seconds between fsyncs

         {merge_interval, 240} % Minutes between merge attempts
        ]}
 ]}.
