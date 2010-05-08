{application, bitcask,
 [
  {description, ""},
  {vsn, "0.1"},
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
         %% Default max file size (in bytes)
         {max_file_size, 16#80000000}, % 2GB default

         %% Wait time to open a keydir (in seconds)
         {open_timeout, 4},

         %% Strategies available for syncing data to disk:
         %% * none          - let the O/S decide
         %% * o_sync        - use the O_SYNC flag to sync each write
         %% * {seconds, N}  - call bitcask:sync/1 every N seconds
         %%
         %% Note that for the {seconds, N} strategy, it is up to the
         %% API caller to execute the call on the interval. This config
         %% option is (currently) a convenient placeholder for calling
         %% applications.
         {sync_strategy, none},

         %% Strategies available for merging:
         %% * {hours, N} - call bitcask:merge/1 every N hours
         %%
         %% Currently this config option is a placeholder for calling
         %% applications.
         {merge_strategy, {hours, 24}},

         %% Data expiration can be caused by setting this to a
         %% positive value.  If so, items older than the value
         %% will be discarded.
         {expiry_secs, -1}

        ]}
 ]}.
