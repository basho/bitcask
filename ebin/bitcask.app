{application, bitcask,
 [
  {description, ""},
  {vsn, "1.1.6"},
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

         %% Merge window. Span of hours during which merge is acceptable.
         %% * {Start, End} - Hours during which merging is permitted
         %% * always       - Merging is always permitted (default)
         %% * never        - Merging is never permitted
         {merge_window, always},

         %% Merge trigger variables. Files exceeding ANY of these
         %% values will cause bitcask:needs_merge/1 to return true.
         %%
         {frag_merge_trigger, 60},              % >= 60% fragmentation
         {dead_bytes_merge_trigger, 536870912}, % Dead bytes > 512 MB

         %% Merge thresholds. Files exceeding ANY of these values
         %% will be included in the list of files marked for merging
         %% by bitcask:needs_merge/1.
         %%
         {frag_threshold, 40},                  % >= 40% fragmentation
         {dead_bytes_threshold, 134217728},     % Dead bytes > 128 MB
         {small_file_threshold, 10485760},      % File is < 10 MB

         %% Data expiration can be caused by setting this to a
         %% positive value.  If so, items older than the value
         %% will be discarded.
         {expiry_secs, -1}

        ]}
 ]}.
