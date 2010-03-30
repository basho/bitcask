
-record(bitcask_entry, { key,
                         file_id,
                         value_sz,
                         value_pos,
                         tstamp }).


%% @type filestate().
-record(filestate, {filename, % Filename
                    tstamp,   % Tstamp portion of filename
                    fd,       % File handle
                    ofs }).   % Current offset for writing


-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

-define(TOMBSTONE, <<"bitcask_tombstone">>).
