
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

-define(OFFSETFIELD,  64).
-define(TSTAMPFIELD,  32).
-define(KEYSIZEFIELD, 16).
-define(VALSIZEFIELD, 32).
-define(HEADER_SIZE,  10). % 4 + 2 + 4 bytes
-define(MAXKEYSIZE, 2#1111111111111111).
-define(MAXVALSIZE, 2#11111111111111111111111111111111).
