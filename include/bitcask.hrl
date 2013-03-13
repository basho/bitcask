
-record(bitcask_entry, { key :: binary(),
                         file_id :: integer(),
                         total_sz :: integer(),
                         offset :: integer() | binary(), 
                         tstamp :: integer() }).


%% @type filestate().
-record(filestate, {mode,     % File mode: read_only, read_write
                    filename, % Filename
                    tstamp,   % Tstamp portion of filename
                    fd,       % File handle
                    hintfd,   % File handle for hints
                    hintcrc=0,% CRC-32 of current hint
                    ofs }).   % Current offset for writing

-record(file_status, { filename,
                       fragmented,
                       dead_bytes,
                       total_bytes,
                       oldest_tstamp,
                       newest_tstamp }).


-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

-define(TOMBSTONE, <<"bitcask_tombstone">>).

-define(OFFSETFIELD,  64).
-define(TSTAMPFIELD,  32).
-define(KEYSIZEFIELD, 16).
-define(TOTALSIZEFIELD, 32).
-define(VALSIZEFIELD, 32).
-define(CRCSIZEFIELD, 32).
-define(HEADER_SIZE,  14). % 4 + 4 + 2 + 4 bytes
-define(MAXKEYSIZE, 2#1111111111111111).
-define(MAXVALSIZE, 2#11111111111111111111111111111111).
-define(MAXOFFSET, 16#ffffffffffffffff). % max 64-bit unsigned

%% for hintfile validation
-define(CHUNK_SIZE, 1048576).
