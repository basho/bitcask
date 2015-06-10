% Run this script to downgrade Bitcask files from the format
% introduced in Riak 2.0 to the format used in Riak 1.4
% Run it by calling escript on it and pointing it to a data
% directory after stopping the Riak node.
% The script will recursively find all Bitcask files under that
% directory and reformat them.
%  $ escript downgrade_bitcask.erl /my/riak/data/bitcask
-module(downgrade_bitcask).
-mode(compile).
-export([main/1]).

-define(HEADER_SIZE, 14).
-record(entry, { crc, tstamp, keysz, valsz, key, val}).

main([DataDir]) ->
    downgrade_if_dir(DataDir).

maybe_downgrade_file(F) ->
    is_bitcask_file(F) andalso downgrade_file(F).

downgrade_if_dir(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            downgrade_dir(Dir);
        false ->
            ok
    end.

downgrade_dir(Dir) ->
    {ok, Children0} = file:list_dir(Dir),
    Children = [filename:join(Dir, Child) || Child <- Children0],
    case is_bitcask_dir(Dir) of
        false ->
            [downgrade_if_dir(Child) || Child <- Children];
        true ->
            [maybe_downgrade_file(Child) || Child <- Children]
    end.

is_bitcask_file(Filename0) ->
    Filename = filename:basename(Filename0),
    Match = re:run(Filename, "^\\d+\\.bitcask\\.data$"),
    nomatch =/= Match.

is_bitcask_dir(Dir) ->
    case filelib:is_dir(Dir) of
        false ->
            false;
        true ->
            {ok, Files} = file:list_dir(Dir),
            lists:any(fun is_bitcask_file/1, Files)
    end.

read_entry(F) ->
    case file:read(F, ?HEADER_SIZE) of
        {ok, <<CRC:32,Tstamp:32,KeySz:16,ValueSz:32>>} ->
            case file:read(F, KeySz+ValueSz) of
                {ok, <<Key:KeySz/bytes, Value:ValueSz/bytes>>} ->
                    % io:format("K: ~p, V: ~p\n", [Key, Value]),
                    {ok, #entry{crc=CRC, tstamp=Tstamp, keysz=KeySz, valsz=ValueSz,
                                key=Key, val=Value}};
                _ ->
                    error
            end;
        eof ->
            eof;
        _ ->
            io:format("Error reading entry\n"),
            error
    end.

downgrade_file(F) ->
    Dir = filename:dirname(F),
    NewF = F ++ ".new",
    HintFile = filename:join(Dir, filename:basename(F, ".data")++".hint"),
    NewHF = HintFile ++ ".new",
    io:format("Downgrading file ~s\n", [F]),
    {ok, Fi} = file:open(F, [read, raw, binary]),
    {ok, Fo} = file:open(NewF, [write, raw, binary]),
    {ok, Fh} = file:open(NewHF, [write, raw, binary]),
    ok = convert_file(Fi, Fo, Fh, 0, 0, fun tx_pre_20/1),
    ok = file:close(Fi),
    ok = file:close(Fo),
    ok = file:close(Fh),
    HintBak = HintFile ++ ".bak",
    FBak = F ++ ".bak",
    ok = file:rename(HintFile, HintBak),
    ok = file:rename(F, FBak),
    ok = file:rename(NewF, F),
    ok = file:rename(NewHF, HintFile),
    ok = file:delete(HintBak),
    ok = file:delete(FBak),
    ok.

convert_file(Fi, Fo, Fh, Ofs, Crc, Tx) ->
    case read_entry(Fi) of
        {ok, Entry} ->
            NewEntry = Tx(Entry),
            Sz = write_entry(Fo, NewEntry),
            NewCrc = write_hint_entry(Fh, Ofs, Sz, Crc, NewEntry),
            convert_file(Fi, Fo, Fh, Ofs+Sz, NewCrc, Tx);
        eof ->
            write_hint_entry(Fh, 16#ffffFFFFffffFFFF, Crc, 0,
                             #entry{key= <<>>, tstamp=0}),
            % io:format("Finished reading file\n", []),
            ok;
        _ ->
            io:format(standard_error, "Error reading file\n", []),
            error
    end.

write_hint_entry(F, Ofs, Sz, Crc, #entry{key=Key, tstamp=Tstamp}) ->
    KeySz = size(Key),
    Hint = [<<Tstamp:32, KeySz:16, Sz:32, Ofs:64>>, Key],
    ok = file:write(F, Hint),
    erlang:crc32(Crc, Hint).

write_entry(F, #entry {key=Key, val=Value, tstamp=Tstamp}) ->
    KeySz = size(Key),
    ValueSz = size(Value),
    Bytes0 = [<<Tstamp:32>>, <<KeySz:16>>, <<ValueSz:32>>, Key, Value],
    Bytes  = [<<(erlang:crc32(Bytes0)):32>> | Bytes0],
    ok = file:write(F, Bytes),
    iolist_size(Bytes).

tx_pre_20(Entry = 
          #entry{key= <<2, BucketSz:16, Bucket:BucketSz/binary,
                        Key/binary>>}) ->
    OldKey=term_to_binary({Bucket, Key}),
    % io:format("Converted B/K ~s/~s\n", [Bucket, Key]),
    tx_pre_20(Entry#entry{key=OldKey, keysz=size(OldKey)});
tx_pre_20(Entry=
          #entry{val= <<"bitcask_tombstone2", _/binary>>}) ->
    NewVal = <<"bitcask_tombstone">>,
    Entry#entry{val=NewVal, valsz=size(NewVal)};
tx_pre_20(Entry) ->
    Entry.
