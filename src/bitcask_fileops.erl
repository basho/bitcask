%% -------------------------------------------------------------------
%%
%% bitcask: Eric Brewer-inspired key/value store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Basic file i/o operations for bitcask.
-module(bitcask_fileops).
-author('Dave Smith <dizzyd@basho.com>').
-author('Justin Sheehy <justin@basho.com>').
-author('Andy Gross <andy@basho.com>').

-export([create_file/2,
         open_file/1,
         close/1,
         write/4,
         read/3,
         sync/1,
         delete/1,
         fold/3,
         fold_keys/3,
         hintfile_fold/3,
         mk_filename/2,
         filename/1,
         hintfile_name/1,
         file_tstamp/1,
         tstamp/0,
         check_write/4]).

-include("bitcask.hrl").

%% @doc Open a new file for writing.
%% Called on a Dirname, will open a fresh file in that directory.
%% @spec create_file(Dirname :: string(), Opts :: [any()]) -> {ok, filestate()}
create_file(DirName, Opts) ->
    create_file_loop(DirName, Opts, tstamp()).


%% @doc Open an existing file for reading.
%% Called with fully-qualified filename.
%% @spec open_file(Filename :: string()) -> {ok, filestate()} | {error, any()}
open_file(Filename) ->
    case file:open(Filename, [read, raw, binary]) of
        {ok, FD} ->
            {ok, #filestate{ filename = Filename, tstamp = file_tstamp(Filename),
                             fd = FD, ofs = 0 }};
        {error, Reason} ->
            {error, Reason}
    end.


%% @doc Use when done writing a file.  (never open for writing again)
%% @spec close(filestate()) -> ok
close(#filestate{ fd = FD }) ->
    file:close(FD),
    ok.

%% @doc Use only after merging, to permanently delete a data file.
%% @spec delete(filestate()) -> ok
delete(#filestate{ filename = FN }) ->
    file:delete(FN),
    ok.

%% @doc Write a Key-named binary data field ("Value") to the Filestate.
%% @spec write(filestate(), Key :: binary(), Value :: binary(), Tstamp :: integer()) ->
%%       {ok, filestate(), Offset :: integer(), Size :: integer()}
write(Filestate=#filestate{fd = FD, ofs = Offset}, Key, Value, Tstamp) ->
    KeySz = size(Key),
    true = (KeySz =< ?MAXKEYSIZE),
    ValueSz = size(Value),
    true = (ValueSz =< ?MAXVALSIZE),

    %% Setup io_list for writing -- avoid merging binaries if we can help it
    Bytes0 = [<<Tstamp:?TSTAMPFIELD>>, <<KeySz:?KEYSIZEFIELD>>,
              <<ValueSz:?VALSIZEFIELD>>, Key, Value],
    Bytes  = [<<(erlang:crc32(Bytes0)):?CRCSIZEFIELD>> | Bytes0],
    ok = file:pwrite(FD, Offset, Bytes),
    FinalSz = iolist_size(Bytes),
    {ok, Filestate#filestate{ofs = Offset + FinalSz}, Offset, FinalSz}.


%% @doc Given an Offset and Size, get the corresponding k/v from Filename.
%% @spec read(Filename :: string(), Offset :: integer(), Size :: integer()) ->
%%       {ok, Key :: binary(), Value :: binary()}
read(Filename, Offset, Size) when is_list(Filename) ->
    case open_file(Filename) of
        {ok, Fstate} ->
            read(Fstate, Offset, Size);
        {error, Reason} ->
            {error, Reason}
    end;
read(#filestate { fd = FD }, Offset, Size) ->
    case file:pread(FD, Offset, Size) of
        {ok, <<Crc32:?CRCSIZEFIELD/unsigned, Bytes/binary>>} ->
            %% Unpack the actual data
            <<_Tstamp:?TSTAMPFIELD, KeySz:?KEYSIZEFIELD, ValueSz:?VALSIZEFIELD,
             Key:KeySz/bytes, Value:ValueSz/bytes>> = Bytes,

            %% Verify the CRC of the data
            case erlang:crc32(Bytes) of
                Crc32 ->
                    {ok, Key, Value};
                _BadCrc ->
                    {error, bad_crc}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

sync(#filestate { fd = Fd }) ->
    ok = file:sync(Fd).

fold(#filestate { fd = Fd }, Fun, Acc) ->
    %% TODO: Add some sort of check that this is a read-only file
    {ok, _} = file:position(Fd, bof),
    case file:read(Fd, ?HEADER_SIZE) of
        {ok, <<_Crc:?CRCSIZEFIELD, _Tstamp:?TSTAMPFIELD, _KeySz:?KEYSIZEFIELD,
              _ValueSz:?VALSIZEFIELD>> = H} ->
            fold_loop(Fd, H, 0, Fun, Acc);
        eof ->
            Acc;
        {error, Reason} ->
            {error, Reason}
    end.

fold_keys(#filestate { fd = Fd }, Fun, Acc) ->
    fold_keys_loop(Fd, 0, Fun, Acc).

hintfile_fold(Fd, Fun, Acc) ->
    {ok, _} = file:position(Fd, bof),
    case file:read(Fd, 18) of
        {ok, H = <<_TS:?TSTAMPFIELD, _KeySz:?KEYSIZEFIELD,
                   _VSZ:?VALSIZEFIELD, _POS:?OFFSETFIELD>>} ->
            hintfile_fold(Fd, H, Fun, Acc);
        eof ->
            Acc;
        {error, Reason} ->
            {error, Reason}
    end.
hintfile_fold(Fd, Header, Fun, Acc0) ->
    <<Tstamp:?TSTAMPFIELD, KeySz:?KEYSIZEFIELD,
      ValueSz:?VALSIZEFIELD, Offset:?OFFSETFIELD>> = Header,
    ReadSz = KeySz + 18,
    case file:read(Fd, ReadSz) of
        {ok, <<Key:KeySz/bytes, Rest/binary>>} ->
            PosInfo = {Offset, ValueSz},
            Acc = Fun(Key, Tstamp, PosInfo, Acc0),
            case Rest of
                <<NextHeader:18/bytes>> ->
                    hintfile_fold(Fd, NextHeader, Fun, Acc);
                <<>> ->
                    Acc
            end;
        {error, Reason} ->
            {error, Reason}
    end.

mk_filename(Dirname, Tstamp) ->
    filename:join(Dirname,
                  lists:concat([integer_to_list(Tstamp),".bitcask.data"])).

filename(#filestate { filename = Fname }) ->
    Fname.

hintfile_name(Filestate) ->
    lists:reverse(lists:nthtail(5, lists:reverse(filename(Filestate))))
        ++ ".hint".

file_tstamp(#filestate{tstamp=Tstamp}) ->
    Tstamp;
file_tstamp(Filename) when is_list(Filename) ->
    list_to_integer(filename:basename(Filename, ".bitcask.data")).

check_write(#filestate { ofs = Offset }, Key, Value, MaxSize) ->
    Size = ?HEADER_SIZE + size(Key) + size(Value),
    case (Offset + Size) > MaxSize of
        true ->
            wrap;
        false ->
            ok
    end.


%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
tstamp() ->
    {Mega, Sec, _Micro} = now(),
    (Mega * 1000000) + Sec.


fold_loop(Fd, Header, Offset, Fun, Acc0) ->
    <<_Crc32:?CRCSIZEFIELD, Tstamp:?TSTAMPFIELD, KeySz:?KEYSIZEFIELD,
     ValueSz:?VALSIZEFIELD>> = Header,
    ReadSz = KeySz + ValueSz + ?HEADER_SIZE,
    case file:read(Fd, ReadSz) of
        {ok, <<Key:KeySz/bytes, Value:ValueSz/bytes, Rest/binary>>} ->
            PosInfo = {Offset, ReadSz},
            Acc = Fun(Key, Value, Tstamp, PosInfo, Acc0),
            case Rest of
                <<NextHeader:?HEADER_SIZE/bytes>> ->
                    fold_loop(Fd, NextHeader, Offset + ReadSz, Fun, Acc);
                <<>> ->
                    Acc
            end;
        {error, Reason} ->
            {error, Reason}
    end.

fold_keys_loop(Fd, Offset, Fun, Acc0) ->
    case file:pread(Fd, Offset, ?HEADER_SIZE) of
        {ok, Header} ->
            <<_Crc32:?CRCSIZEFIELD, Tstamp:?TSTAMPFIELD, KeySz:?KEYSIZEFIELD,
              ValueSz:?VALSIZEFIELD>> = Header,
            ReadSz = KeySz + ValueSz + ?HEADER_SIZE,
            PosInfo = {Offset, ReadSz},
            case file:pread(Fd, Offset + ?HEADER_SIZE, KeySz) of
                {ok, Key} ->
                    Acc = Fun(Key, Tstamp, PosInfo, Acc0),
                    fold_keys_loop(Fd, Offset + ReadSz, Fun, Acc);
                eof ->
                    Acc0;
                {error, Reason} ->
                    {error, Reason}
            end;
        eof ->
            Acc0;
        {error, Reason} ->
            {error, Reason}
    end.


create_file_loop(DirName, Opts, Tstamp) ->
    Filename = mk_filename(DirName, Tstamp),
    ok = filelib:ensure_dir(Filename),
    case bitcask_nifs:create_file(Filename) of
        true ->
            {ok, FD} = file:open(Filename, [read, write, raw, binary]),
            %% If o_sync is specified in the options, try to set that flag on the underlying
            %% file descriptor
            case bitcask:get_opt(sync_strategy, Opts) of
                o_sync ->
                    %% Make a hacky assumption here that if we open a raw file, we get back
                    %% a specific tuple from the Erlang VM. The tradeoff is that we can set the
                    %% O_SYNC flag on the fd, thus improving performance rather dramatically.
                    {file_descriptor, prim_file, {_Port, RealFd}} = FD,
                    case bitcask_nifs:set_osync(RealFd) of
                        ok ->
                            {ok, #filestate{filename = Filename, tstamp = file_tstamp(Filename),
                                            fd = FD, ofs = 0}};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                _ ->
                    {ok, #filestate{filename = Filename, tstamp = file_tstamp(Filename),
                                    fd = FD, ofs = 0}}
            end;
        false ->
            %% Couldn't create a new file with the requested name, increment the
            %% tstamp by 1 and try again. Conceptually, this introduces some drift
            %% into the actual creation time, but given that we only have at most 2
            %% writers (writer + merger) for a given bitcask, it shouldn't be more
            %% than a few seconds. The alternative it to sleep until the next second
            %% rolls around -- but this introduces lengthy, unnecessary delays.
            create_file_loop(DirName, Opts, Tstamp + 1)
    end.

