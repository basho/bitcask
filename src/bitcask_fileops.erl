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

-export([create_file/2,
         open_file/1,
         close/1,
         close_for_writing/1,
         data_file_tstamps/1,
         write/4,
         read/3,
         sync/1,
         delete/1,
         fold/3,
         fold_keys/3, fold_keys/4,
         mk_filename/2,
         filename/1,
         hintfile_name/1,
         file_tstamp/1,
         check_write/4]).

-include_lib("kernel/include/file.hrl").

-include("bitcask.hrl").

-define(HINT_RECORD_SZ, 18). % Tstamp(4) + KeySz(2) + TotalSz(4) + Offset(8)

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-endif.
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Open a new file for writing.
%% Called on a Dirname, will open a fresh file in that directory.
-spec create_file(Dirname :: string(), Opts :: [any()]) -> {ok, #filestate{}}.
create_file(DirName, Opts) ->
    create_file_loop(DirName, [create] ++ Opts, most_recent_tstamp(DirName) + 1).

%% @doc Open an existing file for reading.
%% Called with fully-qualified filename.
-spec open_file(Filename :: string()) -> {ok, #filestate{}} | {error, any()}.
open_file(Filename) ->
    case bitcask_io:file_open(Filename, [readonly]) of
        {ok, FD} ->
            {ok, #filestate{mode = read_only,
                            filename = Filename, tstamp = file_tstamp(Filename),
                            fd = FD, ofs = 0 }};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Use when done writing a file.  (never open for writing again)
-spec close(#filestate{} | fresh | undefined) -> ok.
close(fresh) -> ok;
close(undefined) -> ok;
close(State = #filestate{ fd = FD }) ->
    close_hintfile(State),
    bitcask_io:file_close(FD),
    ok.

%% @doc Close a file for writing, but leave it open for reads.
-spec close_for_writing(#filestate{} | fresh | undefined) -> #filestate{} | ok.
close_for_writing(fresh) -> ok;
close_for_writing(undefined) -> ok;
close_for_writing(State = #filestate{ mode = read_write, fd = Fd }) ->
    S2 = close_hintfile(State),
    bitcask_io:file_sync(Fd),
    S2#filestate { mode = read_only }.

close_hintfile(State = #filestate { hintfd = undefined }) ->
    State;
close_hintfile(State = #filestate { hintfd = HintFd, hintcrc = HintCRC }) ->
    %% Write out CRC check at end of hint file.  Write with an empty key, zero
    %% timestamp and offset as large as the file format supports so opening with
    %% an older version of bitcask will just reject the record at the end of the
    %% hintfile and otherwise work normally.
    Iolist = hintfile_entry(<<>>, 0, {?MAXOFFSET, HintCRC}),
    ok = bitcask_io:file_write(HintFd, Iolist),
    bitcask_io:file_sync(HintFd),
    bitcask_io:file_close(HintFd),
    State#filestate { hintfd = undefined, hintcrc = 0 }.


%% Build a list of {tstamp, filename} for all files in the directory that
%% match our regex. 
-spec data_file_tstamps(Dirname :: string()) -> [{integer(), string()}].
data_file_tstamps(Dirname) ->
    filelib:fold_files(Dirname, "^[0-9]+.bitcask.data$", false,
                       fun(F, Acc) ->
                               [{file_tstamp(F), F} | Acc]
                       end, []).


%% @doc Use only after merging, to permanently delete a data file.
-spec delete(#filestate{}) -> ok | {error, atom()}.
delete(#filestate{ filename = FN } = State) ->
    file:delete(FN),
    case has_hintfile(State) of
        true ->
            file:delete(hintfile_name(State));
        false ->
            ok
    end.

%% @doc Write a Key-named binary data field ("Value") to the Filestate.
-spec write(#filestate{}, 
            Key :: binary(), Value :: binary(), Tstamp :: integer()) ->
        {ok, #filestate{}, Offset :: integer(), Size :: integer()} |
        {error, read_only}.
write(#filestate { mode = read_only }, _K, _V, _Tstamp) ->
    {error, read_only};
write(Filestate=#filestate{fd = FD, hintfd = HintFD, 
                           hintcrc = HintCRC0, ofs = Offset},
      Key, Value, Tstamp) ->
    KeySz = size(Key),
    true = (KeySz =< ?MAXKEYSIZE),
    ValueSz = size(Value),
    true = (ValueSz =< ?MAXVALSIZE),

    %% Setup io_list for writing -- avoid merging binaries if we can help it
    Bytes0 = [<<Tstamp:?TSTAMPFIELD>>, <<KeySz:?KEYSIZEFIELD>>,
              <<ValueSz:?VALSIZEFIELD>>, Key, Value],
    Bytes  = [<<(erlang:crc32(Bytes0)):?CRCSIZEFIELD>> | Bytes0],
    %% Store the full entry in the data file
    ok = bitcask_io:file_pwrite(FD, Offset, Bytes),
    %% Create and store the corresponding hint entry
    TotalSz = KeySz + ValueSz + ?HEADER_SIZE,
    Iolist = hintfile_entry(Key, Tstamp, {Offset, TotalSz}),
    ok = bitcask_io:file_write(HintFD, Iolist),
    %% Record our final offset
    TotalSz = iolist_size(Bytes),
    HintCRC = erlang:crc32(HintCRC0, Iolist), % compute crc of hint
    {ok, Filestate#filestate{ofs = Offset + TotalSz,
                            hintcrc = HintCRC}, Offset, TotalSz}.


%% @doc Given an Offset and Size, get the corresponding k/v from Filename.
-spec read(Filename :: string() | #filestate{}, Offset :: integer(),
           Size :: integer()) ->
        {ok, Key :: binary(), Value :: binary()} |
        {error, bad_crc} | {error, atom()}.
read(Filename, Offset, Size) when is_list(Filename) ->
    case open_file(Filename) of
        {ok, Fstate} ->
            read(Fstate, Offset, Size);
        {error, Reason} ->
            {error, Reason}
    end;
read(#filestate { fd = FD }, Offset, Size) ->
    case bitcask_io:file_pread(FD, Offset, Size) of
        {ok, <<Crc32:?CRCSIZEFIELD/unsigned, Bytes/binary>>} ->
            %% Verify the CRC of the data
            case erlang:crc32(Bytes) of
                Crc32 ->
                    %% Unpack the actual data
                    <<_Tstamp:?TSTAMPFIELD,
                     KeySz:?KEYSIZEFIELD, ValueSz:?VALSIZEFIELD,
                     Key:KeySz/bytes, Value:ValueSz/bytes>> = Bytes,
                    {ok, Key, Value};
                _BadCrc ->
                    {error, bad_crc}
            end;
        eof ->
            {error, eof};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Call the OS's fsync(2) system call on the cask and hint files.
-spec sync(#filestate{}) -> ok.
sync(#filestate { mode = read_write, fd = Fd, hintfd = HintFd }) ->
    ok = bitcask_io:file_sync(Fd),
    ok = bitcask_io:file_sync(HintFd).

-spec fold(fresh | #filestate{},
           fun((binary(), binary(), integer(),
                {list(), integer(), integer(), integer()}, any()) -> any()),
           any()) ->
        any() | {error, any()}.
fold(fresh, _Fun, Acc) -> Acc;
fold(#filestate { fd=Fd, filename=Filename, tstamp=FTStamp }, Fun, Acc0) ->
    %% TODO: Add some sort of check that this is a read-only file
    ok = bitcask_io:file_seekbof(Fd),
    case fold_file_loop(Fd, fun fold_int_loop/6, Fun, Acc0, 
                        {Filename, FTStamp, 0, 0}) of
        {error, Reason} ->
            {error, Reason};
        Acc -> Acc
    end.

-spec fold_keys(fresh | #filestate{}, fun((binary(), integer(), {integer(), integer()}, any()) -> any()), any()) ->
        any() | {error, any()}.
fold_keys(fresh, _Fun, Acc) -> Acc;
fold_keys(State, Fun, Acc) ->
    fold_keys(State, Fun, Acc, default).

-spec fold_keys(fresh | #filestate{}, fun((binary(), integer(), {integer(), integer()}, any()) -> any()), any(), datafile | hintfile | default | recovery) ->
        any() | {error, any()}.
fold_keys(#filestate { fd = Fd } = State, Fun, Acc, Mode) ->
    case Mode of
        datafile ->
            fold_keys_loop(Fd, 0, Fun, Acc);
        hintfile ->
            fold_hintfile(State, Fun, Acc);
        default ->
            case has_hintfile(State) of
                true ->
                    fold_hintfile(State, Fun, Acc);
                false ->
                    fold_keys_loop(Fd, 0, Fun, Acc)
            end;
        recovery -> % if hint files are corrupt, restart scanning cask files
                    % Fun should be side-effect free or tolerant of being
                    % called twice
            case has_valid_hintfile(State) of
                true ->
                    case fold_hintfile(State, Fun, Acc) of
                        {error, {trunc_hintfile, Acc0}} ->
                            Acc0;
                        {error, Reason} ->
                            HintFile = hintfile_name(State),
                            error_logger:error_msg("Hintfile '~s' failed fold: ~p\n",
                                                   [HintFile, Reason]),
                            fold_keys_loop(Fd, 0, Fun, Acc);
                        Acc1 ->
                            Acc1
                    end;
                false ->
                    HintFile = hintfile_name(State),
                    error_logger:error_msg("Hintfile '~s' invalid\n",
                                           [HintFile]),

                    fold_keys_loop(Fd, 0, Fun, Acc)
            end
    end.

-spec mk_filename(string(), integer()) -> string().
mk_filename(Dirname, Tstamp) ->
    filename:join(Dirname,
                  lists:concat([integer_to_list(Tstamp),".bitcask.data"])).

-spec filename(#filestate{}) -> string().
filename(#filestate { filename = Fname }) ->
    Fname.

-spec hintfile_name(string() | #filestate{}) -> string().
hintfile_name(Filename) when is_list(Filename) ->
    filename:rootname(Filename, ".data") ++ ".hint";
hintfile_name(#filestate { filename = Fname }) ->
    hintfile_name(Fname).

-spec file_tstamp(#filestate{} | string()) -> integer().
file_tstamp(#filestate{tstamp=Tstamp}) ->
    Tstamp;
file_tstamp(Filename) when is_list(Filename) ->
    list_to_integer(filename:basename(Filename, ".bitcask.data")).

-spec check_write(fresh | #filestate{}, binary(), binary(), integer()) ->
      fresh | wrap | ok.
check_write(fresh, _Key, _Value, _MaxSize) ->
    %% for the very first write, special-case
    fresh;
check_write(#filestate { ofs = Offset }, Key, Value, MaxSize) ->
    Size = ?HEADER_SIZE + size(Key) + size(Value),
    case (Offset + Size) > MaxSize of
        true ->
            wrap;
        false ->
            ok
    end.

has_hintfile(#filestate { filename = Fname }) ->
    filelib:is_file(hintfile_name(Fname)).

%% Return true if there is a hintfile and it has
%% a valid CRC check
has_valid_hintfile(State) ->
    case has_hintfile(State) of
        true ->
            HintFile = hintfile_name(State),
            case bitcask_io:file_open(HintFile, [readonly, read_ahead]) of
                {ok, HintFd} -> 
                    {ok, HintI} = file:read_file_info(HintFile),
                    HintSize = HintI#file_info.size,
                    hintfile_validate_loop(HintFd, 0, HintSize);
                _ ->
                    false
            end;
        Else -> Else
    end.

hintfile_validate_loop(Fd, CRC0, Rem) ->
    {ReadLen, HasCRC} = 
        case Rem =< ?CHUNK_SIZE of
            true ->
                case Rem < ?HINT_RECORD_SZ of
                    true -> 
                        {0, error};
                    false -> 
                        {Rem - ?HINT_RECORD_SZ, true}
                end;
            false ->
                {?CHUNK_SIZE, false}
        end,
            
    case bitcask_io:file_read(Fd, ReadLen) of
        {ok, Bytes} ->
            case HasCRC of
                true ->
                    ExpectCRC = read_crc(Fd),
                    CRC = erlang:crc32(CRC0, Bytes),
                    ExpectCRC =:= CRC;
                false ->
                    hintfile_validate_loop(Fd, 
                                           erlang:crc32(CRC0, Bytes),
                                           Rem - ReadLen);
                error -> 
                    false                        
            end;
        _ -> false
    end.
        
read_crc(Fd) ->
    case bitcask_io:file_read(Fd, ?HINT_RECORD_SZ) of
        {ok, <<0:?TSTAMPFIELD, 
               0:?KEYSIZEFIELD,
               ExpectCRC:?TOTALSIZEFIELD, 
               (?MAXOFFSET):?OFFSETFIELD>>} ->
            ExpectCRC;
        _ -> error
    end.
        

%% ===================================================================
%% Internal functions
%% ===================================================================

fold_int_loop(_Bytes, _Fun, Acc, _Consumed, {Filename, _, Offset, 20}, _EOI) ->
    error_logger:error_msg("fold_loop: CRC error limit at file ~p offset ~p\n",
                           [Filename, Offset]),
    {done, Acc};
fold_int_loop(<<Crc32:?CRCSIZEFIELD, Tstamp:?TSTAMPFIELD, 
                KeySz:?KEYSIZEFIELD, ValueSz:?VALSIZEFIELD, 
                Key:KeySz/bytes, Value:ValueSz/bytes, Rest/binary>>,
              Fun, Acc0, Consumed0, 
              {Filename, FTStamp, Offset, CrcSkipCount}, 
              EOI) ->
    TotalSz = KeySz + ValueSz + ?HEADER_SIZE,
    case erlang:crc32([<<Tstamp:?TSTAMPFIELD, KeySz:?KEYSIZEFIELD, 
                         ValueSz:?VALSIZEFIELD>>, Key, Value]) of 
        Crc32 ->
            PosInfo = {Filename, FTStamp, Offset, TotalSz},
            Acc = Fun(Key, Value, Tstamp, PosInfo, Acc0),
            fold_int_loop(Rest, Fun, Acc, Consumed0 + TotalSz,
                          {Filename, FTStamp, Offset + TotalSz, 
                           CrcSkipCount}, EOI);
        _ ->
            error_logger:error_msg("fold_loop: CRC error at file ~s offset ~p, "
                                   "skipping ~p bytes\n", 
                                   [Filename, Offset, TotalSz]),
            fold_int_loop(Rest, Fun, Acc0, Consumed0 + TotalSz,
                          {Filename, FTStamp, Offset + TotalSz,
                           CrcSkipCount + 1}, EOI)
    end;
fold_int_loop(<<>>, _Fun, Acc, Consumed, _Args, EOI) when EOI =:= true ->
    {done, Acc, Consumed};
fold_int_loop(_Bytes, _Fun, Acc, Consumed, Args, EOI) when EOI =:= false ->
    {more, Acc, Consumed, Args};
fold_int_loop(Bytes, _Fun, Acc, _Consumed, _Args, EOI) when EOI =:= true -> 
    error_logger:error_msg("Trailing data, discarding (~p bytes)\n",
                           [size(Bytes)]),
    {done, Acc}.

fold_keys_loop(Fd, Offset, Fun, Acc0) ->
    case bitcask_io:file_position(Fd, Offset) of 
        {ok, Offset} -> ok;
        Other -> error(Other)
    end,
    
    case fold_file_loop(Fd, fun fold_keys_int_loop/6, Fun, Acc0, {Offset, 0}) of
        {error, Reason} ->
            {error, Reason};
        Acc -> Acc
    end.

fold_keys_int_loop(<<_Crc32:?CRCSIZEFIELD, Tstamp:?TSTAMPFIELD, 
                     KeySz:?KEYSIZEFIELD, ValueSz:?VALSIZEFIELD, 
                     Key:KeySz/bytes, _:ValueSz/bytes,
                     Rest/binary>>, 
                   Fun, Acc0, Consumed0, 
                   {Offset, AvgValSz0}, 
                   EOI) ->
    TotalSz = KeySz + ValueSz + ?HEADER_SIZE,
    PosInfo = {Offset, TotalSz},
    Consumed = Consumed0 + TotalSz,
    AvgValSz = (AvgValSz0 + ValueSz) div 2,
    Acc = Fun(Key, Tstamp, PosInfo, Acc0),
    fold_keys_int_loop(Rest, Fun, Acc, Consumed, 
                       {Offset + TotalSz, AvgValSz}, EOI);
%% in the case where values are very large, we don't actually want to 
%% get a larger binary if we don't have to, so just issue a skip.
fold_keys_int_loop(<<_Crc32:?CRCSIZEFIELD, Tstamp:?TSTAMPFIELD, 
                     KeySz:?KEYSIZEFIELD, ValueSz:?VALSIZEFIELD, 
                     Key:KeySz/bytes, 
                     _Rest/binary>>, 
                   Fun, Acc0, _Consumed0, 
                   {Offset, AvgValSz0}, 
                   _EOI) when AvgValSz0 > ?CHUNK_SIZE ->
    TotalSz = KeySz + ValueSz + ?HEADER_SIZE,
    PosInfo = {Offset, TotalSz},
    Acc = Fun(Key, Tstamp, PosInfo, Acc0),
    AvgValSz = (AvgValSz0 + ValueSz) div 2,
    NewPos = Offset + TotalSz,
    {skip, Acc, NewPos, {NewPos, AvgValSz}};
fold_keys_int_loop(<<>>, _Fun, Acc, Consumed, _Args, EOI) when EOI =:= true ->
    {done, Acc, Consumed};
fold_keys_int_loop(_Bytes, _Fun, Acc, Consumed, Args, EOI) when EOI =:= false ->
    {more, Acc, Consumed, Args};
fold_keys_int_loop(Bytes, _Fun, Acc, _Consumed, _Args, EOI) when EOI =:= true ->
    error_logger:error_msg("Bad datafile entry 1: ~p\n", [Bytes]),
    {done, Acc}.


fold_hintfile(State, Fun, Acc0) ->
    HintFile = hintfile_name(State),
    case bitcask_io:file_open(HintFile, [readonly, read_ahead]) of
        {ok, HintFd} ->
            try
                {ok, DataI} = file:read_file_info(State#filestate.filename),
                DataSize = DataI#file_info.size,
                case fold_file_loop(HintFd, fun fold_hintfile_loop/6, Fun, Acc0, 
                                    {DataSize, HintFile}) of 
                    {error, Reason} ->
                        {error, Reason};
                    Acc ->
                        Acc
                end
            after
                bitcask_io:file_close(HintFd)
            end;
        {error, Reason} ->
            {error, {fold_hintfile, Reason}}
    end.

%% conditional end match here, checking that we get the expected CRC-containing
%% hint record, three-tuple done indicates that we've exhausted all bytes, or
%% it's an error
fold_hintfile_loop(<<0:?TSTAMPFIELD, 0:?KEYSIZEFIELD,
                     _ExpectCRC:?TOTALSIZEFIELD, (?MAXOFFSET):?OFFSETFIELD>>,
                   _Fun, Acc, Consumed, _Args, EOI) when EOI =:= true ->
    {done, Acc, Consumed + ?HINT_RECORD_SZ};
%% main work loop here, containing the full match of hint record and key.
%% if it gets a match, it proceeds to recurse over the rest of the big
%% binary
fold_hintfile_loop(<<Tstamp:?TSTAMPFIELD, KeySz:?KEYSIZEFIELD,
                     TotalSz:?TOTALSIZEFIELD, Offset:?OFFSETFIELD,
                     Key:KeySz/bytes, Rest/binary>>, 
                   Fun, Acc0, Consumed0, {DataSize, HintFile} = Args,
                   EOI) ->
    case Offset + TotalSz =< DataSize + 1 of 
        true ->
            PosInfo = {Offset, TotalSz},
            Acc = Fun(Key, Tstamp, PosInfo, Acc0),
            Consumed = KeySz + ?HINT_RECORD_SZ + Consumed0,
            fold_hintfile_loop(Rest, Fun, Acc, 
                               Consumed, Args, EOI);
        false ->
            error_logger:error_msg("Hintfile '~s' contains pointer ~p ~p "
                                   "that is greater than total data size ~p\n",
                                   [HintFile, Offset, TotalSz, DataSize]),
            {error, {trunc_hintfile, Acc0}}
    end;
%% error case where we've gotten to the end of the file without the CRC match
fold_hintfile_loop(<<>>, _Fun, Acc, _Consumed, _Args, EOI) when EOI =:= true ->
    case application:get_env(bitcask, require_hint_crc)  of
        {ok, true} ->
            {error, {incomplete_hint, 4}};
        _ ->
            {done, Acc}
    end;
%% catchall case where we don't get enough bytes from fold_file_loop
fold_hintfile_loop(_Bytes, _Fun, _Acc0, _Consumed0, _Args, EOI) 
  when EOI =:= true ->
    {error, {incomplete_hint, 5}};
fold_hintfile_loop(_Bytes, _Fun, Acc0, Consumed0, Args, _EOI) ->
    {more, Acc0, Consumed0, Args}.


%% @doc scaffolding for faster folds over large files.
%% The somewhat tricky thing here is the FoldFn, which is a /6
%% that does all the actual work.  see fold_hintfile_loop as a 
%% commented example
-spec fold_file_loop(port(), 
                     fun((binary(), fun(), any(), integer(),
                          any(), true | false) -> 
                                {more, any(), integer(), any()} |
                                {done, any()} |
                                {done, any(), integer()} |
                                {skip, any(), integer(), any()} |
                                {error, any()}),
                     fun(), any(), any()) -> 
                            {error, any()} | any().
fold_file_loop(Fd, FoldFn, IntFoldFn, Acc, Args) ->
    fold_file_loop(Fd, FoldFn, IntFoldFn, Acc, Args, none, ?CHUNK_SIZE).

fold_file_loop(Fd, FoldFn, IntFoldFn, Acc0, Args0, Prev0, ChunkSz0) ->
    %% analyze what happened in the last loop to determine whether or 
    %% not to change the read size. This is an optimization for large values
    %% in datafile folds and key folds
    {Prev, ChunkSz}
        = case Prev0 of 
              none -> {<<>>, ChunkSz0};
              %if we're skipping around, we're likely too big
              skip -> 
                  CS = case ChunkSz0 >= (?MIN_CHUNK_SIZE * 2) of
                           true -> ChunkSz0 div 2;
                           false -> ?MIN_CHUNK_SIZE
                       end,
                  {<<>>, CS};
              Other -> 
                  CS = case byte_size(Other) of
                           %% to avoid having to rescan the same 
                           %% binaries over and over again.
                           N when N >= ?MAX_CHUNK_SIZE ->
                               ?MAX_CHUNK_SIZE;
                           N when N > ChunkSz0 ->
                               ChunkSz0 * 2;
                           _ -> ChunkSz0
                       end,
                  {Other, CS}
          end,
    case bitcask_io:file_read(Fd, ChunkSz) of
        {ok, <<Bytes0/binary>>} ->
            Bytes = <<Prev/binary, Bytes0/binary>>,
            case FoldFn(Bytes, IntFoldFn, Acc0, 0, Args0, 
                        byte_size(Bytes0) /= ChunkSz) of
                %% foldfuns should return more when they don't have enough
                %% bytes to satisfy their main binary match.
                {more, Acc, Consumed, Args} ->
                    Rest = 
                        case Consumed > byte_size(Bytes) of
                            true -> <<>>;
                            false ->
                                <<_:Consumed/bytes, R/binary>> = Bytes,
                                R
                        end,
                    fold_file_loop(Fd, FoldFn, IntFoldFn, 
                                   Acc, Args, Rest, ChunkSz);
                %% foldfuns should return skip when they have no need for 
                %% the rest of the binary that they've been handed.
                %% see fold_int_loop for proper usage.
                {skip, Acc, SkipTo, Args} ->
                    case bitcask_io:file_position(Fd, SkipTo) of
                        {ok, SkipTo} ->
                            fold_file_loop(Fd, FoldFn, IntFoldFn, 
                                           Acc, Args, skip, ChunkSz);
                        {error, Reason} ->
                            {error, Reason};
                        Other1 ->
                            {error, {file_fold_error, Other1}}
                    end;
                %% the done two tuple is returned when we want to be 
                %% unconditionally successfully finished, i.e. trailing data 
                %% is a non-fatal error
                {done, Acc} ->
                    Acc;
                %% three tuple done requires full consumption of all bytes given
                %% to the internal fold function, to satisfy the pre-existing 
                %% semantics of hintfile folds.
                {done, Acc, Consumed} ->
                    case Consumed =:= byte_size(Bytes) of 
                        true -> Acc;
                        false -> 
                            {error, {partial_fold, Consumed, Bytes, Bytes0}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        eof ->
            Acc0;
        {error, Reason} ->
            {error, Reason}
    end.

open_hint_file(Filename, FinalOpts) ->
    open_hint_file(Filename, FinalOpts, 10).
    
open_hint_file(_Filename, _FinalOpts, 0) ->
    throw(couldnt_open_hintfile);
open_hint_file(Filename, FinalOpts, Count) ->
    case bitcask_io:file_open(hintfile_name(Filename), FinalOpts) of 
        {ok, FD} ->
            FD;
        {error, eexist} -> 
            timer:sleep(50),
            open_hint_file(Filename, FinalOpts, Count - 1)
    end.

create_file_loop(DirName, Opts, Tstamp) ->
    Filename = mk_filename(DirName, Tstamp),
    ok = filelib:ensure_dir(Filename),

    %% Check for o_sync strategy and add to opts
    FinalOpts = case bitcask:get_opt(sync_strategy, Opts) of
                    o_sync ->
                        [o_sync | Opts];
                    _ ->
                        Opts
                end,

    case bitcask_io:file_open(Filename, FinalOpts) of
        {ok, FD} ->
            HintFD = open_hint_file(Filename, FinalOpts),
            {ok, #filestate{mode = read_write,
                            filename = Filename,
                            tstamp = file_tstamp(Filename),
                            hintfd = HintFD, fd = FD, ofs = 0}};

        {error, _} ->
            %% Couldn't create a new file with the requested name,
            %% check for the more recent timestamp and increment by
            %% 1 and try again.
            %% This introduces some drift into the actual creation
            %% time, but given that we only have at most 2 writers
            %% (writer + merger) for a given bitcask, it shouldn't be
            %% more than a few seconds. The alternative it to sleep
            %% until the next second rolls around -- but this
            %% introduces lengthy, unnecessary delays.
            %% This also handles the possibility of clock skew
            MostRecentTS = most_recent_tstamp(DirName),
            create_file_loop(DirName, Opts, MostRecentTS + 1)
    end.

hintfile_entry(Key, Tstamp, {Offset, TotalSz}) ->
    KeySz = size(Key),
    [<<Tstamp:?TSTAMPFIELD>>, <<KeySz:?KEYSIZEFIELD>>,
     <<TotalSz:?TOTALSIZEFIELD>>, <<Offset:?OFFSETFIELD>>, Key].

%% Find the most recent timestamp for a .data file
most_recent_tstamp(DirName) ->
    lists:foldl(fun({TS,_},Acc) -> 
                       erlang:max(TS, Acc)
               end, 0, data_file_tstamps(DirName)).
