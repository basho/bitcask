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
         close_for_writing/1,
         data_file_tstamps/1,
         write/4,
         read/3,
         sync/1,
         delete/1,
         fold/3,
         fold_keys/3, fold_keys/4,
         create_hintfile/1,
         mk_filename/2,
         filename/1,
         hintfile_name/1,
         file_tstamp/1,
         check_write/4]).

-include_lib("kernel/include/file.hrl").

-include("bitcask.hrl").

-define(HINT_RECORD_SZ, 18). % Tstamp(4) + KeySz(2) + TotalSz(4) + Offset(8)

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-compile(export_all).
-endif.
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
    case bitcask_nifs:file_open(Filename, [readonly]) of
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
close(#filestate{ fd = FD, hintfd = HintFd }) ->
    bitcask_nifs:file_close(FD),
    case HintFd of
        undefined ->
            ok;
        _ ->
            bitcask_nifs:file_close(HintFd)
    end,
    ok.

%% @doc Close a file for writing, but leave it open for reads.
-spec close_for_writing(#filestate{} | fresh | undefined) -> #filestate{} | ok.
close_for_writing(fresh) -> ok;
close_for_writing(undefined) -> ok;
close_for_writing(State = 
                  #filestate{ mode = read_write, fd = Fd, 
                              hintfd = HintFd, hintcrc = HintCRC }) ->
    %% Write out CRC check at end of hint file.  Write with an empty key,
    %% zero timestamp and offset as large as the file format supports so
    %% opening with an older version of bitcask will just reject the 
    %% record at the end of the hintfile and otherwise work normally.
    Iolist = hintfile_entry(<<>>, 0, {?MAXOFFSET, HintCRC}),
    ok = bitcask_nifs:file_write(HintFd, Iolist),

    bitcask_nifs:file_sync(Fd),
    bitcask_nifs:file_sync(HintFd),
    bitcask_nifs:file_close(HintFd),
    State#filestate { mode = read_only, hintfd = undefined }.

%% Build a list of {tstamp, filename} for all files in the directory that
%% match our regex. 
-spec data_file_tstamps(Dirname :: string()) -> [{integer(), string()}].
data_file_tstamps(Dirname) ->
    filelib:fold_files(Dirname, "[0-9]+.bitcask.data", false,
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
    ok = bitcask_nifs:file_pwrite(FD, Offset, Bytes),
    %% Create and store the corresponding hint entry
    TotalSz = KeySz + ValueSz + ?HEADER_SIZE,
    Iolist = hintfile_entry(Key, Tstamp, {Offset, TotalSz}),
    ok = bitcask_nifs:file_write(HintFD, Iolist),
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
    case bitcask_nifs:file_pread(FD, Offset, Size) of
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
    ok = bitcask_nifs:file_sync(Fd),
    ok = bitcask_nifs:file_sync(HintFd).

-spec fold(fresh | #filestate{},
           fun((binary(), binary(), integer(),
                {list(), integer(), integer(), integer()}, any()) -> any()),
           any()) ->
        any() | {error, any()}.
fold(fresh, _Fun, Acc) -> Acc;
fold(#filestate { fd=Fd, filename=Filename, tstamp=FTStamp }, Fun, Acc) ->
    %% TODO: Add some sort of check that this is a read-only file
    %% TODO: Need to position+read?!
    ok = bitcask_nifs:file_seekbof(Fd),
    case bitcask_nifs:file_read(Fd, ?HEADER_SIZE) of
        {ok, <<_Crc:?CRCSIZEFIELD, _Tstamp:?TSTAMPFIELD, _KeySz:?KEYSIZEFIELD,
              _ValueSz:?VALSIZEFIELD>> = H} ->
            fold_loop(Fd, Filename, FTStamp, H, 0, Fun, Acc);
        {ok, Bytes} ->
            error_logger:error_msg("~s:fold: ~s: expected ~p bytes but got "
                                   "only ~p bytes, skipping\n",
                                   [?MODULE, Filename, ?HEADER_SIZE,
                                    size(Bytes)]),
            Acc;
        eof ->
            Acc;
        {error, Reason} ->
            {error, Reason}
    end.

-spec fold_keys(fresh | #filestate{}, fun((binary(), integer(), {integer(), integer()}, any()) -> any()), any()) ->
        any() | {error, any()}.
fold_keys(fresh, _Fun, Acc) -> Acc;
fold_keys(State, Fun, Acc) ->
    fold_keys(State, Fun, Acc, default).

-spec fold_keys(fresh | #filestate{}, fun((binary(), integer(), {integer(), integer()}, any()) -> any()), any(), datafile | hintfile | default) ->
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
                        {error, _} ->
                            fold_keys_loop(Fd, 0, Fun, Acc);
                        Acc1 ->
                            Acc1
                    end;
                false ->
                    fold_keys_loop(Fd, 0, Fun, Acc)
            end
    end.

-spec create_hintfile(string() | #filestate{}) -> ok | {error, any()}.
create_hintfile(Filename) when is_list(Filename) ->
    case open_file(Filename) of
        {ok, Fstate} ->
            try
                create_hintfile(Fstate)
            after
                close(Fstate)
            end;
        {error, Reason} ->
            {error, Reason}
    end;
create_hintfile(State) when is_record(State, filestate) ->
    F = fun(K, Tstamp, {Offset, TotalSz}, HintFd) ->
                Iolist = hintfile_entry(K, Tstamp, {Offset, TotalSz}),
                case bitcask_nifs:file_write(HintFd, Iolist) of
                    ok ->
                        HintFd;
                    {error, Reason} ->
                        throw({error, Reason})
                end
        end,
    generate_hintfile(hintfile_name(State),
                      {?MODULE, fold_keys_loop, [State#filestate.fd, 0, F]}).

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
            case fold_hintfile(State, fun has_valid_hintfile_visitor/4, 0) of
                N when is_number(N) ->
                    true;
                _ ->
                    false
            end;
        X ->
            X
    end.



%% ===================================================================
%% Internal functions
%% ===================================================================

fold_loop(Fd, Filename, FTStamp, Header, Offset, Fun, Acc0) ->
    <<Crc32:?CRCSIZEFIELD, Tstamp:?TSTAMPFIELD, KeySz:?KEYSIZEFIELD,
     ValueSz:?VALSIZEFIELD>> = Header,
    <<_:4/binary, HeaderMinusCRC/binary>> = Header,
    TotalSz = KeySz + ValueSz + ?HEADER_SIZE,
    case bitcask_nifs:file_read(Fd, TotalSz) of
        {ok, <<Key:KeySz/bytes, Value:ValueSz/bytes, Rest/binary>>} ->
            case erlang:crc32([HeaderMinusCRC, Key, Value]) of
                Crc32 ->
                    PosInfo = {Filename, FTStamp, Offset, TotalSz},
                    Acc = Fun(Key, Value, Tstamp, PosInfo, Acc0),
                    case Rest of
                        <<NextHeader:?HEADER_SIZE/bytes>> ->
                            fold_loop(Fd, Filename, FTStamp, NextHeader,
                                      Offset + TotalSz, Fun, Acc);
                        <<>> ->
                            Acc;
                        Tail ->
                            error_logger:error_msg("Trailing data, discarding"
                                " (~p bytes)\n", [size(Tail)]),
                            Acc
                    end;
                _ ->
                    {error, {bad_crc, Fd, Offset}}
            end;
        {ok, X} ->
            error_logger:error_msg("Bad datafile entry, discarding"
                                   "(~p/~p bytes)\n", [size(X),TotalSz]),
            Acc0;
        eof ->
            error_logger:error_msg("Unexpected EOF, ignore (~p bytes)\n",
                [TotalSz]),
            Acc0;
        {error, Reason} ->
            {error, Reason}
    end.

fold_keys_loop(Fd, Offset, Fun, Acc0) ->
    case bitcask_nifs:file_pread(Fd, Offset, ?HEADER_SIZE) of
        {ok, Header} when erlang:size(Header) =:= ?HEADER_SIZE ->
            <<_Crc32:?CRCSIZEFIELD, Tstamp:?TSTAMPFIELD, KeySz:?KEYSIZEFIELD,
              ValueSz:?VALSIZEFIELD>> = Header,
            TotalSz = KeySz + ValueSz + ?HEADER_SIZE,
            PosInfo = {Offset, TotalSz},
            %% NOTE: We are intentionally *not* reading the value blob,
            %%       so we cannot check the checksum.
            case bitcask_nifs:file_pread(Fd, Offset + ?HEADER_SIZE, KeySz) of
                {ok, Key} when erlang:size(Key) =:= KeySz ->
                    Acc = Fun(Key, Tstamp, PosInfo, Acc0),
                    fold_keys_loop(Fd, Offset + TotalSz, Fun, Acc);
                eof ->
                    Acc0;
                {error, Reason} ->
                    {error, Reason};
                X ->
                    error_logger:error_msg("Bad datafile entry 1: ~p\n", [X]),
                    Acc0
            end;
        eof ->
            Acc0;
        {error, Reason} ->
            {error, Reason};
        X ->
            error_logger:error_msg("Bad datafile entry 2: ~p\n", [X]),
            Acc0
    end.


fold_hintfile(State, Fun, Acc) ->
    HintFile = hintfile_name(State),
    case bitcask_nifs:file_open(HintFile, [readonly]) of
        {ok, HintFd} ->
            try
                {ok, DataI} = file:read_file_info(State#filestate.filename),
                DataSize = DataI#file_info.size,
                case bitcask_nifs:file_read(HintFd, ?HINT_RECORD_SZ) of
                    {ok, <<H:?HINT_RECORD_SZ/bytes>>} ->
                        fold_hintfile_loop(DataSize, HintFile,
                                           HintFd, 0, H, Fun, Acc);
                    {ok, Bytes} ->
                        error_logger:error_msg("~s:fold_hintfile: ~s: expected "
                                               "~p bytes but got only ~p "
                                               "bytes, skipping\n",
                                   [?MODULE, HintFile,
                                    ?HINT_RECORD_SZ, size(Bytes)]),
                        {error, {incomplete_hint, 1}};
                    eof ->
                        {error, empty_hintfile}; % shoudld never be empty hintfiles
                    {error, Reason} ->
                        {error, {fold_hintfile, Reason}}
                end
            after
                bitcask_nifs:file_close(HintFd)
            end;
        {error, Reason} ->
            {error, {fold_hintfile, Reason}}
    end.


fold_hintfile_loop(_DataSize, HintFile, _Fd, HintCRC,
                   <<0:?TSTAMPFIELD, 0:?KEYSIZEFIELD,
                     ExpectCRC:?TOTALSIZEFIELD, (?MAXOFFSET):?OFFSETFIELD>>, _Fun, Acc0) ->
    case HintCRC of
        ExpectCRC ->
            Acc0;
        _ ->
            error_logger:error_msg("Hintfile '~s' has bad CRC ~p expected ~p\n",
                                   [HintFile, HintCRC, ExpectCRC]),
            {error, {bad_crc, HintCRC, ExpectCRC}}
    end;
fold_hintfile_loop(DataSize, HintFile, Fd, HintCRC0,
                   HintRecord, Fun, Acc0) ->
    <<Tstamp:?TSTAMPFIELD, KeySz:?KEYSIZEFIELD,
      TotalSz:?TOTALSIZEFIELD, Offset:?OFFSETFIELD>> = HintRecord,
    ReadSz = KeySz + ?HINT_RECORD_SZ,
    case bitcask_nifs:file_read(Fd, ReadSz) of
        {ok, <<Key:KeySz/bytes, Rest/binary>>} when
                                               Offset + TotalSz =< DataSize ->
            PosInfo = {Offset, TotalSz},
            Acc = Fun(Key, Tstamp, PosInfo, Acc0),
            case Rest of
                <<NextRecord:?HINT_RECORD_SZ/bytes>> ->
                    HintCRC=erlang:crc32(HintCRC0, [HintRecord, Key]),
                    fold_hintfile_loop(DataSize, HintFile,  Fd, 
                                       HintCRC, NextRecord, Fun, Acc);
                <<>> ->
                    %% Hint files without CRCs will end on a record boundary.
                    %% No way to know whether to expect a crc or not.
                    %% Over time, merges will add CRCs to all hint files.
                    case application:get_env(bitcask, require_hint_crc) of
                        {ok, true} ->
                            {error, {incomplete_hint, 4}};
                        _ ->
                            <<>>
                    end;
                X ->
                    error_logger:error_msg("Bad hintfile data 1: ~p\n", [X]),
                    {error, {incomplete_hint, 2}}
            end;
        {ok, _} ->
            error_logger:error_msg("Hintfile '~s' contains pointer ~p ~p "
                                   "that is greater than total data size ~p\n",
                                   [HintFile, Offset, TotalSz, DataSize]),
            {error, {incomplete_hint, 3}};
        eof ->
            {error, incomplete_key};
        {error, Reason} ->
            {error, Reason}
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

    case bitcask_nifs:file_open(Filename, FinalOpts) of
        {ok, FD} ->
            {ok, HintFD} = bitcask_nifs:file_open(hintfile_name(Filename), FinalOpts),
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

generate_hintfile(Filename, {FolderMod, FolderFn, FolderArgs}) ->
    %% Create the temp file that we will write records out to.
    {ok, Fd, TmpFilename} = temp_file(Filename ++ ".~w"),

    %% Run the provided fold function over whatever the dataset is. The function
    %% is passed the Fd as the accumulator argument, and must return the same
    %% Fd on success. Otherwise, we expect an {error, Reason}.
    try apply(FolderMod, FolderFn, FolderArgs ++ [Fd]) of
        Fd ->
            %% All done writing -- rename to the actual hintfile
            ok = file:rename(TmpFilename, Filename);
        {error, Reason} ->
            {error, Reason}
    after
        file:delete(TmpFilename),
        bitcask_nifs:file_close(Fd)
    end.

hintfile_entry(Key, Tstamp, {Offset, TotalSz}) ->
    KeySz = size(Key),
    [<<Tstamp:?TSTAMPFIELD>>, <<KeySz:?KEYSIZEFIELD>>,
     <<TotalSz:?TOTALSIZEFIELD>>, <<Offset:?OFFSETFIELD>>, Key].

%% Visitor function for hintfile validation - broken out as a
%% separate function to make tracing with dbg easier if there is
%% ever an issue with hintfiles.
has_valid_hintfile_visitor(_Key, _Tstamp, _PosInfo, Acc0) when is_number(Acc0) ->
    Acc0+1;
has_valid_hintfile_visitor(_Key, _Tstamp, _PosInfo, Acc0) ->
    Acc0.

%% Find the most recent timestamp for a .data file
most_recent_tstamp(DirName) ->
    lists:foldl(fun({TS,_},Acc) -> 
                       erlang:max(TS, Acc)
               end, 0, data_file_tstamps(DirName)).
  

temp_file(Template) ->
    temp_file(Template, now()).

temp_file(Template, Seed0) ->
    {Int, Seed} = random:uniform_s(65536, Seed0),
    Filename = ?FMT(Template, [Int]),
    case bitcask_nifs:file_open(Filename, [create]) of
        {ok, FD} ->
            {ok, FD, Filename};
        {error, eexists} ->
            temp_file(Template, Seed)
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).

-define(QC_OUT(P), eqc:on_output(fun(Str, Args) -> ?debugFmt(Str, Args) end, P)).

init_file(Dirname, Kvs) ->
    os:cmd(?FMT("rm -rf ~s", [Dirname])),
    {ok, F} = create_file(Dirname, []),
    {ok, _} = write_kvs(Kvs, F, 0).

write_kvs([], F, _Tstamp) ->
    {ok, F};
write_kvs([{K, V} | Rest], F, Tstamp) ->
    {ok, F2, _, _} = write(F, K, V, Tstamp),
    write_kvs(Rest, F2, Tstamp + 1).

key_value() ->
    {eqc_gen:non_empty(binary()), binary()}.

prop_hintfile() ->
    ?FORALL(Kvs, eqc_gen:non_empty(list(key_value())),
            begin
                 {ok, F} = init_file("/tmp/bc.fop.hintfile", Kvs),
                 ok = create_hintfile(F),
                 true = has_hintfile(F),
                 AccFn = fun(K, Tstamp, Pos, Acc0) ->
                                 [{K, Tstamp, Pos} | Acc0]
                         end,
                 fold_keys(F, AccFn, [], datafile) ==
                     fold_keys(F, AccFn, [], hintfile)
            end).

prop_hintfile_test_() ->
    {timeout, 60, fun() -> ?assert(eqc:quickcheck(prop_hintfile())) end}.

-endif.

-endif.
