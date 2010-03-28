%% -------------------------------------------------------------------
%%
%% bitcask: Eric Brewer-inspired key/value store
%%
%% Copyright (c) 2010 Basho Technologies, Inc. All Rights Reserved.
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
-module(bitcask).

-export([open/1, open/2,
         close/1,
         get/2,
         put/3,
         delete/2]).

-include("bitcask.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @type bc_state().
-record(bc_state, {dirname,
                   write_file,                  % File for writing
                   read_files,                  % Files opened for reading
                   max_file_size,               % Max. size of a written file
                   keydir}).                    % Key directory

-define(TOMBSTONE, <<"bitcask_tombstone">>).
-define(DEFAULT_MAX_FILE_SIZE, 16#80000000). % 2GB default

%% A bitcask is a directory containing:
%% * One or more data files - {integer_timestamp}.bitcask.data
%% * A write lock - bitcask.write.lock (Optional)
%% * A merge lock - bitcask.merge.lock (Optional)

open(Dirname) ->
    open(Dirname, []).

open(Dirname, Opts) ->
    %% Make sure the directory exists
    ok = filelib:ensure_dir(filename:join(Dirname, "bitcask")),

    %% If the read_write option is set, attempt to acquire the write lock file.
    %% Do this first to avoid unnecessary processing of files for reading.
    case proplists:get_bool(read_write, Opts) of
        true ->
            %% Try to acquire the write lock, or bail if unable to
            case bitcask_lockops:lock_acquire(
                   bitcask_lockops:write_lock_filename(Dirname)) of
                true ->
                    %% Open up the new file for writing
                    %% and update the write lock file
                    {ok, WritingFile} = bitcask_fileops:create_file(Dirname),
                    WritingFilename = bitcask_fileops:filename(WritingFile),
                    ok = bitcask_lockops:lock_update(
                           bitcask_lockops:write_lock_filename(Dirname),
                           WritingFilename);
                false ->
                    WritingFile = undefined, % Make erlc happy w/ non-local exit
                    throw({error, write_locked})
            end;
        false ->
            WritingFile = undefined
    end,

    %% If we don't have a WritingFile, check the write lock and see
    %% what file is currently active so we don't attempt to read it.
    case WritingFile of
        undefined ->
            {_ActivePid, ActiveFile} = bitcask_lockops:lock_check(
                                         bitcask_lockops:write_lock_filename(
                                           Dirname));
        _ ->
            ActiveFile = undefined
    end,

    %% Build a list of all the bitcask data files and sort it in
    %% descending order (newest->oldest). Pass the currently active
    %% writing file so that one is not loaded (may be 'undefined')
    SortedFiles = list_data_files(Dirname, ActiveFile),

    %% Setup a keydir and scan all the data files into it
    {ok, KeyDir} = bitcask_nifs:keydir_new(),
    ReadFiles = scan_key_files(SortedFiles, KeyDir, []),

    %% Get the max file size parameter from opts
    case proplists:get_value(max_file_size, Opts, ?DEFAULT_MAX_FILE_SIZE) of
        MaxFileSize when is_integer(MaxFileSize) ->
            ok;
        _ ->
            MaxFileSize = ?DEFAULT_MAX_FILE_SIZE
    end,

    %% Setup basic state
    {ok, #bc_state{dirname = Dirname,
                   read_files = ReadFiles,
                   write_file = WritingFile, % May be undefined
                   max_file_size = MaxFileSize,
                   keydir = KeyDir}}.


close(#bc_state { write_file = WriteFile, read_files = ReadFiles, dirname = Dirname }) ->
    %% Clean up all the reading files
    [ok = bitcask_fileops:close(F) || F <- ReadFiles],

    %% If we have a write file, assume we also currently own the write lock
    %% and cleanup both of those
    case WriteFile of
        undefined ->
            ok;
        _ ->
            ok = bitcask_fileops:close(WriteFile),
            ok = bitcask_lockops:lock_release(
                   bitcask_lockops:write_lock_filename(Dirname))
    end.


get(#bc_state{keydir = KeyDir} = State, Key) ->
    case bitcask_nifs:keydir_get(KeyDir, Key) of
        not_found ->
            {not_found, State};

        E when is_record(E, bitcask_entry) ->
            {Filestate, State2} = get_filestate(E#bitcask_entry.file_id, State),
            case bitcask_fileops:read(Filestate, E#bitcask_entry.value_pos,
                                      E#bitcask_entry.value_sz) of
                {ok, _Key, ?TOMBSTONE} ->
                    {not_found, State2};
                {ok, _Key, Value} ->
                    {ok, Value, State2}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

put(#bc_state{ write_file = undefined }, _Key, _Value) ->
    {error, read_only};
put(#bc_state{ dirname = Dirname, keydir = KeyDir } = State, Key, Value) ->
    case bitcask_fileops:check_write(State#bc_state.write_file,
                                     Key, Value, State#bc_state.max_file_size) of
        wrap ->
            %% Time to start a new write file. Note that we do not close the old one,
            %% just transition it. The thinking is that closing/reopening for read only
            %% access would flush the O/S cache for the file, which may be undesirable.
            ok = bitcask_fileops:sync(State#bc_state.write_file),
            {ok, WriteFile} = bitcask_fileops:create_file(Dirname),
            State1 = State#bc_state{ write_file = WriteFile,
                                     read_files = [State#bc_state.write_file |
                                                   State#bc_state.read_files]};
        ok ->
            WriteFile = State#bc_state.write_file,
            State1 = State
    end,

    Tstamp = bitcask_fileops:tstamp(),
    {ok, NewWriteFile, OffSet, Size} = bitcask_fileops:write(WriteFile, Key, Value, Tstamp),
    ok = bitcask_nifs:keydir_put(KeyDir, Key,
                                 bitcask_fileops:file_tstamp(WriteFile),
                                 Size, OffSet, Tstamp),
    {ok, State1#bc_state { write_file = NewWriteFile }}.

delete(State, Key) ->
    put(State, Key, ?TOMBSTONE).


%% ===================================================================
%% Internal functions
%% ===================================================================

reverse_sort(L) ->
    lists:reverse(lists:sort(L)).

scan_key_files([], _KeyDir, Acc) ->
    Acc;
scan_key_files([Filename | Rest], KeyDir, Acc) ->
    {ok, File} = bitcask_fileops:open_file(Filename),
    F = fun(K, _V, Tstamp, {Offset, TotalSz}, _) ->
                bitcask_nifs:keydir_put(KeyDir,
                                        K,
                                        bitcask_fileops:file_tstamp(File),
                                        TotalSz,
                                        Offset,
                                        Tstamp)
        end,
    bitcask_fileops:fold(File, F, undefined),
    scan_key_files(Rest, KeyDir, [File | Acc]).


get_filestate(FileId, #bc_state{ dirname = Dirname, read_files = ReadFiles } = State) ->
    Fname = bitcask_fileops:mk_filename(Dirname, FileId),
    case lists:keysearch(Fname, #filestate.filename, ReadFiles) of
        {value, Filestate} ->
            {Filestate, State};
        false ->
            {ok, Filestate} = bitcask_fileops:open_file(Fname),
            {Filestate, State#bc_state { read_files = [Filestate | State#bc_state.read_files] }}
    end.

list_data_files(Dirname, WritingFile) ->
    %% Build a list of {tstamp, filename} for all files in the directory that
    %% match our regex. Then reverse sort that list and extract the fully-qualified
    %% filename.
    Files = filelib:fold_files(Dirname, "[0-9]+.bitcask.data", false,
                               fun(F, Acc) ->
                                       [{bitcask_fileops:file_tstamp(F), F} | Acc]
                               end, []),
    [filename:join(Dirname, F) || {_Tstamp, F} <- reverse_sort(Files),
                                  F /= WritingFile].


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

init_dataset(Dirname, KVs) ->
    init_dataset(Dirname, [], KVs).

init_dataset(Dirname, Opts, KVs) ->
    os:cmd(?FMT("rm -rf ~s", [Dirname])),

    {ok, B} = bitcask:open(Dirname, [read_write] ++ Opts),
    lists:foldl(fun({K, V}, Bc0) ->
                        {ok, Bc} = bitcask:put(Bc0, K, V),
                        Bc
                end, B, KVs).


default_dataset() ->
    [{<<"k">>, <<"v">>},
     {<<"k2">>, <<"v2">>},
     {<<"k3">>, <<"v3">>}].


roundtrip_test() ->
    os:cmd("rm -rf /tmp/bc.test.roundtrip"),
    {ok, B} = bitcask:open("/tmp/bc.test.roundtrip", [read_write]),
    {ok, B1} = bitcask:put(B,<<"k">>,<<"v">>),
    {ok, <<"v">>, B2} = bitcask:get(B1,<<"k">>),
    {ok, B3} = bitcask:put(B2, <<"k2">>, <<"v2">>),
    {ok, B4} = bitcask:put(B3, <<"k">>,<<"v3">>),
    {ok, <<"v2">>, B5} = bitcask:get(B4, <<"k2">>),
    {ok, <<"v3">>, B6} = bitcask:get(B5, <<"k">>),
    close(B6).

list_data_files_test() ->
    os:cmd("rm -rf /tmp/bc.test.list; mkdir -p /tmp/bc.test.list"),

    %% Generate a list of files from 12->8 (already in order we expect
    ExpFiles = [?FMT("/tmp/bc.test.list/~w.bitcask.data", [I]) ||
                   I <- lists:seq(12, 8, -1)],

    %% Create each of the files
    [] = os:cmd(?FMT("touch ~s", [string:join(ExpFiles, " ")])),

    %% Now use the list_data_files to scan the dir
    ExpFiles = list_data_files("/tmp/bc.test.list", undefined).

fold_test() ->
    B = init_dataset("/tmp/bc.test.fold", default_dataset()),

    File = B#bc_state.write_file,
    L = bitcask_fileops:fold(File, fun(K, V, _Ts, _Pos, Acc) ->
                                           [{K, V} | Acc]
                                   end, []),
    ?assertEqual(default_dataset(), lists:reverse(L)),
    close(B).

open_test() ->
    close(init_dataset("/tmp/bc.test.open", default_dataset())),

    {ok, B} = bitcask:open("/tmp/bc.test.open"),
    lists:foldl(fun({K, V}, Bc0) ->
                       {ok, V, Bc} = bitcask:get(Bc0, K),
                       Bc
               end, B, default_dataset()).

wrap_test() ->
    %% Initialize dataset with max_file_size set to 1 so that each file will
    %% only contain a single key.
    close(init_dataset("/tmp/bc.test.wrap", [{max_file_size, 1}], default_dataset())),

    {ok, B} = bitcask:open("/tmp/bc.test.wrap"),

    %% Expect 4 files, since we open a new one on startup + the 3
    %% for each key written
    4 = length(B#bc_state.read_files),
    lists:foldl(fun({K, V}, Bc0) ->
                       {ok, V, Bc} = bitcask:get(Bc0, K),
                       Bc
               end, B, default_dataset()).


-endif.
