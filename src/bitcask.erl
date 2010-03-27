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

-export([open/1,
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
                   max_file_size = 16#80000000, % 2GB default max_file size
                   keydir}).                    % Key directory

-define(TOMBSTONE, <<"bitcask_tombstone">>).

%% Filename convention is {integer_timestamp}.bitcask

open(Dirname) ->
    %% Make sure the directory exists
    ok = filelib:ensure_dir(filename:join(Dirname, "bitcask")),

    %% Build a list of all the bitcask data files and sort it in
    %% descending order (newest->oldest)
    SortedFiles = list_data_files(Dirname),

    %% Setup a keydir and scan all the data files into it
    {ok, KeyDir} = bitcask_nifs:keydir_new(),
    ReadFiles = scan_key_files(SortedFiles, KeyDir, []),
    {ok, WriteFile} = bitcask_fileops:create_file(Dirname),
    {ok, #bc_state{dirname = Dirname,
                   write_file = WriteFile,
                   read_files = ReadFiles,
                   keydir = KeyDir}}.

close(#bc_state { write_file = WriteFile, read_files = ReadFiles }) ->
    [ok = bitcask_fileops:close(F) || F <- ReadFiles],
    ok = bitcask_fileops:close(WriteFile).


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

delete(_State, _Key) ->
    %put(State,Key,?TOMBSTONE).
    ok.


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
    Fname = bitcask_fileops:filename(Dirname, FileId),
    case lists:keysearch(Fname, #filestate.filename, ReadFiles) of
        {value, Filestate} ->
            {Filestate, State};
        false ->
            {ok, Filestate} = bitcask_fileops:open_file(Fname),
            {Filestate, State#bc_state { read_files = [Filestate | State#bc_state.read_files] }}
    end.

list_data_files(Dirname) ->
    %% Build a list of {tstamp, filename} for all files in the directory that
    %% match our regex. Then reverse sort that list and extract the fully-qualified
    %% filename.
    Files = filelib:fold_files(Dirname, "[0-9]+.bitcask.data", false,
                               fun(F, Acc) ->
                                       [{bitcask_fileops:file_tstamp(F), F} | Acc]
                               end, []),
    [filename:join(Dirname, F) || {_Tstamp, F} <- reverse_sort(Files)].


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

roundtrip_test() ->
    os:cmd("rm -rf /tmp/bc.test.roundtrip"),
    {ok, B} = bitcask:open("/tmp/bc.test.roundtrip"),
    {ok, B1} = bitcask:put(B,<<"k">>,<<"v">>),
    {ok, <<"v">>, B2} = bitcask:get(B1,<<"k">>),
    {ok, B3} = bitcask:put(B2, <<"k2">>, <<"v2">>),
    {ok, B4} = bitcask:put(B3, <<"k">>,<<"v3">>),
    {ok, <<"v2">>, B5} = bitcask:get(B4, <<"k2">>),
    {ok, <<"v3">>, _} = bitcask:get(B5, <<"k">>).

list_data_files_test() ->
    os:cmd("rm -rf /tmp/bc.test.list; mkdir -p /tmp/bc.test.list"),

    %% Generate a list of files from 12->8 (already in order we expect
    ExpFiles = [?FMT("/tmp/bc.test.list/~w.bitcask.data", [I]) ||
                   I <- lists:seq(12, 8, -1)],

    %% Create each of the files
    [] = os:cmd(?FMT("touch ~s", [string:join(ExpFiles, " ")])),

    %% Now use the list_data_files to scan the dir
    ExpFiles = list_data_files("/tmp/bc.test.list").

fold_test() ->
    os:cmd("rm -rf /tmp/bc.test.fold"),

    Inputs = [{<<"k">>, <<"v">>},
              {<<"k2">>, <<"v2">>},
              {<<"k3">>, <<"v3">>}],

    {ok, B} = bitcask:open("/tmp/bc.test.fold"),
    B1 = lists:foldl(fun({K, V}, Bc0) ->
                             {ok, Bc} = bitcask:put(Bc0, K, V),
                             Bc
                     end, B, Inputs),

    File = B1#bc_state.write_file,
    L = bitcask_fileops:fold(File, fun(K, V, _Ts, _Pos, Acc) ->
                                           [{K, V} | Acc]
                                   end, []),
    Inputs = lists:reverse(L).

open_test() ->
    os:cmd("rm -rf /tmp/bc.test.open"),

    Inputs = [{<<"k">>, <<"v">>},
              {<<"k2">>, <<"v2">>},
              {<<"k3">>, <<"v3">>}],

    {ok, B} = bitcask:open("/tmp/bc.test.open"),
    B1 = lists:foldl(fun({K, V}, Bc0) ->
                             {ok, Bc} = bitcask:put(Bc0, K, V),
                             Bc
                     end, B, Inputs),

    close(B1),

    {ok, B2} = bitcask:open("/tmp/bc.test.open"),
    lists:foldl(fun({K, V}, Bc0) ->
                       {ok, V, Bc} = bitcask:get(Bc0, K),
                       Bc
               end, B2, Inputs).

wrap_test() ->
    os:cmd("rm -rf /tmp/bc.test.wrap"),

    Inputs = [{<<"k">>, <<"v">>},
              {<<"k2">>, <<"v2">>},
              {<<"k3">>, <<"v3">>}],

    {ok, B} = bitcask:open("/tmp/bc.test.wrap"),

    %% Tweak max file size to be 1 byte so that every put should create a new
    %% file
    B1 = B#bc_state { max_file_size = 1 },
    B2 = lists:foldl(fun({K, V}, Bc0) ->
                             {ok, Bc} = bitcask:put(Bc0, K, V),
                             Bc
                     end, B1, Inputs),

    close(B2),

    {ok, B3} = bitcask:open("/tmp/bc.test.wrap"),

    %% Expect 4 files, since we open a new one on startup + the 3
    %% for each key written
    4 = length(B3#bc_state.read_files),
    lists:foldl(fun({K, V}, Bc0) ->
                       {ok, V, Bc} = bitcask:get(Bc0, K),
                       Bc
               end, B3, Inputs).


-endif.
