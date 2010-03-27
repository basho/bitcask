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
         get/2,
         put/3,
         delete/2]).

-include("bitcask.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @type bc_state().
-record(bc_state, {dirname,
                   openfile, % filestate open for writing
                   files,    % List of #filestate
                   keydir}). % Key directory

-define(TOMBSTONE, <<"bitcask_tombstone">>).
-define(LARGE_FILESIZE, 2#1111111111111111111111111111111).

%% Filename convention is {integer_timestamp}.bitcask

open(Dirname) ->
    %% Make sure the directory exists
    ok = filelib:ensure_dir(filename:join(Dirname, "bitcask")),

    %% Build a list of all the bitcask data files and sort it in
    %% descending order (newest->oldest)
    SortedFiles = list_data_files(Dirname),

    %% Setup a keydir and scan all the data files into it
    {ok, KeyDir} = bitcask_nifs:keydir_new(),
    ok = scan_key_files(SortedFiles, KeyDir),  %% MAKE THIS NOT A NOP
    {ok, OpenFS} = bitcask_fileops:create_file(Dirname),
    {ok, #bc_state{dirname=Dirname,
                   openfile=OpenFS,
                   files=[],
                   keydir=KeyDir}}.

get(#bc_state{keydir = KeyDir} = State, Key) ->
    case bitcask_nifs:keydir_get(KeyDir, Key) of
        not_found ->
            {not_found, State};

        E when is_record(E, bitcask_entry) ->
            {Filestate, State2} = get_filestate(E#bitcask_entry.file_id, State),
            case bitcask_fileops:read(Filestate, E#bitcask_entry.value_pos,
                                      E#bitcask_entry.value_sz) of
                {ok, _Key, ?TOMBSTONE} -> {not_found, State2};
                {ok, _Key, Value} -> {ok, Value, State2}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

put(State=#bc_state{dirname=Dirname,openfile=OpenFS,keydir=KeyHash,
                    files=Files},
    Key,Value) ->
    Tstamp = bitcask_fileops:tstamp(),
    {ok, NewFS, OffSet, Size} = bitcask_fileops:write(OpenFS, Key, Value, Tstamp),
    ok = bitcask_nifs:keydir_put(KeyHash, Key,
                                 bitcask_fileops:file_tstamp(OpenFS),
                                 Size, OffSet, Tstamp),
    {FinalFS,FinalFiles} = case OffSet+Size > ?LARGE_FILESIZE of
        true ->
            bitcask_fileops:close(NewFS),
            {ok, OpenFS} = bitcask_fileops:create_file(Dirname),
            {OpenFS,[NewFS#filestate.filename|Files]};
        false ->
            {NewFS,Files}
    end,
    {ok,State#bc_state{openfile=FinalFS,files=FinalFiles}}.

delete(_State, _Key) ->
    %put(State,Key,?TOMBSTONE).
    ok.


%% ===================================================================
%% Internal functions
%% ===================================================================

reverse_sort(L) ->
    lists:reverse(lists:sort(L)).

scan_key_files([], _KeyDir) ->
    ok;
scan_key_files([_Filename | Rest], KeyDir) ->
    scan_key_files(Rest, KeyDir).


get_filestate(FileId, #bc_state{ dirname = Dirname, files = Files } = State) ->
    Fname = bitcask_fileops:filename(Dirname, FileId),
    case lists:keysearch(Fname, #filestate.filename, Files) of
        {value, Filestate} ->
            {Filestate, State};
        false ->
            {ok, Filestate} = bitcask_fileops:open_file(Fname),
            {Filestate, State#bc_state { files = [Filestate | State#bc_state.files] }}
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
    os:cmd("rm -rf /tmp/bc.test"),
    {ok, B} = bitcask:open("/tmp/bc.test"),
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

    File = B1#bc_state.openfile,
    L = bitcask_fileops:fold(File, fun(K, V, _Ts, Acc) -> [{K, V} | Acc] end, []),
    Inputs = lists:reverse(L).

-endif.
