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
-module(bitcask_client).

-behaviour(gen_server).

%% API
-export([start_link/1, start_link/2,
         close/1,
         get/2,
         put/3,
         delete/2,
         merge/1,
         readable_files/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("bitcask.hrl").

-define(DEFAULT_MAX_FILE_SIZE, 16#80000000). % 2GB default

-record(state, { dirname,       % Abs. path we are operating on
                 write_file,    % File for writing
                 read_files,    % Files opened for reading
                 max_file_size, % Max. size of a written file
                 keydir }).     % Key directoryxs

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% ====================================================================
%% API
%% ====================================================================

start_link(Dirname) ->
    gen_server:start_link(?MODULE, [Dirname, []], []).

start_link(Dirname, Opts) ->
    gen_server:start_link(?MODULE, [Dirname, Opts], []).

close(Pid) ->
    Mref = erlang:monitor(process, Pid),
    ok = gen_server:call(Pid, close),
    receive
        {'DOWN', Mref, _, _, _} ->
            ok
    end.

get(Pid, Key) ->
    gen_server:call(Pid, {get, Key}).

put(Pid, Key, Value) ->
    gen_server:call(Pid, {put, Key, Value}).

delete(Pid, Key) ->
    gen_server:call(Pid, {put, Key, ?TOMBSTONE}).

merge(_Pid) ->
    ok.

readable_files(Dirname) ->
    %% Check the write and/or merge locks to see what files are currently
    %% being written to. Generate our list excepting those.
    {_, WritingFile} = bitcask_lockops:check(write, Dirname),
    {_, MergingFile} = bitcask_lockops:check(merge, Dirname),
    list_data_files(Dirname, WritingFile, MergingFile).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([Dirname, Opts]) ->
    %% Make sure the directory exists
    ok = filelib:ensure_dir(filename:join(Dirname, "bitcask")),

    %% If the read_write option is set, attempt to acquire the write lock file.
    %% Do this first to avoid unnecessary processing of files for reading.
    case proplists:get_bool(read_write, Opts) of
        true ->
            %% Try to acquire the write lock, or bail if unable to
            case bitcask_lockops:acquire(write, Dirname) of
                true ->
                    %% Open up the new file for writing
                    %% and update the write lock file
                    {ok, WritingFile} = bitcask_fileops:create_file(Dirname),
                    WritingFilename = bitcask_fileops:filename(WritingFile),
                    ok = bitcask_lockops:update(write, Dirname, WritingFilename);

                false ->
                    WritingFile = undefined, % Make erlc happy w/ non-local exit
                    throw({error, write_locked})
            end;
        false ->
            WritingFile = undefined
    end,

    %% Get the max file size parameter from opts
    case proplists:get_value(max_file_size, Opts, ?DEFAULT_MAX_FILE_SIZE) of
        MaxFileSize when is_integer(MaxFileSize) ->
            ok;
        _ ->
            MaxFileSize = ?DEFAULT_MAX_FILE_SIZE
    end,

    %% Setup empty keydir
    {ok, KeyDir} = bitcask_nifs:keydir_new(),

    %% Queue up an immediate request to load up the key dir based on available data
    gen_server:cast(self(), load_keydir),

    {ok, #state{ dirname = Dirname,
                 read_files = [],
                 write_file = WritingFile, % May be undefined
                 max_file_size = MaxFileSize,
                 keydir = KeyDir }}.

handle_call(close, _From, State) ->
    {stop, normal, ok, State};

handle_call({get, Key}, _From, State) ->
    case bitcask_nifs:keydir_get(State#state.keydir, Key) of
        not_found ->
            {reply, not_found, State};

        E when is_record(E, bitcask_entry) ->
            {Filestate, State2} = get_filestate(E#bitcask_entry.file_id, State),
            case bitcask_fileops:read(Filestate, E#bitcask_entry.value_pos,
                                      E#bitcask_entry.value_sz) of
                {ok, _Key, ?TOMBSTONE} ->
                    {reply, not_found, State2};
                {ok, _Key, Value} ->
                    {reply, {ok, Value}, State2}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({put, _Key, _Value}, _From, #state { write_file = undefined } = State) ->
    {reply, {error, read_only}, State};

handle_call({put, Key, Value}, _From, State) ->
    case bitcask_fileops:check_write(State#state.write_file,
                                     Key, Value, State#state.max_file_size) of
        wrap ->
            %% Time to start a new write file. Note that we do not close the old one,
            %% just transition it. The thinking is that closing/reopening for read only
            %% access would flush the O/S cache for the file, which may be undesirable.
            ok = bitcask_fileops:sync(State#state.write_file),
            {ok, WriteFile} = bitcask_fileops:create_file(State#state.dirname),
            State1 = State#state{ write_file = WriteFile,
                                  read_files = [State#state.write_file |
                                                State#state.read_files]};
        ok ->
            WriteFile = State#state.write_file,
            State1 = State
    end,

    Tstamp = bitcask_fileops:tstamp(),
    {ok, NewWriteFile, OffSet, Size} = bitcask_fileops:write(WriteFile, Key, Value, Tstamp),
    ok = bitcask_nifs:keydir_put(State#state.keydir, Key,
                                 bitcask_fileops:file_tstamp(WriteFile),
                                 Size, OffSet, Tstamp),
    {reply, ok, State1#state { write_file = NewWriteFile }}.



handle_cast(load_keydir, State) ->
    %% Build a list of all the bitcask data files and sort it in
    %% descending order (newest->oldest). Pass the currently active
    %% writing file so that one is not loaded (may be 'undefined')
    ReadFiles = scan_key_files(readable_files(State#state.dirname), State#state.keydir, []),
    {noreply, State#state { read_files = ReadFiles }}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, State) ->
    %% Clean up all the reading files
    [ok = bitcask_fileops:close(F) || F <- State#state.read_files],

    %% If we have a write file, assume we also currently own the write lock
    %% and cleanup both of those
    case State#state.write_file of
        undefined ->
            ok;
        _ ->
            ok = bitcask_fileops:close(State#state.write_file),
            ok = bitcask_lockops:release(write, State#state.dirname)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

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


get_filestate(FileId, #state{ dirname = Dirname, read_files = ReadFiles } = State) ->
    Fname = bitcask_fileops:mk_filename(Dirname, FileId),
    case lists:keysearch(Fname, #filestate.filename, ReadFiles) of
        {value, Filestate} ->
            {Filestate, State};
        false ->
            {ok, Filestate} = bitcask_fileops:open_file(Fname),
            {Filestate, State#state { read_files = [Filestate | State#state.read_files] }}
    end.

list_data_files(Dirname, WritingFile, MergingFile) ->
    %% Build a list of {tstamp, filename} for all files in the directory that
    %% match our regex. Then reverse sort that list and extract the fully-qualified
    %% filename.
    Files = filelib:fold_files(Dirname, "[0-9]+.bitcask.data", false,
                               fun(F, Acc) ->
                                       [{bitcask_fileops:file_tstamp(F), F} | Acc]
                               end, []),
    [filename:join(Dirname, F) || {_Tstamp, F} <- reverse_sort(Files),
                                  F /= WritingFile,
                                  F /= MergingFile].


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

init_dataset(Dirname, KVs) ->
    init_dataset(Dirname, [], KVs).

init_dataset(Dirname, Opts, KVs) ->
    os:cmd(?FMT("rm -rf ~s", [Dirname])),

    {ok, B} = bitcask_client:start_link(Dirname, [read_write] ++ Opts),
    ok = lists:foldl(fun({K, V}, _) ->
                             ok = bitcask_client:put(B, K, V)
                     end, undefined, KVs),
    B.

default_dataset() ->
    [{<<"k">>, <<"v">>},
     {<<"k2">>, <<"v2">>},
     {<<"k3">>, <<"v3">>}].


roundtrip_test() ->
    os:cmd("rm -rf /tmp/bc.test.roundtrip"),
    {ok, B} = bitcask_client:start_link("/tmp/bc.test.roundtrip", [read_write]),
    ok = bitcask_client:put(B,<<"k">>,<<"v">>),
    {ok, <<"v">>} = bitcask_client:get(B,<<"k">>),
    ok = bitcask_client:put(B, <<"k2">>, <<"v2">>),
    ok = bitcask_client:put(B, <<"k">>,<<"v3">>),
    {ok, <<"v2">>} = bitcask_client:get(B, <<"k2">>),
    {ok, <<"v3">>} = bitcask_client:get(B, <<"k">>),
    close(B).

list_data_files_test() ->
    os:cmd("rm -rf /tmp/bc.test.list; mkdir -p /tmp/bc.test.list"),

    %% Generate a list of files from 12->8 (already in order we expect
    ExpFiles = [?FMT("/tmp/bc.test.list/~w.bitcask.data", [I]) ||
                   I <- lists:seq(12, 8, -1)],

    %% Create each of the files
    [] = os:cmd(?FMT("touch ~s", [string:join(ExpFiles, " ")])),

    %% Now use the list_data_files to scan the dir
    ExpFiles = list_data_files("/tmp/bc.test.list", undefined, undefined).

open_test() ->
    close(init_dataset("/tmp/bc.test.open", default_dataset())),

    {ok, B} = bitcask_client:start_link("/tmp/bc.test.open"),
    lists:foldl(fun({K, V}, _) ->
                       {ok, V} = bitcask_client:get(B, K)
               end, undefined, default_dataset()).

wrap_test() ->
    %% Initialize dataset with max_file_size set to 1 so that each file will
    %% only contain a single key.
    close(init_dataset("/tmp/bc.test.wrap", [{max_file_size, 1}], default_dataset())),

    %% Make sure we have 4 files
    4 = length(readable_files("/tmp/bc.test.wrap")),

    %% Spin up a reader to verify the values
    {ok, B} = bitcask_client:start_link("/tmp/bc.test.wrap"),
    lists:foldl(fun({K, V}, _) ->
                        {ok, V} = bitcask:get(B, K)
                end,
                B, default_dataset()).


% merge_test() ->
%     %% Initialize dataset with max_file_size set to 1 so that each file will
%     %% only contain a single key.
%     B0 = init_dataset("/tmp/bc.test.merge", [{max_file_size, 1}], default_dataset()),

%     %% Verify there are 4 files (one for each key + extra)
%     4 = length(B0#bc_state.read_files),
%     close(B0),

%     %% Merge everything
%     ok = merge("/tmp/bc.test.merge"),

%     {ok, B} = bitcask:open("/tmp/bc.test.merge"),
%     1 = length(B#bc_state.read_files),
%     lists:foldl(fun({K, V}, Bc0) ->
%                         {ok, V, Bc} = bitcask:get(Bc0, K),
%                         Bc
%                 end, B, default_dataset()).



-endif.
