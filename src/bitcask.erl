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
-author('Dave Smith <dizzyd@basho.com>').
-author('Justin Sheehy <justin@basho.com>').

-export([open/1, open/2,
         close/1,
         get/2,
         put/3,
         delete/2,
         sync/1,
         list_keys/1,
         fold/3,
         merge/1, merge/2]).

-export([get_opt/2]).

-include("bitcask.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @type bc_state().
-record(bc_state, {dirname,
                   write_file,                  % File for writing
                   write_lock,                  % Reference to write lock
                   read_files,                  % Files opened for reading
                   max_file_size,               % Max. size of a written file
                   opts,                        % Original options used to open the bitcask
                   keydir}).                    % Key directory

-record(mstate, { dirname,
                  max_file_size,
                  input_files,
                  out_file,
                  merged_files,
                  live_keydir,
                  all_keydir,
                  hint_keydir,
                  del_keydir,
                  expiry_time,
                  opts }).

%% A bitcask is a directory containing:
%% * One or more data files - {integer_timestamp}.bitcask.data
%% * A write lock - bitcask.write.lock (Optional)
%% * A merge lock - bitcask.merge.lock (Optional)

%% @doc Open a new or existing bitcask datastore for read-only access.
-spec open(Dirname::string()) -> reference() | {error, any()}.
open(Dirname) ->
    open(Dirname, []).

%% @doc Open a new or existing bitcask datastore with additional options.
-spec open(Dirname::string(), Opts::[_]) -> reference() | {error, any()}.
open(Dirname, Opts) ->
    %% Make sure bitcask app is started so we can pull defaults from env
    ok = start_app(),

    %% Make sure the directory exists
    ok = filelib:ensure_dir(filename:join(Dirname, "bitcask")),

    %% If the read_write option is set, attempt to acquire the write lock file.
    %% Do this first to avoid unnecessary processing of files for reading.
    case proplists:get_bool(read_write, Opts) of
        true ->
            %% Try to acquire the write lock, or bail if unable to
            case bitcask_lockops:acquire(write, Dirname) of
                {ok, WriteLock} ->
                    %% Open up the new file for writing
                    %% and update the write lock file
                    {ok, WritingFile} = bitcask_fileops:create_file(Dirname, Opts),
                    WritingFilename = bitcask_fileops:filename(WritingFile),
                    ok = bitcask_lockops:write_activefile(WriteLock, WritingFilename);

                {error, Reason} ->
                    WriteLock = undefined,
                    WritingFile = undefined, % Make erlc happy w/ non-local exit
                    throw({error, {write_locked, Reason}})
            end;
        false ->
            WriteLock = undefined,
            WritingFile = undefined
    end,

    %% Get the max file size parameter from opts
    MaxFileSize = get_opt(max_file_size, Opts),

    %% Get the named keydir for this directory. If we get it and it's already
    %% marked as ready, that indicates another caller has already loaded
    %% all the data from disk and we can short-circuit scanning all the files.
    case bitcask_nifs:keydir_new(Dirname) of
        {ready, KeyDir} ->
            %% A keydir already exists, nothing more to do here. We'll lazy
            %% open files as needed.
            ReadFiles = [];

        {not_ready, KeyDir} ->
            %% We've just created a new named keydir, so we need to load up all
            %% the data from disk. Build a list of all the bitcask data files
            %% and sort it in descending order (newest->oldest).
            SortedFiles = readable_files(Dirname),
            ReadFiles = scan_key_files(SortedFiles, KeyDir, []),

            %% Now that we loaded all the data, mark the keydir as ready so other callers
            %% can use it
            ok = bitcask_nifs:keydir_mark_ready(KeyDir);

        {error, not_ready} ->
            %% Some other process is loading data into the keydir. Setup a blank ReadFiles
            %% (since we'll lazy load files) and wait for it to be ready.
            ReadFiles = [],
            WaitTime = timer:seconds(get_opt(open_timeout, Opts)),
            case wait_for_keydir(Dirname, WaitTime) of
                {ok, KeyDir} ->
                    ok;
                timeout ->
                    %% Well, we timed out waiting around for the other process to load
                    %% data from disk.
                    KeyDir = undefined, % Make erlc happy w/ non-local exit
                    throw({error, open_timeout})
            end
    end,

    %% Ensure that expiry_secs is in Opts and not just application env
    ExpOpts = [{expiry_secs,get_opt(expiry_secs,Opts)}|Opts],

    Ref = make_ref(),
    erlang:put(Ref, #bc_state {dirname = Dirname,
                               read_files = ReadFiles,
                               write_file = WritingFile, % May be undefined
                               write_lock = WriteLock,
                               max_file_size = MaxFileSize,
                               opts = ExpOpts,
                               keydir = KeyDir}),

    Ref.

%% @doc Close a bitcask data store and flush all pending writes (if any) to disk.
-spec close(reference()) -> ok.
close(Ref) ->
    State = get_state(Ref),
    erlang:erase(Ref),

    %% Clean up all the reading files
    [ok = bitcask_fileops:close(F) || F <- State#bc_state.read_files],

    %% Cleanup the write file and associated lock
    case State#bc_state.write_file of
        undefined ->
            ok;
        _ ->
            ok = bitcask_fileops:close(State#bc_state.write_file),
            ok = bitcask_lockops:release(State#bc_state.write_lock)
    end.

%% @doc Retrieve a value by key from a bitcask datastore.
-spec get(reference(), binary()) -> not_found | {ok, Value::binary()}.
get(Ref, Key) ->
    State = get_state(Ref),

    case bitcask_nifs:keydir_get(State#bc_state.keydir, Key) of
        not_found ->
            not_found;
        E when is_record(E, bitcask_entry) ->
            case E#bitcask_entry.tstamp < expiry_time(State#bc_state.opts) of
                true -> not_found;
                false ->
                    {Filestate, S2} = get_filestate(E#bitcask_entry.file_id,
                                                    State),
                    put_state(Ref, S2),
                    case bitcask_fileops:read(Filestate,
                                              E#bitcask_entry.offset,
                                              E#bitcask_entry.total_sz) of
                        {ok, _Key, ?TOMBSTONE} ->
                            not_found;
                        {ok, _Key, Value} ->
                            {ok, Value}
                    end
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Store a key and value in a bitcase datastore.
-spec put(reference(), Key::binary(), Value::binary()) -> ok | {error, any()}.
put(Ref, Key, Value) ->
    #bc_state { write_file = WriteFile } = State = get_state(Ref),

    %% Make sure we have a file open to write
    case WriteFile of
        undefined ->
            throw({error, read_only});

        _ ->
            ok
    end,

    case bitcask_fileops:check_write(WriteFile, Key, Value,
                                     State#bc_state.max_file_size) of
        wrap ->
            %% Time to start a new write file. Note that we do not close the old
            %% one, just transition it. The thinking is that closing/reopening
            %% for read only access would flush the O/S cache for the file,
            %% which may be undesirable.
            ok = bitcask_fileops:sync(WriteFile),
            {ok, NewWriteFile} = bitcask_fileops:create_file(State#bc_state.dirname,
                                                             State#bc_state.opts),
            State2 = State#bc_state{ write_file = NewWriteFile,
                                     read_files = [State#bc_state.write_file |
                                                   State#bc_state.read_files]};
        ok ->
            State2 = State
    end,

    Tstamp = bitcask_fileops:tstamp(),
    {ok, WriteFile2, OffSet, Size} = bitcask_fileops:write(State2#bc_state.write_file,
                                                           Key, Value, Tstamp),
    ok = bitcask_nifs:keydir_put(State2#bc_state.keydir, Key,
                                 bitcask_fileops:file_tstamp(WriteFile2),
                                 Size, OffSet, Tstamp),

    put_state(Ref, State2#bc_state { write_file = WriteFile2 }),
    ok.


%% @doc Delete a key from a bitcask datastore.
-spec delete(reference(), Key::binary()) -> ok | {error, any()}.
delete(Ref, Key) ->
    put(Ref, Key, ?TOMBSTONE).

%% @doc Force any writes to sync to disk.
-spec sync(reference()) -> ok.
sync(Ref) ->
    State = get_state(Ref),
    case (State#bc_state.write_file) of
        undefined ->
            ok;
        File ->
            ok = bitcask_fileops:sync(File)
    end.


%% @doc List all keys in a bitcask datastore.
-spec list_keys(reference()) -> [Key::binary()] | {error, any()}.
list_keys(Ref) -> fold(Ref, fun(K,_V,Acc) -> [K|Acc] end, []).

%% @doc fold over all K/V pairs in a bitcask datastore.
%% Fun is expected to take F(K,V,Acc0) -> Acc
-spec fold(reference(), fun(), any()) -> any() | {error, any()}.
fold(Ref, Fun, Acc0) ->
    State = get_state(Ref),

    ReadFiles = list_data_files(State#bc_state.dirname, undefined, undefined),
    {_,_,Tseed} = now(),
    {ok, Bloom} = ebloom:new(1000000,0.00003,Tseed), % arbitrary large bloom
    ExpiryTime = expiry_time(State#bc_state.opts),
    SubFun = fun(K,V,TStamp,{Offset,_Sz},Acc) ->
            case (V =:= ?TOMBSTONE) orelse (TStamp < ExpiryTime) of
                true ->
                    Acc;
                false ->
                    case ebloom:contains(Bloom,K) of
                        true ->
                            Acc;
                        false ->
                            case bitcask_nifs:keydir_get(State#bc_state.keydir, K) of
                                not_found ->
                                    Acc;
                                E when is_record(E, bitcask_entry) ->
                                    case Offset =:= E#bitcask_entry.offset of
                                        false ->
                                            Acc;
                                        true ->
                                            ebloom:insert(Bloom,K),
                                            Fun(K,V,Acc)
                                    end;
                                {error,Reason} ->
                                    {error,Reason}
                            end
                    end
            end
    end,
    subfold(SubFun,ReadFiles,Acc0).
subfold(_SubFun,[],Acc) ->
    Acc;
subfold(SubFun,[File|Rest],Acc) ->
    {ok,FD} = bitcask_fileops:open_file(File),
    subfold(SubFun,Rest,bitcask_fileops:fold(FD,SubFun,Acc)).

%% @doc Merge several data files within a bitcask datastore into a more compact form.
-spec merge(Dirname::string()) -> ok | {error, any()}.
merge(Dirname) ->
    merge(Dirname, []).

%% @doc Merge several data files within a bitcask datastore into a more compact form.
-spec merge(Dirname::string(), Opts::[_]) -> ok | {error, any()}.
merge(Dirname, Opts) ->
    %% Make sure bitcask app is started so we can pull defaults from env
    ok = start_app(),

    %% Try to lock for merging
    case bitcask_lockops:acquire(merge, Dirname) of
        {ok, Lock} ->
            ok;
        {error, Reason} ->
            Lock = undefined,
            throw({error, {merge_locked, Reason}})
    end,

    %% Get the list of files we'll be merging
    ReadableFiles = readable_files(Dirname),

    %% Get the live keydir
    case bitcask_nifs:keydir_new(Dirname) of
        {ready, LiveKeyDir} ->
            %% Simplest case; a key dir is already available and loaded. Go ahead and open
            %% all the readable files
            ReadFiles = [begin {ok, Fstate} = bitcask_fileops:open_file(F), Fstate end
                         || F <- ReadableFiles];

        {not_ready, LiveKeyDir} ->
            %% Live keydir is newly created. We need to go ahead and load it
            %% up in case a writer or reader comes along in this same VM.
            ReadFiles = scan_key_files(ReadableFiles, LiveKeyDir, []),
            bitcask_nifs:keydir_mark_ready(LiveKeyDir);

        {error, not_ready} ->
            %% Someone else is loading the keydir. We'll bail here and try again
            %% later.
            LiveKeyDir = undefined, ReadFiles = [], % Make erlc happy w/ non-local exit
            throw({error, not_ready})
    end,

    %% Setup our first output merge file and update the merge lock accordingly
    {ok, Outfile} = bitcask_fileops:create_file(Dirname, Opts),
    ok = bitcask_lockops:write_activefile(Lock, bitcask_fileops:filename(Outfile)),

    %% Initialize the other keydirs we need. The hint keydir will get recreated
    %% each time we wrap a file, so that it only contains keys associated
    %% with the current out_file
    {ok, AllKeyDir} = bitcask_nifs:keydir_new(),
    {ok, HintKeyDir} = bitcask_nifs:keydir_new(),
    {ok, DelKeyDir} = bitcask_nifs:keydir_new(),

    %% Initialize our state for the merge
    State = #mstate { dirname = Dirname,
                      max_file_size = get_opt(max_file_size, Opts),
                      input_files = ReadFiles,
                      out_file = Outfile,
                      merged_files = [],
                      live_keydir = LiveKeyDir,
                      all_keydir = AllKeyDir,
                      hint_keydir = HintKeyDir,
                      del_keydir = DelKeyDir,
                      expiry_time = expiry_time(Opts),
                      opts = Opts },

    %% Finally, start the merge process
    State1 = merge_files(State),

    %% Make sure to close the final output file
    close_outfile(State1),

    %% Cleanup the original input files and release our lock
    [begin
         bitcask_fileops:delete(F),
         bitcask_fileops:close(F)
     end || F <- State#mstate.input_files],
    ok = bitcask_lockops:release(Lock).


%% ===================================================================
%% Internal functions
%% ===================================================================

expiry_time(Opts) ->
    ExpirySecs = get_opt(expiry_secs, Opts),
    case ExpirySecs > 0 of
        true -> bitcask_fileops:tstamp() - ExpirySecs;
        false -> 0
    end.

start_app() ->
    case application:start(?MODULE) of
        ok ->
            ok;
        {error, {already_started, ?MODULE}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

get_state(Ref) ->
    case erlang:get(Ref) of
        S when is_record(S, bc_state) ->
            S;
        undefined ->
            throw({error, {invalid_ref, Ref}})
    end.

get_opt(Key, Opts) ->
    case proplists:get_value(Key, Opts) of
        undefined ->
            case application:get_env(?MODULE, Key) of
                {ok, Value} -> Value;
                undefined -> undefined
            end;
        Value ->
            Value
    end.

put_state(Ref, State) ->
    erlang:put(Ref, State).

reverse_sort(L) ->
    lists:reverse(lists:sort(L)).

scan_key_files([], _KeyDir, Acc) ->
    Acc;
scan_key_files([Filename | Rest], KeyDir, Acc) ->
    {ok, File} = bitcask_fileops:open_file(Filename),
    F = fun(K, Tstamp, {Offset, TotalSz}, _) ->
                ok = bitcask_nifs:keydir_put(KeyDir,
                                             K,
                                             bitcask_fileops:file_tstamp(File),
                                             TotalSz,
                                             Offset,
                                             Tstamp)
        end,
    bitcask_fileops:fold_keys(File, F, undefined),
    scan_key_files(Rest, KeyDir, [File | Acc]).

%%
%% Wait for a named keydir to become ready for usage. The MillisToWait is
%% the remaining time we should continue waiting. Each wait is at max 100ms
%% to avoid blocking for longer than necessary, but still not do a lot of
%% extraneous polling.
%%
wait_for_keydir(Name, MillisToWait) ->
    case bitcask_nifs:keydir_new(Name) of
        {ready, KeyDir} ->
            {ok, KeyDir};

        {error, not_ready} ->
            timer:sleep(100),
            case MillisToWait of
                infinity ->
                    wait_for_keydir(Name, infinity);
                Value when Value =< 0 ->
                    timeout;
                _ ->
                    wait_for_keydir(Name, MillisToWait - 100)
            end
    end.


get_filestate(FileId, #bc_state{ dirname = Dirname, read_files = ReadFiles } = State) ->
    Fname = bitcask_fileops:mk_filename(Dirname, FileId),
    case lists:keysearch(Fname, #filestate.filename, ReadFiles) of
        {value, Filestate} ->
            {Filestate, State};
        false ->
            {ok, Filestate} = bitcask_fileops:open_file(Fname),
            {Filestate, State#bc_state { read_files = [Filestate | State#bc_state.read_files] }}
    end.

list_data_files(Dirname, WritingFile, Mergingfile) ->
    %% Build a list of {tstamp, filename} for all files in the directory that
    %% match our regex. Then reverse sort that list and extract the 
    %% fully-qualified filename.
    Files = filelib:fold_files(Dirname, "[0-9]+.bitcask.data", false,
                               fun(F, Acc) ->
                                    [{bitcask_fileops:file_tstamp(F), F} | Acc]
                               end, []),
    [F || {_Tstamp, F} <- reverse_sort(Files),
          F /= WritingFile,
          F /= Mergingfile].

merge_files(#mstate { input_files = [] } = State) ->
    State;
merge_files(#mstate { input_files = [File | Rest]} = State) ->
    F = fun(K, V, Tstamp, _Pos, State0) ->
                merge_single_entry(K, V, Tstamp, State0)
        end,
    State1 = bitcask_fileops:fold(File, F, State),
    ok = bitcask_fileops:close(File),
    merge_files(State1#mstate { input_files = Rest }).

merge_single_entry(K, V, Tstamp, #mstate { dirname = Dirname } = State) ->
    case out_of_date(K, Tstamp, State#mstate.expiry_time,
                     [State#mstate.all_keydir,
                      State#mstate.del_keydir]) of
        true ->
            State;
        false ->
            case (V =:= ?TOMBSTONE) of
                true ->
                    ok = bitcask_nifs:keydir_put(State#mstate.del_keydir, K,
                                                 Tstamp, 0, 0, Tstamp),
                    State;
                false ->
                    ok = bitcask_nifs:keydir_remove(State#mstate.del_keydir, K),

                    %% See if it's time to rotate to the next file
                    State1 =
                        case bitcask_fileops:check_write(State#mstate.out_file,
                                            K, V, State#mstate.max_file_size) of
                            wrap ->
                                %% Close the current output file
                                close_outfile(State),

                                %% Start our next file and update state
                                {ok, NewFile} = bitcask_fileops:create_file(Dirname,
                                                                            State#mstate.opts),
                                {ok, HintKeyDir} = bitcask_nifs:keydir_new(),
                                State#mstate { out_file = NewFile,
                                               hint_keydir = HintKeyDir };
                            ok ->
                                State
                        end,

                    {ok, Outfile, OffSet, Size} =
                        bitcask_fileops:write(State1#mstate.out_file,
                                              K, V, Tstamp),

                    %% Update live keydir for the current out file. It's possible that
                    %% this is a noop, as someone else may have written a newer value
                    %% whilst we were processing.
                    bitcask_nifs:keydir_put(State#mstate.live_keydir, K,
                                            bitcask_fileops:file_tstamp(Outfile),
                                            Size, OffSet, Tstamp),

                    %% Update the keydir for the current out file
                    ok = bitcask_nifs:keydir_put(State#mstate.hint_keydir, K,
                                         bitcask_fileops:file_tstamp(Outfile),
                                                 Size, OffSet, Tstamp),

                    %% Update the keydir of all keys we've seen over this merge
                    ok = bitcask_nifs:keydir_put(State#mstate.all_keydir, K,
                                         bitcask_fileops:file_tstamp(Outfile),
                                                 Size, OffSet, Tstamp),
                    State1#mstate { out_file = Outfile }
            end
    end.

close_outfile(_State=#mstate{out_file=OutFile,hint_keydir=HintKeyDir}) ->
    %% Close the current output file
    ok = bitcask_fileops:sync(OutFile),
    ok = bitcask_fileops:close(OutFile),

    %% Generate the hints file, using the keydir
    bitcask_fileops:create_hintfile(OutFile, HintKeyDir).


out_of_date(_Key, _Tstamp, _ExpiryTime, []) ->
    false;
out_of_date(_Key, Tstamp, ExpiryTime, _KeyDirs) when Tstamp < ExpiryTime ->
    true;
out_of_date(Key, Tstamp, ExpiryTime, [KeyDir|Rest]) ->
    case bitcask_nifs:keydir_get(KeyDir, Key) of
        not_found ->
            out_of_date(Key, Tstamp, ExpiryTime, Rest);

        E when is_record(E, bitcask_entry) ->
            case Tstamp < E#bitcask_entry.tstamp of
                true  -> true;
                false -> out_of_date(Key, Tstamp, ExpiryTime, Rest)
            end;

        {error, Reason} ->
            {error, Reason}
    end.

readable_files(Dirname) ->
    %% Check the write and/or merge locks to see what files are currently
    %% being written to. Generate our list excepting those.
    WritingFile = bitcask_lockops:read_activefile(write, Dirname),
    MergingFile = bitcask_lockops:read_activefile(merge, Dirname),
    list_data_files(Dirname, WritingFile, MergingFile).



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

init_dataset(Dirname, KVs) ->
    init_dataset(Dirname, [], KVs).

init_dataset(Dirname, Opts, KVs) ->
    os:cmd(?FMT("rm -rf ~s", [Dirname])),

    B = bitcask:open(Dirname, [read_write] ++ Opts),
    lists:foldl(fun({K, V}, _) ->
                        ok = bitcask:put(B, K, V)
                end, undefined, KVs),
    B.


default_dataset() ->
    [{<<"k">>, <<"v">>},
     {<<"k2">>, <<"v2">>},
     {<<"k3">>, <<"v3">>}].

%% HACK: Terrible hack to ensure that the .app file for
%% bitcask is available on the code path. Assumption here
%% is that we're running in .eunit/ as part of rebar.
a0_test() ->
    code:add_pathz("../ebin").

roundtrip_test() ->
    os:cmd("rm -rf /tmp/bc.test.roundtrip"),
    B = bitcask:open("/tmp/bc.test.roundtrip", [read_write]),
    ok = bitcask:put(B,<<"k">>,<<"v">>),
    {ok, <<"v">>} = bitcask:get(B,<<"k">>),
    ok = bitcask:put(B, <<"k2">>, <<"v2">>),
    ok = bitcask:put(B, <<"k">>,<<"v3">>),
    {ok, <<"v2">>} = bitcask:get(B, <<"k2">>),
    {ok, <<"v3">>} = bitcask:get(B, <<"k">>),
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

fold_test() ->
    B = init_dataset("/tmp/bc.test.fold", default_dataset()),

    File = (get_state(B))#bc_state.write_file,
    L = bitcask_fileops:fold(File, fun(K, V, _Ts, _Pos, Acc) ->
                                           [{K, V} | Acc]
                                   end, []),
    ?assertEqual(default_dataset(), lists:reverse(L)),
    close(B).

open_test() ->
    close(init_dataset("/tmp/bc.test.open", default_dataset())),

    B = bitcask:open("/tmp/bc.test.open"),
    lists:foldl(fun({K, V}, _) ->
                        {ok, V} = bitcask:get(B, K)
                end, undefined, default_dataset()).

wrap_test() ->
    %% Initialize dataset with max_file_size set to 1 so that each file will
    %% only contain a single key.
    close(init_dataset("/tmp/bc.test.wrap", [{max_file_size, 1}], default_dataset())),

    B = bitcask:open("/tmp/bc.test.wrap"),

    %% Check that all our data is available
    lists:foldl(fun({K, V}, _) ->
                        {ok, V} = bitcask:get(B, K)
                end, undefined, default_dataset()),

    %% Finally, verify that there are 4 files currently opened for read (one for each key
    %% and the initial writing one)
    4 = length(readable_files("/tmp/bc.test.wrap")).


merge_test() ->
    %% Initialize dataset with max_file_size set to 1 so that each file will
    %% only contain a single key.
    close(init_dataset("/tmp/bc.test.merge", [{max_file_size, 1}], default_dataset())),

    %% Verify number of files in directory
    4 = length(readable_files("/tmp/bc.test.merge")),

    %% Merge everything
    ok = merge("/tmp/bc.test.merge"),

    %% Verify we've now only got one file
    1 = length(readable_files("/tmp/bc.test.merge")),

    %% Make sure all the data is present
    B = bitcask:open("/tmp/bc.test.merge"),
    lists:foldl(fun({K, V}, _) ->
                        {ok, V} = bitcask:get(B, K)
                end, undefined, default_dataset()).


bitfold_test() ->
    os:cmd("rm -rf /tmp/bc.test.bitfold"),
    B = bitcask:open("/tmp/bc.test.bitfold", [read_write]),
    ok = bitcask:put(B,<<"k">>,<<"v">>),
    {ok, <<"v">>} = bitcask:get(B,<<"k">>),
    ok = bitcask:put(B, <<"k2">>, <<"v2">>),
    ok = bitcask:put(B, <<"k">>,<<"v3">>),
    {ok, <<"v2">>} = bitcask:get(B, <<"k2">>),
    {ok, <<"v3">>} = bitcask:get(B, <<"k">>),
    ok = bitcask:delete(B,<<"k">>),
    ok = bitcask:put(B, <<"k7">>,<<"v7">>),
    close(B),
    B2 = bitcask:open("/tmp/bc.test.bitfold"),
    true = ([{<<"k7">>,<<"v7">>},{<<"k2">>,<<"v2">>}] =:=
            bitcask:fold(B2,fun(K,V,Acc) -> [{K,V}|Acc] end,[])),
    close(B2),
    ok.

list_keys_test() ->
    os:cmd("rm -rf /tmp/bc.test.listkeys"),
    B = bitcask:open("/tmp/bc.test.listkeys", [read_write]),
    ok = bitcask:put(B,<<"k">>,<<"v">>),
    {ok, <<"v">>} = bitcask:get(B,<<"k">>),
    ok = bitcask:put(B, <<"k2">>, <<"v2">>),
    ok = bitcask:put(B, <<"k">>,<<"v3">>),
    {ok, <<"v2">>} = bitcask:get(B, <<"k2">>),
    {ok, <<"v3">>} = bitcask:get(B, <<"k">>),
    ok = bitcask:delete(B,<<"k">>),
    ok = bitcask:put(B, <<"k7">>,<<"v7">>),
    true = ([<<"k7">>,<<"k2">>] =:= bitcask:list_keys(B)),
    close(B),
    ok.

expire_test() ->
    os:cmd("rm -rf /tmp/bc.test.expire"),
    B = bitcask:open("/tmp/bc.test.expire", [read_write,{expiry_secs,2}]),
    ok = bitcask:put(B,<<"k">>,<<"v">>),
    {ok, <<"v">>} = bitcask:get(B,<<"k">>),
    ok = bitcask:put(B, <<"k2">>, <<"v2">>),
    ok = bitcask:put(B, <<"k">>,<<"v3">>),
    {ok, <<"v2">>} = bitcask:get(B, <<"k2">>),
    {ok, <<"v3">>} = bitcask:get(B, <<"k">>),
    timer:sleep(3000),
    ok = bitcask:put(B, <<"k7">>,<<"v7">>),
    true = ([<<"k7">>] =:= bitcask:list_keys(B)),
    close(B),
    ok.

expire_merge_test() ->
    %% Initialize dataset with max_file_size set to 1 so that each file will
    %% only contain a single key.
    close(init_dataset("/tmp/bc.test.mergeexpire", [{max_file_size, 1}],
                       default_dataset())),

    %% Wait for it all to expire
    timer:sleep(3000),

    %% Merge everything
    ok = merge("/tmp/bc.test.mergeexpire",[{expiry_secs,2}]),

    %% Verify we've now only got one file
    1 = length(readable_files("/tmp/bc.test.mergeexpire")),

    %% Make sure all the data is present
    B = bitcask:open("/tmp/bc.test.mergeexpire"),

    %% It's gone!
    true = ([] =:= bitcask:list_keys(B)),

    close(B),
    ok.
    

-endif.
