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
         fold_keys/3,
         fold/3,
         merge/1, merge/2, merge/3,
         needs_merge/1,
         status/1]).

-export([get_opt/2,
         get_filestate/2]).

-include("bitcask.hrl").


-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-compile(export_all).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-endif.

%% @type bc_state().
-record(bc_state, {dirname,
                   write_file,     % File for writing
                   write_lock,     % Reference to write lock
                   read_files,     % Files opened for reading
                   max_file_size,  % Max. size of a written file
                   opts,           % Original options used to open the bitcask
                   keydir}).       % Key directory

-record(mstate, { dirname,
                  merge_lock,
                  max_file_size,
                  input_files,
                  out_file,
                  merged_files,
                  partial,
                  live_keydir :: reference(),
                  hint_keydir,
                  del_keydir,
                  expiry_time,
                  opts }).

%% A bitcask is a directory containing:
%% * One or more data files - {integer_timestamp}.bitcask.data
%% * A write lock - bitcask.write.lock (Optional)
%% * A merge lock - bitcask.merge.lock (Optional)

%% @doc Open a new or existing bitcask datastore for read-only access.
-spec open(Dirname::string()) -> reference().
open(Dirname) ->
    open(Dirname, []).

%% @doc Open a new or existing bitcask datastore with additional options.
-spec open(Dirname::string(), Opts::[_]) -> reference().
open(Dirname, Opts) ->
    %% Make sure bitcask app is started so we can pull defaults from env
    ok = start_app(),

    %% Make sure the directory exists
    ok = filelib:ensure_dir(filename:join(Dirname, "bitcask")),

    %% If the read_write option is set, attempt to release any stale write lock.
    %% Do this first to avoid unnecessary processing of files for reading.
    WritingFile = case proplists:get_bool(read_write, Opts) of
        true ->
          bitcask_lockops:delete_stale_lock(write, Dirname),
          fresh;
        false -> undefined
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

            %% Now that we loaded all the data, mark the keydir as ready
            %% so other callers can use it
            ok = bitcask_nifs:keydir_mark_ready(KeyDir);

        {error, not_ready} ->
            %% Some other process is loading data into the keydir.
            %% Setup a blank ReadFiles (since we'll lazy load files)
            %% and wait for it to be ready.
            ReadFiles = [],
            WaitTime = timer:seconds(get_opt(open_timeout, Opts)),
            case wait_for_keydir(Dirname, WaitTime) of
                {ok, KeyDir} ->
                    ok;
                timeout ->
                    %% Well, we timed out waiting around for the other
                    %% process to load data from disk.
                    KeyDir = undefined, % Make erlc happy w/ non-local exit
                    throw({error, open_timeout})
            end
    end,

    %% Ensure that expiry_secs is in Opts and not just application env
    ExpOpts = [{expiry_secs,get_opt(expiry_secs,Opts)}|Opts],

    Ref = make_ref(),
    erlang:put(Ref, #bc_state {dirname = Dirname,
                               read_files = ReadFiles,
                               write_file = WritingFile, % <fd>|undefined|fresh
                               write_lock = undefined,
                               max_file_size = MaxFileSize,
                               opts = ExpOpts,
                               keydir = KeyDir}),

    Ref.

%% @doc Close a bitcask data store and flush any pending writes to disk.
-spec close(reference()) -> ok.
close(Ref) ->
    State = get_state(Ref),
    erlang:erase(Ref),

    %% Manually release the keydir. If, for some reason, this failed GC would
    %% still get the job done.
    bitcask_nifs:keydir_release(State#bc_state.keydir),

    %% Clean up all the reading files
    [ok = bitcask_fileops:close(F) || F <- State#bc_state.read_files],

    %% Cleanup the write file and associated lock
    case State#bc_state.write_file of
        undefined ->
            ok;
        fresh ->
            ok;
        _ ->
            ok = bitcask_fileops:close(State#bc_state.write_file),
            ok = bitcask_lockops:release(State#bc_state.write_lock)
    end.

%% @doc Retrieve a value by key from a bitcask datastore.
-spec get(reference(), binary()) ->
                 not_found | {ok, Value::binary()} | {error, Err::term()}.
get(Ref, Key) ->
    get(Ref, Key, 2).

-spec get(reference(), binary(), integer()) ->
                 not_found | {ok, Value::binary()} | {error, Err::term()}.
get(_Ref, _Key, 0) -> {error, nofile};
get(Ref, Key, TryNum) ->
    State = get_state(Ref),
    case bitcask_nifs:keydir_get(State#bc_state.keydir, Key) of
        not_found ->
            not_found;
        E when is_record(E, bitcask_entry) ->
            case E#bitcask_entry.tstamp < expiry_time(State#bc_state.opts) of
                true -> not_found;
                false ->
                    %% HACK: Use a fully-qualified call to get_filestate/2 so that
                    %% we can intercept calls w/ Pulse tests.
                    case ?MODULE:get_filestate(E#bitcask_entry.file_id, State) of
                        {error, enoent} ->
                            %% merging deleted file between keydir_get and here
                            get(Ref, Key, TryNum-1);
                        {Filestate, S2} ->
                            put_state(Ref, S2),
                            case bitcask_fileops:read(Filestate,
                                                    E#bitcask_entry.offset,
                                                    E#bitcask_entry.total_sz) of
                                {ok, _Key, ?TOMBSTONE} ->
                                    not_found;
                                {ok, _Key, Value} ->
                                    {ok, Value}
                            end
                    end
            end
    end.

%% @doc Store a key and value in a bitcase datastore.
-spec put(reference(), Key::binary(), Value::binary()) -> ok.
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
            LastWriteFile = bitcask_fileops:close_for_writing(WriteFile),
            {ok, NewWriteFile} = bitcask_fileops:create_file(
                                   State#bc_state.dirname,
                                   State#bc_state.opts),
            ok = bitcask_lockops:write_activefile(
                   State#bc_state.write_lock,
                   bitcask_fileops:filename(NewWriteFile)),
            State2 = State#bc_state{ write_file = NewWriteFile,
                                     read_files = [LastWriteFile | 
                                                   State#bc_state.read_files]};
        fresh ->
            %% Time to start our first write file.
            case bitcask_lockops:acquire(write, State#bc_state.dirname) of
                {ok, WriteLock} ->
                    {ok, NewWriteFile} = bitcask_fileops:create_file(
                                           State#bc_state.dirname,
                                           State#bc_state.opts),
                    ok = bitcask_lockops:write_activefile(
                           WriteLock,
                           bitcask_fileops:filename(NewWriteFile)),
                    State2 = State#bc_state{ write_file = NewWriteFile,
                                             write_lock = WriteLock };
                {error, Reason} ->
                    State2 = undefined,
                    throw({error, {write_locked, Reason}})
            end;

        ok ->
            State2 = State
    end,

    Tstamp = bitcask_fileops:tstamp(),
    {ok, WriteFile2, Offset, Size} = bitcask_fileops:write(
                                       State2#bc_state.write_file,
                                       Key, Value, Tstamp),
    ok = bitcask_nifs:keydir_put(State2#bc_state.keydir, Key,
                                 bitcask_fileops:file_tstamp(WriteFile2),
                                 Size, Offset, Tstamp),

    put_state(Ref, State2#bc_state { write_file = WriteFile2 }),
    ok.


%% @doc Delete a key from a bitcask datastore.
-spec delete(reference(), Key::binary()) -> ok.
delete(Ref, Key) ->
    put(Ref, Key, ?TOMBSTONE),
    ok = bitcask_nifs:keydir_remove((get_state(Ref))#bc_state.keydir, Key).

%% @doc Force any writes to sync to disk.
-spec sync(reference()) -> ok.
sync(Ref) ->
    State = get_state(Ref),
    case (State#bc_state.write_file) of
        undefined ->
            ok;
        fresh ->
            ok;
        File ->
            ok = bitcask_fileops:sync(File)
    end.


%% @doc List all keys in a bitcask datastore.
-spec list_keys(reference()) -> [Key::binary()] | {error, any()}.
list_keys(Ref) -> 
    fold_keys(Ref, fun(#bitcask_entry{key=K},Acc) -> [K|Acc] end, []).

%% @doc Fold over all keys in a bitcask datastore.
%% Must be able to understand the bitcask_entry record form.
-spec fold_keys(reference(), Fun::fun(), Acc::term()) ->
                                                       term() | {error, any()}.
fold_keys(Ref, Fun, Acc0) ->
    %% Fun should be of the form F(#bitcask_entry, A) -> A
    ExpiryTime = expiry_time((get_state(Ref))#bc_state.opts),
    RealFun = fun(BCEntry, Acc) ->
        Key = BCEntry#bitcask_entry.key,
        case BCEntry#bitcask_entry.tstamp < ExpiryTime of
            true ->
                Acc;
            false ->
                TSize = size(?TOMBSTONE),
                case BCEntry#bitcask_entry.total_sz -
                            (?HEADER_SIZE + size(Key)) of
                    TSize ->  % might be a deleted record, so check
                        case ?MODULE:get(Ref, Key) of
                            not_found -> Acc;
                            _ -> Fun(BCEntry, Acc)
                        end;
                    _ ->
                        Fun(BCEntry, Acc)
                end
        end
    end,
    bitcask_nifs:keydir_fold((get_state(Ref))#bc_state.keydir, RealFun, Acc0).

%% @doc fold over all K/V pairs in a bitcask datastore.
%% Fun is expected to take F(K,V,Acc0) -> Acc
-spec fold(reference(), fun((binary(), binary(), any()) -> any()), any()) -> any() | {error, any()}.
fold(Ref, Fun, Acc0) ->
    State = get_state(Ref),

    case open_fold_files(State#bc_state.dirname, 3) of
        {ok, Files} ->
            {_,_,Tseed} = now(),
            {ok, Bloom} = ebloom:new(1000000,0.00003,Tseed), % arbitrary large bloom
            ExpiryTime = expiry_time(State#bc_state.opts),
            SubFun = fun(K,V,TStamp,{Offset,_Sz},Acc) ->
                             case ebloom:contains(Bloom,K) orelse (TStamp < ExpiryTime) of
                                 true ->
                                     Acc;
                                 false ->
                                     case bitcask_nifs:keydir_get(
                                            State#bc_state.keydir, K) of
                                         not_found ->
                                             Acc;
                                         E when is_record(E, bitcask_entry) ->
                                             case Offset =:= E#bitcask_entry.offset of
                                                 false ->
                                                     Acc;
                                                 true ->
                                                     ebloom:insert(Bloom,K),
                                                     case V =:= ?TOMBSTONE of
                                                         true ->
                                                             Acc;
                                                         false ->
                                                             Fun(K,V,Acc)
                                                     end
                                             end
                                     end
                             end
                     end,
            subfold(SubFun,Files,Acc0);

        {error, Reason} ->
            {error, Reason}
    end.

%%
%% Get a list of readable files and attempt to open them for a fold. If we can't
%% open any one of the files, get a fresh list of files and try again.
%%
open_fold_files(_Dirname, 0) ->
    {error, max_retries_exceeded_for_fold};
open_fold_files(Dirname, Count) ->
    Filenames = list_data_files(Dirname, undefined, undefined),
    case open_files(Filenames, []) of
        {ok, Files} ->
            {ok, Files};
        error ->
            open_fold_files(Dirname, Count-1)
    end.

%%
%% Open a list of filenames; if any one of them fails to open, error out.
%%
open_files([], Acc) ->
    {ok, lists:reverse(Acc)};
open_files([Filename | Rest], Acc) ->
    case bitcask_fileops:open_file(Filename) of
        {ok, Fd} ->
            open_files(Rest, [Fd | Acc]);
        {error, _} ->
            [bitcask_fileops:close(Fd) || Fd <- Acc],
            error
    end.

%%
%% Apply fold function to a single bitcask file; results are accumulated in
%% Acc
%%
subfold(_SubFun,[],Acc) ->
    Acc;
subfold(SubFun,[FD | Rest],Acc0) ->
    Acc = bitcask_fileops:fold(FD, SubFun, Acc0),
    bitcask_fileops:close(FD),
    subfold(SubFun,Rest,Acc).

%% @doc Merge several data files within a bitcask datastore
%%      into a more compact form.
-spec merge(Dirname::string()) -> ok.
merge(Dirname) ->
    merge(Dirname, [], readable_files(Dirname)).

%% @doc Merge several data files within a bitcask datastore
%%      into a more compact form.
-spec merge(Dirname::string(), Opts::[_]) -> ok.
merge(Dirname, Opts) ->
    merge(Dirname, Opts, readable_files(Dirname)).

%% @doc Merge several data files within a bitcask datastore
%%      into a more compact form.
-spec merge(Dirname::string(), Opts::[_], FilesToMerge::[string()]) -> ok.
merge(_Dirname, _Opts, []) ->
    ok;
merge(Dirname, Opts, FilesToMerge0) ->
    %% Make sure bitcask app is started so we can pull defaults from env
    ok = start_app(),

    %% Filter the files to merge and ensure that they all exist. It's
    %% possible in some circumstances that we'll get an out-of-date
    %% list of files.
    FilesToMerge = [F || F <- FilesToMerge0,
                         filelib:is_file(F)],
    merge1(Dirname, Opts, FilesToMerge).

%% Inner merge function, assumes that bitcask is running and all files exist.
merge1(_Dirname, _Opts, []) ->
    ok;
merge1(Dirname, Opts, FilesToMerge) ->
    %% Test to see if this is a complete or partial merge
    Partial = not(lists:usort(readable_files(Dirname)) == 
                  lists:usort(FilesToMerge)),
    
    %% Try to lock for merging
    case bitcask_lockops:acquire(merge, Dirname) of
        {ok, Lock} ->
            ok;
        {error, Reason} ->
            Lock = undefined,
            throw({error, {merge_locked, Reason}})
    end,

    %% Get the live keydir
    case bitcask_nifs:keydir_new(Dirname) of
        {ready, LiveKeyDir} ->
            %% Simplest case; a key dir is already available and
            %% loaded. Go ahead and open just the files we wish to
            %% merge
            InFiles = [begin 
                           {ok, Fstate} = bitcask_fileops:open_file(F),
                           Fstate
                       end
                       || F <- FilesToMerge];

        {not_ready, LiveKeyDir} ->
            %% Live keydir is newly created. We need to go ahead and
            %% load all available data into the keydir in case another
            %% reader/writer comes along in the same VM. Note that we
            %% won't necessarily merge all these files.
            AllFiles = scan_key_files(readable_files(Dirname), LiveKeyDir, []),

            %% Partition all files to files we'll merge and files we
            %% won't (so that we can close those extra files once
            %% they've been loaded into the keydir)
            P = fun(F) ->
                        lists:member(bitcask_fileops:filename(F), FilesToMerge)
                end,
            {InFiles, UnusedFiles} = lists:partition(P, AllFiles),

            %% Close the unused files
            [bitcask_fileops:close(U) || U <- UnusedFiles],

            bitcask_nifs:keydir_mark_ready(LiveKeyDir);

        {error, not_ready} ->
            %% Someone else is loading the keydir. We'll bail here and
            %% try again later.

            % Make erlc happy w/ non-local exit
            LiveKeyDir = undefined, InFiles = [],
            throw({error, not_ready})
    end,

    %% Setup our first output merge file and update the merge lock accordingly
    {ok, Outfile} = bitcask_fileops:create_file(Dirname, Opts),
    ok = bitcask_lockops:write_activefile(
           Lock, bitcask_fileops:filename(Outfile)),

    %% Initialize the other keydirs we need.
    {ok, DelKeyDir} = bitcask_nifs:keydir_new(),

    %% Initialize our state for the merge
    State = #mstate { dirname = Dirname,
                      merge_lock = Lock,
                      max_file_size = get_opt(max_file_size, Opts),
                      input_files = InFiles,
                      out_file = Outfile,
                      merged_files = [],
                      partial = Partial,
                      live_keydir = LiveKeyDir,
                      del_keydir = DelKeyDir,
                      expiry_time = expiry_time(Opts),
                      opts = Opts },

    %% Finally, start the merge process
    State1 = merge_files(State),

    %% Make sure to close the final output file
    ok = bitcask_fileops:sync(State1#mstate.out_file),
    ok = bitcask_fileops:close(State1#mstate.out_file),

    %% Explicitly release our keydirs instead of waiting for GC
    bitcask_nifs:keydir_release(LiveKeyDir),
    bitcask_nifs:keydir_release(DelKeyDir),    

    %% Cleanup the original input files and release our lock
    [begin
         bitcask_fileops:delete(F),
         bitcask_fileops:close(F)
     end || F <- State#mstate.input_files],
    ok = bitcask_lockops:release(Lock).

-spec needs_merge(reference()) -> {true, [string()]} | false.
needs_merge(Ref) ->
    State = get_state(Ref),
    {_KeyCount, Summary} = status(Ref),

    %% Review all the files we currently have open in read_files and
    %% see if any no longer exist by name (i.e. have been deleted by
    %% previous merges). Close these files so that we don't leak
    %% file descriptors.
    P = fun(F) ->
                filelib:is_file(bitcask_fileops:filename(F))
        end,
    {LiveFiles, DeadFiles} = lists:partition(P, State#bc_state.read_files),

    %% Close the dead files
    [bitcask_fileops:close(F) || F <- DeadFiles],

    %% Update state with live files
    put_state(Ref, State#bc_state { read_files = LiveFiles }),

    %% Triggers that would require a merge:
    %%
    %% frag_merge_trigger - Any file exceeds this % fragmentation
    %% dead_bytes_merge_trigger - Any file has more than this # of dead bytes
    %%
    FragTrigger = get_opt(frag_merge_trigger, State#bc_state.opts),
    DeadBytesTrigger = get_opt(dead_bytes_merge_trigger, State#bc_state.opts),
    NeedsMerge = lists:any(fun({_FileName, Frag, DeadBytes, _}) ->
                                   (Frag >= FragTrigger)
                                       or (DeadBytes >= DeadBytesTrigger)
                           end, Summary),

    case NeedsMerge of
        true ->
            %% Identify those files which need merging. We merge any files
            %% that meet ANY of the following conditions:
            %%
            %% frag_threshold - At least this % fragmented
            %% dead_bytes_threshold - At least this # of dead bytes
            %% small_file_threshold - Any file < this # of bytes
            %%
            FragThreshold = get_opt(frag_threshold,
                                    State#bc_state.opts),
            DeadBytesThreshold = get_opt(dead_bytes_threshold,
                                         State#bc_state.opts),
            SmallFileThreshold = get_opt(small_file_threshold,
                                         State#bc_state.opts),
            FileNames = [FileName ||
                          {FileName, Frag, DeadBytes, TotalBytes} <- Summary,
                            (Frag >= FragThreshold)
                                or (DeadBytes >= DeadBytesThreshold)
                                or (TotalBytes < SmallFileThreshold)],
            {true, FileNames};
        false ->
            false
    end.

-spec status(reference()) -> {integer(), [{string(), integer(), integer(), integer()}]}.
status(Ref) ->
    State = get_state(Ref),

    %% Pull current info for the bitcask. In particular, we want
    %% the file stats so we can determine how much fragmentation
    %% is present
    %%
    %% Fstat has form: [{FileId, LiveCount, TotalCount, LiveBytes, TotalBytes}]
    %% and is only an estimate/snapshot.
    {KeyCount, _KeyBytes, Fstats} = bitcask_nifs:keydir_info(
                                      State#bc_state.keydir),

    %% We want to ignore the file currently being written when
    %% considering status!
    case bitcask_lockops:read_activefile(write, State#bc_state.dirname) of
        undefined ->
            WritingFileId = undefined;
        Filename ->
            WritingFileId = bitcask_fileops:file_tstamp(Filename)
    end,

    %% Convert fstats list into a list with details we're interested in,
    %% specifically:
    %% [{FileName, % Fragmented, Dead Bytes, Total Bytes}]
    %%
    %% Note that we also, filter the WritingFileId from any further
    %% consideration.
    Summary0 = [summarize(State#bc_state.dirname, S) ||
                   S <- Fstats, element(1, S) /= WritingFileId],

    %% Remove any files that don't exist from the initial summary
    Summary = lists:keysort(1, [S || S <- Summary0,
                                     filelib:is_file(element(1, S))]),
    {KeyCount, Summary}.


%% ===================================================================
%% Internal functions
%% ===================================================================

summarize(Dirname, {FileId, LiveCount, TotalCount, LiveBytes, TotalBytes}) ->
    Fragmented = trunc((1 - LiveCount/TotalCount) * 100),
    DeadBytes = TotalBytes - LiveBytes,
    {bitcask_fileops:mk_filename(Dirname, FileId), 
     Fragmented, DeadBytes, TotalBytes}.

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
                bitcask_nifs:keydir_put(KeyDir,
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
                Value when is_integer(Value), Value =< 0 -> %% avoids 'infinity'!
                    timeout;
                _ ->
                    wait_for_keydir(Name, MillisToWait - 100)
            end
    end.


get_filestate(FileId,
              State=#bc_state{ dirname = Dirname, read_files = ReadFiles }) ->
    Fname = bitcask_fileops:mk_filename(Dirname, FileId),
    case lists:keysearch(Fname, #filestate.filename, ReadFiles) of
        {value, Filestate} ->
            {Filestate, State};
        false ->
            case bitcask_fileops:open_file(Fname) of
                {error,enoent} ->
                    %% merge removed the file since the keydir_get
                    {error, enoent};
                {ok, Filestate} ->
                    {Filestate, State#bc_state{read_files =
                                      [Filestate | State#bc_state.read_files]}}
            end
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
    FileId = bitcask_fileops:file_tstamp(File),
    F = fun(K, V, Tstamp, Pos, State0) ->
                merge_single_entry(K, V, Tstamp, FileId, Pos, State0)
        end,
    State1 = bitcask_fileops:fold(File, F, State),
    ok = bitcask_fileops:close(File),
    merge_files(State1#mstate { input_files = Rest }).

merge_single_entry(K, V, Tstamp, FileId, {Offset, _} = Pos, State) ->
    case out_of_date(K, Tstamp, FileId, Pos, State#mstate.expiry_time, false,
                     [State#mstate.live_keydir, State#mstate.del_keydir]) of
        true ->
            bitcask_nifs:keydir_remove(State#mstate.live_keydir, K,
                                       Tstamp, FileId, Offset),
            State;
        false ->
            case (V =:= ?TOMBSTONE) of
                true ->
                    ok = bitcask_nifs:keydir_put(State#mstate.del_keydir, K,
                                                 FileId, 0, Offset, Tstamp),

                    %% Use the conditional remove on the live
                    %% keydir. We only want to actually remove
                    %% whatever is in the live keydir IIF the
                    %% tstamp/fileid we have matches the current
                    %% entry.
                    bitcask_nifs:keydir_remove(State#mstate.live_keydir, K,
                                               Tstamp, FileId, Offset),

                    case State#mstate.partial of
                        true -> inner_merge_write(K, V, Tstamp, State);
                        false -> State
                    end;
                false ->
                    ok = bitcask_nifs:keydir_remove(State#mstate.del_keydir, K),
                    inner_merge_write(K, V, Tstamp, State)
            end
    end.

-spec inner_merge_write(binary(), binary(), integer(), #mstate{}) -> #mstate{}.

inner_merge_write(K, V, Tstamp, State) ->
    %% write a single item while inside the merge process

    %% See if it's time to rotate to the next file
    State1 =
        case bitcask_fileops:check_write(State#mstate.out_file,
                                         K, V, State#mstate.max_file_size) of
            wrap ->
                %% Close the current output file
                ok = bitcask_fileops:sync(State#mstate.out_file),
                ok = bitcask_fileops:close(State#mstate.out_file),
                
                %% Start our next file and update state
                {ok, NewFile} = bitcask_fileops:create_file(
                                  State#mstate.dirname,
                                  State#mstate.opts),
                NewFileName = bitcask_fileops:filename(NewFile),
                ok = bitcask_lockops:write_activefile(
                       State#mstate.merge_lock,
                       NewFileName),
                State#mstate { out_file = NewFile };
            ok ->
                State
        end,
    
    {ok, Outfile, Offset, Size} =
        bitcask_fileops:write(State1#mstate.out_file,
                              K, V, Tstamp),
    
    %% Update live keydir for the current out
    %% file. It's possible that this is a noop, as
    %% someone else may have written a newer value
    %% whilst we were processing.
    bitcask_nifs:keydir_put(
      State#mstate.live_keydir, K,
      bitcask_fileops:file_tstamp(Outfile),
      Size, Offset, Tstamp),
    
    State1#mstate { out_file = Outfile }.


out_of_date(_Key, _Tstamp, _FileId, _Pos, _ExpiryTime, EverFound, []) ->
    %% if we ever found it, and non of the entries were out of date,
    %% then it's not out of date
    EverFound == false;
out_of_date(_Key, Tstamp, _FileId, _Pos, ExpiryTime, _EverFound, _KeyDirs)
  when Tstamp < ExpiryTime ->
    true;
out_of_date(Key, Tstamp, FileId, {Offset,_} = Pos, ExpiryTime, EverFound,
            [KeyDir|Rest]) ->
    case bitcask_nifs:keydir_get(KeyDir, Key) of
        not_found ->
            out_of_date(Key, Tstamp, FileId, Pos, ExpiryTime, EverFound, Rest);

        E when is_record(E, bitcask_entry) ->
            if
                E#bitcask_entry.tstamp == Tstamp ->
                    %% Exact match. In this situation, we use the file
                    %% id and offset as a tie breaker. The assumption
                    %% is that the merge starts with the newest files
                    %% first, thus we want data from the highest
                    %% file_id and the highest offset in that file.
                    if
                        E#bitcask_entry.file_id > FileId ->
                            true;

                        E#bitcask_entry.file_id == FileId ->
                            case E#bitcask_entry.offset > Offset of
                                true ->
                                    true;
                                false ->
                                    out_of_date(
                                      Key, Tstamp, FileId, Pos, 
                                      ExpiryTime, true, Rest)
                            end;

                        true ->
                            %% At this point the following conditions are true:
                            %% The file_id in the keydir is older (<) the file
                            %% id we're currently merging...
                            %%
                            %% OR:
                            %%
                            %% The file_id in the keydir is the same (==) as the
                            %% file we're merging BUT the offset the keydir has
                            %% is older (<=) the offset we are currently
                            %% processing.
                            %%
                            %% Thus, we are NOT out of date. Check the
                            %% rest of the keydirs to ensure this
                            %% holds true.
                            out_of_date(Key, Tstamp, FileId, Pos,
                                        ExpiryTime, true, Rest)
                    end;

                E#bitcask_entry.tstamp < Tstamp ->
                    %% Not out of date -- check rest of the keydirs
                    out_of_date(Key, Tstamp, FileId, Pos,
                                ExpiryTime, true, Rest);

                true ->
                    %% Out of date!
                    true
            end
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

write_lock_perms_test() ->
    os:cmd("rm -rf /tmp/bc.test.writelockperms"),
    B = bitcask:open("/tmp/bc.test.writelockperms", [read_write]),
    ok = bitcask:put(B, <<"k">>, <<"v">>),
    {ok, Info} = file:read_file_info("/tmp/bc.test.writelockperms/bitcask.write.lock"),
    ?assertEqual(8#00600, Info#file_info.mode band 8#00600).

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
    close(init_dataset("/tmp/bc.test.wrap",
                       [{max_file_size, 1}], default_dataset())),

    B = bitcask:open("/tmp/bc.test.wrap"),

    %% Check that all our data is available
    lists:foldl(fun({K, V}, _) ->
                        {ok, V} = bitcask:get(B, K)
                end, undefined, default_dataset()),

    %% Finally, verify that there are 3 files currently opened for read
    %% (one for each key)
    3 = length(readable_files("/tmp/bc.test.wrap")).


merge_test() ->
    %% Initialize dataset with max_file_size set to 1 so that each file will
    %% only contain a single key.
    close(init_dataset("/tmp/bc.test.merge",
                       [{max_file_size, 1}], default_dataset())),

    %% Verify number of files in directory
    3 = length(readable_files("/tmp/bc.test.merge")),

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
    true = ([<<"k2">>,<<"k7">>] =:= lists:sort(bitcask:list_keys(B))),
    close(B),
    ok.

expire_test() ->
    os:cmd("rm -rf /tmp/bc.test.expire"),
    B = bitcask:open("/tmp/bc.test.expire", [read_write,{expiry_secs,1}]),
    ok = bitcask:put(B,<<"k">>,<<"v">>),
    {ok, <<"v">>} = bitcask:get(B,<<"k">>),
    ok = bitcask:put(B, <<"k2">>, <<"v2">>),
    ok = bitcask:put(B, <<"k">>,<<"v3">>),
    {ok, <<"v2">>} = bitcask:get(B, <<"k2">>),
    {ok, <<"v3">>} = bitcask:get(B, <<"k">>),
    timer:sleep(2000),
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
    timer:sleep(2000),

    %% Merge everything
    ok = merge("/tmp/bc.test.mergeexpire",[{expiry_secs,1}]),

    %% Verify we've now only got one file
    1 = length(readable_files("/tmp/bc.test.mergeexpire")),

    %% Make sure all the data is present
    B = bitcask:open("/tmp/bc.test.mergeexpire"),

    %% It's gone!
    true = ([] =:= bitcask:list_keys(B)),

    close(B),
    ok.

fold_deleted_test() ->    
    os:cmd("rm -rf /tmp/bc.test.fold_delete"),
    B = bitcask:open("/tmp/bc.test.fold_delete",
                     [read_write,{max_file_size, 1}]),
    ok = bitcask:put(B,<<"k">>,<<"v">>),
    ok = bitcask:delete(B,<<"k">>),
    true = ([] =:= bitcask:fold(B, fun(K, V, Acc0) -> [{K,V}|Acc0] end, [])),
    close(B),
    ok.

lazy_open_test() ->
    os:cmd("rm -rf /tmp/bc.test.opp_open"),

    %% Just opening/closing should not create any files.
    B1 = bitcask:open("/tmp/bc.test.opp_open", [read_write]),
    bitcask:close(B1),
    0 = length(readable_files("/tmp/bc.test.opp_open")),

    B2 = bitcask:open("/tmp/bc.test.opp_open", [read_write]),
    ok = bitcask:put(B2,<<"k">>,<<"v">>),
    bitcask:close(B2),
    B3 = bitcask:open("/tmp/bc.test.opp_open", [read_write]),
    bitcask:close(B3),
    1 = length(readable_files("/tmp/bc.test.opp_open")),
    ok.

open_reset_open_test() ->
    os:cmd("rm -rf /tmp/bc.test.twice"),
    B1 = bitcask:open("/tmp/bc.test.twice", [read_write]),
    ok = bitcask:put(B1,<<"k">>,<<"v">>),
    bitcask:close(B1),
    os:cmd("rm -rf /tmp/bc.test.twice"),
    B2 = bitcask:open("/tmp/bc.test.twice", [read_write]),
    ok = bitcask:put(B2,<<"x">>,<<"q">>),
    not_found = bitcask:get(B2,<<"k">>),
    bitcask:close(B2).

delete_merge_test() ->
    %% Initialize dataset with max_file_size set to 1 so that each file will
    %% only contain a single key.
    close(init_dataset("/tmp/bc.test.delmerge", [{max_file_size, 1}],
                       default_dataset())),

    %% perform some deletes, tombstones should go in their own files
    B1 = bitcask:open("/tmp/bc.test.delmerge", [read_write,{max_file_size, 1}]),
    ok = bitcask:delete(B1,<<"k2">>),
    ok = bitcask:delete(B1,<<"k3">>),
    A1 = [<<"k">>],
    A1 = bitcask:list_keys(B1),
    close(B1),

    ok = merge("/tmp/bc.test.delmerge",[]),

    %% Verify we've now only got one item left
    B2 = bitcask:open("/tmp/bc.test.delmerge"),
    A = [<<"k">>],
    A = bitcask:list_keys(B2),
    close(B2),

    ok.

delete_partial_merge_test() ->
    %% Initialize dataset with max_file_size set to 1 so that each file will
    %% only contain a single key.
    close(init_dataset("/tmp/bc.test.pardel", [{max_file_size, 1}],
                       default_dataset())),

    %% perform some deletes, tombstones should go in their own files
    B1 = bitcask:open("/tmp/bc.test.pardel", [read_write,{max_file_size, 1}]),
    ok = bitcask:delete(B1,<<"k2">>),
    ok = bitcask:delete(B1,<<"k3">>),
    A1 = [<<"k">>],
    A1 = bitcask:list_keys(B1),
    close(B1),

    %% selective merge, hit all of the files with deletes but not
    %%  all of the ones with deleted data
    ok = merge("/tmp/bc.test.pardel",[],lists:reverse(lists:nthtail(2,
                                           lists:reverse(readable_files(
                                               "/tmp/bc.test.pardel"))))),

    %% Verify we've now only got one item left
    B2 = bitcask:open("/tmp/bc.test.pardel"),
    A = [<<"k">>],
    A = bitcask:list_keys(B2),
    close(B2),

    ok.

corrupt_file_test() ->
    os:cmd("rm -rf /tmp/bc.test.corrupt"),
    B1 = bitcask:open("/tmp/bc.test.corrupt", [read_write]),
    ok = bitcask:put(B1,<<"k">>,<<"v">>),
    {ok, <<"v">>} = bitcask:get(B1,<<"k">>),
    close(B1),

    %% write bogus data at end of hintfile, verify non-crash
    os:cmd("rm -rf /tmp/bc.test.corrupt.hint"),
    os:cmd("mkdir /tmp/bc.test.corrupt.hint"),
    os:cmd("cp -r /tmp/bc.test.corrupt/*hint /tmp/bc.test.corrupt.hint/100.bitcask.hint"),
    os:cmd("cp -r /tmp/bc.test.corrupt/*data /tmp/bc.test.corrupt.hint/100.bitcask.data"),
    HFN = "/tmp/bc.test.corrupt.hint/100.bitcask.hint",
    {ok, HFD} = file:open(HFN, [append, raw, binary]),
    ok = file:write(HFD, <<"1">>),
    file:close(HFD),    
    B2 = bitcask:open("/tmp/bc.test.corrupt.hint"),
    {ok, <<"v">>} = bitcask:get(B2,<<"k">>),
    close(B2),

    %% write bogus data at end of datafile, no hintfile, verify non-crash
    os:cmd("rm -rf /tmp/bc.test.corrupt.data"),
    os:cmd("mkdir /tmp/bc.test.corrupt.data"),
    os:cmd("cp -r /tmp/bc.test.corrupt/*data /tmp/bc.test.corrupt.data/100.bitcask.data"),
    DFN = "/tmp/bc.test.corrupt.data/100.bitcask.data",
    {ok, DFD} = file:open(DFN, [append, raw, binary]),
    ok = file:write(DFD, <<"2">>),
    file:close(DFD),    
    B3 = bitcask:open("/tmp/bc.test.corrupt.data"),
    {ok, <<"v">>} = bitcask:get(B3,<<"k">>),
    close(B3),

    %% as above, but more than just headersize data
    os:cmd("rm -rf /tmp/bc.test.corrupt.data2"),
    os:cmd("mkdir /tmp/bc.test.corrupt.data2"),
    os:cmd("cp -r /tmp/bc.test.corrupt/*data /tmp/bc.test.corrupt.data2/100.bitcask.data"),
    D2FN = "/tmp/bc.test.corrupt.data2/100.bitcask.data",
    {ok, D2FD} = file:open(D2FN, [append, raw, binary]),
    ok = file:write(D2FD, <<"123456789012345">>),
    file:close(D2FD),    
    B4 = bitcask:open("/tmp/bc.test.corrupt.data2"),
    {ok, <<"v">>} = bitcask:get(B4,<<"k">>),
    close(B4),

    ok.

testhelper_keydir_count(B) ->
    KD = (get_state(B))#bc_state.keydir,
    {KeyCount,_,_} = bitcask_nifs:keydir_info(KD),
    KeyCount.
    
expire_keydir_test() ->
    %% Initialize dataset with max_file_size set to 1 so that each file will
    %% only contain a single key.
    close(init_dataset("/tmp/bc.test.mergeexpirekeydir", [{max_file_size, 1}],
                       default_dataset())),

    KDB = bitcask:open("/tmp/bc.test.mergeexpirekeydir"),

    %% three keys in the keydir now
    3 = testhelper_keydir_count(KDB),

    %% Wait for it all to expire
    timer:sleep(2000),

    %% Merge everything
    ok = merge("/tmp/bc.test.mergeexpirekeydir",[{expiry_secs,1}]),

    %% should be no keys in the keydir now
    0 = testhelper_keydir_count(KDB),

    bitcask:close(KDB),
    ok.

delete_keydir_test() ->
    close(init_dataset("/tmp/bc.test.deletekeydir", [],
                       default_dataset())),

    KDB = bitcask:open("/tmp/bc.test.deletekeydir", [read_write]),

    %% three keys in the keydir now
    3 = testhelper_keydir_count(KDB),

    %% Delete something
    ok = bitcask:delete(KDB, <<"k">>),

    %% should be 2 keys in the keydir now
    2 = testhelper_keydir_count(KDB),

    bitcask:close(KDB),
    ok.

frag_status_test() ->
    B1 = bitcask:open("/tmp/bc.test.fragtest", [read_write]),
    ok = bitcask:put(B1,<<"k">>,<<"v">>),
    ok = bitcask:put(B1,<<"k">>,<<"z">>),
    ok = bitcask:close(B1),
    % close and reopen so that status can reflect a closed file
    B2 = bitcask:open("/tmp/bc.test.fragtest", [read_write]),
    {1,[{_,50,16,32}]} = bitcask:status(B2),
    %% 1 key, 50% frag, 16 dead bytes, 32 total bytes
    ok.

-endif.

