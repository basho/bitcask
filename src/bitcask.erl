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
         close_write_file/1,
         get/2,
         put/3,
         delete/2,
         sync/1,
         list_keys/1,
         fold_keys/3, fold_keys/6,
         fold/3, fold/6,
         iterator/3, iterator_next/1, iterator_release/1,
         merge/1, merge/2, merge/3,
         needs_merge/1,
         is_empty_estimate/1,
         status/1]).

-export([get_opt/2,
         get_filestate/2,
         is_tombstone/1]).
-export([has_setuid_bit/1]).                    % For EUnit tests

-include_lib("kernel/include/file.hrl").
-include("bitcask.hrl").


-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-compile(export_all).
-define(OPEN_FOLD_RETRIES, 100).
-else.
-define(OPEN_FOLD_RETRIES, 3).
-endif.

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-export([leak_t0/0, leak_t1/0]).
-endif.

%% In the real world, 1 or 2 retries is usually sufficient.  In the
%% world of QuickCheck, however, QC can create some diabolical
%% races, so use a diabolical number.
-define(DIABOLIC_BIG_INT, 100).

%% In an EQC testing scenario, poll_for_merge_lock() may have failed
%% This atom is the signal that it failed but is harmless in this situation.
-define(POLL_FOR_MERGE_LOCK_PSEUDOFAILURE, pseudo_failure).

%% @type bc_state().
-record(bc_state, {dirname :: string(),
                   write_file :: 'fresh' | 'undefined' | #filestate{},     % File for writing
                   write_lock :: reference(),     % Reference to write lock
                   read_files :: [#filestate{}],     % Files opened for reading
                   max_file_size :: integer(),  % Max. size of a written file
                   opts :: list(),           % Original options used to open the bitcask
                   key_transform :: function(),
                   keydir :: reference(),
                   read_write_p :: integer()    % integer() avoids atom -> NIF
                  }).       % Key directory

-record(mstate, { dirname :: string(),
                  merge_lock :: reference(),
                  max_file_size :: integer(),
                  input_files :: [#filestate{}],
                  out_file :: 'fresh' | #filestate{},
                  partial :: boolean(),
                  live_keydir :: reference(),
                  del_keydir :: reference(),
                  expiry_time :: integer(),
                  expiry_grace_time :: integer(),
                  key_transform :: function(),
                  read_write_p :: integer(),    % integer() avoids atom -> NIF
                  opts :: list(),
                  delete_files :: [#filestate{}]}).

%% A bitcask is a directory containing:
%% * One or more data files - {integer_timestamp}.bitcask.data
%% * A write lock - bitcask.write.lock (Optional)
%% * A merge lock - bitcask.merge.lock (Optional)

%% @doc Open a new or existing bitcask datastore for read-only access.
-spec open(Dirname::string()) -> reference() | {error, timeout}.
open(Dirname) ->
    open(Dirname, []).

%% @doc Open a new or existing bitcask datastore with additional options.
-spec open(Dirname::string(), Opts::[_]) -> 
    reference() | {error, timeout}.
open(Dirname, Opts) ->
    %% Make sure bitcask app is started so we can pull defaults from env
    ok = start_app(),

    %% Make sure the directory exists
    ok = filelib:ensure_dir(filename:join(Dirname, "bitcask")),

    %% If the read_write option is set, attempt to release any stale write lock.
    %% Do this first to avoid unnecessary processing of files for reading.
    WritingFile = case proplists:get_bool(read_write, Opts) of
        true ->
          %% If the lock file is not stale, we'll continue initializing
          %% and loading anyway: if later someone tries to write
          %% something, that someone will get a write_locked exception.
          _ = bitcask_lockops:delete_stale_lock(write, Dirname),
          fresh;
        false -> undefined
    end,

    %% Get the max file size parameter from opts
    MaxFileSize = get_opt(max_file_size, Opts),

    %% Get the number of seconds we are willing to wait for the keydir init to timeout
    WaitTime = timer:seconds(get_opt(open_timeout, Opts)),

    %% Set the key transform for this cask
    KeyTransformFun = get_key_transform(get_opt(key_transform, Opts)),

    %% Loop and wait for the keydir to come available.
    ReadWriteP = WritingFile /= undefined,
    ReadWriteI = case ReadWriteP of true  -> 1;
                                    false -> 0
                 end,
    case init_keydir(Dirname, WaitTime, ReadWriteP, KeyTransformFun) of
        {ok, KeyDir, ReadFiles} ->
            %% Ensure that expiry_secs is in Opts and not just application env
            ExpOpts = [{expiry_secs,get_opt(expiry_secs,Opts)}|Opts],

            Ref = make_ref(),
            erlang:put(Ref, #bc_state {dirname = Dirname,
                                       read_files = ReadFiles,
                                       write_file = WritingFile, % <fd>|undefined|fresh
                                       write_lock = undefined,
                                       max_file_size = MaxFileSize,
                                       opts = ExpOpts,
                                       keydir = KeyDir, 
                                       key_transform = KeyTransformFun,
                                       read_write_p = ReadWriteI}),
            Ref;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Close a bitcask data store and flush any pending writes to disk.
-spec close(reference()) -> ok.
close(Ref) ->
    State = get_state(Ref),
    erlang:erase(Ref),

    %% Cleanup the write file and associated lock
    case State#bc_state.write_file of
        undefined ->
            ok;
        fresh ->
            ok;
        WriteFile ->
            _ = bitcask_fileops:close_for_writing(WriteFile),
            ok = bitcask_lockops:release(State#bc_state.write_lock)
    end,

    %% Manually release the keydir. If, for some reason, this failed GC would
    %% still get the job done.
    bitcask_nifs:keydir_release(State#bc_state.keydir),

    %% Clean up all the reading files
    bitcask_fileops:close_all(State#bc_state.read_files),

    ok.

%% @doc Close the currently active writing file; mostly for testing purposes
close_write_file(Ref) ->
    #bc_state { write_file = WriteFile} = State = get_state(Ref),
    case WriteFile of
        undefined ->
            ok;
        fresh ->
            ok;
        _ ->
            LastWriteFile = bitcask_fileops:close_for_writing(WriteFile),
            ok = bitcask_lockops:release(State#bc_state.write_lock),
            S2 = State#bc_state { write_file = fresh,
                                  read_files = [LastWriteFile | State#bc_state.read_files]},
            put_state(Ref, S2)
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
                true ->
                    %% Expired entry; remove from keydir and free up memory
                    ok = bitcask_nifs:keydir_remove(State#bc_state.keydir, Key),
                    not_found;
                false ->
                    %% HACK: Use a fully-qualified call to get_filestate/2 so that
                    %% we can intercept calls w/ Pulse tests.
                    case ?MODULE:get_filestate(E#bitcask_entry.file_id, State) of
                        {error, enoent} ->
                            %% merging deleted file between keydir_get and here
                            get(Ref, Key, TryNum-1);
                        {error, _} = Else ->
                            Else;
                        {Filestate, S2} ->
                            put_state(Ref, S2),
                            case bitcask_fileops:read(Filestate,
                                                    E#bitcask_entry.offset,
                                                    E#bitcask_entry.total_sz) of
                                {ok, _Key, Value} ->
                                    case is_tombstone(Value) of
                                        true ->
                                            not_found;
                                        false ->
                                            {ok, Value}
                                    end;
                                {error, eof} ->
                                    not_found;
                                {error, _} = Err ->
                                    Err
                            end
                    end
            end
    end.

%% @doc Store a key and value in a bitcase datastore.
put(Ref, Key, Value) ->
    #bc_state { write_file = WriteFile } = State = get_state(Ref),

    %% Make sure we have a file open to write
    case WriteFile of
        undefined ->
            throw({error, read_only});

        _ ->
            ok
    end,

    try
        {Ret, State1} = do_put(Key, Value, State,
                               ?DIABOLIC_BIG_INT, undefined),
        put_state(Ref, State1),
        Ret
    catch throw:{unrecoverable, Error, State2} ->
            put_state(Ref, State2),
            {error, Error}
    end.

%% @doc Delete a key from a bitcask datastore.
-spec delete(reference(), Key::binary()) -> ok.
delete(Ref, Key) ->
    put(Ref, Key, tombstone).

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
    State = get_state(Ref),
    MaxAge = get_opt(max_fold_age, State#bc_state.opts) * 1000, % convert from ms to us
    MaxPuts = get_opt(max_fold_puts, State#bc_state.opts),
    fold_keys(Ref, Fun, Acc0, MaxAge, MaxPuts, false).

%% @doc Fold over all keys in a bitcask datastore with limits on how out of date
%%      the keydir is allowed to be.
%% Must be able to understand the bitcask_entry record form.
-spec fold_keys(reference(), Fun::fun(), Acc::term(), non_neg_integer() | undefined,
                non_neg_integer() | undefined, boolean()) ->
                                                term() | {error, any()}.
fold_keys(Ref, Fun, Acc0, MaxAge, MaxPut, SeeTombstonesP) ->
    %% Fun should be of the form F(#bitcask_entry, A) -> A
    ExpiryTime = expiry_time((get_state(Ref))#bc_state.opts),
    RealFun = fun(BCEntry, Acc) ->
        Key = BCEntry#bitcask_entry.key,
        case BCEntry#bitcask_entry.tstamp < ExpiryTime of
            true ->
                Acc;
            false ->
                TSize = ?TOMBSTONE_SIZE,
                case BCEntry#bitcask_entry.total_sz -
                            (?HEADER_SIZE + size(Key)) of
                    TSize ->  % might be a deleted record, so check
                        case ?MODULE:get(Ref, Key) of
                            not_found when not SeeTombstonesP ->
                                Acc;
                            not_found when SeeTombstonesP ->
                                Fun({tombstone, BCEntry}, Acc);
                            _ -> Fun(BCEntry, Acc)
                        end;
                    _ ->
                        Fun(BCEntry, Acc)
                end
        end
    end,
    bitcask_nifs:keydir_fold((get_state(Ref))#bc_state.keydir, RealFun, Acc0, MaxAge, MaxPut).

%% @doc fold over all K/V pairs in a bitcask datastore.
%% Fun is expected to take F(K,V,Acc0) -> Acc
-spec fold(reference() | tuple(),
           fun((binary(), binary(), any()) -> any()), 
           any()) -> any() | {error, any()}.
fold(Ref, Fun, Acc0) when is_reference(Ref)->
    State = get_state(Ref),
    fold(State, Fun, Acc0);
fold(State, Fun, Acc0) ->
    MaxAge = get_opt(max_fold_age, State#bc_state.opts) * 1000, % convert from ms to us
    MaxPuts = get_opt(max_fold_puts, State#bc_state.opts),
    SeeTombstonesP = get_opt(fold_tombstones, State#bc_state.opts) /= undefined,
    fold(State, Fun, Acc0, MaxAge, MaxPuts, SeeTombstonesP).

%% @doc fold over all K/V pairs in a bitcask datastore specifying max age/updates of
%% the frozen keystore.
%% Fun is expected to take F(K,V,Acc0) -> Acc
-spec fold(reference() | tuple(), fun((binary(), binary(), any()) -> any()), any(),
           non_neg_integer() | undefined, non_neg_integer() | undefined, boolean()) -> 
                  any() | {error, any()}.
fold(Ref, Fun, Acc0, MaxAge, MaxPut, SeeTombstonesP) when is_reference(Ref)->
    State = get_state(Ref),
    fold(State, Fun, Acc0, MaxAge, MaxPut, SeeTombstonesP);
fold(State, Fun, Acc0, MaxAge, MaxPut, SeeTombstonesP) ->
    KT = State#bc_state.key_transform,
    FrozenFun = 
        fun() ->
                CurrentEpoch = bitcask_nifs:keydir_get_epoch(State#bc_state.keydir),
                PendingEpoch = pending_epoch(State#bc_state.keydir),
                FoldEpoch = min(CurrentEpoch, PendingEpoch),
                case open_fold_files(State#bc_state.dirname, ?OPEN_FOLD_RETRIES) of
                    {ok, Files} ->
                        ExpiryTime = expiry_time(State#bc_state.opts),
                        SubFun = fun(K0,V,TStamp,{_FN,FTS,Offset,_Sz},Acc) ->
                                         K = KT(K0),
                                         case (TStamp < ExpiryTime) of
                                             true ->
                                                 Acc;
                                             false ->
                                                 case bitcask_nifs:keydir_get(
                                                        State#bc_state.keydir, K,
                                                        FoldEpoch) of
                                                     not_found ->
                                                         Acc;
                                                     E when is_record(E, bitcask_entry) ->
                                                         case
                                                             Offset =:= E#bitcask_entry.offset
                                                             andalso
                                                             TStamp =:= E#bitcask_entry.tstamp
                                                             andalso
                                                             FTS =:= E#bitcask_entry.file_id of
                                                             false ->
                                                                 Acc;
                                                             true when SeeTombstonesP ->
                                                                 Fun({tombstone, K},V,Acc);
                                                             true when not SeeTombstonesP ->
                                                                 case is_tombstone(V) of
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
                end
        end,
    KeyDir = State#bc_state.keydir,
    bitcask_nifs:keydir_frozen(KeyDir, FrozenFun, MaxAge, MaxPut).

pending_epoch(Keydir) ->
    {_, _, _, {_, _, _, Epoch}} = bitcask_nifs:keydir_info(Keydir),
    Epoch.

%%
%% Get a list of readable files and attempt to open them for a fold. If we can't
%% open any one of the files, get a fresh list of files and try again.
%%
open_fold_files(_Dirname, 0) ->
    {error, max_retries_exceeded_for_fold};
open_fold_files(Dirname, Count) ->
    try
        Filenames = list_data_files(Dirname, undefined, undefined),
        case open_files(Filenames, []) of
            {ok, Files} ->
                {ok, Files};
            error ->
                open_fold_files(Dirname, Count-1)
        end
    catch X:Y ->
            {error, {X,Y}}
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
            bitcask_fileops:close_all(Acc),
            error
    end.

%%
%% Apply fold function to a single bitcask file; results are accumulated in
%% Acc
%%
subfold(_SubFun,[],Acc) ->
    Acc;
subfold(SubFun,[FD | Rest],Acc0) ->
    Acc2 = try bitcask_fileops:fold(FD, SubFun, Acc0) of
               Acc1 ->
                   Acc1
           catch
               throw:{fold_error, Error, _PartialAcc} ->
                   error_logger:error_msg("subfold: skipping file ~s: ~p\n",
                                          [FD#filestate.filename, Error]),
                   Acc0
           after
               bitcask_fileops:close(FD)
           end,
    subfold(SubFun, Rest, Acc2).

%% @doc Start entry iterator
-spec iterator(reference(), integer(), integer()) ->
      ok | out_of_date | {error, iteration_in_process}.
iterator(Ref, MaxAge, MaxPuts) ->
    KeyDir = (get_state(Ref))#bc_state.keydir,
    bitcask_nifs:keydir_itr(KeyDir, MaxAge, MaxPuts).

%% @doc Get next entry from the iterator
-spec iterator_next(reference()) ->
      #bitcask_entry{} |
     {error, iteration_not_started} | allocation_error | not_found.
iterator_next(Ref) ->
    KeyDir = (get_state(Ref))#bc_state.keydir,
    bitcask_nifs:keydir_itr_next(KeyDir).

%% @doc Release iterator
-spec iterator_release(reference()) -> ok.
iterator_release(Ref) ->
    KeyDir = (get_state(Ref))#bc_state.keydir,
    bitcask_nifs:keydir_itr_release(KeyDir).

%% @doc Merge several data files within a bitcask datastore
%%      into a more compact form.
-spec merge(Dirname::string()) -> ok | {error, any()}.
merge(Dirname) ->
    try
        merge(Dirname, [], {readable_files(Dirname), []})
    catch
        Error:Reason ->
            {error, {Error, Reason}}
    end.

%% @doc Merge several data files within a bitcask datastore
%%      into a more compact form.
-spec merge(Dirname::string(), Opts::[_]) -> ok | {error, any()}.
merge(Dirname, Opts) ->
    try
        merge(Dirname, Opts, {readable_files(Dirname), []})
    catch
        Error:Reason ->
            {error, {Error, Reason}}
    end.

%% @doc Merge several data files within a bitcask datastore
%%      into a more compact form.
-spec merge(Dirname::string(), Opts::[_], 
            {FilesToMerge::[string()],FilesToDelete::[string()]})
           -> ok | {error, any()}.
merge(_Dirname, _Opts, []) ->
    ok;
merge(Dirname,Opts,FilesToMerge) when is_list(FilesToMerge) -> 
    merge(Dirname,Opts,{FilesToMerge,[]});
merge(_Dirname, _Opts, {[],_}) ->
    ok;
merge(Dirname, Opts, {FilesToMerge0, ExpiredFiles0}) ->
    try
        %% Make sure bitcask app is started so we can pull defaults from env
        ok = start_app(),
        %% Filter the files to merge and ensure that they all exist. It's
        %% possible in some circumstances that we'll get an out-of-date
        %% list of files.
        FilesToMerge = [F || F <- FilesToMerge0,
                             bitcask_fileops:is_file(F)],
        ExpiredFiles = [F || F <- ExpiredFiles0,
                             bitcask_fileops:is_file(F)],
        merge1(Dirname, Opts, FilesToMerge, ExpiredFiles)
    catch
        throw:Reason ->
            Reason;
        X:Y ->
            {error, {generic_failure, X, Y}}
    end.

%% Inner merge function, assumes that bitcask is running and all files exist.
merge1(_Dirname, _Opts, [], []) ->
    ok;
merge1(Dirname, Opts, FilesToMerge, ExpiredFiles) ->
    KT = get_key_transform(get_opt(key_transform, Opts)),

    %% Try to lock for merging
    case bitcask_lockops:acquire(merge, Dirname) of
        {ok, Lock} ->
            ok;
        {error, Reason} ->
            Lock = undefined,
            throw({error, {merge_locked, Reason, Dirname}})
    end,

    %% Get the live keydir
    case bitcask_nifs:maybe_keydir_new(Dirname) of
        {ready, LiveKeyDir} ->
            %% Simplest case; a key dir is already available and
            %% loaded. Go ahead and open just the files we wish to
            %% merge
            InFiles0 = [begin 
                            %% Handle open errors gracefully.  QuickCheck
                            %% plus PULSE showed that there are races where
                            %% the open below can fail.
                            case bitcask_fileops:open_file(F) of
                                {ok, Fstate}    -> Fstate;
                                {error, _}      -> skip
                            end
                        end
                        || F <- FilesToMerge],
            InFiles1 = [F || F <- InFiles0, F /= skip];
        {error, not_ready} ->
            %% Someone else is loading the keydir, or this cask isn't open. 
            %% We'll bail here and try again later.

            ok = bitcask_lockops:release(Lock),
            % Make erlc happy w/ non-local exit
            LiveKeyDir = undefined, InFiles1 = [],
            throw({error, not_ready})
    end,

    LiveRef = make_ref(),
    put_state(LiveRef, #bc_state{dirname = Dirname, keydir = LiveKeyDir}),
    erlang:erase(LiveRef),
    {InFiles2,InExpiredFiles} = lists:foldl(fun(F, {InFilesAcc,InExpiredAcc}) ->
                                            case lists:member(F#filestate.filename,
                                                    ExpiredFiles) of
                                                false ->
                                                    {[F|InFilesAcc],InExpiredAcc};
                                                true ->
                                                    {InFilesAcc,[F|InExpiredAcc]}
                                            end
                            end, {[],[]}, InFiles1),

    %% Test to see if this is a complete or partial merge
    %% We perform this test now because our efforts to open the input files
    %% in the InFiles0 list comprehension above may have had an open
    %% failure.  The open(2) shouldn't fail, except, of course, when it
    %% does, e.g. EMFILE, ENFILE, the OS decides EINTR because "reasons", ...
    Partial = lists:usort(readable_files(Dirname))
              /=
              lists:usort([F#filestate.filename || F <- InFiles2]),

    % This sort is very important. The merge expects to visit files in order
    % to properly detect current values, and could resurrect old values if not.
    InFiles = lists:sort(fun(#filestate{tstamp=FTL}, #filestate{tstamp=FTR}) ->
                                 FTL =< FTR
                         end, InFiles2),

    %% Initialize the other keydirs we need.
    {ok, DelKeyDir} = bitcask_nifs:keydir_new(),

    %% Initialize our state for the merge
    State = #mstate { dirname = Dirname,
                      merge_lock = Lock,
                      max_file_size = get_opt(max_file_size, Opts),
                      input_files = InFiles,
                      out_file = fresh,  % will be created when needed
                      partial = Partial,
                      live_keydir = LiveKeyDir,
                      del_keydir = DelKeyDir,
                      expiry_time = expiry_time(Opts),
                      expiry_grace_time = expiry_grace_time(Opts),
                      key_transform = KT,
                      read_write_p = 0,
                      opts = Opts,
                      delete_files = []},

    %% Finally, start the merge process
    ExpiredFilesFinished = expiry_merge(InExpiredFiles, LiveKeyDir, KT, []),
    State1 = merge_files(State),

    %% Make sure to close the final output file
    case State1#mstate.out_file of
        fresh ->
            ok;
        Outfile ->
            ok = bitcask_fileops:sync(Outfile),
            ok = bitcask_fileops:close(Outfile)
    end,

    %% Close the original input files, schedule them for deletion,
    %% close keydirs, and release our lock
    bitcask_fileops:close_all(State#mstate.input_files ++ ExpiredFilesFinished),
    {_, _, _, {IterGeneration, _, _, _}} = bitcask_nifs:keydir_info(LiveKeyDir),
    FileNames = [F#filestate.filename || F <- State1#mstate.delete_files ++ ExpiredFilesFinished],
    _ = [catch set_setuid_bit(F) || F <- FileNames],
    bitcask_merge_delete:defer_delete(Dirname, IterGeneration, FileNames),

    %% Explicitly release our keydirs instead of waiting for GC
    bitcask_nifs:keydir_release(LiveKeyDir),
    bitcask_nifs:keydir_release(DelKeyDir),    

    ok = bitcask_lockops:release(Lock).

%% @doc Predicate which determines whether or not a file should be considered for a merge. 
consider_for_merge(FragTrigger, DeadBytesTrigger, ExpirationGraceTime) ->
    fun (F) ->
            (F#file_status.fragmented >= FragTrigger)
                orelse (F#file_status.dead_bytes >= DeadBytesTrigger)
                orelse ((F#file_status.oldest_tstamp > 0) andalso   %% means that the file has data
                        (F#file_status.newest_tstamp < ExpirationGraceTime)
                       )
    end.

-spec needs_merge(reference()) -> {true, {[string()], [string()]}} | false.
needs_merge(Ref) ->
    State = get_state(Ref),
    {_KeyCount, Summary} = summary_info(Ref),

    %% Review all the files we currently have open in read_files and
    %% see if any no longer exist by name (i.e. have been deleted by
    %% previous merges). Close these files so that we don't leak
    %% file descriptors.
    P = fun(F) ->
                bitcask_fileops:is_file(bitcask_fileops:filename(F))
        end,
    {LiveFiles, DeadFiles} = lists:partition(P, State#bc_state.read_files),

    %% Close the dead files and accumulate a list for trimming their 
    %% fstats entries.
    DeadIds0 = 
        [begin
             bitcask_fileops:close(F),
             bitcask_fileops:file_tstamp(F)
         end
         || F <- DeadFiles],
    DeadIds = lists:usort(DeadIds0),

    case bitcask_nifs:keydir_trim_fstats(State#bc_state.keydir, 
                                         DeadIds) of
        {ok, 0} ->
            ok;
        {ok, Warn} ->
            error_logger:info_msg("Trimmed ~p non-existent fstat entries",
                                  [Warn]);
        Err ->
            error_logger:error_msg("Error trimming fstats entries: ~p",
                                   [Err])
    end,

    %% Update state with live files
    put_state(Ref, State#bc_state { read_files = LiveFiles }),

    %% Triggers that would require a merge:
    %%
    %% frag_merge_trigger - Any file exceeds this % fragmentation
    %% dead_bytes_merge_trigger - Any file has more than this # of dead bytes
    %% expiry_time - Any file has an expired key
    %% expiry_grace_time - avoid expiring in the case of continuous writes
    %%
    FragTrigger = get_opt(frag_merge_trigger, State#bc_state.opts),
    DeadBytesTrigger = get_opt(dead_bytes_merge_trigger, State#bc_state.opts),
    ExpirationTime = 
            max(expiry_time(State#bc_state.opts), 0),
    ExpirationGraceTime = 
            max(expiry_time(State#bc_state.opts) - expiry_grace_time(State#bc_state.opts), 0),

    NeedsMerge = lists:any(consider_for_merge(FragTrigger, DeadBytesTrigger, ExpirationGraceTime), 
                           Summary),
    case NeedsMerge of
        true ->
            %% Build a list of threshold checks; a file which meets ANY
            %% of these will be merged
            %%
            %% frag_threshold - At least this % fragmented
            %% dead_bytes_threshold - At least this # of dead bytes
            %% small_file_threshold - Any file < this # of bytes
            %% expiry_secs - Any file has a expired key
            %%
            Thresholds = [frag_threshold(State#bc_state.opts),
                          dead_bytes_threshold(State#bc_state.opts),
                          small_file_threshold(State#bc_state.opts),
                          expired_threshold(ExpirationTime)],

            %% For each file, apply the threshold checks and return a list
            %% of failed threshold checks
            CheckFile = fun(F) ->
                                {F#file_status.filename, lists:flatten([T(F) || T <- Thresholds])}
                        end,
            MergableFiles = [{N, R} || {N, R} <- [CheckFile(F) || F <- Summary],
                                       R /= []],

            %% Log the reasons for needing a merge, if so configured
            %% TODO: At some point we may want to change this API to let the caller
            %%       recv this information and decide if they want it
            case get_opt(log_needs_merge, State#bc_state.opts) of
                true ->
                    error_logger:info_msg("~p needs_merge: ~p\n",
                                          [State#bc_state.dirname, MergableFiles]);
                _ ->
                    ok
            end,
            FileNames = [Filename || {Filename, _Reasons} <- MergableFiles],
            F = fun(X) ->
                    case X of
                        {data_expired,_,_} ->
                            true;
                        _ ->
                            false
                    end
            end,
            ExpiredFiles = [Filename || {Filename, Reasons} <- MergableFiles,  lists:any(F,Reasons)],
            {true, {FileNames, ExpiredFiles}};
        false ->
            false
    end.


frag_threshold(Opts) ->
    FragThreshold = get_opt(frag_threshold, Opts),
    fun(F) ->
            if F#file_status.fragmented >= FragThreshold ->
                    [{fragmented, F#file_status.fragmented}];
               true ->
                    []
            end
    end.

dead_bytes_threshold(Opts) ->
    DeadBytesThreshold = get_opt(dead_bytes_threshold, Opts),
    fun(F) ->
            if F#file_status.dead_bytes >= DeadBytesThreshold ->
                    [{dead_bytes, F#file_status.dead_bytes}];
               true ->
                    []
            end
    end.

small_file_threshold(Opts) ->
    %% We need to do a special check on small_file_threshold for non-integer
    %% values since it is using a less-than check. Other thresholds typically
    %% do a greater-than check and can take advantage of fact that integers
    %% are always greater than an atom.
    case get_opt(small_file_threshold, Opts) of
        Threshold when is_integer(Threshold) ->
            fun(F) ->
                    if F#file_status.total_bytes < Threshold ->
                            [{small_file, F#file_status.total_bytes}];
                       true ->
                            []
                    end
            end;
        disabled ->
            fun(_F) -> [] end
    end.

expired_threshold(Cutoff) ->
    fun(F) ->
            if F#file_status.newest_tstamp < Cutoff  ->
                    [{data_expired, F#file_status.newest_tstamp, Cutoff}];
               true ->
                    []
            end
    end.

-spec is_empty_estimate(reference()) -> boolean().
is_empty_estimate(Ref) ->
    State = get_state(Ref),
    {KeyCount, _, _, _} = bitcask_nifs:keydir_info(State#bc_state.keydir),
    KeyCount == 0.

-spec status(reference()) -> {integer(), [{string(), integer(), integer(), integer()}]}.
status(Ref) ->
    %% Rewrite the new, record-style status from status_info into a backwards-compatible
    %% call.
    %% TODO: Next major revision should remove this variation on status
    {KeyCount, Summary} = summary_info(Ref),
    {KeyCount, [{F#file_status.filename, F#file_status.fragmented,
                 F#file_status.dead_bytes, F#file_status.total_bytes} || F <- Summary]}.


-spec summary_info(reference()) -> {integer(), [#file_status{}]}.
summary_info(Ref) ->
    State = get_state(Ref),

    %% Pull current info for the bitcask. In particular, we want
    %% the file stats so we can determine how much fragmentation
    %% is present
    %%
    %% Fstat has form: [{FileId, LiveCount, TotalCount, LiveBytes, TotalBytes, OldestTstamp, NewestTstamp}]
    %% and is only an estimate/snapshot.
    {KeyCount, _KeyBytes, Fstats, _IterStatus} =
        bitcask_nifs:keydir_info(State#bc_state.keydir),

    %% We want to ignore the file currently being written when
    %% considering status!
    case bitcask_lockops:read_activefile(write, State#bc_state.dirname) of
        undefined ->
            WritingFileId = undefined;
        Filename ->
            WritingFileId = bitcask_fileops:file_tstamp(Filename)
    end,

    %% Convert fstats list into a list of #file_status
    %%
    %% Note that we also, filter the WritingFileId from any further
    %% consideration.
    Summary0 = [summarize(State#bc_state.dirname, S) ||
                   S <- Fstats, element(1, S) /= WritingFileId],

    %% Remove any files that don't exist from the initial summary
    Summary = lists:keysort(1, [S || S <- Summary0,
                                     bitcask_fileops:is_file(element(2, S))]),
    {KeyCount, Summary}.


%% ===================================================================
%% Internal functions
%% ===================================================================

summarize(Dirname, {FileId, LiveCount, TotalCount, LiveBytes, TotalBytes, OldestTstamp, NewestTstamp}) ->
    #file_status { filename = bitcask_fileops:mk_filename(Dirname, FileId),
                   fragmented = trunc((1 - LiveCount/TotalCount) * 100),
                   dead_bytes = TotalBytes - LiveBytes,
                   total_bytes = TotalBytes,
                   oldest_tstamp = OldestTstamp,
                   newest_tstamp = NewestTstamp}.

expiry_time(Opts) ->
    ExpirySecs = get_opt(expiry_secs, Opts),
    case ExpirySecs > 0 of
        true -> bitcask_time:tstamp() - ExpirySecs;
        false -> 0
    end.

to_lower_grace_time_bound(undefined) -> 0;
to_lower_grace_time_bound(X) -> 
    case X > 0 of
        true -> X;
        false -> 0
    end.

expiry_grace_time(Opts) -> to_lower_grace_time_bound(get_opt(expiry_grace_time, Opts)).

is_tombstone(?TOMBSTONE) ->
    true;
is_tombstone(_) ->
    false.

-ifdef(TEST).
start_app() ->
    catch application:start(?MODULE),
    ok.
-else.
start_app() ->
    case application:start(?MODULE) of
        ok ->
            ok;
        {error, {already_started, ?MODULE}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
-endif.

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

kt_id(Key) ->
    Key.

scan_key_files([], _KeyDir, Acc, _CloseFile, _KT) ->
    Acc;
scan_key_files([Filename | Rest], KeyDir, Acc, CloseFile, KT) ->
    %% Restrictive pattern matching below is intentional
    case bitcask_fileops:open_file(Filename) of
        {ok, File} ->
            FileTstamp = bitcask_fileops:file_tstamp(File),
            %% Signal to the keydir that this file exists via
            %% increment_file_id() with optional 2nd arg.  The NIF
            %% needs to know the file exists, even if it contains only
            %% tombstones or data errors.  Otherwise we risk of
            %% reusing the file id for new data.
            _ = bitcask_nifs:increment_file_id(KeyDir, FileTstamp),
            F = fun({tombstone, K}, _Tstamp, {_Offset, _TotalSz}, _) ->
                        _ = bitcask_nifs:keydir_remove(KeyDir, KT(K));
                   (K, Tstamp, {Offset, TotalSz}, _) ->
                        bitcask_nifs:keydir_put(KeyDir,
                                                KT(K),
                                                FileTstamp,
                                                TotalSz,
                                                Offset,
                                                Tstamp,
                                                bitcask_time:tstamp(),
                                                false)
                end,
            bitcask_fileops:fold_keys(File, F, undefined, recovery),
            if CloseFile == true ->
                    bitcask_fileops:close(File);
               true ->
                    ok
            end,
            scan_key_files(Rest, KeyDir, [File | Acc], CloseFile, KT)
    end.

%%
%% Initialize a keydir for a given directory.
%%
init_keydir(Dirname, WaitTime, ReadWriteModeP, KT) ->
    %% Get the named keydir for this directory. If we get it and it's already
    %% marked as ready, that indicates another caller has already loaded
    %% all the data from disk and we can short-circuit scanning all the files.
    case bitcask_nifs:keydir_new(Dirname) of
        {ready, KeyDir} ->
            %% A keydir already exists, nothing more to do here. We'll lazy
            %% open files as needed.
            {ok, KeyDir, []};

        {not_ready, KeyDir} ->
            %% We've just created a new named keydir, so we need to load up all
            %% the data from disk. Build a list of all the bitcask data files
            %% and sort it in descending order (newest->oldest).
            %%
            %% We need the SortedFiles list to be stable: we might be
            %% in a situation:
            %% 1. Someone else starts a merge on this cask.
            %% 2. Our caller opens the cask and gets to here.
            %% 3. The merge races with readable_files(): creating
            %%    new data files and deleting old ones.
            %% 4. SortedFiles doesn't contain the list of all of the
            %%    files that we need.
            Lock = poll_for_merge_lock(Dirname),
            ScanResult =
            try
                if ReadWriteModeP ->
                        %% This purge will acquire the write lock
                        %% prior to doing anything.
                        purge_setuid_files(Dirname);
                   true ->
                        ok
                end,
                init_keydir_scan_key_files(Dirname, KeyDir, KT)
            catch
                _:Detail ->
                    {error, {purge_setuid_or_init_scan, Detail}}
            after
                _ = case Lock of
                        ?POLL_FOR_MERGE_LOCK_PSEUDOFAILURE ->
                            ok;
                        {error, _} = Error ->
                            Error;
                        _ ->
                            ok = bitcask_lockops:release(Lock)
                    end
            end,

            case ScanResult of
                {error, _} ->
                    ScanResult;
                _ ->
                    %% Now that we loaded all the data, mark the keydir as ready
                    %% so other callers can use it
                    ok = bitcask_nifs:keydir_mark_ready(KeyDir),
                    {ok, KeyDir, []}
            end;

        {error, not_ready} ->
            timer:sleep(100),
            case WaitTime of
                Value when is_integer(Value), Value =< 0 -> %% avoids 'infinity'!
                    {error, timeout};
                _ ->
                    init_keydir(Dirname, WaitTime - 100, ReadWriteModeP, KT)
            end
    end.

init_keydir_scan_key_files(Dirname, KeyDir, KT) ->
    init_keydir_scan_key_files(Dirname, KeyDir, KT, ?DIABOLIC_BIG_INT).

init_keydir_scan_key_files(_Dirname, _Keydir, _KT, 0) ->
    %% If someone launches enough parallel merge operations to
    %% interfere with our attempts to scan this keydir for this many
    %% times, then we are just plain unlucky.  Or QuickCheck smites us
    %% from lofty Mt. Stochastic.
    {error, {init_keydir_scan_key_files, too_many_iterations}};
init_keydir_scan_key_files(Dirname, KeyDir, KT, Count) ->
    try
        {SortedFiles, SetuidFiles} = readable_and_setuid_files(Dirname),
        _ = scan_key_files(SortedFiles, KeyDir, [], true, KT),
        %% There may be a setuid data file that has a larger tstamp name than
        %% any non-setuid data file.  Tell the keydir about it, so that we
        %% don't try to reuse that tstamp name.
        case SetuidFiles of
            [] ->
                ok;
            _ ->
                MaxSetuid = lists:max([bitcask_fileops:file_tstamp(F) ||
                                          F <- SetuidFiles]),
                bitcask_nifs:increment_file_id(KeyDir, MaxSetuid)
        end
    catch _X:_Y ->
            error_msg_perhaps("scan_key_files: ~p ~p @ ~p\n",
                              [_X, _Y, erlang:get_stacktrace()]),
            init_keydir_scan_key_files(Dirname, KeyDir, KT, Count - 1)
    end.

get_filestate(FileId,
              State=#bc_state{ dirname = Dirname, read_files = ReadFiles }) ->
    case lists:keysearch(FileId, #filestate.tstamp, ReadFiles) of
        {value, Filestate} ->
            {Filestate, State};
        false ->
            Fname = bitcask_fileops:mk_filename(Dirname, FileId),
            case bitcask_fileops:open_file(Fname) of
                {error,enoent} ->
                    %% merge removed the file since the keydir_get
                    {error, enoent};
                {ok, Filestate} ->
                    {Filestate, State#bc_state{read_files =
                                      [Filestate | State#bc_state.read_files]}};
                {error, _} = Else ->
                    Else
            end
    end.


list_data_files(Dirname, WritingFile, MergingFile) ->
    %% Get list of {tstamp, filename} for all files in the directory then
    %% reverse sort that list and extract the fully-qualified filename.
    Files1 = bitcask_fileops:data_file_tstamps(Dirname),
    Files2 = bitcask_fileops:data_file_tstamps(Dirname),
    if Files1 == Files2 ->
            %% No race, Files1 is a stable list.
            [F || {_Tstamp, F} <- lists:sort(Files1),
                  F /= WritingFile,
                  F /= MergingFile];
       true ->
            list_data_files(Dirname, WritingFile, MergingFile)
    end.

merge_files(#mstate { input_files = [] } = State) ->
    State;
merge_files(#mstate {  dirname = Dirname,
                       input_files = [File | Rest],
                       key_transform = KT
                    } = State) ->
    FileId = bitcask_fileops:file_tstamp(File),
    F = fun(K, V, Tstamp, Pos, State0) ->
                merge_single_entry(KT(K), V, Tstamp, FileId, Pos, State0)
        end,
    State2 = try bitcask_fileops:fold(File, F, State) of
                 #mstate{delete_files = DelFiles} = State1 ->
                     State1#mstate{delete_files = [File|DelFiles]}
             catch
                 throw:{fold_error, Error, _PartialAcc} ->
                     error_logger:error_msg(
                       "merge_files: skipping file ~s in ~s: ~p\n",
                       [File#filestate.filename, Dirname, Error]),
                     State
             end,
    merge_files(State2#mstate { input_files = Rest }).

merge_single_entry(K, V, Tstamp, FileId, {_, _, Offset, _} = Pos, State) ->
    case out_of_date(State, K, Tstamp, FileId, Pos, State#mstate.expiry_time,
                     false,
                     [State#mstate.live_keydir, State#mstate.del_keydir]) of
        true ->
            % Value in keydir is newer, so drop
            State;
        expired ->
            %% Note: we drop a tombstone if it expired. Under normal
            %% circumstances it's OK as any value older than that has expired
            %% too and you wouldn't see values coming back to life after a
            %% merge and reopen.  However with a clock going back in time,
            %% and badly timed quick merges you could end up seeing an old
            %% value after we drop a tombstone that has a lower timestamp
            %% than a value that was actually written before. Likely that other
            %% value would expire soon too, but...

            %% Remove only if this is the current entry in the keydir
            bitcask_nifs:keydir_remove(State#mstate.live_keydir, K,
                                       Tstamp, FileId, Offset),
            State;
        not_found ->
            % Not in keydir. If a tombstone, first we see for this key.
            case is_tombstone(V) of
                true ->
                    % Not in keydir and not already deleted.
                    % Remember we deleted this already during this merge.
                    ok = bitcask_nifs:keydir_put(State#mstate.del_keydir, K,
                                                 FileId, 0, Offset, Tstamp,
                                                 bitcask_time:tstamp()),
                    case State#mstate.partial of
                        true ->
                            % Merging only some files, forward tombstone
                            inner_merge_write(K, V, Tstamp, FileId, Offset,
                                              State);
                        false ->
                            % Full merge, so safe to drop tombstone
                            State
                    end;
                false ->
                    % Regular value not in keydir, ignore
                    State
            end;
        false ->
            % Either a current value or a tombstone with nothing in the keydir
            % but an entry in the del keydir because we've seen another during
            % this merge.
            case is_tombstone(V) of
                true ->
                    %% We have seen a tombstone for this key before, but this
                    %% one is newer than that one.
                    ok = bitcask_nifs:keydir_put(State#mstate.del_keydir, K,
                                                 FileId, 0, Offset, Tstamp,
                                                 bitcask_time:tstamp()),

                    %% Use the conditional remove on the live
                    %% keydir. We only want to actually remove
                    %% whatever is in the live keydir IFF the
                    %% tstamp/fileid we have matches the current
                    %% entry.
                    case State#mstate.partial of
                        true ->
                            inner_merge_write(K, V, Tstamp, FileId, Offset,
                                              State);
                        false ->
                            State
                    end;
                false ->
                    ok = bitcask_nifs:keydir_remove(State#mstate.del_keydir, K),
                    inner_merge_write(K, V, Tstamp, FileId, Offset, State)
            end
    end.

-spec inner_merge_write(binary(), binary(), integer(), integer(), integer(),
                        #mstate{}) -> #mstate{}.

inner_merge_write(K, V, Tstamp, OldFileId, OldOffset, State) ->
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
                                  State#mstate.opts,
                                  State#mstate.live_keydir),
                NewFileName = bitcask_fileops:filename(NewFile),
                ok = bitcask_lockops:write_activefile(
                       State#mstate.merge_lock,
                       NewFileName),
                State#mstate { out_file = NewFile };
            ok ->
                State;
            fresh ->
                %% create the output file and take the lock.
                {ok, NewFile} = bitcask_fileops:create_file(
                                  State#mstate.dirname,
                                  State#mstate.opts,
                                  State#mstate.live_keydir),
                NewFileName = bitcask_fileops:filename(NewFile),
                ok = bitcask_lockops:write_activefile(
                       State#mstate.merge_lock,
                       NewFileName),
                State#mstate { out_file = NewFile }                
        end,
    
    {ok, Outfile, Offset, Size} =
        bitcask_fileops:write(State1#mstate.out_file, K, V, Tstamp),

    OutFileId = bitcask_fileops:file_tstamp(Outfile),
    case OutFileId =< OldFileId of
        true ->
            exit({invariant_violation, K, V, OldFileId, OldOffset, "->",
                  OutFileId, Offset});
        false ->
            ok
    end,
    Outfile2 =
        case is_tombstone(V) of
            false ->
                %% Update live keydir for the current out
                %% file. It's possible that someone else may have written
                %% a newer value whilst we were processing ... and if
                %% they did, we need to undo our write here.
                case bitcask_nifs:keydir_put(State#mstate.live_keydir, K,
                                             OutFileId,
                                             Size, Offset, Tstamp,
                                             bitcask_time:tstamp(),
                                             OldFileId, OldOffset) of
                    ok ->
                        Outfile;
                    already_exists ->
                        {ok, O} = bitcask_fileops:un_write(Outfile),
                        O
                end;
           true ->
                case bitcask_nifs:keydir_get(State#mstate.live_keydir, K) of
                    not_found ->
                        % Still not there, tombstone write is cool
                        Outfile;
                    #bitcask_entry{} ->
                        % New value written, undo
                        {ok, O} = bitcask_fileops:un_write(Outfile),
                        O
                end
        end,
    State1#mstate { out_file = Outfile2 }.


out_of_date(_State, _Key, _Tstamp, _FileId, _Pos, _ExpiryTime,
            EverFound, []) ->
    %% if we ever found it, and none of the entries were out of date,
    %% then it's not out of date
    case EverFound of
        true -> false;
        false -> not_found
    end;
out_of_date(_State, _Key, Tstamp, _FileId, _Pos, ExpiryTime,
            _EverFound, _KeyDirs)
  when Tstamp < ExpiryTime ->
    expired;
out_of_date(State, Key, Tstamp, FileId, {_,_,Offset,_} = Pos,
            ExpiryTime, EverFound, [KeyDir|Rest]) ->
    case bitcask_nifs:keydir_get(KeyDir, Key) of
        not_found ->
            out_of_date(State, Key, Tstamp, FileId, Pos, ExpiryTime,
                        EverFound, Rest);

        E when is_record(E, bitcask_entry) ->
            if
                E#bitcask_entry.tstamp == Tstamp ->
                    %% Exact match. In this situation, we use the file
                    %% id and offset as a tie breaker.
                    %% The assumption is that newer values are written to
                    %% higher file ids and offsets, even in the case of a merge
                    %% racing with the write process, as the writer process
                    %% will detect that and retry the write to an even higher
                    %% file id.
                    if
                        E#bitcask_entry.file_id > FileId ->
                            true;

                        E#bitcask_entry.file_id == FileId ->
                            case E#bitcask_entry.offset > Offset of
                                true ->
                                    true;
                                false ->
                                    out_of_date(
                                      State, Key, Tstamp, FileId, Pos,
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
                            out_of_date(State, Key, Tstamp, FileId, Pos,
                                        ExpiryTime, true, Rest)
                    end;

                E#bitcask_entry.tstamp < Tstamp ->
                    %% Not out of date -- check rest of the keydirs
                    out_of_date(State, Key, Tstamp, FileId, Pos,
                                ExpiryTime, true, Rest);

                true ->
                    %% Out of date!
                    true
            end
    end.

-spec readable_files(string()) -> [string()].  
readable_files(Dirname) ->
    {ReadableFiles, _SetuidFiles} = readable_and_setuid_files(Dirname),
    ReadableFiles.

readable_and_setuid_files(Dirname) ->
    %% Check the write and/or merge locks to see what files are currently
    %% being written to. Generate our list excepting those.
    WritingFile = bitcask_lockops:read_activefile(write, Dirname),
    MergingFile = bitcask_lockops:read_activefile(merge, Dirname),

    %% Filter out files with setuid bit set: they've been marked for
    %% deletion by an earlier *successful* merge.
    Fs = [F || F <- list_data_files(Dirname, WritingFile, MergingFile)],
    lists:partition(fun(F) -> not has_setuid_bit(F) end, Fs).

%% Internal put - have validated that the file is opened for write
%% and looked up the state at this point
do_put(_Key, _Value, State, 0, LastErr) ->
    {{error, LastErr}, State};
do_put(Key, Value, #bc_state{write_file = WriteFile} = State, 
       Retries, _LastErr) ->
    State2 =
        case bitcask_fileops:check_write(WriteFile, Key, Value,
                                         State#bc_state.max_file_size) of
            wrap ->
                %% Time to start a new write file. Note that we do not
                %% close the old one, just transition it. The thinking
                %% is that closing/reopening for read only access
                %% would flush the O/S cache for the file, which may
                %% be undesirable.
                wrap_write_file(State);
            fresh ->
                %% Time to start our first write file.
                case bitcask_lockops:acquire(write, State#bc_state.dirname) of
                    {ok, WriteLock} ->
                        try
                            {ok, NewWriteFile} = bitcask_fileops:create_file(
                                                   State#bc_state.dirname,
                                                   State#bc_state.opts,
                                                   State#bc_state.keydir),
                            ok = bitcask_lockops:write_activefile(
                                   WriteLock,
                                   bitcask_fileops:filename(NewWriteFile)),
                            State#bc_state{ write_file = NewWriteFile,
                                            write_lock = WriteLock }
                        catch error:{badmatch,Error} ->
                                throw({unrecoverable, Error, State})
                        end;
                    {error, _} = Error ->
                        throw({unrecoverable, Error, State})
                end;
            ok ->
                State
        end,

    Tstamp = bitcask_time:tstamp(),
    case Value of
        BinValue when is_binary(BinValue) ->
            case bitcask_fileops:write(State2#bc_state.write_file,
                                       Key, Value, Tstamp) of
                {ok, WriteFile2, Offset, Size} ->
                    case bitcask_nifs:keydir_put(State2#bc_state.keydir, Key,
                                                 bitcask_fileops:file_tstamp(WriteFile2),
                                                 Size, Offset, Tstamp,
                                                 bitcask_time:tstamp(), true) of
                        ok ->
                            {ok, State2#bc_state { write_file = WriteFile2 }};
                        already_exists ->
                            %% Assuming the timestamps in the keydir are
                            %% valid, there is an edge case where the merge thread
                            %% could have rewritten this Key to a file with a greater
                            %% file_id. Rather than synchronize the merge/writer processes, 
                            %% wrap to a new file with a greater file_id and rewrite
                            %% the key there.
                            %% We must undo the write here so a later partial merge
                            %% does not see it.
                            %% Limit the number of recursions in case there is
                            %% a different issue with the keydir.
                            {ok, WriteFile3} = bitcask_fileops:un_write(WriteFile2),
                            State3 = wrap_write_file(
                                       State2#bc_state { write_file = WriteFile3 }),
                            do_put(Key, Value, State3, Retries - 1, already_exists)
                    end;
                Error2 ->
                    throw({unrecoverable, Error2, State2})
            end;
        tombstone ->
            WriteFileId = bitcask_fileops:file_tstamp(State2#bc_state.write_file),
            case bitcask_nifs:keydir_get(State2#bc_state.keydir, Key) of
                not_found ->
                    {ok, State2};
                #bitcask_entry{file_id=OldFileId} when OldFileId > WriteFileId ->
                    State3 = wrap_write_file(State2),
                    do_put(Key, Value, State3, Retries - 1, already_exists);
                #bitcask_entry{tstamp=OldTstamp, file_id=OldFileId,
                               offset=OldOffset} ->
                    Tombstone = ?TOMBSTONE,
                    case bitcask_fileops:write(State2#bc_state.write_file,
                                               Key, Tombstone, Tstamp) of
                        {ok, WriteFile2, _, _} ->
                            case bitcask_nifs:keydir_remove(State2#bc_state.keydir,
                                                            Key, OldTstamp, OldFileId,
                                                            OldOffset) of
                                already_exists ->
                                    %% Merge updated the keydir after tombstone
                                    %% write.  beat us, so undo and retry in a
                                    %% new file.
                                    {ok, WriteFile3} =
                                        bitcask_fileops:un_write(WriteFile2),
                                    State3 = wrap_write_file(
                                               State2#bc_state {
                                                 write_file = WriteFile3 }),
                                    do_put(Key, Value, State3,
                                           Retries - 1, already_exists);
                                ok ->
                                    {ok, State2#bc_state { write_file = WriteFile2 }}
                            end;
                        {error, _} = ErrorTomb ->
                            throw({unrecoverable, ErrorTomb, State2})
                    end
            end
    end.


wrap_write_file(#bc_state{write_file = WriteFile} = State) ->
    try
        LastWriteFile = bitcask_fileops:close_for_writing(WriteFile),
        {ok, NewWriteFile} = bitcask_fileops:create_file(
                               State#bc_state.dirname,
                               State#bc_state.opts,
                               State#bc_state.keydir),
        ok = bitcask_lockops:write_activefile(
               State#bc_state.write_lock,
               bitcask_fileops:filename(NewWriteFile)),
        State#bc_state{ write_file = NewWriteFile,
                        read_files = [LastWriteFile | 
                                      State#bc_state.read_files]}
    catch
        error:{badmatch,Error} ->
            throw({unrecoverable, Error, State})
    end.

set_setuid_bit(File) ->
    %% We're intentionally opinionated about pattern matching here.
    {ok, FI} = bitcask_fileops:read_file_info(File),
    NewFI = FI#file_info{mode = FI#file_info.mode bor 8#4000},
    ok = bitcask_fileops:write_file_info(File, NewFI).

has_setuid_bit(File) ->
    try
        {ok, FI} = bitcask_fileops:read_file_info(File),
        FI#file_info.mode band 8#4000 == 8#4000
    catch _:_ ->
            false
    end.

purge_setuid_files(Dirname) ->
    case bitcask_lockops:acquire(write, Dirname) of
        {ok, WriteLock} ->
            try
                StaleFs = [F || F <- list_data_files(Dirname,
                                                     undefined, undefined),
                                has_setuid_bit(F)],
                _ = [bitcask_fileops:delete(#filestate{filename = F}) ||
                        F <- StaleFs],
                if StaleFs == [] ->
                        ok;
                   true ->
                        error_logger:info_msg("Deleted ~p stale merge input "
                                              "files from ~p\n",
                                              [length(StaleFs), Dirname])
                end
            catch
                X:Y ->
                    error_msg_perhaps("While deleting stale merge input "
                                      "files from ~p: ~p ~p @ ~p\n",
                                      [X, Y, erlang:get_stacktrace()])
            after
                bitcask_lockops:release(WriteLock)
            end;
        Else ->
            error_logger:info_msg("Lock failed trying deleting stale merge "
                                  "input files from ~p: ~p\n", [Dirname, Else])
    end.

poll_for_merge_lock(Dirname) ->
    poll_for_merge_lock(Dirname, 20).

poll_for_merge_lock(_Dirname, 0) ->
    case erlang:get(bitcask_testing_module) of
        undefined ->
            {error, {poll_for_merge_lock, not_testing, max_limit}};
        _TestMod ->
            ?POLL_FOR_MERGE_LOCK_PSEUDOFAILURE
    end;
poll_for_merge_lock(Dirname, N) ->
    case bitcask_lockops:acquire(merge, Dirname) of
        {ok, Lock} ->
            Lock;
        _ ->
            timer:sleep(100),
            poll_for_merge_lock(Dirname, N-1)
    end.

%% Internal merge function for cache_merge functionality.
expiry_merge([], _LiveKeyDir, _KT, Acc) ->
    Acc;
expiry_merge([File | Files], LiveKeyDir, KT, Acc0) ->
    FileId = bitcask_fileops:file_tstamp(File),
    Fun = fun(K, Tstamp, {Offset, _TotalSz}, Acc) ->
                bitcask_nifs:keydir_remove(LiveKeyDir, KT(K), Tstamp, FileId, Offset),
                Acc
        end,
    case bitcask_fileops:fold_keys(File, Fun, ok, default) of
        {error, Reason} ->
            error_logger:error_msg("Error folding keys for ~p: ~p\n", 
                                   [File#filestate.filename,Reason]),
            Acc = Acc0;
        _ ->
            error_logger:info_msg("All keys expired in: ~p scheduling "
                                  "file for deletion\n", 
                                  [File#filestate.filename]),
            Acc = lists:append(Acc0, [File])
    end,
    expiry_merge(Files, LiveKeyDir, KT, Acc).

get_key_transform(KT) 
  when is_function(KT) ->
    KT;
get_key_transform(_State) ->
    fun kt_id/1.

-ifdef(TEST).
error_msg_perhaps(_Fmt, _Args) ->
    ok.
-else. %TEST
error_msg_perhaps(Fmt, Args) ->
    error_logger:error_msg(Fmt, Args).
-endif. %TEST

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
    code:add_pathz("../ebin"),
    application:start(erlang),
    Mode = bitcask_io:determine_file_module(),
    error_logger:info_msg("Bitcask IO mode is: ~p\n", [Mode]).

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

    %% Generate a list of files from 8->12
    ExpFiles = [?FMT("/tmp/bc.test.list/~w.bitcask.data", [I]) ||
                   I <- lists:seq(8, 12)],

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

iterator_test() ->
    B = init_dataset("/tmp/bc.iterator.test.fold", default_dataset()),
    ok = iterator(B, 0, 0),
    Keys = [ begin #bitcask_entry{ key = Key } = iterator_next(B), Key end || 
             _ <- default_dataset() ],
    ?assertEqual(lists:sort(Keys), lists:sort([ Key  || {Key, _} <- default_dataset() ])), 
    iterator_release(B).


fold_corrupt_file_test() ->
    TestDir = "/tmp/bc.test.fold_corrupt_file_test",
    TestDataFile = TestDir ++ "/1.bitcask.data",

    os:cmd("rm -rf " ++ TestDir),
    B = bitcask:open(TestDir, [read_write]),
    ok = bitcask:put(B,<<"k">>,<<"v">>),
    close(B),

    DataList = [{<<"k">>, <<"v">>}],
    FoldFun = fun (K, V, Acc) -> [{K, V} | Acc] end,

    B2 = bitcask:open(TestDir),
    ?assertEqual(DataList, bitcask:fold(B2, FoldFun,[])),
    close(B2),

    % Incomplete header
    {ok, F} = file:open(TestDataFile, [append, raw, binary]),
    ok = file:write(F, <<100:32, 100:32, 100:16>>),
    file:close(F),
    B3 = bitcask:open(TestDir),
    ?assertEqual(DataList, bitcask:fold(B3, FoldFun,[])),
    close(B3),

    % Header without any data
    {ok, F2} = file:open(TestDataFile, [append, raw, binary]),
    ok = file:write(F2, <<100:32>>),
    file:close(F2),
    B4 = bitcask:open(TestDir),
    ?assertEqual(DataList, bitcask:fold(B4, FoldFun,[])),
    close(B4),
    ok.

put_till_frozen(R, Name) ->
    bitcask_nifs:keydir_put(R, crypto:rand_bytes(32), 0, 1234, 0, 1,
                            bitcask_time:tstamp()),
    {ready, Ref2} = bitcask_nifs:keydir_new(Name),
    %%?debugFmt("Putting", []),
    case bitcask_nifs:keydir_itr_int(Ref2, 2000001,
                                     0, 0) of
        ok ->
            %%?debugFmt("keydir still OK", []),
            bitcask_nifs:keydir_itr_release(Ref2),
            put_till_frozen(R, Name);
        out_of_date -> 
            %%?debugFmt("keydir now frozen", []),
            bitcask_nifs:keydir_itr_release(Ref2),
            ok
    end.

%%
%% Check that fold visits the objects at the point the keydir
%% was frozen.  Check with and without wrapping the cask.
%%
fold_visits_frozen_test_() ->
    [?_test(fold_visits_frozen_test(false)),
     ?_test(fold_visits_frozen_test(true))].

fold_visits_frozen_test(RollOver) ->
    Cask = "/tmp/bc.test.frozenfold",
    os:cmd("rm -r " ++ Cask),
    B = init_dataset(Cask, default_dataset()),
    try
        Ref = (get_state(B))#bc_state.keydir,
        Me = self(),
        
        %% Freeze the keydir with default_dataset written
        FrozenFun = fun() ->
                            Me ! frozen,
                            receive
                                done ->
                                    ok
                            end
                    end,
        FreezeWaiter = proc_lib:spawn_link(
                         fun() ->
                                 B2 = bitcask:open(Cask, [read]),
                                 Ref2 = (get_state(B2))#bc_state.keydir,
                                 %% Start a function that waits in a frozen keydir for a message
                                 %% so the test fold can guarantee a frozen keydir
                                 ok = bitcask_nifs:keydir_frozen(Ref2, FrozenFun, -1, -1),
                                 bitcask:close(B2)
                         end),
        receive
            frozen ->
                ok
        after 
            1000 ->
                ?assert(keydir_not_frozen)
        end,

        %% make sure that it's actually frozen
        put_till_frozen(Ref, Cask),

        %% If checking file rollover, update the state so the next write will 
        %% trigger a 'wrap' return.
        case RollOver of
            true ->
                State = get_state(B),
                put_state(B, State#bc_state{max_file_size = 0});
            _ ->
                ok
        end,

        %% A delete, an update and an insert
        ok = delete(B, <<"k">>),
        ok = put(B, <<"k2">>, <<"v2-2">>),
        ok = put(B, <<"k4">>, <<"v4">>),
        
        timer:sleep(900), %% wait for the disk to settle
        CollectAll = fun(K, V, Acc) ->
                             [{K, V} | Acc]
                     end,
        %% force fold over the frozen keydir
        L = fold(B, CollectAll, [], -1, -1, false),
        ?assertEqual(default_dataset(), lists:sort(L)),

        %% Unfreeze the keydir, waiting until complete
        FreezeWaiter ! done,
        ok = bitcask_nifs:keydir_wait_pending(Ref),
        %% TODO: find some ironclad way of coordinating the disk and
        %% test state, instead of using sleeps.
        timer:sleep(900),
        %% Check we see the updated fold
        L2 = fold(B, CollectAll, [], -1, -1, false),
        ?assertEqual([{<<"k2">>,<<"v2-2">>},
                      {<<"k3">>,<<"v3">>},
                      {<<"k4">>,<<"v4">>}], lists:sort(L2))
    after
        bitcask:close(B)
    end.

fold_visits_unfrozen_test_() ->
    [?_test(fold_visits_unfrozen_test(false)),
     ?_test(fold_visits_unfrozen_test(true))].

slow_worker() ->
    {Owner, Values} =
        receive
            {owner, O, Vs} -> {O, Vs}
        end,
    SlowCollect = fun(K, V, Acc) ->
                          if Acc == [] ->
                                  Owner ! i_have_started_folding,
                                  receive
                                      go_ahead_with_fold ->
                                          ok
                                  end;
                             true ->
                                  ok
                          end,
                          receive
                              go -> ok
                          end,
                          [{K, V} | Acc]
                  end,
    B = bitcask:open("/tmp/bc.test.unfrozenfold"),
    Owner ! ready,
    L = fold(B, SlowCollect, [], -1, -1, false),
    case Values =:= lists:sort(L) of 
        true ->
            Owner ! done;
        false ->
            Owner ! {sad, L}
    end,
    %%?debugFmt("slow worker finished~n", []),
    bitcask:close(B).
    
finish_worker_loop(Pid) ->
    receive
        done ->
            done;
        {sad, L} -> 
            {sad, L}
    after 0 ->
            Pid ! go,
            finish_worker_loop(Pid)
    end.


fold_visits_unfrozen_test(RollOver) ->
    %%?debugFmt("rollover is ~p~n", [RollOver]),
    Cask = "/tmp/bc.test.unfrozenfold",
    os:cmd("rm -r "++Cask),

    bitcask_time:test__set_fudge(1),
    B = init_dataset(Cask, default_dataset()),
    try
        Pid = spawn(fun slow_worker/0),
        Pid ! {owner, self(), default_dataset()},
        %% If checking file rollover, update the state so the next write will 
        %% trigger a 'wrap' return.
        case RollOver of
            true ->
                State = get_state(B),
                put_state(B, State#bc_state{max_file_size = 0});
            _ ->
                ok
        end,
        receive
            i_have_started_folding ->
                ok
        after 10*1000 ->
                error(timeout_should_never_happen)
        end,

        %% Until time independent epochs are merged, the fold timestamp
        %% is used to determine the snapshot, which has a 1 second resolution.
        %% So make sure updates happen at least 1 sec after folding starts
        bitcask_time:test__incr_fudge(1),
        
        %% A delete, an update and an insert
        ok = delete(B, <<"k">>),
        ok = put(B, <<"k2">>, <<"v2-2">>),
        ok = put(B, <<"k4">>, <<"v4">>),
        Pid ! go_ahead_with_fold,
     
        CollectAll = fun(K, V, Acc) ->
                             [{K, V} | Acc]
                     end,

        %% Unfreeze the keydir, waiting until complete
        case finish_worker_loop(Pid) of
            done -> ok;
            {sad, L} -> 
                ?assertEqual(default_dataset(), lists:sort(L))
        end,

        %% Check we see the updated fold
        L2 = fold(B, CollectAll, [], -1, -1, false),
        ?assertEqual([{<<"k2">>,<<"v2-2">>},
                      {<<"k3">>,<<"v3">>},
                      {<<"k4">>,<<"v4">>}], lists:sort(L2))
        %%?debugFmt("got past the asserts??~n", [])
    after
        bitcask:close(B),
        bitcask_time:test__clear_fudge()
    end.
    
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
    timer:sleep(900),
    %% Verify number of files in directory
    3 = length(readable_files("/tmp/bc.test.merge")),

    %% test that we can't merge a closed cask.
    {error, not_ready} = (catch merge("/tmp/bc.test.merge")),

    %% Merge everything
    M = bitcask:open("/tmp/bc.test.merge"),
    ok = merge("/tmp/bc.test.merge"),
    bitcask:close(M),

    %% Verify we've now only got one file
    1 = length(readable_files("/tmp/bc.test.merge")),

    %% Make sure all the data is present
    B = bitcask:open("/tmp/bc.test.merge"),
    lists:foldl(fun({K, V}, _) ->
                        R = bitcask:get(B, K),
                        ?assertEqual({K, {ok, V}}, {K, R})
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

fold1_test() ->
    os:cmd("rm -rf /tmp/bc.test.fold1"),
    B = bitcask:open("/tmp/bc.test.fold1", [read_write,{max_file_size, 1}]),
    ok = bitcask:put(B,<<"k">>,<<"v">>),
    ok = bitcask:put(B,<<"k">>,<<"v1">>),
    close(B),
    B2 = bitcask:open("/tmp/bc.test.fold1"),
    true = ([{<<"k">>,<<"v1">>}] =:=
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
    Keys = bitcask:list_keys(B),
    close(B),
    ?assertEqual([<<"k2">>,<<"k7">>], lists:sort(Keys)),
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
    M = bitcask:open("/tmp/bc.test.mergeexpire"),
    ok = merge("/tmp/bc.test.mergeexpire",[{expiry_secs,1}]),
    bitcask:close(M),

    %% With lazy merge file creation there will be no files.
    ok = bitcask_merge_delete:testonly__delete_trigger(),
    0 = length(readable_files("/tmp/bc.test.mergeexpire")),

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
    Dir = "/tmp/bc.test.delmerge",
    close(init_dataset(Dir, [{max_file_size, 1}],
                       default_dataset())),

    %% perform some deletes, tombstones should go in their own files
    B1 = bitcask:open(Dir, [read_write,{max_file_size, 1}]),
    ok = bitcask:delete(B1,<<"k2">>),
    ok = bitcask:delete(B1,<<"k3">>),
    A1 = [<<"k">>],
    A1 = bitcask:list_keys(B1),
    close(B1),

    M = bitcask:open("/tmp/bc.test.delmerge"),
    ok = merge("/tmp/bc.test.delmerge",[]),
    bitcask:close(M),

    %% Verify we've now only got one item left
    B2 = bitcask:open(Dir),
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
    M = bitcask:open("/tmp/bc.test.pardel"),
    ok = merge("/tmp/bc.test.pardel",[],{lists:reverse(lists:nthtail(2,
                                           lists:reverse(readable_files(
                                               "/tmp/bc.test.pardel")))),[]}),
    bitcask:close(M),

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

invalid_data_size_test() ->
    TestDir = "/tmp/bc.test.invalid_data_size_test",
    TestDataFile = TestDir ++ "/1.bitcask.data",

    os:cmd("rm -rf " ++ TestDir),
    B = bitcask:open(TestDir, [read_write]),
    ok = bitcask:put(B,<<"k">>,<<"v">>),
    close(B),

    % Alter data size
    {ok, F} = file:open(TestDataFile, [read, write, raw, binary]),
    {ok, _} = file:position(F, {eof, -6}),
    ok = file:write(F, <<255:?VALSIZEFIELD>>),
    file:close(F),
    B2 = bitcask:open(TestDir),
    ?assertEqual({error, bad_crc}, bitcask:get(B2, <<"k">>)),
    close(B2),
    ok.

testhelper_keydir_count(B) ->
    KD = (get_state(B))#bc_state.keydir,
    {KeyCount,_,_,_} = bitcask_nifs:keydir_info(KD),
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
    os:cmd("rm -rf /tmp/bc.test.fragtest"),
    os:cmd("mkdir /tmp/bc.test.fragtest"),
    B1 = bitcask:open("/tmp/bc.test.fragtest", [read_write]),
    ok = bitcask:put(B1,<<"k">>,<<"v">>),
    ok = bitcask:put(B1,<<"k">>,<<"z">>),
    ok = bitcask:close(B1),
    % close and reopen so that status can reflect a closed file
    B2 = bitcask:open("/tmp/bc.test.fragtest", [read_write]),
    {1,[{_,50,16,32}]} = bitcask:status(B2),
    %% 1 key, 50% frag, 16 dead bytes, 32 total bytes
    ok.

truncated_datafile_test() ->
    %% Mostly stolen from frag_status_test()....
    Dir = "/tmp/bc.test.truncdata",
    os:cmd("rm -rf " ++ Dir),
    os:cmd("mkdir " ++ Dir),
    B1 = bitcask:open(Dir, [read_write]),
    [ok = bitcask:put(B1, <<"k">>, <<X:32>>) || X <- lists:seq(1, 100)],
    ok = bitcask:close(B1),

    [DataFile|_] = filelib:wildcard(Dir ++ "/*.data"),
    truncate_file(DataFile, 512),

    % close and reopen so that status can reflect a closed file
    B2 = bitcask:open(Dir, [read_write]),

    {1, [{_, _, _, 513}]} = bitcask:status(B2),
    ok.

trailing_junk_big_datafile_test() ->
    Dir = "/tmp/bc.test.trailingdata",
    NumKeys = 400,
    os:cmd("rm -rf " ++ Dir),
    os:cmd("mkdir " ++ Dir),
    B1 = bitcask:open(Dir, [read_write, {max_file_size, 1024*1024*1024}]),
    [ok = bitcask:put(B1, <<"k", X:32>>, <<X:1024>>) || X <- lists:seq(1, NumKeys)],
    ok = bitcask:close(B1),

    [DataFile|_] = filelib:wildcard(Dir ++ "/*.data"),
    {ok, FH} = file:open(DataFile, [read, write]),
    {ok, _} = file:position(FH, 40*1024),
    ok = file:write(FH, <<0:(40*1024*8)>>),
    ok = file:close(FH),

    %% Merge everything
    M = bitcask:open(Dir),
    ok = merge(Dir),
    bitcask:close(M),

    B2 = bitcask:open(Dir, [read_write]),
    KeyList = bitcask:fold(B2, fun(K, _V, Acc0) -> [K|Acc0] end, []),
    true = length(KeyList) < NumKeys,
    ArbKey = 5,                         % get arbitrary key near start
    {ok, <<ArbKey:1024>>} = bitcask:get(B2, <<"k", ArbKey:32>>),
    ok = bitcask:close(B2),

    ok.

truncated_merge_test() ->
    Dir = "/tmp/bc.test.truncmerge",
    os:cmd("rm -rf " ++ Dir),
    os:cmd("mkdir " ++ Dir),

    %% Initialize dataset with max_file_size set to 1 so that each file will
    %% only contain a single key.
    %% If anyone ever modifies default_dataset() to return fewer than 3
    %% elements, this test will break.
    DataSet = default_dataset() ++ [{<<"k98">>, <<"v98">>},
                                    {<<"k99">>, <<"v99">>}],
    close(init_dataset(Dir, [{max_file_size, 1}], DataSet)),
    timer:sleep(900),

    %% Verify number of files in directory
    5 = length(readable_files(Dir)),

    [Data1, Data2, _, _, Data5|_] = filelib:wildcard(Dir ++ "/*.data"),
    [_, _, Hint3, Hint4|_] = filelib:wildcard(Dir ++ "/*.hint"),
          
    %% Truncate 1st after data file's header, read by bitcask_fileops:fold/3).
    %% Truncate 2nd in the middle of header, which provokes another
    %%     variation of bug mentioned by Bugzilla 1097.
    %% Truncate 3rd after hint file's header, read by
    %%     bitcask_fileops:fold_hintfile/3).
    %% Truncate 4th in the middle of header, same situation as 2nd....
    %% Truncate 5th data file at byte #1.
    ok = truncate_file(Data1, 15),
    ok = truncate_file(Data2, 5),
    ok = truncate_file(Hint3, 15),
    ok = truncate_file(Hint4, 5),
    ok = corrupt_file(Data5, 15, <<"!">>),
    %% Merge everything
    M = bitcask:open(Dir),
    ok = merge(Dir),
    bitcask:close(M),
    ok = bitcask_merge_delete:testonly__delete_trigger(),

    %% Verify we've now only got one file
    1 = length(readable_files(Dir)),

    %% Make sure all corrupted data is missing, all good data is present
    B = bitcask:open(Dir),
    BadKeys = [<<"k">>, <<"k2">>,               % Trunc of Data1 & Data2
               <<"k99">>],                      % Trunc of Data5
    {BadData, GoodData} =
        lists:partition(fun({K, _V}) -> lists:member(K, BadKeys) end, DataSet),
    lists:foldl(fun({K, _V} = KV, _) ->
                        {KV, not_found} = {KV, bitcask:get(B, K)}
                end, undefined, BadData),
    lists:foldl(fun({K, V} = KV, _) ->
                        {KV, {ok, V}} = {KV, bitcask:get(B, K)}
                end, undefined, GoodData).

truncate_file(Path, Offset) ->
    {ok, FH} = file:open(Path, [read, write]),
    {ok, Offset} = file:position(FH, Offset),
    ok = file:truncate(FH),
    file:close(FH).    

corrupt_file(Path, Offset, Data) ->
    {ok, FH} = file:open(Path, [read, write]),
    {ok, Offset} = file:position(FH, Offset),
    ok = file:write(FH, Data),
    file:close(FH).

%% About leak_t0():
%%
%% If bitcask leaks file descriptors for the 'touch'ed files, output is:
%%    Res =      920
%%
%% If bitcask isn't leaking the 'touch'ed files, output is:
%%    Res =       20

leak_t0() ->
    Dir = "/tmp/goofus",
    os:cmd("rm -rf " ++ Dir),

    _Ref0 = bitcask:open(Dir),
    os:cmd("cd " ++ Dir ++ "; sh -c 'for i in `seq 100 1000`; do touch $i.bitcask.data $i.bitcask.hint; done'"),
    _Ref1 = bitcask:open(Dir),

    Cmd = lists:flatten(io_lib:format("lsof -nP -p ~p | wc -l", [os:getpid()])),
    io:format("Res = ~s\n", [os:cmd(Cmd)]).

leak_t1() ->
    Dir = "/tmp/goofus",
    NumKeys = 300,
    os:cmd("rm -rf " ++ Dir),

    Ref = bitcask:open(Dir, [read_write, {max_file_size, 10}]),
    Used = fun() ->
                   Cmd = lists:flatten(io_lib:format("lsof -nP -p ~p | wc -l", [os:getpid()])),
                   %% io:format("Cmd = ~s\n", [Cmd]),
                   os:cmd(Cmd)
           end,
    [bitcask:put(Ref, <<X:32>>, <<"it's a big, big world!">>) ||
        X <- lists:seq(1, NumKeys)],
    io:format("After putting ~p keys, lsof says: ~s", [NumKeys, Used()]),

    DelKeys = NumKeys div 2,
    [bitcask:delete(Ref, <<X:32>>) || X <- lists:seq(1, DelKeys)],
    io:format("After deleting ~p keys, lsof says: ~s", [DelKeys, Used()]),

    bitcask:merge(Dir),
    io:format("After merging, lsof says: ~s", [Used()]),

    io:format("Q: Are all keys fetchable? ..."),
    [{ok, _} = bitcask:get(Ref, <<X:32>>) ||
        X <- lists:seq(DelKeys + 1, NumKeys)],
    [not_found = bitcask:get(Ref, <<X:32>>) || X <- lists:seq(1, DelKeys)],
    io:format("A: yes\n"),
    io:format("Now, lsof says: ~s", [Used()]),

    ok.

slow_folder(Cask) ->
    Owner = receive
                {owner, O} -> O
            end,
    SlowCollect = fun(K, V, Acc) ->
                          if Acc == [] ->
                                  Owner ! i_have_started_folding,
                                  receive
                                      go_ahead_with_fold ->
                                          ok
                                  end;
                             true ->
                                  ok
                          end,
                          receive
                              go -> ok;
                              go_reply -> Owner ! reply, ok
                          end,
                          [{K, V} | Acc]
                  end,
    B = bitcask:open(Cask),
    L = fold(B, SlowCollect, [], -1, -1, false),
    Owner ! {slow_folder_done, self(), L},
    bitcask:close(B).

finish_worker_loop2(Pid) ->
    receive
        {slow_folder_done, Pid, L} ->
            L
    after 0 ->
            Pid ! go,
            finish_worker_loop2(Pid)
    end.

freeze_close_reopen_test_() ->
    {timeout, 120, fun() -> freeze_close_reopen() end}.

freeze_close_reopen() ->
    Cask = "/tmp/bc.test.freeze_close_reopen_test" ++ os:getpid(),
    %% khash.h has an __ac_prime_list[] entry at 11, and 70% of 11 is
    %% approximately 8. So choose # of keys for Data to be a bit
    %% below 8, and then # of keys for Data2 will definitely be
    %% beyond the khash resizing point, e.g. 5x.
    Keys = 7,
    Data = [{<<K:32>>, <<K:32>>} || K <- lists:seq(1, Keys)],
    DelKey = 2,
    Data2 = [{<<K:32>>, <<(K+1):32>>} ||
                K <- lists:seq(1, Keys*5),
                K /= DelKey],
    B = init_dataset(Cask, Data),
    try
        CollectAll = fun(K, V, Acc) -> [{K, V} | Acc] end,
        PutData = fun(DataList) -> [begin ok = put(B, K, V) end ||
                                       {K, V} <- DataList]
                  end,

        ?assertEqual(Data, lists:sort(fold(B, CollectAll, [], -1, -1, false))),
        if true ->
                State = get_state(B),
                put_state(B, State#bc_state{max_file_size = 0})
        end,

        Pid = spawn(fun() -> slow_folder(Cask) end),
        Pid ! {owner, self()},
        receive
            i_have_started_folding ->
                ok
        after 10*1000 ->
                error(timeout_should_never_happen)
        end,

        %% The difference between the fold_visits_frozen_test test and
        %% this test is that fold_visits_frozen_test will put
        %% random-and-expired-and-thus-invisible keys into the keydir
        %% to freeze it before it modifies any keys that are in the k*
        %% range. This test modifies keys in the k* range
        %% immediately.
        PutData(Data2),
        %% We must be able to check that both kinds of mutation are
        %% tested .... delete a key also!
        ok = delete(B, <<DelKey:32>>),
        not_found = get(B, <<DelKey:32>>),

        %% Sanity check
        [{ok, V} = get(B, K) || {K, V} <- Data2],
        not_found = get(B, <<DelKey:32>>),

        ok = close(B),
        B2 = open(Cask, [read_write]),
        [{ok, V} = get(B2, K) || {K, V} <- Data2],
        not_found = get(B2, <<DelKey:32>>),
        %% It is too difficult here to figure out what fold would tell
        %% us. The multi-folder stuff will allow additions to be made
        %% as long as the khash doesn't resize.

        %% Unfreeze the keydir, waiting until complete
        Pid ! go_ahead_with_fold,
        L1a = finish_worker_loop2(Pid),

        %% Checking here is a bit crazy.
        %% 1. The first item that the worker's fold found was *prior*
        %% to our first mutation by PutData(). But the hash table
        %% scrambles key order, so we have to cheat by assuming that
        %% the last item in the folder's accumulator is the first
        %% key visited by the fold.
        %% 2. All of the other keys that were visited by the folder
        %% should appear frozen, so we compare them to *Data*.
        {FirstItemKey, _FirstItemValue} = FirstItemFound = lists:last(L1a),
        [ExpectedFirstItemFound] = [KV || KV = {K, _} <- Data,
                                          K == FirstItemKey],
        ?assertEqual(ExpectedFirstItemFound, FirstItemFound),
        ?assertEqual([KV || KV = {K, _} <- Data,
                            K /= FirstItemKey],
                     lists:sort(L1a) -- [FirstItemFound]),

        %% Check that we see the updated data yet again
        L3 = fold(B2, CollectAll, [], -1, -1, false),
        ?assertEqual(Data2, lists:sort(L3)),
        [{ok, V} = get(B2, K) || {K, V} <- Data2],
        not_found = get(B2, <<DelKey:32>>),

        bitcask:close(B2),
        ok
    after
        bitcask_time:test__clear_fudge(),
        catch bitcask:close(B),
        os:cmd("rm -rf " ++ Cask)
    end.

fold_itercount_test() ->
    Cask = "/tmp/bc.itercount-test/",
    os:cmd("rm -rf "++Cask),
    Ref = bitcask:open(Cask, [read_write]),
    try
        %% populate the store a little
        [bitcask:put(Ref, <<X:32>>, <<X>>)
         || X <- lists:seq(1, 100)],

        %% open a few slow folders
        Pids = [spawn(fun() -> slow_folder(Cask) end)
                || _ <- lists:seq(1,10)],
        [ Pid ! {owner, self()} || Pid <- Pids],
        [receive i_have_started_folding -> ok end
         || _ <- Pids],

        KD = (get_state(Ref))#bc_state.keydir,
        Info = bitcask_nifs:keydir_info(KD),
        {_,_,_,{_,Count,_,_}} = Info,

        ?assertEqual(length(Pids), Count),

        %% kill them all dead
        [ exit(Pid, brutal_kill) || Pid <- Pids],

        [erlang:garbage_collect(Pid) || Pid <- processes()],

        %% collect the iterator information and make sure that the
        %% count is still 0
        Info2 = bitcask_nifs:keydir_info(KD),
        {_,_,_,{_,Count2,_,_}} = Info2,
        ?assertEqual(0, Count2)
    after
        ok = bitcask:close(Ref),
        os:cmd("rm -rf "++Cask)
    end.


fold_lockstep_test_() ->
    {timeout, 100, fun fold_lockstep_body/0}.

fold_lockstep_body() ->
    Cask = "/tmp/bc.lockstep-test/",
    os:cmd("rm -rf "++Cask),
    Ref = bitcask:open(Cask, [read_write]),
    try
        Initial = 1500,  %% has to be large to avoid resize behavior.

        %% populate the store a little
        [bitcask:put(Ref, <<X:32>>, <<X>>)
         || X <- lists:seq(1, Initial)],

        Folders = 5,
        Me = self(),

        %%io:format(user, "spawning folders~n", []),
        Pids = [begin
                    P = spawn(fun() -> slow_folder(Cask) end),
                    P ! {owner, Me},
                    receive i_have_started_folding -> ok end,
                    P ! go_ahead_with_fold,
                    [bitcask:put(Ref, <<X:32>>, <<X>>)
                     || X <- lists:seq(Initial + 1 + 100*(N-1), Initial + 100*N)],
                    {P, Initial + 100*(N-1)}
                end
                || N <- lists:seq(1, Folders)],

        %%io:format(user, "iterating folders in step~n", []),
        [begin
             [begin
                  case I =< N of
                      true ->
                          P ! go_reply,
                          receive reply -> ok end;
                      false -> ok
                  end
              end|| {P,N} <- Pids],
             %%io:format(user, "step~n", []),
             timer:sleep(5)
         end
         || I <- lists:seq(1, Initial + 100 * Folders)],

        %%io:format(user, "collecting output~n", []),
        [begin
             receive
                 {slow_folder_done, P, L} ->
                     ?assertEqual(N, length(L))
             after 1000 ->
                     throw(maybe_deadlock)
             end
         end
         || {P,N} <- Pids],
        ok
    after
        ok = bitcask:close(Ref),
        os:cmd("rm -rf "++Cask)
    end.

no_tombstones_after_reopen_test() ->
    no_tombstones_after_reopen_test2(false).

zap_hints_no_tombstones_after_reopen_test() ->
    no_tombstones_after_reopen_test2(true).

no_tombstones_after_reopen_test2(DeleteHintFilesP) ->
    Dir = "/tmp/bc.test.truncmerge",
    MaxFileSize = 100,
    os:cmd("rm -rf " ++ Dir),
    os:cmd("mkdir " ++ Dir),

    %% Initialize dataset with max_file_size set to 1 so that each file will
    %% only contain a single key.
    %% If anyone ever modifies default_dataset() to return fewer than 3
    %% elements, this test will break.
    KVs = [{<<X:32>>, <<X:32>>} || X <- lists:seq(33, 52)],
    DataSet = default_dataset() ++ KVs,
    B = init_dataset(Dir, [{max_file_size, MaxFileSize}], DataSet),
    [bitcask:delete(B, <<X:32>>) || X <- lists:seq(40, 41)],
    close(B),

    if DeleteHintFilesP ->
            os:cmd("rm " ++ Dir ++ "/*.hint");
       true ->
            ok
    end,

    B2 = bitcask:open(Dir, [read_write, {max_file_size, MaxFileSize}]),

    Res1 = bitcask:fold(B2, fun(K, _V, Acc0) -> [K|Acc0] end, [], -1, -1, true),
    ?assertNotEqual([], [X || {tombstone, _} = X <- Res1]),

    Res2 = bitcask:fold_keys(B2, fun(K, Acc0) -> [K|Acc0] end, [], -1, -1, true),
    ?assertEqual([], [X || {tombstone, _} = X <- Res2]).

update_tstamp_stats_test() ->
    Dir = "/tmp/bc.tstamp.stats",
    bitcask_time:test__set_fudge(1),
    try
        B = init_dataset(Dir, [read_write, {max_file_size, 1000000}], []),
        Write = fun(KVs) ->
                        [ begin
                              bitcask:put(B, K, V),
                              bitcask_time:test__incr_fudge(1)
                          end || {K, V} <- KVs]
                end,

        Write([{<<"k1">>, <<"v1">>}, {<<"k2">>, <<"v2">>},
               {<<"k3">>, <<"v3">>}]),
        ok = bitcask:close(B),

        B2 = bitcask:open(Dir),
        ?assertMatch({3, [#file_status{oldest_tstamp=1,
                                       newest_tstamp=3}]},
                     summary_info(B2)),
        bitcask:close(B2)
    after
        bitcask_time:test__clear_fudge()
    end.

-endif.
