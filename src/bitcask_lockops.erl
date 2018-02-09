%% -------------------------------------------------------------------
%%
%% bitcask: Eric Brewer-inspired key/value store
%%
%% Copyright (c) 2010 Basho Technologies, Inc. All Rights Reserved.
%% Copyright (c) 2018 Workday, Inc.
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
-module(bitcask_lockops).

-export([acquire/2,
         release/1,
         read_activefile/2,
         try_write_lock_acquisition/1,
         write_activefile/2]).
-ifdef(TEST).
-export([lock_filename/2]).
-endif.

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

-type lock_types() :: merge | write | create.

%% @doc Attempt to lock the specified directory with a specific type of lock
%% (merge or write).
-spec acquire(Type::lock_types(), Dirname::string()) -> {ok, reference()} | {error, any()}.
acquire(Type, Dirname) ->
    LockFilename = lock_filename(Type, Dirname),
    IsWriteLock = 1,
    case bitcask_nifs:lock_acquire(LockFilename, IsWriteLock) of
        {ok, Lock} ->
            %% Successfully acquired our lock. Update the file with our PID.
            case bitcask_nifs:lock_writedata(Lock, iolist_to_binary([os:getpid(), " \n"])) of
                ok ->
                    {ok, Lock};
                {error, _} = Else ->
                    Else
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Release a previously acquired write/merge lock.
-spec release(reference()) -> ok.
release(Lock) ->
    bitcask_nifs:lock_release(Lock).

%% @doc Read the active filename stored in a given lockfile.
-spec read_activefile(Type::lock_types(), Dirname::string()) -> string() | undefined.
read_activefile(Type, Dirname) ->
    LockFilename = lock_filename(Type, Dirname),
    case bitcask_nifs:lock_acquire(LockFilename, 0) of
        {ok, Lock} ->
            try
                case read_lock_data(Lock) of
                    {ok, _Pid, ActiveFile} ->
                        ActiveFile;
                    _ ->
                        undefined
                end
            after
                bitcask_nifs:lock_release(Lock)
            end;
        {error, _Reason} ->
            undefined
    end.

%% @doc Write a new active filename to an open lockfile.
-spec write_activefile(reference(), string()) -> {ftruncate_error, integer()} | {pwrite_error, integer()} | ok | {error, lock_not_writable}.
write_activefile(Lock, ActiveFilename) ->
    Contents = iolist_to_binary([os:getpid(), " ", ActiveFilename, "\n"]),
    bitcask_nifs:lock_writedata(Lock, Contents).

%% ===================================================================
%% Internal functions
%% ===================================================================

lock_filename(Type, Dirname) ->
    filename:join(Dirname, lists:concat(["bitcask.", Type, ".lock"])).

read_lock_data(Lock) ->
    case bitcask_nifs:lock_readdata(Lock) of
        {ok, Contents} ->
            case re:run(Contents, "([0-9]+) (.*)\n",
                        [{capture, all_but_first, list}]) of
                {match, [OsPid, []]} ->
                    {ok, OsPid, undefined};
                {match, [OsPid, LockedFilename]} ->
                    {ok, OsPid, LockedFilename};
                nomatch ->
                    {error, {invalid_data, Contents}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% Attempt to obtain the file lock across OS processes, within the span of the call
%% to check if the file is already in use.
try_write_lock_acquisition(Filename) ->
    IsWriteLock = 1,
    case bitcask_nifs:lock_acquire(lock_filename(write, Filename), IsWriteLock) of
        {ok, Lock} ->
            bitcask_nifs:lock_release(Lock);
        {error, enoent} ->
            %% Failed to open the lock for reading, but only because it doesn't exist
            %% any longer. Treat this as a successful delete; the lock file existed
            %% when we started.
            ok;
        {error, Reason} ->
            %% Failed to open the lock for reading due to other errors.
            error_logger:error_msg("Failed to open lock file ~s: ~p\n",
                                   [Filename, Reason]),
            not_stale
    end.