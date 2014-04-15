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
-module(bitcask_lockops).

-export([acquire/2,
         release/1,
         delete_stale_lock/2,
         read_activefile/2,
         write_activefile/2]).

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

-type lock_types() :: merge | write | create.

%% @doc Attempt to lock the specified directory with a specific type of lock
%% (merge or write).
-spec acquire(Type::lock_types(), Dirname::string()) -> {ok, reference()} | {error, any()}.
acquire(Type, Dirname) ->
    LockFilename = lock_filename(Type, Dirname),
    case bitcask_nifs:lock_acquire(LockFilename, 1) of
        {ok, Lock} ->
            %% Successfully acquired our lock. Update the file with our PID.
            case bitcask_nifs:lock_writedata(Lock, iolist_to_binary([os:getpid(), " \n"])) of
                ok ->
                    {ok, Lock};
                {error, _} = Else ->
                    Else
            end;
        {error, eexist} ->
            %% Lock file already exists, but may be stale. Delete it if it's stale
            %% and try to acquire again
            case delete_stale_lock(LockFilename) of
                ok ->
                    acquire(Type, Dirname);
                not_stale ->
                    {error, locked}
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

delete_stale_lock(Type, Dirname) ->
    delete_stale_lock(lock_filename(Type,Dirname)).

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

os_pid_exists(Pid) ->
    %% Use kill -0 trick to determine if a process exists. This _should_ be
    %% portable across all unix variants we are interested in.
    [] == os:cmd(io_lib:format("kill -0 ~s", [Pid])).


delete_stale_lock(Filename) ->
    %% Open the lock for read-only access. We do this to avoid race-conditions
    %% with other O/S processes that are attempting the same task. Opening a
    %% fd and holding it open until AFTER the unlink ensures that the file we
    %% initially read is the same one we are deleting.
    case bitcask_nifs:lock_acquire(Filename, 0) of
        {ok, Lock} ->
            try
                case read_lock_data(Lock) of
                    {ok, OsPid, _LockedFilename} ->
                        case os_pid_exists(OsPid) of
                            true ->
                                %% The lock IS NOT stale, so we can't delete it.
                                not_stale;
                            false ->
                                %% The lock IS stale; delete the file.
                                _ = file:delete(Filename),
                                ok
                        end;

                    {error, Reason} ->
                        error_logger:error_msg("Failed to read lock data from ~s: ~p\n",
                                               [Filename, Reason]),
                        not_stale
                end
            after
                bitcask_nifs:lock_release(Lock)
            end;

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
