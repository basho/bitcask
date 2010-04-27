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
-author('Dave Smith <dizzyd@basho.com>').
-author('Justin Sheehy <justin@basho.com>').

-export([check/2,
         acquire/2,
         release/2,
         update/3]).

-type lock_types() :: merge | write.

%% @doc Check for the existence of lock in the specified directory. Returns a
%% tuple containing the operating system PID and the file currently locked. If
%% the lock file doesn't exist, the tuple will just be {undefined, undefined}.
-spec check(Type::lock_types(), Dirname::string()) -> { string() , string() }.
check(Type, Dirname) ->
    case check_loop(lock_filename(Type, Dirname), 3) of
        {ok, File, OsPid, LockedFilename} ->
            file:close(File),
            {OsPid, LockedFilename};
        error ->
            {undefined, undefined}
    end.


%% @doc Attempt to lock the specified directory with a specific type of lock (merge or write). Returns
%% true on success.
-spec acquire(Type::lock_types(), Dirname::string()) -> true | false.
acquire(Type, Dirname) ->
    LockFilename = lock_filename(Type, Dirname),
    case bitcask_nifs:create_file(LockFilename) of
        true ->
            %% Write out our PID w/ empty place for file name
            %% (since we don't know it yet)
            ok = file:write_file(LockFilename, [os:getpid(), " \n"]),
            true;
        false ->
            %% The lock file we want already exists. However, it is possible
            %% that it is stale (i.e. owning PID is no longer running). In that
            %% situation, we'll delete the stale file and try to acquire the
            %% lock again.
            %%
            %% It is necessary to be very careful when deleting the stale file,
            %% however, to avoid competing processes tromping on each other's
            %% lock files. To accomplish this, we want to:
            %% 1. Open the lock file and read contents -- DO NOT CLOSE
            %% 2. If the OsPid does not exist, unlink the file
            %% 3. Close the file handle from step 1
            %% 4. Try again to acquire the lock
            case check_loop(LockFilename, 3) of
                {ok, LockFile, OsPid, _LockedFilename} ->
                    case os_pid_exists(OsPid) of
                        true ->
                            %% The lock IS NOT stale, so we can't acquire it
                            false;
                        false ->
                            %% The lock IS stale, so let's unlink the lock file
                            %% and try again
                            file:delete(LockFilename),
                            file:close(LockFile),
                            acquire(Type, Dirname)
                    end;
                error ->
                    %% We timed out or otherwise ran into problems trying to
                    %% read the lock file. As such, simply give up.
                    false
            end
    end.

%% @doc Attempt to remove a write or merge lock on a directory. Ownership by
%% this O/S PID is verified; within the Erlang VM you must serialize calls for a
%% given directory.
-spec release(Type::lock_types(), Dirname::string()) -> ok | {error, not_lock_owner}.
release(Type, Dirname) ->
    ThisOsPid = os:getpid(),
    Filename = lock_filename(Type, Dirname),
    case check_loop(Filename, 3) of
        {ok, File, ThisOsPid, _} ->
            ok = file:delete(Filename),
            file:close(File),
            ok;
        {ok, File, _, _} ->
            file:close(File),
            {error, not_lock_owner};
        _ ->
            {error, not_lock_owner}
    end.

%% @doc Update the contents of a lock file within a directory. Ownership by this
%% O/S PID is verified; within the Erlang VM you must serialize updates to avoid
%% data loss.
-spec update(Type::lock_types(), Dirname::string(), ActiveFileName::string()) -> ok | {error, not_lock_owner}.
update(Type, Dirname, ActiveFileName) ->
    ThisOsPid = os:getpid(),
    Filename = lock_filename(Type, Dirname),
    case check(Type, Dirname) of
        {ThisOsPid, _} ->
            ok = file:write_file(Filename,
                                 [os:getpid(), " ", ActiveFileName, "\n"]);
        _ ->
            {error, not_lock_owner}
    end.


%% ===================================================================
%% Internal functions
%% ===================================================================

lock_filename(Type, Dirname) ->
    filename:join(Dirname, lists:concat(["bitcask.", Type, ".lock"])).

%% @private
check_loop(Filename, 0) ->
    error_logger:error_msg("Timed out waiting for partial write lock in ~s\n",
                           [Filename]),
    error;
check_loop(Filename, Count) ->
    case file:open(Filename, [read, raw, binary]) of
        {ok, File} ->
            Contents = file_contents(File, <<>>),
            case re:run(Contents, "([0-9]+) (.*)\n",
                        [{capture, all_but_first, list}]) of
                {match, [OsPid, []]} ->
                    {ok, File, OsPid, undefined};
                {match, [OsPid, LockedFilename]} ->
                    {ok, eFile, OsPid, LockedFilename};
                nomatch ->
                    %% A lock file exists, but is not complete.
                    file:close(File),
                    timer:sleep(10),
                    check_loop(Filename, Count-1)
            end;
        {error, enoent} ->
            %% Lock file doesn't exist
            error;
        {error, Reason} ->
            error_logger:error_msg("Failed to check write lock in ~s: ~p\n",
                                   [Filename, Reason]),
            error
    end.

file_contents(F, Acc) ->
    case file:read(F, 4096) of
        {ok, Data} ->
            file_contents(F, <<Acc/binary, Data/binary>>);
        _ ->
            Acc
    end.

os_pid_exists(Pid) ->
    %% Use kill -0 trick to determine if a process exists. This _should_ be
    %% portable across all unix variants we are interested in.
    [] == os:cmd(io_lib:format("kill -0 ~s", [Pid])).
