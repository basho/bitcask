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
    check_loop(lock_filename(Type, Dirname), 3).

%% @private
check_loop(Filename, 0) ->
    error_logger:error_msg("Timed out waiting for partial write lock in ~s\n",
                           [Filename]),
    {undefined, undefined};
check_loop(Filename, Count) ->
    case file:read_file(Filename) of
        {ok, Bin} ->
            case re:run(Bin, "([0-9]+) (.*)\n",
                        [{capture, all_but_first, list}]) of
                {match, [WritingPid, []]} ->
                    {WritingPid, undefined};
                {match, [WritingPid, WritingFile]} ->
                    {WritingPid, WritingFile};
                nomatch ->
                    %% A lock file exists, but is not complete.
                    timer:sleep(10),
                    check_loop(Filename, Count-1)
            end;
        {error, enoent} ->
            %% Lock file doesn't exist
            {undefined, undefined};
        {error, Reason} ->
            error_logger:error_msg("Failed to check write lock in ~s: ~p\n",
                                   [Filename, Reason]),
            {undefined, undefined}
    end.

%% @doc Attempt to lock the specified directory with a specific type of lock (merge or write). Returns
%% true on success.
-spec acquire(Type::lock_types(), Dirname::string()) -> true | false.
acquire(Type, Dirname) ->
    Filename = lock_filename(Type, Dirname),
    case bitcask_nifs:create_file(Filename) of
        true ->
            %% Write out our PID w/ empty place for file name
            %% (since we don't know it yet)
            ok = file:write_file(Filename, [os:getpid(), " \n"]),
            true;
        false ->
            false
    end.

%% @doc Attempt to remove a write or merge lock on a directory. Ownership by
%% this O/S PID is verified; within the Erlang VM you must serialize calls for a
%% given directory.
-spec release(Type::lock_types(), Dirname::string()) -> ok | {error, not_lock_owner}.
release(Type, Dirname) ->
    ThisOsPid = os:getpid(),
    Filename = lock_filename(Type, Dirname),
    case check_loop(Filename, 3) of
        {ThisOsPid, _} ->
            ok = file:delete(Filename);
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
    case check_loop(Filename, 3) of
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
    filename:join(Dirname, lists:concat(["bitcast.", Type, ".lock"])).
