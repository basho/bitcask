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

-export([write_lock_check/1,
         write_lock_acquire/1,
         write_lock_update/2,
         write_lock_release/1]).

write_lock_check(Dirname) ->
    write_lock_check(Dirname, 3).

write_lock_check(Dirname, 0) ->
    error_logger:error_msg("Timed out waiting for partial write lock in ~s\n", [Dirname]),
    {undefined, undefined};
write_lock_check(Dirname, Count) ->
    case file:read_file(write_lock_file(Dirname)) of
        {ok, Bin} ->
            case re:run(Bin, "([0-9]+) (.*)\n", [{capture, all_but_first, list}]) of
                {match, [WritingFile, []]} ->
                    {WritingFile, undefined};
                {match, [WritingPid, WritingFile]} ->
                    {WritingPid, WritingFile};
                nomatch ->
                    %% A lock file exists, but is not complete.
                    timer:sleep(10),
                    write_lock_check(Dirname, Count-1)
            end;
        {error, enoent} ->
            %% Lock file doesn't exist
            {undefined, undefined};
        {error, Reason} ->
            error_logger:error_msg("Failed to check write lock in ~s: ~p\n", [Dirname, Reason]),
            {undefined, undefined}
    end.

write_lock_acquire(Dirname) ->
    case bitcask_nifs:create_file(filename:join(Dirname, "bitcask.write.lock")) of
        true ->
            %% Write out our PID w/ empty place for file name (since we don't know it yet)
            ok = file:write_file(write_lock_file(Dirname),
                                 [os:getpid(), " \n"]),
            true;
        false ->
            false
    end.

write_lock_release(Dirname) ->
    ThisOsPid = os:getpid(),
    case write_lock_check(Dirname) of
        {ThisOsPid, _} ->
            ok = file:delete(write_lock_file(Dirname));

        _ ->
            {error, not_write_lock_owner}
    end.

write_lock_update(Dirname, ActiveFileName) ->
    ThisOsPid = os:getpid(),
    case write_lock_check(Dirname) of
        {ThisOsPid, _} ->
            ok = file:write_file(write_lock_file(Dirname),
                                 [os:getpid(), " ", ActiveFileName, "\n"]);
        _ ->
            {error, not_write_lock_owner}
    end.


%% ===================================================================
%% Internal functions
%% ===================================================================

write_lock_file(Dirname) ->
    filename:join(Dirname, "bitcask.write.lock").
