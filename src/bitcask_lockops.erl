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

-export([lock_check/1,
         lock_acquire/1,
         lock_update/2,
         lock_release/1,
         write_lock_filename/1,
         merge_lock_filename/1]).

lock_check(Filename) ->
    lock_check(Filename, 3).

lock_check(Filename, 0) ->
    error_logger:error_msg("Timed out waiting for partial write lock in ~s\n",
                           [Filename]),
    {undefined, undefined};
lock_check(Filename, Count) ->
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
                    lock_check(Filename, Count-1)
            end;
        {error, enoent} ->
            %% Lock file doesn't exist
            {undefined, undefined};
        {error, Reason} ->
            error_logger:error_msg("Failed to check write lock in ~s: ~p\n",
                                   [Filename, Reason]),
            {undefined, undefined}
    end.

lock_acquire(Filename) ->
    case bitcask_nifs:create_file(Filename) of
        true ->
            %% Write out our PID w/ empty place for file name
            %% (since we don't know it yet)
            ok = file:write_file(Filename, [os:getpid(), " \n"]),
            true;
        false ->
            false
    end.

lock_release(Filename) ->
    ThisOsPid = os:getpid(),
    case lock_check(Filename) of
        {ThisOsPid, _} ->
            ok = file:delete(Filename);
        _ ->
            {error, not_write_lock_owner}
    end.

lock_update(Filename, ActiveFileName) ->
    ThisOsPid = os:getpid(),
    case lock_check(Filename) of
        {ThisOsPid, _} ->
            ok = file:write_file(Filename,
                                 [os:getpid(), " ", ActiveFileName, "\n"]);
        _ ->
            {error, not_write_lock_owner}
    end.


%% ===================================================================
%% Internal functions
%% ===================================================================

write_lock_filename(Dirname) ->
    filename:join(Dirname, "bitcask.write.lock").

merge_lock_filename(Dirname) ->
    filename:join(Dirname, "bitcask.merge.lock").
