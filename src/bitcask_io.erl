%% -------------------------------------------------------------------
%%
%% bitcask: Eric Brewer-inspired key/value store
%%
%% Copyright (c) 2013 Basho Technologies, Inc. All Rights Reserved.
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
-module(bitcask_io).
-compile(export_all).

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

file_open(Filename, Opts) ->
    M = file_module(),
    M:file_open(Filename, Opts).

file_close(Ref) ->
    M = file_module(),
    M:file_close(Ref).

file_sync(Ref) ->
    M = file_module(),
    M:file_sync(Ref).

file_pread(Ref, Offset, Size) ->
    M = file_module(),
    M:file_pread(Ref, Offset, Size).

file_pwrite(Ref, Offset, Bytes) ->
    M = file_module(),
    M:file_pwrite(Ref, Offset, Bytes).

file_read(Ref, Size) ->
    M = file_module(),
    M:file_read(Ref, Size).

file_write(Ref, Bytes) ->
    M = file_module(),
    M:file_write(Ref, Bytes).

file_seekbof(Ref) ->
    M = file_module(),
    M:file_seekbof(Ref).

file_position(Ref, Position) ->
    M = file_module(),
    M:file_position(Ref, Position).

file_truncate(Ref) ->
    M = file_module(),
    M:file_truncate(Ref).

file_module() ->
    case get(bitcask_file_mod) of
        undefined ->
            Mod = determine_file_module(),
            put(bitcask_file_mod, Mod),
            Mod;
        Mod ->
            Mod
    end.

-ifdef(TEST).
determine_file_module() ->
    case application:get_env(bitcask, io_mode) of
        {ok, erlang} ->
            bitcask_file;
        {ok, nif} ->
            bitcask_nifs;
        _ ->
            Mode = case os:getenv("BITCASK_IO_MODE") of
                       false    -> 'erlang';
                       "erlang" -> 'erlang';
                       "nif"    -> 'nif'
                    end,
            application:set_env(bitcask, io_mode, Mode),
            determine_file_module()
    end.
-else.
determine_file_module() ->
    case application:get_env(bitcask, io_mode) of
        {ok, erlang} ->
            bitcask_file;
        {ok, nif} ->
            bitcask_nifs;
        _ ->
            bitcask_file
    end.
-endif.
