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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
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

-ifdef(TEST).

truncate_test_() ->
    {timeout, 60, fun truncate_test2/0}.

truncate_test2() ->
    Dir = "/tmp/bc.test.bitcask_io/",
    one_truncate(filename:join(Dir, "truncate_test1.dat"), 50, 50),
    one_truncate(filename:join(Dir, "truncate_test2.dat"), {bof, 50}, 50),
    one_truncate(filename:join(Dir, "truncate_test3.dat"), {cur, -25}, 75),
    one_truncate(filename:join(Dir, "truncate_test4.dat"), {eof, -75}, 25).

one_truncate(Fname, Ofs, ExpectedSize) ->
    ?assertMatch(ok, filelib:ensure_dir(Fname)),
    file:delete(Fname),
    Open1 = file_open(Fname, [create]),
    ?assertMatch({ok, _}, Open1),
    {ok, File} = Open1,
    % Write 100 bytes
    Bytes = <<0:100/integer-unit:8>>,
    ?assertEqual(100, size(Bytes)),
    ok = file_write(File, Bytes),
    ?assertEqual({Ofs, {ok, ExpectedSize}}, {Ofs, file_position(File, Ofs)}),
    ok = file_truncate(File),
    ok = file_close(File),
    % Verify size with regular file operations
    {ok, File3} = file:open(Fname, [read, raw, binary]),
    SizeRes = file:position(File3, {eof, 0}),
    ok = file:close(File3),
    ?assertEqual({ok, ExpectedSize}, SizeRes).

-endif.
