%% -------------------------------------------------------------------
%%
%% bitcask: Eric Brewer-inspired key/value store
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Basic file i/o operations for bitcask.
-module(bitcask_fileops).
-author('Justin Sheehy <justin@basho.com>').

-export([open/1,close/1,read/3,write/3]).

%% @type filestate().
-record(filestate, {filename, fd, ofs}).

-define(BOUNDARY, <<17:16>>).
-define(KEYSIZEFIELD, 16).
-define(VALSIZEFIELD, 32).

%% @doc Open a new filename for writing.
%% This should only be called on a not-yet-existent filename in a writable dir.
%% @spec open(Filename :: string()) -> {ok, filestate()}
open(Filename) ->
    false = filelib:is_file(Filename),
    ok = filelib:ensure_dir(Filename),
    {ok, FD} = file:open(Filename, [read, write, raw, binary]),
    {ok, #filestate{filename=Filename,fd=FD,ofs=0}}.

%% @doc Use when done writing a file.  (never open for writing again)
%% @spec close(filestate()) -> ok
close(_Filestate=#filestate{fd=FD}) ->
    file:close(FD),
    ok.

%% @doc Write a Key-named binary data field ("Bytes") to the Filestate.
%% @spec write(filestate(), Key :: binary(), Bytes :: binary()) ->
%%       {ok, filestate(), Offset :: integer(), Size :: integer()}
write(Filestate=#filestate{fd=FD,ofs=OFS}, Key, Bytes) ->
    KeySize = size(Key),
    true = (KeySize =< ?KEYSIZEFIELD),
    BytesSize = size(Bytes),
    true = (BytesSize =< ?VALSIZEFIELD),
    FullBytes = <<?BOUNDARY/binary,
                  KeySize:?KEYSIZEFIELD,Key/binary,
                  BytesSize:?VALSIZEFIELD,Bytes/binary>>,
    FullSize = size(FullBytes),
    ok = file:pwrite(FD,OFS,FullBytes),
    {ok, Filestate#filestate{ofs=OFS+FullSize}, OFS, FullSize}.

%% @doc Given an Offset and Size, get the corresponding k/v from Filename.
%% @spec read(Filename :: string(), Offset :: integer(), Size :: integer()) ->
%%       {ok, Key :: binary(), Bytes :: binary()}
read(Filename, Offset, Size) ->
    {ok, FD} = file:open(Filename, [read, raw, binary]),
    {ok, <<17:16,
           KeySize:?KEYSIZEFIELD,
           KeyPlusVal/binary>>} = file:pread(FD, Offset, Size),
    <<Key:KeySize/binary,_BytesSize:?VALSIZEFIELD,Bytes/binary>> = KeyPlusVal,
    {ok, Key, Bytes}.

