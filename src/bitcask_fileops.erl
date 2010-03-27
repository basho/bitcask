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

-export([create_file/1,
         open_file/1,
         close/1,
         write/3,
         read/3,
         filename/2]).

-include("bitcask.hrl").

-define(BOUNDARY, <<17:16>>).
-define(KEYSIZEFIELD, 16).
-define(VALSIZEFIELD, 32).

%% @doc Open a new file for writing.
%% Called on a Dirname, will open a fresh file in that directory.
%% @spec create_file(Dirname :: string()) -> {ok, filestate()}
create_file(DirName) ->
    Filename = filename(DirName, tstamp()),
    ok = filelib:ensure_dir(Filename),
    case bitcase_nifs:create_file(Filename) of
        true ->
            {ok, FD} = file:open(Filename, [read, write, raw, binary]),
            {ok, #filestate{filename = Filename, fd = FD, ofs = 0}};
        false ->
            %% Couldn't create a new file with the requested name, so let's
            %% delay 500 ms & try again. The working assumption is that this is
            %% not a highly contentious code point. Latency lovers beware!
            timer:sleep(500),
            create_file(DirName)
    end.


%% @doc Open an existing file for reading.
%% Called with fully-qualified filename.
%% @spec open_file(Filename :: string()) -> {ok, filestate()} | {error, any()}
open_file(Filename) ->
    case file:open(Filename, [read, raw, binary]) of
        {ok, FD} ->
            {ok, #filestate{ filename = Filename, fd = FD, ofs = 0 }};
        {error, Reason} ->
            {error, Reason}
    end.


%% @doc Use when done writing a file.  (never open for writing again)
%% @spec close(filestate()) -> ok
close(#filestate{ fd = FD }) ->
    file:close(FD),
    ok.


%% @doc Write a Key-named binary data field ("Value") to the Filestate.
%% @spec write(filestate(), Key :: binary(), Value :: binary()) ->
%%       {ok, filestate(), Offset :: integer(), Size :: integer()}
write(Filestate=#filestate{fd = FD, ofs = Offset}, Key, Value) ->
    KeySz = size(Key),
    true = (KeySz =< ?KEYSIZEFIELD),
    ValueSz = size(Value),
    true = (ValueSz =< ?VALSIZEFIELD),
    %% Setup io_list for writing -- avoid merging binaries if we can help it
    Bytes = [?BOUNDARY, <<KeySz:?KEYSIZEFIELD>>, Key,
             <<ValueSz:?VALSIZEFIELD>>, Value],
    ok = file:pwrite(FD, Offset, Bytes),
    FinalSz = size(?BOUNDARY) + ?KEYSIZEFIELD + KeySz + ?VALSIZEFIELD + ValueSz,
    {ok, Filestate#filestate{ofs = Offset + FinalSz}, Offset, FinalSz}.


%% @doc Given an Offset and Size, get the corresponding k/v from Filename.
%% @spec read(Filename :: string(), Offset :: integer(), Size :: integer()) ->
%%       {ok, Key :: binary(), Bytes :: binary()}
read(Filename, Offset, Size) when is_list(Filename) ->
    case open_file(Filename) of
        {ok, Fstate} ->
            read(Fstate, Offset, Size);
        {error, Reason} ->
            {error, Reason}
    end;
read(#filestate { fd = FD }, Offset, Size) ->
    case file:pread(FD, Offset, Size) of
        {ok, Bytes} ->
            <<17:16,
             KeySz:?KEYSIZEFIELD,
             Key:KeySz/bytes,
             ValueSz:?VALSIZEFIELD,
             Value:ValueSz/bytes>> = Bytes,
            {ok, Key, Value};
        {error, Reason} ->
            {error, Reason}
    end.

filename(Dirname, Tstamp) ->
    filename:join(Dirname, lists:concat(["bitcask.", Tstamp, ".data"])).


%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
tstamp() ->
    {Mega, Sec, _Micro} = now(),
    (Mega * 1000000) + Sec.

