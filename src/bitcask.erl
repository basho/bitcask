%% -------------------------------------------------------------------
%%
%% bitcask: Eric Brewer-inspired key/value store
%%
%% Copyright (c) 2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% innostore is free software: you can redistribute it and/or modify
%% it under the terms of the GNU General Public License as published by
%% the Free Software Foundation, either version 2 of the License, or
%% (at your option) any later version.
%%
%% innostore is distributed in the hope that it will be useful,
%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
%% GNU General Public License for more details.
%%
%% You should have received a copy of the GNU General Public License
%% along with bitcask.  If not, see <http://www.gnu.org/licenses/>.
%%
%% -------------------------------------------------------------------
-module(bitcask).

-export([open/1,
         get/2,
         put/3,
         delete/2]).

%% @type bc_state().
-record(bc_state, {dirname, filestate, keyhash}).

-define(TOMBSTONE, <<"bitcask_tombstone">>).

open(Dirname) ->

    % scan all files in Dirname to build up initial table
    % (build hash table pointing at those)
    % (use hintfiles when they exist)
    KeyHash = stub_bitcask_nif:build_new_keyhash(Dirname),
    % above is abstract, needs to walk files and also use keyhash nifs

    {ok, FS} = bitcask_fileops:open_new(Dirname),
    {ok, #bc_state{dirname=Dirname,filestate=FS,keyhash=KeyHash}}.

get(_State=#bc_state{dirname=Dirname,keyhash=KeyHash},
    Key) ->

    {Filename, Offset, Size} = stub_bitcask_nif:lookup_in_keyhash(KeyHash, Key),
    % todo: fix nif API use above, I just put in the idea here.
    % note: above might course also be notfound, check and report

    AbsName = filename:absname_join(Dirname,Filename),
    {ok, Key, Data} = bitcask_fileops:read(AbsName, Offset, Size),
    case Data of
        ?TOMBSTONE ->
            {error, notfound};
        _ ->
            {ok, Data}
    end.

put(State=#bc_state{dirname=Dirname,
                    filestate=FS,keyhash=KeyHash},
    Key,Value) ->
    {ok, Filename} = bitcask_fileops:filename(FS),
    {ok, NewFS, OffSet, Size} = bitcask_fileops:write(FS,Key,Value),
    stub_bitcask_nif:update(KeyHash, Key, Filename, OffSet, Size),
    FinalFS = case OffSet+Size > some_large_threshold of
        true ->
            bitcask_fileops:close(NewFS),
            {ok, FS1} = bitcask_fileops:open_new(Dirname),
            FS1;
        false ->
            NewFS
    end,
    {ok,State#bc_state{filestate=FinalFS}}.

delete(State, Key) -> put(State,Key,?TOMBSTONE).

