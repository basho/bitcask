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
%         put/3,
         delete/2]).

-include("bitcask.hrl").

%% @type bc_state().
-record(bc_state, {dirname,
                   files,    % List of #filestate
                   keydir}). % Key directory

-define(TOMBSTONE, <<"bitcask_tombstone">>).


open(Dirname) ->
    %% Make sure the directory exists
    ok = filelib:ensure_dir(filename:join(Dirname, "bitcask")),

    %% Build a list of all the bitcask data files and sort it in
    %% descending order (newest->oldest)
    Files = lists:reverse(lists:sort(filelib:wildcard("bitcask.[0-9]+.data"))),

    %% Setup a keydir and scan all the data files into it
    {ok, KeyDir} = bitcask_nifs:keydir_new(),
    Files = scan_key_files(Files, KeyDir),
    {ok, #bc_state{ dirname = Dirname, files = Files, keydir = KeyDir }}.

get(#bc_state{keydir = KeyDir} = State, Key) ->
    case bitcask_nifs:keydir_get(KeyDir, Key) of
        not_found ->
            {not_found, State};

        E when is_record(E, bitcask_entry) ->
            {Filestate, State2} = get_filestate(E#bitcask_entry.file_id, State),
            case bitcask_fileops:read(Filestate, E#bitcask_entry.value_pos,
                                      E#bitcask_entry.value_sz) of
                {ok, _Key, ?TOMBSTONE} ->
                    {not_found, State};
                {ok, _Key, Value} ->
                    %% TODO: We don't actually USE the key...do we really need to even
                    %% read it? Or does TOMBSTONE play into this?
                    {ok, Value, State}
            end;

        {error, Reason} ->
            {error, Reason}
    end.

% put(State=#bc_state{dirname=Dirname,
%                     filestate=FS,keyhash=KeyHash},
%     Key,Value) ->
%     {ok, Filename} = bitcask_fileops:filename(FS),
%     {ok, NewFS, OffSet, Size} = bitcask_fileops:write(FS,Key,Value),
%     stub_bitcask_nif:update(KeyHash, Key, Filename, OffSet, Size),
%     FinalFS = case OffSet+Size > some_large_threshold of
%         true ->
%             bitcask_fileops:close(NewFS),
%             {ok, FS1} = bitcask_fileops:open_new(Dirname),
%             FS1;
%         false ->
%             NewFS
%     end,
%     {ok,State#bc_state{filestate=FinalFS}}.

delete(State, Key) ->
    %put(State,Key,?TOMBSTONE).
    ok.


%% ===================================================================
%% Internal functions
%% ===================================================================

scan_key_files([], KeyDir) ->
    ok;
scan_key_files([Filename | Rest], KeyDir) ->
    scan_key_files(Rest, KeyDir).


get_filestate(FileId, #bc_state{ dirname = Dirname, files = Files } = State) ->
    Fname = bitcask_fileops:filename(Dirname, FileId),
    case lists:keysearch(Fname, #filestate.filename, Files) of
        {value, Filestate} ->
            {Filestate, State};
        false ->
            {ok, Filestate} = bitcask_fileops:open_file(Fname),
            {Filestate, State#bc_state { files = [Filestate | State#bc_state.files] }}
    end.
