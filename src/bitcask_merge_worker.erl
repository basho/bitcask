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
-module(bitcask_merge_worker).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("bitcask.hrl").

-record(state, {}).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ====================================================================
%% Internal functions
%% ====================================================================

-record(mstate, { dirname,
                  max_file_size,
                  input_files,
                  out_file,
                  all_keydir,
                  hint_keydir,
                  del_keydir }).

%% @doc Merge several data files within a bitcask datastore into a more compact form.
-spec merge(Dirname::string()) -> ok | {error, any()}.
merge(Dirname) ->
    %% Try to lock for merging
    case bitcask_lockops:acquire(merge, Dirname) of
        true -> ok;
        false -> throw({error, merge_locked})
    end,

    %% Setup our first output merge file and update the merge lock accordingly
    {ok, Outfile} = bitcask_fileops:create_file(Dirname),
    ok = bitcask_lockops:update(merge, Dirname,
                                bitcask_fileops:filename(Outfile)),

    %% Initialize all the keydirs we need. The hint keydir will get recreated
    %% each time we wrap a file, so that it only contains keys associated
    %% with the current out_file
    {ok, AllKeyDir} = bitcask_nifs:keydir_new(),
    {ok, HintKeyDir} = bitcask_nifs:keydir_new(),
    {ok, DelKeyDir} = bitcask_nifs:keydir_new(),

    %% TODO: Pull max file size from config

    %% Initialize our state for the merge
    State = #mstate { dirname = Dirname,
                      max_file_size = 16#80000000, % 2GB default
                      input_files = bitcask_client:readable_files(Dirname),
                      out_file = Outfile,
                      all_keydir = AllKeyDir,
                      hint_keydir = HintKeyDir,
                      del_keydir = DelKeyDir },

    %% Finally, start the merge process
    State1 = merge_files(State),

    %% Make sure to close the final output file
    close_outfile(State1),

    %% Cleanup the original input files and release our lock
    [ok = bitcask_fileops:close(F) || F <- State#mstate.input_files],
    [ok = bitcask_fileops:delete(F) || F <- State#mstate.input_files],
    ok = bitcask_lockops:release(merge, Dirname),
    ok.


merge_files(#mstate { input_files = [] } = State) ->
    State;
merge_files(#mstate { input_files = [Filename | Rest]} = State) ->
    %% Open the next file
    {ok, File} = bitcask_fileops:open_file(Filename),
    F = fun(K, V, Tstamp, _Pos, State0) ->
                merge_single_entry(K, V, Tstamp, State0)
        end,
    State1 = bitcask_fileops:fold(File, F, State),
    ok = bitcask_filops:close(File),
    merge_files(State1#mstate { input_files = Rest }).

merge_single_entry(K, V, Tstamp, #mstate { dirname = Dirname } = State) ->
    case out_of_date(K, Tstamp, [State#mstate.all_keydir,
                                 State#mstate.del_keydir]) of
        true ->
            State;

        false ->
            case (V =:= ?TOMBSTONE) of
                true ->
                    ok = bitcask_nifs:keydir_put(State#mstate.del_keydir, K,
                                                 Tstamp, 0, 0, Tstamp),
                    State;
                false ->
                    ok = bitcask_nifs:keydir_remove(State#mstate.del_keydir, K),

                    %% See if it's time to rotate to the next file
                    State1 =
                        case bitcask_fileops:check_write(State#mstate.out_file,
                                                         K, V, State#mstate.max_file_size) of
                            wrap ->
                                %% Close the current output file
                                close_outfile(State),

                                %% Start our next file and update state
                                {ok, NewFile} = bitcask_fileops:create_file(Dirname),
                                {ok, HintKeyDir} = bitcask_nifs:keydir_new(),
                                State#mstate { out_file = NewFile,
                                               hint_keydir = HintKeyDir };
                            ok ->
                                State
                        end,

                    {ok, Outfile, OffSet, Size} = bitcask_fileops:write(State1#mstate.out_file,
                                                                        K, V, Tstamp),
                    %% Update the keydir for the current out file
                    ok = bitcask_nifs:keydir_put(State#mstate.hint_keydir, K,
                                                 bitcask_fileops:file_tstamp(Outfile),
                                                 Size, OffSet, Tstamp),
                    %% Update the keydir of all keys we've seen over this merge
                    ok = bitcask_nifs:keydir_put(State#mstate.all_keydir, K,
                                                 bitcask_fileops:file_tstamp(Outfile),
                                                 Size, OffSet, Tstamp),
                    State1#mstate { out_file = Outfile }
            end
    end.

close_outfile(State) ->
    %% Close the current output file
    ok = bitcask_fileops:sync(State#mstate.out_file),
    ok = bitcask_fileops:close(State#mstate.out_file),

    %% TODO: Dump the hint keydir to disk here

    %% Now iterate over the hint keydir (i.e. those keys
    %% we've processed in just this file) and make sure
    %% that the active client keydir has these updates.
    case bitcask_client_mgr:get_client(State#mstate.dirname) of
        {ok, Pid} ->
            %% There is an active writer client for this directory, send our
            %% updates to it
            Itr = bitcask_nifs:keydir_itr(State#mstate.hint_keydir),
            case update_client_keydir(Pid, bitcask_nifs:keydir_itr_next(Itr)) of
                ok ->
                    ok;
                {error, Reason} ->
                    error_logger:error_msg("Failed to merge ~s with active client: ~p\n",
                                           [bitcask_fileops:filename(State#mstate.out_file),
                                            Reason])
            end;
        _ ->
            %% Nothing to do. When/if a client starts up, it can use the hint
            %% file for this file.
            ok
    end.


update_client_keydir(Pid, not_found) ->
    ok;
update_client_keydir(Pid, {Curr, Next}) ->
    case (catch bitcask_client:update_keydir(Pid, Curr)) of
        ok ->
            update_client_keydir(Pid, bitcask_nifs:keydir_itr_next(Next));
        {'EXIT', Reason} ->
            {error, Reason}
    end.

out_of_date(_Key, _Tstamp, []) ->
    false;
out_of_date(Key, Tstamp, [KeyDir|Rest]) ->
    case bitcask_nifs:keydir_get(KeyDir, Key) of
        not_found ->
            out_of_date(Key, Tstamp, Rest);

        E when is_record(E, bitcask_entry) ->
            case Tstamp < E#bitcask_entry.tstamp of
                true  -> true;
                false -> out_of_date(Key, Tstamp, Rest)
            end;

        {error, Reason} ->
            {error, Reason}
    end.


% make_hintfiles([]) -> ok;
% make_hintfiles([{DataFile,KeyDir}|Rest]) ->
%     %% todo: write this then modify scan_key_files to
%     %%       use hints when present
%    %fold over HintKeyDir, writing each
%    %{KeySize,Key,(Timestamp,MergeFile,Offset,Size)}
%    %in the tail of HintFile
%    % "TSTAMP.bitcask.hint.merging"
%    % "TSTAMP.bitcask.hint"
%     ok.
