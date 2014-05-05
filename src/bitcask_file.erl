%% -------------------------------------------------------------------
%%
%% bitcask: Eric Brewer-inspired key/value store
%%
%% Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
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
-module(bitcask_file).
-compile(export_all).
-behaviour(gen_server).

-include_lib("eunit/include/eunit.hrl").

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

%% API

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {fd    :: file:fd(),
                owner :: pid()}).

%%%===================================================================
%%% API
%%%===================================================================

file_open(Filename, Opts) ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    Owner = self(),
    case gen_server:call(Pid, {file_open, Owner, Filename, Opts}, infinity) of
        ok ->
            {ok, Pid};
        Error ->
            Error
    end.

file_close(Pid) ->
    file_request(Pid, file_close).

file_sync(Pid) ->
    file_request(Pid, file_sync).

file_pread(Pid, Offset, Size) ->
    file_request(Pid, {file_pread, Offset, Size}).

file_pwrite(Pid, Offset, Bytes) ->
    file_request(Pid, {file_pwrite, Offset, Bytes}).

file_read(Pid, Size) ->
    file_request(Pid, {file_read, Size}).

file_write(Pid, Bytes) ->
    file_request(Pid, {file_write, Bytes}).

file_position(Pid, Position) ->
    file_request(Pid, {file_position, Position}).

file_seekbof(Pid) ->
    file_request(Pid, file_seekbof).

file_truncate(Pid) ->
    file_request(Pid, file_truncate).

%%%===================================================================
%%% API helper functions
%%%===================================================================

file_request(Pid, Request) ->
    case check_pid(Pid) of
        ok ->
            try
                gen_server:call(Pid, Request, infinity)
            catch
                exit:{normal,_} when Request == file_close ->
                    %% Honest race condition in bitcask_eqc PULSE test.
                    ok;
                exit:{noproc,_} when Request == file_close ->
                    %% Honest race condition in bitcask_eqc PULSE test.
                    ok;
                X1:X2 ->
                    exit({file_request_error, self(), Request, X1, X2})
            end;
        Error ->
            Error
    end.

check_pid(Pid) ->
    IsPid = is_pid(Pid),
    IsAlive = IsPid andalso is_process_alive(Pid),
    case {IsAlive, IsPid} of
        {true, _} ->
            ok;
        {false, true} ->
            %% Same result as `file' module when accessing closed FD
            {error, einval};
        _ ->
            %% Same result as `file' module when providing wrong arg
            {error, badarg}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({file_open, Owner, Filename, Opts}, _From, State) ->
    monitor(process, Owner),
    IsCreate = proplists:get_bool(create, Opts),
    IsReadOnly = proplists:get_bool(readonly, Opts),
    Mode = case {IsReadOnly, IsCreate} of
               {true, _} ->
                   [read, raw, binary];
               {_, false} ->
                   [read, write, raw, binary];
               {_, true} ->
                   [read, write, exclusive, raw, binary]
           end,
    _ = [error_logger:warning_msg("Bitcask file option '~p' not supported~n", [Opt])
     || Opt <- [o_sync],
        proplists:get_bool(Opt, Opts)],
    case file:open(Filename, Mode) of
        {ok, Fd} ->
            State2 = State#state{fd=Fd, owner=Owner},
            {reply, ok, State2};
        Error ->
            {reply, Error, State}
    end;
handle_call(file_close, From, State=#state{fd=Fd}) -> 
    check_owner(From, State),
    ok = file:close(Fd),
    {stop, normal, ok, State};
handle_call(file_sync, From, State=#state{fd=Fd}) ->
    check_owner(From, State),
    Reply = file:sync(Fd),
    {reply, Reply, State};
handle_call({file_pread, Offset, Size}, From, State=#state{fd=Fd}) ->
    check_owner(From, State),
    Reply = file:pread(Fd, Offset, Size),
    {reply, Reply, State};
handle_call({file_pwrite, Offset, Bytes}, From, State=#state{fd=Fd}) ->
    check_owner(From, State),
    Reply = file:pwrite(Fd, Offset, Bytes),
    {reply, Reply, State};
handle_call({file_read, Size}, From, State=#state{fd=Fd}) ->
    check_owner(From, State),
    Reply = file:read(Fd, Size),
    {reply, Reply, State};
handle_call({file_write, Bytes}, From, State=#state{fd=Fd}) ->
    check_owner(From, State),
    Reply = file:write(Fd, Bytes),
    {reply, Reply, State};
handle_call({file_position, Position}, From, State=#state{fd=Fd}) ->
    check_owner(From, State),
    Reply = file:position(Fd, Position),
    {reply, Reply, State};
handle_call(file_seekbof, From, State=#state{fd=Fd}) ->
    check_owner(From, State),
    {ok, _} = file:position(Fd, bof),
    {reply, ok, State};
handle_call(file_truncate, From, State=#state{fd=Fd}) ->
    check_owner(From, State),
    {reply, file:truncate(Fd), State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, _, _Pid, _Status}, State=#state{fd=Fd}) ->
    %% Owner has stopped, close file and shutdown
    _ = file:close(Fd),
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

check_owner({Pid, _Mref}, #state{owner=Owner}) ->
    case Pid == Owner of
        true ->
            ok;
        false ->
            throw(owner_invariant_failed),
            ok
    end.
