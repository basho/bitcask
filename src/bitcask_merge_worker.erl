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
-author('Dave Smith <dizzyd@basho.com>').
-author('Justin Sheehy <justin@basho.com>').

-behaviour(gen_server).

%% API
-export([start_link/0,
         merge/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { queue,
                 worker,
                 worker_ready = false}).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

merge(Dir) ->
    gen_server:call(?MODULE, {merge, Dir}).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Trap exits of the actual worker process
    process_flag(trap_exit, true),

    %% Use a dedicated worker sub-process to do the actual merging. The
    %% process may ignore messages for a long while during the merge
    %% and we want to ensure that our message queue doesn't fill up with
    %% a bunch of dup requests for the same directory.
    Self = self(),
    WorkerPid = spawn_link(fun() -> worker_loop(Self) end),
    {ok, #state{ queue = queue:new(),
                 worker = WorkerPid }}.

handle_call({merge, Dir}, _From, #state { queue = Q } = State) ->
    case queue:member(Dir, Q) of
        true ->
            {reply, already_queued, State};
        false ->
            case State#state.worker_ready of
                true ->
                    State#state.worker ! {merge, Dir},
                    {reply, ok, State};
                false ->
                    {reply, ok, State#state { queue = queue:in(Dir, Q) }}
            end
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(worker_ready, #state { queue = Q } = State) ->
    case queue:is_empty(Q) of
        true ->
            {noreply, State#state { worker_ready = true }};
        false ->
            {{value, Dir}, Q2} = queue:out(Q),
            State#state.worker ! {merge, Dir},
            {noreply, State#state { queue = Q2,
                                    worker_ready = false }}
    end;
handle_info({'EXIT', Pid, Reason}, #state { worker = Pid } = State) ->
    error_logger:error_msg("Merge worker PID exited: ~p\n", [Reason]),
    {stop, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ====================================================================
%% Internal worker
%% ====================================================================

worker_loop(Parent) ->
    Parent ! worker_ready,
    receive
        {merge, Dir} ->
            Start = now(),
            Result = (catch bitcask:merge(Dir)),
            ElapsedSecs = timer:now_diff(now(), Start) / 1000000,
            case Result of
                ok ->
                    error_logger:info_msg("Merged ~s in ~p seconds.\n",
                                          [Dir, ElapsedSecs]);
                {Error, Reason} when Error == error; Error == 'EXIT' ->
                    error_logger:error_msg("Failed to merge ~s: ~p\n",
                                           [Dir, Reason])
            end,
            worker_loop(Parent)
    end.

