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

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0,
         merge/1, merge/2, merge/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { queue,
                 worker}).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

merge(Dir) ->
    gen_server:call(?MODULE, {merge, [Dir]}, infinity).

merge(Dir, Opts) ->
    gen_server:call(?MODULE, {merge, [Dir, Opts]}, infinity).

merge(Dir, Opts, Files) ->
    gen_server:call(?MODULE, {merge, [Dir, Opts, Files]}, infinity).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Trap exits of the actual worker process
    process_flag(trap_exit, true),

    %% Use a dedicated worker sub-process to do the actual merging. The process
    %% may ignore messages for a long while during the merge and we want to
    %% ensure that our message queue doesn't fill up with a bunch of dup
    %% requests for the same directory.
    %%
    %% The sub-process is created per-merge request to ensure that any
    %% ports/file handles opened during the merge get properly cleaned up, even
    %% in error cases.
    {ok, #state{ queue = queue:new() }}.

handle_call({merge, Args}, _From, #state { queue = Q } = State) ->
    case queue:member(Args, Q) of
        true ->
            {reply, already_queued, State};
        false ->
            case State#state.worker of
                undefined ->
                    WorkerPid = spawn_link(fun() -> do_merge(Args) end),
                    {reply, ok, State#state { worker = WorkerPid }};
                _ ->
                    {reply, ok, State#state { queue = queue:in(Args, Q) }}
            end
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({'EXIT', _Pid, normal}, #state { queue = Q } = State) ->
    case queue:is_empty(Q) of
        true ->
            {noreply, State#state { worker = undefined }};
        false ->
            {{value, Args}, Q2} = queue:out(Q),
            WorkerPid = spawn_link(fun() -> do_merge(Args) end),
            {noreply, State#state { queue = Q2,
                                    worker = WorkerPid }}
    end;

handle_info({'EXIT', Pid, Reason}, #state { worker = Pid } = State) ->
    error_logger:error_msg("Merge worker PID exited: ~p\n", [Reason]),
    {stop, State}.

terminate(_Reason, State) ->
    catch exit(State#state.worker, shutdown),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ====================================================================
%% Internal worker
%% ====================================================================

do_merge(Args) ->
    {_, {Hour, _, _}} = calendar:local_time(),
    case in_merge_window(Hour, merge_window()) of
        true ->
            Start = os:timestamp(),
            Result = (catch apply(bitcask, merge, Args)),
            ElapsedSecs = timer:now_diff(os:timestamp(), Start) / 1000000,
            [_,_,Args3] = Args,
            case Result of
                ok ->
                    error_logger:info_msg("Merged ~p in ~p seconds.\n",
                                          [Args3, ElapsedSecs]);
                {Error, Reason} when Error == error; Error == 'EXIT' ->
                    error_logger:error_msg("Failed to merge ~p: ~p\n",
                                           [Args3, Reason])
            end;
        false ->
            ok
    end.

merge_window() ->
    case application:get_env(bitcask, merge_window) of
        {ok, always} ->
            always;
        {ok, never} ->
            never;
        {ok, {StartHour, EndHour}} when StartHour >= 0, StartHour =< 23,
                                        EndHour >= 0, EndHour =< 23 ->
            {StartHour, EndHour};
        Other ->
            error_logger:error_msg("Invalid bitcask_merge window specified: ~p. "
                                   "Defaulting to 'always'.\n", [Other]),
            always
    end.

in_merge_window(_NowHour, always) ->
    true;
in_merge_window(_NowHour, never) ->
    false;
in_merge_window(NowHour, {Start, End}) when Start =< End ->
    (NowHour >= Start) and (NowHour =< End);
in_merge_window(NowHour, {Start, End}) when Start > End ->
    (NowHour >= Start) or (NowHour =< End).


%% ====================================================================
%% Unit tests
%% ====================================================================

-ifdef(EQC).

prop_in_window() ->
    ?FORALL({NowHour, WindowLen, StartTime}, {choose(0, 23), choose(0, 23), choose(0, 23)},
            begin
                EndTime = (StartTime + WindowLen) rem 24,

                %% Generate a set of all hours within this window
                WindowHours = [H rem 24 || H <- lists:seq(StartTime, StartTime + WindowLen)],

                %% If NowHour is in the set of windows hours, we expect our function
                %% to indicate that we are in the window
                ExpInWindow = lists:member(NowHour, WindowHours),
                ?assertEqual(ExpInWindow, in_merge_window(NowHour, {StartTime, EndTime})),
                true
            end).

prop_in_window_test() ->
    ?assert(eqc:quickcheck(prop_in_window())).


-endif.
