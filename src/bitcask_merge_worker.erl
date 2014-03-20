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
         merge/1, merge/2, merge/3,
         status/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { queue :: list(),
                worker :: undefined | pid()}).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

merge(Dir) ->
    merge(Dir, []).

merge(Dir, Opts) ->
    gen_server:call(?MODULE, {merge, [Dir, Opts]}, infinity).

merge(Dir, Opts, Files) ->
    gen_server:call(?MODULE, {merge, [Dir, Opts, Files]}, infinity).

status() ->
    gen_server:call(?MODULE, {status}, infinity).

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
    {ok, #state{ queue = [] }}.

handle_call({merge, Args0}, _From, #state { queue = Q } = State) ->
    [Dirname|_] = Args0,
    Args1 = 
        case length(Args0) of
            3 ->
                %% presort opts and files tuples for better matches
                %% and less work later
                [_, Opts0, Tuple0] = Args0,
                {Files, Expired} = Tuple0,
                Opts = lists:usort(Opts0),
                Tuple = {lists:usort(Files), 
                         lists:usort(Expired)},
                [Dirname, Opts, Tuple];
            _ ->
                %% whole directory don't need to be sorted
                Args0
        end,
    Args = list_to_tuple(Args1),
    %% convert back and forth from tuples to lists so we can use
    %% keyfind
    case lists:keyfind(Dirname, 1, Q) of
        Args ->
            {reply, already_queued, State};
        Partial when is_tuple(Partial) ->
            New = merge_items(Args, Partial),
            Q1 = lists:keyreplace(Dirname, 1, Q, New),
            {reply, ok, State#state{ queue = Q1 }};
        false ->
            case State#state.worker of
                undefined ->
                    WorkerPid = spawn_link(fun() -> do_merge(Args0) end),
                    {reply, ok, State#state { worker = WorkerPid }};
                _ ->
                    {reply, ok, State#state { queue = Q ++ [Args] }}
            end
    end;
handle_call({status}, _From, #state { queue = Q, worker = Worker } = State) ->
    {reply, {length(Q), Worker}, State};
handle_call(_, _From, State) ->
    {reply, unknown_call, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({'EXIT', _Pid, normal}, #state { queue = Q } = State) ->
    case Q of
        [] ->
            {noreply, State#state { worker = undefined }};
        [Args0|Q2] ->
            Args = tuple_to_list(Args0),
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
%% private functions
%% ====================================================================

merge_items(New, Old) ->
    %% first element will always match
    Dirname = element(1, New), 
    %% old args are already sorted
    OOpts = element(2, Old),
    NOpts = lists:usort(element(2, New)),
    Opts = lists:umerge(OOpts, NOpts),
    case {size(New), size(Old)} of
        {3, 3} ->
            Files = merge_files(element(3, New),
                                element(3, Old)),
            {Dirname, Opts, Files};
        {2, 2} ->
            {Dirname, Opts};
        {2, 3} ->
            {Dirname, Opts, element(3, Old)};
        {3, 2} ->
           {Dirname, Opts, element(3, New)}
    end.

merge_files(New, Old) ->
    {NFiles, NExp} = New,
    {OFiles, OExp} = Old,
    %% old files and expired lists are already sorted
    Files0 = lists:umerge(lists:usort(NFiles), OFiles),
    Expired = lists:umerge(lists:usort(NExp), OExp),
    Files = Files0 -- Expired,
    {Files, Expired}.

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

prop_in_window_test_() ->
    {timeout, 30,
     [fun() -> ?assert(eqc:quickcheck(prop_in_window())) end]}.


-endif.
