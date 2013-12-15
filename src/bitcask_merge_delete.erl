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
-module(bitcask_merge_delete).

-behaviour(gen_server).

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

%% API
-export([start_link/0, defer_delete/3, queue_length/0]).
-export([testonly__delete_trigger/0]).                      % testing only

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("bitcask.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE). 
-define(TIMEOUT, 1000).

-record(state, {q :: queue()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

defer_delete(Dirname, IterGeneration, Files) ->
    gen_server:call(?SERVER, {defer_delete, Dirname, IterGeneration, Files},
                    infinity).

queue_length() ->
    gen_server:call(?SERVER, {queue_length}, infinity).

testonly__delete_trigger() ->
    gen_server:call(?SERVER, {testonly__delete_trigger}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{q = queue:new()}, ?TIMEOUT}.

handle_call({defer_delete, Dirname, IterGeneration, Files}, _From, State) ->
    {reply, ok, State#state{q = queue:in({Dirname, IterGeneration, Files},
                                         State#state.q)}, ?TIMEOUT};
handle_call({queue_length}, _From, State) ->
    {reply, queue:len(State#state.q), State, ?TIMEOUT};
handle_call({testonly__delete_trigger}, _From, State) ->
    {reply, ok, check_status(State), ?TIMEOUT};
handle_call(_Request, _From, State) ->
    Reply = unknown_request,
    {reply, Reply, State, ?TIMEOUT}.

handle_cast(_Msg, State) ->
    {noreply, State, ?TIMEOUT}.

handle_info(timeout, State) ->
    {noreply, check_status(State), ?TIMEOUT};
handle_info(_Info, State) ->
    {noreply, State, ?TIMEOUT}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

check_status(S) ->
    case queue:out(S#state.q) of
        {empty, _} ->
            S;
        {{value, {Dirname, IterGeneration, Files}}, NewQ} ->
            {_, KeyDir} = bitcask_nifs:keydir_new(Dirname),
            try

                {_,_,_,IterStatus} = bitcask_nifs:keydir_info(KeyDir),
                CleanAndGo = fun() ->
                                     delete_files(Files),
                                     bitcask_nifs:keydir_release(KeyDir),
                                     check_status(S#state{q = NewQ})
                             end,
                case IterStatus of
                    {_, _, false} ->
                        CleanAndGo();
                    {CurGen, _, true} when CurGen > IterGeneration ->
                        CleanAndGo();
                    _ ->
                        %% Do nothing, ignore NewQ
                        S
                end
            catch _X:_Y ->
                    %% Not sure what problem was: keydir is no longer
                    %% valid, or a problem deleting files, but in any
                    %% case we're going to wash our hands of the matter and
                    %% let the next merge clean up.
                    S#state{q = NewQ}
            after
                catch bitcask_nifs:keydir_release(KeyDir)
            end
    end.

delete_files(Files) ->
    [bitcask_fileops:delete(#filestate{filename = F}) || F <- Files].

-ifdef(TEST).

multiple_merges_during_fold_test_() ->
    {timeout, 60, fun multiple_merges_during_fold_test_body/0}.

multiple_merges_during_fold_test_body() ->
    Dir = "/tmp/bc.multiple-merges-fold",
    B = bitcask:open(Dir, [read_write, {max_file_size, 50}]),
    PutSome = fun() ->
                      [bitcask:put(B, <<X:32>>, <<"yo this is a value">>) ||
                          X <- lists:seq(1,5)]
              end,
    PutSome(),
    PutSome(),
    Bstuff = get(B),
    FoldFun = fun(_K, _V, 0) ->
                      receive go_ahead -> ok end,
                      1;
                 (_K, _V, 1) ->
                      1
              end,
    SlowPid = spawn(fun() ->
                            put(B, Bstuff),
                            bitcask:fold(B, FoldFun, 0)
                    end),
    CountSetuids = fun() ->
                           Fs = filelib:wildcard(Dir ++ "/*"),
                           length([F || F <- Fs,
                                        bitcask:has_setuid_bit(F)])
                   end,
    PutSome(),
    Count1 = merge_until(Dir, 0, CountSetuids),
    PutSome(),
    bitcask:merge(Dir),
    PutSome(),
    merge_until(Dir, Count1, CountSetuids),
    
    SlowPid ! go_ahead,
    timer:sleep(500),
    ok = ?MODULE:testonly__delete_trigger(),
    0 = CountSetuids(),
    
    ok.

merge_until(Dir, MinCount, CountSetuids) ->
    bitcask:merge(Dir),
    Count = CountSetuids(),
    if (Count > MinCount) ->
            Count;
       true ->
            timer:sleep(100),
            merge_until(Dir, MinCount, CountSetuids)
    end.
    
regression_gh82_test_() ->
    {timeout, 300, ?_assertEqual(ok, regression_gh82_body())}.

regression_gh82_body() ->
    Dir = "/tmp/bc.regression_gh82",

    os:cmd("rm -rf " ++ Dir),
    Reference = bitcask:open(Dir, [read_write | regression_gh82_opts()]),
    bitcask:put(Reference, <<"key_to_delete">>, <<"tr0ll">>),
    [ bitcask:put(Reference, term_to_binary(X), <<1:(8 * 1024 * 100)>>) || X <- lists:seq(1, 3000)],
    bitcask:delete(Reference, <<"key_to_delete">>),
    [ bitcask:put(Reference, term_to_binary(X), <<1:(8 * 1024 * 100)>>) || X <- lists:seq(1, 3000)],
    timer:sleep(1000 + 1000),
    bitcask_merge_worker:merge(Dir, regression_gh82_opts(), {[Dir ++ "/2.bitcask.data"], []}),
    poll_merge_worker(),
    timer:sleep(2*1000),
    bitcask:close(Reference),

    Reference2 = bitcask:open(Dir, [read_write | regression_gh82_opts()]),
    not_found = bitcask:get(Reference2, <<"key_to_delete">>),
    ok.

regression_gh82_opts() ->
    [{max_file_size, 268435456},
     {dead_bytes_threshold, 89478485},
     {dead_bytes_merge_trigger, 178956970}].

poll_merge_worker() ->
    case bitcask_merge_worker:status() of
        {0, undefined} ->
            ok;
        _ ->
            timer:sleep(100),
            poll_merge_worker()
    end.

-endif. %% TEST
