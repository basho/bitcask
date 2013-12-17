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
-compile(export_all).
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

change_open_regression_test_() ->
    {timeout, 300, ?_assertMatch({ok, _}, change_open_regression_body())}.

change_open_regression_body() ->
    Dir = "/tmp/bitcask.qc",
    os:cmd("rm -rf " ++ Dir),
    K1 = <<"K111">>,
    K2 = <<"K22222">>,
    K3 = <<"K33">>,
    K4 = <<"K4444">>,
    K5 = <<"K55">>,
    K6 = <<"K6666">>,
    K6_val1 = <<"K6aaaa">>,
    K6_val2 = <<"K6b">>,
    K7 = <<"k">>,
    _V1 = apply(bitcask_qc_fsm,set_keys,[[K1,K2,K3,K4,K5,K6]]),
    _V2 = apply(bitcask_qc_fsm,truncate_hint,[10,-14]),
    _V3 = apply(bitcask,open,[Dir,[read_write,{open_timeout,0},{sync_strategy,none}]]),
    _V4 = apply(bitcask,delete,[_V3,K3]),
    _V5 = apply(bitcask,merge,[Dir]),
    _V6 = apply(bitcask,delete,[_V3,K5]),
    _V7 = apply(bitcask,get,[_V3,K1]),
    _V8 = apply(bitcask,put,[_V3,K3,<<>>]),
    _V9 = apply(bitcask,put,[_V3,K6,K6_val1]),
    _V10 = apply(bitcask,put,[_V3,K2,<<"x10><">>]),
    _V30 = apply(bitcask,merge,[Dir]),
    _V31 = apply(bitcask,put,[_V3,K1,<<>>]),
    _V32 = apply(bitcask,merge,[Dir]),
    _V33 = apply(bitcask,put,[_V3,K1,<<"x33><">>]),
    _V34 = apply(bitcask,put,[_V3,K3,<<"34">>]),
    _V35 = apply(bitcask,merge,[Dir]),
    _V36 = apply(bitcask,close,[_V3]),
    _V37 = apply(bitcask_qc_fsm,create_stale_lock,[]),
    _V38 = apply(bitcask_qc_fsm,create_stale_lock,[]),
    _V50 = apply(bitcask,open,[Dir,[read_write,{open_timeout,0},{sync_strategy,none}]]),
    _V51 = apply(bitcask,put,[_V50,K7,<<"x51>>><">>]),
    _V52 = apply(bitcask,get,[_V50,K6]),
    _V53 = apply(bitcask,put,[_V50,K2,<<"x53">>]),
    _V54 = apply(bitcask,merge,[Dir]),
    _V55 = apply(bitcask,get,[_V50,K3]),
    _V56 = apply(bitcask,merge,[Dir]),
    _V57 = apply(bitcask,merge,[Dir]),
    _V58 = apply(bitcask,merge,[Dir]),
    _V59 = apply(bitcask,close,[_V50]),
    _V60 = apply(bitcask,open,[Dir,[read_write,{open_timeout,0},{sync_strategy,none}]]),
    _V61 = apply(bitcask,merge,[Dir]),
    _V62 = apply(bitcask,delete,[_V60,K4]),
    _V63 = apply(bitcask,get,[_V60,K2]),
    _V64 = apply(bitcask,get,[_V60,K2]),
    _V65 = apply(bitcask,put,[_V60,K7,<<"x65>><">>]),
    _V66 = apply(bitcask,merge,[Dir]),
    _V67 = apply(bitcask,delete,[_V60,K6]),
    _V68 = apply(bitcask,put,[_V60,K6,K6_val2]),
    _V69 = apply(bitcask,close,[_V60]),
    _V70 = apply(bitcask,open,[Dir,[read_write,{open_timeout,0},{sync_strategy,none}]]),
    _V71 = apply(bitcask,get,[_V70,K6]),
    case _V71 of
        {ok, K6_val1} ->
            {bummer, "Original EQC failure"};
        {ok, K6_val2} ->
            {ok, "this is eqc expected result hooray test passes"};
        Else ->
            {bummer, unexpected_failure, Else}
    end.

new_20131217_a_test_() ->
    {timeout, 300, ?_assertEqual(ok, new_20131217_a_body())}.

new_20131217_a_body() ->
    TestDir = token:get_name(),
    MOD = ?MODULE,
    V1 = <<"v">>,
    V2 = <<"v22">>,
    V3 = <<"v33">>,
    V1017_expected = [{1,<<"v33">>}, {2,<<"v33">>}, {3,<<"v33">>},
            {4,<<"v33">>}, {5,<<"v33">>}, {6,<<"v33">>},
            {7,<<"v33">>}, {8,<<"v33">>}, {9,<<"v33">>},
            {10,<<"v33">>}, {11,<<"v33">>}, {12,<<"v33">>},
            {13,<<"v33">>}, {14,<<"v33">>}, {15,<<"v33">>},
            {16,<<"v22">>}, {17,<<"v22">>}, {18,<<"v22">>},
            {19,<<"v22">>}, {20,<<"v22">>}, {21,<<"v22">>}],

    _Var1 = erlang:apply(MOD,incr_clock,[]),
    _Var2 = erlang:apply(MOD,bc_open,[TestDir]),
    _Var3 = erlang:apply(MOD,puts,[_Var2,{1,13},V1]),
    _Var10 = erlang:apply(MOD,delete,[_Var2,13]),
    not_found = get(_Var2, 13),                 %not from EQC
    _Var14 = erlang:apply(MOD,puts,[_Var2,{1,21},V2]),
    {ok, V2} = get(_Var2, 13),                  %not from EQC
    _Var18 = erlang:apply(MOD,puts,[_Var2,{1,15},V3]),
    {ok, V3} = get(_Var2, 13),                  %not from EQC
    timer:sleep(1234),                  %not from EQC
    {ok, V3} = get(_Var2, 13),                  %not from EQC
    _Var24 = erlang:apply(MOD,fork_merge,[_Var2, TestDir]),
    {ok, V3} = get(_Var2, 13),                  %not from EQC
    timer:sleep(1235),                  %not from EQC
    {ok, V3} = get(_Var2, 13),                  %not from EQC
    _Var27 = erlang:apply(MOD,bc_close,[_Var2]),
    _Var28 = erlang:apply(MOD,incr_clock,[]),
    _Var106 = erlang:apply(MOD,bc_open,[TestDir]),
    {ok, V3} = get(_Var106, 13),                  %not from EQC
    _Var1017 = erlang:apply(MOD,fold,[_Var106]),
    {ok, V3} = get(_Var106, 13),                  %not from EQC
    ?assertEqual(V1017_expected, lists:sort(_Var1017)),
    os:cmd("rm -rf " ++ TestDir),
    ok.

-define(NUM_KEYS, 50).
-define(FILE_SIZE, 1000).

bc_open(Dir) ->
    bitcask:open(Dir, [read_write, {max_file_size, ?FILE_SIZE}, {open_timeout, 1234}]).

nice_key(K) ->
    list_to_binary(io_lib:format("kk~2.2.0w", [K])).

un_nice_key(<<"kk", Num:2/binary>>) ->
    list_to_integer(binary_to_list(Num)).

get(H, K) ->
  bitcask:get(H, nice_key(K)).

put(H, K, V) ->
  ok = bitcask:put(H, nice_key(K), V).

puts(H, {K1, K2}, V) ->
  case lists:usort([ put(H, K, V) || K <- lists:seq(K1, K2) ]) of
    [ok]  -> ok;
    Other -> Other
  end.

delete(H, K) ->
  ok = bitcask:delete(H, nice_key(K)).

fork_merge(H, Dir) ->
  case bitcask:needs_merge(H) of
    {true, Files} -> catch bitcask_merge_worker:merge(Dir, [], Files);
    false         -> not_needed
  end.

incr_clock() ->
    bitcask_time:test__incr_fudge(1).

bc_close(H)    ->
  ok = bitcask:close(H).

fold_keys(H) ->
  bitcask:fold_keys(H, fun(#bitcask_entry{key = KBin}, Ks) -> [un_nice_key(KBin)|Ks] end, []).

fold(H) ->
  bitcask:fold(H, fun(KBin, V, Acc) -> [{un_nice_key(KBin),V}|Acc] end, []).

%% 37> io:format("~w.\n", [C76]).
%% [[{set,{var,1},{call,bitcask_pulse,incr_clock,[]}},{set,{var,2},{call,bitcask_pulse,bc_open,[true]}},{set,{var,3},{call,bitcask_pulse,puts,[{var,2},{1,13},<<0>>]}},{set,{var,10},{call,bitcask_pulse,delete,[{var,2},13]}},{set,{var,14},{call,bitcask_pulse,puts,[{var,2},{1,21},<<0,0,0>>]}},{set,{var,18},{call,bitcask_pulse,puts,[{var,2},{1,15},<<0,0,0>>]}},{set,{var,24},{call,bitcask_pulse,fork_merge,[{var,2}]}},{set,{var,27},{call,bitcask_pulse,bc_close,[{var,2}]}},{set,{var,28},{call,bitcask_pulse,incr_clock,[]}},{set,{var,40},{call,bitcask_pulse,fork,[[{init,{state,undefined,false,false,[]}},{set,{not_var,6},{not_call,bitcask_pulse,bc_open,[false]}},{set,{not_var,17},{not_call,bitcask_pulse,fold,[{not_var,6}]}}]]}}],{99742,1075,90258},[{events,[]}]].

-endif. %% TEST
