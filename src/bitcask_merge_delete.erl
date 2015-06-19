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

-ifdef(namespaced_types).
-type merge_queue() :: queue:queue().
-else.
-type merge_queue() :: queue().
-endif.

-record(state, {q :: merge_queue()}).

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
    {reply, ok, delete_ready_files(State), ?TIMEOUT};
handle_call(_Request, _From, State) ->
    Reply = unknown_request,
    {reply, Reply, State, ?TIMEOUT}.

handle_cast(_Msg, State) ->
    {noreply, State, ?TIMEOUT}.

handle_info(timeout, State) ->
    {noreply, delete_ready_files(State), ?TIMEOUT};
handle_info(_Info, State) ->
    {noreply, State, ?TIMEOUT}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

delete_ready_files(S) ->
    delete_ready_files(S, queue:new()).

delete_ready_files(S, PendingQ) ->
    case queue:out(S#state.q) of
        {empty, _} ->
            S#state{q = PendingQ};
        {{value, {Dirname, IterGeneration, Files} = Entry}, NewQ} ->
            {_, KeyDir} = bitcask_nifs:keydir_new(Dirname),
            try

                {_,_,_,IterStatus,_} = bitcask_nifs:keydir_info(KeyDir),
                ReadyToDelete =
                    case IterStatus of
                        {_, _, false, _} ->
                            true;
                        {CurGen, _, true, _} when CurGen > IterGeneration ->
                            true;
                        _ ->
                            false
                    end,
                S2 = S#state{q = NewQ},
                case ReadyToDelete of
                    true ->
                        delete_files(Files),
                        bitcask_nifs:keydir_release(KeyDir),
                        delete_ready_files(S2, PendingQ);
                    false ->
                        delete_ready_files(S2, queue:in(Entry, PendingQ))
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
    _ = [bitcask_fileops:delete(#filestate{filename = F}) || F <- Files],
    ok.

-ifdef(TEST).

multiple_merges_during_fold_test_() ->
    {timeout, 60, fun multiple_merges_during_fold_body/0}.

multiple_merges_during_fold_body() ->
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
                                        bitcask:has_pending_delete_bit(F)])
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
    %% _V1 = apply(bitcask_qc_fsm,set_keys,[[K1,K2,K3,K4,K5,K6]]),
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

%% 37> io:format("~w.\n", [C76]).
%% [[{set,{var,1},{call,bitcask_pulse,incr_clock,[]}},{set,{var,2},{call,bitcask_pulse,bc_open,[true]}},{set,{var,3},{call,bitcask_pulse,puts,[{var,2},{1,13},<<0>>]}},{set,{var,10},{call,bitcask_pulse,delete,[{var,2},13]}},{set,{var,14},{call,bitcask_pulse,puts,[{var,2},{1,21},<<0,0,0>>]}},{set,{var,18},{call,bitcask_pulse,puts,[{var,2},{1,15},<<0,0,0>>]}},{set,{var,24},{call,bitcask_pulse,fork_merge,[{var,2}]}},{set,{var,27},{call,bitcask_pulse,bc_close,[{var,2}]}},{set,{var,28},{call,bitcask_pulse,incr_clock,[]}},{set,{var,40},{call,bitcask_pulse,fork,[[{init,{state,undefined,false,false,[]}},{set,{not_var,6},{not_call,bitcask_pulse,bc_open,[false]}},{set,{not_var,17},{not_call,bitcask_pulse,fold,[{not_var,6}]}}]]}}],{99742,1075,90258},[{events,[]}]].

new_20131217_a_body() ->
    catch token:stop(),
    TestDir = token:get_name(),
    bitcask_time:test__set_fudge(10),
    MOD = ?MODULE,
    MFS = 1000,
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
    _Var2 = erlang:apply(MOD,bc_open,[TestDir,MFS]),
    _Var3 = erlang:apply(MOD,puts,[_Var2,{1,13},V1]),
    _Var10 = erlang:apply(MOD,delete,[_Var2,13]),
    not_found = get(_Var2, 13),                 %not from EQC
    _Var14 = erlang:apply(MOD,puts,[_Var2,{1,21},V2]),
    {ok, V2} = get(_Var2, 13),                  %not from EQC
    _Var18 = erlang:apply(MOD,puts,[_Var2,{1,15},V3]),
    {ok, V3} = get(_Var2, 13),                  %not from EQC
    timer:sleep(1234),                  %not from EQC
    _Var24 = erlang:apply(MOD,fork_merge,[_Var2, TestDir]),
    timer:sleep(1235),                  %not from EQC
    {ok, V3} = get(_Var2, 13),                  %not from EQC
    {ok, V3} = get(_Var2, 13),                  %not from EQC
    _Var27 = erlang:apply(MOD,bc_close,[_Var2]),
    _Var28 = erlang:apply(MOD,incr_clock,[]),
    _Var106 = erlang:apply(MOD,bc_open,[TestDir,MFS]),
    {ok, V3} = get(_Var106, 13),                  %not from EQC
    _Var1017 = erlang:apply(MOD,fold,[_Var106]),
    {ok, V3} = get(_Var106, 13),                  %not from EQC
    bc_close(_Var106),
    bitcask_time:test__clear_fudge(),
    ?assertEqual(V1017_expected, lists:sort(_Var1017)),
    ok.

new_20131217_c_test_() ->
    {timeout, 300, ?_assertEqual(ok, new_20131217_c_body())}.

new_20131217_c_body() ->
    catch token:stop(),
    TestDir = token:get_name(),
    bitcask_time:test__set_fudge(10),
    MOD = ?MODULE,
    MFS = 1000,
    Val1 = <<"vv">>,
    Val2 = <<"V">>,

    _Var15 = erlang:apply(MOD,bc_open,[TestDir,MFS]),
    _Var17 = erlang:apply(MOD,puts,[_Var15,{1,4},Val1]),
    _Var21 = erlang:apply(MOD,bc_close,[_Var15]),
    _Var22 = erlang:apply(MOD,incr_clock,[]),
    _Var23 = erlang:apply(MOD,bc_open,[TestDir,MFS]),
    _Var24 = erlang:apply(MOD,puts,[_Var23,{1,3},Val2]),
    timer:sleep(1234),          % Sleeps necessary for 100% determinism, alas
    _Var25 = erlang:apply(MOD,merge,[_Var23, TestDir]),
    _Var26 = erlang:apply(MOD,delete,[_Var23,4]),
    not_found = MOD:get(_Var23,4),
    _Var27 = erlang:apply(MOD,bc_close,[_Var23]),
    _Var28 = erlang:apply(MOD,bc_open,[TestDir,MFS]),
    not_found = MOD:get(_Var28,4),
    _Var33 = erlang:apply(MOD,fold,[_Var28]),
    not_found = MOD:get(_Var28,4),
    bc_close(_Var28),
    bitcask_time:test__clear_fudge(),

    Expected33 = [{1,Val2},{2,Val2},{3,Val2}],
    ?assertEqual(Expected33, lists:sort(_Var33)),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% new_20131217_d_test_() ->
%%     {timeout, 300, ?_assertEqual(ok, new_20131217_d_body())}.

%% new_20131217_d_body() ->
%%     catch token:stop(),
%%     TestDir = token:get_name(),
%%     MOD = ?MODULE,
%%     Val1 = <<0,0,0,0,0,0,0,0,0,0>>,%<<"v111111111">>,
%%     Val2 = <<0,0,0,0,0,0,0,0,0,0,0,0,0>>,%<<"v222222222222">>,
%%     Val3 = <<0,0,0,0,0>>,
%%     Val4 = <<0,0>>,
%%     V67_expected = [],

%%     bitcask_time:test__set_fudge(10),
%%     _Var1 = erlang:apply(MOD,bc_open,[TestDir]),
%%     _Var26 = erlang:apply(MOD,puts,[_Var1,{1,1},Val1]),
%%     _Var28 = erlang:apply(MOD,bc_close,[_Var1]),
%%     _Var30 = erlang:apply(MOD,bc_open,[TestDir]),
%%     _Var32 = erlang:apply(MOD,puts,[_Var30,{1,6},Val2]),
%%     _Var33 = erlang:apply(MOD,bc_close,[_Var30]),
%%     _Var36 = erlang:apply(MOD,incr_clock,[]),
%%     _Var38 = erlang:apply(MOD,bc_open,[TestDir]),
%%     _Var39 = erlang:apply(MOD,delete,[_Var38,1]),
%% %% timer:sleep(1234),
%%     _Var40 = erlang:apply(MOD,merge,[_Var38, TestDir]),
%% %% timer:sleep(1235),
%%     _Var41 = erlang:apply(MOD,puts,[_Var38,{1,4},Val3]),
%%     _Var49 = erlang:apply(MOD,puts,[_Var38,{1,2},Val4]),
%%     _Var50 = erlang:apply(MOD,delete,[_Var38,1]),
%%     _Var51 = erlang:apply(MOD,delete,[_Var38,2]),
%%     _Var52 = erlang:apply(MOD,delete,[_Var38,3]),
%%     _Var53 = erlang:apply(MOD,delete,[_Var38,4]),
%%     _Var54 = erlang:apply(MOD,delete,[_Var38,5]),
%%     _Var55 = erlang:apply(MOD,incr_clock,[]),
%%     _Var56 = erlang:apply(MOD,delete,[_Var38,6]),
%% %% timer:sleep(1234),
%%     _Var61 = erlang:apply(MOD,merge,[_Var38, TestDir]),
%% %% timer:sleep(1235),
%%     _Var62 = erlang:apply(MOD,bc_close,[_Var38]),
%%     _Var64 = erlang:apply(MOD,bc_open,[TestDir]),
%%     _Var67 = erlang:apply(MOD,fold,[_Var64]),

%%     bc_close(_Var64),
%%     ?assertEqual(V67_expected, lists:sort(_Var67)),
%%     ok.

new_20131217_e_test_() ->
    {timeout, 300, ?_assertEqual(ok, new_20131217_e_body())}.

new_20131217_e_body() ->
    catch token:stop(),
    TestDir = token:get_name(),
    bitcask_time:test__set_fudge(10),
    MOD = ?MODULE,
    MFS = 400,
    V1 = <<"v111111111111<">>, %<<0,0,0,0,0,0,0,0,0,0,0,0,0,0>>,
    V2 = <<"v222222<">>, %<<0,0,0,0,0,0,0,0>>,
    V3 = <<"v33333333333<">>, %<<0,0,0,0,0,0,0,0,0,0,0,0,0>>,
    V4 = <<"v4444444<">>, %<<0,0,0,0,0,0,0,0,0>>,
    V5 = <<"v5<">>, %<<0,0,0>>,
    V63_expected = [{4,V2},
                    {5,V2},
                    {6,V2},
                    {7,V2},
                    {8,V2},
                    {9,V2},
                    {10,V2},
                    {11,V1},
                    {15,V5},
                    {16,V4},
                    {17,V4},
                    {18,V3},
                    {19,V3},
                    {20,V3},
                    {21,V3},
                    {22,V3},
                    {23,V3},
                    {24,V3},
                    {25,V3},
                    {26,V3},
                    {27,V3},
                    {28,V3},
                    {29,V3},
                    {30,V3},
                    {31,V3},
                    {32,V3}],
    _Var2 = erlang:apply(MOD,bc_open,[TestDir,MFS]),
    _Var4 = erlang:apply(MOD,puts,[_Var2,{1,11},V1]),
    _Var9 = erlang:apply(MOD,bc_close,[_Var2]),
    _Var12 = erlang:apply(MOD,bc_open,[TestDir,MFS]),
    _Var13 = erlang:apply(MOD,incr_clock,[]),
    _Var16 = erlang:apply(MOD,delete,[_Var12,1]),
    _Var19 = erlang:apply(MOD,puts,[_Var12,{1,10},V2]),
    _Var20 = erlang:apply(MOD,delete,[_Var12,1]),
    _Var22 = erlang:apply(MOD,delete,[_Var12,3]),
    _Var24 = erlang:apply(MOD,merge_these,[_Var12, TestDir, [1]]),
    %% _Var24 = erlang:apply(MOD,merge,[_Var12, TestDir]),
    _Var27 = erlang:apply(MOD,puts,[_Var12,{14,32},V3]),
    _Var28 = erlang:apply(MOD,delete,[_Var12,14]),
    _Var37 = erlang:apply(MOD,puts,[_Var12,{15,17},V4]),
    _Var38 = erlang:apply(MOD,delete,[_Var12,2]),
    _Var39 = erlang:apply(MOD,incr_clock,[]),
    _Var45 = erlang:apply(MOD,puts,[_Var12,{15,15},V5]),
    _Var46 = erlang:apply(MOD,merge_these,[_Var12, TestDir, [1,2,4,5,6]]),
    %% _Var46 = erlang:apply(MOD,merge,[_Var12, TestDir]),
    _Var54 = erlang:apply(MOD,bc_close,[_Var12]),
    _Var56 = erlang:apply(MOD,bc_open,[TestDir,MFS]),
    _Var63 = erlang:apply(MOD,fold,[_Var56]),
    bitcask_time:test__clear_fudge(),
    ?assertEqual(V63_expected, lists:sort(_Var63)),
    ok.

bc_open(Dir, MaxFileSize) ->
    bitcask:open(Dir, [read_write, {max_file_size, MaxFileSize}, {open_timeout, 1234}]).

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

merge(H, TestDir) ->
  case bitcask:needs_merge(H) of
    {true, Files} ->
      case catch bitcask:merge(TestDir, [], Files) of
        {'EXIT', Err} -> Err;
        R             -> R
      end;
    false -> not_needed
  end.

fork_merge(H, Dir) ->
  case bitcask:needs_merge(H) of
    {true, Files} -> catch bitcask_merge_worker:merge(Dir, [], Files);
    false         -> not_needed
  end.

merge_these(_H, TestDir, Ids) ->
    Files = [lists:flatten(io_lib:format("~s/~w.bitcask.data", [TestDir,Id])) ||
                Id <- Ids],
    bitcask:merge(TestDir, [], Files).

incr_clock() ->
    bitcask_time:test__incr_fudge(1).

bc_close(H)    ->
  ok = bitcask:close(H).

fold_keys(H) ->
  bitcask:fold_keys(H, fun(#bitcask_entry{key = KBin}, Ks) -> [un_nice_key(KBin)|Ks] end, []).

fold(H) ->
  bitcask:fold(H, fun(KBin, V, Acc) -> [{un_nice_key(KBin),V}|Acc] end, []).

merge_delete_race_pr156_regression_test_() ->
    {timeout, 60, fun merge_delete_race_pr156_regression_test2/0}.

merge_delete_race_pr156_regression_test2() ->
    %% Heart of the problem:
    %% 1. delete a key: this creates 1.bitcask.data
    %% 2. merge.  input files is [1.data.bitcask]
    %% 3. merge defer delete file_id 1.
    %% 4. Close the cask.
    %% 5. Open the cask.  file_id 1 is marked for deferred deletion,
    %%    so delete it immediately.
    %% 6. Cask open finishes.  The largest file_id is 1.
    %% 7. Write some data.  It is written to file_id 1.
    %% 8. The merge_delete_worker now deletes file_id 1, but
    %%    it's far too late and deletes a new incarnation of
    %%    file_id 1.

    Dir = "/tmp/bc.merge_delete_race_pr156_regression",

    os:cmd("rm -rf " ++ Dir),
    _ = application:start(bitcask),
    try
        Ref1 = bitcask:open(Dir, [read_write]),
        bitcask:delete(Ref1, <<"does_not_exist">>),
        bitcask_merge_delete ! timeout,
        timer:sleep(10),
        ok = bitcask:merge(Dir),
        ok = bitcask:close(Ref1),

        Ref2 = bitcask:open(Dir, [read_write]),
        ok = bitcask:merge(Dir),
        ok = bitcask:close(Ref2),

        Ref3 = bitcask:open(Dir, [read_write]),
        Val = <<"val!">>,
        NumKeys = 7,
        [ok = bitcask:put(Ref3, nice_key(K), Val) || K <- lists:seq(1,NumKeys)],
        bitcask_merge_delete ! timeout,
        timer:sleep(2000),

        XX = bitcask:fold(Ref3, fun(KBin, V, Acc) -> [{un_nice_key(KBin),V}|Acc] end, []),
        ?assertEqual(NumKeys, length(XX)),

        ok = bitcask:close(Ref3)
    after
        ok % application:stop(bitcask)
    end.

-endif. %% TEST
