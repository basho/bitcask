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
-module(bitcask_qc).
-author('Dave Smith <dizzyd@basho.com>').
-author('Justin Sheehy <justin@basho.com>').

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> ?debugFmt(Str, Args) end, P)).

qc(P) ->
    case catch(eqc:current_counterexample()) of
        CE when is_list(CE) ->
            ?debugFmt("Using counter example: ~p\n", [CE]),
            ?assert(eqc:check(P, CE));
        _ ->
            ?assert(eqc:quickcheck(P))
    end.

keys() ->
    eqc_gen:non_empty(list(eqc_gen:non_empty(binary()))).

values() ->
    eqc_gen:non_empty(list(binary())).

ops(Keys, Values) ->
    {oneof([put, delete]), oneof(Keys), oneof(Values)}.

apply_kv_ops([], _Ref, Acc0) ->
    Acc0;
apply_kv_ops([{put, K, V} | Rest], Ref, Acc0) ->
    ok = bitcask:put(Ref, K, V),
    apply_kv_ops(Rest, Ref, orddict:store(K, V, Acc0));
apply_kv_ops([{delete, K, _} | Rest], Ref, Acc0) ->
    ok = bitcask:delete(Ref, K),
    apply_kv_ops(Rest, Ref, orddict:store(K, deleted, Acc0)).

prop_merge() ->
    ?LET({Keys, Values}, {keys(), values()},
         ?FORALL({Ops, M1, M2}, {eqc_gen:non_empty(list(ops(Keys, Values))),
                                 choose(1,128), choose(1,128)},
                 begin
                     ?cmd("rm -rf /tmp/bc.prop.merge"),

                     %% Open a bitcask, dump the ops into it and build
                     %% a model of what SHOULD be in the data.
                     Ref = bitcask:open("/tmp/bc.prop.merge",
                                        [read_write, {max_file_size, M1}]),
                     Model = apply_kv_ops(Ops, Ref, []),

                     %% Apply the merge -- note that we keep the
                     %% bitcask open so that a live keydir is
                     %% available to the merge.
                     ok = bitcask:merge("/tmp/bc.prop.merge",
                                        [{max_file_size, M2}]),

                     %% Call needs_merge to close any "dead" files
                     bitcask:needs_merge(Ref),

                     %% Traverse the model and verify that retrieving
                     %% each key returns the expected value. It's
                     %% important to note that the model keeps
                     %% tombstones on deleted values so we can attempt
                     %% to retrieve those deleted values and check the
                     %% corresponding tombstone path in bitcask.
                     %% Verify that the bitcask contains exactly what
                     %% we expect
                     F = fun({K, deleted}) ->
                                 ?assertEqual(not_found, bitcask:get(Ref, K));
                            ({K, V}) ->
                                 ?assertEqual({ok, V}, bitcask:get(Ref, K))
                         end,
                     lists:map(F, Model),

                     bitcask:close(Ref),
                     true
                 end)).


prop_fold() ->
    ?LET({Keys, Values}, {keys(), values()},
         ?FORALL({Ops, M1}, {eqc_gen:non_empty(list(ops(Keys, Values))),
                             choose(1,128)},
                 begin
                     ?cmd("rm -rf /tmp/bc.prop.fold"),

                     %% Open a bitcask, dump the ops into it and build
                     %% a model of what SHOULD be in the data.
                     Ref = bitcask:open("/tmp/bc.prop.fold",
                                        [read_write, {max_file_size, M1}]),
                     Model = apply_kv_ops(Ops, Ref, []),

                     %% Build a list of the K/V pairs available to fold
                     Actual = bitcask:fold(Ref, 
                                           fun(K, V, Acc0) -> 
                                                   [{K, V} | Acc0]
                                           end,
                                           []),

                     %% Traverse the model and verify that retrieving
                     %% each key returns the expected value. It's
                     %% important to note that the model keeps
                     %% tombstones on deleted values so we can attempt
                     %% to retrieve those deleted values and check the
                     %% corresponding tombstone path in bitcask.
                     %% Verify that the bitcask contains exactly what
                     %% we expect
                     F = fun({K, deleted}) ->
                               ?assert(false == lists:keymember(K, 1, Actual));
                            ({K, V}) ->
                               ?assertEqual({K, V}, lists:keyfind(K, 1, Actual))
                         end,
                     lists:map(F, Model),

                     bitcask:close(Ref),
                     true
                 end)).


prop_merge_notest_() ->
    {timeout, 60, fun() -> qc(prop_merge()) end}.

merge1_test() ->
    ?assert(eqc:check(prop_merge(),
                      [{[{put,<<0>>,<<>>},{delete,<<0>>,<<>>}],1,1}])).

merge2_test() ->
    ?assert(eqc:check(prop_merge(),
                      [{[{put,<<1>>,<<>>},{delete,<<0>>,<<>>}],1,1}])).

merge3_test() ->
    ?assert(eqc:check(prop_merge(),
                      [{[{put,<<0>>,<<>>},
                         {delete,<<0>>,<<>>},
                         {delete,<<1>>,<<>>}],
                        1,1}])).

prop_fold_test_() ->
    {timeout, 60, fun() -> qc(prop_fold()) end}.


-endif.

