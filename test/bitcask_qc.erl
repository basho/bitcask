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

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("bitcask.hrl").

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-define(TEST_TIME, 30).                      % seconds

-record(m_fstats, {key_bytes=0 :: integer(),
                   live_keys=0 :: integer(),
                   live_bytes=0 :: integer(),
                   total_keys=0 :: integer(),
                   total_bytes=0 :: integer()}).

qc(P) ->
    qc(P, ?TEST_TIME).

qc(P, TestTime) ->
    ?assert(eqc:quickcheck(?QC_OUT(eqc:testing_time(TestTime, P)))).

keys() ->
    eqc_gen:non_empty(list(noshrink(eqc_gen:non_empty(binary())))).

values() ->
    eqc_gen:non_empty(list(noshrink(binary()))).

ops(Keys, Values) ->
    {oneof([put, delete, itr, itr_next, itr_release]), oneof(Keys), oneof(Values)}.

apply_kv_ops([], Ref, KVs0, Fstats) ->
    bitcask_nifs:keydir_itr_release(get_keydir(Ref)), % release any iterators
    {KVs0, Fstats};
apply_kv_ops([{put, K, V} | Rest], Ref, KVs0, Fstats0) ->
    ok = bitcask:put(Ref, K, V),
    apply_kv_ops(Rest, Ref, orddict:store(K, V, KVs0),
                 update_fstats(put, K ,orddict:find(K, KVs0), V, Fstats0));
apply_kv_ops([{delete, K, _} | Rest], Ref, KVs0, Fstats0) ->
    ok = bitcask:delete(Ref, K),
    case orddict:find(K, KVs0) of 
        error -> 
            apply_kv_ops(Rest, Ref, KVs0, Fstats0);
        {ok, deleted} -> 
            apply_kv_ops(Rest, Ref, KVs0, Fstats0);
        OldVal ->
            apply_kv_ops(Rest, Ref, orddict:store(K, deleted, KVs0),
                         update_fstats(delete, K, OldVal,
                                       ?TOMBSTONE0, Fstats0))
    end;
apply_kv_ops([{itr, _K, _} | Rest], Ref, KVs, Fstats) ->
    %% Don't care about result, just want to intermix with get/put
    bitcask_nifs:keydir_itr(get_keydir(Ref), -1, -1),
    apply_kv_ops(Rest, Ref, KVs, Fstats);
apply_kv_ops([{itr_next, _K, _} | Rest], Ref, KVs, Fstats) ->
    %% Don't care about result, just want to intermix with get/put
    bitcask_nifs:keydir_itr_next(get_keydir(Ref)),
    apply_kv_ops(Rest, Ref, KVs, Fstats);
apply_kv_ops([{itr_release, _K, _} | Rest], Ref, KVs, Fstats) ->
    %% Don't care about result, just want to intermix with get/put
    bitcask_nifs:keydir_itr_release(get_keydir(Ref)),
    apply_kv_ops(Rest, Ref, KVs, Fstats).


%% Delete existing key (i.e. write tombstone)
update_fstats(delete, K, {ok, OldV}, _NewV, Fstats0) -> 
    #m_fstats{key_bytes = KB, live_keys = LK, live_bytes = LB} = Fstats0,
    TotalSz = total_sz(K, OldV),
    Fstats0#m_fstats{key_bytes = KB - size(K),
                     live_keys = LK - 1,
                     live_bytes = LB - TotalSz};
%% Update m_fstats record - this will be the aggregate of all files in the bitcask
update_fstats(put, K, ErrDel, NewV,
              #m_fstats{key_bytes = KB,
                        live_keys = LK, live_bytes = LB, 
                        total_keys = TK, total_bytes = TB} = Fstats)
  when ErrDel =:= error; ErrDel =:= {ok, deleted} ->
    %% Add for first time or update after deletion
    NewTotalSz = total_sz(K, NewV),
    Fstats#m_fstats{key_bytes = KB + size(K),
                    live_keys = LK + 1, live_bytes = LB + NewTotalSz,
                    total_keys = TK + 1, total_bytes = TB + NewTotalSz};
update_fstats(put, K, {ok, OldV}, NewV, #m_fstats{live_bytes = LB, 
                                                  total_keys = TK,
                                                  total_bytes = TB} = Fstats) -> 
    %% update existing key
    OldTotalSz = total_sz(K, OldV),
    NewTotalSz = total_sz(K, NewV),
    Fstats#m_fstats{live_bytes = LB + NewTotalSz - OldTotalSz,
                    total_keys = TK + 1, total_bytes = TB + NewTotalSz}.

check_fstats(Ref, Expect) ->
    Aggregate = fun({_FileId, FileLiveCount, FileTotalCount, FileLiveBytes, FileTotalBytes,
                     _FileOldestTstamp, _FileNewestTstamp, _ExpEpoch},
                    {LiveCount0, TotalCount0, LiveBytes0, TotalBytes0}) ->
                        {LiveCount0 + FileLiveCount, TotalCount0 + FileTotalCount, 
                         LiveBytes0 + FileLiveBytes, TotalBytes0 + FileTotalBytes}
                end,
    {KeyCount, KeyBytes, Fstats, _, _} = bitcask_nifs:keydir_info(get_keydir(Ref)),
    {LiveCount, TotalCount, LiveBytes, TotalBytes} =
        lists:foldl(Aggregate, {0, 0, 0, 0}, Fstats),
    ?assert(Expect#m_fstats.live_keys >= 0),
    ?assert(Expect#m_fstats.key_bytes >= 0),
    ?assert(Expect#m_fstats.live_keys >= 0),
    ?assert(Expect#m_fstats.live_bytes >= 0),
    ?assert(Expect#m_fstats.total_keys >= 0),
    ?assert(Expect#m_fstats.total_bytes >= 0),

    ?assertEqual(Expect#m_fstats.live_keys, KeyCount),
    ?assertEqual(Expect#m_fstats.key_bytes, KeyBytes),
    ?assertEqual(Expect#m_fstats.live_keys, LiveCount),
    ?assertEqual(Expect#m_fstats.live_bytes, LiveBytes),
    ?assertEqual(Expect#m_fstats.total_keys, TotalCount),
    ?assert(Expect#m_fstats.total_bytes =< TotalBytes).

check_model(Ref, Model) ->
    F = fun({K, deleted}) ->
                ?assertEqual({K, not_found}, {K, bitcask:get(Ref, K)});
           ({K, V}) ->
                ?assertEqual({K, {ok, V}}, {K, bitcask:get(Ref, K)})
        end,
    lists:map(F, Model).

total_sz(K, V) -> % Total size of bitcask entry in bytes
    ((32 + % crc
      32 + % tstamps
      16 + % key size
      32) div 8) + % val size
        size(K) + size(V).

prop_merge() ->
    ?LET({Keys, Values}, {keys(), values()},
         ?FORALL({Ops, M1, M2}, {eqc_gen:non_empty(list(ops(Keys, Values))),
                                 choose(1,128), choose(1,128)},
                 begin
                     Tm = tuple_to_list(now()),
                     Dir = lists:flatten(
                             io_lib:format(
                               "/tmp/bc.prop.merge.~w.~w.~w", Tm)),
                     ?cmd("rm -rf " ++ Dir),

                     %% Open a bitcask, dump the ops into it and build
                     %% a model of what SHOULD be in the data.
                     Ref = bitcask:open(Dir,
                                        [read_write, {max_file_size, M1}]),
                     try
                         {Model, Fstats} = apply_kv_ops(Ops, Ref, [], #m_fstats{}),
                         check_fstats(Ref, Fstats),
                         check_model(Ref, Model),

                         %% Apply the merge -- note that we keep the
                         %% bitcask open so that a live keydir is
                         %% available to the merge.  Run in a seperate 
                         %% process so it gets cleaned up on crash
                         %% so quickcheck can shrink correctly.
                         Me = self(),
                         proc_lib:spawn(
                           fun() ->
                                   try
                                       Me ! bitcask:merge(Dir,
                                                          [{max_file_size, M2}])
                                   catch
                                       _:Err ->
                                           Me ! Err
                                   end
                           end),
                         receive
                             X ->
                                 ?assertEqual(ok, X)
                         end,

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
                         check_model(Ref, Model),
                         true
                     after
                         bitcask:close(Ref)
                     end,

                     %% For each of the data files, validate that it has a valid hint file
                     Validate = fun(Fname) ->
                                        {ok, S} = bitcask_fileops:open_file(Fname),
                                        try
                                            ?assertEqual({Fname, true}, {Fname, bitcask_fileops:has_valid_hintfile(S)})
                                        after
                                            bitcask_fileops:close(S)
                                        end
                                end,
                     [Validate(Fname) || {_Ts, Fname} <-
                                             bitcask_fileops:data_file_tstamps(Dir)],
                     ?cmd("rm -rf " ++ Dir),
                     true
                 end)).


prop_fold() ->
    ?LET({Keys, Values, FoldOp}, {keys(), values(), oneof([fold, fold_keys])},
         ?FORALL({Ops, M1}, {eqc_gen:non_empty(list(ops(Keys, Values))),
                             choose(1,128)},
                 begin
                     ?cmd("rm -rf /tmp/bc.prop.fold"),

                     %% Open a bitcask, dump the ops into it and build
                     %% a model of what SHOULD be in the data.
                     Ref = bitcask:open("/tmp/bc.prop.fold",
                                        [read_write, {max_file_size, M1}]),
                     try
                         {Model, Fstats} = apply_kv_ops(Ops, Ref, [], #m_fstats{}),
                         check_fstats(Ref, Fstats),

                         %% Build a list of the K/V pairs available to fold
                         Actual = case FoldOp of
                                      fold_keys ->
                                          bitcask:fold_keys(Ref,
                                                            fun(E, Acc0) ->
                                                                K = E#bitcask_entry.key,
                                                                {ok, V} = bitcask:get(Ref, K),
                                                                [{K, V} | Acc0]
                                                            end, []);
                                      fold ->
                                          bitcask:fold(Ref, fun(K, V, Acc0) ->
                                                                [{K, V} | Acc0]
                                                            end, [])
                                  end,

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
                         lists:map(F, Model)
                     after
                         bitcask:close(Ref)
                     end,
                     true
                 end)).


merge1_test_() ->
    {timeout, 60, fun merge1_test2/0}.

merge1_test2() ->
    ?assert(eqc:check(prop_merge(),
                      [{[{put,<<0>>,<<>>},{delete,<<0>>,<<>>}],1,1}])).

merge2_test_() ->
    {timeout, 60, fun merge2_test2/0}.

merge2_test2() ->
    ?assert(eqc:check(prop_merge(),
                      [{[{put,<<1>>,<<>>},{delete,<<0>>,<<>>}],1,1}])).

merge3_test_() ->
    {timeout, 60, fun merge3_test2/0}.

merge3_test2() ->
    ?assert(eqc:check(prop_merge(),
                      [{[{put,<<0>>,<<>>},
                         {delete,<<0>>,<<>>},
                         {delete,<<1>>,<<>>}],
                        1,1}])).
merge4_test_() ->
    {timeout, 60, fun merge4_test2/0}.

merge4_test2() ->
    ?assert(eqc:check(prop_merge(),
                      [{[{itr,<<1>>,<<>>},{delete,<<0>>,<<>>}],1,1}])).

merge5_test_() ->
    {timeout, 60, fun merge5_test2/0}.

merge5_test2() ->
    ?assert(eqc:check(prop_merge(),
                      [{[{put,<<"test">>,<<>>},{itr,<<"test">>,<<>>},
                         {delete,<<"test">>,<<>>},{delete,<<"test">>,<<>>}],1,1}])).

merge6_test_() ->
    {timeout, 60, fun merge6_test2/0}.

merge6_test2() ->
    ?assert(eqc:check(prop_merge(),
                      [{[{itr,<<"test">>,<<>>},{put,<<"test">>,<<>>},
                         {delete,<<"test">>,<<>>},{delete,<<"test">>,<<>>}],
                        1,1}])).

prop_merge_test_() ->
    {timeout, ?TEST_TIME*2, fun() -> qc(prop_merge()) end}.


fold1_test_() ->
    {timeout, 60, fun fold1_test2/0}.

fold1_test2() ->
    ?assert(eqc:check(prop_fold(),
                      [{[{put,<<0>>,<<>>},
                         {itr,<<0>>,<<>>},
                         {delete,<<0>>,<<>>},
                         {itr_release,<<0>>,<<>>},
                         {put,<<0>>,<<>>}],1}])).

fold2_test_() ->
    {timeout, 60, fun fold2_test2/0}.

fold2_test2() ->
    ?assert(eqc:check(prop_fold(),
                      [{[{put,<<1>>,<<>>},
                         {itr,<<0>>,<<0>>},
                         {put,<<1>>,<<0>>},
                         {itr_release,<<1>>,<<0>>},
                         {put,<<1>>,<<>>}],1}])).

prop_fold_test_() ->
    {timeout, ?TEST_TIME*2, fun() -> qc(prop_fold()) end}.


get_keydir(Ref) ->
    element(9, erlang:get(Ref)).    

-endif.

