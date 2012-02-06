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
-include("bitcask.hrl").

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-record(m_fstats, {key_bytes=0, live_keys=0, live_bytes=0, total_keys=0, total_bytes=0}).

qc(P) ->
    qc(P, 100).

qc(P, NumTests) ->
    ?assert(eqc:quickcheck(?QC_OUT(eqc:numtests(NumTests, P)))).

keys() ->
    eqc_gen:non_empty(list(eqc_gen:non_empty(binary()))).

values() ->
    eqc_gen:non_empty(list(binary())).

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
    apply_kv_ops(Rest, Ref, orddict:store(K, deleted, KVs0),
                 update_fstats(delete, K, orddict:find(K, KVs0), ?TOMBSTONE, Fstats0));
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


update_fstats(delete, K, OldV, NewV, Fstats0) -> %% Delete existing key (i.e. write tombstone)
    %% Delete issues a put with the tombstone
    Fstats1 = update_fstats(put, K, OldV, NewV, Fstats0),
    %% Then removes from the keydir
    #m_fstats{key_bytes = KB, live_keys = LK, live_bytes = LB} = Fstats1,
    TotalSz = total_sz(K, NewV), % remove the tombstone
    Fstats1#m_fstats{key_bytes = KB - size(K),
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
                     _FileOldestTstamp},
                    {LiveCount0, TotalCount0, LiveBytes0, TotalBytes0}) ->
                        {LiveCount0 + FileLiveCount, TotalCount0 + FileTotalCount, 
                         LiveBytes0 + FileLiveBytes, TotalBytes0 + FileTotalBytes}
                end,
    {KeyCount, KeyBytes, Fstats} = bitcask_nifs:keydir_info(get_keydir(Ref)),
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
    ?assertEqual(Expect#m_fstats.total_bytes, TotalBytes).

check_model(Ref, Model) ->
    F = fun({K, deleted}) ->
                ?assertEqual(not_found, bitcask:get(Ref, K));
           ({K, V}) ->
                ?assertEqual({ok, V}, bitcask:get(Ref, K))
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
                     ?cmd("rm -rf /tmp/bc.prop.merge"),

                     %% Open a bitcask, dump the ops into it and build
                     %% a model of what SHOULD be in the data.
                     Ref = bitcask:open("/tmp/bc.prop.merge",
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
                                       Me ! bitcask:merge("/tmp/bc.prop.merge",
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
                     end
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


prop_merge_test_() ->
    {timeout, 300*60, fun() -> qc(prop_merge()) end}.

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
    {timeout, 300*60, fun() -> qc(prop_fold()) end}.


get_keydir(Ref) ->
    element(8, erlang:get(Ref)).    

-endif.

