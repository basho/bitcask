%% -------------------------------------------------------------------
%%
%% bitcask: Eric Brewer-inspired key/value store
%%
%% Copyright (c) 2011 Basho Technologies, Inc. All Rights Reserved.
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
-module(bitcask_qc_expiry).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("bitcask.hrl").

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-define(TEST_TIME, 30).                      % seconds

qc(P) ->
    qc(P, ?TEST_TIME).

qc(P, TestTime) ->
    ?assert(eqc:quickcheck(?QC_OUT(eqc:testing_time(TestTime, P)))).

keys() ->
    eqc_gen:non_empty(list(eqc_gen:non_empty(binary()))).

values() ->
    eqc_gen:non_empty(list(binary())).

ops(Keys, Values) ->
    {oneof([put, delete]), oneof(Keys), oneof(Values)}.

apply_kv_ops([], _Ref, KVs0) ->
    KVs0;
apply_kv_ops([{put, K, V} | Rest], Ref, KVs0) ->
    ok = bitcask:put(Ref, K, V),
    apply_kv_ops(Rest, Ref, orddict:store({K, write_file(Ref)}, {V, current_tstamp()}, KVs0));
apply_kv_ops([{delete, K, _} | Rest], Ref, KVs0) ->
    ok = bitcask:delete(Ref, K),
    apply_kv_ops(Rest, Ref, orddict:store({K, write_file(Ref)}, {deleted, current_tstamp()}, KVs0)).

write_file(Ref) ->
    %% Extract active write_file handle from Bitcask ref
    element(3, erlang:get(Ref)).

current_tstamp() ->
    case erlang:get(meck_tstamp) of
        undefined ->
            next_tstamp();                      % Set it up for us
        Value ->
            Value
    end.

next_tstamp() ->
    Ts = case erlang:get(meck_tstamp) of
             undefined ->
                 1;
             Tstamp ->
                 Tstamp + erlang:get(meck_tstamp_step)
         end,
    erlang:put(meck_tstamp, Ts),
    Ts.

set_tstamp(Tstamp) ->
    erlang:put(meck_tstamp, Tstamp).

set_tstamp_step(Step) ->
    erlang:put(meck_tstamp_step, Step).

prop_expiry() ->
    ?LET({Keys, Values}, {keys(), values()},
         ?FORALL({Ops, Expiry, ExpiryGrace, Timestep, M1},
                 {eqc_gen:non_empty(list(ops(Keys, Values))),
                  choose(1,10), choose(1, 10), choose(5, 50), choose(5,128)},
         ?IMPLIES(true,
                 begin
                     Dirname = "/tmp/bc.prop.expiry",
                     ?cmd("rm -rf " ++ Dirname),

                     %% Initialize how many ticks each operation will
                     %% increment the clock by
                     set_tstamp(undefined),
                     set_tstamp_step(Timestep),

                     Bref = bitcask:open(Dirname,
                                         [read_write,
%                                          {log_needs_merge, true},
                                          {frag_merge_trigger, disabled},
                                          {dead_bytes_merge_trigger, disabled},
                                          {small_file_threshold, disabled},
                                          {frag_threshold, disabled},
                                          {expiry_secs, Expiry},
                                          {expiry_grace_secs, ExpiryGrace},
                                          {max_file_size, M1}]),

                     try
                         %% Dump the ops into the bitcask and build a model of
                         %% what SHOULD be in the data.
                         Model = apply_kv_ops(Ops, Bref, []),
                         %% Assist our model's calculations by incrementing
                         %% the clock one more time.
                         _ = next_tstamp(),

                         %% Close the writing file to ensure that it's included
                         %% in the needs_merge calculation
                         bitcask:close_write_file(Bref),

                         %% Identify items in the Model that should be expired
                         ExpireCutoff = erlang:max(current_tstamp() + Timestep - erlang:max(Expiry - ExpiryGrace, 0), -1),

                         {Expired, _Live} = lists:partition(fun({_K, {_Value, Tstamp}}) ->
                                                                    Tstamp < ExpireCutoff
                                                            end, Model),
%                         io:format(user, "Cutoff: ~p\nExpired: ~120p\nLive: ~120p\n",
%                                   [ExpireCutoff, Expired, Live]),

                         AllDeleteOps = lists:all(fun({delete,_,_}) -> true;
                                                     (_)            -> false
                                                  end, Ops),
                         %% Check that needs_merge has expected result
                         case {AllDeleteOps, Expired} of
                             {true, _} ->
                                 ?assertEqual(false, bitcask:needs_merge(Bref)),
                                 true;
                             {false, []} ->
                                 ?assertEqual(false, bitcask:needs_merge(Bref)),
                                 true;
                             {false, _} ->
                                 ?assertMatch({true, _}, bitcask:needs_merge(Bref)),
                                 true
                         end
                     catch
                         X:Y ->
                             io:format(user, "exception: ~p ~p @ ~p\n",
                                       [X,Y, erlang:get_stacktrace()]),
                             test_exception
                     after
                         bitcask:close(Bref)
                     end
                 end))).


prop_expiry_test_() ->
    {timeout, ?TEST_TIME*2, fun() ->
                              try
                                  meck:new(bitcask_time, [passthrough]),
                                  meck:expect(bitcask_time, tstamp, fun next_tstamp/0),
                                  qc(prop_expiry())
                              after
                                  meck:unload()
                              end
                      end}.




validate_expired([], _) ->
    ok;
validate_expired([{_K, {deleted, _}} | Rest], Actual) ->
    validate_expired(Rest, Actual);
validate_expired([{K, {Value, _Tstamp}} | Rest], Actual) ->
%    ?debugFmt("~p (~p) should be expired; actual: ~p\n", [K, Tstamp, Actual]),
    ?assert(not lists:member({K, Value}, Actual)),
    validate_expired(Rest, Actual).

validate_live([], _) ->
    ok;
validate_live([{K, {deleted, _}} | Rest], Actual) ->
%    ?debugFmt("~p should be deleted; actual: ~p\n", [K, Actual]),
    ?assert(not lists:keymember(K, 1, Actual)),
    validate_live(Rest, Actual);
validate_live([{K, {Value, _}} | Rest], Actual) ->
%    ?debugFmt("~p should have value ~p; actual: ~p\n", [K, Value, Actual]),
    ?assert(lists:member({K, Value}, Actual)),
    validate_live(Rest, Actual).


-endif.

