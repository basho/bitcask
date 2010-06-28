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
-module(merge_pulse_qc).
-author('Dave Smith <dizzyd@basho.com>').
-author('Justin Sheehy <justin@basho.com>').

-ifdef(EQC).
-ifdef(PULSE).

-compile(export_all).

-compile({parse_transform, pulse_instrument}).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("pulse/include/pulse.hrl").

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

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
    apply_kv_ops(Rest, Ref, orddict:store(K, {ok, V}, Acc0));
apply_kv_ops([{delete, K, _} | Rest], Ref, Acc0) ->
    ok = bitcask:delete(Ref, K),
    apply_kv_ops(Rest, Ref, orddict:store(K, not_found, Acc0)).


check_model([], Ref) ->
    ok;
check_model([{K, V} | Rest], Ref) ->
    ?assertEqual(V, bitcask:get(Ref, K)),
    check_model(Rest, Ref).

do_read(Model) ->
    Ref = bitcask:open("/tmp/bc.prop.merge.pulse"),
    check_model(Model, Ref),
    bitcask:close(Ref).


merge_pulse(Model) ->
    Self = self(),
    spawn(fun() -> bitcask:merge("/tmp/bc.prop.merge.pulse"), Self ! done end),
    spawn(fun() -> do_read(Model), Self ! done2 end),
    receive
        done ->
            receive
                done2 ->
                    ok
            end,
            ok
    end.


prop_merge_pulse() ->
    pulse:start(),
    {ok, Cwd} = file:get_cwd(),
    code:add_pathz(filename:join(filename:dirname(Cwd), "ebin")),

    ?LET({Keys, Values}, {keys(), values()},
         ?FORALL({Ops, Seed}, {non_empty(list(ops(Keys, Values))), pulse:seed()},
                 begin
                     ?cmd("rm -rf /tmp/bc.prop.merge.pulse"),

                     %% Open a bitcask and dump a bunch of values into it
                     Ref = bitcask:open("/tmp/bc.prop.merge.pulse",
                                        [read_write, {max_file_size, 1}]),
                     Model = apply_kv_ops(Ops, Ref, []),

                     ?assertEqual(ok, pulse:run_with_seed(fun() -> merge_pulse(Model) end, Seed)),

                     bitcask:close(Ref),

                     true
                 end)).

prop_merge_pulse_test_() ->
    {timeout, 60, fun() ->
                          ?assert(eqc:quickcheck(?QC_OUT(prop_merge_pulse())))
                  end}.


-endif. % PULSE
-endif. % EQC
