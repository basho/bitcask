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
-include_lib("eqc/include/eqc_fsm.hrl").

-compile(export_all).

-record(state,{ bitcask,
                data = [] }).

-define(TEST_DIR, "/tmp/bitcask.qc").

initial_state() ->
    closed.

initial_state_data() ->
    #state{}.

closed(_S) ->
    [{opened, {call, bitcask, open, [?TEST_DIR, [read_write, {open_timeout, 0}, sync_strategy()]]}}].

opened(S) ->
    [{closed, {call, bitcask, close, [S#state.bitcask]}},
     {opened, {call, bitcask, get, [S#state.bitcask, keys()]}},
     {opened, {call, bitcask, put, [S#state.bitcask, keys(), values()]}},
     {opened, {call, bitcask, delete, [S#state.bitcask, keys()]}},
     {opened, {call, bitcask, merge, [?TEST_DIR]}}
     ].


next_state_data(closed, opened, S, Bcask, {call, bitcask, open, _}) ->
    S#state { bitcask = Bcask };
next_state_data(opened, closed, S, _, {call, bitcask, close, _}) ->
    S#state { bitcask = undefined };
next_state_data(opened, opened, S, _, {call, bitcask, put, [_, Key, Value]}) ->
    S#state { data = orddict:store(Key, Value, S#state.data) };
next_state_data(opened, opened, S, _, {call, bitcask, delete, [_, Key]}) ->
    S#state { data = orddict:erase(Key, S#state.data) };
next_state_data(opened, opened, S, _Res, {call, bitcask, _, _}) ->
    S.


%% Precondition (for state data).
%% Precondition is checked before command is added to the command sequence
precondition(_From,_To,_S,{call,_,_,_}) ->
    true.


postcondition(opened, opened, S, {call, bitcask, get, [_, Key]}, not_found) ->
    not orddict:is_key(Key, S#state.data);
postcondition(opened, opened, S, {call, bitcask, get, [_, Key]}, {ok, Value}) ->
    Value == orddict:fetch(Key, S#state.data);
postcondition(_From,_To,_S,{call,_,_,_},_Res) ->
    true.

qc_test_() ->
    {timeout, 120, fun() -> true = eqc:quickcheck(prop_bitcask()) end}.

prop_bitcask() ->
    ?FORALL(Cmds,commands(?MODULE),
            begin
                [] = os:cmd("rm -rf " ++ ?TEST_DIR),
                {H,{_State, StateData}, Res} = run_commands(?MODULE,Cmds),
                case (StateData#state.bitcask) of
                    undefined ->
                        ok;
                    Ref ->
                        bitcask:close(Ref)
                end,
                aggregate(zip(state_names(H),command_names(Cmds)), Res == ok)
            end).

%% Weight for transition (this callback is optional).
%% Specify how often each transition should be chosen
weight(_From,_To,{call,_,_,_}) ->
    1.

keys() ->
    elements(vector(5, binary(5))).

values() ->
    elements(vector(5, binary(5))).

sync_strategy() ->
    {sync_strategy, oneof([none, o_sync])}.

-endif.


