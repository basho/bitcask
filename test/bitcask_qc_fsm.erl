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
-module(bitcask_qc_fsm).

-export([create_stale_lock/0,
         corrupt_hint/2,
         truncate_hint/2]).

-define(TEST_DIR, "/tmp/bitcask.qc." ++ os:getpid()).
-include_lib("kernel/include/file.hrl").

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state,{ bitcask :: reference(),
                data = [] :: list(),
                keys :: list() }). %% Keys to use in the test

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

initial_state() ->
    init.

initial_state_data() ->
    #state{}.

init(_S) ->
    [{closed, {call, ?MODULE, set_keys, [list(key_gen())]}}].

closed(_S) ->
    [{opened, {call, bitcask, open, [?TEST_DIR, [read_write, {open_timeout, 0}, sync_strategy()]]}},
     {closed, {call, ?MODULE, truncate_hint, [int(), int()]}},
     {closed, {call, ?MODULE, corrupt_hint, [int(), int()]}},
     {closed, {call, ?MODULE, create_stale_lock, []}}].

opened(S) ->
    [{closed, {call, bitcask, close, [S#state.bitcask]}},
     {opened, {call, bitcask, get, [S#state.bitcask, key(S)]}},
     {opened, {call, bitcask, put, [S#state.bitcask, key(S), value()]}},
     {opened, {call, bitcask, delete, [S#state.bitcask, key(S)]}},
     {opened, {call, bitcask, merge, [?TEST_DIR]}}
     ].

next_state_data(init, closed, S, _, {call, _, set_keys, [Keys]}) ->
    S#state{ keys = [<<"k">> | Keys] }; % ensure always one key
next_state_data(closed, opened, S, Bcask, {call, bitcask, open, _}) ->
    S#state { bitcask = Bcask };
next_state_data(opened, closed, S, _, {call, _, close, _}) ->
    S#state { bitcask = undefined };
next_state_data(opened, opened, S, _, {call, bitcask, put, [_, Key, Value]}) ->
    S#state { data = orddict:store(Key, Value, S#state.data) };
next_state_data(opened, opened, S, _, {call, bitcask, delete, [_, Key]}) ->
    S#state { data = orddict:erase(Key, S#state.data) };
next_state_data(_From, _To, S, _Res, _Call) ->
    S.




%% Precondition (for state data).
%% Precondition is checked before command is added to the command sequence
precondition(_From,_To,S,{call,_,get,[_,Key]}) ->
    lists:member(Key, S#state.keys); % check the key has not been shrunk away
precondition(_From,_To,S,{call,_,put,[_,Key,_Val]}) ->
    lists:member(Key, S#state.keys); % check the key has not been shrunk away
precondition(_From,_To,_S,{call,_,_,_}) ->
    true.


postcondition(opened, opened, S, {call, _, get, [_, Key]}, not_found) ->
    case orddict:find(Key, S#state.data) of
        error ->
            true;
        {ok, Exp} ->
            {expected, Exp, got, not_found}
    end;
postcondition(opened, opened, S, {call, _, get, [_, Key]}, {ok, Value}) ->
    case orddict:find(Key, S#state.data) of
        {ok, Value} ->
            true;
        Exp ->
            {expected, Exp, got, Value}
    end;
postcondition(opened, opened, _S, {call, _, merge, [_TestDir]}, Res) ->
    case Res == ok of
        true ->
            true;
        false ->
            erlang:display({bad_merge_return, Res}),
            {expected, ok, got, Res}
    end;
postcondition(_From,_To,_S,{call,_,_,_},_Res) ->
    true.

qc_test_() ->
    TestTime = 45,
    ShrinkTime = 600,
    qc_test_(TestTime, ShrinkTime).

qc_test_(TestTime, ShrinkTime) ->
    Timeout = TestTime + ShrinkTime,
    {timeout, Timeout,
     {setup, fun prepare/0, fun cleanup/1,
      [{timeout, Timeout, ?_assertEqual(true,
                eqc:quickcheck(eqc:testing_time(TestTime, ?QC_OUT(prop_bitcask()))))}]}}.

prepare() ->
    error_logger:tty(false),
    application:load(bitcask),
    application:start(bitcask),
    application:set_env(bitcask, require_hint_crc, true).

cleanup(_) ->
    application:stop(bitcask),
    application:unload(bitcask).

prop_bitcask() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
		bitcask_merge_delete:testonly__delete_trigger(),
                [] = os:cmd("rm -rf " ++ ?TEST_DIR),
                {H,{_State, StateData}, Res} = run_commands(?MODULE,Cmds),
                case (StateData#state.bitcask) of
                    undefined ->
                        ok;
                    Ref ->
                        bitcask:close(Ref)
                end,
                application:unload(bitcask),
                aggregate(zip(state_names(H),command_names(Cmds)), 
                          equals(Res, ok))
            end).

%% Weight for transition (this callback is optional).
%% Specify how often each transition should be chosen
weight(_From, _To,{call,_,close,_}) ->
    10;
weight(_From, _To,{call,_,truncate_hint,_}) ->
    10;
weight(_From, _To,{call,_,corrupt_hint,_}) ->
    10;
weight(_From,_To,{call,_,_,_}) ->
    100.

set_keys(_Keys) -> %% next_state sets the keys for use by key()
    ok.

key_gen() ->
    ?SUCHTHAT(X, binary(), X /= <<>>).

key(#state{keys = Keys}) ->
    elements(Keys).

value() ->
    binary().

sync_strategy() ->
    {sync_strategy, oneof([none, o_sync])}.

-endif.

create_stale_lock() ->
    Fname = filename:join(?TEST_DIR, "bitcask.write.lock"),
    filelib:ensure_dir(Fname),
    ok = file:write_file(Fname, "102349430239 abcdef\n").

truncate_hint(Seed, TruncBy0) ->
    case filelib:wildcard(?TEST_DIR ++ "/*.hint") of
        [] ->
            ok;
        Hints->
            Hint = lists:nth(1 + (abs(Seed) rem length(Hints)), Hints),
            {ok, Fi} = file:read_file_info(Hint),
            {ok, Fh} = file:open(Hint, [read, write]),
            TruncBy = (1 + abs(TruncBy0)) rem (Fi#file_info.size+1),
            {ok, _To} = file:position(Fh, {eof, erlang:max(-TruncBy, 0)}),
            %% io:format(user, "Truncating ~p by ~p to ~p\n", [Hint, TruncBy, _To]),
            file:truncate(Fh),
            file:close(Fh)
    end.

corrupt_hint(Seed, CorruptAt0) ->
    case filelib:wildcard(?TEST_DIR ++ "/*.hint") of
        [] ->
            ok;
        Hints->
            Hint = lists:nth(1 + (abs(Seed) rem length(Hints)), Hints),
            {ok, Fi} = file:read_file_info(Hint),
            {ok, Fh} = file:open(Hint, [read, write, binary]),
            Size = Fi#file_info.size,
            CorruptAt = (1 + abs(CorruptAt0)) rem (Size+1),
            try
                {ok, Pos} = file:position(Fh, {eof, -CorruptAt}),
                {ok, <<Byte>>} = file:pread(Fh, Pos, 1),
                BadByte = <<(bnot Byte)>>,
                io:format(user, "Corrupting from ~p to ~p at ~p size ~p\n",
                          [Byte, BadByte, Pos, Size]),
                ok = file:pwrite(Fh, Pos, BadByte)
            catch
                _:Reason ->
                    io:format(user, "corrupt failed corruptat=~p reason=~p\n",
                              [CorruptAt, Reason])
            after
                file:close(Fh)
            end
    end.

