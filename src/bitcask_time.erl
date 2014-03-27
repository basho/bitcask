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
-module(bitcask_time).

-export([tstamp/0]).
-export([test__set_fudge/1, test__get_fudge/0, test__incr_fudge/1,
         test__clear_fudge/0, test__time_travel_loop_sleep/0]).
-define(KEY, bitcask_time_fudge).

%% Return number of seconds since 1970
tstamp() ->
    test__get(?KEY).

test__set_fudge(Amount) ->
    test__clear_fudge(),
    application:set_env(bitcask, ?KEY, Amount).

test__get_fudge() ->
    test__get(?KEY).

test__incr_fudge(Amount) ->
    test__set_fudge(test__get_fudge() + Amount).

test__get(Key) ->
    %% Play games with local process dictionary to avoid looking
    %% at application controller's ETS table for every call.
    case get(Key) of
        undefined ->
            case application:get_env(bitcask, Key) of
                undefined ->
                    put(Key, no_testing);
                 _ ->
                    put(Key, yes_testing)
            end,
            test__get(Key);
        no_testing ->
            {Mega, Sec, _Micro} = os:timestamp(),
            (Mega * 1000000) + Sec;
        yes_testing ->
            {ok, Fudge} = application:get_env(bitcask, Key),
            Fudge
    end.

test__clear_fudge() ->
    application:unset_env(bitcask, ?KEY),
    erase(?KEY).

-ifdef(PULSE).
test__time_travel_loop_sleep() ->
    test__incr_fudge(1).
-else.

test__time_travel_loop_sleep() ->
    timer:sleep(250).
-endif.
