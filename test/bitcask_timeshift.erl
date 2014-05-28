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
-module(bitcask_timeshift).

-include_lib("eunit/include/eunit.hrl").
-include("bitcask.hrl").

-compile(export_all).

current_tstamp() ->
    case erlang:get(meck_tstamp) of
        undefined ->
            erlang:error(uninitialized_meck_tstamp);
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

timeshift_test_() ->
    {timeout, 60, fun() -> timeshift_test2() end}.

timeshift_test2() ->
    try
        Dirname = "/tmp/bc.timeshift",
        meck:new(bitcask_time, [passthrough]),
        meck:expect(bitcask_time, tstamp, fun next_tstamp/0),
        set_tstamp(100),
        set_tstamp_step(-1),

        ?cmd("rm -rf " ++ Dirname),
        Bref = bitcask:open(Dirname, [read_write]),
        ok = bitcask:put(Bref, <<"k1">>, <<"v1">>),
        %% Back when the NIF's internal puts depended on timestamps, this
        %% second put would fail because we've meck'ed time to go backward.
        %% Nowadays, this put should succeed.
        ok = bitcask:put(Bref, <<"k1">>, <<"v2">>),
        bitcask:close(Bref),

        %% For each of the data files, validate that it has a valid hint file
        Validate = fun(Fname) ->
                           {ok, S} = bitcask_fileops:open_file(Fname),
                           try
                               ?assert(bitcask_fileops:has_valid_hintfile(S))
                           after
                               bitcask_fileops:close(S)
                           end
                   end,
        [Validate(Fname) || {_Ts, Fname} <-
                                bitcask_fileops:data_file_tstamps(Dirname)],

        %% In our post-wall-clock timestamp world, verify that we read the newer value.
        Bref2 = bitcask:open(Dirname, [read_write]),
        {ok, <<"v2">>} = bitcask:get(Bref2, <<"k1">>),
        bitcask:close(Bref2)

    after
        meck:unload()
    end.
