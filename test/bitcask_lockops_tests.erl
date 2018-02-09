%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Workday, Inc.
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
-module(bitcask_lockops_tests).

-include_lib("eunit/include/eunit.hrl").

-export([bitcask_locker_vm_main/0]).

lock_cannot_be_obtained_on_already_locked_file_within_same_os_process_test() ->
    Dir = "/tmp",
    Filename = bitcask_lockops:lock_filename(write,Dir),
    ok = file_delete(Filename),
    ok = file:write_file(Filename, ""),
    {ok, _Lock} = bitcask_lockops:acquire(write, Dir),
    ?assertEqual(
       {error, locked},
       bitcask_lockops:acquire(write, Dir)
    ).

lock_can_be_obtained_on_already_locked_file_is_unlocked_test() ->
    Dir = "/tmp",
    Filename = bitcask_lockops:lock_filename(write,Dir),
    ok = file_delete(Filename),
    ok = file:write_file(Filename, ""),
    {ok, Lock} = bitcask_lockops:acquire(write, Dir),
    bitcask_lockops:release(Lock),
    ?assertMatch(
       {ok, _},
       bitcask_lockops:acquire(write, Dir)
    ).

lock_cannot_be_obtained_on_already_locked_file_across_os_process_test() ->
    os:cmd("mkdir -p /tmp/lockbitcask"),
    os:cmd("rm -rf /tmp/lockbitcask/*"),
    %% start another erlang vm that will open the DB and obtain a write
    %% lock, then, when we try to obtain a write lock on the current vm
    %% it should fail.
    Eval1 =
        "erl -pa ebin -pa deps/*/ebin -noinput -noshell "
            "-eval \"bitcask_lockops_tests:bitcask_locker_vm_main().\"", 
    spawn_link(
        fun() ->
            Output = os:cmd(Eval1),
            ?debugFmt("\nCMD OUTPUT: ~s", [Output])
        end),
    timer:sleep(1000),
    DB = bitcask:open("/tmp/lockbitcask",[read_write]),
    ?assertEqual(
       {error,{error,locked}},
       bitcask:put(DB, <<"k">>, <<"v">>)
    ).

%% entry function for another vm to lock a bitcask DB
bitcask_locker_vm_main() ->
    case (catch bitcask:open("/tmp/lockbitcask", [read_write,{open_timeout,1000}])) of
        {error, Error} ->
            io:format("ERROR: ~p", [Error]);
        DB ->
            %% DB is locked on the first write operation
            ok = bitcask:put(DB, <<"k">>, <<"v">>),
            io:format('~p',[os:getpid()]),
            timer:sleep(3000),
            erlang:halt()
    end.

file_delete(Filename) ->
    case file:delete(Filename) of
        ok -> ok;
        {error,enoent} -> ok
    end.

