%% -------------------------------------------------------------------
%%
%% bitcask_async_nif: An async thread-pool layer for Erlang's NIF API
%%
%% Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
%% Author: Gregory Burd <greg@basho.com> <greg@burd.me>
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

-module(bitcask_async_nif).
-export([call/2]).

call(Fun, Args) ->
    NIFRef = erlang:make_ref(),
    %% erlang:display({call, NIFRef, self()}),
    case erlang:apply(Fun, [NIFRef|Args]) of
        {ok, {enqueued, _QDepth}} ->
            %[erlang:bump_reductions(10 * QDepth) || is_integer(QDepth), QDepth > 100],
            receive
                {NIFRef, {error, shutdown}=Error} ->
                    %% Work unit was queued, but not executed.
                    %% erlang:display({ret1, NIFRef, self()}),
                    Error;
                {NIFRef, {error, _Reason}=Error} ->
                    %% Work unit returned an error.
                    %% erlang:display({ret2, NIFRef, self()}),
                    Error;
                {NIFRef, Reply} ->
                    %% erlang:display({ret3, NIFRef, self()}),
                    Reply
            end;
        Other ->
            %% erlang:display({ret4, NIFRef, self()}),
            Other
    end.
