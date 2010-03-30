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
-module(bitcask).

-export([open/1, open/2,
         close/1,
         get/2,
         put/3,
         delete/2]).

-include("bitcask.hrl").

%% A bitcask is a directory containing:
%% * One or more data files - {integer_timestamp}.bitcask.data
%% * A write lock - bitcask.write.lock (Optional)
%% * A merge lock - bitcask.merge.lock (Optional)

%% @doc Open a new or existing bitcask datastore for read-only access.
-spec open(Dirname::string()) -> {ok, pid()} | {error, any()}.
open(Dirname) ->
    open(Dirname, []).


%% @doc Open a new or existing bitcask datastore with additional options.
-spec open(Dirname::string(), Opts::[_]) -> {ok, pid()} | {error, any()}.
open(Dirname, Opts) ->
    bitcask_client_mgr:get_client(Dirname, Opts).


%% @doc Close a bitcask data store and flush all pending writes (if any) to disk.
-spec close(pid()) -> ok.
close(Client) ->
    bitcask_client:close(Client).


%% @doc Retrieve a value by key from a bitcask datastore.
-spec get(pid(), binary()) -> not_found | {ok, Value::binary()}.
get(Client, Key) ->
    bitcask_client:get(Client, Key).


%% @doc Store a key and value in a bitcase datastore.
-spec put(pid(), Key::binary(), Value::binary()) -> ok | {error, any()}.
put(Client, Key, Value) ->
    bitcask_client:put(Client, Key, Value).


%% @doc Delete a key from a bitcask datastore.
-spec delete(pid(), Key::binary()) -> ok | {error, any()}.
delete(Client, Key) ->
    bitcask_client:delete(Client, Key).


