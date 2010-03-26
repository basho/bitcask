%% -------------------------------------------------------------------
%%
%% bitcask: Eric Brewer-inspired key/value store
%%
%% Copyright (c) 2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% innostore is free software: you can redistribute it and/or modify
%% it under the terms of the GNU General Public License as published by
%% the Free Software Foundation, either version 2 of the License, or
%% (at your option) any later version.
%%
%% innostore is distributed in the hope that it will be useful,
%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
%% GNU General Public License for more details.
%%
%% You should have received a copy of the GNU General Public License
%% along with innostore.  If not, see <http://www.gnu.org/licenses/>.
%%
%% -------------------------------------------------------------------
-module(bitcask_nifs).

-export([keydir_new/0,
         keydir_put/6,
         keydir_get/2,
         keydir_remove/2]).

-on_load(init/0).

-include("bitcask.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

init() ->
    case code:priv_dir(bitcask) of
        {error, bad_name} ->
            SoName = filename:join("../priv", bitcask);
        Dir ->
            SoName = filename:join(Dir, bitcask)
    end,
    erlang:load_nif(SoName, 0).

keydir_new() ->
    "NIF library not loaded".

keydir_put(_Ref, _Key, _FileId, _ValueSz, _ValuePos, _Tstamp) ->
    "NIF library not loaded".

keydir_get(_Ref, _Key) ->
    "NIF library not loaded".

keydir_remove(_Ref, _Key) ->
    "NIF library not loaded".

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

keydir_test() ->
    {ok, Ref} = keydir_new(),
    ok = keydir_put(Ref, <<"abc">>, 0, 1234, 0, 1),

    E = keydir_get(Ref, <<"abc">>),
    0 = E#bitcask_entry.file_id,
    1234 = E#bitcask_entry.value_sz,
    0 = E#bitcask_entry.value_pos,
    1 = E#bitcask_entry.tstamp,

    already_exists = keydir_put(Ref, <<"abc">>, 0, 1234, 0, 0),

    ok = keydir_remove(Ref, <<"abc">>),
    not_found = keydir_get(Ref, <<"abc">>).


-endif.
