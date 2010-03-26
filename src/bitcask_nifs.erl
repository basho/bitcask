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
         keydir_get/1,
         keydir_put/1,
         keydir_remove/1]).

-on_load(init/0).

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

keydir_put(_Ref) ->
    "NIF library not loaded".

keydir_get(_Ref) ->
    "NIF library not loaded".

keydir_remove(_Ref) ->
    "NIF library not loaded".

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

keydir_test() ->
    {ok, Ref} = keydir_new(),
    ok = keydir_get(Ref).

-endif.
