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
         keydir_remove/2,
         keydir_copy/1,
         keydir_itr/1,
         keydir_itr_next/1,
         keydir_fold/3]).

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

keydir_copy(_Ref) ->
    "NIF library not loaded".

keydir_itr(_Ref) ->
    "NIF library not loaded".

keydir_itr_next(_Itr) ->
    "NIF library not loaded".

keydir_fold(Ref, Fun, Acc0) ->
    Itr = keydir_itr(Ref),
    keydir_fold_cont(keydir_itr_next(Itr), Fun, Acc0).



%% ===================================================================
%% Internal functions
%% ===================================================================

keydir_fold_cont(not_found, _Fun, Acc0) ->
    Acc0;
keydir_fold_cont({Curr, Next}, Fun, Acc0) ->
    Acc = Fun(Curr, Acc0),
    keydir_fold_cont(keydir_itr_next(Next), Fun, Acc).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

keydir_basic_test() ->
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

keydir_itr_test() ->
    {ok, Ref} = keydir_new(),
    ok = keydir_put(Ref, <<"abc">>, 0, 1234, 0, 1),
    ok = keydir_put(Ref, <<"def">>, 0, 4567, 1234, 2),
    ok = keydir_put(Ref, <<"hij">>, 1, 7890, 0, 3),

    List = keydir_fold(Ref, fun(E, Acc) -> [ E | Acc] end, []),
    3 = length(List),
    true = lists:keymember(<<"abc">>, #bitcask_entry.key, List),
    true = lists:keymember(<<"def">>, #bitcask_entry.key, List),
    true = lists:keymember(<<"hij">>, #bitcask_entry.key, List).

keydir_copy_test() ->
    {ok, Ref1} = keydir_new(),
    ok = keydir_put(Ref1, <<"abc">>, 0, 1234, 0, 1),
    ok = keydir_put(Ref1, <<"def">>, 0, 4567, 1234, 2),
    ok = keydir_put(Ref1, <<"hij">>, 1, 7890, 0, 3),

    {ok, Ref2} = keydir_copy(Ref1),
    ?assertNot(keydir_itr(Ref1) == keydir_itr(Ref2)).


-endif.
