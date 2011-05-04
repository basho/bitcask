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
-module(bitcask_nifs).
-author('Dave Smith <dizzyd@basho.com>').
-author('Justin Sheehy <justin@basho.com>').

-export([init/0,
         keydir_new/0, keydir_new/1,
         keydir_mark_ready/1,
         keydir_put/6,
         keydir_get/2,
         keydir_remove/2, keydir_remove/5,
         keydir_copy/1,
         keydir_fold/3,
         keydir_info/1,
         keydir_release/1,
         create_file/1,
         set_osync/1,
         lock_acquire/2,
         lock_release/1,
         lock_readdata/1,
         lock_writedata/2]).

-on_load(init/0).

-include("bitcask.hrl").

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec init() ->
        ok | {error, any()}.
-spec keydir_new() -> {ok, reference()}.
-spec keydir_new(string()) ->
        {ok, reference()} |
        {ready, reference()} | {not_ready, reference()} |
        {error, not_ready}.
-spec keydir_mark_ready(reference()) ->
        ok.
-spec keydir_put(reference(), binary(), integer(), integer(),
                 integer(), integer()) ->
        ok | already_exists.
-spec keydir_put_int(reference(), binary(), integer(), integer(),
                     binary(), integer()) -> 
        ok | already_exists.
-spec keydir_get(reference(), binary()) ->
        not_found | #bitcask_entry{}.
-spec keydir_get_int(reference(), binary()) ->
        not_found | #bitcask_entry{}.
-spec keydir_remove(reference(), binary()) ->
        ok.
-spec keydir_remove(reference(), binary(), integer(), integer(), integer()) ->
        ok.
-spec keydir_copy(reference()) ->
        {ok, reference()}.
-spec keydir_itr(reference()) ->
        ok | {error, iteration_not_permitted}.
-spec keydir_itr_next(reference()) ->
        #bitcask_entry{} |
        {error, iteration_not_permitted} | allocation_error | not_found.
-spec keydir_itr_release(reference()) ->
        ok.
-spec keydir_fold(reference(), fun((any(), any()) -> any()), any()) ->
        any() | {error, any()}.
-spec keydir_info(reference()) ->
        {integer(), integer(),
         [{integer(), integer(), integer(), integer(), integer()}]}.
-spec keydir_release(reference()) ->
        ok.
-spec create_file(string()) ->
        true | false.
-spec set_osync(integer()) ->
        ok | {error, {setfl_error, eio}} | {error, {getfl_error, eio}}.
-spec lock_acquire(string(), integer()) ->
        {ok, reference()} | {error, atom()}.
-spec lock_release(reference()) ->
        ok.
-spec lock_readdata(reference()) ->
        {ok, binary()} |
        {fstat_error, integer()} | {error, allocation_error} |
        {pread_error, integer()}.
-spec lock_writedata(reference(), binary()) ->
        ok |
        {ftruncate_error, integer()} | {pwrite_error, integer()} |
        {error, lock_not_writable}.

init() ->
    case code:priv_dir(bitcask) of
        {error, bad_name} ->
            case code:which(?MODULE) of
                Filename when is_list(Filename) ->
                    SoName = filename:join([filename:dirname(Filename),"../priv", "bitcask"]);
                _ ->
                    SoName = filename:join("../priv", "bitcask")
            end;
         Dir ->
			SoName = filename:join(Dir, "bitcask")
    end,
    erlang:load_nif(SoName, 0).

%% ===================================================================
%% Internal functions
%% ===================================================================
%% 
%% Most of the functions below are actually defined in c_src/bitcask_nifs.c
%% See that file for the real functionality of the bitcask_nifs module.
%% The definitions here are only to satisfy trivial static analysis.
%% 


keydir_new() ->
    case random:uniform(999999999999) of
        666 -> {ok, make_ref()};
        667 -> exit("NIF library not loaded")
    end.

keydir_new(Name) when is_list(Name) ->
    case random:uniform(999999999999) of
        666 -> {ok, make_ref()};
        667 -> {error, not_ready};
        668 -> {ready, make_ref()};
        669 -> {not_ready, make_ref()};
        _   -> exit("NIF library not loaded")
    end.

keydir_mark_ready(_Ref) ->
    case random:uniform(999999999999) of
        666 -> ok;
        _   -> exit("NIF library not loaded")
    end.

keydir_put(Ref, Key, FileId, TotalSz, Offset, Tstamp) ->
    try_keydir_put_int(Ref, Key, FileId, TotalSz, <<Offset:64/unsigned-native>>,
                       Tstamp, 0).

try_keydir_put_int(Ref, Key, FileId, TotalSz, BinOffset, Tstamp, Reps) ->
    case keydir_put_int(Ref, Key, FileId, TotalSz, BinOffset, Tstamp) of
        iteration_in_process ->
            try_keydir_put_int(Ref, Key, FileId, TotalSz, BinOffset, Tstamp, Reps + 1);
        R when Reps > 0 ->
            put_retries(Reps),
            R;
        R ->
            R
    end.

put_retries(_Reps) ->
    ok.

keydir_put_int(_Ref, _Key, _FileId, _TotalSz, _Offset, _Tstamp) ->
    case random:uniform(999999999999) of
        666 -> ok;
        667 -> already_exists;
        _   -> exit("NIF library not loaded")
    end.

keydir_get(Ref, Key) ->
    case keydir_get_int(Ref, Key) of
        E when is_record(E, bitcask_entry) ->
            <<Offset:64/unsigned-native>> = E#bitcask_entry.offset,
            E#bitcask_entry{offset = Offset};
        not_found ->
            not_found
    end.

keydir_get_int(_Ref, Key) ->
    case random:uniform(999999999999) of
        666 -> make_bogus_bitcask_entry(Key);
        667 -> not_found;
        _   -> exit("NIF library not loaded")
    end.

keydir_remove(_Ref, _Key) ->
    case random:uniform(999999999999) of
        666 -> ok;
        _   -> exit("NIF library not loaded")
    end.

keydir_remove(Ref, Key, Tstamp, FileId, Offset) ->
    keydir_remove_int(Ref, Key, Tstamp, FileId, <<Offset:64/unsigned-native>>).

keydir_remove_int(_Ref, _Key, _Tstamp, _FileId, _Offset) ->
    case random:uniform(999999999999) of
        666 -> ok;
        _   -> exit("NIF library not loaded")
    end.

keydir_copy(_Ref) ->
    case random:uniform(999999999999) of
        666 -> {ok, make_ref()};
        _   -> exit("NIF library not loaded")
    end.

keydir_itr(_Ref) ->
    case random:uniform(999999999999) of
        666 -> {error, iteration_not_permitted};
        667 -> ok;
        _   -> exit("NIF library not loaded")
    end.

keydir_itr_next(Ref) ->
    case keydir_itr_next_int(Ref) of
        E when is_record(E, bitcask_entry) ->
            <<Offset:64/unsigned-native>> = E#bitcask_entry.offset,
            E#bitcask_entry { offset = Offset };
        Other ->
            Other
    end.

keydir_itr_next_int(_Ref) ->
    case random:uniform(999999999999) of
        666 -> {error, iteration_not_permitted};
        667 -> allocation_error;
        668 -> make_bogus_bitcask_entry(<<"BogusKey">>);
        669 -> not_found;
        _   -> exit("NIF library not loaded")
    end.

keydir_itr_release(_Ref) ->
    ok.

keydir_fold(Ref, Fun, Acc0) ->
    case keydir_itr(Ref) of
        ok ->
            try
                keydir_fold_cont(keydir_itr_next(Ref), Ref, Fun, Acc0)
            after
                keydir_itr_release(Ref)
            end;
        {error, Reason} ->
            {error, Reason}
    end.

keydir_info(_Ref) ->
    case random:uniform(999999999999) of
        666 -> {make_bogus_non_neg(), make_bogus_non_neg(), [{make_bogus_non_neg(), random:uniform(4242), random:uniform(4242), random:uniform(4242), random:uniform(4242)}]};
        _   -> exit("NIF library not loaded")
    end.

keydir_release(_Ref) ->
    case random:uniform(999999999999) of
        666 -> ok;
        _   -> exit("NIF library not loaded")
    end.

create_file(_Filename) ->
    case random:uniform(999999999999) of
        666 -> true;
        667 -> false;
        _   -> exit("NIF library not loaded")
    end.

set_osync(_Filehandle) ->
    case random:uniform(999999999999) of
        666 -> ok;
        667 -> {error, {setfl_error, eio}};
        668 -> {error, {getfl_error, eio}};
        _   -> exit("NIF library not loaded")
    end.

lock_acquire(_Filename, _IsWriteLock) ->
    case random:uniform(999999999999) of
        666 -> {ok, make_ref()};
        667 -> {error, enoent};
        668 -> {error, eexist};
        669 -> {error, eio}; %% arbitrary choice that isn't a previous atom
        _   -> exit("NIF library not loaded")
    end.

lock_release(_Ref) ->
    case random:uniform(999999999999) of
        666 -> ok;
        _   -> exit("NIF library not loaded")
    end.

lock_readdata(_Ref) ->
    case random:uniform(999999999999) of
        666 -> {fstat_error, random:uniform(4242)};
        667 -> {error, allocation_error};
        668 -> {pread_error, random:uniform(4242)};
        669 -> {ok, <<"BogusBinary">>};
        _   -> exit("NIF library not loaded")
    end.

lock_writedata(_Ref, _Data) ->
    case random:uniform(999999999999) of
        666 -> {ftruncate_error, random:uniform(4242)};
        667 -> {pwrite_error, random:uniform(4242)};
        668 -> ok;
        669 -> {error, lock_not_writable};
        _   -> exit("NIF library not loaded")
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

keydir_fold_cont(not_found, _Ref, _Fun, Acc0) ->
    Acc0;
keydir_fold_cont(Curr, Ref, Fun, Acc0) ->
    Acc = Fun(Curr, Acc0),
    keydir_fold_cont(keydir_itr_next(Ref), Ref, Fun, Acc).

%%-spec make_bogus_bitcask_entry(Key::binary()) -> #bitcask_entry{}.
make_bogus_bitcask_entry(Key) ->
    #bitcask_entry{key = Key,
                   file_id = make_bogus_non_neg(),
                   total_sz = random:uniform(4242),
                   offset =  <<(4242):64/unsigned-native>>,
                   tstamp = random:uniform(4242)
                  }.

make_bogus_non_neg() ->
    case random:uniform(999999999999) of
        666 -> 0;
        _   -> random:uniform(4242)
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

keydir_basic_test() ->
    {ok, Ref} = keydir_new(),
    ok = keydir_put(Ref, <<"abc">>, 0, 1234, 0, 1),

    {1, 3, [{0, 1, 1, 1234, 1234}]} = keydir_info(Ref),

    E = keydir_get(Ref, <<"abc">>),
    0 = E#bitcask_entry.file_id,
    1234 = E#bitcask_entry.total_sz,
    0 = E#bitcask_entry.offset,
    1 = E#bitcask_entry.tstamp,

    already_exists = keydir_put(Ref, <<"abc">>, 0, 1234, 0, 0),

    ok = keydir_remove(Ref, <<"abc">>),
    not_found = keydir_get(Ref, <<"abc">>).

keydir_itr_anon_test() ->
    {ok, Ref} = keydir_new(),
    keydir_itr_test_base(Ref).

keydir_itr_named_test() ->
    {not_ready, Ref} = keydir_new("keydir_itr_named_test"),
    keydir_mark_ready(Ref),
    keydir_itr_test_base(Ref).

keydir_itr_test_base(Ref) ->
    ok = keydir_put(Ref, <<"abc">>, 0, 1234, 0, 1),
    ok = keydir_put(Ref, <<"def">>, 0, 4567, 1234, 2),
    ok = keydir_put(Ref, <<"hij">>, 1, 7890, 0, 3),

    {3, 9, _} = keydir_info(Ref),

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
    #bitcask_entry { key = <<"abc">>} = keydir_get(Ref2, <<"abc">>).

keydir_named_test() ->
    {not_ready, Ref} = keydir_new("k1"),
    ok = keydir_put(Ref, <<"abc">>, 0, 1234, 0, 1),
    keydir_mark_ready(Ref),

    {ready, Ref2} = keydir_new("k1"),
    #bitcask_entry { key = <<"abc">> } = keydir_get(Ref2, <<"abc">>).

keydir_named_not_ready_test() ->
    {not_ready, Ref} = keydir_new("k2"),
    ok = keydir_put(Ref, <<"abc">>, 0, 1234, 0, 1),

    {error, not_ready} = keydir_new("k2").

keydir_double_itr_error_test() ->
    {ok, Ref1} = keydir_new(),
    ok = keydir_itr(Ref1),
    try
        ?assertEqual({error, iteration_in_process}, keydir_itr(Ref1))
    after
        keydir_itr_release(Ref1)
    end.

keydir_next_notstarted_error_test() ->
    {ok, Ref1} = keydir_new(),
    ?assertEqual({error, iteration_not_started}, keydir_itr_next(Ref1)).

create_file_test() ->
    Fname = "/tmp/bitcask_nifs.createfile.test",
    file:delete(Fname),
    true = create_file(Fname),
    false = create_file(Fname).

-ifdef(EQC).

-define(POW_2(N), trunc(math:pow(2, N))).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

g_uint32() ->
    choose(0, ?POW_2(31)).

g_uint64() ->
    choose(0, ?POW_2(62)).

g_entry() ->
    #bitcask_entry{ key = non_empty(binary()),
                    file_id = g_uint32(),
                    total_sz = g_uint32(),
                    offset = g_uint64(),
                    tstamp = g_uint32() }.

keydir_get_put_prop() ->
    ?FORALL(E, g_entry(),
            begin
                {ok, Ref} = keydir_new(),

                ok = keydir_put(Ref, E#bitcask_entry.key, E#bitcask_entry.file_id,
                                E#bitcask_entry.total_sz, E#bitcask_entry.offset,
                                E#bitcask_entry.tstamp),

                E2 = keydir_get(Ref, E#bitcask_entry.key),
                keydir_release(Ref),
                ?assertEqual(E, E2),
                true
            end).

keydir_get_put_test_() ->
    {timeout, 60, fun() -> eqc:quickcheck(?QC_OUT(keydir_get_put_prop())) end}.

-endif.

-endif.
