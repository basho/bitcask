-module(bitcask_pr156).

-include("bitcask.hrl").

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(BITCASK, "/tmp/bc.pr156_regression1").
%% Number of keys used in the tests
-define(NUM_KEYS, 50).
%% max_file_size given to bitcask.
-define(FILE_SIZE, 400).

pr156_regression1_test_() ->
    %% This is a test for a setuid-bit regression late in the
    %% dev/testing cycle for PR 156.  It is adapted from a
    %% bitcask_pulse.erl counterexample.  Given the fragility of
    %% reusing eqc:check/2 if the model ever changes, I felt it best
    %% to make a standalone EUnit test that will be much more stable.
    %%
    %% Run the test 5 times, to try to avoid getting lucky in an
    %% unlikely race with the 'bitcask_merge_delete' server.
    {timeout, 120,
     fun() ->
             [ok = pr156_regression1(X) || X <- lists:seq(1,5)]
     end}.

pr156_regression2_test_() ->
    {timeout, 120,
     fun() ->
             [ok = pr156_regression2(X) || X <- lists:seq(1,5)]
     end}.

pr156_regression1(X) ->
    io:format(user, "pr156_regression1 ~p at ~p\n", [X, now()]),
    os:cmd("rm -rf " ++ ?BITCASK),
    V3 = goo({call,bitcask_pulse,bc_open,[true]}),
    _V7 = goo({call,bitcask_pulse,puts,[V3,{1,6},<<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>]}),
    _V13 = goo({call,bitcask_pulse,puts,[V3,{1,7},<<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>]}),
    _V15 = goo({call,bitcask_pulse,delete,[V3,2]}),
    _V16 = goo({call,bitcask_pulse,merge,[V3]}),
    _V20 = goo({call,bitcask_pulse,delete,[V3,1]}),
    _V21 = goo({call,bitcask_pulse,make_merge_txt,[{{335,217,333},90}]}),
    _V23 = goo({call,bitcask_pulse,puts,[V3,{3,9},<<0,0,0,0,0,0,0>>]}),
    _V24 = goo({call,bitcask_pulse,merge,[V3]}),
    _V25 = goo({call,bitcask_pulse,bc_close,[V3]}),
    V31 = goo({call,bitcask_pulse,bc_open,[true]}),
    V32 = goo({call,bitcask_pulse,fold_keys,[V31]}),
    _V33 = goo({call,bitcask_pulse,bc_close,[V31]}),
    Res = lists:sort(V32),
    ?assertEqual([3,4,5,6,7,8,9], Res),
    ok.

pr156_regression2(X) ->
    io:format(user, "pr156_regression1 ~p at ~p\n", [X, now()]),
    os:cmd("rm -rf " ++ ?BITCASK),
    V1 = goo({call,bitcask_pulse,bc_open,[true,{true,{413,255,375},100}]}),
    _V3 = goo({call,bitcask_pulse,puts,[V1,{1,8},<<0,0,0,0,0>>]}),
    _V8 = goo({call,bitcask_pulse,bc_close,[V1]}),
    V15 = goo({call,bitcask_pulse,bc_open,[true,{true,{133,23,388},1}]}),
    _V21 = goo({call,bitcask_pulse,puts,[V15,{1,20},<<0,0,0,0,0>>]}),
    _V43 = goo({call,bitcask_pulse,merge,[V15]}),
    _V46 = goo({call,bitcask_pulse,puts,[V15,{1,2},<<0,0,0,0,0,0,0,0,0,0,0,0>>]}),
    _V49 = goo({call,bitcask_pulse,bc_close,[V15]}),
    V50 = goo({call,bitcask_pulse,bc_open,[true,{true,{414,120,104},4}]}),
    _V51 = goo({call,bitcask_pulse,delete,[V50,1]}),
    _V52 = goo({call,bitcask_pulse,merge,[V50]}),
    _V56 = goo({call,bitcask_pulse,delete,[V50,2]}),
    _V60 = goo({call,bitcask_pulse,bc_close,[V50]}),
    V84 = goo({call,bitcask_pulse,bc_open,[true,{true,{291,81,8},40}]}),
    _V90 = goo({call,bitcask_pulse,merge,[V84]}),
    _V93 = goo({call,bitcask_pulse,bc_close,[V84]}),
    V95 = goo({call,bitcask_pulse,bc_open,[true,{true,{190,6,52},1}]}),
    [not_found,not_found] = goo({call,bitcask_pulse,gets,[V95,{1,2}]}),
    ok.

nice_key(K) ->
    list_to_binary(io_lib:format("kk~2.2.0w", [K])).

un_nice_key(<<"kk", Num:2/binary>>) ->
    list_to_integer(binary_to_list(Num)).

put(H, K, V) ->
  ok = bitcask:put(H, nice_key(K), V).

get(H, K) ->
  bitcask:get(H, nice_key(K)).

needs_merge_wrapper(H) ->
    case check_no_tombstones(H, ok) of
        ok ->
            bitcask:needs_merge(H);
        Else ->
            {needs_merge_wrapper_error, Else}
    end.

check_no_tombstones(Ref, Good) ->
    Res = bitcask:fold_keys(Ref, fun(K, Acc0) -> [K|Acc0] end,
                            [], -1, -1, true),
    case [X || {tombstone, _} = X <- Res] of
        [] ->
            Good;
        Else ->
            {check_no_tombstones, Else}
    end.

make_merge_txt(Dir, Seed, Probability) ->
    random:seed(Seed),
    case filelib:is_dir(Dir) of
        true ->
            DataFiles = filelib:wildcard("*.data", Dir),
            {ok, FH} = file:open(Dir ++ "/merge.txt", [write]),
            [case random:uniform(100) < Probability of
                 true ->
                     io:format(FH, "~s\n", [DF]);
                 false ->
                     ok
             end || DF <- DataFiles],
            ok = file:close(FH);
        false ->
            ok
    end.

goo({_, _, bc_open, [_ReadWrite]}) ->
    bitcask:open(?BITCASK, [read_write, {max_file_size, ?FILE_SIZE}, {open_timeout, 1234}]);
goo({_, _, bc_open, [_ReadWrite,{DoMergeP,X,Y}]}) ->
    if DoMergeP ->
            make_merge_txt(?BITCASK, X, Y);
       true ->
            ok
    end,
    bitcask:open(?BITCASK, [read_write, {max_file_size, ?FILE_SIZE}, {open_timeout, 1234}]);
goo({_, _, bc_close, [H]}) ->
    bitcask:close(H);
goo({_, _, puts, [H, {K1, K2}, V]}) ->
  case lists:usort([ put(H, K, V) || K <- lists:seq(K1, K2) ]) of
    [ok]  -> ok;
    Other -> throw({line, ?LINE, Other})
  end;
goo({_, _, gets, [H, {Start, End}]}) ->
    [get(H, K) || K <- lists:seq(Start, End)];
goo({_, _, delete, [H, K]}) ->
    ok = bitcask:delete(H, nice_key(K));
goo({_, _, merge, [H]}) ->
  case needs_merge_wrapper(H) of
    {true, Files} ->
      case catch bitcask:merge(?BITCASK, [], Files) of
        {'EXIT', Err} -> Err;
        R             -> R
      end;
    false -> not_needed
  end;
goo({_, _, make_merge_txt, [{X,Y}]}) ->
    make_merge_txt(?BITCASK, X, Y);
goo({_, _, fold_keys, [H]}) ->
    bitcask:fold_keys(H, fun(#bitcask_entry{key = Kb}, Ks) -> [un_nice_key(Kb)|Ks] end, []).
