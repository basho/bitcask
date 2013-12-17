-module(foo).
-compile(export_all).

-define(FILE_SIZE, 1000).
-define(BITCASK, "test-bitcask-dir").

t() ->
    os:cmd("rm -rf " ++ ?BITCASK),
    BC = bc_open(true),
    puts(BC, {1,22},<<>>),
    puts(BC, {1,11},<<0>>),
    puts(BC,{1,22},<<>>),
    spawn(fun() -> fork_merge(BC), io:format("fork_merge done!\n") end),
    timer:sleep(1000),
    io:format("t pid ~p is done\n", [self()]).

bc_open(Writer) ->
  case Writer of
    true  -> catch bitcask:open(?BITCASK, [read_write, {max_file_size, ?FILE_SIZE}, {open_timeout, 1234}]);
    false -> catch bitcask:open(?BITCASK, [{open_timeout, 1234}])
  end.

puts(H, {K1, K2}, V) ->
  case lists:usort([ put(H, K, V) || K <- lists:seq(K1, K2) ]) of
    [ok]  -> ok;
    Other -> Other
  end.

put(H, K, V) ->
    ok = bitcask:put(H, <<K:32>>, V).

fork_merge(H) ->
  case bitcask:needs_merge(H) of
    {true, Files} -> catch io:format("Yo, gonna merge!\n"), bitcask_merge_worker:merge(?BITCASK, [], Files);
    false         -> not_needed
  end.

