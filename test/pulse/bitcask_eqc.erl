%%% File        : bitcask_eqc.erl
%%% Author      : Ulf Norell, Hans Svensson
%%% Description : Bitcask stress testing
%%% Created     : 19 Mar 2012 by Ulf Norell
-module(bitcask_eqc).

%% The while module is ifdef:ed, rebar should set PULSE
-ifdef(PULSE).

-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-include("../../include/bitcask.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module,
  [{application, pulse_application}]}).
%% The following functions contains side_effects but are run outside
%% PULSE, i.e. PULSE needs to leave them alone
-compile({pulse_skip,[{prop_pulse_test_,0},{really_delete_bitcask,0},{copy_bitcask_app,0}]}).
-compile({pulse_no_side_effect,[{file,'_','_'}, {erlang, now, 0}]}).

%% The token module keeps track of the currently used directory for
%% bitcask. Each test uses a fresh directory!
-define(BITCASK, token:get_name()).
%% Number of keys used in the tests
-define(NUM_KEYS, 50).
%% max_file_size given to bitcask.
-define(FILE_SIZE, 1000).

%% Used for output within EUnit...
-define(QC_FMT(Fmt, Args),
        io:format(user, Fmt, Args)).

%% And to force EUnit to output QuickCheck output...
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> ?QC_FMT(Str, Args) end, P)).


-record(state,
  { handle
  , is_writer = true
  , did_fork_merge = false
  , readers = []
  }).

%% The initial state.
initial_state() ->
  #state{}.

%% Generators
key()   -> choose(1, ?NUM_KEYS).
value() -> ?LET(Bin, noshrink(binary()), ?SHRINK(Bin, [<< <<0>> || <<_>> <= Bin >>])).

key_pair() ->
  ?LET({A, B}, {key(), key()},
    list_to_tuple(lists:sort([A, B]))).

not_commands(Module, State) ->
  ?LET(Cmds, commands(Module, State),
    uncommand(Cmds)).

%% Command generator. S is the state.
command(S) ->
  frequency(
    [ {2, {call, ?MODULE, fork, [not_commands(?MODULE, #state{ is_writer = false })]}}
      || S#state.is_writer ] ++
    [ {10, {call, ?MODULE, get, [S#state.handle, key()]}}
      || S#state.handle /= undefined ] ++
    [ {20, {call, ?MODULE, put, [S#state.handle, key(), value()]}}
      || S#state.is_writer, S#state.handle /= undefined ] ++
    [ {20, {call, ?MODULE, puts, [S#state.handle, key_pair(), value()]}}
      || S#state.is_writer, S#state.handle /= undefined ] ++
    [ {6, {call, ?MODULE, delete, [S#state.handle, key()]}}
      || S#state.is_writer, S#state.handle /= undefined ] ++
    [ {2, {call, ?MODULE, bc_open, [S#state.is_writer]}}
      || S#state.handle == undefined ] ++
    [ {2, {call, ?MODULE, sync, [S#state.handle]}}
      || S#state.handle /= undefined ] ++
    [ {1, {call, ?MODULE, fold_keys, [S#state.handle]}}
      || S#state.handle /= undefined ] ++
    [ {1, {call, ?MODULE, fold, [S#state.handle]}}
      || S#state.handle /= undefined ] ++
    [ {1, {call, ?MODULE, bc_close, [S#state.handle]}}
      || S#state.handle /= undefined ] ++
    %% [ {1, {call, ?MODULE, merge, [S#state.handle]}}
    %%   || S#state.is_writer, not S#state.did_fork_merge, S#state.handle /= undefined ] ++
    [ {1, {call, ?MODULE, fork_merge, [S#state.handle]}}
      || S#state.is_writer, S#state.handle /= undefined ] ++
    [ {0, {call, ?MODULE, join_reader, [elements(S#state.readers)]}}
      || S#state.is_writer, S#state.readers /= []] ++
    [ {1, {call, ?MODULE, kill, [elements([bitcask_merge_worker|S#state.readers])]}}
      || S#state.is_writer, S#state.handle /= undefined ] ++
    [ {2, {call, ?MODULE, needs_merge, [S#state.handle]}}
      || S#state.is_writer, S#state.handle /= undefined ] ++
    []).

%% Precondition, checked before a command is added to the command sequence.
precondition(S, {call, _, fork, _}) ->
  S#state.is_writer;
precondition(S, {call, _, get, [H, _]}) ->
  S#state.handle == H;
precondition(S, {call, _, puts, [H, _, _]}) ->
  S#state.is_writer andalso S#state.handle == H;
precondition(S, {call, _, put, [H, _, _]}) ->
  S#state.is_writer andalso S#state.handle == H;
precondition(S, {call, _, delete, [H, _]}) ->
  S#state.is_writer andalso S#state.handle == H;
precondition(S, {call, _, fork_merge, [H]}) ->
  S#state.is_writer andalso S#state.handle == H;
precondition(S, {call, _, merge, [H]}) ->
  S#state.is_writer andalso not S#state.did_fork_merge andalso S#state.handle == H;
precondition(S, {call, _, needs_merge, [H]}) ->
  S#state.is_writer andalso S#state.handle == H;
precondition(S, {call, _, join_reader, [R]}) ->
  S#state.is_writer andalso lists:member(R, S#state.readers);
precondition(S, {call, _, fold_keys, [H]}) ->
  S#state.handle == H;
precondition(S, {call, _, fold, [H]}) ->
  S#state.handle == H;
precondition(S, {call, _, sync, [H]}) ->
  S#state.handle == H;
precondition(S, {call, _, kill, [Pid]}) ->
  S#state.is_writer andalso S#state.handle /= undefined andalso
  (Pid == bitcask_merge_worker orelse lists:member(Pid, S#state.readers));
precondition(S, {call, _, bc_close, [H]}) ->
  S#state.handle == H;
precondition(S, {call, _, bc_open, [Writer]}) ->
  %% The writer can open for reading but not the other way around.
  S#state.is_writer >= Writer andalso S#state.handle == undefined.

%% Next state transformation, S is the current state and V is the result of the
%% command.
next_state(S, H, {call, _, bc_open, _}) ->
  S#state{ handle = H };
next_state(S, _, {call, _, bc_close, _}) ->
  S#state{ handle = undefined };
next_state(S, _, {call, _, join_reader, [R]}) ->
  S#state{ readers = S#state.readers -- [R] };
next_state(S, _, {call, _, fork_merge, _}) ->
  S#state{ did_fork_merge = true };
next_state(S, P, {call, _, fork, _}) ->
  S#state{ readers = [ P | S#state.readers ] };
next_state(S, _, {call, _, kill, [Pid]}) ->
  S#state{ readers = S#state.readers -- [Pid] };
next_state(S, _V, {call, _, _, _}) ->
  S.

eq(X, X) -> true;
eq(X, Y) -> {X, '/=', Y}.

%% Postcondition, checked after the command has been evaluated. V is the result
%% of the command. Note: S is the state before next_state/3 has been called.
postcondition(_S, {call, _, put, _}, V) ->
  eq(V, ok);
postcondition(_S, {call, _, puts, _}, V) ->
  eq(V, ok);
postcondition(_S, {call, _, delete, _}, V) ->
  eq(V, ok);
postcondition(_S, {call, _, fork_merge, _}, V) ->
  case V of
    ok             -> true;
    not_needed     -> true;
    already_queued -> true;   %% another fork_merge might be in progress
    {'EXIT', {killed, _}} -> true;
    {'EXIT', {noproc, _}} -> true;
    _              -> {fork_merge, V}
  end;
postcondition(_S, {call, _, merge, _}, V) ->
  case V of
    ok                                 -> true;
    not_needed                         -> true;
    {error, {merge_locked, locked, _}} -> true;  %% a fork_merge might be in progress
    _                                  -> {merge, V}
  end;
postcondition(_S, {call, _, join_reader, _}, V) ->
   eq(V, ok);
postcondition(_S, {call, _, fold, _}, V) ->
  case V of
    {error, max_retries_exceeded_for_fold} -> true;
    _ when is_list(V)                      -> true;
    _                                      -> {fold, V}
  end;
postcondition(_S, {call, _, fold_keys, _}, V) ->
  case V of
    {error, _} -> V;
    _          -> true
  end;
postcondition(_S, {call, _, needs_merge, _}, V) ->
  case V of
    {true, _Files} -> true;
    false          -> true;
    _              -> {needs_merge, V}
  end;
postcondition(_S, {call, _, bc_open, _}, V) ->
  case V of
    _ when is_reference(V)                  -> true;
    {'EXIT', {{badmatch,{error,enoent}},_}} -> true;
    {error, timeout}                        -> true;
    _                                       -> {bc_open, V}
  end;
postcondition(_S, {call, _, get, _}, V) ->
  case V of
    {ok, _}   -> true;
    not_found -> true;
    _         -> {get, V}
  end;
postcondition(_S, {call, _, _, _}, _V) ->
  true.

%% Slave node preparation
%%
%% Each test is run on a freshly started node, to avoid problems with
%% cleanup, etc.

node_name() ->
  list_to_atom(lists:concat([slave_name(), "@", host()])).

slave_name() ->
  list_to_atom(lists:concat([hd(string:tokens(atom_to_list(node()),"@")), "-tester"])).

host() ->
  hd(tl(string:tokens(atom_to_list(node()),"@"))).

%% Generate a most likely unique node name
unique_name() ->
  {A, B, C} = erlang:now(),
  list_to_atom(lists:concat([integer_to_list(A), "-",
                             integer_to_list(B), "-",
                             integer_to_list(C)])).

%% Note, rebar starts a shell without distribution, possibly start
%% net_kernel here
start_node(Verbose) ->
  case node() of
    'nonode@nohost' -> net_kernel:start([unique_name(), shortnames]);
    _ -> ok
  end,
  stop_node(),
  {ok, _} = slave:start(host(), slave_name(), "-pa ../../../ebin " ++
                          lists:append(["-detached" || not Verbose ])),
  ok.

stop_node() ->
  slave:stop(node_name()).

run_on_node(Verbose, M, F, A) ->
  start_node(Verbose),
  rpc:call(node_name(), M, F, A).

%% Muting the QuickCheck license printout from the slave node
mute(true,  Fun) -> Fun();
mute(false, Fun) -> mute:run(Fun).

%%
%% The actual code of the property, run on remote node via rpc:call above
%%
run_commands_on_node(Cmds, Seed, Verbose) ->
  mute(Verbose, fun() ->
    event_logger:start_link(),
    pulse:start(),
    bitcask_nifs:set_pulse_pid(utils:whereis(pulse)),
    error_logger:tty(false),
    error_logger:add_report_handler(handle_errors),
    token:next_name(),
    event_logger:start_logging(),
    X =
    try
      {H, S, Res, PidRs, Trace} = pulse:run(fun() ->
          %% pulse_application_controller:start({application, kernel, []}),
          application:start(bitcask),
          receive after 1000000 -> ok end,
          OldVerbose = pulse:verbose([ all || Verbose ]),
          {H, S, R} = run_commands(?MODULE, Cmds),
          Pids = lists:usort(S#state.readers),
          PidRs = fork_results(Pids),
          receive after 1000000 -> ok end,
          pulse:verbose(OldVerbose),
          Trace = event_logger:get_events(),
          receive after 1000000 -> ok end,
          exit(pulse_application_controller, shutdown),
          {H, S, R, PidRs, Trace}
        end, [{seed, Seed},
              {strategy, unfair}]),
      Schedule = pulse:get_schedule(),
      Errors = gen_event:call(error_logger, handle_errors, get_errors),
      {H, S, Res, PidRs, Trace, Schedule, Errors}
    catch
      _:Err ->
        {'EXIT', Err}
    end,
    really_delete_bitcask(),
    X end).

prop_pulse() ->
  prop_pulse(false).

prop_pulse(Verbose) ->
  ?FORALL(Cmds, ?LET(Cmds, more_commands(2, commands(?MODULE)), shrink_commands(Cmds)),
  ?FORALL(Seed, pulse:seed(),
  begin
    case run_on_node(Verbose, ?MODULE, run_commands_on_node, [Cmds, Seed, Verbose]) of
      {'EXIT', Err} ->
        equals({'EXIT', Err}, ok);
      {H, S, Res, PidRs, Trace, Schedule, Errors} ->
        ?WHENFAIL(
          ?QC_FMT("\nState: ~p\n", [S]),
          aggregate(zipwith(fun command_data/2, Cmds, H),
          measure(schedule, length(Schedule),
          %% In the end we check four things:
          %% - That the root process (the writer) returns ok (passes all postconditions)
          %% - That all forked processes returns ok
          %% - That there are no unmasked errors in the error_log
          %% - That all read actions (gets, fold, and fold_keys) return possible values
          conjunction(
            [ {0, equals(Res, ok)}
            | [ {Pid, equals(R, ok)} || {Pid, R} <- PidRs ] ] ++
            [ {errors, equals(Errors, [])}
            , {events, check_trace(Trace)} ]))))
    end
  end)).

%% A EUnit wrapper for the QuickCheck property
prop_pulse_test_() ->
  {timeout, 1000000,
   fun() ->
       copy_bitcask_app(),
       ?assert(eqc:quickcheck(eqc:testing_time(10,?QC_OUT(prop_pulse()))))
   end}.

%% Needed since rebar fails miserably in setting up the .eunit test directory
copy_bitcask_app() ->
  os:cmd("cp ../ebin/bitcask.app ."),
  ok.

%% Using eqc_temporal to keep track of possible values for keys.
%%
%% The Trace contains entries for start and finish of operations. For
%% put(Key, Val) (and delete) we consider Key to have either the old
%% or the new value until the operation has finished. Likewise, a get
%% (or a fold/fold_keys) may see any of the potential values if the
%% operation overlaps with a put/delete.
check_trace(Trace) ->
  %% Turn the trace into a temporal relation
  Events = eqc_temporal:from_timed_list(Trace),

  %% The Calls relation contains {call, Pid, Call} whenever Call by Pid is in
  %% progress.
  Calls  = eqc_temporal:stateful(
      fun({call, Pid, Call}) -> [{call, Pid, Call}] end,
      fun({call, Pid, _Call}, {result, Pid, _}) -> [] end,
      Events),

  %% The initial value for each key is not_found.
  AllKeys = lists:usort(fold(
              fun({put, _, K, _}) -> K;
                 ({get, _, K})    -> K;
                 ({delete, _, K}) -> K end, Trace)),
  InitialPuts = eqc_temporal:elems(eqc_temporal:ret([{K, not_found} || K <- AllKeys])),
  %% For each put or delete the Puts relation contains the appropriate key/value pair.
  Puts    = eqc_temporal:union(InitialPuts,
            eqc_temporal:map(fun({call, _Pid, {put, _H, K, V}}) -> {K, V};
                                ({call, _Pid, {delete, _H, K}}) -> {K, not_found} end, Calls)),
  %% DonePut contains {freeze, K, V} when a put or delete has completed.
  DonePut = eqc_temporal:map(
              fun({K, V}) -> {freeze, K, V} end,
              eqc_temporal:subtract(eqc_temporal:shift(1, Puts), Puts)),
  %% Values contains {K, V} whenever V is a possible value for K. We compute it
  %% by adding {K, V} whenever it appears in Puts and removing any {K, V1} as
  %% soon as we see {freeze, K, V2} (with V1 /= V2) in DonePut. In effect, when
  %% a put/delete call is started both the old and the new value appears in
  %% Values and when the call has completed only the new value does.
  Values  = eqc_temporal:stateful(
              fun({K, V}) -> {K, V} end,
              fun({K, V1}, {freeze, K, V2}) when V1 /= V2 -> [] end,
              eqc_temporal:union(Puts, DonePut)),
  %% Build an orddict for key/values for efficiency reasons.
  ValueDict = eqc_temporal:map(
      fun(KVs) ->
        lists:foldr(fun({K,V}, Dict) -> orddict:append(K, V, Dict) end, orddict:new(), KVs)
      end,
      eqc_temporal:set(Values)),

  %% The Reads relation keeps track of get, fold_keys and fold calls with the
  %% purpose of checking that they return something sensible. For a get call we
  %% check that the result was a possible value at some point during the
  %% evaluation of the call. The fold and fold_keys are checked analogously.
  Reads = eqc_temporal:stateful(
      %% Starting a read call. For get we aggregate the list of possible values
      %% for the corresponding key seen during the call. For fold we keep a
      %% dictionary mapping keys to lists of possible values and for fold_keys
      %% we keep a dictionary mapping keys to a list of found or not_found.
      fun({call, Pid, {get, _H, K}})    -> {get, Pid, K, []};
         ({call, Pid, {fold_keys, _H}}) -> {fold_keys, Pid, orddict:new()};
         ({call, Pid, {fold, _H}})      -> {fold, Pid, orddict:new()} end,

      fun
        %% Update a get call with a new set of possible values.
         ({get, Pid, K, Vs}, {values, Vals}) ->
          %% Get all possible values for K
          Ws = case orddict:find(K, Vals) of
                 error    -> [];
                 {ok, Us} -> lists:sort(Us)
               end,
          %% Add them to the previously seen values
          VWs = lists:umerge(Vs, Ws),
          false = VWs == Vs,  %% Be careful not to update the state if nothing
                              %% changed since eqc_temporal:stateful allows you
                              %% to change the state several times during the
                              %% same time slot.
          [{get, Pid, K, VWs}];

        %% Update a fold_keys call
         ({fold_keys, Pid, Vals1}, {values, Vals2}) ->
          %% We don't need the actual values for fold_keys, just found or
          %% not_found.
          FoundOrNotFound = fun(not_found) -> not_found;
                               (_)         -> found end,
          %% Merge the new values and the old values
          Vals = orddict:merge(
                   fun(_, Vs1, Vs2) -> lists:umerge(Vs1, Vs2) end,
                   Vals1,
                   orddict:map(fun(_, Vs) ->
                      lists:usort(lists:map(FoundOrNotFound, Vs))
                    end, Vals2)),
          %% Make sure to not do any identity updates
          false = orddict:to_list(Vals) == orddict:to_list(Vals1),
          [{fold_keys, Pid, Vals}];

        %% Update a fold call
         ({fold, Pid, Vals1}, {values, Vals2}) ->
          Vals = orddict:merge(
                   fun(_, Vs1, Vs2) ->
                       lists:umerge(Vs1, lists:sort(Vs2))
                   end, Vals1, Vals2),
          false = orddict:to_list(Vals) == orddict:to_list(Vals1),
          [{fold, Pid, Vals}];

        %% Check a call to get
         ({get, Pid, K, Vs}, {result, Pid, R}) ->
            V = case R of not_found -> not_found;
                          {ok, U}   -> U end,
            case lists:member(V, Vs) of
              true  -> [];                      %% V is a good result
              false -> [{bad, {get, K, Vs, V}}] %% V is a bad result!
            end;
        %% Check a call to fold_keys
         ({fold_keys, Pid, Vals}, {result, Pid, Keys}) ->
            %% check_fold_keys_result checks that
            %%  K in Keys     ==> found     in Vals[K] and
            %%  K not in Keys ==> not_found in Vals[K]
            case check_fold_keys_result(orddict:to_list(Vals), lists:sort(Keys)) of
              true  -> [];
              false -> [{bad, {fold_keys, orddict:to_list(Vals), Keys}}]
            end;
        %% Check a call to fold
         ({fold, Pid, Vals}, {result, Pid, KVs}) ->
            %% check_fold_result checks that
            %%  {K, V} in KVs ==> V         in Vals[K] and
            %%  K not in KVs  ==> not_found in Vals[K]
            case check_fold_result(orddict:to_list(Vals), lists:sort(KVs)) of
              true  -> [];
              false -> [{bad, {fold, orddict:to_list(Vals), KVs}}]
            end
        end,
      eqc_temporal:union(Events, eqc_temporal:map(fun(D) -> {values, D} end, ValueDict))),

  %% Filter out the bad stuff from the Reads relation.
  Bad = eqc_temporal:map(fun(X={bad, _}) -> X end, Reads),

  ?WHENFAIL(begin
    ?QC_FMT("Events:\n~p\n", [Events]),
    ?QC_FMT("Bad:\n~p\n", [Bad]) end,
    %% There shouldn't be any Bad stuff
    eqc_temporal:is_false(Bad)).

check_fold_result([{K, Vs}|Expected], [{K, V}|Actual]) ->
  lists:member(V, Vs) andalso check_fold_result(Expected, Actual);
check_fold_result([{_, Vs}|Expected], Actual) ->
  lists:member(not_found, Vs) andalso check_fold_result(Expected, Actual);
check_fold_result([], []) ->
  true.

check_fold_keys_result([{K, Vs}|Expected], [K|Actual]) ->
  lists:member(found, Vs) andalso check_fold_keys_result(Expected, Actual);
check_fold_keys_result([{_, Vs}|Expected], Actual) ->
  lists:member(not_found, Vs) andalso check_fold_keys_result(Expected, Actual);
check_fold_keys_result([], []) ->
  true.

%% Presenting command data statistics in a nicer way
command_data({set, _, {call, _, merge, _}}, {_S, V}) ->
  case V of
    {error, {merge_locked, _, _}} -> {merge, locked};
    _                             -> {merge, V}
  end;
command_data({set, _, {call, _, fork_merge, _}}, {_S, V}) ->
  case V of
    {'EXIT', _} -> {fork_merge, 'EXIT'};
    _           -> {fork_merge, V}
  end;
command_data({set, _, {call, _, bc_open, _}}, {_S, V}) ->
  case V of
    {'EXIT', _} -> {bc_open, 'EXIT'};
    {error, Err} -> {bc_open, Err};
    _  when is_reference(V) -> bc_open
  end;
command_data({set, _, {call, _, needs_merge, _}}, {_S, V}) ->
  case V of
    {true, _} -> {needs_merge, true};
    false     -> {needs_merge, false}
  end;
command_data({set, _, {call, _, kill, [Pid]}}, {_S, _V}) ->
  case Pid of
    bitcask_merge_worker -> {kill, merger};
    _                    -> {kill, reader}
  end;
command_data({set, _, {call, _, Fun, _}}, {_S, _V}) ->
  Fun.

%% Wait for all forks to return their results
fork_results(Pids) ->
  [ receive
      {Pid, done, R} -> {I, R}
    %% after ?TIMEOUT -> {I, timeout}
    end || {I, Pid} <- lists:zip(lists:seq(1, length(Pids)), Pids) ].

%% Implementation of a the commands
-define(CHECK_HANDLE(H, X, Do),
  case H of
    _ when is_reference(H) -> Do;
    _ -> X
  end).

-define(LOG(Tag, MkCall),
  event_logger:event({call, self(), Tag}),
  __Result = MkCall,
  event_logger:event({result, self(), __Result}),
  __Result).

fork(Cmds) ->
  ?LOG(fork, begin
  Mama = self(),
  spawn(fun() ->
    {_, S, R} = run_commands(?MODULE, recommand(Cmds)),
    bc_close(S#state.handle),
    Mama ! {self(), done, R}
  end) end).

get(H, K) ->
  ?LOG({get, H, K},
  ?CHECK_HANDLE(H, not_found, bitcask:get(H, <<K:32>>))).

put(H, K, V) ->
  ?LOG({put, H, K, V},
  ?CHECK_HANDLE(H, ok, bitcask:put(H, <<K:32>>, V))).

puts(H, {K1, K2}, V) ->
  case lists:usort([ put(H, K, V) || K <- lists:seq(K1, K2) ]) of
    [ok]  -> ok;
    Other -> Other
  end.

delete(H, K) ->
  ?LOG({delete, H, K},
  ?CHECK_HANDLE(H, ok, bitcask:delete(H, <<K:32>>))).

fork_merge(H) ->
  ?LOG({fork_merge, H},
  ?CHECK_HANDLE(H, not_needed,
  case bitcask:needs_merge(H) of
    {true, Files} -> catch bitcask_merge_worker:merge(?BITCASK, [], Files);
    false         -> not_needed
  end)).

merge(H) ->
  ?CHECK_HANDLE(H, not_needed,
  case bitcask:needs_merge(H) of
    {true, Files} ->
      case catch bitcask:merge(?BITCASK, [], Files) of
        {'EXIT', Err} -> Err;
        R             -> R
      end;
    false -> not_needed
  end).

kill(Pid) ->
  ?LOG({kill, Pid}, (catch exit(Pid, kill))).

needs_merge(H) ->
  ?LOG({needs_merge, H},
  ?CHECK_HANDLE(H, false, bitcask:needs_merge(H))).

join_reader(ReaderPid) ->
  receive
    {ReaderPid, done, Res} -> Res
  end.

sync(H) ->
  ?LOG({sync, H},
  ?CHECK_HANDLE(H, ok, bitcask:sync(H))).

fold(H) ->
  ?LOG({fold, H},
  ?CHECK_HANDLE(H, [], bitcask:fold(H, fun(<<K:32>>, V, Acc) -> [{K,V}|Acc] end, []))).

fold_keys(H) ->
  ?LOG({fold_keys, H},
  ?CHECK_HANDLE(H, [], bitcask:fold_keys(H, fun(#bitcask_entry{key = <<K:32>>}, Ks) -> [K|Ks] end, []))).

bc_open(Writer) ->
  ?LOG({open, Writer},
  case Writer of
    true  -> catch bitcask:open(?BITCASK, [read_write, {max_file_size, ?FILE_SIZE}, {open_timeout, 1}]);
    false -> catch bitcask:open(?BITCASK, [{open_timeout, 1}])
  end).

bc_close(H)    ->
  ?LOG({close, H},
  ?CHECK_HANDLE(H, ok, bitcask:close(H))).

%% Convenience functions for running tests

test() ->
  test({20, sec}).

test(N) when is_integer(N) ->
  quickcheck(numtests(N, prop_pulse()));
test({Time, sec}) ->
  quickcheck(eqc:testing_time(Time, prop_pulse()));
test({Time, min}) ->
  test({Time * 60, sec});
test({Time, h}) ->
  test({Time * 60, min}).

check() ->
  check(current_counterexample()).

verbose() ->
  verbose(current_counterexample()).

verbose(CE) ->
  erlang:put(verbose, true),
  Ok = check(CE),
  erlang:put(verbose, false),
  Ok.

check(CE) ->
  check(on_output(fun("OK" ++ _, []) -> ok; (Fmt, Args) -> io:format(Fmt, Args) end,
                  prop_pulse(true == erlang:get(verbose))),
        mk_counterexample(CE)).

recheck() ->
  recheck(prop_pulse()).

%% Custom shrinking
%%
%% Applied after normal shrinking or manually via custom_shrink/1

shrink_commands(Cmds) ->
  ?LET(ok, ?SHRINK(ok, begin io:format("|"), [] end),
    shrink_commands1(Cmds)).

shrink_commands1(Cmds) ->
  ?SHRINK(Cmds, lists:map(fun shrink_commands1/1, shrink(Cmds))).

check_preconditions([{init, S}|Cmds]) ->
  check_preconditions(S, Cmds);
check_preconditions(Cmds) ->
  check_preconditions(initial_state(), Cmds).

check_preconditions(_S, []) -> true;
check_preconditions(S, [{set, X, Call}|Cmds]) ->
  precondition(S, Call) andalso
  check_preconditions(next_state(S, X, Call), Cmds).

shrink(Cmds) ->
  [ C || C <- shrink1(Cmds), check_preconditions(C) ].

shrink_nat(0) -> [];
shrink_nat(1) -> [0];
shrink_nat(N) ->
  [0, N div 2, N - 1].

shrink_pos(N) ->
  [ M + 1 || M <- shrink_nat(N - 1) ].

shrink_bounds({A, B}) ->
  [ {A, A + Delta} || Delta <- shrink_nat(B - A) ] ++
  [ {B - Delta, B} || Delta <- shrink_nat(B - A) ] ++
  [ {Delta, B - A + Delta} || Delta <- shrink_pos(A) ].

shrink1(Cmds) ->

  %% Shrink puts to put (identity)
  shrink_sublist(1, Cmds, fun
    ([{set, X, {call, ?MODULE, puts, [H, {K, K}, V]}}]) ->
      [ [{set, X, {call, ?MODULE, put, [H, K, V]}}] ];
    (_) -> [] end) ++

  %% Move forks as late as possible
  shrink_tails(Cmds, fun
    ([Cmd={set, _, {call, _, fork, _}} | Cmds_ ]) ->
      [ Cmds0 ++ [Cmd] ++ Cmds1
        || I <- lists:reverse(lists:seq(1, length(Cmds_))),
           {Cmds0, Cmds1} <- [lists:split(I, Cmds_)],
           lists:any(fun({set, _, {call, _, Fun, _}}) -> Fun /= fork end, Cmds0) ];
    (_) -> [] end) ++

  %% Merge two puts into one
  shrink_sublist(2, Cmds, fun
    ([{set, X, {call, ?MODULE, puts, [H, {K1, K2}, V1]}},
      {set, _, {call, ?MODULE, puts, [H, {K3, K4}, V2]}}]) ->
      V = max_binary(V1, V2),
      [[{set, X, {call, ?MODULE, puts, [H, {K1, K2 + K4 - K3 + 1}, V]}}]];
    ([{set, X, {call, ?MODULE, puts, [H, {K1, K2}, V1]}},
      {set, _, {call, ?MODULE, put, [H, _K, V2]}}]) ->
      V = max_binary(V1, V2),
      [[{set, X, {call, ?MODULE, puts, [H, {K1, K2 + 1}, V]}}]];
    ([{set, X, {call, ?MODULE, put, [H, K, V1]}},
      {set, _, {call, ?MODULE, puts, [H, {K3, K4}, V2]}}]) ->
      V = max_binary(V1, V2),
      [[{set, X, {call, ?MODULE, puts, [H, {K, K + K4 - K3 + 1}, V]}}]];
    ([{set, X, {call, ?MODULE, put, [H, K, V1]}},
      {set, _, {call, ?MODULE, put, [H, _, V2]}}]) ->
      V = max_binary(V1, V2),
      [[{set, X, {call, ?MODULE, puts, [H, {K, K + 1}, V]}}]];
    (_) -> [] end) ++

  %% Shrink values of put
  shrink_sublist(1, Cmds, fun
    ([{set, X, {call, ?MODULE, puts, [H, {K1, K2}, <<_, V/binary>>]}}]) ->
      [[{set, X, {call, ?MODULE, puts, [H, {K1, K2}, V]}}]];
    ([{set, X, {call, ?MODULE, put, [H, K, <<_, V/binary>>]}}]) ->
      [[{set, X, {call, ?MODULE, put, [H, K, V]}}]];
    (_) -> [] end) ++

  %% Shrink bounds of puts
  shrink_sublist(1, Cmds, fun
    ([{set, X, {call, ?MODULE, puts, [H, {K1, K2}, V]}}]) ->
      [[{set, X, {call, ?MODULE, puts, [H, {K3, K4}, V]}}]
        || {K3, K4} <- shrink_bounds({K1, K2}) ];
    (_) -> [] end) ++

  %% Inline a fork
  shrink_sublist(1, Cmds, fun
    ([{set, _, {call, ?MODULE, fork,
                  [[{init, _}, {set, {not_var, H0}, {not_call, _, bc_open, _}} | NotCmds]]}}]) ->
      NextVar = 1 + lists:max([ X || {set, {var, X}, _} <- Cmds ]),
      YesCmds = rename_from(NextVar, recommand(NotCmds)),
      [ map(fun({var, H1}) when H1 == H0 -> H end, YesCmds)
        || H <- fold(fun({set, H, {call, _, bc_open, _}}) -> H end, Cmds) ];
    (_) -> [] end) ++

%%   shrink_tails(Cmds, fun

%%     ([{set, _,  {call, ?MODULE, bc_close, [H1]}},
%%       {set, H2, {call, ?MODULE, bc_open, _}} | Cmds1]) ->
%%       [map(fun(H3={var, _}) when H3 == H2 -> H1 end, Cmds1)];
%%     (_) -> [] end) ++

  shrink_sublist(1, Cmds, fun

    %% Shrink the commands of a fork
    ([{set, X, {call, ?MODULE, fork, [[Init={init, _}|NotCmds]]}}]) ->
      [ [{set, X, {call, ?MODULE, fork, [[Init|uncommand(Cmds1)]]}}]
        || Cmds1 <- shrink(recommand(NotCmds)) ];

    (_) -> [] end) ++

  %% Remove a single command
  shrink_sublist(1, Cmds, fun([_]) -> [[]] end) ++

  [].

max_binary(Bin1,  Bin2) when size(Bin1) > size(Bin2) -> Bin1;
max_binary(_Bin1, Bin2) -> Bin2.

shrink_sublist(N, Xs, Shrink) ->
  shrink_sublist(N, [], Xs, [], Shrink).

shrink_sublist(N, _Pre, Xs, Shrunk, _Shrink) when length(Xs) < N -> lists:append(lists:reverse(Shrunk));
shrink_sublist(N, Pre, Xs0 = [X|Xs], Shrunk, Shrink) ->
  {Ys, Zs}  = lists:split(N, Xs0),
  NewShrunk = [ lists:reverse(Pre) ++ Shr ++ Zs || Shr <- Shrink(Ys) ],
  shrink_sublist(N, [X|Pre], Xs, [NewShrunk|Shrunk], Shrink).

shrink_tails(Xs, Shrink) ->
  [ Ys ++ Ws || I        <- lists:seq(0, length(Xs)),
                {Ys, Zs} <- [lists:split(I, Xs)],
                Ws       <- Shrink(Zs) ].

custom_shrink() ->
  {ok, [Cmds]} = file:consult("shrunk"),
  custom_shrink(Cmds).

custom_shrink(CE) ->
  custom_shrink(CE, 1).

custom_shrink(CE0, Repeat) ->
  CE=[Cmds|_] = mk_counterexample(CE0),
  io:format("~p commands\n", [length(Cmds)]),
  Shrinkings = [ Cmds1 || Cmds1 <- shrink(Cmds) ],
  io:format("~p possible custom_shrinking steps\n", [length(Shrinkings)]),
  custom_shrink(CE, Shrinkings, Repeat).

custom_shrink(CE, [], _) ->
  io:format("\n"), CE;
custom_shrink(CE=[_,Seed|_], [C|Cs], Repeat) ->
  case check_many(Seed, C, Repeat) of
    true ->
      io:format("."),
      custom_shrink(CE, Cs, Repeat);
    Fail ->
      io:format("\n"),
      file:write_file("shrunk", io_lib:format("~p.\n", [Fail])),
      custom_shrink(Fail, Repeat)
  end.

check_many(C, N) ->
  check_many(erlang:now(), C, N).

check_many(_, _, 0) -> true;
check_many(Seed, C0, N) ->
  C = mk_counterexample(C0, Seed),
  case check(C) of
    true  -> check_many(C0, N - 1);
    false -> C
  end.

mk_counterexample(CE = [Cmds, _Seed, Conj]) when is_list(Cmds) andalso is_list(Conj) ->
  CE;
mk_counterexample(CE = [Cmds, _Seed]) when is_list(Cmds) ->
  CE;
mk_counterexample(Cmds) ->
  S = state_after(?MODULE, Cmds),
  [Cmds, erlang:now(),
   [ {0, []} | [ {I, []}
                 || I <- lists:seq(1, length(S#state.readers)) ] ]
   ++ [ {errors, []}, {events, []} ] ].

mk_counterexample(Cmds, Seed) ->
  [_Cmds, _Seed, Conj] = mk_counterexample(Cmds),
  [Cmds, Seed, Conj].

foo() ->
  erlang:now().

%% Helper functions
fold(F, X) ->
  case catch(F(X)) of
    {'EXIT', _} -> fold1(F, X);
    Y           -> [Y]
  end.

fold1(F, [X|Xs]) -> fold(F, X) ++ fold(F, Xs);
fold1(F, Tup) when is_tuple(Tup) ->
  fold1(F, tuple_to_list(Tup));
fold1(_F, _X) -> [].

map(F, X) ->
  case catch(F(X)) of
    {'EXIT', _} -> map1(F, X);
    Y           -> Y
  end.

map1(F, [X|Xs]) -> [map(F, X)|map(F, Xs)];
map1(F, Tup) when is_tuple(Tup) ->
  list_to_tuple(map1(F, tuple_to_list(Tup)));
map1(_F, X) -> X.

zipwith(F, [X|Xs], [Y|Ys]) ->
  [F(X, Y)|zipwith(F, Xs, Ys)];
zipwith(_, _, _) -> [].

uncommand(X) -> map(fun({var, N}) -> {not_var, N} end,
                map(fun({call, M, F, A}) -> {not_call, M, F, A} end, X)).
recommand(X) -> map(fun({not_var, N}) -> {var, N} end,
                map(fun({not_call, M, F, A}) -> {call, M, F, A} end, X)).

rename_from(X, Cmds) ->
  Bound = [ Y || {set, {var, Y}, _} <- Cmds ],
  Env   = lists:zip(Bound, lists:seq(X, X + length(Bound) - 1)),
  map(fun ({var, Y}) ->
        case lists:keyfind(Y, 1, Env) of
          {Y, Z} -> {var, Z}
        end end, Cmds).

really_delete_bitcask() ->
  os:cmd("rm -rf " ++ ?BITCASK),
  case file:read_file_info(?BITCASK) of
    {error, enoent} -> ok;
    {ok, _} ->
      timer:sleep(10),
      really_delete_bitcask()
  end.

-endif.
