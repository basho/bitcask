%%% File        : token.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 20 Mar 2012 by Ulf Norell
-module(token).

-export([next_name/0, get_name/0, stop/0]).

next_name() ->
  rpc(next).

get_name() ->
  rpc(get).

stop() ->
  rpc(stop).

rpc(Msg) ->
  case whereis(?MODULE) of
    undefined ->
      start(),
      timer:sleep(1),
      rpc(Msg);
    Pid ->
      Ref = make_ref(),
      Pid ! {Msg, Ref, self()},
      receive
        {Ref, R} -> R
      after 1000 -> {error, timeout}
      end
  end.

start() ->
  register(?MODULE, spawn(fun() -> init() end)).

init() ->
  loop(mk_name()).

loop(Name) ->
  receive
    {next, Tag, Pid} ->
      Next = mk_name(),
      Pid ! {Tag, Next},
      loop(Next);
    {get, Tag, Pid} ->
      Pid ! {Tag, Name},
      loop(Name);
    {stop, Tag, Pid} ->
      Pid ! {Tag, ok}
  end.

mk_name() ->
  {A, B, C} = erlang:now(),
  lists:concat([A, "-", B, "-", C]).

