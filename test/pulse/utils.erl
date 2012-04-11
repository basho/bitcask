%%% File        : utils.erl
%%% Author      : Ulf Norell
%%% Description : Contains some functions that should be run without being
%%%               PULSE instrumented.
%%% Created     : 21 Mar 2012 by Ulf Norell
-module(utils).

-compile(export_all).

whereis(Name) ->
  erlang:whereis(Name).

unregister(Name) ->
  erlang:unregister(Name).

exit(Reason) ->
  erlang:exit(Reason).
