-module(bitcask_timestamp).
-compile(nowarn_deprecated_function).
-export([timestamp/0]).

timestamp() ->
    try
	erlang:timestamp()
    catch
	error:undef ->
	    erlang:now()
    end.
