%%% File        : mute.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 26 Mar 2012 by Ulf Norell
-module(mute).

-compile(export_all).

% Call Fun with output suppressed.
run(Fun) ->
  Ref    = make_ref(),
  Leader = group_leader(),
  Io     = spawn(?MODULE, dummy_io_server, [Leader]),
  group_leader(Io, self()),
  Res = (catch {Ref, Fun()}),
  Io ! stop,
  group_leader(Leader, self()),
  case Res of
    {Ref, X}      -> X;
    {'EXIT', Err} -> exit(Err);
    Err           -> throw(Err)
  end.

dummy_io_server(Leader) ->
  Reply = fun(From, ReplyAs, X) ->
      From ! {io_reply, ReplyAs, X},
      dummy_io_server(Leader)
    end,
  Forward = fun(From, ReplyAs, Request) ->
      Leader ! {io_request, From, ReplyAs, Request},
      dummy_io_server(Leader)
    end,
  receive
    {io_request, From, ReplyAs, Request} ->
      case Request of
        {put_chars, _, _}       -> Reply(From, ReplyAs, ok);
        {put_chars, _, _, _, _} -> Reply(From, ReplyAs, ok);
        {put_chars, _}          -> Reply(From, ReplyAs, ok);
        {put_chars, _, _, _}    -> Reply(From, ReplyAs, ok);
        {requests, Reqs} ->
          IsOutput = fun(Rq) when is_tuple(Rq) -> case tuple_to_list(Rq) of
                              [put_chars|_] -> true;
                              _             -> false
                            end;
                        (_) -> false
                     end,
          case [ Rq || Rq <- Reqs, not IsOutput(Rq) ] of
            []  -> Reply(From, ReplyAs, ok);
            Rqs -> Forward(From, ReplyAs, {requests, Rqs})
          end;
        _ ->
          % Forward any other requests
          Forward(From, ReplyAs, Request)
      end;
    stop ->
      ok;
    _Unknown ->
      ?MODULE:dummy_io_server()
  end.

