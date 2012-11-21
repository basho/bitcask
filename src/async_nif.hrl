-define(ASYNC_NIF_CALL(Fun, Args),
        begin
            Ref = erlang:make_ref(),
            case erlang:apply(?MODULE, Fun, [Ref|Args]) of
                {ok, _QDepth} ->
                    bitcask_bump:big(), %% TODO: evaluate bumping based on QDepth
                    receive
                        {Ref, Reply} ->
                            Reply
                    end;
                Other -> Other
            end
        end).
