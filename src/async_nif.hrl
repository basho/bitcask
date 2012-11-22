-define(ASYNC_NIF_CALL(Fun, Args),
        begin
            NIFRef = erlang:make_ref(),
            case erlang:apply(Fun, [NIFRef|Args]) of
                {ok, _QDepth} ->
                    bitcask_bump:big(), %% TODO: evaluate bumping based on QDepth
                    receive
                        {NIFRef, Reply} ->
                            Reply
                    end;
                Other -> Other
            end
        end).
