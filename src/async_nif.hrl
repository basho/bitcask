-define(ASYNC_NIF_CALL(Fun, Args),
        begin
            NIFRef = erlang:make_ref(),
            case erlang:apply(Fun, [NIFRef|Args]) of
                {error, shutdown}=Error ->
                    %% Work unit was not executed, requeue it.
                    Error;
                {error, _Reason}=Error ->
                    %% Work unit returned an error.
                    Error;
                {ok, _QDepth} ->
                    bitcask_bump:big(), %% TODO: evaluate bumping based on QDepth
                    receive
                        {NIFRef, Reply} ->
                            Reply
                    end;
                Other -> Other
            end
        end).
