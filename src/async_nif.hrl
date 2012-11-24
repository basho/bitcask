-define(ASYNC_NIF_CALL(Fun, Args),
        begin
            NIFRef = erlang:make_ref(),
            case erlang:apply(Fun, [NIFRef|Args]) of
                {ok, QDepth} ->
                    erlang:bump_reductions(100 * QDepth),
                    receive
                        {NIFRef, Reply} ->
                            Reply
                    end;
                Other ->
                    Other
            end
        end).
