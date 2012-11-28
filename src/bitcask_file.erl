-module(bitcask_file).
-compile(export_all).
-behaviour(gen_server).

%% API

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {fd}).

%%%===================================================================
%%% API
%%%===================================================================

file_open(Filename, Opts) ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    case gen_server:call(Pid, {file_open, Filename, Opts}, infinity) of
        ok ->
            {ok, Pid};
        Error ->
            Error
    end.

file_close(Pid) ->
    file_request(Pid, file_close).

file_sync(Pid) ->
    file_request(Pid, file_sync).

file_pread(Pid, Offset, Size) ->
    file_request(Pid, {file_pread, Offset, Size}).

file_pwrite(Pid, Offset, Bytes) ->
    file_request(Pid, {file_pwrite, Offset, Bytes}).

file_read(Pid, Size) ->
    file_request(Pid, {file_read, Size}).

file_write(Pid, Bytes) ->
    file_request(Pid, {file_write, Bytes}).

file_seekbof(Pid) ->
    file_request(Pid, file_seekbof).

%%%===================================================================
%%% API helper functions
%%%===================================================================

file_request(Pid, Request) ->
    case check_pid(Pid) of
        ok ->
            gen_server:call(Pid, Request, infinity);
        Error ->
            Error
    end.

check_pid(Pid) ->
    IsPid = is_pid(Pid),
    IsAlive = IsPid andalso is_process_alive(Pid),
    case {IsAlive, IsPid} of
        {true, _} ->
            ok;
        {false, true} ->
            %% Same result as `file' module when accessing closed FD
            {error, einval};
        _ ->
            %% Same result as `file' module when providing wrong arg
            {error, badarg}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({file_open, Filename, Opts}, _From, State) ->
    IsCreate = proplists:get_bool(create, Opts),
    IsReadOnly = proplists:get_bool(readonly, Opts),
    Mode = case {IsReadOnly, IsCreate} of
               {true, _} ->
                   [read, raw, binary, read_ahead];
               {_, false} ->
                   [read, write, raw, binary, read_ahead];
               {_, true} ->
                   [read, write, exclusive, raw, binary, read_ahead]
           end,
    %% [lager:warning("Option ~p ignored", [Opt]) || Opt <- [create, o_sync],
    %%                                               proplists:get_bool(Opt, Opts)],
    case file:open(Filename, Mode) of
        {ok, Fd} ->
            State2 = State#state{fd=Fd},
            {reply, ok, State2};
        Error ->
            {reply, Error, State}
    end;
handle_call(file_close, _From, State=#state{fd=Fd}) ->
    ok = file:close(Fd),
    {stop, normal, ok, State};
handle_call(file_sync, _From, State=#state{fd=Fd}) ->
    Reply = file:sync(Fd),
    {reply, Reply, State};
handle_call({file_pread, Offset, Size}, _From, State=#state{fd=Fd}) ->
    Reply = file:pread(Fd, Offset, Size),
    {reply, Reply, State};
handle_call({file_pwrite, Offset, Bytes}, _From, State=#state{fd=Fd}) ->
    Reply = file:pwrite(Fd, Offset, Bytes),
    {reply, Reply, State};
handle_call({file_read, Size}, _From, State=#state{fd=Fd}) ->
    Reply = file:read(Fd, Size),
    {reply, Reply, State};
handle_call({file_write, Bytes}, _From, State=#state{fd=Fd}) ->
    Reply = file:write(Fd, Bytes),
    {reply, Reply, State};
handle_call(file_seekbof, _From, State=#state{fd=Fd}) ->
    {ok, _} = file:position(Fd, bof),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
