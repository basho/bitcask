-module(bitcask_thread_pool).
-behaviour(gen_server).

%% API
-export([start_link/0, worker/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).
-define(WORKERS, 16).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

ets_worker() ->
    try
        X = random:uniform(?WORKERS),
        ets:lookup_element(bitcask_threads, X, 2)
    catch
        _:_ ->
            undefined
    end.

worker() ->
    case ets_worker() of
        undefined ->
            {ok, Worker} = gen_server:call(?MODULE, get_worker),
            Worker;
        W ->
            W
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

generate_workers(Same, Same) ->
    ok;
generate_workers(X, End) ->
    {ok, Pid} = native_process:start(bitcask_nifs),
    ets:insert(bitcask_threads, {X, Pid}),
    generate_workers(X+1, End).

init([]) ->
    ets:new(bitcask_threads, [named_table, protected]),
    generate_workers(1,?WORKERS+1),
    {ok, #state{}}.

handle_call(get_worker, _From, State) ->
    W = case ets_worker() of
            undefined ->
                {ok, Pid} = native_process:start(bitcask_nifs),
                ets:insert(bitcask_threads, {1, Pid}),
                Pid;
            Worker ->
                Worker
        end,
    {reply, {ok,W}, State};

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
