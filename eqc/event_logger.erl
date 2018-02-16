%%% File        : handle_errors.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 26 Mar 2012 by Ulf Norell
-module(event_logger).

-compile(export_all).

-behaviour(gen_server).

%% API
-export([start_link/0, event/1, get_events/0, start_logging/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(event, { timestamp :: integer(),
                data :: term() }).

-record(state, { start_time :: integer(),
                events = [] :: [#event{}] }).


%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start_logging() ->
  gen_server:call(?MODULE, {start, timestamp()}).

event(EventData) ->
  gen_server:call(?MODULE,
    #event{ timestamp = timestamp(), data = EventData }).

async_event(EventData) ->
  gen_server:cast(?MODULE,
    #event{ timestamp = timestamp(), data = EventData }).

get_events() ->
  gen_server:call(?MODULE, get_events).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
  {ok, #state{}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) ->
%%                {reply, Reply, State} |
%%                {reply, Reply, State, Timeout} |
%%                {noreply, State} |
%%                {noreply, State, Timeout} |
%%                {stop, Reason, Reply, State} |
%%                {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(Event = #event{}, _From, State) ->
  {reply, ok, add_event(Event, State)};
handle_call({start, Now}, _From, S) ->
  {reply, ok, S#state{ events = [], start_time = Now }};
handle_call(get_events, _From, S) ->
  {reply, lists:reverse([ {E#event.timestamp, E#event.data} || E <- S#state.events]),
          S#state{ events = [] }};
handle_call(Request, _From, State) ->
  {reply, {error, {bad_call, Request}}, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(Event = #event{}, State) ->
  {noreply, add_event(Event, State)};
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

add_event(#event{timestamp = Now, data = Data}, State) ->
  Event = #event{ timestamp = Now - State#state.start_time, data = Data },
  State#state{ events = [Event|State#state.events] }.

timestamp() ->
  {A, B, C} = erlang:now(),
  1000000 * (1000000 * A + B) + C.

