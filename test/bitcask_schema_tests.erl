-module(bitcask_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

%% basic schema test will check to make sure that all defaults from the schema
%% make it into the generated app.config
basic_schema_test_() ->
    {timeout, 60, fun basic_schema_test2/0}.

basic_schema_test2() ->
    lager:start(),
    %% The defaults are defined in ../priv/bitcask.schema. it is the file under test.
    Config = cuttlefish_unit:generate_templated_config("../priv/bitcask.schema", [], context(), predefined_schema()),

    cuttlefish_unit:assert_config(Config, "bitcask.data_root", "./data/bitcask"),
    cuttlefish_unit:assert_config(Config, "bitcask.open_timeout", 4),
    cuttlefish_unit:assert_config(Config, "bitcask.sync_strategy", none),
    cuttlefish_unit:assert_config(Config, "bitcask.max_file_size", 2147483648),
    cuttlefish_unit:assert_config(Config, "bitcask.merge_window", always),
    cuttlefish_unit:assert_config(Config, "bitcask.frag_merge_trigger", 60),
    cuttlefish_unit:assert_config(Config, "bitcask.dead_bytes_merge_trigger", 536870912),
    cuttlefish_unit:assert_config(Config, "bitcask.frag_threshold", 40),
    cuttlefish_unit:assert_config(Config, "bitcask.dead_bytes_threshold", 134217728),
    cuttlefish_unit:assert_config(Config, "bitcask.small_file_threshold", 10485760),
    cuttlefish_unit:assert_config(Config, "bitcask.max_fold_age", -1),
    cuttlefish_unit:assert_config(Config, "bitcask.max_fold_puts", 0),
    cuttlefish_unit:assert_config(Config, "bitcask.expiry_secs", -1),
    cuttlefish_unit:assert_config(Config, "bitcask.require_hint_crc", true),
    cuttlefish_unit:assert_config(Config, "bitcask.expiry_grace_time", 0),
    cuttlefish_unit:assert_config(Config, "bitcask.io_mode", erlang),

    %% Make sure no multi_backend
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.multi_backend"),
    ok.

merge_window_test_() ->
    {timeout, 60, fun merge_window_test2/0}.

merge_window_test2() ->
    lager:start(),
    Conf = [
        {["bitcask", "merge", "policy"], window},
        {["bitcask", "merge", "window", "start"], 0},
        {["bitcask", "merge", "window", "end"], 12}
    ],

    %% The defaults are defined in ../priv/bitcask.schema. it is the file under test.
    Config = cuttlefish_unit:generate_templated_config("../priv/bitcask.schema", Conf, context(), predefined_schema()),

    cuttlefish_unit:assert_config(Config, "bitcask.data_root", "./data/bitcask"),
    cuttlefish_unit:assert_config(Config, "bitcask.open_timeout", 4),
    cuttlefish_unit:assert_config(Config, "bitcask.sync_strategy", none),
    cuttlefish_unit:assert_config(Config, "bitcask.max_file_size", 2147483648),
    cuttlefish_unit:assert_config(Config, "bitcask.merge_window", {0, 12}),
    cuttlefish_unit:assert_config(Config, "bitcask.frag_merge_trigger", 60),
    cuttlefish_unit:assert_config(Config, "bitcask.dead_bytes_merge_trigger", 536870912),
    cuttlefish_unit:assert_config(Config, "bitcask.frag_threshold", 40),
    cuttlefish_unit:assert_config(Config, "bitcask.dead_bytes_threshold", 134217728),
    cuttlefish_unit:assert_config(Config, "bitcask.small_file_threshold", 10485760),
    cuttlefish_unit:assert_config(Config, "bitcask.max_fold_age", -1),
    cuttlefish_unit:assert_config(Config, "bitcask.max_fold_puts", 0),
    cuttlefish_unit:assert_config(Config, "bitcask.expiry_secs", -1),
    cuttlefish_unit:assert_config(Config, "bitcask.require_hint_crc", true),
    cuttlefish_unit:assert_config(Config, "bitcask.expiry_grace_time", 0),
    cuttlefish_unit:assert_config(Config, "bitcask.io_mode", erlang),

    %% Make sure no multi_backend
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.multi_backend"),
    ok.

override_schema_test_() ->
    {timeout, 60, fun override_schema_test2/0}.

override_schema_test2() ->
    lager:start(),
    %% Conf represents the riak.conf file that would be read in by cuttlefish.
    %% this proplists is what would be output by the conf_parse module
    Conf = [
        {["bitcask", "data_root"], "/absolute/data/bitcask"},
        {["bitcask", "open_timeout"], 2},
        {["bitcask", "sync", "strategy"], interval},
        {["bitcask", "sync", "interval"], "10s"},
        {["bitcask", "max_file_size"], "4GB"},
        {["bitcask", "merge", "policy"], never},
        {["bitcask", "merge", "window", "start"], 0},
        {["bitcask", "merge", "window", "end"], 12},
        {["bitcask", "merge", "triggers", "fragmentation"], 20},
        {["bitcask", "merge", "triggers", "dead_bytes"], "256MB"},
        {["bitcask", "merge", "thresholds", "fragmentation"], 10},
        {["bitcask", "merge", "thresholds", "dead_bytes"], "64MB"},
        {["bitcask", "merge", "thresholds", "small_file"], "5MB"},
        {["bitcask", "fold", "max_age"], "12ms"},
        {["bitcask", "fold", "max_puts"], 7},
        {["bitcask", "expiry"], "20s" },
        {["bitcask", "hintfile_checksums"], "allow_missing"},
        {["bitcask", "expiry", "grace_time"], "15s" },
        {["bitcask", "io_mode"], nif}
    ],

    %% The defaults are defined in ../priv/bitcask.schema. it is the file under test.
    Config = cuttlefish_unit:generate_templated_config("../priv/bitcask.schema", Conf, context(), predefined_schema()),

    cuttlefish_unit:assert_config(Config, "bitcask.data_root", "/absolute/data/bitcask"),
    cuttlefish_unit:assert_config(Config, "bitcask.open_timeout", 2),
    cuttlefish_unit:assert_config(Config, "bitcask.sync_strategy", {seconds, 10}),
    cuttlefish_unit:assert_config(Config, "bitcask.max_file_size", 4294967296),
    cuttlefish_unit:assert_config(Config, "bitcask.merge_window", never),
    cuttlefish_unit:assert_config(Config, "bitcask.frag_merge_trigger", 20),
    cuttlefish_unit:assert_config(Config, "bitcask.dead_bytes_merge_trigger", 268435456),
    cuttlefish_unit:assert_config(Config, "bitcask.frag_threshold", 10),
    cuttlefish_unit:assert_config(Config, "bitcask.dead_bytes_threshold", 67108864),
    cuttlefish_unit:assert_config(Config, "bitcask.small_file_threshold", 5242880),
    cuttlefish_unit:assert_config(Config, "bitcask.max_fold_age", 12000),
    cuttlefish_unit:assert_config(Config, "bitcask.max_fold_puts", 7),
    cuttlefish_unit:assert_config(Config, "bitcask.expiry_secs", 20),
    cuttlefish_unit:assert_config(Config, "bitcask.require_hint_crc", false),
    cuttlefish_unit:assert_config(Config, "bitcask.expiry_grace_time", 15),
    cuttlefish_unit:assert_config(Config, "bitcask.io_mode", nif),

    %% Make sure no multi_backend
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.multi_backend"),
    ok.

multi_backend_test_() ->
    {timeout, 60, fun multi_backend_test2/0}.

multi_backend_test2() ->
    Conf = [
            {["multi_backend", "default", "storage_backend"], bitcask},
            {["multi_backend", "default", "bitcask", "data_root"], "/data/default_bitcask"}
           ],
    %% The defaults are defined in ../priv/bitcask.schema. it is the file under test.
    Config = cuttlefish_unit:generate_templated_config(
               ["../priv/bitcask.schema", "../priv/bitcask_multi.schema", "../test/multi_backend.schema"],
               Conf, context(), predefined_schema()),
    %%io:format("Config: ~p~n", []),

    MultiBackendConfig = proplists:get_value(multi_backend, proplists:get_value(riak_kv, Config)),

    {<<"default">>, riak_kv_bitcask_backend, DefaultBackend} = lists:keyfind(<<"default">>, 1, MultiBackendConfig),

    cuttlefish_unit:assert_config(DefaultBackend, "data_root", "/data/default_bitcask"),
    cuttlefish_unit:assert_config(DefaultBackend, "open_timeout", 4),
    cuttlefish_unit:assert_config(DefaultBackend, "sync_strategy", none),
    cuttlefish_unit:assert_config(DefaultBackend, "max_file_size", 2147483648),
    cuttlefish_unit:assert_config(DefaultBackend, "merge_window", always),
    cuttlefish_unit:assert_config(DefaultBackend, "frag_merge_trigger", 60),
    cuttlefish_unit:assert_config(DefaultBackend, "dead_bytes_merge_trigger", 536870912),
    cuttlefish_unit:assert_config(DefaultBackend, "frag_threshold", 40),
    cuttlefish_unit:assert_config(DefaultBackend, "dead_bytes_threshold", 134217728),
    cuttlefish_unit:assert_config(DefaultBackend, "small_file_threshold", 10485760),
    cuttlefish_unit:assert_config(DefaultBackend, "max_fold_age", -1),
    cuttlefish_unit:assert_config(DefaultBackend, "max_fold_puts", 0),
    cuttlefish_unit:assert_config(DefaultBackend, "expiry_secs", -1),
    cuttlefish_unit:assert_config(DefaultBackend, "require_hint_crc", true),
    cuttlefish_unit:assert_config(DefaultBackend, "expiry_grace_time", 0),
    cuttlefish_unit:assert_config(DefaultBackend, "io_mode", erlang),
    ok.

%% this context() represents the substitution variables that rebar
%% will use during the build process.  riak_core's schema file is
%% written with some {{mustache_vars}} for substitution during
%% packaging cuttlefish doesn't have a great time parsing those, so we
%% perform the substitutions first, because that's how it would work
%% in real life.
context() -> [].

%% This predefined schema covers riak_kv's dependency on
%% platform_data_dir
predefined_schema() ->
    Mapping = cuttlefish_mapping:parse({mapping,
                                        "platform_data_dir",
                                        "riak_core.platform_data_dir", [
                                            {default, "./data"},
                                            {datatype, directory}
                                       ]}),
    {[], [Mapping], []}.
