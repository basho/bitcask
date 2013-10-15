-module(bitcask_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

%% basic schema test will check to make sure that all defaults from the schema
%% make it into the generated app.config
basic_schema_test() ->
    %% The defaults are defined in ../priv/bitcask.schema. it is the file under test. 
    Config = cuttlefish_unit:generate_templated_config("../priv/bitcask.schema", [], context()),

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
    cuttlefish_unit:assert_config(Config, "riak_kv.multi_backend", undefined),
    ok.

merge_window_test() ->
    Conf = [
        {["bitcask", "merge_window"], window},
        {["bitcask", "merge_window", "start"], 0},
        {["bitcask", "merge_window", "end"], 12}
    ],

    %% The defaults are defined in ../priv/bitcask.schema. it is the file under test. 
    Config = cuttlefish_unit:generate_templated_config("../priv/bitcask.schema", Conf, context()),

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
    cuttlefish_unit:assert_config(Config, "riak_kv.multi_backend", undefined),
    ok.

override_schema_test() ->
    %% Conf represents the riak.conf file that would be read in by cuttlefish.
    %% this proplists is what would be output by the conf_parse module
    Conf = [
        {["bitcask", "data_root"], "/absolute/data/bitcask"},
        {["bitcask", "open_timeout"], 2},
        {["bitcask", "sync_strategy"], interval},
        {["bitcask", "sync_interval"], "10s"},
        {["bitcask", "max_file_size"], "4GB"},
        {["bitcask", "merge_window"], never},
        {["bitcask", "merge_window", "start"], 0},
        {["bitcask", "merge_window", "end"], 12},
        {["bitcask", "frag_merge_trigger"], 20},
        {["bitcask", "dead_bytes_merge_trigger"], "256MB"},
        {["bitcask", "frag_threshold"], 10},
        {["bitcask", "dead_bytes_threshold"], "64MB"},
        {["bitcask", "small_file_threshold"], "5MB"},
        {["bitcask", "max_fold_age"], 12},
        {["bitcask", "max_fold_puts"], 7},
        {["bitcask", "expiry"], "20s" },
        {["bitcask", "require_hint_crc"], false },
        {["bitcask", "expiry_grace_time"], "15s" },
        {["bitcask", "io_mode"], nif}
    ],

    %% The defaults are defined in ../priv/bitcask.schema. it is the file under test. 
    Config = cuttlefish_unit:generate_templated_config("../priv/bitcask.schema", Conf, context()),

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
    cuttlefish_unit:assert_config(Config, "bitcask.max_fold_age", 12),
    cuttlefish_unit:assert_config(Config, "bitcask.max_fold_puts", 7),
    cuttlefish_unit:assert_config(Config, "bitcask.expiry_secs", 20),
    cuttlefish_unit:assert_config(Config, "bitcask.require_hint_crc", false),
    cuttlefish_unit:assert_config(Config, "bitcask.expiry_grace_time", 15),
    cuttlefish_unit:assert_config(Config, "bitcask.io_mode", nif),

    %% Make sure no multi_backend
    cuttlefish_unit:assert_config(Config, "riak_kv.multi_backend", undefined),
    ok.

%% this context() represents the substitution variables that rebar will use during the build process.
%% riak_core's schema file is written with some {{mustache_vars}} for substitution during packaging
%% cuttlefish doesn't have a great time parsing those, so we perform the substitutions first, because
%% that's how it would work in real life.
context() ->
    [
        {platform_data_dir, "./data"}
    ].
