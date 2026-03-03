# DuckDB Flight SQL Extension

This extension starts an Apache Arrow Flight SQL server backed by DuckDB.

## Build

```sh
BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release
```

## SQL API

```sql
CALL flight_sql_start_server();
CALL flight_sql_start_server(12345);
CALL flight_sql_stop_server();
SELECT * FROM flight_sql_is_started();
SELECT * FROM flight_sql_get_url();
CALL flight_sql_set_transaction_timeout_seconds(1800);
SELECT * FROM flight_sql_get_transaction_timeout_seconds();
CALL flight_sql_set_prepared_timeout_seconds(1800);
SELECT * FROM flight_sql_get_prepared_timeout_seconds();
CALL flight_sql_set_sweeper_interval_seconds(30);
SELECT * FROM flight_sql_get_sweeper_interval_seconds();
```

## Shell API

```sh
./build/release/duckdb my.db -flight-sql
./build/release/duckdb my.db -flight-sql 12345
```

`-flight-sql` runs as a daemon mode (non-interactive shell) and exits on Ctrl-C.

## Go Benchmark Client

A standalone Go benchmark client is available in `extension/flight/test/go`.

```sh
extension/flight/test/go/run_flight_go_bench.sh
```
