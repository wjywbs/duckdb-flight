# DuckDB Flight SQL Extension

This extension starts an Apache Arrow Flight SQL server backed by DuckDB.

## Build

```sh
BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release
```

## SQL API

```sql
CALL start_flight_sql_server();
CALL start_flight_sql_server(12345);
CALL stop_flight_sql_server();
SELECT * FROM flight_sql_is_started();
SELECT * FROM get_flight_sql_url();
```

## Shell API

```sh
./build/release/duckdb my.db -flight-sql
./build/release/duckdb my.db -flight-sql 12345
```

`-flight-sql` runs as a daemon mode (non-interactive shell) and exits on Ctrl-C.
