# Flight Extension Tests

## Build

```sh
BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release
```

## Shell Integration Tests

```sh
python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q
```

## Extension SQL Lifecycle Test

```sh
build/release/test/unittest "extension/flight/test/sql/flight_lifecycle.test"
```

## Smoke Client (Manual)

1. Start server:

```sh
build/release/duckdb --batch --init /dev/null -flight-sql 12345 :memory:
```

2. In another terminal, run modes:

```sh
build/release/extension/flight/flight_sql_smoke_client --host 127.0.0.1 --port 12345 --mode ping
build/release/extension/flight/flight_sql_smoke_client --host 127.0.0.1 --port 12345 --mode crud
build/release/extension/flight/flight_sql_smoke_client --host 127.0.0.1 --port 12345 --mode metadata
build/release/extension/flight/flight_sql_smoke_client --host 127.0.0.1 --port 12345 --mode prepared
build/release/extension/flight/flight_sql_smoke_client --host 127.0.0.1 --port 12345 --mode transaction
build/release/extension/flight/flight_sql_smoke_client --host 127.0.0.1 --port 12345 --mode timeout
build/release/extension/flight/flight_sql_smoke_client --host 127.0.0.1 --port 12345 --mode cancel
```

## Go Bench/Test Harness

```sh
extension/flight/test/go/run_flight_go_bench.sh
```
