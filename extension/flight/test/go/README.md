# Flight SQL Go Benchmark Client

This directory contains a standalone Go benchmark/test client for the DuckDB Flight SQL server using `database/sql` and the Arrow Go Flight SQL driver.

## Prerequisites

- Go toolchain (`go version`)
- DuckDB built with the `flight` extension

## Dependency

The client uses:

- `github.com/apache/arrow-go/v18/arrow/flight/flightsql/driver`

## Run

From the repository root:

```sh
extension/flight/test/go/run_flight_go_bench.sh
```

Common options:

```sh
extension/flight/test/go/run_flight_go_bench.sh \
  --shell-binary build/release/duckdb \
  --rows 100000 \
  --workers 200 \
  --batch-size 1000 \
  --crud-iters 2000
```

The script:

1. Builds DuckDB with `flight` if needed.
2. Starts `duckdb -flight-sql` on a file-backed database.
3. Waits for readiness using `TestPing`.
4. Runs `TestFlightSQLBenchmarks`.
5. Prints benchmark timings and DB file size.
6. Shuts down and removes temp files (unless `--keep-db` is set).

## Workload

- CRUD single-operation autocommit timing
- Batch insert of 100k rows
- 200-goroutine concurrent insert (per-worker transaction + prepared statement)
- Ordered single-reader scan of 100k rows with correctness checks
- Ordered concurrent shard scans with correctness checks

This benchmark path is script-driven and not part of default pytest/CI runs.

## Latest Results (Analysis)

From a default 100k-row run on this environment:

- `batch_insert_rows`: ~0.94-0.98s (~102k-106k rows/s)
- `concurrent_insert_rows`:
  - before fix (200 goroutines, autocommit row-by-row): ~35.64s (~2.81k rows/s)
  - after fix (200 goroutines, per-worker transaction + prepared statement): ~3.5-3.8s (~26k-29k rows/s)
- `select_ordered_single_rows`: ~18-19ms
- `select_ordered_concurrent_rows`: ~40-46ms

Concise conclusion:

- The slow path was not Flight transport startup overhead; it was write-commit pressure from many tiny autocommit writes.
- With 200 goroutines, autocommit row-by-row causes high commit and writer-lock contention in a file-backed DB.
- Grouping each worker's inserts into one transaction and reusing a prepared statement reduced commit frequency and improved concurrent insert throughput by about 10x in this setup.
