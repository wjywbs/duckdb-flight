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
  --crud-iters 2000 \
  --select-iters 2000
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
- Select point-query mode comparison:
  - direct query (no explicit prepare),
  - prepare every call,
  - prepare once and reuse
- Concurrent transaction commit/rollback benchmark with visibility checks
- Concurrent transaction DDL+DML benchmark (create/drop + insert/select in one tx)
- Concurrent transaction commit-conflict benchmark (pairwise same-row update conflicts)
- Ordered single-reader scan of 100k rows with correctness checks
- Ordered concurrent shard scans with correctness checks
- Ordered concurrent full-table scans with correctness checks (no sharding)

This benchmark path is script-driven and not part of default pytest/CI runs.

## Concurrent Transaction Benchmark

Phase: `CONCURRENT_TRANSACTIONS_COMMIT_ROLLBACK`

- Workers run independent transactions concurrently.
- Even worker ids commit; odd worker ids rollback.
- During each transaction:
  - in-transaction query must see all rows inserted by that worker
  - outside query must not see that worker's uncommitted rows
- After transaction end:
  - committed worker rows must persist
  - rolled-back worker rows must not persist
- Final aggregate checks validate committed row count, distinct id count, and sum.

## Select Prepare-Mode Benchmark

Phase: `SELECT_PREPARE_MODES`

Measures point-select latency/throughput differences for:

- `select_point_direct_no_prepare`: execute direct SQL each iteration (no explicit prepared stmt)
- `select_point_prepare_each_time`: prepare + execute + close each iteration
- `select_point_prepare_once_reuse`: prepare once, execute repeatedly

Correctness checks:

- each mode validates result value per selected id (`val == id * 10`)
- all modes run the same iteration count (`--select-iters`)

## Concurrent Transaction DDL+DML Benchmark

Phase: `CONCURRENT_TRANSACTIONS_DDL_DML`

Scenario design:

- Each worker runs one transaction that performs:
  - `CREATE TABLE <worker_unique_table>(...)`
  - insert worker shard rows into that worker table
  - in-transaction `SELECT COUNT/SUM` from that worker table
  - `DROP TABLE <worker_unique_table>`
  - `COMMIT`

Correctness checks:

- During transaction:
  - outside query cannot see uncommitted DDL table,
  - in-tx aggregate count/sum matches inserted rows for that worker table,
  - dropped table is no longer queryable inside tx.
- After commit:
  - dropped DDL tables do not exist,
  - no leftover worker DDL tables by prefix,
  - total inserted/selected row accounting equals `rows`.

Debug finding (why this was unreliable):

- In Arrow Go Flight SQL driver `v18.5.1`, `Connection.QueryContext` runs direct `client.Execute(...)` and does not branch on active `c.txn` for zero-argument queries.
- `database/sql` may route `tx.QueryRowContext` with no parameters through that path, so a query can execute outside the transaction.
- That explains the intermittent behavior where `BEGIN; CREATE TABLE ...; INSERT ...; SELECT ...` on tx-local tables failed in this benchmark while DuckDB CLI sequence worked.
- Workaround used here: in-tx select is parameterized (`WHERE id >= ?`) so database/sql uses prepared statement execution, which is transaction-bound in the driver.

## Concurrent Transaction Commit-Conflict Benchmark

Phase: `CONCURRENT_TRANSACTIONS_COMMIT_CONFLICTS`

Commit-conflict scenario design:

- Seed `workers` rows in `go_flight_tx_conflict_bench` with `(id, worker_id=-1, attempt=0)`.
- For each `id`, run 2 concurrent contenders (2 transactions) that both:
  - `BEGIN`
  - `UPDATE go_flight_tx_conflict_bench SET worker_id=?, attempt=attempt+1 WHERE id=?`
  - wait at a barrier
  - `COMMIT`
- This creates deterministic write-write contention per row (one winner, one conflict loser).

Correctness checks:

- During transaction (before commit barrier release): outside query must still see `attempt=0, worker_id=-1` for each row.
- During transaction (inside tx after update): contender sees `attempt=1` for that row.
- After transactions:
  - exactly `workers` commits and `workers` conflict failures (for `2*workers` transactions total),
  - table row count remains `workers`,
  - `SUM(attempt) = workers` and winner row count is `workers` (exactly one committed update per row).

## Latest Results (Analysis)

From a default 100k-row run on this environment:

- `batch_insert_rows`: ~0.94-0.98s (~102k-106k rows/s)
- `concurrent_insert_rows`:
  - before fix (200 goroutines, autocommit row-by-row): ~35.64s (~2.81k rows/s)
  - after fix (200 goroutines, per-worker transaction + prepared statement): ~3.5-3.8s (~26k-29k rows/s)
- `SELECT_PREPARE_MODES` (`select-iters=2000`, point lookups on `go_flight_bench`):
  - direct/no-prepare: ~1.01s (~1989 ops/s)
  - prepare each time: ~1.37s (~1464 ops/s)
  - prepare once/reuse: ~0.83s (~2419 ops/s)
- `concurrent_transactions_total` (200 workers mixed commit/rollback): ~3.71s for 200 tx (~54 tx/s)
  - committed rows: `50000`
  - rolled-back rows: `50000`
- `concurrent_tx_ddl_dml_total` (200 workers, create/drop + insert/select): ~3.30s for 200 tx (~60.6 tx/s)
  - inserted rows: `100000`
  - selected rows: `100000`
- `concurrent_tx_conflict_total` (pairwise conflicts, 200 workers => 400 tx): ~0.20s for 400 tx (~2005 tx/s)
  - commits: `200`
  - conflict failures: `200`
- `select_ordered_single_rows`: ~18-19ms
- `select_ordered_concurrent_rows`: ~40-46ms

Concise conclusion:

- The slow path was not Flight transport startup overhead; it was write-commit pressure from many tiny autocommit writes.
- With 200 goroutines, autocommit row-by-row causes high commit and writer-lock contention in a file-backed DB.
- Grouping each worker's inserts into one transaction and reusing a prepared statement reduced commit frequency and improved concurrent insert throughput by about 10x in this setup.

## Ordered Read Scaling (Why Concurrent Can Be Slower)

To check `select_ordered_concurrent_rows` vs `select_ordered_single_rows`, we ran:

```sh
extension/flight/test/go/run_flight_go_bench.sh --rows 100000 --batch-size 1000 --crud-iters 200 --workers <N>
```

Results (rows/sec):

| Workers | Single Ordered Read | Concurrent Ordered Read | Concurrent / Single |
| --- | ---: | ---: | ---: |
| 1 | 6.78M | 8.11M | 1.20x |
| 2 | 6.32M | 10.10M | 1.60x |
| 4 | 6.05M | 14.09M | 2.33x |
| 8 | 6.32M | 12.56M | 1.99x |
| 16 | 5.74M | 10.97M | 1.91x |
| 32 | 5.81M | 7.11M | 1.22x |
| 64 | 5.39M | 7.02M | 1.30x |
| 128 | 5.54M | 4.10M | 0.74x |
| 200 | 5.48M | 2.57M | 0.47x |

Interpretation:

- Throughput does **not** keep increasing with more workers.
- It improves up to low worker counts (best here at `N=4`), then declines.
- At high worker counts (`128`, `200`), concurrent read is slower than a single ordered scan.
- Reason: concurrent mode issues many small shard queries (`WHERE id BETWEEN ? AND ? ORDER BY id`), so query/Flight stream setup and scheduling overhead dominates when each worker reads only a small slice.

## Ordered Read Scaling (No Sharding, Full Scan per Worker)

New test behavior:

- each worker runs `SELECT id, val FROM go_flight_bench ORDER BY id`
- each worker validates all `100000` rows
- total processed rows for throughput = `rows * workers`

Same sweep command:

```sh
extension/flight/test/go/run_flight_go_bench.sh --rows 100000 --batch-size 1000 --crud-iters 200 --workers <N>
```

Results:

| Workers | Single Ordered Read (rows/s) | Sharded Concurrent (rows/s) | Full Concurrent (rows/s, aggregate) |
| --- | ---: | ---: | ---: |
| 1 | 6.04M | 8.04M | 7.88M |
| 2 | 6.36M | 9.97M | 13.89M |
| 4 | 5.36M | 13.92M | 22.06M |
| 8 | 5.81M | 9.72M | 31.78M |
| 16 | 5.61M | 12.39M | 35.54M |
| 32 | 5.64M | 9.45M | 39.60M |
| 64 | 5.56M | 6.76M | 41.58M |
| 128 | 5.55M | 3.56M | 42.39M |
| 200 | 5.70M | 2.58M | 41.32M |

Interpretation:

- For the full-scan test, aggregate throughput increases with workers up to roughly `64-128`, then plateaus around `41-42M rows/s`.
- Unlike sharded mode, full-scan mode does not collapse at high workers in aggregate throughput, but it also does not scale linearly with worker count.
- This indicates shared bottlenecks (CPU scheduling, gRPC/Flight stream overhead, and memory bandwidth) limit scaling once concurrency is high.
