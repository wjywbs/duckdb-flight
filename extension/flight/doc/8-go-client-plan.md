## DuckDB Flight SQL Go Client + 100k Benchmark Harness

### Summary
Add a standalone Go benchmark/test client under `extension/flight/test/go` using Apache Arrow Go Flight SQL with `database/sql`, and add a single runner script that:
1. builds DuckDB with `flight`,
2. starts a file-backed Flight SQL daemon,
3. runs Go benchmark/test workloads (CRUD timing, 100k batch insert, 200-goroutine concurrent insert, single/concurrent ordered reads + correctness checks),
4. prints DB file size and benchmark timing,
5. shuts down and cleans up.

Locked decisions:
- CI scope: script-only (no heavy pytest integration).
- Single-operation insert benchmark semantics: autocommit.
- Row target: exactly `100000`.
- Concurrent worker count: exactly `200`.

### Important Changes / Additions
- New Go module and client test harness:
  - `extension/flight/test/go/go.mod`
  - `extension/flight/test/go/go.sum`
  - `extension/flight/test/go/flightsql_bench_test.go`
  - `extension/flight/test/go/README.md`
- New runner script:
  - `extension/flight/test/go/run_flight_go_bench.sh`
- Optional docs reference update:
  - `extension/flight/README.md` add a short “Go benchmark client” section.
- No changes to user-facing DuckDB SQL/CLI APIs.
- No pytest or C++ test changes for this heavy benchmark path.

### Implementation Plan

1. Create Go module for the Flight SQL benchmark client.
- Initialize module in `extension/flight/test/go`.
- Pin Arrow Go dependency to v18 line.
- Use `database/sql` with Flight SQL driver registration:
  - blank import `github.com/apache/arrow-go/v18/arrow/flight/flightsql/driver`.
- Keep DSN format:
  - `flightsql://127.0.0.1:<port>?timeout=120s&tls=disabled`.
- Add CLI flags via `go test` custom flags:
  - `-host`, `-port`, `-rows` (default `100000`), `-workers` (default `200`), `-batch-size` (default `1000`), `-crud-iters` (default `2000`).

2. Implement Go benchmark/test workload in `flightsql_bench_test.go`.
- Use a shared helper that opens `*sql.DB`, sets pool knobs:
  - `SetMaxOpenConns(256)`, `SetMaxIdleConns(256)`, `SetConnMaxLifetime(0)`.
- Add deterministic schema:
  - `CREATE TABLE IF NOT EXISTS go_flight_bench (id BIGINT PRIMARY KEY, val BIGINT)`.
- Add cleanup helper for table reset between phases.
- Add metric printer helper:
  - emits wall time, ops/sec, ns/op (or us/op), rows/sec.

3. Add CRUD single-operation timing benchmark phase.
- Phase name: `CRUD_SINGLE_OP_AUTOCOMMIT`.
- Measure and print:
  - single `CREATE TABLE` latency (separate temp table),
  - average autocommit `INSERT` latency over `crud-iters` with unique ids,
  - average autocommit `UPDATE` latency over inserted ids,
  - average point `SELECT` latency over inserted ids,
  - average `DELETE` latency over inserted ids,
  - single `DROP TABLE` latency (temp table).
- Keep timing logic explicit (`time.Now()` / `time.Since`) and print per-op units.

4. Add 100k batch insert benchmark phase.
- Phase name: `BATCH_INSERT_100K`.
- Reset target table.
- Insert `rows` records (`id=1..rows`, `val=id*10`) in batches of `batch-size`.
- Use parameterized multi-row `INSERT` with one `ExecContext` per batch.
- Validate postcondition:
  - `COUNT(*) == rows`
  - `MIN(id)=1`, `MAX(id)=rows`.
- Print total duration and rows/sec.

5. Add 200-goroutine concurrent insert benchmark phase.
- Phase name: `CONCURRENT_INSERT_200`.
- Reset target table.
- Partition id-space deterministically across `workers`:
  - each worker inserts disjoint contiguous range,
  - total inserted rows remains exactly `rows`.
- Worker insert mode: autocommit single-row inserts (`ExecContext`) to reflect concurrent operation contention.
- Collect worker errors through channel / errgroup.
- Validate correctness:
  - `COUNT(*) == rows`
  - `COUNT(DISTINCT id) == rows`
  - `SUM(val)` equals expected arithmetic sum (`10 * rows*(rows+1)/2`).
- Print total duration, per-op latency estimate, rows/sec.

6. Add ordered-read benchmark and correctness verification.
- Phase name: `SELECT_ORDERED_SINGLE`.
- Run `SELECT id, val FROM go_flight_bench ORDER BY id`.
- Stream rows via `rows.Next()` and verify:
  - exactly `rows` rows,
  - strictly increasing `id` sequence from `1..rows`,
  - `val == id*10`.
- Print total duration and rows/sec.

7. Add concurrent ordered-read benchmark.
- Phase name: `SELECT_ORDERED_CONCURRENT_200`.
- Keep workload bounded and deterministic:
  - each worker reads one shard with ordered query:
    - `SELECT id, val FROM go_flight_bench WHERE id BETWEEN ? AND ? ORDER BY id`.
  - shard partition covers all ids once across all workers.
- Per-worker verification:
  - row count matches assigned range,
  - ascending id order,
  - value formula correctness.
- Aggregate verification:
  - total verified rows equals `rows`,
  - no overlaps/gaps by deterministic shard math.
- Print total duration and aggregated rows/sec.

8. Add top-level Go test entry and output format.
- Implement one orchestrating test, e.g. `TestFlightSQLBenchmarks`.
- Fail fast on setup/validation errors.
- Print sectioned results with stable markers:
  - `=== PHASE: ...`
  - `duration=... rows=... rate=...`.
- Keep test deterministic and non-random by default.

9. Add runner script `run_flight_go_bench.sh`.
- Script behavior:
  - parse optional args:
    - `--shell-binary`, `--port`, `--rows`, `--workers`, `--batch-size`, `--crud-iters`, `--keep-db`.
  - default shell binary: `build/release/duckdb`.
  - build command if binary missing:
    - `BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release`.
  - create temp DB file path (file-backed benchmark target).
  - start daemon:
    - `duckdb --batch --init /dev/null -flight-sql <port> <db_path>`.
  - readiness loop:
    - run lightweight Go ping/select (`SELECT 1`) via the same Go test harness with `-run TestPing`.
  - run benchmark test:
    - `go test -v -count=1 -run TestFlightSQLBenchmarks ./... -- -host ... -port ... -rows ...`.
  - shutdown daemon with `SIGINT`, wait, force kill on timeout.
  - print DB size:
    - `du -h <db_path>` and `stat -c%s <db_path>`.
  - cleanup temp files unless `--keep-db`.

10. Document usage.
- `extension/flight/test/go/README.md`:
  - prerequisites (`go` toolchain),
  - dependency note for Arrow Go v18 driver,
  - run examples for default and custom row/worker settings,
  - expected output sections,
  - note that this is a heavy benchmark path not run in pytest by default.

### Test Cases and Scenarios

1. Connectivity sanity.
- Go harness can connect with `database/sql` driver and run `SELECT 1`.

2. CRUD single-op timing.
- Outputs timing for create/insert/update/select/delete/drop.
- No SQL errors; phase completes successfully.

3. Batch insert 100k correctness.
- Inserts exactly 100k rows.
- Count/min/max checks pass.
- Time and throughput printed.

4. 200-goroutine concurrent insert correctness.
- Inserts exactly 100k distinct ids.
- Distinct/count/sum checks pass.
- Time and throughput printed.

5. Ordered read single correctness.
- Reads 100k rows in order.
- Value formula and row count validated.

6. Ordered read concurrent correctness.
- 200 workers read disjoint ordered shards.
- Aggregate verified rows equals 100k.
- No worker validation errors.

7. Runner lifecycle robustness.
- Server startup readiness polling works.
- SIGINT shutdown completes cleanly.
- DB file size printed.
- Cleanup removes temp artifacts by default.

### Assumptions and Defaults
- Benchmark is script-driven/manual (not part of pytest default suite).
- Arrow Go import path uses v18 module:
  - `github.com/apache/arrow-go/v18/arrow/flight/flightsql/driver`.
- Flight SQL transport is plaintext (`tls=disabled`).
- Default workload parameters:
  - `rows=100000`,
  - `workers=200`,
  - `batch-size=1000`,
  - `crud-iters=2000`.
- Concurrent read benchmark verifies full dataset coverage once across workers (sharded), not 200 full-table scans.
- Script runs from repo root and expects writable temp area for DB file.
