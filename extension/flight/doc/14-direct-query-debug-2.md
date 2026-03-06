# Direct Query Inflated Count Debug Log 2

## Goal
Continue from `extension/flight/doc/14-direct-query-debug.md` and pinpoint the DuckDB core boundary where the bad direct-query count happens when `rebind = false`.

## Environment
- Date: 2026-03-05
- Build: incremental `ninja -C build/release duckdb`
- Repro target: `extension/flight/test/go/flightsql_raw_repro_test.go`

## New Repro Runs

### Run A
- Server:
  - `DUCKDB_FLIGHT_DEBUG_DIRECT=1 build/release/duckdb --batch --init /dev/null -flight-sql 39022 /tmp/flight_debug_39022.db`
- Client:
  - `FLIGHTSQL_RUN_REPRO=1 FLIGHTSQL_REPRO_ITERS=20 FLIGHTSQL_REPRO_WORKERS=16 FLIGHTSQL_REPRO_ROWS_PER_WORKER=250 go test -v -count=1 -run TestRawNonPreparedCountReproducer ./... -args -host 127.0.0.1 -port 39022`
- Result:
  - failed on iteration 3
  - `worker=12 mismatch direct=250 direct2=500 prepared=250 fresh=250 probe=[250 250 250] expected=250 commit=true`

### Run B
- Server:
  - `DUCKDB_FLIGHT_DEBUG_DIRECT=1 build/release/duckdb --batch --init /dev/null -flight-sql 39023 /tmp/flight_debug_39023.db`
- Client:
  - `FLIGHTSQL_RUN_REPRO=1 FLIGHTSQL_REPRO_ITERS=20 FLIGHTSQL_REPRO_WORKERS=16 FLIGHTSQL_REPRO_ROWS_PER_WORKER=250 go test -v -count=1 -run TestRawNonPreparedCountReproducer ./... -args -host 127.0.0.1 -port 39023`
- Result:
  - failed on iteration 13
  - `worker=10 mismatch direct=250 direct2=500 prepared=250 fresh=250 probe=[250 250 250] expected=250 commit=true`

### Run C
- Server:
  - `DUCKDB_FLIGHT_DEBUG_DIRECT=1 build/release/duckdb --batch --init /dev/null -flight-sql 39026 /tmp/flight_debug_39026.db`
- Client:
  - `FLIGHTSQL_RUN_REPRO=1 FLIGHTSQL_REPRO_ITERS=20 FLIGHTSQL_REPRO_WORKERS=16 FLIGHTSQL_REPRO_ROWS_PER_WORKER=250 go test -v -count=1 -run TestRawNonPreparedCountReproducer ./... -args -host 127.0.0.1 -port 39026`
- Result:
  - failed on iteration 5
  - `worker=4 mismatch direct=500 direct2=250 prepared=250 fresh=250 probe=[250 250 250] expected=250 commit=true`

### Run D
- Server:
  - `DUCKDB_FLIGHT_DEBUG_DIRECT=1 build/release/duckdb --batch --init /dev/null -flight-sql 39027 /tmp/flight_debug_39027.db`
- Client:
  - `FLIGHTSQL_RUN_REPRO=1 FLIGHTSQL_REPRO_ITERS=20 FLIGHTSQL_REPRO_WORKERS=16 FLIGHTSQL_REPRO_ROWS_PER_WORKER=250 go test -v -count=1 -run TestRawNonPreparedCountReproducer ./... -args -host 127.0.0.1 -port 39027`
- Result:
  - failed on iteration 2
  - `worker=10 mismatch direct=500 direct2=250 prepared=250 fresh=250 probe=[250 250 250] expected=250 commit=true`

## Most Important New Finding

The bad direct statement is now pinned below `PendingPreparedStatement(...)` and specifically inside the regular table-scan setup:

- good executions of `SELECT CAST(COUNT(*) AS BIGINT) FROM go_flight_tx_bench WHERE worker_id = 10`
  reach `DuckTableScanState::InitLocalState(...)` with `filters=1`
  and `DuckTableScanState::TableScanFunc(...)` emits `rows=250`
- the bad execution of the exact same SQL reaches `DuckTableScanState::InitLocalState(...)` with `filters=0`
  and `DuckTableScanFunc(...)` emits `rows=500`

That means the overcount is not created in `stream_fetch`, Arrow, or aggregate finalization. It is already present in the rows returned by the table scan because the pushed-down `worker_id` filter disappeared for that prepared execution.

The bad path is now narrowed to reuse of a previously prepared `PreparedStatementData::physical_plan` across the Flight `GetFlightInfoStatement -> DoGetStatement` gap when `RequireRebind(...)` returns false.

That is more specific than the prior note. The direct statement path is not just "using a cached prepared statement" in the abstract. It is:

1. Preparing the physical plan in one short-lived auto-commit transaction during `GetFlightInfoStatement`.
2. Storing that plan inside `PreparedStatementData`.
3. Later executing the same `PreparedStatementData` in a different auto-commit transaction during `DoGetStatement`, if `rebind = false`.

The parameterized `worker_id = ?` path does not do this, because it hits `RequireRebind(...) == true` and rebuilds the prepared data immediately before execution.

## Source Trace

### 1. Flight direct statement prepares during `GetFlightInfoStatement`
- `extension/flight/src/duckdb_flight_sql_server.cpp:772-826`
  - `CreateStatementPreparedState(...)` creates a fresh `Connection` and calls `state->connection->Prepare(query)` for non-transaction statements.
- `extension/flight/src/duckdb_flight_sql_server.cpp:847-869`
  - `GetFlightInfoStatement(...)` calls `CreateStatementPreparedState(...)`.

### 2. DuckDB prepare happens inside its own auto-commit transaction
- `src/main/client_context.cpp:766-776`
  - `ClientContext::PrepareInternal(...)` wraps preparation in `RunFunctionInTransactionInternal(..., false)`.
- `src/main/client_context.cpp:1210-1225`
  - `RunFunctionInTransactionInternal(...)` starts a new transaction when the connection is in auto-commit mode.
  - It commits that transaction after `CreatePreparedStatement(...)` finishes.

### 3. The physical plan is materialized and stored in `PreparedStatementData`
- `src/main/client_context.cpp:382-442`
  - `CreatePreparedStatementInternal(...)` binds, optimizes, and creates `result->physical_plan`.
- `src/include/duckdb/main/prepared_statement_data.hpp`
  - `PreparedStatementData` owns `unique_ptr<PhysicalPlan> physical_plan`.

### 4. Later execute reuses that same prepared object if rebind stays false
- `extension/flight/src/duckdb_flight_sql_server.cpp:872-980`
  - `DoGetStatement(...)` eventually calls `state->prepared->Execute(empty_parameters, true)`.
- `src/main/prepared_statement.cpp:71-115`
  - `PreparedStatement::Execute(...)` routes back into `context->PendingQuery(query, data, parameters)`.
- `src/main/client_context.cpp:610-649`
  - `PendingPreparedStatement(...)` decides whether to rebind.
- `src/main/client_context.cpp:547-607`
  - `PendingPreparedStatementInternal(...)` executes the already stored `PreparedStatementData`.

### 5. Rebind replaces the prepared data entirely
- `src/main/client_context.cpp:508-522`
  - `RebindPreparedStatement(...)` calls `CreatePreparedStatement(...)` on `prepared->unbound_statement->Copy()`.
  - It swaps in a new `PreparedStatementData`.

This is the key behavioral difference:
- `rebind = false`: execute the old `PreparedStatementData::physical_plan`
- `rebind = true`: build a new `PreparedStatementData::physical_plan` and execute that one

## Why This Matters

`RequireRebind(...)` does not consider table/storage-version changes from concurrent commits. It only considers:
- `always_require_rebind`
- unresolved parameters
- parameter type changes
- catalog identity changes

Relevant code:
- `src/main/prepared_statement_data.cpp:64-136`

So the direct literal statement path can legally reuse a plan that was prepared before other workers finished committing/rolling back, as long as the catalog did not change.

That is exactly the suspicious window in the repro:
- first direct query happens while the concurrent tx phase is still in flight
- later fresh/probe direct queries are correct
- parameterized prepared query is correct because it forces rebind

## Updated Interpretation

The failure boundary is no longer just "somewhere in DuckDB execution".

It is now:
- old `PreparedStatementData` produced by `CreatePreparedStatementInternal(...)`
- surviving across the Flight RPC boundary
- then being executed without `RebindPreparedStatement(...)`
- and, in the failing executions I captured, reaching `DuckTableScanState` with `input.filters == nullptr`
- which makes the scan emit `500` rows instead of `250`

I do **not** yet know whether the filter is already missing in the prepared physical plan, or whether it is dropped later while initializing execution state. But the bug is now tightly scoped to:
- prepared physical plan reuse across transaction boundary for direct Flight statements,
- not Arrow decoding,
- not Flight stream conversion,
- not generic query text parsing,
- not the rebind path,
- and not aggregate post-processing.

## Runtime Evidence That Supports This Narrower Scope

From `/tmp/flight_debug_39027.log` for the failing `worker_id = 10` direct query:
- `create_statement ... prepared_data=0xfdf6ec00b860 ... sql="... worker_id = 10"`
- `doget_statement ... prepared_data=0xfdf6ec00b860 ... sql="... worker_id = 10"`
- `require_rebind decision=0 reason=cache_reuse`
- `pending_prepared begin ... prepared_data=0xfdf6ec00b860 ...`
- `table-scan init_local table=go_flight_tx_bench ... filters=0 projection_ids=0`
- `table-scan getdata ... rows=0x1f4 cols=0x1`
- `stream_fetch ... value0=500`

From the good `worker_id = 10` direct executions in the same log:
- `table-scan init_local table=go_flight_tx_bench ... filters=0x1 projection_ids=0`
- `table-scan getdata ... rows=0xfa cols=0x1`
- `stream_fetch ... value0=250`

Across `/tmp/flight_debug_39022.log`, `/tmp/flight_debug_39023.log`, `/tmp/flight_debug_39026.log`, and `/tmp/flight_debug_39027.log` the failures still share the same higher-level signature:
- the wrong value is emitted from core in `stream_fetch`
- rebind remains `0`
- fresh/probe later return `250`
- prepared `worker_id = ?` later returns `250`

The last point is important:
- if the underlying table were actually corrupted, fresh/probe direct queries should also stay wrong
- but they do not
- so the stale/non-rebound execution window remains the strongest explanation

## Instrumentation Progress

The logs that paid off:
- `src/function/table/table_scan.cpp`
  - `DuckTableScanState::InitLocalState(...)`
  - `DuckTableScanState::TableScanFunc(...)`
- these are the first logs that showed the bad path concretely:
  - bad execution: `filters=0`, `rows=500`
  - good execution: `filters=1`, `rows=250`

The logs that still have gaps:
- `src/execution/operator/scan/physical_table_scan.cpp`
- `src/execution/operator/aggregate/physical_ungrouped_aggregate.cpp`
- `src/main/client_context.cpp` one-line plan logging

These either did not show up or were too interleaved to be useful in the failing runs. The lower table-scan logs were much clearer.

## Best Current Hypothesis

The stale object is specifically the prepared physical plan created in `CreatePreparedStatementInternal(...)` during Flight `GetFlightInfoStatement`. When `RequireRebind(...)` returns false, DuckDB executes that old plan in a later transaction during `DoGetStatement`, and that stale plan sometimes overcounts under concurrent commit/rollback activity.

The new evidence sharpens that:
- the bad execution sometimes initializes `DuckTableScanState` without the pushed-down `worker_id` filter
- the scan then returns `500` rows
- the final `COUNT(*)` simply reflects those `500` rows

## Next Debug Targets

1. Determine whether the bad `PreparedStatementData` is born with no pushed-down filter, or whether the filter is dropped later during executor/table-scan setup.
2. Instrument the prepare-time physical plan in a grep-friendly one-line form for each `prepared_data` pointer.
3. Inspect the regular scan binding path for mutable state that can depend on the prepare-time transaction:
   - `TableScanBindData`
   - `PhysicalTableScan.table_filters`
   - `TableFunctionInitInput.filters`
4. Add a one-off experiment to force rebind specifically when executing a `PreparedStatementData` created in a different auto-commit transaction than the current one.
5. If that eliminates the bug cleanly, the likely fix direction is:
   - expand `RequireRebind(...)` criteria for this class of prepared statement reuse, or
   - avoid prepare-on-GetFlightInfo for direct statement tickets and instead prepare at `DoGetStatement` time.
