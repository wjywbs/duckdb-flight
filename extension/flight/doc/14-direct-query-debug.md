# Direct Query Inflated Count Debug Log

## Goal
Investigate why direct (non-prepared) Flight SQL queries can return inflated counts (often multiples of 250) after tx commit/rollback, while prepared queries return correct values.

## Environment
- Date: 2026-03-04
- Build: `BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release`
- Binary: `build/release/duckdb`
- Reproducer target: `extension/flight/test/go/flightsql_raw_repro_test.go`

## Progress Log

### Step 1: Rebuild baseline
- Rebuilt release with flight extension successfully.

### Step 2: Baseline repro (no new instrumentation)
- Started daemon: `build/release/duckdb --batch --init /dev/null -flight-sql 39001 /tmp/flight_debug.db`
- Ran once (20 iterations configured):
  - `cd extension/flight/test/go && FLIGHTSQL_RUN_REPRO=1 FLIGHTSQL_REPRO_ITERS=20 go test -v -count=1 -run TestRawNonPreparedCountReproducer ./... -args -host 127.0.0.1 -port 39001`
- Result: failed immediately at iteration 1.
- Failure signature:
  - `worker=2 mismatch direct=500 direct2=250 prepared=250 fresh=250 probe=[250 250 250] expected=250 commit=true`
- Confirms the issue is reproducible on current code.

### Step 3: Add core + server instrumentation (not server-only)
Added env-gated logs under `DUCKDB_FLIGHT_DEBUG_DIRECT=1` in:
- `extension/flight/src/duckdb_flight_sql_server.cpp`
  - statement create + DoGet lookup/execute context (statement id, prepared/data pointers, thread)
- `src/main/client_context.cpp`
  - `PendingPreparedStatement` rebind decisions (`rebind_initial`, `rebind_final`)
- `src/main/stream_query_result.cpp`
  - final scalar chunk value emitted by DuckDB execution for matching tx-bench count queries

Query filter used in logs:
- `COUNT(*)` queries over `go_flight_tx_bench` / `go_flight_tx_bench_cpp`

### Step 4: Repro with instrumentation enabled
Daemon:
- `DUCKDB_FLIGHT_DEBUG_DIRECT=1 build/release/duckdb --batch --init /dev/null -flight-sql 39001 /tmp/flight_debug.db`

Repro command:
- `cd extension/flight/test/go && FLIGHTSQL_RUN_REPRO=1 FLIGHTSQL_REPRO_ITERS=20 go test -v -count=1 -run TestRawNonPreparedCountReproducer ./... -args -host 127.0.0.1 -port 39001`

Observed failure:
- `worker=14 mismatch direct=750 direct2=250 prepared=250 fresh=250 probe=[250 250 250] expected=250 commit=true`

Critical evidence from core logs:
- `stream_fetch ... query="... worker_id = 14" ... value0=750`
- This proves the inflated result is already produced inside DuckDB execution, before Arrow/Flight/Go decoding.

### Step 5: Controlled experiment - force rebind for statement path
Temporary experiment (later reverted):
- In `CreateStatementPreparedState`, when debug env is on, set:
  - `state->prepared->data->properties.always_require_rebind = true;`

Run (port 39002, same repro command, 20 iterations):
- All 20 iterations passed (test ended in skip/inconclusive, no failure seen).
- Log showed `rebind_initial=1` for direct statement queries.

Interpretation:
- Forcing rebind removes (or strongly suppresses) the bug.
- Strongly suggests the bad result comes from reusing the non-rebound prepared plan/state for direct statement queries.

### Step 6: Revert forced-rebind experiment and re-verify
Reverted temporary forced-rebind line and rebuilt.

Run (port 39003):
- Failure returned quickly again:
  - `worker=130 mismatch direct=250 direct2=1250 prepared=250 fresh=250 probe=[250 250 250] expected=250 commit=true`

Core evidence again:
- `stream_fetch ... query="... worker_id = 130" ... value0=1250`

### Step 7: Instrument `PreparedStatementData::RequireRebind` with reason codes
Added env-gated (`DUCKDB_FLIGHT_DEBUG_DIRECT=1`) reason logging in:
- `src/main/prepared_statement_data.cpp`

Logged fields:
- `values_count`
- `parameter_count`
- `bound_all_parameters`
- `always_require_rebind`
- decision reason:
  - `always_require_rebind`
  - `not_all_parameters_bound`
  - `parameter_type_changed`
  - `read_catalog_changed`
  - `modified_catalog_changed`
  - `cache_reuse`

### Step 8: Reproduce with lower worker count for cleaner logs
Command (single run, 20 iters, 16 workers):
- `FLIGHTSQL_RUN_REPRO=1 FLIGHTSQL_REPRO_ITERS=20 FLIGHTSQL_REPRO_WORKERS=16 FLIGHTSQL_REPRO_ROWS_PER_WORKER=250 go test -v -count=1 -run TestRawNonPreparedCountReproducer ./... -args -host 127.0.0.1 -port 39011`

Result:
- failed at iteration 2
- `worker=14 mismatch direct=500 direct2=250 prepared=250 fresh=250 probe=[250 250 250] expected=250 commit=true`

Clean log evidence (`/tmp/flight_debug_39011.log`):
- Direct literal path for worker 14:
  - `require_rebind ... values_count=0 parameter_count=0 ... query="... worker_id = 14"`
  - `decision=0 reason=cache_reuse`
  - `rebind_initial=0`
  - `stream_fetch ... worker_id = 14 ... value0=500`
- Parameterized path (`worker_id = ?`) in same run:
  - `require_rebind ... values_count=1 parameter_count=1 ... query="... worker_id = ?"`
  - `decision=1 reason=parameter_type_changed ... incoming=BIGINT expected=INVALID`
  - `rebind_initial=1`
  - `stream_fetch ... worker_id = ? ... value0=250` (or `0` for rollback workers)

This directly ties wrong counts to non-rebound execution and correct counts to rebound execution.

### Step 9: What rebind does exactly in DuckDB core
Code path reviewed:
- `ClientContext::PendingPreparedStatement(...)` in `src/main/client_context.cpp`
  - Calls `prepared->RequireRebind(...)`.
  - If true, calls `ClientContext::RebindPreparedStatement(...)`.
- `ClientContext::RebindPreparedStatement(...)`
  - Recreates prepared data from `prepared->unbound_statement->Copy()` via `CreatePreparedStatement(...)`.
  - Preserves parameter count (`new_prepared->properties.parameter_count = prepared->properties.parameter_count`).
  - Replaces `prepared` pointer with newly created object.
  - Sets `prepared->properties.bound_all_parameters = false`.
- Then `PendingPreparedStatementInternal(...)`
  - Calls `BindPreparedStatementParameters(...)` -> `PreparedStatementData::Bind(...)` to bind values.
  - Initializes executor and runs with the freshly rebound physical plan.

So in practice, rebind is a full re-plan/re-prepare from SQL AST (`unbound_statement`) before execution, not just rebinding parameter values into the old physical plan.

### Step 10: Interpretation of `always_require_rebind`
- Setting `state->prepared->data->properties.always_require_rebind = true;` forces `RequireRebind(...)` to return true on every execute.
- That routes statement execution through the same "recreate prepared data from unbound statement" path each time.
- Empirically, this removes the inflated-count symptom in the repro runs.

## Current Working Hypothesis
The issue is in DuckDB execution/planning of literal direct statement path when the statement is prepared in `GetFlightInfoStatement` and executed later in `DoGetStatement` without rebind.

Most likely class of bug:
- stale/non-rebound prepared physical plan behavior under heavy concurrent commit/rollback workload for literal predicate query,
- while parameterized prepared queries (which take a different rebind/bind path) return correct values.

Why this hypothesis is high-confidence:
1. Wrong values are emitted from DuckDB core (`stream_fetch`), not client decoding.
2. Wrong values are multiples of 250 (workload row block), consistent with planning/execution state mismatch.
3. Forced rebind on statement execution makes the reproducer stable in this run.
4. Reverting forced rebind brings the bug back.
5. New reason-level logs show failing direct statements take `decision=0 reason=cache_reuse`, while correct parameterized statements take `decision=1 reason=parameter_type_changed` and run through rebind.

## Next Debug Targets (if continuing)
1. Compare plan/executor state for failing direct literal query with and without forced rebind.
2. Inspect `RebindPreparedStatement` effects vs non-rebind path in `ClientContext::PendingPreparedStatement`.
3. Trace table-scan/filter state construction lifetime for prepared statements executed without rebind under concurrent DML.
