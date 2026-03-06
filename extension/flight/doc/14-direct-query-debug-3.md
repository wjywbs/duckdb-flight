# Direct Query Inflated Count Debug Log 3

## Goal
Continue from `extension/flight/doc/14-direct-query-debug-2.md` and tighten the boundary where the direct Flight statement loses correctness when `rebind = false`.

## Environment
- Date: 2026-03-05
- Build: incremental `ninja -C build/release duckdb`
- Repro target: `extension/flight/test/go/flightsql_raw_repro_test.go`
- Debug env: `DUCKDB_FLIGHT_DEBUG_DIRECT=1`

## New Instrumentation

### `src/execution/physical_plan/plan_get.cpp`
- Added one-line logging for `LogicalGet -> PhysicalTableScan` creation on the benchmark table.
- It logs:
  - logical filter count/text
  - physical filter count/text after `CreateTableFilterSet(...)`
  - projection count
- Important note:
  - the first version logged `op.table_filters` after the filter unique_ptrs had already been moved into the physical set, which showed `worker_id=[null]`
  - I fixed that by capturing the logical filter text before `CreateTableFilterSet(...)`

### `src/execution/operator/scan/physical_table_scan.cpp`
- Added constructor-time logging for `PhysicalTableScan` on the benchmark table.
- It logs:
  - `op` pointer
  - `bind_data`
  - `function`
  - table name if available
  - pushed-down filter count/text

### `src/main/client_context.cpp`
- I tried a prepare-time physical-plan traversal log here.
- That hook caused a standalone `PREPARE` internal error and was removed again.
- The remaining older `pending_prepared` logs are still in place.

## Standalone Sanity Check

To verify the new prepare-time hooks outside Flight, I ran:

```sql
CREATE TABLE go_flight_tx_bench(id BIGINT, worker_id BIGINT, v BIGINT);
INSERT INTO go_flight_tx_bench VALUES (1,14,1),(2,14,2),(3,1,3);
PREPARE s AS SELECT CAST(COUNT(*) AS BIGINT) FROM go_flight_tx_bench WHERE worker_id = 14;
EXECUTE s;
```

The useful output was:

- `[plan] get_create ... logical_filters=1 ... physical_filters_pre=1 physical_filter_text_pre="worker_id=14"`
- `[scan] ctor ... function=seq_scan table=go_flight_tx_bench filters=1 filter_text="worker_id=14"`
- runtime `table-scan init_local ... filters=1`
- runtime count result `2`

So the new hooks do work in a plain DuckDB prepare/execute path, and the healthy prepare path really does build a `PhysicalTableScan` with the `worker_id` filter pushed down.

## New Flight Repro Runs

### Run E
- Server:
  - `DUCKDB_FLIGHT_DEBUG_DIRECT=1 build/release/duckdb --batch --init /dev/null -flight-sql 39030 /tmp/flight_debug_39030.db`
- Client:
  - `FLIGHTSQL_RUN_REPRO=1 FLIGHTSQL_REPRO_ITERS=100 FLIGHTSQL_REPRO_WORKERS=16 FLIGHTSQL_REPRO_ROWS_PER_WORKER=250 go test -v -count=1 -run TestRawNonPreparedCountReproducer ./... -args -host 127.0.0.1 -port 39030`
- Result:
  - failed on iteration 1
  - `worker=14 mismatch direct=500 direct2=250 prepared=250 fresh=250 probe=[250 250 250] expected=250 commit=true`

This run still used the intermediate instrumentation state before the final `plan_get` cleanup, so I used it mainly to confirm the failure still reproduced immediately.

### Run F
- Server:
  - `DUCKDB_FLIGHT_DEBUG_DIRECT=1 build/release/duckdb --batch --init /dev/null -flight-sql 39032 /tmp/flight_debug_39032.db`
- Client:
  - `FLIGHTSQL_RUN_REPRO=1 FLIGHTSQL_REPRO_ITERS=20 FLIGHTSQL_REPRO_WORKERS=16 FLIGHTSQL_REPRO_ROWS_PER_WORKER=250 go test -v -count=1 -run TestRawNonPreparedCountReproducer ./... -args -host 127.0.0.1 -port 39032`
- Result:
  - failed on iteration 1
  - `worker=4 mismatch direct=250 direct2=500 prepared=250 fresh=250 probe=[250 250 250] expected=250 commit=true`

## Most Important New Finding

The key ambiguity from Debug Log 2 is gone:

- the first direct query for `worker_id = 4` in Run F already reached `DuckTableScanState` with `filters=0`
- it returned `250`
- the second direct query for the same SQL, using a different freshly prepared statement, also reached `filters=0`
- it returned `500`

That means:

1. `filters=0` is not "sometimes okay" because of some hidden extra filter we already know about.
2. The first `250` was likely only accidentally correct because the snapshot visible to that first query still had exactly one committed worker's rows.
3. The second `500` is the same bad full-table-style plan after another committed worker became visible.

In other words, the missing pushed-down filter is still the key bad state. The earlier "good" `250` with `filters=0` was just a lucky snapshot.

## Exact Worker 4 Trace From Run F

### First direct query, accidentally correct
- `create_statement id=18 ... prepared_data=0xecba30075210 ... worker_id = 4`
- `doget_statement id=18 ... prepared_data=0xecba30075210`
- `require_rebind decision=0 reason=cache_reuse`
- `table-scan init_local ... filters=0`
- `table-scan getdata ... rows=250`
- `stream_fetch ... value0=250`

### Second direct query, same bad scan state but now visibly wrong
- `create_statement id=19 ... prepared_data=0xecb9c8013a50 ... worker_id = 4`
- `doget_statement id=19 ... prepared_data=0xecb9c8013a50`
- `require_rebind decision=0 reason=cache_reuse`
- `table-scan init_local ... filters=0`
- `table-scan getdata ... rows=500`
- `stream_fetch ... value0=500`

### Later fresh direct queries are good again
- `create_statement id=24 ... prepared_data=0xecb9b800c470 ... worker_id = 4`
- `doget_statement id=24 ...`
- `table-scan init_local ... filters=1`
- `table-scan getdata ... rows=250`
- `stream_fetch ... value0=250`

The prepared `worker_id = ?` verification query still forces rebind and returns `250`.

## Updated Interpretation

This narrows the bug further:

- the direct Flight statement path still prepares in `GetFlightInfoStatement`
- execution still happens later in `DoGetStatement`
- `RequireRebind(...)` still stays `false`
- but now the runtime evidence says the buggy execution state is exactly:
  - a direct literal statement that reaches `DuckTableScanState` with `filters=0`
  - then counts whatever rows happen to be visible in that transaction snapshot

The new important nuance is that this bad state can return the "expected" `250` if only one committed worker is visible at that moment. So `250` is not proof the plan was healthy.

## What Changed In My Confidence

I am now more confident that the wrong direct result is a prepare/bind-time bad state, not an Arrow/streaming issue and not a late aggregate bug.

Why:
- two different direct statements for the same SQL in the same failing run (`prepared_data=0xecba30075210` and `prepared_data=0xecb9c8013a50`) both execute with `filters=0`
- later newly prepared direct statements for the same SQL execute with `filters=1`
- the parameterized path still forces rebind and stays correct

That still does **not** prove the bad `PreparedStatementData` is born wrong in the Flight server path, because the new `plan_get` / scan-constructor logs still did not appear inside the Flight daemon log even though they do appear in the standalone sanity check.

But the prepare-time-vs-execute-time gap is now much smaller:
- separate direct statements can independently land in the same bad `filters=0` runtime state
- the result depends on the visible committed snapshot at execution time

## Remaining Gap

The new prepare-time logs are visible in a standalone DuckDB session, but they still do not show up in the Flight daemon logs for the repro runs.

So one unresolved question remains:
- does the Flight server prepare path actually build the `PhysicalTableScan` without pushed-down filters,
- or is the filter being lost after prepare but before `DuckTableScanState::InitLocalState(...)`?

## Best Next Steps

1. Add a log immediately after `state->connection->Prepare(query)` in `extension/flight/src/duckdb_flight_sql_server.cpp` that inspects the prepared object deeply enough to expose whether its `PreparedStatementData` already points at a plan whose scan has filters.
2. If direct access to the physical plan is awkward there, add a one-off helper on the prepared statement side that returns a concise debug summary for the underlying `PreparedStatementData`.
3. Correlate that server-side prepare summary with:
   - `create_statement` `prepared_data`
   - later `doget_statement` `prepared_data`
   - `table-scan init_local` `filters`
4. Keep the current interpretation in mind during that next pass:
   - a direct result of `250` is not enough
   - the decisive signal is whether the direct statement reached runtime with `filters=0`

## Current Best Hypothesis

The bug still looks like this:

- direct literal Flight statements prepared in one auto-commit transaction can later execute in another auto-commit transaction without rebind
- in the bad window, that direct statement reaches the Duck table scan with no pushed-down `worker_id` filter
- the count then reflects the total currently visible committed rows, which can be `250`, `500`, or likely other multiples of `250` depending on timing
- the parameterized `worker_id = ?` path avoids the bug because it rebuilds via rebind
