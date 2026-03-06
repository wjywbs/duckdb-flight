# Direct Query Inflated Count Debug Log 4

## Goal
Continue from `extension/flight/doc/14-direct-query-debug-3.md` and pin down why `RequireRebind(...)` stays false even after we proved that optimizer statistics can erase the direct literal `worker_id` filter.

## Environment
- Date: 2026-03-05
- Build: incremental `ninja -C build/release duckdb flight.duckdb_extension`
- Repro target: `extension/flight/test/go/flightsql_raw_repro_test.go`
- Debug env: `DUCKDB_FLIGHT_DEBUG_DIRECT=1`

## New Instrumentation

### `src/main/client_context.cpp`
- Added `prepare_props` logging in `ClientContext::CreatePreparedStatementInternal(...)`.
- It logs:
  - prepare-time `tx_global`
  - stored `read_dbs`
  - stored `modified_dbs`
  - parameter / rebind-related properties

### `src/main/prepared_statement_data.cpp`
- Extended `PreparedStatementData::RequireRebind(...)` logging.
- It now logs:
  - execute-time `tx_global`
  - stored read/modified catalog identities
  - current read/modified catalog identities seen during the rebind check

These two hooks finally put prepare-time and execute-time invalidation state into the same trace.

## New Repro Runs

### Run G
- Server:
  - `DUCKDB_FLIGHT_DEBUG_DIRECT=1 build/release/duckdb --batch --init /dev/null -flight-sql 39037 /tmp/flight_debug_39037.db`
- Client:
  - `FLIGHTSQL_RUN_REPRO=1 FLIGHTSQL_REPRO_ITERS=30 FLIGHTSQL_REPRO_WORKERS=16 FLIGHTSQL_REPRO_ROWS_PER_WORKER=250 go test -v -count=1 -run TestRawNonPreparedCountReproducer ./... -args -host 127.0.0.1 -port 39037`
- Result:
  - failed on iteration 1
  - `worker=14 mismatch direct=250 direct2=500 prepared=250 fresh=250 probe=[250 250 250] expected=250 commit=true`

This run produced the cleanest new example of the core issue:
- first direct query prepared with `tx_global=27`
- same statement executed with `tx_global=28`
- `stored_read_dbs == current_read_dbs`
- `RequireRebind(...)` still returned `cache_reuse`

### Run H
- Server:
  - `DUCKDB_FLIGHT_DEBUG_DIRECT=1 build/release/duckdb --batch --init /dev/null -flight-sql 39038 /tmp/flight_debug_39038.db > /tmp/flight_debug_39038.log 2>&1`
- Client:
  - `FLIGHTSQL_RUN_REPRO=1 FLIGHTSQL_REPRO_ITERS=10 FLIGHTSQL_REPRO_WORKERS=16 FLIGHTSQL_REPRO_ROWS_PER_WORKER=250 go test -v -count=1 -run TestRawNonPreparedCountReproducer ./... -args -host 127.0.0.1 -port 39038`
- Result:
  - failed on iteration 5
  - `worker=12 mismatch direct=250 direct2=250 prepared=250 fresh=500 probe=[250 250 250] expected=250 commit=true`

This run is useful because the redirected log file gives stable line numbers for the new rebind instrumentation.

## Most Important New Finding

The bug is now narrower and more precise than in Debug Log 3:

- the stale direct plan is reused across different auto-commit transactions
- `RequireRebind(...)` does notice the transaction is different only indirectly through `tx_global`
- but it does **not** treat that as invalidation
- instead it only compares stored and current catalog identities
- row-level commits that change table statistics do not change those catalog identities

So the bad plan is not surviving because the rebind check is buggy in some random way.
It is surviving because the current rebind contract only knows about:
- parameter typing
- `always_require_rebind`
- catalog identity changes

It does **not** know about:
- data snapshot changes
- table-statistics changes caused by commits in other transactions

That is exactly the mismatch that lets a snapshot-specific `FILTER_ALWAYS_TRUE` / `FILTER_ALWAYS_FALSE` optimization survive into later transactions.

## Clean Log Evidence

### Worker 11: prepare in one transaction, execute in a later transaction, no rebind

From `/tmp/flight_debug_39038.log`:

- line 10:
  - `prepare_props ... tx_global=24 read_dbs="flight_debug_39038{oid=592,version=1}"`
- lines 14-18:
  - pushdown generates `worker_id=11`
- line 29:
  - `stats ... result=FILTER_ALWAYS_FALSE ... stats_before="[Min: 2, Max: 4] ..."`
- lines 30-34:
  - logical/physical plan becomes `EMPTY_RESULT`
- line 60:
  - `doget_statement ... prepared_data=0xe0eff8036270`
- line 61:
  - `require_rebind begin ... tx_global=27 ... stored_read_dbs="flight_debug_39038{oid=592,version=1}" current_read_dbs="flight_debug_39038{oid=592,version=1}"`
- line 62:
  - `require_rebind decision=0 reason=cache_reuse`

This is the strongest direct proof of the missing invalidation boundary:
- prepare happened in transaction 24
- execute happened in transaction 27
- catalog identity stayed at version 1
- rebind reused the plan even though the plan already contained snapshot-derived pruning

### Worker 4: healthy case shows the same reuse policy

The same file also shows the healthy path:

- line 11:
  - `prepare_props ... tx_global=23 ... worker_id = 4`
- line 26:
  - `stats ... result=NO_PRUNING_POSSIBLE`
- lines 28 and 33:
  - plan keeps `worker_id=4`
- lines 76-80:
  - `doget_statement` runs later with `tx_global=28`
- line 77:
  - stored/current catalog identity is still `version=1`
- line 78:
  - `require_rebind decision=0 reason=cache_reuse`

So the rebind rule is consistent:
- if the catalog identity did not change, reuse is allowed
- that policy is fine for catalog-stable plans
- it is **not** fine for plans whose filter pruning depended on the prepare snapshot's table statistics

## Source-Level Interpretation

Reading the relevant code now matches the log behavior:

### `StatementProperties::RegisterDBRead`
- `src/common/enums/statement_type.cpp`
- stores `CatalogIdentity {catalog_oid, catalog.GetCatalogVersion(context)}`

That means prepared-statement invalidation records a catalog version, not a data/snapshot version.

### `PreparedStatementData::RequireRebind`
- `src/main/prepared_statement_data.cpp`
- only checks:
  - `always_require_rebind`
  - unresolved parameter typing
  - parameter type changes
  - read/modified catalog identity changes

It does not check whether the current execution transaction sees a different committed row set than the transaction that produced the physical plan.

### `DuckTransactionManager`
- `src/transaction/duck_transaction_manager.cpp`
- commit only publishes a new catalog version if `transaction.catalog_version >= TRANSACTION_ID_START`
- `transaction.catalog_version` is advanced in `PushCatalogEntry(...)` and `PushAttach(...)`

Inference from source:
- catalog version tracks catalog changes
- it does not appear to track normal row-appending commits
- therefore inserts that change visible worker rows can change table statistics without changing the catalog identity used by `RequireRebind(...)`

That matches the new logs exactly.

## Updated Root Cause

The bug is now best stated as:

- Flight direct statements are prepared in one auto-commit transaction and executed in a later auto-commit transaction when `rebind = false`
- optimizer statistics on `LogicalGet` can prune the literal `worker_id = N` filter to `FILTER_ALWAYS_TRUE` or `FILTER_ALWAYS_FALSE` using the prepare snapshot
- that pruning is stored inside the cached prepared physical plan
- `RequireRebind(...)` only invalidates on catalog identity and parameter conditions
- row-level commits between prepare and execute can change the visible data/statistics without changing catalog identity
- therefore the stale snapshot-specific plan is reused across transactions

That is the first explanation that cleanly accounts for all observed variants:
- direct count `500` after `FILTER_ALWAYS_TRUE`
- empty result after `FILTER_ALWAYS_FALSE`
- parameterized `worker_id = ?` staying correct because it still triggers rebind

## What I Now Think The Actual DuckDB Bug Is

Not just "Flight caches a bad plan", but more specifically:

- DuckDB allows a reusable prepared plan to embed snapshot-sensitive statistics pruning
- the default prepared-statement invalidation logic is only catalog-sensitive
- those two assumptions are incompatible when the prepared plan is executed in a later transaction

That means the deeper bug boundary is probably one of:

1. `RequireRebind(...)` needs an execution-snapshot invalidation signal for plans that were optimized using table statistics.
2. Plans that may execute in a later transaction should not bake in statistics pruning that can collapse filters to always-true/always-false.
3. The Flight direct statement path should force rebind when it prepares in `GetFlightInfoStatement` and executes later in `DoGetStatement` outside a pinned transaction.

I have not chosen between those fixes yet, but the invalid reuse condition is now concrete.

## Best Next Steps

1. Inspect whether DuckDB already has any notion of "data version" or "snapshot invalidation" for prepared statements that can be threaded into `RequireRebind(...)`.
2. If not, test the minimal behavioral fix first:
   - force rebind for non-transaction-owned direct Flight statements between `GetFlightInfoStatement` and `DoGetStatement`
3. After that, decide whether the broader DuckDB fix belongs in:
   - prepared-statement invalidation, or
   - optimizer/statistics pruning for reusable plans
