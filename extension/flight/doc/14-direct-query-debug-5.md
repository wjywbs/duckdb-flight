# Direct Query Inflated Count Debug Log 5

## Goal
Continue from `extension/flight/doc/14-direct-query-debug-4.md`, perform the listed best next steps, and verify whether the prepared Flight SQL path without parameters also needs the same fix.

## Environment
- Date: 2026-03-06
- Build: `ninja -C build/release duckdb flight.duckdb_extension`
- Repro target: `extension/flight/test/go/flightsql_raw_repro_test.go`
- Debug env:
  - `DUCKDB_FLIGHT_DEBUG_DIRECT=1`
  - optional workaround toggle: `DUCKDB_FLIGHT_FORCE_AUTOCOMMIT_REBIND=1`

## What I Did

### 1. Checked for an existing DuckDB data-version or snapshot invalidation path
I searched the relevant DuckDB code for a prepared-statement invalidation signal that tracks row visibility or data snapshot changes rather than only catalog identity.

Main result:
- I did **not** find an existing prepared-statement invalidation hook that tracks row-level data changes across transactions.

Relevant source evidence:
- `src/common/enums/statement_type.cpp`
  - `StatementProperties::RegisterDBRead(...)` stores `CatalogIdentity {catalog_oid, catalog.GetCatalogVersion(context)}`
- `src/include/duckdb/catalog/catalog.hpp`
  - comment on `GetCatalogVersion(...)` says the catalog version characterizes the current catalog snapshot
- `src/main/prepared_statement_data.cpp`
  - `PreparedStatementData::RequireRebind(...)` still only checks:
    - `always_require_rebind`
    - unresolved parameter typing / parameter type changes
    - stored vs current read/modified catalog identities
- `src/transaction/duck_transaction_manager.cpp`
  - catalog version only advances for catalog changes, not normal row-appending commits

Conclusion:
- the current prepared-statement invalidation path is catalog-sensitive
- it is not data-snapshot-sensitive
- so it will not notice row commits that change table statistics but leave catalog identity unchanged

### 2. Tested the minimal Flight-side workaround
I implemented an env-gated mitigation in `extension/flight/src/duckdb_flight_sql_server.cpp`:

- new env flag:
  - `DUCKDB_FLIGHT_FORCE_AUTOCOMMIT_REBIND=1`
- behavior:
  - for non-transaction-owned Flight statements/prepared handles
  - with no parameters
  - set `prepared->data->properties.always_require_rebind = true` immediately before `Execute(...)`

Purpose:
- force DuckDB to rebuild the prepared plan inside the execution transaction instead of reusing the physical plan from the earlier auto-commit prepare transaction

The hook currently runs in:
- `DoGetStatement`
- `DoGetPreparedStatement`
- `DoPutPreparedStatementUpdate` when there are no ordered parameters

This is only a validation workaround, not a final upstream fix.

## Repro Runs

### Run I: direct raw Flight repro with workaround enabled
- Server:
  - `DUCKDB_FLIGHT_DEBUG_DIRECT=1 DUCKDB_FLIGHT_FORCE_AUTOCOMMIT_REBIND=1 build/release/duckdb --batch --init /dev/null -flight-sql 39039 /tmp/flight_debug_39039.db > /tmp/flight_debug_39039.log 2>&1`
- Client:
  - `FLIGHTSQL_RUN_REPRO=1 FLIGHTSQL_REPRO_ITERS=20 FLIGHTSQL_REPRO_WORKERS=16 FLIGHTSQL_REPRO_ROWS_PER_WORKER=250 go test -v -count=1 -run TestRawNonPreparedCountReproducer ./... -args -host 127.0.0.1 -port 39039`
- Result:
  - no failure in 20 iterations
  - test ended with:
    - `raw reproducer did not fail in 20 iterations (inconclusive)`

Important log evidence from `/tmp/flight_debug_39039.log`:
- `force_autocommit_rebind path=doget_statement ...`
- `require_rebind decision=1 reason=always_require_rebind ...`
- rebind then rebuilds the plan in the execution transaction

Interpretation:
- the minimal workaround is strong evidence that the stale-plan reuse boundary is the actual problem

### Run J: prepared Flight SQL path without parameters, no workaround
To check whether the prepared no-parameter path also needs the fix, I added a deterministic raw Flight SQL test:

- new test:
  - `TestRawPreparedLiteralCountReproducer`
- file:
  - `extension/flight/test/go/flightsql_raw_repro_test.go`

The test does:
1. reset the benchmark table
2. insert 250 rows for `worker_id = 14`
3. prepare the literal query:
   - `SELECT CAST(COUNT(*) AS BIGINT) FROM go_flight_tx_bench WHERE worker_id = 14`
4. insert 250 rows for `worker_id = 15`
5. execute the prepared statement with no parameters
6. compare that result to a fresh direct query

No-workaround server:
- `DUCKDB_FLIGHT_DEBUG_DIRECT=1 build/release/duckdb --batch --init /dev/null -flight-sql 39040 /tmp/flight_debug_39040.db > /tmp/flight_debug_39040.log 2>&1`

Client:
- `FLIGHTSQL_RUN_REPRO=1 go test -v -count=1 -run TestRawPreparedLiteralCountReproducer ./... -args -host 127.0.0.1 -port 39040`

Result:
- deterministic failure
- message:
  - `prepared literal mismatch prepared=500 fresh=250 expected=250`

This is the clearest answer to the new question:
- yes, the prepared Flight SQL path without parameters also needs the fix

### Run K: prepared Flight SQL path without parameters, workaround enabled
Using the same test against the workaround server on port `39039`:

- Client:
  - `FLIGHTSQL_RUN_REPRO=1 go test -v -count=1 -run TestRawPreparedLiteralCountReproducer ./... -args -host 127.0.0.1 -port 39039`
- Result:
  - pass

Important log evidence from `/tmp/flight_debug_39039.log`:
- `force_autocommit_rebind path=doget_prepared ... sql="SELECT CAST(COUNT(*) AS BIGINT) FROM go_flight_tx_bench WHERE worker_id = 14"`
- `require_rebind decision=1 reason=always_require_rebind`
- a fresh `prepare_props` entry appears in the execution transaction
- stream returns `value0=250`

## Most Important New Findings

### 1. Debug Log 4's "best next step" test succeeded
The minimal Flight workaround works as predicted:
- force rebind at execution time
- stale snapshot-dependent plan is not reused
- the direct raw Flight repro stops failing in the tested window

### 2. The prepared Flight SQL path without parameters has the same underlying bug
This was previously still an open question.
It is now answered directly:

- prepare literal statement in one auto-commit transaction
- commit rows for a different worker in a later transaction
- execute the prepared literal statement without parameters
- DuckDB reuses the stale plan and returns `500` instead of `250`

So the bug is not limited to the ad hoc direct-statement ticket path.
It also affects prepared Flight statements when:
- the prepared handle is reused across transactions
- no parameter-driven rebind happens

### 3. The workaround scope should cover both Flight paths
If Flight keeps a local mitigation while the deeper DuckDB issue is being resolved, that mitigation likely needs to cover:
- direct statement execution after `GetFlightInfoStatement`
- prepared statement execution with no parameters
- likely prepared statement update execution with no parameters as well

## Clean Log Interpretation

### No-workaround prepared literal failure
From `/tmp/flight_debug_39040.log`:
- prepare-time logs show the statement built for `worker_id = 14`
- optimizer logs show stats-driven pruning based on the prepare snapshot
- execute-time `RequireRebind(...)` logs show:
  - a later `tx_global`
  - unchanged catalog identity
  - `decision=0 reason=cache_reuse`
- stream returns `value0=500`

That is the exact same reuse pattern already seen on the non-prepared direct path.

### Workaround-enabled prepared literal success
From `/tmp/flight_debug_39039.log`:
- `force_autocommit_rebind path=doget_prepared`
- `RequireRebind(...)` flips to `decision=1 reason=always_require_rebind`
- plan is rebuilt in the execution transaction
- stream returns `value0=250`

## Updated Conclusion
The current evidence supports all of the following:

- the deeper bug is in DuckDB prepared-plan reuse semantics, not in Flight result streaming
- the invalid reuse condition happens when a reusable prepared plan captures snapshot-dependent statistics pruning
- `RequireRebind(...)` does not invalidate on row-level data/snapshot changes
- Flight direct statements and Flight prepared statements without parameters are both exposed

## Best Next Step After This
The next decision is no longer "does prepared Flight also need the fix?" because that is now yes.

The next real choice is:
- keep a narrow Flight mitigation for non-transaction-owned no-parameter executions, or
- implement the broader DuckDB fix in prepared-statement invalidation / plan reuse rules

Given the new deterministic prepared-literal repro, the broader DuckDB fix should be preferred if it can be made safely.
