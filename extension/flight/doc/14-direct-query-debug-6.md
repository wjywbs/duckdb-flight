# Direct Query Inflated Count Debug Log 6

## Goal
Continue from:
- `extension/flight/doc/14-direct-query-debug-4.md`
- `extension/flight/doc/14-direct-query-debug-5.md`

Perform the remaining "best next step":
- decide whether the right fix belongs in DuckDB core or only in Flight

## Environment
- Date: 2026-03-06
- Build:
  - `ninja -C build/release src/libduckdb.so duckdb flight.duckdb_extension test/unittest`
- Repros used:
  - standalone C++ repro:
    - `extension/flight/test/client/prepared_prune_repro.cpp`
  - raw Flight repros:
    - `TestRawNonPreparedCountReproducer`
    - `TestRawPreparedLiteralCountReproducer`
- Debug env:
  - `DUCKDB_FLIGHT_DEBUG_DIRECT=1`

## Main New Result
The DuckDB fix can be made in core. It is not necessary to solve this only in Flight code.

I implemented a proof-of-concept core fix and validated it against:
- the standalone `duckdb.hpp` reproducer
- the Flight direct statement path
- the Flight prepared literal no-parameter path

With that core fix in place, the stale-plan bug stops reproducing without using the Flight-side `DUCKDB_FLIGHT_FORCE_AUTOCOMMIT_REBIND=1` workaround.

## What I Tried

### First core idea
Mark the statement as `always_require_rebind` when statistics propagation makes a snapshot-sensitive semantic rewrite.

Concrete hook:
- `src/optimizer/statistics/operator/propagate_get.cpp`
  - call `optimizer.binder.SetAlwaysRequireRebind()` when table-filter propagation returns:
    - `FILTER_ALWAYS_TRUE`
    - `FILTER_TRUE_OR_NULL`
    - `FILTER_FALSE_OR_NULL`
    - `FILTER_ALWAYS_FALSE`
- `src/optimizer/statistics/operator/propagate_aggregate.cpp`
  - call the same hook when `TryExecuteAggregates(...)` replaces `COUNT(*)` with a constant result from partition stats

### Why the first attempt appeared to fail
At first the standalone repro still returned:
- `prepared=500 fresh=250 expected=250`

The new debug logs showed why:
- optimizer-side `binder_always_require_rebind=1` was set correctly during prepare
- but `PreparedStatementData::properties.always_require_rebind` was still `0` during execute

That turned out to be a second bug in the proof-of-concept wiring:
- `ClientContext::CreatePreparedStatementInternal(...)` copies planner/binder properties into `PreparedStatementData` before optimization
- later optimizer changes to binder statement properties were being lost
- there was also a stale overwrite after optimization that restored the pre-optimizer property copy

## Important Fix To The Fix

### `src/main/client_context.cpp`
I changed the prepare path to refresh `PreparedStatementData::properties` from:
- `logical_planner.binder->GetStatementProperties()`

after optimization, while preserving:
- `parameter_count`
- `bound_all_parameters`

This was necessary for the optimizer's `SetAlwaysRequireRebind()` signal to survive into execution.

Without that handoff change, the optimizer fix did nothing at runtime.

## Validation

### 1. Standalone `duckdb.hpp` repro now stops reproducing
Command:

```bash
DUCKDB_FLIGHT_DEBUG_DIRECT=1 /tmp/prepared_prune_repro
```

Before the core fix:
- output:
  - `prepared=500 fresh=250 expected=250`
- exit code:
  - `0`

After the core fix:
- output:
  - `prepared=250 fresh=250 expected=250`
  - `repro did not trigger`
- exit code:
  - `1`

Most important log lines:
- `mark_requires_rebind reason=snapshot_sensitive_rewrite`
- `binder_always_require_rebind=1`
- `require_rebind begin ... always_require_rebind=1`
- `require_rebind decision=1 reason=always_require_rebind`

That is the first clean proof that the standalone DuckDB bug can be fixed in core without relying on Flight.

### 2. New API regression test passes
I added:
- `test/api/test_prepared_api.cpp`
  - `Prepared statements rebind after snapshot-pruned statistics rewrite`

Test shape:
1. create a table
2. insert only `worker_id = 14`
3. prepare `COUNT(*) WHERE worker_id = 14`
4. insert committed rows for `worker_id = 15`
5. execute prepared statement
6. compare to fresh query

Verification:

```bash
build/release/test/unittest "Prepared statements rebind after snapshot-pruned statistics rewrite"
```

Result:
- passed

### 3. Flight prepared literal path passes without Flight workaround
Server:

```bash
DUCKDB_FLIGHT_DEBUG_DIRECT=1 build/release/duckdb --batch --init /dev/null -flight-sql 39041 /tmp/flight_debug_39041.db
```

No `DUCKDB_FLIGHT_FORCE_AUTOCOMMIT_REBIND=1` was set.

Client:

```bash
FLIGHTSQL_RUN_REPRO=1 go test -v -count=1 -run TestRawPreparedLiteralCountReproducer ./... -args -host 127.0.0.1 -port 39041
```

Result:
- pass

Important log evidence:
- prepare-time bad literal plans still sometimes appear with:
  - `binder_always_require_rebind=1`
- execute-time now shows:
  - `always_require_rebind=1`
  - `decision=1 reason=always_require_rebind`
- rebuilt execution returns:
  - `value0=250`

### 4. Flight direct raw repro no longer failed in the tested window
Client:

```bash
FLIGHTSQL_RUN_REPRO=1 FLIGHTSQL_REPRO_ITERS=10 FLIGHTSQL_REPRO_WORKERS=16 FLIGHTSQL_REPRO_ROWS_PER_WORKER=250 go test -v -count=1 -run TestRawNonPreparedCountReproducer ./... -args -host 127.0.0.1 -port 39041
```

Result:
- 10 iterations passed
- test ended as:
  - `raw reproducer did not fail in 10 iterations (inconclusive)`

This is consistent with the standalone fix and the prepared-literal fix.

## Updated Interpretation
The actual core problem is now clearer:

1. DuckDB allows statistics propagation to do snapshot-sensitive semantic rewrites during prepare.
2. Those rewrites can remove filters or collapse the plan to `EMPTY_RESULT` / constant-count forms.
3. Reusable prepared plans only invalidate on catalog identity and parameter conditions by default.
4. Therefore a later execution can reuse a snapshot-pruned plan incorrectly.

The proof-of-concept core fix changes that contract to:
- if statistics propagation makes a semantic rewrite that is snapshot-sensitive,
- mark the prepared statement as requiring rebind on execution

That is enough to make the confirmed bug disappear.

## Decision
The main fix should be in DuckDB core, not only in Flight.

Reason:
- the bug reproduces without Flight
- the standalone `duckdb.hpp` reproducer is now fixed by the core patch
- Flight direct and prepared no-parameter paths both benefit automatically once DuckDB handles the prepared-plan reuse correctly

So the preferred path is:
- upstream DuckDB core fix
- Flight workaround only as a temporary local mitigation if needed while the core fix is under review

## Remaining Caveat
This proof-of-concept core fix is strong for the confirmed failure mode, but it is still targeted.

What it currently covers:
- `LogicalGet` table-filter pruning that changes semantics
- `COUNT(*)` rewrite from partition stats

What I have **not** exhaustively audited yet:
- every other statistics-driven semantic rewrite in the optimizer, e.g. certain join-condition pruning paths

So the current conclusion is:
- yes, a DuckDB fix can be made and works for the confirmed bug
- but before upstreaming, it is worth deciding whether to:
  - keep this targeted fix, or
  - expand the same rule to any other snapshot-sensitive statistics rewrites

## Files Touched During This Step
- `src/include/duckdb/optimizer/statistics_propagator.hpp`
- `src/optimizer/statistics/operator/propagate_get.cpp`
- `src/optimizer/statistics/operator/propagate_aggregate.cpp`
- `src/main/client_context.cpp`
- `test/api/test_prepared_api.cpp`
