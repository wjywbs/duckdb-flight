# Title

Prepared statements can reuse snapshot-pruned plans across transactions because `RequireRebind()` only checks catalog identity

# Description

## Summary

I believe this is a DuckDB core bug in prepared-statement invalidation, not a Flight-only bug.

The failure mode is:

- a statement is prepared in one auto-commit transaction
- optimizer statistics prune a pushed-down literal filter during prepare
- the prepared plan is later executed in a different auto-commit transaction
- `PreparedStatementData::RequireRebind(...)` returns `false`
- the stale plan is reused even though the visible row set has changed

In my case this showed up through Flight SQL because the direct statement path naturally splits:

- prepare in `GetFlightInfoStatement`
- execute later in `DoGetStatement`

But the deeper problem appears to be that a reusable prepared plan can embed snapshot-dependent statistics pruning while the rebind check only invalidates on parameter and catalog-identity changes.

## Actual Behavior

For a query of the form:

```sql
SELECT CAST(COUNT(*) AS BIGINT)
FROM go_flight_tx_bench
WHERE worker_id = 14;
```

the direct prepared execution sometimes returns:

- `500` instead of `250` when the literal filter was optimized away as always true
- `0` / `EMPTY_RESULT` when the literal filter was optimized away as always false

The parameterized version:

```sql
SELECT CAST(COUNT(*) AS BIGINT)
FROM go_flight_tx_bench
WHERE worker_id = ?;
```

stays correct in the same workload because it forces rebind.

## Expected Behavior

A prepared plan should not be reused across a later execution snapshot if the plan contains optimizer decisions derived from snapshot-dependent table statistics.

At minimum, the prepared execution should not return a different result from a fresh execution of the same SQL solely because the plan was prepared in an earlier transaction.

## Why I Think This Is A DuckDB Bug

I added instrumentation and narrowed the problem to these points:

1. Filter pushdown is not the issue.
   - The literal `worker_id = N` predicate is successfully pushed into `LogicalGet.table_filters`.

2. The filter is removed later by statistics propagation.
   - In `src/optimizer/statistics/operator/propagate_get.cpp`, I observed `FILTER_ALWAYS_TRUE` and `FILTER_ALWAYS_FALSE` on the pushed-down `worker_id` filter.

3. The prepared plan is reused in a later transaction without rebind.
   - In one clean trace, prepare happened at `tx_global=24` and execute happened at `tx_global=27`.
   - `RequireRebind(...)` still returned `decision=0 reason=cache_reuse`.

4. The rebind check only compares catalog identity, not data/snapshot identity.
   - `src/common/enums/statement_type.cpp`
     - `StatementProperties::RegisterDBRead(...)` stores `catalog.GetCatalogVersion(context)`.
   - `src/main/prepared_statement_data.cpp`
     - `PreparedStatementData::RequireRebind(...)` checks:
       - `always_require_rebind`
       - bound parameter state
       - parameter type changes
       - read/modified catalog identity changes

5. Normal row commits do not appear to change the catalog version used by that check.
   - `src/transaction/duck_transaction_manager.cpp`
     - catalog version is advanced in `PushCatalogEntry(...)` / `PushAttach(...)`
     - commit only publishes a new catalog version if that transaction made catalog changes

So the contract mismatch seems to be:

- optimizer pruning is snapshot-sensitive
- prepared-plan invalidation is only catalog-sensitive

That allows a snapshot-pruned plan to survive into later transactions.

## Strongest Evidence

From my debug logs:

- prepare-time:
  - `tx_global=24`
  - `read_dbs="flight_debug_39038{oid=592,version=1}"`
  - pushed-down filter `worker_id=11`
  - statistics propagation result `FILTER_ALWAYS_FALSE`
  - resulting physical plan `EMPTY_RESULT`

- execute-time:
  - same prepared object
  - `tx_global=27`
  - `stored_read_dbs="flight_debug_39038{oid=592,version=1}"`
  - `current_read_dbs="flight_debug_39038{oid=592,version=1}"`
  - `RequireRebind(...)` returns `cache_reuse`

I also observed the symmetric `FILTER_ALWAYS_TRUE` case, where the pushed-down filter disappears and the later execution returns the full visible row count instead of the per-worker row count.

## Why Flight Exposes It Reliably

Flight SQL direct statements naturally prepare and execute in separate calls. When there is no pinned transaction handle, those calls land in separate auto-commit transactions. That makes it much easier to hit than in a simple prepare/execute sequence that stays in one transaction context.

I have high confidence the underlying bug is in DuckDB invalidation semantics, even though the current reproducer is through Flight SQL.

## Possible Fix Directions

I have not implemented a fix yet, but the likely directions seem to be:

1. Extend prepared-statement invalidation to account for execution snapshot / data version changes, not just catalog identity.
2. Prevent snapshot-sensitive statistics pruning from being baked into reusable plans that may execute in a later transaction.
3. As a narrower mitigation, force rebind for prepared executions that cross the prepare/execute boundary without a pinned transaction.

## Reproducer Status

Current reproducer:

- Flight SQL direct statement path
- concurrent workers inserting/committing benchmark rows
- direct literal query sometimes returns inflated count or empty result
- parameterized query remains correct

I have not yet reduced this to a pure non-Flight standalone SQL reproducer, so that is the main missing piece if you want the smallest possible test case.
