## Flight SQL Prepared Handle Activity + Timeout Reaping

### Summary
Implement prepared-statement idle timeout using the existing `transaction_timeout_seconds` setting, with per-handle last-activity tracking. Timeout cleanup will remove only non-transaction-linked prepared handles. Prepared handles linked to an active transaction will be skipped by prepared timeout cleanup, as requested.  

I validated current code shape first: transaction timeout sweeper already exists in `RunTransactionSweeper()`, prepared state currently has no activity timestamp, and SQL info currently reports transaction timeout only.

### Important Changes (Public/Interface + Internal)
1. SQL info behavior change:
- `FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT` will be published and set to the same timeout (ms) as `FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT`.
- Timeout source remains `transaction_timeout_seconds`.

2. Internal type changes:
- `PreparedStatementState` gains:
- `std::atomic<uint64_t> last_activity_ms`
- `void UpdateActivityTime()`
- This mirrors `TransactionState` activity tracking semantics.

3. Internal lifecycle behavior:
- `RunTransactionSweeper()` gains a prepared-handle timeout pass.
- Prepared timeout removal uses the same timeout value and same 1s sweeper loop.
- Handles with `transaction_owner` that are still linked to an active transaction are skipped.
- Handles not linked to active transactions are eligible for timeout removal.

### Implementation Plan
1. Add prepared activity tracking.
- File: `extension/flight/src/duckdb_flight_sql_server.cpp`
- Add `last_activity_ms` + `UpdateActivityTime()` in `PreparedStatementState`.
- Initialize activity on successful creation in `CreatePreparedStatement()`.

2. Touch prepared activity on all handle uses (chosen behavior).
- Update once per successful request path in:
- `GetFlightInfoPreparedStatement()`
- `GetSchemaPreparedStatement()`
- `DoPutPreparedStatementQuery()`
- `DoGetPreparedStatement()`
- `DoPutPreparedStatementUpdate()`
- Do not add touch on close path since close removes the handle.

3. Add prepared timeout cleanup in sweeper.
- File: `extension/flight/src/duckdb_flight_sql_server.cpp`
- Extend `RunTransactionSweeper()` with a prepared-snapshot pass:
- Read `transaction_timeout_seconds`; if `<= 0`, skip both tx and prepared timeout actions.
- Snapshot `prepared_statements` under `prepared_statements_mutex`.
- For each prepared entry:
- Check idle duration against timeout.
- Try-lock `PreparedStatementState::mutex`; if lock not available, skip this cycle.
- Recheck idle condition after lock.
- Determine “linked transaction state” as:
- `transaction_owner.has_value()` and that owner exists in `transactions` map.
- If linked, skip removal.
- If not linked and timed out, erase from `prepared_statements` map only if map still points to same state pointer (race-safe erase).
- Keep existing transaction timeout logic unchanged.

4. Update SQL info registration.
- File: `extension/flight/src/duckdb_flight_sql_server.cpp`
- In `UpdateTransactionSqlInfo()`, also register:
- `SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT`
- Value matches computed `timeout_millis` used for transaction timeout.

5. Keep shutdown semantics unchanged.
- Prepared handles continue to be removed by existing map lifecycle and transaction finalization behavior.
- No new SQL API/functions are introduced.

### Test Plan
1. Extend smoke client timeout coverage.
- File: `extension/flight/test/client/flight_sql_smoke_client.cpp`

2. Add non-transaction prepared timeout scenario.
- In prepared-related test flow:
- Set timeout to `1` second via existing SQL function.
- Create raw non-transaction prepared handle.
- Verify handle works immediately.
- Sleep long enough for sweeper (`~3500ms`).
- Verify `GetPreparedFlightInfoRaw` fails with `Prepared statement not found`.
- Reset timeout to default in cleanup.

3. Add “skip linked transaction” scenario.
- In transaction-related flow:
- Set timeout to `1` second.
- Begin transaction.
- Create tx-bound prepared handle using `transaction_id`.
- Keep transaction active beyond timeout by periodic in-transaction queries (`~600ms` interval, repeated).
- Do not touch prepared handle during this window.
- Verify tx-bound prepared handle is still valid (proves prepared timeout skipped because linked tx is active).
- End/rollback transaction.
- Verify handle then becomes invalid (`Prepared statement not found`) via existing tx-end invalidation behavior.

4. Keep existing tests intact.
- Existing shell tests continue to run:
- `tools/shell/tests/test_flight_sql.py`
- Existing lifecycle SQL test unchanged:
- `extension/flight/test/sql/flight_lifecycle.test`

### Validation Commands
1. Build:
- `BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release`

2. Integration tests:
- `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q`

3. Lifecycle SQL test:
- `build/release/test/unittest "/duckdb/extension/flight/test/sql/flight_lifecycle.test"`

### Assumptions and Defaults
1. Timeout source remains a single setting: `transaction_timeout_seconds`.
2. `0` means timeout disabled for both transaction and prepared timeout handling.
3. Sweeper cadence remains 1 second.
4. “Linked transaction state” means `transaction_owner` exists and owner transaction is still present in `transactions`.
5. Timeout expiration error for prepared handles remains `Prepared statement not found`.
6. No additional user-facing SQL functions or CLI flags are added.
