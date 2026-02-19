## Flight SQL Transaction/Prepared Refactor Plan (7 Requested Changes)

### Summary
Refactor `extension/flight` transaction/prepared internals to simplify lock/lifecycle behavior, remove unnecessary transaction lookups in prepared metadata/bind paths, unify prepared update execution logic, and move shutdown cleanup out of `DuckDBFlightSqlServer` destructor into an explicit shutdown cleanup hook.  
Per your clarification, I will **keep** `FlightService` timeout state and document why in the header.

### Important Interface/Behavior Changes
1. No user-facing SQL/CLI additions.
2. Internal lifecycle change: transaction sweeper stop + rollback-all moves from `~DuckDBFlightSqlServer()` into an explicit server cleanup method called by `FlightService::Stop()`.
3. Internal stream wrapper change: `LockedFlightDataStream` no longer takes/executes `on_close`; transaction activity is no longer bumped when stream closes.
4. Internal activity update API change: `TouchTransaction(...)` replaced by `TransactionState::UpdateActivityTime()` and used once per function path.
5. Internal prepared update path change: single merged execution logic for tx-bound and non-tx prepared updates.
6. Per your decision: `FlightService` keeps timeout state, with comments explaining pre-start persistence and start-time propagation.

### Implementation Plan

1. **Timeout ownership comment + server getter usage (keep service state)**
- Files: `extension/flight/src/include/flight_service.hpp`, `extension/flight/src/flight_service.cpp`.
- Keep `transaction_timeout_seconds` atomic in `FlightService`.
- Add header comments explaining:
- it stores desired timeout before server start,
- it is applied to new server instances in `Start()`,
- it remains fallback state when server is stopped.
- Update `GetTransactionTimeoutSeconds()` to return `server->GetTransactionTimeoutSeconds()` when running, otherwise fallback to service atomic.
- Keep `SetTransactionTimeoutSeconds()` behavior of persisting + live-applying to running server.

2. **Remove `on_close` and remove `ResultToLockedFlightStream()`**
- File: `extension/flight/src/duckdb_flight_sql_server.cpp`, header declarations in `extension/flight/src/include/duckdb_flight_sql_server.hpp`.
- Simplify `LockedFlightDataStream`:
- remove `std::function<void()> on_close`,
- remove callback invocation in `Finalize()`.
- Delete `ResultToLockedFlightStream(...)`.
- Replace call sites (`StreamSQLInTransaction`, tx `DoGetPreparedStatement`) with:
- `ResultToFlightStream(...)`,
- wrap returned stream directly in `LockedFlightDataStream(lock + state_guard)`.
- Ensure no transaction activity updates are tied to stream completion.

3. **Move cleanup out of destructor into explicit shutdown hook**
- Files: `extension/flight/src/include/duckdb_flight_sql_server.hpp`, `extension/flight/src/duckdb_flight_sql_server.cpp`, `extension/flight/src/flight_service.cpp`.
- Remove destructor cleanup calls (`StopTransactionSweeper`, `RollbackAllTransactions`) from `~DuckDBFlightSqlServer()`.
- Add explicit idempotent cleanup method on server, e.g. `Status ShutdownFlightSqlState();`.
- Use an atomic guard so cleanup runs once.
- In `FlightService::Stop()`:
- call `server->Shutdown()` first (stop serving),
- then call `server->ShutdownFlightSqlState()` (stop sweeper + rollback active tx),
- then join serve thread.
- On partial failure, still attempt cleanup/join best-effort before propagating failure.

4. **Replace `TouchTransaction` with `TransactionState::UpdateActivityTime()`**
- Files: `extension/flight/src/include/duckdb_flight_sql_server.hpp`, `extension/flight/src/duckdb_flight_sql_server.cpp`.
- Add method on `TransactionState`:
- `void UpdateActivityTime();`
- Remove `DuckDBFlightSqlServer::TouchTransaction(...)`.
- Replace all direct `last_activity_ms.store(...)` writes and `TouchTransaction(...)` calls with `transaction_state->UpdateActivityTime()`.
- Ensure one activity update per function path:
- `CreatePreparedStatement` tx path updates once after successful create/register,
- query/update tx helpers update once per request execution path,
- no extra update on stream close.

5. **Inline/remove temporary owned-prepared vector plumbing**
- Files: `extension/flight/src/include/duckdb_flight_sql_server.hpp`, `extension/flight/src/duckdb_flight_sql_server.cpp`.
- Change `FinalizeTransaction(...)` signature to stop returning owned handles via out-param.
- Call `RemovePreparedStatements(...)` directly from `FinalizeTransaction(...)`.
- Remove caller-side `owned_prepared_handles` variables from:
- `EndTransaction`,
- sweeper timeout path,
- rollback-all path.
- Update `RemovePreparedStatements(...)` to accept the native container used in tx state (`unordered_set<uint64_t>`), or add an overload.

6. **Remove transaction lookup from prepared metadata/bind-only methods**
- File: `extension/flight/src/duckdb_flight_sql_server.cpp`.
- In `GetFlightInfoPreparedStatement(...)`:
- stop looking up owner transaction,
- return schema info based only on prepared handle state.
- In `GetSchemaPreparedStatement(...)`:
- same change; no tx existence check.
- In `DoPutPreparedStatementQuery(...)`:
- remove tx lookup/touch after parameter binding update.
- Keep tx lookups where execution requires active tx connection (`DoGetPreparedStatement`, `DoPutPreparedStatementUpdate`, tx statement execution paths).

7. **Merge tx/non-tx logic in `DoPutPreparedStatementUpdate()`**
- File: `extension/flight/src/duckdb_flight_sql_server.cpp`.
- Refactor into one shared execution flow:
- lookup prepared state and copy parameter definitions,
- read bound rows once,
- acquire execution lock/context:
- tx-bound: lookup transaction and lock tx mutex,
- non-tx: lock prepared-state mutex.
- run a single shared execute loop:
- zero-parameter statement executes once,
- parameterized statement executes once per bound row,
- enforce “at least one row” for parameterized updates.
- accumulate changed-row count via existing `ExtractChangedRows`.
- if tx-bound, call `UpdateActivityTime()` once at end.

8. **Build/test validation**
- Run:
- `BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release`
- `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q`
- `build/release/test/unittest "/duckdb/extension/flight/test/sql/flight_lifecycle.test"`
- If behavior around timeout getter/setter when stopped changes unintentionally, add targeted assertion in `tools/shell/tests/test_flight_sql.py` to lock expected semantics.

### Test Cases and Scenarios
1. Existing integration suite still passes: daemon lifecycle, CRUD, metadata, prepared, transaction.
2. Transaction timeout behavior still correct in `transaction` smoke mode after stream/on-close removal.
3. Prepared metadata/bind methods work without tx lookup while execute methods still correctly fail after tx end (via handle removal / missing tx).
4. Server stop path cleanly exits without destructor-driven rollback/sweeper calls.
5. Same-handle prepared update behavior remains correct with merged tx/non-tx code path.

### Assumptions and Defaults
1. Keep `FlightService` timeout atomic state (per your direction), with explicit comments in header.
2. Arrow `FlightServerBase::Shutdown` is non-virtual; implement explicit cleanup method instead of override.
3. Transaction activity updates are request-path based; stream-close no longer updates activity.
4. Prepared statement invalidation on tx end remains driven by prepared-handle removal, not metadata/bind-time tx lookup.
