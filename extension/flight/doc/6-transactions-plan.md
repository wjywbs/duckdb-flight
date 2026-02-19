## DuckDB Flight SQL Transactions (Timeout + Shutdown Cleanup + Prepared-in-Tx)

### Summary
Implement Flight SQL transaction support in `extension/flight` with these fixed decisions:
- Support `BeginTransaction` and `EndTransaction`; savepoints remain `NotImplemented` in v1.
- Default idle transaction timeout is 30 minutes (`1800s`), configurable at runtime through SQL setter/getter functions.
- Client disconnect cleanup in v1 is timeout-based (no Flight session middleware in this iteration).
- Active transactions are rolled back on server shutdown.
- Prepared statements created with a transaction are transaction-bound and invalidated when that transaction ends (commit, rollback, timeout, shutdown).

### Public API / Interface Changes
1. Flight SQL protocol behavior:
- Implement `BeginTransaction` and `EndTransaction`.
- Keep `BeginSavepoint` and `EndSavepoint` returning `NotImplemented`.
- `CreatePreparedStatement` honors `request.transaction_id` and binds statement lifetime to that transaction.
- Transaction IDs are opaque 8-byte little-endian handles.
2. SQL functions in flight extension:
- `CALL set_flight_sql_transaction_timeout_seconds(<bigint_seconds>);`
- `SELECT * FROM get_flight_sql_transaction_timeout_seconds();`
3. SQL info updates:
- `FLIGHT_SQL_SERVER_TRANSACTION = SQL_SUPPORTED_TRANSACTION_TRANSACTION`
- `FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT = <configured_ms>`

### Implementation Plan (small commits)
1. Add transaction state scaffolding in `extension/flight/src/include/duckdb_flight_sql_server.hpp` and `extension/flight/src/duckdb_flight_sql_server.cpp`.
- Add `TransactionState` map keyed by `uint64_t` handle.
- State stores one `Connection`, lock, last-activity timestamp, and owned prepared-handle IDs.
- Add 8-byte LE encode/decode helpers for transaction handles.
- Add lookup/remove helpers with deterministic errors (`Invalid transaction handle encoding`, `Transaction not found`).

2. Implement transaction RPCs in `DuckDBFlightSqlServer`.
- `BeginTransaction`: create connection, execute `BEGIN TRANSACTION`, register handle, return opaque bytes.
- `EndTransaction`: remove handle, serialize with in-flight ops, commit/rollback, invalidate tx-owned prepared handles.
- `BeginSavepoint`/`EndSavepoint`: explicit `NotImplemented`.

3. Route statement execution through transaction context.
- Update `GetFlightInfoStatement`, `DoGetStatement`, and `DoPutCommandStatementUpdate` to honor `command.transaction_id`.
- Encode/decode both SQL and transaction ID into statement ticket payload so `DoGetStatement` executes against the correct transaction connection.
- Keep streaming with `SendQuery()`.
- For transaction-scoped streams, hold transaction lock for stream lifetime via lock-holding stream wrapper to prevent concurrent use of the same connection while a stream is active.

4. Integrate prepared statements with transactions.
- Extend `PreparedStatementState` with optional transaction owner handle.
- `CreatePreparedStatement` with transaction ID prepares on that transaction connection and records ownership.
- `DoGetPreparedStatement` and `DoPutPreparedStatementUpdate` for tx-bound handles execute under transaction lock.
- On tx end/timeout/shutdown, erase all tx-owned prepared handles so reuse fails with deterministic `Prepared statement not found`.

5. Add timeout sweeper and shutdown rollback semantics.
- Add background sweeper thread in server class to rollback idle transactions past configured timeout.
- Timeout policy: if idle duration exceeds threshold and transaction is lock-acquirable, rollback and remove it.
- Add server cleanup routine invoked during destruction to rollback all remaining transactions and clear tx-bound prepared handles.
- Ensure `FlightService::Stop()` path triggers server cleanup deterministically.

6. Add timeout configuration SQL functions in `extension/flight/src/flight_extension.cpp` and `extension/flight/src/flight_service.cpp`.
- Store timeout in `FlightService` with default `1800s`.
- Setter validates `>= 0`; `0` means no timeout.
- Getter returns current seconds.
- If server is running, applying setter updates live server timeout atomically.

7. Add end-to-end transaction integration mode to smoke client in `extension/flight/test/client/flight_sql_smoke_client.cpp`.
- New mode: `--mode transaction`.
- Validate rollback flow: begin tx, DML, verify visible in tx, rollback, verify outside tx unchanged.
- Validate commit flow: begin tx, DML, commit, verify outside tx changed.
- Validate prepared-in-transaction: create prepared with tx handle, execute query/update, verify, end tx, assert prepared handle invalidated.
- Validate timeout flow: set timeout to short value via SQL function, open tx, perform DML, wait past timeout, assert commit fails and changes are rolled back; reset timeout after test.

8. Add pytest coverage in `tools/shell/tests/test_flight_sql.py`.
- Add `test_flight_sql_transaction_roundtrip(shell)` using daemon mode + smoke client `transaction`.
- Reuse existing readiness polling and SIGINT shutdown assertions.
- Keep existing daemon/crud/metadata/prepared tests unchanged.

### Test Cases and Scenarios
1. Transaction lifecycle:
- begin -> rollback invalidates handle and discards changes.
- begin -> commit invalidates handle and persists changes.
- repeated commit/rollback on ended handle returns deterministic error.
2. Prepared in transaction:
- prepared execute works inside active tx.
- prepared handle becomes unusable after tx commit/rollback/timeout.
3. Timeout cleanup:
- idle tx auto-rolled back after configured timeout.
- timeout value `0` disables auto-timeout.
4. Concurrency safety:
- operations against same tx are serialized.
- streaming result keeps tx connection valid until stream closes.
5. Shutdown:
- all active tx rolled back during server teardown path.
6. Backward compatibility:
- existing non-transaction statement, metadata, CRUD, and prepared tests still pass.

### Assumptions and Defaults
1. Disconnect cleanup in v1 is timeout-based only; no Flight session middleware wiring in this iteration.
2. Savepoints are intentionally out of scope and return `NotImplemented`.
3. Transaction IDs and prepared IDs remain opaque binary handles.
4. Prepared statements created without transaction ID keep current non-transaction behavior.
5. Default transaction timeout is `1800s`; `0` disables timeout.
