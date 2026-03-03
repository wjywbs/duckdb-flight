### Flight SQL `CancelFlightInfo` Without Global Registry (State-Local Active Flags)

## Summary
Implement `CancelFlightInfo` by tracking active execution directly in `PreparedStatementState` and `TransactionState`, and canceling through `Connection::Interrupt()` on the owning connection.  
No separate active-query registry/map will be added.

## Key Design Update
1. No global cancellation registry.
2. Track execution activity in existing state objects:
- `PreparedStatementState`: active execution for non-transaction prepared/statement handles.
- `TransactionState`: active execution for transaction-bound query streams.
3. `CancelFlightInfo` resolves handle from `FlightInfo` ticket, locates the corresponding state, checks active flag, and interrupts the right connection.

## Internal Changes

1. Add activity flags to state structs
- File: `extension/flight/src/duckdb_flight_sql_server.cpp` (state definitions) and header if needed.
- Add:
  - `std::atomic<bool> active_execution{false};`
  - optional `std::atomic<bool> cancel_requested{false};` (only if you want pre-execute cancellation behavior; otherwise omit).
- Helper methods:
  - `MarkExecutionStart()`, `MarkExecutionStop()`.

2. Add `CancelFlightInfo` override
- File: `extension/flight/src/include/duckdb_flight_sql_server.hpp`
  - add `CancelFlightInfo(...) override`.
- File: `extension/flight/src/duckdb_flight_sql_server.cpp`
  - decode `request.info->endpoints()[0].ticket.ticket`:
    - statement ticket -> decode custom statement handle -> `statement_id`
    - prepared query command -> decode prepared handle -> `prepared_id`
  - find state by ID in `prepared_statements`.
  - behavior:
    - if handle/state missing -> `CancelStatus::kNotCancellable`
    - if tx-owned:
      - lookup tx state; if missing -> `kNotCancellable`
      - if `transaction_state->active_execution == true` -> `transaction_state->connection->Interrupt()`, return `kCancelled`
      - else `kNotCancellable`
    - if non-tx:
      - if `prepared_state->active_execution == true` -> `prepared_state->connection->Interrupt()`, return `kCancelled`
      - else `kNotCancellable`
  - malformed payload/encoding -> `Status::Invalid(...)`.

3. Mark active execution in query paths
- File: `extension/flight/src/duckdb_flight_sql_server.cpp`
- `DoGetStatement` and `DoGetPreparedStatement`:
  - set active flag `true` immediately before `Execute(...)`.
  - wrap stream in `LockedFlightDataStream` `on_close` callback that sets active flag back to `false`.
  - on any pre-stream error, reset active flag before returning.
- For tx-bound execution, use `TransactionState::active_execution`.
- For non-tx execution, use `PreparedStatementState::active_execution`.
- Keep existing statement cleanup logic in `on_close` for one-shot statement handles.

4. Keep locking semantics unchanged
- Keep existing per-state / per-transaction stream locks and lifetime behavior.
- No additional lock graph introduced.

## Integration Tests

1. Extend smoke client with cancel mode
- File: `extension/flight/test/client/flight_sql_smoke_client.cpp`
- Add `--mode cancel`.
- Test flow:
  - run long query (`SELECT COUNT(*)::BIGINT FROM range(300000000) t(i) WHERE hash(i) % 2 = 0`) via `Execute`.
  - start `DoGet(...)->ToTable()` in worker thread.
  - call `CancelFlightInfo` on returned `FlightInfo` from main thread.
  - assert cancel status is `kCancelled` (accept `kCancelling` if observed, then retry once/twice).
  - assert worker ends with cancellation/interruption-like error.
  - run `SELECT 1` control query to verify server remains healthy.

2. Add pytest integration case
- File: `tools/shell/tests/test_flight_sql.py`
- Add `test_flight_sql_cancel_roundtrip(shell)`:
  - start daemon, wait ready, run smoke client `--mode cancel`, assert success, SIGINT shutdown checks same as existing tests.

3. Optional negative assertion in smoke test
- call `CancelFlightInfo` on an already-finished `FlightInfo` and assert `kNotCancellable`.

## Validation

1. Build:
- `BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release`

2. Shell integration:
- `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q`

3. Flight lifecycle SQL test:
- `build/release/test/unittest "/duckdb/extension/flight/test/sql/flight_lifecycle.test"`

## Assumptions and Defaults

1. Cancellation is best-effort and only for currently active `DoGet` executions.
2. If `CancelFlightInfo` arrives before execution starts, result is `kNotCancellable` (unless `cancel_requested` is explicitly added later).
3. Cancellation is done via `Connection::Interrupt()` only; no Arrow library changes and no server-side call-context cancellation hooks.
4. Metadata/non-query tickets remain non-cancellable.
