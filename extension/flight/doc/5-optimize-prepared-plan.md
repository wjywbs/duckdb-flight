### Flight SQL Prepared-State Finalization (RWLock + Binary Handles + Cached Prepared Serialization)

### Summary
Implement the prepared-state refactor with these locked decisions:
1. Remove `query` from `PreparedStatementState`.
2. Keep per-state locking as `std::shared_mutex` (`rwlock` semantics).
3. Use 8-byte little-endian `uint64_t` protocol handles (opaque `bytes`) and `uint64_t` map keys.
4. Cache one `Connection` + one `PreparedStatement` per handle.
5. Serialize execution per handle for correctness.

Key finding locked into design: constructing a new `PreparedStatement` with a new context is not a safe clone mechanism. The constructor stores shared pointers; it does not deep-copy prepared internals (`src/main/prepared_statement.cpp:8`, `src/include/duckdb/main/prepared_statement.hpp:26`). `PreparedStatementData` is mutable during binding (`src/main/prepared_statement_data.cpp:81`).

### Important Interface Changes
1. No user-facing SQL/CLI changes.
2. Flight SQL prepared handle payload changes to binary 8-byte LE (still opaque to clients).
3. Internal map/type changes:
- `prepared_statements`: `unordered_map<uint64_t, shared_ptr<PreparedStatementState>>`
- `PreparedStatementState` no longer stores `query`
- `PreparedStatementState` stores cached `unique_ptr<Connection>` and `unique_ptr<PreparedStatement>`

### Implementation Plan

1. **State Model Update**
- File: `extension/flight/src/include/duckdb_flight_sql_server.hpp`
- Change `PreparedStatementState` fields to:
  - `ordered_parameters`
  - `dataset_schema`
  - `query_bound_parameters`
  - `unique_ptr<Connection> connection`
  - `unique_ptr<PreparedStatement> prepared`
  - `std::shared_mutex mutex`
- Remove `query` field.
- Keep map mutex as `std::mutex` (as previously decided).

2. **Binary Handle Encoding**
- File: `extension/flight/src/duckdb_flight_sql_server.cpp`
- Add helpers:
  - `EncodePreparedHandle(uint64_t) -> std::string` (8 bytes LE)
  - `DecodePreparedHandle(const std::string&) -> Result<uint64_t>` (must be length 8)
- `GeneratePreparedHandle()` returns encoded bytes from atomic counter.
- `LookupPreparedStatement` and `ClosePreparedStatement` decode then lookup/erase by `uint64_t`.
- Errors:
  - malformed bytes => `Status::Invalid("Invalid prepared statement handle encoding")`
  - missing id => `Status::Invalid("Prepared statement not found")`

3. **CreatePreparedStatement Caching**
- In `CreatePreparedStatement`:
  - build `Connection` once
  - call `Prepare(request.query)` once
  - derive parameter order/types and schemas from cached prepared
  - store prepared + connection in state
- Return encoded binary handle in `ActionCreatePreparedStatementResult`.

4. **Per-Handle Serialized Execution**
- Use per-state unique lock for execute/update paths.
- Add `LockedFlightDataStream` wrapper that holds:
  - inner `unique_ptr<FlightDataStream>`
  - `unique_lock<shared_mutex>` for the stream lifetime
- `DoGetPreparedStatement`:
  - unique-lock state
  - validate query binding presence
  - execute `state->prepared->Execute(..., true)`
  - convert to stream
  - return wrapped stream so lock survives until stream closes
- `DoPutPreparedStatementUpdate`:
  - unique-lock state
  - execute update(s) using cached prepared, accumulate changed rows
- `DoPutPreparedStatementQuery`:
  - parse rows first
  - unique-lock state only for writing bound query parameters

5. **Read-Only Paths with Shared Lock**
- `GetFlightInfoPreparedStatement`, `GetSchemaPreparedStatement`: shared lock.
- Avoid copying or storing SQL text separately; use cached prepared metadata.

6. **No Constructor-Based Clone Path**
- Do not attempt “clone to new connection” via `PreparedStatement(...)` constructor.
- If future concurrency beyond per-handle serialization is needed, use per-handle session pool with independently prepared statements (explicit future scope).

### Test Cases and Scenarios

1. Existing tests must continue passing:
- `tools/shell/tests/test_flight_sql.py`
- `extension/flight/test/sql/flight_lifecycle.test`

2. Prepared roundtrip remains valid:
- create/bind/execute query and update/close.

3. New/extended prepared tests:
- same handle reused across repeated executes (query and update), validate correctness.
- malformed handle bytes path returns deterministic `Invalid`.
- close+reuse handle fails with `Prepared statement not found`.

4. Serialization correctness test:
- run two concurrent operations against the same prepared handle.
- assert no stream invalidation/crash and deterministic completion (serialized behavior).

### Assumptions and Defaults
1. Prepared state is cached per handle and not shared across handles.
2. Same-handle concurrency is intentionally serialized for correctness.
3. Cross-handle concurrency remains available.
4. Binary handles are opaque protocol bytes; no human-readable contract.
5. No prepared Substrait changes in this iteration.
