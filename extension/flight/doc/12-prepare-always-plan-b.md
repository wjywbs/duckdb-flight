## Flight SQL Statement Single-Execution Refactor (Prepared-Backed + Same-ID Fallback)

### Summary
Refactor `GetFlightInfoStatement`/`DoGetStatement` so statement queries are prepared once in `GetFlightInfoStatement`, executed from stored prepared state in `DoGetStatement`, and removed after stream completion.  
If `DoGetStatement` cannot find the prepared state, it will re-prepare using query text from the ticket, **insert with the same statement id decoded from the ticket**, then execute.

This plan explicitly applies your updates:
1. No backward compatibility for old ticket format.
2. Cleanup/removal can be done via `LockedFlightDataStream` `on_close`.
3. Fallback re-prepare inserts into `prepared_statements` with the decoded statement id.

---

### Important Internal Changes
1. **Statement ticket format is replaced (strict new format only).**
2. **Statement execution path becomes prepared-state-based** (no raw `StreamSQL(decoded.query)` path for new statement tickets).
3. **Prepared state for statement queries is removed after stream finalization**, not before execution.
4. **Fallback path reuses ticket statement id** when re-inserting prepared state.

No user-facing SQL/CLI/API changes.

---

### Test-First Red/Green Sequence

#### Step A: Add failing test first (red)
File:
- `extension/flight/test/client/flight_sql_smoke_client.cpp`

Change in `RunCrud` (at beginning):
- `DROP SEQUENCE IF EXISTS flight_stmt_seq`
- `CREATE SEQUENCE flight_stmt_seq START 1`
- execute `SELECT nextval('flight_stmt_seq')::BIGINT AS v`
- assert first returned value is `1`
- cleanup sequence

Expected before fix:
- Fails (returns `2`) because statement currently executes once in `GetFlightInfoStatement` and again in `DoGetStatement`.

Run:
- `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q -k crud`
- Confirm failure.

#### Step B: Implement refactor (green)
Re-run same test and full suite; must pass.

---

### Implementation Plan

#### 1) Replace statement ticket encoding/decoding (no legacy compatibility)
File:
- `extension/flight/src/duckdb_flight_sql_server.cpp`

Define strict format:
- byte `version` (e.g. `0x01`)
- `statement_id` (8-byte LE)
- `has_transaction_id` (1 byte: `0|1`)
- optional `transaction_id` (8-byte LE if present)
- query bytes (remaining payload)

Add:
- `EncodeStatementTicket(statement_id, query, optional tx_id)`
- `DecodeStatementTicket(payload)` -> `{statement_id, query, optional tx_id}`

Remove old query-only decode branches.

#### 2) Add helper to create/store statement prepared state with optional forced id
Files:
- `extension/flight/src/duckdb_flight_sql_server.cpp`
- `extension/flight/src/include/duckdb_flight_sql_server.hpp` (if signatures needed)

Helper behavior:
- Inputs: `query`, optional `tx_id`, optional `forced_statement_id`.
- Prepare on:
  - non-tx: new `Connection`
  - tx: existing tx connection under tx lock
- Reject parameterized statements (`ordered_parameters` non-empty) with deterministic `Invalid`.
- Build dataset schema from prepared metadata.
- Store in `prepared_statements` map:
  - if forced id present, insert with that id.
  - else allocate new id from counter.
- tx-owned handles added to `transaction_state->owned_prepared_handles`.
- update activity times.
- return `{statement_id, state, dataset_schema}`.

#### 3) Refactor `GetFlightInfoStatement`
File:
- `extension/flight/src/duckdb_flight_sql_server.cpp`

Changes:
- Stop executing query (`SendQuery` / `QueryInTransaction`).
- Call helper from step 2 to prepare/store.
- Build `FlightInfo` from `dataset_schema`.
- Build ticket with new strict format including `statement_id`, query, tx id.

#### 4) Refactor `DoGetStatement` to prepared execution + same-id fallback
File:
- `extension/flight/src/duckdb_flight_sql_server.cpp`

Flow:
1. Decode ticket.
2. Try lookup `prepared_statements[statement_id]`.
3. If found: execute prepared.
4. If missing: fallback:
   - re-prepare from decoded query + tx context
   - insert into map using **same decoded statement_id**
   - execute that prepared state.
5. Return stream.

Concurrency rule for fallback insert:
- Under `prepared_statements_mutex`, if same id already inserted by a racing request, discard newly-created state and use existing mapped state.

#### 5) Remove statement prepared state after stream completion
File:
- `extension/flight/src/duckdb_flight_sql_server.cpp`

Approach:
- Extend `LockedFlightDataStream` with optional `on_close` callback (statement cleanup only).
- For statement DoGet streams, pass callback:
  - remove `statement_id` from `prepared_statements`
  - best-effort remove `statement_id` from tx `owned_prepared_handles` if tx still exists
- Callback executes once in `Finalize()`/`Close()` path.
- No transaction activity timestamp updates on stream close.

#### 6) Statement execution helper reuse (optional but recommended)
File:
- `extension/flight/src/duckdb_flight_sql_server.cpp`

Create helper for executing non-parameter prepared query and wrapping stream with locks/cleanup callback to avoid duplicated lock/error handling in `DoGetStatement` and `DoGetPreparedStatement`.

---

### Edge Cases / Failure Modes
1. Invalid ticket bytes/version -> `Invalid statement ticket encoding`.
2. Parameterized statement via statement-query path -> `Invalid` (non-parameter contract).
3. Tx id present but tx missing -> `Transaction not found`.
4. Fallback race on same statement id -> deterministic winner; other request uses existing mapped state.
5. Stream never fully consumed -> cleanup still runs via stream close/destructor callback.
6. Tx ends before stream close -> callback removal from tx-owned set is best-effort and non-fatal.

---

### Validation
1. Build:
- `BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release`

2. Red test (before fix):
- `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q -k crud`
- Must fail with sequence first value mismatch.

3. Green after fix:
- `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q`
- `build/release/test/unittest "/duckdb/extension/flight/test/sql/flight_lifecycle.test"`

---

### Assumptions and Defaults
1. Only new ticket format is supported after this change.
2. Statement fallback is always allowed if query exists in ticket.
3. Statement handles are effectively one-shot per DoGet stream, with cleanup at stream finalization.
4. No server-side interrupt/cancel behavior changes in this scope.
