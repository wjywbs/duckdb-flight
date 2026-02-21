## Flight SQL Statement Ticket Refactor: Prepared-Backed Execution + Query Fallback + Test-First Red/Green

### Summary
Refactor statement-query handling so `GetFlightInfoStatement` prepares/stores a server-side statement, and `DoGetStatement` executes that stored prepared statement.  
If the stored prepared statement is missing, `DoGetStatement` will recreate a prepared statement from the query encoded in the ticket, add it to `prepared_statements`, execute it, and remove it after streaming finishes.

This plan follows your constraints exactly:
1. Remove statement prepared state **after execution/streaming ends** (not before execution).
2. Encode query in statement ticket so `DoGetStatement` can recover when handle is missing.
3. Add “statement executes once” test first, confirm it fails before fix, then implement fix.

---

### Important Changes / Internal Interface Updates

1. **Statement ticket payload format change**
- Current ticket: query (+ optional tx id).
- New ticket: prepared handle id + query (+ optional tx id), with explicit version/discriminator byte.

2. **Statement execution lifecycle change**
- `GetFlightInfoStatement`:
  - prepare and store statement state in `prepared_statements`.
  - no query execution.
- `DoGetStatement`:
  - execute prepared statement from map if present.
  - fallback: recreate prepared from query in ticket if missing.
  - remove handle from map after stream finalization.

3. **No user-facing SQL/CLI changes.**

---

### Step-by-Step Plan

#### 1) Add failing test first (“statement executes once”)
Files:
- `extension/flight/test/client/flight_sql_smoke_client.cpp`
- (existing pytest driver already calls `crud` mode) `tools/shell/tests/test_flight_sql.py` (no new test required if embedded in `RunCrud`)

Change:
- In `RunCrud`, add a sequence-based assertion before table CRUD:
  - `DROP SEQUENCE IF EXISTS flight_stmt_seq`
  - `CREATE SEQUENCE flight_stmt_seq START 1`
  - `ExecuteQuery("SELECT nextval('flight_stmt_seq')::BIGINT AS v")` and assert returned value is `1`.
  - optional second call assert `2`.
  - cleanup sequence.

Expected pre-fix result:
- Fails (typically first value `2`) because statement currently executes in both `GetFlightInfoStatement` and `DoGetStatement`.

Validation (red phase):
- Run targeted pytest for CRUD roundtrip and confirm failure.

#### 2) Introduce new statement ticket encode/decode format
File:
- `extension/flight/src/duckdb_flight_sql_server.cpp`

Changes:
- Add new ticket struct:
  - `prepared_handle_id` (optional for backward compatibility)
  - `query`
  - `transaction_id` (optional)
- Add encode/decode helpers for new format:
  - version byte for new format (e.g., `0x02`)
  - `uint64_t` prepared handle id (LE)
  - tx presence flag + optional tx id
  - query bytes
- Keep backward decode for existing 0/1 legacy tickets (query-only forms) so old tickets in-flight still decode.

#### 3) Create helper to prepare/store statement state (non-param only)
Files:
- `extension/flight/src/duckdb_flight_sql_server.cpp`
- `extension/flight/src/include/duckdb_flight_sql_server.hpp` (if declarations needed)

Add internal helper (reused by both GetFlightInfo and fallback path):
- Input: `query`, optional `transaction_id`.
- Behavior:
  - Non-tx: create `Connection`, `Prepare(query)`.
  - Tx: lookup tx, lock tx mutex, `transaction_state->connection->Prepare(query)`.
  - Build ordered parameters + dataset schema.
  - Reject if parameter count > 0 with deterministic `Invalid` (statement query path is non-param).
  - Generate handle id, insert into `prepared_statements`.
  - If tx-owned, add handle to `transaction_state->owned_prepared_handles`.
  - Update activity timestamps.
- Output: handle id + state/schema.

#### 4) Refactor `GetFlightInfoStatement` to store prepared state, no execution
File:
- `extension/flight/src/duckdb_flight_sql_server.cpp`

Changes:
- Remove `SendQuery` / `QueryInTransaction(..., streaming=true)` from `GetFlightInfoStatement`.
- Call helper from step 3 to create/store prepared state.
- Build `FlightInfo` from stored dataset schema.
- Encode ticket with:
  - prepared handle id
  - original query
  - optional tx id

This removes the first execution.

#### 5) Refactor `DoGetStatement` to execute prepared, with fallback re-prepare
File:
- `extension/flight/src/duckdb_flight_sql_server.cpp`

Flow:
1. Decode ticket.
2. If new-format ticket has prepared handle:
  - try lookup in `prepared_statements`.
  - if found: execute prepared.
  - if not found: fallback path:
    - recreate prepared via helper from query+tx id,
    - store in map,
    - execute recreated prepared.
3. If legacy ticket without handle:
  - execute query path as legacy behavior.
4. Return stream.

Execution details:
- Tx-owned statement:
  - lock tx mutex through stream lifetime (existing lock-holding stream wrapper pattern).
- Non-tx statement:
  - execute on prepared’s own connection/state lock.

#### 6) Remove statement handle after stream finalization (not before execution)
File:
- `extension/flight/src/duckdb_flight_sql_server.cpp`

Add a stream-finalizer wrapper (new class) used by statement DoGet path:
- Wraps inner stream.
- On `Close()`/destructor finalization:
  - remove handle from `prepared_statements`.
  - best-effort remove handle from tx `owned_prepared_handles` if tx still exists.
- Guarantee callback runs once.

Notes:
- Do not reintroduce generic `on_close` in existing `LockedFlightDataStream` if avoidable; use a dedicated wrapper for statement cleanup path.
- This satisfies “remove after execution/streaming ends”.

#### 7) Keep deterministic error behavior
- Bad ticket encoding: `Invalid statement ticket encoding`.
- Tx id present but tx missing: `Transaction not found`.
- Statement query requiring params: `Invalid` with clear message.
- Re-prepare fallback failure returns underlying `Invalid` message from `Prepare`.

---

### Test Cases and Scenarios

1. **Red test first**
- New sequence assertion in `RunCrud` fails before server fix (proves current double execution).

2. **Green after fix**
- Sequence assertion passes:
  - first `nextval` call returns `1`.
  - optional second returns `2`.

3. **Fallback behavior**
- Force statement handle miss (e.g., via timeout window / map removal path) and verify DoGet still succeeds by re-preparing from ticket query.

4. **Transaction path**
- Statement query in tx still works with tx lock semantics; no regression in existing transaction smoke flow.

5. **No regressions**
- Existing `metadata`, `prepared`, `transaction`, `timeout`, lifecycle tests still pass.

---

### Validation Sequence

1. **Red phase**
- Add test only (sequence assertion in `RunCrud`), run:
  - `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q -k crud`
- Confirm fail due to double execution (document observed mismatch).

2. **Implement fix**
- Apply steps 2–6.

3. **Green phase**
- Re-run:
  - `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q`
  - `build/release/test/unittest "/duckdb/extension/flight/test/sql/flight_lifecycle.test"`

4. **Build**
- `BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release`

---

### Assumptions and Defaults

1. Statement fallback is enabled whenever prepared handle is missing and query is present in ticket.
2. Fallback re-prepare path also inserts into `prepared_statements` and uses same post-stream cleanup semantics.
3. Statement handles are single-use in normal flow (removed at stream end), but ticket-encoded query enables recovery/re-execution when handle is missing.
4. No server-side interrupt/cancel changes are included.
