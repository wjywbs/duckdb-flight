## Flight SQL Streaming + Metadata Integration Coverage

### Summary
Update the Flight SQL server to use `Connection::SendQuery()` for query/metadata streaming paths, and add end-to-end integration coverage for `DoGetTables()`, `DoGetDbSchemas()`, and `DoGetTableTypes()` via the existing Flight smoke-client + shell daemon pytest flow.

### Current State (Grounded)
- `extension/flight/src/duckdb_flight_sql_server.cpp` currently uses `Connection::Query()` in:
  - `StreamSQL(...)`
  - `GetFlightInfoStatement(...)`
  - `DoPutCommandStatementUpdate(...)`
- Metadata RPC methods are implemented:
  - `DoGetDbSchemas(...)`
  - `DoGetTables(...)`
  - `DoGetTableTypes(...)`
- Integration tests currently cover daemon lifecycle + CRUD only:
  - `tools/shell/tests/test_flight_sql.py`
  - `extension/flight/test/client/flight_sql_smoke_client.cpp` supports `ping` and `crud` modes.

## Implementation Plan

### 1. Switch query streaming paths to `SendQuery()`
**File:** `extension/flight/src/duckdb_flight_sql_server.cpp`

1. In `StreamSQL(...)`:
- Replace `conn.Query(sql)` with `conn.SendQuery(sql)`.
- Keep existing `ResultArrowArrayStreamWrapper` + Arrow C Data import path unchanged (it already accepts `unique_ptr<QueryResult>`).

2. In `GetFlightInfoStatement(...)`:
- Replace `conn.Query(command.query)` with `conn.SendQuery(command.query)`.
- Build Arrow schema from `result->types`, `result->names`, `result->client_properties` (same conversion logic).
- Do not materialize rows; only infer schema and issue ticket.

3. Keep `DoPutCommandStatementUpdate(...)` on `Query()`:
- This path needs changed-row semantics and is not a streaming use case.
- No behavior change for DML row-count reporting.

4. Preserve existing error semantics:
- If `!result` or `result->HasError()`, return `Status::Invalid(...)` as today.

### 2. Extend smoke client with metadata mode
**File:** `extension/flight/test/client/flight_sql_smoke_client.cpp`

1. Add CLI mode:
- Extend mode parser to support `--mode metadata`.

2. Add metadata execution function (`RunMetadata(...)`) using `FlightSqlClient` APIs:
- Create controlled objects:
  - `CREATE TABLE flight_meta_tbl (id INTEGER, val VARCHAR)`
  - `CREATE VIEW flight_meta_view AS SELECT * FROM flight_meta_tbl`
- Validate `GetDbSchemas`:
  - Call `GetDbSchemas(..., nullptr, &pattern)` with pattern `"main"`.
  - `DoGet` endpoint and assert at least one row where `db_schema_name == "main"`.
- Validate `GetTableTypes`:
  - Call `GetTableTypes(...)`, `DoGet`, assert returned set contains `"TABLE"` and `"VIEW"`.
- Validate `GetTables`:
  - Call `GetTables(..., nullptr, &schema_pattern, &table_pattern, false, nullptr)` with:
    - `schema_pattern = "main"`
    - `table_pattern = "flight_meta_%"`
  - Assert rows include:
    - `flight_meta_tbl` with `table_type == "TABLE"`
    - `flight_meta_view` with `table_type == "VIEW"`
  - Add second filtered call with `table_types = {"TABLE"}` and assert only `flight_meta_tbl` appears.
- Cleanup:
  - `DROP VIEW flight_meta_view`
  - `DROP TABLE flight_meta_tbl`

3. Add robust table-decoding helpers (reused with existing style):
- Reuse existing `DoGet -> ToTable -> CombineChunks`.
- Parse string columns defensively and emit clear stderr on assertion failures.

### 3. Add metadata integration pytest
**File:** `tools/shell/tests/test_flight_sql.py`

1. Add test:
- `test_flight_sql_metadata_roundtrip(shell)`

2. Flow:
- Pick free port via existing helper.
- Start `duckdb --batch --init /dev/null -flight-sql <port> :memory:`.
- Wait until ready with existing `wait_for_server_ready(...)`.
- Run smoke client with `--mode metadata`.
- Assert return code is zero; include stdout/stderr in failure message.
- Send SIGINT; assert clean daemon exit and no shell prompt/error text (same shutdown assertions as other tests).

3. Keep all existing tests unchanged:
- daemon mode prompt behavior
- default port
- invalid port
- CRUD roundtrip

## Important Interface Changes
1. **Internal server behavior change**
- Query/metadata streaming paths now use `SendQuery()` instead of `Query()`:
  - `StreamSQL`
  - `GetFlightInfoStatement`

2. **Internal test client interface change**
- `flight_sql_smoke_client` gains a new mode:
  - `--mode metadata`

3. **No user-facing DuckDB API/SQL/CLI changes**
- No changes to extension SQL functions or `-flight-sql` CLI contract.

## Test Cases and Scenarios

### New coverage
1. `DoGetDbSchemas()` integration:
- Schema filter pattern (`"main"`) returns expected schema row(s).

2. `DoGetTableTypes()` integration:
- Returned types contain both `"TABLE"` and `"VIEW"`.

3. `DoGetTables()` integration:
- Pattern-filtered lookup returns created table/view with correct table_type values.
- Table-type-filtered lookup returns only table entries.

### Existing coverage retained
1. Daemon startup/shutdown (`-flight-sql`).
2. Default/explicit port behavior.
3. Invalid port handling.
4. End-to-end CRUD query roundtrip.

## Validation Commands
1. Build:
- `BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release`
2. Shell integration tests:
- `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q`
3. Existing extension SQL lifecycle test:
- `build/release/test/unittest "/duckdb/extension/flight/test/sql/flight_lifecycle.test"`

## Assumptions and Defaults
1. `SendQuery()` is applied only where streaming/schema inference matters (`StreamSQL`, `GetFlightInfoStatement`).
2. `DoPutCommandStatementUpdate` remains on `Query()` for deterministic row-count behavior.
3. Metadata tests assert inclusion of expected values, not an exact full system catalog snapshot.
4. Tests continue using plaintext gRPC (`127.0.0.1` client to `-flight-sql` daemon).
