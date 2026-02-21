### Implement `include_schema=true` for Flight SQL `GetTables` + Integration Coverage

## Summary
Implement full `include_schema=true` support in DuckDB Flight SQL for both:
- `GetFlightInfoTables(...)`
- `DoGetTables(...)`

Current behavior returns `NotImplemented` for `include_schema=true`.  
Target behavior will match Arrow Flight SQL expectations:
- schema advertised as `SqlSchema::GetTablesSchemaWithIncludedSchema()`
- each result row includes non-null `table_schema` binary bytes containing IPC-serialized Arrow schema for that table/view.

---

## Important Interface / Behavior Changes
1. **Flight SQL behavior change (public protocol behavior):**
   - `GetFlightInfoTables(..., include_schema=true)` returns `FlightInfo` with `SqlSchema::GetTablesSchemaWithIncludedSchema()`.
   - `DoGetTables(..., include_schema=true)` returns rows with 5 columns:
     - `catalog_name`
     - `db_schema_name`
     - `table_name`
     - `table_type`
     - `table_schema` (`binary`, IPC serialized Arrow schema)

2. **No DuckDB SQL function / CLI changes.**

3. **Internal helpers added in `extension/flight/src/duckdb_flight_sql_server.cpp`:**
   - shared SQL builder for table metadata filters (used by both include/non-include schema branches)
   - identifier quoting helper for fully-qualified table/view references
   - schema serialization helper (`SELECT * FROM <qualified> LIMIT 0` -> Arrow schema -> `ipc::SerializeSchema`)

---

## Implementation Plan

### 1) Update `GetFlightInfoTables()` schema selection
**File:** `extension/flight/src/duckdb_flight_sql_server.cpp`

- Replace current `NotImplemented` branch.
- Return:
  - `SqlSchema::GetTablesSchemaWithIncludedSchema()` when `command.include_schema == true`
  - `SqlSchema::GetTablesSchema()` otherwise (current behavior)

---

### 2) Refactor table metadata SQL construction
**File:** `extension/flight/src/duckdb_flight_sql_server.cpp`

- Extract current `DoGetTables` query-string logic into one helper so both paths reuse identical filtering/order semantics:
  - catalog filter
  - schema pattern filter
  - table-name pattern filter
  - table-type filter
  - stable ordering by catalog/schema/table

This guarantees include-schema and non-include-schema paths return the same row set.

---

### 3) Implement `DoGetTables(include_schema=true)`
**File:** `extension/flight/src/duckdb_flight_sql_server.cpp`

- Keep existing fast path for `include_schema=false`: `StreamSQL(sql)` unchanged.
- For `include_schema=true`:
  1. Execute metadata SQL via a `Connection`.
  2. Iterate rows and collect `(catalog, schema, table_name, table_type)`.
  3. For each row, compute `table_schema` bytes:
     - Build quoted fully-qualified name: `"catalog"."schema"."table"`
     - Run `SELECT * FROM <qualified_name> LIMIT 0`
     - Convert result types/names to Arrow schema via existing `DuckDBSchemaToArrow(...)`
     - Serialize via `arrow::ipc::SerializeSchema(...)`
  4. Build Arrow arrays:
     - strings for metadata columns
     - binary for `table_schema`
  5. Build `RecordBatch` with `SqlSchema::GetTablesSchemaWithIncludedSchema()`
  6. Return `RecordBatchStream` from a one-batch `RecordBatchReader`.

- Error behavior:
  - if schema derivation fails for any row, return deterministic `Status::Invalid(...)` including table identifier context.
  - `table_schema` is always non-null for emitted rows.

---

### 4) Required include additions
**File:** `extension/flight/src/duckdb_flight_sql_server.cpp`

- Add Arrow IPC include for schema serialization (`arrow/ipc/writer.h`).
- Add any builder/reader includes needed for constructing batch stream (if not already covered by `arrow/api.h` in this TU).

---

### 5) Extend integration coverage in smoke client
**File:** `extension/flight/test/client/flight_sql_smoke_client.cpp`

Enhance existing `metadata` mode (no new mode required) with include-schema scenario:

1. Call `GetTables(... include_schema=true ...)` using same `schema_pattern` and `table_pattern`.
2. Assert result contains expected entries:
   - `flight_meta_tbl` with `table_type=TABLE`
   - `flight_meta_view` with `table_type=VIEW`
3. Assert `table_schema` column exists and is non-empty for returned rows.
4. Decode `table_schema` bytes using Arrow IPC schema reader and validate expected fields for at least `flight_meta_tbl`:
   - `id`
   - `val`
   (and optionally validate same for view)

This gives real end-to-end coverage for `DoGetTables(include_schema=true)` output correctness.

---

### 6) Keep pytest wiring minimal
**File:** `tools/shell/tests/test_flight_sql.py`

- Keep existing `test_flight_sql_metadata_roundtrip` invocation.
- Since `metadata` mode is expanded, this test now also validates include-schema behavior.
- No additional daemon lifecycle test changes required.

---

## Test Cases and Scenarios

1. **FlightInfo schema selection**
   - `GetTables(include_schema=false)` returns 4-column schema.
   - `GetTables(include_schema=true)` returns 5-column schema including `table_schema`.

2. **DoGetTables include schema roundtrip**
   - includes both table and view rows under filters.
   - `table_schema` present and non-empty.
   - serialized schema bytes are IPC-deserializable.

3. **Filtering parity**
   - catalog/schema/table-pattern/table-type filters behave identically for include=false and include=true paths.

4. **Identifier safety**
   - quoted fully-qualified references handle special characters in names (escaped `"`).

5. **Regression safety**
   - existing metadata/crud/prepared/transaction shell integration tests still pass.

---

## Validation Commands

1. Build:
- `BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release`

2. Shell integration tests:
- `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q`

3. Flight lifecycle SQL test:
- `build/release/test/unittest "/duckdb/extension/flight/test/sql/flight_lifecycle.test"`

---

## Assumptions and Defaults

1. `table_schema` is generated as IPC-serialized Arrow schema from `SELECT * FROM <object> LIMIT 0`.
2. Include-schema behavior applies to both tables and views.
3. On per-object schema derivation failure, request fails (no partial result emission).
4. Existing metadata test remains the integration entrypoint; include-schema checks are added into that path.
5. No changes to transaction/prepared behavior are required for this feature.
