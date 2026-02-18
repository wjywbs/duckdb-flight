# Flight SQL Prepared Statements + Parameterized Query Support

## Summary
Implement full **SQL prepared statement** support in DuckDB’s Flight SQL server (create, bind, execute query, execute update, close) and add end-to-end integration coverage through the existing daemon + smoke-client pytest flow.

This plan uses the confirmed decisions:
1. Scope: **SQL prepared statements only** (no prepared Substrait in this iteration).
2. Query binding semantics: **single bound row only** for prepared queries.
3. Binding type coverage: **common scalar set** only (bool, integer widths, float/double, string/large_string, null).

## Public API / Interface Changes
1. Flight SQL server behavior changes (protocol-level):
- `CreatePreparedStatement` implemented.
- `ClosePreparedStatement` implemented.
- `GetFlightInfoPreparedStatement` implemented.
- `DoGetPreparedStatement` implemented.
- `DoPutPreparedStatementQuery` implemented.
- `DoPutPreparedStatementUpdate` implemented.
- `GetSchemaPreparedStatement` implemented for completeness and client compatibility.
2. No new DuckDB SQL functions and no shell flag changes.
3. Internal test client CLI gains one mode:
- `flight_sql_smoke_client --mode prepared`

## Implementation Plan

## 1. Add prepared-statement server state and helpers
Files:
- `extension/flight/src/include/duckdb_flight_sql_server.hpp`
- `extension/flight/src/duckdb_flight_sql_server.cpp`

Changes:
1. Add a server-owned prepared-handle store:
- Global map in server instance keyed by opaque handle string.
- Handle state includes:
  - original SQL query text
  - ordered parameter definitions `(name, duckdb::LogicalType, position)`
  - prepared result dataset schema (`std::shared_ptr<arrow::Schema>`)
  - latest bound query parameters (optional, for `DoPutPreparedStatementQuery` -> `DoGetPreparedStatement`)
  - per-handle mutex
- Add server-level mutex for handle map operations.
2. Add handle helpers:
- `GeneratePreparedHandle()`
- `LookupPreparedHandle(handle)` returning typed error on missing handle.
3. Add schema helpers:
- `DuckDBResultSchemaToArrow(types, names, client_properties)` via existing `ArrowConverter::ToArrowSchema` + `arrow::ImportSchema`.
- `ParameterSchemaToArrow(ordered_params, client_properties)` using the same converter path.
4. Add streaming helper:
- `ResultToFlightStream(unique_ptr<QueryResult>)` reusing `ResultArrowArrayStreamWrapper` + Arrow C Data import path.
- Refactor existing SQL stream path to reuse this helper.

## 2. Implement SQL prepared statement protocol methods
Files:
- `extension/flight/src/include/duckdb_flight_sql_server.hpp`
- `extension/flight/src/duckdb_flight_sql_server.cpp`

Methods to override and behavior:

1. `CreatePreparedStatement(...)`
- Create a new `Connection` from server DB.
- `conn.Prepare(request.query)`.
- On error: `Status::Invalid` with DuckDB error text.
- Build ordered parameter list:
  - derive order from `prepared->named_param_map` index values
  - derive types from `prepared->GetExpectedParameterTypes()`
- Build dataset schema from `prepared->GetTypes()` / `prepared->GetNames()`.
- Build parameter schema from ordered parameter definitions.
- Store state under generated handle.
- Return `ActionCreatePreparedStatementResult {dataset_schema, parameter_schema, handle}`.

2. `ClosePreparedStatement(...)`
- Erase handle from map.
- Missing handle -> `Status::Invalid("Prepared statement not found")`.

3. `GetFlightInfoPreparedStatement(...)`
- Lookup handle.
- Return `GetFlightInfoForSchema(descriptor, dataset_schema)`.

4. `GetSchemaPreparedStatement(...)`
- Lookup handle.
- Return `SchemaResult` with stored dataset schema.

5. `DoPutPreparedStatementQuery(...)`
- Lookup handle.
- Read incoming `FlightMessageReader` batches and bind parameters.
- Enforce query semantics:
  - total bound rows must be `<= 1`
  - if `>1`, return `Status::Invalid` (explicit single-row-only behavior)
- Persist latest bound row (or empty binding for zero-parameter statement) into handle state.
- Return `Status::OK()`.

6. `DoGetPreparedStatement(...)`
- Lookup handle and load stored binding.
- If statement expects parameters and no binding exists -> `Status::Invalid`.
- Create new `Connection`, `Prepare(query)`, execute with named bindings.
- Use streaming result path (`SendQuery`-style output via `QueryResult` wrapper) to return `FlightDataStream`.

7. `DoPutPreparedStatementUpdate(...)`
- Lookup handle.
- Read all parameter rows from `FlightMessageReader`.
- Behavior:
  - zero-parameter statement: execute once and return changed rows.
  - parameterized statement: execute once per bound row and accumulate changed rows.
  - if parameterized and zero bound rows: `Status::Invalid`.
- Return total changed rows as protocol requires.

## 3. Arrow parameter binding conversion rules
File:
- `extension/flight/src/duckdb_flight_sql_server.cpp`

Add explicit conversion utility from Arrow batch cell -> `duckdb::BoundParameterData`:

Supported Arrow types in v1:
- `BOOL`
- signed integers: `INT8/16/32/64`
- unsigned integers: `UINT8/16/32/64`
- floating: `FLOAT`, `DOUBLE`
- strings: `STRING`, `LARGE_STRING`
- null value handling for all above

Unsupported types:
- Return `Status::NotImplemented` with clear type name.

Binding semantics:
- Match columns by **prepared parameter order** (from `named_param_map` index), and validate column count.
- Build `case_insensitive_map_t<BoundParameterData>` by parameter name.
- Use `BoundParameterData(value, expected_type)` so DuckDB cast/type checks remain authoritative.

## 4. Extend smoke client for prepared integration mode
File:
- `extension/flight/test/client/flight_sql_smoke_client.cpp`

Changes:
1. Add mode:
- `--mode prepared`
2. Add `RunPrepared(...)` that performs:
- setup:
  - drop test table if exists
  - create `flight_prep_it(id INTEGER, val VARCHAR)`
- prepared update test:
  - prepare `INSERT INTO flight_prep_it VALUES (?, ?)`
  - bind 3 rows in one RecordBatch
  - `ExecuteUpdate()` must return 3
- prepared update-with-filter test:
  - prepare `UPDATE flight_prep_it SET val = ? WHERE id = ?`
  - bind 2 rows
  - `ExecuteUpdate()` must return 2
- prepared query test:
  - prepare `SELECT id, val FROM flight_prep_it WHERE id > ? ORDER BY id`
  - bind one row (`id > 1`)
  - execute + `DoGet` and validate rows `[(2, "bb"), (3, "cc")]`
- single-row-only query enforcement:
  - prepare `SELECT ?::INTEGER AS x`
  - bind 2 rows
  - verify execute path fails (non-zero status from client API) and report clear diagnostics
- cleanup:
  - drop table
- explicitly close prepared statements where possible.
3. Keep existing `ping`, `crud`, `metadata` modes unchanged.

## 5. Add pytest integration coverage for prepared mode
File:
- `tools/shell/tests/test_flight_sql.py`

Add:
- `test_flight_sql_prepared_roundtrip(shell)`

Flow:
1. Allocate free port.
2. Start shell daemon: `duckdb --batch --init /dev/null -flight-sql <port> :memory:`.
3. Wait for readiness via existing `ping`.
4. Run smoke client `--mode prepared`.
5. Assert return code zero and include stdout/stderr in failure text.
6. SIGINT daemon and assert clean shutdown (same checks as existing CRUD/metadata tests).

## 6. Validation and acceptance criteria
Commands:
1. `BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release`
2. `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q`
3. `build/release/test/unittest "/duckdb/extension/flight/test/sql/flight_lifecycle.test"`

Must pass:
1. Existing daemon/invalid-port/CRUD/metadata tests still pass.
2. New prepared roundtrip test passes.
3. No regressions in Flight lifecycle SQL test.

## Test Cases and Scenarios
1. Prepared statement create/close lifecycle works.
2. Parameter schema and dataset schema are returned and usable by Arrow client.
3. Prepared query with one bound row returns correct result.
4. Prepared update with multi-row binding aggregates changed-row count correctly.
5. Missing handle returns deterministic error.
6. Query binding with more than one row returns deterministic `Invalid`.
7. Unsupported Arrow bind type returns deterministic `NotImplemented`.

## Assumptions and Defaults
1. Only SQL prepared statement APIs are in scope; prepared Substrait remains `NotImplemented`.
2. Prepared query bindings accept at most one row; update bindings may contain multiple rows.
3. Arrow binding type support in v1 is limited to common scalar set (bool/int/uint/float/double/string + null).
4. No auth/TLS/session changes are included in this iteration.
5. Tests remain end-to-end through shell daemon + in-tree C++ Flight client.
