## DuckDB Flight SQL Extension + `-flight-sql` Daemon Mode

### Summary
Implement a new in-tree `extension/flight` that provides an Apache Arrow Flight SQL gRPC server for DuckDB, built from `third_party/arrow/cpp` during DuckDB build when `flight` is enabled. Add shell flag `-flight-sql [PORT]` to launch the server and run non-interactively until Ctrl-C/SIGTERM.

### Commit Plan (small, iterative)
1. **Scaffold extension and build wiring**
   - Add `extension/flight/` with:
     - `extension/flight/CMakeLists.txt`
     - `extension/flight/extension_config.cmake`
     - `extension/flight/src/include/flight_extension.hpp`
     - `extension/flight/src/flight_extension.cpp`
     - `extension/flight/README.md`
   - Build Arrow in-tree from `third_party/arrow/cpp` inside this extension CMake:
     - `ARROW_BUILD_STATIC=ON`, `ARROW_BUILD_SHARED=OFF`
     - `ARROW_FLIGHT=ON`, `ARROW_FLIGHT_SQL=ON`
     - tests/examples/utilities OFF
   - Link `flight_extension` and `flight_loadable_extension` against Arrow Flight SQL static targets.

2. **Server lifecycle service in extension**
   - Add:
     - `extension/flight/src/include/flight_service.hpp`
     - `extension/flight/src/flight_service.cpp`
   - Implement singleton service with thread-safe `Start(port, db_instance)`, `Stop()`, `IsStarted()`, `Location()`.
   - `Start` creates `DuckDBFlightSqlServer`, calls Arrow `Init`, then `Serve()` on background thread.
   - `Stop` triggers shutdown and joins thread.
   - Ensure idempotent start/stop semantics.

3. **Flight SQL protocol implementation (Core + Metadata)**
   - Add:
     - `extension/flight/src/include/duckdb_flight_sql_server.hpp`
     - `extension/flight/src/duckdb_flight_sql_server.cpp`
   - Implement subclass of `arrow::flight::sql::FlightSqlServerBase` with:
     - `GetFlightInfoStatement`
     - `DoGetStatement`
     - `DoPutCommandStatementUpdate`
     - `GetFlightInfoCatalogs` + `DoGetCatalogs`
     - `GetFlightInfoSchemas` + `DoGetDbSchemas`
     - `GetFlightInfoTables` + `DoGetTables` (with `include_schema=false` support in v1)
     - `GetFlightInfoTableTypes` + `DoGetTableTypes`
   - Use per-request DuckDB `Connection` (shared `DatabaseInstance`) for concurrency.
   - Use DuckDB `ResultArrowArrayStreamWrapper` + Arrow C Data `ImportRecordBatchReader` to produce `RecordBatchStream`.
   - Register core SQL info entries (`DBMS name/version`, server name/version, identifier quoting, etc.).

4. **Expose SQL control functions**
   - In `flight_extension.cpp`, register:
     - `start_flight_sql_server()` (default port)
     - `start_flight_sql_server(port)`
     - `stop_flight_sql_server()`
     - `flight_sql_is_started()`
     - `get_flight_sql_url()`
   - Return human-readable status strings and deterministic errors.

5. **Shell integration for daemon mode**
   - Modify `tools/shell/include/shell_state.hpp` and `tools/shell/shell.cpp`:
     - Add CLI option text for `-flight-sql [PORT]`.
     - Parse optional numeric port in both passes.
     - On `-flight-sql`, run `CALL start_flight_sql_server(<port>)`.
     - Skip interactive shell and block until interrupt signal.
     - On shutdown path, execute `CALL stop_flight_sql_server()` best-effort, then exit.
   - Validate bad port values with clear error messages.

6. **Tests**
   - Add extension SQL tests:
     - `extension/flight/test/sql/flight_lifecycle.test`:
       - start/stop idempotency
       - started flag
       - URL reporting
   - Add shell test:
     - `tools/shell/tests/test_flight_sql.py`:
       - `-flight-sql` starts daemon mode (no interactive prompt)
       - accepts optional port
       - exits cleanly on SIGINT
   - Add minimal C++ unit/integration test (if test harness permits):
     - start service
     - run two concurrent Flight SQL client statement queries
     - assert both return results.

7. **Docs + build command updates**
   - Add `extension/flight/README.md` usage:
     - SQL lifecycle functions
     - shell flag examples
     - concurrency notes
   - Update root docs snippet for extension build command including `flight`.

### Public API / Interface Changes
1. **New shell option**
   - `-flight-sql [PORT]` in `tools/shell/shell.cpp`.
2. **New extension SQL routines**
   - `CALL start_flight_sql_server();`
   - `CALL start_flight_sql_server(<port>);`
   - `CALL stop_flight_sql_server();`
   - `SELECT * FROM flight_sql_is_started();`
   - `SELECT * FROM get_flight_sql_url();`
3. **New extension target**
   - `flight` in `BUILD_EXTENSIONS`.

### Build & Validation Commands
1. Build:
   - `BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release`
2. Unit tests:
   - `make unittest_release`
3. Shell tests:
   - `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb`
4. Manual smoke:
   - `build/release/duckdb my.db -flight-sql 12345`
   - from client: connect via Flight SQL and run parallel `SELECT` queries.

### Edge Cases / Failure Modes Covered
1. Port invalid/out of range/bind failure.
2. Start when already running, stop when not running.
3. Query handle not found / expired ticket.
4. Database instance invalidation and safe shutdown.
5. Ctrl-C during daemon mode always tears down server thread.

### Assumptions and Defaults
1. Default port is **12345** when omitted.
2. Listen address is **`0.0.0.0`** (TCP service semantics).
3. v1 leaves prepared statements and transaction actions as Arrow `NotImplemented`.
4. `GetTables` supports v1 with `include_schema=false`; `include_schema=true` returns `NotImplemented` for now.
5. No auth/TLS in v1 (plaintext gRPC), to be added in follow-up.
