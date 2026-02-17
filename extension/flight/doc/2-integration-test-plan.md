## Flight SQL Query Integration Tests (CRUD Roundtrip via Real Client)

### Summary
Add a real end-to-end integration test path that starts DuckDB in `-flight-sql` daemon mode and validates `CREATE TABLE`, `INSERT`, `UPDATE`, `SELECT`, and `DROP TABLE` through an Apache Arrow Flight SQL client connection.  
Use a new minimal in-tree C++ test client (recommended approach selected) instead of Arrow `test_app_cli.cc`, to avoid `gflags`/Arrow-examples build coupling and keep the DuckDB build/test flow stable.

### Why this method (vs `test_app_cli.cc`)
1. `third_party/arrow/.../test_app_cli.cc` depends on Arrow’s example/test build path and `gflags`.
2. Our `extension/flight/CMakeLists.txt` intentionally disables Arrow examples/tests.
3. A tiny custom client linked to already-built `arrow_flight_sql_static` keeps dependencies minimal and deterministic.

### Public API / Interface Changes
1. No user-facing DuckDB SQL or shell API changes.
2. New internal test executable target only:
   - `flight_sql_smoke_client` (name can be finalized during implementation, but keep stable in tests).

### Implementation Plan

1. Add a dedicated Flight SQL smoke client source file.
   - File: `extension/flight/test/client/flight_sql_smoke_client.cpp`.
   - Behavior:
     - Parse CLI args `--host`, `--port`, `--mode`.
     - Support `--mode ping` and `--mode crud`.
     - `ping`: execute `SELECT 1` via Flight SQL and exit success on roundtrip.
     - `crud`: run full sequence:
       - `CREATE TABLE flight_it (id INTEGER, val VARCHAR)`.
       - `INSERT INTO flight_it VALUES (1,'a'), (2,'b'), (3,'c')`.
       - `UPDATE flight_it SET val='bb' WHERE id=2`.
       - `SELECT id, val FROM flight_it ORDER BY id`.
       - `DROP TABLE flight_it`.
       - `SELECT COUNT(*) FROM duckdb_tables() WHERE table_name='flight_it'` and assert `0`.
     - Validate returned data from `SELECT` (row count and expected values).
     - Print clear failure diagnostics to `stderr`; return non-zero on any failure.

2. Wire the client executable into extension CMake.
   - File: `extension/flight/CMakeLists.txt`.
   - Add `add_executable(flight_sql_smoke_client ...)`.
   - Link with:
     - `arrow_flight_sql_static`
     - `arrow_flight_static`
     - `arrow_static`
   - Set C++20 for the target.
   - Ensure runtime output is placed under `build/.../extension/flight/` so pytest can discover it relative to the shell binary.

3. Extend shell pytest coverage to run real Flight SQL CRUD.
   - File: `tools/shell/tests/test_flight_sql.py`.
   - Add helpers:
     - `find_free_port()` using Python `socket` bind to `127.0.0.1:0`.
     - `get_flight_client_binary(shell_path)` to resolve client path from shell directory.
     - `wait_for_server_ready(client, port)` that retries `--mode ping` until ready or timeout.
   - Add test:
     - `test_flight_sql_crud_roundtrip(shell)`.
     - Start `duckdb --batch --init /dev/null -flight-sql <free_port> :memory:`.
     - Wait until server responds via client `ping`.
     - Run client in `crud` mode.
     - Send SIGINT to shell daemon and assert clean shutdown.
     - Assert no startup/load errors in stderr.

4. Keep existing daemon tests and invalid-port tests as-is.
   - Existing tests already cover daemon mode behavior and bad port validation.
   - New test is additive and focuses on real query execution semantics over Flight SQL protocol.

### Test Cases and Scenarios

1. Daemon lifecycle remains validated.
   - Existing:
     - no interactive prompt in daemon mode.
     - default port path works.
     - invalid ports fail with clear error.

2. New end-to-end CRUD over Flight SQL.
   - Server starts on dynamic free port.
   - Client connects and executes DDL/DML/query over Flight SQL.
   - Data correctness asserted after update.
   - Table removal verified after drop.
   - Daemon exits cleanly on SIGINT.

3. Startup race resilience.
   - Test retries `ping` with bounded timeout before running CRUD, preventing flaky startup timing failures.

### Build and Validation Commands

1. Build:
   - `BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release`
2. Shell/Flight pytest:
   - `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q`
3. Existing extension SQL lifecycle test:
   - `build/release/test/unittest "/duckdb/extension/flight/test/sql/flight_lifecycle.test"`

### Assumptions and Defaults
1. Test harness choice is fixed to `pytest + custom C++ client` (your selected option).
2. Listen protocol remains plaintext gRPC on `0.0.0.0`; client connects to `127.0.0.1`.
3. No auth/TLS is required for this integration test.
4. Tests are written to be portable with dynamic port allocation to avoid collisions.
5. The new client target is test-only and not installed as a public DuckDB artifact.
