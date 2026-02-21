## Flight SQL Per-Query Client Timeout (No Server Interrupt Path)

### Summary
Implement per-query timeout support and integration coverage entirely on the **client side** (C++ Flight SQL client and Go clients), with **no server-side interrupt/cancel handling changes** in DuckDB Flight SQL server code.

This plan explicitly avoids:
- adding server-side cancellation hooks,
- adding server-side interrupt behavior on `ServerCallContext::is_cancelled()`,
- changing Arrow Flight SQL library code.

It also reflects current branch reality:
- `max_execution_time` / `MaxExecutionTimeSetting` is not available in this branch, so no server-setting integration in this patch.

---

### Go Client Timeout Capability Findings (locked)
1. Go `database/sql` Flight SQL driver does **not** expose C++ `FlightCallOptions`.
2. Go timeout controls available now:
- `context.Context` deadlines (`QueryContext`, `ExecContext`, prepared `Stmt` context methods) for per-query timeout.
- DSN `timeout=` parameter (driver-level default timeout when caller context has no deadline).
3. Raw Go Flight SQL client (`flightsql.Client`) supports:
- per-call timeout via `context.WithTimeout(...)`,
- optional gRPC `grpc.CallOption` varargs (not a dedicated timeout object; deadline is still context-based).

---

### Important Changes (Public Interface / Test Interface)
1. **C++ smoke client CLI mode update**
- `extension/flight/test/client/flight_sql_smoke_client.cpp`
- Add `--mode timeout`.
- No DuckDB SQL/CLI API changes.

2. **Pytest integration update**
- `tools/shell/tests/test_flight_sql.py`
- Add `test_flight_sql_timeout_roundtrip(shell)`.

3. **Go lightweight timeout integration tests**
- `extension/flight/test/go/flightsql_bench_test.go`: add `TestQueryTimeoutDatabaseSQL`.
- `extension/flight/test/go/flightsql_raw_bench_test.go`: add `TestQueryTimeoutRaw`.
- Optional runner-script wiring to invoke these timeout tests before benchmark phases.

---

### Implementation Plan

1. **Add client-timeout mode to C++ smoke client**
- File: `extension/flight/test/client/flight_sql_smoke_client.cpp`
- Update argument parser/usage text:
  - mode list becomes `ping|crud|metadata|prepared|transaction|timeout`.
- Add helper:
  - `IsTimeoutStatus(const Status&)` matching timeout/cancel indicators (`Deadline Exceeded`, `timed out`, `context deadline exceeded`, gRPC timeout variants).
- Add `RunTimeout(FlightSqlClient&)`:
  - Negative path:
    - construct `arrow::flight::FlightCallOptions call_opts;`
    - set `call_opts.timeout` to a near-immediate deadline (e.g., `TimeoutDuration{0}`).
    - run at least one statement path (`client.Execute(call_opts, "SELECT 1")` or `ExecuteUpdate`) and assert timeout-like failure.
  - Positive control:
    - run a normal query with generous timeout (e.g., 5s) and assert successful roundtrip (`SELECT 1`).
  - Optional second negative path:
    - obtain ticket with generous timeout, call `DoGet` with near-immediate timeout, assert timeout-like failure.
- Wire mode dispatch in `RunMain`.

2. **Add shell integration test for timeout mode**
- File: `tools/shell/tests/test_flight_sql.py`
- Add `test_flight_sql_timeout_roundtrip(shell)` using existing daemon flow:
  - start `duckdb --batch --init /dev/null -flight-sql <free_port> :memory:`
  - wait readiness via existing `wait_for_server_ready`
  - run smoke client `--mode timeout`
  - assert return code 0, include stdout/stderr on failure
  - SIGINT daemon and reuse same clean-shutdown assertions.

3. **Add Go `database/sql` timeout test**
- File: `extension/flight/test/go/flightsql_bench_test.go`
- Add `TestQueryTimeoutDatabaseSQL`:
  - open DB via existing `openDB`.
  - use `ctx, cancel := context.WithTimeout(context.Background(), very_short_timeout)`.
  - execute query via `QueryContext` or `QueryRowContext` and assert timeout/deadline error.
  - run a control query with relaxed timeout and assert success.
- Keep test lightweight (not benchmark-sized data).

4. **Add Go raw Flight SQL timeout test**
- File: `extension/flight/test/go/flightsql_raw_bench_test.go`
- Add `TestQueryTimeoutRaw`:
  - open raw client (`openRawClient`).
  - run `client.Execute`/`rawQueryInt64` with short context deadline and assert timeout/deadline error.
  - run control query with normal timeout and assert success.
- No Arrow Go driver code modifications.

5. **Optional script update for quick timeout checks**
- File: `extension/flight/test/go/run_flight_go_bench.sh`
- Before heavy benchmark phases, run a short go test command that includes ping + timeout tests, so failures are caught early.

6. **Documentation update**
- File: `extension/flight/test/go/README.md`
- Add short section:
  - C++ uses `FlightCallOptions.timeout`.
  - Go `database/sql` uses `QueryContext`/`ExecContext` deadlines and DSN `timeout=`.
  - Go raw uses `context` deadlines (+ optional gRPC call options).

---

### Test Cases and Scenarios

1. **C++ FlightCallOptions timeout works**
- near-immediate timeout causes query RPC to fail with timeout-like status.
- same client succeeds with relaxed timeout immediately after.

2. **Shell daemon timeout integration**
- `-flight-sql` server handles timeout-test client mode and remains healthy.
- daemon still exits cleanly on SIGINT.

3. **Go `database/sql` timeout behavior**
- `QueryContext`/`ExecContext` with short deadline fails as expected.
- control query with adequate deadline succeeds.

4. **Go raw API timeout behavior**
- short context deadline fails on execute/query path.
- control query succeeds.

5. **No regressions**
- existing `crud`, `metadata`, `prepared`, `transaction` smoke/pytest tests continue passing.

---

### Validation Commands

1. Build:
- `BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release`

2. Shell integration:
- `python3 -m pytest tools/shell/tests/test_flight_sql.py --shell-binary build/release/duckdb -q`

3. Go timeout-focused:
- `cd extension/flight/test/go && go test -v -count=1 -run 'TestPing|TestQueryTimeoutDatabaseSQL|TestPingRaw|TestQueryTimeoutRaw' ./... -args -host 127.0.0.1 -port <port>`

4. Existing lifecycle SQL test:
- `build/release/test/unittest "/duckdb/extension/flight/test/sql/flight_lifecycle.test"`

---

### Assumptions and Defaults
1. Server-side cancellation/interrupt handling is out of scope for this patch.
2. Timeout enforcement in this patch is client-driven only.
3. `max_execution_time` integration is deferred until branch includes that setting.
4. Arrow Flight SQL libraries (C++ and Go) are consumed as-is; no vendored driver/library modifications.
5. Timeout assertions will accept equivalent timeout/cancellation status text variants for transport portability.
