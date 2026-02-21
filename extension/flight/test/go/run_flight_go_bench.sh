#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

SHELL_BINARY="${ROOT_DIR}/build/release/duckdb"
HOST="127.0.0.1"
PORT=""
ROWS=100000
WORKERS=200
BATCH_SIZE=1000
CRUD_ITERS=2000
SELECT_ITERS=2000
KEEP_DB=0

print_usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --shell-binary PATH   DuckDB shell binary (default: build/release/duckdb)
  --host HOST           Flight SQL host for Go client (default: 127.0.0.1)
  --port PORT           Flight SQL port (default: auto-select free port)
  --rows N              Number of rows for benchmark phases (default: 100000)
  --workers N           Concurrent goroutines (default: 200)
  --batch-size N        Batch size for batch inserts (default: 1000)
  --crud-iters N        Iterations for CRUD single-op phase (default: 2000)
  --select-iters N      Iterations for select prepare-mode phase (default: 2000)
  --keep-db             Keep temp benchmark directory/db file
  --help                Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --shell-binary)
      SHELL_BINARY="$2"
      shift 2
      ;;
    --host)
      HOST="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --rows)
      ROWS="$2"
      shift 2
      ;;
    --workers)
      WORKERS="$2"
      shift 2
      ;;
    --batch-size)
      BATCH_SIZE="$2"
      shift 2
      ;;
    --crud-iters)
      CRUD_ITERS="$2"
      shift 2
      ;;
    --select-iters)
      SELECT_ITERS="$2"
      shift 2
      ;;
    --keep-db)
      KEEP_DB=1
      shift
      ;;
    --help)
      print_usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      print_usage
      exit 1
      ;;
  esac
done

if [[ -z "${PORT}" ]]; then
  PORT="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
  )"
fi

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/duckdb-flight-go-bench.XXXXXX")"
DB_PATH="${TMP_DIR}/flight_go_bench.duckdb"
SERVER_STDOUT="${TMP_DIR}/server_stdout.log"
SERVER_STDERR="${TMP_DIR}/server_stderr.log"
SERVER_PID=""

cleanup() {
  set +e

  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -INT "${SERVER_PID}" 2>/dev/null
    for _ in $(seq 1 200); do
      if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
    if kill -0 "${SERVER_PID}" 2>/dev/null; then
      kill -KILL "${SERVER_PID}" 2>/dev/null
    fi
    wait "${SERVER_PID}" 2>/dev/null
  fi

  if [[ -f "${DB_PATH}" ]]; then
    echo "=== DuckDB File Size ==="
    du -h "${DB_PATH}"
    stat -c '%n %s bytes' "${DB_PATH}"
  fi

  if [[ "${KEEP_DB}" -eq 1 ]]; then
    echo "Keeping benchmark files: ${TMP_DIR}"
  else
    rm -rf "${TMP_DIR}"
  fi
}

trap cleanup EXIT INT TERM

cd "${ROOT_DIR}"

if [[ ! -x "${SHELL_BINARY}" ]]; then
  echo "DuckDB shell binary not found at ${SHELL_BINARY}. Building release with flight extension..."
  BUILD_EXTENSIONS='autocomplete;httpfs;icu;json;tpch;flight' GEN=ninja make release
fi

echo "Preparing Go module dependencies..."
(
  cd "${SCRIPT_DIR}" &&
  go mod download
)

echo "Starting Flight SQL daemon:"
echo "  binary=${SHELL_BINARY}"
echo "  db=${DB_PATH}"
echo "  port=${PORT}"
"${SHELL_BINARY}" --batch --init /dev/null -flight-sql "${PORT}" "${DB_PATH}" >"${SERVER_STDOUT}" 2>"${SERVER_STDERR}" &
SERVER_PID=$!

echo "Waiting for Flight SQL readiness..."
ready=0
for _ in $(seq 1 150); do
  if (
    cd "${SCRIPT_DIR}" &&
    go test -count=1 -run TestPing ./... -args -host "${HOST}" -port "${PORT}" >/dev/null 2>&1
  ); then
    ready=1
    break
  fi

  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    echo "Flight SQL daemon exited unexpectedly." >&2
    echo "stderr:" >&2
    cat "${SERVER_STDERR}" >&2
    exit 1
  fi
  sleep 0.2
done

if [[ "${ready}" -ne 1 ]]; then
  echo "Flight SQL server was not ready in time" >&2
  echo "stderr:" >&2
  cat "${SERVER_STDERR}" >&2
  exit 1
fi

echo "Running Go Flight SQL quick timeout/connectivity tests..."
(
  cd "${SCRIPT_DIR}" &&
  go test -v -count=1 -run 'TestPing|TestQueryTimeoutDatabaseSQL|TestPingRaw|TestQueryTimeoutRaw' ./... -args \
    -host "${HOST}" \
    -port "${PORT}"
)

echo "Running Go Flight SQL benchmark test..."
(
  cd "${SCRIPT_DIR}" &&
  go test -v -count=1 -run TestFlightSQLBenchmarks ./... -args \
    -host "${HOST}" \
    -port "${PORT}" \
    -rows "${ROWS}" \
    -workers "${WORKERS}" \
    -batch-size "${BATCH_SIZE}" \
    -crud-iters "${CRUD_ITERS}" \
    -select-iters "${SELECT_ITERS}"
)
