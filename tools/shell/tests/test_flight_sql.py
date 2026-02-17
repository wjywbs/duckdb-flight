import os
import signal
import socket
import subprocess
import time

import pytest


def _start_flight_shell(shell, *args):
    cmd = [shell, '--batch', '--init', '/dev/null']
    cmd.extend(args)
    # give an explicit db path so optional port parsing doesn't consume a filename
    cmd.append(':memory:')
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def get_flight_client_binary(shell):
    shell_dir = os.path.dirname(os.path.abspath(shell))
    client_binary = os.path.join(shell_dir, 'extension', 'flight', 'flight_sql_smoke_client')
    assert os.path.exists(client_binary), f"missing Flight SQL test client binary: {client_binary}"
    assert os.access(client_binary, os.X_OK), f"Flight SQL test client is not executable: {client_binary}"
    return client_binary


def run_flight_client(client_binary, port, mode, timeout=15):
    return subprocess.run(
        [client_binary, '--host', '127.0.0.1', '--port', str(port), '--mode', mode],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def wait_for_server_ready(client_binary, port, timeout=15):
    deadline = time.time() + timeout
    last_result = None
    while time.time() < deadline:
        last_result = run_flight_client(client_binary, port, 'ping')
        if last_result.returncode == 0:
            return
        time.sleep(0.1)

    stdout = '' if last_result is None else last_result.stdout.strip()
    stderr = '' if last_result is None else last_result.stderr.strip()
    pytest.fail(f"Flight SQL server was not ready within {timeout}s. client stdout='{stdout}' stderr='{stderr}'")


def test_flight_sql_daemon_mode_no_prompt(shell):
    proc = _start_flight_shell(shell, '-flight-sql', '12345')
    try:
        time.sleep(1.0)
        proc.send_signal(signal.SIGINT)
        stdout, stderr = proc.communicate(timeout=15)
        assert proc.returncode == 0
        assert 'Enter ".help" for usage hints.' not in stdout
        assert 'Error:' not in stderr
    finally:
        if proc.poll() is None:
            proc.kill()


def test_flight_sql_daemon_mode_default_port(shell):
    proc = _start_flight_shell(shell, '-flight-sql')
    try:
        time.sleep(1.0)
        proc.send_signal(signal.SIGINT)
        stdout, stderr = proc.communicate(timeout=15)
        assert proc.returncode == 0
        assert 'Enter ".help" for usage hints.' not in stdout
        assert 'Error:' not in stderr
    finally:
        if proc.poll() is None:
            proc.kill()


@pytest.mark.parametrize('port', ['70000', '999999999999999999999999999999'])
def test_flight_sql_invalid_port(shell, port):
    cmd = [shell, '--batch', '--init', '/dev/null', '-flight-sql', port, ':memory:']
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode != 0
    assert f'invalid port for -flight-sql: {port}' in proc.stderr


def test_flight_sql_crud_roundtrip(shell):
    port = find_free_port()
    client_binary = get_flight_client_binary(shell)
    proc = _start_flight_shell(shell, '-flight-sql', str(port))

    try:
        wait_for_server_ready(client_binary, port, timeout=20)
        client_result = run_flight_client(client_binary, port, 'crud', timeout=30)
        assert (
            client_result.returncode == 0
        ), f"CRUD client failed with stdout='{client_result.stdout}' stderr='{client_result.stderr}'"
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)

        try:
            stdout, stderr = proc.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            pytest.fail("Flight SQL daemon did not exit after SIGINT")

        assert proc.returncode == 0
        assert 'Error:' not in stderr
        assert 'Enter ".help" for usage hints.' not in stdout
