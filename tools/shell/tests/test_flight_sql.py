import os
import signal
import subprocess
import time

import pytest


def _start_flight_shell(shell, *args):
    cmd = [shell, '--batch', '--init', '/dev/null']
    cmd.extend(args)
    # give an explicit db path so optional port parsing doesn't consume a filename
    cmd.append(':memory:')
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


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
