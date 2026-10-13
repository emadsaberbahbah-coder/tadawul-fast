"""Exercise the deploy entrypoint without starting a network service."""
import json
import os
from pathlib import Path
import resource
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = ROOT / "scripts" / "start_web.sh"


@pytest.fixture
def launch_env(tmp_path):
    fake = tmp_path / "gunicorn"
    fake.write_text(
        f"#!{sys.executable}\n"
        "import json, resource, sys\n"
        'print("LAUNCH=" + json.dumps({"args": sys.argv[1:], '
        '"nofile": resource.getrlimit(resource.RLIMIT_NOFILE)[0]}))\n'
    )
    fake.chmod(0o755)
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("TFB_", "UVICORN_", "WORKER_"))
           and key not in {"PORT", "HOST", "WEB_CONCURRENCY", "WORKERS_MAX",
                           "WORKERS_MAX_DEFAULT", "APP_MODULE", "APP_DIR"}}
    env["PATH"] = str(tmp_path) + os.pathsep + env.get("PATH", "")
    return env


def run_launcher(env, **kwargs):
    return subprocess.run([str(LAUNCHER)], cwd=ROOT, env=env,
                          text=True, capture_output=True, timeout=10, **kwargs)


def launch_record(result):
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(next(line.removeprefix("LAUNCH=")
                           for line in result.stdout.splitlines()
                           if line.startswith("LAUNCH=")))


def option(record, name):
    args = record["args"]
    return args[args.index(name) + 1]


def test_clean_checkout_executes_with_default_port(launch_env):
    record = launch_record(run_launcher(launch_env))
    assert option(record, "--bind") == "0.0.0.0:10000"
    assert option(record, "--workers") == "1"
    assert record["args"][-1] == "main:app"


@pytest.mark.parametrize("port", ["0", "00000", "65536", "-1", "abc", "9" * 40])
def test_invalid_port_fails_before_launch(launch_env, port):
    launch_env["PORT"] = port
    result = run_launcher(launch_env)
    assert result.returncode == 2
    assert "PORT must be a decimal integer in 1..65535" in result.stderr
    assert "LAUNCH=" not in result.stdout


def test_canonical_settings_reach_gunicorn_and_zero_cap_stays_bounded(launch_env):
    launch_env.update(PORT="08080", WEB_CONCURRENCY="12", WORKERS_MAX="0",
                      UVICORN_BACKLOG="73", UVICORN_KEEPALIVE="25",
                      UVICORN_GRACEFUL_TIMEOUT="60", UVICORN_ACCESS_LOG="off")
    record = launch_record(run_launcher(launch_env))
    assert option(record, "--bind") == "0.0.0.0:8080"
    assert option(record, "--workers") == "1"
    assert option(record, "--backlog") == "73"
    assert option(record, "--keep-alive") == "25"
    assert option(record, "--graceful-timeout") == "60"
    assert option(record, "--access-logfile") == "/dev/null"


def test_file_limit_survives_exec(launch_env):
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard != resource.RLIM_INFINITY and hard <= 64:
        pytest.skip("host hard limit is too low for this check")

    def lower_soft_limit():
        resource.setrlimit(resource.RLIMIT_NOFILE, (64, hard))

    record = launch_record(run_launcher(launch_env, preexec_fn=lower_soft_limit))
    target = 65535 if hard == resource.RLIM_INFINITY else min(65535, hard)
    assert record["nofile"] == target


def test_extreme_integer_values_use_safe_defaults(launch_env):
    launch_env.update(TFB_TIMEOUT="9" * 40, UVICORN_KEEPALIVE="0" * 40 + "25",
                      UVICORN_BACKLOG="9" * 40)
    result = run_launcher(launch_env)
    record = launch_record(result)
    assert option(record, "--timeout") == "180"
    assert option(record, "--keep-alive") == "25"
    assert option(record, "--backlog") == "2048"
    assert "integer expression expected" not in result.stderr
