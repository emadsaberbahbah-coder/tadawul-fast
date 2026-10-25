"""start_web.sh v2.7.0: worker recycling and the glibc arena cap.

Runs the real launcher with ``--print-plan`` (it prints the resolved gunicorn
command and exits 2 without launching) against a stub ``gunicorn`` on PATH, so
no server starts and no network is touched.
"""
import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "start_web.sh"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash required")


def _plan(tmp_path, **env):
    stub = tmp_path / "bin"
    stub.mkdir(exist_ok=True)
    gun = stub / "gunicorn"
    gun.write_text("#!/bin/sh\nexit 0\n")
    gun.chmod(gun.stat().st_mode | stat.S_IEXEC)
    base = {"PATH": f"{stub}:{os.environ.get('PATH', '')}", "HOME": str(tmp_path)}
    base.update(env)
    proc = subprocess.run(["bash", str(SCRIPT), "--print-plan"], cwd=ROOT, env=base,
                          capture_output=True, text=True, timeout=30)
    assert proc.returncode == 2, proc.stderr
    boot = next(line for line in proc.stdout.splitlines() if "boot config:" in line)
    cmd = next(line for line in proc.stdout.splitlines() if line.strip().startswith("gunicorn"))
    return boot, cmd


def test_version_bumped():
    assert 'readonly SCRIPT_VERSION="2.7.0"' in SCRIPT.read_text()


def test_defaults_recycle_workers_and_cap_arenas(tmp_path):
    boot, cmd = _plan(tmp_path)
    assert "--max-requests 200" in cmd
    assert "--max-requests-jitter 50" in cmd
    assert "max_requests=200 max_requests_jitter=50 malloc_arena_max=2" in boot


def test_zero_restores_v261_command_line(tmp_path):
    boot, cmd = _plan(tmp_path, TFB_MAX_REQUESTS="0")
    assert "--max-requests" not in cmd
    assert "max_requests=0 max_requests_jitter=0" in boot


def test_explicit_values_win(tmp_path):
    _, cmd = _plan(tmp_path, TFB_MAX_REQUESTS="500", TFB_MAX_REQUESTS_JITTER="120")
    assert "--max-requests 500" in cmd and "--max-requests-jitter 120" in cmd
    _, cmd = _plan(tmp_path, WORKER_MAX_REQUESTS="300")
    assert "--max-requests 300" in cmd


def test_arena_cap_respects_environment(tmp_path):
    boot, _ = _plan(tmp_path, MALLOC_ARENA_MAX="4")
    assert "malloc_arena_max=4" in boot
    boot, _ = _plan(tmp_path, TFB_MALLOC_ARENA_MAX="off")
    assert "malloc_arena_max=unset" in boot
    boot, _ = _plan(tmp_path, TFB_MALLOC_ARENA_MAX="3")
    assert "malloc_arena_max=3" in boot
