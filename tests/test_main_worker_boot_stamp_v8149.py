"""main.py v8.14.9: /health worker_boot_utc belongs to the worker that answers.

A worker forked after the app module was imported (gunicorn preload, or an
import in the master) must stamp its own boot time; within one process the
stamp stays stable.
"""
import importlib.util
import os
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load_main():
    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("APP_ENV", "production")
    spec = importlib.util.spec_from_file_location("main_boot_stamp_under_test", ROOT / "main.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["main_boot_stamp_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def main_mod():
    return _load_main()


def test_version(main_mod):
    assert main_mod.APP_ENTRY_VERSION == "8.14.9"


def test_stamp_stable_within_process(main_mod):
    a = main_mod._deploy_provenance()["worker_boot_utc"]
    b = main_mod._deploy_provenance()["worker_boot_utc"]
    assert a == b == main_mod._PROCESS_BOOT_UTC


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
def test_forked_worker_gets_its_own_stamp(main_mod):
    parent = main_mod._deploy_provenance()["worker_boot_utc"]
    time.sleep(0.01)
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:  # child: what a gunicorn worker would report
        os.close(r)
        try:
            os.write(w, main_mod._deploy_provenance()["worker_boot_utc"].encode())
        finally:
            os._exit(0)
    os.close(w)
    child = os.read(r, 200).decode()
    os.waitpid(pid, 0)
    assert child and child != parent
    assert child > parent  # ISO-8601 UTC strings order chronologically


def test_pid_guard_without_fork_hook(main_mod, monkeypatch):
    before = main_mod._current_boot_stamp()
    monkeypatch.setattr(main_mod, "_PROCESS_BOOT_PID", -1)
    time.sleep(0.01)
    after = main_mod._current_boot_stamp()
    assert after != before and main_mod._PROCESS_BOOT_PID == os.getpid()
