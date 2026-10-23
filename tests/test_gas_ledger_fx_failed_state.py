"""10_My_Portfolio v1.5.14 + 16_Decision_Top10 v1.13.10: run the native harness
(the REAL Apps Script sources in a vm) and check its outputs against the real
Python consumers, so the whole-suite CI job collects it:
- the FAILED banner a throwing refresh leaves is parsed by the freshness audit
  as a stamped, terminal, not-OK status (the 'running...' line it replaces
  parses as RUNNING);
- the ledger status set GAS now excludes is the set audit_full_refresh_coverage
  excludes (Sold/Closed/Inactive out, blank in).
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from scripts.audit_decision_surface_freshness import parse_surface_status
from scripts.audit_full_refresh_coverage import ledger_symbols

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "js" / "gas_ledger_fx_failed_state.test.js"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


@pytest.fixture(scope="module")
def dump(tmp_path_factory):
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    out = tmp_path_factory.mktemp("gas_fix") / "dump.json"
    env = dict(os.environ, GAS_FIX_DUMP=str(out))
    proc = subprocess.run([node, str(HARNESS)], cwd=ROOT, env=env, capture_output=True,
                          text=True, timeout=180)
    assert proc.returncode == 0, proc.stdout[-2000:] + proc.stderr[-2000:]
    assert "RESULT 11/11 PASS" in proc.stdout, proc.stdout[-2000:]
    return json.loads(out.read_text(encoding="utf-8"))


def test_failed_banner_is_a_stamped_terminal_not_ok_status(dump):
    for key in ("failed", "failed_before_bind"):
        stamp, status = parse_surface_status(dump[key])
        assert stamp is not None, key
        assert status == "FAILED", key
        assert "failed_at_utc=" in dump[key]


def test_running_banner_it_replaces_parses_as_running(dump):
    stamp, status = parse_surface_status(dump["running"])
    assert stamp is not None and status == "RUNNING"


def test_python_ledger_reader_excludes_the_same_statuses():
    grid = [["Symbol", "Status", "Shares"],
            ["AAA.US", "Active", 10], ["AAA.US", "Sold", 10],
            ["BBB.US", " CLOSED ", 4], ["CCC.US", "sold", 1], ["DDD.US", "Inactive", 2],
            ["EEE.US", "", 3]]
    symbols, problems = ledger_symbols(grid)
    assert problems == []
    assert symbols == ["AAA.US", "EEE.US"]
