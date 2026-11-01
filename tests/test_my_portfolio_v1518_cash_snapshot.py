"""10_My_Portfolio v1.5.18 (cash snapshot writer, last-known cash, follow-up
retry; two Script Properties, default off): run the native harness (the REAL
Apps Script source in a vm with service stubs, the v1.5.17 base compared live
when git can supply it, five mutants) so the whole-suite CI job collects it.
Mirrors tests/test_dt10_v1190_hold_live_rule.py."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "test_my_portfolio_v1518_cash_snapshot.js"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def test_my_portfolio_v1518_cash_snapshot_native_harness():
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, str(HARNESS)], cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.rstrip().endswith("PASS 10/10"), result.stdout[-2000:]
    assert "mutants killed 5/5" in result.stdout, result.stdout[-2000:]
    assert "x3 identical digest" in result.stdout, result.stdout[-2000:]
