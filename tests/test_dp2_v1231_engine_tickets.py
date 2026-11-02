"""27_Decision_Pages_v2 v1.2.31 (engine-ticket mirror; with the prepared v1.2.29 D4 and v1.2.30 certification
steps of 28 Oct): run the native harness (the REAL Apps Script source in a vm; the v1.2.30 oracle and the live
v1.2.28 base rebuilt from tests/js/dp2_edits_v1231.json and tests/js/dp2_steps_v1230.json, SHA-256 pinned) so the
whole-suite CI job collects it. Mirrors tests/test_dt10_v1190_hold_live_rule.py."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "test_dp2_v1231_engine_tickets.js"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def test_dp2_v1231_engine_tickets_native_harness():
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, str(HARNESS)], cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.rstrip().endswith("PASS 11/11"), result.stdout[-2000:]
