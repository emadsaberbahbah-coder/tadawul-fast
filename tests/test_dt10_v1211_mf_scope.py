"""16_Decision_Top10 v1.21.1 (the approved Mutual_Funds scope drops 52 dead
fund rows; policy data only): run the native harness (the REAL Apps Script
source in a vm, golden negative on the v1.21.0 base rebuilt from
tests/js/dt10_edits_v1211.json, manifest binding, the 52 against the
commit sheet) so the whole-suite CI job collects it. Mirrors
tests/test_dt10_v1210_exdiv_guard.py."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "test_dt10_v1211_mf_scope.js"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def test_dt10_v1211_mf_scope_native_harness():
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, str(HARNESS)], cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.rstrip().endswith("PASS 7/7"), result.stdout[-2000:]
