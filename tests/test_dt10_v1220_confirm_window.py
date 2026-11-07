"""16_Decision_Top10 v1.22.0 (entry confirmation counts the qualified
day-keys inside a rolling window; Script Property TFB_T10_CONFIRM_WINDOW,
default off = the v1.21.1 clocks byte for byte): run the native harness (the
REAL Apps Script source in a vm, golden negative on the v1.21.1 base rebuilt
from tests/js/dt10_edits_v1220.json, kill identity through
dt10ApplyStability_, service read, sanitizer, self-test, lint, mutants) so the
whole-suite CI job collects it. Mirrors tests/test_dt10_v1211_mf_scope.py."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "test_dt10_v1220_confirm_window.js"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def test_dt10_v1220_confirm_window_native_harness():
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, str(HARNESS)], cwd=ROOT, capture_output=True, text=True, timeout=900)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.rstrip().endswith("PASS 10/10"), result.stdout[-2000:]
