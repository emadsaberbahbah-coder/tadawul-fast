"""27_Decision_Pages_v2 v1.4.4 (plan accept, total return, evidence recent): run the native harness (the REAL Apps
Script source in a vm; golden negative on the v1.4.3 file rebuilt from tests/js/dp2_edits_v1440.json, SHA-256 pinned) so
the whole-suite CI job collects it. Mirrors tests/test_dp2_v1430_stop_of_record.py."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "test_dp2_v1440_three_cells.js"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def test_dp2_v1440_three_cells_native_harness():
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, str(HARNESS)], cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.rstrip().endswith("PASS 7/7"), result.stdout[-2000:]
