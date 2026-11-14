"""30_Action_Plan_Menu v1.0.0 (the Today Action Plan menu, whose items start GitHub workflows from Apps
Script): run the native harness (the REAL Apps Script source in a vm with shims for properties, triggers and UrlFetchApp)
so the whole-suite CI job collects it. Mirrors tests/test_dp2_v1301_slot_carry.py."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "test_apm_v100_action_plan_menu.js"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def test_apm_v100_action_plan_menu_native_harness():
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, str(HARNESS)], cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.rstrip().endswith("PASS 6/6"), result.stdout[-2000:]
