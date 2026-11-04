"""28_Plan_Of_Record v1.0.0 (one locked plan per market session, tracked to execution): run the native
harness (the REAL Apps Script source in a vm with a recording GAS shim and the live 2 Nov board in
tests/fixtures/por_board_2026-11-02.json) so the whole-suite CI job collects it. Mirrors
tests/test_dp2_v1231_engine_tickets.py."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "test_por_v100.js"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def test_por_v100_native_harness():
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, str(HARNESS)], cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.rstrip().endswith("PASS 62/62"), result.stdout[-2000:]


def test_por_v100_source_is_ascii_es5():
    src = (ROOT / "apps_script" / "28_Plan_Of_Record.gs").read_text(encoding="utf-8")
    assert all(ord(ch) < 127 for ch in src.replace("\r", "")), "28_Plan_Of_Record.gs must be ASCII"
    for token in ("=>", "`", ".startsWith(", ".includes(", "Object.assign", ".padStart("):
        assert token not in src, token
    assert "var POR_VERSION = '1.0.1';" in src   # v1.0.1 (render priority)
