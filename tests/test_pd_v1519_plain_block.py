"""10_My_Portfolio v1.5.19 (plain block reasons + ticker-change hint; Script Property PD_PLAIN_REASONS, default
off): run the native harness (the REAL Apps Script source in a vm; golden negative on the v1.5.18 file rebuilt from
tests/js/pd_edits_v1519.json, SHA-256 pinned) so the whole-suite CI job collects it. Mirrors
tests/test_my_portfolio_v1518_cash_snapshot.py."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "test_pd_v1519_plain_block.js"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def test_pd_v1519_plain_block_native_harness():
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, str(HARNESS)], cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.rstrip().endswith("PASS 9/9"), result.stdout[-2000:]
    assert "mutants killed 10/10" in result.stdout, result.stdout[-2000:]
    assert "x2 identical digest" in result.stdout, result.stdout[-2000:]
