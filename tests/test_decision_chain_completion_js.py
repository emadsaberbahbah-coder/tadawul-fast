"""Offline source/claim/native completion integration for the opt-in chain."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_decision_chain_completion():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is unavailable")
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node, str(root / "tests/js/decision_chain_completion.test.js")],
        cwd=root, capture_output=True, text=True, timeout=60, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS 141/141 decision-chain completion checks" in result.stdout
