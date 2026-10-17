"""Offline native completion/unchanged pending-caller integration regressions."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_native_decision_refresh_completion():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is unavailable")
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node, str(root / "tests/js/decision_refresh_completion.test.js")],
        cwd=root, capture_output=True, text=True, timeout=60, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS 43/43" in result.stdout
