"""Full native flag-gated price display and opt-in consumed-run completion."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_native_portfolio_completion_and_precision():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is unavailable")
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node, str(root / "tests/js/pf_completion_precision.test.js")],
        cwd=root, capture_output=True, text=True, timeout=60, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PF completion/precision: 47 checks passed" in result.stdout
