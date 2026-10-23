"""Real native Top10 rendering/completion with synthetic external boundaries."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_native_top10_completion_precision():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is unavailable")
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node, str(root / "tests/js/native_top10_completion_precision.test.js")],
        cwd=root, capture_output=True, text=True, timeout=60, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS 44/44" in result.stdout
