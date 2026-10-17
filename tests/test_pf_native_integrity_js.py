"""Discover full-source, offline native portfolio response/ledger regressions."""
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("script, expected", (
    ("pf_response_integrity.test.js", "PF response integrity: 48 checks passed"),
    ("pf_ledger_cost_proof.test.js", "PF ledger cost proof: 53 checks passed"),
))
def test_native_portfolio_integrity(script, expected):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is unavailable")
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node, str(root / "tests/js" / script)],
        cwd=root, capture_output=True, text=True, timeout=60, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert expected in result.stdout
