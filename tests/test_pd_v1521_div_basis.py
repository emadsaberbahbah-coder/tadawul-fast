"""10_My_Portfolio v1.5.21 [DIV-BASIS] (the dividends a holding has received, from the ledger's ACTIVE lots, go to the
backend with its row so portfolio_actions 1.19.0 does not read a paid-out dividend as a loss): run the native harness
(the REAL Apps Script source in a vm; the v1.5.20 base rebuilt from tests/js/pd_edits_v1521.json, SHA-256 pinned) so
the whole-suite CI job collects it. Mirrors tests/test_pd_v1520_exit_plan_stop.py."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "test_pd_v1521_div_basis.js"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def test_pd_v1521_div_basis_native_harness():
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, str(HARNESS)], cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.rstrip().endswith("PASS 5/5"), result.stdout[-2000:]
    assert "mutants killed 7/7" in result.stdout, result.stdout[-2000:]


def test_the_backend_reads_the_field_this_file_sends():
    """The header the script sends is one the backend's token list accepts."""
    import re
    import sys
    sys.path.insert(0, str(ROOT))
    import core.analysis.portfolio_actions as pa
    gas = (ROOT / "apps_script" / "10_My_Portfolio.gs").read_text(encoding="utf-8")
    sent = set(re.findall(r"obj\['(Dividends[^']*)'\] = ", gas))
    assert sent == {"Dividends Recv"}
    assert re.sub(r"[^a-z0-9]", "", "Dividends Recv".lower()) in pa._DIV_RECV_TOKENS
    assert pa._dividends_from_row({"Dividends Recv": 560.0}) == 560.0
    ledger = (ROOT / "apps_script" / "21_Portfolio_Ledger.gs").read_text(encoding="utf-8")
    assert "'Dividends Recv'" in ledger and "hName === 'Dividends Recv'" in gas       # the ledger's own header
