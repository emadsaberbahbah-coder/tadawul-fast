"""10_My_Portfolio v1.5.22 [TOTAL-RETURN] (Portfolio_Decision's P&L SAR / P&L % cells and tiles show portfolio_actions
1.20.0's total - the price change plus the dividends received - when the backend sends it): run the native harness
(the REAL Apps Script source in a vm; the v1.5.21 base rebuilt from tests/js/pd_edits_v1522.json, SHA-256 pinned) so
the whole-suite CI job collects it. Mirrors tests/test_pd_v1521_div_basis.py."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "test_pd_v1522_total_return.js"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def test_pd_v1522_total_return_native_harness():
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, str(HARNESS)], cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.rstrip().endswith("PASS 6/6"), result.stdout[-2000:]
    assert "mutants killed 8/8" in result.stdout, result.stdout[-2000:]


def test_the_page_reads_the_fields_the_backend_sends():
    """The keys this file renders are the keys portfolio_actions 1.20.0 emits, on a row and in the KPIs."""
    import re
    import sys
    sys.path.insert(0, str(ROOT))
    import core.analysis.portfolio_actions as pa
    gas = (ROOT / "apps_script" / "10_My_Portfolio.gs").read_text(encoding="utf-8")
    read = set(re.findall(r"\b[ak]\.(total_pnl_sar|total_pnl_pct|dividends_sar)\b", gas))
    assert read == {"total_pnl_sar", "total_pnl_pct", "dividends_sar"}
    row = pa._total_return_fields({"pnl_sar": -100.0, "cost_sar": 1000.0, "dd_div_sar": 80.0})
    kpi = pa._total_return_kpis([{"pnl_sar": -100.0, "cost_sar": 1000.0, "dd_div_sar": 80.0}])
    assert set(row) == {"total_pnl_sar", "total_pnl_pct", "dividends_sar"} and set(kpi) == {"total_pnl_sar", "total_pnl_pct"}
