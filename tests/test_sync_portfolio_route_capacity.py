"""tests/test_sync_portfolio_route_capacity.py - run_dashboard_sync v6.73.0 (2026-11-18).

External audit of 17 Nov, confirmed on run 44072218739: at 14:19 Riyadh the backend timed out on
/v1/enriched/sheet-rows for My_Portfolio, the chain fell through to /v1/advanced/sheet-rows, and the page was
written with 0 of 9 holdings priced under a SUCCESS stamp. The healthy 22:24 run used the enrichment route.
  P1 My_Portfolio under 'safe' (default) asks the enrichment route only
  P2 'legacy' and every other page keep the unchanged chain
  P3 a portfolio task whose one route fails is 'failed' and says the prior rows stay
  C1 the grid capacity status counts against 20,000,000 cells (Google's cap since 10 Sep 2026)
  C2 TFB_SYNC_CAPACITY_LIMIT overrides; garbage or < 1M falls back to 20M
  C3 the tracker's own capacity default is 20M too
  V  version pins

Run: pytest -q tests/test_sync_portfolio_route_capacity.py
"""
from __future__ import annotations

import inspect
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts import run_dashboard_sync as sync  # noqa: E402


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in ("TFB_SYNC_PORTFOLIO_ROUTES", "TFB_SYNC_CAPACITY_LIMIT"):
        monkeypatch.delenv(name, raising=False)


def task(name):
    return SimpleNamespace(sheet_name=name)


def test_p1_portfolio_asks_the_enrichment_route_only(monkeypatch):
    assert sync._portfolio_routes_mode() == "safe"
    assert sync._task_endpoint_candidates(task("My_Portfolio"), "enriched") == ["/v1/enriched/sheet-rows"]
    assert sync._task_endpoint_candidates(task("my_portfolio"), "advanced") == ["/v1/enriched/sheet-rows"]
    for word in ("", "safe", "SAFE", "on", "nonsense"):
        monkeypatch.setenv("TFB_SYNC_PORTFOLIO_ROUTES", word)
        assert sync._task_endpoint_candidates(task("My_Portfolio"), "enriched") == ["/v1/enriched/sheet-rows"], word


def test_p2_legacy_and_other_pages_unchanged(monkeypatch):
    full = sync._endpoint_candidates_for_gateway("enriched")
    assert full[0] == "/v1/enriched/sheet-rows" and "/v1/advanced/sheet-rows" in full
    for page in ("Market_Leaders", "Global_Markets", "Top_10_Investments", "Insights_Analysis"):
        for gw in ("enriched", "analysis", "advanced"):
            assert sync._task_endpoint_candidates(task(page), gw) == sync._endpoint_candidates_for_gateway(gw)
    monkeypatch.setenv("TFB_SYNC_PORTFOLIO_ROUTES", " Legacy ")
    assert sync._task_endpoint_candidates(task("My_Portfolio"), "enriched") == full


def test_p3_the_loop_uses_the_task_chain_and_a_failed_portfolio_says_so():
    src = inspect.getsource(sync)
    assert "_task_endpoint_candidates(task, eff_gw)" in src
    assert "for ep in ([] if _use_batching else _endpoint_candidates_for_gateway(eff_gw))" not in src
    block = src[src.index("        if not headers:\n            res.status = \"failed\""):][:900]
    assert "no other route is asked for the portfolio, so the prior" in block
    assert "_portfolio_routes_mode() == \"safe\"" in block and "return res" in block


def test_c1_capacity_counts_against_20_million():
    assert sync._capacity_limit() == 20_000_000
    state, pct = sync._capacity_state(8_800_000)                 # the 17 Nov reading: 44%, not 88%
    assert state == "OK" and round(pct, 1) == 44.0
    assert "free=11,200,000" in sync._capacity_value(8_800_000, "1", "t")
    assert sync._capacity_state(None) == ("UNKNOWN", None)


def test_c2_capacity_override(monkeypatch):
    for raw, want in (("10000000", 10_000_000), ("abc", 20_000_000), ("500", 20_000_000), ("", 20_000_000)):
        monkeypatch.setenv("TFB_SYNC_CAPACITY_LIMIT", raw)
        assert sync._capacity_limit() == want, raw
    monkeypatch.setenv("TFB_SYNC_CAPACITY_LIMIT", "10000000")
    assert sync._capacity_state(8_800_000)[0] != "OK"           # the old cap still reads near/at limit


def test_c3_tracker_capacity_default():
    src = (_REPO / "scripts" / "track_performance.py").read_text(encoding="utf-8")
    body = src[src.index("def _capacity_limit"):][:900]
    assert "20000000" in body or "20_000_000" in body
    assert "12000000" not in body and "12_000_000" not in body


def test_v_version_pins():
    assert sync.SCRIPT_VERSION == "6.73.0"
    head = (_REPO / "scripts" / "run_dashboard_sync.py").read_text(encoding="utf-8")[:4000]
    assert "DASHBOARD SYNC RUNNER (v6.73.0)" in head and "v6.73.0 - My_Portfolio is served by the enrichment" in head
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("run_dashboard_sync", "6.73.0", "dashboard sync")' in vd
    assert '("track_performance", "6.55.1", "track performance")' in vd
    assert re.search(r'^SCRIPT_VERSION = "6\.55\.1"', (_REPO / "scripts" / "track_performance.py")
                     .read_text(encoding="utf-8"), re.M)
