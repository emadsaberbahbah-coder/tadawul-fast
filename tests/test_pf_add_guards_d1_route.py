#!/usr/bin/env python3
"""advanced_analysis 4.19.0: the portfolio route forwards body.post_earnings.

Split from tests/test_pf_add_guards_d1.py so it runs in the schema and route
contract lane (production-pinned FastAPI / Starlette / httpx); the isolated
protective lane that runs the builder cases has no FastAPI. Synthetic data;
the builder is a capturing stub, so only the route's forwarding is under test.
portfolio_actions 1.17.1 adds one full-path case: the real route and the real
builder turn a malformed "sessions": "4" entry into a HOLD under enforce.
"""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from routes import advanced_analysis as advanced


def _calendar():
    asof = (datetime.now(timezone.utc) + timedelta(hours=3)).date().isoformat()
    return {"schema": "tfb.pd-post-earnings/v1", "status": "ok", "asof": asof,
            "updated": "2026-10-27", "reports": 37, "sessions": 5, "sessions_neg": 10,
            "map": {"EARN.US": {"date": "2026-10-22", "sessions": 4, "surprise": -22.2}}}


def test_route_forwards_only_a_mapping(monkeypatch):
    seen = []

    def fake_builder(holdings, controls=None, fx_rates=None, upstream_meta=None):
        seen.append(copy.deepcopy(upstream_meta))
        return {"version": "x", "status": "ok", "kpis": {}, "actions": [],
                "sector_summary": [], "alerts": [], "meta": {}}

    async def fake_health(*_args, **_kwargs):
        return {"unavailable": True, "reason": "synthetic-test"}

    monkeypatch.setattr(advanced, "_auth_passed", lambda **_kwargs: True)
    monkeypatch.setattr(advanced, "_portfolio_build_offloop_enabled", lambda: False)
    monkeypatch.setattr(advanced, "_provider_health_with_budget", fake_health)
    monkeypatch.setattr(advanced, "_build_portfolio_actions", fake_builder)
    assert tuple(map(int, advanced.ADVANCED_ANALYSIS_VERSION.split("."))) >= (4, 19, 0)
    app = FastAPI()
    app.include_router(advanced.router)
    body = {"rows": [{"Symbol": "EARN.US", "Position Qty": 1}], "controls": {}, "fx_rates": {}}
    with TestClient(app) as client:
        for extra in ({}, {"post_earnings": _calendar()},
                      {"post_earnings": ["not", "a", "mapping"]}, {"post_earnings": None}):
            response = client.post("/sheet-rows/portfolio-actions", json={**body, **extra})
            assert response.status_code == 200, response.text
    assert len(seen) == 4
    assert "post_earnings" not in seen[0]
    assert seen[1]["post_earnings"] == _calendar()
    assert "post_earnings" not in seen[2] and "post_earnings" not in seen[3]
    for meta in seen:
        assert "reconciliation_evidence" in meta and "versions" in meta


# v1.17.1 (portfolio_actions): the complete input-to-action path - the real
# route and the real builder, synthetic certified evidence. Owner readiness
# review (28 Oct): "sessions": "4" was dropped and the ADD passed.
def _frst_row():
    from tests.portfolio_reconciliation_fixtures import synthetic_quote_receipt
    return {**synthetic_quote_receipt(),
            "Symbol": "FRST.US", "Name": "Synthetic Equity", "Currency": "USD",
            "Exchange": "NYSE/NASDAQ", "Sector": "Financials", "Asset Class": "Equity",
            "Position Qty": 20, "Avg Cost": 45, "Current Price": 50, "Target Price": 100,
            "Expected ROI 12M": 25, "Forecast Reliability Score": 85, "Data Quality Score": 100,
            "Risk Bucket": "Low", "Investability Status": "INVESTABLE", "Recommendation": "BUY",
            "Volatility 30D": 0.2, "Forecast Source": "provider_target", "Buy Date": "2026-01-01"}


def test_full_path_string_sessions_holds_and_absent_symbol_adds(monkeypatch):
    from core.analysis import portfolio_actions as pa
    from tests.portfolio_reconciliation_fixtures import synthetic_reconciliation_evidence

    for key, value in {"TFB_PF_ENABLED": "1", "TFB_EXIT_BY_RULE_GATE": "0",
                       "TFB_PF_CONFIRM_PERSIST": "0", "TFB_PF_ADD_CONFIRM_DAYS": "0",
                       "TFB_PF_CONFIRM_SESSION": "off"}.items():
        monkeypatch.setenv(key, value)
    for key in ("TFB_PF_ADD_LOSER_VETO", "TFB_PF_MAX_OPEN_TICKETS_MODE", "TFB_PF_DD_EXIT",
                "TFB_PF_POST_EARN_SESSIONS", "TFB_PF_POST_EARN_SESSIONS_NEG"):
        monkeypatch.delenv(key, raising=False)

    async def fake_health(*_args, **_kwargs):
        return {"unavailable": True, "reason": "synthetic-test"}

    monkeypatch.setattr(advanced, "_auth_passed", lambda **_kwargs: True)
    monkeypatch.setattr(advanced, "_portfolio_build_offloop_enabled", lambda: False)
    monkeypatch.setattr(advanced, "_provider_health_with_budget", fake_health)
    monkeypatch.setattr(advanced, "_build_portfolio_actions", pa.build_portfolio_actions)
    controls = {"cash_available_sar": 50000, "target_cash_pct": 10, "max_position_pct": 40,
                "max_sector_pct": 60, "min_reliability_add": 70, "min_dq_add": 80,
                "rebalance_mode": "Advisory", "add_confirm_days": 0}
    fx = {"USD": 4, "SAR": 1}
    app = FastAPI()
    app.include_router(advanced.router)

    def run(mode, entries):
        if mode is None:
            monkeypatch.delenv("TFB_PF_ADD_POST_EARN", raising=False)
        else:
            monkeypatch.setenv("TFB_PF_ADD_POST_EARN", mode)
        pa._ADD_CONFIRM_STORE.clear()
        rows = [_frst_row()]
        body = {"rows": rows, "controls": controls, "fx_rates": fx,
                "reconciliation_evidence": synthetic_reconciliation_evidence(
                    rows, cash_available_sar=50000, fx_rates=fx)}
        if entries is not None:
            calendar = _calendar()
            calendar["map"] = entries
            body["post_earnings"] = calendar
        with TestClient(app) as client:
            response = client.post("/sheet-rows/portfolio-actions", json=body)
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["status"] == "ok" and payload["meta"]["execution_ready"], payload["meta"]
        assert "post_earnings" not in str(payload["meta"].get("upstream"))
        return payload["actions"][0]

    text = {"FRST.US": {"date": "2026-10-22", "sessions": "4", "surprise": -22.2}}
    numeric = {"FRST.US": {"date": "2026-10-22", "sessions": 4, "surprise": -22.2}}
    assert run(None, text)["action"] == "ADD"                     # off: unchanged
    held = run("enforce", text)
    assert held["action"] == "HOLD" and held["action_reason"].startswith(
        "ADD held [POST-EARN]: earnings entry for FRST.US invalid (sessions '4') - the "
        "post-earnings window cannot be verified (was ADD: ")
    held = run("enforce", numeric)
    assert held["action"] == "HOLD" and held["action_reason"].startswith(
        "ADD held [POST-EARN]: reported 2026-10-22, 4/10 sessions since the report "
        "(EPS -22.2%)")
    assert run("enforce", {})["action"] == "ADD"                  # FRST absent -> clean
    observed = run("observe", text)
    assert observed["action"] == "ADD" and "[postearn-observe] earnings entry for FRST.US " \
        "invalid (sessions '4') - would HOLD under enforce" in observed["action_reason"]
