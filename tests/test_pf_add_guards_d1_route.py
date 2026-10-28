#!/usr/bin/env python3
"""advanced_analysis 4.19.0: the portfolio route forwards body.post_earnings.

Split from tests/test_pf_add_guards_d1.py so it runs in the schema and route
contract lane (production-pinned FastAPI / Starlette / httpx); the isolated
protective lane that runs the builder cases has no FastAPI. Synthetic data;
the builder is a capturing stub, so only the route's forwarding is under test.
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
