"""The mounted opportunity endpoint preserves the two-pass board contract."""

from __future__ import annotations

import copy
import os
import json
import subprocess

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from routes import advanced_analysis as advanced


def _row(symbol, roi):
    return {
        "symbol": symbol, "name": "Synthetic " + symbol,
        "sector": "Energy", "market": "Tadawul", "currency": "SAR",
        "current_price": 100.0, "intrinsic_value": 130.0,
        "forecast_reliability_score": 82.0, "data_quality_score": 91.0,
        "risk_bucket": "Moderate", "provider_engine_conflict": "No",
        "volatility_30d": 4.0, "avg_volume_30d": 2_500_000,
        "expected_roi_12m": roi, "recommendation_detailed": "STRONG BUY",
        "investability_status": "INVESTABLE", "block_reason": "",
    }


@pytest.fixture
def mounted_board_route(monkeypatch):
    for key in list(os.environ):
        if key.startswith("TFB_OPP_") or key.startswith("TFB_T10_"):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("APP_TOKEN", "synthetic-board-test")
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    monkeypatch.setattr(advanced, "_auth_passed", lambda **_kwargs: True)
    monkeypatch.setattr(advanced, "_opp_build_offloop_enabled", lambda: False)
    monkeypatch.setattr(advanced, "_news_display_enabled", lambda: True)
    calls = {"trends": 0, "health": 0, "news": 0, "selector": 0}

    def _trends(rows):
        calls["trends"] += 1
        return copy.deepcopy(rows), {"enabled": False, "bound": True}

    async def _health(_timeout_s):
        calls["health"] += 1
        return {"unavailable": True, "reason": "synthetic-test"}

    async def _pool(**_kwargs):
        calls["selector"] += 1
        raise AssertionError("allocation must never recollect the selector pool")

    async def _news(_payload, *, timeout_s):
        assert timeout_s > 0
        calls["news"] += 1
        return {"enabled": True, "reason": "synthetic-test"}

    async def _lease():
        return {"acquired": True, "reason": "synthetic-test"}

    async def _release(_lease):
        return None

    monkeypatch.setattr(advanced, "_enrich_rows_with_trends", _trends)
    monkeypatch.setattr(advanced, "_provider_health_with_budget", _health)
    monkeypatch.setattr(advanced, "_opp_collect_pool", _pool)
    monkeypatch.setattr(advanced, "_attach_news_display", _news)
    monkeypatch.setattr(advanced, "_opp_acquire_build_lease", _lease)
    monkeypatch.setattr(advanced, "_opp_release_build_lease", _release)

    app = FastAPI()
    app.include_router(advanced.router)
    with TestClient(app) as client:
        yield client, calls


def _research_body():
    return {
        "rows": [_row("HIGH.SR", 24.0), _row("LATER.SR", 20.0)],
        "criteria": {
            "Board Funding Stage": "research",
            "max_selected": 2, "max_per_sector": 1,
            "max_per_market": 10, "max_weight_pct": 100.0,
            "pf_max_sector_pct": 100.0, "min_ticket_sar": 5_000.0,
            "rank_by_engine_roi_enabled": True,
            "trust_gate_enabled": False,
        },
        "portfolio": {"cash_available_sar": 10_000.0},
        "fx_rates": {"SAR": 1.0},
    }


def test_normalized_criteria_preserve_snapshot_dict_and_symbol_list():
    frozen = {"rows": [{"symbol": "LATER.SR"}], "snapshot_id": "synthetic-signature"}
    criteria = advanced._opp_criteria_from_body({
        "Board Funding Stage": "allocate",
        "Board Funding Symbols": ["LATER.SR"],
        "Board Funding Snapshot": frozen,
    })
    assert criteria["board_funding_stage"] == "allocate"
    assert criteria["board_funding_symbols"] == ["LATER.SR"]
    assert criteria["board_funding_snapshot"] == frozen


def test_mounted_route_round_trip_replays_snapshot_without_provider_refresh(mounted_board_route):
    client, calls = mounted_board_route
    body = _research_body()
    first = client.post("/sheet-rows/opportunity-candidates", json=body)
    assert first.status_code == 200
    research = first.json()
    assert research["status"] == "ok", research.get("message")
    frozen = research["meta"]["board_funding"]["snapshot"]
    assert calls == {"trends": 1, "health": 1, "news": 1, "selector": 0}

    body["rows"] = frozen["rows"]
    body["criteria"]["Board Funding Stage"] = "allocate"
    body["criteria"]["Board Funding Symbols"] = ["LATER.SR"]
    body["criteria"]["Board Funding Snapshot"] = frozen
    second = client.post("/sheet-rows/opportunity-candidates", json=body)
    assert second.status_code == 200
    allocated = second.json()
    assert allocated["status"] == "ok", allocated.get("message")
    assert [ticket["symbol"] for ticket in allocated["selected"]] == ["LATER.SR"]
    assert allocated["selected"][0]["suggested_sar"] == 10_000.0
    assert allocated["meta"]["board_funding"]["snapshot_id"] == frozen["snapshot_id"]
    assert calls == {"trends": 1, "health": 1, "news": 1, "selector": 0}


def test_real_node_json_round_trip_preserves_signed_snapshot_and_rejects_tampering(
    mounted_board_route,
):
    client, calls = mounted_board_route
    body = _research_body()
    body["rows"][0]["synthetic_numeric_metadata"] = {
        "values": [100.0, -0.0, 1e-7, 1.0000000000000002],
        "nested": {"value": 1e20},
    }
    first = client.post("/sheet-rows/opportunity-candidates", json=body).json()
    frozen = first["meta"]["board_funding"]["snapshot"]
    body["rows"] = frozen["rows"]
    body["criteria"].update({"Board Funding Stage": "allocate",
        "Board Funding Symbols": ["LATER.SR"], "Board Funding Snapshot": frozen})
    # Execute the actual JS transport used by GAS, then submit its JSON bytes.
    # Python->Python JSON would conceal integral float/negative-zero drift.
    transported = subprocess.run(["node", "-e",
        "let s='';process.stdin.on('data',x=>s+=x);"
        "process.stdin.on('end',()=>process.stdout.write(JSON.stringify(JSON.parse(s))));"],
        input=json.dumps(body), text=True, capture_output=True, check=True).stdout
    replay = client.post("/sheet-rows/opportunity-candidates", content=transported,
        headers={"Content-Type": "application/json"}).json()
    assert replay["status"] == "ok", replay.get("message")
    assert [t["symbol"] for t in replay["selected"]] == ["LATER.SR"]
    assert calls == {"trends": 1, "health": 1, "news": 1, "selector": 0}
    tampered = json.loads(transported)
    tampered["criteria"]["Board Funding Snapshot"]["rows"][0]["synthetic_numeric_metadata"]["values"][0] = 101
    rejected = client.post("/sheet-rows/opportunity-candidates", json=tampered).json()
    assert rejected["status"] == "board_funding_mismatch"
    assert rejected["selected"] == []


def test_backend_selector_research_uses_frozen_rows_without_second_collection(
    mounted_board_route, monkeypatch,
):
    client, calls = mounted_board_route
    body = _research_body()
    rows = body.pop("rows")
    async def collect(**_kwargs):
        calls["selector"] += 1
        return copy.deepcopy(rows), {"coverage": "synthetic"}, "synthetic_selector"
    monkeypatch.setattr(advanced, "_opp_collect_pool", collect)
    first = client.post("/sheet-rows/opportunity-candidates", json=body).json()
    frozen = first["meta"]["board_funding"]["snapshot"]
    body["rows"] = frozen["rows"]
    body["criteria"].update({"Board Funding Stage": "allocate",
        "Board Funding Symbols": ["LATER.SR"], "Board Funding Snapshot": frozen})
    second = client.post("/sheet-rows/opportunity-candidates", json=body).json()
    assert second["status"] == "ok", second.get("message")
    assert [t["symbol"] for t in second["selected"]] == ["LATER.SR"]
    assert calls == {"trends": 1, "health": 1, "news": 1, "selector": 1}


@pytest.mark.parametrize("rows", [None, []])
def test_allocation_without_explicit_snapshot_rows_never_falls_back_to_selector(
    mounted_board_route, rows
):
    client, calls = mounted_board_route
    body = _research_body()
    body["criteria"]["Board Funding Stage"] = "allocate"
    body["criteria"]["Board Funding Symbols"] = ["LATER.SR"]
    body["criteria"]["Board Funding Snapshot"] = {}
    if rows is None:
        body.pop("rows")
    else:
        body["rows"] = rows
    response = client.post("/sheet-rows/opportunity-candidates", json=body)
    assert response.status_code == 200
    payload = response.json()
    if rows is None:
        assert payload["status"] == "degraded"
        assert payload["error"] == "board_funding_mismatch"
    else:
        assert payload["status"] == "board_funding_mismatch"
    assert payload["selected"] == []
    assert calls == {"trends": 0, "health": 0, "news": 0, "selector": 0}
