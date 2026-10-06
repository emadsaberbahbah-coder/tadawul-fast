"""Regression tests for the opportunity BLOCKED identity invariant.

The decision cockpit can POST sheet rows directly to the opportunity route,
so the builder must enforce exact engine BLOCKED independently of the broad
WATCHLIST/INVESTABLE policy and independently of request criteria.
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.analysis import opportunity_builder as ob
from routes import advanced_analysis as advanced


def _row(investability: str) -> Dict[str, Any]:
    is_blocked = investability.strip().lower() == "blocked"
    return {
        "symbol": "BAD.SR" if is_blocked else "WATCH.SR",
        "name": "Identity Fixture",
        "sector": "Industrials",
        "market": "Tadawul",
        "currency": "SAR",
        "current_price": 100.0,
        "intrinsic_value": 130.0,
        "forecast_reliability_score": 82.0,
        "data_quality_score": 91.0,
        "risk_bucket": "Moderate",
        "provider_engine_conflict": "No",
        "volatility_30d": 4.0,
        "avg_volume_30d": 2_500_000,
        "expected_roi_12m": 24.0,
        "recommendation_detailed": "STRONG BUY",
        "investability_status": investability,
        "block_reason": "identity mismatch" if is_blocked else "",
    }


def _build(investability: str, criteria: Dict[str, Any]) -> Dict[str, Any]:
    return _build_row(_row(investability), criteria)


def _build_row(row: Dict[str, Any], criteria: Dict[str, Any]) -> Dict[str, Any]:
    return ob.build_opportunity_payload(
        [row],
        criteria=criteria,
        portfolio={"cash_available_sar": 50_000},
    )


def test_builder_rejects_blocked_when_env_and_request_try_to_disarm(monkeypatch):
    monkeypatch.setenv("TFB_OPP_BLOCKED_IDENTITY_GATE", "0")
    payload = _build(
        " BlOcKeD ",
        {
            "investability_gate_enabled": False,
            "blocked_identity_gate_enabled": False,
        },
    )

    candidate = payload["candidates_rows"][0]
    assert candidate["engine_gate"]["investability"] == "BlOcKeD"
    assert candidate["verdict"] == "DO_NOT_INVEST"
    assert candidate["first_fail"]["gate"] == "Blocked Identity"
    assert payload["selected"] == []
    assert payload["meta"]["criteria_snapshot"][
        "blocked_identity_gate_enabled"
    ] is True


def test_blocked_dominates_conflicting_and_colliding_aliases(monkeypatch):
    monkeypatch.setenv("TFB_OPP_BLOCKED_IDENTITY_GATE", "0")

    cases = []
    for raw_key in (
        "investability",
        "investability_status",
        "Investability Gate",
        "Gate Status",
    ):
        row = _row("INVESTABLE")
        row["investability"] = "INVESTABLE"
        row[raw_key] = "BLOCKED"
        cases.append(row)

    colliding = _row("INVESTABLE")
    # These two distinct raw keys normalize to the same alias token. The old
    # generic row view retained the first safe value and discarded BLOCKED.
    colliding["Investability Status"] = "B-L-O-C-K-E-D"
    cases.append(colliding)

    for row in cases:
        payload = _build_row(
            row,
            {
                "investability_gate_enabled": False,
                "blocked_identity_gate_enabled": False,
            },
        )
        candidate = payload["candidates_rows"][0]
        assert ob._norm_token(
            candidate["engine_gate"]["investability"]
        ) == "blocked"
        assert candidate["verdict"] == "DO_NOT_INVEST"
        assert candidate["first_fail"]["gate"] == "Blocked Identity"
        assert payload["selected"] == []


def test_watchlist_policy_is_unchanged(monkeypatch):
    monkeypatch.setenv("TFB_OPP_BLOCKED_IDENTITY_GATE", "0")

    broad_gate_off = _build(
        "WATCHLIST",
        {
            "investability_gate_enabled": False,
            "blocked_identity_gate_enabled": False,
        },
    )
    assert broad_gate_off["candidates_rows"][0]["verdict"] == "INVEST"
    assert [ticket["symbol"] for ticket in broad_gate_off["selected"]] == [
        "WATCH.SR"
    ]
    assert "Blocked Identity" not in {
        gate["gate"] for gate in broad_gate_off["candidates_rows"][0]["gates"]
    }

    broad_gate_on = _build(
        "WATCHLIST",
        {"investability_gate_enabled": True},
    )
    candidate = broad_gate_on["candidates_rows"][0]
    assert candidate["verdict"] == "DO_NOT_INVEST"
    assert candidate["first_fail"]["gate"] == "Investability"
    assert broad_gate_on["selected"] == []


def test_explicit_legacy_enable_keeps_nonblocked_pass_trace(monkeypatch):
    monkeypatch.setenv("TFB_OPP_BLOCKED_IDENTITY_GATE", "1")
    env_enabled = _build(
        "WATCHLIST",
        {"investability_gate_enabled": False},
    )
    identity = [
        gate for gate in env_enabled["candidates_rows"][0]["gates"]
        if gate["gate"] == "Blocked Identity"
    ]
    assert len(identity) == 1 and identity[0]["passed"] is True

    request_disabled = _build(
        "WATCHLIST",
        {
            "investability_gate_enabled": False,
            "blocked_identity_gate_enabled": False,
        },
    )
    assert "Blocked Identity" not in {
        gate["gate"] for gate in request_disabled["candidates_rows"][0]["gates"]
    }

    monkeypatch.setenv("TFB_OPP_BLOCKED_IDENTITY_GATE", "0")
    request_enabled = _build(
        "WATCHLIST",
        {
            "investability_gate_enabled": False,
            "blocked_identity_gate_enabled": True,
        },
    )
    identity = [
        gate for gate in request_enabled["candidates_rows"][0]["gates"]
        if gate["gate"] == "Blocked Identity"
    ]
    assert len(identity) == 1 and identity[0]["passed"] is True


def test_mounted_route_cannot_disarm_blocked_invariant(monkeypatch):
    monkeypatch.setenv("TFB_OPP_BLOCKED_IDENTITY_GATE", "0")
    monkeypatch.setattr(advanced, "_auth_passed", lambda **_kwargs: True)
    monkeypatch.setattr(advanced, "_opp_build_offloop_enabled", lambda: False)
    monkeypatch.setattr(advanced, "_news_display_enabled", lambda: False)
    monkeypatch.setattr(advanced, "_enrich_rows_with_trends", None)

    async def _health(_timeout_s: float) -> Dict[str, Any]:
        return {"unavailable": True, "reason": "test"}

    monkeypatch.setattr(advanced, "_provider_health_with_budget", _health)

    route_row = _row("BLOCKED")
    route_row["investability"] = "INVESTABLE"

    app = FastAPI()
    app.include_router(advanced.router)
    with TestClient(app) as client:
        response = client.post(
            "/sheet-rows/opportunity-candidates",
            json={
                "rows": [route_row],
                "criteria": {
                    "investability_gate_enabled": False,
                    "blocked_identity_gate_enabled": False,
                },
                "portfolio": {"cash_available_sar": 50_000},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    candidate = payload["candidates_rows"][0]
    assert candidate["engine_gate"]["investability"] == "BLOCKED"
    assert candidate["verdict"] == "DO_NOT_INVEST"
    assert candidate["first_fail"]["gate"] == "Blocked Identity"
    assert payload["selected"] == []
    assert payload["meta"]["criteria_snapshot"][
        "blocked_identity_gate_enabled"
    ] is True
