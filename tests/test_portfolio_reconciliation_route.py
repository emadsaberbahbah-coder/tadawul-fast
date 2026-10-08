"""Actual authenticated route transports private evidence and renders safe blocks."""
import copy
import json

import pytest

pytest.importorskip("fastapi")
from fastapi import FastAPI
from fastapi.testclient import TestClient

from core import config
from core.analysis import portfolio_actions as pa
from routes import advanced_analysis as route
from tests.test_portfolio_action_reconciliation import holding, packet

PATH = "/sheet-rows/portfolio-actions"


@pytest.fixture
def client(monkeypatch):
    for name in ("TFB_OPEN_MODE", "OPEN_MODE", "APP_OPEN_MODE", "REQUIRE_AUTH", "TFB_REQUIRE_AUTH", "APP_REQUIRE_AUTH",
                 "APP_TOKEN", "TFB_APP_TOKEN", "BACKEND_TOKEN", "BACKUP_APP_TOKEN", "ALLOWED_TOKENS", "TFB_ALLOWED_TOKENS", "APP_TOKENS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("APP_TOKEN", "synthetic-portfolio-token")
    monkeypatch.setenv("OPEN_MODE", "0")
    monkeypatch.setenv("REQUIRE_AUTH", "1")
    monkeypatch.setenv("TFB_PF_ENABLED", "1")
    monkeypatch.setenv("TFB_EXIT_BY_RULE_GATE", "0")
    monkeypatch.setenv("TFB_PF_ADD_CONFIRM_DAYS", "0")
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", "0")
    monkeypatch.setattr(route, "_portfolio_build_offloop_enabled", lambda: False)
    monkeypatch.setattr(route, "_build_portfolio_actions", pa.build_portfolio_actions)
    async def health(*_args, **_kwargs):
        return {"unavailable": True, "reason": "synthetic-test"}
    monkeypatch.setattr(route, "_provider_health_with_budget", health)
    config._SETTINGS_CACHE.clear()
    app = FastAPI()
    app.include_router(route.router)
    with TestClient(app) as mounted:
        yield mounted
    config._SETTINGS_CACHE.clear()


def body():
    rows = [holding()]
    return {"rows": rows, "controls": {"Cash Available (SAR)": 10_000, "target_cash_pct": 10,
                                        "max_position_pct": 20, "max_sector_pct": 30, "trust_gate_enabled": False},
            "fx_rates": {"SAR": 1}, "reconciliation_evidence": packet(rows)}


def post(client, request):
    response = client.post(PATH, json=request, headers={"X-APP-TOKEN": "synthetic-portfolio-token"})
    assert response.status_code == 200
    return response.json()


def test_private_capture_passes_actual_authenticated_transport_and_is_not_returned(client):
    request = body()
    result = post(client, request)
    assert result["meta"]["execution_ready"]
    assert result["kpis"]["adds_funded_sar"] > 0
    assert result["meta"]["route"]["holdings"]["received"] == 1
    for secret in ("synthetic-account", "synthetic://declared-capture", "reconciliation_evidence", "reservations_complete"):
        assert secret not in json.dumps(result)


def test_route_remains_protected_even_when_evidence_is_complete(client):
    assert client.post(PATH, json=body()).status_code == 401


@pytest.mark.parametrize("mutation", ["missing", "closed", "cash", "malformed"])
def test_actual_transport_missing_or_inconsistent_basis_cannot_refresh_trusted_hold_add(client, mutation):
    request = body()
    if mutation == "missing":
        request.pop("reconciliation_evidence")
    elif mutation == "closed":
        request["reconciliation_evidence"]["accounts"][0]["positions"][0]["quantity"] = 0
    elif mutation == "cash":
        request["controls"]["Cash Available (SAR)"] += 1
    else:
        request["reconciliation_evidence"] = "synthetic-private-invalid"
    result = post(client, request)
    assert result["status"] == "ok" and not result["meta"]["execution_ready"]
    assert result["actions"][0]["action"] == "BLOCK"
    assert result["kpis"]["adds_funded_sar"] == result["kpis"]["deployable_sar"] == 0
    assert result["kpis"]["portfolio_value_sar"] is None


def test_route_cannot_certify_portfolio_after_its_row_cap_or_nonobject_filter(client, monkeypatch):
    request = body()
    request["rows"].append(holding("SECOND.SR"))
    request["reconciliation_evidence"] = packet(request["rows"])
    monkeypatch.setattr(route, "_portfolio_holdings_max", lambda: 1)
    result = post(client, request)
    assert not result["meta"]["execution_ready"]
    assert result["meta"]["route"]["holdings"]["truncated"]
    assert result["kpis"]["adds_funded_sar"] == 0


def test_offloop_builder_uses_same_private_evidence_and_protective_contract(client, monkeypatch):
    monkeypatch.setattr(route, "_portfolio_build_offloop_enabled", lambda: True)
    result = post(client, body())
    assert result["meta"]["execution_ready"] and result["meta"]["route"]["builder_offloop"]
