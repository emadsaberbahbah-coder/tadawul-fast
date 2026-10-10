"""The mounted opportunity endpoint preserves the two-pass board contract."""

from __future__ import annotations

import copy
import os
import json
import subprocess
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from core import config as core_config
from core.analysis import opportunity_builder as ob
from routes import advanced_analysis as advanced
from scripts import verify_core_repair_readback as probe


REAL_ROUTE_AUTH = advanced._auth_passed
AUTH_ENV_KEYS = (
    "APP_TOKEN", "TFB_APP_TOKEN", "BACKEND_TOKEN", "BACKUP_APP_TOKEN",
    "ALLOWED_TOKENS", "TFB_ALLOWED_TOKENS", "APP_TOKENS",
    "X_APP_TOKEN", "API_KEY", "TFB_TOKEN",
)
POLICY_ENV_KEYS = (
    "TFB_FORECAST_BASIS", "TFB_TICKET_FRESHNESS_GATE",
    "TFB_TICKET_MAX_QUOTE_AGE_MIN", "TFB_TICKET_FALLBACK_MAX_AGE_H",
    "TFB_COMPLIANCE_SURFACE_GATE", "TFB_ELIGIBILITY_GATE",
    "TFB_GLOBAL_ACTIVITY_SCREEN", "TFB_SHARIAH_FAIL_LIST",
    "TFB_EXIT_BY_RULE_EXTRA", "TFB_KSA_FOREIGN_RESTRICTED",
)
OPPORTUNITY_PATH = "/sheet-rows/opportunity-candidates"


def _row(symbol, roi):
    from decision_evidence_fixtures import observed_price_fields
    return {
        **observed_price_fields(),
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
    for key in AUTH_ENV_KEYS + POLICY_ENV_KEYS:
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

    async def _health(_timeout_s=None):
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


@pytest.fixture
def real_authenticated_board_route(mounted_board_route, monkeypatch):
    """Restore mounted route auth and its real shared config resolver.

    Ordinary transport tests deliberately bypass auth; these tests must not.
    Clear the settings cache so prior tests cannot silently leave open mode
    or a different REQUIRE_AUTH value active.
    """
    assert advanced.auth_ok is core_config.auth_ok
    assert advanced.is_open_mode is core_config.is_open_mode
    for key in AUTH_ENV_KEYS + (
        "TFB_OPEN_MODE", "OPEN_MODE", "APP_OPEN_MODE", "TFB_REQUIRE_AUTH",
        "REQUIRE_AUTH", "APP_REQUIRE_AUTH",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("OPEN_MODE", "0")
    monkeypatch.setenv("REQUIRE_AUTH", "1")
    monkeypatch.setattr(advanced, "_auth_passed", REAL_ROUTE_AUTH)
    core_config._SETTINGS_CACHE.clear()
    yield mounted_board_route
    core_config._SETTINGS_CACHE.clear()


def _research_body():
    from decision_evidence_fixtures import observed_portfolio
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
        "portfolio": observed_portfolio({"cash_available_sar": 10_000.0}, {"SAR": 1.0}),
        "fx_rates": {"SAR": 1.0},
    }


def _allocation_body(body, research, symbols=("LATER.SR",)):
    result = copy.deepcopy(body)
    frozen = research["meta"]["board_funding"]["snapshot"]
    result["rows"] = copy.deepcopy(frozen["rows"])
    result["criteria"].update({
        "Board Funding Stage": "allocate",
        "Board Funding Symbols": list(symbols),
        "Board Funding Snapshot": copy.deepcopy(frozen),
    })
    return result


def _assert_rejected_before_money(payload):
    assert payload["status"] == "board_funding_mismatch"
    assert payload["selected"] == []
    for key in ("expected_gain_12m_sar", "selected_count", "fundable_now",
                "fundable_by_rotation", "capital_call", "capital_call_topn_sar"):
        assert payload["kpis"].get(key, 0) == 0
    assert not {"capital_call", "rotation_proposal", "unfunded_candidates"} & {
        alert["type"] for alert in payload["alerts"]
    }


@pytest.mark.parametrize("config_name", AUTH_ENV_KEYS[:7])
def test_current_shared_auth_configurations_round_trip_at_real_mounted_route(
    real_authenticated_board_route, monkeypatch, config_name,
):
    client, _calls = real_authenticated_board_route
    token = "accepted-route-token"
    configured = (token + ",accepted-route-backup"
                  if config_name.endswith("TOKENS") else token)
    monkeypatch.setenv(config_name, configured)
    core_config._SETTINGS_CACHE.clear()
    headers = {"X-APP-TOKEN": token}
    body = _research_body()
    assert client.post(OPPORTUNITY_PATH, json=body).status_code == 401
    assert client.post(OPPORTUNITY_PATH, json=body,
        headers={"X-APP-TOKEN": "wrong-route-token"}).status_code == 401

    ordinary_body = copy.deepcopy(body)
    ordinary_body["criteria"].pop("Board Funding Stage")
    ordinary_response = client.post(OPPORTUNITY_PATH, json=ordinary_body, headers=headers)
    assert ordinary_response.status_code == 200
    ordinary = ordinary_response.json()
    assert ordinary["status"] == "ok", ordinary.get("message")
    assert ordinary["selected"][0]["suggested_sar"] == 10_000.0
    assert "board_funding" not in ordinary["meta"]

    response = client.post(OPPORTUNITY_PATH, json=body, headers=headers)
    assert response.status_code == 200
    research = response.json()
    assert research["status"] == "ok", research.get("message")
    assert research["meta"]["board_funding"]["snapshot_available"] is True
    replay_response = client.post(OPPORTUNITY_PATH,
        json=_allocation_body(body, research), headers=headers)
    assert replay_response.status_code == 200
    replay = replay_response.json()
    assert replay["status"] == "ok", replay.get("message")
    assert [ticket["symbol"] for ticket in replay["selected"]] == ["LATER.SR"]
    assert replay["selected"][0]["suggested_sar"] == 10_000.0
    serialized = json.dumps([ordinary, research, replay])
    assert token not in serialized
    assert "accepted-route-backup" not in serialized


@pytest.mark.parametrize("alias", ("X_APP_TOKEN", "API_KEY", "TFB_TOKEN"))
def test_generic_only_aliases_do_not_bypass_real_route_auth_or_mint_snapshot(
    real_authenticated_board_route, monkeypatch, alias,
):
    client, calls = real_authenticated_board_route
    token = "generic-alias-only-token"
    monkeypatch.setenv(alias, token)
    core_config._SETTINGS_CACHE.clear()
    assert core_config.allowed_tokens() == []
    body = _research_body()
    ordinary_body = copy.deepcopy(body)
    ordinary_body["criteria"].pop("Board Funding Stage")
    for headers in ({"X-APP-TOKEN": token}, {"X-API-KEY": token},
                    {"Authorization": "Bearer " + token}):
        assert client.post(OPPORTUNITY_PATH, json=body, headers=headers).status_code == 401
        assert client.post(OPPORTUNITY_PATH, json=ordinary_body,
            headers=headers).status_code == 401
    assert calls == {"trends": 0, "health": 0, "news": 0, "selector": 0}
    direct = ob.build_opportunity_payload(copy.deepcopy(body["rows"]),
        criteria=advanced._opp_criteria_from_body(body["criteria"]),
        portfolio=body["portfolio"], fx_rates=body["fx_rates"])
    funding = direct["meta"]["board_funding"]
    assert funding["snapshot_available"] is False
    assert "key unavailable" in funding["reason"]
    assert not funding.get("snapshot")
    assert token not in json.dumps(direct)


def test_real_route_allowed_override_rotation_invalidates_existing_snapshot(
    real_authenticated_board_route, monkeypatch,
):
    client, _calls = real_authenticated_board_route
    monkeypatch.setenv("APP_TOKEN", "inactive-route-primary")
    monkeypatch.setenv("ALLOWED_TOKENS", "active-route-primary,active-route-backup-v1")
    core_config._SETTINGS_CACHE.clear()
    headers = {"X-APP-TOKEN": "active-route-primary"}
    body = _research_body()
    # The normal route uses the override list, so inactive APP_TOKEN has no
    # authority to authenticate or to become the replay signing basis.
    assert client.post(OPPORTUNITY_PATH, json=body,
        headers={"X-APP-TOKEN": "inactive-route-primary"}).status_code == 401
    research = client.post(OPPORTUNITY_PATH, json=body, headers=headers).json()
    allocation = _allocation_body(body, research)
    monkeypatch.setenv("ALLOWED_TOKENS", "active-route-primary,active-route-backup-v2")
    core_config._SETTINGS_CACHE.clear()

    def forbidden_allocator(*_args, **_kwargs):
        raise AssertionError("active override rotation must reject before allocation")

    monkeypatch.setattr(ob, "_select_and_size", forbidden_allocator)
    response = client.post(OPPORTUNITY_PATH, json=allocation, headers=headers)
    assert response.status_code == 200  # The unchanged active token still authenticates.
    rejected = response.json()
    assert "signature mismatch" in rejected.get("message", "")
    _assert_rejected_before_money(rejected)


def test_mounted_forecast_basis_change_blocks_gain_recalculation_from_old_snapshot(
    mounted_board_route, monkeypatch,
):
    client, _calls = mounted_board_route
    monkeypatch.setenv("TFB_FORECAST_BASIS", "legacy")
    body = _research_body()
    body["rows"] = [_row("LATER.SR", 74.8)]
    body["criteria"]["primary_roi_basis"] = "plan"
    body["criteria"]["period_months"] = 3
    research = client.post(OPPORTUNITY_PATH, json=body).json()
    allocation = _allocation_body(body, research)
    legacy = client.post(OPPORTUNITY_PATH, json=allocation).json()
    assert legacy["status"] == "ok", legacy.get("message")
    assert legacy["kpis"]["expected_gain_12m_sar"] == 7_480.0
    assert legacy["selected"][0]["suggested_sar"] == 10_000.0

    monkeypatch.setenv("TFB_FORECAST_BASIS", "plan3m")
    ordinary_body = copy.deepcopy(body)
    ordinary_body["criteria"].pop("Board Funding Stage")
    changed = client.post(OPPORTUNITY_PATH, json=ordinary_body).json()
    assert changed["status"] == "ok", changed.get("message")
    assert changed["selected"][0]["suggested_sar"] == 10_000.0
    assert changed["kpis"]["expected_gain_12m_sar"] == 1_500.0

    def forbidden_allocator(*_args, **_kwargs):
        raise AssertionError("forecast policy drift must reject before allocation")

    monkeypatch.setattr(ob, "_select_and_size", forbidden_allocator)
    rejected = client.post(OPPORTUNITY_PATH, json=allocation).json()
    assert "basis changed" in rejected.get("message", "")
    _assert_rejected_before_money(rejected)


def test_mounted_admission_list_change_rejects_old_snapshot_before_allocation(
    mounted_board_route, monkeypatch,
):
    client, _calls = mounted_board_route
    monkeypatch.setenv("TFB_EXIT_BY_RULE_EXTRA", "")
    body = _research_body()
    body["rows"] = [_row("LATER.SR", 20.0)]
    research = client.post(OPPORTUNITY_PATH, json=body).json()
    allocation = _allocation_body(body, research)
    baseline = client.post(OPPORTUNITY_PATH, json=allocation).json()
    assert baseline["status"] == "ok", baseline.get("message")
    assert baseline["selected"][0]["suggested_sar"] == 10_000.0

    monkeypatch.setenv("TFB_EXIT_BY_RULE_EXTRA", "LATER.SR")
    ordinary_body = copy.deepcopy(body)
    ordinary_body["criteria"].pop("Board Funding Stage")
    changed = client.post(OPPORTUNITY_PATH, json=ordinary_body).json()
    assert changed["status"] == "ok", changed.get("message")
    assert changed["selected"] == []
    assert changed["kpis"]["passed"] == 0
    assert changed["candidates_rows"][0]["symbol"] == "LATER.SR"
    assert changed["candidates_rows"][0]["verdict"] != "INVEST"

    def forbidden_allocator(*_args, **_kwargs):
        raise AssertionError("admission policy drift must reject before allocation")

    monkeypatch.setattr(ob, "_select_and_size", forbidden_allocator)
    rejected = client.post(OPPORTUNITY_PATH, json=allocation).json()
    assert "basis changed" in rejected.get("message", "")
    _assert_rejected_before_money(rejected)


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


def test_deployment_readback_probe_runs_real_mounted_blocked_snapshot_protocol(
    real_authenticated_board_route, monkeypatch,
):
    from core.analysis import portfolio_actions as pa
    client, calls = real_authenticated_board_route
    monkeypatch.setenv("APP_TOKEN", "synthetic-board-test")
    core_config._SETTINGS_CACHE.clear()
    monkeypatch.setenv("TFB_PF_ENABLED", "1")
    monkeypatch.setattr(advanced, "_portfolio_build_offloop_enabled", lambda: False)
    monkeypatch.setattr(advanced, "_build_portfolio_actions", pa.build_portfolio_actions)
    commit = "synthetic-readback-release"
    monkeypatch.setenv("RENDER_GIT_COMMIT", commit)
    engine = probe.source_version("core/data_engine_v2.py", "__version__")
    health = {"ready": True, "engine_version": engine,
              "deploy": {"render_git_commit": commit},
              "engine_gates": {"margin_publish": "observe"}}
    requests = []
    responses = []

    def forbidden_quote(*_args, **_kwargs):
        raise AssertionError("blocked readback must never fetch an external quote")

    monkeypatch.setattr(ob, "_XCHECK_FETCH_OVERRIDE", forbidden_quote)

    def request(path, body=None, authenticated=False):
        requests.append((path, copy.deepcopy(body), authenticated))
        if path == "/health":
            assert body is None and authenticated is False
            return copy.deepcopy(health)
        assert path in (OPPORTUNITY_PATH, probe.PORTFOLIO_ACTIONS_PATH) and authenticated is True
        response = client.post(path, json=body,
            headers={"X-APP-TOKEN": "synthetic-board-test"})
        assert response.status_code == 200
        payload = response.json()
        responses.append(payload)
        return payload

    result = probe.verify(request, commit, engine, ob.OPPORTUNITY_BUILDER_VERSION)
    assert result["ok"] is True
    assert result["signed_research"] is True
    assert result["empty_allocation"] is True
    assert result["cash_and_fx_changes_rejected"] is True
    assert len(requests) == 7 and len(responses) == 5
    assert result["uncertified_replay_withheld"] is True
    assert result["unreconciled_portfolio_blocked"] is True
    assert result["portfolio_actions_version"] == pa.PORTFOLIO_ACTIONS_VERSION
    protective = responses[-1]
    assert requests[-2][0] == probe.PORTFOLIO_ACTIONS_PATH
    assert "reconciliation_evidence" not in requests[-2][1]
    assert protective["meta"]["execution_ready"] is False
    assert protective["meta"]["input_certification"]["funding_eligible"] is False
    assert protective["actions"][0]["action"] == "BLOCK"
    assert protective["kpis"]["adds_funded_sar"] == 0
    assert protective["kpis"]["portfolio_value_sar"] is None
    assert client.post(probe.PORTFOLIO_ACTIONS_PATH, json=requests[-2][1]).status_code == 401
    body = requests[1][1]
    assert body["rows"][0]["symbol"] == "AAPL.US"
    assert body["rows"][0]["investability_status"] == "BLOCKED"
    assert body["portfolio"] == {"cash_available_sar": 0.0}
    assert body["criteria"] == {"Board Funding Stage": "research"}
    snapshot = responses[0]["meta"]["board_funding"]["snapshot"]
    assert snapshot["rows"] == [] and snapshot["snapshot_id"]
    assert responses[1]["status"] == "no_candidates"
    allocated_board = responses[1]["meta"]["board_funding"]
    assert allocated_board["stage"] == "allocate"
    assert allocated_board["snapshot_available"] is True
    assert requests[2][1]["rows"] == []
    assert requests[2][1]["criteria"]["Board Funding Symbols"] == []
    for rejected in responses[2:4]:
        _assert_rejected_before_money(rejected)
    assert calls == {"trends": 1, "health": 2, "news": 1, "selector": 0}
    assert snapshot["snapshot_id"] not in json.dumps(result)
    assert "synthetic-board-test" not in json.dumps([result, responses])


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
