"""Objective investment safety at route, normalization and durable boundaries."""
from __future__ import annotations

import copy
import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.analysis import opportunity_builder as ob
from core.analysis import portfolio_actions as pa
from core.analysis import top10_selector as selector
from routes import advanced_analysis as advanced


def row(**changes):
    result = {
        "symbol": "SAFE.US", "name": "Safe Fixture", "sector": "Industrials",
        "market": "US", "currency": "USD", "current_price": 100,
        "intrinsic_value": 130, "forecast_reliability_score": 82,
        "data_quality_score": 91, "risk_bucket": "Moderate",
        "provider_engine_conflict": "No", "volatility_30d": 4,
        "avg_volume_30d": 2_500_000, "expected_roi_12m": 24,
        "recommendation_detailed": "STRONG BUY", "investability_status": "INVESTABLE",
    }
    result.update(changes)
    return result


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(advanced, "_auth_passed", lambda **kwargs: True)
    monkeypatch.setattr(advanced, "_opp_build_offloop_enabled", lambda: False)
    monkeypatch.setattr(advanced, "_news_display_enabled", lambda: False)
    monkeypatch.setattr(advanced, "_enrich_rows_with_trends", None)

    async def health(timeout):
        return {"unavailable": True, "reason": "offline fixture"}

    monkeypatch.setattr(advanced, "_provider_health_with_budget", health)
    app = FastAPI()
    app.include_router(advanced.router)
    with TestClient(app) as test_client:
        yield test_client


def mounted(client, candidate, fx=None):
    response = client.post("/sheet-rows/opportunity-candidates", json={
        "rows": [candidate], "fx_rates": fx or {"USD": 3.75, "SAR": 1},
        "criteria": {"investability_gate_enabled": False},
        "portfolio": {"cash_available_sar": 50_000},
    })
    assert response.status_code == 200
    return response.json()


def test_final_fx_mounted_override_cannot_multiply_quantity(client, monkeypatch):
    monkeypatch.setenv("TFB_OPP_FX_SANITY", "1")
    healthy = mounted(client, row())
    assert healthy["selected"][0]["suggested_shares"] == 20
    unsafe = mounted(client, row(fx_to_sar=0.0375))
    assert unsafe["selected"] == []
    assert unsafe["candidates_rows"][0]["first_fail"]["gate"] == "FX"


@pytest.mark.parametrize("candidate,fx", [
    (row(fx_to_sar=375), {"USD": 3.75}),
    (row(fx_to_sar="Infinity"), {"USD": 3.75}),
    (row(fx_to_sar="NaN"), {"USD": 3.75}),
    (row(currency="SAR", fx_to_sar=0.99), {"SAR": 1}),
    (row(currency="UNKNOWN", fx_to_sar=1), {}),
    (row(), {"USD": 0.0375}),
    (row(), {"SAR/USD": 3.75}),
    (row(fx_to_sar=4.7, currency="EUR"), {"EUR": 4.1}),
    (row(fx_to_sar=3.75, **{"FX To SAR": "NaN"}), {"USD": 3.75}),
])
def test_final_fx_invalid_values_never_fund(candidate, fx):
    result = ob.build_opportunity_payload([candidate], fx_rates=fx,
                                         portfolio={"cash_available_sar": 50_000})
    assert result["selected"] == []
    assert result["candidates_rows"][0]["first_fail"]["gate"] == "FX"


@pytest.mark.parametrize("action", ["DO_NOT_INVEST", "BLOCKED"])
def test_same_row_hard_contradictions_agree_at_mounted_ingress(client, action):
    candidate = row(final_action="INVEST", **{"Final Action": action})
    assert selector._t10_row_hard_excluded(candidate)
    result = mounted(client, candidate)
    assert result["selected"] == []
    assert result["candidates_rows"][0]["verdict"] == "DO_NOT_INVEST"


@pytest.mark.parametrize("subunit,parent,rate", [
    ("GBp", "GBP", 4.75), ("GBX", "GBP", 4.75),
    ("ZAc", "ZAR", 0.2), ("ILA", "ILS", 1.05),
])
def test_subunits_apply_once_raw_and_parent_price(subunit, parent, rate):
    criteria = ob.make_criteria()
    raw = ob.normalize_candidate(row(currency=subunit, current_price=1000),
                                 {parent: rate}, criteria)
    normalized = ob.normalize_candidate(row(currency=parent, current_price=10),
                                        {parent: rate}, criteria)
    assert raw["price_sar"] == pytest.approx(normalized["price_sar"])
    explicit = ob.normalize_candidate(
        row(currency=subunit, current_price=1000, fx_to_sar=rate / 100),
        {parent: rate}, criteria)
    assert explicit["price_sar"] == pytest.approx(normalized["price_sar"])


def test_hard_restriction_blocks_pf_even_when_legacy_precedence_off(monkeypatch):
    monkeypatch.setenv("TFB_PA_PRECEDENCE_GATE", "0")
    candidate = row(currency="SAR", symbol="SAFE.SR", quantity=1,
                    buy_price=100, stop=80, final_action="DO_NOT_INVEST")
    result = pa.build_portfolio_actions([candidate], controls={
        "cash_available_sar": 50_000, "add_confirm_days": 1,
    }, fx_rates={"SAR": 1})
    assert result["actions"][0]["action"] == "HOLD"
    assert "Hard eligibility" in result["actions"][0]["action_reason"]


def test_shadow_research_control_does_not_promote_broad_watchlist_policy():
    candidate = row(investability_status="WATCHLIST", shadow_invest_eligible=False)
    result = ob.build_opportunity_payload([candidate], criteria={
        "investability_gate_enabled": False,
    }, fx_rates={"USD": 3.75}, portfolio={"cash_available_sar": 50_000})
    assert result["candidates_rows"][0]["verdict"] == "INVEST"
    assert [ticket["symbol"] for ticket in result["selected"]] == ["SAFE.US"]


def test_held_stop_not_regenerated_into_add_permission(monkeypatch):
    monkeypatch.setenv("TFB_PF_ADD_LOSER_VETO", "enforce")
    holding = row(currency="SAR", symbol="SAFE.SR", current_price=90,
                  quantity=1, buy_price=90, stop=95)
    controls = {"cash_available_sar": 50_000, "add_confirm_days": 1}
    normalized = pa.normalize_holding(holding, {"SAR": 1}, pa.make_controls(controls))
    assert normalized["stop"] == 95
    result = pa.build_portfolio_actions([holding], controls=controls, fx_rates={"SAR": 1})
    assert result["actions"][0]["action"] != "ADD"
    assert not (result["actions"][0]["suggested_delta_shares"] or 0)
    assert result["actions"][0]["stop_sar"] == 95


def test_unknown_held_stop_withholds_additional_equity_exposure(monkeypatch):
    monkeypatch.setenv("TFB_PF_ADD_LOSER_VETO", "off")
    holding = row(currency="SAR", symbol="SAFE.SR", quantity=1, buy_price=100)
    result = pa.build_portfolio_actions([holding], controls={
        "cash_available_sar": 50_000, "add_confirm_days": 1,
    }, fx_rates={"SAR": 1})
    assert result["actions"][0]["action"] != "ADD"
    assert "risk state" in result["actions"][0]["action_reason"].lower()


def test_held_stop_distance_and_reward_ratio_match_published_stop():
    candidate = row(currency="SAR", symbol="SAFE.SR", current_price=100,
                    quantity=1, buy_price=100, stop=99)
    result = pa.build_portfolio_actions([candidate], controls={
        "cash_available_sar": 50_000, "add_confirm_days": 1,
    }, fx_rates={"SAR": 1})
    action = result["actions"][0]
    assert action["stop_sar"] == 99
    assert action["detail"]["stop_pct"] == 1
    assert action["detail"]["rr"] == 30
    assert action["detail"]["entry_stop_sar"] == 90
    assert action["detail"]["entry_stop_pct"] == 10
    assert action["detail"]["entry_rr"] == 3


def test_calendar_failure_cannot_release_add_or_mutate_clock(monkeypatch):
    monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", "enforce")
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", "0")
    monkeypatch.setenv("TFB_PF_ADD_CONFIRM_LEGACY_FAILOPEN", "0")
    monkeypatch.setattr(pa, "_add_confirm_today", lambda: "2026-10-05")
    state = {"count": 1, "date": "2026-10-04"}
    monkeypatch.setattr(pa, "_ADD_CONFIRM_STORE", {"SAFE.US": copy.deepcopy(state)})

    def broken(*args):
        raise RuntimeError("calendar unavailable")

    monkeypatch.setattr(pa, "_confirm_session_key", broken)
    action, reason, previous = pa._apply_add_confirmation(
        "SAFE.US", "ADD", "qualifying", None, {"add_confirm_days": 2})
    assert action == "HOLD" and previous == "ADD"
    assert pa._ADD_CONFIRM_STORE["SAFE.US"] == state
    assert "fail-closed" in reason


def test_legacy_utc_record_cannot_be_reinterpreted_as_completed_session(monkeypatch):
    monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", "enforce")
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", "0")
    monkeypatch.setattr(pa, "_ADD_CONFIRM_STORE", {
        "SAFE.US": {"count": 1, "date": "2026-10-02"}})
    monkeypatch.setattr(pa, "_confirm_clock", lambda symbol: (
        "2026-10-05", "2026-10-02", "session"))
    result = pa._apply_add_confirmation("SAFE.US", "ADD", "q", None,
                                        {"add_confirm_days": 2})
    assert result[0] == "HOLD"
    assert pa._ADD_CONFIRM_STORE["SAFE.US"]["count"] == 1
    assert pa._ADD_CONFIRM_STORE["SAFE.US"]["clock_basis"] == "session"


def test_skipped_session_restarts_even_when_redis_persistence_disabled(monkeypatch):
    monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", "enforce")
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", "0")
    monkeypatch.setattr(pa, "_ADD_CONFIRM_STORE", {"SAFE.US": {
        "count": 1, "date": "2026-10-02", "schema_version": 1,
        "clock_basis": "session", "exchange": "US",
    }})
    monkeypatch.setattr(pa, "_confirm_clock", lambda symbol: (
        "2026-10-06", "2026-10-05", "session"))
    result = pa._apply_add_confirmation("SAFE.US", "ADD", "q", None,
                                        {"add_confirm_days": 2})
    assert result[0] == "HOLD"
    assert pa._ADD_CONFIRM_STORE["SAFE.US"]["count"] == 1


@pytest.mark.parametrize("raw_count,schema_version", [
    (True, 1), ("1", 1), (1.5, 1), (0, 1), (-1, 1), (None, 1), (1, True),
])
def test_corrupt_session_count_fails_closed_through_actual_redis_codec(
    monkeypatch, raw_count, schema_version
):
    class Redis:
        def __init__(self):
            self.values = {}
        def get(self, key):
            return self.values.get(key)
        def set(self, key, value, **kwargs):
            self.values[key] = value

    redis = Redis()
    monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", "enforce")
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", "1")
    monkeypatch.setenv("TFB_PF_ADD_CONFIRM_LEGACY_FAILOPEN", "1")
    monkeypatch.setattr(pa, "_confirm_redis", lambda: redis)
    monkeypatch.setattr(pa, "_ADD_CONFIRM_STORE", {})
    monkeypatch.setattr(pa, "_confirm_clock", lambda symbol: (
        "2026-10-05", "2026-10-02", "session"))
    original = {"count": raw_count, "date": "2026-10-02", "schema_version": schema_version,
                "clock_basis": "session", "exchange": "US"}
    pa._confirm_redis_put("SAFE.US", original)
    before = dict(redis.values)
    result = pa.build_portfolio_actions([row(quantity=1, buy_price=100, stop=80)],
        controls={"cash_available_sar": 50_000, "add_confirm_days": 2},
        fx_rates={"USD": 3.75})
    action = result["actions"][0]
    assert action["action"] == "HOLD"
    assert "confirm-failclosed" in action["action_reason"]
    assert not (action["suggested_delta_shares"] or 0)
    assert pa._ADD_CONFIRM_STORE == {}
    assert redis.values == before


def test_boolean_schema_in_memory_fails_closed_without_clock_migration(monkeypatch):
    original = {"count": 1, "date": "2026-10-02", "schema_version": True,
                "clock_basis": "session", "exchange": "US"}
    monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", "enforce")
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", "0")
    monkeypatch.setattr(pa, "_ADD_CONFIRM_STORE", {"SAFE.US": copy.deepcopy(original)})
    monkeypatch.setattr(pa, "_confirm_clock", lambda symbol: (
        "2026-10-05", "2026-10-02", "session"))
    result = pa._apply_add_confirmation("SAFE.US", "ADD", "q", None,
                                        {"add_confirm_days": 2})
    assert result[0] == "HOLD" and "confirm-failclosed" in result[1]
    assert pa._ADD_CONFIRM_STORE["SAFE.US"] == original


def test_boolean_switch_schema_does_not_promote_actual_persisted_record(monkeypatch):
    encoded = json.dumps({"schema_version": True, "count_unit": "scans",
                          "count": 1, "ts": 1})
    class Redis:
        def get(self, key):
            return encoded
        def set(self, key, value, **kwargs):
            pytest.fail("corrupt switch evidence must not be rewritten")
    monkeypatch.setattr(pa, "_confirm_redis", lambda: Redis())
    monkeypatch.setattr(pa, "_SWITCH_CANDS", {
        "rows": [{"symbol": "NEW.US", "investability_status": "INVEST", "roi_pct": 30}],
        "ts": pa.time.time(),
    })
    monkeypatch.setattr(pa, "advisor_switch_scan", lambda *args, **kwargs: {
        "pairs_checked": 1, "proposals": [{"sell": "OLD.US", "buy": "NEW.US"}],
    })
    result = pa._run_switch_scan([{"symbol": "OLD.US"}], pa.make_controls())
    assert result["status"] == "error:ValueError"
    assert result["proposals"] == [] and result["persist_pending"] == []


def test_proposed_sales_are_not_immediate_buying_power(monkeypatch):
    monkeypatch.setenv("TFB_PF_FEE_FUNDING", "1")
    controls = pa.make_controls({"max_position_pct": 10, "target_cash_pct": 10,
                                 "rebalance_mode": "Advisory"})
    entries = [
        {"action": "EXIT", "proceeds_sar": 5_000},
        {"action": "ADD", "cand": {"symbol": "NEW.SR", "sector": "Industrials",
            "price": 10, "fx_to_sar": 1, "market_value_sar": 0},
         "action_reason": "qualifying", "proceeds_sar": 0},
    ]
    result = pa.fund_adds(entries, controls, cash_sar=0, total_value_sar=10_000)
    assert result[:3] == (0, 0, 0)
    assert entries[1]["suggested_delta_shares"] == 0


def test_switch_state_round_trips_actual_json_serializers(monkeypatch):
    class Redis:
        def __init__(self):
            self.values = {}
        def get(self, key):
            return self.values.get(key)
        def set(self, key, value, **kwargs):
            self.values[key] = value

    redis = Redis()
    monkeypatch.setattr(pa, "_confirm_redis", lambda: redis)
    monkeypatch.setattr(pa, "_SWITCH_CANDS", {
        "rows": [{"symbol": "NEW.US", "investability_status": "INVEST", "roi_pct": 30}],
        "ts": pa.time.time(),
    })
    monkeypatch.setattr(pa, "advisor_switch_scan", lambda *args, **kwargs: {
        "pairs_checked": 1, "proposals": [{"sell": "OLD.US", "buy": "NEW.US"}],
    })
    first = pa._run_switch_scan([{"symbol": "OLD.US"}], pa.make_controls())
    second = pa._run_switch_scan([{"symbol": "OLD.US"}], pa.make_controls())
    assert first["persist_pending"][0]["persist_day"] == 1
    assert second["proposals"][0]["persist_day"] == 2
    saved = json.loads(next(iter(redis.values.values())))
    assert saved["schema_version"] == 1 and saved["count_unit"] == "scans"
