"""Executable prices must carry market-asof proof through the actual builder."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import pytest

from core.analysis import opportunity_builder as ob
sys.path.insert(0, str(Path(__file__).resolve().parent))
from decision_evidence_fixtures import observed_price_fields


def row(**changes):
    result = {
        **observed_price_fields(), "symbol": "TEST.US", "name": "Synthetic Energy",
        "sector": "Energy", "market": "US", "currency": "USD",
        "current_price": 100.0, "intrinsic_value": 125.0,
        "forecast_reliability_score": 85.0, "data_quality_score": 95.0,
        "risk_bucket": "Low", "volatility_30d": 4.0,
        "avg_volume_30d": 2_500_000, "expected_roi_12m": 25.0,
        "recommendation_detailed": "BUY", "investability_status": "INVESTABLE",
    }
    result.update(changes)
    return result


def assess(source):
    return ob._quote_freshness_assessment(ob.normalize_candidate(
        source, {"USD": 3.75}, ob.make_criteria({"trust_gate_enabled": False})))


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    monkeypatch.setenv("TFB_TICKET_FRESHNESS_GATE", "1")
    monkeypatch.setenv("TFB_T10_PRICE_XCHECK", "off")
    monkeypatch.setattr(ob, "_venue_state", lambda *_args: None)


def test_actual_quote_instant_is_not_retrieval_instant(monkeypatch):
    old = datetime.now(timezone.utc) - timedelta(hours=2)
    monkeypatch.setattr(ob, "_venue_state", lambda *_args: (True, old - timedelta(hours=20)))
    passed, _, detail = assess(row(acquisition_quote_asof=old.isoformat()))
    assert not passed
    assert detail["mode"] == "session_open"
    assert detail["quote_ts"] == old.isoformat()
    assert detail["age_min"] >= 119


@pytest.mark.parametrize("changes", [
    {"acquisition_quote_asof": ""},
    {"acquisition_quote_asof": "2026-10-08"},
    {"acquisition_quote_asof": "2026-10-08T12:00:00"},
    {"warnings": "fetch_failed:timeout"},
    {"warnings": "kept_last_good"},
    {"warnings": "price_unverified_live:history"},
    {"warnings": "price_bar_stale"},
    {"acquisition_status": "preserved"},
    {"acquisition_status": "failed"},
    {"data_provider": "history"},
    {"warnings": "acquisition_status:failed"},
    {"Acquisition Quote AsOf": "2001-01-01T00:00:00Z"},
])
def test_unknown_failed_or_conflicting_proof_cannot_pass(changes):
    passed, message, _ = assess(row(**changes))
    assert not passed
    assert "UNVERIFIED_PRICE" in message


def test_retrieval_only_row_does_not_pass():
    source = row()
    for key in list(source):
        if key.startswith("acquisition_"):
            del source[key]
    assert not assess(source)[0]


def test_future_quote_is_not_clamped_to_live():
    future = datetime.now(timezone.utc) + timedelta(minutes=10)
    assert not assess(row(acquisition_quote_asof=future.isoformat()))[0]


def test_fresh_witness_passes_without_calendar():
    passed, _, detail = assess(row())
    assert passed and detail["mode"] == "live"


def test_witnessed_latest_close_passes_when_venue_closed(monkeypatch):
    close = datetime.now(timezone.utc) - timedelta(hours=18)
    monkeypatch.setattr(ob, "_venue_state", lambda *_args: (False, close))
    passed, _, detail = assess(row(acquisition_quote_asof=close.isoformat()))
    assert passed and detail["mode"] == "session_closed"


def test_pre_close_witness_cannot_pass(monkeypatch):
    close = datetime.now(timezone.utc) - timedelta(hours=18)
    monkeypatch.setattr(ob, "_venue_state", lambda *_args: (False, close))
    assert not assess(row(acquisition_quote_asof=(close - timedelta(hours=1)).isoformat()))[0]


def test_old_quote_without_calendar_stays_unknown():
    old = datetime.now(timezone.utc) - timedelta(hours=2)
    passed, _, detail = assess(row(acquisition_quote_asof=old.isoformat()))
    assert not passed and detail["mode"] == "calendar_unavailable"


@pytest.mark.parametrize("value", ["inf", "nan", "-inf", "0", "-5", "junk"])
def test_invalid_age_policy_cannot_bypass_quote_checks(monkeypatch, value):
    monkeypatch.setenv("TFB_TICKET_MAX_QUOTE_AGE_MIN", value)
    monkeypatch.setenv("TFB_TICKET_FALLBACK_MAX_AGE_H", value)
    assert ob._env_quote_max_age_min() == 15
    assert ob._env_freshness_fallback_h() == 78
    old = datetime.now(timezone.utc) - timedelta(hours=2)
    assert not assess(row(acquisition_quote_asof=old.isoformat()))[0]


def test_actual_builder_withholds_positive_failed_price():
    payload = ob.build_opportunity_payload(
        [row(warnings="fetch_failed:timeout")],
        criteria={"trust_gate_enabled": False},
        portfolio={"cash_available_sar": 50_000}, fx_rates={"USD": 3.75})
    assert payload["selected"] == []
    gates = payload["candidates_rows"][0]["gates"]
    assert any(g["gate"] == "Quote Freshness" and not g["passed"] for g in gates)


@pytest.mark.parametrize("changes", [
    {"warnings": "fetch_failed:timeout"}, {"acquisition_quote_asof": ""},
    {"acquisition_status": "preserved"},
])
def test_age_policy_off_cannot_authenticate_failed_or_missing_evidence(monkeypatch, changes):
    monkeypatch.setenv("TFB_TICKET_FRESHNESS_GATE", "0")
    payload = ob.build_opportunity_payload(
        [row(**changes)], criteria={"trust_gate_enabled": False},
        portfolio={"cash_available_sar": 50_000}, fx_rates={"USD": 3.75})
    assert payload["selected"] == []


def test_semicolon_receipt_round_trip_works():
    source = row()
    tokens = [key + ":" + source.pop(key) for key in list(source)
              if key.startswith("acquisition_")]
    source["warnings"] = "; ".join(tokens)
    assert assess(source)[0]
