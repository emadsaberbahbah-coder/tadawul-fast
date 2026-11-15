"""opportunity_builder 1.31.0 [CAP-NO-NEW-MONEY].

US board episodes 10 Jul - 15 Nov 2026, split at the pre-registered line RANK_CAP_ENGINE_ROI_PCT = 34: names at or
above it finished up in 39% / 28% of 20-day checks (Jul-Aug / Sep-Nov) against 63% / 52% below it. Owner, 15 Nov:
"go with the best option". Such a name keeps its research seat and is never sized.

All symbols, prices and cash are invented; no network.
"""
from __future__ import annotations

import copy
import os

import pytest

from core.analysis import opportunity_builder as ob
from decision_evidence_fixtures import build_with_observed_inputs, observed_portfolio, observed_price_fields

FX = {"SAR": 1.0}
CRITERIA = {"max_selected": 2, "max_per_sector": 10, "max_per_market": 10, "max_weight_pct": 30.0,
            "pf_max_sector_pct": 100.0, "min_ticket_sar": 1_000.0, "trust_gate_enabled": False,
            "rank_by_engine_roi_enabled": True}
PORTFOLIO = observed_portfolio({"cash_available_sar": 20_000.0}, FX)


def row(symbol, roi, sector="Energy"):
    out = {"symbol": symbol, "name": "Synthetic " + symbol, "sector": sector, "market": "Tadawul", "currency": "SAR",
           "current_price": 100.0, "intrinsic_value": 130.0, "forecast_reliability_score": 82.0,
           "data_quality_score": 91.0, "risk_bucket": "Moderate", "provider_engine_conflict": "No",
           "volatility_30d": 4.0, "avg_volume_30d": 2_500_000, "expected_roi_12m": roi,
           "recommendation_detailed": "STRONG BUY", "investability_status": "INVESTABLE", "block_reason": ""}
    out.update(observed_price_fields())
    return out


ROWS = [row("CAP.SR", 34.7), row("EDGE.SR", 34.0, "Materials"), row("UNDER.SR", 33.9, "Utilities"),
        row("LOW.SR", 20.0, "Industrials")]


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for key in list(os.environ):
        if key.startswith(("TFB_OPP_", "TFB_T10_", "TFB_TICKET_")):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("RENDER_GIT_COMMIT", raising=False)
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    monkeypatch.setenv("TFB_OPP_FUNDING_PLAN", "1")
    monkeypatch.setenv("APP_TOKEN", "synthetic-cap-test")


def build(rows=ROWS, stage=None, symbols=None, snapshot=None):
    extra = {}
    if stage:
        extra["board_funding_stage"] = stage
    if stage == "allocate":
        extra.update(board_funding_symbols=symbols, board_funding_snapshot=copy.deepcopy(snapshot))
    return build_with_observed_inputs(ob, copy.deepcopy(rows), criteria={**CRITERIA, **extra},
                                      portfolio=copy.deepcopy(PORTFOLIO), fx_rates=dict(FX))


def near(payload, symbol):
    return [r for r in payload["near_miss"] if r["symbol"] == symbol][0]


@pytest.mark.parametrize("value, mode", [("", "enforce"), ("enforce", "enforce"), ("junk", "enforce"),
                                         ("observe", "observe"), (" OFF ", "off"), ("off", "off")])
def test_n1_the_switch_defaults_to_enforce(monkeypatch, value, mode):
    monkeypatch.setenv("TFB_OPP_CAP_NO_NEW_MONEY", value)
    assert ob._env_cap_no_new_money() == mode
    assert ob.RANK_CAP_ENGINE_ROI_PCT == 34.0


def test_n2_the_single_pass_funds_the_names_below_the_line():
    payload = build()
    assert [t["symbol"] for t in payload["selected"]] == ["UNDER.SR", "LOW.SR"]
    assert all(t["suggested_sar"] > 0 for t in payload["selected"])
    for symbol, roi in (("CAP.SR", "34.7"), ("EDGE.SR", "34.0")):              # the line itself counts
        record = [a for a in payload["candidates_rows"] if a["symbol"] == symbol][0]
        assert record["verdict"] == "INVEST" and record["selected"] is False
        assert record["deferral"] == "Forecast at the cap (%s%% >= 34%%) — no new money" % roi
        miss = near(payload, symbol)
        assert miss["failed_gate"] == "Forecast cap" and miss["required"] == "engine 12-month ROI below 34%"
        assert "kept on the board as research, no new money" in miss["improve_note"]
    assert payload["meta"]["cap_no_new_money"] == {"mode": "enforce", "line_pct": 34.0, "deferred": 2,
                                                   "would_defer": 0, "symbols": ["CAP.SR", "EDGE.SR"]}


def test_n3_observe_and_off_fund_as_before(monkeypatch):
    monkeypatch.setenv("TFB_OPP_CAP_NO_NEW_MONEY", "off")
    off = build()
    assert [t["symbol"] for t in off["selected"]] == ["CAP.SR", "EDGE.SR"] and "cap_no_new_money" not in off["meta"]
    monkeypatch.setenv("TFB_OPP_CAP_NO_NEW_MONEY", "observe")
    seen = build()
    assert [t["symbol"] for t in seen["selected"]] == ["CAP.SR", "EDGE.SR"]
    assert [t["suggested_sar"] for t in seen["selected"]] == [t["suggested_sar"] for t in off["selected"]]
    assert seen["meta"]["cap_no_new_money"] == {"mode": "observe", "line_pct": 34.0, "deferred": 0,
                                                "would_defer": 2, "symbols": ["CAP.SR", "EDGE.SR"]}


def test_n4_research_keeps_the_seat_and_the_replay_never_sizes_it():
    research = build(stage="research")
    assert [t["symbol"] for t in research["selected"]] == ["CAP.SR", "EDGE.SR", "UNDER.SR", "LOW.SR"]
    assert "cap_no_new_money" not in research["meta"]
    assert not any(a.get("deferral") for a in research["candidates_rows"])
    snapshot = research["meta"]["board_funding"]["snapshot"]
    allocated = build(snapshot["rows"], "allocate", ["CAP.SR", "UNDER.SR"], snapshot)
    assert allocated["status"] == "ok", allocated.get("message")
    assert [t["symbol"] for t in allocated["selected"]] == ["UNDER.SR"] and allocated["selected"][0]["suggested_sar"] > 0
    assert allocated["meta"]["cap_no_new_money"]["deferred"] == 1
    assert allocated["meta"]["cap_no_new_money"]["symbols"] == ["CAP.SR"]
    alone = build(snapshot["rows"], "allocate", ["CAP.SR"], snapshot)
    assert alone["status"] == "ok" and alone["selected"] == [] and alone["meta"]["execution_ready"] is False


def test_n5_a_name_without_a_forecast_is_not_judged_and_the_switch_is_policy(monkeypatch):
    rows = [dict(row("NONE.SR", 0.0), expected_roi_12m=None), row("LOW.SR", 20.0, "Industrials")]
    payload = build(rows)
    assert payload["meta"]["cap_no_new_money"]["symbols"] == []
    research = build(stage="research")
    snapshot = research["meta"]["board_funding"]["snapshot"]
    monkeypatch.setenv("TFB_OPP_CAP_NO_NEW_MONEY", "off")                       # changed between the two passes
    changed = build(snapshot["rows"], "allocate", ["CAP.SR"], snapshot)
    assert changed["status"] == "board_funding_mismatch" and changed["selected"] == []


def test_v1_version_and_inventory():
    assert ob.OPPORTUNITY_BUILDER_VERSION == "1.31.0" and "Version: 1.31.0" in ob.__doc__
    assert callable(ob._env_cap_no_new_money)
