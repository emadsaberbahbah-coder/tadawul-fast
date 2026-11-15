"""opportunity_builder 1.30.0 [RESEARCH-QUOTE].

Render logs, 12-14 Nov 2026: the INVEST pool is 16-24 US names by day and 1-2 names (HK, SR) from the US open
until the next morning's sync, because "Quote Freshness" fails every name whose venue trades. The research stage
sizes nothing; it now keeps a name whose price is the latest session's price. The allocate replay and the single
pass keep the live rule, so nothing is sized from such a price.

All symbols, prices and cash are invented; no network, no calendar library (the venue state is injected).
"""
from __future__ import annotations

import copy
import os
from datetime import datetime, timedelta, timezone

import pytest

from core.analysis import opportunity_builder as ob
from decision_evidence_fixtures import build_with_observed_inputs, observed_portfolio, observed_price_fields

UTC = timezone.utc
FX = {"SAR": 1.0}
CRITERIA = {"max_selected": 5, "max_per_sector": 10, "max_per_market": 10, "max_weight_pct": 100.0,
            "pf_max_sector_pct": 100.0, "min_ticket_sar": 1_000.0, "trust_gate_enabled": False}
PORTFOLIO = observed_portfolio({"cash_available_sar": 20_000.0}, FX)
REQUIRED = ("successful price acquisition with actual quote as-of; live <= 15m in-session or witnessed latest "
            "venue close; unknown defers")


def row(symbol="AAA.SR", quoted=None, **changes):
    out = {"symbol": symbol, "name": "Synthetic " + symbol, "sector": "Energy", "market": "Tadawul",
           "currency": "SAR", "current_price": 100.0, "intrinsic_value": 130.0,
           "forecast_reliability_score": 82.0, "data_quality_score": 91.0, "risk_bucket": "Moderate",
           "provider_engine_conflict": "No", "volatility_30d": 4.0, "avg_volume_30d": 2_500_000,
           "expected_roi_12m": 24.0, "recommendation_detailed": "STRONG BUY",
           "investability_status": "INVESTABLE", "block_reason": ""}
    out.update(observed_price_fields())
    if quoted is not None:
        out["acquisition_quote_asof"] = quoted.isoformat()
    out.update(changes)
    return out


def cand(source):
    return ob.normalize_candidate(source, FX, ob.make_criteria({"trust_gate_enabled": False}))


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for key in list(os.environ):
        if key.startswith(("TFB_OPP_", "TFB_T10_", "TFB_TICKET_")):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("RENDER_GIT_COMMIT", raising=False)
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    monkeypatch.setenv("TFB_OPP_FUNDING_PLAN", "1")
    monkeypatch.setenv("APP_TOKEN", "synthetic-research-quote-test")
    monkeypatch.setattr(ob, "_venue_state", lambda *_args: None)


def venue(monkeypatch, is_open, close):
    monkeypatch.setattr(ob, "_venue_state", lambda *_args: (is_open, close))


def build(rows, stage=None, symbols=None, snapshot=None, criteria=None):
    extra = dict(criteria or {})
    if stage:
        extra["board_funding_stage"] = stage
    if stage == "allocate":
        extra.update(board_funding_symbols=symbols, board_funding_snapshot=copy.deepcopy(snapshot))
    return build_with_observed_inputs(ob, copy.deepcopy(rows), criteria={**CRITERIA, **extra},
                                      portfolio=copy.deepcopy(PORTFOLIO), fx_rates=dict(FX))


def freshness(payload, symbol):
    record = [item for item in payload["candidates_rows"] if item["symbol"] == symbol][0]
    return record, [gate for gate in record["gates"] if gate["gate"] == "Quote Freshness"][0]


# ------------------------------------------------------------------ the rule itself
def test_q1_while_the_venue_trades_the_last_close_or_newer_passes_for_research(monkeypatch):
    now = datetime.now(UTC)
    close = now - timedelta(hours=20)
    venue(monkeypatch, True, close)
    source = cand(row(quoted=now - timedelta(hours=3)))
    live = ob._quote_freshness_assessment(source)
    assert live[0] is False and live[2]["mode"] == "session_open"
    passed, text, detail = ob._quote_research_assessment(source)
    assert passed is True and detail["mode"] == "research_session_open"
    assert text == "research: last price 3.0h old while the venue trades; sizing needs a live quote"
    assert detail["live_rule"] == live[1] and detail["live_rule"].startswith("STALE_PRICE intraday")
    assert detail["quote_ts"] == live[2]["quote_ts"] and detail["prev_close"] == close.isoformat()
    # the previous close itself (and up to two minutes before it) is the session's price
    assert ob._quote_research_assessment(cand(row(quoted=close)))[0] is True
    assert ob._quote_research_assessment(cand(row(quoted=close - timedelta(minutes=2))))[0] is True
    older = cand(row(quoted=close - timedelta(minutes=2, seconds=1)))
    assert ob._quote_research_assessment(older)[:2] == ob._quote_freshness_assessment(older)[:2]
    assert ob._quote_research_assessment(older)[0] is False
    assert ob._quote_research_assessment(older)[2]["mode"] == "session_open"


def test_q2_after_the_close_a_price_taken_during_that_session_passes_for_research(monkeypatch):
    now = datetime.now(UTC)
    close = now - timedelta(hours=1)
    venue(monkeypatch, False, close)
    source = cand(row(quoted=close - timedelta(hours=1)))
    live = ob._quote_freshness_assessment(source)
    assert live[0] is False and live[1].startswith("STALE_PRICE pre-close")
    passed, text, detail = ob._quote_research_assessment(source)
    assert passed is True and detail["mode"] == "research_last_session" and detail["live_rule"] == live[1]
    assert text == "research: last-session price 2.0h old, taken before the close; sizing needs the close"
    assert ob._quote_research_assessment(cand(row(quoted=close - timedelta(hours=10))))[0] is True
    before = cand(row(quoted=close - timedelta(hours=10, seconds=1)))                # the session before
    assert ob._quote_research_assessment(before)[0] is False
    assert ob._quote_research_assessment(before)[1] == ob._quote_freshness_assessment(before)[1]
    monkeypatch.setenv("TFB_T10_RESEARCH_QUOTE_SESSION_H", "12")
    assert ob._quote_research_assessment(before)[0] is True


@pytest.mark.parametrize("value, hours", [("", 10.0), ("junk", 10.0), ("nan", 10.0), ("0", 1.0), ("-4", 1.0),
                                          ("6.5", 6.5), ("24", 24.0), ("400", 24.0), ("inf", 10.0)])
def test_q3_the_session_window_is_clamped(monkeypatch, value, hours):
    monkeypatch.setenv("TFB_T10_RESEARCH_QUOTE_SESSION_H", value)
    assert ob._env_research_quote_session_h() == hours


@pytest.mark.parametrize("value, armed", [("", True), ("on", True), ("enforce", True), ("1", True), ("typo", True),
                                          ("off", False), ("OFF ", False), ("0", False), ("false", False),
                                          ("no", False)])
def test_q4_the_switch_defaults_on_and_only_an_off_word_disarms_it(monkeypatch, value, armed):
    monkeypatch.setenv("TFB_T10_RESEARCH_QUOTE", value)
    assert ob._env_research_quote() is armed


@pytest.mark.parametrize("changes", [
    {"warnings": "fetch_failed:timeout"},
    {"acquisition_status": "failed"},
    {"acquisition_status": "preserved"},
    {"acquisition_quote_asof": ""},
    {"acquisition_quote_asof": "2026-10-08T12:00:00"},
    {"acquisition_quote_asof": (datetime.now(UTC) + timedelta(hours=1)).isoformat()},
])
def test_q5_an_unproven_price_is_never_a_research_price(monkeypatch, changes):
    venue(monkeypatch, True, datetime.now(UTC) - timedelta(hours=20))
    source = cand(row(**changes))
    live, research = ob._quote_freshness_assessment(source), ob._quote_research_assessment(source)
    assert research[0] is False and research[:2] == live[:2] and research[2]["mode"] == live[2]["mode"]
    assert research[2]["mode"] in ("acquisition_unverified", "quote_asof_unknown", "quote_asof_future")
    assert "live_rule" not in research[2]


def test_q6_no_calendar_and_a_live_quote_are_returned_as_the_live_rule_gives_them(monkeypatch):
    old = cand(row(quoted=datetime.now(UTC) - timedelta(hours=3)))
    research = ob._quote_research_assessment(old)                        # the fixture venue has no calendar
    assert research[0] is False and research[2]["mode"] == "calendar_unavailable"
    fresh = ob._quote_research_assessment(cand(row()))
    assert fresh[0] is True and fresh[2]["mode"] == "live" and "live_rule" not in fresh[2]
    close = datetime.now(UTC) - timedelta(hours=2)
    venue(monkeypatch, False, close)
    certified = ob._quote_research_assessment(cand(row(quoted=close)))
    assert certified[0] is True and certified[1].startswith("last close") and certified[2]["mode"] == "session_closed"


@pytest.mark.parametrize("detail", [
    {"mode": "session_open"},
    {"mode": "session_open", "quote_ts": "not a time", "prev_close": "2026-11-13T21:00:00+00:00", "age_min": 5},
    {"mode": "session_closed", "quote_ts": "2026-11-13T20:00:00+00:00", "prev_close": None, "age_min": 5},
    {"mode": "session_open", "quote_ts": "2026-11-13T20:00:00+00:00", "prev_close": "2026-11-13T21:00:00+00:00",
     "age_min": "soon"},
    {"mode": "session_open", "quote_ts": "2026-11-13T20:00:00", "prev_close": "2026-11-13T21:00:00+00:00",
     "age_min": 5},
])
def test_q7_an_unreadable_detail_stays_a_failure(monkeypatch, detail):
    given = (False, "STALE_PRICE", detail)
    monkeypatch.setattr(ob, "_quote_freshness_assessment", lambda _cand: given)
    assert ob._quote_research_assessment({}) == given


# ------------------------------------------------------------------ through the real builder
def test_b1_the_research_stage_keeps_the_name_and_every_other_stage_keeps_the_live_rule(monkeypatch):
    now = datetime.now(UTC)
    venue(monkeypatch, True, now - timedelta(hours=20))
    rows = [row("AAA.SR", quoted=now - timedelta(hours=3))]
    research = build(rows, "research")
    record, gate = freshness(research, "AAA.SR")
    assert research["status"] == "ok" and record["verdict"] == "INVEST" and gate["passed"] is True
    assert gate["current"].startswith("research: last price 3.0h old while the venue trades")
    assert gate["required"] == REQUIRED and gate["freshness_detail"]["mode"] == "research_session_open"
    assert [ticket["symbol"] for ticket in research["selected"]] == ["AAA.SR"]
    assert research["kpis"]["selected_count"] == 0 and research["kpis"].get("fundable_now", 0) == 0
    assert not research["selected"][0].get("suggested_sar") and not research["selected"][0].get("suggested_shares")
    assert research["meta"]["execution_ready"] is False

    single = build(rows)
    record, gate = freshness(single, "AAA.SR")
    assert record["verdict"] == "DO_NOT_INVEST" and gate["passed"] is False and single["selected"] == []
    assert gate["current"].startswith("STALE_PRICE intraday") and gate["required"] == REQUIRED
    assert gate["freshness_detail"]["mode"] == "session_open"


def test_b2_the_allocate_replay_never_sizes_a_research_price(monkeypatch):
    now = datetime.now(UTC)
    venue(monkeypatch, True, now - timedelta(hours=20))
    rows = [row("OLD.SR", quoted=now - timedelta(hours=3)), row("LIVE.SR", sector="Materials")]
    research = build(rows, "research")
    assert sorted(ticket["symbol"] for ticket in research["selected"]) == ["LIVE.SR", "OLD.SR"]
    snapshot = research["meta"]["board_funding"]["snapshot"]
    allocated = build(snapshot["rows"], "allocate", ["OLD.SR", "LIVE.SR"], snapshot)
    assert allocated["status"] == "ok", allocated.get("message")
    assert allocated["meta"]["board_funding"]["stage"] == "allocate"
    assert [ticket["symbol"] for ticket in allocated["selected"]] == ["LIVE.SR"]
    assert allocated["selected"][0]["suggested_sar"] > 0 and allocated["meta"]["execution_ready"] is True
    record, gate = freshness(allocated, "OLD.SR")
    assert record["verdict"] == "DO_NOT_INVEST" and record["selected"] is False
    assert gate["passed"] is False and gate["current"].startswith("STALE_PRICE intraday")
    alone = build(snapshot["rows"], "allocate", ["OLD.SR"], snapshot)     # only the research price was offered
    assert alone["status"] == "ok" and alone["selected"] == [] and alone["kpis"]["selected_count"] == 0
    assert alone["meta"]["execution_ready"] is False


def test_b3_the_evening_board_keeps_its_names(monkeypatch):
    # 13 Nov 19:01 Riyadh: pool=2 (1030.SR, 0998.HK) against 16 at 15:40. Six trading-venue names here.
    now = datetime.now(UTC)
    trading, closed = now - timedelta(hours=20), now - timedelta(hours=4)
    monkeypatch.setattr(ob, "_venue_state", lambda symbol, _now: (
        (False, closed) if str(symbol).endswith(".SR") else (True, trading)))
    rows = [row("HOME.SR", quoted=closed)] + [
        row("US%d.US" % index, quoted=now - timedelta(hours=3), market="US", sector="Sector %d" % index)
        for index in range(6)]
    evening = build(rows, "research")
    assert evening["kpis"]["passed"] == 7 and len(evening["selected"]) == 7
    assert evening["meta"]["gate_trace_counts"].get("Quote Freshness") is None
    monkeypatch.setenv("TFB_T10_RESEARCH_QUOTE", "off")                  # 1.29.0
    before = build(rows, "research")
    assert before["kpis"]["passed"] == 1 and [ticket["symbol"] for ticket in before["selected"]] == ["HOME.SR"]
    assert before["meta"]["gate_trace_counts"]["Quote Freshness"]["MAJOR"] == 6
    record, gate = freshness(before, "US0.US")
    assert record["verdict"] == "DO_NOT_INVEST" and gate["current"].startswith("STALE_PRICE intraday")


def test_b4_after_the_close_the_board_keeps_its_names_until_the_close_is_fetched(monkeypatch):
    now = datetime.now(UTC)
    close = now - timedelta(hours=2)
    venue(monkeypatch, False, close)
    rows = [row("SESSION.SR", quoted=close - timedelta(hours=1)),
            row("STALE.SR", quoted=close - timedelta(hours=30), sector="Materials")]
    payload = build(rows, "research")
    assert [ticket["symbol"] for ticket in payload["selected"]] == ["SESSION.SR"]
    record, gate = freshness(payload, "SESSION.SR")
    assert gate["freshness_detail"]["mode"] == "research_last_session" and record["verdict"] == "INVEST"
    record, gate = freshness(payload, "STALE.SR")
    assert record["verdict"] == "DO_NOT_INVEST" and gate["current"].startswith("STALE_PRICE pre-close")


def test_b4b_with_live_quotes_the_research_payload_is_the_same_with_the_switch_on_or_off(monkeypatch):
    venue(monkeypatch, True, datetime.now(UTC) - timedelta(hours=20))
    rows = [row("AAA.SR"), row("BBB.SR", sector="Materials")]

    def research():
        payload = build(rows, "research")
        payload["meta"].pop("generated_utc", None)
        funding = payload["meta"]["board_funding"]
        for key in ("snapshot", "snapshot_id"):                          # signed over the policy environment
            funding.pop(key, None)
        return payload

    armed = research()
    monkeypatch.setenv("TFB_T10_RESEARCH_QUOTE", "off")
    off = research()
    assert [ticket["symbol"] for ticket in armed["selected"]] == ["AAA.SR", "BBB.SR"]
    assert armed["candidates_rows"] == off["candidates_rows"] and armed["selected"] == off["selected"]
    assert armed["kpis"] == off["kpis"] and armed["near_miss"] == off["near_miss"]


def test_b5_the_switch_is_part_of_the_snapshot_policy(monkeypatch):
    now = datetime.now(UTC)
    venue(monkeypatch, True, now - timedelta(hours=20))
    research = build([row("LIVE.SR")], "research")
    snapshot = research["meta"]["board_funding"]["snapshot"]
    assert build(snapshot["rows"], "allocate", ["LIVE.SR"], snapshot)["status"] == "ok"
    monkeypatch.setenv("TFB_T10_RESEARCH_QUOTE", "off")
    changed = build(snapshot["rows"], "allocate", ["LIVE.SR"], snapshot)
    assert changed["status"] == "board_funding_mismatch" and changed["selected"] == []


def test_v1_version_and_inventory():
    assert ob.OPPORTUNITY_BUILDER_VERSION in ("1.30.0", "1.31.0")   # 1.31.0 adds the cap rule
    for name in ("_env_research_quote", "_env_research_quote_session_h", "_quote_research_assessment"):
        assert callable(getattr(ob, name))
    assert "v1.30.0 (2026-11-15)" in ob.__doc__
