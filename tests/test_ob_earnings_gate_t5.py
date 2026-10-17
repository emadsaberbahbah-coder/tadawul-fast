"""T5 (2026-10-17): opportunity_builder v1.28.0 — earnings-proximity gate,
dark. TFB_T10_EARNINGS_GATE off|observe|enforce, TFB_T10_EARNINGS_MIN_DAYS.

Pins: the row aliases ('Days To Earnings' / 'next_earnings_date'); the gate is
ALWAYS PASSED (an earnings hold pauses sizing, it never removes a seat); the
F-1b seam tags only in-window rows; enforce marks in-window TICKETS
sizing_hold="earnings" and counts them; meta["earnings_gate"] only when
armed; off is byte-identical to v1.27.0 (version stamp aside); unknown dates
stay n/a and are never tagged.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pytest

from core.analysis import opportunity_builder as ob

sys.path.insert(0, str(Path(__file__).resolve().parent))
from decision_evidence_fixtures import observed_price_fields, observed_portfolio  # noqa: E402

TODAY = date(2026, 10, 17)
CRIT = {"investability_gate_enabled": True, "blocked_identity_gate_enabled": True}


@pytest.fixture(autouse=True)
def _frozen_today(monkeypatch):
    monkeypatch.setattr(ob, "_earn_today", lambda: TODAY)
    monkeypatch.delenv("TFB_T10_EARNINGS_GATE", raising=False)
    monkeypatch.delenv("TFB_T10_EARNINGS_MIN_DAYS", raising=False)
    monkeypatch.delenv("TFB_T10_INVEST_REASONS_LEGACY", raising=False)


def _row(symbol, **over):
    row = {
        **observed_price_fields(),
        "symbol": symbol, "name": symbol + " Co", "sector": "Financials", "market": "Tadawul",
        "currency": "SAR", "current_price": 100.0, "intrinsic_value": 130.0,
        "forecast_reliability_score": 82.0, "data_quality_score": 91.0, "risk_bucket": "Moderate",
        "provider_engine_conflict": "No", "volatility_30d": 4.0, "avg_volume_30d": 2_500_000,
        "expected_roi_12m": 24.0, "recommendation_detailed": "STRONG BUY",
        "investability_status": "INVESTABLE", "block_reason": "",
    }
    row.update(over)
    return row


# ---------------------------------------------------------------------------
# readers and parsing
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("value,expected", (
    (None, "off"), ("", "off"), ("observe", "observe"), ("ENFORCE ", "enforce"),
    ("on", "off"), ("1", "off"), ("junk", "off"),
))
def test_mode_reader(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("TFB_T10_EARNINGS_GATE", raising=False)
    else:
        monkeypatch.setenv("TFB_T10_EARNINGS_GATE", value)
    assert ob._env_earnings_gate_mode() == expected


@pytest.mark.parametrize("value,expected", (
    (None, 5), ("", 5), ("3", 3), ("0", 0), ("60", 60), ("61", 5), ("-1", 5), ("x", 5), ("7.9", 7),
))
def test_min_days_reader_clamps(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("TFB_T10_EARNINGS_MIN_DAYS", raising=False)
    else:
        monkeypatch.setenv("TFB_T10_EARNINGS_MIN_DAYS", value)
    assert ob._env_earnings_min_days() == expected


@pytest.mark.parametrize("fields,expected", (
    ({"days_to_earnings": 4}, 4),
    ({"Days To Earnings": "11"}, 11),
    ({"next_earnings_date": "2026-10-21"}, 4),
    ({"Next Earnings Date": "2026-10-29T00:00:00Z"}, 12),
    ({"next_earnings_date": "2026-10-10"}, -7),
    ({"days_to_earnings": "", "next_earnings_date": ""}, None),
    ({"next_earnings_date": "not a date"}, None),
    ({}, None),
    ({"days_to_earnings": 2, "next_earnings_date": "2026-12-01"}, 2),   # explicit days win
))
def test_days_to_earnings_from_row(fields, expected):
    cand = ob.normalize_candidate(_row("X.SR", **fields), {}, ob.make_criteria(CRIT))
    assert cand["days_to_earnings"] == expected


# ---------------------------------------------------------------------------
# the gate: always passed, trace in note + detail
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("days,mode,note,within", (
    (3, "observe", "[earn-observe] 3d <= 5d", True),
    (0, "observe", "[earn-observe] 0d <= 5d", True),
    (5, "enforce", "[earn-enforce] HOLD 5d <= 5d", True),
    (6, "enforce", "[earn-enforce] 6d ok", False),
    (12, "observe", "[earn-observe] 12d ok", False),
    (-2, "observe", "[earn-observe] -2d ok", False),      # already reported
    (None, "enforce", "[earn-enforce] n/a (no earnings date)", False),
))
def test_gate_is_passed_and_carries_the_trace(days, mode, note, within):
    g = ob._earnings_gate({"days_to_earnings": days}, mode, 5)
    assert g["passed"] is True and g["fail_class"] is None and g["gate"] == "Earnings Proximity"
    assert g["note"] == note and g["required"] == "Days To Earnings > 5"
    assert g["earnings_detail"] == {"days": days, "min_days": 5, "within": within}
    assert g["current"] == ("n/a" if days is None else "%dd" % days)


def test_gate_registered_after_timing_in_gate_order():
    assert ob.GATE_ORDER.index("Earnings Proximity") == ob.GATE_ORDER.index("Timing (52W)") + 1
    assert ob.GATE_ORDER.count("Earnings Proximity") == 1


# ---------------------------------------------------------------------------
# end to end
# ---------------------------------------------------------------------------
# SAR rows: USD rows need certified FX proof (FX Evidence gate) to become tickets.
# distinct sectors: the per-sector cap would otherwise bench half the seats.
ROWS = [
    _row("VICI.SR", sector="Real Estate", next_earnings_date="2026-10-28"),   # 11d
    _row("EPRT.SR", sector="Consumer", next_earnings_date="2026-10-21"),      # 4d  -> within 5
    _row("AX.SR", sector="Financials", days_to_earnings=12),
    _row("GLPI.SR", sector="Energy", days_to_earnings=2),                     # within
    _row("NODATE.SR", sector="Industrials"),
    _row("WL.SR", sector="Utilities", investability_status="WATCHLIST",
         block_reason="Engine recommends HOLD", days_to_earnings=1),          # within, never a ticket
]


def _build(portfolio=True):
    return ob.build_opportunity_payload(
        ROWS, criteria=CRIT,
        portfolio=observed_portfolio({"cash_available_sar": 100_000}) if portfolio else {})


def _gate_of(rec, name):
    return next((g for g in rec["gates"] if g["gate"] == name), None)


def test_off_is_byte_identical_and_has_no_gate(monkeypatch):
    on = _build()
    assert all(_gate_of(c, "Earnings Proximity") is None for c in on["candidates_rows"])
    assert "earnings_gate" not in on["meta"]
    assert not any("earn-" in str(c.get("failure_reason")) for c in on["candidates_rows"])
    assert not any("sizing_hold" in t for t in on["selected"])
    assert on["version"] == "1.28.0"


def test_observe_tags_only_in_window_rows_and_keeps_verdicts(monkeypatch):
    base = _build()
    monkeypatch.setenv("TFB_T10_EARNINGS_GATE", "observe")
    payload = _build()
    by = {c["symbol"]: c for c in payload["candidates_rows"]}
    base_by = {c["symbol"]: c for c in base["candidates_rows"]}
    for sym, c in by.items():
        assert c["verdict"] == base_by[sym]["verdict"]
        assert _gate_of(c, "Earnings Proximity")["passed"] is True
    assert by["EPRT.SR"]["failure_reason"].endswith("[earn-observe] 4d <= 5d")
    assert by["GLPI.SR"]["failure_reason"].endswith("[earn-observe] 2d <= 5d")
    assert by["WL.SR"]["failure_reason"].startswith("Investability: WATCHLIST")
    assert by["WL.SR"]["failure_reason"].endswith(" | [earn-observe] 1d <= 5d")
    for sym in ("VICI.SR", "AX.SR", "NODATE.SR"):
        assert "earn-" not in str(by[sym]["failure_reason"])
    assert _gate_of(by["NODATE.SR"], "Earnings Proximity")["note"] == "[earn-observe] n/a (no earnings date)"
    # tickets are untouched in observe
    assert [t["symbol"] for t in payload["selected"]] == [t["symbol"] for t in base["selected"]]
    assert not any("sizing_hold" in t for t in payload["selected"])
    meta = payload["meta"]["earnings_gate"]
    assert meta == {"mode": "observe", "min_days": 5, "evaluated": 6, "unknown": 1, "within": 3, "held": 0}


def test_enforce_marks_in_window_tickets_and_never_removes_a_seat(monkeypatch):
    base = _build()
    assert sorted(t["symbol"] for t in base["selected"]) == ["AX.SR", "EPRT.SR", "GLPI.SR", "NODATE.SR", "VICI.SR"]
    monkeypatch.setenv("TFB_T10_EARNINGS_GATE", "enforce")
    payload = _build()
    assert [t["symbol"] for t in payload["selected"]] == [t["symbol"] for t in base["selected"]]
    held = {t["symbol"]: t for t in payload["selected"] if t.get("sizing_hold")}
    assert set(held) == {"EPRT.SR", "GLPI.SR"}
    assert held["EPRT.SR"]["earnings_hold_days"] == 4 and held["GLPI.SR"]["earnings_hold_days"] == 2
    assert all(t["sizing_hold"] == "earnings" for t in held.values())
    # capital math is untouched: the builder does not re-size; the cockpit suspends
    for t in payload["selected"]:
        b = next(x for x in base["selected"] if x["symbol"] == t["symbol"])
        assert t["suggested_sar"] == b["suggested_sar"]
    by = {c["symbol"]: c for c in payload["candidates_rows"]}
    assert by["EPRT.SR"]["failure_reason"].endswith("[earn-enforce] HOLD 4d <= 5d")
    assert by["EPRT.SR"]["verdict"] == "INVEST"
    assert payload["meta"]["earnings_gate"]["held"] == 2 and payload["meta"]["earnings_gate"]["within"] == 3


def test_min_days_widens_the_window(monkeypatch):
    monkeypatch.setenv("TFB_T10_EARNINGS_GATE", "enforce")
    monkeypatch.setenv("TFB_T10_EARNINGS_MIN_DAYS", "12")
    payload = _build()
    held = sorted(t["symbol"] for t in payload["selected"] if t.get("sizing_hold"))
    assert held == ["AX.SR", "EPRT.SR", "GLPI.SR", "VICI.SR"]
    assert payload["meta"]["earnings_gate"]["min_days"] == 12


def test_withheld_path_carries_the_read_back(monkeypatch):
    monkeypatch.setenv("TFB_T10_EARNINGS_GATE", "observe")
    payload = _build(portfolio=False)
    assert payload["meta"]["execution_ready"] is False
    assert payload["meta"]["earnings_gate"]["mode"] == "observe"
    assert payload["meta"]["earnings_gate"]["within"] == 3
