#!/usr/bin/env python3
"""tests/test_venue_calendar_ext.py - opportunity_builder v1.26.2: venue
calendar coverage for the venues the live Global_Markets page trades on.

WHY (2026-10-19)
----------------
Every scheduled sync logged `[FRESHNESS v1.26.1] venue calendar unavailable
for '.MX' ... '.BH'` (25 suffixes per run). For those venues an old quote
could never be certified as the venue's last close, whatever its quote
instant said. v1.26.2 adds _VENUE_CAL_EXT_MAP, consulted only after the
16-venue base map misses, behind the kill switch TFB_TICKET_VENUE_CAL_EXT.

Properties pinned here:
  T1  every extension MIC exists in the pinned exchange_calendars and builds
  T2  base-map suffixes resolve identically with the extension on or off
      (byte-identical v1.26.1 behaviour for the 16 original venues)
  T3  the kill switch really removes the extension (base-only lookups)
  T4  an extension suffix now yields a (is_open, prev_close) pair from
      _venue_state instead of None; known-absent suffixes still yield None
  T5  the freshness verdict for an extension venue follows the same three
      answers as the old venues (last close / pre-close stale / intraday
      stale), exercised through the real _quote_freshness_assessment
  T6  no extension key shadows a base key (the base map always wins)
  T7  version and manifest pin
  T8  v1.27.2: Athens ('.AT' -> ASEX, present in 4.13.2) and NSE India ('.NS'
      -> XBOM, the same Mumbai session and holiday list as BSE) resolve, skip
      their own holidays (Athens 28 Oct Ochi Day; Mumbai 2 Oct Gandhi
      Jayanti), follow the same three freshness verdicts, and drop out under
      the kill switch. '.VN' '.AE' '.QA' '.BH' stay uncertified (T4b).

Run: pytest -q tests/test_venue_calendar_ext.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "tests"))

from core.analysis import opportunity_builder as ob  # noqa: E402
from decision_evidence_fixtures import observed_price_fields  # noqa: E402

xc = pytest.importorskip("exchange_calendars")

# Suffixes the Render log named on 2026-10-18/19 that have NO calendar in
# exchange_calendars 4.13.2: they must stay uncertified (None), not be
# guessed onto a neighbouring venue. v1.27.2: '.AT' (ASEX exists in 4.13.2)
# and '.NS' (same session as XBOM, an alias like KOSDAQ->XKRX) left this list.
KNOWN_ABSENT = ("VN", "AE", "QA", "BH")


@pytest.fixture(autouse=True)
def _ext_on(monkeypatch):
    monkeypatch.delenv("TFB_TICKET_VENUE_CAL_EXT", raising=False)
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    monkeypatch.setenv("TFB_TICKET_FRESHNESS_GATE", "1")
    monkeypatch.setenv("TFB_T10_PRICE_XCHECK", "off")


def test_t1_every_extension_mic_exists_and_builds():
    names = set(xc.get_calendar_names(include_aliases=True))
    missing = sorted(c for c in set(ob._VENUE_CAL_EXT_MAP.values()) if c not in names)
    assert not missing, "MICs absent from exchange_calendars: %r" % missing
    for code in sorted(set(ob._VENUE_CAL_EXT_MAP.values())):
        cal = xc.get_calendar(code)          # raises if the calendar is broken
        assert cal.tz is not None, code


def test_t2_base_suffixes_identical_with_extension_on_or_off(monkeypatch):
    base = {s: ob._venue_cal_code(s) for s in ob._VENUE_CAL_MAP}
    assert base == ob._VENUE_CAL_MAP
    monkeypatch.setenv("TFB_TICKET_VENUE_CAL_EXT", "0")
    assert {s: ob._venue_cal_code(s) for s in ob._VENUE_CAL_MAP} == ob._VENUE_CAL_MAP


@pytest.mark.parametrize("value", ["0", "off", "false", "no", " OFF "])
def test_t3_kill_switch_removes_extension(monkeypatch, value):
    monkeypatch.setenv("TFB_TICKET_VENUE_CAL_EXT", value)
    assert ob._venue_cal_ext_enabled() is False
    assert ob._venue_cal_code("MX") is None
    assert ob._venue_state("AMXB.MX", datetime.now(timezone.utc)) is None


@pytest.mark.parametrize("value", ["", "1", "on", "true", "anything"])
def test_t3b_default_and_other_values_enable(monkeypatch, value):
    if value == "":
        monkeypatch.delenv("TFB_TICKET_VENUE_CAL_EXT", raising=False)
    else:
        monkeypatch.setenv("TFB_TICKET_VENUE_CAL_EXT", value)
    assert ob._venue_cal_ext_enabled() is True
    assert ob._venue_cal_code("MX") == "XMEX"


@pytest.mark.parametrize("symbol", ["AMXB.MX", "BHP.AX", "EQNR.OL", "D05.SI",
                                    "ASII.JK", "005930.KS", "PKO.WA", "2330.TW",
                                    "NESTE.HE", "MAYBANK.KL", "NOVO-B.CO",
                                    "PTT.BK", "NPN.JO", "PETR4.SA", "ITUB4.SA"])
def test_t4_extension_suffix_yields_venue_state(symbol):
    now = datetime(2026, 10, 19, 7, 0, tzinfo=timezone.utc)
    vs = ob._venue_state(symbol, now)
    assert vs is not None, symbol
    is_open, prev_close = vs
    assert isinstance(is_open, bool)
    assert prev_close.tzinfo is not None
    assert prev_close <= now
    # the previous close is at most a long weekend + holiday away
    assert now - prev_close < timedelta(days=6), (symbol, prev_close)


@pytest.mark.parametrize("suffix", KNOWN_ABSENT)
def test_t4b_known_absent_suffixes_stay_uncertified(suffix):
    assert suffix not in ob._VENUE_CAL_EXT_MAP
    assert ob._venue_cal_code(suffix) is None
    assert ob._venue_state("TEST." + suffix, datetime.now(timezone.utc)) is None


def _row(symbol, asof):
    return {
        **observed_price_fields(), "symbol": symbol, "name": "Synthetic",
        "sector": "Energy", "market": "GLOBAL", "currency": "USD",
        "current_price": 100.0, "intrinsic_value": 125.0,
        "forecast_reliability_score": 85.0, "data_quality_score": 95.0,
        "risk_bucket": "Low", "volatility_30d": 4.0, "avg_volume_30d": 2_500_000,
        "expected_roi_12m": 25.0, "recommendation_detailed": "BUY",
        "investability_status": "INVESTABLE",
        "acquisition_quote_asof": asof.isoformat(),
    }


def _assess(symbol, asof):
    return ob._quote_freshness_assessment(ob.normalize_candidate(
        _row(symbol, asof), {"USD": 3.75},
        ob.make_criteria({"trust_gate_enabled": False})))


def test_t5_extension_venue_follows_the_same_three_verdicts():
    """Use the real XMEX calendar through the real assessment: an old quote
    stamped at the venue's previous close certifies as 'last close'; a quote
    before that close is STALE_PRICE pre-close; while the venue trades an old
    quote is STALE_PRICE intraday."""
    now = datetime.now(timezone.utc)
    is_open, prev_close = ob._venue_state("AMXB.MX", now)
    if is_open:
        passed, why, detail = _assess("AMXB.MX", now - timedelta(hours=2))
        assert not passed and detail["mode"] == "session_open"
        assert why.startswith("STALE_PRICE intraday")
        return
    passed, why, detail = _assess("AMXB.MX", prev_close - timedelta(seconds=30))
    assert passed and detail["mode"] == "session_closed", (why, detail)
    assert why.startswith("last close")
    passed, why, detail = _assess("AMXB.MX", prev_close - timedelta(hours=3))
    assert not passed and detail["mode"] == "session_closed"
    assert why.startswith("STALE_PRICE pre-close")


def test_t5b_kill_switch_restores_calendar_unavailable_verdict(monkeypatch):
    monkeypatch.setenv("TFB_TICKET_VENUE_CAL_EXT", "0")
    old = datetime.now(timezone.utc) - timedelta(hours=20)
    passed, why, detail = _assess("AMXB.MX", old)
    assert not passed and detail["mode"] == "calendar_unavailable"
    assert "venue calendar unavailable" in why


def test_t6_extension_never_shadows_base():
    assert not (set(ob._VENUE_CAL_EXT_MAP) & set(ob._VENUE_CAL_MAP))
    assert len(ob._VENUE_CAL_EXT_MAP) == 41          # v1.27.2: 39 + AT + NS


def test_t7_version_and_manifest_pin():
    assert ob.OPPORTUNITY_BUILDER_VERSION == "1.27.2"
    src = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '"1.27.2", "opportunity builder"' in src


def test_t8_athens_and_nse_resolve_and_obey_the_kill_switch(monkeypatch):
    assert "AT" not in ob._VENUE_CAL_MAP and "NS" not in ob._VENUE_CAL_MAP
    assert ob._venue_cal_code("AT") == "ASEX"
    assert ob._venue_cal_code("NS") == "XBOM" == ob._venue_cal_code("BO")
    monkeypatch.setenv("TFB_TICKET_VENUE_CAL_EXT", "0")
    assert ob._venue_cal_code("AT") is None and ob._venue_cal_code("NS") is None
    assert ob._venue_state("OTE.AT", datetime.now(timezone.utc)) is None


@pytest.mark.parametrize("symbol,now,closed_day,prev_day", [
    # Athens is shut on 28 Oct (Ochi Day): before the 29 Oct open the previous
    # close is 27 Oct, not 28 Oct.
    ("OTE.AT", datetime(2026, 10, 29, 6, 0, tzinfo=timezone.utc), "2026-10-28", "2026-10-27"),
    # Mumbai is shut on 2 Oct (Gandhi Jayanti, a Friday): during that day the
    # previous close is 1 Oct.
    ("RELIANCE.NS", datetime(2026, 10, 2, 6, 0, tzinfo=timezone.utc), "2026-10-02", "2026-10-01"),
])
def test_t8b_new_venues_skip_their_own_holidays(symbol, now, closed_day, prev_day):
    vs = ob._venue_state(symbol, now)
    assert vs is not None, symbol
    is_open, prev_close = vs
    assert is_open is False
    assert prev_close.strftime("%Y-%m-%d") == prev_day, (symbol, prev_close)
    assert prev_close.strftime("%Y-%m-%d") != closed_day


@pytest.mark.parametrize("symbol", ["OTE.AT", "RELIANCE.NS"])
def test_t8c_new_venues_follow_the_same_three_verdicts(symbol):
    """Same contract as T5, through the real assessment and the real ASEX /
    XBOM calendars."""
    now = datetime.now(timezone.utc)
    is_open, prev_close = ob._venue_state(symbol, now)
    if is_open:
        passed, why, detail = _assess(symbol, now - timedelta(hours=2))
        assert not passed and detail["mode"] == "session_open"
        assert why.startswith("STALE_PRICE intraday")
        return
    passed, why, detail = _assess(symbol, prev_close - timedelta(seconds=30))
    assert passed and detail["mode"] == "session_closed", (why, detail)
    assert why.startswith("last close")
    passed, why, detail = _assess(symbol, prev_close - timedelta(hours=3))
    assert not passed and detail["mode"] == "session_closed"
    assert why.startswith("STALE_PRICE pre-close")


if __name__ == "__main__":   # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
