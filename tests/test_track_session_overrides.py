#!/usr/bin/env python3
"""tests/test_track_session_overrides.py - track_performance v6.57.0
[SESSION-OVERRIDES] (TRACK_SESSION_OVERRIDES, default off = v6.53.0 byte for
byte apart from the version string).

WHY (2026-11-05): the pinned exchange_calendars 4.13.2 gets sessions wrong
that the tracker's base map relies on. XKRX (.KS .KO) has no exam-day (CSAT) session
after 2020, so 19 Nov 2026 reads 09:00-15:30 KST while KRX trades 10:00-16:30;
a regular-market quote stamped inside the 15-minute close slack after 15:30
is an intraday price and matures the record. XJSE (.JSE) keeps 17:00 on the
24 and 31 Dec half days, which close at 12:00 SAST; under
TRACK_HIST_TARGET_CLOSE=enforce a record targeting the afternoon matures on a
close struck before its target. A base-map audit added SGX half days, TASE
Passover Fridays and seven phantom sessions (KRX, TASE and JSE days the
calendar opens while the venue is closed). With the gate on those sessions
take their real hours, or disappear, from _OUTCOME_SESSION_OVERRIDES.

REAL modules (scripts/track_performance.py, core.symbols.normalize) and the
REAL pinned exchange_calendars. The audit cases run the real
PerformanceTrackerApp.audit_active_records loop; the only stubs are the
backend quote call (an AsyncMock returning one witnessed row), the clock, the
historical-bar source and the price fallback / CA guard / feed switches. No
network.

T1  the table, pinned literally: every entry names a session the pinned
    calendar has; a re-time changes it (open < close, same UTC day, a shorter
    day); XKRX re-times are exactly the library's own +1h exam-day rule; XJSE
    half days close 12:00 SAST, XTAE Fridays 13:50 IDT, XSES half days 12:00
    SGT; the "closed" days are exactly the seven phantom sessions
T2  off (unset / blank / off / 0 / false / no / enforce / observe / onn / y):
    _outcome_sessions equals the untouched v6.53.0 over a symbol x day matrix
    that includes every override day
T2b the on-spellings; junk stays off
T3  on: only the named sessions change (re-timed, or gone when "closed"), only
    on their MIC; every other session, MIC and refusal is unchanged; nothing
    is added
T4  the REAL audit loop:
    a  .KS exam day, a 15:40 KST quote at 07:05Z: off MATURES (the defect);
       on stays ACTIVE (target_session_not_complete)
    b  .KS exam day, the 16:30 KST close quote at 08:00Z: on MATURES with
       target_session_close 07:30Z
    c  .JSE half day, hist enforce, a 14:00 SAST target: off MATURES on the
       24 Dec bar (the defect); on matures on the 28 Dec session instead
    d  .JSE half day, a 12:05 SAST quote: on MATURES on the real close; off
       stays ACTIVE
    e  .KO and AAPL.US / 2222.SR / VOD.L on ordinary days: identical on and off
    f  .JSE on the 4 Nov 2026 election day (venue closed): off, a provider bar
       dated that day matures the record; on, 5 Nov's own close matures it
    g  .TA on the 3 Apr 2026 Passover Friday, a 14:00 IDT target: off, the
       Friday bar (struck at 13:50) matures it; on, Monday 6 Apr does
T5  golden negative: T4a-on and T4c-on FAIL on the untouched v6.53.0
T6  ENV lane: both tracker steps map TRACK_SESSION_OVERRIDES with || 'off'
    exactly once and identically; the reader's fallback literal is "off";
    ci.yml's heavy job runs this file with the base required
V1  version floor, verify_deployment pin, title line, WHY order, ASCII file
V1b against the base: ASCII added lines (the title line aside), exactly the
    title and SCRIPT_VERSION lines removed, two functions added, none removed
    (this PR's diff alone: SKIPs once #845 / #857 / #915 are merged in)

Base copy: TFB_TEST_TRACK_BASE=<path> (must be v6.53.0), else `git show
a85edfc6:scripts/track_performance.py`; without either the base legs SKIP, or
FAIL when TFB_TEST_REQUIRE_BASE=1 (ci.yml sets it).
Golden negative of the whole file: TFB_TEST_TRACK_DELIVERED=<base copy>.

Run: python3 tests/test_track_session_overrides.py   (prints PASS k/k)
  or python3 -m pytest -q tests/test_track_session_overrides.py
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

xc = pytest.importorskip("exchange_calendars")

BASE_SHA = "a85edfc6"   # main 2026-11-05; track_performance v6.53.0 there
BASE_FULL = "a85edfc6099ac3b44ff0a24b36d74f712843a33a"
GATE = "TRACK_SESSION_OVERRIDES"
UTC = timezone.utc
_MODS = {}

# The table this build ships, pinned literally (T1).
EXPECTED = {
    "XKRX": {
        "2024-11-14": ("2024-11-14T01:00:00+00:00", "2024-11-14T07:30:00+00:00"),
        "2025-11-13": ("2025-11-13T01:00:00+00:00", "2025-11-13T07:30:00+00:00"),
        "2026-06-03": "closed",
        "2026-07-17": "closed",
        "2026-11-19": ("2026-11-19T01:00:00+00:00", "2026-11-19T07:30:00+00:00"),
        "2027-05-03": "closed",
        "2027-07-19": "closed",
    },
    "XJSE": {
        "2024-12-24": (None, "2024-12-24T10:00:00+00:00"),
        "2024-12-31": (None, "2024-12-31T10:00:00+00:00"),
        "2025-12-24": (None, "2025-12-24T10:00:00+00:00"),
        "2025-12-31": (None, "2025-12-31T10:00:00+00:00"),
        "2026-11-04": "closed",
        "2026-12-24": (None, "2026-12-24T10:00:00+00:00"),
        "2026-12-31": (None, "2026-12-31T10:00:00+00:00"),
    },
    "XTAE": {
        "2026-01-04": "closed",
        "2026-04-03": (None, "2026-04-03T10:50:00+00:00"),
        "2026-10-27": "closed",
        "2027-04-23": (None, "2027-04-23T10:50:00+00:00"),
    },
    "XSES": {
        "2025-12-24": (None, "2025-12-24T04:00:00+00:00"),
        "2025-12-31": (None, "2025-12-31T04:00:00+00:00"),
        "2026-02-16": (None, "2026-02-16T04:00:00+00:00"),
    },
}


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def delivered():
    if "delivered" not in _MODS:
        path = os.getenv("TFB_TEST_TRACK_DELIVERED", "").strip() or str(ROOT / "scripts" / "track_performance.py")
        _MODS["delivered"] = _load("track_performance_v657_delivered", path)
    return _MODS["delivered"]


def _base_or_none():
    if "base" not in _MODS:
        path = os.getenv("TFB_TEST_TRACK_BASE", "").strip()
        if not path:
            try:
                blob = subprocess.run(["git", "-C", str(ROOT), "show", BASE_SHA + ":scripts/track_performance.py"],
                                      capture_output=True, timeout=60)
            except Exception:
                blob = None
            if blob is not None and blob.returncode == 0 and b'SCRIPT_VERSION = "6.53.0"' in blob.stdout:
                tmp = Path(tempfile.mkdtemp()) / "track_performance_v6530_base.py"
                tmp.write_bytes(blob.stdout)
                path = str(tmp)
        mod = _load("track_performance_v6530_base_so", path) if path else None
        if mod is not None:   # whichever path supplied it, it must be the untouched v6.53.0
            assert getattr(mod, "SCRIPT_VERSION", "") == "6.53.0", (path, getattr(mod, "SCRIPT_VERSION", ""))
            assert not hasattr(mod, "_OUTCOME_SESSION_OVERRIDES") and not hasattr(mod, "_session_overrides_enabled"), path
        _MODS["base"] = mod
    return _MODS["base"]


def base():
    if _base_or_none() is None:
        if (os.getenv("TFB_TEST_REQUIRE_BASE") or "").strip() == "1":
            pytest.fail("TFB_TEST_REQUIRE_BASE=1 but the v6.53.0 base copy is unavailable")
        pytest.skip("base copy unavailable (set TFB_TEST_TRACK_BASE)")
    return _MODS["base"]


def _vt(s):
    return tuple(int(x) for x in str(s).split(".")[:3])


def _set_gate(monkeypatch, word):
    if word is None:
        monkeypatch.delenv(GATE, raising=False)
    else:
        monkeypatch.setenv(GATE, word)


def _at(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi, tzinfo=UTC)


def _sessions(tp, sym, around):
    """(calendar name, sessions) or ('RAISES', message) - comparable across copies."""
    tp._outcome_calendar_schedule.cache_clear()
    try:
        name, sess = tp._outcome_sessions(sym, around)
        return name, tuple((d, o.isoformat(), c.isoformat()) for d, o, c in sess)
    except Exception as exc:  # noqa: BLE001
        return "RAISES", "%s:%s" % (type(exc).__name__, exc)


SYMBOLS = ["005930.KS", "035720.KO", "NPN.JSE", "TEVA.TA", "D05.SI", "D05.SG", "AAPL.US", "2222.SR", "VOD.L", "7203.T", "0700.HK", "SAP.DE",
           "BHP.AX", "D05.SI", "TEVA.TA", "FPH.NZ", "^N225", "GC=F", "BTC-USD", "1234", "FOO.ZZ"]
DAYS = [_at(2024, 11, 14, 3, 0), _at(2025, 11, 13, 3, 0), _at(2025, 12, 24, 9, 0), _at(2025, 12, 31, 9, 0),
        _at(2026, 1, 4, 9, 0), _at(2026, 2, 16, 3, 0), _at(2026, 4, 3, 9, 0), _at(2026, 6, 3, 3, 0),
        _at(2026, 7, 17, 3, 0), _at(2026, 10, 27, 9, 0), _at(2026, 11, 4, 9, 0), _at(2026, 11, 5, 12, 0),
        _at(2026, 11, 19, 3, 0), _at(2026, 11, 20, 3, 0), _at(2026, 12, 10, 9, 0), _at(2026, 12, 24, 9, 0),
        _at(2026, 12, 31, 9, 0), _at(2027, 1, 15, 12, 0), _at(2027, 4, 23, 9, 0), _at(2027, 5, 3, 3, 0),
        _at(2027, 7, 19, 3, 0)]


# --------------------------------------------------------------- the loop ---
def _boundaries(tp, monkeypatch, now):
    monkeypatch.setattr(tp, "_utc_now", lambda: now)
    monkeypatch.setattr(tp.RiyadhTime, "now", staticmethod(lambda: now))
    monkeypatch.setattr(tp, "_price_fallback_enabled", lambda: False)
    monkeypatch.setattr(tp, "_ca_guard_enabled", lambda: False)
    monkeypatch.setattr(tp, "_feed_legacy", lambda: True)
    monkeypatch.setattr(tp, "urlopen", lambda *a, **k: pytest.fail("network"))
    for key in ("TRACK_HIST_TARGET_CLOSE", "TRACK_AUDIT_DUE_ONLY", "TRACK_HIST_CLOSE_SOURCE", "TRACK_OUTCOME_CAL_EXT",
                "TRACK_DEDUP_MATURED", "TRACK_RISK_BLANK", "TRACK_LOG_UNFORMATTED_READ"):
        monkeypatch.delenv(key, raising=False)
    tp._outcome_calendar_schedule.cache_clear()


def _record(tp, sym, target, entry=100.0):
    return tp.PerformanceRecord(
        record_id="r-" + sym, symbol=sym, horizon=tp.HorizonType.WEEK_1,
        date_recorded=target - timedelta(days=7), entry_price=entry,
        entry_recommendation=tp.RecommendationType.BUY, entry_score=70, entry_risk_bucket="MEDIUM",
        entry_confidence="HIGH", origin_tab="synthetic", target_price=110, target_roi=10,
        target_date=target, status=tp.PerformanceStatus.ACTIVE)


def _app(tp, rows):
    app = tp.PerformanceTrackerApp.__new__(tp.PerformanceTrackerApp)
    app.backend = tp.BackendClient("https://transport.invalid")
    app.backend.post_json = AsyncMock(return_value=({"rows": rows}, None, 200))
    return app


def audit(tp, monkeypatch, sym, target, quote_at, now, bars=None, price=110.0):
    """The REAL audit loop at `now` with one regular-market quote (the 15-minute
    close slack applies); `bars` arms TRACK_HIST_TARGET_CLOSE=enforce with a stub
    bar source -> (record, receipt, bar calls)."""
    _boundaries(tp, monkeypatch, now)
    calls = []
    if bars is not None:
        monkeypatch.setenv("TRACK_HIST_TARGET_CLOSE", "enforce")

        def fake_bars(symbol, session):
            calls.append((symbol, session))
            return ([{"date": d, "close": c, "split": 0.0} for d, c in bars], "", "stub-history")
        monkeypatch.setattr(tp, "_hist_close_bars_for", fake_bars)
        monkeypatch.setattr(tp, "_hist_close_bars", lambda *a, **k: pytest.fail("network"))
        monkeypatch.setattr(tp, "_ca_ledger_factor", lambda *a, **k: None)
    row = {"symbol": sym, "current_price": price, "data_provider": "yahoo_chart",
           "acquisition_status": "success", "acquisition_provider": "yahoo_chart",
           "acquisition_acquired_at": now.isoformat(), "acquisition_quote_asof": quote_at.isoformat(),
           "regularMarketTime": quote_at.isoformat()}
    rec = _record(tp, sym, target)
    asyncio.run(_app(tp, [row]).audit_active_records([rec]))
    notes = rec.notes.rsplit(tp._OUTCOME_RECEIPT, 1)
    receipt = json.loads(notes[1]) if len(notes) == 2 else {}
    return rec, receipt, calls


def _state(tp, rec):
    notes = rec.notes.rsplit(tp._OUTCOME_RECEIPT, 1)
    receipt = notes[1] if len(notes) == 2 else ""
    receipt = re.sub(r'"tracker_version": "[0-9.]+"', '"tracker_version": "X"', receipt)
    return (str(getattr(rec.status, "value", rec.status)), rec.outcome, rec.realized_roi, notes[0], receipt)


# Scenarios: (symbol, target, quote stamp, audit clock, hist bars or None)
KS_LIVE = ("005930.KS", _at(2026, 11, 19, 5, 0), _at(2026, 11, 19, 6, 40), _at(2026, 11, 19, 7, 5), None)
KS_CLOSE = ("005930.KS", _at(2026, 11, 19, 5, 0), _at(2026, 11, 19, 7, 30), _at(2026, 11, 19, 8, 0), None)
JSE_HIST = ("NPN.JSE", _at(2026, 12, 24, 12, 0), _at(2026, 12, 28, 15, 0), _at(2026, 12, 28, 16, 0),
            [("2026-12-24", 105.0), ("2026-12-28", 110.0)])
JSE_LIVE = ("NPN.JSE", _at(2026, 12, 24, 8, 0), _at(2026, 12, 24, 10, 5), _at(2026, 12, 24, 11, 0), None)


# ------------------------------------------------------------------- T1 ----
def test_t1_table_is_pinned_and_names_real_sessions_only():
    tp = delivered()
    assert tp._OUTCOME_SESSION_OVERRIDES == EXPECTED
    for mic, table in tp._OUTCOME_SESSION_OVERRIDES.items():
        cal = xc.get_calendar(mic)
        for label, spec in table.items():
            assert cal.is_session(label), (mic, label)                 # never adds a trading day
            if spec == "closed":                                       # a phantom session goes
                continue
            o, c = spec
            day = cal.sessions_in_range(label, label)[0]
            cal_o, cal_c = cal.session_open(day).to_pydatetime(), cal.session_close(day).to_pydatetime()
            new_o = datetime.fromisoformat(o) if o else cal_o
            new_c = datetime.fromisoformat(c) if c else cal_c
            assert new_o.tzinfo is not None and new_c.tzinfo is not None
            assert new_o < new_c and (new_o, new_c) != (cal_o, cal_c), (mic, label)
            assert new_o.astimezone(UTC).date().isoformat() == label == new_c.astimezone(UTC).date().isoformat()
            hhmm = new_c.astimezone(UTC).strftime("%H:%M")
            if mic == "XKRX":                                          # the library's own +1h exam-day rule
                assert new_o - cal_o == timedelta(hours=1) and new_c - cal_c == timedelta(hours=1), label
                assert hhmm == "07:30"                                 # 16:30 KST
            if mic == "XJSE":                                          # half day: close 12:00 SAST
                assert o is None and label[5:] in ("12-24", "12-31") and hhmm == "10:00", label
            if mic == "XTAE":                                          # Friday end of trading 13:50 IDT
                assert o is None and day.dayofweek == 4 and hhmm == "10:50", label
                assert cal_c.astimezone(UTC).strftime("%H:%M") == "11:15", label   # what the calendar says
            if mic == "XSES":                                          # eve half day: 12:00 SGT
                assert o is None and hhmm == "04:00", label
            if mic != "XKRX":                                          # a half day / Friday ends earlier
                assert new_c < cal_c and new_o == cal_o, (mic, label)
    closed = sorted((m, d) for m, t in tp._OUTCOME_SESSION_OVERRIDES.items() for d, v in t.items() if v == "closed")
    assert closed == [("XJSE", "2026-11-04"), ("XKRX", "2026-06-03"), ("XKRX", "2026-07-17"), ("XKRX", "2027-05-03"),
                      ("XKRX", "2027-07-19"), ("XTAE", "2026-01-04"), ("XTAE", "2026-10-27")]


# ------------------------------------------------------------------- T2 ----
OFF_WORDS = [None, "", "off", "0", "false", "no", "enforce", "observe", "onn", "y"]


def test_t2_off_equals_the_untouched_v6530(monkeypatch):
    b, d = base(), delivered()
    want = {(s, t): _sessions(b, s, t) for s in SYMBOLS for t in DAYS}
    for word in OFF_WORDS:
        _set_gate(monkeypatch, word)
        got = {(s, t): _sessions(d, s, t) for s in SYMBOLS for t in DAYS}
        assert got == want, (word, [k for k in want if got[k] != want[k]][:3])


@pytest.mark.parametrize("word", ["on", "ON", " on ", "1", "true", "Yes"])
def test_t2b_gate_words(monkeypatch, word):
    _set_gate(monkeypatch, word)
    assert delivered()._session_overrides_enabled() is True


@pytest.mark.parametrize("word", ["off", "0", "", "enforce", "observe", "onn", "y", "enabled", "2"])
def test_t2c_junk_is_off(monkeypatch, word):
    _set_gate(monkeypatch, word)
    assert delivered()._session_overrides_enabled() is False


# ------------------------------------------------------------------- T3 ----
def test_t3_on_changes_only_the_named_sessions(monkeypatch):
    tp = delivered()
    _set_gate(monkeypatch, None)
    off = {(s, t): _sessions(tp, s, t) for s in SYMBOLS for t in DAYS}
    _set_gate(monkeypatch, "on")
    on = {(s, t): _sessions(tp, s, t) for s in SYMBOLS for t in DAYS}
    changed = set()
    for key in off:
        a, b = off[key], on[key]
        assert a[0] == b[0], key                                      # same calendar or same refusal
        if a[0] == "RAISES":
            assert a == b, key
            continue
        table = EXPECTED.get(a[0], {})
        kept = {x[0]: x for x in b[1]}
        for la, oa, ca in a[1]:
            spec = table.get(la)
            if spec == "closed":
                assert la not in kept, (key, la)                      # the phantom session is gone
                changed.add((a[0], la))
                continue
            assert la in kept, (key, la)                              # every other trading day stays
            _lb, ob, cb = kept[la]
            if spec is None:
                assert (oa, ca) == (ob, cb), (key, la)                # untouched session
            else:
                assert ob == (spec[0] or oa) and cb == (spec[1] or ca), (key, la)
                changed.add((a[0], la))
        assert set(kept) <= {x[0] for x in a[1]}, key                 # nothing added
    want = {(m, d) for m, t in EXPECTED.items() for d in t}
    reachable = want - {("XJSE", "2024-12-24"), ("XJSE", "2024-12-31")}   # outside the DAYS windows
    assert reachable <= changed, sorted(reachable - changed)
    assert {m for m, _ in changed} == {"XKRX", "XJSE", "XTAE", "XSES"}


# ------------------------------------------------------------------- T4 ----
def _t4a_on(tp, monkeypatch):
    _set_gate(monkeypatch, "on")
    rec, receipt, _calls = audit(tp, monkeypatch, *KS_LIVE)
    assert rec.status == tp.PerformanceStatus.ACTIVE, (rec.status, receipt)
    assert receipt.get("reason") == "target_session_not_complete", receipt
    assert receipt.get("target_session_close") == "2026-11-19T07:30:00+00:00", receipt


def test_t4a_exam_day_intraday_quote(monkeypatch):
    tp = delivered()
    _set_gate(monkeypatch, None)
    rec, receipt, _c = audit(tp, monkeypatch, *KS_LIVE)
    assert rec.status == tp.PerformanceStatus.MATURED, (rec.status, receipt)      # the defect, gate off
    assert receipt.get("target_session_close") == "2026-11-19T06:30:00+00:00", receipt
    _t4a_on(tp, monkeypatch)


def test_t4b_exam_day_real_close_matures(monkeypatch):
    tp = delivered()
    _set_gate(monkeypatch, "on")
    rec, receipt, _c = audit(tp, monkeypatch, *KS_CLOSE)
    assert rec.status == tp.PerformanceStatus.MATURED and rec.outcome == "WIN", (rec.status, receipt)
    assert receipt.get("target_session") == "2026-11-19" and receipt.get("calendar") == "XKRX", receipt
    assert receipt.get("target_session_close") == "2026-11-19T07:30:00+00:00", receipt
    _set_gate(monkeypatch, None)
    rec_off, receipt_off, _c = audit(tp, monkeypatch, *KS_CLOSE)
    assert rec_off.status == tp.PerformanceStatus.ACTIVE, (rec_off.status, receipt_off)


def _t4c_on(tp, monkeypatch):
    _set_gate(monkeypatch, "on")
    rec, receipt, calls = audit(tp, monkeypatch, *JSE_HIST)
    assert rec.status == tp.PerformanceStatus.MATURED, (rec.status, receipt)
    assert receipt.get("target_session") == "2026-12-28", receipt                  # first close after 12:00Z
    assert receipt.get("hist_bar_date") != "2026-12-24" and abs(rec.realized_roi - 10.0) < 1e-9, receipt
    assert calls == [], calls                                                     # the live close did it


def test_t4c_half_day_hist_target_after_the_real_close(monkeypatch):
    tp = delivered()
    _set_gate(monkeypatch, None)
    rec, receipt, calls = audit(tp, monkeypatch, *JSE_HIST)
    assert rec.status == tp.PerformanceStatus.MATURED, (rec.status, receipt)      # the defect, gate off
    assert receipt.get("hist_bar_date") == "2026-12-24" and abs(rec.realized_roi - 5.0) < 1e-9, receipt
    _t4c_on(tp, monkeypatch)


def test_t4d_half_day_real_close_quote(monkeypatch):
    tp = delivered()
    _set_gate(monkeypatch, "on")
    rec, receipt, _c = audit(tp, monkeypatch, *JSE_LIVE)
    assert rec.status == tp.PerformanceStatus.MATURED, (rec.status, receipt)
    assert receipt.get("target_session_close") == "2026-12-24T10:00:00+00:00", receipt
    _set_gate(monkeypatch, None)
    rec_off, receipt_off, _c = audit(tp, monkeypatch, *JSE_LIVE)
    assert rec_off.status == tp.PerformanceStatus.ACTIVE and receipt_off.get("reason") == "target_session_not_complete"


JSE_CLOSED = ("NPN.JSE", _at(2026, 11, 4, 10, 0), _at(2026, 11, 5, 15, 0), _at(2026, 11, 5, 16, 0),
              [("2026-11-04", 104.0), ("2026-11-05", 110.0)])
TASE_FRIDAY = ("TEVA.TA", _at(2026, 4, 3, 11, 0), _at(2026, 4, 6, 11, 15), _at(2026, 4, 6, 12, 0),
               [("2026-04-03", 105.0), ("2026-04-06", 110.0)])


def test_t4f_phantom_session_election_day(monkeypatch):
    """XJSE 2026-11-04: the JSE was closed (local elections). Off, the target
    session is the phantom day: the 5 Nov quote is outside it and, under hist
    enforce, a provider bar dated 4 Nov matures the record (or it waits for
    ever). On, the target session is 5 Nov and its own close matures it."""
    tp = delivered()
    _set_gate(monkeypatch, None)
    rec, receipt, calls = audit(tp, monkeypatch, *JSE_CLOSED)
    assert receipt.get("target_session") == "2026-11-04", receipt
    assert rec.status == tp.PerformanceStatus.MATURED and receipt.get("hist_bar_date") == "2026-11-04", receipt
    _set_gate(monkeypatch, "on")
    rec, receipt, calls = audit(tp, monkeypatch, *JSE_CLOSED)
    assert rec.status == tp.PerformanceStatus.MATURED and abs(rec.realized_roi - 10.0) < 1e-9, (rec.status, receipt)
    assert receipt.get("target_session") == "2026-11-05" and calls == [], (receipt, calls)


def test_t4g_passover_friday_target_after_the_real_end(monkeypatch):
    """XTAE 2026-04-03: the Friday ended at 13:50 IDT (10:50Z); the calendar says
    14:15. A 14:00 IDT target (11:00Z): off, hist enforce matures it on the
    Friday bar, struck before the target; on, the target session is Monday 6
    April and its own close matures it."""
    tp = delivered()
    _set_gate(monkeypatch, None)
    rec, receipt, _c = audit(tp, monkeypatch, *TASE_FRIDAY)
    assert rec.status == tp.PerformanceStatus.MATURED and receipt.get("hist_bar_date") == "2026-04-03", receipt
    _set_gate(monkeypatch, "on")
    rec, receipt, calls = audit(tp, monkeypatch, *TASE_FRIDAY)
    assert rec.status == tp.PerformanceStatus.MATURED and abs(rec.realized_roi - 10.0) < 1e-9, (rec.status, receipt)
    assert receipt.get("target_session") == "2026-04-06" and calls == [], (receipt, calls)


ORDINARY = [("035720.KO", _at(2026, 11, 12, 5, 0), _at(2026, 11, 12, 6, 31), _at(2026, 11, 12, 7, 5)),
            ("AAPL.US", _at(2026, 11, 12, 18, 0), _at(2026, 11, 12, 21, 0), _at(2026, 11, 12, 22, 0)),
            ("2222.SR", _at(2026, 11, 12, 9, 0), _at(2026, 11, 12, 12, 0), _at(2026, 11, 12, 13, 0)),
            ("VOD.L", _at(2026, 11, 12, 12, 0), _at(2026, 11, 12, 16, 30), _at(2026, 11, 12, 17, 0)),
            ("NPN.JSE", _at(2026, 12, 23, 9, 0), _at(2026, 12, 23, 15, 0), _at(2026, 12, 23, 16, 0))]


@pytest.mark.parametrize("sym,target,quote,now", ORDINARY)
def test_t4e_ordinary_days_identical_on_and_off(monkeypatch, sym, target, quote, now):
    tp = delivered()
    states = []
    for word in (None, "on"):
        _set_gate(monkeypatch, word)
        rec, _r, _c = audit(tp, monkeypatch, sym, target, quote, now)
        states.append(_state(tp, rec))
    assert states[0] == states[1]
    assert states[0][0].upper().startswith("MATURED"), states[0]


# ---------------------------------------------------------------- GOLD -----
def test_t5_golden_negative_on_legs_fail_on_the_base(monkeypatch):
    b = base()
    with pytest.raises(AssertionError):
        _t4a_on(b, monkeypatch)
    with pytest.raises(AssertionError):
        _t4c_on(b, monkeypatch)


# ------------------------------------------------------------------- T6 ----
STEP_NAMES = {".github/workflows/track_performance.yml": "Run performance clock",
              ".github/workflows/daily_sync.yml": "Track Performance (record + audit)"}
MAPPING = "TRACK_SESSION_OVERRIDES: ${{ vars.TRACK_SESSION_OVERRIDES || 'off' }}"


def _tracker_step(rel):
    text = (ROOT / rel).read_text(encoding="utf-8")
    parts = re.split(r"^\s{6}- name: ", text, flags=re.M)
    hits = [p for p in parts if p.splitlines() and STEP_NAMES[rel] in p.splitlines()[0]]
    assert len(hits) == 1, (rel, len(hits))
    return text, hits[0]


def test_t6_env_lane_parity_reader_and_ci():
    lines = {}
    for rel in STEP_NAMES:
        text, step = _tracker_step(rel)
        everywhere = [ln.strip() for ln in text.splitlines() if re.match(r"^\s+%s:" % GATE, ln)]
        in_step = [ln.strip() for ln in step.splitlines() if re.match(r"^\s+%s:" % GATE, ln)]
        assert everywhere == [MAPPING] and in_step == [MAPPING], (rel, everywhere, in_step)
        lines[rel] = in_step
    assert len(set(map(tuple, lines.values()))) == 1                      # ENV PARITY
    src = Path(delivered().__file__).read_text(encoding="utf-8")
    m = re.search(r"^def _session_overrides_enabled\(\)[^\n]*:\n(.*?)(?=^def |\Z)", src, re.M | re.S)
    assert m and re.search(r'os\.getenv\("%s"\)\s*or\s*"off"' % GATE, m.group(1))
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    heavy = ci.split("\n  heavy:\n", 1)[1].split("\n  summary:\n", 1)[0]
    step = heavy.split("- name: Outcome calendar special sessions", 1)[1].split("\n      - name: ", 1)[0]
    assert re.search(r"python -m pytest -q tests/test_track_session_overrides\.py", step)
    assert "git fetch --no-tags --depth=1 origin %s" % BASE_FULL in step
    assert re.search(r'^\s+TFB_TEST_REQUIRE_BASE: "1"$', step, re.M), step
    assert BASE_FULL.startswith(BASE_SHA)


# ------------------------------------------------------------------- V -----
def test_v1_version_pin_title_why_ascii():
    tp = delivered()
    assert _vt(tp.SCRIPT_VERSION) >= (6, 57, 0)
    src = Path(tp.__file__).read_text(encoding="utf-8")
    assert "(v%s)" % tp.SCRIPT_VERSION in src.splitlines()[4]
    vd = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("track_performance", "%s", "track performance")' % tp.SCRIPT_VERSION in vd
    i57 = src.index("# v6.57.0 (2026-11-05) [SESSION-OVERRIDES]")
    assert src.index("# v6.53.0 (2026-11-02) [ARCHIVE-BAR + DUE-ONLY]") < i57 < src.index("\nSCRIPT_VERSION = ")
    assert all(ord(c) < 128 for c in Path(__file__).read_text(encoding="utf-8"))


def test_v1b_against_the_base_ascii_and_functions():
    import ast
    import difflib
    b = base()
    tp = delivered()
    src = Path(tp.__file__).read_text(encoding="utf-8")
    if tp.SCRIPT_VERSION != "6.57.0" or re.search(r"^# v6\.5[456]\.0 \(", src, re.M):
        pytest.skip("the exact a85edfc6 -> 6.57.0 diff checks apply to this PR's diff alone")
    old = Path(b.__file__).read_text(encoding="utf-8").splitlines()
    new = src.splitlines()
    diff = list(difflib.unified_diff(old, new, lineterm="", n=0))
    added = [ln[1:] for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
    removed = [ln[1:] for ln in diff if ln.startswith("-") and not ln.startswith("---")]
    title = "TADAWUL FAST BRIDGE \u2013 ADVANCED PERFORMANCE ANALYTICS ENGINE (v%s)"
    assert sorted(removed) == sorted([title % "6.53.0", 'SCRIPT_VERSION = "6.53.0"']), removed
    fresh = [ln for ln in added if ln != title % "6.57.0"]
    assert fresh and all(ln.isascii() for ln in fresh), [ln for ln in fresh if not ln.isascii()][:3]
    kinds = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)

    def defs(text):
        return {n.name for n in ast.walk(ast.parse(text)) if isinstance(n, kinds)}
    assert defs("\n".join(new)) - defs("\n".join(old)) == {"_session_overrides_enabled", "_outcome_session_override"}
    assert not defs("\n".join(old)) - defs("\n".join(new))


if __name__ == "__main__":
    class _Tally:
        def __init__(self):
            self.passed = self.failed = self.skipped = 0

        def pytest_runtest_logreport(self, report):
            if report.when == "call" and report.passed:
                self.passed += 1
            elif report.skipped:
                self.skipped += 1
            elif report.failed:
                self.failed += 1

    tally = _Tally()
    rc = pytest.main([__file__, "-q", "-p", "no:cacheprovider"], plugins=[tally])
    run_n = tally.passed + tally.failed
    print("PASS %d/%d" % (tally.passed, run_n) + (" (%d skipped)" % tally.skipped if tally.skipped else ""))
    sys.exit(rc)
