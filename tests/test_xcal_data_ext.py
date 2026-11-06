#!/usr/bin/env python3
"""tests/test_xcal_data_ext.py - core/exchange_calendar_ext.py 1.0.0,
track_performance v6.58.0 and opportunity_builder 1.28.3 [XCAL-DATA-EXT]
(TFB_XCAL_DATA_EXT, default off = main a85edfc6 byte for byte apart from the
version strings).

WHY (2026-11-06): the pinned exchange_calendars==4.13.2 records SGX holidays
only to 2026, so XSES ends at the 2026-12-31 close. The tracker's +/-14-day
outcome window raises for every .SI / .SG lookup from 2026-12-18
(outcome_calendar_unknown); the builder's venue state raises from 2026-12-31
09:00Z, so an old .SI quote reads "UNVERIFIED_PRICE: ... venue calendar
unavailable". With the gate on, both read the pinned XSES calendar plus the
eight 2027 SGX closures.

REAL modules (core/exchange_calendar_ext.py, scripts/track_performance.py,
core/analysis/opportunity_builder.py) and the REAL pinned exchange_calendars.
The tracker cases run the real PerformanceTrackerApp.audit_active_records
loop; the only stubs are the backend quote call (an AsyncMock returning one
witnessed row), the clock and the price fallback / CA guard / feed switches.
No network.

X1  the table, pinned literally: only XSES; every added day is a 2027
    weekday after the pinned bound (2026-12-31); the base-class map names
    the same MICs
X2  the gate: unset and ten off words read off (extended_calendar is None
    for every MIC); the six on-spellings read on
X3  on: the extended XSES equals the pinned XSES on every pinned session
    (open and close); 2027 = its weekdays minus the eight closures; bound
    2027-12-31; one instance per process; every other MIC, an unknown name
    and a failing import read None; a table the package already covers
    retires itself
X4  tracker, _outcome_sessions over a symbol x day matrix: off equals the
    untouched v6.53.0 (raising cases included) under the off words; on
    equals it everywhere except .SI / .SG windows past 2026-12-31, where
    the base raises and the delivered file returns the extended sessions
X5  the REAL audit loop:
    a  D05.SI due 2026-12-24 05:00Z, the 17:00 SGT close quote at 09:30Z:
       off stays ACTIVE (outcome_calendar_unknown, the defect); on MATURES
       (target_session_close 2026-12-24T09:00:00+00:00)
    b  D05.SI due 2027-01-08: off unknown; on MATURES
    c  D05.SI due 2027-02-08 03:00Z (Chinese New Year, closed): on, the
       target session is 2027-02-09 and its close matures the record
    d  AAPL.US, 2222.SR, VOD.L: identical on and off
X6  the builder's .SI freshness:
    a  _venue_state('D05.SI', 2027-01-04 03:00Z): off None; on (open, the
       2026-12-31 09:00Z close)
    b  a .SI quote stamped at the 2026-12-31 close, judged on Fri 1 Jan
       2027 (a holiday): off "UNVERIFIED_PRICE ... venue calendar
       unavailable"; on certified "last close"
    c  AAPL.US and 2222.SR venue states identical on and off
X7  golden negative: X5a-on and X6b-on FAIL on the untouched main copies
X8  ENV lane: both tracker steps map TFB_XCAL_DATA_EXT with || 'off'
    exactly once and identically; the reader's fallback literal is "off";
    ci.yml's heavy job runs this file with the base required
V1  version floors, verify_deployment pins, title line, WHY order, ASCII
V1b against the base: only the version lines (and the one re-indented
    get_calendar line) removed, added lines ASCII (the title aside), exactly
    the one new function in each file (this PR's diff alone: SKIPs once an
    open PR's block is merged in)

Base copies: TFB_TEST_TRACK_BASE / TFB_TEST_OB_BASE=<path> (must be v6.53.0 /
1.28.0), else `git show a85edfc6:<path>`; without them the base legs SKIP, or
FAIL when TFB_TEST_REQUIRE_BASE=1 (ci.yml sets it).
Golden negative of a whole file: TFB_TEST_TRACK_DELIVERED / TFB_TEST_OB_DELIVERED
=<base copy>.

Run: python3 tests/test_xcal_data_ext.py   (prints PASS k/k)
  or python3 -m pytest -q tests/test_xcal_data_ext.py
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
pd = pytest.importorskip("pandas")

BASE_SHA = "a85edfc6"   # main 2026-11-06; track_performance v6.53.0, opportunity_builder 1.28.0
BASE_FULL = "a85edfc6099ac3b44ff0a24b36d74f712843a33a"
GATE = "TFB_XCAL_DATA_EXT"
UTC = timezone.utc
OFF_WORDS = [None, "", "off", "0", "false", "no", "enforce", "observe", "onn", "y", "enabled"]
ON_WORDS = ["on", "ON", " on ", "1", "true", "Yes"]
_MODS = {}

# The table this build ships, pinned literally (X1).
EXPECTED = {
    "XSES": ("2027-01-01", "2027-02-08", "2027-03-10", "2027-03-26",
             "2027-05-17", "2027-05-20", "2027-08-09", "2027-10-28"),
}


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def ext():
    import core.exchange_calendar_ext as mod
    return mod


def _delivered(key, env, rel, name):
    if key not in _MODS:
        path = os.getenv(env, "").strip() or str(ROOT / rel)
        _MODS[key] = _load(name, path)
    return _MODS[key]


def tracker():
    return _delivered("tp", "TFB_TEST_TRACK_DELIVERED", "scripts/track_performance.py", "track_performance_v658_delivered")


def builder():
    return _delivered("ob", "TFB_TEST_OB_DELIVERED", "core/analysis/opportunity_builder.py",
                      "opportunity_builder_v1283_delivered")


def _base_copy(key, env, rel, version_re, name, check):
    if key not in _MODS:
        path = os.getenv(env, "").strip()
        if not path:
            try:
                blob = subprocess.run(["git", "-C", str(ROOT), "show", BASE_SHA + ":" + rel],
                                      capture_output=True, timeout=60)
            except Exception:
                blob = None
            if blob is not None and blob.returncode == 0 and re.search(version_re, blob.stdout):
                tmp = Path(tempfile.mkdtemp()) / (key + "_base.py")
                tmp.write_bytes(blob.stdout)
                path = str(tmp)
        mod = _load(name, path) if path else None
        if mod is not None:
            check(mod, path)
        _MODS[key] = mod
    if _MODS[key] is None:
        if (os.getenv("TFB_TEST_REQUIRE_BASE") or "").strip() == "1":
            pytest.fail("TFB_TEST_REQUIRE_BASE=1 but the %s base copy is unavailable" % rel)
        pytest.skip("base copy unavailable (set %s)" % env)
    return _MODS[key]


def _check_tp(mod, path):
    assert getattr(mod, "SCRIPT_VERSION", "") == "6.53.0", path
    assert not hasattr(mod, "_outcome_calendar_ext"), path


def _check_ob(mod, path):
    assert getattr(mod, "OPPORTUNITY_BUILDER_VERSION", "") == "1.28.0", path
    assert not hasattr(mod, "_venue_cal_data_ext"), path


def tracker_base():
    return _base_copy("tp_base", "TFB_TEST_TRACK_BASE", "scripts/track_performance.py",
                      rb'SCRIPT_VERSION = "6\.53\.0"', "track_performance_v6530_base_xcal", _check_tp)


def builder_base():
    return _base_copy("ob_base", "TFB_TEST_OB_BASE", "core/analysis/opportunity_builder.py",
                      rb'OPPORTUNITY_BUILDER_VERSION = "1\.28\.0"', "opportunity_builder_v1280_base_xcal", _check_ob)


def _vt(s):
    return tuple(int(x) for x in str(s).split(".")[:3])


def _set_gate(monkeypatch, word):
    if word is None:
        monkeypatch.delenv(GATE, raising=False)
    else:
        monkeypatch.setenv(GATE, word)


def _at(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi, tzinfo=UTC)


# ------------------------------------------------------------------- X1 ----
def test_x1_table_is_pinned_and_extends_only_past_the_pinned_bound():
    e = ext()
    assert e.XCAL_EXTRA_HOLIDAYS == EXPECTED
    assert set(e._XCAL_BASE_CLASSES) == set(EXPECTED)
    for mic, days in EXPECTED.items():
        cal = xc.get_calendar(mic)
        assert str(cal.last_session.date()) == "2026-12-31", mic          # the pinned bound this build answers
        assert list(days) == sorted(set(days)), mic
        for day in days:
            ts = pd.Timestamp(day)
            assert ts.year == 2027 and ts.weekday() < 5, (mic, day)       # a weekday closure, after the bound


# ------------------------------------------------------------------- X2 ----
@pytest.mark.parametrize("word", OFF_WORDS)
def test_x2_off_words_read_off(monkeypatch, word):
    _set_gate(monkeypatch, word)
    e = ext()
    assert e.calendar_ext_enabled() is False
    for mic in ("XSES", "XNYS", "XSAU", "XSHG", "XBOM", "xses", ""):
        assert e.extended_calendar(mic) is None, mic


@pytest.mark.parametrize("word", ON_WORDS)
def test_x2b_on_words_read_on(monkeypatch, word):
    _set_gate(monkeypatch, word)
    assert ext().calendar_ext_enabled() is True


# ------------------------------------------------------------------- X3 ----
def test_x3_extended_xses_keeps_every_pinned_session_and_adds_2027(monkeypatch):
    _set_gate(monkeypatch, "on")
    e = ext()
    cal = e.extended_calendar("XSES")
    pinned = xc.get_calendar("XSES")
    assert cal is not None and cal.name == "XSES" and str(cal.last_session.date()) == "2027-12-31"
    n = len(pinned.sessions)
    assert (cal.sessions[:n] == pinned.sessions).all()
    assert (cal.opens[:n] == pinned.opens).all() and (cal.closes[:n] == pinned.closes).all()
    s27 = {d.date().isoformat() for d in cal.sessions if d.year == 2027}
    weekdays = {d.date().isoformat() for d in pd.bdate_range("2027-01-01", "2027-12-31")}
    assert s27 == weekdays - set(EXPECTED["XSES"]) and len(s27) == 253
    assert cal.previous_close(pd.Timestamp("2027-01-04 03:00", tz="UTC")) == pd.Timestamp("2026-12-31 09:00", tz="UTC")
    assert e.extended_calendar("XSES") is cal and e.extended_calendar(" xses ") is cal      # one instance
    for mic in ("XNYS", "XSAU", "XSHG", "XBOM", "XKRX", "NOPE", ""):
        assert e.extended_calendar(mic) is None, mic


def test_x3b_failing_import_and_covered_years_read_none(monkeypatch):
    _set_gate(monkeypatch, "on")
    e = ext()
    e._extended_calendar.cache_clear()
    try:
        monkeypatch.setattr(e, "_XCAL_BASE_CLASSES", {"XSES": ("no_such_module_xcal", "Nope")})
        assert e.extended_calendar("XSES") is None                     # never raises
        e._extended_calendar.cache_clear()
        monkeypatch.setattr(e, "_XCAL_BASE_CLASSES", {"XSES": ("exchange_calendars.exchange_calendar_xses",
                                                               "XSESExchangeCalendar")})
        monkeypatch.setattr(e, "XCAL_EXTRA_HOLIDAYS", {"XSES": ("2026-12-25",)})
        assert e.extended_calendar("XSES") is None                     # the package already covers 2026: it wins
    finally:
        e._extended_calendar.cache_clear()


# ------------------------------------------------------------------- X4 ----
def _sessions(tp, sym, around):
    tp._outcome_calendar_schedule.cache_clear()
    try:
        name, sess = tp._outcome_sessions(sym, around)
        return name, tuple((d, o.isoformat(), c.isoformat()) for d, o, c in sess)
    except Exception as exc:  # noqa: BLE001
        return "RAISES", type(exc).__name__


SYMBOLS = ["D05.SI", "D05.SG", "Z74.SI", "2222.SR", "AAPL.US", "005930.KS", "VOD.L", "NPN.JSE", "TEVA.TA",
           "^N225", "BTC-USD", "FOO.ZZ"]
DAYS = [_at(2026, 11, 6, 9, 0), _at(2026, 12, 17, 3, 0), _at(2026, 12, 18, 3, 0), _at(2026, 12, 24, 5, 0),
        _at(2026, 12, 31, 9, 0), _at(2027, 1, 4, 3, 0), _at(2027, 2, 8, 3, 0), _at(2027, 6, 30, 3, 0),
        _at(2027, 12, 17, 3, 0)]
SG = ("D05.SI", "D05.SG", "Z74.SI")


@pytest.mark.parametrize("word", OFF_WORDS)
def test_x4_tracker_off_equals_the_untouched_v6530(monkeypatch, word):
    tp, b = tracker(), tracker_base()
    _set_gate(monkeypatch, word)
    got = {(s, t): _sessions(tp, s, t) for s in SYMBOLS for t in DAYS}
    want = {(s, t): _sessions(b, s, t) for s in SYMBOLS for t in DAYS}
    assert got == want
    assert got[("D05.SI", _at(2026, 12, 18, 3, 0))] == ("RAISES", "DateOutOfBounds")   # the defect, gate off
    assert got[("D05.SI", _at(2026, 12, 17, 3, 0))][0] == "XSES"


def test_x4b_tracker_on_changes_only_singapore_windows_past_2026(monkeypatch):
    tp, b = tracker(), tracker_base()
    _set_gate(monkeypatch, "on")
    cal = ext().extended_calendar("XSES")
    changed = 0
    for s in SYMBOLS:
        for t in DAYS:
            got, base = _sessions(tp, s, t), _sessions(b, s, t)
            if s in SG and (t + timedelta(days=14)).date().isoformat() > "2026-12-31":
                assert base == ("RAISES", "DateOutOfBounds"), (s, t, base)
                day = t.date()
                rng = cal.sessions_in_range((day - timedelta(days=14)).isoformat(),
                                            (day + timedelta(days=14)).isoformat())
                assert got == ("XSES", tuple((str(x.date()), cal.session_open(x).isoformat(),
                                              cal.session_close(x).isoformat()) for x in rng)), (s, t)
                changed += 1
            else:
                assert got == base, (s, t)
    assert changed == 3 * 7


# ------------------------------------------------------------------- X5 ----
def _boundaries(tp, monkeypatch, now):
    monkeypatch.setattr(tp, "_utc_now", lambda: now)
    monkeypatch.setattr(tp.RiyadhTime, "now", staticmethod(lambda: now))
    monkeypatch.setattr(tp, "_price_fallback_enabled", lambda: False)
    monkeypatch.setattr(tp, "_ca_guard_enabled", lambda: False)
    monkeypatch.setattr(tp, "_feed_legacy", lambda: True)
    monkeypatch.setattr(tp, "urlopen", lambda *a, **k: pytest.fail("network"))
    for key in ("TRACK_HIST_TARGET_CLOSE", "TRACK_AUDIT_DUE_ONLY", "TRACK_HIST_CLOSE_SOURCE", "TRACK_OUTCOME_CAL_EXT",
                "TRACK_SESSION_OVERRIDES", "TRACK_DEDUP_MATURED", "TRACK_RISK_BLANK", "TRACK_LOG_UNFORMATTED_READ"):
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


def audit(tp, monkeypatch, sym, target, quote_at, now, price=110.0):
    """The REAL audit loop at `now` with one regular-market quote."""
    _boundaries(tp, monkeypatch, now)
    row = {"symbol": sym, "current_price": price, "data_provider": "yahoo_chart",
           "acquisition_status": "success", "acquisition_provider": "yahoo_chart",
           "acquisition_acquired_at": now.isoformat(), "acquisition_quote_asof": quote_at.isoformat(),
           "regularMarketTime": quote_at.isoformat()}
    rec = _record(tp, sym, target)
    asyncio.run(_app(tp, [row]).audit_active_records([rec]))
    notes = rec.notes.rsplit(tp._OUTCOME_RECEIPT, 1)
    receipt = json.loads(notes[1]) if len(notes) == 2 else {}
    return rec, receipt


def _state(tp, rec):
    notes = rec.notes.rsplit(tp._OUTCOME_RECEIPT, 1)
    receipt = notes[1] if len(notes) == 2 else ""
    receipt = re.sub(r'"tracker_version":"[0-9.]+"', '"tracker_version":"X"', receipt)   # compact JSON
    return (str(getattr(rec.status, "value", rec.status)), rec.outcome, rec.realized_roi, notes[0], receipt)


SG_DEC = ("D05.SI", _at(2026, 12, 24, 5, 0), _at(2026, 12, 24, 9, 0), _at(2026, 12, 24, 9, 30))
SG_JAN = ("D05.SI", _at(2027, 1, 8, 5, 0), _at(2027, 1, 8, 9, 0), _at(2027, 1, 8, 9, 30))
SG_CNY = ("D05.SI", _at(2027, 2, 8, 3, 0), _at(2027, 2, 9, 9, 0), _at(2027, 2, 9, 9, 30))


def _x5a_on(tp, monkeypatch):
    _set_gate(monkeypatch, "on")
    rec, receipt = audit(tp, monkeypatch, *SG_DEC)
    assert rec.status == tp.PerformanceStatus.MATURED and rec.outcome == "WIN", (rec.status, receipt)
    assert receipt.get("calendar") == "XSES" and receipt.get("target_session") == "2026-12-24", receipt
    assert receipt.get("target_session_close") == "2026-12-24T09:00:00+00:00", receipt


def test_x5a_singapore_record_due_on_24_dec_2026(monkeypatch):
    tp = tracker()
    _set_gate(monkeypatch, None)
    rec, receipt = audit(tp, monkeypatch, *SG_DEC)
    assert rec.status == tp.PerformanceStatus.ACTIVE, (rec.status, receipt)             # the defect, gate off
    assert receipt.get("reason") == "outcome_calendar_unknown", receipt
    _x5a_on(tp, monkeypatch)


def test_x5b_singapore_record_due_in_january_2027(monkeypatch):
    tp = tracker()
    _set_gate(monkeypatch, None)
    rec, receipt = audit(tp, monkeypatch, *SG_JAN)
    assert rec.status == tp.PerformanceStatus.ACTIVE and receipt.get("reason") == "outcome_calendar_unknown", receipt
    _set_gate(monkeypatch, "on")
    rec, receipt = audit(tp, monkeypatch, *SG_JAN)
    assert rec.status == tp.PerformanceStatus.MATURED, (rec.status, receipt)
    assert receipt.get("target_session") == "2027-01-08", receipt


def test_x5c_singapore_holiday_target_moves_to_the_next_session(monkeypatch):
    tp = tracker()
    _set_gate(monkeypatch, "on")
    rec, receipt = audit(tp, monkeypatch, *SG_CNY)
    assert rec.status == tp.PerformanceStatus.MATURED, (rec.status, receipt)
    assert receipt.get("target_session") == "2027-02-09", receipt                        # 8 Feb 2027 is closed
    assert receipt.get("target_session_close") == "2027-02-09T09:00:00+00:00", receipt


ORDINARY = [("AAPL.US", _at(2026, 12, 23, 18, 0), _at(2026, 12, 23, 21, 0), _at(2026, 12, 23, 22, 0)),
            ("2222.SR", _at(2026, 12, 23, 9, 0), _at(2026, 12, 23, 12, 0), _at(2026, 12, 23, 13, 0)),
            ("VOD.L", _at(2026, 12, 22, 12, 0), _at(2026, 12, 22, 16, 30), _at(2026, 12, 22, 17, 0))]


@pytest.mark.parametrize("sym,target,quote,now", ORDINARY)
def test_x5d_other_venues_identical_on_and_off(monkeypatch, sym, target, quote, now):
    tp = tracker()
    states = []
    for word in (None, "on"):
        _set_gate(monkeypatch, word)
        rec, _r = audit(tp, monkeypatch, sym, target, quote, now)
        states.append(_state(tp, rec))
    assert states[0] == states[1]
    assert states[0][0].upper().startswith("MATURED"), states[0]


# ------------------------------------------------------------------- X6 ----
class _Clock(datetime):
    fixed = None

    @classmethod
    def now(cls, tz=None):
        return cls.fixed if tz is None else cls.fixed.astimezone(tz)


def _fresh(ob, monkeypatch, symbol, quote_at, now):
    monkeypatch.delenv("TFB_TICKET_MAX_QUOTE_AGE_MIN", raising=False)
    monkeypatch.delenv("TFB_TICKET_VENUE_CAL_EXT", raising=False)
    _Clock.fixed = now
    monkeypatch.setattr(ob, "datetime", _Clock)
    ob._VENUE_CAL_CACHE.clear()
    cand = {"symbol": symbol, "quote_evidence": {"status": "SUCCESS", "quote_asof": quote_at.isoformat(),
                                                 "acquired_at": now.isoformat()}}
    return ob._quote_freshness_assessment(cand)


SG_HOLIDAY = ("D05.SI", _at(2026, 12, 31, 9, 0), _at(2027, 1, 1, 6, 0))


def _x6b_on(ob, monkeypatch):
    _set_gate(monkeypatch, "on")
    ok, text, detail = _fresh(ob, monkeypatch, *SG_HOLIDAY)
    assert ok is True and text.startswith("last close"), (ok, text, detail)
    assert detail.get("mode") == "session_closed" and detail.get("prev_close") == "2026-12-31T09:00:00+00:00", detail


def test_x6a_builder_venue_state_after_2026(monkeypatch):
    ob = builder()
    ob._VENUE_CAL_CACHE.clear()
    _set_gate(monkeypatch, None)
    assert ob._venue_state("D05.SI", _at(2027, 1, 4, 3, 0)) is None                    # the defect, gate off
    assert ob._venue_state("D05.SI", _at(2026, 12, 30, 3, 0)) is not None
    _set_gate(monkeypatch, "on")
    is_open, prev_close = ob._venue_state("D05.SI", _at(2027, 1, 4, 3, 0))
    assert is_open is True and prev_close == _at(2026, 12, 31, 9, 0)


def test_x6b_builder_certifies_the_last_close_on_a_2027_holiday(monkeypatch):
    ob = builder()
    _set_gate(monkeypatch, None)
    ok, text, detail = _fresh(ob, monkeypatch, *SG_HOLIDAY)
    assert ok is False and "venue calendar unavailable" in text, (ok, text)             # the defect, gate off
    assert detail.get("mode") == "calendar_unavailable", detail
    _x6b_on(ob, monkeypatch)


@pytest.mark.parametrize("symbol,now", [("AAPL.US", _at(2027, 1, 4, 3, 0)), ("2222.SR", _at(2027, 1, 4, 3, 0)),
                                        ("AAPL.US", _at(2026, 12, 26, 3, 0))])
def test_x6c_other_venue_states_identical_on_and_off(monkeypatch, symbol, now):
    ob = builder()
    states = []
    for word in (None, "on"):
        _set_gate(monkeypatch, word)
        ob._VENUE_CAL_CACHE.clear()
        states.append(ob._venue_state(symbol, now))
    assert states[0] == states[1] and states[0] is not None


# ----------------------------------------------------------------- GOLD ----
def test_x7_golden_negative_on_legs_fail_on_the_base(monkeypatch):
    tpb, obb = tracker_base(), builder_base()
    with pytest.raises(AssertionError):
        _x5a_on(tpb, monkeypatch)
    with pytest.raises(AssertionError):
        _x6b_on(obb, monkeypatch)


# ------------------------------------------------------------------- X8 ----
STEP_NAMES = {".github/workflows/track_performance.yml": "Run performance clock",
              ".github/workflows/daily_sync.yml": "Track Performance (record + audit)"}
MAPPING = "TFB_XCAL_DATA_EXT: ${{ vars.TFB_XCAL_DATA_EXT || 'off' }}"


def _tracker_step(rel):
    text = (ROOT / rel).read_text(encoding="utf-8")
    parts = re.split(r"^\s{6}- name: ", text, flags=re.M)
    hits = [p for p in parts if p.splitlines() and STEP_NAMES[rel] in p.splitlines()[0]]
    assert len(hits) == 1, (rel, len(hits))
    return text, hits[0]


def test_x8_env_lane_parity_reader_and_ci():
    lines = {}
    for rel in STEP_NAMES:
        text, step = _tracker_step(rel)
        everywhere = [ln.strip() for ln in text.splitlines() if re.match(r"^\s+%s:" % GATE, ln)]
        in_step = [ln.strip() for ln in step.splitlines() if re.match(r"^\s+%s:" % GATE, ln)]
        assert everywhere == [MAPPING] and in_step == [MAPPING], (rel, everywhere, in_step)
        lines[rel] = in_step
    assert len(set(map(tuple, lines.values()))) == 1                      # ENV PARITY
    src = (ROOT / "core" / "exchange_calendar_ext.py").read_text(encoding="utf-8")
    m = re.search(r"^def calendar_ext_enabled\(\)[^\n]*:\n(.*?)(?=^def |^@|\Z)", src, re.M | re.S)
    assert m and re.search(r'os\.getenv\("%s"\)\s*or\s*"off"' % GATE, m.group(1))
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    heavy = ci.split("\n  heavy:\n", 1)[1].split("\n  summary:\n", 1)[0]
    step = heavy.split("- name: Singapore calendar data through 2027", 1)[1].split("\n      - name: ", 1)[0]
    assert re.search(r"python -m pytest -q tests/test_xcal_data_ext\.py", step)
    assert "git fetch --no-tags --depth=1 origin %s" % BASE_FULL in step
    assert re.search(r'^\s+TFB_TEST_REQUIRE_BASE: "1"$', step, re.M), step
    assert BASE_FULL.startswith(BASE_SHA)


# ------------------------------------------------------------------- V -----
def test_v1_versions_pins_title_why_ascii():
    tp, ob, e = tracker(), builder(), ext()
    assert _vt(tp.SCRIPT_VERSION) >= (6, 58, 0)
    assert _vt(ob.OPPORTUNITY_BUILDER_VERSION) >= (1, 28, 3)
    assert _vt(e.CALENDAR_EXT_VERSION) >= (1, 0, 0)
    src = Path(tp.__file__).read_text(encoding="utf-8")
    assert "(v%s)" % tp.SCRIPT_VERSION in src.splitlines()[4]
    i58 = src.index("# v6.58.0 (2026-11-06) [XCAL-DATA-EXT]")
    assert src.index("# v6.53.0 (2026-11-02) [ARCHIVE-BAR + DUE-ONLY]") < i58 < src.index("\nSCRIPT_VERSION = ")
    obs = Path(ob.__file__).read_text(encoding="utf-8")
    j = obs.index("# v1.28.3 (2026-11-06) [XCAL-DATA-EXT]")
    assert obs.index("# v1.28.0 (2026-10-27) - RANKING CHALLENGER IN SHADOW") < j < obs.index("\nOPPORTUNITY_BUILDER_VERSION = ")
    vd = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("track_performance", "%s", "track performance")' % tp.SCRIPT_VERSION in vd
    assert re.search(r'\("core\.analysis\.opportunity_builder", "OPPORTUNITY_BUILDER_VERSION",\s*"%s"'
                     % re.escape(ob.OPPORTUNITY_BUILDER_VERSION), vd)
    assert '("core.exchange_calendar_ext", "CALENDAR_EXT_VERSION", "%s",' % e.CALENDAR_EXT_VERSION in vd
    for path in (Path(e.__file__), Path(__file__)):
        assert all(ord(c) < 128 for c in path.read_text(encoding="utf-8")), path


def _diff(old_text, new_text):
    import difflib
    diff = list(difflib.unified_diff(old_text.splitlines(), new_text.splitlines(), lineterm="", n=0))
    added = [ln[1:] for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
    removed = [ln[1:] for ln in diff if ln.startswith("-") and not ln.startswith("---")]
    return added, removed


def _defs(text):
    import ast
    kinds = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
    return {n.name for n in ast.walk(ast.parse(text)) if isinstance(n, kinds)}


def test_v1b_against_the_base_ascii_and_functions():
    tp, ob = tracker(), builder()
    tpb, obb = tracker_base(), builder_base()
    src = Path(tp.__file__).read_text(encoding="utf-8")
    obs = Path(ob.__file__).read_text(encoding="utf-8")
    if (tp.SCRIPT_VERSION != "6.58.0" or re.search(r"^# v6\.5[4-7]\.0 \(", src, re.M)
            or ob.OPPORTUNITY_BUILDER_VERSION != "1.28.3" or re.search(r"^# v1\.28\.[12] \(", obs, re.M)):
        pytest.skip("the exact a85edfc6 -> 6.58.0 / 1.28.3 diff checks apply to this PR's diff alone")
    old = Path(tpb.__file__).read_text(encoding="utf-8")
    added, removed = _diff(old, src)
    title = "TADAWUL FAST BRIDGE \u2013 ADVANCED PERFORMANCE ANALYTICS ENGINE (v%s)"
    assert sorted(removed) == sorted([title % "6.53.0", 'SCRIPT_VERSION = "6.53.0"',
                                      "    cal = calendars.get_calendar(name)"]), removed
    fresh = [ln for ln in added if ln != title % "6.58.0"]
    assert fresh and all(ln.isascii() for ln in fresh), [ln for ln in fresh if not ln.isascii()][:3]
    assert _defs(src) - _defs(old) == {"_outcome_calendar_ext"} and not _defs(old) - _defs(src)
    old_ob = Path(obb.__file__).read_text(encoding="utf-8")
    added, removed = _diff(old_ob, obs)
    assert removed == ['OPPORTUNITY_BUILDER_VERSION = "1.28.0"'], removed
    assert added and all(ln.isascii() for ln in added), [ln for ln in added if not ln.isascii()][:3]
    assert _defs(obs) - _defs(old_ob) == {"_venue_cal_data_ext"} and not _defs(old_ob) - _defs(obs)


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
