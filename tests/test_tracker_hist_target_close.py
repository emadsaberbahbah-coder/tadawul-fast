#!/usr/bin/env python3
"""tests/test_tracker_hist_target_close.py - track_performance v6.47.0
TRACK_HIST_TARGET_CLOSE: an overdue ACTIVE record matures on the witnessed close
of EXACTLY its target session, and on nothing else.

WHY (2026-10-21)
----------------
Since v6.43.0 a record matures only on a quote from its exact target session.
Once that session has passed, every later audit sees a later session's quote,
the reason is 'quote_outside_target_session', and the record stays ACTIVE for
good (acceptance D10-3b on 2026-10-21: 3,208 overdue, matured flat at 11,791).
v6.47.0 adds a default-off path that fetches the close of that one session from
a separate daily-bar pull and, under enforce, sends it down the UNCHANGED
maturation path. This harness drives the REAL audit_active_records loop.

Substituted: the clock, the lean exchange-calendar fixture (the same one
tests/test_outcome_price_evidence.py uses), the backend transport (AsyncMock),
the corporate-action ledger index (empty) and the daily-bar pull
(_hist_close_pull returns fixture bars). No provider, workbook or network.

Cases
  H1  enforce matures on the bar close (WIN and LOSS), not on today's quote
  H2  off (unset and 7 non-word spellings) never pulls and changes nothing
  H3  observe pulls and logs one line; records equal off
  H4  every refusal keeps the record ACTIVE (never EXPIRED), even past grace
  H5  only 'quote_outside_target_session' triggers a pull
  H6  own cache + budget: one pull per symbol, _CA_BARS_CACHE untouched,
      a spent budget DEFERS
  H7  a split between entry and target is left to the unchanged CA-GUARD
  H8  pure helper table (_hist_target_close)
  H9  the CA-GUARD's 240 s window is not spent by these pulls (observe == off,
      enforce keeps a later CA-verified record maturing)
  H10 failed pulls are counted apart; the breaker stops pulling for the pass
  H11 a substituted record the CA-GUARD leaves pending keeps its live mark
  H12 the REAL _hist_close_pull over a fake yfinance frame, including an
      event yfinance merged onto the target-session bar
  H13 the REAL yfinance 0.2.66 stack over a stubbed transport: an outage is a
      failed pull and opens the breaker; a delisted answer is an empty answer
  N1  off == the v6.45.0 base copy on a mixed fixture (records, Notes, stats,
      log lines); observe == off on records, stats and logs bar one line
  N2  golden negative: the base copy leaves the H1 record ACTIVE under enforce
  N3  workflow mapping, version floor and verifier pin
  N4  added lines are ASCII

Golden negative: TFB_TEST_TP_DELIVERED=<base copy> runs every case against
v6.45.0 as the delivered module (all but N1/N2 fail).
Base copy: TFB_TEST_TP_BASE=<path to main 55b47f24 scripts/track_performance.py>,
else `git show 55b47f24:scripts/track_performance.py`; N1/N2/N4 skip without it.

Run:  python3 -m pytest -q tests/test_tracker_hist_target_close.py
  or  python3 tests/test_tracker_hist_target_close.py   (PASS k/k + digest)
"""
from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import hashlib
import importlib.util
import json
import logging
import math
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
sys.path.insert(0, str(ROOT))
TRACKER = ROOT / "scripts/track_performance.py"
# Golden-negative / mutation runs only: load another copy as the delivered module.
DELIVERED = Path(os.getenv("TFB_TEST_TP_DELIVERED", "").strip() or TRACKER)
BASE_SHA = "55b47f24"
GATES = ("TRACK_HIST_TARGET_CLOSE", "TRACK_HIST_CLOSE_BUDGET", "TRACK_HIST_CLOSE_TIME_SEC")

UTC = timezone.utc
NOW = datetime(2026, 10, 8, 21, tzinfo=UTC)          # Thu, after the US close
CLOSE = datetime(2026, 10, 8, 20, tzinfo=UTC)        # today's session close
TARGET = CLOSE - timedelta(days=7)                   # Thu 2026-10-01 20:00Z
SESSION = "2026-10-01"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


tp = _load("tp_hist_close", DELIVERED)
_BASE = {}


def base():
    """The v6.45.0 copy (main 55b47f24), or skip."""
    if "mod" not in _BASE:
        path = os.getenv("TFB_TEST_TP_BASE", "").strip()
        if not path:
            try:
                text = subprocess.run(["git", "show", BASE_SHA + ":scripts/track_performance.py"],
                                      cwd=ROOT, capture_output=True, text=True, check=True).stdout
            except Exception:
                text = ""
            if text:
                tmp = Path(tempfile.mkdtemp()) / "tp_v6450_base.py"
                tmp.write_text(text, encoding="utf-8")
                path = str(tmp)
        _BASE["mod"] = _load("tp_hist_close_base", path) if path else None
    if _BASE["mod"] is None:
        pytest.skip("base copy unavailable (set TFB_TEST_TP_BASE)")
    return _BASE["mod"]


def calendar_fixture(name, day):
    """Lean offline sessions (copy of tests/test_outcome_price_evidence.py)."""
    around = datetime.fromisoformat(day).replace(tzinfo=UTC)
    out = []
    for offset in range(-14, 15):
        date = around + timedelta(days=offset)
        if (date.weekday() in (4, 5) if name == "XSAU" else date.weekday() >= 5):
            continue
        opening = date.replace(hour=7 if name == "XSAU" else 13, minute=0 if name == "XSAU" else 30)
        closing = date.replace(hour=12 if name == "XSAU" else 20, minute=0)
        out.append((date.date().isoformat(), opening, closing))
    return tuple(out)


def row(symbol="SYNTH.US", price=110.0, quote=CLOSE, acquired=NOW):
    return {"symbol": symbol, "current_price": price, "data_provider": "yahoo_chart",
            "acquisition_status": "success", "acquisition_provider": "yahoo_chart",
            "acquisition_acquired_at": acquired.isoformat(),
            "acquisition_quote_asof": quote.isoformat()}


def record(mod, symbol="SYNTH.US", target=TARGET, entry=100.0, horizon="WEEK_1"):
    return mod.PerformanceRecord(
        record_id="synthetic-%s-%s" % (symbol, horizon), symbol=symbol,
        horizon=getattr(mod.HorizonType, horizon), date_recorded=target - timedelta(days=7),
        entry_price=entry, entry_recommendation=mod.RecommendationType.BUY, entry_score=70,
        entry_risk_bucket="MEDIUM", entry_confidence="HIGH", origin_tab="synthetic",
        target_price=110, target_roi=10, target_date=target,
        status=mod.PerformanceStatus.ACTIVE, last_updated=NOW)


def bars(target_close=104.0, extra=(), drop_target=False):
    """Weekday daily bars 2026-09-21..2026-10-08 (US dates), close 100 + i."""
    out = []
    day = datetime(2026, 9, 21)
    i = 0
    while day <= datetime(2026, 10, 8):
        if day.weekday() < 5:
            d = day.strftime("%Y-%m-%d")
            if d == SESSION:
                if not drop_target:
                    out.append({"date": d, "close": target_close, "split": 0.0})
            else:
                out.append({"date": d, "close": 100.0 + i, "split": 0.0})
            i += 1
        day += timedelta(days=1)
    out.extend(extra)
    return out


class Run:
    def __init__(self):
        self.pulls = []
        self.logs = []


@contextlib.contextmanager
def harness(mod, env=None, bar_map=None, ca_guard=False, patches=None, now=NOW):
    """Swap module attributes and gate ENVs; restore everything afterwards."""
    run = Run()
    saved_env = {k: os.environ.get(k) for k in GATES}
    for k in GATES:
        os.environ.pop(k, None)
    for k, v in (env or {}).items():
        os.environ[k] = v

    def fake_pull(symbol):
        run.pulls.append(symbol)
        src = (bar_map or {}).get(symbol, [])
        return [dict(b) for b in src]

    swaps = {"_utc_now": lambda: now,
             "_outcome_calendar_schedule": calendar_fixture,
             "_price_fallback_enabled": lambda: False,
             "_ca_guard_enabled": lambda: ca_guard,
             "_feed_legacy": lambda: True,
             "_ca_ledger_index": lambda: {},
             "urlopen": lambda *a, **k: (_ for _ in ()).throw(AssertionError("live transport"))}
    if hasattr(mod, "_hist_close_pull"):
        swaps["_hist_close_pull"] = fake_pull
    swaps.update(patches or {})
    saved = {k: getattr(mod, k) for k in swaps}
    saved_now = mod.RiyadhTime.__dict__["now"]
    handler = logging.Handler()
    handler.emit = lambda rec: run.logs.append(rec.getMessage())
    mod.logger.addHandler(handler)
    saved_level = mod.logger.level
    mod.logger.setLevel(logging.INFO)    # pytest leaves the root at WARNING
    try:
        for k, v in swaps.items():
            setattr(mod, k, v)
        mod.RiyadhTime.now = staticmethod(lambda: now)
        yield run
    finally:
        mod.logger.setLevel(saved_level)
        mod.logger.removeHandler(handler)
        mod.RiyadhTime.now = saved_now
        for k, v in saved.items():
            setattr(mod, k, v)
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def audit(mod, records, rows, **kw):
    with harness(mod, **kw) as run:
        app = mod.PerformanceTrackerApp.__new__(mod.PerformanceTrackerApp)
        app.backend = mod.BackendClient("https://transport.invalid")
        app.backend.post_json = AsyncMock(return_value=({"rows": rows}, None, 200))
        asyncio.run(app.audit_active_records(records))
        run.stats = dict(getattr(app, "_last_audit_stats", {}) or {})
    return run


def receipt(mod, rec):
    return json.loads(rec.notes.rsplit(mod._OUTCOME_RECEIPT, 1)[1])


def state(rec):
    """Every record field, with the tracker_version inside Notes normalised so
    a v6.45.0 and a v6.47.0 receipt compare equal when nothing else differs."""
    d = dataclasses.asdict(rec)
    d["notes"] = re.sub(r'"tracker_version":"[0-9.]+"', '"tracker_version":"X"', d["notes"] or "")
    return json.loads(json.dumps(d, default=str, sort_keys=True))


def hist_lines(run):
    return [m for m in run.logs if m.startswith("[HIST-CLOSE v")]


# ---------------------------------------------------------------- H1 -----
def test_h1_enforce_matures_on_the_target_session_close():
    for close, outcome in ((104.0, "WIN"), (95.0, "LOSS"), (100.0, "BREAKEVEN")):
        _h1_case(close, outcome)


def _h1_case(close, outcome):
    tp._LAST_HIST_CLOSE.clear()
    rec = record(tp)
    run = audit(tp, [rec], [row()], env={"TRACK_HIST_TARGET_CLOSE": "enforce"},
                bar_map={"SYNTH.US": bars(close)})
    roi = (close / 100.0 - 1.0) * 100.0
    assert rec.status == tp.PerformanceStatus.MATURED and rec.outcome == outcome
    assert rec.realized_roi == pytest.approx(roi)
    assert rec.realized_roi != pytest.approx(10.0)          # not today's 110 print
    assert rec.current_price == close and rec.unrealized_roi == pytest.approx(roi)
    proof = receipt(tp, rec)
    assert proof["state"] == "MATURED" and proof["reason"] == ""
    assert proof["quote_kind"] == "historical_daily_close"
    assert proof["provider"] == "yfinance-history" and proof["hist_provider"] == "yfinance-history"
    assert proof["quote_asof"] == TARGET.isoformat() and proof["target_session_close"] == TARGET.isoformat()
    assert proof["target_session"] == proof["quote_session"] == proof["hist_bar_date"] == SESSION
    assert proof["current_quote_session"] == "2026-10-08" and proof["current_quote_price"] == 110.0
    assert proof["current_quote_asof"] == CLOSE.isoformat()
    assert proof["actual_horizon_days"] == pytest.approx(7.0)
    assert proof["price"] == close and proof["realized_roi_pp"] == pytest.approx(roi)
    assert proof["tracker_version"] == tp.SCRIPT_VERSION
    assert run.pulls == ["SYNTH.US"]
    assert run.stats["matured"] == 1 and run.stats["outcome_skipped_reasons"] == {}
    assert tp._LAST_HIST_CLOSE == {"mode": "enforce", "candidates": 1, "witnessed": 1,
                                   "by_horizon": {"1W": 1}, "refused": {}, "matured": 1,
                                   "pulls": 1, "ok": 1, "failed": 0, "breaker": False,
                                   "first_error": "", "deferred": 0}
    assert hist_lines(run) == ["[HIST-CLOSE v6.47.0] mode=enforce candidates=1 witnessed=1 "
                               "(1W:1) matured=1 refused=0 (-) pulls=1 ok=1 failed=0 "
                               "breaker=closed deferred=0"]


# ---------------------------------------------------------------- H2 -----
def test_h2_off_never_pulls_and_changes_nothing():
    tp._LAST_HIST_CLOSE.clear()
    tp._LAST_HIST_CLOSE["sentinel"] = 1
    for spelling in (None, "", "off", "1", "true", "on", "yes", "enforced"):
        rec = record(tp)
        env = {} if spelling is None else {"TRACK_HIST_TARGET_CLOSE": spelling}
        run = audit(tp, [rec], [row()], env=env, bar_map={"SYNTH.US": bars()})
        assert rec.status == tp.PerformanceStatus.ACTIVE and rec.realized_roi is None, spelling
        proof = receipt(tp, rec)
        assert proof["reason"] == "quote_outside_target_session", spelling
        assert not any(k.startswith(("hist_", "current_quote")) for k in proof), spelling
        assert run.pulls == [] and hist_lines(run) == [], spelling
        assert run.stats["outcome_skipped_reasons"] == {"quote_outside_target_session": 1}
    assert tp._LAST_HIST_CLOSE == {"sentinel": 1}
    assert tp._hist_target_close_mode() == "off"


# ---------------------------------------------------------------- H3 -----
def test_h3_observe_measures_only():
    off_rec, obs_rec = record(tp), record(tp)
    audit(tp, [off_rec], [row()], bar_map={"SYNTH.US": bars()})
    run = audit(tp, [obs_rec], [row()], env={"TRACK_HIST_TARGET_CLOSE": " Observe "},
                bar_map={"SYNTH.US": bars()})
    assert state(obs_rec) == state(off_rec)
    assert obs_rec.status == tp.PerformanceStatus.ACTIVE
    assert run.pulls == ["SYNTH.US"]
    assert hist_lines(run) == ["[HIST-CLOSE v6.47.0] mode=observe candidates=1 witnessed=1 "
                               "(1W:1) matured=0 refused=0 (-) pulls=1 ok=1 failed=0 "
                               "breaker=closed deferred=0"]
    assert tp._LAST_HIST_CLOSE["witnessed"] == 1 and tp._LAST_HIST_CLOSE["matured"] == 0


def test_h3b_observe_on_the_refusal_path_equals_off():
    for label, bar_list, reason in REFUSALS:
        off_rec, obs_rec = record(tp), record(tp)
        off = audit(tp, [off_rec], [row()], bar_map={"SYNTH.US": bar_list})
        obs = audit(tp, [obs_rec], [row()], env={"TRACK_HIST_TARGET_CLOSE": "observe"},
                    bar_map={"SYNTH.US": bar_list})
        assert state(obs_rec) == state(off_rec), label
        assert "hist_close_reason" not in receipt(tp, obs_rec), label
        assert obs.stats == off.stats, label
        assert [m for m in obs.logs if not m.startswith("[HIST-CLOSE v")] == off.logs, label
        assert tp._LAST_HIST_CLOSE["refused"] == {reason: 1}, label


# ---------------------------------------------------------------- H4 -----
REFUSALS = [
    ("no bars", [], "hist_close_unavailable"),
    ("bar missing", bars(drop_target=True), "hist_close_bar_missing"),
    ("conflict", bars(extra=[{"date": SESSION, "close": 105.0, "split": 0.0}]), "hist_close_bar_conflict"),
    ("zero close", bars(0.0), "hist_close_invalid"),
    ("negative close", bars(-1.0), "hist_close_invalid"),
    ("nan close", bars(float("nan")), "hist_close_invalid"),
    ("inf close", bars(float("inf")), "hist_close_invalid"),
    ("bool close", bars(True), "hist_close_invalid"),
    ("split after", bars(extra=[{"date": "2026-10-05", "close": 52.0, "split": 2.0}]),
     "hist_close_post_target_split"),
    ("bonus after", bars(extra=[{"date": "2026-10-06", "close": 95.0, "split": 1.1}]),
     "hist_close_post_target_split"),
    ("unreadable split", bars(extra=[{"date": "2026-10-07", "close": 104.0, "split": float("nan")}]),
     "hist_close_post_target_split"),
    ("subunit x100", bars(10400.0), "hist_close_scale_mismatch"),
    ("event merged onto the target bar",
     [b if b["date"] != SESSION else dict(b, close=94.5454, split=1.1) for b in bars()],
     "hist_close_post_target_split"),
]


def test_h4_every_refusal_keeps_the_record_active():
    for label, bar_list, reason in REFUSALS:
        for later in (False, True):
            rec = record(tp)
            patches = {"_mature_grace_days": lambda: 0} if later else None
            run = audit(tp, [rec], [row()], env={"TRACK_HIST_TARGET_CLOSE": "enforce"},
                        bar_map={"SYNTH.US": bar_list}, patches=patches)
            assert rec.status == tp.PerformanceStatus.ACTIVE, label
            assert rec.realized_roi is None and rec.outcome is None, label
            assert rec.current_price == 110.0, label                  # v6.45.0 value
            proof = receipt(tp, rec)
            assert proof["state"] == "PENDING" and proof["reason"] == "quote_outside_target_session", label
            assert proof["hist_close_reason"] == reason, label
            assert proof["quote_asof"] == CLOSE.isoformat(), label     # still the current quote
            assert run.stats["outcome_skipped_reasons"] == {"quote_outside_target_session": 1}, label
            assert tp._LAST_HIST_CLOSE["refused"] == {reason: 1}, label


def test_h4b_confirmed_ledger_action_after_target_refuses():
    calls = []

    def ledger(symbol, entry_date, target_session=None):
        calls.append((symbol, str(entry_date)[:10], target_session))
        return 0.5 if (str(entry_date)[:10] == SESSION and target_session is None) else None
    rec = record(tp)
    audit(tp, [rec], [row()], env={"TRACK_HIST_TARGET_CLOSE": "enforce"},
          bar_map={"SYNTH.US": bars()}, patches={"_ca_ledger_factor": ledger})
    assert rec.status == tp.PerformanceStatus.ACTIVE
    assert receipt(tp, rec)["hist_close_reason"] == "hist_close_post_target_action"
    assert ("SYNTH.US", SESSION, None) in calls


# ---------------------------------------------------------------- H5 -----
def test_h5_only_quote_outside_target_session_triggers():
    cases = [([], TARGET, "price_unavailable"),
             ([row(quote=CLOSE - timedelta(days=1))], CLOSE - timedelta(hours=4), "quote_session_stale"),
             ([row(quote=CLOSE - timedelta(seconds=1))], CLOSE - timedelta(hours=4), "quote_before_target_close")]
    for rows, target, reason in cases:
        off_rec, on_rec = record(tp, target=target), record(tp, target=target)
        audit(tp, [off_rec], rows, bar_map={"SYNTH.US": bars()})
        run = audit(tp, [on_rec], rows, env={"TRACK_HIST_TARGET_CLOSE": "enforce"},
                    bar_map={"SYNTH.US": bars()})
        assert run.pulls == [], reason
        assert receipt(tp, on_rec)["reason"] == reason
        assert state(on_rec) == state(off_rec), reason
        assert tp._LAST_HIST_CLOSE["candidates"] == 0, reason


def test_h5b_target_session_not_complete_never_pulls():
    before = CLOSE - timedelta(minutes=1)
    rec = record(tp, target=CLOSE - timedelta(hours=4))
    run = audit(tp, [rec], [row(quote=before - timedelta(minutes=1), acquired=before)],
                env={"TRACK_HIST_TARGET_CLOSE": "enforce"}, bar_map={"SYNTH.US": bars()}, now=before)
    proof = receipt(tp, rec)
    assert proof["reason"] == "target_session_not_complete" and proof["state"] == "PENDING"
    assert rec.status == tp.PerformanceStatus.ACTIVE
    assert run.pulls == [] and tp._LAST_HIST_CLOSE["candidates"] == 0


# ---------------------------------------------------------------- H6 -----
def test_h6_own_cache_and_budget():
    recs = [record(tp, horizon=h) for h in ("WEEK_1", "WEEK_2", "MONTH_1")]
    for r in recs[1:]:
        r.date_recorded = TARGET - timedelta(days=14)
    tp._CA_BARS_CACHE.clear()
    tp._CA_BUDGET.update({"pulls": 0, "t0": 0.0, "deferred": 0})
    run = audit(tp, recs, [row()], env={"TRACK_HIST_TARGET_CLOSE": "enforce"},
                bar_map={"SYNTH.US": bars()})
    assert run.pulls == ["SYNTH.US"]                                   # one pull per symbol
    assert all(r.status == tp.PerformanceStatus.MATURED for r in recs)
    assert tp._CA_BARS_CACHE == {} and tp._CA_BUDGET["pulls"] == 0     # CA-GUARD state untouched
    assert list(tp._HIST_CLOSE_CACHE) == ["SYNTH"]
    assert tp._LAST_HIST_CLOSE["by_horizon"] == {"1M": 1, "1W": 1, "2W": 1}


def test_h6b_spent_budget_defers_never_expires():
    a, b = record(tp, symbol="AAA.US"), record(tp, symbol="BBB.US")
    run = audit(tp, [a, b], [row(symbol="AAA.US"), row(symbol="BBB.US")],
                env={"TRACK_HIST_TARGET_CLOSE": "enforce", "TRACK_HIST_CLOSE_BUDGET": "1"},
                bar_map={"AAA.US": bars(), "BBB.US": bars()},
                patches={"_mature_grace_days": lambda: 0})
    assert run.pulls == ["AAA.US"]
    assert a.status == tp.PerformanceStatus.MATURED
    assert b.status == tp.PerformanceStatus.ACTIVE and b.outcome is None
    assert receipt(tp, b)["hist_close_reason"] == "hist_close_budget_exhausted"
    assert tp._LAST_HIST_CLOSE["deferred"] == 1 and tp._LAST_HIST_CLOSE["pulls"] == 1


def test_h6c_wall_clock_budget_defers():
    clock = FakeClock()

    def slow_pull(symbol):
        clock.t += 150.0
        return bars()
    a, b = record(tp, symbol="AAA.US"), record(tp, symbol="BBB.US")
    pulled = []

    def counting(symbol):
        pulled.append(symbol)
        return slow_pull(symbol)
    audit(tp, [a, b], [row(symbol="AAA.US"), row(symbol="BBB.US")],
          env={"TRACK_HIST_TARGET_CLOSE": "enforce", "TRACK_HIST_CLOSE_TIME_SEC": "100"},
          patches={"time": _clock_module(clock), "_hist_close_pull": counting})
    assert pulled == ["AAA.US"]
    assert a.status == tp.PerformanceStatus.MATURED
    assert b.status == tp.PerformanceStatus.ACTIVE
    assert receipt(tp, b)["hist_close_reason"] == "hist_close_budget_exhausted"
    assert tp._LAST_HIST_CLOSE["deferred"] == 1


# ---------------------------------------------------------------- H7 -----
def test_h7_split_between_entry_and_target_goes_through_ca_guard():
    seen = []

    def adjusted(symbol, entry_date, target_session=None):
        seen.append((symbol, target_session))
        return 4.0
    rec = record(tp)
    split_bars = bars(52.0, extra=[{"date": "2026-09-28", "close": 51.0, "split": 2.0}])
    audit(tp, [rec], [row(price=55.0)], env={"TRACK_HIST_TARGET_CLOSE": "enforce"},
          bar_map={"SYNTH.US": split_bars}, ca_guard=True,
          patches={"_ca_adjusted_roi": adjusted, "_ca_ledger_factor": lambda *a, **k: None})
    assert seen == [("SYNTH.US", SESSION)]
    assert rec.status == tp.PerformanceStatus.MATURED and rec.outcome == "WIN"
    assert rec.realized_roi == pytest.approx(4.0)
    assert "CA-ADJUST] print -48.0%" in rec.notes
    assert receipt(tp, rec)["roi_basis"] == "adjust"


# ---------------------------------------------------------------- H8 -----
def test_h8_pure_helper_table():
    f = tp._hist_target_close
    assert f(bars(), SESSION) == (104.0, "")
    assert f(bars(), SESSION, 110.0) == (104.0, "")
    assert f(bars(extra=[{"date": SESSION, "close": 104.0, "split": 0.0}]), SESSION) == (104.0, "")
    assert f(bars(extra=[{"date": "2026-10-05", "close": 1.0, "split": 1.0}]), SESSION)[1] == ""
    assert f(bars(extra=[{"date": "2026-09-25", "close": 1.0, "split": 2.0}]), SESSION)[1] == ""
    assert f([{"date": SESSION + " 00:00:00", "close": 9.5}], SESSION) == (9.5, "")
    assert f(bars(10.4), SESSION, 104.0) == (None, "hist_close_scale_mismatch")
    assert f(bars(10.41), SESSION, 104.0) == (10.41, "")
    assert f(None, SESSION) == (None, "hist_close_unavailable")
    assert f(["x", 3], SESSION) == (None, "hist_close_unavailable")
    assert f(bars(), "2026-13-01") == (None, "hist_close_invalid")
    assert f([{"date": SESSION, "close": "abc"}], SESSION) == (None, "hist_close_invalid")
    assert f([{"date": SESSION, "close": None}], SESSION) == (None, "hist_close_invalid")
    assert f(bars(extra=[{"date": "2026-10-05", "close": 1.0, "split": "x"}]),
             SESSION) == (None, "hist_close_post_target_split")
    for word, mode in (("observe", "observe"), ("ENFORCE", "enforce"), (" off ", "off"),
                       ("1", "off"), ("", "off")):
        with harness(tp, env={"TRACK_HIST_TARGET_CLOSE": word}):
            assert tp._hist_target_close_mode() == mode, word
    for raw, want in (("", 60), ("5", 5), ("-3", 0), ("x", 60)):
        with harness(tp, env={"TRACK_HIST_CLOSE_BUDGET": raw}):
            assert tp._hist_close_budget() == want, raw
    for raw, want in (("", 240.0), ("0", 0.0), ("-5", 0.0), ("x", 240.0), ("30.5", 30.5)):
        with harness(tp, env={"TRACK_HIST_CLOSE_TIME_SEC": raw}):
            assert tp._hist_close_time_sec() == want, raw


# ---------------------------------------------------------------- H9 -----
class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def time(self):
        return self.t


def _clock_module(clock):
    import time as _time
    import types
    ns = types.SimpleNamespace(**{k: getattr(_time, k) for k in dir(_time) if not k.startswith("_")})
    ns.time = clock.time
    return ns


def test_h9_ca_guard_window_is_not_spent_by_hist_pulls():
    """A hist pull slower than the whole CA window (300 s > 240 s) sits
    between two in-session +20% records that need CA verification."""
    in_session = CLOSE - timedelta(hours=4)
    rows = [row(symbol="AAA.US", price=120.0), row(), row(symbol="CCC.US", price=120.0)]
    out = {}
    for mode in (None, "observe", "enforce"):
        clock = FakeClock()

        order = []

        def slow_pull(symbol, clock=clock, order=order):
            order.append("hist")
            clock.t += 300.0
            return bars()

        def adjusted(symbol, entry_date, target_session=None, clock=clock, order=order):
            order.append("ca")
            clock.t += 1.0
            return 20.0
        recs = [record(tp, symbol="AAA.US", target=in_session), record(tp),
                record(tp, symbol="CCC.US", target=in_session)]
        env = {} if mode is None else {"TRACK_HIST_TARGET_CLOSE": mode}
        run = audit(tp, recs, rows, env=env, ca_guard=True,
                    patches={"time": _clock_module(clock), "_hist_close_pull": slow_pull,
                             "_ca_adjusted_roi": adjusted, "_ca_ledger_factor": lambda *a, **k: None})
        run.order = order
        out[mode] = (recs, run)
    off_recs, off_run = out[None]
    assert [r.status for r in off_recs] == [tp.PerformanceStatus.MATURED, tp.PerformanceStatus.ACTIVE,
                                           tp.PerformanceStatus.MATURED]
    obs_recs, obs_run = out["observe"]
    assert [state(r) for r in obs_recs] == [state(r) for r in off_recs]
    assert obs_run.stats == off_run.stats
    assert obs_run.order == ["ca", "ca", "hist"]          # observe never interleaves with CA work
    assert out["enforce"][1].order == ["ca", "hist", "ca"]
    enf_recs, enf_run = out["enforce"]
    assert all(r.status == tp.PerformanceStatus.MATURED for r in enf_recs)
    assert enf_run.stats["ca_deferred"] == 0
    assert enf_recs[2].realized_roi == pytest.approx(20.0)


# ---------------------------------------------------------------- H10 ----
def test_h10_failed_pulls_are_counted_and_the_breaker_opens():
    enforce = {"TRACK_HIST_TARGET_CLOSE": "enforce"}
    syms = ["E1.US", "E2.US", "E3.US", "E4.US"]

    def failing(symbol):
        raise RuntimeError("boom")
    recs = [record(tp, symbol=s) for s in syms]
    run = audit(tp, recs, [row(symbol=s) for s in syms], env=enforce,
                patches={"_hist_close_pull": failing, "_mature_grace_days": lambda: 0})
    assert all(r.status == tp.PerformanceStatus.ACTIVE for r in recs)
    assert [receipt(tp, r)["hist_close_reason"] for r in recs] == [
        "hist_close_pull_error"] * 3 + ["hist_close_breaker_open"]
    st = tp._LAST_HIST_CLOSE
    assert (st["pulls"], st["ok"], st["failed"], st["breaker"], st["first_error"], st["deferred"]) == \
        (3, 0, 3, True, "RuntimeError", 1)
    assert hist_lines(run)[0].endswith("pulls=3 ok=0 failed=3 first_error=RuntimeError breaker=open deferred=1")

    class YFRateLimitError(Exception):
        pass

    def limited(symbol):
        raise YFRateLimitError("429")
    recs = [record(tp, symbol=s) for s in syms[:2]]
    audit(tp, recs, [row(symbol=s) for s in syms[:2]], env=enforce, patches={"_hist_close_pull": limited})
    assert [receipt(tp, r)["hist_close_reason"] for r in recs] == [
        "hist_close_pull_error", "hist_close_breaker_open"]
    assert tp._LAST_HIST_CLOSE["failed"] == 1 and tp._LAST_HIST_CLOSE["breaker"] is True
    assert tp._LAST_HIST_CLOSE["first_error"] == "YFRateLimitError"

    calls = []

    def flaky(symbol):
        calls.append(symbol)
        if symbol == "E1.US":
            raise RuntimeError("once")
        return bars()
    recs = [record(tp, symbol="E1.US"), record(tp, symbol="E1.US", horizon="WEEK_2"),
            record(tp, symbol="E2.US")]
    audit(tp, recs, [row(symbol="E1.US"), row(symbol="E2.US")], env=enforce,
          patches={"_hist_close_pull": flaky})
    assert calls == ["E1.US", "E2.US"]                        # a failed symbol is not retried
    assert [r.status for r in recs] == [tp.PerformanceStatus.ACTIVE, tp.PerformanceStatus.ACTIVE,
                                        tp.PerformanceStatus.MATURED]
    st = tp._LAST_HIST_CLOSE
    assert (st["ok"], st["failed"], st["breaker"]) == (1, 1, False)


# ---------------------------------------------------------------- H11 ----
def test_h11_unmatured_substitution_keeps_the_live_mark():
    enforce = {"TRACK_HIST_TARGET_CLOSE": "enforce"}
    rec = record(tp)
    audit(tp, [rec], [row(price=150.0)], env=enforce, bar_map={"SYNTH.US": bars(130.0)}, ca_guard=True,
          patches={"_ca_budget_state": lambda s: "exhausted", "_ca_ledger_factor": lambda *a, **k: None})
    assert rec.status == tp.PerformanceStatus.ACTIVE and rec.outcome is None
    assert rec.current_price == 150.0 and rec.unrealized_roi == pytest.approx(50.0)
    proof = receipt(tp, rec)
    assert proof["reason"] == "corporate_action_verification_pending"
    assert proof["quote_kind"] == "historical_daily_close" and proof["price"] == 130.0
    assert tp._LAST_HIST_CLOSE["witnessed"] == 1 and tp._LAST_HIST_CLOSE["matured"] == 0

    def adjusted_none(symbol, entry_date, target_session=None):
        tp._CA_BARS_CACHE["SYNTH"] = [{"date": SESSION, "close": 130.0}]
        return None
    rec = record(tp)
    audit(tp, [rec], [row(price=150.0)], env=enforce, bar_map={"SYNTH.US": bars(130.0)}, ca_guard=True,
          patches={"_ca_adjusted_roi": adjusted_none, "_ca_ledger_factor": lambda *a, **k: None})
    tp._CA_BARS_CACHE.clear()
    assert rec.status == tp.PerformanceStatus.EXPIRED and rec.outcome == "CORP_ACTION_SUSPECT"
    assert rec.current_price == 150.0 and rec.unrealized_roi == pytest.approx(50.0)


# ---------------------------------------------------------------- H12 ----
def _fake_yfinance(frame, seen, fail=None):
    import types
    mod = types.ModuleType("yfinance")

    class Ticker:
        def __init__(self, sym):
            seen.append(("ticker", sym))

        def history(self, **kw):
            seen.append(("history", kw))
            if fail is not None:
                raise fail
            return frame
    mod.Ticker = Ticker
    return mod


@contextlib.contextmanager
def _yf_installed(module):
    saved = sys.modules.get("yfinance")
    sys.modules["yfinance"] = module
    try:
        yield
    finally:
        if saved is None:
            sys.modules.pop("yfinance", None)
        else:
            sys.modules["yfinance"] = saved


def _frame(pd, merged_split=False):
    days = [b["date"] for b in bars()]
    closes = [b["close"] for b in bars()]
    splits = [0.0] * len(days)
    if merged_split:
        i = days.index(SESSION)
        closes[i], splits[i] = 94.5454, 1.1      # an event dated on a no-bar day, merged back
    idx = pd.DatetimeIndex(days).tz_localize("America/New_York")
    return pd.DataFrame({"Open": closes, "High": closes, "Low": closes, "Close": closes,
                         "Adj Close": closes, "Volume": [1000.0] * len(days),
                         "Dividends": [0.0] * len(days), "Stock Splits": splits}, index=idx)


def test_h12_real_pull_parser_over_a_yfinance_frame():
    pd = pytest.importorskip("pandas")
    real_pull = tp._hist_close_pull
    seen = []
    with _yf_installed(_fake_yfinance(_frame(pd), seen)):
        rows = real_pull("SYNTH.US")
    assert seen[0] == ("ticker", "SYNTH")
    assert seen[1] == ("history", {"period": "1y", "interval": "1d", "auto_adjust": False, "actions": True,
                                   "raise_errors": True})
    assert rows == [{"date": b["date"], "close": b["close"], "split": 0.0} for b in bars()]
    with _yf_installed(_fake_yfinance(pd.DataFrame(), [])):
        assert real_pull("SYNTH.US") == []
    with _yf_installed(_fake_yfinance(None, [], fail=RuntimeError("429"))):
        with pytest.raises(RuntimeError):
            real_pull("SYNTH.US")

    class YFPricesMissingError(Exception):
        pass
    with _yf_installed(_fake_yfinance(None, [], fail=YFPricesMissingError("possibly delisted"))):
        assert real_pull("SYNTH.US") == []                    # Yahoo answered: no rows
    rec = record(tp)
    with _yf_installed(_fake_yfinance(_frame(pd, merged_split=True), [])):
        audit(tp, [rec], [row()], env={"TRACK_HIST_TARGET_CLOSE": "enforce"},
              patches={"_hist_close_pull": real_pull})
    assert rec.status == tp.PerformanceStatus.ACTIVE and rec.outcome is None
    assert receipt(tp, rec)["hist_close_reason"] == "hist_close_post_target_split"
    rec = record(tp)
    with _yf_installed(_fake_yfinance(_frame(pd), [])):
        audit(tp, [rec], [row()], env={"TRACK_HIST_TARGET_CLOSE": "enforce"},
              patches={"_hist_close_pull": real_pull})
    assert rec.status == tp.PerformanceStatus.MATURED and rec.realized_roi == pytest.approx(4.0)


# ---------------------------------------------------------------- H13 ----
def test_h13_real_yfinance_outage_is_a_failed_pull():
    """The REAL yfinance 0.2.66 stack under a stubbed transport: an outage
    must count as failed and open the breaker, while a delisted answer is an
    empty answer. With yfinance's default raise_errors=False an outage would
    come back as an empty frame (read as 'no bars') and never trip it."""
    yf = pytest.importorskip("yfinance")
    if getattr(yf, "__version__", "") != "0.2.66":
        pytest.skip("transport stub written against yfinance 0.2.66")
    cr = pytest.importorskip("curl_cffi.requests")
    import yfinance.base as yb
    import yfinance.data as yd
    from curl_cffi.requests import exceptions as cx

    class Resp:
        def __init__(self, code, text):
            self.status_code, self.text, self.url = code, text, "https://query2.finance.yahoo.com/x"

        def json(self):
            return json.loads(self.text)
    delisted = json.dumps({"chart": {"result": None, "error": {
        "code": "Not Found", "description": "No data found, symbol may be delisted"}}})
    modes = {"conn": lambda: (_ for _ in ()).throw(cx.ConnectionError("offline stub")),
             "timeout": lambda: (_ for _ in ()).throw(cx.Timeout("offline stub")),
             "down": lambda: Resp(200, "<html>Will be right back</html>"),
             "delisted": lambda: Resp(404, delisted)}
    syms = ["AAA.US", "BBB.US", "CCC.US", "DDD.US", "EEE.US"]
    saved = (cr.Session.request, yd.YfData.get, yb._tz_info_fetch_ctr)
    yf.set_tz_cache_location(tempfile.mkdtemp(prefix="tfb_yf_tz_"))

    def no_net(self, method, url, *a, **k):
        raise AssertionError("network touched: " + url)
    out = {}
    try:
        cr.Session.request = no_net
        cache = yf.cache.get_tz_cache()
        for sym in syms:
            cache.store(tp._yf_ticker(sym), "America/New_York")
        for mode, make in modes.items():
            yd.YfData.get = lambda self, url, params=None, timeout=30, make=make: make()
            try:
                yd.YfData.cache_get.cache_clear()
            except Exception:
                pass
            yb._tz_info_fetch_ctr = 2
            tp._HIST_CLOSE_CACHE.clear()
            tp._HIST_CLOSE_BUDGET.clear()
            tp._HIST_CLOSE_BUDGET.update({"pulls": 0, "t0": 0.0, "deferred": 0, "ok": 0, "failed": 0,
                                          "consecutive": 0, "breaker": False, "first_error": ""})
            reasons = [tp._hist_close_bars(sym)[1] for sym in syms]
            b = tp._HIST_CLOSE_BUDGET
            out[mode] = (reasons, b["ok"], b["failed"], b["breaker"], b["first_error"], b["deferred"])
    finally:
        cr.Session.request, yd.YfData.get, yb._tz_info_fetch_ctr = saved
        tp._HIST_CLOSE_CACHE.clear()
    outage = ["hist_close_pull_error"] * 3 + ["hist_close_breaker_open"] * 2
    assert out["conn"] == (outage, 0, 3, True, "ConnectionError", 2), out["conn"]
    assert out["timeout"] == (outage, 0, 3, True, "Timeout", 2), out["timeout"]
    assert out["down"] == (outage, 0, 3, True, "RuntimeError", 2), out["down"]
    assert out["delisted"] == ([""] * 5, 5, 0, False, "", 0), out["delisted"]


# ---------------------------------------------------------------- N1 -----
def _mixed(mod):
    """quote_outside / matured-now / price_unavailable / stale / bad entry."""
    return ([record(mod),
             record(mod, symbol="NOW.US", target=CLOSE - timedelta(hours=4)),
             record(mod, symbol="GONE.US"),
             record(mod, symbol="OLD.US", target=CLOSE - timedelta(hours=4)),
             record(mod, symbol="BAD.US", entry=0.0)],
            [row(), row(symbol="NOW.US", price=97.0),
             row(symbol="OLD.US", quote=CLOSE - timedelta(days=1)), row(symbol="BAD.US")])


def _norm_logs(mod, logs):
    return [m.replace(mod.SCRIPT_VERSION, "X") for m in logs]


def test_n1_off_is_byte_identical_to_v6_45_0():
    b = base()
    want_recs, rows = _mixed(b)
    want = audit(b, want_recs, rows, bar_map={})
    for spelling in (None, "off", "1", "true", "enforced"):
        got_recs, rows = _mixed(tp)
        env = {} if spelling is None else {"TRACK_HIST_TARGET_CLOSE": spelling}
        got = audit(tp, got_recs, rows, env=env, bar_map={"SYNTH.US": bars()})
        assert [state(r) for r in got_recs] == [state(r) for r in want_recs], spelling
        assert got.stats == want.stats, spelling
        assert _norm_logs(tp, got.logs) == _norm_logs(b, want.logs), spelling
        assert got.pulls == [], spelling
    for bar_list in (bars(), bars(drop_target=True)):
        obs_recs, rows = _mixed(tp)
        obs = audit(tp, obs_recs, rows, env={"TRACK_HIST_TARGET_CLOSE": "observe"},
                    bar_map={"SYNTH.US": bar_list})
        assert [state(r) for r in obs_recs] == [state(r) for r in want_recs]
        assert obs.stats == want.stats
        assert _norm_logs(tp, [m for m in obs.logs if not m.startswith("[HIST-CLOSE v")]) == \
            _norm_logs(b, want.logs)


# ---------------------------------------------------------------- N2 -----
def test_n2_golden_negative_base_leaves_the_record_active():
    b = base()
    assert b.SCRIPT_VERSION == "6.45.0" and not hasattr(b, "_hist_target_close")
    rec = record(b)
    audit(b, [rec], [row()], env={"TRACK_HIST_TARGET_CLOSE": "enforce"})
    assert rec.status == b.PerformanceStatus.ACTIVE and rec.realized_roi is None
    assert receipt(b, rec)["reason"] == "quote_outside_target_session"
    later = record(b)
    audit(b, [later], [row()], env={"TRACK_HIST_TARGET_CLOSE": "enforce"},
          patches={"_mature_grace_days": lambda: 0})
    assert later.status == b.PerformanceStatus.ACTIVE          # never expires either
    rec2 = record(tp)
    audit(tp, [rec2], [row()], env={"TRACK_HIST_TARGET_CLOSE": "enforce"},
          bar_map={"SYNTH.US": bars()})
    assert rec2.status == tp.PerformanceStatus.MATURED and rec.status != rec2.status


# ---------------------------------------------------------------- N3 -----
def _step(path, name):
    text = path.read_text(encoding="utf-8")
    parts = re.split(r"^\s{6}- name: ", text, flags=re.M)
    hits = [p for p in parts if p.splitlines() and name in p.splitlines()[0]]
    assert len(hits) == 1, (path.name, name, len(hits))
    return text, hits[0]


def test_n3_workflow_mapping_version_and_pin():
    clock, step = _step(ROOT / ".github/workflows/track_performance.yml", "Run performance clock")
    for name, default in (("TRACK_HIST_TARGET_CLOSE", "off"), ("TRACK_HIST_CLOSE_BUDGET", "60"),
                          ("TRACK_HIST_CLOSE_TIME_SEC", "240")):
        want = "${{ vars.%s || '%s' }}" % (name, default)
        assert re.findall(r"^\s+%s:\s*(.+)$" % name, clock, re.M) == [want], name
        assert re.findall(r"^\s+%s:\s*(.+)$" % name, step, re.M) == [want], name
    assert "# VERSION 1.0.4 (2026-10-21)" in clock
    assert clock.index("# VERSION 1.0.4") < clock.index("# VERSION 1.0.3")
    daily = (ROOT / ".github/workflows/daily_sync.yml").read_text(encoding="utf-8")
    assert "TRACK_HIST_TARGET_CLOSE" not in daily
    src = DELIVERED.read_text(encoding="utf-8")
    assert re.search(r'os\.getenv\("TRACK_HIST_TARGET_CLOSE"\)\s*or\s*"off"', src)
    assert tuple(int(x) for x in tp.SCRIPT_VERSION.split(".")) >= (6, 47, 0)
    assert "(v%s)" % tp.SCRIPT_VERSION in src.splitlines()[4]   # docstring follows the constant
    vd = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("track_performance", "%s", "track performance")' % tp.SCRIPT_VERSION in vd
    version_line = re.search(r'^SCRIPT_VERSION = "[0-9.]+"', src, re.M)
    assert version_line, "SCRIPT_VERSION line missing"
    assert src.index("# v6.47.0 (2026-10-21)") < version_line.start()
    assert src.index("# v6.45.0 (2026-10-11): integrate") < src.index("# v6.47.0 (2026-10-21)")


# ---------------------------------------------------------------- N4 -----
def test_n4_added_lines_are_ascii_and_nothing_removed():
    b = base()
    if tp.SCRIPT_VERSION != "6.47.0":
        pytest.skip("exact 55b47f24 -> 6.47.0 diff checks apply to 6.47.0 only")
    import difflib
    old = Path(b.__file__).read_text(encoding="utf-8").splitlines()
    new = DELIVERED.read_text(encoding="utf-8").splitlines()
    diff = list(difflib.unified_diff(old, new, lineterm="", n=0))
    added = [ln[1:] for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
    removed = [ln[1:] for ln in diff if ln.startswith("-") and not ln.startswith("---")]
    title = "TADAWUL FAST BRIDGE \u2013 ADVANCED PERFORMANCE ANALYTICS ENGINE (v%s)"
    assert removed == [title % "6.45.0", 'SCRIPT_VERSION = "6.45.0"'], removed
    # The docstring title keeps its pre-existing en dash; only its version moves.
    assert added.count(title % "6.47.0") == 1
    fresh = [ln for ln in added if ln != title % "6.47.0"]
    assert fresh and all(ln.isascii() for ln in fresh), [ln for ln in fresh if not ln.isascii()][:3]
    import ast
    def fns(text):
        return {n.name for n in ast.walk(ast.parse(text)) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    gone = fns("\n".join(old)) - fns("\n".join(new))
    assert not gone, gone
    assert fns("\n".join(new)) - fns("\n".join(old)) == {
        "_hist_target_close_mode", "_hist_close_budget", "_hist_close_time_sec", "_hist_close_pull",
        "_hist_close_bars", "_hist_target_close", "_hist_close_evidence", "_hist_close_judge"}


def _digest():
    parts = []
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            parts.append(name)
    parts.append(tp.SCRIPT_VERSION)
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()[:16]


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = skipped = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print("ok  ", name)
        except pytest.skip.Exception as exc:
            skipped += 1
            print("SKIP", name, "::", exc)
    print(f"PASS {passed}/{len(tests)}" + (f" (skipped {skipped})" if skipped else "") + f" digest {_digest()}")
