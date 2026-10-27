"""track_performance v6.52.0 [OUTCOME-TALLY] -- one observe line per audit
saying why due records did or did not mature, per exchange calendar
(TRACK_OUTCOME_TALLY = off (default) | observe).

Zero network: the real audit_active_records runs with a fixed clock, an
offline calendar (XNYS 13:30-20:00Z Mon-Fri, XSAU 07:00-12:00Z Sun-Thu, the
lean fixture's shapes), an AsyncMock backend transport, CA-GUARD and the
Yahoo fallback off.

  T0  the mode parser takes the exact word 'observe' only
  T1  observe: the line counts matured / pending / expired per calendar with
      reasons, and buckets the minutes between a refused .SR quote and the
      12:00Z XSAU close (0-15, 15-20, 20-60, 60+); a record not yet due and a
      record that is not ACTIVE are not counted
  T2  observe changes nothing else: records, receipts, audit stats and every
      other log line equal the off run
  T3  OFF (unset, '', off, 0, 1, true, on, enforce, observed): records,
      receipts, audit stats and logs equal the base copy, or its recorded
      digest on a shallow checkout; no collector is ever armed
  T4  the line is INFO, comes after the MATURE-FRESH and HIST-CLOSE lines,
      appears once, and the collectors are disarmed after the pass
  T5  edge cases: a regular_market quote refused after its 15 min slack is
      bucketed; a calendar lookup failure is reported as UNKNOWN; nothing
      evaluated -> no line; the helpers never raise
  T6  workflow and pins: both tracker steps map the Variable once (default
      'off'), header 1.0.8, SCRIPT_VERSION >= 6.52.0 in lockstep with
      docstring line 5 and the verify_deployment pin
  T7  ASCII added lines, zero removals, exactly this release's additions
      (needs the base copy)

The module under test is scripts/track_performance.py, or the file named by
TFB_TRACK_UNDER_TEST (golden negative: point it at the untouched base copy,
git show e8337a16:scripts/track_performance.py). The base for T3 / T7 is
TFB_TRACK_BASE, else `git show e8337a16:scripts/track_performance.py`.

Run: python tests/test_tracker_outcome_tally.py  (prints PASS k/k)
 or: python -m pytest -q tests/test_tracker_outcome_tally.py
"""
from __future__ import annotations

import asyncio
import atexit
import contextlib
import dataclasses
import hashlib
import importlib.util
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
UNDER_TEST = Path(os.environ.get("TFB_TRACK_UNDER_TEST") or ROOT / "scripts" / "track_performance.py")
BASE_SHA = "e8337a16"
ENV = "TRACK_OUTCOME_TALLY"
PREFIXES = ("TRACK_", "TFB_TRACK_", "TFB_PERF_")
UTC = timezone.utc
NOW = datetime(2026, 10, 8, 21, tzinfo=UTC)            # Thursday
US_CLOSE = datetime(2026, 10, 8, 20, tzinfo=UTC)
SR_CLOSE = datetime(2026, 10, 8, 12, tzinfo=UTC)
OBSERVE = {ENV: "observe"}
OFF_SPELLINGS = (None, "", "off", "0", "1", "true", "on", "enforce", "observed")
# sha256[:16] of the base (v6.49.0) T3 output; proven against the base in T3
# when the base is available, checked alone on a shallow CI checkout.
OFF_GOLDEN = "72db2ad8b4251afe"
ADDED = {"_outcome_tally_mode", "_outcome_tally_begin", "_outcome_tally_end",
         "_outcome_tally_stamp", "_outcome_tally_line"}
TAG = "[v6.52.0 OUTCOME-TALLY]"


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod          # dataclasses resolve their module by name
    old = list(sys.path)
    try:
        sys.path.insert(0, str(ROOT))
        spec.loader.exec_module(mod)
    finally:
        sys.path[:] = old
    return mod


TP = _load(UNDER_TEST, "tp_outcome_tally_under_test")


def _base_source():
    env = os.environ.get("TFB_TRACK_BASE")
    if env and Path(env).is_file():
        return Path(env).read_text(encoding="utf-8")
    try:
        proc = subprocess.run(["git", "show", BASE_SHA + ":scripts/track_performance.py"],
                              cwd=str(ROOT), capture_output=True, text=True, timeout=60)
    except Exception:
        return None
    return proc.stdout if proc.returncode == 0 and proc.stdout else None


_BASE = {}


def _base_module():
    if "mod" not in _BASE:
        src = _base_source()
        if src is None:
            _BASE["mod"] = None
        else:
            tmp_dir = tempfile.mkdtemp(prefix="tp_ot_base_")
            atexit.register(shutil.rmtree, tmp_dir, True)
            tmp = Path(tmp_dir) / "track_performance_base.py"
            tmp.write_text(src, encoding="utf-8")
            _BASE["mod"] = _load(tmp, "tp_outcome_tally_base")
    return _BASE["mod"]


# ------------------------------------------------------------------ fixtures
def calendar_fixture(name, day):
    """XNYS-like and XSAU-like sessions only (the lean fixture's shapes)."""
    around = datetime.fromisoformat(day).replace(tzinfo=UTC)
    out = []
    for offset in range(-14, 15):
        d = around + timedelta(days=offset)
        if d.weekday() in ((4, 5) if name == "XSAU" else (5, 6)):
            continue
        if name == "XSAU":
            out.append((d.date().isoformat(), d.replace(hour=7, minute=0), d.replace(hour=12, minute=0)))
        else:
            out.append((d.date().isoformat(), d.replace(hour=13, minute=30), d.replace(hour=20, minute=0)))
    return tuple(out)


def quote(symbol, price, asof, **extra):
    row = {"symbol": symbol, "current_price": price, "data_provider": "yahoo_chart",
           "acquisition_status": "success", "acquisition_provider": "yahoo_chart",
           "acquisition_acquired_at": NOW.isoformat(), "acquisition_quote_asof": asof.isoformat()}
    row.update(extra)
    return row


def rec(mod, rid, symbol, target, status="ACTIVE"):
    r = mod.PerformanceRecord(
        record_id=rid, symbol=symbol, horizon=mod.HorizonType.WEEK_1,
        date_recorded=(target or NOW) - timedelta(days=7), entry_price=100.0,
        entry_recommendation=mod.RecommendationType.BUY, entry_score=70,
        entry_risk_bucket="MODERATE", entry_confidence="HIGH", origin_tab="Top_10_Investments",
        target_price=110.0, target_roi=10.0, target_date=target,
        status=getattr(mod.PerformanceStatus, status), last_updated=NOW)
    return r


class _Logs(list):
    """Log messages; .records keeps (levelno, message) in order."""

    def __init__(self):
        super().__init__()
        self.records = []


@contextlib.contextmanager
def harness(mod, env, extra=None):
    """Clock, calendar, transport and gates pinned; every TRACK_* / TFB_TRACK_*
    / TFB_PERF_* variable of the caller is cleared for the duration. Snapshots
    come first and every change happens inside the try."""
    logs = _Logs()
    stray = {k: os.environ[k] for k in list(os.environ) if k.startswith(PREFIXES)}
    swaps = {"_utc_now": lambda: NOW, "_outcome_calendar_schedule": calendar_fixture,
             "_price_fallback_enabled": lambda: False, "_ca_guard_enabled": lambda: False,
             "_feed_legacy": lambda: True, "_ca_ledger_index": lambda: {},
             "urlopen": lambda *a, **k: (_ for _ in ()).throw(AssertionError("network"))}
    swaps.update(extra or {})
    saved = {k: getattr(mod, k) for k in swaps if hasattr(mod, k)}
    saved_now = mod.RiyadhTime.__dict__["now"]
    level = mod.logger.level
    h = logging.Handler()

    def _emit(r):
        logs.append(r.getMessage())
        logs.records.append((r.levelno, r.getMessage()))

    h.emit = _emit
    try:
        for k in stray:
            os.environ.pop(k, None)
        os.environ.update({k: v for k, v in env.items() if v is not None})
        mod.logger.addHandler(h)
        mod.logger.setLevel(logging.INFO)
        for k, v in swaps.items():
            setattr(mod, k, v)
        mod.RiyadhTime.now = staticmethod(lambda: NOW)
        yield logs
    finally:
        mod.logger.setLevel(level)
        mod.logger.removeHandler(h)
        mod.RiyadhTime.now = saved_now
        for k in swaps:
            if k in saved:
                setattr(mod, k, saved[k])
            elif hasattr(mod, k):
                delattr(mod, k)
        for k in [k for k in os.environ if k.startswith(PREFIXES)]:
            os.environ.pop(k, None)
        os.environ.update(stray)


def audit(mod, records, rows, env, extra=None):
    with harness(mod, env, extra) as logs:
        app = mod.PerformanceTrackerApp.__new__(mod.PerformanceTrackerApp)
        app.backend = mod.BackendClient("https://transport.invalid")
        app.backend.post_json = AsyncMock(return_value=({"rows": rows}, None, 200))
        asyncio.run(app.audit_active_records(records))
    return logs, dict(getattr(app, "_last_audit_stats", {}) or {})


def state(r):
    d = dataclasses.asdict(r)
    d["notes"] = re.sub(r'"tracker_version":"[0-9.]+"', '"tracker_version":"X"', d["notes"] or "")
    return json.loads(json.dumps(d, default=str, sort_keys=True))


def mixed(mod):
    """Nine records: 7 due, 1 not yet due, 1 already matured."""
    today = NOW - timedelta(hours=14)                  # 07:00Z Thu: both calendars' session
    recs = [rec(mod, "A", "AAA.US", US_CLOSE - timedelta(hours=4)),       # matures (XNYS)
            rec(mod, "M", "MISS.US", US_CLOSE - timedelta(hours=4)),      # no row: price_unavailable
            rec(mod, "S1", "2286.SR", today),                             # 12:10:24Z -> 0-15
            rec(mod, "S2", "2222.SR", today),                             # 12:17Z -> 15-20
            rec(mod, "S3", "1150.SR", today),                             # 12:45Z -> 20-60
            rec(mod, "S4", "2010.SR", today),                             # 15:00Z -> 60+
            rec(mod, "S5", "7010.SR", today),                             # 12:00:00Z, at the close -> matures
            rec(mod, "F", "FUT.US", NOW + timedelta(days=3)),             # not yet due
            rec(mod, "D", "DONE.US", US_CLOSE - timedelta(hours=4), status="MATURED")]
    rows = [quote("AAA.US", 104.0, US_CLOSE), quote("MISS.US", 0, US_CLOSE),
            quote("2286.SR", 30.0, SR_CLOSE + timedelta(minutes=10, seconds=24)),
            quote("2222.SR", 27.0, SR_CLOSE + timedelta(minutes=17)),
            quote("1150.SR", 25.0, SR_CLOSE + timedelta(minutes=45)),
            quote("2010.SR", 80.0, SR_CLOSE + timedelta(hours=3)),
            quote("7010.SR", 41.0, SR_CLOSE), quote("FUT.US", 50.0, US_CLOSE)]
    rows = [r for r in rows if r["current_price"]]
    return recs, rows


def _t3_output(mod, spelling):
    env = {} if spelling is None else {ENV: spelling}
    recs, rows = mixed(mod)
    logs, stats = audit(mod, recs, rows, env)
    armed = getattr(mod, "_OUTCOME_TALLY", None)
    return ([state(r) for r in recs], stats, [re.sub(r"v6\.\d+\.\d+", "vX", m) for m in logs], armed is None)


def _digest(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]


def _version(v):
    return tuple(int(x) for x in str(v).split(".")[:3])


def _tally_lines(logs):
    return [m for m in logs if m.startswith(TAG)]


# ------------------------------------------------------------------ cases
def test_t0_mode_parser_exact_word():
    for raw, want in ((None, "off"), ("", "off"), ("off", "off"), ("1", "off"), ("true", "off"),
                      ("on", "off"), ("enforce", "off"), ("observed", "off"), ("observe", "observe"),
                      (" Observe ", "observe"), ("OBSERVE\n", "observe")):
        with harness(TP, {ENV: raw}):
            assert TP._outcome_tally_mode() == want, (raw, TP._outcome_tally_mode())


def test_t1_observe_counts_reasons_per_calendar_and_after_close_minutes():
    recs, rows = mixed(TP)
    logs, stats = audit(TP, recs, rows, OBSERVE)
    lines = _tally_lines(logs)
    assert len(lines) == 1, logs
    line = lines[0]
    # the outcome first: 7 evaluated (FUT not yet due, DONE not ACTIVE)
    assert line.startswith(TAG + " mode=observe evaluated=7 matured=2 expired=0 pending=5 | "), line
    parts = line.split(" | ")[1:]
    assert parts[0] == ("XSAU n=5 matured=1 expired=0 pending=4 reasons=quote_session_unknown:4 "
                        "after_close_min=0-15:1,15-20:1,20-60:1,60+:1"), parts
    assert parts[1] == ("XNYS n=2 matured=1 expired=0 pending=1 reasons=price_unavailable:1 "
                        "after_close_min=-"), parts
    assert len(parts) == 2, parts
    # the tally agrees with the audit's own counters
    assert stats["matured"] == 2 and stats["outcome_skipped_reasons"] == {
        "quote_session_unknown": 4, "price_unavailable": 1}, stats


def test_t2_observe_changes_nothing_else():
    off = _t3_output(TP, None)
    recs, rows = mixed(TP)
    logs, stats = audit(TP, recs, rows, OBSERVE)
    on = ([state(r) for r in recs], stats,
          [re.sub(r"v6\.\d+\.\d+", "vX", m) for m in logs if not m.startswith(TAG)], True)
    assert on == off


def _check_off(spelling):
    out = _t3_output(TP, spelling)
    assert not _tally_lines(out[2]) and out[3] is True, out[2]
    base = _base_module()
    if base is not None:
        assert out == _t3_output(base, spelling)
    if OFF_GOLDEN != "0000000000000000":
        assert _digest(out) == OFF_GOLDEN, _digest(out)


@pytest.mark.parametrize("spelling", OFF_SPELLINGS)
def test_t3_off_equals_base(spelling):
    _check_off(spelling)


def test_t4_line_level_position_once_and_disarmed():
    recs, rows = mixed(TP)
    env = dict(OBSERVE, TRACK_HIST_TARGET_CLOSE="observe")
    logs, _ = audit(TP, recs, rows, env, {"_hist_close_pull": lambda s: []})
    order = [m for _, m in logs.records]
    at = [i for i, m in enumerate(order) if m.startswith(TAG)]
    fresh = [i for i, m in enumerate(order) if "MATURE-FRESH-ONLY" in m]
    hist = [i for i, m in enumerate(order) if "HIST-CLOSE" in m]
    assert len(at) == 1 and fresh and hist and at[0] > max(fresh + hist), order
    assert [lv for lv, m in logs.records if m.startswith(TAG)] == [logging.INFO]
    assert TP._OUTCOME_TALLY is None and TP._OUTCOME_TALLY_STAMPS is None


def test_t5_edges_regular_market_unknown_calendar_nothing_due_never_raises():
    # a regular_market quote refused after its 15-minute slack is still bucketed
    recs = [rec(TP, "R", "4280.SR", NOW - timedelta(hours=14))]
    rows = [quote("4280.SR", 10.0, SR_CLOSE + timedelta(minutes=16),
                  regularMarketTime=(SR_CLOSE + timedelta(minutes=16)).timestamp())]
    logs, _ = audit(TP, recs, rows, OBSERVE)
    assert _tally_lines(logs) == [TAG + " mode=observe evaluated=1 matured=0 expired=0 pending=1 | XSAU n=1 "
                                  "matured=0 expired=0 pending=1 reasons=quote_session_unknown:1 "
                                  "after_close_min=15-20:1"], logs
    # a symbol without a tracker calendar is reported as UNKNOWN, with its reason
    recs = [rec(TP, "X", "ODD.ZZ", NOW - timedelta(hours=14))]
    logs, _ = audit(TP, recs, [quote("ODD.ZZ", 10.0, SR_CLOSE)], OBSERVE)
    lines = _tally_lines(logs)
    assert len(lines) == 1 and " | UNKNOWN n=1 matured=0 expired=0 pending=1 reasons=" in lines[0], lines
    # bucket edges: exactly 15 min is 0-15, exactly 20 min is 15-20
    today = NOW - timedelta(hours=14)
    recs = [rec(TP, "E1", "4001.SR", today), rec(TP, "E2", "4002.SR", today)]
    rows = [quote("4001.SR", 10.0, SR_CLOSE + timedelta(minutes=15)),
            quote("4002.SR", 10.0, SR_CLOSE + timedelta(minutes=20))]
    logs, _ = audit(TP, recs, rows, OBSERVE)
    assert [m.rsplit(" after_close_min=", 1)[1] for m in _tally_lines(logs)] == ["0-15:1,15-20:1"], logs
    # an unpriced record past its grace expires and is counted as expired
    old = rec(TP, "O", "OLD.US", US_CLOSE - timedelta(days=10))
    logs, _ = audit(TP, [old], [], OBSERVE)
    assert old.status == TP.PerformanceStatus.EXPIRED, old.status
    assert _tally_lines(logs) == [TAG + " mode=observe evaluated=1 matured=0 expired=1 pending=0 | XNYS n=1 "
                                  "matured=0 expired=1 pending=0 reasons=expired:price_unavailable:1 "
                                  "after_close_min=-"], logs
    # nothing due -> no line, collectors disarmed
    logs, _ = audit(TP, [rec(TP, "F", "FUT.US", NOW + timedelta(days=3))], [quote("FUT.US", 1.0, US_CLOSE)], OBSERVE)
    assert not _tally_lines(logs) and TP._OUTCOME_TALLY is None
    # only live quote kinds are stamped; helpers never raise on garbage
    sessions = calendar_fixture("XSAU", "2026-10-08")
    TP._OUTCOME_TALLY_STAMPS = {}
    for sym, kind, mins in (("H1.SR", "historical_daily_close", 7), ("H2.SR", "source_quote", 5)):
        TP._outcome_tally_stamp(TP.OutcomePrice(sym, 1.0, "p", NOW, SR_CLOSE + timedelta(minutes=mins), kind), sessions)
    assert TP._OUTCOME_TALLY_STAMPS == {"H2.SR": 5.0}, TP._OUTCOME_TALLY_STAMPS
    TP._outcome_tally_stamp(object(), None)
    TP._outcome_tally_end()
    assert TP._outcome_tally_line(NOW) is None and TP._OUTCOME_TALLY_STAMPS is None


def test_t6_workflow_mapping_and_version_pins():
    import yaml
    assert _version(TP.SCRIPT_VERSION) >= (6, 52, 0), TP.SCRIPT_VERSION
    src = UNDER_TEST.read_text(encoding="utf-8")
    assert src.split("\n")[4].endswith("(v%s)" % TP.SCRIPT_VERSION), src.split("\n")[4]
    verify = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("track_performance", "%s", "track performance")' % TP.SCRIPT_VERSION in verify
    clock = (ROOT / ".github" / "workflows" / "track_performance.yml").read_text(encoding="utf-8")
    assert "# VERSION 1.0.8 (2026-10-27) -- WHY DUE RECORDS DID NOT MATURE (OWNER OPT-IN)" in clock
    want = "${{ vars.TRACK_OUTCOME_TALLY || 'off' }}"
    for name, step_suffix in (("track_performance.yml", "Run performance clock"),
                              ("daily_sync.yml", "Track Performance (record + audit)")):
        text = (ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8")
        assert text.count(ENV + ":") == 1, name
        steps = [st for job in yaml.safe_load(text)["jobs"].values() for st in job.get("steps", [])
                 if ENV in (st.get("env") or {})]
        assert len(steps) == 1 and steps[0]["name"].endswith(step_suffix), (name, steps)
        assert "track_performance.py" in steps[0]["run"], name
        assert steps[0]["env"][ENV] == want, (name, steps[0]["env"][ENV])


def test_t7_ascii_added_lines_and_zero_removals():
    src = _base_source()
    if src is None:
        pytest.skip("base copy unavailable (shallow checkout)")
    import ast
    import difflib
    new = UNDER_TEST.read_text(encoding="utf-8")
    added = [l[1:] for l in difflib.unified_diff(src.split("\n"), new.split("\n"), n=0, lineterm="")
             if l.startswith("+") and not l.startswith("+++")]
    bad = [l for l in added if any(ord(c) > 127 for c in l) and "ADVANCED PERFORMANCE ANALYTICS ENGINE" not in l]
    assert bad == [], bad[:3]

    def names(text):
        return {n.name for n in ast.parse(text).body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}

    assert names(src) - names(new) == set(), sorted(names(src) - names(new))
    added_names = names(new) - names(src)
    if TP.SCRIPT_VERSION == "6.52.0":        # exactly this release's additions
        assert added_names == ADDED, sorted(added_names)
    else:                                    # a later release may add more
        assert ADDED <= added_names, sorted(added_names)


CASES = [test_t0_mode_parser_exact_word,
         test_t1_observe_counts_reasons_per_calendar_and_after_close_minutes,
         test_t2_observe_changes_nothing_else,
         ("test_t3_off_equals_base", lambda: [_check_off(s) for s in OFF_SPELLINGS]),
         test_t4_line_level_position_once_and_disarmed,
         test_t5_edges_regular_market_unknown_calendar_nothing_due_never_raises,
         test_t6_workflow_mapping_and_version_pins,
         test_t7_ascii_added_lines_and_zero_removals]


if __name__ == "__main__":
    passed = failed = skipped = 0
    for case in CASES:
        name, fn = case if isinstance(case, tuple) else (case.__name__, case)
        try:
            fn()
            passed += 1
            print("PASS", name)
        except BaseException as e:
            if type(e).__name__ == "Skipped":
                skipped += 1
                print("SKIP", name, str(e)[:120])
                continue
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            failed += 1
            print("FAIL", name, type(e).__name__, str(e)[:400])
    total = passed + failed
    print("PASS %d/%d%s" % (passed, total, ("  (skipped %d)" % skipped) if skipped else "") if not failed
          else "FAIL %d/%d" % (passed, total))
    sys.exit(1 if failed else 0)
