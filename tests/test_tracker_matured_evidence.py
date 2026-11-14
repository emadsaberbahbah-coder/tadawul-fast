"""track_performance >= 6.54.0 [MATURED-VALID] -- an outcome matured by THIS
audit pass counts as a valid outcome in the same run.

History: this harness was written on 26 Oct for an opt-in gate
(TRACK_MATURED_EVIDENCE_VALID, PR #845). The same fix landed on main on
14 Nov without a gate (track_performance 6.54.0, PR #979), so the harness now
checks the unconditional behaviour: the gate, its log line, its workflow
mappings and its base-diff checks are gone; the path coverage stays.

Zero network: the real audit_active_records runs with a fixed clock, an
offline calendar, an AsyncMock backend transport, CA-GUARD and the Yahoo
fallback off and a fake HIST-CLOSE bar pull. Fixture rows are canonical v2
1W records (M8: legacy rows with no target schema) round-tripped through
_record_to_row -> text -> from_sheet_row, so the blank 'Realized ROI %' cell
gives realized_roi_source_valid=False exactly as a production load does.

  M1  a fresh witnessed close matures the row and the SAME run's summary,
      calibrator, canonical report, S-1 measure and MATURED receipt all use it
  M2  the HIST-CLOSE enforce path reaches the same assignment
  M3  same-run consumers equal the next run's reload
  M5  a stored MATURED row with a malformed cell, a row left PENDING and a
      corp-action-suspect row that goes EXPIRED are never revalidated
  M8  a legacy row (no target schema, the live 24 Oct population) counts in
      the same run
  M9  CA-GUARD 'adjust' and 'ledger' maturities count in the same run and
      their receipts carry forecast_error_pp

The module under test is scripts/track_performance.py, or the file named by
TFB_TRACK_UNDER_TEST.
"""
from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
UNDER_TEST = Path(os.environ.get("TFB_TRACK_UNDER_TEST") or ROOT / "scripts" / "track_performance.py")
PREFIXES = ("TRACK_", "TFB_TRACK_", "TFB_PERF_")
UTC = timezone.utc
NOW = datetime(2026, 10, 8, 21, tzinfo=UTC)
CLOSE = datetime(2026, 10, 8, 20, tzinfo=UTC)
TODAY_TARGET = CLOSE - timedelta(hours=4)
OLD_TARGET = CLOSE - timedelta(days=7)
ENFORCE = {}            # 6.54.0: no gate; the same-run maturity counts unconditionally


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


TP = _load(UNDER_TEST, "tp_matured_evidence_under_test")


# ------------------------------------------------------------------ fixtures
def calendar_fixture(name, day):
    around = datetime.fromisoformat(day).replace(tzinfo=UTC)
    out = []
    for offset in range(-14, 15):
        d = around + timedelta(days=offset)
        if d.weekday() < 5:
            out.append((d.date().isoformat(), d.replace(hour=13, minute=30), d.replace(hour=20, minute=0)))
    return tuple(out)


def bars(close):
    out, day, i = [], datetime(2026, 9, 21), 0
    while day <= datetime(2026, 10, 8):
        if day.weekday() < 5:
            d = day.strftime("%Y-%m-%d")
            out.append({"date": d, "close": close if d == "2026-10-01" else 100.0 + i, "split": 0.0})
            i += 1
        day += timedelta(days=1)
    return out


def quote(symbol, price):
    return {"symbol": symbol, "current_price": price, "data_provider": "yahoo_chart",
            "acquisition_status": "success", "acquisition_provider": "yahoo_chart",
            "acquisition_acquired_at": NOW.isoformat(), "acquisition_quote_asof": CLOSE.isoformat()}


def loaded(mod, rid, symbol, target, realized_cell="", status="ACTIVE", realized=None, schema=True):
    """A canonical v2 1W row as a production load sees it (text cells);
    schema=False gives a legacy row (no target schema in Notes)."""
    rec = mod.PerformanceRecord(
        record_id=rid, symbol=symbol, horizon=mod.HorizonType.WEEK_1,
        date_recorded=target - timedelta(days=7), entry_price=100.0,
        entry_recommendation=mod.RecommendationType.BUY, entry_score=70,
        entry_risk_bucket="MODERATE", entry_confidence="HIGH", origin_tab="Top_10_Investments",
        target_price=110.0, target_roi=10.0, target_date=target,
        status=getattr(mod.PerformanceStatus, status), last_updated=NOW, realized_roi=realized)
    rec.notes = (mod._TARGET_SCHEMA_TAG + json.dumps({"version": 2, "horizon": "1W",
                 "roi_unit": "percent_points", "basis": "forecast_price",
                 "producer_version": "fixture"}) + "]") if schema else ""
    store = object.__new__(mod.PerformanceStore)
    row = [str("" if v is None else v) for v in store._record_to_row(rec)]
    row[store.HEADERS.index("Realized ROI %")] = realized_cell
    return mod.PerformanceRecord.from_sheet_row(row, store.HEADERS)


def reload(mod, rec):
    store = object.__new__(mod.PerformanceStore)
    return mod.PerformanceRecord.from_sheet_row(
        [str("" if v is None else v) for v in store._record_to_row(rec)], store.HEADERS)


class _Logs(list):
    """Log messages (as before); .records keeps (levelno, message) in order."""

    def __init__(self):
        super().__init__()
        self.records = []


@contextlib.contextmanager
def harness(mod, env, extra=None):
    """Clock, calendar, transport and gates pinned; every TRACK_* / TFB_TRACK_*
    / TFB_PERF_* variable of the caller is cleared for the duration. Every
    snapshot is taken before the first change, and every change happens inside
    the try, so a failing setup still restores the caller's state."""
    logs = _Logs()
    stray = {k: os.environ[k] for k in list(os.environ) if k.startswith(PREFIXES)}
    swaps = {"_utc_now": lambda: NOW, "_outcome_calendar_schedule": calendar_fixture,
             "_price_fallback_enabled": lambda: False, "_ca_guard_enabled": lambda: False,
             "_feed_legacy": lambda: True, "_ca_ledger_index": lambda: {},
             "_hist_close_pull": lambda s: bars(108.0),
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
        os.environ.update({"TRACK_CANONICAL_TARGETS": "1", "TFB_PERF_TARGET_UNIT_SENTRY": "enforce"})
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


def measure(mod, records):
    s = mod.PerformanceAnalyzer().analyze(records)
    cr = mod.canonical_performance_report(records)
    cal = mod.ReliabilityCalibrator().calibrate(records)
    s1 = mod.s1_checkpoint_calibration(records)
    return {"summary": (s.matured_records, s.invalid_matured_excluded, s.wins, s.losses, round(s.avg_roi, 9)),
            "canonical": {k: (v["n"], round(v["mean_signed_error_pp"], 9)) for k, v in cr["by_horizon"].items()},
            "calibrator": (cal.matured_records, cal.invalid_matured_excluded, cal.decided_records),
            "s1_n": s1["n"]}


def audit(mod, records, rows, env, extra=None):
    with harness(mod, env, extra) as logs:
        app = mod.PerformanceTrackerApp.__new__(mod.PerformanceTrackerApp)
        app.backend = mod.BackendClient("https://transport.invalid")
        app.backend.post_json = AsyncMock(return_value=({"rows": rows}, None, 200))
        asyncio.run(app.audit_active_records(records))
        consumers = measure(mod, records)
    return logs, consumers, dict(getattr(app, "_last_audit_stats", {}) or {})


def receipt(mod, rec):
    return json.loads(rec.notes.rsplit(mod._OUTCOME_RECEIPT, 1)[1])


# ------------------------------------------------------------------ cases
def test_m1_same_run_consumers_and_receipt_use_the_new_outcome():
    rec = loaded(TP, "A", "AAA.US", TODAY_TARGET)
    assert rec.realized_roi_source_valid is False            # the loader fact, unchanged
    # an in-memory record (built this run, flag already True) matures too: not counted as revalidated
    fresh = loaded(TP, "G", "GGG.US", TODAY_TARGET)
    fresh.realized_roi_source_valid = True
    logs, got, _ = audit(TP, [rec, fresh], [quote("AAA.US", 104.0), quote("GGG.US", 104.0)], ENFORCE)
    assert rec.status == TP.PerformanceStatus.MATURED and rec.realized_roi == pytest.approx(4.0)
    # the outcome first: on the base copy this is where the golden negative fails
    assert got["summary"][:2] == (2, 0) and got["calibrator"][:2] == (2, 0), got
    proof = receipt(TP, rec)
    assert proof["forecast_error_pp"] == pytest.approx(-6.0)
    assert proof["target_achieved_at_horizon"] is False
    assert got["canonical"] == {"1W": (2, pytest.approx(-6.0))} and got["s1_n"] == 2
    assert rec.realized_roi_source_valid is True


def test_m2_hist_close_is_covered_by_the_same_assignment():
    rec = loaded(TP, "B", "BBB.US", OLD_TARGET)
    env = dict(ENFORCE, TRACK_HIST_TARGET_CLOSE="enforce")
    _, got, _ = audit(TP, [rec], [quote("BBB.US", 111.0)], env)
    assert rec.status == TP.PerformanceStatus.MATURED and rec.realized_roi == pytest.approx(8.0)
    assert got["summary"][:2] == (1, 0) and got["s1_n"] == 1, got
    proof = receipt(TP, rec)
    assert proof["quote_kind"] == "historical_daily_close"
    assert proof["forecast_error_pp"] == pytest.approx(-2.0)


def test_m3_same_run_equals_next_run_reload():
    recs = [loaded(TP, "A", "AAA.US", TODAY_TARGET), loaded(TP, "E", "EEE.US", TODAY_TARGET, "bad"),
            loaded(TP, "B", "BBB.US", OLD_TARGET)]
    rows = [quote("AAA.US", 104.0), quote("EEE.US", 96.0), quote("BBB.US", 111.0)]
    env = dict(ENFORCE, TRACK_HIST_TARGET_CLOSE="enforce")
    _, same, _ = audit(TP, recs, rows, env)
    with harness(TP, env):
        nxt = measure(TP, [reload(TP, r) for r in recs])
    assert same == nxt and same["summary"][:2] == (3, 0), (same, nxt)


def test_m5_stored_malformed_pending_and_expired_stay_invalid():
    stored = loaded(TP, "C", "CCC.US", TODAY_TARGET, "bad", status="MATURED", realized=3.0)
    pending = loaded(TP, "D", "DDD.US", OLD_TARGET, "bad")
    suspect = loaded(TP, "F", "FFF.US", TODAY_TARGET)
    extra = {"_ca_guard_enabled": lambda: True, "_ca_adjusted_roi": lambda *a, **k: None,
             "_ca_target_bar": lambda *a, **k: ({"close": 1.0}, ""),
             "_ca_budget_state": lambda s: "ok", "_ca_ledger_factor": lambda *a, **k: None}
    logs, got, _ = audit(TP, [stored, pending, suspect],
                         [quote("DDD.US", 104.0), quote("FFF.US", 160.0)], ENFORCE, extra)
    assert [r.realized_roi_source_valid for r in (stored, pending, suspect)] == [False] * 3
    assert stored.status == TP.PerformanceStatus.MATURED and got["summary"][:2] == (0, 1)
    assert pending.status == TP.PerformanceStatus.ACTIVE and pending.realized_roi is None
    assert suspect.status == TP.PerformanceStatus.EXPIRED and suspect.outcome == "CORP_ACTION_SUSPECT"


def test_m8_legacy_row_without_target_schema_counts_in_the_same_run():
    def run(env):
        rec = loaded(TP, "L", "LLL.US", TODAY_TARGET, schema=False)
        assert TP._target_schema(rec.notes) is None and rec.realized_roi_source_valid is False
        logs, got, _ = audit(TP, [rec], [quote("LLL.US", 104.0)], env)
        return rec, logs, got
    rec, logs, got = run(ENFORCE)
    assert rec.status == TP.PerformanceStatus.MATURED and rec.realized_roi == pytest.approx(4.0)
    assert got["summary"][:2] == (1, 0) and got["calibrator"][:2] == (1, 0), got
    assert rec.realized_roi_source_valid is True
    assert got["canonical"] == {} and got["s1_n"] == 0          # legacy rows never reach S-1
    assert "forecast_error_pp" not in receipt(TP, rec)          # no target schema, no target fields


def test_m9_ca_guard_adjust_and_ledger_maturities_count_in_the_same_run():
    adj = loaded(TP, "J", "JJJ.US", TODAY_TARGET)
    led = loaded(TP, "K", "KKK.US", TODAY_TARGET)
    extra = {"_ca_guard_enabled": lambda: True, "_ca_budget_state": lambda s: "ok",
             "_ca_target_bar": lambda *a, **k: ({"close": 1.0}, ""),
             "_ca_adjusted_roi": lambda symbol, *a, **k: 5.0 if symbol == "JJJ.US" else None,
             "_ca_ledger_factor": lambda symbol, *a, **k: 0.5 if symbol == "KKK.US" else None}
    logs, got, _ = audit(TP, [adj, led], [quote("JJJ.US", 160.0), quote("KKK.US", 200.0)], ENFORCE, extra)
    pa, pl = receipt(TP, adj), receipt(TP, led)
    assert (pa["roi_basis"], pl["roi_basis"]) == ("adjust", "ledger"), (pa, pl)
    assert adj.status == led.status == TP.PerformanceStatus.MATURED
    assert adj.realized_roi == pytest.approx(5.0) and led.realized_roi == pytest.approx(0.0)
    assert got["summary"][:2] == (2, 0) and got["calibrator"][:2] == (2, 0), got
    assert [adj.realized_roi_source_valid, led.realized_roi_source_valid] == [True, True]
    assert pa["forecast_error_pp"] == pytest.approx(-5.0) and pl["forecast_error_pp"] == pytest.approx(-10.0)
    assert got["s1_n"] == 2


CASES = [test_m1_same_run_consumers_and_receipt_use_the_new_outcome,
         test_m2_hist_close_is_covered_by_the_same_assignment,
         test_m3_same_run_equals_next_run_reload,
         test_m5_stored_malformed_pending_and_expired_stay_invalid,
         test_m8_legacy_row_without_target_schema_counts_in_the_same_run,
         test_m9_ca_guard_adjust_and_ledger_maturities_count_in_the_same_run]


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
            print("FAIL", name, type(e).__name__, str(e)[:300])
    total = passed + failed
    print("PASS %d/%d%s" % (passed, total, ("  (skipped %d)" % skipped) if skipped else "") if not failed
          else "FAIL %d/%d" % (passed, total))
    sys.exit(1 if failed else 0)
