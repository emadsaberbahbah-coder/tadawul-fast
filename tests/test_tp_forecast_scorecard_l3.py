"""L3 (2026-10-17): track_performance v6.48.0 — the Forecast Scorecard.
forecast_scorecard_rows folds matured Performance_Log records into one row
per cohort (horizon x dimension) with n, wins, losses, decided-only hit rate,
Wilson 95% interval, mean and median realized ROI; the audit lane overwrites
the _Forecast_Scorecard tab in place. Offline: fake sheet handle, no network.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib
import sys

import pytest

sys.path.insert(0, ".")
tp = importlib.import_module("scripts.track_performance")
dq = importlib.import_module("scripts.tfb_workbook_dq_audit")

NOW = datetime(2026, 10, 17, 8, 0, tzinfo=timezone.utc)


def rec(sym, hz, roi, inv="INVESTABLE", recm="BUY", score=72.0, origin="Top_10_Investments",
        selected="YES", status=tp.PerformanceStatus.MATURED, valid=True):
    r = tp.PerformanceRecord(
        record_id=sym + hz, symbol=sym, horizon=tp.HorizonType(hz), date_recorded=NOW - timedelta(days=40),
        entry_price=100.0, entry_recommendation=tp.RecommendationType(recm), entry_score=score,
        entry_risk_bucket="Moderate", entry_confidence="HIGH", origin_tab=origin, target_price=110.0,
        target_roi=10.0, target_date=NOW - timedelta(days=5), status=status, realized_roi=roi,
    )
    r.entry_investability_status = inv
    r.entry_selected = selected
    r.realized_roi_source_valid = valid
    return r


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.delenv("TRACK_FORECAST_SCORECARD", raising=False)
    monkeypatch.delenv("TRACK_SCORECARD_MIN_N", raising=False)


def test_version_header_and_gates(monkeypatch):
    assert tp.SCRIPT_VERSION == "6.48.0"
    src = open(tp.__file__, encoding="utf-8").read()
    assert "# v6.48.0 (2026-10-17) - L3 FORECAST SCORECARD" in src
    assert tp.FORECAST_SCORECARD_TAB == "_Forecast_Scorecard"
    assert tp.FORECAST_SCORECARD_HEADER[:4] == ["As Of (Riyadh)", "Horizon", "Dimension", "Cohort"]
    assert tp._forecast_scorecard_enabled() is True
    for v in ("0", "false", "OFF", "no"):
        monkeypatch.setenv("TRACK_FORECAST_SCORECARD", v)
        assert tp._forecast_scorecard_enabled() is False
    monkeypatch.setenv("TRACK_FORECAST_SCORECARD", "1")
    assert tp._forecast_scorecard_enabled() is True
    assert tp._forecast_scorecard_min_n() == 5
    monkeypatch.setenv("TRACK_SCORECARD_MIN_N", "0"); assert tp._forecast_scorecard_min_n() == 1
    monkeypatch.setenv("TRACK_SCORECARD_MIN_N", "junk"); assert tp._forecast_scorecard_min_n() == 5
    monkeypatch.setenv("TRACK_SCORECARD_MIN_N", "5000"); assert tp._forecast_scorecard_min_n() == 1000


def _records():
    rows = []
    # 1M: INVESTABLE/BUY 6 records: 4 wins, 1 loss, 1 breakeven
    for i, roi in enumerate((3.0, 5.0, 1.0, 2.0, -4.0, 0.0)):
        rows.append(rec("I%d" % i, "1M", roi))
    # 1M: WATCHLIST/HOLD 5 records: 1 win, 4 losses, Decision_Coverage origin, unselected
    for i, roi in enumerate((2.0, -1.0, -2.0, -3.0, -0.5)):
        rows.append(rec("W%d" % i, "1M", roi, inv="WATCHLIST", recm="HOLD", score=41.0,
                        origin="Decision_Coverage", selected="NO"))
    # 3M: 2 INVESTABLE records (below min_n for the marginals), 1 blank investability
    rows.append(rec("T1", "3M", 8.0)); rows.append(rec("T2", "3M", -6.0, inv="", selected=""))
    # excluded: active, invalid outcome, matured without ROI
    rows.append(rec("A1", "1M", 9.0, status=tp.PerformanceStatus.ACTIVE))
    rows.append(rec("X1", "1M", 9.0, valid=False))
    rows.append(rec("N1", "1M", None))
    return rows


def _by(rows):
    return {(r[1], r[2], r[3]): r for r in rows}


def test_rows_fold_matured_records_into_cohorts_with_wilson_and_medians():
    rows = tp.forecast_scorecard_rows(_records(), as_of="2026-10-17 11:00:00")
    assert all(len(r) == len(tp.FORECAST_SCORECARD_HEADER) for r in rows)
    assert all(r[0] == "2026-10-17 11:00:00" and r[-1] == tp.SCRIPT_VERSION for r in rows)
    by = _by(rows)
    all_1m = by[("1M", "ALL", "ALL")]
    assert all_1m[4:7] == [11, 5, 5] and all_1m[7] == 50.0
    lo, hi = tp.wilson_interval(5, 10)
    assert (all_1m[8], all_1m[9]) == (lo, hi)
    assert all_1m[10] == round(sum((3.0, 5.0, 1.0, 2.0, -4.0, 0.0, 2.0, -1.0, -2.0, -3.0, -0.5)) / 11, 2)
    assert all_1m[11] == 0.0, "median of 11 values"
    inv = by[("1M", "Investability", "INVESTABLE")]
    assert inv[4:7] == [6, 4, 1] and inv[7] == 80.0 and inv[10] == round(7.0 / 6, 2) and inv[11] == 1.5
    assert by[("1M", "Investability", "WATCHLIST")][7] == 20.0
    assert by[("1M", "Recommendation", "BUY")][4] == 6 and by[("1M", "Recommendation", "HOLD")][4] == 5
    assert by[("1M", "Score Decile", "D7")][4] == 6 and by[("1M", "Score Decile", "D4")][4] == 5
    assert by[("1M", "Origin Tab", "Top_10_Investments")][7] == 80.0
    assert by[("1M", "Origin Tab", "Decision_Coverage")][7] == 20.0
    assert by[("1M", "Entry Selected", "YES")][4] == 6 and by[("1M", "Entry Selected", "NO")][4] == 5
    assert by[("1M", "Investability x Recommendation", "INVESTABLE|BUY")][7] == 80.0
    # 3M marginals are below min_n (5) and dropped; the ALL row stays
    assert ("3M", "ALL", "ALL") in by and by[("3M", "ALL", "ALL")][4] == 2
    assert ("3M", "Investability", "INVESTABLE") not in by
    # the ALL horizon aggregates every matured record
    assert by[("ALL", "ALL", "ALL")][4] == 13
    assert ("ALL", "Investability", "UNSPECIFIED") not in by, "n=1 cohort is below min_n"
    assert by[("ALL", "Investability", "INVESTABLE")][4] == 7
    # excluded records never count
    assert all(r[4] <= 13 for r in rows)
    # ordering: horizon order (1M before 3M before ALL), dimension order, n desc
    hz = [r[1] for r in rows]
    assert hz.index("1M") < hz.index("3M") < hz.index("ALL")
    first_1m = [r for r in rows if r[1] == "1M"]
    assert first_1m[0][2] == "ALL" and first_1m[1][2] == "Investability" and first_1m[1][3] == "INVESTABLE"


def test_min_n_and_empty_inputs():
    assert tp.forecast_scorecard_rows([]) == [] and tp.forecast_scorecard_rows(None) == []
    rows = tp.forecast_scorecard_rows(_records(), as_of="x", min_n=1)
    by = _by(rows)
    assert by[("3M", "Investability", "INVESTABLE")][4] == 1 and by[("3M", "Investability", "UNSPECIFIED")][4] == 1
    assert by[("ALL", "Entry Selected", "UNSPECIFIED")][4] == 1
    only_losses = tp.forecast_scorecard_rows([rec("L", "1W", -1.0)], as_of="x", min_n=1)
    r = _by(only_losses)[("1W", "ALL", "ALL")]
    assert r[7] == 0.0 and (r[8], r[9]) == tp.wilson_interval(0, 1)
    flat = tp.forecast_scorecard_rows([rec("F", "1W", 0.0)], as_of="x", min_n=1)
    assert _by(flat)[("1W", "ALL", "ALL")][7] == 0.0, "no decided outcome -> 0.0, not a crash"
    junk = tp.forecast_scorecard_rows(["junk", None, rec("J", "2W", 1.5)], as_of="x", min_n=1)
    assert _by(junk)[("2W", "ALL", "ALL")][4] == 1


class _WS:
    def __init__(self):
        self.cleared = 0; self.updates = []
    def clear(self):
        self.cleared += 1
    def update(self, rng, values):
        self.updates.append((rng, values))


class _Sheet:
    def __init__(self, existing=False):
        self.ws = _WS(); self.existing = existing; self.added = []
    def worksheet(self, name):
        if not self.existing:
            raise KeyError(name)
        return self.ws
    def add_worksheet(self, title, rows, cols):
        self.added.append((title, rows, cols)); return self.ws


class _Backoff:
    def execute_sync(self, fn, *args):
        return fn(*args)


class _Store:
    def __init__(self, sheet, available=True):
        self.sheet = sheet; self._a = available; self.backoff = _Backoff()
    def is_available(self):
        return self._a


def _app(store):
    app = object.__new__(tp.PerformanceTrackerApp)
    app.store = store
    return app


def test_publish_overwrites_in_place_and_creates_the_tab(capsys):
    sheet = _Sheet(existing=False)
    assert _app(_Store(sheet))._publish_forecast_scorecard(_records()) is True
    assert sheet.added and sheet.added[0][0] == "_Forecast_Scorecard" and sheet.added[0][2] == 14
    assert sheet.ws.cleared == 1 and len(sheet.ws.updates) == 1
    rng, values = sheet.ws.updates[0]
    assert rng == "A1" and values[0] == tp.FORECAST_SCORECARD_HEADER
    assert len(values) - 1 == len(tp.forecast_scorecard_rows(_records(), as_of="x"))
    assert "FORECAST SCORECARD:" in capsys.readouterr().out
    again = _Sheet(existing=True)
    assert _app(_Store(again))._publish_forecast_scorecard(_records()) is True
    assert again.added == [] and again.ws.cleared == 1, "existing tab is cleared then rewritten, never appended"


def test_publish_is_best_effort_and_gated(monkeypatch):
    assert _app(_Store(_Sheet(), available=False))._publish_forecast_scorecard(_records()) is False
    app = _app(None)
    assert app._publish_forecast_scorecard(_records()) is False
    boom = _Sheet(existing=True)
    boom.ws.update = lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("APIError"))
    assert _app(_Store(boom))._publish_forecast_scorecard(_records()) is False, "a sheet fault never raises"
    monkeypatch.setenv("TRACK_FORECAST_SCORECARD", "0")
    quiet = _Sheet(existing=True)
    assert _app(_Store(quiet))._publish_forecast_scorecard(_records()) is False and quiet.ws.cleared == 0


def test_hook_sits_in_the_audit_lane_after_the_calibration_publish():
    src = open(tp.__file__, encoding="utf-8").read()
    cal = src.index("_get_executor(), self._publish_s1_calibration, records")
    fs = src.index("_get_executor(), self._publish_forecast_scorecard, records")
    audit = src.rindex("if self.args.audit:", 0, cal)
    analyze = src.index("self.analyzer.analyze(records)", fs)
    assert audit < cal < fs < analyze, "scorecard publish inside the audit branch, after calibration, before analyze"


def test_dq_audit_registers_the_tab():
    assert dq.KNOWN_TABS["_Forecast_Scorecard"][4] == "overwrite"
    assert dq.TAB_STAMP_COLUMN["_Forecast_Scorecard"] == "As Of (Riyadh)"
