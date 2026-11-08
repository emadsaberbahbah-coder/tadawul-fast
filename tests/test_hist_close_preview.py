"""track_performance v6.60.0 [HIST-PREVIEW] harness.

TRACK_HIST_TARGET_CLOSE=observe logs how MANY past-due records the archive
close would mature ('[HIST-CLOSE v6.47.0] mode=observe ... witnessed=3598'
on 2026-11-08) but never WHAT enforce would publish. With
TRACK_HIST_CLOSE_PREVIEW on, the observe pass keeps the judge's verdicts and
logs the directional win / loss split, the ROI spread, the split by horizon
and by entry investability, and the largest moves. Off (default) is v6.53.0.

Runs as a script ("PASS k/k") and under pytest. Zero network: the real
audit loop (PerformanceTrackerApp.audit_active_records) runs with a fake
backend transport, offline calendar sessions and a fake BigQuery session.
Controls: TFB_TEST_PREVIEW_TARGET (the tracker under test; the untouched main
copy there is the golden negative), TFB_TEST_TRACK_BASE (a path to the main
c7e666a4 copy instead of `git show`), TFB_TEST_REQUIRE_BASE=1 (a missing base
FAILS the base legs instead of skipping them; set in CI).

  G1  the gate: unset and off words read off, the on words read on
  O1  off: observe / enforce / off audits equal the untouched main copy
      (records, receipts, stats, _LAST_HIST_CLOSE, every log line); no
      preview line; the gate is not read outside observe
  O2  on: records, receipts, stats and every v6.53.0 log line unchanged; the
      preview lines follow the HIST-CLOSE and ARCHIVE-BAR lines
  N1  the summary of a nine-record pass (one refused): counts, ROI, call
      ROI, by horizon, by bucket, outliers, and the exact lines; the yfinance
      source; one symbol at two horizons; a weekend target's session date
  N5  the CA-GUARD on (its production default): a confirmed ledger factor
      re-bases the ROI as enforce does, the verify band is counted, and the
      'settled' tally equals what a real enforce pass matures
  N2  outlier samples: the five largest |ROI|, ties by symbol
  N3  no witnessed record: one 'witnessed=0' line
  N4  the pure summary: refusals, NaN ROIs, HOLD, breakeven, sells, -0.00,
      the inclusive 50 pp mark
  R1  robustness: a raising summary (one WARNING, one 'unavailable' line),
      malformed summaries
  W1  the clock step maps the Variable once, default 'off'; daily_sync none
  V1  version floor, title, WHY order, verifier pin, the CI step, ASCII
  V2  against main: ASCII additions, nothing removed, the additions
  X1  the main copy logs no preview with the gate on
"""
from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import difflib
import importlib.util
import json
import logging
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
DELIVERED = Path(os.getenv("TFB_TEST_PREVIEW_TARGET", "").strip() or TRACKER)
BASE_SHA = "c7e666a4bba1165cace2cb839d7839a2bc01c78f"   # main at track_performance 6.53.0
GATE = "TRACK_HIST_CLOSE_PREVIEW"
MAPPING = "TRACK_HIST_CLOSE_PREVIEW: ${{ vars.TRACK_HIST_CLOSE_PREVIEW || 'off' }}"
TAG = "[v6.60.0 HIST-PREVIEW]"
HELPERS = {"_hist_close_preview_enabled", "_hist_close_preview", "_hist_close_preview_lines"}
NESTED = {"tally", "rate", "part"}      # local helpers inside the two new functions
GATES = ("TRACK_HIST_TARGET_CLOSE", "TRACK_HIST_CLOSE_BUDGET", "TRACK_HIST_CLOSE_TIME_SEC",
         "TRACK_HIST_CLOSE_SOURCE", "TRACK_AUDIT_DUE_ONLY", "TFB_BQ_PROJECT", "TFB_BQ_DATASET",
         "TFB_BQ_LOCATION", "TFB_TRACK_FALLBACK_SHUFFLE", "TFB_TRACK_FEED_LEGACY",
         "TFB_PERF_LEGACY_FEED", "TFB_TRACK_PRICE_FALLBACK", GATE)
OFF_WORDS = (None, "", " ", "0", "off", "OFF", "false", "no", "enable", "enabled",
             "observe", "enforce", "2", "onn", "of", "yes please")
ON_WORDS = ("1", "on", "ON", " true ", "yes", "Yes")
TITLE = "TADAWUL FAST BRIDGE \u2013 ADVANCED PERFORMANCE ANALYTICS ENGINE (v%s)"
UNAVAILABLE = ("%s mode=observe unavailable (the preview could not be computed; see the log)" % TAG)

UTC = timezone.utc
NOW = datetime(2026, 10, 8, 21, tzinfo=UTC)          # Thu, after the US close
CLOSE = datetime(2026, 10, 8, 20, tzinfo=UTC)        # today's session close
TARGET = CLOSE - timedelta(days=7)                   # Thu 2026-10-01 20:00Z
SESSION = "2026-10-01"
BQ_PROJECT = "project-2e3530e6-2311-4aa3-b36"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


tp = _load("tp_hist_preview", DELIVERED)
_BASE = {}


def base():
    """The untouched main copy (c7e666a4, v6.53.0), or skip (or fail when
    TFB_TEST_REQUIRE_BASE=1)."""
    if "mod" not in _BASE:
        path = os.getenv("TFB_TEST_TRACK_BASE", "").strip()
        if not path:
            try:
                text = subprocess.run(["git", "show", BASE_SHA + ":scripts/track_performance.py"],
                                      cwd=ROOT, capture_output=True, text=True, check=True).stdout
            except Exception:
                text = ""
            if text:
                tmp = Path(tempfile.mkdtemp()) / "tp_v6530_main.py"
                tmp.write_text(text, encoding="utf-8")
                path = str(tmp)
        _BASE["path"] = path
        _BASE["mod"] = _load("tp_hist_preview_base", path) if path else None
    if _BASE["mod"] is None:
        if os.getenv("TFB_TEST_REQUIRE_BASE", "").strip() == "1":
            raise AssertionError("base copy required (TFB_TEST_REQUIRE_BASE=1) but unavailable")
        pytest.skip("base copy unavailable (set TFB_TEST_TRACK_BASE)")
    return _BASE["mod"]


@contextlib.contextmanager
def gate(value):
    old = os.environ.get(GATE)
    try:
        if value is None:
            os.environ.pop(GATE, None)
        else:
            os.environ[GATE] = value
        yield
    finally:
        if old is None:
            os.environ.pop(GATE, None)
        else:
            os.environ[GATE] = old


# --------------------------------------------------------------- fixtures ---
def calendar_fixture(name, day):
    """Lean offline sessions (as tests/test_outcome_price_evidence.py)."""
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


def record(mod, symbol, reco="BUY", inv="INVESTABLE", horizon="WEEK_1", entry=100.0, target=TARGET):
    return mod.PerformanceRecord(
        record_id="synthetic-%s-%s" % (symbol, horizon), symbol=symbol,
        horizon=getattr(mod.HorizonType, horizon), date_recorded=target - timedelta(days=7),
        entry_price=entry, entry_recommendation=getattr(mod.RecommendationType, reco), entry_score=70,
        entry_risk_bucket="MEDIUM", entry_confidence="HIGH", origin_tab="synthetic",
        target_price=entry * 1.1, target_roi=10, target_date=target,
        status=mod.PerformanceStatus.ACTIVE, last_updated=NOW, entry_investability_status=inv)


def row(symbol, price):
    return {"symbol": symbol, "current_price": price, "data_provider": "yahoo_chart",
            "acquisition_status": "success", "acquisition_provider": "yahoo_chart",
            "acquisition_acquired_at": NOW.isoformat(), "acquisition_quote_asof": CLOSE.isoformat()}


def bars(target_close, source="yahoo_chart_v8", session=SESSION):
    """Weekday daily bars 2026-09-21..2026-10-08; the target session closes at
    `target_close`, every other day at 100 + i."""
    out, day, i = [], datetime(2026, 9, 21), 0
    while day <= datetime(2026, 10, 8):
        if day.weekday() < 5:
            d = day.strftime("%Y-%m-%d")
            out.append({"date": d, "close": target_close if d == session else 100.0 + i,
                        "source": source if d == session else "yahoo_chart_v8"})
            i += 1
        day += timedelta(days=1)
    return out


# symbol: (call, entry investability, horizon, entry, target close, current quote, bar source)
CASE = {
    "AAA.US": ("BUY", "INVESTABLE", "WEEK_1", 100.0, 104.0, 105.0, "yahoo_chart_v8"),   # +4 win
    "BBB.US": ("BUY", "INVESTABLE", "MONTH_1", 100.0, 97.0, 98.0, "yahoo_chart_v8"),    # -3 loss
    "CCC.US": ("SELL", "WATCHLIST", "WEEK_1", 100.0, 95.0, 96.0, "yahoo_chart_v8"),     # -5 sell win
    "DDD.US": ("SELL", "WATCHLIST", "WEEK_2", 50.0, 52.0, 53.0, "yahoo_chart_v8"),      # +4 sell loss
    "EEE.US": ("HOLD", "WATCHLIST", "MONTH_1", 100.0, 102.0, 103.0, "yahoo_chart_v8"),  # +2 neutral
    "FFF.US": ("BUY", "BLOCKED", "MONTH_3", 10.0, 16.0, 16.5, "yahoo_chart_v8"),        # +60 win, outlier
    "GGG.US": ("BUY", "INVESTABLE", "WEEK_1", 100.0, 100.0, 101.0, "yahoo_chart_v8"),   # 0 breakeven
    "HHH.US": ("BUY", "WATCHLIST", "WEEK_1", 100.0, 110.0, 111.0, "yahoo_chart_v8_1h"),  # refused: not daily
    "III.US": ("BUY", "INVESTABLE", "WEEK_1", 100.0, 40.0, 41.0, "yahoo_chart_v8"),     # -60 loss, outlier
}


def fixture(mod, case=None):
    """case: {symbol: spec} or [(symbol, spec), ...] (one symbol may repeat at
    other horizons; its first spec sets the quote and the archive series).
    spec = (call, investability, horizon, entry, target close, quote, bar
    source[, target time, target session])."""
    case = case or CASE
    items = list(case.items()) if isinstance(case, dict) else list(case)
    records = [record(mod, s, c[0], c[1], c[2], c[3], *(c[7:8] or [TARGET])) for s, c in items]
    rows, archive = [], {}
    for s, c in items:
        if s not in archive:
            rows.append(row(s, c[5]))
            archive[s] = bars(c[4], c[6], *(c[8:9] or [SESSION]))
    return records, rows, archive


# ------------------------------------------------------------ fake BigQuery --
BQ_FIELDS = ["symbol", "date", "open", "high", "low", "close", "adj_close", "source"]


class FakeResp:
    def __init__(self, code, payload):
        self.status_code = code
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class FakeSession:
    """An AuthorizedSession stand-in answering jobs.query from `archive`."""

    def __init__(self, archive):
        self.archive = archive

    def post(self, url, json=None, timeout=None, **kw):
        params = {p["name"]: p for p in json["queryParameters"]}
        syms = [v["value"] for v in params["symbols"]["parameterValue"]["arrayValues"]]
        since, until = params["since"]["parameterValue"]["value"], params["until"]["parameterValue"]["value"]
        rows = []
        for sym in syms:
            for bar in self.archive.get(sym, []):
                if since <= bar["date"] <= until:
                    rows.append({"f": [{"v": sym}, {"v": bar["date"]}, {"v": None}, {"v": None}, {"v": None},
                                       {"v": str(bar["close"])}, {"v": None}, {"v": bar["source"]}]})
        return FakeResp(200, {"kind": "bigquery#queryResponse", "jobComplete": True,
                              "jobReference": {"projectId": BQ_PROJECT, "jobId": "j1", "location": "US"},
                              "schema": {"fields": [{"name": n, "type": "STRING"} for n in BQ_FIELDS]},
                              "rows": rows, "totalRows": str(len(rows))})

    def get(self, url, params=None, timeout=None, **kw):
        raise AssertionError("no poll expected")


class Run:
    def __init__(self):
        self.logs = []


@contextlib.contextmanager
def harness(mod, env, archive, pull=None, patches=None):
    run = Run()
    saved_env = {k: os.environ.get(k) for k in GATES}
    for k in GATES:
        os.environ.pop(k, None)
    for k, v in env.items():
        if v is not None:
            os.environ[k] = v
    swaps = {"_utc_now": lambda: NOW,
             "_outcome_calendar_schedule": calendar_fixture,
             "_price_fallback_enabled": lambda: False,
             "_ca_guard_enabled": lambda: False,
             "_feed_legacy": lambda: False,
             "_ca_ledger_index": lambda: {},
             "_archive_bar_session": lambda: FakeSession(archive),
             "_hist_close_pull": pull or (lambda symbol: (_ for _ in ()).throw(AssertionError("yfinance pull"))),
             "urlopen": lambda *a, **k: (_ for _ in ()).throw(AssertionError("live transport"))}
    swaps.update(patches or {})
    saved = {k: getattr(mod, k) for k in swaps}
    saved_now = mod.RiyadhTime.__dict__["now"]
    handler = logging.Handler()
    handler.emit = lambda rec: run.logs.append((rec.levelname, rec.getMessage()))
    mod.logger.addHandler(handler)
    saved_level = mod.logger.level
    mod.logger.setLevel(logging.DEBUG)
    try:
        for k, v in swaps.items():
            setattr(mod, k, v)
        mod.RiyadhTime.now = staticmethod(lambda: NOW)
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


def audit(mod, mode, preview, case=None, source="bq_archive", pull=None, patches=None):
    records, rows, archive = fixture(mod, case)
    env = {"TRACK_HIST_TARGET_CLOSE": mode, "TRACK_HIST_CLOSE_SOURCE": source, GATE: preview}
    if hasattr(mod, "_LAST_HIST_PREVIEW"):
        mod._LAST_HIST_PREVIEW.clear()
    with harness(mod, env, archive, pull, patches) as run:
        app = mod.PerformanceTrackerApp.__new__(mod.PerformanceTrackerApp)
        app.backend = mod.BackendClient("https://transport.invalid")
        app.backend.post_json = AsyncMock(return_value=({"rows": rows}, None, 200))
        asyncio.run(app.audit_active_records(records))
        run.stats = dict(getattr(app, "_last_audit_stats", {}) or {})
        run.hist = json.loads(json.dumps(dict(mod._LAST_HIST_CLOSE), default=str, sort_keys=True))
        run.preview = dict(getattr(mod, "_LAST_HIST_PREVIEW", {}) or {})
        run.records = records
    return run


def daily_pull(case=None):
    """A yfinance pull answering the fixture's archive series as daily bars."""
    archive = fixture(tp, case)[2]
    daily = {s: [dict(bar, split=0.0, source="yahoo_chart_v8") for bar in v] for s, v in archive.items()}

    def pull(symbol):
        key = symbol if symbol in daily else symbol + ".US"
        return [dict(bar) for bar in daily.get(key, [])]
    return pull


def state(rec):
    d = dataclasses.asdict(rec)
    d["notes"] = re.sub(r'"tracker_version":"[0-9.]+"', '"tracker_version":"X"', d["notes"] or "")
    d["last_updated"] = None
    return json.loads(json.dumps(d, default=str, sort_keys=True))


def norm_logs(mod, logs):
    """Wall-clock figures out; everything else exact (no non-preview line
    carries the script version)."""
    out = []
    for lvl, msg in logs:
        msg = re.sub(r"query_sec=\d+\.\d+", "query_sec=T", msg)
        msg = re.sub(r"\b\d+\.\d+s\b", "Ts", msg)
        out.append((lvl, msg))
    return out


def preview_lines(run):
    return [msg for _lvl, msg in run.logs if msg.startswith(TAG)]


def ci(w, n):
    lo, hi = tp.wilson_interval(w, n)
    return "CI[%d,%d]" % (round(lo), round(hi))


# ----------------------------------------------------------------- G1 -------
@pytest.mark.parametrize("word", OFF_WORDS)
def test_g1_off_words(word):
    with gate(word):
        assert tp._hist_close_preview_enabled() is False


@pytest.mark.parametrize("word", ON_WORDS)
def test_g1_on_words(word):
    with gate(word):
        assert tp._hist_close_preview_enabled() is True


# ----------------------------------------------------------------- O1 -------
@pytest.mark.parametrize("word", (None, "off", "enable", "observe"))
def test_o1_off_observe_has_no_preview(word):
    run = audit(tp, "observe", word)
    assert preview_lines(run) == [] and run.preview == {}
    hist = [m for _l, m in run.logs if m.startswith("[HIST-CLOSE v")]
    assert hist == ["[HIST-CLOSE v6.47.0] mode=observe candidates=9 witnessed=8 (1M:2 1W:4 2W:1 3M:1) "
                    "matured=0 refused=1 (hist_close_archive_not_daily:1) pulls=0 ok=0 failed=0 "
                    "breaker=closed deferred=0"]


def test_o1_off_equals_main_copy():
    b = base()
    pairs = (("observe", None), ("observe", "off"), ("observe", "enable"), ("observe", "0"),
             ("enforce", "on"), ("off", "on"), ("enforce", None))
    for source in ("bq_archive", "yfinance", "bq_archive_then_yfinance"):
        for mode, word in pairs:
            got = audit(tp, mode, word, source=source, pull=daily_pull())
            want = audit(b, mode, word, source=source, pull=daily_pull())
            key = (source, mode, word)
            assert [state(r) for r in got.records] == [state(r) for r in want.records], key
            assert got.stats == want.stats and got.hist == want.hist, key
            assert norm_logs(tp, got.logs) == norm_logs(b, want.logs), key
            assert preview_lines(got) == [], key


def test_o1_gate_not_read_outside_observe():
    calls = []
    real = tp._hist_close_preview_enabled
    try:
        tp._hist_close_preview_enabled = lambda: calls.append(1) or True
        for mode in ("off", "enforce"):
            run = audit(tp, mode, "on")
            assert preview_lines(run) == [], mode
        assert calls == []
        run = audit(tp, "observe", "on")
        assert calls == [1] and preview_lines(run)
    finally:
        tp._hist_close_preview_enabled = real


# ----------------------------------------------------------------- O2 -------
def test_o2_on_adds_lines_only():
    off, on = audit(tp, "observe", None), audit(tp, "observe", "on")
    assert [state(r) for r in on.records] == [state(r) for r in off.records]
    assert on.stats == off.stats and on.hist == off.hist
    added = [x for x in on.logs if x[1].startswith(TAG)]
    assert norm_logs(tp, [x for x in on.logs if not x[1].startswith(TAG)]) == norm_logs(tp, off.logs)
    assert added and all(lvl == "WARNING" for lvl, _m in added)
    msgs = [m for _l, m in on.logs]
    first = msgs.index(added[0][1])
    assert msgs[first - 1].startswith("[ARCHIVE-BAR v6.53.0]")       # right after the archive line
    assert msgs[first - 2].startswith("[HIST-CLOSE v6.47.0]")
    assert msgs[first:first + len(added)] == [m for _l, m in added]   # contiguous
    assert all(m.isascii() for _l, m in added)


# ----------------------------------------------------------------- N1 -------
def test_n1_summary_and_lines():
    run = audit(tp, "observe", "on")
    pv = run.preview
    # AAA +4 W, BBB -3 L, CCC -5 sell W, DDD +4 sell L, EEE +2 HOLD, FFF +60 W,
    # GGG 0 BE, III -60 L; HHH refused (hourly-rebuilt bar)
    assert (pv["witnessed"], pv["n"], pv["decided"], pv["wins"], pv["losses"], pv["breakeven"], pv["hold"]) == \
           (8, 8, 6, 3, 3, 1, 1)
    assert pv["win_rate"] == 50.0
    assert (pv["price_roi_mean"], pv["price_roi_median"], pv["price_roi_min"], pv["price_roi_max"]) == \
           (0.25, 1.0, -60.0, 60.0)
    assert pv["call_roi_mean"] == round((4 - 3 + 5 - 4 + 60 + 0 - 60) / 7, 2) == 0.29
    assert pv["outliers"] == 2
    assert list(pv["by_horizon"]) == ["1W", "2W", "1M", "3M"]
    assert {h: (t["n"], t["decided"], t["wins"]) for h, t in pv["by_horizon"].items()} == \
           {"1W": (4, 3, 2), "2W": (1, 1, 0), "1M": (2, 1, 0), "3M": (1, 1, 1)}
    assert {b: (t["n"], t["decided"], t["wins"]) for b, t in pv["by_bucket"].items()} == \
           {"BLOCKED": (1, 1, 1), "INVESTABLE": (4, 3, 1), "WATCHLIST": (3, 2, 1)}
    assert list(pv["by_bucket"]) == ["BLOCKED", "INVESTABLE", "WATCHLIST"]
    assert (pv["ca_guard"], pv["ca_ledger"], pv["ca_verify"]) == (False, 0, 0)
    assert pv["witnessed"] == int(re.search(r"witnessed=(\d+)", [m for _l, m in run.logs
                                  if m.startswith("[HIST-CLOSE v")][0]).group(1))
    assert preview_lines(run) == [
        "%s mode=observe witnessed=8 decided=6 wins=3 losses=3 breakeven=1 hold=1 win=50.0%% %s "
        "price_roi mean=+0.25%% median=+1.00%% min=-60.00%% max=+60.00%% call_roi=+0.29%% "
        "outliers(|roi|>=50pp)=2 ca_guard=off (ledger factors applied; dedup is enforce's; nothing written)"
        % (TAG, ci(3, 6)),
        "%s by horizon: 1W n=4 decided=3 win=66.7%% %s | 2W n=1 decided=1 win=0.0%% %s | "
        "1M n=2 decided=1 win=0.0%% %s | 3M n=1 decided=1 win=100.0%% %s" % (
            TAG, ci(2, 3), ci(0, 1), ci(0, 1), ci(1, 1)),
        "%s by bucket: BLOCKED n=1 decided=1 win=100.0%% %s | INVESTABLE n=4 decided=3 win=33.3%% %s | "
        "WATCHLIST n=3 decided=2 win=50.0%% %s" % (TAG, ci(1, 1), ci(1, 3), ci(1, 2)),
        "%s outliers: FFF.US 3M +60.00%% (entry 10.0000 -> close 16.0000, 2026-10-01) | "
        "III.US 1W -60.00%% (entry 100.0000 -> close 40.0000, 2026-10-01)" % TAG,
    ]


def test_n1b_yfinance_source_previews_too():
    run = audit(tp, "observe", "on", source="yfinance", pull=daily_pull())
    pv = run.preview
    # no archive refusal: HHH (+10, a buy) is witnessed too
    assert (pv["witnessed"], pv["wins"], pv["losses"], pv["breakeven"], pv["hold"]) == (9, 4, 3, 1, 1)
    assert len(preview_lines(run)) == 4


def test_n1c_one_symbol_two_horizons():
    case = [("DUP.US", ("BUY", "INVESTABLE", "WEEK_1", 100.0, 104.0, 105.0, "yahoo_chart_v8")),
            ("DUP.US", ("SELL", "WATCHLIST", "MONTH_1", 100.0, 104.0, 105.0, "yahoo_chart_v8"))]
    run = audit(tp, "observe", "on", case)
    pv = run.preview
    assert (pv["witnessed"], pv["decided"], pv["wins"], pv["losses"]) == (2, 2, 1, 1)
    assert {h: (t["n"], t["decided"], t["wins"]) for h, t in pv["by_horizon"].items()} == \
           {"1W": (1, 1, 1), "1M": (1, 1, 0)}
    assert any("witnessed=2 (1M:1 1W:1)" in m for _l, m in run.logs if m.startswith("[HIST-CLOSE v"))


def test_n1d_weekend_target_names_its_session():
    sat = datetime(2026, 10, 3, 20, tzinfo=UTC)          # Saturday; the session is Mon 5 Oct
    case = {"WKD.US": ("BUY", "INVESTABLE", "WEEK_1", 10.0, 16.0, 16.5, "yahoo_chart_v8", sat, "2026-10-05")}
    run = audit(tp, "observe", "on", case)
    assert run.preview["outlier_samples"][0]["session"] == "2026-10-05"
    assert preview_lines(run)[-1] == ("%s outliers: WKD.US 1W +60.00%% (entry 10.0000 -> close 16.0000, "
                                      "2026-10-05)" % TAG)


# ----------------------------------------------------------------- N2 -------
def test_n2_outlier_samples_top_five_ties_by_symbol():
    case = {"Z%d.US" % i: ("BUY", "INVESTABLE", "WEEK_1", 10.0, 10.0 + 6 + i % 3, 16.0 + i % 3, "yahoo_chart_v8")
            for i in range(7)}
    case["AB.US"] = ("SELL", "WATCHLIST", "WEEK_1", 10.0, 2.0, 2.1, "yahoo_chart_v8")   # -80
    run = audit(tp, "observe", "1", case)
    samples = run.preview["outlier_samples"]
    assert run.preview["outliers"] == 8 and len(samples) == 5
    assert [s["symbol"] for s in samples] == ["AB.US", "Z2.US", "Z5.US", "Z1.US", "Z4.US"]
    assert [s["roi"] for s in samples] == [-80.0, 80.0, 80.0, 70.0, 70.0]
    assert preview_lines(run)[-1].count(" | ") == 4


# ----------------------------------------------------------------- N3 -------
def test_n3_nothing_witnessed():
    case = {"HHH.US": CASE["HHH.US"]}
    run = audit(tp, "observe", "on", case)
    assert preview_lines(run) == ["%s mode=observe witnessed=0 (nothing for enforce to mature this pass)" % TAG]
    assert run.preview["witnessed"] == 0 and run.preview["by_horizon"] == {}


# ----------------------------------------------------------------- N4 -------
class _Ev:
    def __init__(self, price):
        self.price = price


def test_n4_pure_summary():
    # the CA-GUARD on (its default) with no ledger entry; hermetic: no sheet read
    with harness(tp, {}, {}, patches={"_ca_guard_enabled": lambda: True,
                                      "_ca_ledger_factor": lambda *a, **k: None}):
        _n4_body()


def _n4_body():
    mk = lambda sym, reco, inv="WATCHLIST": record(tp, sym, reco, inv)   # noqa: E731
    judged = [(mk("A.US", "BUY"), {"target_session": SESSION}, (_Ev(1.0), "", 1.0)),
              (mk("B.US", "SELL"), {"target_session": SESSION}, (None, "hist_close_bar_missing", float("nan"))),
              (mk("C.US", "HOLD"), {"target_session": SESSION}, (_Ev(1.0), "", float("nan"))),
              (mk("D.US", "HOLD"), {}, (_Ev(1.0), "", -0.004)),
              (mk("E.US", "REDUCE", ""), {"target_session": SESSION}, (_Ev(1.0), "", -2.0)),
              (mk("F.US", "SELL"), {"target_session": SESSION}, (_Ev(1.0), "", 0.0))]
    pv = tp._hist_close_preview(judged)
    assert (pv["witnessed"], pv["wins"], pv["losses"], pv["hold"], pv["decided"], pv["breakeven"]) == \
           (4, 2, 0, 1, 2, 1)
    assert pv["price_roi_min"] == -2.0 and str(pv["price_roi_median"]) == "0.0"   # -0.004 rounds to +0.00
    assert pv["call_roi_mean"] == 1.0 and pv["outliers"] == 0 and pv["outlier_samples"] == []
    assert set(pv["by_bucket"]) == {"UNSPECIFIED", "WATCHLIST"}
    lines = tp._hist_close_preview_lines(pv)
    assert len(lines) == 4 and "median=+0.00%" in lines[0]
    assert " ca_guard=on ledger=0 verify(|roi|>=15pp)=0 " in lines[0] and " settled " in lines[1]
    holds = tp._hist_close_preview([(mk("H.US", "HOLD"), {}, (_Ev(1.0), "", 3.0))])
    assert holds["call_roi_mean"] is None and holds["win_rate"] is None
    assert "win=n/a" in tp._hist_close_preview_lines(holds)[0] and "call_roi=n/a" in tp._hist_close_preview_lines(holds)[0]
    assert tp._hist_close_preview([])["witnessed"] == 0
    # the outlier mark is inclusive: exactly 50 pp either way counts, 49.99 does not
    edge = tp._hist_close_preview([(mk("P.US", "BUY"), {}, (_Ev(1.0), "", 50.0)),
                                   (mk("Q.US", "SELL"), {}, (_Ev(1.0), "", -50.0)),
                                   (mk("R.US", "BUY"), {}, (_Ev(1.0), "", 49.99))])
    assert edge["outliers"] == 2 and [o["symbol"] for o in edge["outlier_samples"]] == ["P.US", "Q.US"]



# ----------------------------------------------------------------- N5 -------
def _ca_patches(mod):
    """The CA-GUARD on (production default), a confirmed 21:20 bonus on
    BBB.US between entry and the target session, and no adjusted history
    (no network): the verify band cannot be resolved this pass."""
    def ledger(symbol, entry_date, target_session=None):
        if target_session is None:            # the judge's post-target check
            return None
        return 1.05 if mod._safe_str(symbol).upper() == "BBB.US" else None
    return {"_ca_guard_enabled": lambda: True, "_ca_ledger_factor": ledger,
            "_ca_adjusted_roi": lambda *a, **k: None}


def test_n5_ca_guard_on_matches_enforce():
    run = audit(tp, "observe", "on", patches=_ca_patches(tp))
    pv = run.preview
    assert (pv["ca_guard"], pv["ca_ledger"], pv["ca_verify"], pv["ca_verify_pp"]) == (True, 1, 2, 15.0)
    bbb = ((1 - 0.03) * 1.05 - 1) * 100.0                     # print -3% re-based by the bonus
    assert pv["price_roi_mean"] == round((4 + bbb - 5 + 4 + 2 + 60 + 0 - 60) / 8, 2)
    assert (pv["wins"], pv["losses"], pv["breakeven"], pv["hold"]) == (4, 2, 1, 1)   # BBB a win now
    # a real enforce pass on the same fixture, same CA patches
    enf = audit(tp, "enforce", None, patches=_ca_patches(tp))
    matured = [r for r in enf.records if r.status == tp.PerformanceStatus.MATURED]
    assert sorted(r.symbol for r in matured) == ["AAA.US", "BBB.US", "CCC.US", "DDD.US", "EEE.US", "GGG.US"]
    assert [r.realized_roi for r in matured if r.symbol == "BBB.US"] == [bbb]
    outcomes = [tp._record_outcome(r) for r in matured]
    st = pv["settled"]
    assert (st["n"], st["wins"], st["losses"], st["breakeven"], st["hold"]) == (
        len(matured), outcomes.count("WIN"), outcomes.count("LOSS"), outcomes.count("BREAKEVEN"),
        outcomes.count(None))
    lines = preview_lines(run)
    assert " ca_guard=on ledger=1 verify(|roi|>=15pp)=2 " in lines[0]
    assert lines[1] == ("%s settled (the verify band left out: what enforce matures as shown): all n=6 "
                        "decided=4 win=75.0%% %s wins=3 losses=1 breakeven=1 hold=1" % (TAG, ci(3, 4)))
    assert len(lines) == 5


# ----------------------------------------------------------------- R1 -------
def test_r1_robustness():
    with harness(tp, {}, {}) as run:
        assert tp._hist_close_preview([("not a triple",)]) == {}
    assert run.logs == [("WARNING", "%s preview unavailable: ValueError" % TAG)]
    for junk in (None, {}, [], "x", {"witnessed": 2}):
        assert tp._hist_close_preview_lines(junk) == [UNAVAILABLE]
    # a summary that fails inside the audit pass: one WARNING, one
    # 'unavailable' line, records and the HIST-CLOSE line unchanged
    real = tp.wilson_interval

    def raising(*a, **k):
        raise RuntimeError("boom")
    try:
        tp.wilson_interval = raising
        run = audit(tp, "observe", "on")
    finally:
        tp.wilson_interval = real
    off = audit(tp, "observe", None)
    assert preview_lines(run) == ["%s preview unavailable: RuntimeError" % TAG, UNAVAILABLE]
    assert run.preview == {}
    assert [state(r) for r in run.records] == [state(r) for r in off.records] and run.hist == off.hist


# ----------------------------------------------------------------- W1 -------
def test_w1_clock_lane_only():
    import yaml
    clock = ROOT / ".github/workflows/track_performance.yml"
    daily = ROOT / ".github/workflows/daily_sync.yml"
    text = clock.read_text(encoding="utf-8")
    assert text.count(GATE + ":") == 1 and text.count(MAPPING) == 1
    doc = yaml.safe_load(text)
    envs = [s.get("env") or {} for j in doc["jobs"].values() for s in j.get("steps") or []
            if "Run performance clock" in str(s.get("name") or "")]
    assert len(envs) == 1 and envs[0][GATE] == "${{ vars.TRACK_HIST_CLOSE_PREVIEW || 'off' }}"
    assert envs[0]["TRACK_HIST_TARGET_CLOSE"] == "${{ vars.TRACK_HIST_TARGET_CLOSE || 'off' }}"
    assert GATE not in daily.read_text(encoding="utf-8")            # the v6.47.0 lane rule
    assert text.index("# VERSION 1.0.14 (2026-11-08)") < text.index("# VERSION 1.0.7 (2026-11-02)")
    src = DELIVERED.read_text(encoding="utf-8")
    assert re.search(r'os\.getenv\("TRACK_HIST_CLOSE_PREVIEW"\)\s*or\s*"off"', src)


# ----------------------------------------------------------------- V1 -------
def test_v1_version_title_why_pin_ci_ascii():
    assert tuple(int(x) for x in tp.SCRIPT_VERSION.split(".")[:3]) >= (6, 60, 0), tp.SCRIPT_VERSION
    src = DELIVERED.read_text(encoding="utf-8")
    assert "(v%s)" % tp.SCRIPT_VERSION in src.splitlines()[4]
    v = re.search(r'^SCRIPT_VERSION = "%s"' % re.escape(tp.SCRIPT_VERSION), src, re.M)
    assert v and src.index("# v6.53.0 (2026-11-02)") < src.index("# v6.60.0 (2026-11-08)") < v.start()
    vd = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("track_performance", "%s", "track performance")' % tp.SCRIPT_VERSION in vd
    import yaml
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))["jobs"]
    steps = [s for s in jobs["lean-unit"]["steps"] if "tests/test_hist_close_preview.py" in str(s.get("run") or "")]
    assert len(steps) == 1, steps
    step = steps[0]
    assert "if" not in step and "continue-on-error" not in step and "continue-on-error" not in jobs["lean-unit"]
    assert str((step.get("env") or {}).get("TFB_TEST_REQUIRE_BASE")) == "1"
    run = step["run"].strip().splitlines()
    assert any(BASE_SHA in ln for ln in run) and run[-1].strip() == "python -m pytest -q tests/test_hist_close_preview.py"
    assert "|| true" not in step["run"]
    assert tp._HIST_PREVIEW_TAG == TAG and tp._HIST_PREVIEW_OUTLIER_PP == 50.0
    assert Path(__file__).read_text(encoding="utf-8").isascii()


# ----------------------------------------------------------------- V2 -------
def test_v2_against_main_ascii_nothing_removed():
    base()
    import ast
    old_text = Path(_BASE["path"]).read_text(encoding="utf-8")
    new_text = DELIVERED.read_text(encoding="utf-8")
    diff = list(difflib.unified_diff(old_text.splitlines(), new_text.splitlines(), lineterm="", n=0))
    added = [ln[1:] for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
    removed = [ln[1:] for ln in diff if ln.startswith("-") and not ln.startswith("---")]
    fresh = [ln for ln in added if ln != TITLE % tp.SCRIPT_VERSION]
    assert fresh and all(ln.isascii() for ln in fresh), [ln for ln in fresh if not ln.isascii()][:3]
    mine = {TITLE % "6.53.0", 'SCRIPT_VERSION = "6.53.0"',
            "                _hist_close_judge(_r, _ctx, _ev, now_r, _hist_stats)"}
    assert mine <= set(removed), removed
    assert not [ln for ln in removed if ln.startswith("# v6.")], removed

    def defs(text):
        return {n.name for n in ast.walk(ast.parse(text))
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    gone = defs(old_text) - defs(new_text)
    assert not gone, gone
    others = re.findall(r"^# v6\.(?:(?:5[4-9]|[7-9]\d)\.\d+|60\.[1-9]\d*|6[1-9]\.\d+) \(", new_text, re.M)
    if tp.SCRIPT_VERSION == "6.60.0" and not others:      # exactly this release
        assert set(removed) == mine, removed
        assert defs(new_text) - defs(old_text) == HELPERS | NESTED
    else:                                                  # merged with, or followed by, other releases
        assert HELPERS <= defs(new_text) - defs(old_text)


# ----------------------------------------------------------------- X1 -------
def test_x1_main_copy_logs_no_preview():
    b = base()
    assert not hasattr(b, "_hist_close_preview")
    run = audit(b, "observe", "on")
    assert preview_lines(run) == []
    assert any(m.startswith("[HIST-CLOSE v6.47.0] mode=observe candidates=9 witnessed=8") for _l, m in run.logs)


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = skipped = total = 0
    for name, fn in tests:
        params = [m for m in getattr(fn, "pytestmark", []) if m.name == "parametrize"]
        calls = ([((p,), "%s[%r]" % (name, p)) for p in params[0].args[1]] if params else [((), name)])
        for args, label in calls:
            total += 1
            try:
                fn(*args)
                passed += 1
                print("ok  ", label)
            except pytest.skip.Exception as exc:
                skipped += 1
                print("SKIP", label, "::", exc)
    print(f"PASS {passed}/{total}" + (f" (skipped {skipped})" if skipped else ""))
