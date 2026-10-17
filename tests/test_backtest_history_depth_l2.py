"""L2 (2026-10-17): the hypothesis backtest has no bars - track_performance
v6.46.0.

Reproduces the 2026-10-11 backtest_harness run offline: a Yahoo-shaped
provider whose `fetch_history(symbol, period="1mo", interval="1d")` has no
`days` keyword returns ~22 bars for "1mo" and ~505 for "2y". Pins:
* `_bt_days_to_period` (730 -> "2y"), rounding up;
* `_fetch_backtest_history` asks for the translated period on the TypeError
  path (default on) and falls back to the bare 1mo call only under the kill
  switch TFB_BACKTEST_PERIOD_FROM_DAYS=0;
* `run_backtest` on 16 x 22 bars: Universe 0, History Days 0, and the Notes
  cell names the cause; on 16 x 505 bars the universe is 16;
* `_bt_history_depth` / `_bt_empty_verdict`: the loud verdict only when every
  hypothesis has Universe Size 0;
* version 6.46.0 and the changelog entry.
No network, no Sheets, no engine readers (trigger_fn is passed explicitly).
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta
import sys
import types
from types import SimpleNamespace

import pytest

from scripts import track_performance as tracker

SYMBOLS = ["1120.SR", "2222.SR", "1180.SR", "2010.SR", "7010.SR", "1211.SR",
           "4013.SR", "1010.SR", "2350.SR", "4002.SR",
           "AAPL.US", "MSFT.US", "NVDA.US", "JPM.US", "INTC.US", "T.US"]
BARS_BY_PERIOD = {"1mo": 22, "2y": 505, "5y": 1259}


def _bars(n, start=date(2024, 1, 2)):
    out, d = [], start
    for i in range(n):
        d = d + timedelta(days=1)
        px = 100.0 + (i % 7) - (i % 3)
        out.append({"date": d.isoformat(), "open": px, "high": px + 1, "low": px - 1,
                    "close": px, "volume": 1000 + i})
    return out


def _fake_yahoo(calls):
    mod = types.ModuleType("core.providers.yahoo_chart_provider")

    async def fetch_history(symbol, period="1mo", interval="1d"):
        calls.append((symbol, period))
        return _bars(BARS_BY_PERIOD.get(period, 22))
    mod.fetch_history = fetch_history
    return mod


def _fake_eodhd(calls):
    mod = types.ModuleType("core.providers.eodhd_provider")

    async def fetch_history(symbol, days=None, *args, **kwargs):
        calls.append((symbol, "eodhd", days))
        return []
    mod.fetch_history = fetch_history
    return mod


@pytest.fixture
def providers(monkeypatch):
    calls = []
    monkeypatch.setitem(sys.modules, "core.providers.yahoo_chart_provider", _fake_yahoo(calls))
    monkeypatch.setitem(sys.modules, "core.providers.eodhd_provider", _fake_eodhd(calls))
    monkeypatch.delenv("TFB_BACKTEST_KSA_YF", raising=False)
    monkeypatch.delenv("TFB_BACKTEST_PERIOD_FROM_DAYS", raising=False)
    return calls


def _hypothesis(h=20):
    return tracker.Hypothesis("FLOW_STRONG_ACCUM", "Flow strong accumulation precedes upside",
                              tracker.HypothesisType.PRICE_STRUCTURE, "flow", "STRONG_ACCUMULATION",
                              tracker.HypothesisDirection.BULLISH, horizon_days=h)


# ---------------------------------------------------------------------------
# period translation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("days,period", (
    (0, "5d"), (5, "5d"), (6, "1mo"), (30, "1mo"), (31, "1mo"), (32, "3mo"), (90, "3mo"),
    (180, "6mo"), (365, "1y"), (366, "1y"), (367, "2y"), (730, "2y"), (731, "2y"), (732, "5y"),
    (1825, "5y"), (3650, "10y"), (5000, "max"), ("730", "2y"), (None, "5d"), ("junk", "5d"),
))
def test_days_round_up_to_the_next_yahoo_range(days, period):
    assert tracker._bt_days_to_period(days) == period


def test_default_history_window_translates_to_two_years(monkeypatch):
    monkeypatch.delenv("BACKTEST_HISTORY_DAYS", raising=False)
    assert tracker._BT_DEFAULT_DAYS == 730
    assert tracker._bt_days_to_period(tracker._BT_DEFAULT_DAYS) == "2y"


@pytest.mark.parametrize("value,expected", (
    (None, True), ("", True), ("1", True), ("on", True), ("0", False), ("false", False),
    ("off", False), ("no", False), ("OFF", False),
))
def test_period_translation_kill_switch(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("TFB_BACKTEST_PERIOD_FROM_DAYS", raising=False)
    else:
        monkeypatch.setenv("TFB_BACKTEST_PERIOD_FROM_DAYS", value)
    assert tracker._bt_period_from_days_enabled() is expected


# ---------------------------------------------------------------------------
# the fetch path, reproduced offline
# ---------------------------------------------------------------------------
def _fetch(symbols, days=730):
    app = SimpleNamespace()
    coro = tracker.PerformanceTrackerApp._fetch_backtest_history(app, symbols, days)
    return asyncio.run(coro)


def test_fetch_asks_yahoo_for_the_translated_period_after_the_typeerror(providers):
    history = _fetch(SYMBOLS, 730)
    assert sorted(history) == sorted(SYMBOLS)
    assert {len(v) for v in history.values()} == {505}
    yahoo_calls = [c for c in providers if c[1] != "eodhd"]
    assert sorted(yahoo_calls) == sorted((s, "2y") for s in SYMBOLS)   # exactly one translated call each
    assert not [c for c in providers if c[1] == "eodhd"]               # first non-empty wins, as before
    # ascending, no-lookahead order is preserved
    dates = [b["date"] for b in history["AAPL.US"]]
    assert dates == sorted(dates)


def test_fetch_kill_switch_restores_the_bare_one_month_call(providers, monkeypatch):
    monkeypatch.setenv("TFB_BACKTEST_PERIOD_FROM_DAYS", "0")
    history = _fetch(SYMBOLS[:3], 730)
    assert {len(v) for v in history.values()} == {22}
    assert sorted(providers) == sorted((s, "1mo") for s in SYMBOLS[:3])


def test_fetch_passes_days_straight_through_when_the_provider_accepts_it(providers, monkeypatch):
    mod = types.ModuleType("core.providers.yahoo_chart_provider")

    async def fetch_history(symbol, days=None, **kwargs):
        providers.append((symbol, "days", days))
        return _bars(60)
    mod.fetch_history = fetch_history
    monkeypatch.setitem(sys.modules, "core.providers.yahoo_chart_provider", mod)
    history = _fetch(["AAPL.US"], 730)
    assert len(history["AAPL.US"]) == 60
    assert providers == [("AAPL.US", "days", 730)]


# ---------------------------------------------------------------------------
# run_backtest: the cause is named, the depth is honest
# ---------------------------------------------------------------------------
def test_shallow_history_yields_universe_zero_with_a_named_cause():
    history = {s: _bars(22) for s in SYMBOLS}
    res = tracker.run_backtest(_hypothesis(20), history, trigger_fn=lambda bars: False,
                               nonoverlap=False, tstat_floor=0.0)
    assert res.status == tracker.HypothesisStatus.INSUFFICIENT_DATA
    assert res.universe_size == 0 and res.history_days == 0 and res.sample_size == 0
    assert res.notes.startswith("no usable history: 0/16 symbols with >= 33 bars (deepest 22); "
                                "provider window too shallow -- 0 trigger events (< min ")


def test_deep_history_counts_the_universe_and_drops_the_cause_text():
    history = {s: _bars(505) for s in SYMBOLS}
    res = tracker.run_backtest(_hypothesis(20), history, trigger_fn=lambda bars: len(bars) % 9 == 0,
                               nonoverlap=False, tstat_floor=0.0)
    assert res.universe_size == 16 and res.history_days == 505
    assert res.sample_size > 0
    assert "no usable history" not in res.notes


def test_empty_history_keeps_the_legacy_note():
    res = tracker.run_backtest(_hypothesis(20), {}, trigger_fn=lambda bars: False,
                               nonoverlap=False, tstat_floor=0.0)
    assert res.universe_size == 0
    assert res.notes.startswith("0 trigger events (< min ")


def test_depth_and_empty_verdict():
    shallow = {s: _bars(22) for s in SYMBOLS}
    depth = tracker._bt_history_depth(shallow, 33, requested=16)
    assert depth == {"requested": 16, "fetched": 16, "usable": 0, "need": 33, "deepest": 22, "median": 22}
    zero = [SimpleNamespace(universe_size=0) for _ in range(6)]
    cause = tracker._bt_empty_verdict(zero, depth)
    assert cause.startswith("[BACKTEST-EMPTY v6.46.0] 16/16 symbols returned bars but 0 reached the 33-bar minimum (deepest 22, median 22)")
    assert "TFB_BACKTEST_PERIOD_FROM_DAYS" in cause
    assert tracker._bt_empty_verdict([], depth) is None
    assert tracker._bt_empty_verdict(zero + [SimpleNamespace(universe_size=3)], depth) is None
    mixed = {"A": _bars(22), "B": _bars(40), "C": None}
    assert tracker._bt_history_depth(mixed, 33) == {"requested": 3, "fetched": 2, "usable": 1,
                                                     "need": 33, "deepest": 40, "median": 40}


def test_runlog_warning_row_is_fail_open_and_names_the_registry():
    rows = []

    class _WS:
        def append_row(self, row, value_input_option=None):
            rows.append((row, value_input_option))

    class _Sheet:
        def worksheet(self, name):
            assert name == "_Run_Log"
            return _WS()

    registry = SimpleNamespace(sheet=_Sheet(), backoff=SimpleNamespace(execute_sync=lambda fn: fn()))
    app = SimpleNamespace()
    tracker.PerformanceTrackerApp._append_runlog_backtest_warning(app, registry, "cause text", {"usable": 0})
    assert len(rows) == 1
    row, option = rows[0]
    assert option == "USER_ENTERED"
    assert row[1:6] == ["WARNING", "track_performance", "Hypothesis_Registry", "BACKTEST_EMPTY", "cause text"]
    assert row[9] == '{"usable": 0}'
    # no sheet -> silent no-op; a raising sheet -> swallowed
    tracker.PerformanceTrackerApp._append_runlog_backtest_warning(app, SimpleNamespace(sheet=None), "x", {})

    class _Boom:
        def worksheet(self, name):
            raise RuntimeError("offline")
    tracker.PerformanceTrackerApp._append_runlog_backtest_warning(
        app, SimpleNamespace(sheet=_Boom(), backoff=None), "x", {})


def test_version_and_changelog():
    assert tracker.SCRIPT_VERSION == "6.46.0"
    src = open(tracker.__file__, encoding="utf-8").read()
    assert "# v6.46.0 (2026-10-17) - L2 HYPOTHESIS BACKTEST HAS NO BARS" in src
    assert "TFB_BACKTEST_PERIOD_FROM_DAYS=0" in src
