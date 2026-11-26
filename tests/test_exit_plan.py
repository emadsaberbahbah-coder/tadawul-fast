#!/usr/bin/env python3
"""tests/test_exit_plan.py - scripts/exit_plan.py 1.1.0: stop and target per holding, reviewed after every close.

  X1  Yahoo chart parsing: incomplete bars dropped, dividend factor applied (an implausible one ignored)
  X2  ATR of the last 14 true ranges
  X3  the initial stop from the stock's own ATR, kept between 8% and 12% (Tadawul at most 8%)
  X4  before +1R: the initial stop holds, the target is entry + 1R
  X5  after +1R: at least the entry, trailing 3xATR under the highest close; the target steps up per R
  X6  the tightening triggers (news, forecast with reliability, a lost 20-day average after +2R) raise the stop
  X7  the stop never moves down; RAISE STOP when it rises
  X8  SELL when the last close is at or below the stop
  X9  TAKE PROFIT once, only while the close is at the target; a touch the price left is a note
      (Codex P2 on #956: an intraday touch on the last bar is not actionable after the close)
  X10 REVIEW after the time stop without +1R; earnings and broker-stop notes
  X11 dividend-adjusted entry; a dividend paid after a stop was recorded rebases that stop (Codex P1 on #956)
  X12 workbook parsers: lots, news window, broker stops freshness, plan picks, prior row
  X13 run(): the gate, the two tabs, a same-day re-run keeps the published state (Codex P2 on #956), the
      e-mail, fixed income, no price history
  X14 the workflow, the manifest pin and the CI lane (1.1.0: hourly, the e-mail only after the 07:30 run)
  X15 (1.1.0) an open session's bar is set aside: levels on completed closes, its price shown as Live;
      TAKE PROFIT stays for the day it was issued; live_quote for the Daily Plan
  X16 (1.4.0) a dividend whose tax is recorded in _Dividend_Log is counted net of that tax: the log parser, the
      rescaled bars, the entry and the rebased stop, the run with the gate on and off
  X19 (1.6.0) [MARKET-TURN]: the gate and its labels; which holdings carry a market label; observe = a note, the
      action unchanged; on = SELL; off = nothing read; the Market ledger column and a 1.5.0 ledger read as before;
      the page rule line and summary; the workflow knobs
  X20 (1.6.1) [LOG-PRIVACY]: on GitHub Actions the run log names no holding, price or pick; elsewhere, and with
      TFB_EXIT_LOG_DETAIL=on, the full lines; the page, the ledger and the e-mail are unchanged

Run: pytest -q tests/test_exit_plan.py
"""
from __future__ import annotations

import datetime as dt
import json
import re
import sys
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import exit_plan as ep  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 11, 4, 30, tzinfo=UTC)        # 07:30 Riyadh
TODAY = dt.date(2026, 11, 11)
D0 = dt.date(2026, 9, 1)


def days(n, start=D0):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def mk(closes, spread=0.01, start=D0, factor=1.0):
    """Bars with a constant high-low spread around each close: ATR = 2 x spread x close on a flat series."""
    return [(d, c, c * (1 + spread), c * (1 - spread), c, factor) for d, c in zip(days(len(closes), start), closes)]


def K(**over):
    k = ep.knobs()
    k.update(over)
    return k


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in list(__import__("os").environ):
        if name.startswith("TFB_EXIT_"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_EXIT_PLAN", "on")
    monkeypatch.setenv("TFB_EXIT_LOG_DETAIL", "on")         # 1.6.1: the suite reads the detailed lines (X20: the rest)


def lv(bars, entry=100.0, entry_date=None, info=None, prev=None, sym="AAA.US"):
    entry_date = entry_date or bars[29][0]
    return ep.exit_levels(sym, entry, entry_date, bars, info or {}, prev or {}, K(), TODAY)


# ----------------------------------------------------------------------------- X1
def chart(ts, o, h, lo, c, adj=None, off=-18000):
    ind = {"quote": [{"open": o, "high": h, "low": lo, "close": c}]}
    if adj is not None:
        ind["adjclose"] = [{"adjclose": adj}]
    return {"chart": {"result": [{"meta": {"gmtoffset": off}, "timestamp": ts, "indicators": ind}]}}


def test_x1_parse_chart():
    t0 = int(dt.datetime(2026, 11, 9, 14, 30, tzinfo=UTC).timestamp())
    ts = [t0, t0 + 86400, t0 + 2 * 86400, t0 + 3 * 86400]
    p = chart(ts, [10, None, 11, 12], [11, 11, 12, 13], [9, 9, 10, 11], [10.5, 10, 11.5, 12.5], adj=[8.4, 8, 11.5, 12.5])
    bars = ep.parse_chart(p)
    assert [b[0] for b in bars] == [dt.date(2026, 11, 9), dt.date(2026, 11, 11), dt.date(2026, 11, 12)]
    assert bars[0][5] == pytest.approx(0.8) and bars[0][4] == pytest.approx(8.4) and bars[0][2] == pytest.approx(8.8)
    assert bars[-1][5] == 1.0
    odd = ep.parse_chart(chart(ts[:1], [10], [11], [9], [10], adj=[1.0]))          # factor 0.1: not applied
    assert odd[0][4] == 10 and odd[0][5] == 1.0
    assert ep.parse_chart(chart(ts[:1], [10], [9], [11], [10])) == []            # high under low
    assert ep.parse_chart({}) == [] and ep.parse_chart({"chart": {"result": []}}) == []
    assert ep.yahoo_symbol("brk.b.us") == "BRK-B" and ep.yahoo_symbol("2222.SR") == "2222.SR"


# ----------------------------------------------------------------------------- X2
def test_x2_atr():
    bars = mk([100.0] * 30, spread=0.01)
    assert ep.atr(bars, 29) == pytest.approx(2.0)
    gap = bars[:20] + [(days(21)[20], 110, 111, 109, 110, 1.0)]
    assert ep.atr(gap, 20) == pytest.approx((13 * 2.0 + 11.0) / 14)          # the gap counts in the true range
    assert ep.atr(bars, 3) is None and ep.atr(bars, 0) is None


# ----------------------------------------------------------------------------- X3
@pytest.mark.parametrize("spread,sym,dist", [(0.01, "A.US", 8.0), (0.015, "A.US", 9.0), (0.03, "A.US", 12.0),
                                             (0.015, "2222.SR", 8.0), (0.005, "2222.SR", 8.0)])
def test_x3_initial_stop_from_the_stocks_own_atr(spread, sym, dist):
    r = lv(mk([100.0] * 40, spread), sym=sym)
    assert r["dist_pct"] == pytest.approx(dist)
    assert r["init_stop"] == pytest.approx(100 - dist) and r["r"] == pytest.approx(dist)
    p = ep.pick_levels(sym, 100.0, mk([100.0] * 40, spread), K())
    assert p["stop"] == pytest.approx(100 - dist) and p["target"] == pytest.approx(100 + dist)


# ----------------------------------------------------------------------------- X4 / X5
def test_x4_before_one_r_the_initial_stop_holds():
    r = lv(mk([100.0] * 30 + [103.0, 105.0, 104.0], 0.015))
    assert r["r"] == pytest.approx(9.0) and r["stop"] == pytest.approx(91.0)
    assert r["target"] == pytest.approx(109.0) and r["action"] == "HOLD" and not r["tp1"]
    assert r["stop_gap_pct"] == pytest.approx((1 - 91.0 / 104.0) * 100) and "12.5% under the last close" in r["why"][0]


def test_x5_after_one_r_the_stop_trails_and_the_target_steps_up():
    closes = [100.0] * 30 + [104.0, 108.0, 112.0, 121.0, 120.0]
    r = lv(mk(closes, 0.015))
    a_now = ep.atr(mk(closes, 0.015), len(closes) - 1)
    assert r["r_high"] == pytest.approx(21.0 / 9.0)
    assert r["stop"] == pytest.approx(max(100.3, 121.0 - 3 * a_now))
    assert r["target"] == pytest.approx(100 + 3 * 9.0)                            # 2.3R -> the next step is 3R
    flat = lv(mk([100.0] * 30 + [109.5, 109.6], 0.015))                            # just over 1R: breakeven floor
    assert flat["stop"] == pytest.approx(max(100.3, 109.6 - 3 * ep.atr(mk([100.0] * 30 + [109.5, 109.6], 0.015), 31)))
    assert flat["stop"] >= 100.3 - 1e-9


# ----------------------------------------------------------------------------- X6
def test_x6_tightening_triggers_raise_the_stop():
    closes = [100.0] * 30 + [104.0, 108.0, 112.0, 121.0, 120.0]
    base = lv(mk(closes, 0.015))
    news = lv(mk(closes, 0.015), info={"news_neg": 2})
    assert news["trail_k"] == 2.0 and news["stop"] > base["stop"] and "negative news (2 items in 3 days)" in news["tighten"][0]
    fc = lv(mk(closes, 0.015), info={"forecast_1m": 115.0, "reliability": 72})
    assert fc["trail_k"] == 2.0 and "1-month forecast" in fc["tighten"][0]
    low_rel = lv(mk(closes, 0.015), info={"forecast_1m": 115.0, "reliability": 60})
    assert low_rel["trail_k"] == 3.0 and not low_rel["tighten"]
    up = lv(mk(closes, 0.015), info={"forecast_1m": 130.0, "reliability": 90})
    assert up["trail_k"] == 3.0
    mom = lv(mk([100.0] * 30 + [110.0, 120.0, 125.0] + [104.0] * 18, 0.015))       # +2.8R then under the 20-day mean
    assert any("20-day average" in t for t in mom["tighten"]) and mom["trail_k"] == 2.0
    one = lv(mk([100.0] * 30 + [110.0, 115.0] + [104.0] * 18, 0.015))             # +1.7R under the mean: no
    assert one["r_high"] < 2 and not one["tighten"] and one["trail_k"] == 3.0
    early = lv(mk([100.0] * 30 + [95.0] * 3, 0.015), info={"news_neg": 3})        # under 1R: the stop does not move
    assert early["stop"] == pytest.approx(91.0)


# ----------------------------------------------------------------------------- X7 / X8
def test_x7_the_stop_never_moves_down_and_raise_stop():
    closes = [100.0] * 30 + [104.0, 108.0, 112.0, 121.0, 120.0]
    r = lv(mk(closes, 0.015), prev={"Stop": 118.0, "TP1 Reached": "yes"})
    assert r["stop"] == 118.0 and r["prev_stop"] == 118.0 and r["action"] == "HOLD"
    up = lv(mk(closes, 0.015), prev={"Stop": 105.0, "TP1 Reached": "yes"})
    assert up["action"] == "RAISE STOP" and up["stop"] > 105.0 and "105.00 -> " in up["why"][0]
    tiny = lv(mk(closes, 0.015), prev={"Stop": up["stop"] - 0.01, "TP1 Reached": "yes"})
    assert tiny["action"] == "HOLD"                                                # under 0.25%: no action
    first = lv(mk(closes, 0.015), prev={"TP1 Reached": "yes"})
    assert first["action"] == "RAISE STOP" and "belongs at" in first["why"][0]


def test_x8_sell_at_or_below_the_stop():
    r = lv(mk([100.0] * 30 + [95.0, 90.5], 0.015))
    assert r["action"] == "SELL" and r["stop"] == pytest.approx(91.0)
    at = lv(mk([100.0] * 30 + [104.0, 108.0, 112.0, 121.0, 120.0], 0.015), prev={"Stop": 120.0, "TP1 Reached": "yes"})
    assert at["action"] == "SELL"                                                  # the ratcheted stop is hit


# ----------------------------------------------------------------------------- X9
def test_x9_take_profit_once_and_only_while_at_the_target():
    closes = [100.0] * 30 + [104.0, 110.0]
    r = lv(mk(closes, 0.015))
    assert r["action"] == "TAKE PROFIT" and "sell half" in r["why"][0]
    assert ep.tp1_flag(dict(r, prev={})) == "yes"
    again = lv(mk(closes, 0.015), prev={"Stop": r["stop"], "TP1 Reached": "yes"})
    assert again["action"] != "TAKE PROFIT"
    bars = mk(closes[:30] + [104.0, 105.0, 104.5], 0.015)
    bars[30] = (bars[30][0], 104.0, 112.0, 103.0, 104.0, 1.0)                      # intraday touch, closed under
    left = ep.exit_levels("AAA.US", 100.0, bars[29][0], bars, {}, {}, K(), TODAY)
    assert left["tp1"] and left["action"] == "HOLD"
    assert any("touched on %s (high 112.00)" % bars[30][0].isoformat() in w for w in left["why"])
    assert ep.tp1_flag(dict(left, prev={})) == "no"                                # it can still come back
    lastbar = mk(closes[:30] + [104.0, 105.0], 0.015)
    lastbar[-1] = (lastbar[-1][0], 105.0, 112.0, 103.0, 104.0, 1.0)              # today's high touched, close under
    lb = ep.exit_levels("AAA.US", 100.0, lastbar[29][0], lastbar, {}, {}, K(), TODAY)
    assert lb["tp1"] and lb["action"] == "HOLD" and ep.tp1_flag(dict(lb, prev={})) == "no"
    assert any("touched on %s" % lastbar[-1][0].isoformat() in w for w in lb["why"])
    buy_day = mk(closes[:30] + [101.0], 0.015)
    buy_day[29] = (buy_day[29][0], 100.0, 115.0, 99.0, 100.0, 1.0)                 # the buy day's high is ignored
    assert not ep.exit_levels("AAA.US", 100.0, buy_day[29][0], buy_day, {}, {}, K(), TODAY)["tp1"]


# ----------------------------------------------------------------------------- X10
def test_x10_review_earnings_and_broker_notes():
    long = mk([100.0] * 30 + [101.0] * 46, 0.015)
    r = lv(long)
    assert r["sessions"] == 46 and r["action"] == "REVIEW" and "46 sessions without +1R" in r["why"][0]
    assert lv(mk([100.0] * 30 + [101.0] * 44, 0.015))["action"] == "HOLD"
    assert lv(mk([100.0] * 30 + [101.0] * 45, 0.015))["action"] == "REVIEW"       # exactly the limit
    e = lv(mk([100.0] * 35, 0.015), info={"earn_days": 0, "broker_stop": 85.0})
    assert any(w.startswith("earnings today") for w in e["why"])
    assert any("broker stop 85.00 is under the plan's 91.00" in w for w in e["why"])
    assert not any("earnings" in w for w in lv(mk([100.0] * 35, 0.015), info={"earn_days": 6})["why"])
    assert not any("broker" in w for w in lv(mk([100.0] * 35, 0.015), info={"broker_stop": 91.0})["why"])
    nob = ep.exit_levels("AAA.US", 100.0, D0, [], {}, {}, K(), TODAY)
    assert nob["action"] == "CHECK" and not nob["ok"]


# ----------------------------------------------------------------------------- X11
def test_x11_dividend_adjusted_entry():
    pre = mk([35.0 * 0.77] * 30, 0.02, factor=0.77)                              # adjusted prices before the ex-date
    post = mk([27.6] * 6, 0.02, start=days(31)[30])
    r = ep.exit_levels("SUPN.US", 35.43, pre[25][0], pre + post, {}, {}, K(), TODAY)
    assert r["entry"] == pytest.approx(35.43 * 0.77)
    assert r["action"] == "HOLD" and r["stop"] < 27.6
    assert any("adjusted for the dividends" in w for w in r["why"])
    # a stop recorded at 91 on basis 1.0; a dividend then re-adjusts the history by 0.77
    before = mk([100.0] * 30 + [104.0, 110.0], 0.015)
    led = [{"Date": "2026-11-10", "Lot": "L", "Stop": 91.0, "TP1 Reached": "yes", "Basis": 1.0}]
    adj = [(b[0], b[1] * 0.77, b[2] * 0.77, b[3] * 0.77, b[4] * 0.77, 0.77) for b in before[:-1]]
    adj.append((days(33)[32], 85.0, 86.0, 84.0, 85.0, 1.0))                     # unchanged economics after the ex-date
    reb = ep.exit_levels("AAA.US", 100.0, adj[29][0], adj, {}, ep.prior_row(led, "L", TODAY), K(), TODAY)
    assert reb["prev_stop"] == pytest.approx(91.0 * 0.77) and reb["action"] != "SELL"
    old = ep.exit_levels("AAA.US", 100.0, adj[29][0], adj, {}, {"Stop": 91.0, "TP1 Reached": "yes"}, K(), TODAY)
    assert old["prev_stop"] == 91.0 and old["action"] == "SELL"                  # without the basis it would sell


# ----------------------------------------------------------------------------- X12
LOTS = [["_Portfolio_CostBasis"], ["Status:"], ["legend"],
        ["Symbol", "Name", "Ccy", "Status", "Buy Date", "Buy Price", "Shares", "Dividends Recv"],
        ["DDI.US", "DoubleDown", "USD", "Active", "2026-11-02", "13.42", "230", ""],
        ["DDI.US", "DoubleDown", "USD", "Active", "2026-11-03", "14.00", "10", ""],
        ["5023.SR", "Sukuk", "SAR", "Active", "2025-11-23", "100", "100", "637.5"],
        ["FRST.US", "Primis", "USD", "Inactive", "2026-10-26", "15.41", "174", ""],
        ["BAD.US", "x", "USD", "Active", "", "10", "5", ""]]


def test_x12_parsers():
    lots = ep.read_lots(LOTS)
    assert set(lots) == {"DDI.US", "5023.SR"}
    assert lots["DDI.US"]["shares"] == 240 and lots["DDI.US"]["date"] == dt.date(2026, 11, 3)
    assert lots["DDI.US"]["entry"] == pytest.approx((13.42 * 230 + 14 * 10) / 240)
    news = [["Item ID", "Published (UTC)", "Scope", "Direction", "Severity", "Headline"],
            ["1", "2026-11-10 12:00", "DDI.US", "-1.0", "H", "DDI misses"],
            ["2", "2026-11-09 12:00", "DDI.US", "-1", "M", "older"],
            ["3", "2026-11-06 12:00", "DDI.US", "-1", "H", "too old"],
            ["4", "2026-11-10 12:00", "DDI.US", "-1", "L", "low"],
            ["5", "2026-11-10 12:00", "GLPI.US", "1", "H", "up"],
            ["6", "2026-11-10 12:00", "MACRO", "-1", "H", "war"]]
    got = ep.read_news(news, NOW, 3)
    assert got == {"DDI.US": {"n": 2, "headline": "DDI misses", "at": dt.datetime(2026, 11, 10, 12, 0, tzinfo=UTC),
                              "ignored": 0}}
    assert ep.read_news(news, NOW, 0) == {}
    acct = [["key", "value"], ["as_of", "2026-11-11 06:00 Riyadh"]]
    orders = [["symbol", "side", "type", "qty", "price", "status"], ["DDI", "SELL", "STOP", 240, 12.9, "NEW"],
              ["DDI", "SELL", "LIMIT", 240, 16, "NEW"], ["GLPI", "SELL", "STOP", 79, 37.33, "CANCELLED"],
              ["2222.SR", "SELL", "STOP", 10, 25, "NEW"]]
    stops, note = ep.read_broker_stops(acct, orders, NOW)
    assert stops == {"DDI.US": 12.9, "2222.SR": 25.0} and note == "broker orders read 2026-11-11 06:00"
    stale, note2 = ep.read_broker_stops([["as_of", "2026-11-09 06:00 Riyadh"]], orders, NOW)
    assert stale == {} and "unknown" in note2 and "50 h old" in note2
    assert ep.read_broker_stops([], orders, NOW)[0] == {}
    plan = [["Plan Date", "Kind", "Symbol", "Entry", "Qty", "Stop", "Status", "Ccy"],
            ["2026-11-10", "PICK", "TEN.US", 44.77, 47, 41.19, "CHECK", "USD"],
            ["2026-11-10", "PICK", "ZTO.US", 20.66, 102, 19.01, "INVALIDATED", "USD"],
            ["2026-11-10", "PIPELINE", "EFC.US", 11.4, "", "", "", "USD"],
            ["2026-11-09", "PICK", "OLD.US", 10, 1, 9, "OPEN", "USD"]]
    assert [p["symbol"] for p in ep.read_plan_picks(plan)] == ["TEN.US"]
    # 1.3.2: the plan's date is the newest of any row; a newer plan cut with no pick lists none, not older picks
    nopick = plan + [["2026-11-11", "PIPELINE", "AX.US", 90, "", "", "", "USD"]]
    assert ep.read_plan_picks(nopick) == [] and ep.read_plan_picks(nopick, TODAY) == []
    assert [p["symbol"] for p in ep.read_plan_picks(plan, dt.date(2026, 11, 10))] == ["TEN.US"]
    assert ep.read_plan_picks(plan, TODAY) == []                    # yesterday's picks are not today's
    fresh = plan + [["2026-11-11", "PICK", "NEW.US", 5, 10, 4.5, "OPEN", "USD"]]
    assert [p["symbol"] for p in ep.read_plan_picks(fresh, TODAY)] == ["NEW.US"]
    led = [{"Date": "2026-11-09", "Lot": "L", "Stop": 10.0, "TP1 Reached": "yes", "Basis": 0.5},
           {"Date": "2026-11-10", "Lot": "L", "Stop": 9.5, "TP1 Reached": "no", "Basis": 1.0},
           {"Date": "2026-11-11", "Lot": "L", "Stop": 12.0, "TP1 Reached": "no", "Basis": 1.0},
           {"Date": "2026-11-12", "Lot": "L", "Stop": 50.0, "TP1 Reached": "no", "Basis": 1.0},
           {"Date": "2026-11-10", "Lot": "M", "Stop": 5.0, "TP1 Reached": "no"}]
    pr = ep.prior_row(led, "L", TODAY)
    assert pr["Stop"] == 12.0 and pr["TP1 Reached"] == "yes" and pr["Date"] == "2026-11-11"   # today counts
    assert pr["StopNorm"] == 20.0                                                 # 10 on basis 0.5
    assert ep.prior_row(led, "M", TODAY)["StopNorm"] == 5.0                       # no basis = 1
    assert ep.prior_row(led, "Z", TODAY) == {}


# ----------------------------------------------------------------------------- X13
class WorksheetNotFound(Exception):
    pass


def _colnum(letters):
    n = 0
    for ch in letters:
        n = n * 26 + ord(ch) - 64
    return n


class Sheet:
    def __init__(self, rows):
        self.values = deepcopy(rows)
        self.row_count, self.col_count = 500, 30
        self.updates = 0

    def get(self, rng):
        return deepcopy(self.values)

    def batch_get(self, ranges):
        out = []
        for r in ranges:
            m = re.match(r"([A-Z]+)(\d+):([A-Z]+)$", r)
            c, start = _colnum(m.group(1)) - 1, int(m.group(2)) - 1
            out.append([[row[c] if c < len(row) else ""] for row in self.values[start:]])
        return out

    def resize(self, rows=None, cols=None):
        pass

    def update(self, values=None, range_name=None, value_input_option=None):
        self.updates += 1
        self.values = [list(r) for r in values if any(str(c) != "" for c in r)]


class Book:
    def __init__(self, tabs):
        self.tabs = dict(tabs)

    def worksheet(self, name):
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]

    def add_worksheet(self, title, rows, cols):
        self.tabs[title] = Sheet([])
        return self.tabs[title]


PORT = [["Symbol", "Name", "Asset Class", "Current Price", "Forecast Price (1M)", "Forecast Reliability Score"],
        ["DDI.US", "DoubleDown", "Equity", 15.7, 16.0, 75], ["5023.SR", "Sukuk", "Fixed Income / Sukuk", 99, "", ""],
        ["GONE.US", "Gone", "Equity", 5, "", ""]]
LOTS2 = [["Symbol", "Name", "Ccy", "Status", "Buy Date", "Buy Price", "Shares"],
         ["DDI.US", "DoubleDown", "USD", "Active", days(30)[29].isoformat(), "100", "240"],
         ["5023.SR", "Sukuk", "SAR", "Active", "2025-11-23", "100", "100"],
         ["GONE.US", "Gone", "USD", "Active", days(30)[29].isoformat(), "5", "10"]]
PLAN = [["Plan Date", "Kind", "Symbol", "Entry", "Qty", "Stop", "Status", "Ccy"],
        ["2026-11-11", "PICK", "TEN.US", 100, 47, 92, "CHECK", "USD"]]
SERIES = {"DDI": [100.0] * 30 + [104.0, 110.0], "TEN.US": [100.0] * 40, "TEN": [100.0] * 40}


class Resp:
    def __init__(self, b):
        self.b = b

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def read(self):
        return self.b


def opener(req, timeout):
    sym = req.full_url.split("/chart/")[1].split("?")[0]
    if sym not in SERIES:
        e = Exception("404")
        e.code = 404
        raise e
    bars = mk(SERIES[sym], 0.015)
    ts = [int(dt.datetime(b[0].year, b[0].month, b[0].day, 14, 30, tzinfo=UTC).timestamp()) for b in bars]
    p = chart(ts, [b[1] for b in bars], [b[2] for b in bars], [b[3] for b in bars], [b[4] for b in bars], off=0)
    return Resp(json.dumps(p).encode())


def wbook():
    return Book({"_Portfolio_CostBasis": Sheet(LOTS2), "My_Portfolio": Sheet(PORT), "_Daily_Plan": Sheet(PLAN)})


def test_x13_run(monkeypatch):
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    sent = []
    monkeypatch.setenv("TFB_EXIT_PLAN", "observe")
    b = wbook()
    assert ep.run(False, True, book=b, now=NOW, opener=opener, mailer=lambda *a: sent.append(a)) == 0
    assert "Exit_Plan" not in b.tabs and "_Exit_Plan" not in b.tabs and not sent
    monkeypatch.setenv("TFB_EXIT_PLAN", "on")
    assert ep.run(True, True, book=b, now=NOW, opener=opener, mailer=lambda *a: sent.append(a)) == 0
    assert "Exit_Plan" not in b.tabs and not sent                                  # dry run
    assert ep.run(False, True, book=b, now=NOW, opener=opener, mailer=lambda *a: sent.append(a)) == 0
    assert not sent                                                                # TFB_EXIT_MAIL is not 1
    led = b.tabs["_Exit_Plan"].values
    assert led[0] == ep.LEDGER_HEADERS and len(led) == 2 and led[1][2] == "DDI.US" and led[1][13] == "TAKE PROFIT"
    assert led[1][12] == "yes"
    page = "\n".join(" | ".join(str(c) for c in r) for r in b.tabs["Exit_Plan"].values)
    assert "DDI.US | DoubleDown | 240 | 100.00 | 110.00" in page and "TAKE PROFIT" in page
    assert "5023.SR" in page and "no stop rule for fixed income" in page
    assert "GONE.US" in page and "no daily price history" in page
    assert "TEN.US | 100.00 | 92.00 | 91.00 | -9.0 | 109.00" in page
    first = deepcopy(b.tabs["_Exit_Plan"].values)
    monkeypatch.setenv("TFB_EXIT_MAIL", "1")
    assert ep.run(False, True, book=b, now=NOW, opener=opener, mailer=lambda *a: sent.append(a)) == 0
    led = b.tabs["_Exit_Plan"].values
    assert len(led) == 2 and led[1][12] == "yes" and led[1][10] >= first[1][10]   # the same day keeps its state
    assert led[1][13] == "TAKE PROFIT"                                             # 1.1.0: it stays for the day
    assert len(sent) == 1 and sent[0][0] == "TFB Exit Plan 2026-11-11 - 2 actions: DDI.US take profit, GONE.US check"
    assert "<b>DDI.US</b>" in sent[0][2] and "Today's plan picks" in sent[0][2]
    fresh = wbook()
    assert ep.run(False, True, book=fresh, now=NOW, opener=opener, mailer=lambda *a: sent.append(a)) == 0
    assert sent[-1][0] == "TFB Exit Plan 2026-11-11 - 2 actions: DDI.US take profit, GONE.US check"
    later = NOW + dt.timedelta(days=1)
    assert ep.run(False, False, book=b, now=later, opener=opener, mailer=lambda *a: sent.append(a)) == 0
    led = b.tabs["_Exit_Plan"].values
    assert len(led) == 3 and led[2][13] == "TAKE PROFIT" and len(sent) == 2        # 1.1.0: no new close yet
    monkeypatch.setitem(SERIES, "DDI", SERIES["DDI"] + [110.5])                   # the next close
    assert ep.run(False, False, book=b, now=later, opener=opener, mailer=lambda *a: sent.append(a)) == 0
    led = b.tabs["_Exit_Plan"].values
    assert len(led) == 3 and led[2][13] != "TAKE PROFIT" and led[2][12] == "yes"   # issued once; no --send
    def boom(*a):
        raise OSError("smtp down")
    assert ep.run(False, True, book=b, now=later, opener=opener, mailer=boom) == 4


def test_x13b_the_run_lists_only_todays_plan_picks(monkeypatch):
    """1.3.2 (14 Nov review): a plan of an earlier day never shows as today's plan picks."""
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    monkeypatch.setenv("TFB_EXIT_PLAN", "on")
    old = [PLAN[0], ["2026-11-10"] + PLAN[1][1:]]
    b = Book({"_Portfolio_CostBasis": Sheet(LOTS2), "My_Portfolio": Sheet(PORT), "_Daily_Plan": Sheet(old)})
    assert ep.run(False, False, book=b, now=NOW, opener=opener, mailer=lambda *a: None) == 0
    page = "\n".join(" | ".join(str(c) for c in r) for r in b.tabs["Exit_Plan"].values)
    assert "TODAY'S PLAN PICKS - per-stock levels (0)" in page and "TEN.US | 100.00" not in page


def test_x13c_broker_stops_come_from_the_ibkr_capture(monkeypatch, capsys):
    """1.3.2: the stops come from the same IBKR capture as the Today Action Plan; the tabs stay the fallback."""
    cap = {"ok": True, "as_of": NOW - dt.timedelta(hours=2), "stops": {"DDI.US": [(98.5, 200.0), (97.0, 40.0)],
                                                                      "NOPX.US": [(None, 5.0)]}}
    stops, note = ep.capture_stops(cap, NOW)
    assert stops == {"DDI.US": 97.0} and note == "broker orders from the IBKR capture of 2026-11-11 05:30 Riyadh"
    assert ep.capture_stops(dict(cap, as_of=NOW - dt.timedelta(hours=31)), NOW) is None      # too old
    assert ep.capture_stops(dict(cap, as_of=NOW - dt.timedelta(hours=31)), NOW, 40.0) is not None
    assert ep.capture_stops({"ok": False, "reason": "x"}, NOW) is None and ep.capture_stops(None, NOW) is None
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    monkeypatch.setenv("TFB_EXIT_PLAN", "on")
    b = wbook()
    assert ep.run(False, False, book=b, now=NOW, opener=opener, mailer=lambda *a: None, capture=cap) == 0
    page = "\n".join(" | ".join(str(c) for c in r) for r in b.tabs["Exit_Plan"].values)
    assert "broker orders from the IBKR capture of 2026-11-11 05:30 Riyadh" in page and "97.00" in page
    old = dict(cap, as_of=NOW - dt.timedelta(hours=50))
    b2 = wbook()
    assert ep.run(False, False, book=b2, now=NOW, opener=opener, mailer=lambda *a: None, capture=old) == 0
    page2 = "\n".join(" | ".join(str(c) for c in r) for r in b2.tabs["Exit_Plan"].values)
    assert "IBKR capture" not in page2 and "broker orders unknown" in page2
    assert "IBKR capture not used for stops (older than 30 h)" in capsys.readouterr().out


def test_x13_gate_and_unreadable(monkeypatch, capsys):
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    monkeypatch.setenv("TFB_EXIT_PLAN", "off")
    assert ep.main(["--dry-run"]) == 0 and "nothing to do" in capsys.readouterr().out
    monkeypatch.setenv("TFB_EXIT_PLAN", "anything")
    assert ep.mode() == "off"
    monkeypatch.setenv("TFB_EXIT_PLAN", "on")

    class Bad(Book):
        def worksheet(self, name):
            raise RuntimeError("quota")
    assert ep.run(False, True, book=Bad({}), now=NOW, opener=opener) == 2
    monkeypatch.setenv("TFB_EXIT_MIN_PCT", "20")
    monkeypatch.setenv("TFB_EXIT_TRAIL_TIGHT_ATR", "5")
    k = ep.knobs()
    assert k["min_pct"] == k["max_pct"] == 12 and k["trail_tight"] == k["trail"] == 3


# ----------------------------------------------------------------------------- X14
def test_x14_workflow_manifest_ci():
    raw = (ROOT / ".github/workflows/exit_plan.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(raw)
    on = wf[True] if True in wf else wf["on"]
    assert on["schedule"] == [{"cron": "30 4 * * *"}, {"cron": "35 5-21 * * *"}] and "workflow_dispatch" in on
    hours = set(range(0, 4)) | set(range(5, 24))
    assert len(hours) == 23                                                         # every hour, 04 is the 04:30 run
    assert '"$EVENT_CRON" == "30 4 * * *"' in raw and "EVENT_CRON: ${{ github.event.schedule }}" in raw
    assert wf["permissions"] == {"contents": "read"}
    step = [s for s in wf["jobs"]["exit_plan"]["steps"] if "exit_plan.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_EXIT_PLAN"] == "${{ vars.TFB_EXIT_PLAN || 'on' }}"
    assert step["env"]["TFB_EXIT_MAIL"] == "${{ vars.TFB_EXIT_MAIL || '1' }}"
    assert step["env"]["TFB_EXIT_DIV_NET"] == "${{ vars.TFB_EXIT_DIV_NET || 'on' }}"      # 1.4.0
    assert "--send" in step["run"] and "--dry-run" in step["run"]
    src = (ROOT / "scripts/exit_plan.py").read_text(encoding="utf-8")
    for name in re.findall(r"(TFB_EXIT_[A-Z_]+): \$\{\{", raw):
        assert '"%s"' % name in src, name
    assert '("exit_plan", "%s", "exit plan")' % ep.__version__ in (ROOT / "scripts/verify_deployment.py").read_text(
        encoding="utf-8")
    assert "tests/test_exit_plan.py" in (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert not re.search(r"create_order|place_order|requests\.post|cancel_order", src)


def test_x14b_only_a_send_mail_start_sends_the_email(tmp_path):
    """PR #963 (Codex P2): the Apps Script 07:30 lane is the one automatic sender; a schedule (on time, late or doubled)
    never e-mails. Runs the workflow's own bash with the python call replaced by an echo of its arguments."""
    import os
    import subprocess

    wf = yaml.safe_load((ROOT / ".github/workflows/exit_plan.yml").read_text(encoding="utf-8"))
    step = [s for s in wf["jobs"]["exit_plan"]["steps"] if "exit_plan.py" in str(s.get("run", ""))][0]
    script = step["run"].replace('python scripts/exit_plan.py "${ARGS[@]}"', 'echo "ARGS=${ARGS[*]}"')
    assert script != step["run"]

    def run(event, cron="", dry="", send=""):
        env = {"PATH": os.environ.get("PATH", ""), "EVENT_NAME": event, "EVENT_CRON": cron, "IN_DRY_RUN": dry, "IN_SEND": send}
        out = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True, timeout=30)
        assert out.returncode == 0, out.stderr
        return [ln for ln in out.stdout.splitlines() if ln.startswith(("ARGS=", "::notice::"))]

    assert run("schedule", "30 4 * * *")[-1] == "ARGS="
    assert run("schedule", "30 4 * * *")[0].startswith("::notice::no e-mail from the schedule")
    assert run("schedule", "35 5-21 * * *") == ["ARGS="]
    assert run("workflow_dispatch", dry="false", send="true") == ["ARGS=--send"]
    assert run("workflow_dispatch", dry="false", send="false") == ["ARGS="]
    assert run("workflow_dispatch", dry="true", send="false") == ["ARGS=--dry-run"]
    assert run("workflow_dispatch", dry="true", send="true") == ["ARGS=--dry-run --send"]
    gs = (ROOT / "apps_script/29_Page_Refresh_Dispatch.gs").read_text(encoding="utf-8")
    assert "{ id: 'mail', workflow: 'exit_plan.yml', minute: 30, hours: [7], inputs: { dry_run: 'false', send_mail: 'true' } }" in gs
    assert gs.count("send_mail: 'true'") == 1


# ----------------------------------------------------------------------------- X15
def _open_payload(bars, start, end, off=0, price=None, ts=None):
    t = [int(dt.datetime(b[0].year, b[0].month, b[0].day, 14, 30, tzinfo=UTC).timestamp()) for b in bars]
    p = chart(t, [b[1] for b in bars], [b[2] for b in bars], [b[3] for b in bars], [b[4] for b in bars], off=off)
    meta = p["chart"]["result"][0]["meta"]
    meta["currentTradingPeriod"] = {"regular": {"start": start, "end": end}}
    if price is not None:
        meta["regularMarketPrice"], meta["regularMarketTime"] = price, ts
    return p


def test_x15_open_session_bar_is_set_aside():
    bars = mk([100.0] * 30 + [104.0, 110.0], 0.015)
    last_day = bars[-1][0]
    start = int(dt.datetime(last_day.year, last_day.month, last_day.day, 14, 30, tzinfo=UTC).timestamp())
    end = start + int(6.5 * 3600)
    p = _open_payload(bars, start, end)
    parsed = ep.parse_chart(p)
    done, live = ep.split_open_session(p, parsed, start + 3600)                  # an hour into the session
    assert len(done) == len(parsed) - 1 and live == pytest.approx(110.0)
    assert ep.split_open_session(p, parsed, end + 60) == (parsed, None)           # after the close: completed
    assert ep.split_open_session(p, parsed, start - 60) == (parsed, None)         # before the open
    prev_day = _open_payload(bars, start + 86400, end + 86400)                    # session open, no bar for it yet
    assert ep.split_open_session(prev_day, parsed, start + 86400 + 60) == (parsed, None)
    assert ep.split_open_session({}, parsed, start) == (parsed, None)
    # the levels: the spike to 110 is not a completed close, so no TAKE PROFIT and no ratchet on it
    r = ep.exit_levels("AAA.US", 100.0, done[29][0], done, {"live": live}, {}, K(), TODAY)
    assert r["last"] == 104.0 and r["action"] == "HOLD" and r["live"] == 110.0
    under = ep.exit_levels("AAA.US", 100.0, done[29][0], done, {"live": 90.0}, {}, K(), TODAY)
    assert any("live price 90.00 is at or under the stop 91.00" in w for w in under["why"])
    assert under["action"] == "HOLD"                                              # the rule acts on the close


def test_x15_take_profit_stays_for_the_day_and_not_after():
    closes = [100.0] * 30 + [104.0, 110.0]
    led = [{"Date": "2026-11-11", "Lot": "L", "Stop": 100.3, "TP1 Reached": "yes", "Action": "TAKE PROFIT"}]
    today = ep.prior_row(led, "L", TODAY)
    assert today["TP1 Today"] == "yes" and today["TP1 Before"] == "no"
    r = lv(mk(closes, 0.015), prev=today)
    assert r["action"] == "TAKE PROFIT" and "(issued on the close of" in r["why"][0]
    tomorrow = ep.prior_row(led, "L", TODAY + dt.timedelta(days=1))
    assert tomorrow["TP1 Today"] == "no" and tomorrow["TP1 Before"] == "yes"
    assert lv(mk(closes, 0.015), prev=tomorrow)["action"] != "TAKE PROFIT"
    sold = lv(mk(closes[:30] + [104.0, 110.0, 90.0], 0.015), prev=dict(today, Stop=100.3))
    assert sold["action"] == "SELL"                                               # a stop hit outranks it


def test_x15_live_quote(monkeypatch):
    bars = mk([100.0] * 30, 0.015)
    t = int(dt.datetime(2026, 11, 11, 15, 0, tzinfo=UTC).timestamp())
    p = _open_payload(bars, t - 3600, t + 3600, price=101.25, ts=t)

    def op(req, timeout):
        assert "/chart/TEN?" in req.full_url
        return Resp(json.dumps(p).encode())
    got = ep.live_quote("TEN.US", op)
    assert got == (101.25, dt.datetime(2026, 11, 11, 15, 0, tzinfo=UTC))
    p2 = _open_payload(bars, t - 3600, t + 3600)
    assert ep.live_quote("TEN.US", lambda req, timeout: Resp(json.dumps(p2).encode())) is None
    p0 = _open_payload(bars, t - 3600, t + 3600, price=0.0, ts=t)                  # a zero price is no quote
    assert ep.live_quote("TEN.US", lambda req, timeout: Resp(json.dumps(p0).encode())) is None
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)

    def gone(req, timeout):
        e = Exception("404")
        e.code = 404
        raise e
    assert ep.live_quote("TEN.US", gone) is None


def test_x15_run_shows_the_live_column(monkeypatch):
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    bars = mk(SERIES["DDI"], 0.015)
    last_day = bars[-1][0]
    start = int(dt.datetime(last_day.year, last_day.month, last_day.day, 14, 30, tzinfo=UTC).timestamp())

    def op(req, timeout):
        sym = req.full_url.split("/chart/")[1].split("?")[0]
        if sym != "DDI":
            return opener(req, timeout)
        return Resp(json.dumps(_open_payload(bars, start, start + 23400)).encode())
    now = dt.datetime.fromtimestamp(start + 3600, UTC)
    b = wbook()
    assert ep.run(False, False, book=b, now=now, opener=op) == 0
    page = b.tabs["Exit_Plan"].values
    hdr = next(r for r in page if r and r[0] == "Symbol" and "Live" in r)
    ddi = next(r for r in page if r and r[0] == "DDI.US")
    assert ddi[hdr.index("Last close")] == "104.00" and ddi[hdr.index("Live")] == "110.00"
    assert ddi[hdr.index("Action")] == "HOLD"                                     # 110 is not a completed close yet


def test_x16_the_action_belongs_to_the_close():
    # 23:35 Riyadh on 10 Nov saw the US close of 10 Nov and raised the stop; the 07:30 e-mail run on 11 Nov is on the
    # same close, so it must show the same RAISE STOP, not HOLD
    c9, c10, c11 = dt.date(2026, 11, 9), dt.date(2026, 11, 10), dt.date(2026, 11, 11)
    led = [{"Date": "2026-11-09", "Lot": "L", "Stop": 95.0, "TP1 Reached": "no", "Action": "HOLD", "Bars To": "2026-11-09"},
           {"Date": "2026-11-10", "Lot": "L", "Stop": 98.0, "TP1 Reached": "no", "Action": "RAISE STOP",
            "Bars To": "2026-11-10"}]
    same = ep.prior_row(led, "L", TODAY, c10)
    assert same["Stop"] == 98.0 and same["Was"] == 95.0 and same["WasNorm"] == 95.0
    assert same["TP1 Today"] == "no" and same["TP1 Before"] == "no"
    r = lv(mk([100.0] * 40, 0.015), prev=same)
    assert r["action"] == "RAISE STOP" and r["why"][0] == "raise the stop 95.00 -> 98.00" and r["prev_stop"] == 95.0
    nxt = ep.prior_row(led, "L", TODAY, c11)                                      # the next close: it was done
    assert nxt["Was"] == 98.0 and lv(mk([100.0] * 40, 0.015), prev=nxt)["action"] == "HOLD"
    assert ep.prior_row(led[1:], "L", TODAY, c10)["Was"] is None                  # first seen on this close
    assert ep.prior_row(led, "L", TODAY, c9)["Was"] is None
    assert ep.prior_row(led, "L", TODAY)["Was"] == 98.0                           # no close given: the run's day
    # TAKE PROFIT issued at 23:35 stays for the 07:30 run on the same close and is not repeated on the next close
    tp = led + [{"Date": "2026-11-10", "Lot": "L", "Stop": 100.3, "TP1 Reached": "yes", "Action": "TAKE PROFIT",
                 "Bars To": "2026-11-10"}]
    on = ep.prior_row(tp, "L", TODAY, c10)
    assert on["TP1 Today"] == "yes" and on["TP1 Before"] == "no"
    after = ep.prior_row(tp, "L", TODAY, c11)
    assert after["TP1 Today"] == "no" and after["TP1 Before"] == "yes"
    # a same-close row that only carries the flag (TAKE PROFIT came on an earlier close whose row was replaced)
    carried = [{"Date": "2026-11-11", "Lot": "L", "Stop": 101.0, "TP1 Reached": "yes", "Action": "HOLD",
                "Bars To": "2026-11-11"}]
    c = ep.prior_row(carried, "L", TODAY, c11)
    assert c["TP1 Today"] == "no" and c["TP1 Before"] == "yes"


def test_x16_bought_in_a_session_that_has_not_closed():
    # Codex P2 on #959: closes of 100, bought today at 120, live 120 - the completed closes predate the purchase,
    # so no close rule may act on them (it read SELL: 100 is under the 110.40 stop)
    bars = mk([100.0] * 40, 0.015)
    bought = bars[-1][0] + dt.timedelta(days=1)
    r = ep.exit_levels("NEW.US", 120.0, bought, bars, {"live": 120.0}, {}, K(), TODAY)
    assert r["action"] == "HOLD" and r["fresh"] and r["stop"] == pytest.approx(110.4)
    assert r["why"][0].startswith("bought on %s: no close since the purchase yet" % bought.isoformat())
    assert "110.40" in r["why"][0] and "129.60" in r["why"][0] and r["pl_pct"] == pytest.approx(0.0)
    assert r["r_high"] == 0.0 and r["hc"] == 120.0
    slid = ep.exit_levels("NEW.US", 120.0, bought, bars, {"live": 108.0}, {}, K(), TODAY)
    assert slid["action"] == "HOLD" and any("live price 108.00 is at or under the stop 110.40" in w for w in slid["why"])
    none = ep.exit_levels("NEW.US", 120.0, bought, bars, {}, {}, K(), TODAY)
    assert none["pl_pct"] is None
    page = ep.page_rows([dict(none, name="New", shares=10, lot="L", broker_stop=None, prev={})], [],
                        {"headline": "As of", "summary": "Today: HOLD 1"}, K())
    assert any(row[0] == "NEW.US" and row[6] == "—" for row in page)
    # once the purchase day has closed, the rules act on that close
    done = mk([100.0] * 40 + [120.0], 0.015)
    after = ep.exit_levels("NEW.US", 120.0, done[-1][0], done, {}, {}, K(), TODAY)
    assert not after["fresh"] and after["action"] in ("HOLD", "RAISE STOP")


def test_x17_dividend_note():
    """1.2.0 [DIV-TAX]: the price falls by the full dividend; a Saudi resident keeps 70% of a US one."""
    k = K()
    n = ep.dividend_note("GLPI.US", 79, 40.0, {"exdiv_days": 4.0, "div_amount": 0.5}, k)
    assert n == ("ex-dividend in 4 day(s): 0.50 a share (1.2% of the price) - the price falls by the full dividend; "
                 "you keep 70% after the 30% US withholding (11.85 withheld on 79 shares) - if you plan to sell within "
                 "weeks, selling before the ex-date avoids it")
    assert ep.dividend_note("GLPI.US", 79, 40.0, {"exdiv_days": 0.0, "div_amount": 0.5}, k).startswith("ex-dividend today:")
    sr = ep.dividend_note("2222.SR", 100, 25.0, {"exdiv_days": 2.0, "div_amount": 0.5}, k)
    assert sr == "ex-dividend in 2 day(s): 0.50 a share (2.0% of the price) - the price falls by the full dividend"
    assert "withholding" not in ep.dividend_note("BBOX.L", 100, 140.0, {"exdiv_days": 2.0, "div_amount": 1.8}, k)
    assert ep.dividend_note("GLPI.US", 79, 40.0, {"exdiv_days": 11.0, "div_amount": 0.5}, k) == ""
    assert ep.dividend_note("GLPI.US", 79, 40.0, {"exdiv_days": 4.0, "div_amount": None}, k) == ""
    assert ep.dividend_note("GLPI.US", 79, None, {"exdiv_days": 4.0, "div_amount": 0.5}, k) == ""
    assert ep.dividend_note("GLPI.US", 79, 40.0, {}, k) == ""
    assert ep.dividend_note("GLPI.US", 79, 40.0, {"exdiv_days": 4.0, "div_amount": 0.5}, K(wht=15.0)).endswith(
        "you keep 85% after the 15% US withholding (5.92 withheld on 79 shares) - if you plan to sell within weeks, "
        "selling before the ex-date avoids it")
    assert ep.dividend_note("GLPI.US", 79, 40.0, {"exdiv_days": 4.0, "div_amount": 0.5}, K(div_warn_days=3)) == ""
    assert k["div_warn_days"] == 10 and k["wht"] == 30.0


def test_x17_run_carries_the_dividend_note(monkeypatch):
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    monkeypatch.setenv("TFB_EXIT_PLAN", "on")
    b = wbook()
    b.tabs["Dividend_Events"] = Sheet([["Symbol", "Ex-Div Date", "Days To ExDiv", "Amount", "Currency"],
                                       ["DDI.US", "", 3, 1.10, "USD"]])
    assert ep.run(False, False, book=b, now=NOW, opener=opener) == 0
    page = "\n".join(" | ".join(str(c) for c in r) for r in b.tabs["Exit_Plan"].values)
    assert "ex-dividend in 3 day(s): 1.10 a share (1.0% of the price)" in page
    assert "(79.20 withheld on 240 shares)" in page
    wf = yaml.safe_load((ROOT / ".github" / "workflows" / "exit_plan.yml").read_text(encoding="utf-8"))
    env = [s for s in wf["jobs"][list(wf["jobs"])[0]]["steps"] if "TFB_EXIT_WHT_PCT" in (s.get("env") or {})][0]["env"]
    assert env["TFB_EXIT_WHT_PCT"] == "${{ vars.TFB_EXIT_WHT_PCT || '30' }}"
    assert env["TFB_EXIT_DIV_WARN_DAYS"] == "${{ vars.TFB_EXIT_DIV_WARN_DAYS || '10' }}"


# ----------------------------------------------------------------------------- X18 (1.3.0)
def test_x18_law_firm_notices_and_unconfirmed_items_never_tighten_a_stop():
    """ChatGPT review, 12 Nov: DDI's trail was tightened by 'BRODSKY & SMITH SHAREHOLDER UPDATE: Notifying Investors
    of the Following Investigations' - an advertisement carried on a tier-A wire."""
    hdr = ["Item ID", "Published (UTC)", "Scope", "Event Type", "Direction", "Severity", "Tier", "Cluster Grade", "Headline"]
    rows = [hdr,
            ["1", "2026-11-10 12:00", "DDI.US", "legal_regulatory", "-1", "M", "A", "1",
             "BRODSKY & SMITH SHAREHOLDER UPDATE: Notifying Investors of the Following Investigations: DoubleDown"],
            ["2", "2026-11-10 13:00", "DDI.US", "law_firm_noise", "-1", "H", "A", "1", "anything"],
            ["3", "2026-11-10 14:00", "DDI.US", "earnings", "-1", "H", "C", "4", "rumour only"],
            ["4", "2026-11-10 15:00", "DDI.US", "earnings", "-1", "H", "E", "6", "social only"],
            ["5", "2026-11-10 11:00", "GLPI.US", "earnings", "-1", "H", "B", "2", "GLPI cuts guidance"],
            ["6", "2026-11-10 11:30", "GLPI.US", "legal_regulatory", "-1", "M", "A", "",
             "Pomerantz Law Firm announces a class action against GLPI"],
            ["7", "2026-11-10 10:00", "IBOC.US", "earnings", "-1", "M", "B", "3", "IBOC misses"]]
    got = ep.read_news(rows, NOW, 3)
    assert got["DDI.US"] == {"n": 0, "headline": "", "at": None, "ignored": 4}
    assert got["GLPI.US"]["n"] == 1 and got["GLPI.US"]["ignored"] == 1 and got["GLPI.US"]["headline"] == "GLPI cuts guidance"
    assert got["IBOC.US"]["n"] == 1 and got["IBOC.US"]["ignored"] == 0            # grade 3 still counts
    assert ep.news_is_noise("earnings", "Company misses estimates", "") is False   # no grade: counted, as before
    assert ep.news_is_noise("earnings", "Company misses estimates", 3.0) is False
    assert ep.news_is_noise("earnings", "Company misses estimates", 4) is True
    for h in ("SHAREHOLDER ALERT: act now", "Rosen Law Firm reminds investors", "Investigation on behalf of holders",
              "Levi & Korsinsky notifies investors", "DEADLINE ALERT for XYZ", "Investors who lost money in XYZ"):
        assert ep.news_is_noise("legal_regulatory", h, 1) is True, h
    for h in ("SEC charges ABC with securities fraud", "ABC hit with class action over data breach",          # Codex P2, #964
              "Court approves securities litigation settlement with ABC", "Regulator opens investigation into ABC"):
        assert ep.news_is_noise("legal_regulatory", h, 1) is False, h
    for h in ("Class action lawsuit has been filed against ABC", "ABC SECURITIES FRAUD ALERT", "CLASS ACTION DEADLINE: ABC"):
        assert ep.news_is_noise("legal_regulatory", h, 1) is True, h
    lots = ep.read_lots(LOTS)
    assert lots["DDI.US"]["lots"] == 2 and lots["5023.SR"]["lots"] == 1
    tax = json.loads((ROOT / "data" / "news_gate_taxonomy.json").read_text(encoding="utf-8"))
    flat = json.dumps(tax).lower()
    assert "brodsky & smith" in flat and "shareholder update" in flat and "notifying investors" in flat
    assert '("exit_plan", "%s", "exit plan")' % ep.__version__ in (ROOT / "scripts/verify_deployment.py").read_text(
        encoding="utf-8")


# ----------------------------------------------------------------------------- X16 (1.4.0)
DIVLOG = [["Logged At", "Type", "Symbol", "Lot Buy Date", "Ex-Date", "Amount/Share", "Shares", "Amount", "Ccy", "Note"],
          ["2026-10-14 07:00:00", "AUTO", "TAX.US", "2026-10-12", "2026-10-13", 10, 40, 400, "USD", "yahoo"],
          ["2026-10-20 09:00:00", "MANUAL", "TAX.US", "2026-10-12", "2026-10-13", -3, 40, "(120.00)", "USD", "tax"],
          ["2026-10-14 07:00:00", "AUTO", "GROSS.US", "2026-10-12", "2026-10-13", 1, 50, 50, "USD", "yahoo"],
          ["2026-10-14 07:00:00", "AUTO", "ODD.US", "2026-10-12", "2026-10-13", 2, 50, 100, "USD", "yahoo"],
          ["2026-10-15 07:00:00", "MANUAL", "ODD.US", "2026-10-12", "2026-10-13", -1.6, 50, -80, "USD", "typing error"],
          ["2026-10-14 07:00:00", "AUTO", "TWO.US", "2026-08-03", "2026-09-10", 1, 10, 10, "USD", "yahoo"],
          ["2026-10-14 07:00:00", "AUTO", "TWO.US", "2026-08-03", "2026-10-13", 2, 10, 20, "USD", "yahoo"],
          ["2026-10-15 07:00:00", "MANUAL", "two.us", "2026-08-03", "2026-10-13", -0.3, 10, -3, "USD", "tax"],
          ["2026-10-15 07:00:00", "MANUAL", "", "2026-08-03", "2026-10-13", -0.3, 10, -3, "USD", "no symbol"],
          ["2026-10-15 07:00:00", "MANUAL", "TAX.US", "2026-10-12", "no date", -1, 40, -40, "USD", "no ex-date"]]


def test_x16_dividend_log_parser():
    t = ep.read_dividend_tax(DIVLOG)
    assert set(t) == {"TAX.US", "TWO.US"}                                          # no tax row = gross; 20% kept = not applied
    assert t["TAX.US"] == {dt.date(2026, 10, 13): pytest.approx(0.7)}
    assert t["TWO.US"] == {dt.date(2026, 10, 13): pytest.approx(0.85)}             # per ex-date; the symbol's case folded
    assert ep.read_dividend_tax([]) == {} and ep.read_dividend_tax([["Symbol", "Amount"], ["TAX.US", -5]]) == {}
    full = [DIVLOG[0], DIVLOG[1], ["", "MANUAL", "TAX.US", "", "2026-10-13", "", "", -2, "", ""]]
    assert ep.read_dividend_tax(full) == {}                                        # 99.5% kept: rounding, not a tax


def _div_bars(raw=50.0, div=10.0, n_pre=30, n_post=6, post=40.0):
    """A stock at `raw` that pays `div`: Yahoo's adjusted bars before the ex-date carry the factor 1 - div / raw."""
    f = 1 - div / raw
    ds = days(n_pre + n_post)
    pre = [(d, raw * f, raw * f * 1.01, raw * f * 0.99, raw * f, f) for d in ds[:n_pre]]
    return pre + [(d, post, post * 1.01, post * 0.99, post, 1.0) for d in ds[n_pre:]], ds[n_pre]


def test_x16_bars_net_of_the_tax():
    bars, ex = _div_bars()
    buy_day = bars[25][0]
    net, applied = ep.net_of_tax(bars, {ex: 0.7}, buy_day)
    assert applied == [pytest.approx(30.0)]
    assert net[0][5] == pytest.approx(0.86) and net[29][4] == pytest.approx(43.0)   # 50 less 70% of 10
    assert net[0][2] == pytest.approx(43.0 * 1.01) and net[0][3] == pytest.approx(43.0 * 0.99)
    assert net[30:] == bars[30:] and bars[0][5] == pytest.approx(0.8)               # after the ex-date; the input kept
    assert ep.net_of_tax(bars, {ex: 0.7}, ex) == (bars, [])                         # bought on the ex-date: no dividend
    assert ep.net_of_tax(bars, {ex: 0.7}, ex + dt.timedelta(days=3)) == (bars, [])
    assert ep.net_of_tax(bars, None, buy_day) == (bars, []) and ep.net_of_tax(bars, {}, buy_day) == (bars, [])
    assert ep.net_of_tax(bars, {ex: 0.2}, buy_day) == (bars, [])                    # an implausible share
    assert ep.net_of_tax(bars, {ex: 1.0}, buy_day) == (bars, [])
    flat = mk([40.0] * 36, 0.01)
    assert ep.net_of_tax(flat, {ex: 0.7}, flat[25][0]) == (flat, [])                # no dividend step in the history
    assert ep.net_of_tax(bars, {ex - dt.timedelta(days=30): 0.7}, bars[2][0]) == (bars, [])   # a week from any step
    assert ep.net_of_tax(bars, {bars[0][0] - dt.timedelta(days=1): 0.7}, dt.date(2026, 1, 1)) == (bars, [])
    assert ep.net_of_tax(bars, {bars[-1][0] + dt.timedelta(days=1): 0.7}, buy_day) == (bars, [])
    weekend, _ = ep.net_of_tax(bars, {ex - dt.timedelta(days=1): 0.7}, buy_day)     # ex-date on a day with no bar
    assert weekend[0][5] == pytest.approx(0.86 if (ex - dt.timedelta(days=1)) > bars[29][0] else 0.8)
    gap = bars[:20] + bars[30:]                                                     # a halt: no bar for two weeks
    far = bars[30][0] - dt.timedelta(days=10)
    assert far > gap[19][0] and ep.net_of_tax(gap, {far: 0.7}, gap[5][0]) == (gap, [])   # the step is not that date's
    near, hit = ep.net_of_tax(gap, {bars[30][0] - dt.timedelta(days=6): 0.7}, gap[5][0])
    assert hit == [pytest.approx(30.0)] and near[19][5] == pytest.approx(0.86)
    short = [b[:5] for b in bars]
    assert ep.net_of_tax(short, {ex: 0.7}, buy_day) == (short, [])                  # bars without a factor


def test_x16_entry_stop_and_reason_net_of_the_tax():
    bars, ex = _div_bars()
    buy_day = bars[25][0]
    gross = ep.exit_levels("TAX.US", 50.0, buy_day, bars, {}, {}, K(), TODAY)
    net_bars, pct = ep.net_of_tax(bars, {ex: 0.7}, buy_day)
    net = ep.exit_levels("TAX.US", 50.0, buy_day, net_bars, {"div_tax_pct": pct}, {}, K(), TODAY)
    assert gross["entry"] == pytest.approx(40.0) and net["entry"] == pytest.approx(43.0)
    assert net["basis"] == pytest.approx(0.86) and net["stop"] == pytest.approx(gross["stop"] * 0.86 / 0.8)
    assert net["dist_pct"] == pytest.approx(gross["dist_pct"])                     # the same percent of a higher entry
    assert net["pl_pct"] == pytest.approx((40.0 / 43.0 - 1) * 100) and gross["pl_pct"] == pytest.approx(0.0)
    assert any(w.endswith("adjusted for the dividends paid since, net of the 30% tax withheld (dividend log)")
               for w in net["why"])
    assert any(w.endswith("adjusted for the dividends paid since") for w in gross["why"])
    # a stop recorded on the gross basis is rebased to the net one, and back when the gate is turned off
    bars2, ex2 = _div_bars(post=42.0)
    net2, _ = ep.net_of_tax(bars2, {ex2: 0.7}, buy_day)
    led = [{"Date": "2026-11-10", "Lot": "L", "Stop": 38.0, "TP1 Reached": "no", "Basis": 0.8}]
    reb = ep.exit_levels("TAX.US", 50.0, buy_day, net2, {}, ep.prior_row(led, "L", TODAY), K(), TODAY)
    assert reb["prev_stop"] == pytest.approx(38.0 / 0.8 * 0.86) and reb["stop"] == pytest.approx(40.85)
    assert reb["action"] == "HOLD"                                                 # rebased, not a RAISE STOP
    led2 = [{"Date": "2026-11-10", "Lot": "L", "Stop": 40.85, "TP1 Reached": "no", "Basis": 0.86}]
    back = ep.exit_levels("TAX.US", 50.0, buy_day, bars2, {}, ep.prior_row(led2, "L", TODAY), K(), TODAY)
    assert back["stop"] == pytest.approx(38.0) and back["action"] == "HOLD"
    low = ep.exit_levels("TAX.US", 50.0, buy_day, net_bars, {}, ep.prior_row(led, "L", TODAY), K(), TODAY)
    assert low["action"] == "SELL"                                                 # last close 40 under the net stop 40.85


def _div_opener(req, timeout):
    bars, _ = _div_bars()
    ts = [int(dt.datetime(b[0].year, b[0].month, b[0].day, 14, 30, tzinfo=UTC).timestamp()) for b in bars]
    raw = [b[4] / b[5] for b in bars]
    p = chart(ts, raw, [c * 1.01 for c in raw], [c * 0.99 for c in raw], raw, adj=[b[4] for b in bars], off=0)
    return Resp(json.dumps(p).encode())


def _div_book(log=True):
    bars, ex = _div_bars()
    lots = [["Symbol", "Name", "Ccy", "Status", "Buy Date", "Buy Price", "Shares"],
            ["TAX.US", "Tax Co", "USD", "Active", bars[25][0].isoformat(), "50", "40"]]
    port = [["Symbol", "Name", "Asset Class", "Current Price"], ["TAX.US", "Tax Co", "Equity", 40]]
    rows = [DIVLOG[0], ["", "AUTO", "TAX.US", "", ex.isoformat(), 10, 40, 400, "USD", ""],
            ["", "MANUAL", "TAX.US", "", ex.isoformat(), -3, 40, -120, "USD", "tax"]]
    tabs = {"_Portfolio_CostBasis": Sheet(lots), "My_Portfolio": Sheet(port)}
    if log:
        tabs["_Dividend_Log"] = Sheet(rows)
    return Book(tabs)


def test_x16_run_gate_on_off_and_missing_log(monkeypatch, capsys):
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    b = _div_book()
    assert ep.run(False, False, book=b, now=NOW, opener=_div_opener) == 0
    led = b.tabs["_Exit_Plan"].values
    assert led[1][2] == "TAX.US" and led[1][4] == pytest.approx(43.0) and led[1][16] == pytest.approx(0.86)
    assert "net of the 30% tax withheld (dividend log)" in capsys.readouterr().out
    stop_net = led[1][10]
    monkeypatch.setenv("TFB_EXIT_DIV_NET", "off")                                   # the gate: the 1.3.2 behaviour
    assert ep.knobs()["div_net"] is False
    assert ep.run(False, False, book=b, now=NOW, opener=_div_opener) == 0
    led = b.tabs["_Exit_Plan"].values
    assert led[1][4] == pytest.approx(40.0) and led[1][16] == pytest.approx(0.8)
    assert led[1][10] == pytest.approx(stop_net * 0.8 / 0.86, abs=1e-3)             # the stop of record returns
    assert "tax withheld" not in capsys.readouterr().out
    monkeypatch.delenv("TFB_EXIT_DIV_NET")
    assert ep.knobs()["div_net"] is True
    for off in ("0", "false", "no", "OFF"):
        monkeypatch.setenv("TFB_EXIT_DIV_NET", off)
        assert ep.knobs()["div_net"] is False
    monkeypatch.delenv("TFB_EXIT_DIV_NET")
    nolog = _div_book(log=False)                                                    # no dividend log: gross, no failure
    assert ep.run(False, False, book=nolog, now=NOW, opener=_div_opener) == 0
    assert nolog.tabs["_Exit_Plan"].values[1][4] == pytest.approx(40.0)



# ----------------------------------------------------------------------------- X17 (1.5.0)
def test_x17_the_stop_of_record_is_never_below_the_brokers(monkeypatch):
    """1.5.0 [BROKER-FLOOR] (18 Nov: five holdings showed a plan stop under the stop working at IBKR)."""
    flat = mk([100.0] * 35, 0.015)
    assert round(lv(flat)["stop"], 6) == 91.0                                                # the rule's own stop
    r = lv(flat, info={"broker_stop": 93.0})
    assert r["stop"] == 93.0 and r["action"] == "HOLD" and round(r["stop_gap_pct"], 6) == 7.0
    assert any("the stop of record is the stop working at IBKR (93.00), above the rule's 91.00: a stop never falls "
               "back" in w for w in r["why"])
    assert any("stop 7.0% under the last close" in w for w in r["why"])            # the room is the real one
    assert not any("is under the plan's" in w for w in r["why"])
    # the gate off = the 1.4.0 levels
    off = ep.exit_levels("AAA.US", 100.0, flat[29][0], flat, {"broker_stop": 93.0}, {}, K(broker_floor=False), TODAY)
    assert round(off["stop"], 6) == 91.0 and not any("working at IBKR" in w for w in off["why"])
    monkeypatch.setenv("TFB_EXIT_BROKER_FLOOR", "off")
    assert ep.knobs()["broker_floor"] is False
    monkeypatch.delenv("TFB_EXIT_BROKER_FLOOR")
    assert ep.knobs()["broker_floor"] is True
    # a broker stop a rounding above, at the last close or above it is not taken; one below is the old note
    assert round(lv(flat, info={"broker_stop": 91.2})["stop"], 6) == 91.0
    assert round(lv(flat, info={"broker_stop": 100.0})["stop"], 6) == 91.0
    assert round(lv(flat, info={"broker_stop": 104.0})["stop"], 6) == 91.0
    low = lv(flat, info={"broker_stop": 85.0})
    assert round(low["stop"], 6) == 91.0 and any("broker stop 85.00 is under the plan's 91.00" in w for w in low["why"])
    # a stop the broker already carries is never a RAISE STOP to-do
    prev = {"Stop": 91.0, "StopNorm": 91.0}
    assert lv(flat, info={"broker_stop": 93.0}, prev=prev)["action"] == "HOLD"
    up = mk([100.0] * 30 + [104.0, 110.0], 0.015)
    took = {"Stop": 91.0, "StopNorm": 91.0, "TP1 Reached": "yes"}                  # the first target is behind it
    rule = lv(up, prev=took)
    assert rule["action"] == "RAISE STOP" and 91.0 < rule["stop"] < 105.0          # the rule alone asks for a raise
    cov = lv(up, info={"broker_stop": 105.0}, prev=took)
    assert cov["stop"] == 105.0 and cov["action"] == "HOLD"
    under = lv(up, info={"broker_stop": 95.0}, prev=took)                          # the broker is behind: still a raise
    assert under["action"] == "RAISE STOP" and under["stop"] == rule["stop"]
    # a re-run of the same close (review of #1021): the ledger already holds the floor and "Was" is still the stop
    # before that close - the row stays HOLD, with a note, and is not a RAISE STOP to-do
    again = lv(flat, info={"broker_stop": 93.0}, prev={"Stop": 93.0, "StopNorm": 93.0, "Was": 91.0, "WasNorm": 91.0})
    assert again["stop"] == 93.0 and again["action"] == "HOLD"
    assert any("the stop working at IBKR (93.00) already carries the stop of record 93.00" in w for w in again["why"])
    gone = lv(flat, prev={"Stop": 93.0, "StopNorm": 93.0, "Was": 91.0, "WasNorm": 91.0})   # no broker stop read
    assert gone["action"] == "RAISE STOP"                                          # then the raise is a to-do
    # the workbook's older broker tabs (the highest stop per symbol) never set the floor: only the capture does
    fb = lv(flat, info={"broker_stop": 94.0, "broker_stop_floor": None})
    assert round(fb["stop"], 6) == 91.0 and not any("working at IBKR" in w for w in fb["why"])
    assert lv(flat, info={"broker_stop": 94.0, "broker_stop_floor": 93.0})["stop"] == 93.0
    # once stored it holds, whatever happens to the order
    kept = lv(flat, prev={"Stop": 93.0, "StopNorm": 93.0})
    assert kept["stop"] == 93.0 and kept["action"] == "HOLD"
    # a close at or under the floored stop is a SELL like any other stop
    fell = mk([100.0] * 34 + [92.5], 0.015)
    assert lv(fell, prev={"Stop": 93.0, "StopNorm": 93.0})["action"] == "SELL"


def test_x17b_run_stores_the_brokers_stop(monkeypatch):
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    monkeypatch.setitem(SERIES, "DDI", [100.0] * 35)
    cap = {"ok": True, "as_of": NOW, "stops": {"DDI.US": [(93.0, 200.0), (94.0, 40.0)]}}
    b = wbook()
    assert ep.run(False, False, book=b, now=NOW, opener=opener, capture=cap) == 0
    row = ep.parse_ledger(b.tabs["_Exit_Plan"].values)[0]
    assert row["Symbol"] == "DDI.US" and row["Stop"] == 93.0 and row["Action"] == "HOLD"   # the lowest working stop
    page = "\n".join(" | ".join(str(c) for c in r) for r in b.tabs["Exit_Plan"].values)
    assert "the stop of record is the stop working at IBKR (93.00), above the rule's 91.00" in page
    # the same close again, an hour later: still HOLD, one row for the day, the same stop
    assert ep.run(False, False, book=b, now=NOW + dt.timedelta(hours=1), opener=opener,
                  capture=dict(cap, as_of=NOW + dt.timedelta(hours=1))) == 0
    rows = ep.parse_ledger(b.tabs["_Exit_Plan"].values)
    assert len(rows) == 1 and rows[0]["Stop"] == 93.0 and rows[0]["Action"] == "HOLD"
    # no usable capture: the workbook's broker tabs are read, and they never set the floor
    b2 = wbook()
    b2.tabs["_IBKR_Account"] = Sheet([["as_of", "2026-11-11 07:30"]])              # Riyadh time = NOW
    b2.tabs["_IBKR_Orders"] = Sheet([["symbol", "side", "type", "price", "status"],
                                     ["DDI", "SELL", "STOP", 93.0, "Submitted"], ["DDI", "SELL", "STOP", 94.0, "Submitted"]])
    assert ep.run(False, False, book=b2, now=NOW, opener=opener, capture={"ok": False, "reason": "none"}) == 0
    assert round(ep.parse_ledger(b2.tabs["_Exit_Plan"].values)[0]["Stop"], 6) == 91.0
    page2 = "\n".join(" | ".join(str(c) for c in r) for r in b2.tabs["Exit_Plan"].values)
    assert "94.00" in page2 and "working at IBKR" not in page2                      # the tabs' stop was read, as a note only
    # the next day the order is gone from the capture: the stop of record stays
    assert ep.run(False, False, book=b, now=NOW + dt.timedelta(days=1), opener=opener,
                  capture={"ok": True, "as_of": NOW + dt.timedelta(days=1), "stops": {}}) == 0
    assert [r["Stop"] for r in ep.parse_ledger(b.tabs["_Exit_Plan"].values)][-1] == 93.0
    step_env = yaml.safe_load((ROOT / ".github/workflows/exit_plan.yml").read_text(encoding="utf-8"))
    step = [s for s in step_env["jobs"]["exit_plan"]["steps"] if "exit_plan.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_EXIT_BROKER_FLOOR"] == "${{ vars.TFB_EXIT_BROKER_FLOOR || 'on' }}"
    assert ep.__version__ == "1.7.1"                                                 # 2026-11-26: 1.7.0, 1.7.1
    assert '("exit_plan", "1.7.1", "exit plan")' in (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")


# ----------------------------------------------------------------------------- X19 (1.6.0 [MARKET-TURN])
CRASH = {"us": {"label": "crash_day", "day": dt.date(2026, 11, 10),
                "text": "the US market on a crash day as of 2026-11-10 (S&P 500 fund -3.6% on the day)"},
         "sa": {"label": "calm", "day": dt.date(2026, 11, 10), "text": "the Saudi market calm as of 2026-11-10"}}


def test_x19_gate_and_labels(monkeypatch):
    assert ep.market_turn_mode() == "observe" and K()["market_turn_labels"] == frozenset(ep.MARKET_TURN_HOLDABLE)
    for raw, want in (("on", "on"), ("ENFORCE", "on"), ("off", "off"), ("0", "off"), ("observe", "observe"),
                      ("maybe", "observe"), ("", "observe")):
        monkeypatch.setenv("TFB_EXIT_MARKET_TURN", raw)
        assert ep.market_turn_mode() == want, raw
    monkeypatch.setenv("TFB_EXIT_MARKET_TURN_LABELS", " Crash_Day ,drawdown,calm,junk")
    assert ep.market_turn_labels() == frozenset({"crash_day", "drawdown"})
    monkeypatch.setenv("TFB_EXIT_MARKET_TURN_LABELS", "")
    assert ep.market_turn_labels() == frozenset()


def test_x19_which_holdings_carry_a_label():
    k = K()
    got = ep.market_turn("DDI.US", "Equity", CRASH, k)
    assert got == {"market": "us", "label": "crash_day", "day": "2026-11-10", "text": CRASH["us"]["text"],
                   "turned": True}
    assert ep.market_turn("SUPN", "Equity", CRASH, k)["market"] == "us"                    # a bare ticker is the US
    assert ep.market_turn("2222.SR", "Equity", CRASH, k)["turned"] is False                # calm never turns
    assert ep.market_turn("BBOX.L", "Equity", CRASH, k) is None                            # Europe not read
    assert ep.market_turn("RY.TO", "Equity", CRASH, k) is None                             # no read market
    for asset in ("Commodity", "FX", "Index", "Sukuk", "Crypto", "Future", "Fixed Income / Sukuk"):
        assert ep.market_turn("GLDX.US", asset, CRASH, k) is None, asset
    assert ep.market_turn("SPY.US", "ETF", CRASH, k)["turned"] is True                    # a fund on a held market
    k2 = K(market_turn_labels=frozenset({"drawdown"}))
    assert ep.market_turn("DDI.US", "Equity", CRASH, k2)["turned"] is False


def test_x19_observe_on_and_the_waits():
    mt = ep.market_turn("DDI.US", "Equity", CRASH, K())
    res = {"ok": True, "action": "HOLD", "why": ["+0.4R at the high"]}
    ep.apply_market_turn(res, mt, K(market_turn="observe"))
    assert res["action"] == "HOLD" and res["market"] == "us:crash_day:2026-11-10" and res["market_turned"]
    assert res["why"][-1].startswith("market not calm (observe): the US market on a crash day as of 2026-11-10")
    assert res["why"][-1].endswith("the market-turn rule would sell; nothing changes until TFB_EXIT_MARKET_TURN=on")
    for before in ("HOLD", "TAKE PROFIT", "RAISE STOP", "REVIEW", "SELL"):
        res = {"ok": True, "action": before, "why": ["x"]}
        ep.apply_market_turn(res, mt, K(market_turn="on"))
        assert res["action"] == "SELL" and res["why"][0].startswith("the market turned: the US market on a crash day")
        assert res["why"][1] == "x"
    calm = ep.market_turn("2222.SR", "Equity", CRASH, K())
    res = {"ok": True, "action": "HOLD", "why": []}
    ep.apply_market_turn(res, calm, K(market_turn="on"))
    assert res == {"ok": True, "action": "HOLD", "why": [], "market": "sa:calm:2026-11-10"}
    res = {"ok": True, "action": "HOLD", "why": [], "fresh": True}
    ep.apply_market_turn(res, mt, K(market_turn="on"))
    assert res["action"] == "HOLD" and not res.get("market_turned"), "no close since the purchase: it waits"
    res = {"ok": False, "action": "CHECK", "why": ["no daily price history"]}
    ep.apply_market_turn(res, mt, K(market_turn="on"))
    assert res == {"ok": False, "action": "CHECK", "why": ["no daily price history"]}
    ep.apply_market_turn(res, None, K(market_turn="on"))


def _page(b):
    return "\n".join(" | ".join(str(c) for c in r) for r in b.tabs["Exit_Plan"].values)


def test_x19_run_observe_on_off(monkeypatch):
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    sent = []
    monkeypatch.setenv("TFB_EXIT_MAIL", "1")
    b = wbook()
    assert ep.run(False, True, book=b, now=NOW, opener=opener, mailer=lambda *a: sent.append(a), market=CRASH) == 0
    led = b.tabs["_Exit_Plan"].values
    assert led[0] == ep.LEDGER_HEADERS and led[0][17:] == ["Market", "Judgment"] and len(led[0]) == 19   # 1.7.0
    assert led[1][2] == "DDI.US" and led[1][13] == "TAKE PROFIT" and led[1][17] == "us:crash_day:2026-11-10"
    page = _page(b)
    assert "market not calm (observe): the US market on a crash day as of 2026-11-10" in page
    assert "market not calm: 1 holding (observe)" in page
    assert "Market turn (observe): a holding whose market is not calm (crash_day, drawdown, stress, weak) is " \
           "flagged; its action does not change." in page
    assert sent[-1][0] == "TFB Exit Plan 2026-11-11 - 2 actions: DDI.US take profit, GONE.US check"
    monkeypatch.setenv("TFB_EXIT_MARKET_TURN", "on")
    b2 = wbook()
    assert ep.run(False, True, book=b2, now=NOW, opener=opener, mailer=lambda *a: sent.append(a), market=CRASH) == 0
    led = b2.tabs["_Exit_Plan"].values
    assert led[1][13] == "SELL" and led[1][17] == "us:crash_day:2026-11-10"
    page = _page(b2)
    assert "the market turned: the US market on a crash day" in page and "is a SELL." in page
    assert "(observe)" not in page
    assert sent[-1][0] == "TFB Exit Plan 2026-11-11 - 2 actions: DDI.US sell, GONE.US check"
    # off: no read, no note, the Market column empty
    monkeypatch.setenv("TFB_EXIT_MARKET_TURN", "off")

    def no_read(*a, **k):
        raise AssertionError("TFB_EXIT_MARKET_TURN=off reads no market")

    monkeypatch.setattr(ep, "read_market", no_read)
    b3 = wbook()
    assert ep.run(False, False, book=b3, now=NOW, opener=opener) == 0
    led = b3.tabs["_Exit_Plan"].values
    assert led[1][13] == "TAKE PROFIT" and led[1][17] == ""
    assert "Market turn" not in _page(b3) and "market not calm" not in _page(b3)


def test_x19_live_read_and_its_faults(monkeypatch):
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    calls = []

    def op(req, timeout):
        calls.append(req.full_url)
        return opener(req, timeout)

    b = wbook()
    assert ep.run(False, False, book=b, now=NOW, opener=op) == 0                 # observe by default: a live read
    assert any("/chart/SPY?" in u for u in calls) and any("%5ETASI.SR" in u for u in calls)
    led = b.tabs["_Exit_Plan"].values
    assert led[1][17] == "" and "market not calm" not in _page(b), "no label read: no note"
    monkeypatch.setattr("core.market_condition.read", lambda **kw: 1 / 0)
    assert ep.read_market(NOW, opener) == {}


def test_x19_a_1_5_0_ledger_is_read_and_carried(monkeypatch):
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    old_head = ep.LEDGER_HEADERS_V15
    assert len(old_head) == 17 and "Market" not in old_head
    key = "DDI.US|%s|100.0000" % days(30)[29].isoformat()
    old_row = ["2026-11-10", "2026-11-10 07:35", "DDI.US", key, 100, 240, 108, 108, 3.0, 9.0, 95.0, 109.0, "no",
               "HOLD", "", "2026-11-09", 1.0]
    got = ep.parse_ledger([old_head, old_row])
    assert len(got) == 1 and got[0]["Stop"] == 95.0 and got[0]["Market"] == ""
    assert ep.parse_ledger([old_head[:-1], old_row]) == [], "any other header is not a ledger"
    b = wbook()
    b.tabs["_Exit_Plan"] = Sheet([old_head, old_row])
    assert ep.run(False, False, book=b, now=NOW, opener=opener, market=CRASH) == 0
    led = b.tabs["_Exit_Plan"].values
    assert led[0] == ep.LEDGER_HEADERS
    assert led[1][:17] == old_row and led[1][17] == ""                                 # the old row, kept as written
    assert led[2][2] == "DDI.US" and led[2][17] == "us:crash_day:2026-11-10"
    assert led[2][10] >= 95.0, "the stop of the 1.5.0 ledger still holds (it never moves down)"


def test_x19_workflow_knobs():
    wf = yaml.safe_load((ROOT / ".github/workflows/exit_plan.yml").read_text(encoding="utf-8"))
    step = [s for s in wf["jobs"]["exit_plan"]["steps"] if "exit_plan.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_EXIT_MARKET_TURN"] == "${{ vars.TFB_EXIT_MARKET_TURN || 'observe' }}"
    assert step["env"]["TFB_EXIT_MARKET_TURN_LABELS"] == \
        "${{ vars.TFB_EXIT_MARKET_TURN_LABELS || 'weak,stress,drawdown,crash_day' }}"
    assert ep.MARKET_TURN_LABELS_DEFAULT == "weak,stress,drawdown,crash_day"


# ----------------------------------------------------------------------------- X20 (1.6.1 [LOG-PRIVACY])
def test_x20_the_run_log_names_no_holding_on_actions(monkeypatch, capsys):
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    monkeypatch.setenv("TFB_EXIT_MAIL", "1")
    monkeypatch.delenv("TFB_EXIT_LOG_DETAIL")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert ep.log_detail() is False
    sent = []
    b = wbook()
    assert ep.run(False, True, book=b, now=NOW, opener=opener, mailer=lambda *a: sent.append(a), market=CRASH) == 0
    out = capsys.readouterr().out
    for word in ("DDI", "GONE", "5023", "TEN", "110.00", "100.00", "DoubleDown"):
        assert word not in out, word
    assert re.search(r"holding \d of 3: TAKE PROFIT \(market not calm\)", out)
    assert "plan pick 1 of 1: levels computed" in out
    assert "e-mail sent: TFB Exit Plan 2026-11-11 - 2 actions" in out
    assert sent and "DDI.US" in sent[-1][0], "the e-mail itself is unchanged"
    page = _page(b)
    assert "DDI.US" in page and "market not calm (observe)" in page, "the page is unchanged"
    assert b.tabs["_Exit_Plan"].values[1][2] == "DDI.US", "the ledger is unchanged"
    assert ep.run(True, False, book=wbook(), now=NOW, opener=opener, market=CRASH) == 0
    out = capsys.readouterr().out
    assert "dry run - nothing written, nothing sent | TFB Exit Plan 2026-11-11 - 2 actions" in out and "DDI" not in out


def test_x20_detail_elsewhere_and_by_choice(monkeypatch, capsys):
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    monkeypatch.delenv("TFB_EXIT_LOG_DETAIL")
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    assert ep.log_detail() is True                                                   # a local run: the full lines
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    for raw, want in (("on", True), ("1", True), ("off", False), ("auto", False), ("", False), ("maybe", False)):
        monkeypatch.setenv("TFB_EXIT_LOG_DETAIL", raw)
        assert ep.log_detail() is want, raw
    monkeypatch.setenv("TFB_EXIT_LOG_DETAIL", "on")
    assert ep.run(True, False, book=wbook(), now=NOW, opener=opener, market=CRASH) == 0
    out = capsys.readouterr().out
    assert "DDI.US    TAKE PROFIT" in out and "PICK TEN.US" in out
    wf = (ROOT / ".github/workflows/exit_plan.yml").read_text(encoding="utf-8")
    assert "TFB_EXIT_LOG_DETAIL" not in wf, "the workflow keeps the default: no detail on Actions"


# ----------------------------------------------------------------------------- X24 (1.7.1: the newest news rows)
class RangeSheet(Sheet):
    """A worksheet that returns only the A1 range it is asked for, as the Sheets API does. The plain Sheet above
    returns every row whatever the range - which is how "A1:M12000" passed every test while, on the live tab, it had
    stopped seeing the rows the news gate appends."""

    def get(self, rng):
        m = re.match(r"([A-Z]+)(\d+):([A-Z]+)(\d*)$", rng)
        c0, r0, c1 = _colnum(m.group(1)) - 1, int(m.group(2)) - 1, _colnum(m.group(3))
        r1 = int(m.group(4)) if m.group(4) else len(self.values)
        out = [list(row[c0:c1]) for row in self.values[r0:r1]]
        while out and not any(str(x) != "" for x in out[-1]):
            out.pop()
        return out


NEWS_HDR = ["Item ID", "First Seen (Riyadh)", "Published (UTC)", "Scope", "Event Type", "Direction", "Severity",
            "Rumour/Denial", "Source", "Domain", "Tier", "Cluster Grade", "Headline", "URL", "Cluster", "Run ID"]


def _old_rows(n):
    return [["o%d" % i, "", "2026-10-28 09:00", "MACRO", "war_escalation", -1, "H", "", "google_news", "reuters.com",
             "B", 1, "older market-wide headline %d" % i, "u", "c", "r"] for i in range(n)]


FRESH_NEG = ["n1", "", "2026-11-10 12:00", "DDI.US", "auditor_change", -1, "M", "", "edgar", "sec.gov", "A", 1,
             "8-K 4.01 auditor change", "u", "c", "r"]


def test_x24_the_newest_news_rows_are_read():
    """21 Nov 2026: _News_Items passed 12,000 rows; by 26 Nov 7,527 newer rows were outside "A1:M12000"."""
    book = Book({"_News_Items": RangeSheet([NEWS_HDR] + _old_rows(12500) + [FRESH_NEG])})
    assert len(ep._read(book, "_News_Items", "A1:M12000")) == 12000                # what 1.7.0 asked for ...
    assert ep.read_news(ep._read(book, "_News_Items", "A1:M12000"), NOW, 3) == {}  # ... and what it could not see
    grid, span = ep.read_newest(book, "_News_Items", ep.NEWS_COLS)
    assert span == (2, 12502) and grid[0] == list(ep.NEWS_COLS) and len(grid) == 12502
    assert ep.read_news(grid, NOW, 3) == {"DDI.US": {"n": 1, "headline": "8-K 4.01 auditor change", "ignored": 0,
                                                     "at": dt.datetime(2026, 11, 10, 12, 0, tzinfo=UTC)}}
    # a tab longer than the budget: the rows left out are the OLDEST
    grid, span = ep.read_newest(book, "_News_Items", ep.NEWS_COLS, rows=100)
    assert span == (12403, 12502) and len(grid) == 101 and grid[-1][1] == "DDI.US" and grid[1][1] == "MACRO"
    assert ep.read_news(grid, NOW, 3)["DDI.US"]["n"] == 1
    assert ep.NEWS_ROWS == 20000
    # the same answer as a whole read while the tab is short
    short = [NEWS_HDR, FRESH_NEG, ["n2", "", "2026-11-10 13:00", "GLPI.US", "earnings", -1, "H", "", "google_news", "w.com",
                                   "B", 2, "GLPI cuts guidance", "u", "c", "r"]]
    for kind in (Sheet, RangeSheet):
        grid, span = ep.read_newest(Book({"_News_Items": kind(short)}), "_News_Items", ep.NEWS_COLS)
        assert span == (2, 3) and ep.read_news(grid, NOW, 3) == ep.read_news(short, NOW, 3) != {}
    # a blank cell inside a column tail does not shift the rows under it
    gaps = [NEWS_HDR, ["a", "", "2026-11-10 12:00", "DDI.US", "earnings", -1, "M", "", "s", "d", "B", "", "", "u"],
            ["b", "", "2026-11-10 13:00", "GLPI.US", "earnings", -1, "H", "", "s", "d", "B", 2, "GLPI cuts guidance", "u"]]
    grid, _span = ep.read_newest(Book({"_News_Items": RangeSheet(gaps)}), "_News_Items", ep.NEWS_COLS)
    got = ep.read_news(grid, NOW, 3)
    assert got["GLPI.US"]["headline"] == "GLPI cuts guidance" and got["DDI.US"]["headline"] == ""
    # nothing to read
    assert ep.read_newest(Book({}), "_News_Items", ep.NEWS_COLS) == ([], None)                       # no tab
    assert ep.read_newest(Book({"_News_Items": RangeSheet([])}), "_News_Items", ep.NEWS_COLS) == ([], None)
    assert ep.read_newest(Book({"_News_Items": RangeSheet([["A", "B"]])}), "_News_Items", ep.NEWS_COLS) == ([], None)
    assert ep.read_newest(Book({"_News_Items": RangeSheet([NEWS_HDR])}), "_News_Items", ep.NEWS_COLS) \
        == ([list(ep.NEWS_COLS)], None)

    class Down(Book):
        def worksheet(self, name):
            raise ConnectionError("down")
    with pytest.raises(ConnectionError):                       # a fault is not an empty tab: the run retries it
        ep.read_newest(Down({}), "_News_Items", ep.NEWS_COLS)


def test_x24_the_page_carries_a_negative_item_recorded_after_row_12000(monkeypatch, capsys):
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    monkeypatch.setenv("TFB_EXIT_PLAN", "on")
    monkeypatch.setenv("TFB_EXIT_LOG_DETAIL", "off")
    quiet = wbook()
    assert ep.run(False, False, book=quiet, now=NOW, opener=opener) == 0
    capsys.readouterr()
    b = wbook()
    b.tabs["_News_Items"] = RangeSheet([NEWS_HDR] + _old_rows(12500) + [FRESH_NEG])
    assert ep.run(False, False, book=b, now=NOW, opener=opener) == 0
    out = capsys.readouterr().out
    assert "news: rows 2-12502 of _News_Items read (the newest 12501)" in out
    assert "DDI" not in out and "auditor" not in out           # the public log names no holding and no headline

    def why(book):
        return next(" | ".join(str(c) for c in r) for r in book.tabs["Exit_Plan"].values if r and r[0] == "DDI.US")
    assert "negative news (1 item in 3 days)" in why(b) and "latest negative item: 8-K 4.01 auditor change" in why(b)
    assert "negative news" not in why(quiet) and "latest negative item" not in why(quiet)
    led = {r[2]: r for r in b.tabs["_Exit_Plan"].values[1:]}
    assert "negative news (1 item in 3 days)" in str(led["DDI.US"])
