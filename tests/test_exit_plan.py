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
    assert got == {"DDI.US": {"n": 2, "headline": "DDI misses", "at": dt.datetime(2026, 11, 10, 12, 0, tzinfo=UTC)}}
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
        ["2026-11-10", "PICK", "TEN.US", 100, 47, 92, "CHECK", "USD"]]
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
    assert on["schedule"] == [{"cron": "30 4 * * *"}, {"cron": "35 0-3,5-23 * * *"}] and "workflow_dispatch" in on
    hours = set(range(0, 4)) | set(range(5, 24))
    assert len(hours) == 23                                                         # every hour, 04 is the 04:30 run
    assert '"$EVENT_CRON" == "30 4 * * *"' in raw and "EVENT_CRON: ${{ github.event.schedule }}" in raw
    assert wf["permissions"] == {"contents": "read"}
    step = [s for s in wf["jobs"]["exit_plan"]["steps"] if "exit_plan.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_EXIT_PLAN"] == "${{ vars.TFB_EXIT_PLAN || 'on' }}"
    assert step["env"]["TFB_EXIT_MAIL"] == "${{ vars.TFB_EXIT_MAIL || '1' }}"
    assert "--send" in step["run"] and "--dry-run" in step["run"]
    src = (ROOT / "scripts/exit_plan.py").read_text(encoding="utf-8")
    for name in re.findall(r"(TFB_EXIT_[A-Z_]+): \$\{\{", raw):
        assert '"%s"' % name in src, name
    assert '("exit_plan", "%s", "exit plan")' % ep.__version__ in (ROOT / "scripts/verify_deployment.py").read_text(
        encoding="utf-8")
    assert "tests/test_exit_plan.py" in (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert not re.search(r"create_order|place_order|requests\.post|cancel_order", src)


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
