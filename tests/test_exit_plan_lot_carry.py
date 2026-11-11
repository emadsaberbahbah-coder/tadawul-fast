#!/usr/bin/env python3
"""tests/test_exit_plan_lot_carry.py - scripts/exit_plan.py 1.2.0 [LOT-CARRY].

read_lots merges a symbol's active lots (weighted buy price, latest buy date) and the ledger keys the holding by
both. A sale from one of several lots, or a closed lot, changes that key, prior_row finds nothing, and the holding
loses the highest stop ever recommended and its once-only TAKE PROFIT flag: 1.1.0 then issues 'sell half' again
and can publish a lower stop. With TFB_EXIT_LOT_CARRY on, the holding's earlier rows carry over.

Runs as a script ("PASS k/k") and under pytest. Zero network: the real run() reads an in-memory workbook and a fake
Yahoo chart opener. Controls: TFB_TEST_CARRY_TARGET (the exit_plan.py under test; the untouched main copy there is
the golden negative), TFB_TEST_EXIT_BASE (a path to the main 53f2abdc copy instead of `git show`),
TFB_TEST_REQUIRE_BASE=1 (a missing base FAILS the base legs instead of skipping them; set in CI).

  L0  version floor; the gate words (unset and anything else read off)
  L1  two lots (230 @ 100.00, 10 @ 104.00): TAKE PROFIT on the 110 close, 120 shares of the first lot sold; the next
      close: on -> no second TAKE PROFIT, TP1 kept, the stop not lower, the note names the earlier key, the e-mail
      says 'hold all'; off (= 1.1.0) -> TAKE PROFIT again and 'take profit' in the e-mail subject; a one-share and
      a fractional sale also carry
  L2  two fills a few cents apart on one day, a sale after the stop was raised: off publishes a LOWER stop with
      'RAISE STOP' and loses the TP1 flag; on keeps the stop and the flag and reads HOLD
  L3  the newest lot closed entirely (the key's date moves earlier): carried
  L4  a top-up (a lot bought since): nothing carried - on equals off
  L5  sold out, bought again later: nothing carried - on equals off
  L6  a single lot sold down (the key does not change): on equals off
  L7  a second run on the same close after the change: the same action, the same stop, no RAISE STOP of its own
  L8  OFF IDENTITY: every scenario, gate unset / off / junk, against the untouched 1.1.0 module: ledger, page,
      e-mail and every log line (the version tokens folded)
  L9  carry_rows (the previous key, the three tests, the exact return, malformed rows, relabel, the basis move),
      _factor_at and carry_was directly
  L10 the workflow maps the gate (default 'off'), the manifest pins the version, ci.yml runs this file with the
      base fetched by SHA
  L11 a top-up after TAKE PROFIT, then a small sale from the old lot: the state from before the top-up does not come
      back (1.2.0's first commit gave SELL at the old stop on a close of 100, and hid TAKE PROFIT at 117)
  L12 the newest lot closed across an ex-dividend date between the two buy dates: the carried stop is not rescaled
  L13 sold out and bought again on the same day: the new position starts fresh (no SELL at the old stop)
  L14 TAKE PROFIT under the new key, then the next close: no second 'sell half'
  L15 two runs on one Riyadh date, the second on a newer close: the stop holds, no RAISE STOP to a lower stop
  L16 a weekend re-run on the same close: TAKE PROFIT stays, as on the one-key path
  L17 sold out, bought again in two lots, then a sale: nothing of the old holding comes back
  L18 a lot closed (carried), then re-activated with a sale: the lot reads like one bought since (as 1.1.0)
  L19 two sales on consecutive closes, then a close: the note names the key just before, then goes away
  L20 a carried holding without price history (CHECK): no note
  L21 the gate switched on after a lot change made while it was off: the first-target state comes back; the stop
      stays at what was published last (a carried stop is never above it)
  L22 a top-up typed in after its own close was computed (or bought after hours), also one dated on the newest
      lot's day: a new position, as on time (round 2 of the review)
  L23 a same-day sell-out and re-buy typed in after that close was computed: the new position starts fresh
  L24 a top-up lot and one fill closed: the topped-up position's state carries; only the top-up lot closed: the key
      returns exactly and its own rows apply (on equals off)
  L25 L21 over a weekend (two Riyadh dates on one close): the stop published last holds, 'was' is that stop
  L26 a re-run on the same close keeps the stop the first run published (a tightening input gone)
  L27 the cap on a seeded ledger: an earlier key's higher stop never comes back above the stop published last; the
      0.25% 'was' threshold at its boundary on a re-run; a dividend between the two stops
  L28 L21 with a dividend factor on every bar: the cap compares normalised stops
  L29 a single lot on a dividend factor not exact at 4 decimals: on equals off
  L30 a sale recorded between two runs on one close: TAKE PROFIT stays for the close, then HOLD (round 3)
  L31 a price typo corrected, a 2:1 split and a Tadawul bonus issue typed into the lots, each followed by sales:
      the key from before the edit never comes back (on equals off, or the post-split key carries)
  L32 a lot typed by mistake, then deleted or closed: on equals off (the key returns exactly)
  L33 a sale typed with too many shares and corrected: the count goes up, nothing carries (on equals off)
  L34 a raise close after carried sales, run twice: no note on either run
  L35 edits and sales on one Riyadh date (each run overwrites that date's rows): a typo corrected then a sale, a
      split typed in then a sale, an exact return then a sale; and a previous key computed only inside its newest
      lot's session, that lot then removed: never a stop that was not published last (round 4)

Run: python tests/test_exit_plan_lot_carry.py  |  pytest -q tests/test_exit_plan_lot_carry.py
"""
from __future__ import annotations

import contextlib
import copy
import datetime as dt
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BASE_SHA = "53f2abdc5be5c13294f4009b3b1bc3d1964907d5"     # main: exit_plan 1.1.0 (#956, #957, #959)
GATE = "TFB_EXIT_LOT_CARRY"
DELIVERED = os.getenv("TFB_TEST_CARRY_TARGET", "").strip() or str(ROOT / "scripts" / "exit_plan.py")
UTC = dt.timezone.utc


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


ep = _load("ep_lot_carry", DELIVERED)
_BASE = {}


def base():
    """The untouched main copy (53f2abdc, exit_plan 1.1.0), or skip (or fail when TFB_TEST_REQUIRE_BASE=1)."""
    if "mod" not in _BASE:
        path = os.getenv("TFB_TEST_EXIT_BASE", "").strip()
        if not path:
            try:
                text = subprocess.run(["git", "show", BASE_SHA + ":scripts/exit_plan.py"], cwd=ROOT,
                                      capture_output=True, text=True, check=True).stdout
            except Exception:
                text = ""
            if text:
                d = Path(tempfile.mkdtemp()) / "base"
                d.mkdir()
                (d / "exit_plan_v110_main.py").write_text(text, encoding="utf-8")
                path = str(d / "exit_plan_v110_main.py")
        _BASE["mod"] = _load("ep_lot_carry_base", path) if path else None
    if _BASE["mod"] is None:
        if os.getenv("TFB_TEST_REQUIRE_BASE", "").strip() == "1":
            raise AssertionError("base copy required (TFB_TEST_REQUIRE_BASE=1) but unavailable")
        pytest.skip("base copy unavailable (set TFB_TEST_EXIT_BASE)")
    return _BASE["mod"]


# ----------------------------------------------------------------------------- fakes
class WorksheetNotFound(Exception):
    pass


def _colnum(letters):
    n = 0
    for ch in letters:
        n = n * 26 + ord(ch) - 64
    return n


class Sheet:
    def __init__(self, rows):
        self.values = copy.deepcopy(rows)
        self.row_count, self.col_count = 500, 30

    def get(self, rng):
        return copy.deepcopy(self.values)

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
        # what a later read returns: the non-empty rows, every cell as the text Sheets shows
        self.values = [["" if c is None else str(c) for c in r] for r in values if any(str(c) != "" for c in r)]


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


class Resp:
    def __init__(self, b):
        self.b = b

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def read(self):
        return self.b


D0 = dt.date(2026, 9, 1)


def days(n, start=D0):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


DAYS = days(60)


def opener_for(series, adj=None):
    """A Yahoo chart opener serving `series` {YAHOO_SYMBOL: [closes]}: a 1.5% high-low spread around each close.
    `adj` {YAHOO_SYMBOL: [dividend factors]} adds the adjusted close (close x factor)."""
    def opener(req, timeout):
        sym = req.full_url.split("/chart/")[1].split("?")[0]
        if sym not in series:
            e = Exception("404")
            e.code = 404
            raise e
        closes = series[sym]
        ts = [int(dt.datetime(d.year, d.month, d.day, 14, 30, tzinfo=UTC).timestamp()) for d in DAYS[:len(closes)]]
        ind = {"quote": [{"open": list(closes), "high": [c * 1.015 for c in closes],
                          "low": [c * 0.985 for c in closes], "close": list(closes)}]}
        if adj and sym in adj:
            ind["adjclose"] = [{"adjclose": [c * f for c, f in zip(closes, adj[sym])]}]
        return Resp(json.dumps({"chart": {"result": [{"meta": {"gmtoffset": 0}, "timestamp": ts,
                                                       "indicators": ind}]}}).encode())
    return opener


LOT_HDR = ["Symbol", "Name", "Ccy", "Status", "Buy Date", "Buy Price", "Shares"]


def lot(sym, i, price, shares, status="Active"):
    return [sym, sym, "USD", status, DAYS[i].isoformat(), str(price), str(shares)]


def morning_after(i, hours=0):
    """07:30 Riyadh on the day after bar i (its close is complete), plus `hours`."""
    d = DAYS[i] + dt.timedelta(days=1)
    return dt.datetime(d.year, d.month, d.day, 4, 30, tzinfo=UTC) + dt.timedelta(hours=hours)


FLAT = [100.0] * 30
SCENARIOS = {
    # L1: two lots at different prices; TAKE PROFIT on the 110 close; 120 shares of the first lot sold
    "l1_sell_half_from_one_lot": [
        (31, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10), lot("BBB.US", 28, "50", 40)],
         {"AAA": FLAT + [104.0, 110.0], "BBB": [50.0] * 32}),
        (32, 0, [lot("AAA.US", 28, "100.00", 110), lot("AAA.US", 30, "104.00", 10), lot("BBB.US", 28, "50", 40)],
         {"AAA": FLAT + [104.0, 110.0, 110.5], "BBB": [50.0] * 33}),
    ],
    # L2: two fills a few cents apart on one day; TAKE PROFIT, a raised stop, then a sale on a wide down day
    "l2_close_fills_stop_drop": [
        (31, 0, [lot("AAA.US", 30, "100.00", 230), lot("AAA.US", 30, "100.30", 10)],
         {"AAA": FLAT + [101.0, 110.0]}),
        (32, 0, [lot("AAA.US", 30, "100.00", 230), lot("AAA.US", 30, "100.30", 10)],
         {"AAA": FLAT + [101.0, 110.0, 118.0]}),
        (33, 0, [lot("AAA.US", 30, "100.00", 110), lot("AAA.US", 30, "100.30", 10)],
         {"AAA": FLAT + [101.0, 110.0, 118.0, 108.0]}),
    ],
    # L3: the newest lot closed entirely: the key's date moves back to the first lot
    "l3_newest_lot_closed": [
        (31, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0]}),
        (32, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10, "Closed")],
         {"AAA": FLAT + [104.0, 110.0, 110.5]}),
    ],
    # L4: a top-up after TAKE PROFIT: a new lot bought since - a new position, nothing carried
    "l4_top_up": [
        (31, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0]}),
        (32, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10), lot("AAA.US", 32, "110.50", 20)],
         {"AAA": FLAT + [104.0, 110.0, 110.5]}),
        (33, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10), lot("AAA.US", 32, "110.50", 20)],
         {"AAA": FLAT + [104.0, 110.0, 110.5, 111.0]}),
    ],
    # L5: sold out, bought again later: the old rows predate the new holding's first buy - nothing carried
    "l5_sold_out_rebought": [
        (31, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0]}),
        (34, 0, [lot("AAA.US", 28, "100.00", 230, "Closed"), lot("AAA.US", 30, "104.00", 10, "Closed"),
                 lot("AAA.US", 33, "105.00", 50)],
         {"AAA": FLAT + [104.0, 110.0, 108.0, 105.0, 106.0]}),
    ],
    # L6: a single lot sold down: the key does not change
    "l6_single_lot": [
        (31, 0, [lot("AAA.US", 30, "100.00", 240)], {"AAA": FLAT + [104.0, 110.0]}),
        (32, 0, [lot("AAA.US", 30, "100.00", 120)], {"AAA": FLAT + [104.0, 110.0, 110.5]}),
    ],
    # L7: L1, then a second run on the same close two hours later
    "l7_same_close_rerun": [
        (31, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0]}),
        (32, 0, [lot("AAA.US", 28, "100.00", 110), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0, 110.5]}),
        (32, 2, [lot("AAA.US", 28, "100.00", 110), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0, 110.5]}),
    ],
}


# L11-L21 (the review of 1.2.0's first commit). A step may carry a 5th element {"adj": {SYM: [factors]}}.
A0 = [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10)]
A_SOLD = [lot("AAA.US", 28, "100.00", 110), lot("AAA.US", 30, "104.00", 10)]
P11 = FLAT + [104.0, 110.0, 110.5, 111.0, 112.0, 113.0]
B33 = lot("AAA.US", 33, "111.00", 50)


def _a100(shares):
    return lot("AAA.US", 28, "100.00", shares)


def _top_up_then_sale(last_close, sale=True):
    """One lot; TAKE PROFIT on the 110 close; half sold (the key holds); a top-up (a new position); then 10 shares of
    the old lot sold (a new key again)."""
    return [(31, 0, [_a100(100)], {"AAA": P11[:32]}), (32, 0, [_a100(50)], {"AAA": P11[:33]}),
            (33, 0, [_a100(50), B33], {"AAA": P11[:34]}), (34, 0, [_a100(50), B33], {"AAA": P11[:35]}),
            (35, 0, [_a100(50), B33], {"AAA": P11[:36]}),
            (36, 0, [_a100(40) if sale else _a100(50), B33], {"AAA": P11 + [last_close]})]


P12 = FLAT + [104.0, 110.0, 118.0, 108.0]
ADJ12 = {"AAA": [0.95] * 30 + [1.0] * 10}                 # an ex-dividend date on bar 30, the newest lot's buy day


def _dividend(closed):
    a, b = lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10, "Closed" if closed else "Active")
    return [(31, 0, [a, lot("AAA.US", 30, "104.00", 10)], {"AAA": P12[:32]}, {"adj": ADJ12}),
            (32, 0, [a, lot("AAA.US", 30, "104.00", 10)], {"AAA": P12[:33]}, {"adj": ADJ12}),
            (33, 0, [a, b], {"AAA": P12[:34]}, {"adj": ADJ12})]


P13 = FLAT + [104.0, 110.0]
OLD13, NEW13 = lot("AAA.US", 32, "110.00", 100), lot("AAA.US", 32, "100.00", 100)
OLD13C = lot("AAA.US", 32, "110.00", 100, "Closed")
P14 = [lot("AAA.US", 26, "100.00", 110), lot("AAA.US", 28, "104.00", 10)]
P17 = [lot("AAA.US", 28, "100.00", 230, "Closed"), lot("AAA.US", 30, "104.00", 10, "Closed")]
SCENARIOS.update({
    "l11_top_up_then_sale_100": _top_up_then_sale(100.0),
    "l11_top_up_then_sale_117": _top_up_then_sale(117.0),
    "l11_top_up_no_sale_117": _top_up_then_sale(117.0, sale=False),
    "l12_dividend_newest_closed": _dividend(True),
    "l12_dividend_newest_kept": _dividend(False),
    # L13: the morning run sees the first lot; it is sold and a new one bought the same day; the next close is 99
    "l13_same_day_round_trip": [(31, 0, [OLD13], {"AAA": P13}), (31, 3, [OLD13C, NEW13], {"AAA": P13}),
                                (32, 0, [OLD13C, NEW13], {"AAA": P13 + [99.0]})],
    "l13_fresh_reference": [(31, 3, [NEW13], {"AAA": P13}), (32, 0, [NEW13], {"AAA": P13 + [99.0]})],
    # L14: a sale before the first target; TAKE PROFIT under the new key; the next close
    "l14_tp_after_change_next_close": [
        (29, 0, [lot("AAA.US", 26, "100.00", 230), lot("AAA.US", 28, "104.00", 10)], {"AAA": FLAT}),
        (30, 0, P14, {"AAA": FLAT + [101.0]}), (31, 0, P14, {"AAA": FLAT + [101.0, 110.0]}),
        (32, 0, P14, {"AAA": FLAT + [101.0, 110.0, 110.5]})],
    # L15: the same Riyadh date, a later run on a newer close
    "l15_same_date_new_close": [(31, 0, A0, {"AAA": FLAT + [104.0, 110.0]}),
                                (32, 0, A_SOLD, {"AAA": FLAT + [104.0, 110.0, 118.0]}),
                                (32, 12, A_SOLD, {"AAA": FLAT + [104.0, 110.0, 118.0, 108.0]})],
    # L16: TAKE PROFIT on Friday's close at the Saturday run after the change; the Sunday run on the same close
    "l16_weekend_same_close": [(32, 0, A0, {"AAA": FLAT + [100.0, 101.0, 102.0]}),
                               (33, 0, A_SOLD, {"AAA": FLAT + [100.0, 101.0, 102.0, 110.0]}),
                               (33, 24, A_SOLD, {"AAA": FLAT + [100.0, 101.0, 102.0, 110.0]})],
    "l16_weekend_one_key": [(32, 0, A_SOLD, {"AAA": FLAT + [100.0, 101.0, 102.0]}),
                            (33, 0, A_SOLD, {"AAA": FLAT + [100.0, 101.0, 102.0, 110.0]}),
                            (33, 24, A_SOLD, {"AAA": FLAT + [100.0, 101.0, 102.0, 110.0]})],
    # L17: sold out (the closed lots listed first), bought again in two lots, then a sale from one of them
    "l17_rebought_two_lots_then_sale": [
        (31, 0, A0, {"AAA": FLAT + [104.0, 110.0]}),
        (34, 0, P17 + [lot("AAA.US", 33, "105.00", 50), lot("AAA.US", 33, "105.20", 10)],
         {"AAA": FLAT + [104.0, 110.0, 108.0, 105.0, 106.0]}),
        (35, 0, P17 + [lot("AAA.US", 33, "105.00", 30), lot("AAA.US", 33, "105.20", 10)],
         {"AAA": FLAT + [104.0, 110.0, 108.0, 105.0, 106.0, 106.5]})],
    # L18: a lot closed, then re-activated (a corrected status) while part of the first lot is sold
    "l18_closed_reactivated_sale": [
        (31, 0, A0, {"AAA": FLAT + [104.0, 110.0]}),
        (32, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10, "Closed")],
         {"AAA": FLAT + [104.0, 110.0, 110.5]}),
        (33, 0, A_SOLD, {"AAA": FLAT + [104.0, 110.0, 110.5, 111.0]})],
    # L19: L1, then a second sale, then one more close
    "l19_two_sales_then_close": [
        (31, 0, A0, {"AAA": FLAT + [104.0, 110.0]}),
        (32, 0, A_SOLD, {"AAA": FLAT + [104.0, 110.0, 110.5]}),
        (33, 0, [lot("AAA.US", 28, "100.00", 60), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0, 110.5, 111.0]}),
        (34, 0, [lot("AAA.US", 28, "100.00", 60), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0, 110.5, 111.0, 111.5]})],
    # L20: L1 whose second run has no price history for AAA
    "l20_carried_check": [(31, 0, A0, {"AAA": FLAT + [104.0, 110.0]}), (32, 0, A_SOLD, {})],
})
P22 = FLAT + [104.0, 110.0, 112.0, 105.0]
P26 = FLAT + [104.0, 110.0, 110.5]
MY_PF_TIGHT = [["Symbol", "Name", "Asset Class", "Current Price", "Forecast Price (1M)", "Forecast Reliability Score"],
               ["AAA.US", "AAA", "Equity", "110.5", "100", "90"]]
F29 = 0.987654321
P29 = [1000.0] * 30 + [1050.0, 1100.0, 1150.0, 1200.0, 1250.0, 1300.0, 1240.0, 1290.0, 1230.0, 1280.0]
SCENARIOS.update({
    # L22: a top-up bought on day 32 and typed in after the close-32 run (or bought after hours); close 33 = 105
    "l22_late_top_up": [(31, 0, [_a100(100)], {"AAA": P22[:32]}), (32, 0, [_a100(50)], {"AAA": P22[:33]}),
                        (33, 0, [_a100(50), lot("AAA.US", 32, "112.00", 50)], {"AAA": P22[:34]})],
    "l22_on_time_top_up": [(31, 0, [_a100(100)], {"AAA": P22[:32]}),
                           (32, 0, [_a100(50), lot("AAA.US", 32, "112.00", 50)], {"AAA": P22[:33]}),
                           (33, 0, [_a100(50), lot("AAA.US", 32, "112.00", 50)], {"AAA": P22[:34]})],
    # L22b: a top-up dated on the newest lot's own buy day, typed in after a run on a later close
    "l22_late_top_up_same_date": [(31, 0, A0, {"AAA": FLAT + [104.0, 110.0]}),
                                  (32, 0, A0 + [lot("AAA.US", 30, "108.00", 20)], {"AAA": FLAT + [104.0, 110.0, 105.0]})],
    # L23: sold out and bought again on day 31, typed in after the close-31 run (same share count)
    "l23_late_round_trip": [(31, 0, [lot("AAA.US", 31, "110.00", 100)], {"AAA": FLAT + [104.0, 103.0]}),
                            (31, 3, [lot("AAA.US", 31, "110.00", 100, "Closed"), lot("AAA.US", 31, "100.00", 100)],
                             {"AAA": FLAT + [104.0, 103.0]}),
                            (32, 0, [lot("AAA.US", 31, "110.00", 100, "Closed"), lot("AAA.US", 31, "100.00", 100)],
                             {"AAA": FLAT + [104.0, 103.0, 100.0]})],
    # L24: two fills; TAKE PROFIT; a top-up (a new position); the top-up lot and one fill closed again
    "l24_top_up_lot_closed": [
        (31, 0, [lot("AAA.US", 28, "100.00", 60), lot("AAA.US", 28, "100.30", 40)],
         {"AAA": FLAT + [104.0, 110.0]}),
        (32, 0, [lot("AAA.US", 28, "100.00", 30), lot("AAA.US", 28, "100.30", 20)],
         {"AAA": FLAT + [104.0, 110.0, 113.0]}),
        (33, 0, [lot("AAA.US", 28, "100.00", 30), lot("AAA.US", 28, "100.30", 20)],
         {"AAA": FLAT + [104.0, 110.0, 113.0, 114.0]}),
        (34, 0, [lot("AAA.US", 28, "100.00", 30), lot("AAA.US", 28, "100.30", 20), lot("AAA.US", 33, "114.00", 50)],
         {"AAA": FLAT + [104.0, 110.0, 113.0, 114.0, 112.0]}),
        (35, 0, [lot("AAA.US", 28, "100.00", 30), lot("AAA.US", 28, "100.30", 20), lot("AAA.US", 33, "114.00", 50)],
         {"AAA": FLAT + [104.0, 110.0, 113.0, 114.0, 112.0, 108.0]}),
        (36, 0, [lot("AAA.US", 28, "100.00", 30), lot("AAA.US", 28, "100.30", 20, "Closed"),
                 lot("AAA.US", 33, "114.00", 50, "Closed")],
         {"AAA": FLAT + [104.0, 110.0, 113.0, 114.0, 112.0, 108.0, 107.0]})],
    # L26: L1's change; the next close's first run sees a tightening forecast, a re-run on that close does not
    "l26_same_close_rerun_tightened": [(31, 0, A0, {"AAA": FLAT + [104.0, 110.0]}),
                                       (32, 0, A_SOLD, {"AAA": P26}, {"tabs": {"My_Portfolio": MY_PF_TIGHT}}),
                                       (32, 2, A_SOLD, {"AAA": P26}, {"tabs": {"My_Portfolio": None}})],
    # L29: a single lot on a dividend factor that is not exact at 4 decimals: the gate changes nothing
    "l29_single_lot_dividend": [(i, 0, [lot("AAA.US", 30, "1000.00", 100)], {"AAA": P29[:i + 1]},
                                 {"adj": {"AAA": [F29] * len(P29)}}) for i in range(31, len(P29))],
})
# L24b: L24 with only the top-up lot closed: the key returns exactly to the one held before the top-up
SCENARIOS["l24_top_up_only_closed"] = SCENARIOS["l24_top_up_lot_closed"][:5] + [
    (36, 0, [lot("AAA.US", 28, "100.00", 30), lot("AAA.US", 28, "100.30", 20),
             lot("AAA.US", 33, "114.00", 50, "Closed")], SCENARIOS["l24_top_up_lot_closed"][5][3])]
P31 = FLAT + [104.0, 110.0, 112.0, 113.0, 114.0]
S31 = [200.0] * 30 + [208.0, 220.0]                      # before a 2:1 split; after it the feed halves every bar
SR = [50.0] * 30 + [52.0, 55.0]                          # 2222.SR before a 1-for-10 bonus (feed x 10/11 after)
TYPO = lot("AAA.US", 30, "1040.00", 10)
SCENARIOS.update({
    # L30: L1's sale recorded between two runs on one close (an hourly run), then the next close
    "l30_sale_between_runs_on_one_close": [(31, 0, A0, {"AAA": FLAT + [104.0, 110.0]}),
                                           (31, 3, A_SOLD, {"AAA": FLAT + [104.0, 110.0]}),
                                           (32, 0, A_SOLD, {"AAA": FLAT + [104.0, 110.0, 110.5]})],
    # L31: a buy price typed as 1000.00 and corrected the next day (same date and count), TAKE PROFIT, then sales
    # from the single lot (the key holds)
    "l31_price_typo_then_sales": [(31, 0, [lot("AAA.US", 30, "1000.00", 100)], {"AAA": P31[:32]}),
                                  (32, 0, [lot("AAA.US", 30, "100.00", 100)], {"AAA": P31[:33]}),
                                  (33, 0, [lot("AAA.US", 30, "100.00", 100)], {"AAA": P31[:34]}),
                                  (34, 0, [lot("AAA.US", 30, "100.00", 50)], {"AAA": P31[:35]}),
                                  (35, 0, [lot("AAA.US", 30, "100.00", 40)], {"AAA": P31[:36]})],
    # L31b: two lots, TAKE PROFIT, a 2:1 split typed into both lots (the feed halves the history), then a sale
    "l31_split_then_sale": [(31, 0, [lot("AAA.US", 28, "200.00", 50), lot("AAA.US", 30, "208.00", 50)],
                             {"AAA": S31}),
                            (32, 0, [lot("AAA.US", 28, "100.00", 100), lot("AAA.US", 30, "104.00", 100)],
                             {"AAA": [c / 2 for c in S31] + [112.0]}),
                            (33, 0, [lot("AAA.US", 28, "100.00", 40), lot("AAA.US", 30, "104.00", 20)],
                             {"AAA": [c / 2 for c in S31] + [112.0, 113.0]})],
    # L31c: a Tadawul 1-for-10 bonus typed in as 110 @ 45.4545 (the feed x 10/11), then half sold (the key holds)
    "l31_bonus_then_sale": [(31, 0, [lot("2222.SR", 28, "50.00", 100)], {"2222.SR": SR}),
                            (32, 0, [lot("2222.SR", 28, "45.4545", 110)], {"2222.SR": [c * 10 / 11 for c in SR] + [50.0]}),
                            (33, 0, [lot("2222.SR", 28, "45.4545", 55)],
                             {"2222.SR": [c * 10 / 11 for c in SR] + [50.0, 49.91]}),
                            (34, 0, [lot("2222.SR", 28, "45.4545", 55)],
                             {"2222.SR": [c * 10 / 11 for c in SR] + [50.0, 49.91, 49.95]})],
    # L32: a lot typed by mistake (10 @ 1040.00, dated the run's own day) seen by an hourly run, then deleted; and
    # the same lot marked Closed the next morning
    "l32_mistyped_lot_deleted": [(29, 0, [lot("AAA.US", 28, "100.00", 230)], {"AAA": FLAT}),
                                 (30, 0, [lot("AAA.US", 28, "100.00", 230)], {"AAA": FLAT + [101.0]}),
                                 (30, 10, [lot("AAA.US", 28, "100.00", 230), TYPO], {"AAA": FLAT + [101.0]}),
                                 (30, 11, [lot("AAA.US", 28, "100.00", 230)], {"AAA": FLAT + [101.0]}),
                                 (31, 0, [lot("AAA.US", 28, "100.00", 230)], {"AAA": FLAT + [101.0, 99.0]})],
    "l32_mistyped_lot_closed": [(29, 0, [lot("AAA.US", 28, "100.00", 230)], {"AAA": FLAT}),
                                (30, 10, [lot("AAA.US", 28, "100.00", 230), TYPO], {"AAA": FLAT + [101.0]}),
                                (31, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "1040.00", 10, "Closed")],
                                 {"AAA": FLAT + [101.0, 99.0]})],
    # L33: a sale of 120 typed as 130, corrected on the next run (the count goes up again)
    "l33_corrected_sale": [(31, 0, [lot("AAA.US", 28, "100.00", 200), lot("AAA.US", 30, "104.00", 40)],
                            {"AAA": FLAT + [104.0, 115.0]}),
                           (32, 0, [lot("AAA.US", 28, "100.00", 70), lot("AAA.US", 30, "104.00", 40)],
                            {"AAA": FLAT + [104.0, 115.0, 116.0]}),
                           (33, 0, [lot("AAA.US", 28, "100.00", 80), lot("AAA.US", 30, "104.00", 40)],
                            {"AAA": FLAT + [104.0, 115.0, 116.0, 117.0]})],
})
# L35 (round 4): edits and sales on one Riyadh date; each run overwrites that date's rows of the ledger
S35 = [c / 2 for c in S31]
R35 = FLAT + [104.0, 110.0, 118.0, 119.0, 120.0, 121.0]
SCENARIOS.update({
    "l35_typo_then_sale_same_date": [(31, 0, [lot("AAA.US", 30, "1000.00", 100)], {"AAA": P31[:32]}),
                                     (32, 0, [lot("AAA.US", 30, "100.00", 100)], {"AAA": P31[:33]}),
                                     (32, 5, [lot("AAA.US", 30, "100.00", 50)], {"AAA": P31[:33]}),
                                     (33, 0, [lot("AAA.US", 30, "100.00", 50)], {"AAA": P31[:34]})],
    "l35_split_then_sale_same_date": [
        (31, 0, [lot("AAA.US", 28, "200.00", 50), lot("AAA.US", 30, "208.00", 50)], {"AAA": S31}),
        (32, 0, [lot("AAA.US", 28, "100.00", 100), lot("AAA.US", 30, "104.00", 100)], {"AAA": S35 + [112.0]}),
        (32, 3, [lot("AAA.US", 28, "100.00", 40), lot("AAA.US", 30, "104.00", 20)], {"AAA": S35 + [112.0]}),
        (32, 4, [lot("AAA.US", 28, "100.00", 40), lot("AAA.US", 30, "104.00", 20)], {"AAA": S35 + [112.0]}),
        (33, 0, [lot("AAA.US", 28, "100.00", 40), lot("AAA.US", 30, "104.00", 20)], {"AAA": S35 + [112.0, 113.0]})],
    "l35_exact_return_then_sale": [(31, 0, A0, {"AAA": R35[:32]}), (32, 0, A0, {"AAA": R35[:33]}),
                                   (33, 0, A0 + [lot("AAA.US", 33, "1040.00", 10)], {"AAA": R35[:34]}),
                                   (34, 0, A0, {"AAA": R35[:35]}), (34, 3, A_SOLD, {"AAA": R35[:35]}),
                                   (35, 0, A_SOLD, {"AAA": R35[:36]})],
    "l35_control_no_typo": [(31, 0, A0, {"AAA": R35[:32]}), (32, 0, A0, {"AAA": R35[:33]}),
                            (33, 0, A0, {"AAA": R35[:34]}), (34, 0, A0, {"AAA": R35[:35]}),
                            (34, 3, A_SOLD, {"AAA": R35[:35]}), (35, 0, A_SOLD, {"AAA": R35[:36]})],
    "l35_fresh_key_lot_removed": [(29, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "1040.00", 10)],
                                   {"AAA": FLAT}),
                                  (30, 0, [lot("AAA.US", 28, "100.00", 230)], {"AAA": FLAT + [101.0]})],
})
# L34: L19, then a raise close run twice (00:30 and 07:30 Riyadh)
SCENARIOS["l34_raise_close_two_runs"] = SCENARIOS["l19_two_sales_then_close"] + [
    (35, -7, [lot("AAA.US", 28, "100.00", 60), lot("AAA.US", 30, "104.00", 10)],
     {"AAA": FLAT + [104.0, 110.0, 110.5, 111.0, 111.5, 118.0]}),
    (35, 0, [lot("AAA.US", 28, "100.00", 60), lot("AAA.US", 30, "104.00", 10)],
     {"AAA": FLAT + [104.0, 110.0, 110.5, 111.0, 111.5, 118.0]})]
SCENARIOS["l12_dividend_newest_closed_next"] = SCENARIOS["l12_dividend_newest_closed"] + [
    (34, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10, "Closed")],
     {"AAA": P12 + [109.0]}, {"adj": ADJ12})]
# L25: L21 over a weekend: the Saturday run (key A) and the Sunday run after the sale (key B, gate off: 1.1.0
# lowers the stop) both read Friday's close; the gate is on from the Tuesday run
P25 = FLAT + [101.0, 110.0, 118.0, 108.0, 107.5]
W_A = [lot("AAA.US", 30, "100.00", 230), lot("AAA.US", 30, "100.30", 10)]
W_B = [lot("AAA.US", 30, "100.00", 110), lot("AAA.US", 30, "100.30", 10)]
WEEKEND = ([(31, 0, W_A, {"AAA": P25[:32]}), (32, 0, W_A, {"AAA": P25[:33]}), (33, 0, W_A, {"AAA": P25[:34]}),
            (33, 24, W_B, {"AAA": P25[:34]}), (34, 0, W_B, {"AAA": P25[:35]})], (None, None, None, None, "on"))
# L21: L2 played with the gate off (1.1.0 lowers the stop and drops TP1), then the gate switched on: (steps, gates)
ROLLOUT = ([(31, 0, [lot("AAA.US", 30, "100.00", 230), lot("AAA.US", 30, "100.30", 10)],
             {"AAA": FLAT + [101.0, 110.0]}),
            (32, 0, [lot("AAA.US", 30, "100.00", 230), lot("AAA.US", 30, "100.30", 10)],
             {"AAA": FLAT + [101.0, 110.0, 118.0]}),
            (33, 0, [lot("AAA.US", 30, "100.00", 110), lot("AAA.US", 30, "100.30", 10)],
             {"AAA": FLAT + [101.0, 110.0, 118.0, 108.0]}),
            (34, 0, [lot("AAA.US", 30, "100.00", 110), lot("AAA.US", 30, "100.30", 10)],
             {"AAA": FLAT + [101.0, 110.0, 118.0, 108.0, 107.5]})], (None, None, None, "on"))


@contextlib.contextmanager
def env(gate):
    saved = {k: v for k, v in os.environ.items() if k.startswith("TFB_EXIT_")}
    try:
        for k in saved:
            os.environ.pop(k, None)
        os.environ["TFB_EXIT_PLAN"] = "on"
        os.environ["TFB_EXIT_MAIL"] = "1"
        if gate is not None:
            os.environ[GATE] = gate
        yield
    finally:
        for k in [k for k in os.environ if k.startswith("TFB_EXIT_")]:
            os.environ.pop(k, None)
        os.environ.update(saved)


def play(mod, name, gate, gates=None, ledger=None):
    """Run a scenario (a SCENARIOS name, or a list of steps); return [(rc, stdout, ledger, page, mails)] after each
    step. `gates` = one gate value per step instead of `gate`; `ledger` = rows to seed _Exit_Plan with. A step's
    5th element may carry "adj" (dividend factors) and "tabs" ({tab: rows, or None to remove it})."""
    book = Book({"_Exit_Plan": Sheet([list(ep.LEDGER_HEADERS)] + ledger)} if ledger else {})
    out = []
    steps = SCENARIOS[name] if isinstance(name, str) else name
    for n, st in enumerate(steps):
        i, hours, lots, series = st[:4]
        extra = st[4] if len(st) > 4 else {}
        for tab, vals in (extra.get("tabs") or {}).items():
            if vals is None:
                book.tabs.pop(tab, None)
            else:
                book.tabs[tab] = Sheet(vals)
        book.tabs["_Portfolio_CostBasis"] = Sheet([LOT_HDR] + lots)
        mails = []
        buf = io.StringIO()
        with env(gates[n] if gates is not None else gate), contextlib.redirect_stdout(buf):
            rc = mod.run(False, True, book=book, now=morning_after(i, hours), opener=opener_for(series, extra.get(
                "adj")), mailer=lambda *a: mails.append(a))
        led = copy.deepcopy(book.tabs["_Exit_Plan"].values) if "_Exit_Plan" in book.tabs else None
        page = copy.deepcopy(book.tabs["Exit_Plan"].values) if "Exit_Plan" in book.tabs else None
        out.append((rc, buf.getvalue(), led, page, mails))
    return out


def rows(led, sym=None, day=None):
    h = ep.LEDGER_HEADERS
    return [dict(zip(h, r)) for r in led[1:] if (sym is None or r[2] == sym) and (day is None or r[0] == day)]


def last_row(led, sym):
    return rows(led, sym)[-1]


def num(x):
    return float(x) if str(x).strip() not in ("", "None") else None


def page_line(page, sym):
    return [r for r in page if r and r[0] == sym][0]


def fold(x):
    """The only bytes this exit_plan may differ in from main while the gate is off: its version token (any
    version, so a later gated release keeps this check)."""
    return re.sub(r"exit_plan \d+\.\d+\.\d+", "exit_plan V", json.dumps(x, sort_keys=True, default=str))


NOTE = "the lots changed (a sale or a closed lot)"


def ln(log, sym="AAA.US"):
    """The holding's log line."""
    return [x for x in log.splitlines() if "] %s " % sym in x and " last " in x][0]


def key_of(day_i, price):
    return "AAA.US|%s|%.4f" % (DAYS[day_i].isoformat(), price)


# ----------------------------------------------------------------------------- L0
def test_l0_version_floor_and_gate_words():
    assert tuple(int(x) for x in ep.__version__.split(".")) >= (1, 2, 0)
    for word, want in ((None, False), ("off", False), ("", False), ("no", False), ("0", False), ("enforce", False),
                       ("observe", False),
                       ("on", True), ("ON", True), (" on ", True), ("1", True), ("true", True), ("Yes", True)):
        with env(word):
            assert ep.lot_carry_on() is want, word


# ----------------------------------------------------------------------------- L1
def test_l1_no_second_take_profit_after_selling_half():
    on = play(ep, "l1_sell_half_from_one_lot", "on")
    off = play(ep, "l1_sell_half_from_one_lot", None)
    for steps in (on, off):
        r0 = last_row(steps[0][2], "AAA.US")
        assert r0["Action"] == "TAKE PROFIT" and r0["TP1 Reached"] == "yes"
        assert steps[0][4][0][0].endswith("1 action: AAA.US take profit")
    stop0 = num(last_row(on[0][2], "AAA.US")["Stop"])
    rc, log, led, page, mails = on[1]
    r1 = last_row(led, "AAA.US")
    assert rc == 0 and r1["Action"] != "TAKE PROFIT" and r1["TP1 Reached"] == "yes"
    assert r1["Lot"] != last_row(on[0][2], "AAA.US")["Lot"]                    # the key did change
    assert num(r1["Stop"]) >= stop0
    note = [ln for ln in log.splitlines() if "AAA.US" in ln and "the lots changed" in ln]
    assert len(note) == 1 and note[0].endswith(
        "the lots changed (a sale or a closed lot): the stop and the first-target state carry over from %s"
        % last_row(on[0][2], "AAA.US")["Lot"])
    assert mails[0][0].endswith("hold all") and "take profit" not in mails[0][0]
    assert "TAKE PROFIT" not in " ".join(str(c) for c in page_line(page, "AAA.US"))
    # 1.1.0 (off): the holding lost its history - 'sell half' again
    r1off = last_row(off[1][2], "AAA.US")
    assert r1off["Action"] == "TAKE PROFIT" and "take profit" in off[1][4][0][0]
    assert "the lots changed" not in off[1][1]
    # the other holding is untouched by the carry
    assert last_row(led, "BBB.US") == last_row(off[1][2], "BBB.US")
    # a one-share and a fractional sale: the smallest drops of the count still carry
    first, after = SCENARIOS["l1_sell_half_from_one_lot"]
    for left in ("229", "229.5"):
        one = play(ep, [first, (32, 0, [lot("AAA.US", 28, "100.00", left)] + after[2][1:], after[3])], "on")[1]
        r1 = last_row(one[2], "AAA.US")
        assert r1["Action"] != "TAKE PROFIT" and r1["TP1 Reached"] == "yes" and NOTE in ln(one[1]), left
        assert one[4][0][0].endswith("hold all"), left


# ----------------------------------------------------------------------------- L2
def test_l2_close_fills_off_lowers_the_stop_on_keeps_it():
    on = play(ep, "l2_close_fills_stop_drop", "on")
    off = play(ep, "l2_close_fills_stop_drop", None)
    assert [last_row(s[2], "AAA.US")["Action"] for s in on[:2]] == ["TAKE PROFIT", "RAISE STOP"]
    raised = num(last_row(on[1][2], "AAA.US")["Stop"])
    assert raised == num(last_row(off[1][2], "AAA.US")["Stop"])
    r_off = last_row(off[2][2], "AAA.US")
    assert num(r_off["Stop"]) < raised and r_off["Action"] == "RAISE STOP"      # 1.1.0: a lower stop, 'raised'
    assert r_off["TP1 Reached"] == "no"                                          # 1.1.0: the flag is lost too
    r_on = last_row(on[2][2], "AAA.US")
    assert num(r_on["Stop"]) == raised and r_on["Action"] == "HOLD" and r_on["TP1 Reached"] == "yes"


# ----------------------------------------------------------------------------- L3
def test_l3_newest_lot_closed_is_carried():
    on = play(ep, "l3_newest_lot_closed", "on")
    off = play(ep, "l3_newest_lot_closed", None)
    k0, k1 = last_row(on[0][2], "AAA.US")["Lot"], last_row(on[1][2], "AAA.US")["Lot"]
    assert k0.split("|")[1] == DAYS[30].isoformat() and k1.split("|")[1] == DAYS[28].isoformat()
    assert last_row(on[1][2], "AAA.US")["Action"] != "TAKE PROFIT"
    assert last_row(off[1][2], "AAA.US")["Action"] == "TAKE PROFIT"
    assert num(last_row(on[1][2], "AAA.US")["Stop"]) >= num(last_row(on[0][2], "AAA.US")["Stop"])


# ----------------------------------------------------------------------------- L4 / L5 / L6
@pytest.mark.parametrize("name", ["l4_top_up", "l5_sold_out_rebought", "l6_single_lot"])
def test_l4_l5_l6_nothing_to_carry_on_equals_off(name):
    on = play(ep, name, "on")
    off = play(ep, name, None)
    assert fold(on) == fold(off)
    assert all("the lots changed" not in s[1] for s in on)


def test_l4_a_top_up_stays_a_new_position():
    on = play(ep, "l4_top_up", "on")
    keys = [last_row(s[2], "AAA.US")["Lot"] for s in on]
    assert keys[0] != keys[1] == keys[2] and keys[1].split("|")[1] == DAYS[32].isoformat()


# ----------------------------------------------------------------------------- L7
def test_l7_same_close_rerun_after_the_change():
    on = play(ep, "l7_same_close_rerun", "on")
    first, second = last_row(on[1][2], "AAA.US"), last_row(on[2][2], "AAA.US")
    assert first["Action"] == second["Action"] != "TAKE PROFIT" and second["Action"] != "RAISE STOP"
    assert first["Stop"] == second["Stop"] and second["TP1 Reached"] == "yes"
    assert all("the lots changed" in s[1] for s in on[1:])          # no row on an earlier close under the new key yet
    assert ln(on[2][1]).endswith("carry over from %s" % key_of(30, 100.1667))      # the earlier key, not the own row
    assert len(rows(on[2][2], "AAA.US", morning_after(32).astimezone(ep.RIYADH).date().isoformat())) == 1


# ----------------------------------------------------------------------------- L8
@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_l8_off_identity_against_main(name):
    b = base()
    want = fold(play(b, name, None))
    for gate in (None, "off", "OFF ", "no", "0", "enforce", ""):
        assert fold(play(ep, name, gate)) == want, (name, gate)


# ----------------------------------------------------------------------------- L9
def test_l9_carry_rows():
    d28, d30, d31, d33 = DAYS[28], DAYS[30], DAYS[31], DAYS[33]
    k0, k1, k2 = ("AAA.US|%s|%s" % (DAYS[i].isoformat(), p) for i, p in ((30, "100.1667"), (30, "100.3333"),
                                                                           (30, "100.5714")))

    def r(key, bars_to=d31, shares="240", **extra):
        d = {"Symbol": key.split("|")[0], "Lot": key, "Stop": 100.0, "Bars To": bars_to.isoformat(), "Basis": 1.0,
             "Date": bars_to.isoformat(), "Shares": shares}
        d.update(extra)
        return d
    a0, a1 = r(k0), r(k0, bars_to=DAYS[32])
    led = [a0, a1, r("BBB.US|%s|50.0000" % d30.isoformat()), dict(a0, Lot="garbage"), dict(a0, Lot="AAA.US|x|1.0"),
           dict(a0, Lot="AAA.US|%s|abc" % d30.isoformat())]
    snapshot = copy.deepcopy(led)
    got = ep.carry_rows(led, "AAA.US", k1, d30, 120.0)
    assert got == [dict(a0, Lot=k1, **{"Carried From": k0}), dict(a1, Lot=k1, **{"Carried From": k0})]
    assert led == snapshot                                                        # the ledger is not mutated
    assert ep.carry_rows(led, "aaa.us", k1, d30, 120.0) == got                    # symbol case
    assert ep.carry_rows(led, "AAA.US", "AAA.US|%s|100.0000" % d28.isoformat(), d28, 230.0) != []   # newest closed
    # the three tests, on the previous key's last row
    assert ep.carry_rows(led, "AAA.US", k1, d30, 239.0) != []
    assert ep.carry_rows(led, "AAA.US", k1, d30, 240.0) == [] == ep.carry_rows(led, "AAA.US", k1, d30, 260.0)
    assert ep.carry_rows(led, "AAA.US", k1, d31, 120.0) == []                     # a lot newer than the key's
    assert ep.carry_rows([r(k0, bars_to=DAYS[29])], "AAA.US", k1, d30, 120.0) == []   # computed before it was bought
    assert ep.carry_rows([r(k0, bars_to=d30)], "AAA.US", k1, d30, 120.0) != []
    # inside its own newest lot's session, also when that lot is the one removed (the key date moves earlier)
    cur28 = "AAA.US|%s|100.0000" % d28.isoformat()
    assert ep.carry_rows([r(k0, bars_to=DAYS[29])], "AAA.US", cur28, d28, 230.0) == []
    assert ep.carry_rows([r(k0, bars_to=d30)], "AAA.US", cur28, d28, 230.0) != []
    assert ep.carry_rows([a0, dict(a1, Shares="")], "AAA.US", k1, d30, 120.0) == []
    assert ep.carry_rows([a0, dict(a1, **{"Bars To": ""})], "AAA.US", k1, d30, 120.0) == []
    # the shares right after the change: the current key's first trailing row, not the shares now
    own = r(k1, bars_to=DAYS[32], shares="240")
    assert ep.carry_rows([a0, own], "AAA.US", k1, d30, 100.0) == []               # an edit, then a sale later
    own = r(k1, bars_to=DAYS[32], shares="120")
    assert [c["Carried From"] for c in ep.carry_rows([a0, own, dict(own, Shares="100")], "AAA.US", k1, d30, 50.0)] \
        == [k0]
    # only the key just before: an older key is not carried directly (its state is in the previous key's rows)
    b0 = r(k2, bars_to=DAYS[32], shares="200")
    assert [c["Carried From"] for c in ep.carry_rows([a0, b0], "AAA.US", k1, d30, 120.0)] == [k2]
    assert ep.carry_rows([a0, b0, r(k0, bars_to=d33, shares="150")], "AAA.US", k1, d30, 120.0)[0]["Carried From"] \
        == k0
    # the lots returned to an earlier key exactly: its own rows apply, nothing carries
    assert ep.carry_rows([r(k1, shares="240"), a0], "AAA.US", k1, d30, 120.0) == []
    assert ep.carry_rows([r(k1, shares="240"), b0, a0], "AAA.US", k1, d30, 120.0) == []     # two keys back
    assert ep.carry_rows([], "AAA.US", k1, d30, 120.0) == [] == ep.carry_rows([r(k1)], "AAA.US", k1, d30, 120.0)
    assert ep.carry_rows(led, "CCC.US", k1, d30, 120.0) == []
    # with bars: the basis moves from the previous key's buy date to the current one (an ex-date on bar 30)
    bars = [(DAYS[i], 1.0, 1.0, 1.0, 1.0, 0.95 if i < 30 else 1.0) for i in range(34)]
    cur28 = "AAA.US|%s|100.0000" % d28.isoformat()
    c = ep.carry_rows([r(k0, Basis="1.0"), r(k0, Basis="")], "AAA.US", cur28, d28, 120.0, bars)
    assert [round(x["Basis"], 12) for x in c] == [0.95, 0.95]
    c = ep.carry_rows([r("AAA.US|%s|100.0000" % d28.isoformat(), Basis="0.5")], "AAA.US", k1, d28, 120.0, bars)
    assert abs(c[0]["Basis"] - 0.5) < 1e-12
    assert ep._factor_at(bars, DAYS[0] - dt.timedelta(days=1)) == 1.0 and ep._factor_at(bars, d28) == 0.95
    assert ep._factor_at(bars, d30) == 1.0 and ep._factor_at([], d30) == 1.0
    assert ep._factor_at([(d28, 1.0, 1.0, 1.0, 1.0)], d30) == 1.0
    # carry_last: the last run up to today (the latest Date, the last row on a tie), dividend-normalised
    t = DAYS[34]
    rl = [r(k0, bars_to=d31, Stop="100", Date=d31.isoformat()), r(k1, bars_to=d31, Stop="98", Basis="0.98",
                                                                   Date=DAYS[32].isoformat()),
          r(k0, bars_to=d31, Stop="120", Date=(t + dt.timedelta(days=1)).isoformat())]
    assert ep.carry_last(rl, t) == (98.0, 100.0) and ep.carry_last(rl[:1], t) == (100.0, 100.0)
    assert ep.carry_last(rl[:2] + [dict(rl[1], Stop="97")], t) == (97.0, 97.0 / 0.98)
    assert ep.carry_last([dict(rl[0], Stop="")], t) == (None, None) and ep.carry_last([], t) == (None, None)
    assert ep.carry_last(rl[2:], t) == (None, None)
    # read_lots is 1.1.0's
    assert "first" not in ep.read_lots([LOT_HDR, lot("AAA.US", 30, "104", 10), lot("AAA.US", 28, "100", 230)])["AAA.US"]


def test_l9_carry_was():
    c31, c32, c33, c34 = (DAYS[i].isoformat() for i in (31, 32, 33, 34))
    today = DAYS[34] + dt.timedelta(days=1)

    def r(close, stop, basis="1.0", date=None):
        return {"Date": date or close, "Bars To": close, "Stop": stop, "Basis": basis}
    rows_ = [r(c31, "100.31"), r(c32, "105.9036"), r(c33, "104.0564"), r(c34, "110")]
    assert ep.carry_was(rows_, today, DAYS[34]) == (104.0564, 104.0564)          # the last run
    assert ep.carry_was(rows_[:2], today, DAYS[34]) == (105.9036, 105.9036)
    assert ep.carry_was([r(c33, "104", "0.5"), r(c33, "103", "0.25")], today, DAYS[34]) == (104.0, 412.0)
    assert ep.carry_was([r(c34, "110")], today, DAYS[34]) == (None, None)        # only this close
    assert ep.carry_was([], today, DAYS[34]) == (None, None)
    assert ep.carry_was([r(c33, "")], today, DAYS[34]) == (None, None)          # no stop
    future = r(c33, "120", date=(today + dt.timedelta(days=1)).isoformat())
    assert ep.carry_was([r(c32, "100"), future], today, DAYS[34]) == (100.0, 100.0)
    # a later run that read an older close (a stale feed): the stop published last
    sat0, sun0 = (DAYS[33] + dt.timedelta(days=1)).isoformat(), (DAYS[33] + dt.timedelta(days=2)).isoformat()
    assert ep.carry_was([r(c33, "104", date=sat0), r(c32, "106", date=sun0)], today, DAYS[34]) == (106.0, 106.0)
    # two runs on the latest earlier close (a weekend): the later run's stop
    sat, sun = (DAYS[33] + dt.timedelta(days=1)).isoformat(), (DAYS[33] + dt.timedelta(days=2)).isoformat()
    wk = [r(c33, "104.0564", date=sun), r(c33, "105.9036", date=sat), r(c32, "106")]
    assert ep.carry_was(wk, today, DAYS[34]) == (104.0564, 104.0564)
    assert ep.carry_was(wk[1:2] + wk[:1], today, DAYS[34]) == (104.0564, 104.0564)
    assert ep.carry_was([r(c33, "104", date="junk"), r(c33, "103", date=sat)], today, DAYS[34]) == (103.0, 103.0)
    # without bars_to the calendar day of the run stands in for the close
    assert ep.carry_was([r(c32, "100", date=c32), r(c33, "101", date=c33)], DAYS[34], None) == (101.0, 101.0)


# ----------------------------------------------------------------------------- L11
def test_l11_top_up_then_sale():
    on, off = play(ep, "l11_top_up_then_sale_100", "on"), play(ep, "l11_top_up_then_sale_100", None)
    k1, k2 = key_of(33, 105.5), key_of(33, 106.1111)
    r_on, r_off = last_row(on[-1][2], "AAA.US"), last_row(off[-1][2], "AAA.US")
    assert last_row(on[-2][2], "AAA.US")["Lot"] == k1 and r_on["Lot"] == k2
    assert r_on["Action"] == "RAISE STOP" and r_off["Action"] == "HOLD"     # first commit: SELL at the old 100.30
    assert num(r_on["Stop"]) == num(r_off["Stop"]) and r_on["TP1 Reached"] == "no"
    stop_k1 = num(last_row(on[-2][2], "AAA.US")["Stop"])
    assert num(r_on["Stop"]) > stop_k1 and ("raise the stop %s -> %s" % (ep.fmt(stop_k1), ep.fmt(num(r_on["Stop"])))
                                            in ln(on[-1][1]))
    assert ln(on[-1][1]).endswith(NOTE + ": the stop and the first-target state carry over from %s" % k1)
    tp = play(ep, "l11_top_up_then_sale_117", "on")
    ref = play(ep, "l11_top_up_no_sale_117", "on")
    assert last_row(tp[-1][2], "AAA.US")["Action"] == last_row(ref[-1][2], "AAA.US")["Action"] == "TAKE PROFIT"
    assert tp[-1][4][0][0].endswith("1 action: AAA.US take profit")


# ----------------------------------------------------------------------------- L12
def test_l12_dividend_between_the_buy_dates():
    on, off = play(ep, "l12_dividend_newest_closed", "on"), play(ep, "l12_dividend_newest_closed", None)
    kept = play(ep, "l12_dividend_newest_kept", "on")
    before = num(last_row(on[1][2], "AAA.US")["Stop"])
    r_on = last_row(on[2][2], "AAA.US")
    assert r_on["Lot"] == key_of(28, 100.0) and num(r_on["Basis"]) == 0.95
    assert num(r_on["Stop"]) == before == num(last_row(kept[2][2], "AAA.US")["Stop"])
    assert r_on["Action"] == "HOLD" and r_on["TP1 Reached"] == "yes"
    assert "stop %s (was %s)" % (ep.fmt(before), ep.fmt(before)) in ln(on[2][1])
    page = page_line(on[2][3], "AAA.US")
    assert ep.fmt(before) in [str(c) for c in page]
    r_off = last_row(off[2][2], "AAA.US")
    assert num(r_off["Stop"]) < before and r_off["Action"] == "TAKE PROFIT"      # 1.1.0: history lost
    nxt = play(ep, "l12_dividend_newest_closed_next", "on")[3]
    assert last_row(nxt[2], "AAA.US")["Action"] == "HOLD" and NOTE not in nxt[1]
    assert num(last_row(nxt[2], "AAA.US")["Stop"]) == before


# ----------------------------------------------------------------------------- L13
def test_l13_same_day_round_trip_starts_fresh():
    on, off = play(ep, "l13_same_day_round_trip", "on"), play(ep, "l13_same_day_round_trip", None)
    ref = play(ep, "l13_fresh_reference", "on")
    assert fold(on) == fold(off) and all(NOTE not in s[1] for s in on)
    r = last_row(on[-1][2], "AAA.US")
    assert r["Action"] == "HOLD" and num(r["Stop"]) == num(last_row(ref[-1][2], "AAA.US")["Stop"])
    assert num(r["Stop"]) < num(last_row(on[0][2], "AAA.US")["Stop"])                  # not the old 99.45


# ----------------------------------------------------------------------------- L14
def test_l14_take_profit_under_the_new_key_then_the_next_close():
    on = play(ep, "l14_tp_after_change_next_close", "on")
    acts = [last_row(s[2], "AAA.US")["Action"] for s in on]
    assert acts[2] == "TAKE PROFIT" and acts[3] == "HOLD"
    assert NOTE in ln(on[1][1]) and NOTE not in ln(on[3][1])
    assert num(last_row(on[3][2], "AAA.US")["Stop"]) >= num(last_row(on[2][2], "AAA.US")["Stop"])
    assert on[3][4][0][0].endswith("hold all")


# ----------------------------------------------------------------------------- L15
def test_l15_same_date_newer_close_keeps_the_stop():
    on = play(ep, "l15_same_date_new_close", "on")
    first, second = last_row(on[1][2], "AAA.US"), last_row(on[2][2], "AAA.US")
    assert first["Action"] == "RAISE STOP" and second["Bars To"] > first["Bars To"]
    assert num(second["Stop"]) == num(first["Stop"]) and second["Action"] == "HOLD"
    assert len(rows(on[2][2], "AAA.US", first["Date"])) == 1


# ----------------------------------------------------------------------------- L16
def test_l16_weekend_rerun_keeps_take_profit():
    on, ref = play(ep, "l16_weekend_same_close", "on"), play(ep, "l16_weekend_one_key", "on")
    assert [last_row(s[2], "AAA.US")["Action"] for s in on[1:]] == ["TAKE PROFIT", "TAKE PROFIT"]
    assert [last_row(s[2], "AAA.US")["Action"] for s in ref[1:]] == ["TAKE PROFIT", "TAKE PROFIT"]
    assert all(s[4][0][0].endswith("1 action: AAA.US take profit") for s in on[1:])


# ----------------------------------------------------------------------------- L17
def test_l17_rebought_in_two_lots_then_sale():
    on, off = play(ep, "l17_rebought_two_lots_then_sale", "on"), play(ep, "l17_rebought_two_lots_then_sale", None)
    r_on, r_off = last_row(on[-1][2], "AAA.US"), last_row(off[-1][2], "AAA.US")
    assert (r_on["Stop"], r_on["TP1 Reached"]) == (r_off["Stop"], "no")
    assert num(r_on["Stop"]) < num(last_row(on[0][2], "AAA.US")["Stop"])            # not the old 100.47
    assert ln(on[-1][1]).endswith("carry over from %s" % last_row(on[1][2], "AAA.US")["Lot"])


# ----------------------------------------------------------------------------- L18
def test_l18_closed_then_reactivated_with_a_sale():
    on, off = play(ep, "l18_closed_reactivated_sale", "on"), play(ep, "l18_closed_reactivated_sale", None)
    assert last_row(on[1][2], "AAA.US")["Action"] == "HOLD" and NOTE in ln(on[1][1])    # the close: carried
    # the re-activated lot is newer than the key in between: it reads like a lot bought since (as 1.1.0)
    assert fold(on[2][1]) == fold(off[2][1]) and fold(last_row(on[2][2], "AAA.US")) == fold(
        last_row(off[2][2], "AAA.US"))


# ----------------------------------------------------------------------------- L19
def test_l19_note_names_the_key_just_before_then_goes():
    on = play(ep, "l19_two_sales_then_close", "on")
    keys = [last_row(s[2], "AAA.US")["Lot"] for s in on]
    assert len(set(keys[:3])) == 3 and keys[3] == keys[2]
    assert ln(on[1][1]).endswith("carry over from %s" % keys[0])
    assert ln(on[2][1]).endswith("carry over from %s" % keys[1])
    assert NOTE not in on[3][1] and NOTE not in json.dumps(on[3][3]) and NOTE not in json.dumps(on[3][4])
    assert NOTE in json.dumps(on[2][3]) and NOTE in json.dumps(on[2][4])                 # page and e-mail
    assert [last_row(s[2], "AAA.US")["Action"] for s in on] == ["TAKE PROFIT", "HOLD", "HOLD", "HOLD"]


# ----------------------------------------------------------------------------- L20
def test_l20_carried_check_has_no_note():
    on = play(ep, "l20_carried_check", "on")
    line = ln(on[1][1])
    assert "CHECK" in line and "no daily price history" in line and NOTE not in on[1][1]


# ----------------------------------------------------------------------------- L21
def test_l21_gate_switched_on_after_a_change_made_while_off():
    steps, gates = ROLLOUT
    on = play(ep, steps, None, gates)
    low, high = num(last_row(on[2][2], "AAA.US")["Stop"]), num(last_row(on[1][2], "AAA.US")["Stop"])
    assert low < high and last_row(on[2][2], "AAA.US")["TP1 Reached"] == "no"         # 1.1.0 on step 3
    r = last_row(on[3][2], "AAA.US")
    assert num(r["Stop"]) == low and r["TP1 Reached"] == "yes" and r["Action"] == "HOLD"
    assert "stop %s (was %s)" % (ep.fmt(low), ep.fmt(low)) in ln(on[3][1]) and NOTE not in ln(on[3][1])
    assert on[3][4][0][0].endswith("hold all")
    stay = play(ep, steps, None, (None, None, None, None))
    assert num(last_row(stay[3][2], "AAA.US")["Stop"]) == low and last_row(stay[3][2], "AAA.US")["TP1 Reached"] == "no"


# ----------------------------------------------------------------------------- L22
def test_l22_late_top_up_is_a_new_position():
    for late, ref in (("l22_late_top_up", "l22_on_time_top_up"),):
        on, off, on_time = play(ep, late, "on"), play(ep, late, None), play(ep, ref, "on")
        assert fold(on) == fold(off) and all(NOTE not in st[1] for st in on)
        r = last_row(on[-1][2], "AAA.US")
        assert (r["Lot"], r["Stop"], r["TP1 Reached"], r["Action"]) == tuple(
            last_row(on_time[-1][2], "AAA.US")[k] for k in ("Lot", "Stop", "TP1 Reached", "Action"))
        assert r["TP1 Reached"] == "no" and num(r["Stop"]) < num(last_row(on[1][2], "AAA.US")["Stop"])
    on, off = play(ep, "l22_late_top_up_same_date", "on"), play(ep, "l22_late_top_up_same_date", None)
    assert fold(on) == fold(off) and last_row(on[1][2], "AAA.US")["TP1 Reached"] == "no"


# ----------------------------------------------------------------------------- L23
def test_l23_late_round_trip_starts_fresh():
    on, off = play(ep, "l23_late_round_trip", "on"), play(ep, "l23_late_round_trip", None)
    assert fold(on) == fold(off) and all(NOTE not in st[1] for st in on)
    old, new = last_row(on[0][2], "AAA.US"), last_row(on[-1][2], "AAA.US")
    assert old["Bars To"] == DAYS[31].isoformat() and new["Lot"] == key_of(31, 100.0)
    assert new["Action"] == "HOLD" and num(new["Stop"]) < 100.0 < num(old["Stop"])


# ----------------------------------------------------------------------------- L24
def test_l24_top_up_lot_closed_keeps_the_top_up_position():
    on, off = play(ep, "l24_top_up_lot_closed", "on"), play(ep, "l24_top_up_lot_closed", None)
    top = last_row(on[4][2], "AAA.US")
    assert top["Lot"] == key_of(33, 107.06) and top["TP1 Reached"] == "no"
    r = last_row(on[5][2], "AAA.US")
    assert r["Lot"] == key_of(28, 100.0) and r["TP1 Reached"] == "no" and r["Action"] == "RAISE STOP"
    assert num(r["Stop"]) == num(last_row(off[5][2], "AAA.US")["Stop"]) > num(top["Stop"])
    assert "raise the stop %s -> %s" % (ep.fmt(num(top["Stop"])), ep.fmt(num(r["Stop"]))) in ln(on[5][1])
    assert ln(on[5][1]).endswith("carry over from %s" % key_of(33, 107.06))
    # only the top-up lot closed: the key returns exactly, its own rows apply (on equals off)
    on, off = play(ep, "l24_top_up_only_closed", "on"), play(ep, "l24_top_up_only_closed", None)
    assert last_row(on[5][2], "AAA.US")["Lot"] == key_of(28, 100.12)
    assert fold(on[5][1]) == fold(off[5][1]) and fold(on[5][2]) == fold(off[5][2])


# ----------------------------------------------------------------------------- L25
def test_l25_weekend_rollout_holds_the_stop_published_last():
    steps, gates = WEEKEND
    on = play(ep, steps, None, gates)
    sat, sun = last_row(on[2][2], "AAA.US"), last_row(on[3][2], "AAA.US")
    assert sat["Bars To"] == sun["Bars To"] and num(sun["Stop"]) < num(sat["Stop"])
    r = last_row(on[4][2], "AAA.US")
    assert r["Action"] == "HOLD" and num(r["Stop"]) == num(sun["Stop"]) and r["TP1 Reached"] == "yes"
    assert "(was %s)" % ep.fmt(num(sun["Stop"])) in ln(on[4][1])


# ----------------------------------------------------------------------------- L26
def test_l26_same_close_rerun_keeps_the_published_stop():
    on = play(ep, "l26_same_close_rerun_tightened", "on")
    first, second = last_row(on[1][2], "AAA.US"), last_row(on[2][2], "AAA.US")
    assert "trail tightened" in ln(on[1][1]) and "trail tightened" not in ln(on[2][1])
    assert first["Action"] == second["Action"] == "RAISE STOP" and second["Stop"] == first["Stop"]
    assert NOTE in ln(on[1][1]) and NOTE in ln(on[2][1])
    assert ln(on[2][1]).endswith("carry over from %s" % key_of(30, 100.1667))


# ----------------------------------------------------------------------------- L27
def _seeded(old_stop, new_stop="100.0", new_basis="1.0", same=None, adj=None,
            closes=(101.0, 102.0, 103.0, 105.0)):
    k_old, k_new = key_of(30, 100.1667), key_of(30, 100.3333)

    def row(i, key, stop, shares, basis="1.0", hour="07:30"):
        run = (DAYS[i] + dt.timedelta(days=1)).isoformat()
        return [run, run + " " + hour, "AAA.US", key, "100.2", shares, "100", "100", "1.5", "8", stop, "108", "no",
                "HOLD", "", DAYS[i].isoformat(), basis]
    led = [row(31, k_old, old_stop, "240"), row(32, k_new, new_stop, "120", new_basis)]
    if same is not None:                                   # this close's 07:30 row: the run is a re-run
        led.append(row(33, k_new, same, "120"))
    extra = {"adj": {"AAA": adj}} if adj else {}
    return play(ep, [(33, 0 if same is None else 1, A_SOLD, {"AAA": FLAT + list(closes)}, extra)], "on",
                ledger=led)[0]


def test_l27_cap_and_threshold():
    # an earlier key's higher stop never comes back above the 100.00 published last; 'was' is not above the stop
    for old in ("100.20", "100.25", "100.30", "105.00"):
        st = _seeded(old)
        r = last_row(st[2], "AAA.US")
        assert (r["Action"], num(r["Stop"])) == ("HOLD", 100.0), old
        assert "stop 100.00 (was 100.00)" in ln(st[1]) and NOTE not in ln(st[1]), old
    # a re-run of the close: the stop this close already published holds; 'was' is the earlier close's own stop when
    # that is within 0.25% of the earlier key's, else the stop published on the last earlier run
    for old, was in (("100.20", "100.20"), ("100.25", "100.25"), ("100.255", "100.00"), ("100.30", "100.00")):
        st = _seeded(old, same="100.40")
        r = last_row(st[2], "AAA.US")
        assert num(r["Stop"]) == 100.4 and "(was %s)" % was in ln(st[1]), old
    # a dividend between the earlier key's stop (basis 1.0) and the stop published last (95.00 on basis 0.95 =
    # 100.00): the cap compares the normalised stops
    st = _seeded("100.20", new_stop="95.0", new_basis="0.95", adj=[0.95] * 34)
    assert last_row(st[2], "AAA.US")["Action"] == "HOLD" and "stop 95.00 (was 95.00)" in ln(st[1])


# ----------------------------------------------------------------------------- L28
def test_l28_rollout_with_a_dividend_factor():
    steps, gates = ROLLOUT
    on = play(ep, [st[:4] + ({"adj": {"AAA": [0.95] * 40}},) for st in steps], None, gates)
    low = num(last_row(on[2][2], "AAA.US")["Stop"])
    r = last_row(on[3][2], "AAA.US")
    assert r["Action"] == "HOLD" and num(r["Stop"]) == low and r["TP1 Reached"] == "yes"
    # L19 on a factor not exact at 4 decimals: the note still goes on the close after the change
    nxt = play(ep, [st[:4] + ({"adj": {"AAA": [F29] * 40}},) for st in SCENARIOS["l19_two_sales_then_close"]], "on")
    assert NOTE in ln(nxt[2][1]) and NOTE not in nxt[3][1]


# ----------------------------------------------------------------------------- L29
def test_l29_dividend_factor_single_lot_on_equals_off():
    on, off = play(ep, "l29_single_lot_dividend", "on"), play(ep, "l29_single_lot_dividend", None)
    assert fold(on) == fold(off) and all(NOTE not in st[1] for st in on)


# ----------------------------------------------------------------------------- L30
def test_l30_sale_between_runs_on_one_close():
    on = play(ep, "l30_sale_between_runs_on_one_close", "on")
    assert [st[0] for st in on] == [0, 0, 0]
    first, rerun, nxt = (last_row(st[2], "AAA.US") for st in on)
    assert first["Bars To"] == rerun["Bars To"] and rerun["Lot"] != first["Lot"]
    assert first["Action"] == rerun["Action"] == "TAKE PROFIT" and "(issued on the close of" in ln(on[1][1])
    assert ln(on[1][1]).endswith("carry over from %s" % first["Lot"])
    assert nxt["Action"] == "HOLD" and NOTE not in on[2][1] and num(nxt["Stop"]) >= num(rerun["Stop"])


# ----------------------------------------------------------------------------- L31
def test_l31_edits_that_are_not_sales():
    for name in ("l31_price_typo_then_sales", "l31_bonus_then_sale"):
        on, off = play(ep, name, "on"), play(ep, name, None)
        assert fold(on) == fold(off) and all(NOTE not in st[1] for st in on), name
    sr = play(ep, "l31_bonus_then_sale", "on")
    assert [last_row(st[2], "2222.SR")["Action"] for st in sr[2:]] == ["HOLD", "HOLD"]     # not SELL at 50.15
    on, off = play(ep, "l31_split_then_sale", "on"), play(ep, "l31_split_then_sale", None)
    pre, post, r = (last_row(st[2], "AAA.US") for st in on)
    assert num(pre["Stop"]) > 150 > num(r["Stop"]) == num(post["Stop"])                  # the post-split key's stop
    assert r["Action"] == "HOLD" and r["TP1 Reached"] == "yes"
    assert ln(on[2][1]).endswith("carry over from %s" % post["Lot"])
    assert last_row(off[2][2], "AAA.US")["Action"] == "TAKE PROFIT"


# ----------------------------------------------------------------------------- L32
def test_l32_mistyped_lot_deleted_or_closed():
    for name in ("l32_mistyped_lot_deleted", "l32_mistyped_lot_closed"):
        on, off = play(ep, name, "on"), play(ep, name, None)
        assert fold(on) == fold(off) and all(NOTE not in st[1] for st in on), name
        assert last_row(on[-1][2], "AAA.US")["Action"] == "HOLD", name
        assert any(last_row(st[2], "AAA.US")["Action"] == "SELL" for st in on), name      # the typo's own run


# ----------------------------------------------------------------------------- L33
def test_l33_corrected_sale_starts_fresh():
    on, off = play(ep, "l33_corrected_sale", "on"), play(ep, "l33_corrected_sale", None)
    assert NOTE in ln(on[1][1]) and last_row(on[1][2], "AAA.US")["Action"] == "RAISE STOP"
    assert fold(on[2][1]) == fold(off[2][1]) and NOTE not in on[2][1]


# ----------------------------------------------------------------------------- L34
def test_l34_raise_close_after_carried_sales_has_no_note():
    on = play(ep, "l34_raise_close_two_runs", "on")
    assert all(NOTE not in st[1] for st in on[3:])
    a, b = last_row(on[4][2], "AAA.US"), last_row(on[5][2], "AAA.US")
    assert a["Action"] == b["Action"] == "RAISE STOP" and a["Stop"] == b["Stop"] and a["Bars To"] == b["Bars To"]


# ----------------------------------------------------------------------------- L35
def test_l35_same_date_edits_never_publish_an_unpublished_stop():
    for name in ("l35_typo_then_sale_same_date", "l35_fresh_key_lot_removed"):
        on, off = play(ep, name, "on"), play(ep, name, None)
        assert fold(on[-1][1]) == fold(off[-1][1]) and fold(on[-1][2]) == fold(off[-1][2]), name
        assert last_row(on[-1][2], "AAA.US")["Action"] != "SELL", name
    on = play(ep, "l35_split_then_sale_same_date", "on")
    post = num(last_row(on[1][2], "AAA.US")["Stop"])
    assert all(last_row(st[2], "AAA.US")["Action"] != "SELL" for st in on[1:])
    assert all(num(last_row(st[2], "AAA.US")["Stop"]) == post for st in on[1:])          # the post-split key's
    on, ref = play(ep, "l35_exact_return_then_sale", "on"), play(ep, "l35_control_no_typo", "on")
    assert last_row(on[2][2], "AAA.US")["Action"] == "SELL"                              # the typo's own run
    for a, b in zip(on[3:], ref[3:]):
        ra, rb = last_row(a[2], "AAA.US"), last_row(b[2], "AAA.US")
        assert (ra["Action"], ra["Stop"], ra["TP1 Reached"]) == (rb["Action"], rb["Stop"], rb["TP1 Reached"])
    assert on[-1][4][0][0] == ref[-1][4][0][0]


# ----------------------------------------------------------------------------- L10
def test_l10_workflow_manifest_ci():
    raw = (ROOT / ".github/workflows/exit_plan.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(raw)
    step = [s for s in wf["jobs"]["exit_plan"]["steps"] if "exit_plan.py" in str(s.get("run", ""))][0]
    assert step["env"][GATE] == "${{ vars.TFB_EXIT_LOT_CARRY || 'off' }}"
    src = Path(DELIVERED).read_text(encoding="utf-8")
    assert '"%s"' % GATE in src
    manifest = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("exit_plan", "%s", "exit plan")' % ep.__version__ in manifest
    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    hits = [(j, s) for j, job in ci["jobs"].items() for s in job.get("steps", [])
            if "tests/test_exit_plan_lot_carry.py" in str(s.get("run", ""))]
    assert len(hits) == 1 and hits[0][0] == "lean-unit"
    s = hits[0][1]
    assert str((s.get("env") or {}).get("TFB_TEST_REQUIRE_BASE")) == "1" and BASE_SHA in s["run"]
    assert not re.search(r"create_order|place_order|requests\.post|cancel_order", src)


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = skipped = total = 0
    for name, fn in tests:
        params = [m for m in getattr(fn, "pytestmark", []) if m.name == "parametrize"]
        calls = ([((p,), "%s[%s]" % (name, p)) for p in params[0].args[1]] if params else [((), name)])
        for args, label in calls:
            total += 1
            try:
                fn(*args)
                passed += 1
                print("ok  ", label)
            except pytest.skip.Exception as exc:
                skipped += 1
                print("SKIP", label, "::", exc)
            except Exception as exc:  # noqa: BLE001
                print("FAIL", label, "::", type(exc).__name__, str(exc)[:300])
    print("PASS %d/%d" % (passed, total) + (" (skipped %d)" % skipped if skipped else ""))
    sys.exit(0 if passed + skipped == total else 1)
