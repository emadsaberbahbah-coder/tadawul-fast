#!/usr/bin/env python3
"""tests/test_daily_plan_settle_prev.py - scripts/daily_plan.py 1.0.2 [SETTLE-PREV].

A pick the owner buys in the US session (17:30-24:00 Riyadh) is bought after
the last scheduled refresh of its plan day (08:40 / 12:40 / 16:40 Riyadh), and
run_once never re-statuses an older plan day. 1.0.1 therefore leaves that
row OPEN for good, and the next day's Stability line counts the bought name
as 'dropped'. With TFB_PLAN_SETTLE_PREV on, each run marks EXECUTED the
newest previous plan's open picks that were bought since that plan, and the
Stability line counts against every pick of that plan (', N executed').

Runs as a script ("PASS k/k") and under pytest. Zero network: run_once reads
and writes an in-memory fake workbook. Controls: TFB_TEST_SETTLE_TARGET (the
daily_plan.py under test; the untouched main copy there is the golden
negative), TFB_TEST_PLAN_BASE (a path to the main 50f55740 copy instead of
`git show`), TFB_TEST_REQUIRE_BASE=1 (a missing base FAILS the base legs
instead of skipping them; set in CI).

  S0  version floor; the gate words (unset and anything else read off)
  S1  the 10 Nov shape: NMM.US bought on its plan day, next run 11 Nov 08:40:
      on -> the 10 Nov row reads EXECUTED 'bought 2026-11-10 (settled
      2026-11-11)', the page '2 kept, 1 new, 0 dropped, 1 executed', one
      SETTLE-PREV log line; today's picks and levels equal the off run
  S2  OFF IDENTITY: every scenario below, gate unset / off / junk, against the
      untouched 1.0.1 module: ledger, page, return codes and every log line
  S3  an unbought previous pick keeps its status, reason, stamp and last
      price even through its stop (no past-day INVALIDATED); a lot bought
      before the plan date is not settled; a sold lot is not settled
  S4  terminal rows: INVALIDATED is not re-labelled; a same-day EXECUTED row
      is unchanged and counted 'executed'
  S5  rows older than the newest previous plan date are byte-equal
  S6  bought today and picked again today: the previous row is not settled,
      today's row records the purchase, the name stays 'kept'
  S7  lot entered after today's cut (bought on the plan day, picked again
      today): the previous row is settled and the name stays 'kept'; the
      next day the carried pick settles too (its run of PICK days starts on
      the buy date) and the page reads 0 dropped, 1 executed
  S8  bought today, not picked today: settled 'bought <today>'
  S9  a run after Riyadh midnight cuts the next day and settles
  S10 observe and --dry-run settle in memory, print the line, write nothing
  S11 the counts: kept + dropped + executed = the previous plan's picks and
      kept + new = today's picks, in every on scenario
  S11b the counts read only the previous plan's rows
  S12 no previous plan: on equals off (ledger, page, log)
  S13 the workflow maps the gate (default 'off'), the manifest pins the
      version, ci.yml runs this file with the base fetched by SHA
  S14 two names settle in one run: ledger order and the exact log line
  S15 a bought pick left at CHECK (an unverified cut, as on 10 Nov) or at
      WAIT settles; the settle writes the three status cells and nothing else
  S16 the run of PICK days, directly on settle_prev: a three-day run reaches
      back to its first day; a day the name was not picked (absent, or only
      in the pipeline) ends the run; the walk goes newest first

Run: python tests/test_daily_plan_settle_prev.py  |  pytest -q tests/test_daily_plan_settle_prev.py
"""
from __future__ import annotations

import contextlib
import copy
import datetime as dt
import importlib.util
import io
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

BASE_SHA = "50f55740b993254b5e77d395ae12c18e543ff1b4"     # main: daily_plan 1.0.1 (#953)
GATE = "TFB_PLAN_SETTLE_PREV"
DELIVERED = os.getenv("TFB_TEST_SETTLE_TARGET", "").strip() or str(ROOT / "scripts" / "daily_plan.py")
UTC = dt.timezone.utc


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


dp = _load("dp_settle_prev", DELIVERED)
_BASE = {}


def base():
    """The untouched main copy (50f55740, daily_plan 1.0.1), or skip (or fail when TFB_TEST_REQUIRE_BASE=1)."""
    if "mod" not in _BASE:
        path = os.getenv("TFB_TEST_PLAN_BASE", "").strip()
        if not path:
            try:
                text = subprocess.run(["git", "show", BASE_SHA + ":scripts/daily_plan.py"], cwd=ROOT,
                                      capture_output=True, text=True, check=True).stdout
            except Exception:
                text = ""
            if text:
                tmp = Path(tempfile.mkdtemp()) / "daily_plan_v101_main.py"
                tmp.write_text(text, encoding="utf-8")
                path = str(tmp)
        _BASE["mod"] = _load("dp_settle_prev_base", path) if path else None
    if _BASE["mod"] is None:
        if os.getenv("TFB_TEST_REQUIRE_BASE", "").strip() == "1":
            raise AssertionError("base copy required (TFB_TEST_REQUIRE_BASE=1) but unavailable")
        pytest.skip("base copy unavailable (set TFB_TEST_PLAN_BASE)")
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
    def __init__(self, rows, row_count=200, col_count=30):
        self.values = copy.deepcopy(rows)
        self.row_count, self.col_count = row_count, col_count

    def get(self, range_name):
        return copy.deepcopy(self.values)

    def batch_get(self, ranges):
        out = []
        for r in ranges:
            m = re.match(r"([A-Z]+)(\d+):([A-Z]+)$", r)
            c, start = _colnum(m.group(1)) - 1, int(m.group(2)) - 1
            out.append([[row[c] if c < len(row) else ""] for row in self.values[start:]])
        return out

    def resize(self, rows=None, cols=None):
        self.row_count = rows or self.row_count
        self.col_count = cols or self.col_count

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
        self.tabs[title] = Sheet([], rows, cols)
        return self.tabs[title]


QH = ["Rank", "Symbol", "Name", "Market", "Sector", "ROI % (TP1)", "Engine ROI % (12M)", "Ann ROI %", "R/R (TP2)",
      "Rel", "DQ", "Risk", "Score", "Selected", "Why Not Selected"]
HELD1 = "Held target (analyst_lkg:1h) - no new money until the provider leg returns"
CONF2 = "stability: awaiting confirmation (2/3 days) - qualified on merit, seat withheld until confirmed"


def qrow(rank, sym, sector, roi, why=""):
    return [rank, sym, sym + " Inc", "NYSE/NASDAQ", sector, "", "%s%%" % roi, "", "", 71.5, 100, "Low",
            80 - rank, "No", why]


QUAL = [qrow(1, "ITRN.US", "Technology", 34.7, HELD1), qrow(2, "TEN.US", "Energy", 34.3, CONF2),
        qrow(3, "ZTO.US", "Industrials", 32.4, HELD1), qrow(4, "NMM.US", "Industrials", 29.9, HELD1),
        qrow(5, "SBLK.US", "Industrials", 27.2, HELD1), qrow(6, "LADR.US", "Real Estate", 29.5, CONF2),
        qrow(7, "BBOX.L", "Real Estate", 31.2), qrow(8, "AX.US", "Financials", 25.3, CONF2)]
SRC = [["Symbol", "Name", "Currency", "Sector", "Current Price", "Warnings"],
       ["ITRN.US", "Ituran", "USD", "Technology", "51.82", ""], ["TEN.US", "Tsakos", "USD", "Energy", "25.00", ""],
       ["ZTO.US", "ZTO", "USD", "Industrials", "20.66", ""], ["NMM.US", "Navios", "USD", "Industrials", "60.00", ""],
       ["SBLK.US", "Star Bulk", "USD", "Industrials", "27.07", ""], ["LADR.US", "Ladder", "USD", "Real Estate", "11.00", ""],
       ["BBOX.L", "Tritax", "GBX", "Real Estate", "147.70", ""], ["AX.US", "Axos", "USD", "Financials", "80.00", ""]]
CAL = [["Symbol", "Next Earnings Date", "Days To Earnings", "Next Ex-Div Date", "Days To ExDiv"],
       ["ITRN.US", "2026-11-24", 14, "", ""], ["TEN.US", "2026-12-01", 21, "", ""], ["ZTO.US", "2026-11-17", 7, "", ""],
       ["NMM.US", "2026-12-10", 30, "", ""], ["SBLK.US", "2026-11-12", 2, "", ""], ["LADR.US", "2027-01-20", 71, "", ""],
       ["AX.US", "2027-01-28", 79, "", ""]]
LOTS = [["_Portfolio_CostBasis"], ["Status:"], ["legend"],
        ["Symbol", "Name", "Ccy", "Status", "Buy Date", "Sell Date", "Market Value SAR"],
        ["5023.SR", "Sukuk", "SAR", "Active", "2025-11-23", "", "9,900"],
        ["DDI.US", "DoubleDown", "USD", "Active", "2026-11-02", "", "12,256"],
        ["GLPI.US", "GLPI", "USD", "Active", "2026-11-02", "", "34,117"],
        ["FRST.US", "Primis", "USD", "Inactive", "2026-10-26", "2026-11-04", ""]]


def line(day, hhmm="09:38", verified=True):
    if not verified:
        return "Last run %s %s:18 | status: ok | output: WITHHELD | source readiness: partial (run 1)" % (day, hhmm)
    return ("Last run %s %s:18 | status: ok | output: QUALIFIED_UNFUNDED | source readiness VERIFIED (run 1)"
            % (day, hhmm))


def board(day, qual=None, cash="33,125 SAR", hhmm="09:38", verified=True):
    qual = QUAL if qual is None else qual
    return ([["TOP 10 INVESTMENTS - DECISION"], ["Status:", line(day, hhmm, verified)], ["CONTROL PANEL"],
             ["T10: Universe Scope", "All Main Sheets", "", "T10: Max Selected", 10],
             ["T10: Allow Negative Sector", "No", "", "T10: Max Per Sector", 2],
             ["T10: Include Portfolio Holdings", "No", "", "T10: Base Currency", "SAR", "",
              "Cash Available (SAR)", cash],
             [], ["ALL QUALIFIED - INVEST opportunity set (%d)" % len(qual)], QH] + qual + [[], ["NEAR MISS"]])


def lot(sym, buy, status="Active", sell="", mv="8,000"):
    return [sym, sym, "USD", status, buy, sell, mv]


def at(day, hhmm):
    """A Riyadh wall-clock time as the UTC datetime run_once receives."""
    y, m, d = (int(x) for x in day.split("-"))
    h, mi = (int(x) for x in hhmm.split(":"))
    return dt.datetime(y, m, d, h, mi, tzinfo=dt.timezone(dt.timedelta(hours=3))).astimezone(UTC)


def new_book():
    return Book({"Top_10_Investments": Sheet(board("2026-11-10")), "Global_Markets": Sheet(SRC),
                 "Market_Leaders": Sheet([["x"]]), "Calendar_Events": Sheet(CAL),
                 "_Portfolio_CostBasis": Sheet(LOTS)})


@contextlib.contextmanager
def env(gate, mode="on"):
    saved = {k: v for k, v in os.environ.items() if k.startswith("TFB_PLAN_") or k == "TFB_DAILY_PLAN"}
    try:
        for k in saved:
            os.environ.pop(k, None)
        os.environ["TFB_DAILY_PLAN"] = mode
        if gate is not None:
            os.environ[GATE] = gate
        yield
    finally:
        for k in [k for k in os.environ if k.startswith("TFB_PLAN_") or k == "TFB_DAILY_PLAN"]:
            os.environ.pop(k, None)
        os.environ.update(saved)


def run(mod, book, now, gate, argv=(), mode="on"):
    buf = io.StringIO()
    with env(gate, mode), contextlib.redirect_stdout(buf):
        rc = mod.main(list(argv), book=book, now=now)
    return rc, buf.getvalue()


# ----------------------------------------------------------------------------- scenarios
# Each step: (riyadh day, riyadh hh:mm, edits applied to the book before the run, argv, TFB_DAILY_PLAN).
def _set(tab, rows):
    def f(b):
        b.tabs[tab] = Sheet(rows)
    return f


def _lots(*extra):
    return _set("_Portfolio_CostBasis", LOTS + [list(r) for r in extra])


def _board(day, qual=None, hhmm="08:20", verified=True):
    return _set("Top_10_Investments", board(day, qual, hhmm=hhmm, verified=verified))


def _src(**prices):
    rows = [list(r) for r in SRC]
    for r in rows[1:]:
        if r[0] in prices:
            r[4] = prices[r[0]]
    return _set("Global_Markets", rows)


DAY1 = [("2026-11-10", "10:00", [], (), "on"), ("2026-11-10", "12:40", [], (), "on"),
        ("2026-11-10", "16:40", [], (), "on")]
Q2_NMM9 = [qrow(1, "ITRN.US", "Technology", 34.7), qrow(2, "TEN.US", "Energy", 34.3),
           qrow(6, "LADR.US", "Real Estate", 29.5), qrow(9, "NMM.US", "Industrials", 29.9)]

SCENARIOS = {
    # S1: NMM.US bought in the 10 Nov US session; the next run is 11 Nov 08:40
    "s1_bought_on_plan_day": DAY1 + [
        ("2026-11-11", "08:40", [_lots(lot("NMM.US", "2026-11-10")), _board("2026-11-11")], (), "on"),
        ("2026-11-11", "12:40", [], (), "on")],
    # S3: nothing bought; TEN.US falls through its stop overnight; ITRN.US lot dated before the plan; NMM sold
    "s3_unbought_and_old_lots": DAY1 + [
        ("2026-11-11", "08:40", [_lots(lot("ITRN.US", "2026-11-09"),
                                       lot("NMM.US", "2026-11-10", "Inactive", "2026-11-10", "")),
                                 _board("2026-11-11"), _src(**{"TEN.US": "20.00"})], (), "on")],
    # S4: TEN through its stop on its own day (INVALIDATED), NMM bought and seen the same day (EXECUTED)
    "s4_terminal_rows": DAY1[:1] + [
        ("2026-11-10", "16:40", [_src(**{"TEN.US": "22.00"}), _lots(lot("NMM.US", "2026-11-10"))], (), "on"),
        ("2026-11-11", "08:40", [_src(), _lots(lot("NMM.US", "2026-11-10"), lot("TEN.US", "2026-11-10")),
                                 _board("2026-11-11")], (), "on")],
    # S5: three plan days. ZTO.US is a 9 Nov pick only (earnings block it on 10 Nov); its lot (bought 10 Nov)
    # appears on 11 Nov with TEN.US's: only the newest previous day (10 Nov) settles
    "s5_older_days": [
        ("2026-11-09", "10:00", [_board("2026-11-09", hhmm="09:38")], (), "on"),
        ("2026-11-10", "10:00", [_board("2026-11-10", hhmm="09:38")], (), "on"),
        ("2026-11-11", "08:40", [_board("2026-11-11"), _lots(lot("ZTO.US", "2026-11-10"),
                                                             lot("TEN.US", "2026-11-10"))], (), "on")],
    # S6: TEN.US kept on 11 Nov and bought on 11 Nov after the cut
    "s6_bought_today_picked_today": DAY1 + [
        ("2026-11-11", "08:40", [_board("2026-11-11")], (), "on"),
        ("2026-11-11", "16:40", [_lots(lot("TEN.US", "2026-11-11"))], (), "on")],
    # S7: NMM.US bought on 10 Nov, its lot entered on 11 Nov after the 08:40 cut (which kept it as a pick)
    "s7_late_lot_entry": DAY1 + [
        ("2026-11-11", "08:40", [_board("2026-11-11")], (), "on"),
        ("2026-11-11", "12:40", [_lots(lot("NMM.US", "2026-11-10"))], (), "on")],
    # S8: NMM.US falls to rank 9 on 11 Nov (not kept, not picked) and is bought on 11 Nov
    "s8_bought_today_not_picked": DAY1 + [
        ("2026-11-11", "08:40", [_board("2026-11-11", Q2_NMM9)], (), "on"),
        ("2026-11-11", "12:40", [_lots(lot("NMM.US", "2026-11-11"))], (), "on")],
    # S9: the sync-triggered run after Riyadh midnight cuts 11 Nov from a fresh render
    "s9_after_midnight": DAY1 + [
        ("2026-11-11", "00:30", [_lots(lot("NMM.US", "2026-11-10")), _board("2026-11-11", hhmm="00:20")], (), "on")],
    # S10: the next-day run in observe mode, then as a dry run
    "s10_observe_and_dry_run": DAY1 + [
        ("2026-11-11", "08:40", [_lots(lot("NMM.US", "2026-11-10")), _board("2026-11-11")], (), "observe"),
        ("2026-11-11", "08:45", [], ("--dry-run",), "on")],
    # S7b: S7, then the 12 Nov 08:40 run: the 11 Nov pick (carried because the lot came late) settles
    "s7b_late_lot_next_day": DAY1 + [
        ("2026-11-11", "08:40", [_board("2026-11-11")], (), "on"),
        ("2026-11-11", "12:40", [_lots(lot("NMM.US", "2026-11-10"))], (), "on"),
        ("2026-11-12", "08:40", [_board("2026-11-12")], (), "on")],
    # S14: TEN.US and NMM.US both bought on 10 Nov
    "s14_two_settled": DAY1 + [
        ("2026-11-11", "08:40", [_lots(lot("NMM.US", "2026-11-10"), lot("TEN.US", "2026-11-10")),
                                 _board("2026-11-11")], (), "on")],
    # S15: the 10 Nov cut is unverified (15:30, every pick CHECK, as the first live plan); NMM.US bought
    "s15_check_bought": [
        ("2026-11-10", "15:30", [_board("2026-11-10", hhmm="15:20", verified=False)], (), "on"),
        ("2026-11-11", "08:40", [_lots(lot("NMM.US", "2026-11-10")), _board("2026-11-11")], (), "on")],
    # S15: NMM.US above its entry zone at the 16:40 refresh (WAIT), then bought
    "s15_wait_bought": DAY1[:2] + [
        ("2026-11-10", "16:40", [_src(**{"NMM.US": "65.00"})], (), "on"),
        ("2026-11-11", "08:40", [_src(), _lots(lot("NMM.US", "2026-11-10")), _board("2026-11-11")], (), "on")],
    # S12: a single day, no previous plan
    "s12_no_previous_plan": DAY1[:1] + [("2026-11-10", "16:40", [_lots(lot("NMM.US", "2026-11-10"))], (), "on")],
}


def play(mod, name, gate):
    """Run a scenario; return [(rc, stdout, ledger, page)] after each step."""
    b = new_book()
    out = []
    for day, hhmm, edits, argv, mode in SCENARIOS[name]:
        for f in edits:
            f(b)
        rc, log = run(mod, b, at(day, hhmm), gate, argv, mode)
        led = copy.deepcopy(b.tabs["_Daily_Plan"].values) if "_Daily_Plan" in b.tabs else None
        page = copy.deepcopy(b.tabs["Daily_Plan"].values) if "Daily_Plan" in b.tabs else None
        out.append((rc, log, led, page))
    return out


def rows(led, day=None, kind="PICK"):
    h = dp.LEDGER_HEADERS
    return [dict(zip(h, r)) for r in led[1:] if (day is None or r[0] == day) and (kind is None or r[4] == kind)]


def by_sym(led, day):
    return {d["Symbol"]: d for d in rows(led, day)}


def stability(page):
    return [str(r[0]) for r in page if str(r[0]).startswith("Stability:")][0]


def counts(page):
    m = re.search(r"(\d+) kept, (\d+) new, (\d+) dropped(?:, (\d+) executed)?", stability(page))
    return tuple(int(x) for x in m.groups() if x is not None)


def frozen_cols(led, day):
    keep = ("Symbol", "Entry", "Zone Low", "Zone High", "Qty", "Alloc SAR", "Stop", "TP1", "TP2", "Risk SAR",
            "Kind", "Rank", "Labels", "Waiting For")
    return [[d[k] for k in keep] for d in rows(led, day, kind=None)]


# ----------------------------------------------------------------------------- S0
def test_s0_version_floor_and_gate_words():
    assert tuple(int(x) for x in dp.__version__.split(".")) >= (1, 0, 2)
    for word, want in ((None, False), ("off", False), ("", False), ("no", False), ("0", False), ("enforce", False),
                       ("on", True), ("ON", True), (" on ", True), ("1", True), ("true", True), ("Yes", True)):
        with env(word):
            assert dp.settle_prev_on() is want, word


# ----------------------------------------------------------------------------- S1
def test_s1_bought_on_plan_day_is_settled():
    on = play(dp, "s1_bought_on_plan_day", "on")
    off = play(dp, "s1_bought_on_plan_day", None)
    rc, log, led, page = on[3]
    assert rc == 0
    prev = by_sym(led, "2026-11-10")
    assert (prev["NMM.US"]["Status"], prev["NMM.US"]["Status Reason"], prev["NMM.US"]["Status At (Riyadh)"]) == (
        "EXECUTED", "bought 2026-11-10 (settled 2026-11-11)", "2026-11-11 08:40")
    for s in ("ITRN.US", "TEN.US"):                       # not bought: as the 16:40 refresh left them
        assert prev[s]["Status"] == "OPEN" and prev[s]["Status At (Riyadh)"] == "2026-11-10 16:40", s
    assert [d["Symbol"] for d in rows(led, "2026-11-11")] == ["ITRN.US", "TEN.US", "LADR.US"]
    assert stability(page).endswith("versus the previous plan: 2 kept, 1 new, 0 dropped, 1 executed")
    assert [x for x in log.splitlines() if "SETTLE-PREV" in x] == [
        "SETTLE-PREV previous plan 2026-11-10: settled NMM.US (bought 2026-11-10) | kept 2, new 1, dropped 0, "
        "executed 1"]
    # off: the defect as shipped in 1.0.1
    _, log0, led0, page0 = off[3]
    assert by_sym(led0, "2026-11-10")["NMM.US"]["Status"] == "OPEN"
    assert stability(page0).endswith("versus the previous plan: 2 kept, 1 new, 1 dropped")
    assert "SETTLE-PREV" not in log0
    # the settle changes no pick, level, quantity or pipeline row of today's plan
    assert frozen_cols(led, "2026-11-11") == frozen_cols(led0, "2026-11-11")
    # a later run the same day: the settled row is terminal; it is not settled again
    _, log2, led2, page2 = on[4]
    assert by_sym(led2, "2026-11-10")["NMM.US"]["Status At (Riyadh)"] == "2026-11-11 08:40"
    assert "settled none | kept 2, new 1, dropped 0, executed 1" in log2
    assert counts(page2) == (2, 1, 0, 1)


# ----------------------------------------------------------------------------- S2
@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_s2_off_identity_against_main(name):
    b = base()
    want = play(b, name, None)
    for gate in (None, "off", "OFF ", "no", "0", "enforce", ""):
        got = play(dp, name, gate)
        assert len(got) == len(want)
        for i, (g, w) in enumerate(zip(got, want)):
            assert g == w, (name, gate, i)
            assert "SETTLE-PREV" not in g[1]


# ----------------------------------------------------------------------------- S3
def test_s3_unbought_old_and_sold_lots_are_not_settled():
    day1 = play(dp, "s3_unbought_and_old_lots", "on")
    before = by_sym(day1[2][2], "2026-11-10")
    rc, log, led, page = day1[3]
    after = by_sym(led, "2026-11-10")
    assert rc == 0
    assert after == before                           # TEN under its stop, ITRN held since 9 Nov, NMM sold: untouched
    assert all(d["Status"] == "OPEN" for d in after.values())
    assert "settled none" in log
    p = rows(led, "2026-11-11", kind=None)
    assert {d["Symbol"]: d["Waiting For"] for d in p}["ITRN.US"] == "already held"


# ----------------------------------------------------------------------------- S4
def test_s4_terminal_rows_are_kept_and_counted():
    on = play(dp, "s4_terminal_rows", "on")
    off = play(dp, "s4_terminal_rows", None)
    day1 = by_sym(on[1][2], "2026-11-10")
    assert day1["TEN.US"]["Status"] == "INVALIDATED" and day1["NMM.US"]["Status"] == "EXECUTED"
    _, log, led, page = on[2]
    assert by_sym(led, "2026-11-10") == day1         # TEN bought later stays INVALIDATED; NMM keeps its own reason
    assert by_sym(led, "2026-11-10")["NMM.US"]["Status Reason"] == "bought 2026-11-10"
    assert "settled none" in log
    k, n, d, e = counts(page)
    assert e == 1 and k + d + e == 3                 # the same-day EXECUTED pick is counted, not hidden
    assert counts(off[2][3]) == (k, n, d)            # off hides it: kept / new / dropped are the same


# ----------------------------------------------------------------------------- S5
def test_s5_rows_older_than_the_previous_plan_are_untouched():
    on = play(dp, "s5_older_days", "on")
    assert [d["Symbol"] for d in rows(on[0][2], "2026-11-09")] == ["ITRN.US", "TEN.US", "ZTO.US"]
    assert "ZTO.US" not in by_sym(on[1][2], "2026-11-10")
    nov9 = rows(on[1][2], "2026-11-09", kind=None)
    _, log, led, page = on[2]
    assert rows(led, "2026-11-09", kind=None) == nov9  # ZTO bought on 10 Nov: its 9 Nov row is not settled
    assert by_sym(led, "2026-11-09")["ZTO.US"]["Status"] == "OPEN"
    prev = by_sym(led, "2026-11-10")
    assert prev["TEN.US"]["Status"] == "EXECUTED"
    assert prev["TEN.US"]["Status Reason"] == "bought 2026-11-10 (settled 2026-11-11)"
    assert rows(led, "2026-11-10", kind="PIPELINE") == rows(on[1][2], "2026-11-10", kind="PIPELINE")
    assert [x for x in log.splitlines() if "SETTLE-PREV" in x] == [
        "SETTLE-PREV previous plan 2026-11-10: settled TEN.US (bought 2026-11-10) | kept 2, new 1, dropped 0, "
        "executed 1"]


# ----------------------------------------------------------------------------- S6
def test_s6_bought_today_and_picked_today_is_left_to_todays_row():
    on = play(dp, "s6_bought_today_picked_today", "on")
    _, log, led, page = on[4]
    assert by_sym(led, "2026-11-10")["TEN.US"]["Status"] == "OPEN"
    today = by_sym(led, "2026-11-11")
    assert today["TEN.US"]["Status"] == "EXECUTED" and today["TEN.US"]["Status Reason"] == "bought 2026-11-11"
    assert [d["Symbol"] for d in rows(led, "2026-11-11")] == ["ITRN.US", "TEN.US", "NMM.US"]
    assert counts(page) == (3, 0, 0, 0)               # TEN stays kept, not executed-and-dropped
    assert "settled none" in log


# ----------------------------------------------------------------------------- S7
def test_s7_lot_entered_after_todays_cut_settles_and_stays_kept():
    on = play(dp, "s7_late_lot_entry", "on")
    assert [d["Symbol"] for d in rows(on[3][2], "2026-11-11")] == ["ITRN.US", "TEN.US", "NMM.US"]
    _, log, led, page = on[4]
    prev = by_sym(led, "2026-11-10")["NMM.US"]
    assert (prev["Status"], prev["Status Reason"]) == ("EXECUTED", "bought 2026-11-10 (settled 2026-11-11)")
    assert counts(page) == (3, 0, 0, 0)               # NMM is today's pick too: kept, not new
    assert "settled NMM.US (bought 2026-11-10)" in log


def test_s7b_carried_pick_settles_the_next_day():
    on = play(dp, "s7b_late_lot_next_day", "on")
    assert by_sym(on[4][2], "2026-11-11")["NMM.US"]["Status"] == "OPEN"      # bought before its own plan day
    _, log, led, page = on[5]
    nov11 = by_sym(led, "2026-11-11")["NMM.US"]
    assert (nov11["Status"], nov11["Status Reason"], nov11["Status At (Riyadh)"]) == (
        "EXECUTED", "bought 2026-11-10 (settled 2026-11-12)", "2026-11-12 08:40")
    assert by_sym(led, "2026-11-10")["NMM.US"]["Status Reason"] == "bought 2026-11-10 (settled 2026-11-11)"
    assert {d["Symbol"]: d["Waiting For"] for d in rows(led, "2026-11-12", kind=None)}["NMM.US"] == "already held"
    assert counts(page) == (2, 1, 0, 1)
    assert [x for x in log.splitlines() if "SETTLE-PREV" in x] == [
        "SETTLE-PREV previous plan 2026-11-11: settled NMM.US (bought 2026-11-10) | kept 2, new 1, dropped 0, "
        "executed 1"]
    off = play(dp, "s7b_late_lot_next_day", None)
    assert counts(off[5][3]) == (2, 1, 1)                                   # 1.0.1: the bought name is dropped


# ----------------------------------------------------------------------------- S8
def test_s8_bought_today_not_picked_today_is_settled():
    on = play(dp, "s8_bought_today_not_picked", "on")
    assert counts(on[3][3]) == (2, 1, 1, 0)
    _, log, led, page = on[4]
    prev = by_sym(led, "2026-11-10")["NMM.US"]
    assert (prev["Status"], prev["Status Reason"]) == ("EXECUTED", "bought 2026-11-11 (settled 2026-11-11)")
    assert counts(page) == (2, 1, 0, 1)
    assert [x for x in log.splitlines() if "SETTLE-PREV" in x] == [
        "SETTLE-PREV previous plan 2026-11-10: settled NMM.US (bought 2026-11-11) | kept 2, new 1, dropped 0, "
        "executed 1"]


# ----------------------------------------------------------------------------- S9
def test_s9_after_midnight():
    on = play(dp, "s9_after_midnight", "on")
    rc, log, led, page = on[3]
    assert rc == 0 and "CUT 2026-11-11" in log
    prev = by_sym(led, "2026-11-10")["NMM.US"]
    assert (prev["Status"], prev["Status At (Riyadh)"]) == ("EXECUTED", "2026-11-11 00:30")
    assert counts(page) == (2, 1, 0, 1)
    off = play(dp, "s9_after_midnight", None)
    assert by_sym(off[3][2], "2026-11-10")["NMM.US"]["Status"] == "OPEN" and counts(off[3][3]) == (2, 1, 1)


# ----------------------------------------------------------------------------- S10
def test_s10_observe_and_dry_run_write_nothing():
    on = play(dp, "s10_observe_and_dry_run", "on")
    led_day1, page_day1 = on[2][2], on[2][3]
    for i in (3, 4):
        rc, log, led, page = on[i]
        assert rc == 0 and led == led_day1 and page == page_day1
        assert "nothing written" in log
        assert "SETTLE-PREV previous plan 2026-11-10: settled NMM.US (bought 2026-11-10)" in log


# ----------------------------------------------------------------------------- S11
def test_s11_counts_add_up():
    seen = 0
    status = ("Status", "Status Reason", "Status At (Riyadh)")
    for name in sorted(SCENARIOS):
        steps = play(dp, name, "on")
        for i, (rc, log, led, page) in enumerate(steps):
            m = re.search(r"SETTLE-PREV previous plan (\S+): .* \| kept (\d+), new (\d+), dropped (\d+), "
                          r"executed (\d+)", log)
            if not m or "nothing written" in log:
                continue
            prev_day, k, n, d, e = m.group(1), *(int(x) for x in m.groups()[1:])
            assert counts(page) == (k, n, d, e), (name, i)
            if i and steps[i - 1][2]:
                # the settle writes the three status cells of a settled PICK row and nothing else
                was = {(r0["Kind"], r0["Symbol"]): r0 for r0 in rows(steps[i - 1][2], None, kind=None)
                       if r0["Plan Date"] <= prev_day}
                now = {(r1["Kind"], r1["Symbol"]): r1 for r1 in rows(led, None, kind=None) if r1["Plan Date"] <= prev_day}
                assert set(was) == set(now), (name, i)
                for key, r1 in now.items():
                    r0 = was[key]
                    assert {h: r1[h] for h in r1 if h not in status} == {h: r0[h] for h in r0 if h not in status}
                    if any(r1[h] != r0[h] for h in status):
                        assert key[0] == "PICK" and r1["Plan Date"] == prev_day and r1["Status"] == "EXECUTED"
                        assert "(settled " in r1["Status Reason"], (name, i, key)
            last = max(r[0] for r in led[1:])
            assert k + d + e == len(rows(led, prev_day)), (name, i)
            assert k + n == len(rows(led, last)), (name, i)
            seen += 1
    assert seen >= 9


def test_s11b_counts_read_only_the_previous_plan():
    def r(day, sym, st):
        return {"Plan Date": day, "Kind": "PICK", "Symbol": sym, "Status": st}
    # ITRN bought on 20 Oct (EXECUTED then), sold, picked again on 9 Nov and not bought: 9 Nov counts it dropped
    older = [r("2026-10-20", "ITRN.US", "EXECUTED"), r("2026-11-09", "ITRN.US", "OPEN"),
             r("2026-11-09", "TEN.US", "EXECUTED"), r("2026-11-09", "NMM.US", "OPEN"),
             dict(r("2026-11-09", "LADR.US", ""), Kind="PIPELINE")]
    today = [r("2026-11-10", "NMM.US", "OPEN"), r("2026-11-10", "AX.US", "OPEN")]
    assert dp.stability_counts(older, "2026-11-09", today) == (1, 1, 1, 1)
    assert dp.stability_counts(older, "2026-10-20", today) == (0, 2, 0, 1)
    assert dp.stability_counts([], "2026-11-09", today) == (0, 2, 0, 0)


# ----------------------------------------------------------------------------- S12
def test_s12_no_previous_plan_on_equals_off():
    assert play(dp, "s12_no_previous_plan", "on") == play(dp, "s12_no_previous_plan", None)


# ----------------------------------------------------------------------------- S13
def test_s13_workflow_manifest_ci():
    wf_text = (ROOT / ".github/workflows/daily_plan.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(wf_text)
    step = [s for s in wf["jobs"]["daily_plan"]["steps"] if "daily_plan.py" in str(s.get("run", ""))][0]
    assert step["env"][GATE] == "${{ vars.TFB_PLAN_SETTLE_PREV || 'off' }}"
    src = Path(DELIVERED).read_text(encoding="utf-8")
    assert '"%s"' % GATE in src
    manifest = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("daily_plan", "%s", "daily plan")' % dp.__version__ in manifest
    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    hits = [(j, s) for j, job in ci["jobs"].items() for s in job.get("steps", [])
            if "tests/test_daily_plan_settle_prev.py" in str(s.get("run", ""))]
    assert len(hits) == 1 and hits[0][0] == "lean-unit"
    s = hits[0][1]
    assert str((s.get("env") or {}).get("TFB_TEST_REQUIRE_BASE")) == "1" and BASE_SHA in s["run"]
    assert not re.search(r"create_order|place_order|requests\.post|urlopen", src)


# ----------------------------------------------------------------------------- S14
def test_s14_two_names_settle_in_ledger_order():
    on = play(dp, "s14_two_settled", "on")
    _, log, led, page = on[3]
    prev = by_sym(led, "2026-11-10")
    for s in ("TEN.US", "NMM.US"):
        assert (prev[s]["Status"], prev[s]["Status Reason"]) == ("EXECUTED", "bought 2026-11-10 (settled 2026-11-11)")
    assert prev["ITRN.US"]["Status"] == "OPEN"
    assert [x for x in log.splitlines() if "SETTLE-PREV" in x] == [
        "SETTLE-PREV previous plan 2026-11-10: settled TEN.US (bought 2026-11-10), NMM.US (bought 2026-11-10) | "
        "kept 1, new 2, dropped 0, executed 2"]
    assert counts(page) == (1, 2, 0, 2)


# ----------------------------------------------------------------------------- S15
def test_s15_check_and_wait_rows_settle():
    chk = play(dp, "s15_check_bought", "on")
    assert all(d["Status"] == "CHECK" and d["Cut Quality"] == "UNVERIFIED" for d in rows(chk[0][2], "2026-11-10"))
    wait = play(dp, "s15_wait_bought", "on")
    assert by_sym(wait[2][2], "2026-11-10")["NMM.US"]["Status"] == "WAIT"
    for steps in (chk, wait):
        _, log, led, page = steps[-1]
        before = by_sym(steps[-2][2], "2026-11-10")["NMM.US"]
        after = by_sym(led, "2026-11-10")["NMM.US"]
        assert (after["Status"], after["Status Reason"], after["Status At (Riyadh)"]) == (
            "EXECUTED", "bought 2026-11-10 (settled 2026-11-11)", "2026-11-11 08:40")
        status = ("Status", "Status Reason", "Status At (Riyadh)")
        assert {h: after[h] for h in after if h not in status} == {h: before[h] for h in before if h not in status}
        assert counts(page) == (2, 1, 0, 1)


# ----------------------------------------------------------------------------- S16
def test_s16_the_run_of_pick_days():
    def r(day, sym, kind="PICK", st="OPEN"):
        return {"Plan Date": day, "Kind": kind, "Symbol": sym, "Status": st, "Status Reason": "x",
                "Status At (Riyadh)": "y"}
    d9, d10, d11, d12 = (dt.date(2026, 11, x) for x in (9, 10, 11, 12))

    def settle(older, bd):
        got = dp.settle_prev(older, "2026-11-11", {"NMM.US": bd}, [], d12, "2026-11-12 08:40")
        row = [o for o in older if o["Plan Date"] == "2026-11-11" and o["Symbol"] == "NMM.US"][0]
        return got, row["Status"]
    run3 = [r("2026-11-09", "NMM.US"), r("2026-11-10", "NMM.US"), r("2026-11-11", "NMM.US")]
    assert settle(copy.deepcopy(run3), d9) == ([("NMM.US", d9)], "EXECUTED")       # back to the run's first day
    assert settle(copy.deepcopy(run3), dt.date(2026, 11, 8)) == ([], "OPEN")      # bought before the run
    gap = [r("2026-11-09", "NMM.US"), r("2026-11-10", "ITRN.US"), r("2026-11-11", "NMM.US")]
    assert settle(gap, d9) == ([], "OPEN")                                         # 10 Nov did not pick it
    pipe = [r("2026-11-10", "NMM.US", kind="PIPELINE"), r("2026-11-11", "NMM.US")]
    assert settle(pipe, d10) == ([], "OPEN")                                       # a pipeline day is no pick
    order = [r("2026-11-09", "ITRN.US"), r("2026-11-10", "NMM.US"), r("2026-11-11", "NMM.US")]
    assert settle(order, d10) == ([("NMM.US", d10)], "EXECUTED")                   # newest first: run = 10-11 Nov
    assert order[2]["Status Reason"] == "bought 2026-11-10 (settled 2026-11-12)"
    assert order[2]["Status At (Riyadh)"] == "2026-11-12 08:40" and order[1]["Status"] == "OPEN"
    assert dp.settle_prev(copy.deepcopy(run3), "not a date", {"NMM.US": d9}, [], d12, "s") == []


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
