#!/usr/bin/env python3
"""tests/test_daily_plan.py - scripts/daily_plan.py 1.2.0: one plan a day, frozen.

  H1-H2 (1.2.0) hourly status on live prices: the live price decides OPEN / WAIT / INVALIDATED and is named;
      a pick without a live price keeps the page price; the cut and the frozen levels never use it
  E1-E6 (1.1.0) the evidence rule: off adds nothing; observe labels every
      qualified name and changes no pick; on keeps a name that is not PASS in
      the pipeline; the two thresholds at their edge; Tadawul and a missing
      number read n/a; the two columns are read from the source page

  D1  parsers: numbers, the main page's status line (finished / today /
      verified), the control panel, the ALL QUALIFIED table, the engine's
      deferral reasons (soft: held target inside the limit, awaiting
      confirmation; anything else blocks)
  D2  the decision on a board shaped like 10 Nov 2026: three picks in the
      engine's order, sizing (cash floor, equal split, risk cap), levels
  D3  execution gates: held, cool-down, currency, no price, earnings,
      ex-dividend, held target past the limit, unknown engine reason, sector
  D4  sizing edges: the minimum ticket and a share dearer than the allocation
      move a pick to the pipeline; the risk cap binds when cash is ample
  D5  FREEZE: a second run on the same day with a different merit list and
      different prices changes no pick, entry, quantity, stop or target;
      only the status moves (WAIT, INVALIDATED, EXECUTED; terminal stays)
  D6  no cut from an unfinished page, yesterday's render, or an unverified
      page before the fallback hour; an unverified cut after it reads CHECK
  D7  the next day: yesterday's unexecuted pick keeps its place inside the
      keep-rank; the stability line counts kept / new / dropped
  D8  the gate: off touches nothing; observe and --dry-run write nothing
  D9  --wait-min looks again after 5 minutes and cuts when the page is ready
  D10 the workflow, the manifest pin and the CI lane

Run: pytest -q tests/test_daily_plan.py
"""
from __future__ import annotations

import datetime as dt
import re
import sys
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import daily_plan as dp  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 10, 7, 0, tzinfo=UTC)          # 10:00 Riyadh
OK_LINE = "Last run 2026-11-10 09:38:18 | status: ok | output: QUALIFIED_UNFUNDED | source readiness VERIFIED (run 42749059415)"
QH = ["Rank", "Symbol", "Name", "Market", "Sector", "ROI % (TP1)", "Engine ROI % (12M)", "Ann ROI %", "R/R (TP2)",
      "Rel", "DQ", "Risk", "Score", "Selected", "Why Not Selected"]
HELD1 = "Held target (analyst_lkg:1h) — no new money until the provider leg returns"
CONF2 = "stability: awaiting confirmation (2/3 days) — qualified on merit, seat withheld until confirmed"


class WorksheetNotFound(Exception):
    pass


def _colnum(letters):
    n = 0
    for ch in letters:
        n = n * 26 + ord(ch) - 64
    return n


class Sheet:
    def __init__(self, rows, row_count=200, col_count=30):
        self.values = deepcopy(rows)
        self.row_count, self.col_count = row_count, col_count
        self.updates = []

    def get(self, range_name):
        return deepcopy(self.values)

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
        self.updates.append(range_name)
        self.values = [list(r) for r in values if any(str(c) != "" for c in r)]


class Book:
    def __init__(self, tabs):
        self.tabs = dict(tabs)
        self.reads = 0

    def worksheet(self, name):
        self.reads += 1
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]

    def add_worksheet(self, title, rows, cols):
        self.tabs[title] = Sheet([], rows, cols)
        return self.tabs[title]


def qrow(rank, sym, sector, roi, why, name=None):
    return [rank, sym, name or sym + " Inc", "NYSE/NASDAQ", sector, "", "%s%%" % roi, "", "", 71.5, 100, "Low",
            80 - rank, "No", why]


def board(line=OK_LINE, qual=None, cash="33,125 SAR"):
    qual = QUAL if qual is None else qual
    rows = [["TOP 10 INVESTMENTS — DECISION"], ["Status:", line], ["CONTROL PANEL"],
            ["T10: Universe Scope", "All Main Sheets", "", "T10: Max Selected", 10],
            ["T10: Allow Negative Sector", "No", "", "T10: Max Per Sector", 2],
            ["T10: Include Portfolio Holdings", "No", "", "T10: Base Currency", "SAR", "", "Cash Available (SAR)", cash],
            [], ["Rank", "Symbol", "Name"], ["—", "ZTO.US", "ZTO Express"], [],
            ["ALL QUALIFIED — INVEST opportunity set (%d)" % len(qual)], QH] + qual + [[], ["NEAR MISS"]]
    return rows


QUAL = [qrow(1, "ITRN.US", "Technology", 34.7, HELD1), qrow(2, "TEN.US", "Energy", 34.3, CONF2),
        qrow(3, "ZTO.US", "Industrials", 32.4, HELD1), qrow(4, "NMM.US", "Industrials", 29.9, HELD1),
        qrow(5, "SBLK.US", "Industrials", 27.2, HELD1), qrow(6, "LADR.US", "Real Estate", 29.5, CONF2),
        qrow(7, "BBOX.L", "Real Estate", 31.2, ""), qrow(8, "AX.US", "Financials", 25.3, CONF2)]
SRC = [["Symbol", "Name", "Currency", "Sector", "Current Price", "Warnings"],
       ["ITRN.US", "Ituran", "USD", "Technology", "51.82", "analyst_lkg:1h"],
       ["TEN.US", "Tsakos", "USD", "Energy", "25.00", ""], ["ZTO.US", "ZTO", "USD", "Industrials", "20.66", ""],
       ["NMM.US", "Navios", "USD", "Industrials", "60.00", ""], ["SBLK.US", "Star Bulk", "USD", "Industrials", "27.07", ""],
       ["LADR.US", "Ladder", "USD", "Real Estate", "11.00", ""], ["BBOX.L", "Tritax", "GBX", "Real Estate", "147.70", ""],
       ["AX.US", "Axos", "USD", "Financials", "80.00", ""]]
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


def book(**over):
    tabs = {"Top_10_Investments": Sheet(board()), "Global_Markets": Sheet(SRC), "Market_Leaders": Sheet([["x"]]),
            "Calendar_Events": Sheet(CAL), "_Portfolio_CostBasis": Sheet(LOTS)}
    tabs.update(over)
    return Book(tabs)


def K(**over):
    k = dp.knobs()
    k.update(over)
    return k


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in list(__import__("os").environ):
        if name.startswith("TFB_PLAN_") or name == "TFB_DAILY_PLAN":
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_PLAN_LOG_DETAIL", "on")          # 1.10.2: the suite reads the detailed lines (Q1: the rest)
    monkeypatch.setenv("TFB_DAILY_PLAN", "on")
    # 1.8.0: the 10-11 Nov fixtures carry names at the forecast cap (ITRN.US 34.7%); the suites written before
    # the cap rule pin it off. test_c1-c5 at the end test the rule itself.
    monkeypatch.setenv("TFB_PLAN_ROI_CAP", "off")
    # 1.9.0: the suites written before the wait rule cut at 10:00 Riyadh with whatever capture they pass;
    # test_w1-w7 at the end test the rule itself.
    monkeypatch.setenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", "0")
    # 1.9.1: the source pages of the suites written before the quote-time rule carry no 'Last Updated (UTC)'
    # column; test_q1-q3 at the end test the rule itself.
    monkeypatch.setenv("TFB_PLAN_QUOTE_TIME", "optional")


def lots():
    return dp.read_ledger_lots(LOTS, dt.date(2026, 11, 10), 10)


def plan(qual=QUAL, cash=33125.0, k=None, prior=(), src=SRC, cal=CAL, mps=2):
    return dp.decide(dp.read_qualified(board(qual=qual)), dp.read_sources([src]), dp.read_calendar(cal, []), lots(),
                     cash, mps, k or K(), prior_picks=prior)


# ----------------------------------------------------------------------------- E (1.1.0)
SRC_EV = [["Symbol", "Name", "Currency", "Sector", "Current Price", "Warnings", "Risk Score", "Target Price"],
          ["ITRN.US", "Ituran", "USD", "Technology", "51.82", "", "29.36", "74.5"],        # PASS (cool-down none here)
          ["TEN.US", "Tsakos", "USD", "Energy", "25.00", "", "41.09", "35.0"],             # FAIL: risk
          ["ZTO.US", "ZTO", "USD", "Industrials", "20.66", "", "29.8", "22.00"],           # FAIL: upside +6%
          ["NMM.US", "Navios", "USD", "Industrials", "60.00", "", "29.4", "77.0"],         # PASS
          ["SBLK.US", "Star Bulk", "USD", "Industrials", "27.07", "", "", "34.44"],        # n/a: no risk score
          ["LADR.US", "Ladder", "USD", "Real Estate", "11.00", "", "25.0", ""],            # n/a: no target
          ["BBOX.L", "Tritax", "GBX", "Real Estate", "147.70", "", "35.0", "190"],            # FAIL, and not fundable
          ["AX.US", "Axos", "USD", "Financials", "80.00", "", "26.36", "101.5"]]           # PASS
CAL_EV = [["Symbol", "Next Earnings Date", "Days To Earnings", "Next Ex-Div Date", "Days To ExDiv"]] + [
    [sym, "2027-01-20", 71, "", ""] for sym in ("ITRN.US", "TEN.US", "ZTO.US", "NMM.US", "SBLK.US", "LADR.US", "AX.US")]


def _ev(p):
    return {c["symbol"]: [x for x in c["labels"] if x.startswith("evidence rule")] for c in p["picks"] + p["pipeline"]}


def test_e1_off_adds_no_label_and_reads_as_before():
    assert dp.ev_mode() == "off" and K()["ev_mode"] == "off"
    p = plan(src=SRC_EV, cal=CAL_EV)
    assert all(v == [] for v in _ev(p).values())
    assert all("ev" not in c for c in p["picks"] + p["pipeline"])


def test_e2_observe_labels_every_name_and_changes_no_pick(monkeypatch):
    off = plan(src=SRC_EV, cal=CAL_EV)
    monkeypatch.setenv("TFB_PLAN_EVIDENCE", "observe")
    obs = plan(src=SRC_EV, cal=CAL_EV)
    assert [c["symbol"] for c in obs["picks"]] == [c["symbol"] for c in off["picks"]] == ["ITRN.US", "TEN.US", "ZTO.US"]
    for key in ("qty", "entry", "stop", "tp1", "tp2", "alloc_sar"):
        assert [c[key] for c in obs["picks"]] == [c[key] for c in off["picks"]], key
    ev = _ev(obs)
    assert ev["ITRN.US"] == ["evidence rule PASS (risk 29.4, analyst upside +44%)"]
    assert ev["TEN.US"] == ["evidence rule FAIL (risk 41.1, analyst upside +40%; needs risk under 30)"]
    assert ev["ZTO.US"] == ["evidence rule FAIL (risk 29.8, analyst upside +6%; needs upside +15% or more)"]
    assert ev["SBLK.US"] == ["evidence rule n/a (no risk score)"] and ev["LADR.US"] == ["evidence rule n/a (no analyst target)"]
    assert len(ev) == len(QUAL) and all(len(v) == 1 for v in ev.values())      # blocked names are labelled too
    assert {c["symbol"]: c["ev"] for c in obs["picks"]} == {"ITRN.US": "PASS", "TEN.US": "FAIL", "ZTO.US": "FAIL"}


def test_e3_on_picks_only_names_that_pass(monkeypatch):
    monkeypatch.setenv("TFB_PLAN_EVIDENCE", "on")
    p = plan(src=SRC_EV, cal=CAL_EV)
    assert [c["symbol"] for c in p["picks"]] == ["ITRN.US", "NMM.US", "AX.US"]
    wait = {c["symbol"]: c["waiting"] for c in p["pipeline"]}
    for sym in ("TEN.US", "ZTO.US", "SBLK.US", "LADR.US"):
        assert wait[sym] == "evidence rule not met", sym
    assert wait["BBOX.L"].startswith("currency GBX")                           # an earlier gate keeps its own reason
    assert all(c["qty"] > 0 and c["stop"] < c["entry"] for c in p["picks"])


def test_e4_thresholds_at_the_edge(monkeypatch):
    rule = dp.evidence_rule
    assert rule("A.US", {"price": 100.0, "risk": 29.99, "target": 115.0}, 30, 15)[0] == "PASS"
    assert rule("A.US", {"price": 100.0, "risk": 30.0, "target": 115.0}, 30, 15)[0] == "FAIL"     # under, not up to
    assert rule("A.US", {"price": 100.0, "risk": 29.99, "target": 114.99}, 30, 15)[0] == "FAIL"
    both = rule("A.US", {"price": 100.0, "risk": 45.0, "target": 105.0}, 30, 15)
    assert both == ("FAIL", "evidence rule FAIL (risk 45.0, analyst upside +5%; needs risk under 30 and upside +15% or more)")
    monkeypatch.setenv("TFB_PLAN_EVIDENCE", "on")
    monkeypatch.setenv("TFB_PLAN_EV_RISK_MAX", "45")
    monkeypatch.setenv("TFB_PLAN_EV_UPSIDE_MIN", "5")
    assert (K()["ev_risk_max"], K()["ev_upside_min"]) == (45, 5)
    assert [c["symbol"] for c in plan(src=SRC_EV, cal=CAL_EV)["picks"]] == ["ITRN.US", "TEN.US", "ZTO.US"]
    monkeypatch.setenv("TFB_PLAN_EVIDENCE", "ON ")                              # trimmed, any case
    assert dp.ev_mode() == "on"
    monkeypatch.setenv("TFB_PLAN_EVIDENCE", "enforce")                          # unknown value = off
    assert dp.ev_mode() == "off"


def test_e5_tadawul_and_missing_numbers_read_na():
    rule = dp.evidence_rule
    assert rule("2222.SR", {"price": 25.0, "risk": 10.0, "target": 40.0}, 30, 15) == (
        "NA", "evidence rule n/a (not measured for Tadawul)")
    assert rule("A.US", {"price": 25.0, "risk": None, "target": 40.0}, 30, 15)[1] == "evidence rule n/a (no risk score)"
    for bad in ({"price": 25.0, "risk": 10.0, "target": None}, {"price": 25.0, "risk": 10.0, "target": 0.0},
                {"price": None, "risk": 10.0, "target": 40.0}, {"price": 0.0, "risk": 10.0, "target": 40.0}, {}):
        assert rule("A.US", bad, 30, 15)[0] == "NA", bad


def test_e6_the_two_columns_are_read_and_the_run_writes_the_label(monkeypatch):
    wide = [["Symbol", "Name", "Currency", "Sector", "Current Price", "Warnings", "Other", "Risk Score", "Target Price"]] + [
        r[:6] + ["x"] + r[6:] for r in SRC_EV[1:]]
    grid = dp.read_source_page(Book({"Global_Markets": Sheet(wide)}), "Global_Markets")
    assert grid[0] == ["Symbol", "Current Price", "Currency", "Sector", "Warnings", "Risk Score", "Target Price"]
    got = dp.read_sources([grid])
    assert (got["NMM.US"]["risk"], got["NMM.US"]["target"]) == (29.4, 77.0)
    assert got["SBLK.US"]["risk"] is None and got["LADR.US"]["target"] is None
    assert dp.read_sources([SRC])["NMM.US"]["risk"] is None                    # a page without the columns still reads
    monkeypatch.setenv("TFB_PLAN_EVIDENCE", "observe")
    b = book(Global_Markets=Sheet(SRC_EV), Calendar_Events=Sheet(CAL_EV))
    assert dp.main([], book=b, now=NOW) == 0
    led = b.tabs["_Daily_Plan"].values
    lab = dp.LEDGER_HEADERS.index("Labels")
    by = {r[6]: r[lab] for r in led[1:]}
    assert "evidence rule PASS (risk 29.4, analyst upside +44%)" in by["ITRN.US"]
    assert "evidence rule FAIL" in by["TEN.US"] and "evidence rule n/a" in by["SBLK.US"]
    page = "\n".join(" | ".join(str(c) for c in r) for r in b.tabs["Daily_Plan"].values)
    assert "evidence rule PASS (risk 29.4, analyst upside +44%)" in page


# ----------------------------------------------------------------------------- D1
def test_d1_parsers():
    assert dp.num("33,125 SAR") == 33125.0 and dp.num("31.2%") == 31.2 and dp.num("▲ 5.62%") == 5.62
    assert dp.num("(100)") == -100.0 and dp.num("—") is None and dp.num(True) is None and dp.num(None) is None
    s = dp.board_state(board(), NOW)
    assert s["finished"] and s["today"] and s["verified"] and s["output"] == "QUALIFIED_UNFUNDED"
    assert s["run_at"] == "2026-11-10 09:38"
    r = dp.board_state(board(line="Last run 2026-11-10 12:09:13 | status: running…"), NOW)
    assert not r["finished"] and "no finished render" in r["reason"]
    y = dp.board_state(board(line=OK_LINE.replace("2026-11-10", "2026-11-09")), NOW)
    assert y["finished"] and not y["today"] and "2026-11-09" in y["reason"]
    w = dp.board_state(board(line="Last run 2026-11-10 09:08:00 | status: ok | output: WITHHELD | source readiness: unverified"), NOW)
    assert w["finished"] and w["today"] and not w["verified"]
    assert dp.board_state([["nothing"]], NOW)["reason"] == "no status line"
    assert dp.num(dp.panel_value(board(), "Cash Available (SAR)")) == 33125.0
    assert dp.num(dp.panel_value(board(), "T10: Max Per Sector")) == 2
    q = dp.read_qualified(board())
    assert [x["symbol"] for x in q] == [r[1] for r in QUAL] and q[0]["roi12"] == 34.7 and q[0]["rank"] == 1
    assert dp.read_qualified([["no table"]]) == []
    assert dp.engine_deferral(HELD1, 48) == ("", "analyst target held 1h")
    assert dp.engine_deferral(HELD1.replace("1h", "60h"), 48)[0].startswith("analyst target held 60h")
    assert dp.engine_deferral(CONF2, 48) == ("", "confirmation 2/3")
    assert dp.engine_deferral("", 48) == ("", "") and dp.engine_deferral("Qualified (INVEST) — deferred by diversification cap", 48) == ("", "")
    assert dp.engine_deferral("venue not tradable", 48) == ("venue not tradable", "")
    for soft in ("\u2014 selected", "not selected \u2014 no seat was taken this run; see Status line",
                 "not selected \u2014 qualified, but absent from this run's ticket set (2 seat(s) taken); see Status line",
                 "ranked below the Max Selected cut"):
        assert dp.engine_deferral(soft, 48) == ("", ""), soft
    held = "Held research seat \u2014 GRACE-HELD; stability: GRACE (0/3 missed); no executable allocation today"
    assert dp.engine_deferral(held, 48) == ("", "main page seat: grace-held")
    assert dp.engine_deferral("structural: Eligibility (Venue)", 48)[0] == "structural: Eligibility (Venue)"


def test_d1b_the_main_pages_selected_rows_are_picks():
    """Codex P1 on PR #951: a row the main page selected reads '\u2014 selected' and must lead the plan."""
    sel = qrow(1, "ITRN.US", "Technology", 34.7, "\u2014 selected")
    sel[13] = "Yes"
    odd = qrow(2, "TEN.US", "Energy", 34.3, "anything the cockpit writes beside a Yes")
    odd[13] = "Yes"
    q = [sel, odd, qrow(3, "NMM.US", "Industrials", 29.9, "not selected \u2014 no seat was taken this run; see Status line"),
         qrow(4, "LADR.US", "Real Estate", 29.5, "structural: Eligibility (Venue)")]
    p = plan(qual=q)
    assert [c["symbol"] for c in p["picks"]] == ["ITRN.US", "TEN.US", "NMM.US"]
    assert [c["waiting"] for c in p["pipeline"]] == ["structural: Eligibility (Venue)"]


# ----------------------------------------------------------------------------- D2
def test_d2_decision_on_the_10_nov_shape():
    p = plan()
    assert [c["symbol"] for c in p["picks"]] == ["ITRN.US", "TEN.US", "NMM.US"]
    nav = 9900 + 12256 + 34117 + 33125
    assert p["nav"] == nav and abs(p["deployable"] - (33125 - 0.10 * nav)) < 1e-6
    per = min(0.15 * nav, 0.01 * nav / 0.08, p["deployable"] / 3)
    assert abs(p["per_pick"] - per) < 1e-6
    it = p["picks"][0]
    assert it["qty"] == int(per // (51.82 * 3.75)) and it["stop"] == round(51.82 * 0.92, 4)
    assert it["zone_low"] == round(51.82 * 0.97, 4) and it["zone_high"] == round(51.82 * 1.01, 4)
    assert it["tp2"] == round(51.82 * 1.347, 4) and it["tp1"] == round(51.82 * (1 + 0.347 / 2), 4)
    assert abs(it["risk_sar"] - it["qty"] * 51.82 * 0.08 * 3.75) < 0.5 and it["risk_sar"] / nav < 0.0101
    assert "analyst target held 1h" in it["labels"] and "confirmation 2/3" in p["picks"][1]["labels"]
    wait = {c["symbol"]: c["waiting"] for c in p["pipeline"]}
    assert wait["ZTO.US"] == "earnings in 7 day(s)" and wait["SBLK.US"] == "earnings in 2 day(s)"
    assert wait["BBOX.L"].startswith("currency GBX is not fundable") and wait["LADR.US"] == "next in line (rank 6)"
    assert [c["rank"] for c in p["pipeline"]] == sorted(c["rank"] for c in p["pipeline"])


# ----------------------------------------------------------------------------- D3
def test_d3_execution_gates():
    q = [qrow(1, "DDI.US", "Technology", 30, ""), qrow(2, "FRST.US", "Financials", 30, ""),
         qrow(3, "NOPX.US", "Energy", 30, ""), qrow(4, "OLD.US", "Energy", 30, HELD1.replace("1h", "60h")),
         qrow(5, "ODD.US", "Energy", 30, "venue not tradable"), qrow(6, "DIV.US", "Utilities", 30, ""),
         qrow(7, "A.US", "Energy", 30, ""), qrow(8, "B.US", "Energy", 30, ""), qrow(9, "C.US", "Energy", 30, "")]
    src = [SRC[0], ["DDI.US", "", "USD", "", "15.7", ""], ["FRST.US", "", "USD", "", "10", ""],
           ["OLD.US", "", "USD", "", "10", ""], ["ODD.US", "", "USD", "", "10", ""], ["DIV.US", "", "USD", "", "100", ""],
           ["A.US", "", "USD", "", "10", ""], ["B.US", "", "USD", "", "10", ""], ["C.US", "", "USD", "", "10", ""]]
    div = [["Symbol", "Ex-Div Date", "Days To ExDiv", "Amount", "Currency"], ["DIV.US", "2026-11-12", 2, 8.13, "USD"],
           ["A.US", "2026-11-12", 2, 0.05, "USD"]]
    p = dp.decide(dp.read_qualified(board(qual=q)), dp.read_sources([src]), dp.read_calendar([], div), lots(), 33125.0,
                  2, K())
    wait = {c["symbol"]: c["waiting"] for c in p["pipeline"]}
    assert wait["DDI.US"] == "already held" and wait["FRST.US"] == "sold 6 day(s) ago (cool-down 10 days)"
    assert wait["NOPX.US"] == "no price on its source page" and wait["OLD.US"].startswith("analyst target held 60h")
    assert wait["ODD.US"] == "venue not tradable" and wait["DIV.US"] == "ex-dividend 8.1% in 2 day(s)"
    assert [c["symbol"] for c in p["picks"]] == ["A.US", "B.US"]          # 0.5% dividend does not block A
    assert wait["C.US"] == "sector cap (Energy already has 2)"
    assert all("earnings date not on the calendar" in c["labels"] for c in p["picks"])


def test_d3b_event_days_count_from_the_date_not_the_stored_count():
    """1.0.1: 10 Nov plan; Calendar_Events synced 9 Nov read ZTO.US '2026-11-17 | 8' - 7 days away by date."""
    stale = [list(r) for r in CAL]
    for r in stale[1:]:
        r[2] = r[2] + 1                                   # every stored count is one day old
    cal = dp.read_calendar(stale, [], dt.date(2026, 11, 10))
    assert cal["ZTO.US"]["earn_days"] == 7.0 and cal["SBLK.US"]["earn_days"] == 2.0
    assert dp.read_calendar(stale, [])["ZTO.US"]["earn_days"] == 8.0          # no date context: stored count
    no_date = [list(r) for r in stale]
    no_date[3][1] = ""                                    # ZTO row without a date
    assert dp.read_calendar(no_date, [], dt.date(2026, 11, 10))["ZTO.US"]["earn_days"] == 8.0
    div = [["Symbol", "Ex-Div Date", "Days To ExDiv", "Amount", "Currency"], ["DIV.US", "2026-11-12", 3, 8.13, "USD"]]
    assert dp.read_calendar([], div, dt.date(2026, 11, 10))["DIV.US"]["exdiv_days"] == 2.0
    b = book(**{"Calendar_Events": Sheet(stale)})
    assert dp.main([], book=b, now=NOW) == 0
    led = b.tabs["_Daily_Plan"].values[1:]
    assert "ZTO.US" not in [r[6] for r in led if r[4] == "PICK"]
    assert [r[23] for r in led if r[6] == "ZTO.US"] == ["earnings in 7 day(s)"]


def test_d3c_a_pick_without_a_12_month_roi_is_labelled():
    q = [qrow(1, "ITRN.US", "Technology", "\u2014", "")]
    p = plan(qual=q)
    assert p["picks"][0]["tp1"] is None and "no 12-month target" in p["picks"][0]["labels"]


# ----------------------------------------------------------------------------- D4
def test_d4_sizing_edges():
    small = plan(cash=12000.0)                    # NAV 68,273: deployable 5,173 = 1,724 a pick for three
    assert [c["symbol"] for c in small["picks"]] == ["ITRN.US", "TEN.US"]
    edge = plan(cash=13000.0)                     # 2,024 a pick, but whole shares give 1,943 / 1,969 / 1,800
    assert [c["symbol"] for c in edge["picks"]] == ["ITRN.US", "TEN.US"]
    assert all(c["alloc_sar"] >= 2000 for c in edge["picks"])
    assert [c["waiting"] for c in edge["pipeline"] if c["symbol"] == "NMM.US"][0].endswith("minimum ticket in whole shares")
    assert all(c["alloc_sar"] >= 2000 for c in small["picks"])
    assert any(c["waiting"].startswith("cash:") and "minimum ticket" in c["waiting"] for c in small["pipeline"])
    dear = [list(r) for r in SRC]
    dear[2][4] = "9000"                                                  # TEN.US: one share = 33,750 SAR
    p = plan(src=dear)
    assert "TEN.US" not in [c["symbol"] for c in p["picks"]]
    assert [c["waiting"] for c in p["pipeline"] if c["symbol"] == "TEN.US"][0].endswith("is below one share")
    rich = plan(cash=900000.0)
    nav = rich["nav"]
    assert abs(rich["per_pick"] - 0.01 * nav / 0.08) < 1e-6 and rich["per_pick"] < 0.15 * nav
    assert all(c["risk_sar"] <= 0.01 * nav + 1 for c in rich["picks"])
    assert plan(cash=0.0)["picks"] == []


# ----------------------------------------------------------------------------- D5
def frozen(b):
    return [[r[i] for i in (6, 11, 12, 13, 14, 15, 16, 17, 18)] for r in b.tabs["_Daily_Plan"].values[1:] if r[4] == "PICK"]


def test_d5_freeze_and_status():
    b = book()
    assert dp.main([], book=b, now=NOW) == 0
    led = b.tabs["_Daily_Plan"].values
    assert led[0] == dp.LEDGER_HEADERS and [r[6] for r in led[1:] if r[4] == "PICK"] == ["ITRN.US", "TEN.US", "NMM.US"]
    assert all(r[24] == "OPEN" for r in led[1:] if r[4] == "PICK") and led[1][3] == "VERIFIED"
    before = frozen(b)
    page = b.tabs["Daily_Plan"].values
    assert page[0][0].startswith("DAILY PLAN") and any(r[0] == "TODAY'S PICKS (3)" for r in page)
    assert any(str(r[0]).startswith("PIPELINE (5)") for r in page)
    # second run: another merit list (new leader, ITRN gone), prices moved, NMM bought, TEN through its stop
    q2 = [qrow(1, "NEW.US", "Utilities", 40, "")] + [list(r) for r in QUAL[1:]]
    src2 = [list(r) for r in SRC] + [["NEW.US", "", "USD", "Utilities", "10", ""]]
    src2[1][4] = "55.00"                                                 # ITRN above its zone (52.34)
    src2[2][4] = "22.00"                                                 # TEN at/below its stop (23.00)
    lots2 = [list(r) for r in LOTS] + [["NMM.US", "Navios", "USD", "Active", "2026-11-10", "", "8,000"]]
    b.tabs["Top_10_Investments"] = Sheet(board(qual=q2, cash="1,000 SAR"))
    b.tabs["Global_Markets"] = Sheet(src2)
    b.tabs["_Portfolio_CostBasis"] = Sheet(lots2)
    assert dp.main([], book=b, now=NOW + dt.timedelta(hours=7)) == 0
    assert frozen(b) == before                       # symbol, entry, zone, qty, amount, stop, TP1, TP2
    st = {r[6]: (r[24], r[25]) for r in b.tabs["_Daily_Plan"].values[1:] if r[4] == "PICK"}
    assert st["ITRN.US"][0] == "WAIT" and "above the entry zone" in st["ITRN.US"][1]
    assert st["TEN.US"][0] == "INVALIDATED" and "at or below the stop" in st["TEN.US"][1]
    assert st["NMM.US"] == ("EXECUTED", "bought 2026-11-10")
    assert "NEW.US" not in [r[6] for r in b.tabs["_Daily_Plan"].values[1:]]
    b.tabs["_Daily_Plan"].values = [[str(c) for c in r] for r in b.tabs["_Daily_Plan"].values]
    # third run: TEN recovers, NMM lot disappears from the ledger - terminal statuses stay
    b.tabs["Global_Markets"] = Sheet(SRC)
    b.tabs["_Portfolio_CostBasis"] = Sheet(LOTS)
    assert dp.main([], book=b, now=NOW + dt.timedelta(hours=8)) == 0
    st = {r[6]: r[24] for r in b.tabs["_Daily_Plan"].values[1:] if r[4] == "PICK"}
    assert st == {"ITRN.US": "OPEN", "TEN.US": "INVALIDATED", "NMM.US": "EXECUTED"}
    assert [[r[0]] + [dp.num(c) for c in r[1:]] for r in frozen(b)] == [[r[0]] + [dp.num(c) for c in r[1:]] for r in before]


# ----------------------------------------------------------------------------- D6
@pytest.mark.parametrize("line,frag", [
    ("Last run 2026-11-10 12:09:13 | status: running…", "no finished render"),
    (OK_LINE.replace("2026-11-10", "2026-11-09"), "is from 2026-11-09"),
    ("Last run 2026-11-10 09:08:00 | status: ok | output: WITHHELD | source readiness: x", "not verified"),
])
def test_d6_no_cut(line, frag):
    b = book(**{"Top_10_Investments": Sheet(board(line=line))})
    assert dp.main([], book=b, now=NOW) == 0
    assert b.tabs["_Daily_Plan"].values == [dp.LEDGER_HEADERS]
    assert any(frag in str(c) for r in b.tabs["Daily_Plan"].values for c in r)
    assert any(r[0] == "TODAY'S PICKS (0)" for r in b.tabs["Daily_Plan"].values)


def test_d6_unverified_cut_after_the_fallback_hour_reads_check():
    line = "Last run 2026-11-10 15:20:00 | status: ok | output: WITHHELD | source readiness: x"
    b = book(**{"Top_10_Investments": Sheet(board(line=line))})
    assert dp.main([], book=b, now=dt.datetime(2026, 11, 10, 12, 30, tzinfo=UTC)) == 0     # 15:30 Riyadh
    picks = [r for r in b.tabs["_Daily_Plan"].values[1:] if r[4] == "PICK"]
    assert len(picks) == 3 and all(r[3] == "UNVERIFIED" and r[24] == "CHECK" for r in picks)
    assert all("not verified at the cut" in r[25] for r in picks)


# ----------------------------------------------------------------------------- D7
def test_d7_next_day_keeps_an_unexecuted_pick():
    b = book()
    assert dp.main([], book=b, now=NOW) == 0
    day2 = NOW + dt.timedelta(days=1)
    # ITRN falls to rank 6 (inside keep-rank 8); three other names lead
    q2 = [qrow(1, "AX.US", "Financials", 25.3, ""), qrow(2, "LADR.US", "Real Estate", 29.5, ""),
          qrow(3, "TEN.US", "Energy", 34.3, ""), qrow(6, "ITRN.US", "Technology", 34.7, "")]
    line2 = OK_LINE.replace("2026-11-10", "2026-11-11")
    b.tabs["Top_10_Investments"] = Sheet(board(line=line2, qual=q2))
    assert dp.main([], book=b, now=day2) == 0
    today = [r for r in b.tabs["_Daily_Plan"].values[1:] if r[0] == "2026-11-11" and r[4] == "PICK"]
    assert [r[6] for r in today] == ["TEN.US", "ITRN.US", "AX.US"]            # kept first (by rank), then new
    assert all("kept from the previous plan" in r[22] for r in today[:2]) and "kept" not in today[2][22]
    assert [r[0] for r in b.tabs["_Daily_Plan"].values[1:]].count("2026-11-10") > 0   # history kept
    line = [r[0] for r in b.tabs["Daily_Plan"].values if str(r[0]).startswith("Stability:")][0]
    assert "2 kept, 1 new, 1 dropped" in line


def test_d7_keep_rank_limit():
    p = plan(qual=[qrow(1, "AX.US", "Financials", 25, ""), qrow(2, "LADR.US", "Real Estate", 29, ""),
                   qrow(3, "TEN.US", "Energy", 34, ""), qrow(9, "ITRN.US", "Technology", 34, "")], prior=["ITRN.US"])
    assert [c["symbol"] for c in p["picks"]] == ["AX.US", "LADR.US", "TEN.US"]


# ----------------------------------------------------------------------------- D8
def test_d8_gate(monkeypatch):
    monkeypatch.setenv("TFB_DAILY_PLAN", "off")
    b = book()
    assert dp.main([], book=b, now=NOW) == 0 and b.reads == 0 and "_Daily_Plan" not in b.tabs
    monkeypatch.delenv("TFB_DAILY_PLAN")
    assert dp.mode() == "off"
    monkeypatch.setenv("TFB_DAILY_PLAN", "observe")
    assert dp.main([], book=b, now=NOW) == 0 and b.reads > 0 and "_Daily_Plan" not in b.tabs
    monkeypatch.setenv("TFB_DAILY_PLAN", "on")
    assert dp.main(["--dry-run"], book=b, now=NOW) == 0 and "Daily_Plan" not in b.tabs
    monkeypatch.setenv("TFB_PLAN_PICKS", "99")
    monkeypatch.setenv("TFB_PLAN_FUNDABLE_CCY", "GBP, usd")
    k = dp.knobs()
    assert k["picks"] == 3 and k["ccy"] == ["USD"]


def test_d8_unreadable_workbook_writes_nothing():
    b = book()
    del b.tabs["Top_10_Investments"]
    assert dp.main([], book=b, now=NOW) == 2 and "_Daily_Plan" not in b.tabs


def test_d8_a_transient_read_error_is_retried(monkeypatch):
    class Flaky(Sheet):
        fails = 2

        def get(self, range_name):
            if Flaky.fails:
                Flaky.fails -= 1
                raise TimeoutError("read timed out")
            return super().get(range_name)
    naps = []
    monkeypatch.setattr(dp, "_SLEEP", naps.append)
    b = book(**{"Top_10_Investments": Flaky(board())})
    assert dp.main([], book=b, now=NOW) == 0 and naps == [20, 20]
    assert len([r for r in b.tabs["_Daily_Plan"].values[1:] if r[4] == "PICK"]) == 3
    Flaky.fails = 3
    b2 = book(**{"Top_10_Investments": Flaky(board())})
    assert dp.main([], book=b2, now=NOW) == 2 and "_Daily_Plan" not in b2.tabs


# ----------------------------------------------------------------------------- D9
def test_d9_wait_looks_again():
    b = book(**{"Top_10_Investments": Sheet(board(line="Last run 2026-11-10 09:50:00 | status: running…"))})
    naps = []

    def nap(s):
        naps.append(s)
        b.tabs["Top_10_Investments"] = Sheet(board())
    assert dp.main(["--wait-min", "45"], book=b, now=NOW, sleep=nap) == 0
    assert naps == [300] and len([r for r in b.tabs["_Daily_Plan"].values[1:] if r[4] == "PICK"]) == 3
    b2 = book(**{"Top_10_Investments": Sheet(board(line="Last run 2026-11-10 09:50:00 | status: running…"))})
    naps2 = []
    assert dp.main(["--wait-min", "10"], book=b2, now=NOW, sleep=naps2.append) == 0
    assert naps2 == [300, 300] and b2.tabs["_Daily_Plan"].values == [dp.LEDGER_HEADERS]


# ----------------------------------------------------------------------------- D10
def test_d10_workflow_manifest_ci():
    wf = yaml.safe_load((ROOT / ".github/workflows/daily_plan.yml").read_text(encoding="utf-8"))
    on = wf[True] if True in wf else wf["on"]
    assert on["workflow_run"]["workflows"] == ["\U0001F3E6 Tadawul Dashboard Advanced Sync"]
    assert on["workflow_run"]["types"] == ["completed"] and "workflow_dispatch" in on and on["schedule"]
    assert wf["permissions"] == {"contents": "read"} and wf["concurrency"]["cancel-in-progress"] is False
    job = wf["jobs"]["daily_plan"]
    assert "github.event.workflow_run.event == 'schedule'" in job["if"] and "'workflow_dispatch'" in job["if"]
    step = [s for s in job["steps"] if "daily_plan.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_DAILY_PLAN"] == "${{ vars.TFB_DAILY_PLAN || 'on' }}"
    assert step["env"]["TFB_PLAN_EVIDENCE"] == "${{ vars.TFB_PLAN_EVIDENCE || 'observe' }}"   # 1.1.0: a label, not a gate
    assert step["env"]["TFB_PLAN_LIVE_PRICE"] == "${{ vars.TFB_PLAN_LIVE_PRICE || 'on' }}"    # 1.2.0
    assert on["schedule"] == [{"cron": "20 4-20 * * *"}]                                      # 1.3.0: active hours
    assert "--wait-min 45" in step["run"] and "--dry-run" in step["run"]
    src = (ROOT / "scripts/daily_plan.py").read_text(encoding="utf-8")
    for name in re.findall(r"(TFB_PLAN_[A-Z_]+): \$\{\{", (ROOT / ".github/workflows/daily_plan.yml").read_text(encoding="utf-8")):
        assert '"%s"' % name in src, name
    manifest = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("daily_plan", "%s", "daily plan")' % dp.__version__ in manifest
    assert "tests/test_daily_plan.py" in (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert not re.search(r"create_order|place_order|requests\.post|urlopen", src)


# ----------------------------------------------------------------------------- H (1.2.0)
def test_h1_hourly_status_uses_the_live_price(monkeypatch):
    import scripts.exit_plan as ep
    b = book()
    assert dp.main([], book=b, now=NOW) == 0                                       # the cut, live price off
    before = frozen(b)
    calls = []
    at = dt.datetime(2026, 11, 10, 12, 0, tzinfo=UTC)                              # 15:00 Riyadh
    quotes = {"ITRN.US": 55.00, "TEN.US": 22.00}                                   # NMM.US: no live price

    def fake(sym):
        calls.append(sym)
        return (quotes[sym], at) if sym in quotes else None
    monkeypatch.setattr(ep, "live_quote", fake)
    monkeypatch.setenv("TFB_PLAN_LIVE_PRICE", "on")
    assert dp.main([], book=b, now=NOW + dt.timedelta(hours=1)) == 0
    assert frozen(b) == before                                                     # levels never move
    rows = {r[6]: r for r in b.tabs["_Daily_Plan"].values[1:] if r[4] == "PICK"}
    st = {s: (r[24], r[25], r[26]) for s, r in rows.items()}
    assert st["ITRN.US"][0] == "WAIT" and st["ITRN.US"][1].endswith("(live 15:00 Riyadh)") and st["ITRN.US"][2] == 55.0
    assert st["TEN.US"][0] == "INVALIDATED" and "(live 15:00 Riyadh)" in st["TEN.US"][1]
    assert st["NMM.US"][0] == "OPEN" and "live" not in st["NMM.US"][1] and st["NMM.US"][2] == 60.0   # page price
    assert sorted(calls) == ["ITRN.US", "NMM.US", "TEN.US"]
    calls.clear()
    assert dp.main([], book=b, now=NOW + dt.timedelta(hours=2)) == 0
    assert "TEN.US" not in calls                                                   # terminal: no quote asked


def test_h2_live_overlay_edges(monkeypatch):
    rows = [{"Kind": "PICK", "Symbol": "a.us", "Status": "OPEN"}, {"Kind": "PIPELINE", "Symbol": "B.US", "Status": ""},
            {"Kind": "PICK", "Symbol": "C.US", "Status": "EXECUTED"}, {"Kind": "PICK", "Symbol": "D.US", "Status": "CHECK"},
            {"Kind": "PICK", "Symbol": "E.US", "Status": "WAIT"}]
    src = {"A.US": {"price": 1.0, "ccy": "USD"}}
    at = dt.datetime(2026, 11, 10, 20, 5, tzinfo=UTC)

    def q(sym):
        if sym == "D.US":
            raise OSError("network")
        return {"A.US": (2.5, at), "E.US": (0.0, at)}.get(sym)
    got = dp.live_overlay(rows, src, q)
    assert got == {"A.US": "23:05"} and src["A.US"] == {"price": 2.5, "ccy": "USD", "quote_at": at} and "E.US" not in src
    monkeypatch.delenv("TFB_PLAN_LIVE_PRICE", raising=False)
    assert dp.knobs()["live"] is False
    monkeypatch.setenv("TFB_PLAN_LIVE_PRICE", "ON")
    assert dp.knobs()["live"] is True


# ----------------------------------------------------------------------------- H3
def test_h3_no_cut_before_the_morning_hour(monkeypatch):
    # 1.3.0: a verified render made at 00:12 Riyadh must not cut the new day's plan; the 07:20 run cuts it
    line = OK_LINE.replace("2026-11-10 09:", "2026-11-10 00:")
    b = book(**{"Top_10_Investments": Sheet(board(line=line))})
    night = dt.datetime(2026, 11, 9, 21, 20, tzinfo=UTC)                         # 00:20 Riyadh on 10 Nov
    assert dp.main([], book=b, now=night) == 0
    assert not [r for r in b.tabs["_Daily_Plan"].values[1:] if r[0] == "2026-11-10"]
    page = "\n".join(" | ".join(str(c) for c in r) for r in b.tabs["Daily_Plan"].values)
    assert "the plan is cut from 07:00 Riyadh, after the morning sync" in page
    assert dp.main([], book=b, now=dt.datetime(2026, 11, 10, 4, 20, tzinfo=UTC)) == 0   # 07:20 Riyadh
    assert [r for r in b.tabs["_Daily_Plan"].values[1:] if r[0] == "2026-11-10" and r[4] == "PICK"]
    monkeypatch.setenv("TFB_PLAN_CUT_FROM_H", "0")
    b2 = book(**{"Top_10_Investments": Sheet(board(line=line))})
    assert dp.main([], book=b2, now=night) == 0                                   # the knob turns it off
    assert [r for r in b2.tabs["_Daily_Plan"].values[1:] if r[0] == "2026-11-10" and r[4] == "PICK"]
    monkeypatch.setenv("TFB_PLAN_CUT_FROM_H", "99")
    assert dp.knobs()["cut_h"] == 7                                               # out of range: the default


# ----------------------------------------------------------------------------- H4 (1.4.0)
def test_h4_dividend_tax_waits_for_the_ex_date(monkeypatch):
    """1.4.0 [DIV-TAX]: a Saudi resident keeps 70% of a US dividend while the price falls by all of it."""
    q = [qrow(1, "X.US", "Energy", 30, ""), qrow(2, "Y.US", "Financials", 30, ""), qrow(3, "Z.SR", "Materials", 30, ""),
         qrow(4, "W.US", "Utilities", 30, ""), qrow(5, "V.US", "Industrials", 30, "")]
    src = [SRC[0], ["X.US", "", "USD", "", "50", ""], ["Y.US", "", "USD", "", "10", ""], ["Z.SR", "", "SAR", "", "100", ""],
           ["W.US", "", "USD", "", "20", ""], ["V.US", "", "USD", "", "50", ""]]
    div = [["Symbol", "Ex-Div Date", "Days To ExDiv", "Amount", "Currency"],
           ["X.US", "", 6, 1.00, "USD"],      # 2.0% in 6 days: 30% withheld = 0.60% of the price -> wait
           ["Y.US", "", 5, 0.02, "USD"],      # 0.2%: 0.06% -> a label only
           ["Z.SR", "", 6, 3.00, "SAR"],      # Tadawul: no US withholding
           ["W.US", "", 12, 1.00, "USD"],     # outside the 10-day window
           ["V.US", "", 0, 1.00, "USD"]]      # the 3-day gate answers first
    run = lambda k: dp.decide(dp.read_qualified(board(qual=q)), dp.read_sources([src]), dp.read_calendar([], div), lots(),
                              33125.0, 5, k)
    p = run(K(picks=5))
    wait = {c["symbol"]: c["waiting"] for c in p["pipeline"]}
    assert wait["X.US"] == ("ex-dividend in 6 day(s): 1.00 a share (2.0%); bought before it, 30% is withheld = 0.60% "
                            "of the price - buy after the ex-date")
    assert wait["V.US"] == "ex-dividend 2.0% in 0 day(s)"
    picks = {c["symbol"]: c for c in p["picks"]}
    assert set(picks) == {"Y.US", "Z.SR", "W.US"}
    assert "ex-dividend in 5 day(s): 30% withholding costs 0.06%" in picks["Y.US"]["labels"]
    assert not [x for x in picks["Z.SR"]["labels"] + picks["W.US"]["labels"] if x.startswith("ex-dividend")]
    assert "X.US" in [c["symbol"] for c in run(K(picks=5, div_tax=False))["picks"]]           # the switch
    assert "X.US" in [c["symbol"] for c in run(K(picks=5, div_tax_min=0.7))["picks"]]         # the threshold
    assert "X.US" in [c["symbol"] for c in run(K(picks=5, div_tax_days=5))["picks"]]         # the window
    assert "X.US" in [c["symbol"] for c in run(K(picks=5, wht=0.0))["picks"]]                 # no withholding
    assert dp.withholding_pct("ABC.US", "", 30) == 30 and dp.withholding_pct("2222.SR", "SAR", 30) == 0
    assert dp.withholding_pct("2222.SR", "USD", 30) == 0                     # a Tadawul listing, whatever the currency
    assert dp.withholding_pct("BBOX.L", "GBX", 30) == 0 and dp.withholding_pct("ABC", "USD", 15) == 15
    assert dp.dividend_tax("X.US", "USD", 50, {"exdiv_days": 6, "div_amount": None}, 10, 30) is None
    assert dp.dividend_tax("X.US", "USD", 0, {"exdiv_days": 6, "div_amount": 1}, 10, 30) is None
    assert dp.dividend_tax("X.US", "USD", 50, {"exdiv_days": -1, "div_amount": 1}, 10, 30) is None
    assert dp.dividend_tax("X.US", "USD", 50, {"exdiv_days": 10, "div_amount": 1}, 10, 30)["cost_pct"] == pytest.approx(0.6)
    monkeypatch.setenv("TFB_PLAN_DIV_TAX", "off")
    assert dp.knobs()["div_tax"] is False
    monkeypatch.delenv("TFB_PLAN_DIV_TAX", raising=False)
    k = dp.knobs()
    assert k["div_tax"] is True and k["div_tax_days"] == 10 and k["div_tax_min"] == 0.25 and k["wht"] == 30.0


def test_h4_workflow_maps_the_dividend_knobs():
    wf = yaml.safe_load((ROOT / ".github" / "workflows" / "daily_plan.yml").read_text(encoding="utf-8"))
    env = [s for s in wf["jobs"][list(wf["jobs"])[0]]["steps"] if "TFB_PLAN_DIV_TAX" in (s.get("env") or {})][0]["env"]
    assert env["TFB_PLAN_DIV_TAX"] == "${{ vars.TFB_PLAN_DIV_TAX || 'on' }}"
    assert env["TFB_PLAN_WHT_PCT"] == "${{ vars.TFB_PLAN_WHT_PCT || '30' }}"
    assert env["TFB_PLAN_DIV_TAX_DAYS"] == "${{ vars.TFB_PLAN_DIV_TAX_DAYS || '10' }}"
    assert env["TFB_PLAN_DIV_TAX_MIN_PCT"] == "${{ vars.TFB_PLAN_DIV_TAX_MIN_PCT || '0.25' }}"


# ----------------------------------------------------------------------------- H5 (1.5.0)
def test_h5_an_old_price_reads_check_and_never_opens_or_invalidates():
    """ChatGPT review, 12 Nov: a quote dated 2020 returned OPEN in 2026. A price past the age limit reads CHECK."""
    row = {"symbol": "A.US", "stop": 9.0, "zone_low": 9.7, "zone_high": 10.1}
    now = dt.datetime(2026, 11, 12, 9, 0, tzinfo=UTC)
    old, fresh = dt.datetime(2020, 1, 2, tzinfo=UTC), now - dt.timedelta(hours=20)
    base = (row, 10.0, {}, dt.date(2026, 11, 12), None, 7, "VERIFIED")
    assert dp.pick_status(*base)[0] == "OPEN"                                       # no time known: as before
    assert dp.pick_status(*base, old, now, 96)[0] == "CHECK"
    assert "days old (limit 96 h)" in dp.pick_status(*base, old, now, 96)[1]
    assert dp.pick_status(*base, fresh, now, 96)[0] == "OPEN"
    assert dp.pick_status(*base, old, now, 0)[0] == "OPEN"                          # 0 switches the check off
    assert dp.pick_status(row, 8.0, {}, dt.date(2026, 11, 12), None, 7, "VERIFIED", old, now, 96)[0] == "CHECK"
    assert dp.pick_status(row, 8.0, {}, dt.date(2026, 11, 12), None, 7, "VERIFIED", fresh, now, 96)[0] == "INVALIDATED"
    edge = now - dt.timedelta(hours=96)
    assert dp.pick_status(*base, edge, now, 96)[0] == "OPEN"                        # exactly at the limit passes
    assert dp.pick_status(*base, now - dt.timedelta(hours=50), now, 48)[1].startswith("the price 10.00 is 2 days old")
    assert dp.pick_status(*base, now - dt.timedelta(hours=30), now, 24)[1].startswith("the price 10.00 is 30 h old")
    assert dp.parse_utc("2026-11-11 19:22:31") == dt.datetime(2026, 11, 11, 19, 22, 31, tzinfo=UTC)
    assert dp.parse_utc("2026-11-11T19:22Z") == dt.datetime(2026, 11, 11, 19, 22, tzinfo=UTC)
    assert dp.parse_utc("") is None and dp.parse_utc("n/a") is None and dp.parse_utc("2026-13-40 99:99") is None
    grid = [["Symbol", "Current Price", "Currency", "Last Updated (UTC)"], ["A.US", 10, "USD", "2026-11-11 19:22:31"],
            ["B.US", 5, "USD", ""]]
    src = dp.read_sources([grid])
    assert src["A.US"]["quote_at"] == dt.datetime(2026, 11, 11, 19, 22, 31, tzinfo=UTC) and src["B.US"]["quote_at"] is None
    rows = [{"Kind": "PICK", "Symbol": "A.US", "Status": "OPEN", "Stop": 9, "Zone Low": 9.7, "Zone High": 10.1,
             "Cut Quality": "VERIFIED", "Status Reason": "", "Status At (Riyadh)": "", "Last Price": ""}]
    dp.refresh_status(rows, src, {}, {}, dt.date(2026, 11, 12), 7, "s", None, now=now + dt.timedelta(days=30), max_age_h=96)
    assert rows[0]["Status"] == "CHECK"
    dp.refresh_status(rows, src, {}, {}, dt.date(2026, 11, 12), 7, "s", None, now=now, max_age_h=96)
    assert rows[0]["Status"] == "OPEN"
    assert dp.knobs()["quote_max_h"] == 96
    # Codex P2, #964: the production read path fetches the timestamp column, so the check has a time to use
    ws = Sheet([["Global_Markets"], ["Symbol", "Name", "Current Price", "Currency", "Last Updated (UTC)"],
                ["A.US", "A", 10, "USD", "2020-01-02 10:00:00"]])
    got = dp.read_sources([dp.read_source_page(Book({"Global_Markets": ws}), "Global_Markets")])
    assert got["A.US"]["quote_at"] == dt.datetime(2020, 1, 2, 10, 0, tzinfo=UTC)
    rows[0]["Status"] = "OPEN"
    dp.refresh_status(rows, got, {}, {}, dt.date(2026, 11, 12), 7, "s", None, now=now, max_age_h=96)
    assert rows[0]["Status"] == "CHECK" and "days old" in rows[0]["Status Reason"]


def _cand(sym, rank, score, sector):
    return {"rank": rank, "symbol": sym, "name": sym, "market": "US", "sector": sector, "roi12": 20.0, "risk": "Low",
            "score": score, "rel": 80.0, "selected": True, "why": ""}


def test_h5_two_level_rule_prefers_names_at_the_entry_level(monkeypatch):
    """Claude review, 12 Nov: a new name enters at 72; a previous pick stays down to 67."""
    q = [_cand("A.US", 1, 70.5, "S1"), _cand("B.US", 2, 73.0, "S2"), _cand("C.US", 3, 68.0, "S3"),
         _cand("D.US", 4, 75.0, "S4"), _cand("E.US", 5, 66.0, "S5")]
    src = {c["symbol"]: {"price": 10.0, "ccy": "USD"} for c in q}
    lots = {"held": {}, "recent": {}, "mv_sar": 0.0}

    def picks(k, prior=()):
        plan = dp.decide([dict(c) for c in q], dict(src), {}, lots, 100000.0, 3, k, prior_picks=prior)
        return [p["symbol"] for p in plan["picks"]], plan
    assert dp.knobs()["stable"] == "prefer" and dp.knobs()["stable_enter"] == 72 and dp.knobs()["stable_exit"] == 67
    got, plan = picks(K(picks=3))
    assert got == ["B.US", "D.US", "A.US"]                                          # 72+ first, then by rank
    assert any("score 70.5 is under the entry level 72" in x for x in plan["picks"][2]["labels"])
    assert picks(K(picks=3, stable="off"))[0] == ["A.US", "B.US", "C.US"]           # the old order
    got, plan = picks(K(picks=3, stable="strict"))
    assert got == ["B.US", "D.US"]
    waits = {c["symbol"]: c["waiting"] for c in plan["pipeline"]}
    assert waits["A.US"] == "score 70.5 is under the entry level 72 (a pick stays down to 67)" and "E.US" in waits
    got, plan = picks(K(picks=3), prior=("C.US", "E.US"))                           # C kept at 68; E fell under 67
    assert got == ["C.US", "B.US", "D.US"] and "kept from the previous plan" in plan["picks"][0]["labels"]
    assert picks(K(picks=3, stable="strict"), prior=("C.US", "E.US"))[0] == ["C.US", "B.US", "D.US"]
    assert picks(K(picks=3, stable="off"), prior=("E.US",))[0] == ["E.US", "A.US", "B.US"]   # off: rank only
    assert picks(K(picks=3, stable_enter=70.5))[0] == ["A.US", "B.US", "D.US"]      # exactly at the level enters
    none = [dict(c, score=None) for c in q]
    plan = dp.decide(none, dict(src), {}, lots, 100000.0, 3, K(picks=3), prior_picks=())
    assert [p["symbol"] for p in plan["picks"]] == ["A.US", "B.US", "C.US"]         # no score: never held back
    monkeypatch.setenv("TFB_PLAN_STABLE", "junk")
    assert dp.stable_mode() == "prefer"
    monkeypatch.setenv("TFB_PLAN_STABLE", "STRICT")
    assert dp.stable_mode() == "strict"


def test_h5_workflow_maps_the_new_knobs():
    wf = yaml.safe_load((ROOT / ".github" / "workflows" / "daily_plan.yml").read_text(encoding="utf-8"))
    env = [s for s in wf["jobs"][list(wf["jobs"])[0]]["steps"] if "TFB_PLAN_STABLE" in (s.get("env") or {})][0]["env"]
    assert env["TFB_PLAN_STABLE"] == "${{ vars.TFB_PLAN_STABLE || 'prefer' }}"
    assert env["TFB_PLAN_STABLE_ENTER"] == "${{ vars.TFB_PLAN_STABLE_ENTER || '72' }}"
    assert env["TFB_PLAN_STABLE_EXIT"] == "${{ vars.TFB_PLAN_STABLE_EXIT || '67' }}"
    assert env["TFB_PLAN_QUOTE_MAX_AGE_H"] == "${{ vars.TFB_PLAN_QUOTE_MAX_AGE_H || '96' }}"


# ----------------------------------------------------------------------------- H6 (1.6.0) cash from the cash ledger
SNAP = [["Date", "Time", "Type", "Amount SAR", "Balance SAR", "Delta vs Prev", "Note"],
        ["2026-07-17", "", "OPENING (broker read)", "17,025.00", "17,025.00", "", ""],
        ["2026-11-10", "", "SNAPSHOT (broker read)", "33,125.00", "", "", "Derayah USD 9130"],
        ["2026-11-12", "", "CALCULATED SNAPSHOT", "-15,578.18", "17,546.82", "-15,578.18", "User-requested deduction"]]


def test_h6_the_cut_sizes_from_the_cash_ledger_when_it_is_lower(monkeypatch, capsys):
    """13 Nov: the typed cell read 33,125 SAR the morning after two buys; the cash ledger read 17,547."""
    assert dp.snapshot_cash(SNAP) == (17546.82, "2026-11-12 · CALCULATED SNAPSHOT")
    assert dp.snapshot_cash(SNAP[:3]) == (33125.0, "2026-11-10 · SNAPSHOT (broker read)")    # the only figure of the row
    assert dp.snapshot_cash([]) == (None, "") and dp.snapshot_cash([["x"], ["2026-11-12", "5"]]) == (None, "")
    assert dp.snapshot_cash(SNAP[:1] + [["not a date", "", "X", "", "9"], ["2026-11-12", "", "CALCULATED", "-5", ""]]) == (None, "")
    assert dp.snapshot_cash(SNAP[:1] + [["2026-11-12", "", "CALCULATED SNAPSHOT", "500", ""]]) == (None, "")   # a change, not a balance
    # Codex P2, #971: the latest balance stands when it is negative - no buying power, never the older figure
    assert dp.snapshot_cash(SNAP + [["2026-11-13", "", "CALCULATED SNAPSHOT", "-17,646.82", "-100.00"]]) == (
        0.0, "2026-11-13 · CALCULATED SNAPSHOT")
    assert dp.snapshot_cash(SNAP[:1] + [["2026-11-12", "", "WITHDRAWAL", "500", ""]]) == (None, "")   # not a broker read
    assert dp.sizing_cash(33125.0, 0.0, "lower") == (0.0, "snapshot (lower than the panel)")
    assert dp.sizing_cash(33125.0, 17546.82, "lower") == (17546.82, "snapshot (lower than the panel)")
    assert dp.sizing_cash(10000.0, 17546.82, "lower") == (10000.0, "panel (not above the snapshot)")
    assert dp.sizing_cash(33125.0, 17546.82, "snapshot")[0] == 17546.82 and dp.sizing_cash(33125.0, 17546.82, "panel") == (33125.0, "panel")
    assert dp.sizing_cash(33125.0, None, "lower") == (33125.0, "panel")
    assert dp.cash_source_mode() == "ibkr"                                                # 1.7.0: the broker first
    monkeypatch.setenv("TFB_DAILY_PLAN", "on")
    rich, poor = book(), book(_Cash_Snapshot=Sheet(SNAP))
    assert dp.main([], book=rich, now=NOW) == 0                                           # no cash ledger: as before
    out_rich = capsys.readouterr().out
    assert dp.main([], book=poor, now=NOW) == 0
    out_poor = capsys.readouterr().out
    assert "cash for sizing: from the panel | snapshot not read" in out_rich
    assert "cash for sizing: from the snapshot (lower than the panel) | snapshot 2026-11-12 · CALCULATED SNAPSHOT" in out_poor
    assert "33,125" not in out_poor and "17,547" not in out_poor and "NAV" not in out_poor   # no account total is logged
    assert "| cash from the snapshot (lower than the panel)" in out_poor
    assert "IBKR capture not used for sizing: no capture" in out_poor
    qty = lambda b: sum(float(r[14] or 0) * float(r[11] or 0) for r in b.tabs["_Daily_Plan"].values[1:] if r[4] == "PICK")
    assert 0 < qty(poor) < qty(rich)                                                      # smaller tickets, never larger
    monkeypatch.setenv("TFB_PLAN_CASH_SOURCE", "panel")
    old = book(_Cash_Snapshot=Sheet(SNAP))
    assert dp.main([], book=old, now=NOW) == 0 and qty(old) == qty(rich)
    assert "cash for sizing: from the panel" in capsys.readouterr().out
    raw = (ROOT / ".github/workflows/daily_plan.yml").read_text(encoding="utf-8")
    assert "TFB_PLAN_CASH_SOURCE: ${{ vars.TFB_PLAN_CASH_SOURCE || 'ibkr' }}" in raw


# ----------------------------------------------------------------------------- H7 (1.7.0) cash from the broker
def cap_of(cash=4679.15, settled=4679.15, age_h=2.0, buys=None, ok=True, reason=""):
    return {"ok": ok, "reason": reason, "age_h": age_h, "cash_usd": cash, "settled_usd": settled, "buys": buys or {},
            "positions": {}, "stops": {}, "limits": {}, "n_orders": 0, "name": "TFB_IBKR_Capture_x.json"}


def test_h7_the_cut_sizes_from_the_ibkr_capture(monkeypatch, capsys):
    """Owner, 13 Nov 11:51: the plan should follow the cash available at IBKR."""
    assert dp.capture_cash(cap_of(), 30) == (4679.15 * 3.75, "")
    assert dp.capture_cash(cap_of(cash=5000.0, settled=4000.0), 30)[0] == 4000.0 * 3.75        # the smaller of the two
    assert dp.capture_cash(cap_of(cash=4000.0, settled=None), 30)[0] == 15000.0
    held = {"INSW.US": [(99.4, 20.0)], "AX.US": [(90.0, 10.0)]}
    assert dp.capture_cash(cap_of(buys=held), 30)[0] == pytest.approx((4679.15 - 1988.0 - 900.0) * 3.75)
    assert dp.capture_cash(cap_of(cash=100.0, settled=100.0, buys=held), 30)[0] == 0.0         # never negative
    assert dp.capture_cash(cap_of(buys={"X.US": [(None, 5.0)]}), 30) == (
        None, "a working buy order has no limit price, so the cash it holds is not known")
    assert dp.capture_cash(cap_of(age_h=31.0), 30) == (None, "the newest capture is 31 h old (limit 30 h)")
    assert dp.capture_cash(cap_of(ok=False, reason="capture is not JSON"), 30) == (None, "capture is not JSON")
    assert dp.capture_cash(None, 30) == (None, "no capture")
    assert dp.sizing_cash(33125.0, 20000.0, "ibkr", 17546.81) == (17546.81, "IBKR capture")
    assert dp.sizing_cash(33125.0, 20000.0, "ibkr", None) == (20000.0, "snapshot (lower than the panel)")   # the 1.6.0 rule
    assert dp.sizing_cash(33125.0, 20000.0, "lower", 17546.81)[0] == 20000.0 and dp.sizing_cash(1.0, 2.0, "panel", 3.0)[0] == 1.0
    assert dp.sizing_cash(33125.0, 20000.0, "ibkr", 40000.0) == (40000.0, "IBKR capture")      # the broker rules, up or down
    monkeypatch.setenv("TFB_DAILY_PLAN", "on")
    qty = lambda b: sum(float(r[14] or 0) * float(r[11] or 0) for r in b.tabs["_Daily_Plan"].values[1:] if r[4] == "PICK")
    rich, ib, stale = book(), book(), book()
    assert dp.main([], book=rich, now=NOW) == 0
    capsys.readouterr()
    assert dp.main([], book=ib, now=NOW, capture=cap_of()) == 0
    out = capsys.readouterr().out
    assert "cash for sizing: from the IBKR capture | snapshot not read" in out and "17,5" not in out and "| cash from the IBKR capture" in out
    assert 0 < qty(ib) < qty(rich)
    assert dp.main([], book=stale, now=NOW, capture=cap_of(age_h=40.0)) == 0
    out = capsys.readouterr().out
    assert "IBKR capture not used for sizing: the newest capture is 40 h old (limit 30 h)" in out and qty(stale) == qty(rich)
    monkeypatch.setenv("TFB_PLAN_CASH_SOURCE", "lower")
    low = book()
    assert dp.main([], book=low, now=NOW, capture=cap_of()) == 0 and qty(low) == qty(rich)     # the capture is not consulted
    assert "IBKR" not in capsys.readouterr().out
    raw = (ROOT / ".github/workflows/daily_plan.yml").read_text(encoding="utf-8")
    assert "TFB_PLAN_CASH_SOURCE: ${{ vars.TFB_PLAN_CASH_SOURCE || 'ibkr' }}" in raw and "TFB_IBKR_CAPTURE_FOLDER_ID" in raw


# ----------------------------------------------------------------------------- C (1.8.0) the forecast cap
def test_c1_the_rule_reader_and_its_defaults(monkeypatch):
    monkeypatch.delenv("TFB_PLAN_ROI_CAP", raising=False)
    assert dp.roi_cap_mode() == "on" and dp.knobs()["roi_cap"] == "on" and dp.knobs()["roi_cap_pct"] == 34
    for value, mode in (("observe", "observe"), (" OFF ", "off"), ("on", "on"), ("junk", "on"), ("", "on")):
        monkeypatch.setenv("TFB_PLAN_ROI_CAP", value)
        assert dp.roi_cap_mode() == mode, value
    for value, pct in (("30", 30), ("20", 20), ("100", 100), ("5", 34), ("500", 34), ("x", 34)):   # house rule: out of range reads the default
        monkeypatch.setenv("TFB_PLAN_ROI_CAP_PCT", value)
        assert dp.knobs()["roi_cap_pct"] == pct, value


def _caps():
    q = [dict(_cand("CAP.US", 1, 75.0, "S1"), roi12=34.7), dict(_cand("EDGE.US", 2, 75.0, "S2"), roi12=34.0),
         dict(_cand("UNDER.US", 3, 75.0, "S3"), roi12=33.9), dict(_cand("NONE.US", 4, 75.0, "S4"), roi12=None),
         dict(_cand("LOW.US", 5, 75.0, "S5"), roi12=12.0)]
    src = {c["symbol"]: {"price": 10.0, "ccy": "USD"} for c in q}
    return q, src, {"held": {}, "recent": {}, "mv_sar": 0.0}


def test_c2_on_a_name_at_the_cap_waits_and_the_next_names_are_picked():
    q, src, lots_ = _caps()
    plan_ = dp.decide([dict(c) for c in q], dict(src), {}, lots_, 100000.0, 5, K(picks=3, roi_cap="on"))
    assert [p["symbol"] for p in plan_["picks"]] == ["UNDER.US", "NONE.US", "LOW.US"]
    waits = {c["symbol"]: c["waiting"] for c in plan_["pipeline"]}
    assert waits["CAP.US"] == "12-month forecast 34.7% is at the cap (34%+): no new money"
    assert waits["EDGE.US"] == "12-month forecast 34.0% is at the cap (34%+): no new money"   # the line itself counts
    assert all("cap" not in x for p in plan_["picks"] for x in p["labels"])
    assert "no 12-month target" in [p for p in plan_["picks"] if p["symbol"] == "NONE.US"][0]["labels"]


def test_c3_observe_labels_and_off_changes_nothing():
    q, src, lots_ = _caps()
    run = lambda mode: dp.decide([dict(c) for c in q], dict(src), {}, lots_, 100000.0, 5, K(picks=3, roi_cap=mode))
    off, seen = run("off"), run("observe")
    assert [p["symbol"] for p in off["picks"]] == [p["symbol"] for p in seen["picks"]] == ["CAP.US", "EDGE.US", "UNDER.US"]
    assert "[observe] 12-month forecast 34.7% is at the cap (34%+): no new money" in seen["picks"][0]["labels"]
    assert not any("cap" in x for p in off["picks"] for x in p["labels"])
    assert [p["qty"] for p in off["picks"]] == [p["qty"] for p in seen["picks"]]


def test_c4_another_gate_keeps_its_own_reason_and_the_line_is_a_knob():
    q, src, lots_ = _caps()
    held = dict(lots_, held={"CAP.US": dt.date(2026, 11, 2)})
    plan_ = dp.decide([dict(c) for c in q], dict(src), {}, held, 100000.0, 5, K(picks=3, roi_cap="on"))
    waits = {c["symbol"]: c["waiting"] for c in plan_["pipeline"]}
    assert waits["CAP.US"] == "already held"                                            # the first reason stands
    plan_ = dp.decide([dict(c) for c in q], dict(src), {}, lots_, 100000.0, 5, K(picks=3, roi_cap="on", roi_cap_pct=30))
    waits = {c["symbol"]: c["waiting"] for c in plan_["pipeline"]}
    assert waits["UNDER.US"] == "12-month forecast 33.9% is at the cap (30%+): no new money"


def test_c5_the_10_nov_board_with_the_rule_on(monkeypatch):
    monkeypatch.setenv("TFB_PLAN_ROI_CAP", "on")
    p = plan()
    assert "ITRN.US" not in [c["symbol"] for c in p["picks"]]
    wait = {c["symbol"]: c["waiting"] for c in p["pipeline"]}
    assert wait["ITRN.US"] == "12-month forecast 34.7% is at the cap (34%+): no new money"
    wf = yaml.safe_load((ROOT / ".github/workflows/daily_plan.yml").read_text(encoding="utf-8"))
    env = [s for s in wf["jobs"]["daily_plan"]["steps"] if "daily_plan.py" in str(s.get("run", ""))][0]["env"]
    assert env["TFB_PLAN_ROI_CAP"] == "${{ vars.TFB_PLAN_ROI_CAP || 'on' }}"
    assert env["TFB_PLAN_ROI_CAP_PCT"] == "${{ vars.TFB_PLAN_ROI_CAP_PCT || '34' }}"


# ----------------------------------------------------------------------------- W (1.9.0) the cut waits for the broker
W_OLD = "the newest capture is 40 h old (limit 30 h)"
W_WAIT = ("No plan yet today: waiting for the broker's cash figure (%s). The plan is cut when an IBKR capture "
          "arrives, or from 11:00 Riyadh without it.")


def _ledger(b):
    tab = b.tabs.get("_Daily_Plan")
    return [r for r in (tab.values[1:] if tab else []) if r and r[0] == "2026-11-10"]


def _cut(b):
    return [(r[4], r[6], r[14], r[22], r[23]) for r in _ledger(b)]


def test_w1_the_reader_and_its_default(monkeypatch):
    monkeypatch.delenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", raising=False)
    assert dp.knobs()["ibkr_wait_h"] == 11
    for value, hour in (("0", 0), ("9", 9), ("23", 23), ("24", 11), ("-1", 11), ("x", 11), ("", 11)):
        monkeypatch.setenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", value)
        assert dp.knobs()["ibkr_wait_h"] == hour, value
    wf = yaml.safe_load((ROOT / ".github/workflows/daily_plan.yml").read_text(encoding="utf-8"))
    env = [s for s in wf["jobs"]["daily_plan"]["steps"] if "daily_plan.py" in str(s.get("run", ""))][0]["env"]
    assert env["TFB_PLAN_IBKR_WAIT_UNTIL_H"] == "${{ vars.TFB_PLAN_IBKR_WAIT_UNTIL_H || '11' }}"
    assert dp.__version__ == "1.11.0"                                 # 1.11.0 [RANKER] bumped it


def test_w2_sunday_15_nov_a_capture_too_old_at_the_cut_waits_and_the_next_capture_is_used(monkeypatch, capsys):
    """Run 43676720706: the newest capture was 32.5 h old at the 07:36 cut, so the plan was sized without the
    broker's figure and froze; a usable capture arrived an hour later."""
    monkeypatch.delenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", raising=False)
    b = book()
    assert dp.main([], book=b, now=NOW, capture=cap_of(age_h=40.0)) == 0            # 10:00 Riyadh, before 11:00
    out = capsys.readouterr().out
    assert _ledger(b) == [] and W_WAIT % W_OLD in out and "CUT " not in out and "cash for sizing" not in out
    assert any(W_WAIT % W_OLD in str(c) for r in b.tabs["Daily_Plan"].values for c in r)   # the page says why
    assert dp.main([], book=b, now=NOW + dt.timedelta(minutes=30), capture=cap_of(age_h=0.6)) == 0
    out = capsys.readouterr().out
    assert "| cash from the IBKR capture" in out and [r for r in _cut(b) if r[0] == "PICK"]
    assert not any("not the broker" in str(c) for r in _ledger(b) for c in r)
    same = book()
    monkeypatch.setenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", "0")
    assert dp.main([], book=same, now=NOW + dt.timedelta(minutes=30), capture=cap_of(age_h=0.6)) == 0
    assert _cut(same) == _cut(b)                                                     # a usable capture: the rule adds nothing
    frozen = _cut(b)
    monkeypatch.delenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", raising=False)
    assert dp.main([], book=b, now=NOW + dt.timedelta(minutes=45), capture=cap_of(age_h=40.0)) == 0
    assert _cut(b) == frozen                                                         # a plan that exists is never cut again


def test_w3_from_the_hour_the_cut_goes_ahead_and_says_where_the_cash_came_from(monkeypatch, capsys):
    monkeypatch.delenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", raising=False)
    at_11 = NOW + dt.timedelta(hours=1)
    b = book()
    assert dp.main([], book=b, now=at_11, capture=cap_of(age_h=40.0)) == 0
    out = capsys.readouterr().out
    assert "IBKR capture not used for sizing: " + W_OLD in out and "| cash from the panel" in out
    note = "cash from the typed cash cell, not the broker (%s)" % W_OLD
    picks = [r for r in _cut(b) if r[0] == "PICK"]
    assert picks and all(note in r[3] for r in picks)
    assert not any("not the broker" in r[4] for r in _cut(b) if r[0] == "PIPELINE")  # nothing waits for cash here
    before = book()
    monkeypatch.setenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", "0")
    assert dp.main([], book=before, now=at_11, capture=cap_of(age_h=40.0)) == 0
    strip = lambda rows: [(k, sym, q, lab.replace(" · " + note, "").replace(note, ""), w) for k, sym, q, lab, w in rows]
    assert strip(_cut(b)) == _cut(before)                                            # the label is the only difference
    # a fallback figure that leaves nothing above the floor (invented): every fundable name waits for cash
    monkeypatch.delenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", raising=False)
    poor = book(Top_10_Investments=Sheet(board(cash="2,500 SAR")))
    assert dp.main([], book=poor, now=at_11, capture=cap_of(age_h=40.0)) == 0
    rows = _cut(poor)
    waits = [r[4] for r in rows if r[0] == "PIPELINE" and r[4].startswith("cash:")]
    assert not [r for r in rows if r[0] == "PICK"] and waits and all(w.endswith(" · " + note) for w in waits)
    assert all("not the broker" not in r[4] for r in rows if not r[4].startswith("cash:"))


def test_w3b_the_cash_ledger_is_named_when_it_was_the_source(monkeypatch, capsys):
    monkeypatch.delenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", raising=False)
    b = book(_Cash_Snapshot=Sheet(SNAP))
    assert dp.main([], book=b, now=NOW + dt.timedelta(hours=1), capture=cap_of(age_h=40.0)) == 0
    assert "| cash from the snapshot (lower than the panel)" in capsys.readouterr().out
    note = "cash from the cash ledger, not the broker (%s)" % W_OLD
    rows = _cut(b)
    assert rows and all(note in r[3] for r in rows if r[0] == "PICK")
    assert all(r[4].endswith(" · " + note) for r in rows if r[0] == "PIPELINE" and r[4].startswith("cash:"))
    assert any(note in r[3] or note in r[4] for r in rows) and not any("typed cash cell" in str(c) for r in rows for c in r)


def test_w4_off_is_the_previous_behaviour(monkeypatch, capsys):
    b, ref = book(), book()                                                          # the fixture pins the rule off
    assert dp.main([], book=b, now=NOW, capture=cap_of(age_h=40.0)) == 0
    out = capsys.readouterr().out
    assert "waiting for the broker" not in out and "CUT 2026-11-10" in out
    assert dp.main([], book=ref, now=NOW) == 0
    assert _cut(b) == _cut(ref) and not any("not the broker" in str(c) for r in _ledger(b) for c in r)


def test_w5_only_a_capture_that_was_read_and_refused_makes_the_cut_wait(monkeypatch, capsys):
    monkeypatch.delenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", raising=False)
    nothing_read = book()
    assert dp.main([], book=nothing_read, now=NOW) == 0                              # no capture object at all
    assert [r for r in _cut(nothing_read) if r[0] == "PICK"]
    assert not any("not the broker" in str(c) for r in _ledger(nothing_read) for c in r)
    for bad in (cap_of(ok=False, reason="capture is not JSON"), cap_of(buys={"X.US": [(None, 5.0)]})):
        b = book()
        assert dp.main([], book=b, now=NOW, capture=bad) == 0 and _ledger(b) == []
    out = capsys.readouterr().out
    assert W_WAIT % "capture is not JSON" in out
    assert W_WAIT % "a working buy order has no limit price, so the cash it holds is not known" in out
    monkeypatch.setenv("TFB_PLAN_CASH_SOURCE", "lower")                              # the broker is not the source
    other = book()
    assert dp.main([], book=other, now=NOW, capture=cap_of(age_h=40.0)) == 0
    assert [r for r in _cut(other) if r[0] == "PICK"] and "waiting for the broker" not in capsys.readouterr().out


def test_w6_the_hour_is_a_knob_and_the_boundary_is_exact(monkeypatch):
    monkeypatch.setenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", "10")
    b = book()
    assert dp.main([], book=b, now=NOW - dt.timedelta(minutes=1), capture=cap_of(age_h=40.0)) == 0   # 09:59 Riyadh
    assert _ledger(b) == []
    assert dp.main([], book=b, now=NOW, capture=cap_of(age_h=40.0)) == 0                              # 10:00: the cut
    assert [r for r in _cut(b) if r[0] == "PICK"]


def test_w7_the_wait_log_line_carries_no_amount(monkeypatch, capsys):
    monkeypatch.delenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", raising=False)
    assert dp.main([], book=book(), now=NOW, capture=cap_of(age_h=40.0)) == 0
    out = capsys.readouterr().out
    assert "SAR" not in out and str(cap_of()["cash_usd"])[:4] not in out
    assert not __import__("re").search(r"\d,\d{3}", out)                              # no amount of any kind


def test_w8_a_live_run_with_the_reader_off_or_not_set_up_does_not_wait(monkeypatch, capsys):
    """Codex P2, #993: TFB_IBKR_CAPTURE=off answers with a refusal without reading anything; the cut is not held."""
    import scripts.ibkr_capture as ic
    import scripts.run_calendar_sync as rcs
    monkeypatch.delenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", raising=False)
    folder = "1AbCdEfGhIjKlMnOpQrStUvWxYz012345"
    for switch, fid, expected in (("on", folder, True), ("off", folder, False), ("on", "", False), ("on", "bad id", False)):
        monkeypatch.setenv("TFB_IBKR_CAPTURE", switch)
        monkeypatch.setenv("TFB_IBKR_CAPTURE_FOLDER_ID", fid)
        assert dp.capture_expected() is expected, (switch, fid)
    with monkeypatch.context() as m:
        m.setattr(ic, "enabled", lambda: 1 / 0)
        assert dp.capture_expected() is False                                        # never raises
    monkeypatch.setenv("TFB_IBKR_CAPTURE_FOLDER_ID", folder)
    # the reader is off: the real latest() refuses without a network call, and the plan is cut at once (as in 1.8.0)
    monkeypatch.setenv("TFB_IBKR_CAPTURE", "off")
    off = book()
    monkeypatch.setattr(rcs, "_open_book", lambda: off)
    assert dp.main([], now=NOW) == 0
    out = capsys.readouterr().out
    assert "IBKR capture not used for sizing: TFB_IBKR_CAPTURE is off" in out and "waiting for the broker" not in out
    assert [r for r in _cut(off) if r[0] == "PICK"] and not any("not the broker" in str(c) for r in _ledger(off) for c in r)
    # the reader is on and the newest capture is refused: a live run waits
    monkeypatch.setenv("TFB_IBKR_CAPTURE", "on")
    on = book()
    monkeypatch.setattr(rcs, "_open_book", lambda: on)
    monkeypatch.setattr(ic, "latest", lambda now=None, **kw: cap_of(age_h=40.0))
    assert dp.main([], now=NOW) == 0
    assert _ledger(on) == [] and W_WAIT % W_OLD in capsys.readouterr().out


# ----------------------------------------------------------------------------- Q (1.9.1) a price without a quote time
def test_q1_a_price_without_a_quote_time_reads_check_and_never_opens_or_invalidates():
    """Review of 16 Nov: a replay of the status refresh returned OPEN for a price whose quote time was missing."""
    row = {"symbol": "A.US", "stop": 9.0, "zone_low": 9.7, "zone_high": 10.1}
    now = dt.datetime(2026, 11, 17, 9, 0, tzinfo=UTC)
    base = (row, 10.0, {}, dt.date(2026, 11, 17), None, 7, "VERIFIED")
    assert dp.pick_status(*base, None, now, 96)[0] == "OPEN"                         # the function's default: 1.9.0
    st, why = dp.pick_status(*base, None, now, 96, True)
    assert st == "CHECK" and why == "the price 10.00 has no quote time: not shown to be current"
    under = (row, 8.0, {}, dt.date(2026, 11, 17), None, 7, "VERIFIED")
    assert dp.pick_status(*under, None, now, 96, True)[0] == "CHECK"                 # no time: it cannot invalidate either
    assert dp.pick_status(*under, now - dt.timedelta(hours=1), now, 96, True)[0] == "INVALIDATED"
    assert dp.pick_status(*base, now - dt.timedelta(hours=1), now, 96, True)[0] == "OPEN"   # a timed, fresh price opens
    assert dp.pick_status(*base, now - dt.timedelta(hours=200), now, 96, True)[1].startswith("the price 10.00 is 8 days old")
    assert dp.pick_status(*base, None, now, 0, True)[0] == "OPEN"                    # age limit off: no time is asked for
    assert dp.pick_status(*base, None, None, 96, True)[0] == "OPEN"                  # no clock given: nothing to compare
    # the rules that come first keep their place
    assert dp.pick_status(row, 10.0, {"A.US": dt.date(2026, 11, 17)}, dt.date(2026, 11, 17), None, 7, "VERIFIED",
                          None, now, 96, True)[0] == "EXECUTED"
    assert dp.pick_status(row, 10.0, {}, dt.date(2026, 11, 17), 3, 7, "VERIFIED", None, now, 96, True)[0] == "INVALIDATED"
    assert dp.pick_status(row, None, {}, dt.date(2026, 11, 17), None, 7, "VERIFIED", None, now, 96, True)[1] == "no current price"


def test_q2_the_knob_its_default_and_the_workflow(monkeypatch):
    monkeypatch.delenv("TFB_PLAN_QUOTE_TIME", raising=False)
    assert dp.knobs()["quote_time"] is True
    for value, want in (("required", True), ("REQUIRED", True), ("", True), ("x", True), ("optional", False),
                        ("Optional", False), ("off", False), ("0", False)):
        monkeypatch.setenv("TFB_PLAN_QUOTE_TIME", value)
        assert dp.knobs()["quote_time"] is want, value
    wf = yaml.safe_load((ROOT / ".github/workflows/daily_plan.yml").read_text(encoding="utf-8"))
    env = [s for s in wf["jobs"]["daily_plan"]["steps"] if "daily_plan.py" in str(s.get("run", ""))][0]["env"]
    assert env["TFB_PLAN_QUOTE_TIME"] == "${{ vars.TFB_PLAN_QUOTE_TIME || 'required' }}"


def test_q3_the_status_refresh_of_a_frozen_plan(monkeypatch):
    """A whole run: the page price of a pick has no time. Required: CHECK, levels untouched. Optional: 1.9.0."""
    def statuses(b):
        return dict((r[6], (r[24], r[25])) for r in _ledger(b) if r[4] == "PICK")

    b = book()
    assert dp.main([], book=b, now=NOW) == 0                                         # the cut (fixture: optional)
    before = [r[:24] for r in _ledger(b)]
    assert all(st == "OPEN" for st, _ in statuses(b).values())
    monkeypatch.setenv("TFB_PLAN_QUOTE_TIME", "required")
    assert dp.main([], book=b, now=NOW + dt.timedelta(hours=1)) == 0
    got = statuses(b)
    assert got and all(st == "CHECK" and why.endswith("has no quote time: not shown to be current") for st, why in got.values())
    assert [r[:24] for r in _ledger(b)] == before                                    # the frozen plan never moves
    timed = [SRC[0] + ["Last Updated (UTC)"]] + [r + [(NOW - dt.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")] for r in SRC[1:]]
    b.tabs["Global_Markets"] = Sheet(timed)
    assert dp.main([], book=b, now=NOW + dt.timedelta(hours=2)) == 0                 # the time is there again: OPEN again
    assert all(st == "OPEN" for st, _ in statuses(b).values())
    assert [r[:24] for r in _ledger(b)] == before



# ----------------------------------------------------------------------------- D (1.9.2) a day order read before the US close
D_SEEN = NOW - dt.timedelta(hours=11)                                    # 23:00 Riyadh the evening before: the US session is open
D_WHY = ("the newest capture was read before the last US close and lists a day order to buy (GGG.US) that may have "
         "filled or expired")


def day_cap(**over):
    """An evening capture with one working day order to buy 40 GGG.US at 19.50 (invented figures)."""
    c = cap_of(age_h=11.0, buys={"GGG.US": [(19.5, 40.0)]})
    c.update({"as_of": D_SEEN, "day_buys": {"GGG.US": 40.0}})
    c.update(over)
    return c


def test_d1_the_reader_its_default_and_the_workflow(monkeypatch):
    monkeypatch.delenv("TFB_PLAN_DAY_ORDER", raising=False)
    assert dp.knobs()["day_order"] is True
    for value, on in (("off", False), (" OFF ", False), ("0", False), ("false", False), ("no", False), ("on", True),
                      ("junk", True), ("", True)):
        monkeypatch.setenv("TFB_PLAN_DAY_ORDER", value)
        assert dp.knobs()["day_order"] is on, value
    wf = yaml.safe_load((ROOT / ".github/workflows/daily_plan.yml").read_text(encoding="utf-8"))
    env = [s for s in wf["jobs"]["daily_plan"]["steps"] if "daily_plan.py" in str(s.get("run", ""))][0]["env"]
    assert env["TFB_PLAN_DAY_ORDER"] == "${{ vars.TFB_PLAN_DAY_ORDER || 'on' }}"


def test_d2_a_day_order_read_before_a_close_that_has_passed_gives_no_cash_figure():
    held = (4679.15 - 19.5 * 40.0) * 3.75
    assert dp.capture_cash(day_cap(), 30, NOW, True) == (None, D_WHY)
    # the same capture while that session is still open: the order holds its cash, as before
    assert dp.capture_cash(day_cap(), 30, D_SEEN + dt.timedelta(minutes=30), True)[0] == pytest.approx(held)
    # read after the close, a GTC buy, no "day_buys" key, a zero quantity, no capture time: used as in 1.9.1
    assert dp.capture_cash(day_cap(as_of=NOW - dt.timedelta(hours=2)), 30, NOW, True)[0] == pytest.approx(held)
    assert dp.capture_cash(day_cap(day_buys={}), 30, NOW, True)[0] == pytest.approx(held)
    gtc = day_cap()
    del gtc["day_buys"]
    assert dp.capture_cash(gtc, 30, NOW, True)[0] == pytest.approx(held)
    assert dp.capture_cash(day_cap(day_buys={"GGG.US": 0.0}), 30, NOW, True)[0] == pytest.approx(held)
    assert dp.capture_cash(day_cap(as_of=None), 30, NOW, True)[0] == pytest.approx(held)
    # the rule off, or a caller of 1.9.1 that passes neither argument: the 1.9.1 figure
    assert dp.capture_cash(day_cap(), 30, NOW, False)[0] == pytest.approx(held)
    assert dp.capture_cash(day_cap(), 30)[0] == pytest.approx(held)
    # the older refusals keep their own words and come first
    assert dp.capture_cash(day_cap(age_h=31.0), 30, NOW, True) == (None, "the newest capture is 31 h old (limit 30 h)")
    two = day_cap(day_buys={"ZZZ.US": 5.0, "GGG.US": 40.0})
    assert "(GGG.US, ZZZ.US)" in dp.capture_cash(two, 30, NOW, True)[1]
    # a weekend: Friday evening's capture, read again on Sunday morning - Friday's close lies between
    fri = dt.datetime(2026, 11, 13, 20, 0, tzinfo=UTC)
    assert dp.capture_cash(day_cap(as_of=fri, age_h=29.0), 30, dt.datetime(2026, 11, 15, 4, 30, tzinfo=UTC), True)[0] is None


def test_d3_the_cut_waits_for_the_next_capture_and_then_sizes_from_it(monkeypatch, capsys):
    """17 Nov: the 07:31 cut sized from the 23:05 capture, whose day order had expired at the close an hour later;
    every candidate read 'cash: 0 SAR per pick' and the plan froze before the 08:41 capture."""
    monkeypatch.delenv("TFB_PLAN_DAY_ORDER", raising=False)
    monkeypatch.delenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", raising=False)
    b = book()
    assert dp.main([], book=b, now=NOW, capture=day_cap()) == 0
    out = capsys.readouterr().out
    assert _ledger(b) == [] and W_WAIT % D_WHY in out and "CUT " not in out
    assert any(W_WAIT % D_WHY in str(c) for r in b.tabs["Daily_Plan"].values for c in r)      # the page says why
    assert dp.main([], book=b, now=NOW + dt.timedelta(minutes=40), capture=cap_of(age_h=0.2)) == 0
    assert "| cash from the IBKR capture" in capsys.readouterr().out
    fresh = book()
    assert dp.main([], book=fresh, now=NOW + dt.timedelta(minutes=40), capture=cap_of(age_h=0.2)) == 0
    assert _cut(b) == _cut(fresh) and [r for r in _cut(b) if r[0] == "PICK"]                  # the full cash sized it
    # GOLDEN NEGATIVE: the rule off = 1.9.1 - the cut goes ahead at once on the cash net of the day order
    monkeypatch.setenv("TFB_PLAN_DAY_ORDER", "off")
    old = book()
    assert dp.main([], book=old, now=NOW, capture=day_cap()) == 0
    out = capsys.readouterr().out
    assert "| cash from the IBKR capture" in out and "waiting for the broker" not in out and _ledger(old)
    qty = lambda bk: sum(float(r[14] or 0) * float(r[11] or 0) for r in bk.tabs["_Daily_Plan"].values[1:] if r[4] == "PICK")
    assert qty(old) < qty(b)                                                                  # smaller: the order held cash


# ----------------------------------------------------------------------------- G (1.10.0) market regime and held sectors
STRESS = {"ok": True, "label": "stress", "date": dt.date(2026, 11, 17),
          "text": "STRESS as of 2026-11-17 (S&P 500 fund -1.0% on the day, 4.0% under its 50-day average, VIX 25.5)"}
CALM = {"ok": True, "label": "calm", "date": dt.date(2026, 11, 17), "text": "CALM as of 2026-11-17 (VIX 15.0)"}
BLOCK = ["weak", "stress", "drawdown", "crash_day"]


def test_g1_regime_rule_is_pure_and_never_blocks_unread():
    assert dp.regime_rule(STRESS, "off", BLOCK, ["USD"]) == {"mode": "off", "blocks": False, "ccy": ("USD",),
                                                             "words": "", "note": ""}
    r = dp.regime_rule(STRESS, "enforce", BLOCK, ["USD"])
    assert r["blocks"] and r["words"] == ("US market is STRESS as of 2026-11-17 (S&P 500 fund -1.0% on the day, 4.0% "
                                          "under its 50-day average, VIX 25.5): no new US buy until the market is calm")
    assert r["note"].startswith("US market regime: STRESS as of 2026-11-17")
    assert not dp.regime_rule(CALM, "enforce", BLOCK, ["USD"])["blocks"]
    assert not dp.regime_rule(STRESS, "enforce", ["crash_day"], ["USD"])["blocks"]     # the label is not on the list
    for unread in (None, {}, {"ok": False, "why": "URLError"}):
        u = dp.regime_rule(unread, "enforce", BLOCK, ["USD"])
        assert not u["blocks"] and u["note"].startswith("market regime not read (") and "not applied" in u["note"]
    assert dp.regime_rule({"ok": False, "why": "URLError"}, "enforce", BLOCK, ["USD"])["note"] == \
        "market regime not read (URLError): the rule is not applied"


def _g_decide(k):
    q = [qrow(1, "AAA.US", "Technology", 30, ""), qrow(2, "1120.SR", "Financials", 30, ""),
         qrow(3, "BBB.US", "Energy", 30, ""), qrow(4, "DDI.US", "Technology", 30, "")]
    src = [SRC[0], ["AAA.US", "", "USD", "", "10", ""], ["1120.SR", "", "SAR", "", "90", ""],
           ["BBB.US", "", "USD", "", "20", ""], ["DDI.US", "", "USD", "", "15.7", ""]]
    return dp.decide(dp.read_qualified(board(qual=q)), dp.read_sources([src]), {}, lots(), 33125.0, 2, k)


def test_g2_enforce_holds_new_us_buys_only(monkeypatch):
    base = _g_decide(K())
    assert [c["symbol"] for c in base["picks"]] == ["AAA.US", "1120.SR", "BBB.US"]     # the gate is off by default
    k = K(regime=dp.regime_rule(STRESS, "enforce", BLOCK, ["USD"]))
    p = _g_decide(k)
    assert [c["symbol"] for c in p["picks"]] == ["1120.SR"]                         # a SAR name is not a US buy
    wait = {c["symbol"]: c["waiting"] for c in p["pipeline"]}
    assert wait["AAA.US"].startswith("US market is STRESS as of 2026-11-17") and wait["AAA.US"].endswith(
        "no new US buy until the market is calm") and wait["BBB.US"] == wait["AAA.US"]
    assert wait["DDI.US"] == "already held"                                         # an earlier reason keeps its words
    # observe: the same words as a label, every pick unchanged
    o = _g_decide(K(regime=dp.regime_rule(STRESS, "observe", BLOCK, ["USD"])))
    assert [c["symbol"] for c in o["picks"]] == [c["symbol"] for c in base["picks"]]
    us = [c for c in o["picks"] if c["symbol"] == "AAA.US"][0]
    assert any(lab.startswith("[observe] US market is STRESS") for lab in us["labels"])
    assert not any("US market" in lab for c in o["picks"] if c["symbol"] == "1120.SR" for lab in c["labels"])
    # calm, or a regime that was not read: nothing changes
    for reg in (CALM, None):
        same = _g_decide(K(regime=dp.regime_rule(reg, "enforce", BLOCK, ["USD"])))
        assert [c["symbol"] for c in same["picks"]] == [c["symbol"] for c in base["picks"]]
    # the currencies are a setting
    both = _g_decide(K(regime=dp.regime_rule(STRESS, "enforce", BLOCK, ["USD", "SAR"])))
    assert both["picks"] == []
    # knobs
    assert dp.knobs()["regime_mode"] == "off" and dp.knobs()["regime_block"] == BLOCK and dp.knobs()["regime_ccy"] == ["USD"]
    monkeypatch.setenv("TFB_PLAN_REGIME", "Enforce")
    monkeypatch.setenv("TFB_PLAN_REGIME_BLOCK", "stress, calm, nonsense ,CRASH_DAY")
    monkeypatch.setenv("TFB_PLAN_REGIME_CCY", "usd, sar")
    k = dp.knobs()
    assert (k["regime_mode"], k["regime_block"], k["regime_ccy"]) == ("enforce", ["stress", "crash_day"], ["USD", "SAR"])
    monkeypatch.setenv("TFB_PLAN_REGIME", "maybe")
    assert dp.knobs()["regime_mode"] == "off"


def test_g3_sector_limit_counts_what_is_held(monkeypatch):
    q = [qrow(1, "TEC1.US", "Technology", 30, ""), qrow(2, "ENE1.US", "Energy", 30, ""),
         qrow(3, "TEC2.US", "Technology", 30, "")]
    src = [SRC[0], ["TEC1.US", "", "USD", "", "10", ""], ["ENE1.US", "", "USD", "", "20", ""],
           ["TEC2.US", "", "USD", "", "30", ""], ["DDI.US", "", "USD", "Technology", "15.7", ""]]
    args = (dp.read_qualified(board(qual=q)), dp.read_sources([src]), {}, lots(), 33125.0, 2)
    assert "DDI.US" in lots()["held"]
    assert [c["symbol"] for c in dp.decide(*args, K())["picks"]] == ["TEC1.US", "ENE1.US", "TEC2.US"]   # off (0)
    p = dp.decide(*args, K(sector_held_max=2))                                     # one held + one picked = 2
    assert [c["symbol"] for c in p["picks"]] == ["TEC1.US", "ENE1.US"]
    assert {c["symbol"]: c["waiting"] for c in p["pipeline"]}["TEC2.US"] == \
        "sector limit with holdings (Technology: 1 held + 1 picked; the limit is 2)"
    p = dp.decide(*args, K(sector_held_max=1))                                     # the held name alone fills it
    assert [c["symbol"] for c in p["picks"]] == ["ENE1.US"]
    assert {c["symbol"]: c["waiting"] for c in p["pipeline"]}["TEC1.US"] == \
        "sector limit with holdings (Technology: 1 held + 0 picked; the limit is 1)"
    assert [c["symbol"] for c in dp.decide(*args, K(sector_held_max=3))["picks"]] == ["TEC1.US", "ENE1.US", "TEC2.US"]
    # a held name the source pages do not list is not counted
    nosec = (args[0], dp.read_sources([src[:4]]), *args[2:])
    assert [c["symbol"] for c in dp.decide(*nosec, K(sector_held_max=2))["picks"]] == ["TEC1.US", "ENE1.US", "TEC2.US"]
    # with nothing held the limit still counts the day's picks (review of #1022): 0 held + 1 picked reaches 1
    one = dp.decide(*nosec, K(sector_held_max=1))
    assert [c["symbol"] for c in one["picks"]] == ["TEC1.US", "ENE1.US"]
    assert {c["symbol"]: c["waiting"] for c in one["pipeline"]}["TEC2.US"] == \
        "sector limit with holdings (Technology: 0 held + 1 picked; the limit is 1)"
    monkeypatch.setenv("TFB_PLAN_SECTOR_HELD_MAX", "3")
    assert dp.knobs()["sector_held_max"] == 3
    monkeypatch.setenv("TFB_PLAN_SECTOR_HELD_MAX", "x")
    assert dp.knobs()["sector_held_max"] == 0


def test_g4_the_run_reads_the_regime_once_and_says_what_it_did(monkeypatch, capsys):
    monkeypatch.setenv("TFB_PLAN_REGIME", "enforce")
    b = book()
    assert dp.main([], book=b, now=NOW, regime=STRESS) == 0
    out = capsys.readouterr().out
    assert "US market regime: STRESS as of 2026-11-17" in out and out.count("US market regime:") == 1
    rows = dp.parse_ledger(b.tabs["_Daily_Plan"].values)
    us = [d for d in rows if text_ccy(d) == "USD"]
    assert us and all(d["Kind"] == "PIPELINE" and "no new US buy until the market is calm" in d["Waiting For"] for d in us
                      if d["Waiting For"].startswith("US market"))
    assert not [d for d in rows if d["Kind"] == "PICK" and text_ccy(d) == "USD"]
    # a regime that cannot be read does not stop the plan: the picks say the rule was not applied
    b2 = book()
    assert dp.main([], book=b2, now=NOW, regime={"ok": False, "why": "URLError"}) == 0
    rows2 = dp.parse_ledger(b2.tabs["_Daily_Plan"].values)
    picks2 = [d for d in rows2 if d["Kind"] == "PICK"]
    assert picks2 and all("market regime not read (URLError): the rule is not applied" in d["Labels"] for d in picks2)
    # the workflow turns both rules on
    wf = yaml.safe_load((ROOT / ".github/workflows/daily_plan.yml").read_text(encoding="utf-8"))
    env = [s for s in wf["jobs"]["daily_plan"]["steps"] if "daily_plan.py" in str(s.get("run", ""))][0]["env"]
    assert env["TFB_PLAN_REGIME"] == "${{ vars.TFB_PLAN_REGIME || 'enforce' }}"
    assert env["TFB_PLAN_REGIME_BLOCK"] == "${{ vars.TFB_PLAN_REGIME_BLOCK || 'weak,stress,drawdown,crash_day' }}"
    assert env["TFB_PLAN_REGIME_CCY"] == "${{ vars.TFB_PLAN_REGIME_CCY || 'USD' }}"
    assert env["TFB_PLAN_SECTOR_HELD_MAX"] == "${{ vars.TFB_PLAN_SECTOR_HELD_MAX || '3' }}"


def text_ccy(d):
    return str(d.get("Ccy", "")).strip().upper()


def test_g4_sector_limits_count_one_vocabulary():
    """1.10.1: a held bank listed as 'Financial Services' on the pages and a board candidate listed as 'Financials'
    are one sector; so are a page-only 'Healthcare' row and a board 'Health Care' row for the day's own cap."""
    q = [qrow(1, "BNK1.US", "Financials", 30, ""), qrow(2, "ENE1.US", "Energy", 30, ""),
         qrow(3, "BNK2.US", "Financials", 30, "")]
    src = [SRC[0], ["BNK1.US", "", "USD", "", "10", ""], ["ENE1.US", "", "USD", "", "20", ""],
           ["BNK2.US", "", "USD", "", "30", ""], ["DDI.US", "", "USD", "Financial Services", "15.7", ""]]
    args = (dp.read_qualified(board(qual=q)), dp.read_sources([src]), {}, lots(), 33125.0, 2)
    p = dp.decide(*args, K(sector_held_max=2))                       # held bank (pages' name) + one picked = 2
    assert [c["symbol"] for c in p["picks"]] == ["BNK1.US", "ENE1.US"]
    assert {c["symbol"]: c["waiting"] for c in p["pipeline"]}["BNK2.US"] == \
        "sector limit with holdings (Financials: 1 held + 1 picked; the limit is 2)"
    p = dp.decide(*args, K(sector_held_max=1))                       # the held bank alone fills it
    assert [c["symbol"] for c in p["picks"]] == ["ENE1.US"]
    # the day's own cap: a board row without a sector takes the pages' spelling and still counts with its GICS twin
    q2 = [qrow(1, "HC1.US", "Health Care", 30, ""), qrow(2, "HC2.US", "", 30, "")]
    src2 = [SRC[0], ["HC1.US", "", "USD", "", "10", ""], ["HC2.US", "", "USD", "Healthcare", "20", ""]]
    p2 = dp.decide(dp.read_qualified(board(qual=q2)), dp.read_sources([src2]), {}, lots(), 33125.0, 1, K())
    assert [c["symbol"] for c in p2["picks"]] == ["HC1.US"]
    assert {c["symbol"]: c["waiting"] for c in p2["pipeline"]}["HC2.US"] == "sector cap (Healthcare already has 1)"
    # the fold is the shared one and leaves every other name alone
    from core.sectors import YAHOO_TO_GICS_SECTOR
    assert dp.SECTOR_FOLD == YAHOO_TO_GICS_SECTOR
    assert dp.sector_key(" Financial Services ") == "Financials" and dp.sector_key("Real Estate") == "Real Estate"
    assert dp.sector_key(None) == "" and dp.sector_key("") == ""



# ----------------------------------------------------------------------------- Q1 (1.10.2 [QUOTA] [LOG-PRIVACY])
class _Quota(Exception):
    def __str__(self):
        return "APIError: [429]: Quota exceeded for quota metric 'Read requests'"


def test_q1_a_quota_refusal_waits_into_the_next_minute_and_the_write_is_tried_again(monkeypatch, capsys):
    """23 Nov 05:21 UTC: three reads 20 s apart all fell inside the same quota minute; 22 Nov 20:29 UTC the write
    met the quota. A read now waits 65 s after a quota answer, and the page writes are tried again on one."""
    naps = []
    monkeypatch.setattr(dp, "_SLEEP", naps.append)
    b = book()
    real_get = b.worksheet(dp.PAGE).get
    calls = {"n": 0}

    def flaky_get(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise _Quota()
        return real_get(*a, **kw)
    monkeypatch.setattr(b.worksheet(dp.PAGE), "get", flaky_get)
    real_write = dp.write_grid
    wrote = {"n": 0}

    def flaky_write(*a, **kw):
        wrote["n"] += 1
        if wrote["n"] == 1:
            raise _Quota()
        return real_write(*a, **kw)
    monkeypatch.setattr(dp, "write_grid", flaky_write)
    assert dp.main([], book=b, now=NOW, capture=cap_of(age_h=0.6)) == 0
    out = capsys.readouterr().out
    assert "retrying in 65 s" in out and "Sheets quota reached while writing the Daily Plan: waiting 65 s" in out
    assert naps[:2] == [65.0, 65.0] and "wrote " in out


def test_q1_the_run_log_names_no_pick_on_actions(monkeypatch, capsys):
    monkeypatch.delenv("TFB_PLAN_LOG_DETAIL")
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    assert dp.log_detail() is True                                                   # a local run: the full lines
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    for raw, want in (("on", True), ("1", True), ("off", False), ("auto", False), ("", False), ("maybe", False)):
        monkeypatch.setenv("TFB_PLAN_LOG_DETAIL", raw)
        assert dp.log_detail() is want, raw
    monkeypatch.delenv("TFB_PLAN_LOG_DETAIL")
    b = book()
    assert dp.main([], book=b, now=NOW, capture=cap_of(age_h=0.6)) == 0
    out = capsys.readouterr().out
    picks = [r for r in _cut(b) if r[0] == "PICK"]
    assert picks and re.search(r"plan pick 1 of %d: " % len(picks), out)
    assert " qty " not in out and "  PICK " not in out
    for r in _ledger(b):
        if r[4] == "PICK":
            assert ("%s " % r[6]) not in out, "the symbol is not in the log"



# ----------------------------------------------------------------------------- Q2 (1.10.3, holding_read 1.0.2, exit_plan 1.6.3)
@pytest.mark.parametrize("modname", ["scripts.daily_plan", "scripts.holding_read", "scripts.exit_plan"])
def test_q2_opening_the_workbook_waits_out_the_quota(monkeypatch, capsys, modname):
    """Run 45123566985 (23 Nov): "cannot open workbook: APIError" - the open is a metadata read under the same quota."""
    import importlib
    from scripts import run_calendar_sync as rcs
    mod = importlib.import_module(modname)
    naps, calls = [], {"n": 0}
    monkeypatch.setattr(mod, "_SLEEP", naps.append)

    def refuse(*a, **kw):
        calls["n"] += 1
        raise _Quota()
    monkeypatch.setattr(rcs, "_open_book", refuse)
    for gate in ("TFB_DAILY_PLAN", "TFB_HOLDING_READ", "TFB_EXIT_PLAN"):
        monkeypatch.setenv(gate, "on")
    if modname.endswith("daily_plan"):
        code, _ = mod.run_once(True, None, NOW, None, None)
    else:
        code = mod.run(True, book=None, now=NOW) if modname.endswith("holding_read") else mod.run(True, False, book=None,
                                                                                                    now=NOW)
    out = capsys.readouterr().out
    assert code == 2 and calls["n"] == 3 and naps == [65.0, 65.0]               # three tries, two waits into new minutes
    assert "Sheets quota reached while opening the workbook: waiting 65 s, then try 1 of 2" in out
    assert "cannot open workbook: _Quota [429]" in out


# ----------------------------------------------------------------------------- R (1.11.0, the ranker)
def _syms(items):
    return [c["symbol"] for c in items]


def _labels(p, prefix):
    return {c["symbol"]: [x for x in c["labels"] if x.startswith(prefix)] for c in p["picks"] + p["pipeline"]}


def test_r1_the_ranker_knob_and_the_pure_helpers(monkeypatch):
    assert dp.ranker_mode() == "engine" and K()["ranker"] == "engine" and K()["ranker_ccy"] == ["USD"]
    for word, want in (("observe", "observe"), ("UPSIDE", "upside"), ("engine", "engine"), ("on", "engine"),
                       ("", "engine")):
        monkeypatch.setenv("TFB_PLAN_RANKER", word)
        assert dp.ranker_mode() == want, word
    monkeypatch.setenv("TFB_PLAN_RANKER_CCY", " usd , sar ")
    assert dp.knobs()["ranker_ccy"] == ["USD", "SAR"]
    monkeypatch.setenv("TFB_PLAN_RANKER_CCY", " , ")
    assert dp.knobs()["ranker_ccy"] == ["USD"]
    up = dp.analyst_upside
    assert up({"price": 80.0, "target": 101.5}) == 26.875 and up({"price": "51.82", "target": "74.5"}) == 43.7669
    for src in ({}, {"price": 10.0}, {"target": 12.0}, {"price": 0, "target": 12.0}, {"price": 10.0, "target": 0},
                {"price": -1.0, "target": 12.0}):
        assert up(src) is None, src
    c = lambda sym, rank, ccy, u, sec="S": {"symbol": sym, "rank": rank, "ccy": ccy, "upside": u, "sector": sec}
    order = dp.upside_order([c("A", 1, "USD", 5.0), c("B", 2, "SAR", 90.0), c("C", 3, "USD", 30.0),
                             c("D", 4, "USD", None), c("E", 5, "USD", 30.0)], ["USD"])
    assert [x["symbol"] for x in order] == ["C", "E", "A", "B", "D"]       # ties by engine rank; the rest by rank
    picks = dp.would_pick([c("A", 1, "USD", 9, "Energy"), c("B", 2, "USD", 8, "Energy"), c("C", 3, "USD", 7, "Energy"),
                           c("D", 4, "USD", 6, "Banks")], 3, 2)
    assert [x["symbol"] for x in picks] == ["A", "B", "D"]                   # the day's sector cap, no other check


def test_r2_engine_is_the_default_and_adds_nothing(monkeypatch):
    p = plan(src=SRC_EV, cal=CAL_EV)
    assert _syms(p["picks"]) == ["ITRN.US", "TEN.US", "ZTO.US"]
    assert all(not x.startswith("[observe] analyst") and not x.startswith("ranked by analyst")
               for c in p["picks"] + p["pipeline"] for x in c["labels"])
    assert p["ranker"] == {"mode": "engine", "candidates": 7, "with_upside": 6, "alt": []}   # counted, not used
    nxt = {c["symbol"]: c["waiting"] for c in p["pipeline"]}
    assert nxt["SBLK.US"] == "next in line (rank 5)" and nxt["NMM.US"] == "next in line (rank 4)"


def test_r3_observe_changes_no_pick_and_labels_every_candidate(monkeypatch):
    base = plan(src=SRC_EV, cal=CAL_EV)
    monkeypatch.setenv("TFB_PLAN_RANKER", "observe")
    p = plan(src=SRC_EV, cal=CAL_EV)
    assert _syms(p["picks"]) == _syms(base["picks"]) == ["ITRN.US", "TEN.US", "ZTO.US"]
    for key in ("qty", "entry", "stop", "alloc_sar"):
        assert [c[key] for c in p["picks"]] == [c[key] for c in base["picks"]], key
    lab = _labels(p, "[observe]")
    assert lab["ITRN.US"] == ["[observe] analyst upside +44% - upside order 1 of 7",
                              "[observe] the upside ranker would pick this (before the cash and held-sector checks)"]
    assert lab["TEN.US"][0] == "[observe] analyst upside +40% - upside order 2 of 7" and len(lab["TEN.US"]) == 2
    assert lab["NMM.US"][0] == "[observe] analyst upside +28% - upside order 3 of 7" and len(lab["NMM.US"]) == 2
    assert lab["ZTO.US"] == ["[observe] analyst upside +6% - upside order 6 of 7"]          # picked, but not by upside
    assert lab["LADR.US"] == ["[observe] no analyst upside (no target price on its source page)"]
    assert lab["BBOX.L"] == []                                       # stopped earlier by its currency: not a candidate
    assert p["ranker"] == {"mode": "observe", "candidates": 7, "with_upside": 6, "alt": ["ITRN.US", "TEN.US", "NMM.US"]}


def test_r4_upside_orders_the_us_candidates_by_the_analyst_target(monkeypatch):
    monkeypatch.setenv("TFB_PLAN_RANKER", "upside")
    p = plan(src=SRC_EV, cal=CAL_EV)
    assert _syms(p["picks"]) == ["ITRN.US", "TEN.US", "NMM.US"]
    lab = _labels(p, "ranked by analyst upside")
    assert lab["ITRN.US"] == ["ranked by analyst upside +44% (engine rank 1)"]
    assert lab["NMM.US"] == ["ranked by analyst upside +28% (engine rank 4)"]
    assert all(not v for s, v in lab.items() if s not in _syms(p["picks"]))   # only picks carry it
    wait = {c["symbol"]: c["waiting"] for c in p["pipeline"]}
    assert wait["SBLK.US"] == "next in line (upside order 4; engine rank 5)"
    assert wait["ZTO.US"] == "next in line (upside order 6; engine rank 3)"     # engine rank 3, upside +6%: demoted
    assert wait["LADR.US"] == "next in line (upside order 7; engine rank 6)"     # no upside: after the ranked names
    assert wait["BBOX.L"].startswith("currency GBX")                            # earlier gates keep their own reason
    assert all(c["qty"] > 0 and c["stop"] < c["entry"] for c in p["picks"])
    assert p["ranker"]["mode"] == "upside" and p["ranker"]["alt"] == []


def test_r5_upside_keeps_the_plans_own_rules(monkeypatch):
    monkeypatch.setenv("TFB_PLAN_RANKER", "upside")
    kept = plan(src=SRC_EV, cal=CAL_EV, prior=("ZTO.US",))                     # yesterday's pick keeps its place
    assert _syms(kept["picks"])[0] == "ZTO.US" and "kept from the previous plan" in kept["picks"][0]["labels"]
    k = K(ranker="upside", ranker_ccy=["SAR"])                                  # no USD name is ranked by upside
    sar = plan(src=SRC_EV, cal=CAL_EV, k=k)
    assert _syms(sar["picks"]) == ["ITRN.US", "TEN.US", "ZTO.US"] and sar["ranker"]["with_upside"] == 0
    assert not any(x.startswith("ranked by analyst") for c in sar["picks"] for x in c["labels"])
    assert {c["symbol"]: c["waiting"] for c in sar["pipeline"]}["NMM.US"] == "next in line (upside order 4; engine rank 4)"
    k = K(ranker="upside", regime=dp.regime_rule(STRESS, "enforce", BLOCK, ["USD"]))
    blocked = plan(src=SRC_EV, cal=CAL_EV, k=k)                                 # the regime gate still holds US buys
    assert blocked["picks"] == [] and all(c["waiting"] for c in blocked["pipeline"])


def test_r6_the_run_log_gives_counts_only(monkeypatch, capsys):
    monkeypatch.setenv("TFB_PLAN_RANKER", "observe")
    monkeypatch.setenv("TFB_PLAN_LOG_DETAIL", "off")
    b = book(Global_Markets=Sheet(SRC_EV), Calendar_Events=Sheet(CAL_EV))
    assert dp.main([], book=b, now=NOW) == 0
    out = capsys.readouterr().out
    line = [x for x in out.splitlines() if "ranker observe:" in x]
    assert len(line) == 1 and line[0].endswith(
        "ranker observe: 6 of 7 candidate(s) carry an analyst upside; the upside order would pick 3 name(s), 2 of them "
        "among today's 3 pick(s)")
    for sym in ("ITRN", "TEN", "ZTO", "NMM", "SBLK", "LADR"):
        assert sym not in line[0], sym
    monkeypatch.setenv("TFB_PLAN_RANKER", "engine")
    assert dp.main([], book=book(Global_Markets=Sheet(SRC_EV), Calendar_Events=Sheet(CAL_EV)), now=NOW) == 0
    assert "ranker" not in capsys.readouterr().out


def test_r7_the_workflow_arms_observe_and_the_header_says_why():
    wf = yaml.safe_load((ROOT / ".github/workflows/daily_plan.yml").read_text(encoding="utf-8"))
    env = [s for s in wf["jobs"]["daily_plan"]["steps"] if "daily_plan.py" in str(s.get("run", ""))][0]["env"]
    assert env["TFB_PLAN_RANKER"] == "${{ vars.TFB_PLAN_RANKER || 'observe' }}"
    assert env["TFB_PLAN_RANKER_CCY"] == "${{ vars.TFB_PLAN_RANKER_CCY || 'USD' }}"
    src = (ROOT / "scripts/daily_plan.py").read_text(encoding="utf-8")
    assert dp.__version__ == "1.11.0" and "1.11.0 (2026-11-28): THE RANKER CAN BE THE ANALYST UPSIDE" in src
    assert '("daily_plan", "1.11.0", "daily plan")' in (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
