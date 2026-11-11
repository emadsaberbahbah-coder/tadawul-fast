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
    monkeypatch.setenv("TFB_DAILY_PLAN", "on")


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
