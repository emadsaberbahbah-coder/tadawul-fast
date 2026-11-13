#!/usr/bin/env python3
"""tests/test_consistency_check.py - scripts/consistency_check.py 1.2.0: the same figure on every page.

  K1  a workbook shaped like 11 Nov 2026 11:30: every mismatch of that morning is reported
  K2  an aligned workbook reads MATCH on every check (NAV as a named INFO)
  K3  edges: no plan / no exit rows are INFO; tolerances; unreadable v2 stamp
  K4  the gate, the page, read failure
  K5  the workflow (1.1.0: hourly), the manifest pin and the CI lane
  K6  (1.1.0) C11: how old each output page is; STALE past the limit; a page without a build time is INFO
  K7  (1.2.0) the workbook of 13 Nov 12:34: C11 counts market-open hours; a position-cap TRIM is C12, not C10;
      an unsized open pick is INFO; C8 names each typed cell; C9 says which stop rules
  K8  (1.2.0) market-open hours: Tadawul Sun-Thu, the US session on New York's clock, weekends, the idle limit

Run: pytest -q tests/test_consistency_check.py
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

from scripts import consistency_check as cc  # noqa: E402

NOW = dt.datetime(2026, 11, 11, 8, 30, tzinfo=dt.timezone.utc)
TODAY = dt.date(2026, 11, 11)
SH = ["Rank", "Symbol", "Name", "Ticket SAR", "Shares", "Funds From"]
PH = ["Action", "Symbol", "Qty", "Price", "Price SAR", "Stop SAR"]


def wb(**over):
    g = {
        "Top_10_Investments": [["TOP 10"], ["Status:", "Last run 2026-11-11 11:11:14 | status: ok | output: EXECUTABLE"],
                               ["x", "", "", "", "", "", "Cash Available (SAR)", "33,125 SAR"], [], SH,
                               [1, "TEN.US", "Tsakos", "0 SAR", 0, "Held — owner rule"],
                               [2, "ZTO.US", "ZTO", "5,071 SAR", 66, "Cash 5,071 SAR"], [], ["ALL QUALIFIED"]],
        "_Daily_Plan": [["Plan Date", "Kind", "Symbol", "Status"], ["2026-11-10", "PICK", "OLD.US", "OPEN"],
                        ["2026-11-11", "PICK", "TEN.US", "OPEN"], ["2026-11-11", "PICK", "NMM.US", "WAIT"],
                        ["2026-11-11", "PICK", "BAD.US", "INVALIDATED"], ["2026-11-11", "PIPELINE", "EFC.US", ""]],
        "Calendar_Events": [["Symbol", "Next Earnings Date", "Days To Earnings", "Next Ex-Div Date", "Days To ExDiv"],
                            ["ZTO.US", "2026-11-17", 6, "", ""], ["TEN.US", "2026-12-01", 20, "", ""]],
        "Top_10_Investments_v2": [["TOP 10 v2"], ["Feed"], ["Status: Last run 2026-11-10 16:03:23 | status: ok"]],
        "Portfolio_Decision": [["MY PORTFOLIO"], ["Status:", "Last run 2026-11-11 11:07:18 | status: ok"],
                               ["PF: Cash Available SAR", "33,125 SAR"], ["KPIs"],
                               ["Portfolio (SAR)", "Holdings (SAR)", "Cash (SAR)"], ["58,880 SAR", "25,755 SAR", "33,125 SAR"],
                               [], PH, ["TRIM", "DDI.US", 240, 15.70, 58.88, 54.15], ["HOLD", "GLPI.US", 79, 39.24, 147.15, 134.63],
                               [], ["SECTOR SUMMARY"]],
        "Portfolio_Decision_v2": [["PORTFOLIO DECISION — v2"] + [""] * 11 + ["UNCERTIFIED · page built 2026-11-10 16:05"],
                                  ["x"], ["Sources: ledger 2026-11-10 13:33:57"]],          # the stamp sits in column M
        "_Portfolio_CostBasis": [["ledger"], ["Symbol", "Ccy", "Status", "Shares", "Current Price", "FX→SAR", "Market Value SAR"],
                                 ["DDI.US", "USD", "Active", 240, 15.70, 3.757, "14,156"], ["GLPI.US", "USD", "Active", 79, 39.24, 3.757, "11,647"],
                                 ["5023.SR", "SAR", "Active", 100, 99.0, 1.0, "9,900"], ["OLD.US", "USD", "Inactive", 5, 1, 3.757, ""]],
        "My_Portfolio": [["Symbol", "Name", "Current Price"], ["DDI.US", "x", 15.70], ["GLPI.US", "x", 39.24], ["5023.SR", "x", 99.0]],
        "_Exit_Plan": [["Date", "Symbol", "Stop", "Action"], ["2026-11-11", "DDI.US", 14.24, "TAKE PROFIT"],
                       ["2026-11-11", "GLPI.US", 35.74, "HOLD"], ["2026-11-10", "DDI.US", 1.0, "SELL"]],
    }
    g.update(over)
    return g


def res(g, **k):
    kk = cc.knobs()
    kk.update(k)
    return {(r[0], i): r for i, r in enumerate(cc.run_checks(g, TODAY, kk))}


def by(g, name, **k):
    return [r for (n, _i), r in res(g, **k).items() if n == name]


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in list(__import__("os").environ):
        if name.startswith("TFB_CONS") or name == "TFB_PLAN_EARN_DAYS":
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_CONSISTENCY", "on")


def test_k1_the_11_nov_morning():
    g = wb()
    c1 = by(g, "C1 decision")[0]
    assert c1[1] == "MISMATCH" and c1[3] == "ZTO.US" and c1[4] == "NMM.US, TEN.US"
    assert "ZTO.US: ticket on the main page, not an open pick" in c1[2]
    assert "TEN.US: open pick, main page says 'Held — owner rule'" in c1[2] and "NMM.US: open pick, main page says 'not seated'" in c1[2]
    c2 = by(g, "C2 earnings")[0]
    assert c2[1] == "MISMATCH" and "ZTO.US (earnings in 6 day(s))" in c2[2]
    c3 = by(g, "C3 v2 freshness")
    assert [r[1] for r in c3] == ["STALE", "STALE"] and "19.1 h older" in c3[0][2] and c3[1][4] == "2026-11-10 16:05"
    assert by(g, "C4 prices")[0][1] == "MATCH" and by(g, "C5 quantities")[0][1] == "MATCH"
    c6 = by(g, "C6 FX")[0]
    assert c6[1] == "MISMATCH" and c6[3] == "3.7570" and "48 SAR higher" in c6[2]
    c7 = by(g, "C7 NAV")[0]
    assert c7[1] == "INFO" and "adds 5023.SR (9,900 SAR)" in c7[2] and c7[3] == "58,880" and c7[4] == "68,828"
    assert by(g, "C8 cash")[0][1] == "MATCH"
    c9 = by(g, "C9 stops")[0]
    assert c9[1] == "MISMATCH" and "DDI.US Portfolio_Decision 14.44 vs Exit Plan 14.24" in c9[2] and "GLPI" not in c9[2]
    assert c9[2].endswith(" - the stop to place is the Exit Plan's (the Today Action Plan shows it)")   # 1.2.0
    assert by(g, "C10 actions")[0][1] == "MATCH"                                   # TRIM and TAKE PROFIT both sell


def test_k2_an_aligned_workbook_matches():
    g = wb()
    g["Top_10_Investments"][5] = [1, "TEN.US", "Tsakos", "7,900 SAR", 47, "Cash"]
    g["Top_10_Investments"][6] = [2, "NMM.US", "Navios", "7,900 SAR", 24, "Cash"]
    g["Top_10_Investments_v2"][2] = ["Status: Last run 2026-11-11 11:11:14"]
    g["Portfolio_Decision_v2"][0][12] = "page built 2026-11-11 11:20"
    for r in g["_Portfolio_CostBasis"][2:4]:
        r[5] = 3.75
    g["_Portfolio_CostBasis"][2][6], g["_Portfolio_CostBasis"][3][6] = "14,130", "11,625"
    g["Portfolio_Decision"][8][5], g["Portfolio_Decision"][9][5] = 53.40, 134.03
    got = cc.run_checks(g, TODAY, cc.knobs())
    assert {r[1] for r in got} == {"MATCH", "INFO"} and [r[0] for r in got if r[1] == "INFO"] == ["C7 NAV"]
    assert len(got) == 11


def test_k3_edges():
    g = wb(_Daily_Plan=[["Plan Date", "Kind", "Symbol", "Status"]], _Exit_Plan=[])
    assert by(g, "C1 decision")[0][1] == "INFO" and "main page tickets: ZTO.US" in by(g, "C1 decision")[0][2]
    assert by(g, "C9 stops")[0][1] == "INFO" and not by(g, "C10 actions")
    assert cc.RANGES["Portfolio_Decision_v2"] == "A1:Z4"                          # Codex P2 on #958: column M is read
    assert all(p in cc.RANGES for p, _ in cc.FRESHNESS)                           # 1.1.0: every C11 page is read
    g = wb(Portfolio_Decision_v2=[["v2"], ["x"], ["Sources: ledger 2026-11-10 13:33:57"]])
    assert by(g, "C3 v2 freshness")[1][1] == "INFO"                               # another time on the page is not the build time
    g = wb(My_Portfolio=[])                                                       # Codex P2 on #958: unread is not agreement
    assert by(g, "C4 prices")[0][1] == "INFO" and "no rows read from My_Portfolio" in by(g, "C4 prices")[0][2]
    assert "one price only for 5023.SR" in by(g, "C4 prices")[0][2]
    g = wb(_Portfolio_CostBasis=[])
    assert by(g, "C4 prices")[0][1] == "INFO" and "the ledger" in by(g, "C4 prices")[0][2]
    g = wb()
    g["My_Portfolio"][1][2] = 15.80                                               # 0.64% apart
    assert by(g, "C4 prices")[0][1] == "MISMATCH" and by(g, "C4 prices", price_tol=1.0)[0][1] == "MATCH"
    g["Portfolio_Decision"][8][2] = 230
    assert "DDI.US ledger 240 vs Portfolio_Decision 230" in by(g, "C5 quantities")[0][2]
    assert by(wb(), "C2 earnings", earn_days=5)[0][1] == "MATCH"
    assert by(wb(), "C3 v2 freshness", stale_h=24)[0][1] == "MATCH"
    g = wb(Top_10_Investments_v2=[["no time here"]])
    assert by(g, "C3 v2 freshness")[0][1] == "INFO"
    g = wb()
    g["Top_10_Investments"][2][7] = "30,000 SAR"
    c8 = by(g, "C8 cash")[0]
    assert c8[1] == "MISMATCH" and "Top_10_Investments 'Cash Available (SAR)' reads 30,000 SAR (3,125 SAR less)" in c8[2]
    assert "PF: Cash Available SAR" not in c8[2] and c8[4] == "30,000"           # 1.2.0: the cell is named
    g = wb()
    g["Portfolio_Decision"][5][0] = "70,000 SAR"                                  # a gap the outside holding does not explain
    assert by(g, "C7 NAV")[0][1] == "MISMATCH"
    g = wb()
    g["Portfolio_Decision"][9][0] = "TRIM"                                        # GLPI: TRIM vs HOLD
    assert "GLPI.US Portfolio_Decision TRIM vs Exit Plan HOLD" in by(g, "C10 actions")[0][2]


class WorksheetNotFound(Exception):
    pass


class Sheet:
    def __init__(self, rows):
        self.values, self.row_count, self.col_count = deepcopy(rows), 100, 10

    def get(self, rng):
        return deepcopy(self.values)

    def resize(self, rows=None, cols=None):
        pass

    def update(self, values=None, range_name=None, value_input_option=None):
        self.values = [list(r) for r in values if any(str(c) != "" for c in r)]


class Book:
    def __init__(self, tabs):
        self.tabs = {k: Sheet(v) for k, v in tabs.items()}

    def worksheet(self, name):
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]

    def add_worksheet(self, title, rows, cols):
        self.tabs[title] = Sheet([])
        return self.tabs[title]


def test_k4_gate_page_and_read_failure(monkeypatch, capsys):
    monkeypatch.setattr(cc, "_SLEEP", lambda s: None)
    b = Book(wb())
    monkeypatch.setenv("TFB_CONSISTENCY", "observe")
    assert cc.run(False, book=b, now=NOW) == 0 and "Consistency_Check" not in b.tabs
    monkeypatch.setenv("TFB_CONSISTENCY", "on")
    assert cc.run(True, book=b, now=NOW) == 0 and "Consistency_Check" not in b.tabs
    assert cc.run(False, book=b, now=NOW) == 0
    page = b.tabs["Consistency_Check"].values
    # C11 adds six rows: Top_10 11:11 and Portfolio_Decision 11:07 MATCH; Portfolio_Decision_v2 STALE;
    # Daily_Plan, Exit_Plan (not in this workbook) and Top_10_v2 (no 'page built') INFO
    assert page[1][0].startswith("As of 2026-11-11 11:30 Riyadh · 4 mismatch(es) · 3 stale · 6 match · 4 note(s)")
    assert page[2] == ["Check", "Status", "Detail", "First page", "Second page", "Pages compared"]
    assert [r[1] for r in page[3:]] == ["MISMATCH"] * 4 + ["STALE"] * 3 + ["INFO"] * 4 + ["MATCH"] * 6
    assert set(b.tabs) - set(wb()) == {"Consistency_Check"}                       # no other page is touched
    log = capsys.readouterr().out                                                 # 1.2.0: the run log is public
    assert "SAR" not in log and "58,880" not in log and "14.44" not in log and "3.7570" not in log
    assert "C8 cash          MATCH    (figures on the Consistency_Check page)" in log
    assert "C9 stops         MISMATCH (figures on the Consistency_Check page)" in log
    assert "ZTO.US: ticket on the main page, not an open pick" in log and "ZTO.US (earnings in 6 day(s))" in log
    assert "Portfolio_Decision: built 2026-11-11 11:07 Riyadh" in log
    assert cc.log_detail(["C12 position cap", "OVER CAP", "AAA.US 18.0%"]) == "(figures on the Consistency_Check page)"
    assert cc.log_detail(["C1 decision", "INFO", "x"]) == "x" and cc.log_detail(["C10 actions", "MATCH", "y"]) != "y"
    monkeypatch.setenv("TFB_CONSISTENCY", "off")
    assert cc.main(["--dry-run"]) == 0 and "nothing to do" in capsys.readouterr().out
    monkeypatch.setenv("TFB_CONSISTENCY", "yes")
    assert cc.mode() == "off"
    monkeypatch.setenv("TFB_CONSISTENCY", "on")

    class Bad(Book):
        def worksheet(self, name):
            raise RuntimeError("quota")
    assert cc.run(False, book=Bad({}), now=NOW) == 2


def test_k5_workflow_manifest_ci():
    raw = (ROOT / ".github/workflows/consistency_check.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(raw)
    on = wf[True] if True in wf else wf["on"]
    assert on["schedule"] == [{"cron": "50 4-21 * * *"}] and "workflow_run" not in on              # 1.2.0: active hours
    assert wf["permissions"] == {"contents": "read"}
    step = [s for s in wf["jobs"]["consistency"]["steps"] if "consistency_check.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_CONSISTENCY"] == "${{ vars.TFB_CONSISTENCY || 'on' }}"
    src = (ROOT / "scripts/consistency_check.py").read_text(encoding="utf-8")
    for name in re.findall(r"(TFB_CONS[A-Z_]+): \$\{\{", raw):
        assert '"%s"' % name in src, name
    assert '("consistency_check", "%s", "consistency check")' % cc.__version__ in (
        ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert "tests/test_consistency_check.py" in (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "ws.update" not in src and "batch_update" not in src


def test_k6_page_freshness():
    now = dt.datetime(2026, 11, 11, 10, 0, tzinfo=dt.timezone.utc)                # 13:00 Riyadh
    g = wb(Daily_Plan=[["DAILY PLAN"], ["Plan of 2026-11-11 · cut 2026-11-11 07:39 Riyadh from the main page's render of "
                                        "2026-11-11 07:39 (VERIFIED) · status checked 2026-11-11 12:20"]],
           Exit_Plan=[["EXIT PLAN"], ["As of 2026-11-11 07:30 Riyadh · daily bars to 2026-11-10 · broker orders unknown"]])
    g["Top_10_Investments_v2"][0] = ["TOP 10 v2", "EXECUTABLE", "run 1 · cockpit 2026-11-10 16:03:23 · page built 2026-11-10 16:06"]
    rows = cc.freshness(g, now, 2.0)
    got = {r[2].split(":")[0]: r for r in rows}
    assert [r[0] for r in rows] == ["C11 freshness"] * 6
    assert got["Daily_Plan"][1] == "MATCH" and "built 2026-11-11 12:20 Riyadh, 0.7 h ago" in got["Daily_Plan"][2]
    assert got["Exit_Plan"][1] == "STALE" and "5.5 h ago, 3.0 h of it with a market open (limit 2 h)" in got["Exit_Plan"][2]
    assert got["Top_10_Investments"][1] == "MATCH" and "1.8 h ago" in got["Top_10_Investments"][2]
    assert got["Portfolio_Decision"][1] == "MATCH"
    assert got["Top_10_Investments_v2"][1] == "STALE" and got["Top_10_Investments_v2"][3] == "2026-11-10 16:06"
    assert got["Portfolio_Decision_v2"][1] == "STALE" and got["Portfolio_Decision_v2"][3] == "2026-11-10 16:05"
    assert cc.freshness(g, now, 24.0)[4][1] == "MATCH"
    blank = cc.freshness({}, now, 2.0)
    assert {r[1] for r in blank} == {"INFO"} and "Daily_Plan: no build time found" in blank[2][2]
    assert not [r for r in cc.run_checks(g, TODAY, cc.knobs()) if r[0] == "C11 freshness"]   # needs the time


PH7 = ["Action", "Symbol", "Qty", "Price", "Price SAR", "Weight %", "Stop SAR", "Advisor Note"]


def wb13():
    """The shape of the workbook of Fri 13 Nov 2026 12:34 Riyadh. Every figure and ticker is invented (public repository)."""
    g = wb()
    g["Top_10_Investments"] = [["TOP 10"], ["Status:", "Last run 2026-11-13 10:52:11 | status: ok | output: QUALIFIED_UNFUNDED"],
                               ["x", "", "", "", "", "", "Cash Available (SAR)", "50,000 SAR"], [], SH,
                               [1, "AAA.US", "Alpha", "0 SAR", 0, "Research — awaiting confirmation"],
                               [2, "BBB.US", "Beta", "0 SAR", 0, "Research — awaiting confirmation"], [], ["ALL QUALIFIED"]]
    g["_Daily_Plan"] = [["Plan Date", "Kind", "Symbol", "Status"], ["2026-11-13", "PICK", "AAA.US", "OPEN"],
                        ["2026-11-13", "PICK", "BBB.US", "OPEN"], ["2026-11-13", "PICK", "CCC.US", "WAIT"]]
    g["Calendar_Events"] = [["Symbol", "Next Earnings Date", "Days To Earnings", "Next Ex-Div Date", "Days To ExDiv"]]
    g["Portfolio_Decision"] = [["MY PORTFOLIO"], ["Status:", "Last run 2026-11-13 08:42:10 | status: ok"],
                               ["PF: Cash Available SAR", "50,000 SAR", "", "PF: Max Position %", "15.0"], ["KPIs"],
                               ["Portfolio (SAR)", "Holdings (SAR)", "Cash (SAR)"], ["130,000 SAR", "100,000 SAR", "30,000 SAR"],
                               [], PH7,
                               ["TRIM", "HHH.US", 100, 20.00, 75.0, "18.0%", 67.5,
                                "Position 18.0% > Max Position 15.0% \u2014 trim to cap"],
                               ["TRIM", "JJJ.US", 50, 40.00, 150.0, "16.0%", 135.0, "Position 16.0% > Max Position 15.0%"],
                               ["HOLD", "KKK.US", 80, 30.00, 112.5, "12.5%", 101.25, ""], [], ["SECTOR SUMMARY"]]
    g["_Exit_Plan"] = [["Date", "Symbol", "Stop", "Action"], ["2026-11-13", "HHH.US", 19.00, "RAISE STOP"],
                       ["2026-11-13", "JJJ.US", 36.00, "HOLD"], ["2026-11-13", "KKK.US", 27.00, "HOLD"]]
    return g


def test_k7_the_13_nov_differences():
    now = dt.datetime(2026, 11, 13, 8, 50, tzinfo=dt.timezone.utc)                # Fri 11:50 Riyadh
    today = dt.date(2026, 11, 13)
    got = cc.run_checks(wb13(), today, cc.knobs(), now)
    one = lambda name: [r for r in got if r[0] == name]                           # noqa: E731
    # C11: built 08:42 on a Friday, 3.1 h ago, and no market open since - not stale
    pdf = [r for r in one("C11 freshness") if r[5] == "Portfolio_Decision"][0]
    assert pdf[1] == "MATCH" and pdf[2] == "Portfolio_Decision: built 2026-11-13 08:42 Riyadh, 3.1 h ago, no market open since"
    # C10 / C12: the cap, not a price exit
    assert one("C10 actions")[0][1] == "MATCH"
    c12 = one("C12 position cap")
    assert len(c12) == 1 and c12[0][1] == "OVER CAP"
    assert c12[0][2] == ("above the position cap of 15%: HHH.US 18.0% (Exit Plan: RAISE STOP); JJJ.US 16.0% (Exit Plan: HOLD) - "
                         "Portfolio_Decision says TRIM to the cap; a trim is a sizing decision, not a price exit")
    # C1: three open picks the main page has not sized - information, the plan is the order of record
    c1 = one("C1 decision")[0]
    assert c1[1] == "INFO" and c1[3] == "none" and c1[4] == "AAA.US, BBB.US, CCC.US"
    assert "CCC.US: open pick, main page says 'not seated'" in c1[2] and "the Today Action Plan is the order of record" in c1[2]
    # C8: both typed cells named against the broker's cash
    c8 = one("C8 cash")[0]
    assert c8[1] == "MISMATCH" and c8[2].startswith("typed cash is not Portfolio_Decision's cash of 30,000 SAR: ")
    assert "Top_10_Investments 'Cash Available (SAR)' reads 50,000 SAR (20,000 SAR more)" in c8[2]
    assert "Portfolio_Decision 'PF: Cash Available SAR' reads 50,000 SAR (20,000 SAR more)" in c8[2]
    # C9: unchanged in logic
    c9 = one("C9 stops")[0]
    assert c9[1] == "MISMATCH" and "HHH.US Portfolio_Decision 18.00 vs Exit Plan 19.00" in c9[2] and "KKK" not in c9[2]
    # the typed cells corrected: C8 matches
    g = wb13()
    g["Top_10_Investments"][2][7] = g["Portfolio_Decision"][2][1] = "30,000 SAR"
    assert [r for r in cc.run_checks(g, today, cc.knobs(), now) if r[0] == "C8 cash"][0][1] == "MATCH"
    g["Portfolio_Decision"][2][1] = "40,000 SAR"                                  # one cell alone is still named
    c8 = [r for r in cc.run_checks(g, today, cc.knobs(), now) if r[0] == "C8 cash"][0]
    assert c8[1] == "MISMATCH" and "Top_10_Investments" not in c8[2] and "(10,000 SAR more)" in c8[2]
    # a dangerous direction stays a MISMATCH: a main-page ticket that is not an open pick
    g = wb13()
    g["Top_10_Investments"][5] = [1, "ZTO.US", "ZTO", "5,071 SAR", 66, "Cash"]
    c1 = [r for r in cc.run_checks(g, today, cc.knobs(), now) if r[0] == "C1 decision"][0]
    assert c1[1] == "MISMATCH" and "order of record" not in c1[2]
    # a TRIM that is not about the cap is still compared: below the cap, no note
    g = wb13()
    g["Portfolio_Decision"][9][5], g["Portfolio_Decision"][9][7] = "12.0%", "Reduce: weak momentum"
    got = cc.run_checks(g, today, cc.knobs(), now)
    assert "JJJ.US Portfolio_Decision TRIM vs Exit Plan HOLD" in [r for r in got if r[0] == "C10 actions"][0][2]
    assert "JJJ" not in [r for r in got if r[0] == "C12 position cap"][0][2]
    # the note alone is enough (no weight column value); a cap TRIM the Exit Plan also sells is not listed
    g = wb13()
    g["Portfolio_Decision"][8][5] = ""
    g["_Exit_Plan"][2][3] = "SELL"
    got = cc.run_checks(g, today, cc.knobs(), now)
    c12 = [r for r in got if r[0] == "C12 position cap"][0]
    assert "HHH.US (Exit Plan: RAISE STOP)" in c12[2] and "JJJ" not in c12[2]
    assert [r for r in got if r[0] == "C10 actions"][0][1] == "MATCH"
    # the page: its own count and place
    page = cc.page_rows(cc.run_checks(wb13(), today, cc.knobs(), now), "2026-11-13 11:50")
    assert " · 1 over the position cap · " in page[1][0] and "consistency_check 1.2.0" in page[1][0]
    order = [r[1] for r in page[4:]]
    assert order.index("OVER CAP") > max(i for i, s in enumerate(order) if s in ("MISMATCH", "STALE"))
    assert order.index("OVER CAP") < min(i for i, s in enumerate(order) if s in ("INFO", "MATCH"))
    assert cc.RANGES["Portfolio_Decision"] == "A1:Z60"                            # the Advisor Note is column Z
    assert " over the position cap" not in cc.page_rows(cc.run_checks(wb(), TODAY, cc.knobs()), "x")[1][0]


def test_k8_market_open_hours():
    R = dt.timezone(dt.timedelta(hours=3))
    t = lambda d, h, m=0: dt.datetime(2026, 11, d, h, m, tzinfo=R)               # noqa: E731 - Riyadh, Nov 2026 (13 = Friday)
    assert cc.open_hours(t(13, 8, 42), t(13, 11, 50)) == 0.0                      # Friday morning: nothing open
    assert cc.open_hours(t(13, 8, 42), t(13, 19, 30)) == pytest.approx(2.0)       # US open 17:30 Riyadh in winter
    assert cc.open_hours(t(12, 9, 0), t(12, 23, 0)) == pytest.approx(5.0 + 5.5)   # Thursday: Tadawul 10-15, then the US
    assert cc.open_hours(t(13, 23, 0), t(15, 9, 59)) == pytest.approx(1.0)        # Fri 23:00 -> Sun 09:59: the US close only
    assert cc.open_hours(t(14, 0, 0), t(15, 12, 0)) == pytest.approx(2.0)         # Saturday shut; Sunday Tadawul from 10:00
    assert cc.open_hours(t(15, 16, 0), t(15, 23, 59)) == 0.0                      # Sunday evening: no US session
    assert cc.open_hours(t(13, 12, 0), t(13, 11, 0)) == 0.0 and cc.open_hours(t(13, 12, 0), t(13, 12, 0)) == 0.0
    summer = dt.datetime(2026, 7, 6, 16, 0, tzinfo=R)                             # Mon 6 Jul: US opens 16:30 Riyadh
    assert cc.open_hours(summer, summer + dt.timedelta(hours=2)) == pytest.approx(1.5)
    g = {"Portfolio_Decision": [["x"], ["Status:", "Last run 2026-11-13 08:42:10"]]}
    row_at = lambda now, **k: [r for r in cc.freshness(g, now, 2.0, **k) if r[5] == "Portfolio_Decision"][0]   # noqa: E731
    assert row_at(t(13, 11, 50))[1] == "MATCH"
    r = row_at(t(13, 19, 0))                                                      # 1.5 h of the US session: within the limit
    assert r[1] == "MATCH" and r[2].endswith("10.3 h ago, 1.5 h of it with a market open")
    r = row_at(t(13, 19, 45))
    assert r[1] == "STALE" and r[2].endswith("11.1 h ago, 2.2 h of it with a market open (limit 2 h)")
    g = {"Portfolio_Decision": [["x"], ["Status:", "Last run 2026-11-14 00:30:00"]]}   # built after the US close
    r = row_at(t(15, 3, 0))                                                       # 26.5 h later, markets shut throughout
    assert r[1] == "STALE" and r[2].endswith("26.5 h ago (limit 26 h with the markets closed)")
    assert row_at(t(15, 2, 0))[1] == "MATCH" and row_at(t(15, 3, 0), idle_h=48.0)[1] == "MATCH"
    assert cc.knobs()["idle_h"] == 26.0 and cc.knobs()["page_h"] == 2.0
    raw = (ROOT / ".github/workflows/consistency_check.yml").read_text(encoding="utf-8")
    assert "TFB_CONS_PAGE_IDLE_MAX_H: ${{ vars.TFB_CONS_PAGE_IDLE_MAX_H || '26' }}" in raw
