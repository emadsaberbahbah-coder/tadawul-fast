#!/usr/bin/env python3
"""tests/test_consistency_check.py - scripts/consistency_check.py 1.0.0: the same figure on every page.

  K1  a workbook shaped like 11 Nov 2026 11:30: every mismatch of that morning is reported
  K2  an aligned workbook reads MATCH on every check (NAV as a named INFO)
  K3  edges: no plan / no exit rows are INFO; tolerances; unreadable v2 stamp
  K4  the gate, the page, read failure
  K5  the workflow, the manifest pin and the CI lane

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
        "Portfolio_Decision_v2": [["PORTFOLIO DECISION — v2", "UNCERTIFIED", "page built 2026-11-10 16:05"]],
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
    assert by(g, "C10 actions")[0][1] == "MATCH"                                   # TRIM and TAKE PROFIT both sell


def test_k2_an_aligned_workbook_matches():
    g = wb()
    g["Top_10_Investments"][5] = [1, "TEN.US", "Tsakos", "7,900 SAR", 47, "Cash"]
    g["Top_10_Investments"][6] = [2, "NMM.US", "Navios", "7,900 SAR", 24, "Cash"]
    g["Top_10_Investments_v2"][2] = ["Status: Last run 2026-11-11 11:11:14"]
    g["Portfolio_Decision_v2"][0] = ["v2", "", "page built 2026-11-11 11:20"]
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
    assert by(g, "C8 cash")[0][1] == "MISMATCH" and "3,125 SAR" in by(g, "C8 cash")[0][2]
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
    assert page[1][0].startswith("As of 2026-11-11 11:30 Riyadh · 4 mismatch(es) · 2 stale · 4 match · 1 note(s)")
    assert page[2] == ["Check", "Status", "Detail", "First page", "Second page", "Pages compared"]
    assert [r[1] for r in page[3:]] == ["MISMATCH"] * 4 + ["STALE"] * 2 + ["INFO"] + ["MATCH"] * 4
    assert set(b.tabs) - set(wb()) == {"Consistency_Check"}                       # no other page is touched
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
    assert on["workflow_run"]["workflows"] == ["\U0001F5D3️ Daily Plan", "\U0001F3AF Exit Plan"] and on["schedule"]
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
