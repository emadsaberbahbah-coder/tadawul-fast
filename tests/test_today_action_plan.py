"""scripts/today_action_plan.py v1.0.0 - the Today Action Plan page (owner, 12 Nov 2026): one page that orders the
day's steps and copies every level from Exit_Plan, _Daily_Plan, Evidence_Board and Consistency_Check."""
from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import pytest
import yaml

from scripts import today_action_plan as tap

ROOT = Path(__file__).resolve().parents[1]
UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 12, 6, 0, tzinfo=UTC)          # 09:00 Riyadh
K = {"stale_h": 3.0, "stop_tol": 1.0, "pipeline": 5.0}

EXIT = [["EXIT PLAN - stop and target per holding, reviewed after every close"],
        ["As of 2026-11-12 07:31 Riyadh · daily bars to 2026-11-11"], ["Rule: ..."], ["1 action"], [],
        ["HOLDINGS (5)"],
        ["Symbol", "Name", "Qty", "Entry", "Last close", "Live", "P/L %", "Highest close", "ATR %", "Gain at high (R)",
         "Stop", "Was", "Target", "Action", "Why", "Broker stop"],
        ["DDI.US", "DoubleDown", "240", "13.44", "15.72", "", "+17.0%", "15.9", "3.1", "2.1", "14.80", "14.73", "16.64",
         "RAISE STOP", "raise the stop 14.73 -> 14.80", "14.24"],
        ["GLPI.US", "Gaming & Leisure", "79", "38.88", "39.27", "39.40", "+1.0%", "39.5", "1.5", "0.3", "35.74", "35.74",
         "41.96", "HOLD", "+0.3R at the high", "35.74"],
        ["IBOC.US", "Intl Bancshares", "52", "68.74", "68.02", "", "-1.1%", "69", "1.8", "0.0", "63.20", "63.20", "74.20",
         "HOLD", "+0.0R", ""],
        ["SUPN.US", "Supernus", "95", "27.49", "29.45", "", "+7.1%", "31", "2.9", "0.6", "24.25", "24.25", "30.73",
         "TAKE PROFIT", "first target reached", "23.00"],
        ["5023.SR", "Sukuk", "10", "", "", "", "", "", "", "", "", "", "", "NO RULE", "fixed income", ""],
        [], ["PICKS"], ["Symbol", "Entry", "Plan stop"]]
HDR = ["Plan Date", "Cut At (Riyadh)", "Source Render", "Cut Quality", "Kind", "Rank", "Symbol", "Name", "Market", "Sector",
       "Ccy", "Entry", "Zone Low", "Zone High", "Qty", "Alloc SAR", "Stop", "TP1", "TP2", "Risk SAR", "Score", "ROI 12M %",
       "Labels", "Waiting For", "Status", "Status Reason", "Last Price", "Status At (Riyadh)"]


def lrow(day, kind, rank, sym, status, qty=24, entry=87.14, zl=84.5, zh=88.01, stop=80.17, tp1=95.0, wait="", labels="",
         reason="price is inside the entry zone", price=87.0, quality="VERIFIED"):
    return [day, day + " 08:25", "08:10", quality, kind, rank, sym, sym[:3], "US", "Sec", "USD", entry, zl, zh, qty, 7800,
            stop, tp1, 105, 600, 74.8, 20, labels, wait, status, reason, price, day + " 08:40"]


LEDGER = [HDR, lrow("2026-11-11", "PICK", 1, "OLD.US", "OPEN"),
          lrow("2026-11-12", "PICK", 2, "NMM.US", "OPEN"),
          lrow("2026-11-12", "PICK", 1, "EFC.US", "WAIT", reason="price 11.60 is above the entry zone"),
          lrow("2026-11-12", "PICK", 3, "ZERO.US", "OPEN", qty=0),
          lrow("2026-11-12", "PIPELINE", 9, "ZTO.US", "", wait="next in line (rank 9)"),
          lrow("2026-11-12", "PIPELINE", 4, "MBIN.US", "", wait="sector cap")]
EVID = [["EVIDENCE BOARD"], ["Built 2026-11-12 07:40 Riyadh · evidence_board 1.1.0"], [], ["1. MARKET STATE"],
        ["Market", "Bars to", "Above 50-day avg", "Above 200-day avg", "State", "After this state: all names up", "Avg return",
         "Strong-trend names up", "Weeks seen", "Reading"],
        ["US", "2026-11-10", "40.4%", "48.1%", "MIXED", "55.1%", "1.09%", "50.6%", "16", ""],
        ["Tadawul", "2026-11-09", "14.2%", "12.7%", "WASHED OUT", "42.1%", "-0.77%", "47.8%", "25", ""], [],
        ["2. NAMES"],
        ["Symbol", "Name", "Engine label", "Engine score", "Shadow score", "Chance up, 21 bars", "Based on"],
        ["NMM.US (pick)", "Navios", "INVESTABLE", "74.8", "86.1", "56.2%", "US, mixed market, middle trend: 8897 name-weeks"],
        ["EFC.US", "Ellington", "INVESTABLE", "75.2", "66.1", "52.1%", "US, mixed market, weak trend"], [],
        ["6. LEARNING GATE - copied from the S1_Gate page"], ["S-1 GATE", "FAIL", "day 28/28"], ["Challenger", "-3.58%"], [],
        ["How to read"]]
CONS = [["CONSISTENCY CHECK - the same figure on every page"], ["As of 2026-11-12 07:51 Riyadh · 1 mismatch(es)"], [],
        ["Check", "Status", "Detail", "First page", "Second page", "Pages compared"],
        ["C5 quantities", "MATCH", "quantities agree"], ["C9 stops", "MISMATCH", "DDI.US Portfolio_Decision 14.44 vs Exit Plan 14.80"],
        ["C11 freshness", "STALE", "Top_10_Investments_v2 built 5.0 h ago"]]
DAILY = [["DAILY PLAN"], ["Plan of 2026-11-12 · cut 2026-11-12 08:25 Riyadh from the render of 08:10 · status checked 2026-11-12 08:40"]]


class WorksheetNotFound(Exception):
    pass


class Sheet:
    def __init__(self, rows=None, hidden=False):
        self.values, self.row_count, self.col_count, self.updates = list(rows or []), 200, 30, 0
        self.isSheetHidden, self.hides = hidden, 0

    def get(self, rng):
        return [list(r) for r in self.values]

    def resize(self, rows=None, cols=None):
        self.row_count, self.col_count = rows or self.row_count, cols or self.col_count

    def update(self, values=None, range_name=None, value_input_option=None):
        self.updates += 1
        self.values = [list(r) for r in values]

    def hide(self):
        self.hides += 1
        self.isSheetHidden = True


class Book:
    def __init__(self, tabs):
        self.tabs = dict(tabs)

    def worksheet(self, name):
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]

    def add_worksheet(self, title, rows, cols):
        self.tabs[title] = Sheet()
        return self.tabs[title]


def full_book(**over):
    tabs = {"Exit_Plan": Sheet(EXIT), "Daily_Plan": Sheet(DAILY), "_Daily_Plan": Sheet(LEDGER), "Evidence_Board": Sheet(EVID),
            "Consistency_Check": Sheet(CONS), "_Exit_Plan": Sheet([["x"]]), "_Evidence_Log": Sheet([["x"]], hidden=True)}
    tabs.update(over)
    return Book(tabs)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in list(__import__("os").environ):
        if name.startswith("TFB_TAP_") or name == "TFB_TODAY_PLAN":
            monkeypatch.delenv(name, raising=False)


# ----------------------------------------------------------------------------- T1 readers
def test_t1_readers_copy_the_source_pages():
    hold = tap.read_holdings(EXIT)
    assert [h["symbol"] for h in hold] == ["DDI.US", "GLPI.US", "IBOC.US", "SUPN.US", "5023.SR"]
    ddi, glpi, iboc = hold[0], hold[1], hold[2]
    assert ddi["action"] == "RAISE STOP" and ddi["stop"] == 14.80 and ddi["was"] == 14.73 and ddi["broker_stop"] == 14.24
    assert ddi["qty"] == 240 and ddi["last"] == 15.72 and glpi["last"] == 39.40           # the live price when there is one
    assert iboc["broker_stop"] is None and tap.read_holdings([["x"]]) == [] and tap.read_holdings([]) == []
    plan = tap.read_plan(LEDGER, dt.date(2026, 11, 12))
    assert plan["date"] == "2026-11-12" and plan["is_today"] and plan["quality"] == "VERIFIED" and plan["cut_at"] == "2026-11-12 08:25"
    assert [p["symbol"] for p in plan["picks"]] == ["NMM.US", "EFC.US", "ZERO.US"]
    assert [p["symbol"] for p in plan["pipeline"]] == ["MBIN.US", "ZTO.US"]              # by rank
    assert tap.read_plan(LEDGER, dt.date(2026, 11, 13))["is_today"] is False
    assert tap.read_plan([], dt.date(2026, 11, 12))["picks"] == [] and tap.read_plan([["a", "b"]], dt.date(2026, 11, 12))["date"] == ""
    ev = tap.read_evidence(EVID)
    assert [s["market"] for s in ev["states"]] == ["US", "Tadawul"] and ev["states"][1]["state"] == "WASHED OUT"
    assert ev["names"]["NMM.US"] == {"shadow": "86.1", "chance": "56.2%", "basis": "US, mixed market, middle trend: 8897 name-weeks"}
    assert set(ev["names"]) == {"NMM.US", "EFC.US"} and ev["gate"] == [["S-1 GATE", "FAIL", "day 28/28"], ["Challenger", "-3.58%"]]
    assert tap.read_evidence([]) == {"states": [], "names": {}, "gate": []}
    assert tap.read_consistency(CONS) == [
        {"check": "C9 stops", "status": "MISMATCH", "detail": "DDI.US Portfolio_Decision 14.44 vs Exit Plan 14.80"},
        {"check": "C11 freshness", "status": "STALE", "detail": "Top_10_Investments_v2 built 5.0 h ago"}]
    assert tap.read_consistency([]) == []


def test_t1_page_time_takes_the_latest_time_that_is_not_in_the_future():
    assert tap.page_time(DAILY, NOW) == dt.datetime(2026, 11, 12, 8, 40, tzinfo=tap.RIYADH)       # checked, not cut
    assert tap.page_time(EXIT, NOW) == dt.datetime(2026, 11, 12, 7, 31, tzinfo=tap.RIYADH)
    fut = [["x"], ["As of 2026-11-12 07:00 · next 2026-11-19 16:00"]]
    assert tap.page_time(fut, NOW) == dt.datetime(2026, 11, 12, 7, 0, tzinfo=tap.RIYADH)
    assert tap.page_time([["no time"], ["2026-13-45 99:99"]], NOW) is None and tap.page_time([], NOW) is None
    assert tap.page_time([["a"], ["b"], ["c"], ["2026-11-12 08:00"]], NOW) is None                # only the top rows
    assert tap.num("14.80") == 14.8 and tap.num("+17.0%") == 17.0 and tap.num("") is None and tap.num("1,200 SAR") == 1200


# ----------------------------------------------------------------------------- T2 steps
def test_t2_holding_steps_by_action_and_broker_stop():
    hold = {h["symbol"]: h for h in tap.read_holdings(EXIT)}
    ddi = tap.holding_steps(hold["DDI.US"], 1.0)
    assert len(ddi) == 1 and ddi[0]["urgency"] == 2 and ddi[0]["source"] == "Exit_Plan"
    assert ddi[0]["do"] == "Raise the stop on DDI.US to 14.80 for all 240 shares."
    assert ddi[0]["levels"] == "was 14.73 · broker stop 14.24"
    assert tap.holding_steps(hold["GLPI.US"], 1.0) == []                                  # HOLD, broker stop in place
    iboc = tap.holding_steps(hold["IBOC.US"], 1.0)
    assert len(iboc) == 1 and iboc[0]["do"].startswith("Check the broker stop on IBOC.US: none was read")
    supn = tap.holding_steps(hold["SUPN.US"], 1.0)
    assert [s["urgency"] for s in supn] == [1, 2.5]
    assert supn[0]["do"] == "Sell half of SUPN.US (47 shares) and keep a stop at 24.25 on the rest."
    assert supn[1]["do"] == "The broker stop on SUPN.US (23.00) is below the plan's stop (24.25): raise it for all 95 shares."
    assert tap.holding_steps(hold["5023.SR"], 1.0) == []                                  # NO RULE: nothing
    near = dict(hold["GLPI.US"], broker_stop=35.50)                                       # 0.67% under: inside the tolerance
    assert tap.holding_steps(near, 1.0) == [] and len(tap.holding_steps(near, 0.5)) == 1
    sell = tap.holding_steps(dict(hold["GLPI.US"], action="SELL"), 1.0)
    assert sell[0]["urgency"] == 0 and sell[0]["do"] == "Sell all 79 shares of GLPI.US." and len(sell) == 1
    rev = tap.holding_steps(dict(hold["GLPI.US"], action="REVIEW"), 1.0)
    assert rev[0]["urgency"] == 3 and rev[0]["do"].startswith("Review GLPI.US today")
    odd = tap.holding_steps(dict(hold["SUPN.US"], qty=None, broker_stop=24.25), 1.0)
    assert odd[0]["do"] == "Sell half of SUPN.US (half the position) and keep a stop at 24.25 on the rest." and len(odd) == 1


def test_t2_pick_steps_and_the_order_of_the_day():
    plan, ev = tap.read_plan(LEDGER, dt.date(2026, 11, 12)), tap.read_evidence(EVID)
    nmm = tap.pick_step(plan["picks"][0], ev["names"], True)
    assert nmm["do"] == "Buy 24 NMM.US with a limit between 84.50 and 88.01; place the stop at 80.17."
    assert nmm["levels"] == "price 87.00 · first target 95.00" and nmm["source"] == "Daily_Plan"
    assert "finished up 56.2% of the time over 21 bars" in nmm["why"]
    assert tap.pick_step(plan["picks"][1], ev["names"], True) is None                     # WAIT
    assert tap.pick_step(plan["picks"][2], ev["names"], True) is None                     # OPEN with no quantity
    unv = tap.pick_step(plan["picks"][0], {}, False)
    assert unv["do"].startswith("Before buying, check the main page") and "finished up" not in unv["why"]
    d = tap.build(tap.read_holdings(EXIT), plan, ev, tap.read_consistency(CONS), {"Exit_Plan": 1.5, "Daily_Plan": 0.3, "X": None,
                                                                                   "Old": 3.1}, K)
    assert [(s["symbol"], s["urgency"]) for s in d["steps"]] == [
        ("SUPN.US", 1), ("DDI.US", 2), ("IBOC.US", 2.5), ("SUPN.US", 2.5), ("NMM.US", 5.002)]
    assert d["stale"] == ["Old"] and len(d["picks"]) == 3 and len(d["pipeline"]) == 2
    old = tap.build(tap.read_holdings(EXIT), tap.read_plan(LEDGER, dt.date(2026, 11, 13)), ev, [], {}, K)
    assert [s["symbol"] for s in old["steps"]] == ["SUPN.US", "DDI.US", "IBOC.US", "SUPN.US"] and old["picks"] == []


# ----------------------------------------------------------------------------- T3 the run
def test_t3_full_run_writes_one_page_and_hides_the_ledgers():
    book = full_book()
    assert tap.run_once(False, book=book, now=NOW) == 0
    page = book.tabs["Today_Action_Plan"].values
    assert all(len(r) == tap.WIDTH for r in page)
    assert page[0][0] == "TODAY ACTION PLAN - Thu 12 Nov 2026" and "Built 2026-11-12 09:00 Riyadh" in page[1][0]
    assert page[2][0] == "4 step(s) on holdings · 1 new buy(s) open · 1 pick(s) waiting for price · 2 page difference(s) to know about"
    a = [i for i, r in enumerate(page) if r[0] == "A. DO TODAY - in this order"][0]
    assert page[a + 2][:3] == [1, "SUPN.US", "Sell half of SUPN.US (47 shares) and keep a stop at 24.25 on the rest."]
    assert page[a + 3][:3] == [2, "DDI.US", "Raise the stop on DDI.US to 14.80 for all 240 shares."] and page[a + 3][5] == "Exit_Plan"
    assert page[a + 6][:2] == [5, "NMM.US"] and page[a + 6][5] == "Daily_Plan"
    b = [i for i, r in enumerate(page) if str(r[0]).startswith("B. HOLDINGS (5)")][0]
    assert page[b + 2] == ["DDI.US", "DoubleDown", "240", "15.72", "+17.0%", "RAISE STOP", "14.80", "14.24", "16.64",
                           "broker stop below the plan"]
    assert page[b + 3][9] == "stop in place" and page[b + 4][9] == "no broker stop read" and page[b + 6][9] == ""
    c = [i for i, r in enumerate(page) if str(r[0]).startswith("C. NEW BUYS - plan of 2026-11-12, cut 2026-11-12 08:25 (VERIFIED)")][0]
    assert page[c + 2][:4] == ["NMM.US", "OPEN", "24", "84.50 - 88.01"] and page[c + 2][7:9] == ["56.2%", "86.1"]
    assert page[c + 3][:2] == ["EFC.US", "WAIT"] and page[c + 3][9] == "price 11.60 is above the entry zone"
    dd = [i for i, r in enumerate(page) if str(r[0]).startswith("D. PIPELINE - next in line (2 of 2)")][0]
    assert page[dd + 2][:1] == ["MBIN.US"] and page[dd + 3][4] == "next in line (rank 9)"
    assert any(r[:4] == ["Tadawul", "2026-11-09", "14.2%", "WASHED OUT"] for r in page)
    assert any(r[:3] == ["S-1 GATE", "FAIL", "day 28/28"] for r in page)
    assert any(r[:2] == ["C9 stops", "MISMATCH"] for r in page)
    ages = {r[0]: (r[1], r[2]) for r in page if r[0] in ("Exit_Plan", "Daily_Plan", "Evidence_Board", "Consistency_Check")}
    assert ages == {"Exit_Plan": ("1.5", "current"), "Daily_Plan": ("0.3", "current"), "Evidence_Board": ("1.3", "current"),
                    "Consistency_Check": ("1.1", "current")}
    assert book.tabs["_Daily_Plan"].hides == 1 and book.tabs["_Exit_Plan"].hides == 1
    assert book.tabs["_Evidence_Log"].hides == 0                                          # already hidden: left alone
    for name in ("Exit_Plan", "Daily_Plan", "_Daily_Plan", "Evidence_Board", "Consistency_Check", "_Exit_Plan"):
        assert book.tabs[name].updates == 0 and name in book.tabs                         # read only; nothing deleted
    assert book.tabs["Exit_Plan"].hides == 0 and book.tabs["Daily_Plan"].hides == 0


def test_t3_old_sources_empty_pages_dry_run_and_off(monkeypatch, capsys):
    late = NOW + dt.timedelta(hours=6)
    book = full_book()
    assert tap.run_once(False, book=book, now=late) == 0
    page = book.tabs["Today_Action_Plan"].values
    assert page[2][0].endswith("· OLD SOURCE: Consistency_Check, Daily_Plan, Evidence_Board, Exit_Plan")
    assert any(r[0] == "Exit_Plan" and r[2] == "OLD - refresh it before acting" for r in page)
    empty = Book({})
    assert tap.run_once(False, book=empty, now=NOW) == 0
    page = empty.tabs["Today_Action_Plan"].values
    flat = " | ".join(str(c) for r in page for c in r)
    assert "Nothing to do today" in flat and "No plan has been cut today yet." in flat and "The pages agree" in flat
    assert "lists no holding" in flat and "no time found" in flat and "OLD SOURCE" not in flat
    dry = full_book()
    assert tap.run_once(True, book=dry, now=NOW) == 0 and "Today_Action_Plan" not in dry.tabs and dry.tabs["_Daily_Plan"].hides == 0
    assert "dry run" in capsys.readouterr().out
    monkeypatch.setenv("TFB_TAP_HIDE_TABS", "none")
    keep = full_book()
    assert tap.run_once(False, book=keep, now=NOW) == 0 and keep.tabs["_Daily_Plan"].hides == 0
    monkeypatch.setenv("TFB_TODAY_PLAN", "off")
    off = full_book()
    assert tap.run_once(False, book=off, now=NOW) == 0 and "Today_Action_Plan" not in off.tabs

    class Broken(Book):
        def worksheet(self, name):
            raise RuntimeError("quota")
    monkeypatch.setenv("TFB_TODAY_PLAN", "on")
    assert tap.run_once(False, book=Broken({}), now=NOW) == 2
    assert "nothing written" in capsys.readouterr().out
    assert tap.hide_list("_A, _B ,Today_Action_Plan,bad;name,_A") == ["_A", "_B"] and tap.hide_list(" NONE ") == []


# ----------------------------------------------------------------------------- T4 wiring
def test_t4_workflow_manifest_and_single_write():
    raw = (ROOT / ".github/workflows/today_action_plan.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(raw)
    on = wf[True] if True in wf else wf["on"]
    assert on["workflow_run"] == {"workflows": ["🗓️ Daily Plan", "🎯 Exit Plan"], "types": ["completed"]}
    for name, path in (("🗓️ Daily Plan", "daily_plan.yml"), ("🎯 Exit Plan", "exit_plan.yml")):
        assert yaml.safe_load((ROOT / ".github/workflows" / path).read_text(encoding="utf-8"))["name"] == name
    assert "workflow_dispatch" in on and "schedule" not in on and wf["permissions"] == {"contents": "read"}
    job = wf["jobs"]["today_plan"]
    assert job["if"] == "github.event_name != 'workflow_run' || github.event.workflow_run.conclusion == 'success'"
    step = [s for s in job["steps"] if "today_action_plan.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_TODAY_PLAN"] == "${{ vars.TFB_TODAY_PLAN || 'on' }}" and "--dry-run" in step["run"]
    assert step["env"]["TFB_TAP_HIDE_TABS"] == "${{ vars.TFB_TAP_HIDE_TABS || '_Daily_Plan,_Exit_Plan,_Evidence_Log' }}"
    src = (ROOT / "scripts/today_action_plan.py").read_text(encoding="utf-8")
    for name in re.findall(r"(TFB_T[A-Z_]+): \$\{\{", raw):
        assert '"%s"' % name in src, name
    assert src.count("write_grid(") == 1 and "write_grid(book, TAB," in src
    assert ".del_worksheet" not in src and "delete" not in src.lower().replace("never deletes", "").replace("not deleted", "")
    assert '("today_action_plan", "%s", "today action plan")' % tap.__version__ in (ROOT / "scripts/verify_deployment.py").read_text(
        encoding="utf-8")
