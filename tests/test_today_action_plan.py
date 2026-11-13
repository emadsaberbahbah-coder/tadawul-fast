"""scripts/today_action_plan.py v1.3.2 - the Today Action Plan page (owner, 12 Nov 2026): one page that orders the
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
    _ids = __import__("itertools").count(100)

    def __init__(self, rows=None, hidden=False):
        self.values, self.row_count, self.col_count, self.updates = list(rows or []), 200, 30, 0
        self.isSheetHidden, self.hides = hidden, 0
        self.id, self.merges, self.formats, self.grid, self.sizes, self.plain_updates = next(Sheet._ids), [], [], {}, {}, 0

    def get(self, rng):
        return [list(r) for r in self.values]

    def resize(self, rows=None, cols=None):
        self.row_count, self.col_count = rows or self.row_count, cols or self.col_count

    def update(self, values=None, range_name=None, value_input_option=None):
        self.updates += 1
        self.plain_updates += 1
        self.values = [list(r) for r in values]

    def hide(self):
        self.hides += 1
        self.isSheetHidden = True

    def show(self):
        self.shows = getattr(self, "shows", 0) + 1
        self.isSheetHidden = False


class Book:
    def __init__(self, tabs):
        self.tabs = dict(tabs)
        self.batches = []

    def batch_update(self, body):
        """What Sheets does with one batch, as far as the tests look: whole or not at all."""
        by_id = dict((ws.id, ws) for ws in self.tabs.values())
        for rq in body["requests"]:                                 # refuse the whole batch before applying any of it
            (kind, arg), = rq.items()
            assert kind in ("unmergeCells", "updateSheetProperties", "updateCells", "mergeCells", "repeatCell",
                            "updateDimensionProperties", "autoResizeDimensions"), kind
        self.batches.append(body)
        for rq in body["requests"]:
            (kind, arg), = rq.items()
            if kind == "unmergeCells":
                by_id[arg["range"]["sheetId"]].merges = []
            elif kind == "updateSheetProperties":
                by_id[arg["properties"]["sheetId"]].grid = dict(arg["properties"]["gridProperties"])
            elif kind == "updateCells":
                ws, rg = by_id[arg["range"]["sheetId"]], arg["range"]
                vals = [[""] * rg["endColumnIndex"] for _ in range(rg["endRowIndex"])]
                fmts = [[None] * rg["endColumnIndex"] for _ in range(rg["endRowIndex"])]
                for i, row in enumerate(arg["rows"]):
                    for j, cell in enumerate(row["values"]):
                        v = cell.get("userEnteredValue") or {}
                        vals[i][j] = v.get("numberValue", v.get("stringValue", ""))
                        fmts[i][j] = cell.get("userEnteredFormat")
                ws.values, ws.formats = vals, fmts
                ws.updates += 1
            elif kind == "mergeCells":
                rg = arg["range"]
                by_id[rg["sheetId"]].merges.append((rg["startRowIndex"], rg["endRowIndex"], rg["startColumnIndex"],
                                                    rg["endColumnIndex"]))
            elif kind == "updateDimensionProperties":
                rg = arg["range"]
                for i in range(rg["startIndex"], rg["endIndex"]):
                    by_id[rg["sheetId"]].sizes[(rg["dimension"], i)] = arg["properties"]["pixelSize"]
            elif kind == "repeatCell":
                assert arg["cell"] == {} and arg["fields"] == "userEnteredFormat" and list(arg["range"]) == ["sheetId"]
                by_id[arg["range"]["sheetId"]].formats = []


    def worksheet(self, name):
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]

    def add_worksheet(self, title, rows, cols):
        self.tabs[title] = Sheet()
        return self.tabs[title]


def cards(page):
    """Section A as (step, symbol, what to do, source, levels, why): each step is two rows."""
    a = [i for i, r in enumerate(page) if r[0] == "A. DO TODAY - in this order"][0]
    out, i = [], a + 2
    while i + 1 < len(page) and isinstance(page[i][0], int):
        out.append((page[i][0], page[i][1], page[i][2], page[i + 1][1], page[i + 1][2], page[i + 1][6]))
        i += 2
    return out


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
    assert [s["urgency"] for s in supn] == [1]                                            # one step, sized for the rest
    assert supn[0]["do"] == ("Sell half of SUPN.US (47 shares); then set the stop at 24.25 for the 48 shares that remain "
                             "(broker stop now: 23.00).")
    assert "all 95" not in supn[0]["do"]
    ok = tap.holding_steps(dict(hold["SUPN.US"], broker_stop=24.25), 1.0)
    assert len(ok) == 1 and ok[0]["do"] == "Sell half of SUPN.US (47 shares) and keep a stop at 24.25 on the rest."
    nob = tap.holding_steps(dict(hold["SUPN.US"], broker_stop=None), 1.0)
    assert len(nob) == 1 and nob[0]["do"].endswith("for the 48 shares that remain (broker stop now: none read).")
    assert tap.holding_steps(hold["5023.SR"], 1.0) == []                                  # NO RULE: nothing
    near = dict(hold["GLPI.US"], broker_stop=35.50)                                       # 0.67% under: inside the tolerance
    assert tap.holding_steps(near, 1.0) == [] and len(tap.holding_steps(near, 0.5)) == 1
    sell = tap.holding_steps(dict(hold["GLPI.US"], action="SELL"), 1.0)
    assert sell[0]["urgency"] == 0 and sell[0]["do"] == "Sell all 79 shares of GLPI.US." and len(sell) == 1
    rev = tap.holding_steps(dict(hold["GLPI.US"], action="REVIEW"), 1.0)
    assert rev[0]["urgency"] == 3 and rev[0]["do"].startswith("Review GLPI.US today")
    odd = tap.holding_steps(dict(hold["SUPN.US"], qty=None, broker_stop=24.25), 1.0)
    assert odd[0]["do"] == "Sell half of SUPN.US (half the position) and keep a stop at 24.25 on the rest." and len(odd) == 1
    odd2 = tap.holding_steps(dict(hold["SUPN.US"], qty=None), 1.0)
    assert odd2[0]["do"] == ("Sell half of SUPN.US (half the position); then set the stop at 24.25 for the shares that remain "
                             "(broker stop now: 23.00).")


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
    assert [(s["symbol"], tap.step_kind(s)) for s in d["steps"]] == [
        ("DDI.US", "verify"), ("GLPI.US", "verify"), ("IBOC.US", "verify"), ("SUPN.US", "verify"), ("NMM.US", "verify")]
    assert d["stale"] == ["Old"] and len(d["picks"]) == 3 and len(d["pipeline"]) == 2
    old = tap.build(tap.read_holdings(EXIT), tap.read_plan(LEDGER, dt.date(2026, 11, 13)), ev, [], {}, K)
    assert [s["symbol"] for s in old["steps"]] == ["DDI.US", "GLPI.US", "IBOC.US", "SUPN.US"] and old["picks"] == []
    assert all("update time is unknown" in s["do"] for s in old["steps"])


# ----------------------------------------------------------------------------- T3 the run
def test_t3_full_run_writes_one_page_and_hides_the_ledgers():
    book = full_book()
    assert tap.run_once(False, book=book, now=NOW, capture=plan_capture()) == 0
    page = book.tabs["Today_Action_Plan"].values
    assert all(len(r) == tap.WIDTH for r in page)
    assert page[0][0] == "TODAY ACTION PLAN - Thu 12 Nov 2026" and "Built 2026-11-12 09:00 Riyadh" in page[1][0]
    assert page[2][0] == "3 step(s) on holdings · 1 new buy(s) open · 1 pick(s) waiting for price · 2 page difference(s) to know about"
    a = [i for i, r in enumerate(page) if r[0] == "A. DO TODAY - in this order"][0]
    assert page[a + 2][:2] == [1, "SUPN.US"] and "for the 48 shares that remain" in page[a + 2][2]
    cs = cards(page)                                                # 1.1.0: a step is two rows
    assert cs[1][:4] == (2, "DDI.US", "Raise the stop on DDI.US to 14.80 for all 240 shares.", "Exit_Plan")
    assert cs[1][4] == "was 14.73 · broker stop 14.24 on 240 of 240 shares" and cs[1][5] == "raise the stop 14.73 -> 14.80"
    assert cs[3][:2] == (4, "NMM.US") and cs[3][3] == "Daily_Plan" and len(cs) == 4
    b = [i for i, r in enumerate(page) if str(r[0]).startswith("B. HOLDINGS (5)")][0]
    assert page[b + 2] == ["DDI.US", "DoubleDown", "240", "15.72", "+17.0%", "RAISE STOP", "14.80", "14.24", "16.64",
                           "broker stop below the plan"]
    assert page[b + 3][9] == "stop in place" and page[b + 4][9] == "stop price not read" and page[b + 6][9] == ""
    c = [i for i, r in enumerate(page) if str(r[0]).startswith("C. NEW BUYS - plan of 2026-11-12, cut 2026-11-12 08:25 (VERIFIED)")][0]
    assert page[c + 2][:4] == ["NMM.US", "OPEN", "24", "84.50 - 88.01"] and page[c + 2][7:9] == ["56.2%", "86.1"]
    assert page[c + 3][:2] == ["EFC.US", "WAIT"] and page[c + 3][9] == "price 11.60 is above the entry zone"
    dd = [i for i, r in enumerate(page) if str(r[0]).startswith("D. PIPELINE - next in line (2 of 2)")][0]
    assert page[dd + 2][:1] == ["MBIN.US"] and page[dd + 3][4] == "next in line (rank 9)"
    assert any(r[:4] == ["Tadawul", "2026-11-09", "14.2%", "WASHED OUT"] for r in page)
    assert any(r[:3] == ["S-1 GATE", "", "FAIL · day 28/28"] for r in page)        # 1.1.0: label, then its figures
    assert any(r[:4] == ["C9 stops", "", "MISMATCH", "DDI.US Portfolio_Decision 14.44 vs Exit Plan 14.80"] for r in page)
    ages = {r[0]: (r[2], r[3]) for r in page if r[0] in ("Exit_Plan", "Daily_Plan", "Evidence_Board", "Consistency_Check")}
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
    # 1.1.0: the Evidence Board is rebuilt at most every 6 hours, so it has its own limit (18 h)
    assert page[2][0].endswith("· OLD SOURCE: Consistency_Check, Daily_Plan, Exit_Plan")
    assert any(r[0] == "Exit_Plan" and r[3] == "OLD - refresh it before acting" for r in page)
    assert any(r[0] == "Evidence_Board" and r[2:4] == ["7.3", "current"] for r in page)
    book = full_book()
    assert tap.run_once(False, book=book, now=NOW + dt.timedelta(hours=17)) == 0
    page = book.tabs["Today_Action_Plan"].values
    assert page[2][0].endswith("· OLD SOURCE: Consistency_Check, Daily_Plan, Evidence_Board, Exit_Plan")
    assert tap.stale_limit("Evidence_Board", {"stale_h": 3.0}) == 3.0 and tap.stale_limit("Exit_Plan", {"stale_h": 3, "stale_ev": 18}) == 3
    assert tap.stale_limit("Evidence_Board", {"stale_h": 24.0, "stale_ev": 18.0}) == 24.0       # never shorter than the others
    empty = Book({})
    assert tap.run_once(False, book=empty, now=NOW) == 0
    page = empty.tabs["Today_Action_Plan"].values
    flat = " | ".join(str(c) for r in page for c in r)
    # Codex P2, #966: a source that could not be read is never reported as an all-clear
    assert "Nothing to do today" not in flat and "The pages agree" not in flat
    assert "No step can be given: Consistency_Check, Evidence_Board, Exit_Plan, _Daily_Plan could not be read." in flat
    assert "NOT READ: Consistency_Check, Evidence_Board, Exit_Plan, _Daily_Plan" in page[2][0]
    assert "The Consistency_Check page could not be read." in flat and "No plan has been cut today yet." in flat
    assert "lists no holding" in flat and "no time found" in flat and "OLD SOURCE" not in flat
    calm = [list(r) for r in EXIT]
    for r in calm[7:12]:
        r[13], r[15] = ("NO RULE" if r[0] == "5023.SR" else "HOLD"), r[10]                # every holding HOLD, stops in place
    quiet = full_book(Exit_Plan=Sheet(calm), _Daily_Plan=Sheet([HDR, lrow("2026-11-12", "PICK", 1, "EFC.US", "WAIT")]),
                      Consistency_Check=Sheet(CONS[:5]))
    assert tap.run_once(False, book=quiet, now=NOW) == 0
    qflat = " | ".join(str(c) for r in quiet.tabs["Today_Action_Plan"].values for c in r)
    assert "Nothing to do today" not in qflat and "Verify GLPI.US at IBKR" in qflat
    assert "The pages agree on every checked figure." in qflat
    assert "NOT READ" not in qflat
    part = full_book(Exit_Plan=Sheet([["EXIT PLAN"], ["As of 2026-11-12 07:31 Riyadh"]]))  # holdings table unreadable
    assert tap.run_once(False, book=part, now=NOW) == 0
    pflat = " | ".join(str(c) for r in part.tabs["Today_Action_Plan"].values for c in r)
    assert "INCOMPLETE: Exit_Plan could not be read, so steps may be missing." in pflat and "Verify NMM.US" in pflat
    assert "Buy 24 NMM.US" not in pflat
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
    # 1.1.0: the page goes out as ONE batch; the plain grid is only the fallback when Sheets refuses the styled one
    assert src.count("book.batch_update(") == 2 and src.count("write_grid(") == 1 and "write_grid(book, tab," in src
    assert ".del_worksheet" not in src and "delete" not in src.lower().replace("never deletes", "").replace("not deleted", "")
    assert '("today_action_plan", "%s", "today action plan")' % tap.__version__ in (ROOT / "scripts/verify_deployment.py").read_text(
        encoding="utf-8")


# ----------------------------------------------------------------------------- T5 (1.0.1)
def test_t5_one_step_when_the_exit_plan_could_not_read_the_broker_orders():
    """First live run, 12 Nov: three 'none was read' steps while the stops were in place at the broker."""
    unk = [list(r) for r in EXIT]
    unk[1] = ["As of 2026-11-12 07:31 Riyadh · daily bars to 2026-11-11 · broker orders unknown (last read 2026-10-17 11:20, 622 h old)"]
    for r in unk[7:11]:
        r[15] = ""                                                                       # no broker stop on any row
    assert tap.broker_unknown(unk) == "broker orders unknown (last read 2026-10-17 11:20, 622 h old)"
    assert tap.broker_unknown(EXIT) == "" and tap.broker_unknown([]) == ""
    assert tap.broker_unknown([["x"]] * 6 + [["broker orders unknown"]]) == ""            # only the top rows
    book = full_book(Exit_Plan=Sheet(unk))
    assert tap.run_once(False, book=book, now=NOW) == 0
    page = book.tabs["Today_Action_Plan"].values
    steps = cards(page)
    assert [c[1] for c in steps] == ["DDI.US", "GLPI.US", "IBOC.US", "SUPN.US", "NMM.US"]
    assert all("unverified" in c[2] for c in steps)
    assert not any(c[1] == "ALL" or c[2].startswith(("Sell ", "Raise ", "Buy ")) for c in steps)
    b = [i for i, r in enumerate(page) if str(r[0]).startswith("B. HOLDINGS")][0]
    assert all(page[b + i][5] == "VERIFY" for i in (2, 3, 4, 5))
    assert page[b + 6][5] == "NO RULE"
    sell = [list(r) for r in unk]
    sell[9][13], sell[9][14] = "SELL", "closed below the stop"
    d = tap.build(tap.read_holdings(sell), tap.read_plan([], dt.date(2026, 11, 12)), {}, [], {}, K,
                  (), tap.broker_unknown(sell))
    assert all(tap.step_kind(step) == "verify" for step in d["steps"])
    assert any(step["symbol"] == "IBOC.US" and "update time is unknown" in step["do"] for step in d["steps"])


# ----------------------------------------------------------------------------- T6 (1.0.2)
class APIError(Exception):
    pass


QUOTA = ("APIError: [429]: Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute per user' "
         "of service 'sheets.googleapis.com'")


class Flaky(Sheet):
    """A tab whose first `fail` reads raise `err`."""
    def __init__(self, rows, fail=0, err=None):
        Sheet.__init__(self, rows)
        self.fail, self.err, self.calls = fail, err or APIError(QUOTA), 0

    def get(self, rng):
        self.calls += 1
        if self.calls <= self.fail:
            raise self.err
        return Sheet.get(self, rng)


class FlakyBook(Book):
    """A workbook whose first `fail` batch writes raise `err`."""
    def __init__(self, tabs, fail=0, err=None):
        Book.__init__(self, tabs)
        self.fail, self.err, self.calls = fail, err or APIError(QUOTA), 0

    def batch_update(self, body):
        self.calls += 1
        if self.calls <= self.fail:
            raise self.err
        return Book.batch_update(self, body)


def test_t6_a_sheets_quota_answer_is_waited_out_and_tried_again(monkeypatch, capsys):
    """12 Nov 11:04: two runs stopped with [429] right after the Daily Plan and the Exit Plan finished together."""
    assert tap.quota_error(APIError(QUOTA)) and tap.quota_error(Exception("RESOURCE_EXHAUSTED"))
    resp = type("R", (), {"status_code": 429})()
    e429 = Exception("x")
    e429.response = resp
    assert tap.quota_error(e429)
    assert not tap.quota_error(APIError("APIError: [403]: The caller does not have permission"))
    assert not tap.quota_error(ValueError("row 1429 is short"))
    waits = []
    book = full_book(Exit_Plan=Flaky(EXIT, fail=2))                                     # two quota answers, then the page
    assert tap.run_once(False, book=book, now=NOW, sleep=waits.append) == 0
    assert waits == [20.0, 40.0] and book.tabs["Today_Action_Plan"].updates == 1
    out = capsys.readouterr().out
    assert "Sheets quota reached while reading the source pages: waiting 20 s, then try 1 of 3" in out
    waits = []
    book = full_book(Exit_Plan=Flaky(EXIT, fail=9))                                     # the quota never clears
    assert tap.run_once(False, book=book, now=NOW, sleep=waits.append) == 2
    assert waits == [20.0, 40.0, 65.0] and "Today_Action_Plan" not in book.tabs          # 3 more tries, nothing written
    assert "nothing written" in capsys.readouterr().out
    waits = []
    book = full_book(Exit_Plan=Flaky(EXIT, fail=1, err=APIError("APIError: [403]: The caller does not have permission")))
    assert tap.run_once(False, book=book, now=NOW, sleep=waits.append) == 2 and waits == []   # other errors: no wait
    monkeypatch.setenv("TFB_TAP_RETRIES", "0")
    book = full_book(Exit_Plan=Flaky(EXIT, fail=1))
    assert tap.run_once(False, book=book, now=NOW, sleep=waits.append) == 2 and waits == []   # 0 = as before 1.0.2
    monkeypatch.delenv("TFB_TAP_RETRIES")
    book = FlakyBook(full_book(Today_Action_Plan=Sheet([["old"]])).tabs, fail=1)        # the write meets the quota once
    assert tap.run_once(False, book=book, now=NOW, sleep=waits.append) == 0 and waits == [20.0]
    assert book.tabs["Today_Action_Plan"].values[0][0].startswith("TODAY ACTION PLAN")
    raw = (ROOT / ".github/workflows/today_action_plan.yml").read_text(encoding="utf-8")
    assert "TFB_TAP_RETRIES: ${{ vars.TFB_TAP_RETRIES || '3' }}" in raw
    assert sum(tap.QUOTA_WAITS) * 2 < 10 * 60 - 120                                      # inside the job's 10 minutes


# ----------------------------------------------------------------------------- T7 (1.1.0) the page's design
def test_t7_layout_styles_merges_and_heights():
    book = full_book()
    assert tap.run_once(False, book=book, now=NOW, capture=plan_capture()) == 0
    ws = book.tabs["Today_Action_Plan"]
    page, fm = ws.values, ws.formats
    pg = tap.page_layout(tap.build(tap.read_holdings(EXIT), tap.read_plan(LEDGER, dt.date(2026, 11, 12)),
                                   tap.read_evidence(EVID), tap.read_consistency(CONS), {}, K), "s", "d", K)
    assert len(pg.rows) == len(pg.styles) and all(len(r) == tap.WIDTH == len(st) for r, st in zip(pg.rows, pg.styles))
    assert all(name in tap.STYLES or name == "" for st in pg.styles for name in st)       # every style name exists
    assert len(tap.COL_PX) == tap.WIDTH and tap.page_rows(tap.build([], tap.read_plan([], dt.date(2026, 11, 12)), {}, [], {}, K),
                                                          "s", "d", K)[0][0] == "TODAY ACTION PLAN - d"
    # the title bar, the summary band, the section bands - each one cell across the page
    assert (0, 1, 0, 10) in ws.merges and fm[0][0]["backgroundColor"] == tap._rgb("1B2A41") and fm[0][0]["textFormat"]["fontSize"] == 16
    assert fm[2][0]["backgroundColor"] == tap._rgb(tap.STYLES["summary"]["bg"])            # nothing old, nothing unread
    a = [i for i, r in enumerate(page) if r[0] == "A. DO TODAY - in this order"][0]
    assert (a, a + 1, 0, 10) in ws.merges and fm[a][0]["backgroundColor"] == tap._rgb("0F5C63")
    # a step is a card: number over the kind's name, instruction across C:J, then levels (C:F) and reason (G:J)
    r1, r2 = page[a + 2], page[a + 3]
    assert r1[:3] == [1, "SUPN.US", r1[2]] and r2[0] == "TAKE PROFIT" and r2[1] == "Exit_Plan"
    assert fm[a + 2][0]["backgroundColor"] == fm[a + 3][0]["backgroundColor"] == tap._rgb(tap.CHIP["profit"][0])
    assert fm[a + 2][2]["textFormat"]["bold"] and (a + 2, a + 3, 2, 10) in ws.merges
    assert (a + 3, a + 4, 2, 6) in ws.merges and (a + 3, a + 4, 6, 10) in ws.merges
    kinds = [page[a + 3 + 2 * i][0] for i in range(4)]
    assert kinds == ["TAKE PROFIT", "RAISE STOP", "CONFIRM", "BUY"]                       # SUPN, DDI, IBOC's stop, NMM
    assert [tap.step_kind({"urgency": u}) for u in (0, 1, 2, 2.5, 2.6, 3, 4, 5.002)] == [
        "sell", "profit", "stop", "broker", "broker", "review", "review", "buy"]
    # holdings: the action and the P/L carry their colour; figures sit right
    b = [i for i, r in enumerate(page) if str(r[0]).startswith("B. HOLDINGS")][0]
    assert fm[b + 2][5]["backgroundColor"] == tap._rgb(tap.STYLES["act_stop"]["bg"]) and page[b + 2][5] == "RAISE STOP"
    assert fm[b + 2][4]["textFormat"]["foregroundColor"] == tap._rgb(tap.STYLES["pos"]["fg"])      # +17.0%
    assert fm[b + 4][4]["textFormat"]["foregroundColor"] == tap._rgb(tap.STYLES["neg"]["fg"])      # -1.1%
    assert fm[b + 2][6]["horizontalAlignment"] == "RIGHT" and fm[b + 1][6]["horizontalAlignment"] == "RIGHT"
    assert "backgroundColor" not in fm[b + 3][5] and page[b + 3][5] == "HOLD"
    c = [i for i, r in enumerate(page) if str(r[0]).startswith("C. NEW BUYS")][0]
    assert fm[c + 2][1]["backgroundColor"] == tap._rgb(tap._GOOD[0]) and fm[c + 3][1]["backgroundColor"] == tap._rgb(tap._WARN[0])
    # context: the mismatch is marked, a current source reads green
    m = [i for i, r in enumerate(page) if r[0] == "C9 stops"][0]
    assert fm[m][2]["backgroundColor"] == tap._rgb(tap._WARN[0]) and (m, m + 1, 0, 2) in ws.merges and (m, m + 1, 3, 10) in ws.merges
    g = [i for i, r in enumerate(page) if r[0] == "S-1 GATE"][0]
    assert fm[g][2]["textFormat"]["foregroundColor"] == tap._rgb(tap._BAD[1])              # "FAIL" reads red
    # no cell shows as a link, every cell wraps, and the plain cells are still formatted (old looks are cleared)
    assert all(f["hyperlinkDisplayType"] == "PLAIN_TEXT" and f["wrapStrategy"] == "WRAP" for row in fm[:len(pg.rows)] for f in row)
    # the sheet: gridlines off, title frozen, cut to the page; ten sized columns; a height for every merged row
    assert ws.grid == {"rowCount": len(page), "columnCount": 10, "frozenRowCount": 1, "hideGridlines": True}
    assert [ws.sizes[("COLUMNS", i)] for i in range(10)] == list(tap.COL_PX)
    merged_rows = set(r for r0, r1, _, _ in ws.merges for r in range(r0, r1))
    assert merged_rows and all(("ROWS", r) in ws.sizes for r in merged_rows)
    assert ws.sizes[("ROWS", 0)] == 44 and ws.sizes[("ROWS", a + 2)] >= 34
    body = book.batches[-1]["requests"]
    kinds = [list(rq)[0] for rq in body]
    assert kinds[:3] == ["unmergeCells", "updateSheetProperties", "updateCells"] and len(book.batches) == 1
    assert kinds.index("autoResizeDimensions") < max(i for i, x in enumerate(kinds) if x == "updateDimensionProperties")
    assert body[2]["updateCells"]["fields"] == "userEnteredValue,userEnteredFormat"
    assert ws.plain_updates == 0                                                          # the one batch is the only write
    # values as typed: a number stays a number, text stays text, an empty cell carries no value
    assert tap.cell_data(3, "")["userEnteredValue"] == {"numberValue": 3}
    assert tap.cell_data("=1+1", "")["userEnteredValue"] == {"stringValue": "=1+1"} and "userEnteredValue" not in tap.cell_data("", "cell")
    # heights: wrapped by words, in the cell's own width; a merge takes the tallest of its cells
    assert tap.text_px("short", 600) == tap.text_px("", 600) and tap.text_px("word " * 60, 600) > tap.text_px("word " * 20, 600)
    assert tap.text_px("Tsakos Energy Navigation Limited", 160) > tap.text_px("Tsakos", 160)
    assert tap.text_px("x" * 300, 200) > tap.text_px("x" * 100, 200) and tap.text_px("a\nb\nc", 600) > tap.text_px("a b c", 600)
    p2 = tap._Page()
    r = p2.add(["TEN.US", "Tsakos Energy Navigation Limited", "1", "81.0", "earnings in 7 day(s)"],
               ["cell_b", "cell", "num", "num", "cell"], [(4, 10)])
    assert p2.heights[r] == tap.text_px("Tsakos Energy Navigation Limited", 160) and p2.styles[r][9] == "cell"
    assert p2.add(["a", "b"], "cell") == 1 and 1 not in p2.heights                        # no merge: Sheets sizes the row


def test_t7_plain_fallback_style_off_and_preview(monkeypatch, capsys):
    class Refusing(Book):                                                                # Sheets refuses the styled batch
        def batch_update(self, body):
            if any("updateCells" in rq for rq in body["requests"]):
                raise APIError("APIError: [400]: Invalid requests[2].updateCells")
            return Book.batch_update(self, body)
    book = Refusing(full_book(Today_Action_Plan=Sheet([["old"]])).tabs)
    book.tabs["Today_Action_Plan"].merges = [(6, 8, 0, 1)]                                # left by an earlier styled page
    assert tap.run_once(False, book=book, now=NOW) == 0
    ws = book.tabs["Today_Action_Plan"]
    out = capsys.readouterr().out
    assert ws.plain_updates == 1 and ws.merges == [] and ws.values[0][0].startswith("TODAY ACTION PLAN")
    assert ws.formats == []                                                               # no looks left to hide a value
    assert "::warning::Today Action Plan: the styled write was refused (APIError)" in out and ", plain)" in out
    assert cards(ws.values)[0][:2] == (1, "DDI.US")                                      # every instruction is on the page
    quota = FlakyBook(full_book().tabs, fail=9)                                           # a quota that never clears is
    with pytest.raises(APIError):                                                        # not a reason to write plain
        tap.run_once(False, book=quota, now=NOW, sleep=lambda s: None)
    assert "Today_Action_Plan" not in quota.tabs or quota.tabs["Today_Action_Plan"].plain_updates == 0


    class QuotaOnStyled(Book):                                                           # only the big batch meets the quota
        def batch_update(self, body):
            if any("updateCells" in rq for rq in body["requests"]):
                raise APIError(QUOTA)
            return Book.batch_update(self, body)
    q2 = QuotaOnStyled(full_book(Today_Action_Plan=Sheet([["old"]])).tabs)
    with pytest.raises(APIError):
        tap.run_once(False, book=q2, now=NOW, sleep=lambda s: None)
    assert q2.tabs["Today_Action_Plan"].values == [["old"]] and q2.tabs["Today_Action_Plan"].plain_updates == 0
    # Codex P2, #969: style off AFTER a styled run - the styled page's looks must not stay on the sheet
    off = full_book()
    assert tap.run_once(False, book=off, now=NOW) == 0
    ws = off.tabs["Today_Action_Plan"]
    ws.row_count = before = len(ws.values)                                                # what the styled run left
    assert ws.merges and ws.formats and ws.sizes[("ROWS", 3)] == 8                        # e.g. an 8 px spacer row
    monkeypatch.setenv("TFB_TAP_STYLE", "off")
    assert tap.run_once(False, book=off, now=NOW) == 0
    assert ws.plain_updates == 1 and ", plain)" in capsys.readouterr().out
    assert ws.merges == [] and ws.formats == []
    assert all(ws.sizes[("ROWS", i)] == 21 for i in range(before)) and len(ws.values) >= 80   # rows added later are new
    assert [ws.sizes[("COLUMNS", i)] for i in range(10)] == [tap.PLAIN_COL_PX] * 10
    assert [list(rq)[0] for rq in off.batches[-1]["requests"]] == [
        "unmergeCells", "repeatCell", "updateDimensionProperties", "updateDimensionProperties"]
    monkeypatch.delenv("TFB_TAP_STYLE")
    monkeypatch.setenv("TFB_TAP_PREVIEW", "true")
    pre = full_book(Today_Action_Plan=Sheet([["live"]]))
    assert tap.run_once(False, book=pre, now=NOW) == 0
    assert pre.tabs["Today_Action_Plan"].values == [["live"]] and pre.tabs["Today_Action_Plan"].updates == 0
    assert pre.tabs[tap.PREVIEW_TAB].values[0][0].startswith("TODAY ACTION PLAN") and pre.tabs["_Daily_Plan"].hides == 0
    assert "wrote the Today_Action_Plan_Preview page" in capsys.readouterr().out
    monkeypatch.delenv("TFB_TAP_PREVIEW")
    assert tap.run_once(False, book=pre, now=NOW) == 0                                    # the next normal run
    assert pre.tabs[tap.PREVIEW_TAB].hides == 1 and pre.tabs["Today_Action_Plan"].values[0][0].startswith("TODAY ACTION PLAN")
    monkeypatch.setenv("TFB_TAP_PREVIEW", "true")                                         # Codex P2, #969: a later preview
    assert pre.tabs[tap.PREVIEW_TAB].isSheetHidden
    assert tap.run_once(False, book=pre, now=NOW) == 0
    assert not pre.tabs[tap.PREVIEW_TAB].isSheetHidden and pre.tabs[tap.PREVIEW_TAB].shows == 1   # is shown again
    monkeypatch.delenv("TFB_TAP_PREVIEW")
    tucked = full_book(Today_Action_Plan=Sheet([["live"]], hidden=True))                  # the owner hid the live page:
    assert tap.run_once(False, book=tucked, now=NOW) == 0                                 # a run writes it, never shows it
    assert tucked.tabs["Today_Action_Plan"].isSheetHidden and not hasattr(tucked.tabs["Today_Action_Plan"], "shows")
    assert tucked.tabs["Today_Action_Plan"].updates == 1

    class SlowTab(Book):                                                                 # Codex P2, #969: the quota answers
        def __init__(self, tabs):                                                         # the tab lookup of the write
            Book.__init__(self, tabs)
            self.armed, self.failed = False, 0

        def worksheet(self, name):
            if self.armed and name == "Today_Action_Plan" and self.failed < 2:
                self.failed += 1
                raise APIError(QUOTA)
            return Book.worksheet(self, name)

        def batch_update(self, body):
            return Book.batch_update(self, body)
    slow, waits = SlowTab(full_book().tabs), []
    real_read = tap.read_consistency

    def arm(values):                                                                      # reads are done: arm the lookup
        slow.armed = True
        return real_read(values)
    monkeypatch.setattr(tap, "read_consistency", arm)
    assert tap.run_once(False, book=slow, now=NOW, sleep=waits.append) == 0
    assert waits == [20.0, 40.0] and slow.tabs["Today_Action_Plan"].updates == 1
    raw = (ROOT / ".github/workflows/today_action_plan.yml").read_text(encoding="utf-8")
    assert "TFB_TAP_PREVIEW: ${{ github.event_name == 'workflow_dispatch' && inputs.preview || 'false' }}" in raw
    assert ("TFB_TAP_STYLE: ${{ (github.event_name == 'workflow_dispatch' && inputs.plain) && 'off' || vars.TFB_TAP_STYLE "
            "|| 'on' }}") in raw and "TFB_TAP_STALE_EVIDENCE_H: ${{ vars.TFB_TAP_STALE_EVIDENCE_H || '18' }}" in raw


# ----------------------------------------------------------------------------- T8 (1.2.0) aligned with the other pages
T10 = [["TOP 10 INVESTMENTS - DECISION"], ["Status:", "Last run 2026-11-12 08:52:11 | status: ok | output: QUALIFIED_UNFUNDED"],
       [], ["FEED ACTIONABLE - HELD - 0 EXECUTABLE TICKETS + 3 RESEARCH (NO ALLOCATION)"],
       ["Rank", "Symbol", "Name", "Price", "Ticket SAR", "Shares", "Funds From", "Stability"],
       ["1", "ZTO.US", "ZTO", "20.46", "6,000 SAR", "78", "Cash", "ACTIVE (day 3)"],
       ["2", "NMM.US", "Navios", "88.00", "0 SAR", "0", "Research — no allocation", "FAST-TRACK"],
       ["3", "EFC.US", "Ellington", "11.60", "—", "—", "", "GRACE (1/3 missed)"],
       [], ["ALL QUALIFIED"], ["Rank", "Symbol", "Name"], ["1", "OTHER.US", "x"]]
V2 = [["TOP 10 INVESTMENTS — DECISION v2", "", "EXECUTABLE", "run 1 · cockpit 2026-11-12 06:46:43 · page built 2026-11-12 08:16"],
      ["Feed: released"], [],
      ["#", "Symbol · name", "", "3-month close", "Seat status", "Price", "Alloc SAR", "Buy qty", "Flags"],
      ["2", "TEN.US · Tsakos Energy", "", "", "ACTIVE (day 3)", "47.04 USD", "5,117", "29", "engine ticket"],
      ["4", "NMM.US · Navios", "", "", "FAST-TRACK (day 2)", "88.00 USD", "—", "—", "research"],
      [], ["next table"]]
NEWS = [["Run At (Riyadh)", "Run ID", "Verdict", "Drivers"],
        ["2026-11-11 04:34", "1", "GO", ""],
        ["2026-11-12 03:32", "2", "NO_NEW_ENTRY", "macro: war_escalation/hormuz grade 1, 17 sources"]]
CASH = [["Date", "Time", "Type", "Amount SAR", "Balance SAR", "Delta vs Prev", "Note"],
        ["2026-11-10", "", "SNAPSHOT (broker read)", "33,125.00", "", "", ""],
        ["2026-11-12", "", "CALCULATED SNAPSHOT", "-15,578.18", "17,546.82", "-15,578.18", "deduction"]]


def aligned_book(**over):
    tabs = {"Top_10_Investments": Sheet(T10), "Top_10_Investments_v2": Sheet(V2), "_News_Gate": Sheet(NEWS),
            "_Cash_Snapshot": Sheet(CASH)}
    tabs.update(over)
    return full_book(**tabs)


def test_t8_readers_of_the_top10_pages_the_news_gate_and_cost():
    t = tap.read_top10(T10)
    assert t["read"] and t["built"] == "2026-11-12 08:52" and t["banner"].startswith("FEED ACTIONABLE - HELD")
    assert list(t["seats"]) == ["ZTO.US", "NMM.US", "EFC.US"]                             # stops at the first blank row
    assert t["seats"]["ZTO.US"] == {"ticket": 6000.0, "shares": 78.0, "seat": "Cash"}
    assert t["seats"]["NMM.US"]["seat"] == "Research — no allocation" and t["seats"]["EFC.US"]["seat"] == "GRACE (1/3 missed)"
    v = tap.read_top10_v2(V2)
    assert v["read"] and v["banner"] == "EXECUTABLE" and v["built"] == "2026-11-12 08:16"
    assert v["seats"] == {"TEN.US": {"ticket": 5117.0, "shares": 29.0, "seat": "ACTIVE (day 3)"},
                          "NMM.US": {"ticket": 0.0, "shares": 0.0, "seat": "FAST-TRACK (day 2)"}}
    assert not tap.read_top10([])["read"] and not tap.read_top10_v2([["x"]])["read"]
    broken = [list(r) for r in V2]                                                       # live page, 13 Nov: header cells
    broken[3] = ["#", "Symbol \u00b7 name", "", "3-month\nclose", "Seat\nstatus", "Price", "Alloc\u00a0SAR", "Buy\n qty", "Flags"]
    assert tap.read_top10_v2(broken)["seats"] == v["seats"]                               # broken over two lines
    prev = [list(r) for r in V2]                                                         # live page, 13 Nov 11:16: no engine
    prev[3][6:8] = ["Preview SAR", "Preview qty"]                                        # ticket -> a research preview
    pv = tap.read_top10_v2(prev)
    assert pv["read"] and pv["seats"]["TEN.US"] == {"ticket": 0.0, "shares": 0.0, "seat": "ACTIVE (day 3)", "preview": (5117.0, 29.0)}
    assert "preview" not in pv["seats"]["NMM.US"]
    assert tap._seat_text(pv, "TEN.US") == ("ACTIVE (day 3) · preview only (29 shares), not a ticket", False)
    rows = tap.align(tap.read_plan(LEDGER, dt.date(2026, 11, 12)), t, pv)
    assert [(a["symbol"], a["level"]) for a in rows] == [("NMM.US", "soft"), ("EFC.US", "ok"), ("ZERO.US", "ok"), ("ZTO.US", "hard")]
    wrapped = [list(r) for r in T10]
    wrapped[4] = ["Rank", "Symbol", "Name", "Price", "Ticket\nSAR", "Shares", "Funds\nFrom", "Stability"]
    assert tap.read_top10(wrapped)["seats"] == t["seats"]
    n = tap.read_news_gate(NEWS, NOW)                                                     # 09:00 Riyadh, 12 Nov
    assert (n["verdict"], n["at"]) == ("NO_NEW_ENTRY", "2026-11-12 03:32") and round(n["age_h"], 2) == 5.47
    assert not tap.read_news_gate([], NOW)["read"] and tap.read_news_gate(NEWS[:2], NOW)["verdict"] == "GO"
    assert tap.pick_cost({"alloc": 7800.0, "qty": 1, "price": 1, "ccy": "USD"}) == 7800.0
    assert tap.pick_cost({"qty": 10, "price": 100.0, "ccy": "usd"}) == 3750.0 and tap.pick_cost({"qty": 10, "price": 5.0, "ccy": "SAR"}) == 50.0
    assert tap.pick_cost({"qty": 10, "price": 5.0, "ccy": "GBX"}) is None and tap.pick_cost({"qty": 0, "price": 5.0, "ccy": "USD"}) is None
    # Codex P2, #971: the cost is taken at the highest price the buy step allows, not at the entry price
    assert tap.pick_cost({"alloc": 3750.0, "qty": 10, "price": 100.0, "zone_high": 101.0, "ccy": "USD"}) == 3787.5
    assert tap.pick_cost({"alloc": 3750.0, "qty": 10, "price": 101.5, "zone_high": 101.0, "ccy": "USD"}) == 3806.25
    assert tap.pick_cost({"alloc": 3750.0, "qty": 10, "price": 99.0, "zone_high": 99.5, "ccy": "USD"}) == 3750.0   # never under
    assert tap.pick_cost({"alloc": 900.0, "qty": 10, "price": 5.0, "zone_high": 5.1, "ccy": "GBX"}) == 900.0        # no rate
    tight = [{"symbol": "T", "status": "OPEN", "qty": 10, "rank": 1, "alloc": 3750.0, "price": 101.0, "zone_high": 101.0, "ccy": "USD"}]
    assert tap.fund(tight, 3750.0) == 3750.0 and tight[0]["unfunded"] == (3787.5, 3750.0)
    picks = [{"symbol": "B", "status": "OPEN", "qty": 1, "rank": 2, "alloc": 6000.0},
             {"symbol": "A", "status": "OPEN", "qty": 1, "rank": 1, "alloc": 7000.0},
             {"symbol": "C", "status": "OPEN", "qty": 1, "rank": 3, "alloc": 3000.0},
             {"symbol": "W", "status": "WAIT", "qty": 1, "rank": 0, "alloc": 9000.0}]
    assert tap.fund(picks, 10500.0) == 500.0                                             # A, then C fits; B does not
    assert [p["symbol"] for p in picks if p.get("unfunded")] == ["B"] and picks[0]["unfunded"] == (6000.0, 3500.0)
    assert tap.fund([dict(p, unfunded=None) for p in picks], None) is None                # cash not known: nothing marked


def test_t8_one_order_of_record_and_the_other_pages_flagged(capsys):
    """13 Nov: this page said buy three names, the v2 page sized a fourth, the main page sized none."""
    book = aligned_book()
    assert tap.run_once(False, book=book, now=NOW, capture=plan_capture()) == 0
    page = book.tabs["Today_Action_Plan"].values
    cs = cards(page)
    assert [(c[0], c[1]) for c in cs] == [(1, "SUPN.US"), (2, "DDI.US"), (3, "IBOC.US"), (4, "TEN.US"), (5, "ZTO.US"),
                                          (6, "NEWS"), (7, "NMM.US")]
    a = [i for i, r in enumerate(page) if r[0] == "A. DO TODAY - in this order"][0]
    assert [page[a + 3 + 2 * i][0] for i in (3, 4, 5, 6)] == ["DO NOT BUY", "DO NOT BUY", "READ FIRST", "BUY"]
    assert cs[3][2] == "Do not buy TEN.US from Top_10_Investments_v2: it is not in today's plan."
    assert cs[3][3] == "Top_10_Investments_v2" and cs[3][4] == "shown there: 29 shares, 5,117 SAR"
    assert cs[4][2] == "Do not buy ZTO.US from Top_10_Investments: the plan holds it back (next in line (rank 9))."
    assert cs[5][2] == "Read before buying: the News & Macro Gate reads NO NEW ENTRY (2026-11-12 03:32 Riyadh)."
    assert cs[5][5] == "macro: war_escalation/hormuz grade 1, 17 sources" and cs[5][4].startswith("observe-only gate")
    assert page[2][0] == ("3 step(s) on holdings · 1 new buy(s) open · 1 pick(s) waiting for price · 2 page difference(s) to "
                          "know about · PAGES DISAGREE: ZTO.US, TEN.US · NEWS GATE: NO NEW ENTRY")
    fm = book.tabs["Today_Action_Plan"].formats
    assert fm[2][0]["backgroundColor"] == tap._rgb(tap.STYLES["alert"]["bg"])
    assert fm[a + 2 + 6][0]["backgroundColor"] == tap._rgb(tap.CHIP["skip"][0])           # the TEN card
    g = [i for i, r in enumerate(page) if str(r[0]).startswith("PAGE ALIGNMENT")][0]
    assert page[g + 2][0] == "Top 10 (main)" and page[g + 2][2].endswith("built 2026-11-12 08:52")
    assert page[g + 3][:3] == ["Top 10 v2", "", "EXECUTABLE · built 2026-11-12 08:16"]
    rows = {r[0]: r for r in page[g + 5:g + 10]}
    assert rows["NMM.US"][1] == "OPEN - buy 24" and rows["NMM.US"][3] == "Research — no allocation"
    assert rows["NMM.US"][5] == "FAST-TRACK (day 2)" and rows["NMM.US"][8] == (
        "FOLLOW THIS PAGE: the plan is the order of record; the Top 10 pages show no ticket.")
    assert rows["EFC.US"][1] == "WAIT - price 11.60 is above the entry zone" and rows["EFC.US"][8] == "ALIGNED: no page sizes it."
    assert rows["TEN.US"][1] == "not in today's plan" and rows["TEN.US"][3] == "not seated" and rows["TEN.US"][5] == "ticket: 29 shares, 5,117 SAR"
    assert rows["TEN.US"][8] == "DO NOT BUY from Top_10_Investments_v2: it is not in today's plan."
    assert rows["ZTO.US"][1] == "pipeline: next in line (rank 9)" and rows["ZTO.US"][3] == "ticket: 78 shares, 6,000 SAR"
    assert fm[g + 5 + list(rows).index("TEN.US")][8]["textFormat"]["foregroundColor"] == tap._rgb(tap._BAD[1])
    assert any(r[0] == "NO NEW ENTRY" and r[2] == "2026-11-12 03:32 Riyadh · macro: war_escalation/hormuz grade 1, 17 sources" for r in page)
    assert any(r[0] == "17,547 SAR" and r[2] == ("IBKR capture of 2026-11-12 09:00 Riyadh · every open pick costs 7,921 SAR · left after the "
                                                 "funded buys 9,626 SAR") for r in page)   # 24 x 88.01 x 3.75
    out = capsys.readouterr().out
    assert "ALIGN TEN.US    hard" in out and "news gate NO_NEW_ENTRY (2026-11-12 03:32) · cash read (IBKR capture of 2026-11-12 09:00 Riyadh)" in out and "17,547" not in out
    for name in ("Top_10_Investments", "Top_10_Investments_v2", "_News_Gate", "_Cash_Snapshot"):
        assert book.tabs[name].updates == 0 and book.tabs[name].hides == 0                # read only
    both = aligned_book(Top_10_Investments=Sheet(T10[:6] + [["2", "NMM.US", "Navios", "88", "7,800 SAR", "24", "Cash", ""]]),
                        Top_10_Investments_v2=Sheet(V2[:4] + [["4", "NMM.US · Navios", "", "", "ACTIVE", "", "7,800", "24"]]),
                        _News_Gate=Sheet(NEWS[:2]))
    assert tap.run_once(False, book=both, now=NOW, capture=plan_capture()) == 0
    p2 = both.tabs["Today_Action_Plan"].values
    assert any(r[0] == "NMM.US" and r[8] == "ALIGNED: all three pages size it." for r in p2)
    assert [c[1] for c in cards(p2)] == ["SUPN.US", "DDI.US", "IBOC.US", "ZTO.US", "NMM.US"]   # GO: no READ FIRST step
    assert "NEWS GATE" not in p2[2][0] and p2[2][0].endswith("· PAGES DISAGREE: ZTO.US")


def test_t8_funding_news_age_optional_reads_and_off(monkeypatch, capsys):
    poor = [list(r) for r in CASH] + [["2026-11-12", "", "CALCULATED SNAPSHOT", "-12,546.82", "5,000.00", "", ""]]
    book = aligned_book(_Cash_Snapshot=Sheet(poor), Top_10_Investments_v2=Sheet(V2[:4]), Top_10_Investments=Sheet(T10[:5]))
    assert tap.run_once(False, book=book, now=NOW, capture=plan_capture(cash_sar=5000)) == 0
    page = book.tabs["Today_Action_Plan"].values
    cs = cards(page)
    assert [(c[1]) for c in cs] == ["SUPN.US", "DDI.US", "IBOC.US", "NMM.US"]              # no buy left: no READ FIRST step
    assert cs[3][2] == ("NMM.US is not funded today: its 24 shares need 7,921 SAR and 5,000 SAR of cash is left after "
                        "the buys above.")
    a = [i for i, r in enumerate(page) if r[0] == "A. DO TODAY - in this order"][0]
    assert page[a + 3 + 2 * 3][0] == "NOT FUNDED" and "Buy 24 NMM.US" not in " ".join(str(c) for r in page for c in r)
    cc = [i for i, r in enumerate(page) if str(r[0]).startswith("C. NEW BUYS")][0]
    assert [page[cc + 2 + i][:2] for i in (0, 1)] == [["NMM.US", "NOT FUNDED"], ["EFC.US", "WAIT"]]   # section C says what A says
    assert page[2][0].startswith("3 step(s) on holdings · 0 new buy(s) open") and "· NOT FUNDED: NMM.US · NEWS GATE: NO NEW ENTRY" in page[2][0]
    assert any(r[0] == "NMM.US" and r[1] == "OPEN, not funded today" and r[8] == "ALIGNED: no page sizes it." for r in page)
    fresh_go = aligned_book(_News_Gate=Sheet(NEWS + [["2026-11-12 08:00", "3", "GO", ""]]))   # the latest verdict is GO
    assert tap.run_once(False, book=fresh_go, now=NOW, capture=plan_capture()) == 0
    gp = fresh_go.tabs["Today_Action_Plan"].values
    assert not any(r[1] == "NEWS" for r in gp) and "NEWS GATE" not in gp[2][0] and any(r[0] == "GO" and r[2] == "2026-11-12 08:00 Riyadh · no driver recorded" for r in gp)
    late = aligned_book()                                                                 # the verdict is 29.5 h old
    assert tap.run_once(False, book=late, now=NOW + dt.timedelta(hours=24), capture=plan_capture(now=NOW + dt.timedelta(hours=24))) == 0
    lp = late.tabs["Today_Action_Plan"].values
    assert not any(r[1] == "NEWS" for r in lp) and "NEWS GATE" not in lp[2][0]
    assert any(r[0] == "NO NEW ENTRY" and str(r[2]).startswith("2026-11-12 03:32 Riyadh (older than the limit) · macro") for r in lp)
    monkeypatch.setenv("TFB_TAP_NEWS_MAX_H", "48")
    assert tap.run_once(False, book=late, now=NOW + dt.timedelta(hours=24), capture=plan_capture(now=NOW + dt.timedelta(hours=24))) == 0
    assert "NEWS GATE: NO NEW ENTRY" in late.tabs["Today_Action_Plan"].values[2][0]
    monkeypatch.delenv("TFB_TAP_NEWS_MAX_H")

    class Odd(Sheet):                                                                    # an optional tab that will not read
        def get(self, rng):
            raise RuntimeError("bad range")
    odd = aligned_book(Top_10_Investments=Odd([]), _Cash_Snapshot=Odd([]))
    assert tap.run_once(False, book=odd, now=NOW, capture=plan_capture()) == 0
    op = odd.tabs["Today_Action_Plan"].values
    out = capsys.readouterr().out
    assert "Top_10_Investments not read: RuntimeError" in out and "_Cash_Snapshot not read: RuntimeError" in out
    assert any(r[0] == "Top 10 (main)" and r[2] == "page not read" for r in op)
    assert tiles_of(op)["CASH AT IBKR"][0] == "17,547 SAR"
    assert [c[1] for c in cards(op)] == ["SUPN.US", "DDI.US", "IBOC.US", "TEN.US", "NEWS", "NMM.US"]   # v2 still read
    waits = []
    slow = aligned_book(_News_Gate=Flaky(NEWS, fail=1))                                   # the quota on an optional tab
    assert tap.run_once(False, book=slow, now=NOW, sleep=waits.append, capture=plan_capture()) == 0 and waits == [20.0]
    assert any(r[1] == "NEWS" for r in slow.tabs["Today_Action_Plan"].values)
    monkeypatch.setenv("TFB_TAP_ALIGN", "off")
    off = aligned_book(Top_10_Investments=Odd([]))
    assert tap.run_once(False, book=off, now=NOW) == 0
    fp = off.tabs["Today_Action_Plan"].values
    assert not any(str(r[0]).startswith("PAGE ALIGNMENT") for r in fp)
    assert [c[1] for c in cards(fp)] == ["DDI.US", "GLPI.US", "IBOC.US", "SUPN.US", "NMM.US"]
    assert not any(c[2].startswith("Buy ") for c in cards(fp))
    assert fp[2][0] == "4 step(s) on holdings · 0 new buy(s) open · 1 pick(s) waiting for price · 2 page difference(s) to know about"
    raw = (ROOT / ".github/workflows/today_action_plan.yml").read_text(encoding="utf-8")
    assert "TFB_TAP_ALIGN: ${{ vars.TFB_TAP_ALIGN || 'on' }}" in raw and "TFB_TAP_NEWS_MAX_H: ${{ vars.TFB_TAP_NEWS_MAX_H || '24' }}" in raw


# ----------------------------------------------------------------------------- T9 (1.3.0) the broker's cash and orders
from scripts import ibkr_capture as ic  # noqa: E402


def ib_order(desc, typ, side, second, total, limit=None, status="NEW"):
    o = {"order_status": status, "order_type": typ, "side": side, "total_shares_qty": str(total),
         "remaining_shares_qty": str(total), "primary_description": desc, "secondary_description": second}
    if limit is not None:
        o["limit_price"] = str(limit)
    return o


def ib_capture(orders=None, positions=None, cash=4679.15, settled=4679.15, at="2026-11-12T08:41:01+03:00", now=NOW):
    pos = positions if positions is not None else [("DDI", 240), ("GLPI", 79), ("IBOC", 52), ("SUPN", 95)]
    raw = {"schema": "tfb.ibkr-capture/v1", "captured_at": at,
           "positions": {"positions": [{"contract_description": s, "position": float(q)} for s, q in pos]},
           "balances": {"balances": [{"currency": "USD", "cash_balance": cash, "settled_cash": settled}]},
           "orders": {"orders": orders if orders is not None else [
               ib_order("Sell 120 DDI", "STOP", "SELL", "STP 14.24, GTC", 120),
               ib_order("Sell 79 GLPI", "STOP", "SELL", "STP 35.74, GTC", 79),
               ib_order("Sell 95 SUPN", "STOP", "SELL", "STP 23.00, GTC", 95),
               ib_order("Sell 95 SUPN", "LIMIT", "SELL", "Limit 45.00, GTC", 95, limit=45),
               ib_order("Buy 24 NMM", "LIMIT", "BUY", "Limit 87.90, Day", 24, limit=87.9),
               ib_order("Buy 10 OTHER", "LIMIT", "BUY", "Limit 50.00, GTC", 10, limit=50)]}}
    return ic.parse(raw, None, now, "TFB_IBKR_Capture_20261112-084101.json")


def plan_capture(cash_sar=17546.82, now=NOW):
    """Current custody matching EXIT, with each displayed stop and no working buy."""
    orders = [ib_order("Sell 240 DDI", "STOP", "SELL", "STP 14.24, GTC", 240),
              ib_order("Sell 79 GLPI", "STOP", "SELL", "STP 35.74, GTC", 79),
              ib_order("Sell 52 IBOC", "STOP", "SELL", "STP, GTC", 52),
              ib_order("Sell 95 SUPN", "STOP", "SELL", "STP 23.00, GTC", 95)]
    return ib_capture(orders=orders, cash=cash_sar / tap.PEG, settled=cash_sar / tap.PEG,
                      at=now.astimezone(tap.RIYADH).isoformat(), now=now)


def tiles_of(page):
    i = [n for n, r in enumerate(page) if r[0] in ("CASH AT IBKR", "CASH (LEDGER)", "CASH")][0]
    return dict((page[i][c], (page[i + 1][c], page[i + 2][c])) for c in range(0, 10, 2))


def test_t9_the_page_follows_the_brokers_cash_and_orders(capsys):
    """Owner, 13 Nov 11:51: the action plan should be as per the cash available on IBKR."""
    unk = [list(r) for r in EXIT]
    unk[1] = ["As of 2026-11-12 07:31 Riyadh · broker orders unknown (last read 2026-10-17 18:15, 618 h old)"]
    for r in unk[7:11]:
        r[15] = ""                                                                       # the workbook read no broker stop
    book = aligned_book(Exit_Plan=Sheet(unk))
    assert tap.run_once(False, book=book, now=NOW, capture=ib_capture()) == 0
    page = book.tabs["Today_Action_Plan"].values
    cs = cards(page)
    assert [c[1] for c in cs] == ["SUPN.US", "DDI.US", "IBOC.US", "TEN.US", "ZTO.US", "NEWS", "NMM.US"]   # no "ALL" step
    assert cs[1][2] == "Raise the stop on DDI.US to 14.80 for all 240 shares." and cs[1][4] == "was 14.73 · broker stop 14.24 on 120 of 240 shares"
    assert cs[2][2] == "No stop order is working at IBKR for IBOC.US: place one at 63.20 for all 52 shares."
    assert cs[6][2] == "The buy order for NMM.US is working at IBKR: 24 shares at 87.90. After it fills, place the stop at 80.17."
    assert cs[6][4] == "plan: 24 shares · limit 84.50 - 88.01 · first target 95.00"
    a = [i for i, r in enumerate(page) if r[0] == "A. DO TODAY - in this order"][0]
    assert page[a + 3 + 2 * 6][0] == "PLACED" and "for the 48 shares that remain (broker stop now: 23.00)" in cs[0][2]
    assert any(r[:2] == ["NMM.US", "PLACED"] for r in page)                               # section C says what A says
    b = [i for i, r in enumerate(page) if str(r[0]).startswith("B. HOLDINGS")][0]
    assert page[b][0] == "B. HOLDINGS (5) - broker stops from the IBKR capture of 2026-11-12 08:41 Riyadh"
    notes = dict((page[b + i][0], (page[b + i][7], page[b + i][9])) for i in (2, 3, 4, 5, 6))
    assert notes == {"DDI.US": ("14.24", "stop covers 120 of 240 shares"), "GLPI.US": ("35.74", "stop in place"),
                     "IBOC.US": ("", "no stop at IBKR"), "SUPN.US": ("23.00", "broker stop below the plan"), "5023.SR": ("", "")}
    fm = book.tabs["Today_Action_Plan"].formats
    assert fm[b + 2][9]["textFormat"]["foregroundColor"] == tap._rgb(tap._BAD[1]) == fm[b + 4][9]["textFormat"]["foregroundColor"]
    t = tiles_of(page)                                             # (4,679.15 - 10 x 50 on another name) x 3.75
    assert t["CASH AT IBKR"] == ("15,672 SAR", "IBKR capture of 2026-11-12 08:41 Riyadh")
    # the working order counts at its own 24 shares x 87.90 x 3.75 = 7,911 (Codex P1, #972), not at the plan's top limit
    assert t["BUYS TODAY"] == ("1 of 1", "7,911 SAR · 1 placed") and t["CASH AFTER THE BUYS"][0] == "7,761 SAR"
    assert t["NEWS GATE"][0] == "NO NEW ENTRY" and t["PAGES"] == ("DISAGREE", "do not buy: ZTO.US, TEN.US")
    ti = [n for n, r in enumerate(page) if r[0] == "CASH AT IBKR"][0]
    assert fm[ti + 1][6]["textFormat"]["foregroundColor"] == tap._rgb(tap._BAD[1]) and (ti, ti + 1, 0, 2) in book.tabs["Today_Action_Plan"].merges
    assert any(r[0] == "Cash (IBKR read-only capture)" for r in page)
    assert any("1,875 SAR is held by working buy orders on other names and is not counted." in str(r[1]) for r in page)
    assert book.tabs["Today_Action_Plan"].grid["rowCount"] == len(page)
    out = capsys.readouterr().out
    assert "15,672" not in out and "4,679" not in out and "cash read (IBKR capture of 2026-11-12 08:41 Riyadh)" in out


def test_t9_stop_states_fallbacks_and_a_stop_the_broker_already_carries(capsys):
    full = [ib_order("Sell 240 DDI", "STOP", "SELL", "STP 14.80, GTC", 240), ib_order("Sell 79 GLPI", "STOP", "SELL", "STP 35.74, GTC", 79),
            ib_order("Sell 52 IBOC", "STOP", "SELL", "STP, GTC", 52), ib_order("Sell 95 SUPN", "STOP", "SELL", "STP 24.25, GTC", 95)]
    book = full_book()
    assert tap.run_once(False, book=book, now=NOW, capture=ib_capture(full)) == 0
    page = book.tabs["Today_Action_Plan"].values
    cs = cards(page)
    assert [c[1] for c in cs] == ["SUPN.US", "IBOC.US", "NMM.US"]                          # DDI's raise is already at the broker
    assert cs[1][2] == "Check the stop on IBOC.US at IBKR: its price could not be read. The plan's stop is 63.20."
    assert cs[2][2].startswith("Buy 24 NMM.US")                                           # no order working: an ordinary buy
    assert any(r[0] == "DDI.US" and r[9] == "stop in place" for r in page) and any(r[0] == "IBOC.US" and r[9] == "stop price not read" for r in page)
    h = {"symbol": "X.US", "qty": 100.0, "stop": 10.0, "action": "HOLD", "why": "", "broker_stop": 9.95, "broker_cover": 100.0}
    assert tap.stop_state(h, 1.0) == "ok" and tap.stop_state(dict(h, broker_stop=9.80), 1.0) == "low"
    assert tap.stop_state(dict(h, broker_cover=0.0, broker_stop=None), 1.0) == "none" and tap.stop_state(dict(h, broker_cover=99.0), 1.0) == "partial"
    assert tap.stop_state(dict(h, broker_stop=None), 1.0) == "unpriced" and tap.stop_state(dict(h, broker_cover=None), 1.0) == ""
    assert tap.stop_state(dict(h, stop=None), 1.0) == ""
    low = tap.holding_steps(dict(h, broker_stop=9.80), 1.0)
    assert low[0]["do"] == "The broker stop on X.US (9.80) is below the plan's stop (10.00): raise it for all 100 shares."
    part = tap.holding_steps(dict(h, broker_cover=60.0), 1.0)
    assert part[0]["do"] == "The stop on X.US covers 60 of 100 shares: add a stop at 10.00 for the other 40." and part[0]["urgency"] == 2.4
    raise_done = dict(h, action="RAISE STOP", broker_stop=10.0)
    assert tap.holding_steps(raise_done, 1.0) == [] and len(tap.holding_steps(dict(raise_done, broker_stop=9.99), 1.0)) == 1
    assert len(tap.holding_steps(dict(raise_done, broker_cover=50.0), 1.0)) == 1          # raised, but on half the shares
    away = ib_capture(positions=[("DDI", 240), ("GLPI", 79), ("SUPN", 95)])               # IBOC is held elsewhere
    b2 = full_book()
    assert tap.run_once(False, book=b2, now=NOW, capture=away) == 0
    assert any(c[2] == "Verify IBOC.US at IBKR before acting: the holding is absent from the broker capture." for c in cards(b2.tabs["Today_Action_Plan"].values))
    assert any(r[0] == "IBOC.US" and r[9] == "the holding is absent from the broker capture" for r in b2.tabs["Today_Action_Plan"].values)
    capsys.readouterr()
    for cap, why in ((ib_capture(at="2026-11-11T02:00:00+03:00"), "the newest capture is 31 h old (limit 30 h)"),
                     ({"ok": False, "reason": "no capture file visible in the folder (is it shared with the service account?)"},
                      "no capture file visible in the folder (is it shared with the service account?)"),
                     (None, "no capture given")):
        b3 = aligned_book()
        assert tap.run_once(False, book=b3, now=NOW, capture=cap) == 0
        p3 = b3.tabs["Today_Action_Plan"].values
        assert tiles_of(p3)["CASH (LEDGER)"][0] == "17,547 SAR" and any("IBKR capture not used: %s." % why in str(r[1]) for r in p3)
        assert "IBKR capture not used: %s" % why in capsys.readouterr().out
        assert any(r[0] == "IBOC.US" and "unverified" in r[9] for r in p3)       # the workbook's own column, as before
    tight = ib_capture(cash=1000.0, settled=1000.0)                                       # the placed order is not covered
    b4 = aligned_book()
    assert tap.run_once(False, book=b4, now=NOW, capture=tight) == 0
    p4 = b4.tabs["Today_Action_Plan"].values
    assert any(c[1] == "NMM.US" and c[2].endswith("Cash does not cover it: it needs 7,911 SAR and 1,875 SAR is left.") for c in cards(p4))
    assert tiles_of(p4)["BUYS TODAY"] == ("1 of 1", "no open buy · 1 placed · 1 not funded")
    assert tiles_of(p4)["CASH AFTER THE BUYS"][0] == "0 SAR"                                # the order stands at the broker
    assert tap.apply_capture([], tap.read_plan([], dt.date(2026, 11, 12)), ib_capture(cash=5000.0, settled=4000.0), 30)["cash"] == pytest.approx((4000.0 - 500 - 2109.6) * 3.75)   # no pick: every buy order is held back


def test_t9_review_fixes_actual_commitments_partial_fills_and_unpriced_reservations(capsys):
    """Codex P1 x3 on #972."""
    # (1) an order already working counts at its own shares and limit, and before any unplaced pick
    a = {"symbol": "A.US", "status": "OPEN", "qty": 10.0, "rank": 2, "alloc": 375.0, "price": 10.0, "zone_high": 10.0,
         "ccy": "USD", "placed": [(10.0, 80.0)]}
    b = {"symbol": "B.US", "status": "OPEN", "qty": 50.0, "rank": 1, "alloc": 1875.0, "price": 10.0, "zone_high": 10.0, "ccy": "USD"}
    assert tap.commit_cost(a) == 3000.0 and tap.commit_cost(b) == 1875.0                    # 80 x 10 x 3.75, not 10 x 10
    assert tap.fund([a, b], 3750.0) == 750.0 and "unfunded" not in a and b["unfunded"] == (1875.0, 750.0)   # 1,000 USD
    mkt = dict(a, placed=[(None, 20.0), (9.0, 10.0)], zone_high=10.5)
    assert tap.commit_cost(mkt) == (20 * 10.5 + 10 * 9.0) * 3.75                            # a market order: the plan's top
    assert tap.commit_cost(dict(a, ccy="GBX")) == 375.0                                    # no rate: the plan's allocation
    over = dict(a, placed=[(10.0, 200.0)])
    c = dict(b)
    c.pop("unfunded", None)
    assert tap.fund([over, c], 3750.0) == 0.0 and over["unfunded"] == (7500.0, 3750.0) and c["unfunded"] == (1875.0, 0.0)
    # (2) a partially filled order is still working for the shares that remain
    part = [ib_order("Sell 240 DDI", "STOP", "SELL", "STP 14.80, GTC", 240, status="PARTIALLY_FILLED"),
            ib_order("Buy 24 NMM", "LIMIT", "BUY", "Limit 87.90, Day", 24, limit=87.9, status="Partially Filled"),
            ib_order("Sell 79 GLPI", "STOP", "SELL", "STP 35.74, GTC", 79, status="Filled"),
            ib_order("Sell 52 IBOC", "STOP", "SELL", "STP 63.20, GTC", 52, status="Api Cancelled"),
            ib_order("Sell 95 SUPN", "STOP", "SELL", "STP 24.25, GTC", 95, status="PENDING_CANCEL")]
    part[0]["remaining_shares_qty"], part[1]["remaining_shares_qty"] = "140", "9"
    cap = ib_capture(part)
    assert cap["stops"] == {"DDI.US": [(14.8, 140.0)], "SUPN.US": [(24.25, 95.0)]}          # filled and cancelled: gone
    assert cap["buys"] == {"NMM.US": [(87.9, 9.0)]} and cap["n_orders"] == 3
    # (3) a working buy on another name without a limit: the broker's cash is not used for the funding check
    odd = ib_capture([ib_order("Buy 10 OTHER", "MARKET", "BUY", "Market, Day", 10),
                      ib_order("Sell 79 GLPI", "STOP", "SELL", "STP 35.74, GTC", 79)])
    book = aligned_book()
    assert tap.run_once(False, book=book, now=NOW, capture=odd) == 0
    page = book.tabs["Today_Action_Plan"].values
    assert tiles_of(page)["CASH (LEDGER)"][0] == "17,547 SAR"                               # the established fallback
    assert any("IBKR cash not used: a working buy order on another name has no limit price" in str(r[1]) for r in page)
    assert not any(c[2].startswith("Buy ") for c in cards(page))
    assert any(c[1] == "NMM.US" and "reservation has no limit price" in c[2] for c in cards(page))
    assert any(r[0] == "NMM.US" and r[1] == "VERIFY" for r in page)
    assert tiles_of(page)["CASH AFTER THE BUYS"][0] == "-"
    assert "IBKR cash not used: a working buy order has no limit price" in capsys.readouterr().out
    assert any(r[0] == "GLPI.US" and r[9] == "stop in place" for r in page)                 # the orders are still used
    assert str([r for r in page if str(r[0]).startswith("B. HOLDINGS")][0][0]).endswith("IBKR capture of 2026-11-12 08:41 Riyadh")
    assert tap.apply_capture([], tap.read_plan([], dt.date(2026, 11, 12)), odd, 30)["cash"] is None
    own = ib_capture([ib_order("Buy 24 NMM", "MARKET", "BUY", "Market, Day", 24)])          # a market order on the PICK
    b2 = aligned_book()
    assert tap.run_once(False, book=b2, now=NOW, capture=own) == 0                          # is priced at the plan's top
    assert tiles_of(b2.tabs["Today_Action_Plan"].values)["BUYS TODAY"] == ("1 of 1", "7,921 SAR · 1 placed")


@pytest.mark.parametrize("capture,reason", [
    (None, "no capture given"),
    ({"ok": False, "reason": "capture has no USD balance"}, "capture has no USD balance"),
    ("stale", "31 h old"),
])
def test_t10_custody_missing_or_stale_keeps_calculated_cash_as_research(capture, reason):
    if capture == "stale":
        capture = ib_capture(at="2026-11-11T02:00:00+03:00")
    book = aligned_book()
    assert tap.run_once(False, book=book, now=NOW, capture=capture) == 0
    page = book.tabs["Today_Action_Plan"].values
    steps = cards(page)
    assert all(not c[2].startswith(("Buy ", "Sell ", "Raise ")) for c in steps)
    assert any(c[1] == "NMM.US" and reason in c[2] and "research: 24 shares" in c[4] for c in steps)
    assert tiles_of(page)["CASH (LEDGER)"][0] == "17,547 SAR"
    assert tiles_of(page)["CASH (LEDGER)"][1].startswith("research only:")
    assert tiles_of(page)["CASH AFTER THE BUYS"][0] == "-"
    row = next(i for i, r in enumerate(page) if r[0] == "NMM.US" and r[1] == "VERIFY")
    assert reason in page[row][9]
    assert book.tabs["Today_Action_Plan"].formats[row][1]["backgroundColor"] == tap._rgb(tap._WARN[0])


def test_t10_partial_fill_quantity_mismatch_reconciles_before_any_exit_or_stop():
    cap = ib_capture(positions=[("DDI", 40), ("GLPI", 79), ("IBOC", 52), ("SUPN", 95)])
    book = aligned_book()
    assert tap.run_once(False, book=book, now=NOW, capture=cap) == 0
    page = book.tabs["Today_Action_Plan"].values
    ddi = next(c for c in cards(page) if c[1] == "DDI.US")
    assert ddi[2] == "Verify DDI.US at IBKR before acting: plan quantity 240 differs from broker quantity 40."
    assert not any(c[1] == "DDI.US" and c[2].startswith(("Sell ", "Raise ")) for c in cards(page))
    assert any(r[0] == "DDI.US" and r[5] == "VERIFY" for r in page)
    assert any(c[1] == "NMM.US" and "working at IBKR" in c[2] for c in cards(page))
    assert any(c[1] == "SUPN.US" and c[2].startswith("Sell half") for c in cards(page))


@pytest.mark.parametrize("tab,rows,reason", [
    ("Exit_Plan", [[EXIT[0][0]], ["As of 2026-11-11 07:31 Riyadh"]] + EXIT[2:], "Exit_Plan is 25.5 h old"),
    ("Exit_Plan", [[EXIT[0][0]], ["time unknown"]] + EXIT[2:], "Exit_Plan update time is unknown"),
    ("Daily_Plan", [["DAILY PLAN"], ["checked 2026-11-11 08:40"]], "Daily_Plan is 24.3 h old"),
    ("Daily_Plan", [["DAILY PLAN"], ["time unknown"]], "Daily_Plan update time is unknown"),
])
def test_t10_source_currentness_applies_before_order_advice(tab, rows, reason):
    book = aligned_book(**{tab: Sheet(rows)})
    assert tap.run_once(False, book=book, now=NOW, capture=plan_capture()) == 0
    steps = cards(book.tabs["Today_Action_Plan"].values)
    source = "Exit_Plan" if tab == "Exit_Plan" else "Daily_Plan"
    affected = [c for c in steps if c[3] == source]
    assert affected and all(reason in c[2] for c in affected)
    assert all(not c[2].startswith(("Buy ", "Sell ", "Raise ")) for c in affected)


def test_t10_working_order_stays_visible_when_source_or_funding_needs_verification():
    orders = [ib_order("Buy 24 NMM", "LIMIT", "BUY", "Limit 87.90, Day", 24, limit=87.9),
              ib_order("Buy 10 OTHER", "MARKET", "BUY", "Market, Day", 10)]
    old = [["DAILY PLAN"], ["checked 2026-11-11 08:40"]]
    book = aligned_book(Daily_Plan=Sheet(old))
    assert tap.run_once(False, book=book, now=NOW, capture=ib_capture(orders)) == 0
    page = book.tabs["Today_Action_Plan"].values
    order = next(c for c in cards(page) if c[1] == "NMM.US")
    assert "24 shares is already working at IBKR" in order[2]
    assert "Verify NMM.US" in order[2] and "place the stop" not in order[2]
    assert any(r[0] == "NMM.US" and r[1] == "PLACED" and "Daily_Plan is 24.3 h old" in r[9] for r in page)
    assert tiles_of(page)["CASH AFTER THE BUYS"][0] == "-"


def test_t10_repeated_build_drops_previous_order_and_funding_annotations():
    holdings, plan = tap.read_holdings(EXIT), tap.read_plan(LEDGER, NOW.astimezone(tap.RIYADH).date())
    ages = {"Exit_Plan": 1.5, "Daily_Plan": 0.3}
    broker = tap.apply_capture(holdings, plan, ib_capture(), 30)
    first = tap.build(holdings, plan, {}, [], ages, K, extra={"broker": broker, "cash": 0.0})
    pick = first["picks"][0]
    assert pick.get("placed") and pick.get("unfunded")

    broker = tap.apply_capture(holdings, plan, None, 30)
    second = tap.build(holdings, plan, {}, [], ages, K, extra={"broker": broker, "cash": 17546.82})
    pick = second["picks"][0]
    assert "placed" not in pick and "unfunded" not in pick and pick.get("review_reason")
    assert any(s["symbol"] == "NMM.US" and tap.step_kind(s) == "verify" for s in second["steps"])

    broker = tap.apply_capture(holdings, plan, plan_capture(), 30)
    third = tap.build(holdings, plan, {}, [], ages, K, extra={"broker": broker, "cash": broker["cash"]})
    pick = third["picks"][0]
    assert all(k not in pick for k in ("placed", "unfunded", "review_reason"))
    assert any(s["symbol"] == "NMM.US" and s["do"].startswith("Buy 24") for s in third["steps"])


# ----------------------------------------------------------------------------- T11 (1.3.2) a position over the cap
def test_t11_a_position_over_the_cap_is_listed_and_not_counted_as_a_difference():
    cap_row = ["C12 position cap", "OVER CAP", "above the position cap of 15%: AAA.US 18.0% (Exit Plan: RAISE STOP)"]
    cons = [list(r) for r in CONS] + [cap_row, ["C1 decision", "INFO", "open picks the main page has not sized"]]
    got = tap.read_consistency(cons)
    assert [x["status"] for x in got] == ["MISMATCH", "STALE", "OVER CAP"] and got[2]["check"] == "C12 position cap"
    assert tap.STATUS_STYLE["OVER CAP"] == "st_warn"
    book = full_book()
    book.tabs["Consistency_Check"].values = cons
    assert tap.run_once(False, book=book, now=NOW) == 0
    page = book.tabs["Today_Action_Plan"].values
    assert "· 2 page difference(s) to know about" in page[2][0] and "· OVER THE POSITION CAP" in page[2][0]
    at = [i for i, r in enumerate(page) if r[0] == "Page differences (Consistency_Check)"][0]
    block = page[at + 2:at + 5]
    assert [r[2] for r in block] == ["MISMATCH", "STALE", "OVER CAP"] and block[2][0] == "C12 position cap"
    assert block[2][3].startswith("above the position cap of 15%: AAA.US 18.0%")
    plain = full_book()                                                               # without the row: no flag
    assert tap.run_once(False, book=plain, now=NOW) == 0
    assert "OVER THE POSITION CAP" not in plain.tabs["Today_Action_Plan"].values[2][0]
