#!/usr/bin/env python3
"""harness_vd_130.py — proof battery for validate_dashboard.py v1.3.0 (W1A-1).

Imports the REAL module (top-level imports are stdlib-only; engine/registry
loads are lazy) and drives the REAL functions. Doubles exist only at the
external boundaries (a fake read_range callable and a minimal registry
object). Fixture cockpit grids reproduce the LIVE 2026-08-23 Top_10 export
shape — banner row 17, header row 18, 1050.SR ACTIVE with sizing withheld —
so the P0-5 tripwire is proven against reality, then against a synthetic
regression. Exit 0 iff all checks pass.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import sys

MODULE_PATH = os.environ.get("VD_UNDER_TEST", "/home/claude/new_vd_130.py")
spec = importlib.util.spec_from_file_location("vd_under_test", MODULE_PATH)
V = importlib.util.module_from_spec(spec)
sys.modules["vd_under_test"] = V
spec.loader.exec_module(V)  # type: ignore[union-attr]

PASS = FAIL = 0
def check(name, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  PASS  {name}")
    else:    FAIL += 1; print(f"  FAIL  {name}  {detail}")

for k in ("VALIDATE_DECISION_SURFACE", "VALIDATE_FULL", "VALIDATE_T10_HEADER_SCAN"):
    os.environ.pop(k, None)

# --------------------------------------------------------------------------- #
print("== H1  version + additive exports ==")
check("SCRIPT_VERSION 1.3.0", V.SCRIPT_VERSION == "1.3.0")
check("check_top10_surface exported", "check_top10_surface" in V.__all__)
check("legacy check_top10 retained", callable(V.check_top10))

print("== H2  decision-page routing predicates ==")
check("Top_10 is decision page", V._is_decision_page("Top_10_Investments"))
check("case-insensitive", V._is_decision_page("top_10_investments"))
check("market page is not", not V._is_decision_page("Global_Markets"))
check("surface enabled by default", V._decision_surface_enabled() is True)
os.environ["VALIDATE_DECISION_SURFACE"] = "0"
check("kill-switch restores v1.2.0 scope", V._decision_surface_enabled() is False)
os.environ.pop("VALIDATE_DECISION_SURFACE")
check("Top_10 back in default pages", "Top_10_Investments" in V._DEFAULT_PAGES)
check("five market pages unchanged", V._DEFAULT_PAGES[:5] == [
    "Market_Leaders", "Global_Markets", "Commodities_FX",
    "Mutual_Funds", "My_Portfolio"])

# --------------------------------------------------------------------------- #
# Fixture: the LIVE 2026-08-23 cockpit shape (condensed columns)
HDR = ["Rank", "Symbol", "Name", "Market", "Price", "Ticket SAR", "Shares"]
def cockpit(banner, rows):
    g = [["TOP 10 INVESTMENTS — DECISION"], [""], ["Status:", "Last run ..."]]
    g += [[""]] * 12
    g.append([banner])
    g.append(HDR)
    g += rows
    g.append(["", "", "", "", "", "", ""])          # blank ends SELECTED grid
    g.append(["ALL QUALIFIED — INVEST opportunity set (1)"])
    g.append(["1", "9999.SR", "Ghost", "SAU", "1.00", "9,999", "9"])  # later section
    return g

BLOCKED = "⛔ FEED NOT ACTIONABLE — no verdict published — SIZING WITHHELD"
LIVE_ROWS = [
    ["—", "1150.SR", "Alinma Bank", "SAU", "24.86", "—", "—"],
    ["2",  "1050.SR", "Banque Saudi Fransi", "SAU", "20.79", "—", "—"],
]

print("== H3  header + banner finders on the live shape ==")
g = cockpit(BLOCKED, LIVE_ROWS)
hr = V._find_decision_header(g)
check("header found at row 16 (0-based)", hr == 16, f"hr={hr}")
check("banner text located", "NOT ACTIONABLE" in V._find_feed_banner(g, hr))
check("no banner above header -> ''", V._find_feed_banner(g[:3], 3) == "")
check("registry-token detector still can't see it (v1.1.0 WHY intact)",
      V._detect_header_row(g, {"symbol", "current_price", "day_high"}) == -1)

print("== H4  P0-5 tripwire: live-blocked grid PASSES ==")
res = {c.name: c for c in V.check_top10_surface("Top_10_Investments", g)}
check("header_found PASS", res["decision.header_found"].status == "PASS")
check("banner PASS", res["decision.feed_banner_present"].status == "PASS")
c = res["decision.sizing_withheld_when_blocked"]
check("withheld PASS on '—' sizing", c.status == "PASS" and c.count == 2, c.detail)
check("later ALL-QUALIFIED section NOT parsed as selected",
      "9999.SR" not in " ".join(c.examples))

print("== H5  P0-5 tripwire: regression grid FAILS with the symbol named ==")
bad = cockpit(BLOCKED, [
    ["1", "1050.SR", "Banque Saudi Fransi", "SAU", "20.79", "8,378", "403"],
])
res = {c.name: c for c in V.check_top10_surface("Top_10_Investments", bad)}
c = res["decision.sizing_withheld_when_blocked"]
check("FAIL on funded ticket while blocked", c.status == "FAIL" and c.count == 1)
check("example names 1050.SR with sizing",
      c.examples and "1050.SR" in c.examples[0] and "8,378" in c.examples[0])
check("FAIL drives exit 2 (daily_sync reds)",
      V._exit_code(list(res.values())) == 2)

print("== H6  executable feed path ==")
ok = cockpit("FEED EXECUTABLE — verdict published 08:58",
             [["1", "1050.SR", "BSF", "SAU", "20.79", "8,378", "403"],
              ["2", "9002.SR", "NoPrice", "SAU", "", "1,000", "50"]])
res = {c.name: c for c in V.check_top10_surface("Top_10_Investments", ok)}
check("withheld SKIP when not blocked",
      res["decision.sizing_withheld_when_blocked"].status == "SKIP")
p = res["decision.price_present"]
check("price_present WARN names the gap", p.status == "WARN" and p.count == 1
      and p.examples == ["9002.SR"])

print("== H7  degraded grids ==")
res = V.check_top10_surface("T", [["random"], ["noise"]])
check("no header -> single FAIL", len(res) == 1
      and res[0].name == "decision.header_found" and res[0].status == "FAIL")
nb = cockpit(BLOCKED, LIVE_ROWS); nb[15] = ["no verdict text here"]  # 15 = banner row (16 is the header, per H3)
res = {c.name: c for c in V.check_top10_surface("T", nb)}
check("missing banner -> FAIL + treated as not-blocked",
      res["decision.feed_banner_present"].status == "FAIL"
      and res["decision.sizing_withheld_when_blocked"].status == "SKIP")

print("== H8  sanity Open checks (REAL check_sanity) ==")
K2H = {"symbol": "Symbol", "current_price": "Price", "open_price": "Open",
       "day_high": "Day High", "day_low": "Day Low",
       "week_52_high": "52W High", "week_52_low": "52W Low"}
ACT = set(K2H.values())
def row(sym, o, lo, hi, p=10.0):
    return {"Symbol": sym, "Price": p, "Open": o, "Day Low": lo, "Day High": hi,
            "52W High": 100, "52W Low": 1}
rows = [row("GOOD", 10, 9, 11), row("HIGHOPEN", 185.3, 27.02, 28.11),
        row("BLANK", "", 9, 11), row("LOWOPEN", 5, 9, 11)]
res = {c.name: c for c in V.check_sanity("GM", K2H, ACT, rows)}
op = res["sanity.open_present"]; orng = res["sanity.open_in_day_range"]
check("open_present WARN n=1 names BLANK",
      op.status == "WARN" and op.count == 1 and op.examples == ["BLANK"])
check("open_in_day_range WARN n=2 with values",
      orng.status == "WARN" and orng.count == 2
      and any("HIGHOPEN=185.3" in e for e in orng.examples))
all_blank = [row(f"S{i}", "", 9, 11) for i in range(3)]
res = {c.name: c for c in V.check_sanity("ML", K2H, ACT, all_blank)}
check("entirely-blank Open -> SKIP structural (ML case)",
      res["sanity.open_present"].status == "SKIP"
      and res["sanity.open_present"].count == 3)
res = {c.name: c for c in V.check_sanity("X", {"symbol": "Symbol"}, {"Symbol"},
                                          [{"Symbol": "A"}])}
check("no Open column -> checks absent, v1.2.0 set only",
      "sanity.open_present" not in res and "sanity.open_in_day_range" not in res)
clean = [row("A", 10, 9, 11), row("B", 9.5, 9, 11)]
res = {c.name: c for c in V.check_sanity("GM", K2H, ACT, clean)}
check("clean page -> both PASS",
      res["sanity.open_present"].status == "PASS"
      and res["sanity.open_in_day_range"].status == "PASS")

print("== H9  sampled plumbing via REAL _read_page ==")
class FakeReg:
    # three tokens: _detect_header_row needs overlap >= 3 (v1.x behavior)
    def headers(self, p): return ["Symbol", "Price", "Open"]
    def keys(self, p): return ["symbol", "current_price", "open_price"]
def make_grid(nrows):
    return [["Symbol", "Price", "Open"]] + [[f"S{i}", "1", "1"] for i in range(nrows)]
def rr_factory(nrows):
    def rr(sid, rng): return make_grid(nrows)
    return rr
async def read(nrows, cap):
    return await V._read_page("P", "SID", FakeReg(), rr_factory(nrows),
                              None, cap)
pd = asyncio.run(read(40, 10))
check("read_range: 40 avail cap 10 -> sampled, 10 rows",
      pd.sampled is True and len(pd.rows) == 10)
pd = asyncio.run(read(8, 10))
check("read_range: 8 avail cap 10 -> FULL", pd.sampled is False
      and len(pd.rows) == 8)
pd = asyncio.run(read(10, 10))
check("exactly-cap -> FULL (no phantom sample)", pd.sampled is False)
def rows_reader(nrows):
    def fn(sheet=None, limit=None, **kw):
        return {"rows": [{"symbol": f"S{i}"} for i in range(min(nrows, limit or nrows))]}
    return (fn, False, "fake")
async def read2(nrows, cap):
    return await V._read_page("P", "SID", FakeReg(), None,
                              rows_reader(nrows), cap)
pd = asyncio.run(read2(40, 10))
check("get_sheet_rows: probe=cap+1 -> sampled, trimmed to cap",
      pd.sampled is True and len(pd.rows) == 10)
pd = asyncio.run(read2(7, 10))
check("get_sheet_rows: under cap -> FULL", pd.sampled is False
      and len(pd.rows) == 7)

print("== H10  scope.coverage + parser + exit semantics ==")
res = [V.CheckResult("GM", "scope.coverage", "WARN")]
check("SAMPLE_ONLY WARN exits 1 (prod-safe)", V._exit_code(res) == 1)
pr = V.create_parser().parse_args(["--sheet-id", "X"])
check("--full default off", pr.full is False)
pr = V.create_parser().parse_args(["--sheet-id", "X", "--full"])
check("--full flag parses", pr.full is True)
os.environ["VALIDATE_FULL"] = "1"
pr = V.create_parser().parse_args(["--sheet-id", "X"])
check("VALIDATE_FULL env honored", pr.full is True)
os.environ.pop("VALIDATE_FULL")

print(f"\nRESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
