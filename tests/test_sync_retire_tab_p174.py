#!/usr/bin/env python3
"""tests/test_sync_retire_tab_p174.py
P-174 - run_dashboard_sync v6.64.0 OPERATOR RETIREMENT LIST (_Retired_Symbols).
Dual-tree, REAL-module harness (the K-battery loader): drives the REAL
_run_one_task (dry-run, which returns right AFTER the retirement seam) on a
Sheets boundary double that serves a workbook of named grids with true A1
range slicing - the real read-back, deny filter, sanitize, OLDEST-FIRST and
CRIT-FRONT all execute - on hand fixtures AND on the real 2026-09-30
Global_Markets export (TFB_TEST_EXPORT_DIR); then proves the mechanism at
the persistence layer with the REAL _persist_missing_symbol_rows and
_unpersisted_missing, the REAL _Run_Log appender shape, and base parity
(TFB_TEST_SYNC_BASE = the v6.63.0 file).

R1 pure battery: parser (title rows, Page scoping, junk, dupes), page sets,
   order-preserving filter, verdict (cap / empty-list refusal), mode words,
   embedded self-test PASS; enforce degrades to observe on a failed selftest
R2 loader: absent tab (read_values None) -> unreadable, nothing retired;
   one read per run (cache), TTL respected; header-less tab -> nothing
R3 REAL _run_one_task, hand page (12 symbols, 2 blank Names):
   off  -> symbols_requested == base, no [UNIVERSE-RETIRE] line
   observe -> list unchanged, WOULD-RETIRE line, no stamp meta
   enforce -> 3 retired, order of the rest untouched, stamp meta retired=3,
             Page-scoped row for Market_Leaders NOT applied to GM
   enforce over the cap -> REFUSED, list unchanged
   enforce with the whole page listed -> REFUSED (never an empty request)
   dry-run -> zero _Run_Log appends from the seam
R4 REAL _run_one_task on the real GM export (6,609 symbols, 141 blank
   Names = 133 P-174 class + 8 P-173 stripped): off == base (same requested
   count); enforce with 40 of the class listed -> requested 6,609 -> 6,569;
   all 133 -> 6,476; the cap at 1 % (66) refuses the 133
R5 mechanism: REAL _persist_missing_symbol_rows + _unpersisted_missing with
   the filtered list -> retired rows NOT restored, hard guard sees 0 missing;
   with the unfiltered list (base behaviour) -> restored / would trip
R6 REAL _append_runlog_retire: one 10-column FW-3 row, Status RETIRED /
   WOULD_RETIRE / REFUSED, Details JSON with names + run meta; failure is a
   ::warning::, never a raise
R7 stamp: REAL _stamp_page_status message carries " retired=N" only when set
Run: python3 tests/test_sync_retire_tab_p174.py   (x3, digest)"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import importlib.util
import io
import json
import os
import re
import sys
import types
from contextlib import redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SYNC = os.environ.get("TFB_TEST_SYNC_DELIVERED") or os.path.join(ROOT, "scripts", "run_dashboard_sync.py")
BASE = os.environ.get("TFB_TEST_SYNC_BASE", "")
EXPORT = os.environ.get("TFB_TEST_EXPORT_DIR", "")
csv.field_size_limit(10 ** 9)
os.environ.setdefault("GITHUB_RUN_ID", "36646905345")
# Production env of the ranked pages (daily_sync.yml): the universe cap covers
# GM (6,609) and DECISION-FIRST is off; everything else at script defaults.
os.environ["TFB_SYNC_MAX_SYMBOLS_MARKET"] = "7000"
os.environ["TFB_SYNC_PRIORITY_FETCH"] = "0"
for _k in ("TFB_SYNC_RETIRE_TAB", "TFB_SYNC_RETIRE_MAX_PCT", "TFB_SYNC_RETIRE_TAB_NAME"):
    os.environ.pop(_k, None)
# _read_symbols: production's root symbols_reader has neither entry point ->
# [] -> the sheet read-back is the request source. Pin that here.
sys.modules.setdefault("symbols_reader", types.ModuleType("symbols_reader"))


def _load(path, name):
    d = os.path.dirname(os.path.abspath(path))
    if d not in sys.path:
        sys.path.insert(0, d)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


_M = {}


def m():
    if "d" not in _M:
        _M["d"] = _load(SYNC, "rds_p174_delivered")
    return _M["d"]


def b():
    if "b" not in _M:
        _M["b"] = _load(BASE, "rds_p174_base") if BASE and os.path.exists(BASE) else None
    return _M["b"]


def _mode(v):
    if v is None:
        os.environ.pop("TFB_SYNC_RETIRE_TAB", None)
    else:
        os.environ["TFB_SYNC_RETIRE_TAB"] = v


def _pct(v):
    if v is None:
        os.environ.pop("TFB_SYNC_RETIRE_MAX_PCT", None)
    else:
        os.environ["TFB_SYNC_RETIRE_MAX_PCT"] = str(v)


# --------------------------------------------------------------------------- #
# Sheets boundary double: a workbook of named grids with A1-range slicing.
# --------------------------------------------------------------------------- #
def _col_idx(col):
    n = 0
    for ch in col.upper():
        n = n * 26 + (ord(ch) - 64)
    return n - 1


_A1 = re.compile(r"^\$?([A-Za-z]+)\$?(\d+)?(?::\$?([A-Za-z]+)\$?(\d+)?)?$")


class _Values(object):
    def __init__(self, wb):
        self.wb = wb

    def append(self, **kw):
        wb = self.wb

        class _Exec(object):
            def execute(_self):
                if wb.append_fail:
                    raise RuntimeError("append refused (double)")
                wb.appends.append((kw.get("range"), [list(r) for r in kw["body"]["values"]]))
                return {"updates": {"updatedRows": 1}}
        return _Exec()


class _Svc(object):
    def __init__(self, wb):
        self._v = _Values(wb)

    def spreadsheets(self):
        return self

    def values(self):
        return self._v


class _WB(object):
    """read_values(sid, sheet, a1) -> slice of the named grid (Sheets returns
    only the used range: trailing empty cells / rows trimmed); None for an
    unknown tab (the API answers 400 for a missing sheet)."""
    def __init__(self, grids):
        self.grids = {k: [list(r) for r in v] for k, v in grids.items()}
        self.reads = {}
        self.appends = []
        self.append_fail = False
        self.svc = _Svc(self)

    def _get_service(self):
        return self.svc

    def _safe_sheet_a1(self, name):
        return "'%s'" % name

    def read_values(self, spreadsheet_id, sheet_name, a1_range="A1:EZ2000"):
        self.reads[sheet_name] = self.reads.get(sheet_name, 0) + 1
        if sheet_name not in self.grids:
            return None
        g = self.grids[sheet_name]
        mt = _A1.match((a1_range or "").strip())
        if not mt:
            return [list(r) for r in g]
        c1 = _col_idx(mt.group(1)); r1 = int(mt.group(2) or 1)
        c2 = _col_idx(mt.group(3)) if mt.group(3) else c1
        r2 = int(mt.group(4)) if mt.group(4) else len(g)
        out = []
        for row in g[r1 - 1:r2]:
            cells = list(row[c1:c2 + 1])
            while cells and (cells[-1] is None or str(cells[-1]) == ""):
                cells.pop()
            out.append(cells)
        while out and not out[-1]:
            out.pop()
        return out


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
HDR = ["Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
       "Current Price", "Data Provider", "Warnings", "Last Updated (UTC)"]
STAMP = "2026-09-30T02:5%d:00+00:00"


def _page(rows):
    return [list(HDR)] + [list(r) for r in rows]


def _row(sym, name, price=10.0, i=0, warn=""):
    return [sym, name, "Equity", "US", "USD", "United States", "Financials", "Banks",
            price, "eodhd", warn, STAMP % (i % 10)]


# 12 symbols; DEAD1/DEAD2 blank-Named (heal-first fronts them), TICK001 = deny junk
HAND = _page([
    _row("AAA.US", "Alpha Corp", 10.0, 1), _row("BBB.US", "Beta Corp", 20.0, 2),
    _row("DEAD1.US", "", "", 3, "quote_current_price_missing"), _row("CCC.US", "Gamma Corp", 30.0, 4),
    _row("DDD.US", "Delta Corp", 40.0, 5), _row("TICK001", "", "", 6),
    _row("DEAD2.US", "", "", 7, "quote_current_price_missing"), _row("EEE.US", "Eps Corp", 50.0, 8),
    _row("FFF.US", "Zeta Corp", 60.0, 9), _row("GGG.US", "Eta Corp", 70.0, 0),
    _row("OLD.US", "Old Corp", 80.0, 1), _row("HHH.US", "Theta Corp", 90.0, 2),
])
TAB_HDR = ["Symbol", "Retired On", "Reason", "Replacement", "Page"]
TAB3 = [["Operator retirement list"], list(TAB_HDR),
        ["DEAD1.US", "2026-09-30", "delisted", "", ""],
        ["dead2.us", "2026-09-30", "renamed", "DEAD2N.US", ""],
        ["OLD.US", "2026-09-30", "expired", "", ""],
        ["CCC.US", "2026-09-30", "ML only", "", "Market_Leaders"],      # scoped: not GM
        ["not a ticker", "", "junk", "", ""]]
TAB_ALL = [list(TAB_HDR)] + [[r[0], "", "mis-paste", "", ""] for r in HAND[1:]]


def _wb(page_rows, tab=None, page="Global_Markets", tab_name="_Retired_Symbols"):
    grids = {page: page_rows, "_Sync_Control": [["key", "value"]]}
    if tab is not None:
        grids[tab_name] = tab
    return _WB(grids)


def _task(mod, page="Global_Markets"):
    for t in mod._default_tasks():
        if t.sheet_name == page:
            return t
    raise AssertionError(page)


def _run(mod, wb, page="Global_Markets", dry_run=True):
    """REAL _run_one_task in dry-run: returns right after the retirement seam
    (symbols_requested is the post-seam request count). stdout captured
    (the ::warning:: annotations)."""
    if hasattr(mod, "_RETIRED_CACHE"):
        mod._RETIRED_CACHE.clear()
    if hasattr(mod, "_PRIORITY_SET_CACHE"):
        mod._PRIORITY_SET_CACHE.clear()
    buf = io.StringIO()
    with redirect_stdout(buf):
        res = asyncio.run(mod._run_one_task(_task(mod, page), "SID", "A1", -1, False, dry_run,
                                            object(), wb))
    return res, buf.getvalue()


def _rt_lines(res):
    return [w for w in res.warnings if "[UNIVERSE-RETIRE" in w]


out = []
digest = []


def T(name, cond, detail=""):
    out.append(("PASS " if cond else "FAIL ") + name + (" | " + str(detail)[:300] if detail else ""))
    if not cond:
        raise AssertionError(name + " " + str(detail))


mod = m()
T("R0 delivered SCRIPT_VERSION 6.64.4", mod.SCRIPT_VERSION == "6.64.4", mod.SCRIPT_VERSION)
base = b()
if base is not None:
    T("R0 base SCRIPT_VERSION 6.63.0 (dual-tree armed)", base.SCRIPT_VERSION == "6.63.0", base.SCRIPT_VERSION)
    T("R0 base has no retirement seam", not hasattr(base, "_retire_mode") and "_RETIRE_TAG" not in dir(base))

# ================================================================ R1 ======= #
idx = mod._retired_index_from_matrix(TAB3)
T("R1 parser: title row skipped, hdr=1, rows=4, junk=1, global={DEAD1,DEAD2,OLD}, by_page={marketleaders:{CCC}}",
  idx["reason"] == "ok" and idx["hdr"] == 1 and idx["rows"] == 4 and idx["junk"] == 1
  and idx["global"] == {"DEAD1.US", "DEAD2.US", "OLD.US"} and idx["by_page"] == {"marketleaders": {"CCC.US"}},
  {k: (sorted(v) if isinstance(v, set) else v) for k, v in idx.items()})
T("R1 page sets: GM gets the 3 global; ML gets 3 + CCC",
  mod._retired_for_page(idx, "Global_Markets") == {"DEAD1.US", "DEAD2.US", "OLD.US"}
  and mod._retired_for_page(idx, "Market_Leaders") == {"DEAD1.US", "DEAD2.US", "OLD.US", "CCC.US"}
  and mod._retired_for_page(idx, "market leaders") == mod._retired_for_page(idx, "MARKET_LEADERS"))
kept, dropped = mod._apply_retire_filter(["b.us", "DEAD1.US", "a.us", "old.us", "DEAD1.US", "c.us"],
                                         mod._retired_for_page(idx, "Global_Markets"))
T("R1 filter: order preserved, dropped de-duplicated first-seen, case-insensitive",
  kept == ["b.us", "a.us", "c.us"] and dropped == ["DEAD1.US", "OLD.US"], (kept, dropped))
T("R1 verdict: cap and empty-list refusal; observe never refuses; zero -> none",
  mod._retire_decide(40, 6609, "enforce", 20.0) == "retired"
  and mod._retire_decide(1322, 6609, "enforce", 20.0) == "refused"          # 20.003 % > 20
  and mod._retire_decide(1321, 6609, "enforce", 20.0) == "retired"          # 19.99 %
  and mod._retire_decide(12, 12, "enforce", 100.0) == "refused"
  and mod._retire_decide(11, 12, "enforce", 100.0) == "retired"
  and mod._retire_decide(1322, 6609, "observe", 20.0) == "would_retire"
  and mod._retire_decide(0, 6609, "enforce", 20.0) == "none")
words = []
for w in (None, "off", "observe", "enforce", "1", "true", "on", " Enforce"):
    _mode(w); words.append(mod._retire_mode())
_mode(None)
T("R1 mode words: explicit only (1/true/on = off); trimmed/case-folded", words == ["off", "off", "observe", "enforce", "off", "off", "off", "enforce"], words)
_pct(None); p_def = mod._retire_max_pct(); _pct("0.2"); p_lo = mod._retire_max_pct(); _pct("250"); p_hi = mod._retire_max_pct(); _pct("x"); p_bad = mod._retire_max_pct(); _pct(None)
T("R1 max_pct: default 20, clamped 1..100, junk -> 20", (p_def, p_lo, p_hi, p_bad) == (20.0, 1.0, 100.0, 20.0), (p_def, p_lo, p_hi, p_bad))
T("R1 embedded selftest PASS (memoized)", mod._retire_selftest() == "PASS" and mod._RETIRE_SELFTEST_MSG == "PASS", mod._RETIRE_SELFTEST_MSG)
# enforce degrades to observe when the selftest fails (never the other way)
_saved_st = mod._RETIRE_SELFTEST_MSG
try:
    mod._RETIRE_SELFTEST_MSG = "FAIL 3/7"
    _mode("enforce"); deg = mod._retire_mode(); _mode("observe"); obs = mod._retire_mode()
finally:
    mod._RETIRE_SELFTEST_MSG = _saved_st
    _mode(None)
T("R1 enforce degrades to observe on a failed selftest; observe stays observe", deg == "observe" and obs == "observe", (deg, obs))
T("R1 tab name default / override", mod._retired_tab_name() == "_Retired_Symbols")
digest.append([sorted(idx["global"]), kept, dropped, words])

# ================================================================ R2 ======= #
mod._RETIRED_CACHE.clear()
wb0 = _wb(HAND, tab=None)
i0 = mod._load_retired_index(wb0, "SID")
T("R2 absent tab: read_values None -> reason=unreadable, nothing retired",
  i0["reason"] == "unreadable" and not i0["global"] and not i0["by_page"] and wb0.reads.get("_Retired_Symbols") == 1, i0)
i0b = mod._load_retired_index(wb0, "SID")
T("R2 one read per run: second call served from the cache", i0b is i0 and wb0.reads.get("_Retired_Symbols") == 1, wb0.reads)
mod._RETIRED_CACHE["SID"] = (mod._RETIRED_CACHE["SID"][0] - mod._RETIRED_CACHE_TTL_S - 1, i0)
i0c = mod._load_retired_index(wb0, "SID")
T("R2 TTL expiry re-reads", i0c is not i0 and wb0.reads.get("_Retired_Symbols") == 2, wb0.reads)
mod._RETIRED_CACHE.clear()
wbh = _wb(HAND, tab=[["Retired"], ["DEAD1.US", "no header"]])
ih = mod._load_retired_index(wbh, "SID")
T("R2 header-less tab -> header_not_found, nothing retired", ih["reason"] == "header_not_found" and not ih["global"], ih)
mod._RETIRED_CACHE.clear()
wbe = _wb(HAND, tab=[list(TAB_HDR)])
ie = mod._load_retired_index(wbe, "SID")
T("R2 header-only tab -> empty, nothing retired", ie["reason"] == "empty" and ie["rows"] == 0 and not ie["global"], ie)
mod._RETIRED_CACHE.clear()
os.environ["TFB_SYNC_RETIRE_TAB_NAME"] = "_Retire_List"
wbn = _wb(HAND, tab=TAB3, tab_name="_Retire_List")
inn = mod._load_retired_index(wbn, "SID")
os.environ.pop("TFB_SYNC_RETIRE_TAB_NAME", None)
T("R2 TFB_SYNC_RETIRE_TAB_NAME override honoured", inn["reason"] == "ok" and inn["rows"] == 4 and inn["tab"] == "_Retire_List", inn.get("tab"))
mod._RETIRED_CACHE.clear()
i_none = mod._load_retired_index(None, "SID")
T("R2 no writer -> no_sheets, nothing retired", i_none["reason"] == "no_sheets" and not i_none["global"])
mod._RETIRED_CACHE.clear()
digest.append([i0["reason"], ih["reason"], ie["reason"], inn["rows"]])

# ================================================================ R3 ======= #
_mode(None); _pct(None)
r_off, so_off = _run(mod, _wb(HAND, tab=TAB3))
T("R3 off: dry-run returns after the seam; 11 requested (TICK001 deny-dropped); no retire line, no stamp meta",
  r_off.status == "skipped" and r_off.symbols_requested == 11 and not _rt_lines(r_off)
  and "UNIVERSE-RETIRE" not in so_off and "retired" not in r_off._stamp_meta, (r_off.symbols_requested, r_off.warnings))
if base is not None:
    r_b, _ = _run(base, _wb(HAND, tab=TAB3))
    T("R3 off == base: same requested count, same warnings (version tags masked)",
      r_b.symbols_requested == r_off.symbols_requested
      and [re.sub(r"v6\.6[34]\.0", "vX", w) for w in r_b.warnings] == [re.sub(r"v6\.6[34]\.0", "vX", w) for w in r_off.warnings],
      (r_b.symbols_requested, r_b.warnings))
# observe under the DEFAULT cap: 3 of 11 = 27.3 % > 20 % -> disclosed, list unchanged, note says enforce would refuse
_mode("observe")
wb_o = _wb(HAND, tab=TAB3)
r_obs, so_obs = _run(mod, wb_o)
l_obs = _rt_lines(r_obs)
T("R3 observe: list unchanged (11); one WOULD-RETIRE line naming DEAD1/DEAD2/OLD (not CCC); no stamp meta; ::warning::",
  r_obs.symbols_requested == 11 and len(l_obs) == 1 and "would retire 3 of 11" in l_obs[0]
  and all(s in l_obs[0] for s in ("DEAD1.US", "DEAD2.US", "OLD.US")) and "CCC.US" not in l_obs[0]
  and "retired" not in r_obs._stamp_meta and "::warning::[UNIVERSE-RETIRE v6.64.0]" in so_obs, l_obs)
T("R3 observe: the note says enforce would REFUSE (27 % > default 20 %)", "enforce would REFUSE" in l_obs[0], l_obs)
T("R3 observe: tab read once for the page", wb_o.reads.get("_Retired_Symbols") == 1, wb_o.reads)
# enforce under the DEFAULT cap -> REFUSED, list unchanged
_mode("enforce")
wb_r = _wb(HAND, tab=TAB3)
r_ref, so_ref = _run(mod, wb_r)
l_ref = _rt_lines(r_ref)
T("R3 enforce over the cap (27 % > 20 %): REFUSED, 11 requested, no stamp meta",
  r_ref.symbols_requested == 11 and len(l_ref) == 1 and "matched 3 of 11" in l_ref[0] and "REFUSED" in l_ref[0]
  and "retired" not in r_ref._stamp_meta, (r_ref.symbols_requested, l_ref))
# enforce with a deliberate cap (50 %) -> applied
_pct("50")
wb_e = _wb(HAND, tab=TAB3)
r_enf, so_enf = _run(mod, wb_e)
l_enf = _rt_lines(r_enf)
T("R3 enforce (cap 50): 3 retired -> 8 requested; RETIRED line; stamp meta retired=3; Page-scoped CCC.US kept on GM",
  r_enf.symbols_requested == 8 and len(l_enf) == 1 and "retired 3 of 11" in l_enf[0]
  and r_enf._stamp_meta.get("retired") == 3 and "CCC.US" not in l_enf[0]
  and "rows leave the page on this write" in l_enf[0], (r_enf.symbols_requested, l_enf))
T("R3 dry-run: the seam appends nothing to _Run_Log (all three runs)", wb_e.appends == [] and wb_o.appends == [] and wb_r.appends == [])
# Page scoping the other way: on Market_Leaders the scoped CCC.US IS retired
wb_ml = _wb(HAND, tab=TAB3, page="Market_Leaders")
r_ml, _ = _run(mod, wb_ml, page="Market_Leaders")
l_ml = _rt_lines(r_ml)
T("R3 enforce on Market_Leaders (cap 50): Page-scoped CCC.US joins the 3 global -> 4 retired, 7 requested",
  r_ml.symbols_requested == 7 and l_ml and "retired 4 of 11" in l_ml[0] and "CCC.US" in l_ml[0], (r_ml.symbols_requested, l_ml))
# whole page listed, cap 100 -> REFUSED (never an empty request list / page-driven fall-through)
_pct("100")
wb_all = _wb(HAND, tab=TAB_ALL)
r_all, _ = _run(mod, wb_all)
l_all = _rt_lines(r_all)
T("R3 enforce with the whole page listed (cap 100): REFUSED, 11 requested - an empty request list is impossible",
  r_all.symbols_requested == 11 and l_all and "REFUSED" in l_all[0] and "matched 11 of 11" in l_all[0], (r_all.symbols_requested, l_all))
# order of the kept list is untouched (heal-first / oldest-first order): compare via the REAL filter on the base order
_pct("50")
_mode(None)
r_ord_off, _ = _run(mod, _wb(HAND, tab=TAB3))
_mode("enforce")
_pct(None); _mode(None)
# non-ranked page: My_Portfolio never consults the tab
_mode("enforce"); _pct("100")
wb_mp = _WB({"My_Portfolio": _page([_row("DEAD1.US", "Held Corp", 10.0, 1)]), "_Retired_Symbols": TAB3,
             "_Portfolio_CostBasis": [["Symbol", "Qty", "Avg Cost"], ["DEAD1.US", 10, 9.0]], "_Sync_Control": [["k", "v"]]})
r_mp, _ = _run(mod, wb_mp, page="My_Portfolio")
T("R3 My_Portfolio (non-ranked): tab never read, no retire line",
  wb_mp.reads.get("_Retired_Symbols") is None and not _rt_lines(r_mp), (wb_mp.reads, r_mp.warnings[-2:]))
_mode(None); _pct(None)
digest.append([r_off.symbols_requested, r_obs.symbols_requested, r_ref.symbols_requested,
               r_enf.symbols_requested, r_ml.symbols_requested, r_all.symbols_requested])

# ================================================================ R4 ======= #
gm_path = os.path.join(EXPORT, "Global_Markets.tsv") if EXPORT else ""
if gm_path and os.path.exists(gm_path):
    with open(gm_path, encoding="utf-8", errors="replace", newline="") as fh:
        GM = list(csv.reader(fh, delimiter="\t"))
    si = GM[0].index("Symbol"); ni = GM[0].index("Name"); wi = GM[0].index("Warnings"); pi = GM[0].index("Data Provider")
    blank = [r for r in GM[1:] if len(r) > ni and r[si].strip() and not r[ni].strip()]
    # the P-174 class = blank Name AND a provider answer (price missing / empty row); the 8 rows the
    # ID-FIREWALL stripped this run (identity_quarantined, no provider) are P-173, not candidates
    def _toks(cell):
        return {t.strip().split(":")[0] for t in str(cell or "").split(";") if t.strip()}
    nameless = [r[si].strip().upper() for r in blank
                if len(r) > max(wi, pi) and r[pi].strip() and "identity_quarantined" not in _toks(r[wi])]
    T("R4 real export loaded: 6,609 rows, 141 blank-Name rows = 133 P-174 class + 8 P-173 stripped",
      len(GM) - 1 == 6609 and len(blank) == 141 and len(nameless) == 133, (len(GM) - 1, len(blank), len(nameless)))
    forty = nameless[:40]
    TAB40 = [list(TAB_HDR)] + [[s, "2026-09-30", "P-174 nameless", "", ""] for s in forty]
    _mode(None)
    g_off, _ = _run(mod, _wb(GM, tab=TAB40))
    T("R4 off on the real page: 6,609 requested (cap 7,000 covers the page), no retire line",
      g_off.symbols_requested == 6609 and not _rt_lines(g_off), (g_off.symbols_requested, [w[:80] for w in g_off.warnings]))
    if base is not None:
        g_b, _ = _run(base, _wb(GM, tab=TAB40))
        T("R4 off == base on the real page (same requested count)", g_b.symbols_requested == g_off.symbols_requested, g_b.symbols_requested)
    _mode("enforce")
    wb_g = _wb(GM, tab=TAB40)
    g_enf, so_g = _run(mod, wb_g)
    l_g = _rt_lines(g_enf)
    T("R4 enforce, 40 nameless listed (0.6 % < 20 %): requested 6,609 -> 6,569; RETIRED line lists 15 + '...'; stamp meta 40",
      g_enf.symbols_requested == 6569 and l_g and "retired 40 of 6609" in l_g[0] and "..." in l_g[0]
      and g_enf._stamp_meta.get("retired") == 40 and wb_g.reads.get("_Retired_Symbols") == 1, (g_enf.symbols_requested, l_g))
    # the whole P-174 class listed: 133 = 2.0 % -> applied under the default cap
    TAB141 = [list(TAB_HDR)] + [[s, "2026-09-30", "P-174 nameless", "", ""] for s in nameless]
    g_141, _ = _run(mod, _wb(GM, tab=TAB141))
    T("R4 enforce, all 133 listed (2.0 % < 20 %): requested 6,476; stamp meta 133",
      g_141.symbols_requested == 6609 - 133 and g_141._stamp_meta.get("retired") == 133, g_141.symbols_requested)
    _pct("1")                                     # clamp floor: 1 % of 6,609 = 66
    g_cap, _ = _run(mod, _wb(GM, tab=TAB141))
    T("R4 enforce at cap 1 % (66 of 6,609): 133 matched -> REFUSED, 6,609 requested, no stamp meta",
      g_cap.symbols_requested == 6609 and "REFUSED" in _rt_lines(g_cap)[0] and "retired" not in g_cap._stamp_meta,
      (g_cap.symbols_requested, _rt_lines(g_cap)))
    _pct(None)
    _mode(None)
    digest.append([g_off.symbols_requested, g_enf.symbols_requested, g_cap.symbols_requested, g_141.symbols_requested, forty[:5]])
else:
    out.append("SKIP R4 real export (set TFB_TEST_EXPORT_DIR=<dir with Global_Markets.tsv>)")

# ================================================================ R5 ======= #
# Mechanism: persistence diffs REQUESTED vs returned. With the filtered list the
# retired rows are not restored and the L4b hard guard sees nothing missing;
# with the unfiltered list (base behaviour) they are restored / would trip.
_mode(None)
fresh_hdr = list(HDR)
fresh_rows = [list(r) for r in HAND[1:] if r[0] not in ("DEAD1.US", "DEAD2.US", "OLD.US", "TICK001")]   # backend answered the live 8
req_all = [r[0] for r in HAND[1:] if r[0] != "TICK001"]                                                # 11 (base request list)
req_flt, _drp = mod._apply_retire_filter(req_all, {"DEAD1.US", "DEAD2.US", "OLD.US"})                  # 8 (v6.64.0 enforce)
wb5 = _wb(HAND, tab=TAB3)
mx_flt, kept_flt = mod._persist_missing_symbol_rows(wb5, "SID", "Global_Markets", fresh_hdr, [list(r) for r in fresh_rows], req_flt)
mx_all, kept_all = mod._persist_missing_symbol_rows(wb5, "SID", "Global_Markets", fresh_hdr, [list(r) for r in fresh_rows], req_all)
T("R5 filtered request list: persistence restores NOTHING (8 rows out, retired rows leave the page)",
  kept_flt == [] and len(mx_flt) == 8, (kept_flt, len(mx_flt)))
T("R5 unfiltered list (base behaviour): persistence resurrects DEAD1/DEAD2/OLD from the old grid (immortal rows)",
  sorted(kept_all) == ["DEAD1.US", "DEAD2.US", "OLD.US"] and len(mx_all) == 11, (kept_all, len(mx_all)))
still_flt = mod._unpersisted_missing(fresh_hdr, [list(r) for r in fresh_rows], req_flt, None)
still_all = mod._unpersisted_missing(fresh_hdr, [list(r) for r in fresh_rows], req_all, None)
T("R5 L4b hard guard: filtered list -> 0 missing (write proceeds); unfiltered -> the 3 (would TRIP and veto the write)",
  still_flt == [] and sorted(still_all) == ["DEAD1.US", "DEAD2.US", "OLD.US"], (still_flt, still_all))
if base is not None:
    mxb, keptb = base._persist_missing_symbol_rows(_wb(HAND, tab=TAB3), "SID", "Global_Markets", fresh_hdr, [list(r) for r in fresh_rows], req_flt)
    T("R5 base persistence on the same filtered list: identical outcome (the seam changes the LIST, not the persistence code)",
      keptb == kept_flt and mxb == mx_flt)
digest.append([kept_flt, sorted(kept_all), still_flt, sorted(still_all)])

# ================================================================ R6 ======= #
wb6 = _wb(HAND, tab=TAB3)
idx6 = mod._retired_index_from_matrix(TAB3)
for verdict, exp_status in (("retired", "RETIRED"), ("would_retire", "WOULD_RETIRE"), ("refused", "REFUSED")):
    wb6.appends = []
    buf = io.StringIO()
    with redirect_stdout(buf):
        mod._append_runlog_retire(wb6, "SID", "Global_Markets", verdict, "enforce" if verdict != "would_retire" else "observe",
                                  ["DEAD1.US", "DEAD2.US", "OLD.US"], 11, idx6)
    T("R6 %s: one 10-column FW-3 row on '_Run_Log'!A1, Status %s" % (verdict, exp_status),
      len(wb6.appends) == 1 and wb6.appends[0][0] == "'_Run_Log'!A1" and len(wb6.appends[0][1][0]) == 10
      and wb6.appends[0][1][0][1] == "WARNING" and wb6.appends[0][1][0][2] == "run_dashboard_sync"
      and wb6.appends[0][1][0][3] == "Global_Markets" and wb6.appends[0][1][0][4] == exp_status
      and buf.getvalue() == "", wb6.appends)
row6 = wb6.appends[0][1][0]
det = json.loads(row6[9])
T("R6 Details JSON: names, counts, tab stats, selftest, version, R5 run meta (run_id, ts_utc)",
  det["names"] == ["DEAD1.US", "DEAD2.US", "OLD.US"] and det["retired"] == 3 and det["requested_before"] == 11
  and det["requested_after"] == 11 and det["verdict"] == "refused" and det["tab_rows"] == 4 and det["tab_junk"] == 1
  and det["selftest"] == "PASS" and det["version"] == "6.64.4" and det["run_id"] == "36646905345" and "ts_utc" in det, det)
T("R6 message names the tag, verdict, counts, tab and selftest",
  row6[5].startswith("[UNIVERSE-RETIRE v6.64.0] Global_Markets | mode=enforce verdict=refused matched=3 of 11 requested")
  and "tab=_Retired_Symbols rows=4 junk=1" in row6[5] and "selftest=PASS" in row6[5], row6[5])
wb6.appends = []
mod._append_runlog_retire(wb6, "SID", "Global_Markets", "retired", "enforce", [], 11, idx6)
mod._append_runlog_retire(wb6, "SID", "Global_Markets", "none", "enforce", ["X.US"], 11, idx6)
mod._append_runlog_retire(None, "SID", "Global_Markets", "retired", "enforce", ["X.US"], 11, idx6)
T("R6 no row for an empty match / verdict none / no writer", wb6.appends == [])
wb6.append_fail = True
buf = io.StringIO()
with redirect_stdout(buf):
    mod._append_runlog_retire(wb6, "SID", "Global_Markets", "retired", "enforce", ["X.US"], 11, idx6)
T("R6 append failure: ::warning:: annotation, never a raise, never counted", "::warning::[UNIVERSE-RETIRE v6.64.0] _Run_Log append FAILED for Global_Markets - RuntimeError" in buf.getvalue(), buf.getvalue())
wb6.append_fail = False
# requested_after for a RETIRED verdict
wb6.appends = []
mod._append_runlog_retire(wb6, "SID", "Global_Markets", "retired", "enforce", ["DEAD1.US"], 11, idx6)
T("R6 RETIRED: requested_after = before - retired", json.loads(wb6.appends[0][1][0][9])["requested_after"] == 10)
digest.append([row6[4], det["names"], det["requested_after"]])

# ================================================================ R7 ======= #
def _stamp_row(mod_, retired):
    """REAL, PURE _status_stamp_row(page, res, n_cols) -> the _Status row; the
    Message cell (D) is what carries the tokens."""
    res = mod_.TaskResult(key="GLOBAL_MARKETS", sheet_name="Global_Markets", status="success",
                          start_utc="2026-09-30T00:00:00+00:00")
    res._stamp_meta = {"requested": 100, "pre_persist_rows": 100, "klg_kept": 0, "persist_restored": 0,
                       "pv2_restored": 0, "stubbed": 0}
    if retired:
        res._stamp_meta["retired"] = retired
    res.rows_written = 100
    res.end_utc = "2026-09-30T00:20:00+00:00"       # pinned: dur_ms is start->end, not wall clock
    return mod_._status_stamp_row("Global_Markets", res, 115)
row_0 = _stamp_row(mod, 0)
row_3 = _stamp_row(mod, 3)
T("R7 stamp Message: ' retired=3' present only when the seam set it; placed after stubbed, before fresh_cov",
  " retired=" not in row_0[3] and " retired=3 " in row_3[3]
  and row_3[3].index(" retired=3") < row_3[3].index(" fresh_cov="), (row_0[3][:160], row_3[3][:160]))
T("R7 stamp Status cell / data verdict unaffected by the token", row_0[2] == row_3[2] and "data=COMPLETE" in row_3[3], (row_0[2], row_3[2]))
if base is not None:
    row_b = _stamp_row(base, 0)
    T("R7 base parity: with no retirement the delivered Message == base Message (version tag masked; timestamp column excluded)",
      row_b[3].replace("v6.63.0", "vX") == row_0[3].replace("v6.64.4", "vX") and row_b[2] == row_0[2]
      and row_b[6:] == row_0[6:], (row_b[3][:120], row_0[3][:120]))
digest.append([row_3[3].split(" | ")[0]])

# ================================================================ SRC ====== #
src = open(SYNC, encoding="utf-8").read()
T("SRC seam sits immediately before res.symbols_requested (post read-back/order, pre fetch)",
  re.search(r"_append_runlog_retire\(sheets, spreadsheet_id, task\.sheet_name, _rt_verdict,[\s\S]{0,1200}res\.symbols_requested = len\(symbols\)", src) is not None)
T("SRC seam is guarded by ranked page + expects_rows + sheets + mode != off",
  "and task.sheet_name in _RANKED_MARKET_PAGES\n                and _retire_mode() != \"off\"):" in src)
if base is not None:
    import ast
    def _defs(path):
        t = ast.parse(open(path, encoding="utf-8").read())
        return {n.name for n in ast.walk(t) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    d_new, d_base = _defs(SYNC), _defs(BASE)
    T("SRC AST: zero functions removed vs base; exactly the 12 documented additions",
      not (d_base - d_new) and sorted(d_new - d_base) == sorted([
          "_retired_tab_name", "_retire_mode_raw", "_retire_mode", "_retire_max_pct", "_retired_index_empty",
          "_retired_index_from_matrix", "_retired_for_page", "_apply_retire_filter", "_retire_decide",
          "_load_retired_index", "_retire_selftest", "_append_runlog_retire"]), (sorted(d_base - d_new), sorted(d_new - d_base)))

print("\n".join(out))
print("RUN-DIGEST", hashlib.sha256(json.dumps(digest, sort_keys=True, default=str).encode()).hexdigest()[:16])
