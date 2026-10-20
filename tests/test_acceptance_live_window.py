#!/usr/bin/env python3
"""tests/test_acceptance_live_window.py
scripts/tfb_acceptance.py v1.0.10: TFB_ACCEPTANCE_LIVE_WINDOW lets the LIVE
reader give the decision-integrity checks A1, A2a and A3 the rows and columns
they parse (default off = the v1.0.9 reads). v1.0.11 (Codex P2 on #808): with
the gate on, A2a also reads the committed hidden _T10_Candidates grid when
DT10_CAND_TAB=enforce has moved the audit there (W7).

The defect (run 39420446126, 2026-10-20 12:12Z): A1 'Top_10 board section not
found', A2a 'Top_10 audit section not found', A3-<page> 'Symbol/Name columns
not found' -- 4 hard NAs every day. LiveSource read Top_10_Investments as rows
1-60 x columns 1-12 while the board header is 30 columns wide with Stability in
column 25 (row 19) and the audit header sits at about row 61; the page reads
never included Name.

The harness drives the REAL LiveSource (built without credentials) over a fake
gspread worksheet layer that reproduces that geometry and gspread's trimming of
trailing empty cells and rows. Fixtures are synthetic.
  W1 on: A1, A2a and both A3 rows measure on the live geometry (PASS fixture)
  W2 on: each defect class is caught (untradable seat, FX off the peg, a
     futures / crypto name on an equity row)
  W3 on: every other check is unchanged (verdict, measured, evidence)
  W4 on: the extra reads are exactly 1 Top_10 get + per page 1 header get and
     1 batch_get of 2 columns
  W5 on: a failed window read is recorded in .errors (fail-closed)
  W6 the export / in-memory sources ignore the gate, and the live window
     measures exactly what the export path measures on the same grid
  W7 v1.0.11, on, DT10_CAND_TAB=enforce layout (Codex P2 on #808): the cockpit audit is
     one pointer line and the grid sits in the hidden _T10_Candidates (row 1
     stamp + receipt, row 2 headers, rows 3..). A COMMITTED receipt with the
     same row count is read; PENDING, a row-count mismatch or a missing tab
     stay NA (a missing tab is recorded); the export path agrees
  N1 [needs TFB_TEST_ACC_BASE] off (unset and 8 non-truthy spellings): every
     check row and the worksheet call log equal the v1.0.9 base copy's
  N2 [needs TFB_TEST_ACC_BASE] golden negative: the base copy leaves A1, A2a
     and both A3 rows NA(parse) on the same live grid -- W1 fails on it
  N3 truthy spellings, version floor, the workflow maps the Variable at job
     level with default '0', outside the step #795 edits
  N4 the inline --selftest still passes 8/8
  N5 added lines are ASCII
  G1 [needs TFB_TEST_ACC_EXPORT=<dir of browser TSVs>] the real export grid:
     base live -> NA, delivered live (on) == the export path, row for row
Offline: no network, no credentials, no live Sheet.

Run: pytest -q tests/test_acceptance_live_window.py
  or python3 tests/test_acceptance_live_window.py   (PASS k/k + digest)
  TFB_TEST_ACC_BASE=<git show aa1f440c:scripts/tfb_acceptance.py> (v1.0.9) for N1/N2;
  TFB_TEST_ACC_DELIVERED=<git show 97fa9915:...> (v1.0.10) fails W7 only."""
from __future__ import annotations

import contextlib
import csv
import datetime as _dt
import hashlib
import importlib.util
import io
import json
import os
import re
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
DELIVERED = os.environ.get("TFB_TEST_ACC_DELIVERED") or os.path.join(ROOT, "scripts", "tfb_acceptance.py")
BASE = os.environ.get("TFB_TEST_ACC_BASE", "")
EXPORT = os.environ.get("TFB_TEST_ACC_EXPORT", "")
GATE = "TFB_ACCEPTANCE_LIVE_WINDOW"
A_IDS = ("A1", "A2a", "A3-Global_Markets", "A3-Market_Leaders")
EM, ARROW, STOP = "\u2014", "\u2192", "\u26d4"

_M = {}


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def m():
    if "d" not in _M:
        _M["d"] = _load(DELIVERED, "tfb_acceptance_delivered")
    return _M["d"]


def b():
    if "b" not in _M:
        _M["b"] = _load(BASE, "tfb_acceptance_base") if BASE and os.path.exists(BASE) else None
    return _M["b"]


def _need_base():
    if b() is None:
        pytest.skip("TFB_TEST_ACC_BASE unset (git show aa1f440c:scripts/tfb_acceptance.py)")


@contextlib.contextmanager
def gate(value):
    prev = os.environ.get(GATE)
    if value is None:
        os.environ.pop(GATE, None)
    else:
        os.environ[GATE] = value
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop(GATE, None)
        else:
            os.environ[GATE] = prev


# --------------------------------------------------------------------------- #
# fake gspread layer                                                          #
# --------------------------------------------------------------------------- #
def _col(letters):
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch) - 64)
    return n


def _a1(rng):
    mt = re.match(r"^([A-Z]+)(\d+):([A-Z]+)(\d+)$", rng)
    assert mt, rng
    return _col(mt.group(1)), int(mt.group(2)), _col(mt.group(3)), int(mt.group(4))


class FakeWs(object):
    """gspread.Worksheet stand-in: get / batch_get / get_all_values with the
    API's trimming of trailing empty cells and rows; every call is logged."""

    def __init__(self, title, grid, log, fail_on=None):
        self.title, self.grid, self.log, self.fail_on = title, grid, log, fail_on
        self.row_count = len(grid)
        self.col_count = max((len(r) for r in grid), default=0)

    def _slice(self, rng):
        c0, r0, c1, r1 = _a1(rng)
        assert r1 <= self.row_count and c1 <= max(self.col_count, 1), ("exceeds grid limits", rng)
        out = []
        for r in self.grid[r0 - 1:r1]:
            cells = [str(v) for v in r[c0 - 1:c1]]
            while cells and cells[-1] == "":
                cells.pop()
            out.append(cells)
        while out and not out[-1]:
            out.pop()
        return out

    def get(self, rng):
        self.log.append(("get", self.title, rng))
        if self.fail_on and self.fail_on(rng):
            raise RuntimeError("APIError: [429] quota exceeded (simulated)")
        return self._slice(rng)

    def batch_get(self, ranges):
        self.log.append(("batch_get", self.title, tuple(ranges)))
        return [self._slice(r) for r in ranges]

    def get_all_values(self):
        self.log.append(("get_all_values", self.title))
        return [list(r) for r in self.grid]


class FakeBook(object):
    def __init__(self, tabs, fail_on=None):
        self.tabs, self.log, self.fail_on = tabs, [], fail_on or {}

    def worksheet(self, name):
        self.log.append(("worksheet", name))
        if name not in self.tabs:
            raise RuntimeError(f"WorksheetNotFound: {name}")
        return FakeWs(name, self.tabs[name], self.log, self.fail_on.get(name))


def live(mod, tabs, fail_on=None):
    """The REAL LiveSource of `mod`, without credentials (no gspread import)."""
    src = object.__new__(mod.LiveSource)
    src.sheet_id = "TESTSHEET"
    src._cache = {}
    src.errors = []
    src.inputs = [{"tab": "*live*", "workbook_id": "TESTSHEET"}]
    src.gc = None
    src.book = FakeBook(tabs, fail_on)
    return src


# --------------------------------------------------------------------------- #
# fixtures: the 2026-10-14 export geometry, synthetic values                  #
# --------------------------------------------------------------------------- #
def _now_utc():
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


BOARD_HDR = (["Rank", "Symbol", "Name", "Market", "Sector", "Ccy", "FX" + ARROW + "SAR", "Price", "Price SAR",
              "Entry Zone", "Ticket SAR", "Shares", "Stop SAR", "TP1 SAR", "TP2 SAR", "ROI % (TP1)"]
             + [f"B{i}" for i in range(17, 25)] + ["Stability"] + [f"B{i}" for i in range(26, 31)])
QUAL_HDR = ["Rank", "Symbol", "Name", "Market", "Sector", "ROI % (TP1)", "Engine ROI % (12M)", "Ann ROI %",
            "R/R (TP2)", "Rel", "DQ", "Risk", "Score", "Selected", "Why Not Selected"]
AUDIT_HDR = (["Symbol", "Name", "Market", "Sector", "Ccy", "Price", "Price SAR", "ROI % (TP1)",
              "Engine ROI % (12M)", "Ann ROI %", "R/R (TP2)", "Rel", "DQ", "Risk", "News", "Sector Trend"]
             + [f"C{i}" for i in range(17, 26)])
PAGE_HDR = (["Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
             "Current Price", "Previous Close", "Open", "Day High", "Day Low"]
            + [f"P{i}" for i in range(14, 100)]
            + ["Expected ROI 12M", "Forecast Source", "Target Price", "Investability Status", "Final Action",
               "Warnings", "Last Updated (UTC)"] + [f"P{i}" for i in range(107, 116)])


def _pad(row, n):
    return list(row) + [""] * (n - len(row))


def _audit_rows(usd_fx, n_audit):
    out = []
    for i in range(n_audit):
        if i % 5 == 0:
            out.append([f"{2000 + i}.SR", f"Saudi {i}", "TADAWUL", "Fin", "SAR", "20.00", "20.00", "8.0"])
        else:
            px = 5.0 + i * 0.37
            out.append([f"S{i}.US", f"US Co {i}", "NYSE", "Tech", "USD", f"{px:.2f}", f"{px * usd_fx:.2f}", "9.0"])
    return out


POINTER = ("Full audit (400 rows) written to hidden tab _T10_Candidates " + EM +
           " route v4.16.0 | builder v1.27.0 " + EM + " View \u25b8 Hidden sheets to open")


def top10_grid(seats, usd_fx, n_audit=400, relocated=False):
    W = len(BOARD_HDR)
    g = [[""] * W for _ in range(60 + 2 + n_audit + 40)]

    def put(r, cells):
        g[r - 1] = _pad(cells, W)
    put(1, ["TOP 10 INVESTMENTS " + EM + " DECISION"])
    put(2, ["Status:", "Last run 2026-10-20 14:53:32 | status: ok | output: WITHHELD | builder v1.27.0"])
    put(4, ["KPIs"])
    put(5, ["Deployable (SAR)", "Exp. Gain 12M (SAR)", "Selected", "Blended Reliability", "Blended R/R (TP2)",
            "Scanned", "Passed", "Unallocated (SAR)", "Fundable Now", "By Rotation", "Capital Call (SAR)"])
    put(6, ["0 SAR", "0 SAR", "0 / 10", "71.5", "3.71", "9,786", "8", "0 SAR", "0", "0", "0"])
    put(8, ["ALERTS (1)"])
    put(9, ["Type", "Count", "Required Action"])
    put(10, ["capital_call", "2", "x"])
    put(18, [STOP + " FEED NOT ACTIONABLE " + EM + " final_publication"])
    put(19, BOARD_HDR)
    for i, (sym, ccy) in enumerate(seats):
        row = [str(i + 1), sym, f"Co {sym}", "MKT", "Tech", ccy, "3.75", "10.00", "37.50"]
        row = _pad(row, W)
        row[BOARD_HDR.index("Stability")] = "stable"
        put(20 + i, row)
    put(25, ["ALL QUALIFIED " + EM + " INVEST opportunity set (2)"])
    put(26, QUAL_HDR)
    put(27, ["1", "AAPL.US", "Apple", "NASDAQ", "Tech", "12.0"])
    put(28, ["2", "1120.SR", "Al Rajhi", "TADAWUL", "Fin", "10.0"])
    put(32, ["NEAR MISS " + EM + " closest to qualifying (1)"])
    put(33, ["Symbol", "Failed Gate", "Current", "Required", "Verdict", "How To Qualify"])
    put(34, ["MSFT.US", "rel", "60", "65", "NEAR", "raise rel"])
    put(60, ["CANDIDATES " + EM + " FULL AUDIT " + EM + " top 500 rows written"])
    put(61, AUDIT_HDR)
    if relocated:                      # DT10_CAND_TAB=enforce: header + one pointer line
        put(62, [POINTER])
        put(64, ["route v4.16.0 | builder v1.27.0"])
        return g
    for i, row in enumerate(_audit_rows(usd_fx, n_audit)):
        put(62 + i, row)
    return g


def cand_tab_grid(usd_fx, n_audit=400, state="COMMITTED", rows=None):
    W = len(AUDIT_HDR)
    receipt = json.dumps({"state": state, "run_id": "req-1", "rows": n_audit if rows is None else rows,
                          "cols": W, "checksum": "x"})
    g = [_pad(["run 2026-10-20T11:50:00.000Z | route v4.16.0 | rows %d | DT10 v1.13.10" % n_audit, receipt], W),
         list(AUDIT_HDR)]
    return g + [_pad(r, W) for r in _audit_rows(usd_fx, n_audit)]


def tabs_relocated(**kw):
    t = tabs_good()
    t["Top_10_Investments"] = top10_grid([("1120.SR", "SAR"), ("AAPL.US", "USD"), ("7203.T", "JPY"),
                                          ("SAP.DE", "EUR")], 3.7505, relocated=True)
    t["_T10_Candidates"] = cand_tab_grid(3.7505, **kw)
    return t


def page_grid(rows):
    W = len(PAGE_HDR)
    now = _now_utc()
    g = [list(PAGE_HDR)]
    ix = {h: i for i, h in enumerate(PAGE_HDR)}
    for sym, name in rows:
        r = [""] * W
        r[ix["Symbol"]], r[ix["Name"]] = sym, name
        for k, v in (("Open", "10"), ("Day High", "11"), ("Day Low", "9"), ("Expected ROI 12M", "0.12"),
                     ("Forecast Source", "provider_target"), ("Target Price", "12"),
                     ("Investability Status", "WATCHLIST"), ("Final Action", "WATCH"),
                     ("Last Updated (UTC)", now)):
            r[ix[k]] = v
        g.append(r)
    return g


CLEAN = [("AAPL.US", "Apple Inc."), ("MSFT.US", "Microsoft Corporation"), ("1120.SR", "Al Rajhi Bank"),
         ("7203.T", "Toyota Motor Corporation"), ("CC.US", "The Chemours Company"),
         ("FTFT.US", "Future FinTech Group Inc."), ("NG=F", "Natural Gas Nov 26"), ("BTC-USD", "Bitcoin USD")]
DIRTY = CLEAN + [("KE.US", "KC HRW Wheat Futures,Dec-2026"), ("LINK.US", "Chainlink USD")]


def tabs_good():
    return {"Top_10_Investments": top10_grid([("1120.SR", "SAR"), ("AAPL.US", "USD"), ("7203.T", "JPY"),
                                              ("SAP.DE", "EUR")], 3.7505),
            "Global_Markets": page_grid(CLEAN), "Market_Leaders": page_grid(CLEAN[:4])}


def tabs_bad():
    return {"Top_10_Investments": top10_grid([("1120.SR", "SAR"), ("HDFCBANK.NS", "INR"), ("AAPL.US", "USD")],
                                             3.809),
            "Global_Markets": page_grid(DIRTY), "Market_Leaders": page_grid(CLEAN[:4])}


def rows_of(mod, src):
    out = {}
    for c in mod.run_all(src, 5):
        out[c.cid] = (c.verdict, c.measured, c.evidence, c.na_kind)
    return out


def run_live(mod, tabs, value, fail_on=None):
    with gate(value):
        src = live(mod, tabs, fail_on)
        res = rows_of(mod, src)
    return res, src


# --------------------------------------------------------------------------- #
def _assert_w1(res):
    assert res["A1"][0] == "PASS" and res["A1"][1] == 0 and "seats=4" in res["A1"][2], res["A1"]
    assert res["A2a"][0] == "PASS" and abs(res["A2a"][1] - 3.75) < 0.002, res["A2a"]
    for p in ("Global_Markets", "Market_Leaders"):
        assert res[f"A3-{p}"][0] == "PASS" and res[f"A3-{p}"][1] == 0, res[f"A3-{p}"]
    assert all(res[k][3] is None for k in A_IDS), [res[k] for k in A_IDS]


def test_w1_on_measures_a1_a2a_a3_on_the_live_geometry():
    res, src = run_live(m(), tabs_good(), "1")
    _assert_w1(res)
    assert "equity_rows=6" in res["A3-Global_Markets"][2]          # NG=F / BTC-USD are not equity rows
    assert not [e for e in src.errors if "integrity" in e]


def test_w2_on_catches_each_defect_class():
    res, _src = run_live(m(), tabs_bad(), "1")
    assert res["A1"][0] == "FAIL" and res["A1"][1] == 1 and "HDFCBANK.NS(.NS)" in res["A1"][2], res["A1"]
    assert res["A2a"][0] == "FAIL" and abs(res["A2a"][1] - 3.809) < 0.002, res["A2a"]
    gm = res["A3-Global_Markets"]
    assert gm[0] == "FAIL" and gm[1] == 2 and "KE.US" in gm[2] and "LINK.US" in gm[2], gm
    assert "CC.US" not in gm[2] and "FTFT.US" not in gm[2] and "NG=F" not in gm[2], gm
    assert res["A3-Market_Leaders"][0] == "PASS"


def test_w3_on_leaves_every_other_check_unchanged():
    for tabs in (tabs_good(), tabs_bad()):
        off, _s1 = run_live(m(), tabs, None)
        on, _s2 = run_live(m(), tabs, "1")
        assert set(off) == set(on)
        others = [k for k in off if k not in A_IDS]
        assert len(others) >= 20 and len(others) == len(off) - len(A_IDS)
        assert [off[k] for k in others] == [on[k] for k in others]
        assert all(off[k][0] == "NA" and off[k][3] == "parse" for k in A_IDS), [off[k] for k in A_IDS]
        assert all(on[k][0] != "NA" for k in A_IDS), [on[k] for k in A_IDS]


def test_w4_on_reads_exactly_the_window():
    _r0, off = run_live(m(), tabs_good(), None)
    _r1, on = run_live(m(), tabs_good(), "1")
    extra, j = [], 0
    for call in on.book.log:                                         # the v1.0.9 reads, in order, plus extras
        if j < len(off.book.log) and call == off.book.log[j]:
            j += 1
        else:
            extra.append(call)
    assert j == len(off.book.log), (j, len(off.book.log))
    gets = [c for c in extra if c[0] == "get"]
    batches = [c for c in extra if c[0] == "batch_get"]
    assert ("get", "Top_10_Investments", "A1:AD%d" % len(tabs_good()["Top_10_Investments"])) in gets, gets
    assert len(gets) == 3 and len(batches) == 2, extra               # Top_10 + 2 page headers; 2 batches
    for c in batches:
        assert c[2] == ("A2:A%d" % len(tabs_good()[c[1]]), "B2:B%d" % len(tabs_good()[c[1]])), c
    assert sum(1 for c in extra if c[0] == "worksheet") == 3


def test_w5_on_a_failed_window_read_is_recorded():
    wide = lambda rng: rng.startswith("A1:") and not rng.endswith(":L60")
    res, src = run_live(m(), tabs_good(), "1", fail_on={"Top_10_Investments": wide})
    assert any("Top_10_Investments (integrity window) unreadable" in e for e in src.errors), src.errors
    assert res["A1"][0] == "NA" and res["A2a"][0] == "NA"
    assert m()._overall_verdict(m().run_all(src, 5), src.errors) == "FAIL"
    off, _s = run_live(m(), tabs_good(), None)
    assert res["D10-1"] == off["D10-1"]                              # check_board kept its own read


def _write_export(tabs, d):
    """Browser TSV exports write cell text raw (no quoting, no escaping)."""
    for name, grid in tabs.items():
        with open(os.path.join(d, f"TFB_-_{name}.tsv"), "w", encoding="utf-8", newline="") as fh:
            for r in grid:
                assert not any(("\t" in str(c)) or ("\n" in str(c)) for c in r)
                fh.write("\t".join(str(c) for c in r) + "\n")


def test_w6_export_and_memory_sources_ignore_the_gate_and_match_the_live_window(tmp_path):
    mod = m()
    for tabs in (tabs_good(), tabs_bad()):
        d = tmp_path / ("x%d" % len(tabs["Global_Markets"]))
        d.mkdir()
        _write_export(tabs, str(d))
        with gate(None):
            e_off = rows_of(mod, mod.ExportSource(str(d)))
        with gate("1"):
            e_on = rows_of(mod, mod.ExportSource(str(d)))
            mem = {c.cid: (c.verdict, c.measured, c.evidence, c.na_kind)
                   for c in mod.check_decision_integrity(mod._MemSource(tabs))}
        assert e_off == e_on
        lv, _s = run_live(mod, tabs, "1")
        for k in A_IDS:
            assert lv[k] == e_on[k] == mem[k], (k, lv[k], e_on[k], mem[k])


def test_w7_on_reads_the_committed_relocated_audit(tmp_path):
    mod = m()
    on, src = run_live(mod, tabs_relocated(), "1")
    assert on["A2a"][0] == "PASS" and abs(on["A2a"][1] - 3.75) < 0.002 and "n=320" in on["A2a"][2], on["A2a"]
    assert ("worksheet", "_T10_Candidates") in src.book.log
    assert not [e for e in src.errors if "_T10_Candidates" in e], src.errors
    off, osrc = run_live(mod, tabs_relocated(), None)
    assert off["A2a"][0] == "NA" and "audit section not found" in off["A2a"][2], off["A2a"]   # v1.0.9
    assert ("worksheet", "_T10_Candidates") not in osrc.book.log
    for kw in ({"state": "PENDING"}, {"rows": 399}):
        r, _s = run_live(mod, tabs_relocated(**kw), "1")
        assert r["A2a"][0] == "NA" and "USD rows with both prices: 0" in r["A2a"][2], (kw, r["A2a"])
    gone = tabs_relocated()
    del gone["_T10_Candidates"]
    r, gsrc = run_live(mod, gone, "1")
    assert r["A2a"][0] == "NA" and any("_T10_Candidates unreadable" in e for e in gsrc.errors), gsrc.errors
    assert {k: on[k] for k in on if k not in A_IDS} == {k: off[k] for k in off if k not in A_IDS}
    _write_export(tabs_relocated(), str(tmp_path))
    with gate("1"):
        e_on = rows_of(mod, mod.ExportSource(str(tmp_path)))
    with gate(None):
        e_off = rows_of(mod, mod.ExportSource(str(tmp_path)))
    assert e_on["A2a"] == on["A2a"], (e_on["A2a"], on["A2a"])
    assert e_off["A2a"][0] == "NA" and "USD rows with both prices: 0" in e_off["A2a"][2], e_off["A2a"]


OFF_SPELLINGS = (None, "", "0", "off", "false", "no", "2", "enabled")


def test_n1_off_is_byte_identical_to_v1_0_9():
    _need_base()
    for tabs in (tabs_good(), tabs_bad(), tabs_relocated()):
        b_rows, b_src = run_live(b(), tabs, None)
        for v in OFF_SPELLINGS:
            d_rows, d_src = run_live(m(), tabs, v)
            assert d_rows == b_rows, v
            assert d_src.book.log == b_src.book.log, v
            assert d_src.errors == b_src.errors, v


def test_n2_golden_negative_base_never_measures_the_live_window():
    _need_base()
    res, _src = run_live(b(), tabs_good(), "1")                     # the base copy ignores the gate
    assert [res[k][0] for k in A_IDS] == ["NA"] * 4 and all(res[k][3] == "parse" for k in A_IDS), \
        [res[k] for k in A_IDS]
    assert "board section not found" in res["A1"][2] and "audit section not found" in res["A2a"][2]
    assert "Symbol/Name columns not found" in res["A3-Global_Markets"][2]
    with pytest.raises(AssertionError):
        _assert_w1(res)
    rel, _r = run_live(b(), tabs_relocated(), "1")
    assert rel["A2a"][0] == "NA", rel["A2a"]


def test_n3_spellings_version_and_workflow_mapping():
    mod = m()
    for v in ("1", "true", "TRUE", " yes ", "On"):
        with gate(v):
            assert mod._live_window_enabled() is True, v
    for v in OFF_SPELLINGS:
        with gate(v):
            assert mod._live_window_enabled() is False, v
    assert tuple(int(x) for x in mod.VERSION.split(".")) >= (1, 0, 10)
    wf = open(os.path.join(ROOT, ".github", "workflows", "acceptance_check.yml"), encoding="utf-8").read()
    line = "      TFB_ACCEPTANCE_LIVE_WINDOW: ${{ vars.TFB_ACCEPTANCE_LIVE_WINDOW || '0' }}\n"
    assert wf.count(line) == 1
    job_env = wf.index("    env:\n", wf.index("timeout-minutes"))
    assert job_env < wf.index(line) < wf.index("    steps:\n")         # job level, not the live step
    assert "TFB_ACCEPTANCE_LIVE_WINDOW" not in wf[wf.index("- name: Measure the live workbook"):]


def test_n4_inline_selftest_still_passes():
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        assert m()._selftest() == 0
    assert "selftest: PASS 8/8 fixtures" in buf.getvalue()


def test_n5_added_lines_are_ascii():
    assert open(__file__, "rb").read().isascii()
    lines = open(DELIVERED, encoding="utf-8").read().splitlines()
    if b() is not None:
        base = set(open(BASE, encoding="utf-8").read().splitlines())
        added = [ln for ln in lines if ln not in base]
        assert added and all(ln.isascii() for ln in added), [ln for ln in added if not ln.isascii()]
    else:
        marks = ("v1.0.10", "LIVE_WINDOW", "integrity_tab", "integrity window", "_T10_WIDE_COLS", "_A3_COLS",
                 "_cand_tab", "_CAND_TAB")
        picked = [ln for ln in lines if any(k in ln for k in marks)]
        assert len(picked) >= 10 and all(ln.isascii() for ln in picked), [ln for ln in picked if not ln.isascii()]


def _grid_from_tsv(path):
    with open(path, encoding="utf-8", newline="") as fh:
        return [list(r) for r in csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)]


def test_g1_real_export_grid_live_window_equals_the_export_path():
    if not (EXPORT and os.path.isdir(EXPORT)):
        pytest.skip("TFB_TEST_ACC_EXPORT unset (a directory of '<book>_-_<Tab>.tsv' browser exports)")
    mod = m()
    tabs = {}
    for fn in sorted(os.listdir(EXPORT)):
        for t in ("Top_10_Investments", "Global_Markets", "Market_Leaders"):
            if fn.endswith(f"_-_{t}.tsv"):
                tabs[t] = _grid_from_tsv(os.path.join(EXPORT, fn))
    assert set(tabs) == {"Top_10_Investments", "Global_Markets", "Market_Leaders"}, sorted(tabs)
    with gate(None):
        exp = {c.cid: (c.verdict, c.measured, c.evidence, c.na_kind)
               for c in mod.check_decision_integrity(mod.ExportSource(EXPORT))}
    with gate("1"):
        lv = {c.cid: (c.verdict, c.measured, c.evidence, c.na_kind)
              for c in mod.check_decision_integrity(live(mod, tabs))}
    for k in A_IDS:
        assert lv[k] == exp[k], (k, lv[k], exp[k])
    with gate(None):
        off = {c.cid: c.verdict for c in mod.check_decision_integrity(live(mod, tabs))}
    assert [off[k] for k in A_IDS] == ["NA"] * 4, off
    print("G1 export parity:", " | ".join(f"{k}={lv[k][0]}:{lv[k][1]}" for k in A_IDS))


def _digest():
    parts = []
    for tabs in (tabs_good(), tabs_bad(), tabs_relocated()):
        for v in (None, "1"):
            res, src = run_live(m(), tabs, v)
            keep = {k: res[k] for k in res if not k.startswith(("G1-", "D10-2b"))}   # clock-bound evidence
            parts.append(json.dumps([keep, src.book.log], sort_keys=True, default=str))
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()[:16]


if __name__ == "__main__":
    import tempfile
    from pathlib import Path
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = skipped = 0
    for name, fn in tests:
        try:
            if "tmp_path" in fn.__code__.co_varnames[:fn.__code__.co_argcount]:
                with tempfile.TemporaryDirectory() as td:
                    fn(Path(td))
            else:
                fn()
            passed += 1
            print("ok  ", name)
        except pytest.skip.Exception as exc:
            skipped += 1
            print("SKIP", name, "::", exc)
    print(f"PASS {passed}/{len(tests)}" + (f" (skipped {skipped})" if skipped else "") + f" digest {_digest()}")
