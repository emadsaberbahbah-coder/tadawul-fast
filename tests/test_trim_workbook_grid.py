"""scripts/trim_workbook_grid.py 1.0.0 - the empty grid Google counts, trimmed without touching a value.

WHY (full project review, 27 Nov 2026): 9,736,507 of 20,000,000 cells allocated, about half of them empty grid.

  G1  names and ranges
  G2  the metadata: grid sheets only; charts, slicers, merged ranges and named ranges are extent too
  G3  the used extent is exact: the probe (row 1, column A) and whatever lies outside it; a failed read
  G4  the targets: rows for every tab, columns only for the big tables
  G5  the plan: reductions only; managed, excluded and unmeasured tabs are skipped; the gain floor; structure
  G6  the trim on a fake workbook that REFUSES to delete a cell with a value or under a chart: nothing is lost
  G7  a tab that changed since the plan is skipped; one failing tab does not stop the others; the quota wait
  G8  the four gates; a refused apply never opens the workbook
  G9  the report: counts and tab names only
  G10 the lists held equal to their owners; the workflow; what the tool can and cannot write
  G11 400 random workbooks: every value and object stays where it was, no grid grows, the result equals the plan
"""
from __future__ import annotations

import ast
import random
import re
from pathlib import Path

import pytest

from scripts import trim_workbook_grid as tg

ROOT = Path(__file__).resolve().parents[1]
_RNG = re.compile(r"^'((?:[^']|'')*)'!([A-Z]+)(\d+):([A-Z]+)(\d+)$")


def col_n(letters: str) -> int:
    n = 0
    for ch in letters:
        n = n * 26 + ord(ch) - 64
    return n


def up(a: int, b: int) -> int:
    return -(-a // b)


class FakeBook:
    """An in-memory workbook that answers like the Sheets API: a read drops trailing empty cells and rows and
    fails when it starts outside the grid; a delete that would remove a value, or the cells under a chart, a
    merged range or a named range, RAISES (the tool must never ask for one). Charts are held to their real reach at
    the default cell size (21 x 100 px)."""

    def __init__(self, tabs, charts=None, merges=None, names=None):
        # tabs: {title: (rows, cols, {(r, c): value}, frozen_rows, frozen_cols)}; coordinates start at 1
        self.tabs = {t: {"rows": v[0], "cols": v[1], "cells": dict(v[2]), "fr": (v[3] if len(v) > 3 else 0),
                         "fc": (v[4] if len(v) > 4 else 0), "id": 100 + i, "rows0": v[0], "cols0": v[1]}
                     for i, (t, v) in enumerate(tabs.items())}
        self.charts = charts or {}      # {title: [(anchor row, anchor col, width px, height px)]}
        self.merges = merges or {}      # {title: [(r1, c1, r2, c2)]}
        self.names = names or []        # [(title, r1, c1, r2 or None, c2 or None)]
        self.reads, self.writes, self.fail_tabs, self.quota_once = [], [], set(), set()

    def fetch_meta(self):
        sheets = []
        for t, d in self.tabs.items():
            sh = {"properties": {"title": t, "sheetId": d["id"], "sheetType": "GRID",
                                 "gridProperties": {"rowCount": d["rows"], "columnCount": d["cols"],
                                                    "frozenRowCount": d["fr"], "frozenColumnCount": d["fc"]}}}
            if t in self.charts:
                sh["charts"] = [{"chartId": k, "position": {"overlayPosition": {
                    "anchorCell": {"sheetId": d["id"], "rowIndex": r - 1, "columnIndex": c - 1},
                    "widthPixels": w, "heightPixels": h}}} for k, (r, c, w, h) in enumerate(self.charts[t])]
            if t in self.merges:
                sh["merges"] = [{"sheetId": d["id"], "startRowIndex": r1 - 1, "endRowIndex": r2,
                                 "startColumnIndex": c1 - 1, "endColumnIndex": c2} for r1, c1, r2, c2 in self.merges[t]]
            sheets.append(sh)
        named = []
        for t, r1, c1, r2, c2 in self.names:
            rng = {"sheetId": self.tabs[t]["id"], "startRowIndex": r1 - 1, "startColumnIndex": c1 - 1}
            if r2:
                rng["endRowIndex"] = r2
            if c2:
                rng["endColumnIndex"] = c2
            named.append({"namedRangeId": "n%d" % len(named), "name": "N%d" % len(named), "range": rng})
        return {"sheets": sheets, "namedRanges": named}

    def needs(self, t):
        """(rows, cols) the tab's objects really reach."""
        r = c = 0
        for row, col, w, h in self.charts.get(t, []):
            r, c = max(r, row - 1 + up(h, 21)), max(c, col - 1 + up(w, 100))
        for _r1, _c1, r2, c2 in self.merges.get(t, []):
            r, c = max(r, r2), max(c, c2)
        for tt, r1, c1, r2, c2 in self.names:
            if tt == t:
                r, c = max(r, r2 or r1), max(c, c2 or c1)
        d = self.tabs[t]
        return min(r, d["rows0"]), min(c, d["cols0"])

    def _values(self, rng):
        m = _RNG.match(rng)
        assert m, rng
        d = self.tabs[m.group(1).replace("''", "'")]
        c1, r1, c2, r2 = col_n(m.group(2)), int(m.group(3)), col_n(m.group(4)), int(m.group(5))
        assert 1 <= r1 <= d["rows"] and 1 <= c1 <= d["cols"], "range starts outside the grid: %s" % rng
        r2, c2 = min(r2, d["rows"]), min(c2, d["cols"])
        rows = {}
        for (r, c), v in d["cells"].items():
            if r1 <= r <= r2 and c1 <= c <= c2 and v != "":
                rows.setdefault(r, {})[c] = v
        out = []
        for r in range(r1, max(rows, default=r1 - 1) + 1):
            got = rows.get(r)
            out.append([got.get(c, "") for c in range(c1, max(got) + 1)] if got else [])
        return out

    def batch_get(self, ranges):
        self.reads.append(list(ranges))
        return [self._values(r) for r in ranges]

    def batch_update(self, requests):
        by_id = {d["id"]: (t, d) for t, d in self.tabs.items()}
        title, d = by_id[requests[0]["deleteDimension"]["range"]["sheetId"]]
        if title in self.quota_once:
            self.quota_once.discard(title)
            raise RuntimeError("APIError: [429]: Quota exceeded for quota metric 'Write requests'")
        if title in self.fail_tabs:
            raise PermissionError("protected")
        self.writes.append(list(requests))
        for rq in requests:
            assert list(rq) == ["deleteDimension"], rq                      # the only request the tool may send
            g = rq["deleteDimension"]["range"]
            assert g["sheetId"] == d["id"], "one tab a call"
            a, b = g["startIndex"], g["endIndex"]
            if g["dimension"] == "ROWS":
                assert b == d["rows"] and d["fr"] < a < b, (title, g)       # the tail only, never the whole tab
                assert not [k for k in d["cells"] if k[0] > a], "%s: a row with a value would be deleted" % title
                d["rows"] = a
            else:
                assert g["dimension"] == "COLUMNS" and b == d["cols"] and d["fc"] < a < b, (title, g)
                assert not [k for k in d["cells"] if k[1] > a], "%s: a column with a value would be deleted" % title
                d["cols"] = a
        need = self.needs(title)
        assert d["rows"] >= need[0] and d["cols"] >= need[1], "%s: a chart, merge or name was cut: %s" % (title, need)

    def allocated(self):
        return sum(d["rows"] * d["cols"] for d in self.tabs.values())


def table(rows, cols):
    """A header row and rows of values, every cell filled."""
    return {(r, c): "v" for r in range(1, rows + 1) for c in range(1, cols + 1)}


QUIET = lambda *_: None      # noqa: E731


# ----------------------------------------------------------------------------- G1 names and ranges
def test_g1_names_and_ranges():
    assert [tg.col_letter(n) for n in (1, 26, 27, 52, 115, 702, 703)] == ["A", "Z", "AA", "AZ", "DK", "ZZ", "AAA"]
    with pytest.raises(ValueError):
        tg.col_letter(0)
    assert all(col_n(tg.col_letter(n)) == n for n in range(1, 2000))
    assert tg.quoted("Investable Symbol") == "'Investable Symbol'" and tg.quoted("Owner's") == "'Owner''s'"
    assert tg.a1("_Run_Log", 2, 11, 28361, 26) == "'_Run_Log'!K2:Z28361"
    assert _RNG.match(tg.a1("Owner's tab", 1, 1, 5, 3)).group(1) == "Owner''s tab"


# ----------------------------------------------------------------------------- G2 the metadata
def test_g2_grid_sheets_only():
    meta = {"sheets": [
        {"properties": {"title": "A", "sheetId": 1, "gridProperties": {"rowCount": 1000, "columnCount": 26}}},
        {"properties": {"title": "B", "sheetId": 2, "sheetType": "GRID",
                        "gridProperties": {"rowCount": 50, "columnCount": 4, "frozenRowCount": 1, "frozenColumnCount": 2}}},
        {"properties": {"title": "Chart1", "sheetId": 3, "sheetType": "OBJECT"}},
        {"properties": {"title": "Broken", "sheetId": 4, "gridProperties": {"rowCount": "x"}}},
        {"properties": {"sheetId": 5, "gridProperties": {"rowCount": 1, "columnCount": 1}}}, None]}
    assert tg.tabs_from_meta(meta) == [
        {"title": "A", "sheet_id": 1, "rows": 1000, "cols": 26, "frozen_rows": 0, "frozen_cols": 0,
         "obj_rows": 0, "obj_cols": 0},
        {"title": "B", "sheet_id": 2, "rows": 50, "cols": 4, "frozen_rows": 1, "frozen_cols": 2,
         "obj_rows": 0, "obj_cols": 0}]
    assert tg.tabs_from_meta(None) == [] and tg.tabs_from_meta({"sheets": "x"}) == [] and tg.tabs_from_meta({}) == []


def test_g2_structure_is_extent_too():
    def chart(row, col, w=None, h=None, dx=0, dy=0):
        ov = {"anchorCell": {"rowIndex": row, "columnIndex": col}, "offsetXPixels": dx, "offsetYPixels": dy}
        if w is not None:
            ov.update(widthPixels=w, heightPixels=h)
        return {"chartId": 1, "position": {"overlayPosition": ov}}
    assert (tg.OBJ_ROW_PX, tg.OBJ_COL_PX, tg.OBJ_DEFAULT_H, tg.OBJ_DEFAULT_W) == (15, 50, 371, 600)
    # a chart: its anchor cell plus its size, counted at 15 px a row and 50 px a column (further than it reaches)
    assert tg.structure_extent({"charts": [chart(304, 0, 600, 200)]}) == (304 + 14, 12)
    assert tg.structure_extent({"charts": [chart(10, 8, 480, 300, dx=30, dy=5)]}) == (10 + 21, 8 + 11)
    assert tg.structure_extent({"charts": [chart(0, 0)]}) == (25, 12)                       # the API's default size
    assert tg.structure_extent({"charts": [{"chartId": 2, "position": {"sheetId": 9}}]}) == (0, 0)   # on its own sheet
    assert tg.structure_extent({"slicers": [chart(40, 20, 200, 60)]}) == (44, 24)
    # merged ranges end where they end
    assert tg.structure_extent({"merges": [{"startRowIndex": 0, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 10},
                                           {"startRowIndex": 399, "endRowIndex": 402, "startColumnIndex": 0, "endColumnIndex": 4}]}) == (402, 10)
    # a named range is kept whole where it has an end; an open one (A:A) keeps its first row and column
    assert tg.structure_extent({}, [{"startRowIndex": 63, "endRowIndex": 118, "startColumnIndex": 0, "endColumnIndex": 7}]) == (118, 7)
    assert tg.structure_extent({}, [{"startRowIndex": 5, "startColumnIndex": 3}]) == (6, 4) and tg.structure_extent({}, [{}]) == (1, 1)
    # the largest of all of them; nothing and nonsense are nothing
    assert tg.structure_extent({"charts": [chart(0, 0, 100, 30)], "merges": [{"endRowIndex": 7, "endColumnIndex": 30}]},
                               [{"endRowIndex": 90, "endColumnIndex": 2}]) == (90, 30)
    for junk in (None, {}, {"charts": "x", "merges": 3, "slicers": None}, {"charts": [None, 5, {"position": None}]}):
        assert tg.structure_extent(junk) == (0, 0), junk
    # through the metadata: by sheet id, never beyond the grid
    meta = {"sheets": [
        {"properties": {"title": "A", "sheetId": 1, "gridProperties": {"rowCount": 1000, "columnCount": 26}},
         "charts": [chart(304, 0, 600, 200)]},
        {"properties": {"title": "B", "sheetId": 2, "gridProperties": {"rowCount": 50, "columnCount": 4}},
         "charts": [chart(45, 0, 600, 371)]},
        {"properties": {"title": "C", "sheetId": 3, "gridProperties": {"rowCount": 500, "columnCount": 9}}}],
        "namedRanges": [{"name": "LIST", "range": {"sheetId": 3, "startRowIndex": 63, "endRowIndex": 118, "endColumnIndex": 7}},
                        {"name": "OTHER", "range": {"sheetId": 99, "endRowIndex": 5}}, {"name": "BROKEN"}, None]}
    got = {t["title"]: (t["obj_rows"], t["obj_cols"]) for t in tg.tabs_from_meta(meta)}
    assert got == {"A": (318, 12), "B": (50, 4), "C": (118, 7)}


# ----------------------------------------------------------------------------- G3 the used extent
def test_g3_the_extent_is_exact():
    book = FakeBook({
        "log": (1000, 26, table(120, 10)),                                   # a full table: the probe is the answer
        "page": (1000, 26, {(1, 1): "TITLE", (3, 1): "a", (3, 12): "wide", (40, 2): "low"}),   # values off the probe
        "gap": (500, 8, {(1, 1): "h", (1, 2): "h", (300, 2): "x"}),          # column A ends at row 1, a value at row 300
        "formula": (200, 6, {(1, 1): "h", (150, 5): '=IF(A1="","",A1)'}),    # a formula that shows nothing is a value
        "empty": (1000, 26, {}),
        "full": (30, 3, table(30, 3)),                                       # nothing outside the probe to read
        "Owner's": (100, 5, {(2, 2): "q"}),
    })
    tabs = tg.tabs_from_meta(book.fetch_meta())
    ext = tg.measure(book, tabs, QUIET)
    assert ext == {"log": (120, 10), "page": (40, 12), "gap": (300, 2), "formula": (150, 5), "empty": (0, 0),
                   "full": (30, 3), "Owner's": (2, 2)}
    # two reads for the whole set: the probes, then the regions outside them
    assert len(book.reads) == 2 and len(book.reads[0]) == 2 * len(tabs)
    assert "'log'!A1:Z1" in book.reads[0] and "'log'!A1:A1000" in book.reads[0]
    assert "'log'!A121:Z1000" in book.reads[1] and "'log'!K1:Z120" in book.reads[1]
    assert not [r for r in book.reads[1] if r.startswith("'full'")]         # a tab with no region outside its probe
    # the pure pieces
    assert tg.probe_extent([["a", "b", "c"]], [["a"], [], ["x"]]) == (3, 3) and tg.probe_extent([], []) == (0, 0)
    assert tg.verify_ranges(tabs[0], 120, 10) == ["'log'!A121:Z1000", "'log'!K1:Z120"]
    assert tg.verify_ranges(tabs[0], 1000, 26) == [None, None] and tg.verify_ranges(tabs[0], 0, 0) == ["'log'!A1:Z1000", None]
    assert tg.used_extent(120, 10, [], []) == (120, 10)
    assert tg.used_extent(120, 10, [[], ["", "", "x"]], []) == (122, 10)             # two more rows below
    assert tg.used_extent(120, 10, [["a"] * 14], []) == (121, 14)                    # a wider row below
    assert tg.used_extent(120, 10, [], [[], ["", "y"]]) == (120, 12)                 # two more columns to the right
    assert tg.used_extent(0, 0, None, None) == (0, 0)
    # the client reads with FORMULA rendering - a formula that shows nothing must count as used
    src = (ROOT / "scripts" / "trim_workbook_grid.py").read_text(encoding="utf-8")
    assert '"valueRenderOption": "FORMULA"' in src and "FORMATTED_VALUE" not in src and "UNFORMATTED" not in src


def test_g3_more_tabs_than_a_chunk_and_a_failed_read():
    names = ["t%02d" % i for i in range(45)]
    book = FakeBook({n: (1000, 26, table(10 + i, 3)) for i, n in enumerate(names)})
    ext = tg.measure(book, tg.tabs_from_meta(book.fetch_meta()), QUIET)
    assert ext == {n: (10 + i, 3) for i, n in enumerate(names)}
    assert len(book.reads) == 6 and max(len(r) for r in book.reads) == 2 * tg.CHUNK == 40      # 3 chunks x 2 reads
    # one tab whose read fails: its chunk is read again tab by tab, and only that tab is left unmeasured (None)
    real = book.batch_get

    def flaky(ranges):
        if any(r.startswith("'t27'") for r in ranges):
            raise TimeoutError("read timed out")
        return real(ranges)
    book.batch_get = flaky
    said = []
    ext = tg.measure(book, tg.tabs_from_meta(book.fetch_meta()), said.append)
    assert [n for n in names if ext[n] is None] == ["t27"] and ext["t26"] == (36, 3) and ext["t28"] == (38, 3)
    assert len(ext) == 45 and ext["t00"] == (10, 3) and ext["t44"] == (54, 3)
    assert said == ["measure: a read of 20 tab(s) failed (TimeoutError) - reading them one by one",
                    "measure failed for t27: TimeoutError"]
    # a short answer is a failed read too: nothing is guessed - from the probe ...
    book.batch_get = lambda ranges: real(ranges)[:-1]
    assert set(tg.measure(book, tg.tabs_from_meta(book.fetch_meta()), QUIET).values()) == {None}
    # ... or from the second read (the regions outside the probe)
    n = {"calls": 0}

    def short_second(ranges):
        n["calls"] += 1
        return real(ranges) if n["calls"] % 2 else real(ranges)[:-1]
    book.batch_get = short_second
    assert set(tg.measure(book, tg.tabs_from_meta(book.fetch_meta()), QUIET).values()) == {None}
    # the managed pages are never read
    book2 = FakeBook({"Global_Markets": (6613, 115, table(20, 115)), "x": (1000, 26, table(5, 2))})
    assert tg.measure(book2, tg.tabs_from_meta(book2.fetch_meta()), QUIET) == {"x": (5, 2)}
    assert not [r for rs in book2.reads for r in rs if "Global_Markets" in r]


# ----------------------------------------------------------------------------- G4 the targets
def test_g4_targets():
    assert (tg.BIG_ROWS, tg.BIG_ROW_MARGIN, tg.SMALL_ROW_MARGIN, tg.COL_MARGIN, tg.MIN_GAIN) == (5000, 1000, 300, 4, 5000)
    # rows: a small tab keeps the larger of twice its rows and its rows + 300
    assert [tg.target_rows(n, 0) for n in (0, 1, 78, 300, 301, 2500, 5000)] == [300, 301, 378, 600, 602, 5000, 10000]
    # a table that grows by appends keeps 1,000 spare rows
    assert [tg.target_rows(n, 0) for n in (5001, 28312, 40715)] == [6001, 29312, 41715]
    assert tg.target_rows(0, 400) == 402                                     # never inside the frozen rows
    # columns: only the big tables are narrowed
    assert tg.target_cols(78, 10, 26, 0) == 26 and tg.target_cols(5000, 10, 26, 0) == 26
    assert tg.target_cols(5001, 10, 26, 0) == 14 and tg.target_cols(28312, 10, 26, 0) == 14
    assert tg.target_cols(9000, 2, 26, 9) == 10                              # never inside the frozen columns


# ----------------------------------------------------------------------------- G5 the plan
def tab(title, rows, cols, fr=0, fc=0, sid=7, obj=(0, 0)):
    return {"title": title, "sheet_id": sid, "rows": rows, "cols": cols, "frozen_rows": fr, "frozen_cols": fc,
            "obj_rows": obj[0], "obj_cols": obj[1]}


def test_g5_plan_rules():
    # the default tab the review found: 1,000 x 26 for a 78-row page
    p = tg.plan_tab(tab("Today_Action_Plan", 1000, 26), (78, 10))
    assert (p["action"], p["to_rows"], p["to_cols"], p["freed"]) == ("TRIM", 378, 26, 1000 * 26 - 378 * 26)
    assert p["allocated"] == 26000 and p["used"] == 780 and p["archive_candidate"] is False and "held_rows" not in p
    # the run log: rows are tight, 16 unused columns
    p = tg.plan_tab(tab("_Run_Log", 28361, 26), (28312, 10))
    assert (p["action"], p["to_rows"], p["to_cols"], p["freed"]) == ("TRIM", 28361, 14, 28361 * 12)
    # a grid is never grown: the target is capped at the grid
    p = tg.plan_tab(tab("tight", 80, 26), (78, 10))
    assert (p["action"], p["to_rows"], p["to_cols"], p["reason"]) == ("SKIP", 80, 26, "gain under 5,000 cells")
    p = tg.plan_tab(tab("big tight", 41000, 22), (40715, 21))
    assert p["action"] == "SKIP" and p["to_rows"] == 41000 and p["to_cols"] == 22 and p["freed"] == 0
    # the gain floor: 4,992 cells is not worth a touch, 5,018 is
    assert tg.plan_tab(tab("a", 570, 26), (78, 10))["action"] == "SKIP"      # (570 - 378) x 26 = 4,992
    assert tg.plan_tab(tab("a", 571, 26), (78, 10))["action"] == "TRIM"      # 5,018
    assert tg.plan_tab(tab("a", 578, 25), (78, 10))["freed"] == 5000         # exactly the floor is enough
    # never: the pages the sync sizes, an excluded tab, a tab that was not measured
    for name in tg.MANAGED_TABS:
        assert tg.plan_tab(tab(name, 9000, 122), (5, 122))["reason"] == "sized by the sync"
    assert tg.plan_tab(tab("x", 1000, 26), (5, 2), exclude=["x"])["reason"] == "excluded"
    p = tg.plan_tab(tab("x", 1000, 26), None)
    assert p["action"] == "SKIP" and p["reason"] == "extent not measured" and p["freed"] == 0 and "used" not in p
    # frozen rows hold the floor
    assert tg.plan_tab(tab("f", 1000, 26, fr=500), (10, 3))["to_rows"] == 502
    # archive candidates are marked, and planned like any other tab (the tool never deletes a tab)
    for name in ("Sheet88", "_SM_TEST", "System_Logs", "Daily_Review_2026_10_14", "Broker_Reconciliation_2026_10_1",
                 "_PIT_Backup_20261126"):
        assert tg.plan_tab(tab(name, 1000, 26), (5, 2))["archive_candidate"] is True, name
    for name in ("Signal_History", "_News_Items", "S1_Gate", "Sheet970", "Daily_Plan", "Top_10_Investments_v2"):
        assert tg.is_archive_candidate(name) is False, name


def test_g5_structure_holds_the_floor():
    # 28 charts down column A of a page that holds five rows of values: the charts are the extent
    p = tg.plan_tab(tab("_DP2_Probe", 1000, 26, obj=(318, 12)), (5, 2))
    assert (p["action"], p["to_rows"], p["to_cols"]) == ("TRIM", 636, 26)
    assert (p["used_rows"], p["used_cols"], p["held_rows"], p["held_cols"]) == (5, 2, 318, 12)
    # a named list at rows 900-950 of a default tab: nothing worth trimming is left
    p = tg.plan_tab(tab("lists", 1000, 26, obj=(950, 3)), (1, 1))
    assert (p["action"], p["to_rows"], p["reason"]) == ("SKIP", 1000, "gain under 5,000 cells") and p["held_rows"] == 950
    # a chart to the right of a big table holds its columns
    p = tg.plan_tab(tab("big", 40000, 40, obj=(30, 30)), (20000, 10))
    assert (p["to_rows"], p["to_cols"]) == (21000, 34) and (p["held_rows"], p["held_cols"]) == (20000, 30)
    # structure inside the values changes nothing and is not noted
    p = tg.plan_tab(tab("page", 1000, 26, obj=(40, 8)), (78, 10))
    assert (p["to_rows"], p["to_cols"]) == (378, 26) and "held_rows" not in p


def test_g5_requests_and_ranges():
    p = tg.plan_tab(tab("_Run_Log", 30000, 26, sid=42), (28312, 10))
    assert (p["to_rows"], p["to_cols"]) == (29312, 14)
    assert tg.delete_ranges(p) == ["'_Run_Log'!A29313:Z30000", "'_Run_Log'!O1:Z29312"]
    assert tg.delete_requests(p) == [
        {"deleteDimension": {"range": {"sheetId": 42, "dimension": "ROWS", "startIndex": 29312, "endIndex": 30000}}},
        {"deleteDimension": {"range": {"sheetId": 42, "dimension": "COLUMNS", "startIndex": 14, "endIndex": 26}}}]
    p = tg.plan_tab(tab("page", 1000, 26, sid=9), (78, 10))
    assert tg.delete_ranges(p) == ["'page'!A379:Z1000"]
    assert [r["deleteDimension"]["range"]["dimension"] for r in tg.delete_requests(p)] == ["ROWS"]


# ----------------------------------------------------------------------------- G6 the trim
TRIMMED = ["Today_Action_Plan", "_Run_Log", "Signal_History", "Sheet88", "empty", "frozen", "_DP2_Probe", "banner"]


def workbook():
    return FakeBook({
        "Global_Markets": (6613, 115, table(60, 115)),
        "Today_Action_Plan": (1000, 26, {(1, 1): "T", (78, 10): "last", (40, 3): "mid"}),
        "_Run_Log": (28361, 26, table(5200, 10)),
        "Signal_History": (60000, 26, table(5100, 21)),
        "Sheet88": (1000, 61, {(5, 61): "x", (1, 1): "h"}),
        "tight": (90, 12, table(80, 12)),
        "empty": (1000, 26, {}),
        "frozen": (1000, 26, {(1, 1): "h"}, 2, 1),
        "_DP2_Probe": (1000, 26, {(1, 1): "h", (5, 2): "x"}),
        "banner": (1000, 26, {(1, 1): "T"}),
        "lists": (1000, 26, {(1, 1): "h"}),
    }, charts={"_DP2_Probe": [(8 + 11 * k, 1, 600, 200) for k in range(28)]},        # the last one at row 305
       merges={"banner": [(400, 1, 402, 12)]},
       names=[("lists", 900, 1, 950, 3), ("tight", 2, 1, None, 1)])


def test_g6_nothing_is_lost_and_the_totals_hold():
    book = workbook()
    cells_before = {t: dict(d["cells"]) for t, d in book.tabs.items()}
    before = book.allocated()
    plan = tg.build_plan(book, log=QUIET)
    by = {p["title"]: p for p in plan}
    assert [p["title"] for p in plan] == list(book.tabs)                     # one row per tab, in the workbook's order
    assert by["Global_Markets"]["reason"] == "sized by the sync" and by["tight"]["action"] == "SKIP"
    assert (by["Today_Action_Plan"]["to_rows"], by["Today_Action_Plan"]["to_cols"]) == (378, 26)
    assert (by["_Run_Log"]["to_rows"], by["_Run_Log"]["to_cols"]) == (6200, 14)
    assert (by["Signal_History"]["to_rows"], by["Signal_History"]["to_cols"]) == (6100, 25)
    assert (by["Sheet88"]["to_rows"], by["Sheet88"]["to_cols"]) == (305, 61)     # a small tab keeps its columns
    assert (by["empty"]["to_rows"], by["frozen"]["to_rows"]) == (300, 301)
    # values alone would leave the chart page 305 rows and cut its last chart; the charts hold 318, so it keeps 636
    assert (by["_DP2_Probe"]["used_rows"], by["_DP2_Probe"]["held_rows"], by["_DP2_Probe"]["to_rows"]) == (5, 318, 636)
    assert book.needs("_DP2_Probe") == (314, 6)
    assert (by["banner"]["held_rows"], by["banner"]["to_rows"]) == (402, 804)
    assert (by["lists"]["action"], by["lists"]["held_rows"]) == ("SKIP", 950)
    assert [p["title"] for p in plan if p["action"] == "TRIM"] == TRIMMED
    assert sum(p["allocated"] for p in plan) == before
    reads_before_apply = len(book.reads)
    slept = []
    freed, problems = tg.apply_plan(book, plan, QUIET, slept.append)
    assert problems == [] and freed == sum(p["freed"] for p in plan) and book.allocated() == before - freed
    assert {t: d["cells"] for t, d in book.tabs.items()} == cells_before     # every value where it was
    for p in plan:
        d = book.tabs[p["title"]]
        assert (d["rows"], d["cols"]) == (p["to_rows"], p["to_cols"]), p["title"]
    trimmed = [p for p in plan if p["action"] == "TRIM"]
    assert len(book.writes) == 8 and len(book.reads) == reads_before_apply + 8
    assert slept == [tg.PAUSE_S] * 8                                         # one pause a tab: under the quota
    # every region was read again just before its delete
    for p, reads in zip(trimmed, book.reads[reads_before_apply:]):
        assert reads == tg.delete_ranges(p)
    # a second pass finds nothing left to do, and the measured extents did not move
    plan2 = tg.build_plan(book, log=QUIET)
    assert [p for p in plan2 if p["action"] == "TRIM"] == [] and sum(p["freed"] for p in plan2) == 0
    assert {p["title"]: (p.get("used_rows"), p.get("used_cols")) for p in plan2} == {
        p["title"]: (p.get("used_rows"), p.get("used_cols")) for p in plan}
    assert tg.apply_plan(book, plan2, QUIET, slept.append) == (0, []) and len(book.writes) == 8


# ----------------------------------------------------------------------------- G7 skips, failures, quota
def test_g7_a_tab_that_changed_since_the_plan_is_skipped():
    book = workbook()
    plan = tg.build_plan(book, log=QUIET)
    by = {p["title"]: p for p in plan}
    book.tabs["Today_Action_Plan"]["cells"][(900, 2)] = "a writer appended after the plan"      # in the row region
    book.tabs["_Run_Log"]["cells"][(3, 20)] = "a wider row after the plan"                      # in the column region
    book.tabs["empty"]["rows"] = 1200                                                           # a writer resized it
    said, slept = [], []
    freed, problems = tg.apply_plan(book, plan, said.append, slept.append)
    assert problems == ["Today_Action_Plan: gained a value since the plan - skipped",
                        "_Run_Log: gained a value since the plan - skipped",
                        "empty: grid changed since the plan - skipped"]
    assert (book.tabs["Today_Action_Plan"]["rows"], book.tabs["_Run_Log"]["cols"], book.tabs["_Run_Log"]["rows"],
            book.tabs["empty"]["rows"]) == (1000, 26, 28361, 1200)                              # untouched
    assert book.tabs["Today_Action_Plan"]["cells"][(900, 2)] and book.tabs["_Run_Log"]["cells"][(3, 20)]
    assert freed == sum(p["freed"] for p in plan) - sum(by[t]["freed"] for t in ("Today_Action_Plan", "_Run_Log", "empty"))
    assert "skip Today_Action_Plan: its trim region is no longer empty" in said
    assert "skip empty: its grid changed since the plan" in said
    assert not [s for s in said if "appended" in s or "wider row" in s]          # no cell value in the log
    assert slept == [tg.PAUSE_S] * 8                                             # a skipped tab is paced too
    # a short answer to the second read is not an empty region either
    book = workbook()
    plan = tg.build_plan(book, log=QUIET)
    real = book.batch_get
    book.batch_get = lambda ranges: real(ranges)[:-1]
    freed, problems = tg.apply_plan(book, plan, QUIET, QUIET)
    assert freed == 0 and len(problems) == 8 and book.writes == []
    # the grids cannot be read again: nothing is read, nothing is written
    book = workbook()
    plan = tg.build_plan(book, log=QUIET)
    reads = len(book.reads)

    def down():
        raise ConnectionError("x")
    book.fetch_meta = down
    assert tg.apply_plan(book, plan, QUIET, QUIET) == (0, ["grids unreadable before the trim: ConnectionError"])
    assert book.writes == [] and len(book.reads) == reads


def test_g7_one_failure_does_not_stop_the_others_and_the_quota_wait():
    book = workbook()
    plan = tg.build_plan(book, log=QUIET)
    book.fail_tabs = {"Sheet88"}
    book.quota_once = {"_Run_Log"}
    said, slept = [], []
    freed, problems = tg.apply_plan(book, plan, said.append, slept.append)
    assert problems == ["Sheet88: PermissionError"] and "FAILED Sheet88: PermissionError" in said
    assert book.tabs["Sheet88"]["rows"] == 1000 and book.tabs["_Run_Log"]["cols"] == 14            # the quota answer was retried
    assert slept.count(tg.QUOTA_WAIT_S) == 1 and slept.count(tg.PAUSE_S) == 8
    assert freed == sum(p["freed"] for p in plan if p["title"] != "Sheet88")
    # what counts as a quota answer
    for msg in ("APIError: [429]: slow down", "RESOURCE_EXHAUSTED", "Quota exceeded for quota metric 'Read requests'"):
        assert tg._quota(RuntimeError(msg)) is True, msg
    assert tg._quota(RuntimeError("APIError: [403]: The caller does not have permission")) is False
    # the wait is one wait and one more try; any other error is not retried
    calls = []

    def always_quota():
        calls.append(1)
        raise RuntimeError("429 RESOURCE_EXHAUSTED")
    with pytest.raises(RuntimeError):
        tg._with_quota_wait(always_quota, QUIET)
    assert len(calls) == 2

    def other():
        calls.append(1)
        raise KeyError("x")
    with pytest.raises(KeyError):
        tg._with_quota_wait(other, QUIET)
    assert len(calls) == 3


# ----------------------------------------------------------------------------- G8 the gates
def test_g8_gates(monkeypatch, capsys):
    env = {"TFB_GRID_TRIM_APPLY": "1"}
    assert tg.gates_missing(True, True, "TRIM", env) == []
    assert tg.gates_missing(False, True, "TRIM", env) == ["--apply"]
    assert tg.gates_missing(True, False, "TRIM", env) == ["--i-have-backup"]
    assert tg.gates_missing(True, True, "trim", env) == ["--confirm TRIM"]
    assert tg.gates_missing(True, True, "RESIZE", env) == ["--confirm TRIM"]            # the August tool's word is not this one
    assert tg.gates_missing(True, True, "TRIM", {}) == ["env TFB_GRID_TRIM_APPLY=1"]
    assert tg.gates_missing(True, True, "TRIM", {"TFB_GRID_TRIM_APPLY": "0"}) == ["env TFB_GRID_TRIM_APPLY=1"]
    assert tg.gates_missing(False, False, "", {}) == ["--apply", "--i-have-backup", "--confirm TRIM", "env TFB_GRID_TRIM_APPLY=1"]

    opened = []
    book = workbook()

    class Client:
        def __init__(self, sid):
            opened.append(sid)
            self.fetch_meta, self.batch_get, self.batch_update = book.fetch_meta, book.batch_get, book.batch_update

        def connect(self):
            pass
    slept = []
    monkeypatch.setattr(tg, "GridClient", Client)
    monkeypatch.setattr(tg.time, "sleep", slept.append)
    for name in ("TFB_GRID_TRIM_APPLY", "DEFAULT_SPREADSHEET_ID", "TFB_SPREADSHEET_ID"):
        monkeypatch.delenv(name, raising=False)
    # a half-armed apply is refused before the workbook is opened
    for argv in (["--apply"], ["--apply", "--i-have-backup"], ["--confirm", "TRIM"],
                 ["--apply", "--i-have-backup", "--confirm", "TRIM"]):                   # the env gate is missing
        assert tg.main(["--spreadsheet-id", "S"] + argv) == 2
    assert opened == [] and capsys.readouterr().out.count("APPLY REFUSED - missing:") == 4
    assert tg.main([]) == 2 and opened == []                                             # no workbook named
    capsys.readouterr()
    # the report writes nothing
    before = book.allocated()
    assert tg.main(["--spreadsheet-id", "S"]) == 0 and book.writes == [] and book.allocated() == before
    out = capsys.readouterr().out
    assert "REPORT ONLY - nothing written." in out and "archive candidates" in out and opened == ["S"]
    # every gate open: the trim runs and the closing line gives before and after
    monkeypatch.setenv("TFB_GRID_TRIM_APPLY", "1")
    assert tg.main(["--spreadsheet-id", "S", "--apply", "--i-have-backup", "--confirm", "TRIM", "--exclude", "empty, frozen"]) == 0
    out = capsys.readouterr().out
    assert len(book.writes) == 6 and book.tabs["empty"]["rows"] == 1000 and book.tabs["frozen"]["rows"] == 1000
    assert "applied: freed %s cells; allocated before %s, after %s; 0 problem(s)" % (
        format(before - book.allocated(), ","), format(before, ","), format(book.allocated(), ",")) in out
    assert "NOTE:" not in out and slept == [tg.PAUSE_S] * 6
    # a tab that could not be trimmed turns the run red (exit 1) and is named
    book.tabs["empty"]["cells"][(1, 1)] = "x"
    book.fail_tabs = {"frozen"}
    assert tg.main(["--spreadsheet-id", "S", "--apply", "--i-have-backup", "--confirm", "TRIM"]) == 1
    assert "1 problem(s): frozen: PermissionError" in capsys.readouterr().out


# ----------------------------------------------------------------------------- G9 the report
def test_g9_report_counts_and_names_only():
    book = workbook()
    book.tabs["Today_Action_Plan"]["cells"][(2, 2)] = "SECRET-CELL-VALUE 12,345 SAR"
    plan = tg.build_plan(book, log=QUIET)
    lines = tg.report_lines(plan)
    text = "\n".join(lines)
    before, freed = book.allocated(), sum(p["freed"] for p in plan)
    assert lines[0] == "plan: 11 tab(s); allocated %s of 20,000,000 (%.1f%%); used range %s cells" % (
        format(before, ","), 100.0 * before / 20_000_000, format(sum(p.get("used", 0) for p in plan), ","))
    assert "freed %s cells in 8 tab(s); allocated after: %s" % (format(freed, ","), format(before - freed, ",")) in text
    assert "archive candidates (never deleted by this tool; the owner decides): 1 tab(s), 61,000 cells allocated: Sheet88" in text
    assert "SECRET" not in text and "SAR" not in text
    row = [ln for ln in lines if ln.startswith("_Run_Log")][0].split()
    assert row[:5] == ["_Run_Log", "28361x26", "5200x10", "6200x14", format(28361 * 26 - 6200 * 14, ",")] and row[5] == "TRIM"
    assert [ln for ln in lines if ln.startswith("Global_Markets")][0].endswith("skip: sized by the sync")
    assert [ln for ln in lines if ln.startswith("Sheet88")][0].endswith("TRIM | archive candidate")
    assert [ln for ln in lines if ln.startswith("_DP2_Probe")][0].endswith("TRIM | charts, merges or names reach 318x12")
    assert [ln for ln in lines if ln.startswith("lists")][0].endswith(
        "skip: gain under 5,000 cells | charts, merges or names reach 950x3")
    order = [ln.split()[0] for ln in lines[2:13]]
    assert sorted(order) == sorted(book.tabs) and order[0] == "Signal_History"                     # largest gain first
    assert order.index("_Run_Log") < order.index("Sheet88") < order.index("Global_Markets")
    assert tg.CELL_LIMIT == 20_000_000 and len(lines) == 2 + 11 + 2


# ----------------------------------------------------------------------------- G10 lists, workflow, what it can write
def _literal(path, name):
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(name)


def test_g10_lists_are_their_owners():
    assert list(tg.DEAD_TABS) == _literal("scripts/tfb_export_audit.py", "DEAD_TABS")
    # the pages the sync sizes: the August tool's own map, less the run log (a log, trimmed like any other)
    src = (ROOT / "scripts" / "repair_grid_capacity.py").read_text(encoding="utf-8")
    block = src[src.index("DEFAULT_TARGETS: Dict[str, int] = {"):]
    block = block[:block.index("}")]
    assert sorted(re.findall(r'"([A-Za-z_]+)":', block)) == sorted(tg.MANAGED_TABS) and "RUN_LOG_TAB:" in block
    assert len(tg.MANAGED_TABS) == 5 and "_Run_Log" not in tg.MANAGED_TABS


def test_g10_workflow_and_what_the_tool_can_write():
    import yaml
    wf = yaml.safe_load((ROOT / ".github" / "workflows" / "grid_trim.yml").read_text(encoding="utf-8"))
    on = wf[True] if True in wf else wf["on"]
    assert list(on) == ["workflow_dispatch"]                                  # by hand only: no schedule, no chain
    inputs = on["workflow_dispatch"]["inputs"]
    assert inputs["apply"]["default"] == "false" and inputs["have_backup"]["default"] == "no" and inputs["confirm"]["default"] == ""
    assert wf["permissions"] == {"contents": "read", "actions": "read"}
    # a private group: a run waiting in the production-write group would evict a waiting Daily Sync
    # (track_performance.yml, 2026-10-06). The armed trim refuses instead while a production write is in flight.
    text = (ROOT / ".github" / "workflows" / "grid_trim.yml").read_text(encoding="utf-8")
    assert wf["concurrency"] == {"group": "tfb-grid-trim", "cancel-in-progress": False}
    assert "group: tadawul-production-write" not in text
    steps = wf["jobs"]["grid-trim"]["steps"]
    names = [s.get("name") for s in steps]
    guard = steps[names.index("Refuse to trim while a production write is in flight")]
    assert names.index("Refuse to trim while a production write is in flight") < names.index("Report or trim")
    assert guard["if"] == "${{ inputs.apply == 'true' }}" and guard["env"] == {"GH_TOKEN": "${{ github.token }}"}
    assert "for WF in daily_sync.yml page_refresh_recovery.yml grid_capacity_repair.yml; do" in guard["run"]
    assert guard["run"].count("exit 1") == 2 and "exit 0" not in guard["run"] and "inputs." not in guard["run"]   # fail-closed
    for status in ("in_progress", "queued", "waiting", "pending", "requested"):
        assert '.status=="%s"' % status in guard["run"], status
    from scripts import release_decision_feed as rdf
    assert tuple(rdf.INFLIGHT_WORKFLOWS) == ("daily_sync.yml", "page_refresh_recovery.yml", "grid_capacity_repair.yml")
    step = steps[names.index("Report or trim")]
    assert "if" not in step                                                  # the report always runs
    assert step["env"]["TFB_GRID_TRIM_APPLY"] == "${{ inputs.apply == 'true' && '1' || '0' }}"
    assert "${{" not in step["run"] and 'python scripts/trim_workbook_grid.py "${ARGS[@]}"' in step["run"]   # inputs reach the shell as env only
    # the tool reads values and deletes empty grid; it cannot write a cell, a status key or a run token
    src = (ROOT / "scripts" / "trim_workbook_grid.py").read_text(encoding="utf-8")
    assert src.isascii() and tg.__version__ == "1.0.0" and "v1.0.0 (2026-11-27)" in src
    code = src.replace("p.update(", "").replace("ov.update(", "").replace("extents.update(", "").replace("batch_update", "")
    for banned in ("append_row", "values_update", ".update(", "values_append", "del_worksheet", "deleteSheet",
                   "updateCells", "values_clear", "batch_clear", "add_worksheet", "GITHUB_RUN_ID", "_Status", "run="):
        assert banned not in code, banned
    assert src.count("self.sh.batch_update(") == 1 and src.count('"deleteDimension"') == 2
    pin = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("trim_workbook_grid", "1.0.0", "workbook grid trim")' in pin


# ----------------------------------------------------------------------------- G11 random workbooks
def test_g11_random_workbooks_keep_every_value_and_object():
    rnd = random.Random(20261127)
    trims = held = 0
    for case in range(400):
        tabs, charts, merges, names = {}, {}, {}, []
        for k in range(rnd.randint(1, 6)):
            name = "t%d_%d" % (case, k)
            rows = rnd.choice([5, 60, 400, 1000, 7000, 30000])
            cols = rnd.choice([1, 3, 12, 26, 61])
            cells = {}
            shape = rnd.choice(["empty", "table", "sparse", "page", "tail"])
            if shape == "table":
                h, w = rnd.randint(1, min(rows, 6500)), rnd.randint(1, cols)
                cells = {(r, c): 1 for r in (1, h) for c in range(1, w + 1)}
                cells.update({(r, 1): 1 for r in range(1, h + 1, max(1, h // 40))})
            elif shape == "sparse":
                for _ in range(rnd.randint(1, 8)):
                    cells[(rnd.randint(1, rows), rnd.randint(1, cols))] = "s"
            elif shape == "page":
                cells = {(1, 1): "T", (rnd.randint(1, min(rows, 200)), rnd.randint(1, cols)): "p"}
            elif shape == "tail":
                cells = {(rows, cols): "last cell of the grid"}
            fr = rnd.choice([0, 0, 1, 3]) if rows > 5 else 0
            fc = rnd.choice([0, 0, 1]) if cols > 2 else 0
            tabs[name] = (rows, cols, cells, fr, fc)
            if rnd.random() < 0.3:
                charts[name] = [(rnd.randint(1, rows), rnd.randint(1, cols), rnd.choice([300, 600]), rnd.choice([200, 371, 600]))
                                for _ in range(rnd.randint(1, 3))]
            if rnd.random() < 0.2:
                r1, c1 = rnd.randint(1, rows), rnd.randint(1, cols)
                merges[name] = [(r1, c1, rnd.randint(r1, min(rows, r1 + 3)), rnd.randint(c1, cols))]
            if rnd.random() < 0.15:
                r1, c1 = rnd.randint(1, rows), rnd.randint(1, cols)
                names.append((name, r1, c1, rnd.choice([None, rnd.randint(r1, rows)]), rnd.choice([None, rnd.randint(c1, cols)])))
        book = FakeBook(tabs, charts=charts, merges=merges, names=names)
        before_cells = {t: dict(d["cells"]) for t, d in book.tabs.items()}
        before_grid = {t: (d["rows"], d["cols"]) for t, d in book.tabs.items()}
        before = book.allocated()
        plan = tg.build_plan(book, log=QUIET)
        for p in plan:                                                       # the measured extent is the true one
            cells = before_cells[p["title"]]
            true = (max((r for r, _ in cells), default=0), max((c for _, c in cells), default=0))
            assert (p["used_rows"], p["used_cols"]) == true, (case, p["title"])
        freed, problems = tg.apply_plan(book, plan, QUIET, QUIET)           # FakeBook raises if a value or object would go
        assert problems == [], (case, problems)
        assert {t: d["cells"] for t, d in book.tabs.items()} == before_cells
        assert book.allocated() == before - freed
        for p in plan:
            d = book.tabs[p["title"]]
            need = book.needs(p["title"])
            assert (d["rows"], d["cols"]) == (p["to_rows"], p["to_cols"])
            assert d["rows"] <= before_grid[p["title"]][0] and d["cols"] <= before_grid[p["title"]][1]     # never grown
            assert d["rows"] >= max(p["used_rows"], need[0], d["fr"] + 1)
            assert d["cols"] >= max(p["used_cols"], need[1], d["fc"] + 1)
            if p["action"] == "TRIM":
                assert p["freed"] >= tg.MIN_GAIN
                trims += 1
                held += "held_rows" in p
    assert trims >= 250 and held >= 80, (trims, held)                        # the cases did exercise both paths
