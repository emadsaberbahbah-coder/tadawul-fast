#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/trim_workbook_grid.py
================================================================================
TFB workbook grid trim - the empty rows and columns Google counts, tab by tab
--------------------------------------------------------------------------------
v1.0.0 (2026-11-27)
  WHY: full project review of 27 Nov 2026 (owner, 14:06 Riyadh: "ok approved and start doing the need"). Google
       counts ALLOCATED cells - every tab's rows x columns, used or not - against a 20,000,000 limit. The workbook
       stood at 9,736,507 (48.7%) and grows about 100,000 a day; the owner's export of the same hour holds values in
       4,581,643 cells of used range. About half of the grid is empty: tabs left at the default 1,000 x 26, log tabs
       with 16 unused columns. scripts/repair_grid_capacity.py (August) trims the rows of six named tabs only, and
       its dry run of today found five of them already tight.
  WHAT, report (the default - nothing is written):
       every tab's grid, its used extent measured exactly, its target and the cells a trim would free; the totals;
       and the tabs the export audit calls dead or dated, listed as candidates to archive (this tool never deletes
       a tab).
  WHAT, apply (four gates, as the August tool: --apply, --i-have-backup, --confirm TRIM, env TFB_GRID_TRIM_APPLY=1):
       trailing EMPTY rows and columns are deleted, one tab at a time. Before a tab is touched, the region to delete
       is read again and must come back empty - a tab that gained a value since the plan is skipped.
  RULES (reductions only - a grid is never grown and no cell that holds a value or a formula is ever removed):
       used extent  the last row and the last column holding a value or a formula (read with FORMULA rendering, so a
                    formula that shows nothing still counts). Probe: row 1 and column A; then the two regions outside
                    the probe are read, and anything found there extends the extent.
       structure    charts and slicers (their anchor cell plus their size in pixels, counted at 15 px a row and 50 px
                    a column - deliberately small, so an object is taken to reach further than it does), merged
                    ranges and named ranges are read from the workbook metadata and held as used extent as well.
       rows         a tab with more than 5,000 used rows keeps used + 1,000; a smaller one keeps the larger of
                    2 x used and used + 300; never fewer than its frozen rows + 2.
       columns      only a tab with more than 5,000 used rows is narrowed, to used + 4 (never fewer than its frozen
                    columns + 1). Smaller tabs keep their columns: their readers and writers use fixed widths.
       gain         a tab is trimmed only when that frees at least 5,000 cells.
       never        the five pages the sync sizes itself (MANAGED_TABS), any tab named in --exclude, a sheet that
                    is not a grid, a tab whose extent could not be measured.
  LIMITS, stated: the API does not show an image placed over cells or a dropdown rule in an empty cell; one that
       sits below the kept margin goes with its rows. A formula whose range ends on a fixed row (A2:A1000) is
       shortened to the new last row; the owner's export of 27 Nov holds 674 formulas and none points at a tab this
       tool may trim with such a range. --exclude leaves any tab alone.
  LOGS: tab names and counts only - no cell value is printed.
  ROLLBACK: a deleted empty row or column holds nothing; any writer that needs more grid adds it (gspread add_rows /
       add_cols, Apps Script insertRowsAfter). To give a tab its room back by hand: Insert > Rows / Columns.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.0"

CELL_LIMIT = 20_000_000
BIG_ROWS = 5_000            # a tab with more used rows is a table that grows by appends
BIG_ROW_MARGIN = 1_000
SMALL_ROW_MARGIN = 300
COL_MARGIN = 4
MIN_GAIN = 5_000            # cells; a smaller gain is not worth touching a tab for
CHUNK = 20                  # tabs per batchGet (two ranges each)
PAUSE_S = 1.2               # between tabs while applying: under the Sheets quota of 60 reads and 60 writes a minute
QUOTA_WAIT_S = 65.0         # one wait and one more try when Google answers 429
OBJ_ROW_PX, OBJ_COL_PX = 15, 50          # a chart's reach in cells: smaller than the defaults (21, 100) on purpose
OBJ_DEFAULT_H, OBJ_DEFAULT_W = 371, 600  # the size the API gives a chart that states none
# the pages run_dashboard_sync sizes itself after every write (repair_grid_capacity's own map)
MANAGED_TABS = ("Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds", "My_Portfolio")
# scripts/tfb_export_audit.py DEAD_TABS (held equal by tests/test_trim_workbook_grid.py) ...
DEAD_TABS = ("_Diag_Response_Shape", "_Data_Audit", "Diagnostic_Report", "Sheet97", "Sheet88", "_SM_TEST",
             "_Shariah_Upload", "_SM_TEST_LIVE", "System_Logs")
# ... and the dated one-offs: a review, reconciliation or backup tab that carries its day in its name
DATED_TAB = re.compile(r"(?:^|_)(?:20\d{2}_\d{2}_\d{1,2}|20\d{6})$")

try:  # pragma: no cover - environment dependent
    import gspread  # type: ignore
    from google.oauth2 import service_account  # type: ignore
    GSPREAD_AVAILABLE = True
except Exception:  # pragma: no cover
    gspread = None  # type: ignore
    service_account = None  # type: ignore
    GSPREAD_AVAILABLE = False


def _out(msg: str) -> None:
    print("[grid_trim %s] %s" % (__version__, msg), flush=True)


# ----------------------------------------------------------------------------- pure: names and ranges
def col_letter(n: int) -> str:
    """1 -> A, 26 -> Z, 27 -> AA."""
    if n < 1:
        raise ValueError("column numbers start at 1")
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def quoted(title: str) -> str:
    return "'" + str(title).replace("'", "''") + "'"


def a1(title: str, r1: int, c1: int, r2: int, c2: int) -> str:
    return "%s!%s%d:%s%d" % (quoted(title), col_letter(c1), r1, col_letter(c2), r2)


def is_archive_candidate(title: str) -> bool:
    return title in DEAD_TABS or bool(DATED_TAB.search(title))


# ----------------------------------------------------------------------------- pure: metadata
def _int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:  # noqa: BLE001
        return default


def _list(v: Any) -> List[Any]:
    return v if isinstance(v, list) else []


def _range_end(rng: Any) -> Tuple[int, int]:
    """(rows, cols) a GridRange needs kept: its end where it has one, else its first row / column."""
    if not isinstance(rng, dict):
        return 0, 0
    r = _int(rng.get("endRowIndex")) if rng.get("endRowIndex") is not None else _int(rng.get("startRowIndex")) + 1
    c = (_int(rng.get("endColumnIndex")) if rng.get("endColumnIndex") is not None
         else _int(rng.get("startColumnIndex")) + 1)
    return max(r, 0), max(c, 0)


def _overlay_end(obj: Any) -> Tuple[int, int]:
    """(rows, cols) a chart or slicer over the cells reaches: anchor cell + size, at OBJ_ROW_PX / OBJ_COL_PX."""
    ov = ((obj or {}).get("position") or {}).get("overlayPosition") if isinstance(obj, dict) else None
    if not isinstance(ov, dict):
        return 0, 0                                             # a chart on its own sheet holds no cell
    cell = ov.get("anchorCell") or {}
    h = _int(ov.get("offsetYPixels")) + _int(ov.get("heightPixels"), OBJ_DEFAULT_H)
    w = _int(ov.get("offsetXPixels")) + _int(ov.get("widthPixels"), OBJ_DEFAULT_W)
    return _int(cell.get("rowIndex")) + -(-h // OBJ_ROW_PX), _int(cell.get("columnIndex")) + -(-w // OBJ_COL_PX)


def structure_extent(sheet: Any, named: Sequence[Any] = ()) -> Tuple[int, int]:
    """(rows, cols) the sheet's charts, slicers, merged ranges and named ranges reach. (0, 0) = none."""
    sh = sheet if isinstance(sheet, dict) else {}
    ends = [_overlay_end(o) for key in ("charts", "slicers") for o in _list(sh.get(key))]
    ends += [_range_end(m) for m in _list(sh.get("merges"))]
    ends += [_range_end(n) for n in named]
    return max((e[0] for e in ends), default=0), max((e[1] for e in ends), default=0)


def tabs_from_meta(meta: Any) -> List[Dict[str, Any]]:
    """One dict per GRID sheet: title, sheet_id, rows, cols, frozen_rows, frozen_cols, obj_rows, obj_cols.
    Malformed -> []."""
    out: List[Dict[str, Any]] = []
    sheets = meta.get("sheets") if isinstance(meta, dict) else None
    names = meta.get("namedRanges") if isinstance(meta, dict) else None
    by_sheet: Dict[int, List[Any]] = {}
    for n in names if isinstance(names, list) else []:
        rng = (n or {}).get("range") if isinstance(n, dict) else None
        if isinstance(rng, dict):
            by_sheet.setdefault(_int(rng.get("sheetId")), []).append(rng)
    for sh in sheets if isinstance(sheets, list) else []:
        p = (sh or {}).get("properties") or {}
        g = p.get("gridProperties") or {}
        if p.get("sheetType", "GRID") != "GRID":
            continue
        try:
            rows, cols = int(g["rowCount"]), int(g["columnCount"])
            sid = int(p["sheetId"])
            obj_r, obj_c = structure_extent(sh, by_sheet.get(sid, ()))
            out.append({"title": str(p["title"]), "sheet_id": sid, "rows": rows, "cols": cols,
                        "frozen_rows": int(g.get("frozenRowCount") or 0),
                        "frozen_cols": int(g.get("frozenColumnCount") or 0),
                        "obj_rows": min(obj_r, rows), "obj_cols": min(obj_c, cols)})
        except Exception:  # noqa: BLE001
            continue
    return out


# ----------------------------------------------------------------------------- pure: the used extent
def _height(values: Any) -> int:
    return len(values) if isinstance(values, list) else 0


def _width(values: Any) -> int:
    return max((len(r) for r in values if isinstance(r, list)), default=0) if isinstance(values, list) else 0


def probe_ranges(tab: Dict[str, Any]) -> List[str]:
    """Row 1 and column A of a tab."""
    t = tab["title"]
    return [a1(t, 1, 1, 1, tab["cols"]), a1(t, 1, 1, tab["rows"], 1)]


def probe_extent(row1: Any, col_a: Any) -> Tuple[int, int]:
    """(r0, c0): the last non-empty row of column A and the last non-empty column of row 1. The API drops trailing
    empty cells and rows, so the lengths are the extents."""
    return _height(col_a), _width(row1)


def verify_ranges(tab: Dict[str, Any], r0: int, c0: int) -> List[Optional[str]]:
    """[below, right]: every row under r0, and the columns right of c0 down to r0. None where there is none."""
    t, rows, cols = tab["title"], tab["rows"], tab["cols"]
    below = a1(t, r0 + 1, 1, rows, cols) if r0 < rows else None
    right = a1(t, 1, c0 + 1, r0, cols) if (c0 < cols and r0 >= 1) else None
    return [below, right]


def used_extent(r0: int, c0: int, below: Any, right: Any) -> Tuple[int, int]:
    """The exact used extent: the probe, extended by whatever the two regions outside it hold."""
    last_row = r0 + _height(below)
    last_col = max(c0, _width(below), (c0 + _width(right)) if _width(right) else 0)
    return last_row, last_col


# ----------------------------------------------------------------------------- pure: the plan
def target_rows(used_rows: int, frozen_rows: int) -> int:
    keep = used_rows + BIG_ROW_MARGIN if used_rows > BIG_ROWS else max(2 * used_rows, used_rows + SMALL_ROW_MARGIN)
    return max(keep, frozen_rows + 2, 2)


def target_cols(used_rows: int, used_cols: int, grid_cols: int, frozen_cols: int) -> int:
    if used_rows <= BIG_ROWS:
        return grid_cols                                        # a small tab keeps its columns
    return max(used_cols + COL_MARGIN, frozen_cols + 1, 1)


def plan_tab(tab: Dict[str, Any], extent: Optional[Tuple[int, int]], exclude: Sequence[str] = ()) -> Dict[str, Any]:
    """One planned row for a tab. Reductions only; everything else is a SKIP with its reason."""
    rows, cols = tab["rows"], tab["cols"]
    p: Dict[str, Any] = {"title": tab["title"], "sheet_id": tab["sheet_id"], "rows": rows, "cols": cols,
                         "allocated": rows * cols, "action": "SKIP", "freed": 0, "to_rows": rows, "to_cols": cols,
                         "archive_candidate": is_archive_candidate(tab["title"])}
    if tab["title"] in MANAGED_TABS:
        return dict(p, reason="sized by the sync")
    if tab["title"] in set(exclude):
        return dict(p, reason="excluded")
    if extent is None:
        return dict(p, reason="extent not measured")
    used_r, used_c = extent
    p.update(used_rows=used_r, used_cols=used_c, used=used_r * used_c)
    keep_r = max(used_r, min(int(tab.get("obj_rows") or 0), rows))          # values and structure together
    keep_c = max(used_c, min(int(tab.get("obj_cols") or 0), cols))
    if (keep_r, keep_c) != (used_r, used_c):
        p.update(held_rows=keep_r, held_cols=keep_c)
    to_r = min(rows, target_rows(keep_r, tab["frozen_rows"]))
    to_c = min(cols, target_cols(keep_r, keep_c, cols, tab["frozen_cols"]))
    if to_r < keep_r or to_c < keep_c:                          # cannot happen by the rules; never trim into data
        return dict(p, reason="target inside the data")
    freed = rows * cols - to_r * to_c
    if freed < MIN_GAIN:
        return dict(p, reason="gain under %s cells" % format(MIN_GAIN, ","))
    return dict(p, action="TRIM", to_rows=to_r, to_cols=to_c, freed=freed)


def delete_ranges(p: Dict[str, Any]) -> List[str]:
    """The regions a TRIM removes, as A1 ranges (read again before the delete: they must be empty)."""
    out = []
    if p["to_rows"] < p["rows"]:
        out.append(a1(p["title"], p["to_rows"] + 1, 1, p["rows"], p["cols"]))
    if p["to_cols"] < p["cols"]:
        out.append(a1(p["title"], 1, p["to_cols"] + 1, p["to_rows"], p["cols"]))
    return out


def delete_requests(p: Dict[str, Any]) -> List[Dict[str, Any]]:
    """deleteDimension requests for a TRIM: rows first (to the grid's end), then columns."""
    out = []
    if p["to_rows"] < p["rows"]:
        out.append({"deleteDimension": {"range": {"sheetId": p["sheet_id"], "dimension": "ROWS",
                                                  "startIndex": p["to_rows"], "endIndex": p["rows"]}}})
    if p["to_cols"] < p["cols"]:
        out.append({"deleteDimension": {"range": {"sheetId": p["sheet_id"], "dimension": "COLUMNS",
                                                  "startIndex": p["to_cols"], "endIndex": p["cols"]}}})
    return out


def gates_missing(apply_flag: bool, backup_flag: bool, confirm: str, env: Dict[str, str]) -> List[str]:
    """Empty list = every gate open."""
    missing = []
    if not apply_flag:
        missing.append("--apply")
    if not backup_flag:
        missing.append("--i-have-backup")
    if (confirm or "").strip() != "TRIM":
        missing.append("--confirm TRIM")
    if (env.get("TFB_GRID_TRIM_APPLY") or "").strip().lower() not in ("1", "true", "yes", "on"):
        missing.append("env TFB_GRID_TRIM_APPLY=1")
    return missing


def report_lines(plan: Sequence[Dict[str, Any]]) -> List[str]:
    before = sum(p["allocated"] for p in plan)
    freed = sum(p["freed"] for p in plan)
    used = sum(p.get("used", 0) for p in plan)
    out = ["plan: %d tab(s); allocated %s of %s (%.1f%%); used range %s cells"
           % (len(plan), format(before, ","), format(CELL_LIMIT, ","), 100.0 * before / CELL_LIMIT, format(used, ",")),
           "%-34s %15s %15s %15s %11s  %s" % ("tab", "grid", "used", "target", "freed", "note")]
    for p in sorted(plan, key=lambda x: (-x["freed"], -x["allocated"], x["title"])):
        grid = "%dx%d" % (p["rows"], p["cols"])
        usedx = "%dx%d" % (p["used_rows"], p["used_cols"]) if "used_rows" in p else "-"
        tgt = "%dx%d" % (p["to_rows"], p["to_cols"]) if p["action"] == "TRIM" else "-"
        note = ("TRIM" if p["action"] == "TRIM" else "skip: " + p.get("reason", ""))
        if "held_rows" in p:
            note += " | charts, merges or names reach %dx%d" % (p["held_rows"], p["held_cols"])
        if p["archive_candidate"]:
            note += " | archive candidate"
        out.append("%-34s %15s %15s %15s %11s  %s" % (p["title"][:34], grid, usedx, tgt, format(p["freed"], ","), note))
    after = before - freed
    out.append("freed %s cells in %d tab(s); allocated after: %s (%.1f%% of the limit)"
               % (format(freed, ","), sum(1 for p in plan if p["action"] == "TRIM"), format(after, ","),
                  100.0 * after / CELL_LIMIT))
    cand = [p for p in plan if p["archive_candidate"]]
    if cand:
        out.append("archive candidates (never deleted by this tool; the owner decides): %d tab(s), %s cells allocated: %s"
                   % (len(cand), format(sum(p["allocated"] for p in cand), ","), ", ".join(p["title"] for p in cand)))
    return out


# ----------------------------------------------------------------------------- the Google boundary
class GridClient:
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    def __init__(self, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self.sh = None

    @staticmethod
    def _creds():
        raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64")
               or os.getenv("GOOGLE_CREDENTIALS") or "").strip()
        if not raw:
            return None
        s = raw
        if not s.startswith("{"):
            try:
                dec = base64.b64decode(s).decode("utf-8", errors="replace").strip()
                if dec.startswith("{"):
                    s = dec
            except Exception:  # noqa: BLE001
                pass
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and service_account is not None:
                return service_account.Credentials.from_service_account_info(obj, scopes=GridClient.SCOPES)
        except Exception:  # noqa: BLE001
            return None
        return None

    def connect(self) -> None:  # pragma: no cover - network
        if not GSPREAD_AVAILABLE:
            raise RuntimeError("gspread/google-auth not installed")
        creds = self._creds()
        gc = gspread.authorize(creds) if creds else gspread.service_account()
        self.sh = gc.open_by_key(self.spreadsheet_id)

    def fetch_meta(self) -> Any:  # pragma: no cover - network
        return self.sh.fetch_sheet_metadata()

    def batch_get(self, ranges: Sequence[str]) -> List[Any]:  # pragma: no cover - network
        """The values of each range, in order ([] for an empty one). FORMULA rendering: a formula is a value."""
        got = self.sh.values_batch_get(ranges=list(ranges), params={"majorDimension": "ROWS",
                                                                    "valueRenderOption": "FORMULA"})
        return [(vr or {}).get("values") or [] for vr in (got or {}).get("valueRanges") or []]

    def batch_update(self, requests: Sequence[Dict[str, Any]]) -> None:  # pragma: no cover - network
        self.sh.batch_update({"requests": list(requests)})


# ----------------------------------------------------------------------------- measure, plan, apply
def _measure_chunk(client: Any, chunk: Sequence[Dict[str, Any]]) -> Dict[str, Tuple[int, int]]:
    """The exact extents of a few tabs in two reads: the probes, then the regions outside them. Raises on a read
    that fails or answers short."""
    ranges = [r for t in chunk for r in probe_ranges(t)]
    got = client.batch_get(ranges)
    if len(got) != len(ranges):
        raise RuntimeError("probe answered %d of %d ranges" % (len(got), len(ranges)))
    probes = {t["title"]: probe_extent(got[2 * k], got[2 * k + 1]) for k, t in enumerate(chunk)}
    asks: List[Tuple[str, int, str]] = []                        # (title, slot, range)
    for t in chunk:
        for slot, rng in enumerate(verify_ranges(t, *probes[t["title"]])):
            if rng:
                asks.append((t["title"], slot, rng))
    found: Dict[Tuple[str, int], Any] = {}
    if asks:
        ans = client.batch_get([x[2] for x in asks])
        if len(ans) != len(asks):
            raise RuntimeError("verify answered %d of %d ranges" % (len(ans), len(asks)))
        found = {(x[0], x[1]): v for x, v in zip(asks, ans)}
    return {t["title"]: used_extent(probes[t["title"]][0], probes[t["title"]][1],
                                    found.get((t["title"], 0)), found.get((t["title"], 1))) for t in chunk}


def measure(client: Any, tabs: Sequence[Dict[str, Any]], log: Callable[[str], None] = _out
            ) -> Dict[str, Optional[Tuple[int, int]]]:
    """{title: (used rows, used cols)} - None where the reads failed (the tab is then skipped, never guessed).
    A chunk whose read fails is read again tab by tab, so one heavy tab does not take its neighbours with it."""
    extents: Dict[str, Optional[Tuple[int, int]]] = {}
    todo = [t for t in tabs if t["title"] not in MANAGED_TABS]
    for i in range(0, len(todo), CHUNK):
        chunk = todo[i:i + CHUNK]
        try:
            extents.update(_measure_chunk(client, chunk))
            continue
        except Exception as e:  # noqa: BLE001
            log("measure: a read of %d tab(s) failed (%s) - reading them one by one" % (len(chunk), type(e).__name__))
        for t in chunk:
            try:
                extents.update(_measure_chunk(client, [t]))
            except Exception as e:  # noqa: BLE001 - this tab is skipped, the others still measured
                extents[t["title"]] = None
                log("measure failed for %s: %s" % (t["title"], type(e).__name__))
    return extents


def build_plan(client: Any, exclude: Sequence[str] = (), log: Callable[[str], None] = _out) -> List[Dict[str, Any]]:
    tabs = tabs_from_meta(client.fetch_meta())
    if not tabs:
        raise RuntimeError("no grid sheet in the metadata")
    extents = measure(client, tabs, log)
    return [plan_tab(t, extents.get(t["title"]), exclude) for t in tabs]


def _quota(e: BaseException) -> bool:
    txt = str(e)
    return "429" in txt or "RESOURCE_EXHAUSTED" in txt or "Quota exceeded" in txt


def _with_quota_wait(fn: Callable[[], Any], sleep: Callable[[float], None]) -> Any:
    """fn(); on a quota answer wait once and try once more. Any other error, or a second one, is raised."""
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        if not _quota(e):
            raise
        sleep(QUOTA_WAIT_S)
        return fn()


def apply_plan(client: Any, plan: Sequence[Dict[str, Any]], log: Callable[[str], None] = _out,
               sleep: Optional[Callable[[float], None]] = None) -> Tuple[int, List[str]]:
    """Trim tab by tab. -> (cells freed, problems). The grids are read again first: a tab whose grid changed since
    the plan is skipped, and so is one whose delete region is no longer empty."""
    sleep = sleep or time.sleep
    todo = [p for p in plan if p["action"] == "TRIM"]
    if not todo:
        return 0, []
    try:
        now = {t["title"]: (t["sheet_id"], t["rows"], t["cols"]) for t in tabs_from_meta(client.fetch_meta())}
    except Exception as e:  # noqa: BLE001 - nothing was written
        log("the grids could not be read again before the trim: %s - nothing written" % type(e).__name__)
        return 0, ["grids unreadable before the trim: %s" % type(e).__name__]
    freed, problems = 0, []
    for p in todo:
        try:
            if now.get(p["title"]) != (p["sheet_id"], p["rows"], p["cols"]):
                problems.append("%s: grid changed since the plan - skipped" % p["title"])
                log("skip %s: its grid changed since the plan" % p["title"])
            else:
                again = _with_quota_wait(lambda: client.batch_get(delete_ranges(p)), sleep)
                if len(again) != len(delete_ranges(p)) or any(_height(v) for v in again):
                    problems.append("%s: gained a value since the plan - skipped" % p["title"])
                    log("skip %s: its trim region is no longer empty" % p["title"])
                else:
                    _with_quota_wait(lambda: client.batch_update(delete_requests(p)), sleep)
                    freed += p["freed"]
                    log("trimmed %-34s %dx%d -> %dx%d  freed %s" % (p["title"][:34], p["rows"], p["cols"],
                                                                    p["to_rows"], p["to_cols"], format(p["freed"], ",")))
        except Exception as e:  # noqa: BLE001 - one tab never stops the others
            problems.append("%s: %s" % (p["title"], type(e).__name__))
            log("FAILED %s: %s" % (p["title"], type(e).__name__))
        sleep(PAUSE_S)
    return freed, problems


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB workbook grid trim (report by default)")
    ap.add_argument("--spreadsheet-id", default=os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("TFB_SPREADSHEET_ID") or "")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--i-have-backup", action="store_true")
    ap.add_argument("--confirm", default="")
    ap.add_argument("--exclude", default="", help="tab names to leave alone, comma separated")
    a = ap.parse_args(list(argv) if argv is not None else None)
    if not a.spreadsheet_id:
        _out("ERROR: no spreadsheet id (DEFAULT_SPREADSHEET_ID)")
        return 2
    exclude = [x.strip() for x in a.exclude.split(",") if x.strip()]
    wants_apply = a.apply or a.i_have_backup or bool(a.confirm.strip())
    missing = gates_missing(a.apply, a.i_have_backup, a.confirm, dict(os.environ)) if wants_apply else []
    if wants_apply and missing:
        _out("APPLY REFUSED - missing: " + "; ".join(missing))
        return 2
    client = GridClient(a.spreadsheet_id)
    try:
        client.connect()
        plan = build_plan(client, exclude)
    except Exception as e:  # noqa: BLE001
        _out("ERROR: the workbook could not be read: %s" % type(e).__name__)
        return 1
    for line in report_lines(plan):
        _out(line)
    if not wants_apply:
        _out("REPORT ONLY - nothing written. To trim: --apply --i-have-backup --confirm TRIM + env TFB_GRID_TRIM_APPLY=1")
        return 0
    freed, problems = apply_plan(client, plan)
    try:
        after = sum(t["rows"] * t["cols"] for t in tabs_from_meta(client.fetch_meta()))
    except Exception:  # noqa: BLE001
        after = None
    before = sum(p["allocated"] for p in plan)
    _out("applied: freed %s cells; allocated before %s, after %s; %d problem(s)%s"
         % (format(freed, ","), format(before, ","), "unknown" if after is None else format(after, ","),
            len(problems), (": " + "; ".join(problems[:8])) if problems else ""))
    if after is not None and after != before - freed:
        _out("NOTE: the metadata reads %s, the plan expected %s (a writer resized a tab meanwhile)"
             % (format(after, ","), format(before - freed, ",")))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
