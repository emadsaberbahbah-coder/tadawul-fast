#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
repair_grid_capacity.py — v1.0.0 (IR-078 / audit P0-1, morning review 2026-08-23)
================================================================================

WHY THIS TOOL EXISTS
--------------------
The live workbook is at 9,999,977 of Google Sheets' 10,000,000-cell allocation
limit (23 cells of headroom): _Run_Log, Performance_Log and Signal_History
appends fail HTTP 400 while jobs stay green, freezing the S-1 evidence clock.
The 2026-08-23 independent review proved the column trim (115 cols) did NOT
recover capacity because ALLOCATED ROWS are the real consumer — e.g.
My_Portfolio carries 9,905 allocated rows x 122 cols for 5 data rows.

The fix is grid-ROW reduction on six tabs. Doing ~28,000 row deletions by hand
across six tabs is exactly the class of destructive hand-work this project
bans, so this tool does it the house way:

  * REPORT-FIRST / DRY-RUN BY DEFAULT — running with no flags only prints the
    per-tab plan (current grid, data extent, effective target, freed cells,
    projected after-total, PASS/FAIL vs the 7,000,000 closure bar). Nothing
    is written.
  * TRIPLE-GATED APPLY — a mutation requires ALL of:
        --apply  AND  --i-have-backup  AND  --confirm RESIZE
        AND env TFB_GRID_REPAIR_APPLY=1
    Any missing gate aborts with exit 2 and a list of what was missing.
  * ARCHIVE BEFORE PRUNE — _Run_Log is dumped IN FULL to a CSV artifact
    before a single row is touched. clearContents does not release
    allocation; only row deletion does — and deletion without an archive
    would destroy tripwire evidence.
  * NEWEST-KEPT PRUNE — _Run_Log keeps the header plus the NEWEST
    (target-1) data rows; the deletion span is computed against the actual
    data extent, never assumed.
  * REDUCTIONS ONLY — a tab whose current grid is already at/below target is
    SKIPPED. This tool can never grow a grid, never touches columns, and
    never touches a tab outside its explicit target map.
  * DATA-EXTENT GUARD — every effective target is lifted to at least
    (last data row + MIN_BUFFER) and (frozen rows + 2), so a resize can
    never truncate data even if the target map is wrong.
  * CLOSURE TEST BUILT IN — after apply, the tool re-reads metadata and
    attempts ONE test append to _Run_Log. Exit 0 requires BOTH
    after-total < 7,000,000 AND the append landing; anything else exits 1.

DEFAULT TARGET MAP (2026-08-23 review, adjudicated)
---------------------------------------------------
    My_Portfolio      -> 250      Commodities_FX -> 1,000
    Mutual_Funds      -> 3,000    Market_Leaders -> 500
    Global_Markets    -> 7,000    _Run_Log       -> 5,000 (archive+prune)

Expected recovery on the review's metadata: 3,780,881 cells
(9,999,977 -> ~6,219,096). The planner in this file reproduces that table
cell-for-cell in scripts/harness_grid_repair_100.py.

ENVIRONMENT
-----------
    GOOGLE_SHEETS_CREDENTIALS[_B64] / GOOGLE_CREDENTIALS   service account
    DEFAULT_SPREADSHEET_ID | TFB_SPREADSHEET_ID            workbook id
    TFB_GRID_REPAIR_APPLY                                  "1" to allow apply
    TFB_GRID_REPAIR_MIN_BUFFER                             default 25

This tool never reads or writes any tab other than: metadata (all tabs,
read-only), column A of targeted tabs (extent probe), the full _Run_Log
(archive read), and the mutations listed in the printed plan.
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

TOOL_VERSION = "1.0.0"
CELL_LIMIT = 10_000_000
CLOSURE_BAR = 7_000_000
RUN_LOG_TAB = "_Run_Log"
DEFAULT_MIN_BUFFER = 25

DEFAULT_TARGETS: Dict[str, int] = {
    "My_Portfolio": 250,
    "Commodities_FX": 1_000,
    "Mutual_Funds": 3_000,
    "Market_Leaders": 500,
    "Global_Markets": 7_000,
    RUN_LOG_TAB: 5_000,
}

# --------------------------------------------------------------------------- #
# Optional imports (SAFE — report logic must run without them)                #
# --------------------------------------------------------------------------- #
try:  # pragma: no cover - environment dependent
    import gspread  # type: ignore
    from google.oauth2 import service_account  # type: ignore
    GSPREAD_AVAILABLE = True
except Exception:  # pragma: no cover
    gspread = None  # type: ignore
    service_account = None  # type: ignore
    GSPREAD_AVAILABLE = False


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------- #
# PURE planners (harness-tested; no I/O)                                      #
# --------------------------------------------------------------------------- #
def _allocated_from_meta(meta: Any) -> Tuple[Optional[int], List[Dict[str, Any]]]:
    """Total ALLOCATED cells + per-tab inventory from fetch_sheet_metadata().
    Allocation (rowCount x columnCount) is what Google's 10M limit counts.
    Returns (None, []) on malformed input — callers fail loud, not wrong."""
    tabs: List[Dict[str, Any]] = []
    try:
        sheets = meta.get("sheets") if isinstance(meta, dict) else None
        if not isinstance(sheets, list) or not sheets:
            return None, []
        total = 0
        for sh in sheets:
            try:
                props = sh["properties"]
                gp = props["gridProperties"]
                rows = int(gp["rowCount"])
                cols = int(gp["columnCount"])
                tabs.append({
                    "title": str(props.get("title", "?")),
                    "sheet_id": int(props.get("sheetId", -1)),
                    "rows": rows,
                    "cols": cols,
                    "cells": rows * cols,
                    "frozen": int(gp.get("frozenRowCount", 0) or 0),
                })
                total += rows * cols
            except Exception:
                continue
        return (total if tabs else None), tabs
    except Exception:
        return None, []


def _effective_target(requested: int, data_rows: int, frozen: int,
                      min_buffer: int) -> int:
    """The guard that makes a wrong target map harmless: never below the
    data extent plus buffer, never below frozen+2, never below 2."""
    return max(int(requested), int(data_rows) + int(min_buffer),
               int(frozen) + 2, 2)


def _runlog_prune_span(data_rows: int, keep_newest: int) -> Optional[Tuple[int, int]]:
    """0-based half-open row span to DELETE from _Run_Log so that the header
    (sheet row 1) plus the NEWEST `keep_newest` data rows survive.

    Data occupies sheet rows 2..(1+data_rows). Keeping the newest K means
    deleting sheet rows 2..(1+data_rows-K), i.e. dims [1, 1+data_rows-K).
    Returns None when there is nothing to prune (data_rows <= keep_newest)."""
    d, k = int(data_rows), int(keep_newest)
    if d <= k or d <= 0 or k < 0:
        return None
    return (1, 1 + (d - k))


def _delete_rows_request(sheet_id: int, start_idx: int, end_idx: int) -> Dict[str, Any]:
    return {"deleteDimension": {"range": {
        "sheetId": int(sheet_id), "dimension": "ROWS",
        "startIndex": int(start_idx), "endIndex": int(end_idx)}}}


def _plan(tabs: List[Dict[str, Any]], targets: Dict[str, int],
          extents: Dict[str, int], min_buffer: int) -> List[Dict[str, Any]]:
    """One planned row per TARGETED tab. Reductions only; everything else is
    an explicit SKIP with a reason. `extents` maps title -> last data row
    (column-A probe); a missing extent is treated as unknown and the tab is
    SKIPPED — this tool never resizes what it could not measure."""
    by_title = {t["title"]: t for t in tabs}
    out: List[Dict[str, Any]] = []
    for title, requested in targets.items():
        t = by_title.get(title)
        if t is None:
            out.append({"title": title, "action": "SKIP", "reason": "tab absent",
                        "freed": 0})
            continue
        if title not in extents or extents[title] is None:
            out.append({"title": title, "action": "SKIP",
                        "reason": "data extent unknown", "freed": 0,
                        "rows": t["rows"], "cols": t["cols"]})
            continue
        data_rows = max(0, int(extents[title]) - 1)  # extent includes header
        if title == RUN_LOG_TAB:
            # v1.0.0 harness finding H5 (source defect, fixed pre-ship): the
            # generic data-extent guard must NOT see the CURRENT extent here —
            # the prune runs first, so the grid target is guarded against the
            # POST-prune extent (header + newest keep rows). Guarding on the
            # current 30,695 data rows lifted the target to 30,720 and made
            # the tool SKIP its own primary mission.
            keep = max(int(requested) - 1, 1)
            span = _runlog_prune_span(data_rows, keep)
            post_data = keep if span is not None else data_rows
            eff = max(int(requested), post_data + 1, t["frozen"] + 2, 2)
            if span is None and t["rows"] <= eff:
                out.append({"title": title, "action": "SKIP",
                            "reason": f"grid {t['rows']} <= target {eff}, "
                                      "nothing to prune",
                            "rows": t["rows"], "cols": t["cols"], "freed": 0})
                continue
            out.append({
                "title": title, "action": "ARCHIVE+PRUNE+RESIZE",
                "sheet_id": t["sheet_id"], "rows": t["rows"],
                "cols": t["cols"], "data_rows": data_rows,
                "requested": int(requested), "effective": eff,
                "keep_newest": keep, "prune_span": span,
                # deleteDimension itself releases the pruned rows' allocation;
                # the trailing resize is an idempotent belt. Freed accounting
                # is therefore simply current-grid minus final-grid.
                "freed": max(0, (t["rows"] - eff)) * t["cols"],
            })
            continue
        eff = _effective_target(requested, data_rows, t["frozen"], min_buffer)
        if t["rows"] <= eff:
            out.append({"title": title, "action": "SKIP",
                        "reason": f"grid {t['rows']} <= target {eff}",
                        "rows": t["rows"], "cols": t["cols"], "freed": 0})
            continue
        out.append({
            "title": title, "action": "RESIZE", "sheet_id": t["sheet_id"],
            "rows": t["rows"], "cols": t["cols"], "data_rows": data_rows,
            "requested": int(requested), "effective": eff,
            "freed": (t["rows"] - eff) * t["cols"],
        })
    return out


def _fmt_report(plan: List[Dict[str, Any]], before_total: Optional[int]) -> str:
    freed = sum(int(p.get("freed", 0)) for p in plan)
    after = (before_total - freed) if isinstance(before_total, int) else None
    lines = [
        f"[GRID-REPAIR v{TOOL_VERSION}] plan @ {_now_utc()}",
        f"  allocated before : {before_total:,}" if isinstance(before_total, int)
        else "  allocated before : UNKNOWN (metadata unreadable)",
        f"  cell limit       : {CELL_LIMIT:,}   closure bar: <{CLOSURE_BAR:,}",
        "  " + "-" * 76,
    ]
    for p in plan:
        if p["action"].startswith("SKIP"):
            lines.append(f"  SKIP    {p['title']:<16} — {p.get('reason','')}")
            continue
        extra = ""
        if p.get("prune_span") is not None:
            s, e = p["prune_span"]
            extra = f"  prune rows[{s},{e}) keep newest {p['keep_newest']:,}"
        elif p["title"] == RUN_LOG_TAB:
            extra = "  (no prune needed)"
        lines.append(
            f"  {p['action']:<7} {p['title']:<16} "
            f"{p['rows']:>6,}x{p['cols']:<3} -> {p['effective']:>6,}x{p['cols']:<3}"
            f"  data={p['data_rows']:,}  freed={p['freed']:,}{extra}")
    lines.append("  " + "-" * 76)
    lines.append(f"  freed total      : {freed:,}")
    if after is not None:
        verdict = "PASS" if after < CLOSURE_BAR else "FAIL"
        lines.append(f"  projected after  : {after:,}  -> closure {verdict}")
    return "\n".join(lines)


def _apply_gates_missing(apply_flag: bool, backup_flag: bool, confirm: str,
                         env: Dict[str, str]) -> List[str]:
    """Pure gate check. Empty list == all gates open."""
    missing: List[str] = []
    if not apply_flag:
        missing.append("--apply")
    if not backup_flag:
        missing.append("--i-have-backup")
    if (confirm or "").strip() != "RESIZE":
        missing.append("--confirm RESIZE")
    if (env.get("TFB_GRID_REPAIR_APPLY") or "").strip().lower() not in (
            "1", "true", "yes", "on"):
        missing.append("env TFB_GRID_REPAIR_APPLY=1")
    return missing


# --------------------------------------------------------------------------- #
# Google boundary (thin; everything above is pure)                            #
# --------------------------------------------------------------------------- #
class GridClient:
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    def __init__(self, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self.gc = None
        self.sh = None

    # Credential shape mirrors scripts/track_performance.py
    # _load_sa_credentials_best_effort (JSON or base64(JSON)).
    def _creds(self):
        raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS")
               or os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64")
               or os.getenv("GOOGLE_CREDENTIALS") or "").strip()
        if not raw:
            return None
        s = raw
        if not s.startswith("{"):
            try:
                dec = base64.b64decode(s).decode("utf-8", errors="replace").strip()
                if dec.startswith("{"):
                    s = dec
            except Exception:
                pass
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and service_account is not None:
                return service_account.Credentials.from_service_account_info(
                    obj, scopes=self.SCOPES)
        except Exception:
            return None
        return None

    def connect(self) -> None:
        if not GSPREAD_AVAILABLE:
            raise RuntimeError("gspread/google-auth not installed")
        creds = self._creds()
        self.gc = gspread.authorize(creds) if creds else gspread.service_account()
        self.sh = self.gc.open_by_key(self.spreadsheet_id)

    def fetch_meta(self) -> Any:
        return self.sh.fetch_sheet_metadata()

    def col_a_extent(self, title: str) -> Optional[int]:
        """Last non-empty row in column A (1-based, header included)."""
        try:
            return len(self.sh.worksheet(title).col_values(1))
        except Exception:
            return None

    def dump_tab_csv(self, title: str, path: str) -> int:
        ws = self.sh.worksheet(title)
        values = ws.get_all_values()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerows(values)
        return len(values)

    def delete_rows(self, sheet_id: int, start_idx: int, end_idx: int) -> None:
        self.sh.batch_update(
            {"requests": [_delete_rows_request(sheet_id, start_idx, end_idx)]})

    def resize_rows(self, title: str, rows: int) -> None:
        self.sh.worksheet(title).resize(rows=rows)

    def append_test_line(self, before_total: Any, after_total: Any) -> None:
        self.sh.worksheet(RUN_LOG_TAB).append_row(
            [datetime.now().strftime("%m/%d/%Y %H:%M:%S"), "INFO",
             f"grid_capacity_repair v{TOOL_VERSION}", RUN_LOG_TAB, "OK",
             f"capacity restored: before={before_total} after={after_total}",
             "", "", "", json.dumps({"tool": "repair_grid_capacity",
                                     "version": TOOL_VERSION})],
            value_input_option="RAW")


# --------------------------------------------------------------------------- #
# Execution (boundary calls only through the client — harness uses a double)  #
# --------------------------------------------------------------------------- #
def _execute_plan(client: Any, plan: List[Dict[str, Any]], artifacts_dir: str,
                  skip_archive: bool, log=print) -> List[str]:
    """Runs the destructive steps IN ORDER: _Run_Log archive -> prune ->
    resize, then the remaining resizes. Per-tab failures are recorded and the
    remaining tabs still run; the caller decides exit semantics. Returns the
    error list (empty == clean)."""
    errors: List[str] = []
    ordered = ([p for p in plan if p["title"] == RUN_LOG_TAB]
               + [p for p in plan if p["title"] != RUN_LOG_TAB])
    for p in ordered:
        if not p["action"].startswith(("RESIZE", "ARCHIVE")):
            continue
        title = p["title"]
        try:
            if p["action"].startswith("ARCHIVE"):
                if skip_archive:
                    log(f"  !! --skip-archive set — NOT archiving {title} "
                        "(discouraged; evidence leaves the sheet unpreserved)")
                else:
                    path = os.path.join(
                        artifacts_dir,
                        f"run_log_archive_{datetime.now():%Y%m%d_%H%M%S}.csv")
                    n = client.dump_tab_csv(title, path)
                    log(f"  archived {title}: {n:,} rows -> {path}")
                span = p.get("prune_span")
                if span is not None:
                    s, e = span
                    client.delete_rows(p["sheet_id"], s, e)
                    log(f"  pruned {title}: deleted rows[{s},{e}) "
                        f"({e - s:,} rows), kept newest {p['keep_newest']:,}")
            client.resize_rows(title, p["effective"])
            log(f"  resized {title}: {p['rows']:,} -> {p['effective']:,} rows "
                f"(freed {p['freed']:,} cells)")
        except Exception as exc:  # keep going; fail loud at the end
            errors.append(f"{title}: {exc}")
            log(f"  !! {title} FAILED: {exc}")
    return errors


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--spreadsheet-id",
                    default=(os.getenv("DEFAULT_SPREADSHEET_ID")
                             or os.getenv("TFB_SPREADSHEET_ID")
                             or os.getenv("SPREADSHEET_ID") or ""))
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--i-have-backup", action="store_true")
    ap.add_argument("--confirm", default="")
    ap.add_argument("--targets-json", default="",
                    help="optional override map {tab: rows}")
    ap.add_argument("--artifacts-dir", default="artifacts")
    ap.add_argument("--skip-archive", action="store_true")
    ap.add_argument("--min-buffer", type=int,
                    default=int(os.getenv("TFB_GRID_REPAIR_MIN_BUFFER")
                                or DEFAULT_MIN_BUFFER))
    args = ap.parse_args(argv)

    if not args.spreadsheet_id:
        print("FATAL: no spreadsheet id (arg or DEFAULT_SPREADSHEET_ID)")
        return 2
    targets = dict(DEFAULT_TARGETS)
    if args.targets_json.strip():
        try:
            override = json.loads(args.targets_json)
            assert isinstance(override, dict)
            targets = {str(k): int(v) for k, v in override.items()}
        except Exception as exc:
            print(f"FATAL: --targets-json unparseable: {exc}")
            return 2

    client = GridClient(args.spreadsheet_id)
    try:
        client.connect()
    except Exception as exc:
        print(f"FATAL: connect failed: {exc}")
        return 2

    meta = client.fetch_meta()
    before_total, tabs = _allocated_from_meta(meta)
    extents = {t: client.col_a_extent(t) for t in targets}
    plan = _plan(tabs, targets, extents, args.min_buffer)
    report = _fmt_report(plan, before_total)
    print(report)
    os.makedirs(args.artifacts_dir, exist_ok=True)
    with open(os.path.join(args.artifacts_dir, "grid_repair_report.txt"),
              "w", encoding="utf-8") as fh:
        fh.write(report + "\n")

    if not args.apply:
        print(f"\nDRY-RUN ONLY — nothing written. To execute: --apply "
              f"--i-have-backup --confirm RESIZE  + env TFB_GRID_REPAIR_APPLY=1")
        return 0

    missing = _apply_gates_missing(args.apply, args.i_have_backup,
                                   args.confirm, dict(os.environ))
    if missing:
        print("\nAPPLY REFUSED — missing gate(s): " + ", ".join(missing))
        return 2

    print("\nAPPLYING (reductions only, _Run_Log archived first):")
    errors = _execute_plan(client, plan, args.artifacts_dir, args.skip_archive)

    after_meta = client.fetch_meta()
    after_total, _ = _allocated_from_meta(after_meta)
    print(f"\n[GRID-REPAIR v{TOOL_VERSION}] allocated before="
          f"{before_total if before_total is not None else '?'} "
          f"after={after_total if after_total is not None else '?'}")

    append_ok = False
    try:
        client.append_test_line(before_total, after_total)
        append_ok = True
        print("  test append to _Run_Log: OK — the evidence clock is unfrozen")
    except Exception as exc:
        print(f"  test append to _Run_Log: FAILED — {exc}")

    ok = (not errors and append_ok and isinstance(after_total, int)
          and after_total < CLOSURE_BAR)
    if errors:
        print("  errors: " + " | ".join(errors))
    print(f"  CLOSURE: {'PASS' if ok else 'FAIL'} "
          f"(bar: after<{CLOSURE_BAR:,} AND append OK AND zero errors)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
