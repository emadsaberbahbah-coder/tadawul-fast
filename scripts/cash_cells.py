#!/usr/bin/env python3
"""
scripts/cash_cells.py - the typed cash cells follow the broker
==============================================================
VERSION 1.0.0 (2026-11-13) - NEW SCRIPT

WHY (owner, Fri 13 Nov 2026 12:34 Riyadh, with the Consistency_Check rows:
"need to solve it"). C8 read MISMATCH: Portfolio_Decision's cash was the
broker's (the read-only IBKR capture), while the two typed cells still held
an older figure:
  Top_10_Investments   'Cash Available (SAR)'
  Portfolio_Decision   'PF: Cash Available SAR'
The main page sizes from its typed cell whenever no capture under 14 minutes
old is applied (its status line that morning: "funding evidence NOT APPLIED
(capture too old (2h9m))"), so a stale cell is cash the account does not hold.
The owner asked the same day that the plan follow the cash available at IBKR.

WHAT: reads the newest capture (scripts/ibkr_capture.py, read-only). When it
can be used - not older than TFB_PLAN_IBKR_MAX_AGE_H hours (30) and every
working buy order priced - the SAR the broker shows free for new buys
(scripts/daily_plan.capture_cash: the smaller of cash and settled cash, less
working buy orders, at the 3.75 peg, rounded down to a whole SAR) is written
into the value cell right of each label, when it differs by more than
TFB_CASH_CELLS_TOL_SAR (1). One cell per page; nothing else is touched. A
label that is not found, or a value cell that does not hold a number, is left
alone. Without a usable capture nothing is written.

The run log is public: no amount is printed, only which cell changed.

GATE: TFB_CASH_CELLS = off (default) | observe (print only) | on.
Rollback: repository variable TFB_CASH_CELLS=off (the cells keep their last
value and are typed by hand again).
"""
from __future__ import annotations

import argparse
import datetime as _dt
import math
import os
import sys
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.daily_plan import _col, _read, capture_cash, num, text  # noqa: E402

__version__ = "1.0.0"

UTC = _dt.timezone.utc
CELLS = (("Top_10_Investments", "Cash Available (SAR)"), ("Portfolio_Decision", "PF: Cash Available SAR"))
SCAN = "A1:AE15"                                                   # the control panels sit in the top rows


def _out(msg: str) -> None:
    print("[cash cells %s] %s" % (__version__, msg), flush=True)


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _env_num(name: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(_env(name, str(default)))
    except ValueError:
        return default
    return min(hi, max(lo, v)) if v == v else default


def mode() -> str:
    v = _env("TFB_CASH_CELLS", "off").lower()
    return v if v in ("off", "observe", "on") else "off"


# ----------------------------------------------------------------------------- #
# Pure
# ----------------------------------------------------------------------------- #
def find_cell(values: Sequence[Sequence[Any]], label: str) -> Optional[Tuple[int, int, Any]]:
    """(row, column, current value) of the cell right of the first cell that is exactly `label` (0-based)."""
    for i, r in enumerate(values):
        for j, c in enumerate(r):
            if text(c) == label:
                return i, j + 1, (r[j + 1] if j + 1 < len(r) else "")
    return None


def a1(r: int, c: int) -> str:
    return "%s%d" % (_col(c + 1), r + 1)


def decide(values: Sequence[Sequence[Any]], label: str, cash: float, tol: float) -> Tuple[str, str]:
    """(what to do, cell): 'write' | 'same' | 'no label' | 'not a number'. Only a cell that already holds a number
    is ever written, so a page whose layout changed is left alone."""
    hit = find_cell(values, label)
    if hit is None:
        return "no label", ""
    r, c, old = hit
    was = num(old)
    if was is None:
        return "not a number", a1(r, c)
    return ("same" if abs(was - cash) <= tol else "write"), a1(r, c)


def broker_cash(cap: Optional[Dict[str, Any]], max_age_h: float) -> Tuple[Optional[int], str]:
    """(whole SAR free for new buys, why not)."""
    sar, why = capture_cash(cap, max_age_h)
    if sar is None:
        return None, why
    return int(math.floor(sar + 1e-9)), ""


# ----------------------------------------------------------------------------- #
# IO
# ----------------------------------------------------------------------------- #
def run_once(dry_run: bool, book=None, now: Optional[_dt.datetime] = None, capture: Optional[Dict[str, Any]] = None,
             out: Callable[[str], None] = _out) -> int:
    m = mode()
    if m == "off":
        out("TFB_CASH_CELLS=off - nothing to do")
        return 0
    write = m == "on" and not dry_run
    now = now or _dt.datetime.now(UTC)
    if capture is None:
        from scripts import ibkr_capture  # noqa: PLC0415
        capture = ibkr_capture.latest(now, out=out)
    cash, why = broker_cash(capture, _env_num("TFB_PLAN_IBKR_MAX_AGE_H", 30, 0.25, 240))
    if cash is None:
        out("no broker cash to follow (%s) - the typed cells are left as they are" % why)
        return 0
    if book is None:
        from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
        try:
            book = _open_book()
        except Exception as e:  # noqa: BLE001
            out("ERROR: cannot open workbook: %s" % type(e).__name__)
            return 2
    tol = _env_num("TFB_CASH_CELLS_TOL_SAR", 1, 0, 1e6)
    bad = 0
    for tab, label in CELLS:
        try:
            what, cell = decide(_read(book, tab, SCAN), label, float(cash), tol)
            if what == "write" and write:
                book.worksheet(tab).update(values=[[cash]], range_name=cell, value_input_option="RAW")
        except Exception as e:  # noqa: BLE001
            bad += 1
            out("ERROR: %s: %s" % (tab, type(e).__name__))
            continue
        out("%s '%s': %s" % (tab, label, {
            "write": ("cell %s set to the broker's figure" if write else "cell %s differs from the broker's figure (not "
                      "written: %s)" % ("%s", "dry run" if dry_run else "observe")) % cell,
            "same": "already the broker's figure",
            "no label": "label not found - nothing written",
            "not a number": "cell %s does not hold a number - left alone" % cell}[what]))
    return 3 if bad else 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Write the broker's free cash into the two typed cash cells.")
    ap.add_argument("--dry-run", action="store_true", help="compute and print, write nothing")
    args = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
