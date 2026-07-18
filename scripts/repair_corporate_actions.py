"""
scripts/repair_corporate_actions.py — Performance_Log CA sweep & repair
========================================================================
VERSION 1.0.0  (2026-07-18)  — NEW SCRIPT (Wave A0, deliverable #6b)

WHY: see core/corporate_actions.py v1.0.0 (the pure library this tool drives).
The first 1M cohort matures 2026-07-28; anchors must be split-safe before then.

GOVERNANCE (hard rules):
  * DRY-RUN by default. Nothing is written without --apply.
  * Only CONFIRMED actions (Source != AUTO_DETECT) are ever applied.
  * Detection writes PROPOSAL rows (Source=AUTO_DETECT) to `_Corporate_Actions`
    for the operator to confirm by editing Source — never self-confirming.
  * Idempotent: records already tagged `ca_adjusted` in Notes are skipped.

USAGE:
  --selftest                       offline fixtures, no network
  --scan            (default)     dry-run: print plan + proposals
  --apply                         execute the confirmed plan + write proposals
  --sheet-id ID                   override spreadsheet id

ENV: TARGET_SHEET_ID | TRACK_SHEET_ID | DEFAULT_SPREADSHEET_ID ;
     GOOGLE_APPLICATION_CREDENTIALS | GOOGLE_SHEETS_CREDENTIALS(_B64)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import tempfile
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Allow both repo-root and scripts/ execution
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core import corporate_actions as ca  # noqa: E402

SCRIPT_VERSION = "1.0.0"
PL_TAB = "Performance_Log"


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------------------------------- #
# pure planning core (unit-tested offline)                                     #
# --------------------------------------------------------------------------- #
def locate_header(values: Sequence[Sequence[Any]]) -> Optional[int]:
    for i, row in enumerate(values[:10]):
        if row and str(row[0]).strip() == "Record ID":
            return i
    return None


def _col(hdr: List[str], name: str) -> Optional[int]:
    try:
        return hdr.index(name)
    except ValueError:
        return None


def plan_repairs(values: Sequence[Sequence[Any]],
                 index: Dict[str, List[Tuple[date, float]]]
                 ) -> Tuple[List[Dict[str, Any]], Optional[int], Dict[str, int]]:
    """-> (plan rows, header_row_index, column map). Plan entries carry the
    sheet row number (1-based) plus old/new Entry & Target prices and factor."""
    hdr_i = locate_header(values)
    if hdr_i is None:
        return [], None, {}
    hdr = [str(h).strip() for h in values[hdr_i]]
    cols = {k: _col(hdr, k) for k in
            ("Record ID", "Symbol", "Date Recorded (Riyadh)", "Entry Price",
             "Target Price", "Status", "Current Price", "Notes")}
    plan: List[Dict[str, Any]] = []
    for r_i in range(hdr_i + 1, len(values)):
        row = values[r_i]
        sym = str(row[cols["Symbol"]]).strip().upper() \
            if cols["Symbol"] is not None and len(row) > cols["Symbol"] else ""
        if not sym:
            continue
        notes = str(row[cols["Notes"]]) if cols["Notes"] is not None \
            and len(row) > cols["Notes"] else ""
        if ca.APPLIED_TAG in notes:
            continue
        d = ca._as_date(row[cols["Date Recorded (Riyadh)"]]
                        if cols["Date Recorded (Riyadh)"] is not None
                        and len(row) > cols["Date Recorded (Riyadh)"] else None)
        f = ca.cumulative_factor(sym, d, index)
        if f == 1.0:
            continue
        ep = ca._as_float(row[cols["Entry Price"]]) \
            if cols["Entry Price"] is not None else None
        tp = ca._as_float(row[cols["Target Price"]]) \
            if cols["Target Price"] is not None else None
        if ep is None:
            continue
        plan.append({
            "sheet_row": r_i + 1, "symbol": sym, "date": str(d or ""),
            "factor": f,
            "entry_old": ep, "entry_new": round(ep / f, 6),
            "target_old": tp,
            "target_new": (round(tp / f, 6) if tp is not None else None),
            "notes_old": notes,
        })
    return plan, hdr_i, cols


def detect_proposals(values: Sequence[Sequence[Any]],
                     index: Dict[str, List[Tuple[date, float]]],
                     existing_actions: Sequence[ca.Action]
                     ) -> List[List[str]]:
    """Active records whose entry/current ratio matches a canonical split and
    which have NO logged action -> proposal rows for `_Corporate_Actions`."""
    hdr_i = locate_header(values)
    if hdr_i is None:
        return []
    hdr = [str(h).strip() for h in values[hdr_i]]
    cols = {k: _col(hdr, k) for k in
            ("Symbol", "Date Recorded (Riyadh)", "Entry Price",
             "Current Price", "Status", "Notes")}
    logged = {a.symbol for a in existing_actions}
    seen: Dict[str, List[str]] = {}
    for r_i in range(hdr_i + 1, len(values)):
        row = values[r_i]
        def g(key: str) -> Any:
            c = cols.get(key)
            return row[c] if c is not None and len(row) > c else None
        sym = str(g("Symbol") or "").strip().upper()
        if not sym or sym in logged or sym in seen:
            continue
        if ca.APPLIED_TAG in str(g("Notes") or ""):
            continue  # terminal: already repaired — never re-proposed
        if str(g("Status") or "").strip().lower() != "active":
            continue
        d = ca._as_date(g("Date Recorded (Riyadh)"))
        ep_adj = ca.adjust_price(sym, ca._as_float(g("Entry Price")), d, index)
        hit = ca.detect_split_candidate(ep_adj, ca._as_float(g("Current Price")))
        if hit:
            atype, k = hit
            seen[sym] = [sym, atype, str(date.today()),
                         str(int(k) if float(k).is_integer() else k),
                         "", "", ca.SOURCE_AUTO, _now_utc(),
                         f"auto-detected from Performance_Log row {r_i + 1}; "
                         f"CONFIRM by editing Source before any repair"]
    return list(seen.values())


# --------------------------------------------------------------------------- #
# sheets I/O                                                                   #
# --------------------------------------------------------------------------- #
def _sheet_id(cli: Optional[str]) -> str:
    for v in (cli, os.getenv("TARGET_SHEET_ID"), os.getenv("TRACK_SHEET_ID"),
              os.getenv("DEFAULT_SPREADSHEET_ID")):
        if v and str(v).strip():
            return str(v).strip()
    raise SystemExit("::error::Spreadsheet ID missing.")


def _open_sheet(cli: Optional[str]):
    import gspread
    from google.oauth2.service_account import Credentials
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    raw, b64 = os.getenv("GOOGLE_SHEETS_CREDENTIALS"), os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64")
    if not path and (raw or b64):
        data = raw or base64.b64decode(b64).decode("utf-8")
        f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        f.write(data); f.close(); path = f.name
    if not path:
        raise SystemExit("::error::Google credentials missing.")
    creds = Credentials.from_service_account_file(
        path, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds).open_by_key(_sheet_id(cli))


def _col_letter(idx0: int) -> str:
    s, n = "", idx0 + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--sheet-id")
    args = ap.parse_args(argv)
    if args.selftest:
        return _selftest()

    sh = _open_sheet(args.sheet_id)

    # actions tab (create with header if missing)
    try:
        ws_a = sh.worksheet(ca.TAB_ACTIONS)
    except Exception:
        ws_a = sh.add_worksheet(title=ca.TAB_ACTIONS, rows=200,
                                cols=len(ca.ACTIONS_HEADER))
        ws_a.update(values=[ca.ACTIONS_HEADER], range_name="A1")
    actions = ca.parse_actions(ws_a.get_all_values())
    index = ca.build_adjustment_index(actions, confirmed_only=True)

    ws_pl = sh.worksheet(PL_TAB)
    values = ws_pl.get_all_values()
    plan, hdr_i, cols = plan_repairs(values, index)
    proposals = detect_proposals(values, index, actions)

    print(f"[CA-REPAIR v{SCRIPT_VERSION}] confirmed_actions="
          f"{sum(1 for a in actions if a.confirmed)} plan={len(plan)} "
          f"proposals={len(proposals)} mode={'APPLY' if args.apply else 'DRY-RUN'}")
    for p in plan[:15]:
        print(f"  row {p['sheet_row']:>5} {p['symbol']:<10} f={p['factor']:g} "
              f"entry {p['entry_old']} -> {p['entry_new']}")
    for pr in proposals[:15]:
        print(f"  PROPOSAL {pr[0]:<10} {pr[1]} ratio={pr[3]}")

    if args.apply:
        updates = []
        for p in plan:
            r = p["sheet_row"]
            updates.append({"range": f"{_col_letter(cols['Entry Price'])}{r}",
                            "values": [[p["entry_new"]]]})
            if p["target_new"] is not None and cols.get("Target Price") is not None:
                updates.append({"range": f"{_col_letter(cols['Target Price'])}{r}",
                                "values": [[p["target_new"]]]})
            if cols.get("Notes") is not None:
                tag = (f"{p['notes_old']} | " if p["notes_old"] else "") + \
                      f"{ca.APPLIED_TAG}:v{SCRIPT_VERSION}:f={p['factor']:g}"
                updates.append({"range": f"{_col_letter(cols['Notes'])}{r}",
                                "values": [[tag[:250]]]})
        for i in range(0, len(updates), 50):
            ws_pl.batch_update(updates[i:i + 50])
        if proposals:
            ws_a.append_rows(proposals, value_input_option="RAW")
        try:
            sh.worksheet("_Run_Log").append_row(
                [_now_utc(), "INFO", "ca_repair", PL_TAB, "OK",
                 f"[CA-REPAIR v{SCRIPT_VERSION}] applied={len(plan)} "
                 f"proposals={len(proposals)}", "", "", "",
                 json.dumps({"version": SCRIPT_VERSION})],
                value_input_option="RAW")
        except Exception:
            pass
        print(f"[CA-REPAIR v{SCRIPT_VERSION}] APPLIED cells="
              f"{len(updates)} proposal_rows={len(proposals)}")
    elif proposals:
        print("  (dry-run: proposals NOT written; re-run with --apply)")
    return 0


# --------------------------------------------------------------------------- #
# SELFTEST                                                                     #
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []
    pl = [
        ["", ""], ["", ""], ["", ""], ["", ""],
        ["Record ID", "Key", "Symbol", "Horizon", "Date Recorded (Riyadh)",
         "Entry Price", "Entry Recommendation", "Entry Score", "Risk Bucket",
         "Confidence", "Origin Tab", "Target Price", "Target ROI %",
         "Target Date (Riyadh)", "Status", "Current Price", "Unrealized ROI %",
         "Realized ROI %", "Outcome", "Volatility", "Max Drawdown %",
         "Sharpe Ratio", "Sector", "Factor Exposures",
         "Last Updated (Riyadh)", "Maturity Date", "Notes"],
        ["id1", "k", "ABCD.US", "1M", "2026-07-01", "100", "", "", "", "", "",
         "110", "", "", "active", "26.1", "", "", "", "", "", "", "", "", "",
         "", ""],
        ["id2", "k", "ABCD.US", "1M", "2026-07-16", "26", "", "", "", "", "",
         "28", "", "", "active", "26.1", "", "", "", "", "", "", "", "", "",
         "", ""],
        ["id3", "k", "SPLT.US", "1M", "2026-06-30", "200", "", "", "", "", "",
         "220", "", "", "active", "49.6", "", "", "", "", "", "", "", "", "",
         "", ""],
        ["id4", "k", "DONE.US", "1M", "2026-06-30", "80", "", "", "", "", "",
         "", "", "", "active", "20.1", "", "", "", "", "", "", "", "", "",
         "", "ca_adjusted:v1.0.0:f=4"],
    ]
    acts = ca.parse_actions([
        ca.ACTIONS_HEADER,
        ["ABCD.US", "SPLIT", "2026-07-10", "4", "", "", "CONFIRMED", "", ""],
    ])
    idx = ca.build_adjustment_index(acts)

    plan, hdr_i, cols = plan_repairs(pl, idx)
    checks.append(("header located at row 5", hdr_i == 4))
    checks.append(("plan: only pre-split ABCD row",
                   len(plan) == 1 and plan[0]["symbol"] == "ABCD.US"
                   and plan[0]["sheet_row"] == 6))
    checks.append(("plan math entry 100->25 target 110->27.5",
                   plan[0]["entry_new"] == 25.0 and plan[0]["target_new"] == 27.5))

    props = detect_proposals(pl, idx, acts)
    syms = [p[0] for p in props]
    checks.append(("proposal for unlogged SPLT.US 4:1",
                   syms == ["SPLT.US"] and props[0][1] == "SPLIT"
                   and props[0][3] == "4"))
    checks.append(("logged symbol not re-proposed", "ABCD.US" not in syms))
    checks.append(("proposal source is AUTO_DETECT",
                   props[0][6] == ca.SOURCE_AUTO))
    checks.append(("already-tagged DONE.US untouched everywhere",
                   all(p["symbol"] != "DONE.US" for p in plan)
                   and "DONE.US" not in syms))
    checks.append(("adjusted anchor kills false detection",
                   ca.detect_split_candidate(
                       ca.adjust_price("ABCD.US", 100, date(2026, 7, 1), idx),
                       26.1) is None))
    checks.append(("col letter math", _col_letter(0) == "A"
                   and _col_letter(26) == "AA"))

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[repair_corporate_actions v{SCRIPT_VERSION}] SELFTEST "
          f"{passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    sys.exit(main())
