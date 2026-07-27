#!/usr/bin/env python3
# scripts/quarantine_placeholder_records.py
"""
================================================================================
Quarantine Placeholder-Sourced Records — v1.0.0 (2026-07-27)
================================================================================
NEW script. One-time surgical repair. Modelled on repair_corporate_actions.py:
DRY-RUN by default, idempotent, symbol-keyed, never self-confirming.

WHY
    24 Performance_Log records recorded 2026-07-25/26 were built from fail-soft
    PLACEHOLDER rows, not market data. core/data_engine.py's own changelog
    documents the generator it once shipped:

        current_price = 100.0 + idx     ->  101, 102, 103, ...
        overall_score = 100 - idx*3     ->   97,  94,  91, ...

    The live rows, recorded 2026-07-25:

        1120.SR   entry 102   target_roi 94      idx 2
        AAPL      entry 103   target_roi 91      idx 3
        MSFT      entry 104   target_roi 88      idx 4
        NVDA      entry 105   target_roi 85      idx 5

    Both formulas, same idx run. The 07-26 rows carry the same entry values
    with horizon-scaled targets (1W = 94 * 7/30 = 21.93; 2W = 94 * 14/30 =
    43.87), so the same ladder sits underneath them.

WHY THEY CANNOT BE REPAIRED, ONLY VOIDED
    A repair would need a true entry price AND a true forecast. The price could
    be recovered from _PIT_Fundamentals. The FORECAST could not: target_roi
    94/91/88/85 came from the same ladder, so there is no real prediction
    underneath to restore. Writing a corrected entry with an invented target
    would fabricate a forecast that was never made, and calibration would then
    score the engine on a claim it never issued. Excluding a corrupt
    observation is honest; reconstructing one is not.

WHAT IT WRITES (per matched record, --apply only)
    Status         -> "expired"
    Realized ROI % -> "" (blank)
    Outcome        -> "PLACEHOLDER_SOURCE"
    Notes          -> existing + " | voided:v1.0.0:placeholder_ladder"

    EXPIRED is an EXISTING PerformanceStatus enum member and is the codebase's
    established structural-exclusion path (see track_performance v6.x:
    "EXPIRED outcome CORP_ACTION_SUSPECT (structurally excluded)" and
    "EXPIRED with realized_roi = None and outcome 'UNPRICED'"). Calibration
    filters on `status == MATURED and realized_roi is not None`, so an EXPIRED
    record with a blank realized ROI can never enter a calibration statistic.

    A NEW status was deliberately NOT introduced: track_performance's loader
    falls back to ACTIVE on any status it does not recognise, so an invented
    value would silently re-activate these rows on the next run.

SAFETY — why this cannot touch anything else
    1. EXPLICIT TARGET SET. The 24 records are enumerated below by
       (symbol, date_recorded, expected_entry_price). Nothing is matched by
       heuristic, pattern or row position. A record not in the set is never
       considered.
    2. VALUE VERIFICATION. A row is only written if its CURRENT entry price
       still equals the expected poison value. If anything has changed it is
       reported as DRIFTED and skipped, never guessed at.
    3. IDEMPOTENT. Rows already carrying the voided tag are skipped.
    4. DRY-RUN DEFAULT. Nothing is written without --apply.
    5. COUNT ASSERTION. --apply refuses to run if the matched count exceeds
       the expected 24.

USAGE
    python scripts/quarantine_placeholder_records.py --selftest
    python scripts/quarantine_placeholder_records.py --scan      # default
    python scripts/quarantine_placeholder_records.py --apply
================================================================================
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

SCRIPT_VERSION = "1.0.0"

PL_TAB = "Performance_Log"
RUN_LOG_TAB = "_Run_Log"

VOID_TAG = "voided:v%s:placeholder_ladder" % SCRIPT_VERSION
VOID_STATUS = "expired"
VOID_OUTCOME = "PLACEHOLDER_SOURCE"

# --------------------------------------------------------------------------- #
# THE EXPLICIT TARGET SET — (symbol, date_recorded, expected_entry_price)      #
# Derived from the live export 2026-07-27 13:50. 4 symbols x 2 dates x         #
# horizons = 24 records. Nothing outside this set is ever touched.             #
# --------------------------------------------------------------------------- #
TARGETS: Tuple[Tuple[str, str, float], ...] = (
    ("1120.SR", "2026-07-25", 102.0),
    ("1120.SR", "2026-07-26", 102.0),
    ("AAPL",    "2026-07-25", 103.0),
    ("AAPL",    "2026-07-26", 103.0),
    ("MSFT",    "2026-07-25", 104.0),
    ("MSFT",    "2026-07-26", 104.0),
    ("NVDA",    "2026-07-25", 105.0),
    ("NVDA",    "2026-07-26", 105.0),
)
EXPECTED_RECORDS = 24


# --------------------------------------------------------------------------- #
# HELPERS                                                                      #
# --------------------------------------------------------------------------- #
def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _s(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _f(v: Any) -> Optional[float]:
    try:
        t = _s(v).replace(",", "").replace("%", "")
        return float(t) if t else None
    except Exception:
        return None


def _col_letter(idx0: int) -> str:
    n, out = idx0 + 1, ""
    while n:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


def _find_header(values: Sequence[Sequence[Any]]) -> int:
    """Locate the header row. track_performance seeds the grid at row 5, but
    the row index is discovered, never assumed."""
    for i, row in enumerate(values[:40]):
        cells = [_s(c) for c in row]
        if "Symbol" in cells and "Entry Price" in cells and "Status" in cells:
            return i
    return -1


def build_plan(values: Sequence[Sequence[Any]]
               ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int,
                          Dict[str, int]]:
    """Returns (plan, anomalies, header_index, column_map).

    plan      — rows verified against the expected poison value, safe to void
    anomalies — rows in the target set whose entry price has DRIFTED, or which
                are already voided. Reported, never written.
    """
    hdr_i = _find_header(values)
    if hdr_i < 0:
        return [], [], -1, {}
    header = [_s(c) for c in values[hdr_i]]
    cols = {name: i for i, name in enumerate(header) if name}

    need = ("Symbol", "Date Recorded (Riyadh)", "Entry Price", "Status")
    for n in need:
        if n not in cols:
            return [], [], hdr_i, cols

    want = {(s, d): px for s, d, px in TARGETS}
    plan: List[Dict[str, Any]] = []
    anomalies: List[Dict[str, Any]] = []

    for r_off, row in enumerate(values[hdr_i + 1:], start=hdr_i + 2):
        def g(name: str) -> str:
            i = cols.get(name)
            return _s(row[i]) if i is not None and i < len(row) else ""

        sym = g("Symbol").upper()
        if not sym:
            continue
        rec_date = g("Date Recorded (Riyadh)")[:10]
        key = (sym, rec_date)
        if key not in want:
            continue

        notes = g("Notes") if "Notes" in cols else ""
        if VOID_TAG.split(":")[0] in notes:
            anomalies.append({"sheet_row": r_off, "symbol": sym,
                              "date": rec_date, "why": "already voided"})
            continue

        entry = _f(g("Entry Price"))
        expected = want[key]
        if entry is None or abs(entry - expected) > 1e-9:
            anomalies.append({"sheet_row": r_off, "symbol": sym,
                              "date": rec_date,
                              "why": "DRIFTED: entry=%s expected=%s"
                                     % (entry, expected)})
            continue

        plan.append({
            "sheet_row": r_off, "symbol": sym, "date": rec_date,
            "horizon": g("Horizon"), "entry": entry,
            "status_old": g("Status"), "notes_old": notes,
        })

    return plan, anomalies, hdr_i, cols


# --------------------------------------------------------------------------- #
# SHEET IO                                                                     #
# --------------------------------------------------------------------------- #
def _open_sheet(cli_id: Optional[str]):
    import gspread                                    # noqa: WPS433
    from google.oauth2.service_account import Credentials  # noqa: WPS433

    sid = None
    for v in (cli_id, os.getenv("TARGET_SHEET_ID"), os.getenv("TRACK_SHEET_ID"),
              os.getenv("DEFAULT_SPREADSHEET_ID"), os.getenv("SPREADSHEET_ID")):
        if _s(v):
            sid = _s(v)
            break
    if not sid:
        raise SystemExit("No spreadsheet id (--sheet-id or TARGET_SHEET_ID).")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    raw = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    b64 = os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64")
    if raw or b64:
        if b64 and not raw:
            import base64
            raw = base64.b64decode(b64).decode("utf-8")
        creds = Credentials.from_service_account_info(json.loads(raw),
                                                      scopes=scopes)
    elif path:
        creds = Credentials.from_service_account_file(path, scopes=scopes)
    else:
        raise SystemExit("No Google credentials in environment.")
    return gspread.authorize(creds).open_by_key(sid)


# --------------------------------------------------------------------------- #
# MAIN                                                                         #
# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--sheet-id")
    args = ap.parse_args()

    if args.selftest:
        return _selftest()

    sh = _open_sheet(args.sheet_id)
    ws = sh.worksheet(PL_TAB)
    values = ws.get_all_values()
    plan, anomalies, hdr_i, cols = build_plan(values)

    mode = "APPLY" if args.apply else "DRY-RUN"
    print("[QUARANTINE v%s] header_row=%s matched=%d anomalies=%d "
          "expected=%d mode=%s"
          % (SCRIPT_VERSION, hdr_i + 1 if hdr_i >= 0 else "NOT-FOUND",
             len(plan), len(anomalies), EXPECTED_RECORDS, mode))

    for p in plan:
        print("  row %5d  %-9s %-11s %-3s entry=%-7g status=%s -> %s"
              % (p["sheet_row"], p["symbol"], p["date"], p["horizon"],
                 p["entry"], p["status_old"] or "(blank)", VOID_STATUS))
    for a in anomalies:
        print("  SKIP row %5d  %-9s %-11s  %s"
              % (a["sheet_row"], a["symbol"], a["date"], a["why"]))

    if hdr_i < 0:
        print("  ERROR: header row not found — nothing done.")
        return 2

    if not args.apply:
        if plan:
            print("  (dry-run: nothing written; re-run with --apply)")
        return 0

    if len(plan) > EXPECTED_RECORDS:
        print("  REFUSING: matched %d > expected %d. Investigate before "
              "applying." % (len(plan), EXPECTED_RECORDS))
        return 3
    if not plan:
        print("  nothing to do.")
        return 0

    updates: List[Dict[str, Any]] = []
    for p in plan:
        r = p["sheet_row"]
        updates.append({"range": "%s%d" % (_col_letter(cols["Status"]), r),
                        "values": [[VOID_STATUS]]})
        if "Realized ROI %" in cols:
            updates.append({"range": "%s%d"
                            % (_col_letter(cols["Realized ROI %"]), r),
                            "values": [[""]]})
        if "Outcome" in cols:
            updates.append({"range": "%s%d" % (_col_letter(cols["Outcome"]), r),
                            "values": [[VOID_OUTCOME]]})
        if "Notes" in cols:
            tag = (p["notes_old"] + " | " if p["notes_old"] else "") + VOID_TAG
            updates.append({"range": "%s%d" % (_col_letter(cols["Notes"]), r),
                            "values": [[tag[:250]]]})

    for i in range(0, len(updates), 50):
        ws.batch_update(updates[i:i + 50])

    try:
        sh.worksheet(RUN_LOG_TAB).append_row(
            [_now_utc(), "WARNING", "quarantine_placeholder", PL_TAB, "OK",
             "[QUARANTINE v%s] voided=%d cells=%d reason=placeholder_ladder"
             % (SCRIPT_VERSION, len(plan), len(updates)), "", "", "",
             json.dumps({"version": SCRIPT_VERSION,
                         "symbols": sorted({p["symbol"] for p in plan})})],
            value_input_option="RAW")
    except Exception:
        pass

    print("[QUARANTINE v%s] APPLIED voided=%d cells=%d"
          % (SCRIPT_VERSION, len(plan), len(updates)))
    return 0


# --------------------------------------------------------------------------- #
# SELFTEST — offline, no network                                               #
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []

    hdr = ["Record ID", "Key", "Symbol", "Horizon", "Date Recorded (Riyadh)",
           "Entry Price", "Entry Recommendation", "Entry Score", "Risk Bucket",
           "Confidence", "Origin Tab", "Target Price", "Target ROI %",
           "Target Date (Riyadh)", "Status", "Current Price",
           "Unrealized ROI %", "Realized ROI %", "Outcome", "Volatility",
           "Max Drawdown %", "Sharpe Ratio", "Sector", "Factor Exposures",
           "Last Updated (Riyadh)", "Maturity Date", "Notes"]

    def row(sym, date, entry, status="active", notes=""):
        r = [""] * len(hdr)
        r[2], r[3], r[4], r[5] = sym, "1M", date, str(entry)
        r[14], r[26] = status, notes
        return r

    values = [[""], [""], [""], [""], hdr,
              row("1120.SR", "2026-07-25", 102),      # target, clean
              row("AAPL",    "2026-07-26", 103),      # target, clean
              row("MSFT",    "2026-07-25", 999),      # target, DRIFTED
              row("NVDA",    "2026-07-26", 105, notes="voided:v1.0.0:x"),
              row("MA",      "2026-07-25", 539.66),   # not a target
              row("AAPL",    "2026-07-10", 283.78)]   # right symbol, wrong day

    plan, anomalies, hdr_i, cols = build_plan(values)
    syms = sorted(p["symbol"] for p in plan)

    checks.append(("header discovered at index 4", hdr_i == 4))
    checks.append(("plan contains exactly the two clean targets",
                   syms == ["1120.SR", "AAPL"]))
    checks.append(("untargeted symbol MA never considered",
                   all(p["symbol"] != "MA" for p in plan)))
    checks.append(("right symbol on an untargeted DATE is ignored",
                   not any(p["symbol"] == "AAPL" and p["date"] == "2026-07-10"
                           for p in plan)))
    checks.append(("drifted entry price is skipped, not guessed",
                   any(a["symbol"] == "MSFT" and "DRIFTED" in a["why"]
                       for a in anomalies)))
    checks.append(("already-voided row is idempotent-skipped",
                   any(a["symbol"] == "NVDA" and a["why"] == "already voided"
                       for a in anomalies)))
    checks.append(("sheet rows are 1-based and correct",
                   [p["sheet_row"] for p in plan] == [6, 7]))
    checks.append(("column letters", _col_letter(0) == "A"
                   and _col_letter(14) == "O" and _col_letter(26) == "AA"))
    checks.append(("EXPIRED is the enum's existing exclusion value",
                   VOID_STATUS == "expired"))
    checks.append(("target set is 8 keys / 24 expected records",
                   len(TARGETS) == 8 and EXPECTED_RECORDS == 24))

    empty_plan, _, _, _ = build_plan([[""], hdr])
    checks.append(("no data rows -> empty plan, no crash", empty_plan == []))

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print("[quarantine_placeholder_records v%s] SELFTEST %d/%d"
          % (SCRIPT_VERSION, passed, len(checks)))
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    sys.exit(main())
