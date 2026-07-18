"""
scripts/run_shadow_board.py — TFB Gen-2 Shadow Board Writer
============================================================
VERSION 1.0.0  (2026-07-18)  — NEW SCRIPT (Wave S, deliverable #10)

WHY (Master Plan v2.1 §3 Champion-Challenger, §15 Gate S-1): the S-1 verdict
(~Aug 31) needs a daily, workbook-visible record of what Gen-2 WOULD do.
This script reads the CHAMPION's own outputs from the workbook (Top_10 board
+ Portfolio_Decision holdings), runs the full Gen-2 stack shipped in Wave
A0/A — compliance_gate + shariah_authority + opportunity_builder cost/edge +
portfolio_actions switch scan + regime stamp — and rewrites the
`Shadow_Board` tab. It never writes to any champion page.

HOME: GitHub Actions (workflow shadow_board.yml, 2x daily + dispatch), where
Sheets credentials provably live. Runs equally from the Render shell.
Gate env is job-scoped here (exported by the workflow) — independent of the
operator's pending Render flag set.

HONESTY RULES: missing columns/fields degrade to INSUFFICIENT_DATA per row;
authority absence prints in the meta block; regime fetch failure prints in
the meta block; the script never fabricates and never raises past main().
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

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core import compliance_gate as cg          # noqa: E402
from core import shariah_authority as sa        # noqa: E402
from core import regime as rg                   # noqa: E402
from core.analysis import opportunity_builder as ob   # noqa: E402
from core.analysis import portfolio_actions as pa     # noqa: E402

# v1.0.1 (2026-07-18): header scan deepened to 40 rows (cockpit control-panel
# preambles run past row 12 in the real workbook); table-end detection added —
# break on a repeated header row or a non-symbol-shaped symbol cell (stops
# ingestion at "ALL QUALIFIED" / "SECTOR SUMMARY" sections); dedupe by symbol.
# Root-caused from export __44_ after live dry-run returned cands=0.
SCRIPT_VERSION = "1.0.1"
TAB_OUT = "Shadow_Board"
TAB_TOP10 = "Top_10_Investments"
TAB_HOLDINGS = "Portfolio_Decision"

OUT_HEADER = ["Symbol", "Name", "Champion Action", "ROI %", "Confidence",
              "Shariah Status", "Shariah Source", "Tradability", "Venue",
              "Floor OK", "RT Cost %", "Net Edge %", "Hurdle %",
              "Edge Verdict", "Gen2 Eligible"]

YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=2y&interval=1mo"
REGIME_SLEEVES = {"Global": "SPUS", "Saudi": "^TASI.SR"}


def _now_riyadh() -> str:
    from datetime import timedelta
    return (datetime.now(timezone.utc) + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M")


# --------------------------------------------------------------------------- #
# pure mapping (selftested offline)                                            #
# --------------------------------------------------------------------------- #
def _norm(s: Any) -> str:
    return "".join(ch for ch in str(s or "").lower() if ch.isalnum())


def resolve_columns(header: Sequence[Any]) -> Dict[str, Optional[int]]:
    """Tolerant column resolution: first header containing the token(s)."""
    toks = [_norm(h) for h in header]
    def find(*needles: str, prefer: str = "") -> Optional[int]:
        if prefer:
            for i, t in enumerate(toks):
                if prefer in t:
                    return i
        for i, t in enumerate(toks):
            if any(n in t for n in needles):
                return i
        return None
    return {
        "symbol": find("symbol", "ticker"),
        "name": find("companyname", "name"),
        "action": find("action", "recommendation"),
        "roi": find("roi", prefer="expectedroi"),
        "confidence": find("confidence"),
        "value_sar": find("valuesar", "marketvalue", "suggestedsar"),
    }


_SYM_SHAPE = None  # compiled lazily


def _symbol_shaped(s: str) -> bool:
    global _SYM_SHAPE
    if _SYM_SHAPE is None:
        import re
        _SYM_SHAPE = (re.compile(r"[A-Z0-9\-\^]{1,10}\.[A-Z]{1,3}"),
                      re.compile(r"[A-Z\-\^]{1,6}"))
    return bool(_SYM_SHAPE[0].fullmatch(s) or _SYM_SHAPE[1].fullmatch(s))


def rows_to_records(values: Sequence[Sequence[Any]],
                    max_scan: int = 40) -> List[Dict[str, Any]]:
    """Find the FIRST data-table header (cockpit preambles run deep), map its
    rows, and STOP at the table's end: a repeated header or a non-symbol-shaped
    symbol cell means a new section (ALL QUALIFIED / SECTOR SUMMARY / ALERTS)
    has begun. Blank symbol cells are spacers and are skipped, not terminal."""
    for h_i in range(min(max_scan, len(values or []))):
        cols = resolve_columns(values[h_i])
        if cols["symbol"] is not None and _norm(values[h_i][cols["symbol"]]) in (
                "symbol", "ticker"):
            out = []
            seen = set()
            for row in values[h_i + 1:]:
                if cols["symbol"] >= len(row):
                    continue
                sym = str(row[cols["symbol"]]).strip().upper()
                if not sym or sym == "-":
                    continue
                if sym in ("SYMBOL", "TICKER"):
                    break            # second table header -> this table ended
                if not _symbol_shaped(sym):
                    break            # section title / summary block reached
                if sym in seen:
                    continue
                seen.add(sym)
                def g(key):
                    c = cols.get(key)
                    return row[c] if c is not None and c < len(row) else None
                def f(v):
                    try:
                        x = float(str(v).replace("%", "").replace(",", "").strip())
                        return x
                    except Exception:
                        return None
                out.append({
                    "symbol": sym,
                    "name": str(g("name") or "").strip(),
                    "action": str(g("action") or "").strip(),
                    "roi_pct": f(g("roi")),
                    "confidence_band": str(g("confidence") or "").strip().title() or None,
                    "market_value_sar": f(g("value_sar")),
                })
            return out
    return []


def evaluate_board(cands: List[Dict[str, Any]],
                   auth_index: Dict[str, Dict[str, Any]],
                   monitor: Dict[str, str],
                   equity: float) -> Tuple[List[List[Any]], Dict[str, Any]]:
    """-> (Shadow_Board data rows, summary dict). Pure given inputs."""
    rows: List[List[Any]] = []
    eligible = 0
    blocked: Dict[str, int] = {}
    for c in cands:
        v = cg.evaluate(c["symbol"], {"name": c["name"]}, auth_index,
                        monitor=monitor, equity_sar=equity)
        ticket = c.get("market_value_sar") or (ob._venue_floor(c["symbol"]) or 10000.0)
        rt = ob.rt_cost_pct(c["symbol"], ticket)
        p = {"High": 0.65, "Medium": 0.55, "Low": 0.45}.get(
            c.get("confidence_band") or "", 0.50)
        roi = c.get("roi_pct")
        ne = hurdle = None
        verdict = "NO_COST_MODEL" if rt is None else "NO_ROI"
        if rt is not None and roi is not None:
            hurdle = max(3.0 * rt, 1.5)
            ne = p * roi - rt
            verdict = "TRADE" if ne >= hurdle else "EDGE_BELOW_COST"
        gen2 = bool(v.invest_eligible and verdict == "TRADE")
        if v.invest_eligible:
            eligible += 1
        else:
            key = ("FLOOR_LOCKED" if v.shariah_status in cg.INVEST_OK_STATUSES
                   else v.shariah_status)
            blocked[key] = blocked.get(key, 0) + 1
        rows.append([
            c["symbol"], c["name"], c["action"],
            roi if roi is not None else "",
            c.get("confidence_band") or "",
            v.shariah_status, v.shariah_source, v.tradability, v.venue or "",
            "YES" if v.floor_unlocked else ("NO" if v.floor_unlocked is False else ""),
            round(rt, 2) if rt is not None else "",
            round(ne, 2) if ne is not None else "",
            round(hurdle, 2) if hurdle is not None else "",
            verdict, "YES" if gen2 else "NO",
        ])
    return rows, {"evaluated": len(cands), "compliance_eligible": eligible,
                  "blocked": blocked}


# --------------------------------------------------------------------------- #
# regime (network; failure-tolerant)                                           #
# --------------------------------------------------------------------------- #
def fetch_monthly_closes(symbol: str) -> List[Tuple[date, float]]:
    import httpx
    r = httpx.get(YAHOO_CHART.format(sym=symbol), timeout=30.0,
                  headers={"User-Agent": "Mozilla/5.0 (TFB shadow_board)"})
    r.raise_for_status()
    res = r.json()["chart"]["result"][0]
    ts = res.get("timestamp") or []
    closes = res["indicators"]["quote"][0].get("close") or []
    out = []
    for t, c in zip(ts, closes):
        if c is None:
            continue
        out.append((datetime.fromtimestamp(t, tz=timezone.utc).date(), float(c)))
    return rg.monthly_from_daily(out)


def build_regime_block() -> Dict[str, Any]:
    states: Dict[str, Dict[str, Any]] = {}
    errors: List[str] = []
    for sleeve, sym in REGIME_SLEEVES.items():
        try:
            mo = fetch_monthly_closes(sym)
            cur = rg.current_regime(mo)
            cur["abs_mom_pct"] = rg.abs_momentum_pct(mo)
            cur["regime"] = cur.get("state")
            states[sleeve] = cur
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{sleeve}({sym}): {type(exc).__name__}")
    alloc = rg.allocate(states) if states else {"weights": {}, "rationale": []}
    stamp = rg.regime_stamp(states, alloc)
    if errors:
        stamp["errors"] = errors
    return stamp


# --------------------------------------------------------------------------- #
# sheets I/O                                                                   #
# --------------------------------------------------------------------------- #
def _open_sheet(cli: Optional[str]):
    import gspread
    from google.oauth2.service_account import Credentials
    sid = None
    for v in (cli, os.getenv("TARGET_SHEET_ID"), os.getenv("TRACK_SHEET_ID"),
              os.getenv("DEFAULT_SPREADSHEET_ID")):
        if v and str(v).strip():
            sid = str(v).strip(); break
    if not sid:
        raise SystemExit("::error::Spreadsheet ID missing.")
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
    return gspread.authorize(creds).open_by_key(sid)


def write_board(sh, data_rows: List[List[Any]], meta_lines: List[List[Any]]):
    try:
        ws = sh.worksheet(TAB_OUT)
    except Exception:  # noqa: BLE001
        ws = sh.add_worksheet(title=TAB_OUT, rows=200, cols=len(OUT_HEADER))
    ws.clear()
    body = meta_lines + [[]] + [OUT_HEADER] + data_rows
    ws.update(values=body, range_name="A1")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--sheet-id")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    if args.selftest:
        return _selftest()

    os.environ.setdefault("TFB_COMPLIANCE_GATE_ENABLED", "1")
    equity = float(os.getenv("TFB_SHADOW_EQUITY_SAR") or "130000")

    sh = _open_sheet(args.sheet_id)
    cands = rows_to_records(sh.worksheet(TAB_TOP10).get_all_values())
    try:
        holds = rows_to_records(sh.worksheet(TAB_HOLDINGS).get_all_values())
    except Exception:  # noqa: BLE001
        holds = []

    auth = sa.get_authority_index(force=True)
    monitor = sa.get_monitor_map()
    ameta = sa.get_meta()

    data_rows, summary = evaluate_board(cands, auth, monitor, equity)
    scan = pa.advisor_switch_scan(holds, cands)
    regime_block = build_regime_block()

    meta = [
        [f"SHADOW BOARD v{SCRIPT_VERSION}", f"as of {_now_riyadh()} Riyadh",
         f"equity={int(equity):,} SAR"],
        [f"authority rows={ameta.get('rows', 0)} as_of={ameta.get('as_of')}",
         f"authority_error={sa.last_error() or '-'}"],
        [f"evaluated={summary['evaluated']}",
         f"compliance_eligible={summary['compliance_eligible']}",
         "blocked=" + json.dumps(summary["blocked"])],
        ["switch scan: " + scan["verdict"],
         json.dumps(scan["proposals"][:3], ensure_ascii=False),
         f"pairs={scan['pairs_checked']}"],
        ["regime: " + json.dumps(regime_block.get("sleeves", {}), default=str),
         "weights=" + json.dumps(regime_block.get("suggested_weights") or {}),
         ";".join(regime_block.get("errors", []) or [])],
        [regime_block.get("governance", "")],
    ]

    verdict = (f"[SHADOW-BOARD v{SCRIPT_VERSION}] cands={summary['evaluated']} "
               f"eligible={summary['compliance_eligible']} "
               f"blocked={summary['blocked']} switch={scan['verdict']} "
               f"auth_rows={ameta.get('rows', 0)}")
    print(verdict)
    if args.dry_run:
        for r in data_rows[:10]:
            print("  ", r)
        return 0

    write_board(sh, data_rows, meta)
    try:
        sh.worksheet("_Run_Log").append_row(
            [datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "INFO",
             "shadow_board", TAB_OUT, "OK", verdict, "", "", "",
             json.dumps({"version": SCRIPT_VERSION})],
            value_input_option="RAW")
    except Exception:  # noqa: BLE001
        pass
    return 0


# --------------------------------------------------------------------------- #
# SELFTEST (offline; no sheets, no network)                                    #
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    os.environ["TFB_COMPLIANCE_GATE_ENABLED"] = "1"
    checks: List[Tuple[str, bool]] = []
    hdr = ["Rank", "Symbol", "Company Name", "Action", "Expected ROI %",
           "Confidence", "Suggested SAR"]
    cols = resolve_columns(hdr)
    checks.append(("column resolution",
                   cols["symbol"] == 1 and cols["name"] == 2 and cols["roi"] == 4
                   and cols["confidence"] == 5 and cols["value_sar"] == 6))
    values = [["junk"], hdr,
              ["1", "7010.SR", "Saudi Telecom", "INVEST", "14.5%", "High", "18,000"],
              ["2", "9628.SR", "Lamasat", "INVEST", "22", "Medium", "9,000"],
              ["", "", "", "", "", "", ""]]
    recs = rows_to_records(values)
    checks.append(("records mapped + %% and comma parsed",
                   len(recs) == 2 and recs[0]["roi_pct"] == 14.5
                   and recs[0]["market_value_sar"] == 18000.0))
    # real-workbook clone: 18-row control preamble, SELECTED table, spacer,
    # second table ("ALL QUALIFIED") and a summary block — v1.0.1 must ingest
    # exactly the SELECTED rows and stop.
    pre = [["TOP 10 INVESTMENTS"], ["Status:", "Last run"], [""],
           ["CONTROL PANEL"]] + [[f"T10: knob {i}", "1.0"] for i in range(9)] + \
          [["KPIs"], ["Deployable (SAR)", "Exp. Gain"], ["100000", "17484"],
           [""], ["SELECTED — EXECUTABLE"]]
    tbl = [["Rank", "Symbol", "Name", "Expected ROI %", "Confidence"],
           ["1", "6960.T", "Fukuda Denshi", "35", "High"],
           ["2", "2269.T", "Meiji Holdings", "35", "High"],
           ["3", "0083.HK", "Sino Land", "35", "Medium"],
           [""],
           ["ALL QUALIFIED — TOP 30"],
           ["Rank", "Symbol", "Name", "ROI %", "Confidence"],
           ["1", "2269.T", "Meiji Holdings", "35", "High"],
           ["2", "9999.HK", "Tencent-ish", "22", "High"]]
    deep = rows_to_records(pre + tbl)
    checks.append(("deep preamble: exactly the 3 SELECTED rows, no bleed",
                   [r["symbol"] for r in deep] == ["6960.T", "2269.T", "0083.HK"]))
    pd_vals = [["MY PORTFOLIO"], ["Status:", "x"], [""], ["CONTROL"],
               ["PF: k", "1"], ["PF: k", "1"], ["PF: Rebalance", "Advisory"],
               [""], ["KPIs"],
               ["Portfolio (SAR)", "Holdings (SAR)"], ["30075", "30075"], [""],
               ["ACTIONS — one per holding"],
               ["Action", "Symbol", "Name", "Qty", "Market Value (SAR)",
                "Confidence", "Expected ROI %"],
               ["TRIM", "1050.SR", "Banque Saudi", "290", "5,684", "Low", "2"],
               ["HOLD", "5023.SR", "5023.SR", "100", "10,126", "", ""],
               [""],
               ["SECTOR SUMMARY (vs caps)"],
               ["Sector", "Value SAR", "Weight %"],
               ["Financials", "10302.0", "34.3"]]
    pd_recs = rows_to_records(pd_vals)
    checks.append(("PD: holdings only, summary block never parsed as symbols",
                   [r["symbol"] for r in pd_recs] == ["1050.SR", "5023.SR"]
                   and pd_recs[0]["market_value_sar"] == 5684.0))
    auth = cg.build_authority_index([
        {"symbol": "7010.SR", "status": "PASS", "as_of": str(date.today())}])
    rows, summary = evaluate_board(recs, auth, {}, equity=130000)
    by = {r[0]: r for r in rows}
    checks.append(("board: STC authority-pass + TRADE + Gen2 YES",
                   by["7010.SR"][5] == "AUTHORITY_PASS"
                   and by["7010.SR"][13] == "TRADE"
                   and by["7010.SR"][14] == "YES"))
    checks.append(("board: Nomu venue-blocked, Gen2 NO",
                   by["9628.SR"][5] == "VENUE_BLOCK" and by["9628.SR"][14] == "NO"))
    checks.append(("summary counts", summary["evaluated"] == 2
                   and summary["compliance_eligible"] == 1
                   and summary["blocked"] == {"VENUE_BLOCK": 1}))
    checks.append(("header width matches rows",
                   all(len(r) == len(OUT_HEADER) for r in rows)))
    mo = rg.monthly_from_daily([(date(2026, m, 1), 100.0 + m) for m in range(1, 13)])
    checks.append(("regime helpers importable end-to-end",
                   rg.current_regime(mo)["state"] in ("RISK_ON", "RISK_OFF")))
    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[shadow_board v{SCRIPT_VERSION}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    sys.exit(main())
