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
from core import risk_limits as rl              # noqa: E402
from core.analysis import opportunity_builder as ob   # noqa: E402
from core.analysis import portfolio_actions as pa     # noqa: E402

# v1.0.1 (2026-07-18): header scan deepened to 40 rows (cockpit control-panel
# preambles run past row 12 in the real workbook); table-end detection added —
# break on a repeated header row or a non-symbol-shaped symbol cell (stops
# ingestion at "ALL QUALIFIED" / "SECTOR SUMMARY" sections); dedupe by symbol.
# Root-caused from export __44_ after live dry-run returned cands=0.
# v1.1.0 (2026-07-18): BOARD FUNDAMENTALS — per-symbol market cap + total debt
# + sector/industry fetched via yfinance inside the job (<=12 symbols, gentle
# pacing) so the Rajhi-style model screen resolves REAL pass/fail instead of
# UNKNOWN. Mapper fixes from export-evidence: `Conf` and `Ticket SAR` header
# tokens (both were missed -> p defaulted to 0.50 and tickets to venue
# floors); board `Sector` captured. ROI binding verified CORRECT: col `ROI %`
# is the champion's stability-adjusted display ROI (17.5 vs Engine 34.8 on
# 2269.T is real data, not a bug). Board gains Sector + Debt/MCap %% columns;
# verdict line now carries fundamentals coverage + regime summary.
# v1.1.1 (2026-07-18): live-memo findings. (1) Yahoo-native symbol mapping —
# `.US` suffix stripped for fundamentals queries (TRMD.US -> TRMD); Tokyo/
# Brussels/HK suffixes are Yahoo-native and pass through. (2) Coverage
# honesty — yfinance returns an empty shell instead of raising on unknown
# symbols; a symbol now counts as covered ONLY if it actually carried data
# (market cap, debt, or sector), else it lands in the error list. (3) Regime
# history widened to 5y (2y left ^TASI.SR under the 10-month minimum).
# v1.1.2 (2026-07-18): ELIGIBILITY-COHERENT SWITCH SCAN. The live memo caught
# the scan proposing to BUY names Gen-2 itself blocks (TRMD.US/MRP.US,
# MODEL_SCREEN_FAIL). Plan rule (§4): non-eligible can never be a BUY —
# candidates are now filtered to Gen2-eligible before advisor_switch_scan,
# via the shared eligible_symbols() helper (the weekly brief imports it).
# v1.1.3 (2026-07-19): CONCENTRATION VERDICT. risk_limits v1.0.0 is live but
# nothing called it. The eligible book is now scored for correlation-adjusted
# heat and country/sector/theme caps, and the verdict lands in the meta block
# + the [SHADOW-BOARD] line. Measured on the live board this reports
# BREACH (Japan 60% vs 40% cap) with the honest structural note. Advisory:
# it annotates, never filters the board.
SCRIPT_VERSION = "1.1.3"
TAB_OUT = "Shadow_Board"
TAB_TOP10 = "Top_10_Investments"
TAB_HOLDINGS = "Portfolio_Decision"

OUT_HEADER = ["Symbol", "Name", "Champion Action", "Sector", "ROI %",
              "Confidence", "Shariah Status", "Shariah Source", "Tradability",
              "Venue", "Floor OK", "RT Cost %", "Net Edge %", "Hurdle %",
              "Edge Verdict", "Debt/MCap %", "Gen2 Eligible"]

YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=5y&interval=1mo"
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
        "sector": find("sector"),
        "roi": find("roi", prefer="expectedroi"),
        "confidence": find("confidence", "conf"),
        "value_sar": find("ticketsar", "valuesar", "marketvalue", "suggestedsar"),
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
                    "sector": str(g("sector") or "").strip(),
                })
            return out
    return []


def build_risk_block(cands: List[Dict[str, Any]], eligible: set,
                     fnd: Optional[Dict[str, Dict[str, Any]]] = None
                     ) -> Dict[str, Any]:
    """v1.1.3: concentration + correlation-adjusted heat on the ELIGIBLE book
    (what Gen-2 would actually hold). Never raises; never edits the board."""
    try:
        fnd = fnd or {}
        rows = [{"symbol": c["symbol"], "name": c.get("name", ""),
                 "sector": (fnd.get(c["symbol"], {}).get("sector")
                            or c.get("sector") or ""),
                 "industry": fnd.get(c["symbol"], {}).get("industry") or ""}
                for c in cands if c["symbol"] in eligible]
        pos = rl.enrich_positions(rows)
        res = rl.check_limits(pos)
        trims = rl.suggest_trims(pos)
        return {"verdict": res["verdict"],
                "naive_heat_pct": res["heat"]["naive_heat_pct"],
                "effective_heat_pct": res["heat"]["effective_heat_pct"],
                "diversification_ratio": res["heat"]["diversification_ratio"],
                "country": res["concentration"]["country"],
                "theme": res["concentration"]["theme"],
                "breaches": [b["detail"] for b in res["breaches"]],
                "trims": [d["symbol"] for d in trims["drops"]],
                "feasible": trims["feasible"], "note": trims["note"],
                "rho": res["rho"]}
    except Exception as exc:  # noqa: BLE001
        return {"verdict": "ERROR", "error": f"{type(exc).__name__}:{exc}"}


def eligible_symbols(rows: List[List[Any]]) -> set:
    """Symbols whose board row says Gen2 Eligible == YES (last column)."""
    return {r[0] for r in rows or [] if r and r[-1] == "YES"}


def fetch_board_fundamentals(symbols: Sequence[str], sleep_s: float = 0.4
                             ) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    """Per-symbol marketCap/totalDebt/sector/industry for the model screen.
    Board-sized only (<=12), gently paced; every miss is reported, never
    fabricated. yfinance absent => empty coverage with the reason."""
    out: Dict[str, Dict[str, Any]] = {}
    errs: List[str] = []
    try:
        import yfinance as yf
    except Exception as exc:  # noqa: BLE001
        return out, [f"yfinance_unavailable:{type(exc).__name__}"]
    import time as _t
    for sym in list(symbols)[:12]:
        try:
            info = yf.Ticker(_yahoo_symbol(sym)).get_info()
            rec = {"market_cap": info.get("marketCap"),
                   "interest_debt": info.get("totalDebt"),
                   "sector": info.get("sector") or "",
                   "industry": info.get("industry") or ""}
            if _fnd_valid(rec):
                out[sym] = rec
            else:
                errs.append(f"{sym}:no_data")
        except Exception as exc:  # noqa: BLE001
            errs.append(f"{sym}:{type(exc).__name__}")
        _t.sleep(sleep_s)
    return out, errs


def _yahoo_symbol(sym: str) -> str:
    """Yahoo-native mapping: `.US` is a TFB convention, not a Yahoo suffix."""
    s = str(sym).strip().upper()
    return s[:-3] if s.endswith(".US") else s


def _fnd_valid(rec: Dict[str, Any]) -> bool:
    return bool(rec.get("market_cap") or rec.get("interest_debt")
                or rec.get("sector"))


def evaluate_board(cands: List[Dict[str, Any]],
                   auth_index: Dict[str, Dict[str, Any]],
                   monitor: Dict[str, str],
                   equity: float,
                   fundamentals: Optional[Dict[str, Dict[str, Any]]] = None
                   ) -> Tuple[List[List[Any]], Dict[str, Any]]:
    """-> (Shadow_Board data rows, summary dict). Pure given inputs."""
    rows: List[List[Any]] = []
    eligible = 0
    blocked: Dict[str, int] = {}
    fnd_all = fundamentals or {}
    for c in cands:
        fnd = fnd_all.get(c["symbol"], {})
        sector = fnd.get("sector") or c.get("sector") or ""
        gate_row = {"name": c["name"], "sector": sector,
                    "industry": fnd.get("industry") or "",
                    "market_cap": fnd.get("market_cap"),
                    "interest_debt": fnd.get("interest_debt")}
        v = cg.evaluate(c["symbol"], gate_row, auth_index,
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
        mc, dbt = fnd.get("market_cap"), fnd.get("interest_debt")
        ratio = round(float(dbt) / float(mc) * 100.0, 1) if (mc and dbt) else ""
        rows.append([
            c["symbol"], c["name"], c["action"], sector,
            roi if roi is not None else "",
            c.get("confidence_band") or "",
            v.shariah_status, v.shariah_source, v.tradability, v.venue or "",
            "YES" if v.floor_unlocked else ("NO" if v.floor_unlocked is False else ""),
            round(rt, 2) if rt is not None else "",
            round(ne, 2) if ne is not None else "",
            round(hurdle, 2) if hurdle is not None else "",
            verdict, ratio, "YES" if gen2 else "NO",
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

    fnd, fnd_errs = fetch_board_fundamentals([c["symbol"] for c in cands])
    data_rows, summary = evaluate_board(cands, auth, monitor, equity, fnd)
    ok = eligible_symbols(data_rows)
    risk_block = build_risk_block(cands, ok, fnd)
    scan = pa.advisor_switch_scan(
        holds, [c for c in cands if c["symbol"] in ok])
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
        [f"fundamentals coverage={len(fnd)}/{len(cands)}",
         ";".join(fnd_errs[:4]) or "-"],
        ["risk: " + risk_block.get("verdict", "?"),
         (f"heat naive {risk_block.get('naive_heat_pct')}% -> effective "
          f"{risk_block.get('effective_heat_pct')}% "
          f"(div ratio {risk_block.get('diversification_ratio')})"),
         "country=" + json.dumps(risk_block.get("country") or {}),
         "breaches=" + json.dumps(risk_block.get("breaches") or [])],
        [risk_block.get("note", ""), "rho=" + json.dumps(risk_block.get("rho") or {})],
        ["regime: " + json.dumps(regime_block.get("sleeves", {}), default=str),
         "weights=" + json.dumps(regime_block.get("suggested_weights") or {}),
         ";".join(regime_block.get("errors", []) or [])],
        [regime_block.get("governance", "")],
    ]

    verdict = (f"[SHADOW-BOARD v{SCRIPT_VERSION}] cands={summary['evaluated']} "
               f"eligible={summary['compliance_eligible']} "
               f"blocked={summary['blocked']} switch={scan['verdict']} "
               f"auth_rows={ameta.get('rows', 0)} fnd={len(fnd)}/{len(cands)} "
               f"risk={risk_block.get('verdict')} "
               f"heat={risk_block.get('effective_heat_pct')}% "
               f"regime={'ERR' if regime_block.get('errors') else 'OK'} "
               f"w={json.dumps(regime_block.get('suggested_weights') or {})}")
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
                   by["7010.SR"][6] == "AUTHORITY_PASS"
                   and by["7010.SR"][14] == "TRADE"
                   and by["7010.SR"][16] == "YES"))
    checks.append(("board: Nomu venue-blocked, Gen2 NO",
                   by["9628.SR"][6] == "VENUE_BLOCK" and by["9628.SR"][16] == "NO"))
    checks.append(("summary counts", summary["evaluated"] == 2
                   and summary["compliance_eligible"] == 1
                   and summary["blocked"] == {"VENUE_BLOCK": 1}))
    checks.append(("header width matches rows",
                   all(len(r) == len(OUT_HEADER) for r in rows)))
    # cockpit header variant: `Conf` + `Ticket SAR` + `Sector` must bind
    v_hdr = ["Rank", "Symbol", "Name", "Sector", "Ticket SAR", "ROI %",
             "Engine ROI %", "Conf"]
    vcols = resolve_columns(v_hdr)
    checks.append(("cockpit tokens bind (Conf/Ticket SAR/Sector, ROI=first)",
                   vcols["confidence"] == 7 and vcols["value_sar"] == 4
                   and vcols["sector"] == 3 and vcols["roi"] == 5))
    fnd = {"2269.T": {"market_cap": 1.1e12, "interest_debt": 2.0e11,
                      "sector": "Consumer Staples", "industry": "Food"},
           "BBD.US": {"market_cap": 3.0e10, "interest_debt": 5.0e9,
                      "sector": "Financials", "industry": "Banks"}}
    f_cands = [{"symbol": "2269.T", "name": "Meiji Holdings", "action": "",
                "roi_pct": 17.5, "confidence_band": "High",
                "market_value_sar": 18000, "sector": ""},
               {"symbol": "BBD.US", "name": "Banco Bradesco", "action": "",
                "roi_pct": 9.0, "confidence_band": "High",
                "market_value_sar": 8000, "sector": ""},
               {"symbol": "4502.T", "name": "Takeda", "action": "",
                "roi_pct": 12.0, "confidence_band": "High",
                "market_value_sar": 15000, "sector": "Health Care"}]
    f_rows, _ = evaluate_board(f_cands, {}, {}, 130000, fnd)
    fb = {r[0]: r for r in f_rows}
    checks.append(("fundamentals -> Meiji real MODEL_SCREEN pass + ratio + YES",
                   fb["2269.T"][6] == "MODEL_SCREEN_PASS_NOT_FATWA"
                   and fb["2269.T"][15] == 18.2 and fb["2269.T"][16] == "YES"))
    checks.append(("fundamentals -> bank fails the screen",
                   fb["BBD.US"][6] == "MODEL_SCREEN_FAIL"
                   and fb["BBD.US"][16] == "NO"))
    checks.append(("no fundamentals -> honest UNKNOWN persists",
                   fb["4502.T"][6] == "UNKNOWN"))
    checks.append(("yahoo symbol mapping (.US stripped, natives pass)",
                   _yahoo_symbol("TRMD.US") == "TRMD"
                   and _yahoo_symbol("2269.T") == "2269.T"
                   and _yahoo_symbol("0083.HK") == "0083.HK"))
    checks.append(("coverage honesty: empty shell rejected, partial accepted",
                   not _fnd_valid({"market_cap": None, "interest_debt": None,
                                   "sector": "", "industry": ""})
                   and _fnd_valid({"market_cap": None, "interest_debt": None,
                                   "sector": "Energy", "industry": ""})))
    checks.append(("eligibility filter reads the YES column",
                   eligible_symbols(rows) == {"7010.SR"}))
    rb = build_risk_block(
        [{"symbol": "6960.T", "name": "Fukuda Denshi", "sector": "Healthcare"},
         {"symbol": "4503.T", "name": "Astellas Pharma", "sector": "Healthcare"},
         {"symbol": "2269.T", "name": "Meiji Holdings", "sector": "Consumer Defensive"},
         {"symbol": "TNK.US", "name": "Teekay Tankers", "sector": "Energy"},
         {"symbol": "EXE.US", "name": "Expand Energy", "sector": "Energy"}],
        {"6960.T", "4503.T", "2269.T", "TNK.US", "EXE.US"})
    checks.append(("risk block: live board reports BREACH + Japan 60%",
                   rb["verdict"] == "BREACH" and rb["country"]["Japan"] == 60.0))
    checks.append(("risk block: heat is correlation-adjusted below naive",
                   0 < rb["effective_heat_pct"] < rb["naive_heat_pct"]))
    checks.append(("risk block: structural infeasibility surfaced",
                   rb["feasible"] is False and "ADDING" in rb["note"]))
    checks.append(("risk block: only the eligible subset is scored",
                   build_risk_block(
                       [{"symbol": "A.US", "name": "A", "sector": "Energy"},
                        {"symbol": "B.T", "name": "B", "sector": "Utilities"}],
                       {"A.US"})["country"] == {"USA": 100.0}))
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
