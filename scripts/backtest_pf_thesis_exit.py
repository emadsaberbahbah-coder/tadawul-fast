#!/usr/bin/env python3
"""scripts/backtest_pf_thesis_exit.py — P-169 EVIDENCE: the thesis-failure exit rule replayed on the program's own data
================================================================================
VERSION 1.0.0 (2026-09-24) — READ-ONLY. Never writes a cell, never places an order.

WHY. The entry gate requires overall >= 68 and reliability >= 70; the HOLD side
requires nothing, so a holding that no longer meets its own entry thesis stays
HOLD until a broker stop (-7..-10%) fires (P-169, register candidate 09-24). Before
that asymmetry is closed with a live rule (portfolio_actions v1.13.0,
TFB_PF_THESIS_EXIT), this script measures what the rule WOULD have done on the
lots the ledger already holds — the stop-outs included (16 of 31 closed lots), so
"exit sooner" is not assumed to be free.

RULE UNDER TEST (per lot, from its Buy Date):
  walk the symbol's Signal_History snapshots in date order; count CONSECUTIVE
  snapshots with Overall Score < S; when the count reaches N, exit at the NEXT
  snapshot's price (never the same snapshot — no look-ahead); an active lot with
  no trigger is "still held". Grid: S in {60, 65, 68} x N in {3, 5, 7}.

INPUTS
  --export-dir DIR   browser export TSVs ("_Market Share Deepseek-V3 - <Tab>.tsv" or
                     "..._-_<Tab>.tsv"): _Portfolio_CostBasis (required), Signal_History
                     (required for the replay; without it only the ledger summary and a
                     coverage verdict are produced), Performance_Log (optional).
  --live             read the same tabs from the workbook (gspread, read-only scopes;
                     DEFAULT_SPREADSHEET_ID | SPREADSHEET_ID | TARGET_SHEET_ID +
                     GOOGLE_APPLICATION_CREDENTIALS | GOOGLE_SHEETS_CREDENTIALS(_B64)).
  --selftest         offline proof of the mechanics on synthetic series + four real lot shapes.
OUTPUTS
  --out-md FILE      markdown evidence (default artifacts/thesis_exit_backtest.md)
  --out-json FILE    machine-readable evidence (default artifacts/thesis_exit_backtest.json)
Exit code 0 always (an instrument, not a blocker); a missing Signal_History is reported, not hidden.

WHAT THIS DOES NOT DO: it does not model fees/FX drift/dividends in the rule leg (both
legs are compared PRICE-ONLY in the lot's currency x the ledger's FX->SAR; the ledger's
own Total Return incl. dividends is shown beside them), does not model slippage, and does
not decide anything — it produces the evidence the enforce sitting requires.
"""
from __future__ import annotations

import argparse
import base64
import csv
import glob
import json
import os
import re
import sys
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

VERSION = "1.0.0"
GRID_S = (60.0, 65.0, 68.0)
GRID_N = (3, 5, 7)
DEFAULT_S, DEFAULT_N = 60.0, 5
WHIPSAW_LOOKAHEAD = 10       # snapshots after a rule exit in which a +3% recovery counts as a whipsaw
WHIPSAW_PCT = 3.0
SUKUK_HINT = ("SUKUK", "5023.SR")


# ------------------------------------------------------------------ helpers
def _s(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _f(v: Any) -> Optional[float]:
    s = _s(v).replace(",", "").replace("%", "").replace("\u25b2", "").replace("\u25bc", "").strip()
    if s in ("", "-", "\u2014", "n/a", "N/A", "None", "nan"):
        return None
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1]
    try:
        x = float(s)
    except Exception:
        return None
    return -x if neg else x


def _d(v: Any) -> Optional[date]:
    s = _s(v)[:10]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None


# ------------------------------------------------------------------ sources
class Source:
    name = "abstract"
    errors: List[str]

    def tab(self, name: str) -> List[List[str]]:  # pragma: no cover
        raise NotImplementedError


class ExportSource(Source):
    """Browser-export TSVs; tab names matched on the filename suffix."""
    name = "export"

    def __init__(self, folder: str):
        self.folder = folder
        self.errors = []
        self.files = sorted(glob.glob(os.path.join(folder, "*.tsv")) + glob.glob(os.path.join(folder, "*.csv")))

    def _find(self, tab: str) -> Optional[str]:
        pats = [tab, tab.replace("_", " ")]
        for p in self.files:
            base = os.path.basename(p)
            stem = re.sub(r"\.(tsv|csv)$", "", base, flags=re.I)
            stem = re.sub(r"\(\d+\)$", "", stem).strip()
            for t in pats:
                if stem.endswith(" - " + t) or stem.endswith("_-_" + t) or stem == t:
                    return p
        return None

    def tab(self, tab: str) -> List[List[str]]:
        p = self._find(tab)
        if not p:
            self.errors.append(f"{tab}: not in export")
            return []
        try:
            with open(p, encoding="utf-8-sig", newline="") as fh:
                dialect = csv.excel_tab if p.lower().endswith(".tsv") else csv.excel
                return [list(r) for r in csv.reader(fh, dialect=dialect)]
        except Exception as e:
            self.errors.append(f"{tab}: unreadable ({type(e).__name__})")
            return []


class LiveSource(Source):
    name = "live"

    def __init__(self, sheet_id: str):
        self.sheet_id = sheet_id
        self.errors = []
        self.gc = self._client()
        self.book = self.gc.open_by_key(sheet_id)

    @staticmethod
    def _client():
        import gspread  # CI-only import
        from google.oauth2 import service_account
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        path = _s(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
        if path and os.path.exists(path):
            creds = service_account.Credentials.from_service_account_file(path, scopes=scopes)
        else:
            raw = (_s(os.getenv("GOOGLE_SHEETS_CREDENTIALS")) or _s(os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64"))
                   or _s(os.getenv("GOOGLE_CREDENTIALS")))
            if not raw:
                raise RuntimeError("no Google credentials in env")
            s = raw
            if not s.startswith("{"):
                try:
                    dec = base64.b64decode(s).decode("utf-8", errors="replace").strip()
                    if dec.startswith("{"):
                        s = dec
                except Exception:
                    pass
            creds = service_account.Credentials.from_service_account_info(json.loads(s), scopes=scopes)
        return gspread.authorize(creds)

    def tab(self, tab: str) -> List[List[str]]:
        try:
            ws = self.book.worksheet(tab)
            return [list(r) for r in ws.get_all_values()]
        except Exception as e:
            self.errors.append(f"{tab}: {type(e).__name__}")
            return []


class MemSource(Source):
    name = "mem"

    def __init__(self, tabs: Dict[str, List[List[str]]]):
        self.tabs = tabs
        self.errors = []

    def tab(self, tab: str) -> List[List[str]]:
        if tab not in self.tabs:
            self.errors.append(f"{tab}: not in fixture")
        return [list(r) for r in self.tabs.get(tab, [])]


# ------------------------------------------------------------------ parsing
@dataclass
class Lot:
    symbol: str
    name: str
    ccy: str
    status: str            # Active | Inactive
    buy_date: Optional[date]
    buy_price: Optional[float]
    shares: Optional[float]
    sell_date: Optional[date]
    sell_price: Optional[float]
    fx: float
    cost_basis: Optional[float]      # native ccy (buy price x shares + fees, ledger-computed)
    current_price: Optional[float]
    dividends: Optional[float]
    total_return_sar: Optional[float]
    holding_days: Optional[float]

    @property
    def is_sukuk(self) -> bool:
        blob = (self.symbol + " " + self.name).upper()
        return any(h in blob for h in SUKUK_HINT)

    def price_pnl_sar(self, exit_price: Optional[float]) -> Optional[float]:
        """PRICE-ONLY P&L in SAR: (exit - buy) x shares x FX. None when unknown."""
        if exit_price is None or self.buy_price is None or self.shares is None:
            return None
        return round((exit_price - self.buy_price) * self.shares * self.fx, 2)


def _header_index(rows: List[List[str]], must: str, max_scan: int = 8) -> Tuple[int, Dict[str, int]]:
    for i, r in enumerate(rows[:max_scan]):
        if must in [_s(c) for c in r]:
            return i, {_s(c): j for j, c in enumerate(r) if _s(c)}
    return -1, {}


def parse_ledger(rows: List[List[str]]) -> List[Lot]:
    hi, idx = _header_index(rows, "Cost Basis")
    if hi < 0 or "Symbol" not in idx:
        return []
    g = lambda r, k: (r[idx[k]] if k in idx and idx[k] < len(r) else "")
    out: List[Lot] = []
    for r in rows[hi + 1:]:
        sym = _s(g(r, "Symbol"))
        if not sym or sym.startswith("TOTALS"):
            continue
        out.append(Lot(
            symbol=sym, name=_s(g(r, "Name")), ccy=_s(g(r, "Ccy")) or "USD",
            status=_s(g(r, "Status")) or "Active",
            buy_date=_d(g(r, "Buy Date")), buy_price=_f(g(r, "Buy Price")), shares=_f(g(r, "Shares")),
            sell_date=_d(g(r, "Sell Date")), sell_price=_f(g(r, "Sell Price")),
            fx=_f(g(r, "FX\u2192SAR")) or _f(g(r, "FX->SAR")) or 1.0,
            cost_basis=_f(g(r, "Cost Basis")), current_price=_f(g(r, "Current Price")),
            dividends=_f(g(r, "Dividends Recv")), total_return_sar=_f(g(r, "Total Return SAR")),
            holding_days=_f(g(r, "Holding Days")),
        ))
    return out


@dataclass
class Snap:
    day: date
    score: Optional[float]
    price: Optional[float]
    rec: str = ""
    inv: str = ""
    rel: Optional[float] = None


def parse_signal_history(rows: List[List[str]]) -> Dict[str, List[Snap]]:
    hi, idx = _header_index(rows, "Overall Score", max_scan=6)
    if hi < 0 or "Symbol" not in idx:
        return {}
    g = lambda r, k: (r[idx[k]] if k in idx and idx[k] < len(r) else "")
    by: Dict[str, Dict[date, Snap]] = {}
    for r in rows[hi + 1:]:
        sym = _s(g(r, "Symbol")).upper()
        d = _d(g(r, "Date (Riyadh)")) or _d(g(r, "Recorded At (Riyadh)"))
        if not sym or not d:
            continue
        sn = Snap(day=d, score=_f(g(r, "Overall Score")), price=_f(g(r, "Price")),
                  rec=_s(g(r, "Recommendation")), inv=_s(g(r, "Investability")),
                  rel=_f(g(r, "Forecast Reliability")))
        by.setdefault(sym, {})[d] = sn          # one snapshot per symbol per day (the store's own contract)
    return {s: [m[k] for k in sorted(m)] for s, m in by.items()}


# ------------------------------------------------------------------ the rule
@dataclass
class Trigger:
    fired: bool
    exit_day: Optional[date] = None
    exit_price: Optional[float] = None
    streak_start: Optional[date] = None
    streak_days: int = 0
    snapshots_in_window: int = 0
    snapshots_below: int = 0
    note: str = ""


def simulate(lot: Lot, series: List[Snap], score_thr: float, n_days: int) -> Trigger:
    """Walk the lot's holding window (Buy Date .. Sell Date or open end). No look-ahead:
    the exit is priced at the FIRST snapshot AFTER the one that completes the streak."""
    if lot.buy_date is None:
        return Trigger(False, note="no buy date")
    end = lot.sell_date
    win = [s for s in series if s.day >= lot.buy_date and (end is None or s.day <= end)]
    t = Trigger(False, snapshots_in_window=len(win))
    if not win:
        t.note = "no snapshots in window"
        return t
    streak = 0
    start: Optional[date] = None
    for i, s in enumerate(win):
        if s.score is None:
            continue                                  # unscored day: neither breaks nor extends
        if s.score < score_thr:
            t.snapshots_below += 1
            streak += 1
            start = start or s.day
            if streak >= n_days:
                nxt = next((x for x in win[i + 1:] if x.price is not None), None)
                if nxt is None:
                    t.note = "streak complete, no later priced snapshot (still held)"
                    t.streak_days, t.streak_start = streak, start
                    return t
                return Trigger(True, exit_day=nxt.day, exit_price=nxt.price, streak_start=start,
                               streak_days=streak, snapshots_in_window=len(win),
                               snapshots_below=t.snapshots_below)
        else:
            streak, start = 0, None
    t.note = "no trigger"
    return t


def whipsaw(series: List[Snap], exit_day: date, exit_price: float) -> Tuple[bool, Optional[float]]:
    """True when price recovers >= WHIPSAW_PCT above the exit within WHIPSAW_LOOKAHEAD snapshots."""
    after = [s for s in series if s.day > exit_day and s.price is not None][:WHIPSAW_LOOKAHEAD]
    if not after:
        return False, None
    hi = max(s.price for s in after)
    return (hi >= exit_price * (1 + WHIPSAW_PCT / 100.0)), round(100.0 * (hi / exit_price - 1), 2)


# ------------------------------------------------------------------ the study
@dataclass
class LotResult:
    symbol: str
    status: str
    buy_date: str
    sell_date: str
    holding_days: Optional[float]
    coverage: str                 # snapshots in window / calendar days
    actual_price_pnl_sar: Optional[float]
    ledger_total_return_sar: Optional[float]
    rule: Dict[str, Any]          # for the default (S, N)
    rule_price_pnl_sar: Optional[float]
    delta_sar: Optional[float]    # rule - actual (price-only)
    whipsaw: Optional[bool]
    recovery_pct: Optional[float]
    days_below_before_actual_exit: Optional[int]


def run_study(lots: List[Lot], hist: Dict[str, List[Snap]], score_thr: float, n_days: int) -> Dict[str, Any]:
    results: List[LotResult] = []
    grid: Dict[str, Dict[str, Any]] = {}
    for S in GRID_S:
        for N in GRID_N:
            grid[f"S{S:g}_N{N}"] = {"fired": 0, "evaluable": 0, "delta_sar_sum": 0.0, "whipsaws": 0}
    for lot in lots:
        if lot.is_sukuk:
            continue
        series = hist.get(lot.symbol.upper(), [])
        end = lot.sell_date or date.today()
        cal = max(1, (end - lot.buy_date).days) if lot.buy_date else 0
        # actual price-only outcome
        actual_exit = lot.sell_price if lot.status.lower() == "inactive" else lot.current_price
        actual_pnl = lot.price_pnl_sar(actual_exit)
        # grid
        for S in GRID_S:
            for N in GRID_N:
                tr = simulate(lot, series, S, N)
                cell = grid[f"S{S:g}_N{N}"]
                if tr.snapshots_in_window >= N and actual_pnl is not None:
                    cell["evaluable"] += 1
                    if tr.fired:
                        cell["fired"] += 1
                        rp = lot.price_pnl_sar(tr.exit_price)
                        if rp is not None:
                            cell["delta_sar_sum"] += rp - actual_pnl
                        ws, _ = whipsaw(series, tr.exit_day, tr.exit_price)
                        cell["whipsaws"] += 1 if ws else 0
        tr = simulate(lot, series, score_thr, n_days)
        rule_pnl = lot.price_pnl_sar(tr.exit_price) if tr.fired else (actual_pnl if tr.snapshots_in_window else None)
        ws, rec = (whipsaw(series, tr.exit_day, tr.exit_price) if tr.fired else (None, None))
        # how many below-threshold snapshots preceded the ACTUAL exit of a closed lot
        below_before = None
        if lot.status.lower() == "inactive" and lot.sell_date and series:
            below_before = sum(1 for s in series if lot.buy_date and lot.buy_date <= s.day <= lot.sell_date
                               and s.score is not None and s.score < score_thr)
        results.append(LotResult(
            symbol=lot.symbol, status=lot.status, buy_date=str(lot.buy_date or ""), sell_date=str(lot.sell_date or ""),
            holding_days=lot.holding_days, coverage=f"{tr.snapshots_in_window}/{cal}d",
            actual_price_pnl_sar=actual_pnl, ledger_total_return_sar=lot.total_return_sar,
            rule={"fired": tr.fired, "exit_day": str(tr.exit_day or ""), "exit_price": tr.exit_price,
                  "streak_start": str(tr.streak_start or ""), "streak_days": tr.streak_days,
                  "snapshots_below": tr.snapshots_below, "note": tr.note},
            rule_price_pnl_sar=rule_pnl,
            delta_sar=(round(rule_pnl - actual_pnl, 2) if (rule_pnl is not None and actual_pnl is not None) else None),
            whipsaw=ws, recovery_pct=rec, days_below_before_actual_exit=below_before,
        ))
    evaluable = [r for r in results if r.rule["fired"] or "no trigger" in r.rule["note"]]
    fired = [r for r in results if r.rule["fired"]]
    delta = sum(r.delta_sar for r in fired if r.delta_sar is not None)
    return {
        "version": VERSION, "rule": {"score_thr": score_thr, "n_days": n_days, "grid_S": GRID_S, "grid_N": GRID_N,
                                     "whipsaw_lookahead": WHIPSAW_LOOKAHEAD, "whipsaw_pct": WHIPSAW_PCT},
        "lots_total": len(lots), "lots_equity": sum(1 for l in lots if not l.is_sukuk),
        "lots_with_history": sum(1 for r in results if not r.rule["note"].startswith("no snapshots")),
        "evaluable": len(evaluable), "fired": len(fired),
        "whipsaws": sum(1 for r in fired if r.whipsaw),
        "delta_price_pnl_sar": round(delta, 2),
        "grid": grid, "lots": [asdict(r) for r in results],
    }


# ------------------------------------------------------------------ rendering
def render_md(study: Dict[str, Any], ledger_summary: Dict[str, Any], source: str, errors: List[str]) -> str:
    L = []
    L.append(f"# P-169 thesis-failure exit — backtest evidence v{VERSION}")
    L.append(f"Source: {source} · generated {datetime.now().isoformat(timespec='seconds')} · READ-ONLY")
    if errors:
        L.append("")
        L.append("**Source notes:** " + "; ".join(errors))
    L.append("")
    L.append("## Ledger (the program's own record)")
    L.append(f"- lots: {ledger_summary['lots']} (active {ledger_summary['active']}, closed {ledger_summary['closed']}); "
             f"closed winners {ledger_summary['winners']} / losers {ledger_summary['losers']}; "
             f"closed total return {ledger_summary['closed_total_return_sar']:,.0f} SAR; "
             f"active price-only P&L {ledger_summary['active_price_pnl_sar']:,.0f} SAR")
    L.append("")
    r = study["rule"]
    L.append(f"## Rule S<{r['score_thr']:g} for N={r['n_days']} consecutive snapshots — exit at the next snapshot")
    L.append(f"- equity lots {study['lots_equity']}, with Signal_History coverage {study['lots_with_history']}, "
             f"evaluable {study['evaluable']}, rule fired on {study['fired']}, whipsaws {study['whipsaws']} "
             f"(+{r['whipsaw_pct']:g}% within {r['whipsaw_lookahead']} snapshots)")
    L.append(f"- **net delta vs actual (price-only, SAR): {study['delta_price_pnl_sar']:+,.0f}**")
    if study["lots_with_history"] == 0:
        L.append("- VERDICT: NOT DECIDABLE — Signal_History was not available to this run; the rule cannot be scored. "
                 "Re-run with the Signal_History tab in the export or with --live.")
    L.append("")
    L.append("| Symbol | Status | Buy | Sell | Cov | Actual P&L | Rule exit | Rule P&L | Δ | Whipsaw | below<S before actual exit |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for x in study["lots"]:
        ru = x["rule"]
        L.append("| {sym} | {st} | {b} | {s} | {cov} | {a} | {re} | {rp} | {d} | {w} | {nb} |".format(
            sym=x["symbol"], st=x["status"], b=x["buy_date"], s=x["sell_date"] or "open", cov=x["coverage"],
            a=("" if x["actual_price_pnl_sar"] is None else f"{x['actual_price_pnl_sar']:+,.0f}"),
            re=(f"{ru['exit_day']} @ {ru['exit_price']:g}" if ru["fired"] else ru["note"]),
            rp=("" if x["rule_price_pnl_sar"] is None else f"{x['rule_price_pnl_sar']:+,.0f}"),
            d=("" if x["delta_sar"] is None else f"{x['delta_sar']:+,.0f}"),
            w=("" if x["whipsaw"] is None else ("YES" if x["whipsaw"] else "no")),
            nb=("" if x["days_below_before_actual_exit"] is None else x["days_below_before_actual_exit"])))
    L.append("")
    L.append("## Grid (S × N): fired / evaluable · net Δ SAR · whipsaws")
    L.append("| | " + " | ".join(f"N={n}" for n in r["grid_N"]) + " |")
    L.append("|---|" + "---|" * len(r["grid_N"]))
    for S in r["grid_S"]:
        cells = []
        for N in r["grid_N"]:
            c = study["grid"][f"S{S:g}_N{N}"]
            cells.append(f"{c['fired']}/{c['evaluable']} · {c['delta_sar_sum']:+,.0f} · ws {c['whipsaws']}")
        L.append(f"| S<{S:g} | " + " | ".join(cells) + " |")
    L.append("")
    L.append("Reading: a positive Δ means the rule would have left the book better off on that lot (price-only); "
             "whipsaws are exits the market reversed within the look-ahead. Coverage 0/…d rows are lots the tracker "
             "never snapshotted — they are not evidence either way.")
    return "\n".join(L)


def ledger_summary(lots: List[Lot]) -> Dict[str, Any]:
    closed = [l for l in lots if l.status.lower() == "inactive"]
    active = [l for l in lots if l.status.lower() != "inactive"]
    ctr = [l.total_return_sar for l in closed if l.total_return_sar is not None]
    apx = [l.price_pnl_sar(l.current_price) for l in active]
    return {"lots": len(lots), "active": len(active), "closed": len(closed),
            "winners": sum(1 for v in ctr if v > 0), "losers": sum(1 for v in ctr if v < 0),
            "closed_total_return_sar": round(sum(ctr), 2),
            "active_price_pnl_sar": round(sum(v for v in apx if v is not None), 2)}


# ------------------------------------------------------------------ selftest
def _selftest() -> int:
    def series(start: str, scores: List[Optional[float]], prices: List[float]) -> List[List[str]]:
        d0 = _d(start)
        rows = [["Snapshot ID", "Key", "Symbol", "Date (Riyadh)", "Recorded At (Riyadh)", "Recommendation",
                 "Final Action", "Investability", "Overall Score", "Forecast Reliability", "Data Quality",
                 "Risk Score", "Price", "Origin Tab"]]
        from datetime import timedelta
        for i, (sc, p) in enumerate(zip(scores, prices)):
            dd = d0 + timedelta(days=i)
            rows.append(["id%d" % i, "K", "T.US", dd.isoformat(), dd.isoformat() + " 08:00", "HOLD", "HOLD",
                         "WATCHLIST", "" if sc is None else str(sc), "70", "90", "50", str(p), "My_Portfolio"])
        return rows

    def ledger(rows_extra: List[List[str]]) -> List[List[str]]:
        hdr = ["Symbol", "Name", "Ccy", "Status", "Buy Date", "Buy Price", "Shares", "Buy Fees", "Sell Date",
               "Sell Price", "Sell Fees", "Dividends Recv", "Notes", "Cost Basis", "Current Price", "FX\u2192SAR",
               "Market Value SAR", "Unrealized G/L SAR", "Sell Proceeds", "Realized G/L SAR", "Total Return SAR",
               "Return %", "Holding Days"]
        return [["_Portfolio_CostBasis \u2014 LEDGER"], ["Status:", "x"], ["legend"], hdr] + rows_extra

    passed = 0
    # T1: five consecutive sub-60 snapshots -> exit priced at the SIXTH snapshot (no look-ahead)
    sh = series("2026-09-01", [70, 59, 58, 57, 56, 55, 54, 60, 61, 62], [100, 99, 98, 97, 96, 95, 94, 93, 99, 100])
    lg = ledger([["T.US", "Test", "USD", "Active", "2026-09-01", "100.00", "10", "", "", "", "", "0.00", "", "1,000.00",
                  "100.00", "3.7601", "", "", "", "", "", "", "9"]])
    lots = parse_ledger(lg); hist = parse_signal_history(sh)
    tr = simulate(lots[0], hist["T.US"], 60.0, 5)
    assert tr.fired and str(tr.exit_day) == "2026-09-07" and tr.exit_price == 94.0 and tr.streak_days == 5, asdict(tr)
    passed += 1
    # T2: the streak resets on a >= S day; N=7 never fires on that series
    assert not simulate(lots[0], hist["T.US"], 60.0, 7).fired
    sh2 = series("2026-09-01", [59, 59, 59, 65, 59, 59, 59, 59, 59, 59], [100] * 10)
    h2 = parse_signal_history(sh2)
    t2 = simulate(lots[0], h2["T.US"], 60.0, 5)
    assert t2.fired and str(t2.streak_start) == "2026-09-05" and str(t2.exit_day) == "2026-09-10", asdict(t2)
    passed += 1
    # T3: unscored snapshots neither break nor extend the streak
    sh3 = series("2026-09-01", [59, None, 59, None, 59, 59, 59, 61], [100] * 8)
    t3 = simulate(lots[0], parse_signal_history(sh3)["T.US"], 60.0, 5)
    assert t3.fired and str(t3.exit_day) == "2026-09-08", asdict(t3)
    passed += 1
    # T4: streak completes on the last snapshot -> still held (no later price), not fired
    sh4 = series("2026-09-01", [59, 59, 59, 59, 59], [100] * 5)
    t4 = simulate(lots[0], parse_signal_history(sh4)["T.US"], 60.0, 5)
    assert not t4.fired and "still held" in t4.note and t4.streak_days == 5
    passed += 1
    # T5: the holding window clips the series (snapshots before Buy Date / after Sell Date ignored)
    lg5 = ledger([["T.US", "Test", "USD", "Inactive", "2026-09-03", "100.00", "10", "", "2026-09-06", "97.00", "", "0.00", "",
                   "1,000.00", "\u2014", "3.7601", "\u2014", "\u2014", "970.00", "(113)", "(113)", "-3.0%", "3"]])
    l5 = parse_ledger(lg5)[0]
    t5 = simulate(l5, hist["T.US"], 60.0, 3)
    assert t5.snapshots_in_window == 4 and t5.fired and str(t5.exit_day) == "2026-09-06" and t5.exit_price == 95.0, asdict(t5)
    assert l5.price_pnl_sar(t5.exit_price) == round((95 - 100) * 10 * 3.7601, 2) and l5.total_return_sar == -113.0
    passed += 1
    # T6: whipsaw detection and the study/grid aggregate on the T1 series
    ws, rec = whipsaw(hist["T.US"], _d("2026-09-07"), 94.0)
    assert ws is True and rec == round(100 * (100 / 94 - 1), 2)
    st = run_study(lots, hist, 60.0, 5)
    assert st["fired"] == 1 and st["whipsaws"] == 1 and st["lots_with_history"] == 1
    x = st["lots"][0]
    assert x["rule_price_pnl_sar"] == round((94 - 100) * 10 * 3.7601, 2) and x["actual_price_pnl_sar"] == 0.0
    assert x["delta_sar"] == round((94 - 100) * 10 * 3.7601, 2)
    assert st["grid"]["S60_N5"]["fired"] == 1 and st["grid"]["S60_N7"]["fired"] == 0 and st["grid"]["S68_N3"]["fired"] == 1
    passed += 1
    # T7: real lot shapes from the 2026-09-24 ledger (dates/prices/qty verbatim) with synthetic scores:
    #     YUM (open, 24 sh @144.01), SBAC (open, 21 @181.01), VEL (stopped 09-16 @16.97), EPRT (stopped 09-11 @28.955)
    real = ledger([
        ["YUM.US", "Yum! Brands", "USD", "Active", "2026-08-12", "144.01", "24", "", "", "", "", "18.00", "", "3,473.04", "140.63", "3.7601", "12,690.79", "(368)", "", "", "(301)", "-2.3%", "43"],
        ["SBAC.US", "SBA Communications", "USD", "Active", "2026-09-08", "181.01", "21", "", "", "", "", "26.25", "", "3,801.21", "170.53", "3.7601", "13,465.41", "(828)", "", "", "(729)", "-5.8%", "16"],
        ["VEL.US", "Velocity Financial", "USD", "Inactive", "2026-09-09", "18.19", "24", "", "2026-09-16", "16.97", "", "0.00", "", "436.56", "\u2014", "3.7601", "\u2014", "\u2014", "407.28", "(110)", "(110)", "-6.7%", "7"],
        ["EPRT.US", "Essential Properties", "USD", "Inactive", "2026-08-27", "30.64", "100", "", "2026-09-11", "28.955", "", "0.00", "", "3,064.00", "\u2014", "3.7601", "\u2014", "\u2014", "2,895.50", "(627)", "(627)", "-5.5%", "15"],
        ["5023.SR", "Arabian Centres Sukuk", "SAR", "Active", "2025-11-23", "100.00", "100", "", "", "", "", "637.50", "", "10,000.00", "100.50", "1.0000", "10,050.00", "50", "", "", "688", "0.5%", "305"],
    ])
    rl = parse_ledger(real)
    assert len(rl) == 5 and rl[4].is_sukuk and rl[0].fx == 3.7601 and rl[2].sell_date == _d("2026-09-16")
    # SBAC synthetic series: scores slide below 60 from day 3 of the hold; exit lands on 09-15 @ 178
    sbac = [["Snapshot ID", "Key", "Symbol", "Date (Riyadh)", "Recorded At (Riyadh)", "Recommendation", "Final Action",
             "Investability", "Overall Score", "Forecast Reliability", "Data Quality", "Risk Score", "Price", "Origin Tab"]]
    days = ["2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17"]
    sc = [66, 61, 59, 58, 57, 56, 55, 57]
    px = [181.0, 180.2, 179.5, 179.0, 178.6, 178.0, 176.9, 175.5]
    for i, dd in enumerate(days):
        sbac.append(["s%d" % i, "K", "SBAC.US", dd, dd + " 08:00", "HOLD", "HOLD", "WATCHLIST", str(sc[i]), "71.5", "100", "40", str(px[i]), "My_Portfolio"])
    hs = parse_signal_history(sbac)
    st7 = run_study(rl, hs, 60.0, 5)
    row = next(x for x in st7["lots"] if x["symbol"] == "SBAC.US")
    assert row["rule"]["fired"] and row["rule"]["exit_day"] == "2026-09-17" and row["rule"]["exit_price"] == 175.5, row["rule"]
    assert row["actual_price_pnl_sar"] == round((170.53 - 181.01) * 21 * 3.7601, 2)
    assert row["delta_sar"] == round((175.5 - 170.53) * 21 * 3.7601, 2) and row["delta_sar"] > 0
    assert sum(1 for x in st7["lots"] if x["symbol"] == "5023.SR") == 0        # sukuk exempt
    yum = next(x for x in st7["lots"] if x["symbol"] == "YUM.US")
    assert yum["rule"]["note"] == "no snapshots in window" and yum["delta_sar"] is None   # no coverage = no evidence
    md = render_md(st7, ledger_summary(rl), "selftest", [])
    assert "SBAC.US" in md and "NOT DECIDABLE" not in md
    js = json.dumps(st7, default=str)
    assert '"fired": 1' in js
    passed += 1
    # T8: no Signal_History -> NOT DECIDABLE verdict, exit 0 path
    st8 = run_study(rl, {}, 60.0, 5)
    md8 = render_md(st8, ledger_summary(rl), "selftest", ["Signal_History: not in export"])
    assert st8["lots_with_history"] == 0 and "NOT DECIDABLE" in md8
    passed += 1
    # T9: parenthesised negatives, em-dash blanks, FX arrows, and the sukuk hint in the real header shape
    assert _f("(1,234.50)") == -1234.5 and _f("\u2014") is None and _f("\u25b2 12.5%") == 12.5
    ls = ledger_summary(rl)
    assert ls["closed"] == 2 and ls["losers"] == 2 and ls["closed_total_return_sar"] == -737.0
    passed += 1
    print(f"selftest: PASS {passed}/9 (streak/no-look-ahead, reset, unscored days, still-held, window clip, "
          f"whipsaw+grid, real lot shapes, not-decidable, parsing)")
    return 0


# ------------------------------------------------------------------ main
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--export-dir")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--sheet-id")
    ap.add_argument("--score", type=float, default=DEFAULT_S)
    ap.add_argument("--days", type=int, default=DEFAULT_N)
    ap.add_argument("--out-md", default="artifacts/thesis_exit_backtest.md")
    ap.add_argument("--out-json", default="artifacts/thesis_exit_backtest.json")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args(argv)
    if a.selftest:
        return _selftest()
    if a.live:
        sid = a.sheet_id or _s(os.getenv("DEFAULT_SPREADSHEET_ID")) or _s(os.getenv("SPREADSHEET_ID")) or _s(os.getenv("TARGET_SHEET_ID"))
        if not sid:
            print("no sheet id (DEFAULT_SPREADSHEET_ID | SPREADSHEET_ID | TARGET_SHEET_ID)"); return 0
        src: Source = LiveSource(sid)
    elif a.export_dir:
        src = ExportSource(a.export_dir)
    else:
        ap.print_help(); return 0
    lots = parse_ledger(src.tab("_Portfolio_CostBasis"))
    hist = parse_signal_history(src.tab("Signal_History"))
    if not lots:
        print("ledger unreadable — nothing to score"); return 0
    study = run_study(lots, hist, a.score, a.days)
    ls = ledger_summary(lots)
    md = render_md(study, ls, src.name, src.errors)
    for p in (a.out_md, a.out_json):
        d = os.path.dirname(p)
        if d:
            os.makedirs(d, exist_ok=True)
    with open(a.out_md, "w", encoding="utf-8") as fh:
        fh.write(md + "\n")
    with open(a.out_json, "w", encoding="utf-8") as fh:
        json.dump({"study": study, "ledger": ls, "source": src.name, "errors": src.errors,
                   "generated": datetime.now().isoformat(timespec="seconds")}, fh, indent=1, default=str)
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
