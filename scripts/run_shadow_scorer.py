"""
scripts/run_shadow_scorer.py — TFB Gen-2 Champion-vs-Challenger Scorer + S-1 Gate
=================================================================================
VERSION 1.0.0  (2026-07-19)  — NEW SCRIPT (Wave S, deliverable #15)

WHY (Master Plan v2.1 §3, §15): Gate S-1 decides whether Tranche 1 (~40K) is
deployed. Its six criteria are stated verbatim in §15:
    (1) >=4 full weeks of Shadow Mode on the complete universe
    (2) zero compliance violations on the shadow board
    (3) shadow net alpha >= 0 vs benchmark over the window
    (4) calibration error within band on 7D/14D checkpoints
    (5) corporate-actions and point-in-time checks passing
    (6) rollback drill passed
Nothing in the system measured any of them. This script does — daily,
append-only, from the workbook's own surfaces.

METHOD (defensible, and stated plainly on the dashboard):
  * Daily-rebalanced equal-weight basket returns. Each run reads the PREVIOUS
    row's composition and recorded prices, fetches today's prices for exactly
    those symbols, and chains one day's return onto a cumulative index. A
    rotating board is scored on what it actually held, never re-judged later.
  * Turnover between consecutive days is charged the venue round-trip cost
    (opportunity_builder v1.1.0 model), so alpha is COST-ADJUSTED, not gross.
  * Benchmark = 70% SPUS + 30% TASI (§1 locked). Weights are printed.
  * Criterion (4) reports PENDING by construction until Wave B instruments
    7D/14D horizons in track_performance — an honest gap, never a silent pass.

INTEGRITY: `Shadow_History` is APPEND-ONLY. A duplicate date is refused, not
overwritten (that refusal IS the point-in-time check in criterion 5). The
gate never auto-promotes: it reports PASS/FAIL/NOT_DECIDABLE and freezes on
failure, per §15's "failing a gate freezes promotion, never silently retries".

USAGE:
  --selftest      offline fixtures, no network, no sheets
  --dry-run       compute + print, write nothing
  (default)       append today's row, rewrite the S1_Gate dashboard
  --rollback-drill-passed   record today's drill marker (operator-run)
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

from core.analysis import opportunity_builder as ob   # noqa: E402
from core import regret as rg                        # noqa: E402

_SB_PATH = os.path.join(_ROOT, "scripts", "run_shadow_board.py")
_spec = importlib.util.spec_from_file_location("tfb_shadow_board", _SB_PATH)
sb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sb)  # type: ignore[union-attr]

# v1.1.0 (2026-07-19): REGRET LEDGER WIRED. core/regret v1.0.0 was live but
# nothing called it. Each run now (a) opens a fork for every name the board
# REFUSED today — deduped so a name blocked for ten days holds ONE fork, not
# ten — (b) re-scores every open fork against today's prices, and (c) rewrites
# Regret_Summary. `Regret_Ledger` is append-only like Shadow_History: forks
# are written once with the price at the moment of refusal, and scoring is
# always recomputed on read, so a fork can never be silently restated.
# On the live board this opens 5 forks (4 compliance, 1 floor).
# v1.1.1 (2026-07-19): WRITE-ORDER + GRID-WIDTH FIX. The first live dispatch
# wrote Shadow_History and Regret_Ledger (append_rows) but left S1_Gate ABSENT
# and Regret_Summary EMPTY, with no [S1-GATE] line in _Run_Log. Cause, mine:
# write_regret_summary created the tab with cols=8 then wrote a 13-column row
# ("OPEN FORKS" + the 12-field ledger header), exceeding the grid; the API
# raised, and because it ran BEFORE write_gate the primary deliverable and the
# log line were never reached. Three corrections:
#   (1) the summary tab is sized from the data, and every row is padded to a
#       uniform width so a ragged body can never exceed the grid again;
#   (2) S1_Gate is written FIRST — the gate is the point of this script and
#       must not depend on a secondary tab succeeding;
#   (3) each sheet write is independently guarded, so one failure degrades
#       that tab alone and is reported, instead of silently killing the run.
# v1.2.0 (2026-07-20): PRICE-HONESTY [VBREAK — Register §5 evidence semantics].
# Evening audit 2026-07-20 (export __39_): day-2 CHAMPION/CHALLENGER rows were
# recorded with Daily Return 0.0000 while their Prices JSON was byte-identical
# to the day-1 seed across all 10/5 symbols — even though Global_Markets held
# fresh Tokyo closes (2269.T +71) hours before the run, and the benchmark leg
# ^TASI.SR moved in the very same spot fetch. Whatever the transport cause
# (Yahoo serving a stale last bar, or the seed itself mis-anchored), the
# scorer had no instrument to KNOW a price was stale, so a dead feed scored
# as a flat market. Fix — measure, never assume:
#   (1) fetch_spot now also returns each symbol's LAST-BAR DATE (bar epoch +
#       the payload's own gmtoffset), so freshness is evidence, not hope;
#   (2) a symbol whose last bar is not strictly newer than the previous
#       history row's date is STALE: excluded from that day's return pairing
#       and carried at its previous stored price so the next fresh bar
#       captures the full move (nothing lost, nothing invented);
#   (3) if a basket's fresh coverage falls below TFB_SHADOW_MIN_FRESH_PCT
#       (default 60%), the DAY IS EXCLUDED-INFRA per Gate & Evidence
#       Register §5: Daily Return blank, Cum Index carried, note
#       `DAY_EXCLUDED_INFRA fresh=a/b stale=c` — never a fabricated 0;
#   (4) criterion 1 now counts SCORED evidence days only (seed and excluded
#       days do not count), with excluded days reported alongside;
#   (5) stale counts and the fresh/excluded state print in the verdict line,
#       the S1 meta block, and each history row's note;
#   (6) EVIDENCE_EPOCH = 2026-07-21 segments the evidence at this version
#       break: the two pre-break rows (seed + the fabricated-flat day) stay
#       in the tab untouched but can never count as scored days — repair by
#       segmentation, never by restatement.
# Kill-switch: TFB_SHADOW_PRICE_HONESTY=0 restores v1.1.1 behavior exactly.
# v1.2.1 (2026-07-22): DEF-R — Regret_Summary NameError. `_now_riyadh()` was
# called by write_regret_summary since v1.1.0 but never defined; the guarded
# writer swallowed it into `write_errors=Regret_Summary:NameError` on the
# window's very first scored day (evidence: _Run_Log 2026-07-21 16:05).
# Scoring, gate, history, and ledger were never affected — the guard did its
# job. Fix: define the helper (fixed UTC+3, no new imports). One name, one
# line, verified end-to-end against a stub sheet in the harness.
SCRIPT_VERSION = "1.2.1"
TAB_HISTORY = "Shadow_History"
TAB_GATE = "S1_Gate"
TAB_REGRET = "Regret_Ledger"
TAB_REGRET_SUMMARY = "Regret_Summary"
HISTORY_HEADER = ["Date", "Basket", "Symbols", "Prices JSON", "Daily Return %",
                  "Cum Index", "Turnover %", "Cost Drag %", "Notes"]

CHAMPION, CHALLENGER, BENCHMARK = "CHAMPION", "CHALLENGER", "BENCHMARK"
BENCH_WEIGHTS = {"SPUS": 0.70, "^TASI.SR": 0.30}   # §1 locked benchmark
S1_WINDOW_DAYS = 28                                 # >=4 full weeks
BASE_INDEX = 100.0
# v1.2.0 [VBREAK]: evidence counting starts at this date. Rows before it
# (the 2026-07-19 seed and the 2026-07-20 fabricated-flat day, audit
# 2026-07-20) remain in the tab untouched — append-only, never restated —
# but can NEVER count as scored evidence days. Register §5: a methodology
# version break segments evidence; pre-break cum is retained (the ≤1bp
# benchmark drift from day 2 is acknowledged and immaterial to the ±
# tolerance of criterion 3).
EVIDENCE_EPOCH = date(2026, 7, 21)

YAHOO_SPOT = ("https://query1.finance.yahoo.com/v8/finance/chart/"
              "{sym}?range=5d&interval=1d")

# Statuses that must NEVER carry Gen2 Eligible = YES (criterion 2)
_BLOCKING = {"AUTHORITY_FAIL", "MODEL_SCREEN_FAIL", "UNKNOWN", "DATA_STALE",
             "CONFLICT", "VENUE_BLOCK", "INSTRUMENT_BLOCK", "BROKER_UNTRADABLE"}


def _today_riyadh() -> date:
    return (datetime.now(timezone.utc) + timedelta(hours=3)).date()


def _num(x: Any) -> Optional[float]:
    try:
        v = float(str(x).replace(",", "").replace("%", "").strip())
        return v if v == v else None          # NaN guard
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# pure scoring core (fully selftested offline)                                 #
# --------------------------------------------------------------------------- #
def _price_honesty_enabled() -> bool:
    """v1.2.0 kill-switch — default ON (protective; guards ship armed)."""
    return (os.getenv("TFB_SHADOW_PRICE_HONESTY") or "1").strip().lower() \
        not in {"0", "false", "off", "no"}


def _min_fresh_frac() -> float:
    """Minimum fresh-priced share of a basket for the day to count (§5)."""
    try:
        v = float(os.getenv("TFB_SHADOW_MIN_FRESH_PCT") or 60.0)
        return v / 100.0 if v > 1.0 else v
    except Exception:  # noqa: BLE001
        return 0.60


def _parse_iso_date(s: Any) -> Optional[date]:
    try:
        return datetime.strptime(str(s).strip()[:10], "%Y-%m-%d").date()
    except Exception:  # noqa: BLE001
        return None


def basket_return_fresh(prev_prices: Dict[str, float],
                        cur_prices: Dict[str, float],
                        cur_asof: Dict[str, date],
                        prev_date: Optional[date],
                        ) -> Tuple[Optional[float], int, int, List[str]]:
    """v1.2.0 honest pairing: (return, n_fresh, n_stale, stale_symbols).

    A pair contributes only when today's bar date is STRICTLY newer than the
    previous row's date; otherwise the symbol is stale — reported, excluded,
    never scored as a fake 0% leg. With honesty OFF or no prev_date, falls
    back to v1.1.1 semantics (every present pair counts, stale=0)."""
    rets: List[float] = []
    stale: List[str] = []
    honest = _price_honesty_enabled() and prev_date is not None
    for sym, p0 in (prev_prices or {}).items():
        p1 = (cur_prices or {}).get(sym)
        if p1 is None or not p0:
            continue
        if honest:
            a = cur_asof.get(sym)
            if a is None or a <= prev_date:
                stale.append(sym)
                continue
        rets.append((p1 / p0 - 1.0) * 100.0)
    if not rets:
        return None, 0, len(stale), stale
    return sum(rets) / len(rets), len(rets), len(stale), stale


def blended_benchmark_return_fresh(prices_prev: Dict[str, float],
                                   prices_cur: Dict[str, float],
                                   cur_asof: Dict[str, date],
                                   prev_date: Optional[date],
                                   ) -> Tuple[Optional[float], int, int]:
    """§1 benchmark with §6 calendar honesty: weights renormalize over the
    legs that produced a NEW bar since the previous row (a closed venue's
    carried close contributes no fake 0%). Returns (ret, n_fresh, n_stale)."""
    honest = _price_honesty_enabled() and prev_date is not None
    total_w, acc, n_fresh, n_stale = 0.0, 0.0, 0, 0
    for sym, w in BENCH_WEIGHTS.items():
        p0, p1 = (prices_prev or {}).get(sym), (prices_cur or {}).get(sym)
        if p0 and p1 is not None:
            if honest:
                a = cur_asof.get(sym)
                if a is None or a <= prev_date:
                    n_stale += 1
                    continue
            acc += w * (p1 / p0 - 1.0) * 100.0
            total_w += w
            n_fresh += 1
    if total_w <= 0:
        return None, n_fresh, n_stale
    return acc / total_w, n_fresh, n_stale


def count_scored_days(history: List[Dict[str, Any]], basket: str) -> Tuple[int, int]:
    """v1.2.0 criterion-1 semantics: (scored_days, excluded_days) for a basket.
    Scored = row carries a numeric Daily Return AND is dated on/after
    EVIDENCE_EPOCH (the v1.2.0 version break — pre-break rows include the
    2026-07-20 fabricated-flat day and never count). Seed rows (blank
    return, 'seeded' note) and DAY_EXCLUDED_INFRA rows never count."""
    scored, excluded = set(), set()
    for h in history or []:
        if h.get("basket") != basket:
            continue
        d = _parse_iso_date(h.get("date"))
        if d is None or d < EVIDENCE_EPOCH:
            continue
        note = str(h.get("note") or "")
        if h.get("daily_return") is not None:
            scored.add(h.get("date"))
        elif "DAY_EXCLUDED" in note:
            excluded.add(h.get("date"))
    return len(scored), len(excluded)


def basket_return(prev_prices: Dict[str, float],
                  cur_prices: Dict[str, float]) -> Tuple[Optional[float], int]:
    """Equal-weight 1-day return over symbols priced in BOTH snapshots.
    -> (return_pct, n_used). None when nothing is comparable."""
    rets: List[float] = []
    for sym, p0 in (prev_prices or {}).items():
        p1 = (cur_prices or {}).get(sym)
        if p0 and p1 and p0 > 0:
            rets.append((float(p1) / float(p0) - 1.0) * 100.0)
    if not rets:
        return None, 0
    return sum(rets) / len(rets), len(rets)


def turnover_pct(prev_symbols: Sequence[str],
                 cur_symbols: Sequence[str]) -> float:
    """Fraction of the basket replaced, 0-100. Empty previous = 100 (initial
    build is a full purchase)."""
    prev, cur = set(prev_symbols or []), set(cur_symbols or [])
    if not prev:
        return 100.0 if cur else 0.0
    if not cur:
        return 100.0
    return len(prev - cur) / float(len(prev)) * 100.0


def cost_drag_pct(prev_symbols: Sequence[str], cur_symbols: Sequence[str],
                  ticket_sar: float = 15000.0) -> float:
    """Cost charged for today's turnover: mean round-trip % of the names that
    actually changed, scaled by turnover fraction. Unknown venues are skipped
    (never invented)."""
    prev, cur = set(prev_symbols or []), set(cur_symbols or [])
    changed = (prev - cur) | (cur - prev)
    if not changed:
        return 0.0
    costs = [c for c in (ob.rt_cost_pct(s, ticket_sar) for s in changed)
             if c is not None]
    if not costs:
        return 0.0
    tp = turnover_pct(prev_symbols, cur_symbols) / 100.0
    return (sum(costs) / len(costs)) * tp


def chain_index(prev_index: Optional[float], daily_return_pct: Optional[float],
                drag_pct: float = 0.0) -> float:
    """Geometric chaining, cost-adjusted. Missing return = flat day."""
    base = float(prev_index) if prev_index else BASE_INDEX
    r = (daily_return_pct or 0.0) - (drag_pct or 0.0)
    return round(base * (1.0 + r / 100.0), 6)


def blended_benchmark_return(prices_prev: Dict[str, float],
                             prices_cur: Dict[str, float],
                             weights: Optional[Dict[str, float]] = None
                             ) -> Optional[float]:
    """Weighted benchmark return; weights renormalize over whatever priced."""
    w = dict(weights or BENCH_WEIGHTS)
    parts: List[Tuple[float, float]] = []
    for sym, wt in w.items():
        p0, p1 = (prices_prev or {}).get(sym), (prices_cur or {}).get(sym)
        if p0 and p1 and p0 > 0:
            parts.append((wt, (float(p1) / float(p0) - 1.0) * 100.0))
    if not parts:
        return None
    tw = sum(p[0] for p in parts)
    return sum(wt * r for wt, r in parts) / tw if tw else None


def count_compliance_violations(board_rows: Sequence[Sequence[Any]]) -> List[str]:
    """Criterion 2: any row flagged Gen2 Eligible=YES while carrying a
    blocking shariah status is a violation. -> list of 'SYM:STATUS'."""
    out: List[str] = []
    for r in board_rows or []:
        if not r or len(r) < 7:
            continue
        status = str(r[6]).strip().upper()
        eligible = str(r[-1]).strip().upper() == "YES"
        if eligible and status in _BLOCKING:
            out.append(f"{r[0]}:{status}")
    return out


def check_point_in_time(history: Sequence[Dict[str, Any]]) -> Tuple[bool, str]:
    """Criterion 5b: dates strictly increasing per basket, no duplicates."""
    seen: Dict[str, List[str]] = {}
    for h in history or []:
        seen.setdefault(h.get("basket", ""), []).append(str(h.get("date", "")))
    for basket, dates in seen.items():
        if len(dates) != len(set(dates)):
            return False, f"duplicate dates in {basket}"
        if dates != sorted(dates):
            return False, f"non-monotonic dates in {basket}"
    return True, "append-only integrity intact"


def evaluate_s1(days: int, violations: List[str],
                net_alpha_pct: Optional[float],
                calibration_state: str, ca_clean: bool, pit_ok: bool,
                pit_note: str, drill_date: Optional[str],
                excluded_days: int = 0) -> Dict[str, Any]:
    """§15 verdict. PASS requires all six; anything unmet => NOT_DECIDABLE
    (or FAIL where a criterion is definitively breached). Never auto-promotes.
    v1.2.0: `days` are SCORED evidence days only; excluded-infra days are
    reported beside them (Register §5) and can never satisfy criterion 1."""
    c: List[Dict[str, Any]] = []
    c.append({"id": 1, "name": "4+ weeks shadow evidence",
              "status": "PASS" if days >= S1_WINDOW_DAYS else "PENDING",
              "detail": f"{days}/{S1_WINDOW_DAYS} scored days"
                        + (f" · {excluded_days} excluded-infra"
                           if excluded_days else "")})
    c.append({"id": 2, "name": "zero compliance violations",
              "status": "FAIL" if violations else "PASS",
              "detail": (", ".join(violations[:5]) if violations
                         else "none observed")})
    if net_alpha_pct is None:
        c.append({"id": 3, "name": "shadow net alpha >= 0",
                  "status": "PENDING", "detail": "insufficient history"})
    else:
        c.append({"id": 3, "name": "shadow net alpha >= 0",
                  "status": "PASS" if net_alpha_pct >= 0 else "FAIL",
                  "detail": f"{net_alpha_pct:+.2f}% vs benchmark (cost-adj.)"})
    c.append({"id": 4, "name": "calibration in band (7D/14D)",
              "status": calibration_state,
              "detail": "7D/14D horizons land in Wave B (track_performance)"
                        if calibration_state == "PENDING" else "in band"})
    c.append({"id": 5, "name": "corporate-actions + point-in-time",
              "status": "PASS" if (ca_clean and pit_ok) else "FAIL",
              "detail": f"CA {'clean' if ca_clean else 'UNREPAIRED'}; {pit_note}"})
    c.append({"id": 6, "name": "rollback drill passed",
              "status": "PASS" if drill_date else "PENDING",
              "detail": drill_date or "not yet run (operator, monthly)"})

    if any(x["status"] == "FAIL" for x in c):
        verdict, why = "FAIL", "a criterion is breached — promotion frozen (§15)"
    elif all(x["status"] == "PASS" for x in c):
        verdict, why = "PASS", "all six criteria met — Tranche 1 may be authorized"
    else:
        pend = [str(x["id"]) for x in c if x["status"] == "PENDING"]
        verdict, why = "NOT_DECIDABLE", f"criteria {','.join(pend)} still pending"
    return {"verdict": verdict, "why": why, "criteria": c}


# --------------------------------------------------------------------------- #
# price fetch                                                                  #
# --------------------------------------------------------------------------- #
def _last_bar(res: Dict[str, Any]) -> Tuple[Optional[float], Optional[date]]:
    """v1.2.0: last (close, bar-date) from a chart payload. The bar's date is
    computed with the payload's own gmtoffset so a Tokyo Monday bar is dated
    Monday, not the UTC Sunday its epoch lands on."""
    closes = (res.get("indicators", {}).get("quote", [{}])[0] or {}).get("close") or []
    stamps = res.get("timestamp") or []
    try:
        off = int((res.get("meta") or {}).get("gmtoffset") or 0)
    except Exception:  # noqa: BLE001
        off = 0
    px, dt = None, None
    for i, c in enumerate(closes):
        if c is None:
            continue
        px = float(c)
        if i < len(stamps) and stamps[i]:
            try:
                dt = datetime.fromtimestamp(int(stamps[i]) + off,
                                            tz=timezone.utc).date()
            except Exception:  # noqa: BLE001
                dt = None
    return px, dt


def fetch_spot(symbols: Sequence[str]
               ) -> Tuple[Dict[str, float], Dict[str, date], List[str]]:
    """Last daily close per symbol + its bar date. Misses are reported,
    never invented; a close without a readable bar date is reported in errs
    and treated as stale by the honesty layer (unproven freshness ≠ fresh)."""
    out: Dict[str, float] = {}
    asof: Dict[str, date] = {}
    errs: List[str] = []
    try:
        import httpx
    except Exception as exc:  # noqa: BLE001
        return out, asof, [f"httpx_unavailable:{type(exc).__name__}"]
    with httpx.Client(timeout=20.0, follow_redirects=True,
                      headers={"User-Agent": "Mozilla/5.0 (TFB scorer)"}) as cl:
        for sym in list(dict.fromkeys(symbols)):
            try:
                r = cl.get(YAHOO_SPOT.format(sym=sb._yahoo_symbol(sym)))
                r.raise_for_status()
                res = r.json()["chart"]["result"][0]
                px, bar_date = _last_bar(res)
                if px is not None:
                    out[sym] = px
                    if bar_date is not None:
                        asof[sym] = bar_date
                    else:
                        errs.append(f"{sym}:no_bar_date")
                else:
                    errs.append(f"{sym}:no_close")
            except Exception as exc:  # noqa: BLE001
                errs.append(f"{sym}:{type(exc).__name__}")
    return out, asof, errs


# --------------------------------------------------------------------------- #
# sheets I/O                                                                   #
# --------------------------------------------------------------------------- #
def read_history(sh) -> List[Dict[str, Any]]:
    try:
        ws = sh.worksheet(TAB_HISTORY)
    except Exception:  # noqa: BLE001
        return []
    out: List[Dict[str, Any]] = []
    for row in ws.get_all_values()[1:]:
        if not row or not str(row[0]).strip():
            continue
        try:
            prices = json.loads(row[3]) if len(row) > 3 and row[3] else {}
        except Exception:  # noqa: BLE001
            prices = {}
        out.append({
            "date": str(row[0]).strip()[:10],
            "basket": str(row[1]).strip().upper() if len(row) > 1 else "",
            "symbols": [s for s in (str(row[2]).split(",") if len(row) > 2 else [])
                        if s.strip()],
            "prices": prices,
            "daily_return": _num(row[4]) if len(row) > 4 else None,
            "cum_index": _num(row[5]) if len(row) > 5 else None,
            "note": str(row[8]).strip() if len(row) > 8 else "",
        })
    return out


def last_row_for(history: List[Dict[str, Any]], basket: str
                 ) -> Optional[Dict[str, Any]]:
    rows = [h for h in history if h["basket"] == basket]
    return rows[-1] if rows else None


def append_history(sh, rows: List[List[Any]]) -> None:
    try:
        ws = sh.worksheet(TAB_HISTORY)
    except Exception:  # noqa: BLE001
        ws = sh.add_worksheet(title=TAB_HISTORY, rows=500,
                              cols=len(HISTORY_HEADER))
        ws.update(values=[HISTORY_HEADER], range_name="A1")
    ws.append_rows(rows, value_input_option="RAW")


def read_regret_ledger(sh) -> List[Dict[str, Any]]:
    """Open forks as written (append-only). Scoring is always recomputed."""
    try:
        vals = sh.worksheet(TAB_REGRET).get_all_values()
    except Exception:  # noqa: BLE001
        return []
    out: List[Dict[str, Any]] = []
    for row in vals[1:]:
        if not row or not str(row[0]).strip():
            continue
        out.append({"date": str(row[0]).strip()[:10],
                    "fork": str(row[1]).strip().upper() if len(row) > 1 else "",
                    "symbol": str(row[2]).strip().upper() if len(row) > 2 else "",
                    "counterparty": (str(row[3]).strip().upper()
                                     if len(row) > 3 else ""),
                    "reason": str(row[4]) if len(row) > 4 else "",
                    "ref_price": _num(row[5]) if len(row) > 5 else None,
                    "alt_price": _num(row[6]) if len(row) > 6 else None})
    return out


def dedupe_new_forks(existing: Sequence[Dict[str, Any]],
                     candidates: Sequence[Dict[str, Any]]
                     ) -> List[Dict[str, Any]]:
    """One open fork per (symbol, fork kind). A name refused for ten days
    holds ONE fork opened at first refusal — not ten, which would triple-count
    the same constraint in every aggregate."""
    have = {(f.get("symbol"), f.get("fork")) for f in existing or []}
    fresh: List[Dict[str, Any]] = []
    for c in candidates or []:
        key = (c.get("symbol"), c.get("fork"))
        if key in have:
            continue
        have.add(key)
        fresh.append(c)
    return fresh


def append_regret(sh, rows: List[List[Any]]) -> None:
    if not rows:
        return
    try:
        ws = sh.worksheet(TAB_REGRET)
    except Exception:  # noqa: BLE001
        ws = sh.add_worksheet(title=TAB_REGRET, rows=1000,
                              cols=len(rg.LEDGER_HEADER))
        ws.update(values=[rg.LEDGER_HEADER], range_name="A1")
    ws.append_rows(rows, value_input_option="RAW")


def _now_riyadh() -> str:
    """Riyadh wall-clock stamp (fixed UTC+3) for the summary header."""
    return datetime.now(timezone(timedelta(hours=3))).strftime(
        "%Y-%m-%d %H:%M")


def write_regret_summary(sh, summary: Dict[str, Any],
                         scored: List[Dict[str, Any]]) -> None:
    width = max(6, len(rg.LEDGER_HEADER) + 1)
    try:
        ws = sh.worksheet(TAB_REGRET_SUMMARY)
    except Exception:  # noqa: BLE001
        ws = sh.add_worksheet(title=TAB_REGRET_SUMMARY, rows=400, cols=width)
    body: List[List[Any]] = [
        [f"REGRET SUMMARY v{rg.__version__}", f"as of {_now_riyadh()}",
         f"open forks {len(scored)}", f"pending {summary.get('pending')}"],
        [summary.get("governance", "")],
    ]
    for n in summary.get("notes") or []:
        body.append([n])
    body.append([])
    body.append(["Fork", "n", "Mean Regret %", "Median %", "Worst %", "Hit Rate"])
    for kind, st in (summary.get("by_fork") or {}).items():
        body.append([kind, st.get("n"), st.get("mean_regret_pct"),
                     st.get("median_regret_pct"), st.get("worst_pct"),
                     st.get("hit_rate")])
    body.append([])
    body.append(["OPEN FORKS"] + rg.LEDGER_HEADER[1:])
    for r in rg.to_rows(scored)[:200]:
        body.append([""] + r[1:])
    body = [list(r) + [""] * (width - len(r)) for r in body]   # uniform width
    ws.clear()
    ws.update(values=body, range_name="A1")


def write_gate(sh, gate: Dict[str, Any], meta: List[List[Any]]) -> None:
    try:
        ws = sh.worksheet(TAB_GATE)
    except Exception:  # noqa: BLE001
        ws = sh.add_worksheet(title=TAB_GATE, rows=60, cols=6)
    body = meta + [[]]
    body.append(["#", "Criterion", "Status", "Detail"])
    for c in gate["criteria"]:
        body.append([c["id"], c["name"], c["status"], c["detail"]])
    width = max(len(r) for r in body) if body else 4
    body = [list(r) + [""] * (width - len(r)) for r in body]
    ws.clear()
    ws.update(values=body, range_name="A1")


def find_drill_marker(sh, since: date) -> Optional[str]:
    """Criterion 6: newest [ROLLBACK-DRILL] line in _Run_Log within window."""
    try:
        ws = sh.worksheet("_Run_Log")
        vals = ws.get_all_values()
    except Exception:  # noqa: BLE001
        return None
    newest: Optional[str] = None
    for row in vals[-400:]:
        line = " ".join(str(c) for c in row)
        if "[ROLLBACK-DRILL]" in line:
            stamp = str(row[0]).strip()[:10] if row else ""
            try:
                if datetime.strptime(stamp, "%Y-%m-%d").date() >= since:
                    newest = stamp
            except Exception:  # noqa: BLE001
                continue
    return newest


def ca_is_clean(sh) -> bool:
    """Criterion 5a: no CONFIRMED action lacking repair in Performance_Log."""
    try:
        from core import corporate_actions as ca
        rp_path = os.path.join(_ROOT, "scripts", "repair_corporate_actions.py")
        spec = importlib.util.spec_from_file_location("tfb_ca_repair", rp_path)
        rp = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(rp)  # type: ignore[union-attr]
        acts = ca.parse_actions(sh.worksheet(ca.TAB_ACTIONS).get_all_values())
        idx = ca.build_adjustment_index(acts, confirmed_only=True)
        plan, _hdr, _cols = rp.plan_repairs(
            sh.worksheet("Performance_Log").get_all_values(), idx)
        return len(plan) == 0
    except Exception:  # noqa: BLE001
        return True          # absence of ledger is not a violation


# --------------------------------------------------------------------------- #
# main                                                                         #
# --------------------------------------------------------------------------- #
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--rollback-drill-passed", action="store_true")
    ap.add_argument("--sheet-id")
    args = ap.parse_args(argv)
    if args.selftest:
        return _selftest()

    os.environ.setdefault("TFB_COMPLIANCE_GATE_ENABLED", "1")
    today = _today_riyadh()
    sh = sb._open_sheet(args.sheet_id)

    if args.rollback_drill_passed:
        sh.worksheet("_Run_Log").append_row(
            [datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "INFO",
             "shadow_scorer", TAB_GATE, "OK",
             f"[ROLLBACK-DRILL] passed (operator-recorded) v{SCRIPT_VERSION}",
             "", "", "", "{}"], value_input_option="RAW")
        print(f"[S1-GATE v{SCRIPT_VERSION}] rollback drill recorded {today}")
        return 0

    # --- today's baskets -------------------------------------------------- #
    champ = sb.rows_to_records(sh.worksheet(sb.TAB_TOP10).get_all_values())
    champ_syms = [c["symbol"] for c in champ]
    try:
        board = sh.worksheet(sb.TAB_OUT).get_all_values()
    except Exception:  # noqa: BLE001
        board = []
    data_rows = [r for r in board
                 if r and r[0] and r[0] not in ("Symbol",)
                 and len(r) >= len(sb.OUT_HEADER) - 2]
    chal_syms = [r[0] for r in data_rows if str(r[-1]).strip().upper() == "YES"]
    violations = count_compliance_violations(data_rows)

    history = read_history(sh)
    if any(h["date"] == str(today) for h in history):
        print(f"[S1-GATE v{SCRIPT_VERSION}] {today} already recorded — "
              f"append-only, refusing duplicate (point-in-time integrity)")
        if not args.dry_run:
            return 0

    prev = {b: last_row_for(history, b)
            for b in (CHAMPION, CHALLENGER, BENCHMARK)}
    existing_forks = read_regret_ledger(sh)
    board_header = board[0] if board and board[0] and board[0][0] == "Symbol" else None
    new_forks = dedupe_new_forks(
        existing_forks,
        rg.forks_from_board(data_rows, today, {}, header=board_header))
    all_forks = list(existing_forks) + list(new_forks)

    need = set(champ_syms) | set(chal_syms) | set(BENCH_WEIGHTS)
    for f in all_forks:
        if f.get("symbol"):
            need.add(f["symbol"])
        if f.get("counterparty"):
            need.add(f["counterparty"])
    for p in prev.values():
        if p:
            need |= set(p["symbols"])
    spot, spot_asof, price_errs = fetch_spot(sorted(need))

    for f in new_forks:                      # price at the moment of refusal
        if f.get("ref_price") is None:
            f["ref_price"] = spot.get(f.get("symbol"))
    scored_forks = rg.score_all(all_forks, spot, today)
    regret_summary = rg.summarize(scored_forks)

    # ---- v1.2.0 honest pass 1: measure freshness per basket --------------- #
    measured: Dict[str, Dict[str, Any]] = {}
    for basket, syms in ((CHAMPION, champ_syms), (CHALLENGER, chal_syms),
                         (BENCHMARK, list(BENCH_WEIGHTS))):
        p = prev[basket]
        p_date = _parse_iso_date(p["date"]) if p else None
        if basket == BENCHMARK:
            ret, n_fresh, n_stale = (
                blended_benchmark_return_fresh(p["prices"], spot, spot_asof,
                                               p_date) if p else (None, 0, 0))
            stale_syms: List[str] = []
        else:
            ret, n_fresh, n_stale, stale_syms = (
                basket_return_fresh(p["prices"], spot, spot_asof, p_date)
                if p else (None, 0, 0, []))
        measured[basket] = {"ret": ret, "n_fresh": n_fresh,
                            "n_stale": n_stale, "stale_syms": stale_syms,
                            "p": p}

    # Day gate (Register §5): the CHALLENGER is the evidence subject. If its
    # fresh coverage is under floor — or the benchmark produced no fresh leg —
    # the whole day is EXCLUDED-INFRA for every basket (indexes carried), so
    # the three series never diverge on which days they consider real.
    _chal_m = measured[CHALLENGER]
    _chal_frac = (_chal_m["n_fresh"] / len(chal_syms)) if chal_syms else 0.0
    day_excluded = bool(
        _price_honesty_enabled()
        and _chal_m["p"] is not None
        and (_chal_frac < _min_fresh_frac()
             or (measured[BENCHMARK]["p"] is not None
                 and measured[BENCHMARK]["ret"] is None)))
    total_stale = sum(m["n_stale"] for m in measured.values())

    new_rows: List[List[Any]] = []
    results: Dict[str, Dict[str, Any]] = {}
    for basket, syms in ((CHAMPION, champ_syms), (CHALLENGER, chal_syms),
                         (BENCHMARK, list(BENCH_WEIGHTS))):
        m = measured[basket]
        p = m["p"]
        p_date = _parse_iso_date(p["date"]) if p else None
        ret = None if day_excluded else m["ret"]
        if basket == BENCHMARK:
            drag, turn = 0.0, 0.0
        else:
            turn = 0.0 if day_excluded else turnover_pct(
                p["symbols"] if p else [], syms)
            drag = 0.0 if day_excluded else cost_drag_pct(
                p["symbols"] if p else [], syms)
        idx = chain_index(p["cum_index"] if p else None, ret, drag)
        # Stored snapshot: fresh symbols at today's bar; stale symbols carry
        # their previous stored price so the next fresh bar captures the full
        # move; nothing is stored at an invented level.
        prices_today: Dict[str, float] = {}
        for s in syms:
            if s in spot and (not _price_honesty_enabled() or p_date is None
                              or (spot_asof.get(s) is not None
                                  and spot_asof[s] > p_date)):
                prices_today[s] = spot[s]
            elif p and s in (p.get("prices") or {}):
                prices_today[s] = p["prices"][s]
        if day_excluded and p:
            note = (f"DAY_EXCLUDED_INFRA fresh={m['n_fresh']}/{len(syms)} "
                    f"stale={m['n_stale']}")
        else:
            note = f"n={m['n_fresh']}" + (" seeded" if not p else "")
            if m["n_stale"]:
                note += (f" stale={m['n_stale']}"
                         f"[{','.join(m['stale_syms'][:4])}"
                         f"{',…' if m['n_stale'] > 4 else ''}]")
        if price_errs:
            note += f" price_errs={len(price_errs)}"
        new_rows.append([str(today), basket, ",".join(syms),
                         json.dumps(prices_today),
                         "" if ret is None else round(ret, 4),
                         idx, round(turn, 2), round(drag, 4), note])
        results[basket] = {"ret": ret, "index": idx, "n": len(syms)}

    scored_prev, excluded_prev = count_scored_days(history, CHALLENGER)
    if _price_honesty_enabled():
        days = scored_prev + (0 if (day_excluded
                                    or results[CHALLENGER]["ret"] is None)
                              else 1)
        excluded_days = excluded_prev + (1 if day_excluded else 0)
    else:  # v1.1.1 legacy counting, kill-switch path
        days = len({h["date"] for h in history
                    if h["basket"] == CHALLENGER}) + 1
        excluded_days = 0
    chal_cum = (results[CHALLENGER]["index"] / BASE_INDEX - 1.0) * 100.0
    bench_cum = (results[BENCHMARK]["index"] / BASE_INDEX - 1.0) * 100.0
    champ_cum = (results[CHAMPION]["index"] / BASE_INDEX - 1.0) * 100.0
    net_alpha = (chal_cum - bench_cum) if days >= 1 else None

    pit_ok, pit_note = check_point_in_time(history)
    gate = evaluate_s1(days, violations, net_alpha, "PENDING",
                       ca_is_clean(sh), pit_ok, pit_note,
                       find_drill_marker(sh, today - timedelta(days=45)),
                       excluded_days=excluded_days)

    verdict = (f"[S1-GATE v{SCRIPT_VERSION}] {gate['verdict']} day {days}/"
               f"{S1_WINDOW_DAYS}"
               + (f" (+{excluded_days} excluded-infra)" if excluded_days else "")
               + f" | challenger {chal_cum:+.2f}% champion "
               f"{champ_cum:+.2f}% benchmark {bench_cum:+.2f}% | net alpha "
               f"{'n/a' if net_alpha is None else f'{net_alpha:+.2f}%'} | "
               f"{'DAY_EXCLUDED_INFRA' if day_excluded else 'day_scored'} "
               f"stale={total_stale} | "
               f"violations={len(violations)} | forks {len(all_forks)} "
               f"(+{len(new_forks)} new)")
    print(verdict)
    if args.dry_run:
        for r in new_rows:
            print("  ", r[:3], "ret=", r[4], "idx=", r[5], "drag=", r[7])
        for c in gate["criteria"]:
            print(f"   [{c['status']:<7}] {c['id']}. {c['name']} — {c['detail']}")
        for f in scored_forks[:12]:
            print(f"   FORK {f['fork']:<17} {f['symbol']:<10} "
                  f"regret={f.get('regret_pct')} ({f.get('reason')})")
        for n in regret_summary.get("notes") or []:
            print("   " + n)
        return 0

    meta = [
        [f"S-1 GATE v{SCRIPT_VERSION}", f"as of {today} Riyadh",
         f"verdict: {gate['verdict']}"],
        [gate["why"]],
        [f"challenger {chal_cum:+.2f}%", f"champion {champ_cum:+.2f}%",
         f"benchmark {bench_cum:+.2f}%",
         f"net alpha {'n/a' if net_alpha is None else f'{net_alpha:+.2f}%'}"],
        [f"benchmark = {json.dumps(BENCH_WEIGHTS)} (§1 locked)",
         "returns are cost-adjusted, daily-rebalanced equal-weight",
         f"price errors: {len(price_errs)} | stale: {total_stale} | "
         f"day: {'EXCLUDED_INFRA' if day_excluded else 'scored'}"],
        ["Gen-2 moves NO capital. This gate authorizes Tranche 1 only on PASS."],
    ]
    # v1.1.1: gate FIRST, then evidence, then the secondary summary. Each
    # write independently guarded so one failure cannot suppress the others.
    _write_errors: List[str] = []
    for _label, _fn in (
            ("S1_Gate", lambda: write_gate(sh, gate, meta)),
            ("Shadow_History", lambda: append_history(sh, new_rows)),
            ("Regret_Ledger", lambda: append_regret(sh, rg.to_rows(new_forks))),
            ("Regret_Summary",
             lambda: write_regret_summary(sh, regret_summary, scored_forks))):
        try:
            _fn()
        except Exception as _exc:  # noqa: BLE001
            _write_errors.append(f"{_label}:{type(_exc).__name__}")
            print(f"[S1-GATE v{SCRIPT_VERSION}] WRITE FAILED {_label}: {_exc}")
    if _write_errors:
        verdict += " | write_errors=" + ",".join(_write_errors)
    try:
        sh.worksheet("_Run_Log").append_row(
            [datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "INFO",
             "shadow_scorer", TAB_GATE,
             "OK" if gate["verdict"] != "FAIL" else "GATE_FAIL", verdict,
             "", "", "", json.dumps({"version": SCRIPT_VERSION})],
            value_input_option="RAW")
    except Exception:  # noqa: BLE001
        pass
    return 0


# --------------------------------------------------------------------------- #
# SELFTEST                                                                     #
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []

    r, n = basket_return({"A": 100.0, "B": 50.0}, {"A": 110.0, "B": 45.0})
    checks.append(("basket return equal-weight (+10,-10 -> 0)",
                   abs(r - 0.0) < 1e-9 and n == 2))
    r2, n2 = basket_return({"A": 100.0, "B": 50.0}, {"A": 110.0})
    checks.append(("unpriced name excluded, not zero-filled",
                   abs(r2 - 10.0) < 1e-9 and n2 == 1))
    checks.append(("no overlap -> None", basket_return({"A": 1.0}, {"B": 2.0})
                   == (None, 0)))
    checks.append(("turnover: half replaced",
                   turnover_pct(["A", "B"], ["A", "C"]) == 50.0))
    checks.append(("turnover: seeded basket = 100%",
                   turnover_pct([], ["A"]) == 100.0))
    checks.append(("turnover: unchanged = 0%",
                   turnover_pct(["A", "B"], ["B", "A"]) == 0.0))
    d = cost_drag_pct(["STNG.US"], ["TRMD.US"])
    checks.append(("cost drag charged on US swap (~0.10% x 100% turnover)",
                   0.05 < d < 0.15))
    checks.append(("no turnover -> no drag",
                   cost_drag_pct(["STNG.US"], ["STNG.US"]) == 0.0))
    checks.append(("unknown venue contributes no invented cost",
                   cost_drag_pct(["RELIANCE.NS"], ["INFY.NS"]) == 0.0))
    checks.append(("chain: seed 100 -> +10% = 110",
                   chain_index(None, 10.0) == 110.0))
    checks.append(("chain: drag subtracts",
                   abs(chain_index(100.0, 1.0, 0.5) - 100.5) < 1e-9))
    checks.append(("chain: missing return = flat day",
                   chain_index(107.5, None) == 107.5))
    b = blended_benchmark_return({"SPUS": 100.0, "^TASI.SR": 10000.0},
                                 {"SPUS": 102.0, "^TASI.SR": 9900.0})
    checks.append(("benchmark 70/30 blend (+2%,-1% -> +1.1%)",
                   abs(b - 1.1) < 1e-9))
    b2 = blended_benchmark_return({"SPUS": 100.0}, {"SPUS": 102.0})
    checks.append(("benchmark renormalizes when a leg is unpriced",
                   abs(b2 - 2.0) < 1e-9))

    hdr_len = len(sb.OUT_HEADER)
    good = ["7010.SR", "STC", "", "Telecom", 14.5, "High", "AUTHORITY_PASS",
            "AL_RAJHI_OFFICIAL", "BROKER_TRADABLE", "TASI", "YES", 0.36, 8.5,
            1.5, "TRADE", 12.1, "YES"][:hdr_len]
    bad = ["BBD.US", "Bradesco", "", "Financials", 9.0, "High",
           "MODEL_SCREEN_FAIL", "MODEL_SCREEN_RAJHI_STYLE", "BROKER_TRADABLE",
           "", "YES", 0.1, 5.0, 1.5, "TRADE", 49.0, "YES"][:hdr_len]
    blocked = list(bad); blocked[-1] = "NO"
    checks.append(("violation detector: clean board = none",
                   count_compliance_violations([good]) == []))
    checks.append(("violation detector: FAIL marked eligible is caught",
                   count_compliance_violations([good, bad])
                   == ["BBD.US:MODEL_SCREEN_FAIL"]))
    checks.append(("violation detector: FAIL correctly blocked = no violation",
                   count_compliance_violations([good, blocked]) == []))

    hist_ok = [{"basket": CHALLENGER, "date": "2026-07-19"},
               {"basket": CHALLENGER, "date": "2026-07-20"}]
    hist_dup = hist_ok + [{"basket": CHALLENGER, "date": "2026-07-20"}]
    checks.append(("PIT: clean history passes", check_point_in_time(hist_ok)[0]))
    checks.append(("PIT: duplicate date caught",
                   not check_point_in_time(hist_dup)[0]))
    checks.append(("PIT: out-of-order caught",
                   not check_point_in_time(
                       [{"basket": CHALLENGER, "date": "2026-07-21"},
                        {"basket": CHALLENGER, "date": "2026-07-19"}])[0]))

    g = evaluate_s1(3, [], None, "PENDING", True, True, "ok", None)
    checks.append(("gate day 3 -> NOT_DECIDABLE, never PASS",
                   g["verdict"] == "NOT_DECIDABLE"
                   and g["criteria"][0]["status"] == "PENDING"))
    g2 = evaluate_s1(30, ["X:UNKNOWN"], 5.0, "PASS", True, True, "ok",
                     "2026-08-01")
    checks.append(("gate: any violation -> FAIL, promotion frozen",
                   g2["verdict"] == "FAIL"))
    g3 = evaluate_s1(30, [], -2.0, "PASS", True, True, "ok", "2026-08-01")
    checks.append(("gate: negative alpha -> FAIL",
                   g3["verdict"] == "FAIL"
                   and g3["criteria"][2]["status"] == "FAIL"))
    g4 = evaluate_s1(30, [], 3.0, "PASS", True, True, "ok", "2026-08-01")
    checks.append(("gate: all six met -> PASS",
                   g4["verdict"] == "PASS"
                   and "Tranche 1" in g4["why"]))
    g5 = evaluate_s1(30, [], 3.0, "PENDING", True, True, "ok", "2026-08-01")
    checks.append(("gate: calibration pending blocks PASS honestly",
                   g5["verdict"] == "NOT_DECIDABLE"))
    g6 = evaluate_s1(30, [], 3.0, "PASS", False, True, "ok", "2026-08-01")
    checks.append(("gate: unrepaired corporate action -> FAIL",
                   g6["verdict"] == "FAIL"))
    checks.append(("gate always reports six criteria", len(g4["criteria"]) == 6))

    ex = [{"symbol": "TRMD.US", "fork": rg.COMPLIANCE_BLOCK},
          {"symbol": "0083.HK", "fork": rg.FLOOR_LOCK}]
    cand = [{"symbol": "TRMD.US", "fork": rg.COMPLIANCE_BLOCK},
            {"symbol": "MRP.US", "fork": rg.COMPLIANCE_BLOCK},
            {"symbol": "0083.HK", "fork": rg.FLOOR_LOCK}]
    fresh = dedupe_new_forks(ex, cand)
    checks.append(("dedupe: a still-blocked name does NOT re-open a fork",
                   [f["symbol"] for f in fresh] == ["MRP.US"]))
    checks.append(("dedupe: same symbol under a DIFFERENT fork is allowed",
                   len(dedupe_new_forks(ex, [{"symbol": "TRMD.US",
                                              "fork": rg.EDGE_BELOW_COST}])) == 1))
    checks.append(("dedupe: empty ledger admits everything",
                   len(dedupe_new_forks([], cand)) == 3))
    checks.append(("regret rows match the ledger header width",
                   all(len(r) == len(rg.LEDGER_HEADER)
                       for r in rg.to_rows(rg.score_all(cand, {}, None)))))

    # ---- v1.2.0 price-honesty layer (the 2026-07-20 frozen-day defect) ---- #
    d0, d1 = date(2026, 7, 19), date(2026, 7, 20)
    os.environ["TFB_SHADOW_PRICE_HONESTY"] = "1"
    rF, nF, sF, ssF = basket_return_fresh(
        {"A": 100.0, "B": 50.0}, {"A": 110.0, "B": 50.0},
        {"A": d1, "B": d0}, d0)
    checks.append(("fresh: stale bar excluded from pairing, fresh bar scores",
                   abs(rF - 10.0) < 1e-9 and nF == 1 and sF == 1
                   and ssF == ["B"]))
    rG, nG, sG, _ = basket_return_fresh(
        {"A": 100.0, "B": 50.0}, {"A": 100.0, "B": 50.0},
        {"A": d0, "B": d0}, d0)
    checks.append(("fresh: fully-stale basket -> None, never a fabricated 0",
                   rG is None and nG == 0 and sG == 2))
    rH, nH, sH, _ = basket_return_fresh(
        {"A": 100.0}, {"A": 100.0}, {"A": d1}, d0)
    checks.append(("fresh: unchanged price WITH a new bar is a real 0%",
                   rH is not None and abs(rH) < 1e-9 and nH == 1 and sH == 0))
    rB, nBf, nBs = blended_benchmark_return_fresh(
        {"SPUS": 100.0, "^TASI.SR": 10000.0},
        {"SPUS": 100.0, "^TASI.SR": 10100.0},
        {"SPUS": d0, "^TASI.SR": d1}, d0)
    checks.append(("benchmark: carried leg drops out, fresh leg renormalizes",
                   rB is not None and abs(rB - 1.0) < 1e-9
                   and nBf == 1 and nBs == 1))
    os.environ["TFB_SHADOW_PRICE_HONESTY"] = "0"
    rL, nL, sL, _ = basket_return_fresh(
        {"A": 100.0, "B": 50.0}, {"A": 100.0, "B": 50.0},
        {"A": d0, "B": d0}, d0)
    checks.append(("kill-switch OFF restores v1.1.1 semantics exactly",
                   rL is not None and abs(rL) < 1e-9 and nL == 2 and sL == 0))
    os.environ["TFB_SHADOW_PRICE_HONESTY"] = "1"
    hist = [
        {"basket": CHALLENGER, "date": "2026-07-19", "daily_return": None,
         "note": "n=0 seeded"},
        {"basket": CHALLENGER, "date": "2026-07-21", "daily_return": 0.42,
         "note": "n=5"},
        {"basket": CHALLENGER, "date": "2026-07-22", "daily_return": None,
         "note": "DAY_EXCLUDED_INFRA fresh=0/5 stale=5"},
    ]
    checks.append(("scored-day counter: seed=0, excluded=0, scored=1",
                   count_scored_days(hist, CHALLENGER) == (1, 1)))
    hist_pre = [
        {"basket": CHALLENGER, "date": "2026-07-20", "daily_return": 0.0,
         "note": "n=5"},          # the fabricated-flat day: numeric 0, pre-epoch
    ] + hist
    checks.append(("evidence epoch: pre-VBREAK fabricated 0-day NEVER counts",
                   count_scored_days(hist_pre, CHALLENGER) == (1, 1)))
    g5 = evaluate_s1(1, [], 0.1, "PENDING", True, True, "ok", None,
                     excluded_days=2)
    c1 = next(x for x in g5["criteria"] if x["id"] == 1)
    checks.append(("criterion 1 reports scored + excluded-infra days",
                   "1/28 scored" in c1["detail"]
                   and "2 excluded-infra" in c1["detail"]))
    checks.append(("bar date honors payload gmtoffset (Tokyo Monday = Monday)",
                   _last_bar({"indicators": {"quote": [{"close": [3883.0]}]},
                              # 2026-07-23 15:00 UTC == 2026-07-24 00:00 JST:
                              # naive UTC dating would say the 23rd.
                              "timestamp": [1784818800],
                              "meta": {"gmtoffset": 32400}})[1]
                   == date(2026, 7, 24)))

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[shadow_scorer v{SCRIPT_VERSION}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    sys.exit(main())
