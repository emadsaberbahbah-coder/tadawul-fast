"""
core/regret.py — TFB Gen-2 Regret Ledger
=========================================
VERSION 1.0.0  (2026-07-19)  — NEW MODULE (Wave B foundation, deliverable #20)

WHY (Master Plan v2.1 §8 self-learning, §18.4 do-nothing as a first-class
action): the system makes decisions every day that it never scores. A
compliance block, a venue floor, a NO_ACTION verdict, a switch taken — each
is a fork, and only one branch is ever observed unless something records the
other. Without that record the feedback loop has nothing honest to learn from,
and the operator cannot see what his constraints actually cost him.

Built now, before the calibrator, for the same reason the 7D/14D checkpoints
were: regret is a TIME SERIES. Every day this is not running is a day of
counterfactual evidence that cannot be recovered later.

WHAT IT MEASURES (five forks, each recorded the day it happens and scored
later against realized prices):
  COMPLIANCE_BLOCK  a name the shariah screen refused (e.g. TRMD.US at 37.1%
                    debt/mcap). If it then rises, the screen cost that return.
                    This is stated plainly and without apology: a faith-based
                    constraint is a CHOICE, and knowing its price is respect
                    for the choice, not an argument against it. The ledger
                    NEVER recommends overriding a compliance verdict.
  FLOOR_LOCK        a compliant name blocked only by a venue minimum (0083.HK
                    at 27.2K). Its regret is the honest answer to "what would
                    more equity have earned me?" — actionable, unlike the above.
  EDGE_BELOW_COST   eligible, but net edge failed the cost hurdle.
  NO_ACTION         a switch proposal the advisor declined (§18.4). Regret =
                    what the proposed buy did MINUS what the held name did.
  SWITCH_TAKEN      a switch that was executed; same difference, opposite sign.

SIGN CONVENTION (one rule, applied everywhere): regret_pct > 0 means THE PATH
NOT TAKEN WOULD HAVE BEEN BETTER. Negative regret means the decision was
vindicated. Aggregates therefore read directly: a positive mean for
COMPLIANCE_BLOCK is the running cost of the screen.

PURITY: stdlib only. No I/O, no env, no network. The caller supplies prices;
this module never fetches, never guesses, and returns None rather than
inventing a number when a price is missing.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.0"
REGRET_VERSION = __version__

COMPLIANCE_BLOCK = "COMPLIANCE_BLOCK"
FLOOR_LOCK = "FLOOR_LOCK"
EDGE_BELOW_COST = "EDGE_BELOW_COST"
NO_ACTION = "NO_ACTION"
SWITCH_TAKEN = "SWITCH_TAKEN"

FORK_KINDS = (COMPLIANCE_BLOCK, FLOOR_LOCK, EDGE_BELOW_COST, NO_ACTION,
              SWITCH_TAKEN)

# Statuses the compliance gate uses to refuse a name (mirrors compliance_gate)
_COMPLIANCE_REFUSALS = {"AUTHORITY_FAIL", "MODEL_SCREEN_FAIL", "VENUE_BLOCK",
                        "INSTRUMENT_BLOCK", "BROKER_UNTRADABLE", "CONFLICT",
                        "DATA_STALE", "UNKNOWN"}

LEDGER_HEADER = ["Date", "Fork", "Symbol", "Counterparty", "Reason",
                 "Ref Price", "Alt Price", "Horizon Days", "Scored On",
                 "Forgone %", "Held %", "Regret %", "Notes"]


def _f(x: Any) -> Optional[float]:
    try:
        v = float(str(x).replace(",", "").replace("%", "").strip())
        return v if v == v else None
    except Exception:
        return None


def _as_date(x: Any) -> Optional[date]:
    if isinstance(x, date):
        return x
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(x).strip()[:10], fmt).date()
        except Exception:
            continue
    return None


# --------------------------------------------------------------------------- #
# 1. recording forks (the day they happen)                                     #
# --------------------------------------------------------------------------- #
def forks_from_board(board_rows: Sequence[Sequence[Any]], as_of: Any,
                     prices: Optional[Dict[str, float]] = None,
                     header: Optional[Sequence[str]] = None
                     ) -> List[Dict[str, Any]]:
    """Read a Shadow_Board-shaped table and emit one fork per REFUSED name.
    Column positions are resolved from the header when given, else the
    v1.1.3 layout is assumed. A row without a usable price is still recorded
    (ref_price None) so the fork is not lost — it simply cannot be scored."""
    px = prices or {}
    idx = {"symbol": 0, "status": 6, "verdict": 14, "eligible": -1}
    if header:
        low = [str(h).strip().lower() for h in header]
        def find(*names):
            for n in names:
                if n in low:
                    return low.index(n)
            return None
        idx = {"symbol": find("symbol") if find("symbol") is not None else 0,
               "status": find("shariah status") if find("shariah status") is not None else 6,
               "verdict": find("edge verdict") if find("edge verdict") is not None else 14,
               "eligible": find("gen2 eligible") if find("gen2 eligible") is not None else -1}
    out: List[Dict[str, Any]] = []
    d = _as_date(as_of) or date.today()
    for row in board_rows or []:
        if not row:
            continue
        sym = str(row[idx["symbol"]]).strip().upper() if len(row) > idx["symbol"] else ""
        if not sym or sym in ("SYMBOL", "-"):
            continue
        status = (str(row[idx["status"]]).strip().upper()
                  if len(row) > idx["status"] else "")
        verdict = (str(row[idx["verdict"]]).strip().upper()
                   if idx["verdict"] is not None and len(row) > idx["verdict"] else "")
        eligible = str(row[idx["eligible"]]).strip().upper() == "YES"
        if eligible:
            continue
        if status in _COMPLIANCE_REFUSALS:
            kind, reason = COMPLIANCE_BLOCK, status
        elif verdict == "EDGE_BELOW_COST":
            kind, reason = EDGE_BELOW_COST, verdict
        elif status:
            kind, reason = FLOOR_LOCK, "FLOOR_LOCKED"
        else:
            continue
        out.append({"date": str(d), "fork": kind, "symbol": sym,
                    "counterparty": "", "reason": reason,
                    "ref_price": px.get(sym)})
    return out


def fork_from_switch(proposal: Dict[str, Any], taken: bool, as_of: Any,
                     prices: Optional[Dict[str, float]] = None
                     ) -> Optional[Dict[str, Any]]:
    """A switch proposal is a two-sided fork: `symbol` is the BUY leg,
    `counterparty` the SELL leg. Declining it is NO_ACTION; executing it is
    SWITCH_TAKEN. Either way both legs are priced so the pair can be scored."""
    p = proposal or {}
    buy, sell = str(p.get("buy") or "").upper(), str(p.get("sell") or "").upper()
    if not buy or not sell:
        return None
    px = prices or {}
    d = _as_date(as_of) or date.today()
    return {"date": str(d), "fork": SWITCH_TAKEN if taken else NO_ACTION,
            "symbol": buy, "counterparty": sell,
            "reason": (f"delta {p.get('delta_pct')}% vs hurdle "
                       f"{p.get('hurdle_pct')}%"),
            "ref_price": px.get(sell), "alt_price": px.get(buy)}


# --------------------------------------------------------------------------- #
# 2. scoring forks (later, against realized prices)                            #
# --------------------------------------------------------------------------- #
def score_fork(fork: Dict[str, Any], current_prices: Dict[str, float],
               as_of: Any = None) -> Dict[str, Any]:
    """Score one fork. Returns the fork with forgone/held/regret filled, or
    with regret_pct=None and a stated reason when prices are unavailable.
    NEVER invents a price."""
    f = dict(fork or {})
    px = current_prices or {}
    scored_on = _as_date(as_of) or date.today()
    opened = _as_date(f.get("date"))
    f["scored_on"] = str(scored_on)
    f["horizon_days"] = ((scored_on - opened).days if opened else None)

    kind = f.get("fork")
    if kind in (NO_ACTION, SWITCH_TAKEN):
        p_buy0, p_sell0 = _f(f.get("alt_price")), _f(f.get("ref_price"))
        p_buy1, p_sell1 = px.get(f.get("symbol")), px.get(f.get("counterparty"))
        if not all(v for v in (p_buy0, p_sell0, p_buy1, p_sell1)):
            f.update({"regret_pct": None, "note": "missing price on one leg"})
            return f
        forgone = (p_buy1 / p_buy0 - 1.0) * 100.0
        held = (p_sell1 / p_sell0 - 1.0) * 100.0
        f["forgone_pct"] = round(forgone, 4)
        f["held_pct"] = round(held, 4)
        # declining: regret is what the buy beat the hold by.
        # taking it: the sign flips — the decision WAS the buy.
        f["regret_pct"] = round(forgone - held
                                if kind == NO_ACTION else held - forgone, 4)
        return f

    p0 = _f(f.get("ref_price"))
    p1 = px.get(f.get("symbol"))
    if not p0 or not p1:
        f.update({"regret_pct": None, "note": "no reference or current price"})
        return f
    forgone = (float(p1) / float(p0) - 1.0) * 100.0
    f["forgone_pct"] = round(forgone, 4)
    f["held_pct"] = None
    f["regret_pct"] = round(forgone, 4)     # up => the block cost that much
    return f


def score_all(forks: Sequence[Dict[str, Any]],
              current_prices: Dict[str, float],
              as_of: Any = None) -> List[Dict[str, Any]]:
    return [score_fork(f, current_prices, as_of) for f in forks or []]


# --------------------------------------------------------------------------- #
# 3. aggregation                                                               #
# --------------------------------------------------------------------------- #
def summarize(scored: Sequence[Dict[str, Any]],
              min_horizon_days: int = 1) -> Dict[str, Any]:
    """Aggregate by fork kind. Only forks with a computed regret AND at least
    `min_horizon_days` elapsed are counted; the rest are reported as pending
    so a thin sample can never masquerade as a conclusion."""
    buckets: Dict[str, List[float]] = {k: [] for k in FORK_KINDS}
    pending = 0
    for f in scored or []:
        r = f.get("regret_pct")
        h = f.get("horizon_days")
        if r is None or h is None or h < min_horizon_days:
            pending += 1
            continue
        buckets.setdefault(f.get("fork", "OTHER"), []).append(float(r))
    out: Dict[str, Any] = {"version": __version__, "pending": pending,
                           "min_horizon_days": min_horizon_days, "by_fork": {}}
    for kind, vals in buckets.items():
        if not vals:
            out["by_fork"][kind] = {"n": 0, "mean_regret_pct": None,
                                    "worst_pct": None, "hit_rate": None}
            continue
        vals_sorted = sorted(vals)
        mid = len(vals_sorted) // 2
        median = (vals_sorted[mid] if len(vals_sorted) % 2
                  else (vals_sorted[mid - 1] + vals_sorted[mid]) / 2.0)
        out["by_fork"][kind] = {
            "n": len(vals),
            "mean_regret_pct": round(sum(vals) / len(vals), 4),
            "median_regret_pct": round(median, 4),
            "worst_pct": round(max(vals), 4),
            "hit_rate": round(sum(1 for v in vals if v > 0) / len(vals), 4),
        }
    cb = out["by_fork"].get(COMPLIANCE_BLOCK, {})
    fl = out["by_fork"].get(FLOOR_LOCK, {})
    notes: List[str] = []
    if cb.get("n"):
        notes.append(f"shariah screen has cost {cb['mean_regret_pct']:+.2f}% "
                     f"on average across {cb['n']} blocked name(s) — a measured "
                     f"price of a chosen constraint, not a recommendation")
    if fl.get("n"):
        notes.append(f"venue floors have cost {fl['mean_regret_pct']:+.2f}% "
                     f"across {fl['n']} name(s) — this one is actionable via "
                     f"equity")
    out["notes"] = notes
    out["governance"] = ("measurement only — the ledger never recommends "
                         "overriding a compliance verdict")
    return out


def to_rows(scored: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    """Ledger rows in LEDGER_HEADER order, for an append-only sheet tab."""
    rows: List[List[Any]] = []
    for f in scored or []:
        rows.append([
            f.get("date", ""), f.get("fork", ""), f.get("symbol", ""),
            f.get("counterparty", ""), f.get("reason", ""),
            f.get("ref_price", ""), f.get("alt_price", ""),
            f.get("horizon_days", ""), f.get("scored_on", ""),
            f.get("forgone_pct", ""), f.get("held_pct", ""),
            f.get("regret_pct", ""), f.get("note", ""),
        ])
    return rows


# --------------------------------------------------------------------------- #
# SELFTEST                                                                     #
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []
    today = date(2026, 7, 19)
    later = date(2026, 8, 18)

    # board shaped like Shadow_Board v1.1.3 (17 cols)
    board = [
        ["6960.T", "Fukuda", "", "Healthcare", 35.0, "High",
         "MODEL_SCREEN_PASS_NOT_FATWA", "MODEL", "BROKER_TRADABLE", "", "YES",
         0.8, 21.9, 2.4, "TRADE", 1.1, "YES"],
        ["TRMD.US", "TORM", "", "Energy", 35.0, "High", "MODEL_SCREEN_FAIL",
         "MODEL", "BROKER_TRADABLE", "", "YES", 0.1, 22.6, 1.5, "TRADE",
         37.1, "NO"],
        ["0083.HK", "Sino Land", "", "Real Estate", 17.5, "High",
         "MODEL_SCREEN_PASS_NOT_FATWA", "MODEL", "BROKER_TRADABLE", "HongKong",
         "NO", 0.85, 10.5, 2.55, "TRADE", 5.1, "NO"],
        ["ENKAI.IS", "Enka", "", "Industrials", 17.5, "High",
         "BROKER_UNTRADABLE", "MODEL", "BROKER_UNTRADABLE", "", "", "", "",
         "", "NO_COST_MODEL", 1.3, "NO"],
    ]
    px0 = {"TRMD.US": 30.0, "0083.HK": 8.0, "ENKAI.IS": 50.0}
    forks = forks_from_board(board, today, px0)
    kinds = {f["symbol"]: f["fork"] for f in forks}
    checks.append(("eligible names produce no fork", "6960.T" not in kinds))
    checks.append(("screen failure -> COMPLIANCE_BLOCK",
                   kinds.get("TRMD.US") == COMPLIANCE_BLOCK))
    checks.append(("compliant but floor-locked -> FLOOR_LOCK",
                   kinds.get("0083.HK") == FLOOR_LOCK))
    checks.append(("untradable venue -> COMPLIANCE_BLOCK (broker refusal)",
                   kinds.get("ENKAI.IS") == COMPLIANCE_BLOCK))
    checks.append(("ref prices captured at fork time",
                   all(f.get("ref_price") for f in forks)))

    # a month later: TRMD +20%, 0083.HK -10%
    px1 = {"TRMD.US": 36.0, "0083.HK": 7.2, "ENKAI.IS": 50.0}
    scored = score_all(forks, px1, later)
    by = {f["symbol"]: f for f in scored}
    checks.append(("blocked name that rose => POSITIVE regret (screen cost)",
                   abs(by["TRMD.US"]["regret_pct"] - 20.0) < 1e-6))
    checks.append(("blocked name that fell => NEGATIVE regret (vindicated)",
                   abs(by["0083.HK"]["regret_pct"] + 10.0) < 1e-6))
    checks.append(("horizon days computed", by["TRMD.US"]["horizon_days"] == 30))
    checks.append(("flat name => zero regret",
                   abs(by["ENKAI.IS"]["regret_pct"]) < 1e-9))

    # switch forks
    prop = {"buy": "6960.T", "sell": "4200.SR", "delta_pct": 29.9,
            "hurdle_pct": 2.59}
    px_s0 = {"6960.T": 100.0, "4200.SR": 50.0}
    declined = fork_from_switch(prop, taken=False, as_of=today, prices=px_s0)
    taken = fork_from_switch(prop, taken=True, as_of=today, prices=px_s0)
    checks.append(("switch fork records both legs",
                   declined["symbol"] == "6960.T"
                   and declined["counterparty"] == "4200.SR"
                   and declined["fork"] == NO_ACTION and taken["fork"] == SWITCH_TAKEN))
    px_s1 = {"6960.T": 115.0, "4200.SR": 51.0}     # buy +15%, hold +2%
    sd = score_fork(declined, px_s1, later)
    st = score_fork(taken, px_s1, later)
    checks.append(("declining a switch that would have won => +regret",
                   abs(sd["regret_pct"] - 13.0) < 1e-6))
    checks.append(("taking that same switch => mirror-image regret",
                   abs(st["regret_pct"] + 13.0) < 1e-6))
    px_s2 = {"6960.T": 90.0, "4200.SR": 55.0}      # buy -10%, hold +10%
    checks.append(("declining a switch that would have lost => -regret",
                   score_fork(declined, px_s2, later)["regret_pct"] < 0))
    checks.append(("missing leg price => None, never invented",
                   score_fork(declined, {"6960.T": 115.0}, later)["regret_pct"]
                   is None))
    checks.append(("missing current price => None with a stated reason",
                   score_fork({"date": str(today), "fork": COMPLIANCE_BLOCK,
                               "symbol": "X.US", "ref_price": 10.0}, {},
                              later)["regret_pct"] is None))

    summ = summarize(scored + [sd, st])
    checks.append(("summary buckets by fork kind",
                   summ["by_fork"][COMPLIANCE_BLOCK]["n"] == 2
                   and summ["by_fork"][FLOOR_LOCK]["n"] == 1))
    checks.append(("compliance mean regret = (+20 + 0)/2",
                   abs(summ["by_fork"][COMPLIANCE_BLOCK]["mean_regret_pct"]
                       - 10.0) < 1e-6))
    checks.append(("hit rate counts only positive regrets",
                   summ["by_fork"][COMPLIANCE_BLOCK]["hit_rate"] == 0.5))
    checks.append(("note states the screen's price without recommending override",
                   any("not a recommendation" in n for n in summ["notes"])))
    checks.append(("governance forbids override advice",
                   "never recommends overriding" in summ["governance"]))
    pend = summarize([{"fork": COMPLIANCE_BLOCK, "regret_pct": None,
                       "horizon_days": None}])
    checks.append(("unscoreable forks counted as pending, not as zero",
                   pend["pending"] == 1
                   and pend["by_fork"][COMPLIANCE_BLOCK]["n"] == 0))
    thin = summarize(scored, min_horizon_days=60)
    checks.append(("min horizon filter defers immature forks",
                   thin["pending"] == len(scored)))
    rows = to_rows(scored)
    checks.append(("ledger rows match header width",
                   all(len(r) == len(LEDGER_HEADER) for r in rows)))
    checks.append(("empty inputs are safe",
                   forks_from_board([], today) == [] and score_all([], {}) == []
                   and summarize([])["pending"] == 0))

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[regret v{__version__}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(_selftest())
