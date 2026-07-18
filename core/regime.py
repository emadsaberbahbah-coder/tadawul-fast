"""
core/regime.py — TFB Gen-2 Regime Gate & Momentum Allocator (pure library)
===========================================================================
VERSION 1.0.0  (2026-07-18)  — NEW MODULE (Wave A, deliverable #9)

WHY (Master Plan v2.1 §7.6, §12): drawdown budget B (0.75%/position, 6% heat,
12%/yr) needs a slow, evidence-worthy market-state signal. The 10-month-MA
trend filter is the classic, heavily-studied specimen (Faber-style timing);
we implement it with HYSTERESIS + MIN-HOLD so it cannot whipsaw monthly, plus
an absolute/relative momentum allocator between sleeves with hard bounds.

GOVERNANCE (non-negotiable, §8): this module DRIVES NOTHING. It computes and
stamps. Activation follows the Hypothesis_Registry protocol exactly like the
flow/candle hypotheses (registered hypothesis -> backtest -> min sample ->
effect + t-stat -> human-reviewed weight change). Until that gate passes,
consumers may only display the stamp (Shadow_Board / Weekly memo).

PURITY: stdlib only; no I/O, no env. Callers (worker) fetch SPUS/TASI series
via the existing chart provider and feed them in. Execution semantics for any
future activation are next-session-open, monthly cadence (§18.1).
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.0"
REGIME_VERSION = __version__

RISK_ON = "RISK_ON"
RISK_OFF = "RISK_OFF"


# --------------------------------------------------------------------------- #
# series helpers                                                               #
# --------------------------------------------------------------------------- #
def monthly_from_daily(daily: Sequence[Tuple[date, float]]
                       ) -> List[Tuple[date, float]]:
    """Collapse a (date, close) daily series to month-end observations
    (last close seen in each calendar month; input order preserved)."""
    out: List[Tuple[date, float]] = []
    for d, c in daily or []:
        if c is None:
            continue
        if out and (out[-1][0].year, out[-1][0].month) == (d.year, d.month):
            out[-1] = (d, float(c))
        else:
            out.append((d, float(c)))
    return out


def sma(values: Sequence[float], n: int) -> Optional[float]:
    vals = [v for v in values[-n:] if v is not None]
    if len(vals) < n:
        return None
    return sum(vals) / float(n)


# --------------------------------------------------------------------------- #
# regime state machine (10M MA, hysteresis band, min-hold)                     #
# --------------------------------------------------------------------------- #
def regime_series(monthly: Sequence[Tuple[date, float]],
                  n: int = 10,
                  band: float = 0.01,
                  min_hold: int = 1) -> List[Dict[str, Any]]:
    """Walk the monthly series and emit one state row per month once the MA
    exists. Hysteresis: enter RISK_OFF only when close < MA*(1-band); return
    to RISK_ON only when close > MA*(1+band); inside the band the previous
    state persists. min_hold: a state younger than min_hold months cannot
    flip (whipsaw brake). First classifiable month seeds the state with the
    plain close-vs-MA sign (no band, no hold)."""
    closes: List[float] = []
    out: List[Dict[str, Any]] = []
    state: Optional[str] = None
    months_in = 0
    for d, c in monthly or []:
        closes.append(float(c))
        ma = sma(closes, n)
        if ma is None:
            continue
        dist = (float(c) / ma - 1.0) * 100.0
        if state is None:
            state = RISK_ON if c >= ma else RISK_OFF
            months_in = 1
        else:
            want = state
            if state == RISK_ON and c < ma * (1.0 - band):
                want = RISK_OFF
            elif state == RISK_OFF and c > ma * (1.0 + band):
                want = RISK_ON
            if want != state and months_in >= min_hold:
                state, months_in = want, 1
            else:
                months_in += 1
        out.append({"date": d, "close": float(c), "ma": round(ma, 4),
                    "state": state, "months_in_state": months_in,
                    "distance_pct": round(dist, 2)})
    return out


def current_regime(monthly: Sequence[Tuple[date, float]],
                   n: int = 10, band: float = 0.01,
                   min_hold: int = 1) -> Dict[str, Any]:
    series = regime_series(monthly, n=n, band=band, min_hold=min_hold)
    if not series:
        return {"state": "UNKNOWN", "reason": f"need >= {n} monthly closes",
                "version": __version__}
    last = dict(series[-1])
    last.update({"version": __version__, "n": n, "band": band,
                 "min_hold": min_hold})
    return last


# --------------------------------------------------------------------------- #
# momentum                                                                     #
# --------------------------------------------------------------------------- #
def abs_momentum_pct(monthly: Sequence[Tuple[date, float]],
                     lookback: int = 12, skip: int = 1) -> Optional[float]:
    """Classic 12-1: return over `lookback` months ending `skip` months ago,
    e.g. close[-1-skip] / close[-1-skip-lookback] - 1 (in %)."""
    closes = [c for _, c in monthly or [] if c is not None]
    if len(closes) < lookback + skip + 1:
        return None
    end = closes[-1 - skip]
    start = closes[-1 - skip - lookback]
    if not start:
        return None
    return (end / start - 1.0) * 100.0


def relative_momentum(sleeves: Dict[str, Sequence[Tuple[date, float]]],
                      lookback: int = 12, skip: int = 1
                      ) -> List[Tuple[str, Optional[float]]]:
    ranked = [(name, abs_momentum_pct(series, lookback, skip))
              for name, series in (sleeves or {}).items()]
    return sorted(ranked, key=lambda x: (x[1] is None, -(x[1] or 0.0)))


# --------------------------------------------------------------------------- #
# allocator (bounded, interpretable, deterministic)                            #
# --------------------------------------------------------------------------- #
def allocate(sleeve_states: Dict[str, Dict[str, Any]],
             base_weights: Optional[Dict[str, float]] = None,
             bounds: Optional[Dict[str, Tuple[float, float]]] = None,
             tilt: float = 0.10) -> Dict[str, Any]:
    """Rules, in order (each application logged in `rationale`):
      1. start from base_weights (default Global 0.70 / Saudi 0.30);
      2. any sleeve RISK_OFF or abs_mom_pct <= 0 -> its weight moves to Cash;
      3. among surviving sleeves, shift up to `tilt` from the weakest to the
         strongest by abs momentum (needs >= 2 survivors with momentum);
      4. clamp every sleeve into its (min, max) bounds; residue -> Cash.
    Weights always sum to 1.0; Cash is the honest remainder, never negative."""
    base = dict(base_weights or {"Global": 0.70, "Saudi": 0.30})
    bnds = dict(bounds or {"Saudi": (0.0, 0.30), "Global": (0.0, 0.85)})
    rationale: List[str] = []
    w = dict(base)
    cash = max(0.0, 1.0 - sum(w.values()))

    for name in list(w):
        st = (sleeve_states or {}).get(name, {})
        mom = st.get("abs_mom_pct")
        if st.get("regime") == RISK_OFF or (mom is not None and mom <= 0):
            rationale.append(
                f"{name}: {'RISK_OFF' if st.get('regime') == RISK_OFF else 'abs momentum <= 0'}"
                f" -> weight {w[name]:.2f} to Cash")
            cash += w[name]
            w[name] = 0.0

    live = [(n, (sleeve_states or {}).get(n, {}).get("abs_mom_pct"))
            for n in w if w[n] > 0]
    live = [(n, m) for n, m in live if m is not None]
    if len(live) >= 2 and tilt > 0:
        live.sort(key=lambda x: -x[1])
        strong, weak = live[0][0], live[-1][0]
        if live[0][1] > live[-1][1]:
            shift = min(tilt, w[weak])
            w[weak] -= shift
            w[strong] += shift
            rationale.append(f"relative momentum: +{shift:.2f} {weak} -> {strong}")

    for name in list(w):
        lo, hi = bnds.get(name, (0.0, 1.0))
        if w[name] > hi:
            rationale.append(f"{name}: capped {w[name]:.2f} -> {hi:.2f}; residue to Cash")
            cash += w[name] - hi
            w[name] = hi
        elif 0 < w[name] < lo:
            rationale.append(f"{name}: floored {w[name]:.2f} -> {lo:.2f} from Cash")
            cash -= (lo - w[name])
            w[name] = lo

    cash = max(0.0, round(cash, 6))
    total = sum(w.values()) + cash
    if total > 0 and abs(total - 1.0) > 1e-9:
        w = {k: v / total for k, v in w.items()}
        cash = cash / total
    out = {k: round(v, 4) for k, v in w.items()}
    out["Cash"] = round(cash, 4)
    return {"weights": out, "rationale": rationale, "version": __version__,
            "governance": "STAMP-ONLY until Hypothesis_Registry gate passes"}


def regime_stamp(states: Dict[str, Dict[str, Any]],
                 alloc: Dict[str, Any]) -> Dict[str, Any]:
    """Compact block for meta / Shadow_Board / Weekly memo."""
    return {
        "version": __version__,
        "sleeves": {k: {"state": v.get("state") or v.get("regime"),
                        "distance_pct": v.get("distance_pct"),
                        "months_in_state": v.get("months_in_state"),
                        "abs_mom_pct": (round(v["abs_mom_pct"], 2)
                                        if v.get("abs_mom_pct") is not None
                                        else None)}
                    for k, v in (states or {}).items()},
        "suggested_weights": alloc.get("weights"),
        "governance": "advisory stamp — drives nothing until gated",
    }


# --------------------------------------------------------------------------- #
# SELFTEST                                                                     #
# --------------------------------------------------------------------------- #
def _mk(vals: Sequence[float], start_year: int = 2024) -> List[Tuple[date, float]]:
    out = []
    y, m = start_year, 1
    for v in vals:
        out.append((date(y, m, 28), float(v)))
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []

    daily = [(date(2026, 7, d), 100.0 + d) for d in (1, 2, 15)] + \
            [(date(2026, 8, 1), 130.0)]
    mo = monthly_from_daily(daily)
    checks.append(("monthly collapse keeps last close per month",
                   len(mo) == 2 and mo[0][1] == 115.0 and mo[1][1] == 130.0))
    checks.append(("sma needs n points", sma([1, 2, 3], 4) is None
                   and sma([1, 2, 3, 5], 4) == 2.75))

    flat = _mk([100] * 10 + [104, 99.5, 98, 89, 88, 102, 112])
    rs = regime_series(flat, n=10, band=0.01, min_hold=1)
    checks.append(("seed state RISK_ON at first MA month",
                   rs[0]["state"] == RISK_ON))
    # 99.5 vs MA~100.4: inside 1% band -> stays ON; 89 breaks band -> OFF
    st = {r["date"].month: r["state"] for r in rs}
    checks.append(("hysteresis holds inside band", st[12] == RISK_ON))
    checks.append(("break below band flips OFF", st[2] == RISK_OFF))
    checks.append(("re-entry above band flips ON", st[5] == RISK_ON))
    # min_hold: break at months_in=2 -> held; at months_in=3 -> flips.
    brk = _mk([100] * 10 + [90, 90, 90])
    h1 = {r["date"].month: r["state"]
          for r in regime_series(brk, n=10, band=0.01, min_hold=1)}
    h3 = {r["date"].month: r["state"]
          for r in regime_series(brk, n=10, band=0.01, min_hold=3)}
    checks.append(("min_hold=3 delays the flip",
                   h1[11] == RISK_OFF and h3[11] == RISK_ON
                   and h3[12] == RISK_ON and h3[1] == RISK_OFF))
    checks.append(("months_in_state increments",
                   rs[1]["months_in_state"] == 2))

    mom = _mk([100] * 2 + [100, 105, 110, 115, 120, 125, 130, 135, 140, 145,
                           150, 155, 160])
    m12 = abs_momentum_pct(mom, lookback=12, skip=1)
    checks.append(("12-1 momentum = 55%", abs(m12 - 55.0) < 1e-9))
    rel = relative_momentum({"A": mom, "B": _mk([100] * 15)})
    checks.append(("relative ranking A first", rel[0][0] == "A" and rel[1][1] == 0.0))

    a1 = allocate({"Global": {"regime": RISK_ON, "abs_mom_pct": 12.0},
                   "Saudi": {"regime": RISK_ON, "abs_mom_pct": 12.0}}, tilt=0.0)
    checks.append(("both ON, no tilt -> base 0.70/0.30/0",
                   a1["weights"] == {"Global": 0.70, "Saudi": 0.30, "Cash": 0.0}))
    a2 = allocate({"Global": {"regime": RISK_ON, "abs_mom_pct": 12.0},
                   "Saudi": {"regime": RISK_OFF, "abs_mom_pct": 5.0}})
    checks.append(("Saudi OFF -> its 0.30 to Cash",
                   a2["weights"]["Saudi"] == 0.0 and a2["weights"]["Cash"] == 0.30
                   and "RISK_OFF" in a2["rationale"][0]))
    a3 = allocate({"Global": {"regime": RISK_ON, "abs_mom_pct": 20.0},
                   "Saudi": {"regime": RISK_ON, "abs_mom_pct": 2.0}})
    checks.append(("tilt 0.10 weak->strong then Global capped at 0.85",
                   a3["weights"]["Global"] == 0.80 and a3["weights"]["Saudi"] == 0.20))
    a4 = allocate({"Global": {"regime": RISK_ON, "abs_mom_pct": 20.0},
                   "Saudi": {"regime": RISK_ON, "abs_mom_pct": 2.0}}, tilt=0.20)
    checks.append(("bounds clamp: cap residue to Cash, sum=1",
                   a4["weights"]["Global"] == 0.85 and a4["weights"]["Cash"] == 0.05
                   and abs(sum(a4["weights"].values()) - 1.0) < 1e-9))
    stamp = regime_stamp({"Global": {**a3, "state": RISK_ON,
                                     "abs_mom_pct": 20.0}}, a3)
    checks.append(("stamp carries governance line",
                   "drives nothing" in stamp["governance"]))

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[regime v{__version__}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(_selftest())
