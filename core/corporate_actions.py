"""
core/corporate_actions.py — TFB Gen-2 Corporate-Actions Immunizer (library)
============================================================================
VERSION 1.0.0  (2026-07-18)  — NEW MODULE (Wave A0, deliverable #6a)

WHY (Master Plan v2.1 §5 + §13; urgency: first 1M cohort matures 2026-07-28):
  * Performance_Log stores raw Entry Price anchors. A 4-for-1 split between
    entry and maturity would score as a fake −75% "loss" and poison the very
    first calibration cohort. This library makes prices comparable across
    splits/reverse-splits and symbol renames.
  * Governance split: this module is PURE (parse/adjust/detect; no I/O).
    scripts/repair_corporate_actions.py owns the sheet tab `_Corporate_Actions`
    and the human-confirmed apply loop. AUTO-DETECTED candidates are proposals
    only — an operator-confirmed Source is required before any adjustment.

CONVENTIONS:
  * ratio semantics: SPLIT ratio r = r-for-1 (price divides by r after the
    effective date). REVERSE_SPLIT ratio r = 1-for-r (price multiplies by r);
    internally both become one price_factor f where
        comparable_old_price = old_price / f
    (SPLIT: f = r ; REVERSE_SPLIT: f = 1/r). Stacked actions multiply.
  * date rule: an action with Effective Date D applies to prices dated < D
    (strictly before). Prices on/after D are already on the new basis.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.0"
CORPORATE_ACTIONS_VERSION = __version__

TAB_ACTIONS = "_Corporate_Actions"
ACTIONS_HEADER = ["Symbol", "Action Type", "Effective Date", "Ratio",
                  "Old Symbol", "New Symbol", "Source", "Detected At", "Notes"]
SOURCE_AUTO = "AUTO_DETECT"          # proposals: never applied automatically
APPLIED_TAG = "ca_adjusted"          # Notes tag written by the repair tool

_SPLIT = "SPLIT"
_REVERSE = "REVERSE_SPLIT"
_RENAME = "SYMBOL_CHANGE"

_CANONICAL_RATIOS = (2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 10.0, 15.0, 20.0)
_LOG_TOL = 0.06        # |ln(observed) − ln(k)| tolerance for detection
_MIN_MOVE = 0.40       # |ratio − 1| must exceed this to even consider


def _as_date(v: Any) -> Optional[date]:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v or "").strip()[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _as_float(v: Any) -> Optional[float]:
    try:
        f = float(str(v).replace(",", "").strip())
        return f if math.isfinite(f) else None
    except Exception:
        return None


@dataclass(frozen=True)
class Action:
    symbol: str
    action_type: str          # SPLIT | REVERSE_SPLIT | SYMBOL_CHANGE
    effective: date
    ratio: Optional[float]    # split semantics above; None for renames
    old_symbol: str = ""
    new_symbol: str = ""
    source: str = ""
    notes: str = ""

    @property
    def price_factor(self) -> Optional[float]:
        if self.action_type == _SPLIT and self.ratio:
            return float(self.ratio)
        if self.action_type == _REVERSE and self.ratio:
            return 1.0 / float(self.ratio)
        return None

    @property
    def confirmed(self) -> bool:
        return bool(self.source) and self.source.strip().upper() != SOURCE_AUTO


def parse_actions(values: Sequence[Sequence[Any]]) -> List[Action]:
    """Parse `_Corporate_Actions` values (header row 1). Malformed rows skip."""
    out: List[Action] = []
    for row in (values or [])[1:]:
        if not row or not str(row[0]).strip():
            continue
        sym = str(row[0]).strip().upper()
        atype = str(row[1]).strip().upper() if len(row) > 1 else ""
        eff = _as_date(row[2] if len(row) > 2 else None)
        ratio = _as_float(row[3] if len(row) > 3 else None)
        if atype not in (_SPLIT, _REVERSE, _RENAME) or eff is None:
            continue
        if atype in (_SPLIT, _REVERSE) and (ratio is None or ratio <= 0):
            continue
        out.append(Action(
            symbol=sym, action_type=atype, effective=eff, ratio=ratio,
            old_symbol=str(row[4]).strip().upper() if len(row) > 4 else "",
            new_symbol=str(row[5]).strip().upper() if len(row) > 5 else "",
            source=str(row[6]).strip() if len(row) > 6 else "",
            notes=str(row[8]).strip() if len(row) > 8 else ""))
    return out


def build_adjustment_index(
        actions: Sequence[Action],
        confirmed_only: bool = True) -> Dict[str, List[Tuple[date, float]]]:
    """{symbol: sorted [(effective_date, price_factor), ...]} for price actions."""
    idx: Dict[str, List[Tuple[date, float]]] = {}
    for a in actions:
        f = a.price_factor
        if f is None:
            continue
        if confirmed_only and not a.confirmed:
            continue
        idx.setdefault(a.symbol, []).append((a.effective, f))
    for sym in idx:
        idx[sym].sort()
    return idx


def cumulative_factor(symbol: str, price_date: Optional[date],
                      index: Dict[str, List[Tuple[date, float]]]) -> float:
    """Product of factors for actions effective AFTER price_date (strict).
    Unknown price_date conservatively applies all factors (oldest anchors)."""
    total = 1.0
    for eff, f in index.get(str(symbol).strip().upper(), []):
        if price_date is None or price_date < eff:
            total *= f
    return total


def adjust_price(symbol: str, price: Optional[float], price_date: Optional[date],
                 index: Dict[str, List[Tuple[date, float]]]) -> Optional[float]:
    """Historic price -> current comparable basis (see conventions)."""
    if price is None:
        return None
    f = cumulative_factor(symbol, price_date, index)
    return price if f == 1.0 else price / f


def build_symbol_map(actions: Sequence[Action],
                     confirmed_only: bool = True) -> Dict[str, str]:
    """old -> newest symbol (rename chains collapsed)."""
    direct: Dict[str, str] = {}
    for a in sorted((x for x in actions if x.action_type == _RENAME),
                    key=lambda x: x.effective):
        if confirmed_only and not a.confirmed:
            continue
        old = a.old_symbol or a.symbol
        new = a.new_symbol
        if old and new:
            direct[old] = new
    def _resolve(s: str, hops: int = 8) -> str:
        while s in direct and hops > 0:
            s = direct[s]; hops -= 1
        return s
    return {old: _resolve(new) for old, new in direct.items()}


def resolve_symbol(symbol: str, symbol_map: Dict[str, str]) -> str:
    return symbol_map.get(str(symbol).strip().upper(), str(symbol).strip().upper())


def detect_split_candidate(entry_price: Optional[float],
                           current_price: Optional[float]
                           ) -> Optional[Tuple[str, float]]:
    """Discontinuity detector for the sweep. Returns (SPLIT, k) when
    entry/current ≈ k, or (REVERSE_SPLIT, k) when current/entry ≈ k, for a
    canonical k — else None. Dividend-sized moves never trigger (min 40%)."""
    e, c = _as_float(entry_price), _as_float(current_price)
    if not e or not c or e <= 0 or c <= 0:
        return None
    r = e / c
    if abs(r - 1.0) < _MIN_MOVE and abs((1.0 / r) - 1.0) < _MIN_MOVE:
        return None
    for k in _CANONICAL_RATIOS:
        if abs(math.log(r) - math.log(k)) <= _LOG_TOL:
            return (_SPLIT, k)
        if abs(math.log(1.0 / r) - math.log(k)) <= _LOG_TOL:
            return (_REVERSE, k)
    return None


# --------------------------------------------------------------------------- #
# SELFTEST                                                                     #
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []
    vals = [
        ACTIONS_HEADER,
        ["ABCD.US", "SPLIT", "2026-07-10", "4", "", "", "CONFIRMED", "", ""],
        ["ABCD.US", "SPLIT", "2026-07-15", "2", "", "", "CONFIRMED", "", ""],
        ["WXYZ.US", "REVERSE_SPLIT", "2026-07-12", "10", "", "", "CONFIRMED", "", ""],
        ["PROP.US", "SPLIT", "2026-07-12", "5", "", "", "AUTO_DETECT", "", ""],
        ["OLDN.US", "SYMBOL_CHANGE", "2026-07-01", "", "OLDN.US", "MIDN.US", "CONFIRMED", "", ""],
        ["MIDN.US", "SYMBOL_CHANGE", "2026-07-05", "", "MIDN.US", "NEWN.US", "CONFIRMED", "", ""],
        ["BAD.US", "SPLIT", "not-a-date", "2", "", "", "CONFIRMED", "", ""],
    ]
    acts = parse_actions(vals)
    checks.append(("parse: 6 valid, malformed skipped", len(acts) == 6))
    idx = build_adjustment_index(acts)
    checks.append(("auto-detect excluded from index", "PROP.US" not in idx))
    checks.append(("stacked split factor 8x",
                   abs(cumulative_factor("ABCD.US", date(2026, 7, 1), idx) - 8.0) < 1e-9))
    checks.append(("between-actions factor 2x",
                   abs(cumulative_factor("ABCD.US", date(2026, 7, 12), idx) - 2.0) < 1e-9))
    checks.append(("on-date already new basis",
                   cumulative_factor("ABCD.US", date(2026, 7, 15), idx) == 1.0))
    checks.append(("split adjust 100 -> 12.5",
                   adjust_price("ABCD.US", 100.0, date(2026, 7, 1), idx) == 12.5))
    checks.append(("reverse 1-for-10 adjust 3 -> 30",
                   adjust_price("WXYZ.US", 3.0, date(2026, 7, 1), idx) == 30.0))
    checks.append(("unknown date conservative (all factors)",
                   adjust_price("ABCD.US", 80.0, None, idx) == 10.0))
    smap = build_symbol_map(acts)
    checks.append(("rename chain collapses",
                   resolve_symbol("OLDN.US", smap) == "NEWN.US"
                   and resolve_symbol("AAPL", smap) == "AAPL"))
    checks.append(("detect 4:1 split", detect_split_candidate(100, 25.3) == (_SPLIT, 4.0)))
    checks.append(("detect 1-for-10 reverse",
                   detect_split_candidate(2.0, 19.6) == (_REVERSE, 10.0)))
    checks.append(("dividend-size move ignored",
                   detect_split_candidate(100, 92) is None))
    checks.append(("non-canonical big move ignored",
                   detect_split_candidate(100, 37) is None))

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[corporate_actions v{__version__}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(_selftest())
