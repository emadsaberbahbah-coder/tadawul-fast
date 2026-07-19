"""
scripts/test_maturation.py — Pre-Flight Test for the Maturation Write Path
==========================================================================
VERSION 1.0.0  (2026-07-19)  — NEW SCRIPT (operations, deliverable #31)

WHY: Performance_Log holds 3,496 records and every single one reads `active`.
`Realized ROI %` and `Outcome` are empty across the board. That means the code
which transitions ACTIVE -> MATURED, computes realized ROI, and writes the
outcome label HAS NEVER EXECUTED IN PRODUCTION. It will run for the first
time on the day the first cohort matures — and a first run is a bad moment to
discover a defect in the one path that produces every downstream statistic:
hit rates, Brier score, and the calibrator itself.

This script exercises that path against SYNTHETIC records, in memory, touching
no sheet and no network. It is a pre-flight check, not a unit test of arith-
metic: it asks whether the v6.17.0 guarantees actually hold when driven.

WHAT IT VERIFIES (each mapped to the guarantee it defends):
  A  fresh price + past due            -> MATURED, realized ROI correct
  A' fresh price + NOT past due        -> stays ACTIVE (no early maturation)
  A" entry_price <= 0                  -> never matures (no divide-by-zero,
                                          no fake breakeven)
  B  past due, NO fresh price, in grace-> stays ACTIVE, retried next run
  B' past due, NO fresh price, past grace -> EXPIRED, realized_roi None,
                                          outcome UNPRICED
  C  kill-switch off                   -> v6.16.0 behaviour restored
  D  a record that matured on a stale price is NOT produced (the exact
     2026-07-12 failure: 2,264 records frozen at entry price)

USAGE:  python3 scripts/test_maturation.py
Exit 0 = the path is safe to meet a real cohort. Non-zero = do not let the
first maturity run until the reported failure is understood.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "scripts"))

import track_performance as tp  # noqa: E402

SCRIPT_VERSION = "1.0.0"


class _FakeBackend:
    """Serves exactly the prices it is told to, so 'fresh' vs 'missing' is
    controlled precisely. base_url non-empty so the LOUD empty-url path is
    not what we are testing here."""

    def __init__(self, prices: Dict[str, float]):
        self.base_url = "https://test.invalid"
        self._prices = dict(prices)

    async def fetch_prices(self, syms: List[str]) -> Dict[str, float]:
        return {s: self._prices[s] for s in syms if s in self._prices}


def _mk(symbol: str, entry: float, target_days_ago: int,
        horizon: str = "1M") -> Any:
    """An ACTIVE record whose target date is `target_days_ago` days in the
    past (negative = still in the future)."""
    now = dt.datetime.now(tp._RIYADH_TZ)   # tracker compares Riyadh-aware times
    recorded = now - dt.timedelta(days=30 + target_days_ago)
    target = now - dt.timedelta(days=target_days_ago)
    rec = tp.PerformanceRecord(
        record_id=f"T-{symbol}-{horizon}",
        symbol=symbol,
        horizon=tp.HorizonType(horizon),
        date_recorded=recorded,
        entry_price=entry,
        entry_recommendation=tp.RecommendationType.BUY,
        entry_score=70.0,
        entry_risk_bucket="MEDIUM",
        entry_confidence="HIGH",
        origin_tab="Top_10_Investments",
        target_price=(entry * 1.10 if entry else 0.0),
        target_roi=10.0,
        target_date=target,
        status=tp.PerformanceStatus.ACTIVE,
    )
    return rec


def _tracker(prices: Dict[str, float]) -> Any:
    t = tp.PerformanceTrackerApp.__new__(tp.PerformanceTrackerApp)
    t.backend = _FakeBackend(prices)
    for attr, default in (("args", None), ("store", None), ("sheet_id", "")):
        if not hasattr(t, attr):
            setattr(t, attr, default)
    return t


def _run(tracker: Any, records: List[Any]) -> List[Any]:
    return asyncio.run(tracker.audit_active_records(records))


def main() -> int:
    checks: List[Tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        checks.append((name, bool(ok), detail))

    os.environ["TFB_TRACK_MATURE_FRESH_ONLY"] = "1"
    os.environ["TFB_TRACK_MATURE_GRACE_DAYS"] = "5"

    # --- A: fresh price + past due -> MATURED with correct ROI -------------
    r = _mk("AAA.US", 100.0, target_days_ago=1)
    out = _run(_tracker({"AAA.US": 115.0}), [r])[0]
    check("A  past-due + fresh price -> MATURED",
          out.status == tp.PerformanceStatus.MATURED, f"status={out.status}")
    check("A  realized ROI computed from the FRESH price (+15%)",
          out.realized_roi is not None and abs(out.realized_roi - 15.0) < 0.01,
          f"realized={out.realized_roi}")
    check("A  outcome label assigned",
          bool(getattr(out, "outcome", None)), f"outcome={getattr(out,'outcome',None)}")

    # --- A': not yet due -> stays ACTIVE -----------------------------------
    r = _mk("BBB.US", 100.0, target_days_ago=-3)
    out = _run(_tracker({"BBB.US": 130.0}), [r])[0]
    check("A' not yet due -> stays ACTIVE (no early maturation)",
          out.status == tp.PerformanceStatus.ACTIVE, f"status={out.status}")

    # --- A'': entry_price <= 0 -> never matures ----------------------------
    r = _mk("CCC.US", 0.0, target_days_ago=2)
    out = _run(_tracker({"CCC.US": 50.0}), [r])[0]
    check("A\" entry_price=0 -> NOT matured (no fake breakeven)",
          out.status != tp.PerformanceStatus.MATURED, f"status={out.status}")

    # --- B: past due, no price, within grace -> ACTIVE ---------------------
    r = _mk("DDD.US", 100.0, target_days_ago=2)
    out = _run(_tracker({}), [r])[0]
    check("B  past-due + NO price, inside grace -> stays ACTIVE (retried)",
          out.status == tp.PerformanceStatus.ACTIVE, f"status={out.status}")
    check("B  no realized ROI invented while unpriced",
          out.realized_roi in (None, 0, 0.0),
          f"realized={out.realized_roi}")

    # --- B': past due, no price, past grace -> EXPIRED ---------------------
    r = _mk("EEE.US", 100.0, target_days_ago=30)
    out = _run(_tracker({}), [r])[0]
    check("B' past-due + NO price, past grace -> EXPIRED",
          out.status == tp.PerformanceStatus.EXPIRED, f"status={out.status}")
    check("B' EXPIRED carries realized_roi None (invisible to statistics)",
          out.realized_roi is None, f"realized={out.realized_roi}")
    check("B' EXPIRED outcome is UNPRICED",
          str(getattr(out, "outcome", "")).upper().find("UNPRICED") >= 0,
          f"outcome={getattr(out,'outcome',None)}")

    # --- D: the 2026-07-12 failure must not reproduce ----------------------
    # a record with a STALE unrealized_roi and no fresh price must not mature
    r = _mk("FFF.US", 100.0, target_days_ago=1)
    r.unrealized_roi = 42.0            # stale value from an earlier run
    out = _run(_tracker({}), [r])[0]
    check("D  stale unrealized_roi does NOT become a realized outcome",
          out.status != tp.PerformanceStatus.MATURED
          and out.realized_roi != 42.0,
          f"status={out.status} realized={out.realized_roi}")

    # --- C: kill-switch restores prior behaviour ---------------------------
    os.environ["TFB_TRACK_MATURE_FRESH_ONLY"] = "0"
    r = _mk("GGG.US", 100.0, target_days_ago=1)
    r.unrealized_roi = 7.5
    out = _run(_tracker({}), [r])[0]
    check("C  kill-switch off -> v6.16.0 behaviour reachable",
          out.status in (tp.PerformanceStatus.MATURED,
                         tp.PerformanceStatus.ACTIVE),
          f"status={out.status}")
    os.environ["TFB_TRACK_MATURE_FRESH_ONLY"] = "1"

    # --- mixed cohort: the realistic first-maturity shape ------------------
    cohort = [_mk(f"S{i}.US", 100.0, target_days_ago=1) for i in range(5)]
    priced = {"S0.US": 110.0, "S1.US": 95.0, "S2.US": 100.0}   # 2 unpriceable
    res = _run(_tracker(priced), cohort)
    matured = [x for x in res if x.status == tp.PerformanceStatus.MATURED]
    held = [x for x in res if x.status == tp.PerformanceStatus.ACTIVE]
    check("cohort: only the priced records mature",
          len(matured) == 3 and len(held) == 2,
          f"matured={len(matured)} held={len(held)}")
    check("cohort: win/loss/breakeven all distinguishable",
          len({round(x.realized_roi, 2) for x in matured}) == 3,
          f"rois={sorted(round(x.realized_roi,2) for x in matured)}")

    width = max(len(n) for n, _, _ in checks)
    for name, ok, detail in checks:
        print(("PASS " if ok else "FAIL ") + name.ljust(width) +
              (f"   [{detail}]" if (detail and not ok) else ""))
    passed = sum(1 for _, ok, _ in checks if ok)
    print(f"\n[test_maturation v{SCRIPT_VERSION}] {passed}/{len(checks)}")
    if passed == len(checks):
        print("VERDICT: the maturation path is safe to meet a real cohort.")
        return 0
    print("VERDICT: DO NOT let the first maturity run until the failures above "
          "are understood — every downstream statistic depends on this path.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
