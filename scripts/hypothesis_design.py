# -*- coding: utf-8 -*-
"""
scripts/hypothesis_design.py — design (b) news->sector hypotheses for testability
=================================================================================
Version: 1.0.0

WHY THIS EXISTS
---------------
The strategy is emphatic that every (b) news->sector and (c) theme-rotation claim
must be DESIGNED for testability BEFORE it is registered — retrofitting fails.
This harness encodes candidate hypotheses and runs each through the core.stats
power gates, so we keep only the ones that can actually be detected when
News_Archive matures, and we see the design levers (horizon, trigger frequency,
universe) that flip an untestable design into a testable one.

It evaluates each candidate under BOTH gates:
  * PROPORTION gate (testability_report)  - does the post-event directional
    hit-rate differ from baseline? Robust but weak: needs hundreds of events.
  * MAGNITUDE gate (magnitude_testability_report) - does the CAR differ from
    zero (one-sample t-test on cumulative abnormal returns)? Far more powerful:
    uses how much the sector moved, so it needs ~10x fewer events. This is the
    test a real event study should use.

EVERYTHING IN THE CONFIG IS AN EDITABLE DESIGN ASSUMPTION, not a claim about the
world. Trigger rates are rough estimates; the OUTPUT (the testability verdict) is
exact given the inputs. Replace the candidates and rates with the events/sectors
you actually care about and re-run.

Run:  python scripts/hypothesis_design.py
"""

from __future__ import annotations

from typing import Any, Dict, List

try:
    from core.stats import testability_report, magnitude_testability_report
except Exception:  # standalone/test fallback
    from stats import testability_report, magnitude_testability_report  # type: ignore

__version__ = "1.0.0"

TRADING_DAYS_PER_MONTH = 21

# Accumulation windows to evaluate (months of News_Archive history).
ACCUMULATION_MONTHS = [3, 6, 12, 24]

# Shared design defaults (editable).
BASE_RATE = 0.50            # baseline daily up-rate of a sector (proportion test)
PLAUSIBLE_PROPORTION = 0.10  # smallest directional-rate lift worth detecting (10pp)
PLAUSIBLE_D = 0.50          # smallest standardized CAR effect worth detecting (medium)

# ---------------------------------------------------------------------------
# CANDIDATE HYPOTHESES  ===  EDIT WITH YOUR DOMAIN KNOWLEDGE
# Each: trigger (specific, measurable) -> sector response (specific, directional)
# over a SHORT horizon, with an estimated per-trading-day trigger frequency.
# ---------------------------------------------------------------------------
HYPOTHESES: List[Dict[str, Any]] = [
    {
        "id": "ENERGY_OIL",
        "name": "Oil shock -> Energy",
        "trigger": "Brent daily move >= +/-3%",
        "response": "TASI Energy sector same-direction CAR",
        "horizon_days": 5,
        "trigger_rate_per_day": 0.12,
        "rate_basis": "~12% of trading days see a >=3% Brent move (vol-dependent est.)",
    },
    {
        "id": "PETCHEM_OIL",
        "name": "Oil shock -> Petrochemicals",
        "trigger": "Brent daily move >= +/-3%",
        "response": "Materials/Petrochemical (SABIC-heavy) same-direction CAR",
        "horizon_days": 5,
        "trigger_rate_per_day": 0.12,
        "rate_basis": "same trigger as ENERGY_OIL",
    },
    {
        "id": "BANKS_RATES",
        "name": "Policy rate -> Banks",
        "trigger": "SAMA policy-rate change (follows Fed via the riyal peg)",
        "response": "Banks sector CAR",
        "horizon_days": 5,
        "trigger_rate_per_day": 8 / 252,
        "rate_basis": "~8 scheduled decisions/year -> rare trigger",
    },
    {
        "id": "RISKOFF_DEF",
        "name": "Global risk-off -> Defensives",
        "trigger": "Global risk-off day (S&P 500 <= -1.5%)",
        "response": "TASI defensives (utilities/staples/healthcare) outperform cyclicals",
        "horizon_days": 5,
        "trigger_rate_per_day": 0.09,
        "rate_basis": "~9% of days see an S&P <=-1.5% move (est.)",
    },
    {
        "id": "EARN_SURPRISE",
        "name": "Earnings surprise -> own sector (pooled)",
        "trigger": "Large-cap earnings surprise beyond threshold, pooled across constituents",
        "response": "The surprising name's sector CAR",
        "horizon_days": 3,
        "trigger_rate_per_day": 0.14,
        "rate_basis": "~30 names x 4 reports/yr x ~30% surprise -> ~36 events/yr",
    },
    {
        "id": "AI_THEME",
        "name": "AI/tech momentum -> KSA tech/IPO rotation (component c)",
        "trigger": "Global AI/tech momentum regime shift (Nasdaq tech 20d strength)",
        "response": "KSA tech / recent-IPO basket rotation CAR",
        "horizon_days": 10,
        "trigger_rate_per_day": 0.03,
        "rate_basis": "regime shifts ~6-10/yr; NOTE: 'regime' is persistent/"
                      "autocorrelated -> reframe as discrete shift events to test cleanly",
    },
]


def evaluate(h: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Run one hypothesis through both gates across the accumulation windows."""
    out: List[Dict[str, Any]] = []
    for months in ACCUMULATION_MONTHS:
        td = months * TRADING_DAYS_PER_MONTH
        prop = testability_report(total_windows=td,
                                  trigger_rate=h["trigger_rate_per_day"],
                                  base_rate=BASE_RATE,
                                  plausible_effect=PLAUSIBLE_PROPORTION)
        mag = magnitude_testability_report(total_windows=td,
                                           trigger_rate=h["trigger_rate_per_day"],
                                           plausible_d=PLAUSIBLE_D)
        out.append({"months": months, "events": prop["effective_n"],
                    "proportion": prop["verdict"], "magnitude": mag["verdict"]})
    return out


def first_testable_month(rows: List[Dict[str, Any]], gate: str) -> Any:
    for r in rows:
        if r[gate] == "TESTABLE":
            return r["months"]
    return None


def main() -> int:
    print(f"TFB (b) hypothesis testability design  (core.stats power gates)")
    print(f"defaults: base_rate={BASE_RATE}, plausible_proportion_lift="
          f"{PLAUSIBLE_PROPORTION}, plausible_CAR_d={PLAUSIBLE_D}")
    print("=" * 78)
    summary: List[str] = []
    for h in HYPOTHESES:
        rows = evaluate(h)
        print(f"\n[{h['id']}] {h['name']}")
        print(f"   trigger : {h['trigger']}  (rate~{h['trigger_rate_per_day']:.3f}/day)")
        print(f"   response: {h['response']}   horizon={h['horizon_days']}d")
        print(f"   {'months':>7} {'events':>7}  {'proportion-test':>18}  {'magnitude-test':>16}")
        for r in rows:
            print(f"   {r['months']:>7} {r['events']:>7.1f}  "
                  f"{r['proportion']:>18}  {r['magnitude']:>16}")
        ft_mag = first_testable_month(rows, "magnitude")
        ft_prop = first_testable_month(rows, "proportion")
        verdict = (f"magnitude-testable from ~{ft_mag} months"
                   if ft_mag else "NOT magnitude-testable within 24 months -> redesign")
        prop_note = (f"; proportion-testable from ~{ft_prop} months"
                     if ft_prop else "; proportion-test never reachable in 24 months")
        summary.append(f"[{h['id']}] {verdict}{prop_note}")

    print("\n" + "=" * 78)
    print("SUMMARY (magnitude test is the realistic one):")
    for s in summary:
        print("  " + s)
    print("\nTakeaway: the proportion (directional) test needs hundreds of events and")
    print("is mostly unreachable for years; design (b) on CAR MAGNITUDE. Even then,")
    print("only reasonably common triggers clear the gate within 12 months, and rare")
    print("triggers (rate decisions) or persistent 'regime' triggers (AI theme) need")
    print("redesign or multi-year data. These verdicts assume a medium (d=0.5) effect")
    print("actually exists -- if the true effect is smaller, push every date later.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
