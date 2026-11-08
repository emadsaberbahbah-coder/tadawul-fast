# Commit Sheet: risk-budget ticket sizing (opportunity_builder 1.29.0), 2026-11-08

**Scope.** Owner, 8 Nov 2026 (verbatim: "there is last week big loss happend how we can avoid that in the future"; "ok let's do the requirment and implement it the project"). Item 3 of the post-mortem plan (`TFB_PostMortem_Week_2026-11-02_to_06.md`, section 4b): size each ticket so the loss at its stop is a fixed share of the account. Default off (`TFB_OPP_SIZING_MODE` unset = cap, byte-identical).

## Why
| Finding (2-6 Nov, measured) | Why it matters | Fix |
|---|---|---|
| Three stop-outs in one session (5 Nov: SPY -1.1 %, VIX +15 %) plus the SUPN special-dividend / merger mechanics cost 4.7 % of the account in a week. Every ticket was sized at the per-seat cap (PF Max Position 15 %) and stopped 5-8 % below entry, so one stop-out cost 0.75-1.2 % and three cost the week. | `_size_one` sizes by capital: min(MaxWeight % x basis, remaining). It does not know how far the stop sits, so a wide-stop name and a tight-stop name get the same ticket and very different losses. | `TFB_OPP_SIZING_MODE=risk`: each seat's target = `TFB_OPP_RISK_BUDGET_PCT` (default 0.5 %) of the sizing basis divided by its own printed stop distance, within the per-seat cap. The loss at the stop is the same for every seat. |
| The conviction mode (v1.27.0) spreads the whole budget and tops up the remainder, so cash is always fully deployed. | After a loss week the owner's first ask is to lose less, not to deploy more. | Risk mode has **no top-up**: cash the risk budget does not spend stays in cash by design. |

## Changes
| Change | Tests | Arm |
|---|---|---|
| `core/analysis/opportunity_builder.py` 1.28.0 -> 1.29.0 [RISK SIZING]: `_env_sizing_mode` accepts `risk`; `_env_risk_budget_pct` (default 0.5, clamp 0.1..2.0, invalid -> 0.5); `_risk_targets` (PURE: basis x risk / stop_pct, capped, a sub-floor target released, a missing stop counts as `stop_max_pct`); the pre-loop branch computes targets for every INVEST seat (they do not depend on each other) and `meta.sizing = {mode, risk_pct, risk_sar, basis_sar, cap_sar, budget_sar, cohort, stop_pct, targets, released}`; in the loop a seat is sized `_size_one(..., min(remaining, target))` and a released seat is a countable deferral; `_near_miss_rows` classes a `Risk sizing:` deferral as a Funding near-miss with its own how-to. Conviction branch byte-identical; cap runs none of it. +2 functions, 0 removed | `tests/test_ob_risk_sizing.py` R1-R10 (10 passed); `tests/test_ob_conviction_sizing.py` 13 passed; every suite importing the builder: 636 passed, 2 skipped | `TFB_OPP_SIZING_MODE=risk` (+ optional `TFB_OPP_RISK_BUDGET_PCT`) on Render, outside a sync |
| `scripts/verify_deployment.py` manifest pin 1.28.0 -> 1.29.0 (pin only) | test_verify_manifest_pins | - |
| `.github/workflows/ci.yml` 1.1.67 -> 1.1.69 (the new suite in the lean lane; 1.1.68 is claimed by open PR #935) | test_repository_workflow_audit, test_workflow_audit_repairs | - |

## Pre-merge checks
- **Golden negative (R2, R9).** Basis 30,000, risk 2 %, stops 10 / 20 / 8 % -> 6,000 / 3,000 / 7,500: the loss at every stop is exactly 600. The WHY example (NAV 100,000, 0.5 %, cap 15 %): 8 % -> 6,250; 10 % -> 5,000; 20 % -> 2,500; 35 % -> 1,429; ten seats at 10 % stops deploy 50 % and a three-stop day costs 1.5 %, not 4.7 %.
- **Rules (R2-R4, R6-R8).** The per-seat cap binds (15 % of 30,000 = 4,500); a sub-floor ticket is released to cash as a Funding near-miss naming `TFB_OPP_RISK_BUDGET_PCT`; raising the budget or dropping the floor funds it; no top-up (the rest is cash); the research stage is never sized; the allocate replay sizes only the eligible seats on the same rule; the enforced absolute cash floor still bounds the budget while the risk basis stays NAV + deployable.
- **Kill (R1, R5).** Unset / `cap` / junk -> `{"mode": "cap"}` and the v1.28.0 sizes; `conviction` unchanged (spreads, tops up, no risk keys). Budget knob: `0.05` -> 0.1, `5` -> 2.0, `abc` / `nan` / `inf` -> 0.5.
- **Mutants.** Seven one-line mutations each caught by the suite: multiply instead of divide by the stop, cap dropped, floor release dropped, clamp dropped, stop fallback replaced, top-up under risk, research stage sized.
- **Fingerprint (R10).** `TFB_OPP_RISK_BUDGET_PCT` is read through `_env_str` in the `TFB_OPP_` family, so a change between a research pass and its allocate pass fails that one replay closed: arm outside a sync.
- **Not done here.** The cockpit prints the ticket the builder returns; nothing on the sheet changes until the mode is armed. The stop distance is the existing rule (max(8 %, 2.5 x vol30) capped at 35 %); the event-aware stop (item 4) and the pre-open re-check (item 2) are separate PRs.

## Post-merge order (lead)
1. Render ENV (owner): `TFB_OPP_SIZING_MODE=risk`; leave `TFB_OPP_RISK_BUDGET_PCT` at its 0.5 default or set `0.75`/`1` after reading one board. Set it outside 10:00-15:00 Riyadh and outside a sync; the redeploy is the arming step.
2. Read the next board's `meta.sizing` (targets, released) on `/v1/opportunities` and the Top_10 export; a released seat shows as a Funding near-miss "Risk sizing: ...".
3. Rollback: unset `TFB_OPP_SIZING_MODE` (or set `cap`).

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count increases (risk sizing only lowers tickets and may release a seat to cash).
