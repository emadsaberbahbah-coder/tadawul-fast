# Hypothesis H-29 — "Entry Score separates winners" (registered 2026-09-01; first evidence same day; NOT yet a gate change)

| Field | Value |
|---|---|
| ID | H-29 |
| Statement | The engine's `Entry Score` (the overall score at cohort entry) separates WIN from LOSS cohorts: the top half of scores wins materially more often than the bottom half, and the effect holds out of sample. Corollary: an `Entry Score` floor (or tilt) is a legitimate replacement for the reliability floor that H-28 rejected. |
| Data | Performance_Log 2026-08-31 export; matured WIN/LOSS, deduplicated by Key (first occurrence). All cohorts n=5,401; board-origin (`Origin Tab=Top_10_Investments`) n=4,413; out-of-sample window `Date Recorded ≥ 2026-08-01` n=1,594. |
| Method | `scripts/tfb_backtest.py` v1.1.0 (quintiles; 5-fold CV Brier vs base rate; z-scored win spread). |

## Evidence
| scope | verdict | detail |
|---|---|---|
| all cohorts | WEAK | Q1–Q2 ≈ 58% vs Q3–Q5 ≈ 63–65%, z 3.5, CV gain +0.0004 |
| board-origin cohorts (n=4,413) | **SEPARATES** | CV gain and spread both pass the rule |
| since 2026-08-01 (n=1,594, most recent, cleanest data) | **SEPARATES** | quintiles **49.4 / 49.5 / 61.4 / 66.0 / 66.6 %**, spread 17.2 pp (z 4.4), CV gain +0.0046; median realized ROI −0.06 / 0.00 / +0.26 / +0.75 / **+1.78 %** |
| `Entry Recommendation` (same window) | WEAK | ACCUMULATE **74.6%** (n=126, median ROI +2.55%) vs HOLD 58.1% vs BUY 51.9% (n=77) |

## Reading
- The bottom two score quintiles are coin flips (49%); the top two win two out of three. That is the first signal in this log that behaves like a signal — and it is strongest on the most recent cohorts, i.e. it is not an artefact of the pre-08-16 window.
- ACCUMULATE outperforms BUY in every scope; the BUY label is the weakest of the three in the recent window (n=77 — small).

## Decision rule proposed for the operator (no code until decided)
- **H-29a (floor):** replace `Min Reliability ≥ 70` with `Entry/Overall Score ≥ median of the current pool` (or a fixed floor at the Q3 boundary), keeping `DQ ≥ 80`. Expected effect on the recent window: base win 58.6% → ~65% among admitted names.
- **H-29b (label):** treat ACCUMULATE as an eligible recommendation on par with BUY for board entry (it already is in the panel; confirm), and stop privileging BUY.
- **Validation gate before arming:** re-run `backtest_signals.yml` after the 3M cohorts mature (mid-September) and require SEPARATES on the out-of-sample window again; then arm as a Render flag with a before/after ticket diff, per the arming ledger.

## Reproduce
`python scripts/tfb_backtest.py --xlsx <export> --signal "Entry Score" --signal "Entry Recommendation" --since 2026-08-01` — JSON: `docs/evidence/backtest_h29_out_of_sample_since_2026-08-01.json`.
