# Hypothesis H-28 — "Stated Forecast Reliability predicts cohort outcomes" (registered 2026-09-01, backtested same day)

## Registration (Hypothesis_Registry row, paste-ready)
| Field | Value |
|---|---|
| ID | H-28 |
| Registered | 2026-09-01 (Claude, from the track_performance analyzer finding of run #81) |
| Statement | Higher stated `Forecast Reliability Score` at entry → higher probability of WIN and higher realized ROI over 1W/2W/1M horizons; therefore (a) band-level calibration factors improve Brier over bucket-level, and (b) the Top-10 `Min Reliability ≥ 70` floor improves selected-cohort outcomes. |
| Data | Performance_Log as of 2026-08-31 export; matured, decided (WIN/LOSS), **deduplicated by Key (v6.35.0 rule)**: n = 5,401 (1W 1,450 · 2W 1,454 · 1M 2,497 · 3M 0 matured yet). Stated reliability = `Entry Forecast Reliability`; realized = `Realized ROI %`. |
| Method | Win rate and realized-ROI distribution by reliability band; Brier of raw reliability as probability vs naive 0.5 vs constant base rate; 5-fold cross-validated Brier of bucket-level, band-level, band×bucket calibration (shrinkage k=20); Spearman rank correlations. |
| Result | **REJECTED (both parts).** |

## Evidence (measured)
| band | n | win % | mean ROI % | median ROI % |
|---|---|---|---|---|
| 0–50 | 1,900 | 61.2 | +1.45 | +0.85 |
| 50–70 | 1,969 | 61.6 | +1.34 | +0.75 |
| 70–85 | 1,441 | 62.7 | +2.26 | +1.58 |
| 85–100 | 91 | 51.6 | +0.43 | +0.18 |

- Brier, raw reliability as probability: **0.2679** — worse than a coin flip (0.2500) and worse than the constant base rate (0.2365).
- 5-fold CV Brier: bucket-level 0.2370 · band-level 0.2370 · band×bucket 0.2368 · **constant base rate 0.2368** → no calibration scheme extracts information from the score.
- Reliability quintiles → win %: 59 / 63 / 59 / 67 / 60 (noise). Spearman(reliability, realized ROI) = **0.033**; Spearman(entry score, realized ROI) = 0.058.
- Confidence label: HIGH 53% win (n=452) < MEDIUM 63% (n=4,560) — inverted.
- Caveats: outcomes are target-hit within horizon (WIN/LOSS), not sign of return; 3M cohorts have not matured yet; the 85–100 band is small (n=91); the population is the recorded universe (Decision_Coverage + Top_10 origins), not board-executed trades only.

## Consequences (decisions for the operator — recommendation-touching, no code until decided)
1. **Do NOT build calibrator v2 (band-level factors):** it cannot beat the base rate on this data. The bucket-level factors already published (`INVESTABLE:0.811`) are honest about the *level* (investable names hit their targets less often than stated) but carry no ranking value.
2. **The Top-10 `Min Reliability ≥ 70` floor is gating on noise.** It excluded 6 of the 16 sane candidates on 08-31 for a score that does not separate winners. Options: (a) lower the floor to a data-quality floor only (DQ ≥ 80 stays), keep reliability as display; (b) replace it with a floor on a signal that DOES separate — to be found by the same backtest method (candidates: engine ROI band, provider-target presence, sector, horizon); (c) keep it (status quo, knowingly). Proposal: **(a) now, (b) as H-29 next week** on the same 5,401 cohorts.
3. `TFB_CALIBRATION_ADJUST` (reliability haircut) remains safe to arm — it lowers a number that has no predictive content; it changes nothing material.
4. The Strategy's self-learning loop worked exactly as designed here: a weight change was proposed, registered, backtested, and rejected before touching a recommendation.

## Reproduce
`python3` on the Performance_Log export: dedupe by Key, filter matured WIN/LOSS, band by `Entry Forecast Reliability`, Brier + 5-fold CV as above (script logic in the session transcript; can be shipped as `scripts/backtest_h28.py` on request).
