# Commit sheet: truth in labels and units (data_engine_v2 5.163.0, scoring 5.11.5)

Date: 24 Nov 2026. Package 2 of the week plan in the review of the 23 Nov deep audit
(https://claude.ai/artifact/LZSNP5XMc4egz58hvYRXMr), built on the morning of 24 Nov.

## Why

These are audit findings 3, 6, 7 and 10. Each was reproduced against main and the archive of 23 Nov.

| # | Finding | Evidence |
|---|---|---|
| 3 | The page shows 365 days / "1Y" while the score used the one-month weights | All 6,602 Global_Markets and 255 Market_Leaders rows read 1Y / 365 on 23 Nov. With no horizon on the row, `core.scoring` scores with `Horizon.MONTH` and labels it "3M". `_horizon_coherence_fill` then fills 365, and `core/sheet_presentation.py` derives "1Y" from it. |
| 6a | An explicit percent is divided twice | `_as_fraction("200%")` = 0.02, the same as "2%". |
| 6b | Outstanding shares stand in for the float | `float_shares` lists `sharesOutstanding` / `SharesOutstanding` among its aliases. |
| 7 | "High leverage" never fires on ratio data | The test is D/E >= 100, but D/E is stored as a ratio (median 0.47). Only 6 of 6,054 Global rows can trip it, and 744 names with D/E from 2 to 100 never can. |
| 10 | The Data_Dictionary types Country as a number | "count" matches inside "country". |

## What changes

One switch, `TFB_LABEL_TRUTH = off | observe | enforce`:
- Unset or blank -> **observe**: tags only, every value and verdict unchanged.
- off -> no tag and the v5.162.0 values.
- enforce -> the fixes apply.

| Fix | observe | enforce |
|---|---|---|
| Horizon (`_label_truth_horizon`, end of `_apply_page_row_backfill`) | `label_truth_horizon:<shown>d_vs_scored_<days>d:observe` where the row's days differ from the scored horizon | `horizon_days` and `invest_period_label` take the scored pair: month -> 3M / 90, week -> 1W / 6, day -> 1D / 1, long -> 1Y / 365. `horizon_days_effective` is also set while blank. A stamped row rescored reads 90 -> MONTH, so the weights never change. |
| Percent (`core.scoring._as_fraction`) | `label_truth_percent:<field>:observe` before scoring, for an explicit percent string of 150 % or more | The explicit percent keeps one division ("200%" -> 2.0). |
| Leverage (`_build_top_factors_and_risks`) | `label_truth_leverage:would_flag\|would_clear:observe` where the rules disagree | D/E read as a ratio (100 or more is percent points, divided by 100), flagged at 2.0. |
| Float (`_canonicalize_provider_row`) | `label_truth_float:outstanding:observe`, legacy value kept | `float_shares` stays blank when only outstanding shares exist. |
| Country (`_dd_infer_dtype_fmt`) | (nothing to tag on that page) | country / countries typed as text. |

- No tag contains a word the reliability penalty reads (cap, forecast, target, roi, drop, reject).
- `surface_gate_states()` gains `"label_truth"`.
- `docs/render_env_contract.md` is regenerated.

## Files

| File | Change |
|---|---|
| core/data_engine_v2.py | 5.162.0 -> 5.163.0: WHY block, the helpers, four call sites, the surface key |
| core/scoring.py | 5.11.4 -> 5.11.5: `_label_truth_enforced`, the single division under enforce |
| tests/test_de_label_truth.py | NEW, 24 cases. Each fix has a positive case (the audit's defect) and a negative control (a row it must leave alone). Also covered: off vs observe identical but for warnings, idempotence, the scored-label round trip, the tag word check and the manifest. |
| tests/test_de_gate_evidence.py, test_de_market_and_selection_gate.py, test_de_evidence_cohort.py, test_eodhd_analyst_target.py, test_core_repair_tiny_quotes.py | Version pins 5.163.0 |
| scripts/verify_deployment.py | Pins: data_engine_v2 5.163.0, scoring 5.11.5 |
| .github/workflows/ci.yml | 1.1.98: the new suite in the blocking data engine job, after test_de_gate_evidence.py |
| docs/render_env_contract.md | Regenerated (row `label_truth`) |

## Tests run before the PR

| Suite | Result |
|---|---|
| tests/test_de_label_truth.py | 24 passed |
| The 62 suites that import the engine or its touched functions | 1,716 passed, 11 skipped |
| 17 more scoring / route / presentation suites | 458 passed, 10 skipped |
| Manifest and env-contract checks | Green |

## Read-back after deploy (observe, the default)

- The next nightly archive should show `label_truth_horizon:365d_vs_scored_90d:observe` on nearly every Global_Markets and Market_Leaders row.
- It should also show a few `label_truth_leverage:would_flag:observe` tags (names with D/E 2 to 100) and `label_truth_float:outstanding:observe` where a provider sent no float.
- After one clean observed run, the owner may set `TFB_LABEL_TRUTH=enforce` on Render. The page then shows 3M / 90 for those rows.

## Rollback

`TFB_LABEL_TRUTH=off` gives the v5.162.0 values with no tag.

Not a licensed financial adviser; the owner decides and executes every trade.
