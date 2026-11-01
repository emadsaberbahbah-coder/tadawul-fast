# Commit Sheet: the win rate over the outcome cohort, whole-book weights (apply_bq_views 1.4.1), 2026-11-01

**Scope.** Follow-up to #886 (views 1.4.0, merged 1 Nov 11:14 Riyadh). The Codex review of #886 left two P2 findings after the merge; both are real data-accuracy points on the two new honesty views, so they are fixed before the first install (1.4.0 is still PENDING, so no installed body changes twice). Read-only for the workbook and the live system: no decision, gate, floor, executable-count or broker change.

## Findings and fixes
| Finding (Codex, #886) | Why it matters | Fix | View |
|---|---|---|---|
| `v_outcome_coverage` counted every matured Record ID in `scored` and `wins`, while the sheet's Directional Win Rate (`track_performance._summary_outcome_cohort`) counts a repeated stable key once. The source view already exposes `in_outcome_cohort` for exactly this. | Two archived copies of one winning key beside one loss would publish 66.7 % where the sheet says 50 %. Live today the cohort filter changes no rate (27 matured copies sit outside the cohort, none of them scored), so the fix is protective, not corrective. | `scored = COUNTIF(is_scored AND in_outcome_cohort)`, `wins = SUM(IF(in_outcome_cohort, IFNULL(win_flag, 0), 0))`; new `matured_cohort` and `matured_excluded` beside the raw `matured`; the ALL row sums them. `records`, `due`, `matured` stay raw for coverage. | 69_v_outcome_coverage.sql |
| `v_portfolio_insight.weight_sar_pct` divided by `SUM(value_sar) OVER ()`, which silently drops a holding whose `value_sar` is NULL (no decision row and no rate for its currency, the supported JPY case). | A SAR holding beside an unconvertible JPY holding would read 100 %: a share of the valued subset published as a portfolio weight. | `weight_sar_pct` is NULL for every holding while any holding has no riyal value; new `holdings_without_value` says how many, so the page can say "weights unavailable: N holdings have no riyal value". Live today: 0 holdings without a value, weights 50.61 / 49.39 % unchanged. | 68_v_portfolio_insight.sql |

## Changes
| Change | Tests | Installed |
|---|---|---|
| `69_v_outcome_coverage.sql`: cohort-only scored / wins, matured_cohort, matured_excluded | V23 (cohort rows in the simulator, the two-copies golden negative) | Not installed (never was; 1.4.0 pin replaced) |
| `68_v_portfolio_insight.sql`: whole-book weight, holdings_without_value | V22 (`_weights` simulator, partial-book golden negative) | Not installed (1.3.0 body live; 1.4.0 pin replaced) |
| `scripts/apply_bq_views.py` 1.4.0 -> 1.4.1; `verify_deployment` pin | manifest pins | - |
| `tests/test_bq_views.py`: pins, V22 / V23 | - | - |

Pins (PENDING until the next `bq_views` write): v_outcome_coverage `ab308c8c736da1bb8e9bfa052b5fccbe5151c15e483a6773e59ed98daf90fe7f` (was d84d0770), v_portfolio_insight `a905416997fa03335516b028edfa60a215d21d22ee6c75fdc54aba52eb64216b` (was 23425289); v_symbol_candles, v_fx_sar, v_symbol_rating, v_top10_insight unchanged from 1.3.1 / 1.4.0.

## Pre-merge checks
- **Live BigQuery (read-only, 1 Nov 12:50 Riyadh).** The 1.4.1 coverage body ran in full against the archive: 1W 3,728 matured / 3,715 cohort / 13 excluded, scored 1,038, wins 594 (57.2 %); 2W 3,290 / 3,277 / 13, 895, 495 (55.3 %); 1M 4,367 / 4,366 / 1, 1,299, 745 (57.4 %); 3M 1,092 / 1,092 / 0, 400, 164 (41.0 %); ALL 12,477 / 12,450 / 27, 3,632, 1,998 (55.0 %), overdue open 5,356, oldest target 3 Oct. Every rate equals 1.4.0's today. The weight expression ran on a synthetic book: full book 25 / 75 % with 0 missing; a book with one NULL value gives NULL, NULL, NULL with 1 missing. (The full portfolio body cannot run live until 1.4.0's v_fx_sar and v_symbol_rating are installed; its structure is 1.4.0's with the one expression changed.)
- **Tests.** test_bq_views 23/23; with test_bq_archive, test_archive_market_series, test_archive_symbol_deep, test_repository_workflow_audit, test_verify_manifest_pins: 97 passed. `apply_bq_views --dry-run`: 38 objects, static checks pass. `audit_repository_workflows`: errors=0 warnings=0. CI ruff rules (E9, F63, F7, F82): clean. compileall: clean.
- **Golden negatives.** V22 and V23 fail on the 1.4.0 files. Six SQL mutants of the 1.4.1 bodies, each killed: scored over every matured copy; wins over every matured copy; excluded counted as cohort; the ALL row dropping the cohort; the weight over the valued subset; the missing-value count removed.

## Post-merge order (agent)
1. The scheduled `bq_views` write at 15:05 Riyadh (outside 10:00-15:00, not during a sync) installs 1.3.1 + 1.4.0 + 1.4.1 together; verify must read 38/38.
2. Looker wiring per the 1.4.0 commit sheet, plus: the My Portfolio weight column shows "-" with a note when `holdings_without_value` > 0; the track-record coverage table shows `matured_cohort` and `matured_excluded` beside `matured`.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
