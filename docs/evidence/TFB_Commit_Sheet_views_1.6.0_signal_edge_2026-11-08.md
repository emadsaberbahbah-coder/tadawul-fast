# Commit Sheet: the signal edge (apply_bq_views 1.6.0), 2026-11-08

**Scope.** Owner request 8 Nov 2026 11:02 Riyadh (verbatim: "Please make full review for the Project and need to improve the outcome and analysis"). Two research views keep the 8 Nov signal study running on every archived day. Nothing live reads BigQuery: no decision, gate, count, ticket or broker action changes.

## Why
| Finding (8 Nov archive study, 8,230 symbol-days, 28 Jun - 8 Nov) | Why it matters | Fix |
|---|---|---|
| The engine's ROI forecast does not rank the next weeks; its top fifth trails the middle, and seats at the 34 % cap lost on average. | Ranking and the owner rule lean on that forecast. | `v_signal_edge_daily` tests every forecast horizon every day. |
| Forecast reliability and confidence carry no rank edge in any market. | The pre-registered ranking challenger sorts by reliability band first. | Same view; the verdict says `no edge` while that holds. |
| `overall_score` is the one signal with a stable positive rank IC over the next week (US names). | A score-ordered challenger needs evidence that stays current. | `v_signal_edge` states one verdict per signal, market group and horizon. |
| The study was a set of one-off queries. | The next review must not rebuild 200 lines of SQL. | Two views, one fixed rule. |

## Changes
| Change | Tests | Installed |
|---|---|---|
| `sql/bigquery/74_v_signal_edge_daily.sql` (new) | V28: first recording time per symbol-day; signals AS OF the forecast (newest at or before it, 3-day floor, never later); 20-name floor and two distinct values; average ranks for ties keyed as INT64; top / bottom thirds; horizon lengths; no window partitioned by a FLOAT64 | pending (17d8d490) |
| `sql/bigquery/75_v_signal_edge.sql` (new) | V29: overlap-adjusted t; halves by NTILE(2); verdict arms in order thin / edge / inverse / no edge / unstable | pending (14dbbb5d) |
| `scripts/apply_bq_views.py` 1.5.0 -> 1.6.0 (header WHY); `scripts/verify_deployment.py` pin | manifest pins | - |
| `tests/test_bq_views.py`: the four 1.5.0 bodies moved to INSTALLED (read back from INFORMATION_SCHEMA on 8 Nov: all four match); PENDING = the two 1.6.0 bodies | V1, V4 | - |

## Pre-merge checks
- **Live BigQuery (read-only, 8 Nov, both bodies run in full with the daily view inlined).** Verdicts today: `edge` - overall_score 1W (ALL: mean IC +0.095, t_adj 2.18, both halves positive, 70 % of days positive; US: +0.143, t_adj 2.31, top third +0.98 pp a week, bottom third -0.44) and stance 1W (ALL, +0.076, t_adj 2.06). `no edge` - reliability (ALL 1W +0.023, US +0.005), confidence (ALL 1W -0.014). `unstable` - engine ROI forecasts at 1W (t_adj 1.1-1.5). Every Tadawul signal `no edge` at 1W; 1M rows `no edge` or `thin` (80 days are under three 1M windows).
- **Tests.** test_bq_views 29/29 (27 + V28 / V29); with test_verify_manifest_pins and test_bq_archive 64 passed.
- **Golden negatives / mutants.** 13 SQL mutants, each killed by V28 or V29 (pins aside): a 12-hour look-ahead in the as-of join, the oldest instead of the newest signal, plain RANK() without the tie term, a 10-name floor, a 3,000 % return filter, dropping the first-recording join, a bottom half instead of a third, a 21-day 1M overlap, the naive t, an edge without the halves check, thin without the horizon rule, the full mean as the second half, NULL ICs in the summary. V28's tie fixture reads IC 0 with average ranks and -0.09 with min ranks.

## Post-merge order (lead)
1. A fresh `bq_views` dispatch with mode=write outside 10:00-15:00 Riyadh and outside a running sync installs the two bodies; verify must read 43/43, then the two pins move to INSTALLED.
2. Looker: a "Signal edge" table (v_signal_edge filtered to verdict != thin) beside the calibration table.
3. Read it before any ranking promotion or owner-rule change.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
