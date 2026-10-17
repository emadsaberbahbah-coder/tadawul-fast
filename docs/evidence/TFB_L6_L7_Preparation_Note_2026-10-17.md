# TFB L6 / L7 — preparation note (not delivered as code in this branch)

**Date:** 2026-10-17 · **Brief items:** Codex Brief v2 §2 L6 (target ROI unit contract, P-204) and L7 (risk fields / risk statistics, P-203 / P-205) · **Status:** scoped with the exact anchors; **no code change**. Both items touch the Performance_Log record-creation site and need Emad's decisions (unit migration policy; NAV-based vs per-horizon risk) before a dark build is worth a commit.

## L6 — Target ROI unit contract

Measured (brief): 17,642 records store `Target ROI %` as a fraction, 4,793 as percent points, 34 inconsistent; the percent share rises (Jul 9 % → Oct 42 %). The P-158 sentry (`TFB_PERF_TARGET_UNIT_SENTRY`, `_s1_unit_sentry_measure` / `_s1_unit_sentry_apply`, `_S1_Calibration` rows 4–9) already measures the corrected cohort (n=6,512, MAE 3.24 pp, 135 unresolved) in observe.

| Part | Anchor | Prepared change |
|---|---|---|
| (a) writer fix | `PerformanceTrackerApp._record_to_row` (track_performance.py ≈ L6254) and the record constructor at the `--record` site | write `target_roi` in ONE unit (percent points) and append an explicit `Target ROI Unit` column (`pct`) at the END of `HEADERS` (end-append law, v6.6.0); the loader reads the unit by header name and falls back to the P-158 inference for rows without it. |
| (b) migration | new `scripts/migrate_target_roi_unit.py` (report-first, triple-gated like `repair_grid_capacity.py`) | stamps the inferred unit (`pct` / `frac` / `unresolved`) into the new column on historical rows; never rewrites `Target ROI %`; unresolved stays unresolved; archive CSV of the touched rows before any write; `_Run_Log` row with counts and sha. |
| (c) calibration | `_s1_unit_sentry_apply` enforce path + the `_S1_Calibration` rows 1–2 writer | criterion-4 primary = unit-corrected basis; legacy figure kept beside it (`Legacy MAE (pp)` column) for one cycle; the S-1 scorer reads row 2 by header name, so the appended column cannot move anything. |
| Acceptance | `tests/test_tp_target_unit_contract_l6.py` (to write) | new records 100 % one unit; the "percent-consistent" count frozen at 4,793 after the migration; `_S1_Calibration` reports the corrected basis as primary. |

Decision asked (D-L6): approve the percent-points unit for new records and the archive-first migration window (one audit lane run, 20:47Z).

## L7 — Risk fields and risk statistics

Measured (brief): `Volatility`, `Max Drawdown %`, `Sharpe Ratio` are 0.0 on all 22,469 records; `PerformanceAnalyzer.analyze` pools every matured ROI across 1W/2W/1M/3M (track_performance.py ≈ L7207 `s.sharpe_ratio = self.risk.sharpe_ratio(rois)`) and `RiskCalculator.sharpe_ratio` (≈ L7070) applies `− rfr/252` and `× √252` to that pool — a daily-Sharpe formula on mixed-horizon percentage points.

| Part | Anchor | Prepared change |
|---|---|---|
| (a) blanks, not zeros | `_risk_blank_enabled` / `_record_to_row` (≈ L3823–3848) — already built in v6.41.0 behind `TRACK_RISK_BLANK` (mapped in both lanes, unarmed) | **Emad's variable** (§8): `TRACK_RISK_BLANK=1` after the ENV-parity read-back (`tests/test_workflow_flag_mappings.py` proves both writer lanes map it). No code. |
| (b) per-horizon dispersion | `forecast_scorecard_rows` (L3, v6.48.0) | add `Std ROI %` and `Max Drawdown %` (min realized ROI) per cohort row; one column append each (header end-append). The Scorecard then carries the per-horizon dispersion the brief asks for; the pooled Sharpe is not needed for it. |
| (b, preferred) account risk block | new pure `account_risk_block(nav_series)` fed by the IBKR TWR series the portfolio reconciliation already pulls read-only (inception 2026-05-13; HWM 23,140.26 SAR on 7 Sep; −4.1 % today) | daily-NAV Sharpe/Sortino (√252 is correct on a daily series), max drawdown from the HWM path, published as one `Account risk` block on `_S1_Calibration` (rows ≥ 20, outside everything the S-1 consumer reads) and surfaced on Portfolio_Decision's KPI strip; reconciles to the IBKR series within rounding. Needs the NAV series transport (the capture JSON carries balances, not the TWR path → D-CAP / C2 decision). |
| (c) retire the pooled figure | the five surfaces: `PerformanceSummary.sharpe_ratio` (≈ L5174), `--analyze` stdout (≈ L10738 `| Sharpe:`), the summary tab writer (≈ L6409 `["Sharpe Ratio", …]`), the HTML report card (≈ L7922), `update_summary` | replace with `n/a (pooled retired; see Account risk)` under `TRACK_POOLED_SHARPE=off` (default off keeps today's text until the account block exists; on → retired). The deflated-Sharpe backtest path (≈ L8292, per-hypothesis series) is a different, valid use and stays. |

Decision asked (D-L7): NAV-based account risk (preferred, needs the TWR transport from C2's capture producer) or per-horizon-only; and the arming of `TRACK_RISK_BLANK=1`.
