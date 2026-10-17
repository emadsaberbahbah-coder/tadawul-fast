# TFB Commit Sheet — L3 Forecast Scorecard (track_performance v6.48.0, 16_Decision_Top10 v1.17.0)

**Date:** 2026-10-17 · **Brief item:** Codex Brief v2 §2 L3 · **Branch:** `claude/new-session-twoifp` · **Base:** main `11ef134` · **Previous commit on branch:** `5dd40a0`

## 1. Problem and delivery

Performance_Log holds 11,791 matured records; their hit-rates are computed by `--analyze` and printed to a job log nobody reads (INVESTABLE 47.1 % n=963 = WATCHLIST 47.1 % n=10,100; BLOCKED 20.2 %; ACCUMULATE/1M 63.3 %; Origin Top_10 49.5 % vs Decision_Coverage 36.6 %; mean realized ROI 1M +0.94 %, 3M −1.32 %). The operator never sees them.

| File | Change |
|---|---|
| `scripts/track_performance.py` v6.47.0 → **v6.48.0** | `forecast_scorecard_rows(records)` (pure): one row per **horizon × dimension × cohort** over matured records with a valid realized ROI; dimensions ALL, Entry Investability, Entry Recommendation, Entry Score decile (D0–D9), Origin Tab, Entry Selected, Investability × Recommendation; horizons = each present horizon + ALL; columns `As Of (Riyadh), Horizon, Dimension, Cohort, n, Wins, Losses, Hit Rate %, Hit Lo %, Hit Hi %, Mean ROI %, Median ROI %, Tracker Version`; hit-rate decided-only (the analyzer's own definition), Wilson 95 % from `scripts.performance_math`; cohorts below `TRACK_SCORECARD_MIN_N` (default 5) dropped except the ALL rows. `_publish_forecast_scorecard` overwrites `_Forecast_Scorecard` in place (clear + `A1` update, header row 1) on the **audit lane only**, right after the S-1 calibration publish, best-effort: a tab fault never fails the clock. Gate `TRACK_FORECAST_SCORECARD` default ON (0/false/off/no → no write). |
| `scripts/tfb_workbook_dq_audit.py` | `_Forecast_Scorecard` registered in `KNOWN_TABS` (overwrite, per audited run) and `TAB_STAMP_COLUMN` ("As Of (Riyadh)") so the orphan-tab check and freshness read it. |
| `apps_script/16_Decision_Top10.gs` v1.16.0 → **v1.17.0** | 3-line **MODEL EVIDENCE** strip at the top of the dynamic zone (under the KPI strip, before SELECTED): (1) hit-rate by Entry Investability at 1M and 3M with n and the Wilson band; (2) every seat's own cohort (Investability × Recommendation at 1M, fallback to the Recommendation cohort, else n/a) with hit-rate and n; (3) Origin Top_10 vs Decision_Coverage and mean realized ROI per horizon — "evidence, not a gate (L4 observe decides)". Pure cores `dt10ScorecardIndex_` (header-name driven) and `dt10ModelEvidenceLines_`; `dt10ReadScorecard_` never throws; an absent/empty tab or the kill switch `DT10_MODEL_EVIDENCE_LEGACY='1'` draws nothing (page byte-identical to v1.16.0). Default ON (display-only). |

## 2. Acceptance

- **Tab written on every audited run:** `test_hook_sits_in_the_audit_lane_after_the_calibration_publish` pins the call inside `if self.args.audit:` after `_publish_s1_calibration` and before `analyze`; `test_publish_overwrites_in_place_and_creates_the_tab` proves create-then-write and clear-then-rewrite (never append).
- **Strip renders with n shown:** harness L2 renders the three lines from a synthetic grid shaped like the measured numbers (INVESTABLE 47.1 % n=963, Origin 49.5 % vs 36.6 %, 1M +0.94 %).
- **No scoring/gating change:** no builder, selector or clock code touched; the strip's closer names L4 as the only place outcomes may change a decision.

## 3. Decisions (flag for Emad)

- **Entry Forecast Source is not a Performance_Log column** (the record dataclass has no such field), so that dimension is deferred to the L6 writer change that adds unit/source columns at record creation. Entry Selected (champion stamp) is published instead as a sixth dimension.
- **Score deciles** are `D0`–`D9` on the 0–100 entry score (`int(score // 10)`, clamped).
- **min_n = 5** keeps the tab readable (a 7-dimension cross-product over 4 horizons would otherwise be thousands of sparse rows); the ALL rows always publish so the denominators are visible.
- **The strip sits in the dynamic zone, not in a new fixed row** — inserting rows above `DT10_ZONES_START` would change the layout fingerprint (`buildDecisionTop10Layout` guard) and the KPI-strip positional reads of the DQ audit.

## 4. Protocol evidence

- **Anchored edits:** tracker 5 + DQ 2 (`scratchpad/edit_l3_tracker.py`), GAS 6 (`scratchpad/edit_l3_gas.py`); every anchor `count == 1`; reversal layer `tests/js/dt10_v1170_history.js` → v1.16.0 → … → pinned v1.13.8 digest (V2).
- **Zero-removal proof:** tracker removed lines = `SCRIPT_VERSION` (rewritten); DQ audit removed 0; GAS removed = header `Version:` and `DT10_VERSION` (rewritten). Functions added: tracker 4 (+1 method), GAS 5; vars GAS 1; removed 0. Census 212 → 217 / 105 → 106.
- **ES5:** every added GAS line passes the V3 guard.
- **Dark/kill:** tracker gate `TRACK_FORECAST_SCORECARD=0`; GAS kill `DT10_MODEL_EVIDENCE_LEGACY=1`; nothing renders until the tab exists. No ENV, Script Property or cell changed by this PR.
- **Harness:** pytest `tests/test_tp_forecast_scorecard_l3.py` 7/7 + L5/L2/p191/audit-lane/DQ suites green (version pin moved to 6.48.0); `node tests/js/dt10_v1170_model_evidence.test.js` 7/7; the nine other GAS harnesses pass against v1.17.0 (pins 1.17.0 / 217 / 106; `model evidence core: ok` in the self-test list); ruff clean; `ci.yml` lean list + JS step updated.

## 5. Read-back (Claude/Emad)

1. First scheduled audit lane (20:47Z) after deploy: job log `FORECAST SCORECARD: N cohort row(s) from 11,791 matured record(s) (min_n=5)`; `_Forecast_Scorecard` exists with header row 1 and an `As Of (Riyadh)` stamp of that run; the DQ audit lists it (no orphan).
2. Paste v1.17.0; `dt10SelfTest` → `model evidence core: ok | ON`; next refresh shows the three MODEL EVIDENCE lines under the KPI strip; the INVESTABLE/WATCHLIST figures match the tab's 1M rows.
3. L4 (next commit) reads the `Investability x Recommendation` cohort from this tab in observe mode.
