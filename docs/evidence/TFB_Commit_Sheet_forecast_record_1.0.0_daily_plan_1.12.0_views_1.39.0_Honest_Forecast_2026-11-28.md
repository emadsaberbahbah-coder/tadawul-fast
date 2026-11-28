# TFB Commit Sheet — forecast_record 1.0.0 + daily_plan 1.12.0 + views 1.39.0 [HONEST] (each name shows what calls like it delivered)

Date: 2026-11-28 (Saturday) · Lane: BigQuery view + GitHub Actions (📏 Forecast Record) + the Daily Plan · Gate: observe only (labels) · No pick, order, size, level, gate or Render change

## Why
Owner, 28 Nov 2026 01:02 Riyadh, in Arabic: a return of at least 5 % a month on the capital, in all cases, or stop before the losses grow. At 01:30: "Try to make full review and engening restructured looking for better results".

The pages show each name's promise, which is its forecast price over its price. They also show a HIGH / MEDIUM / LOW confidence, which the record does not bear out (`v_confidence_calibration`: Saudi HIGH rose 23.8 % of the time).

What such promises delivered stood in no page. This is Build 2 of the review of that night (review doc, build table).

## What
**`sql/bigquery/152_v_forecast_record.sql` (new, views 1.39.0):** the record of TFB's scored INVESTABLE buy calls from `v_outcome_engine`.
- The population is the first call per Key, stance bullish, horizons 1W, 2W, 1M and 3M. It is the same population as `v_projection_scorecard` and `v_learning_scorecard`.
- Rows are per market, horizon and band, for two periods.
  - **Bands:** `all`, plus the thirds `low` / `mid` / `high` of the market's promises. The thirds are cut once, on the calls scored by 27 Nov 2026, so they never move.
  - **Periods:** `all` calls, and `forward` (calls recorded from 29 Nov 2026, which did not shape the thirds).
- The measures per row:
  - the calls and their mean promise and result;
  - the share of the promise delivered and how often the calls rose;
  - how often they beat their market's own index over the same dates. The index is SPY, ^TASI.SR, ^HSI, ^STOXX, ^FTSE, ^N225 or 000300.SS, else ACWI. Each window runs from the last index close before the recording day to the last close on or before the outcome bar.
- **The reading, fixed on 28 Nov.** The first rule that applies sets the reading:
  - `too few calls`: under 30 calls or 10 days;
  - `lost money`: the mean result is 0 or below;
  - `beat its index`: the excess is above 0 and half or more beat the index;
  - `made money, behind its index`: everything else.

**`scripts/forecast_record.py` 1.0.0 (new):** one SELECT of the view, written as the `Forecast_Record` tab. The live system never reads BigQuery.
- The tab carries four lines on how to read it, then the table, with the markets that have the most calls first.
- The tab and the run log name markets only.
- A failed query, an empty record or a failed write leaves the previous tab in place (exit 2).
- Switch: `TFB_FORECAST_RECORD` = `on` (default) | `off`.

**`.github/workflows/forecast_record.yml` 1.0.0 (new):** runs after every successful run on main of 🔬 BigQuery Symbol Deep Data and 🌍 BigQuery Market Series, and on dispatch. It has no cron.

**`scripts/daily_plan.py` 1.12.0 [HONEST]:** `TFB_PLAN_HONEST` = `observe` (default, armed in the workflow) | `off`.
- In observe, the cut reads `Forecast_Record` once.
- Every name it judges gets one label per horizon (1M, 3M), whether it is a pick or a waiting name. Example: `[observe] honest 1M +3.3% vs page +5.6% (US buy calls like it: 72% rose, 71% beat SPY, 153 calls)`.
- The promise is the source page's Forecast Price 1M or 3M over its Current Price.
- "Like it" is the band whose fixed range holds that promise, or all the market's calls when the band has too few.
- Otherwise the label says `not measured` and why: too few calls, no forecast price, or a listing with no scored call.
- The labels change no pick, order, size or level. The Today Action Plan shows them in a pick's "why", as it shows every Daily Plan label.
- A tab that cannot be read leaves the labels out, and the cut goes on.
- Run log: counts only, for example `honest observe: 8 name(s) labelled; 1M measured for 6 (3 under 0), ...`.

**Also changed:**
- `scripts/apply_bq_views.py` 1.39.0.
- `daily_plan.yml` 1.10.0 (`TFB_PLAN_HONEST` observe).
- `verify_deployment` pins: `apply_bq_views` 1.39.0, `daily_plan` 1.12.0, `forecast_record` 1.0.0 on arrival.
- ci.yml 1.1.109: `tests/test_forecast_record.py` after `test_regime_shadow.py`.

## Read on 28 Nov (the record as it stands, the view run read-only)
INVESTABLE buy calls scored to 27 Nov 2026, period `all`:

| Market · horizon | Band (promise) | Calls | Promised | Delivered | Rose | Beat its index | Reading |
|---|---|---|---|---|---|---|---|
| US · 1M | all | 459 | +3.66 % | +1.13 % | 54.5 % | 59.7 % (SPY) | beat its index |
| US · 1M | low (≤ 2.67 %) | 153 | +2.03 % | −1.28 % | 37.9 % | 43.1 % | lost money |
| US · 1M | mid (≤ 4.21 %) | 153 | +3.35 % | +1.42 % | 53.6 % | 65.4 % | beat its index |
| US · 1M | high (> 4.21 %) | 153 | +5.61 % | **+3.26 %** | **71.9 %** | **70.6 %** | beat its index |
| US · 3M | all | 160 | +8.99 % | +2.88 % | 53.1 % | 41.9 % | made money, behind its index |
| Saudi · 1M | all | 204 | +4.64 % | −4.37 % | 32.8 % | 45.6 % (^TASI.SR) | lost money |
| Saudi · 1M | high (> 5.49 %) | 68 | +6.25 % | **−8.53 %** | 20.6 % | 26.5 % | lost money |
| Saudi · 3M | high (> 12.09 %) | 32 | +14.38 % | −15.39 % | 18.8 % | 21.9 % | lost money |
| Hong Kong · 1M | all | 66 | +2.97 % | +3.28 % | 87.9 % | 83.3 % (^HSI) | beat its index |

- In the US, one-month delivery rose with the promise: −1.28 %, +1.42 % and +3.26 % from the lowest third to the highest. The promise is informative there, but it overstates by about 2.5 points.
- On Tadawul, every band of every horizon lost money, and the highest promises lost the most.
- This is a record, not a test. The thirds were cut on the same calls. Neighbouring calls overlap (a 1M call holds 21 sessions), and the US 1M window spans one falling month for SPY (−1.32 % on the same dates). The `forward` rows, from 29 Nov, are where a band's order can be confirmed.

## Tests
- `tests/test_forecast_record.py`: 20 pass. They cover:
  - the view: last in order, two sources, the population, the thirds cut once, the same band rule as the plan, the index window strictly before the call, the reading, every copied column;
  - the writer: one SELECT of named columns with no symbol, the header within the eight rows the plan scans, the order, off, a bad dataset, a failed query, an empty record, dry run and write, whole numbers;
  - the plan: the tab read back (raw or formatted), the honest read and its label, the fallback to all, the reasons for "not measured", every judged name labelled and no pick changed (also under the regime gate), the run log, one read per cut and none for the frozen plan, off, a missing tab, a tab that cannot be read;
  - the workflows, the pins and the CI lane.
- `tests/test_daily_plan.py`: 73 pass. Version pins now follow the version; no behaviour test changed.
- `tests/test_bq_views.py`: `EXPECTED_ORDER` and `EXPECTED_KINDS` gain the view, whose hash goes to `PENDING`. The position checks move by one, with no other change. All 105 view and regime tests pass.
- Whole suite: 8,297 pass. One more, `test_regime_shadow`'s views pin check, now follows the views version and passes. Two fail the same way on the base commit in this workspace (Python 3.13; CI runs 3.11) and are outside this change: `test_security_pins_libsmoke_20261013` l17 and `test_tracker_log_read_precision` l2.
- Workflow audit: errors 0, warnings 0.
- The view body was run read-only against BigQuery on 28 Nov (0.56 GB).
- Twenty-two hand mutants of `daily_plan.py`'s honest code and `forecast_record.py` were run. Each fails a test (two needed a new assertion).

## Owner
Merge after #1113 (both change the views registry); nothing to set.

After the merge, Claude:
1. dispatches 🗄️ BigQuery Looker Views with `write`, and checks that `verify` reads `match`;
2. dispatches 📏 Forecast Record once, which writes the `Forecast_Record` tab;
3. reads the next Daily Plan cut's `honest observe:` line and the labels on its rows.

Rollback: `TFB_PLAN_HONEST=off`, `TFB_FORECAST_RECORD=off`.
