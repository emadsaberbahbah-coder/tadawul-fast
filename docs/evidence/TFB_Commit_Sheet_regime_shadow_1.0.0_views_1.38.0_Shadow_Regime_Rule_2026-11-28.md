# TFB Commit Sheet — regime_shadow 1.0.0 + views 1.38.0 [REGIME-SHADOW] (one shadow regime rule per market, frozen daily)

Date: 2026-11-28 (Saturday) · Lane: GitHub Actions (🧭 Regime Shadow) + BigQuery views · Gate: observe only, nothing reads it · No page, tab, gate or Render change

## Why
Week plan of 23 Nov 2026, package 6: "the audit's bull / choppy / risk-off regime and our adverse-state count become one shadow rule". Its exit test: "regime labels are logged daily". The market-state count was folded into this package as the candidate gate.

The live gate reads one test per market: v_market_condition's five labels, which the engine's market gate and the Daily Plan use. Nothing says whether a market's trend, its breadth and its volatility agree.

## What
**`sql/bigquery/150_v_regime_shadow.sql` (new):** one row per equity market (us, sa, eu, jp, cn) and close. Every input uses that close and earlier ones only.
- **Trend:** the index against its 200-close average. Up means the close is over a rising average (above its level 20 closes back); down means under a falling one.
- **Breadth:** the share of TFB's followed names of that market over their own 50-close average, from the archive's dividend-adjusted closes up to that day. A name whose last 50 closes hold a jump of 1.5 times, or of two thirds or less, is left out that day. `v_bars_clean` is not used because it decides with later bars (Codex P2 on #1113). It needs at least 20 names.
- **Volatility percentile:** today's 20-close volatility against the last 252 closes (at least 200 known).
- **Adverse count (US only):** counted exactly as `v_projection_scorecard` counts it.
- **TFB's own condition label** that close.
- **The rule** (`rule_version` 1, fixed on 28 Nov before any forward close; first match wins):
  - `RISK_OFF`: trend down, or volatility percentile 80 or more, or breadth under 30, or 3 or more adverse US states;
  - `BULL`: trend up, volatility percentile under 60, breadth 50 or more (or unknown) and at most 1 adverse US state;
  - `CHOPPY`: everything else.
  - `regime_why` names the tests.

**`sql/bigquery/151_v_regime_shadow_insample.sql` (new):** per market and label, what followed, for the shadow labels and the condition labels alike.
- The index 5 and 21 closes later, the hit rate, and the excess over the market's mean.
- An overlap-adjusted t.
- Only closes up to 28 Nov, labelled "in-sample, not evidence".

**`scripts/regime_shadow.py` 1.0.0 (new):** after each run of the morning bars (🔬 BigQuery Symbol Deep Data) or the index closes (🌍 BigQuery Market Series), it freezes each final close's row once into BigQuery `regime_shadow_log`. A close is final when the archive holds a later close of that market's index.
- One row per market and close, appended into the close's partition.
- A close whose breadth or US adverse count is missing waits one close, then is frozen as it is and says so.
- The forward record starts with the closes of 29 Nov (Tadawul) and 30 Nov (the rest).
- Logs: market, date, label and its reasons, never a symbol.
- Switch: `TFB_REGIME_SHADOW` = `observe` (default) | `off`.

**Also changed:**
- `.github/workflows/regime_shadow.yml` 1.0.0 (new): chained, no cron. A manual run is a dry run unless `write=true`.
- `scripts/apply_bq_views.py` 1.38.0.
- `verify_deployment` pins: `apply_bq_views` 1.38.0; `regime_shadow` 1.0.0 on arrival.
- ci.yml 1.1.108: `tests/test_regime_shadow.py` joins the lane after `test_challenger_books.py`.

## Read on 28 Nov (in-sample, not evidence)
Labelled closes from October 2024 to 27 November 2026. Results are the index's own, 21 closes later.

| Market | BULL | CHOPPY | RISK_OFF |
|---|---|---|---|
| US | +0.47 % (225 days) | +0.29 % (136) | **+2.62 %** (175) |
| Saudi | −2.02 % (31) | −2.39 % (129) | +0.21 % (345) |
| Europe | +0.20 % (232) | +1.84 % (159) | +1.45 % (143) |
| Japan | +2.77 % (135) | +1.06 % (161) | +2.44 % (218) |
| HK | +1.63 % (202) | +1.02 % (103) | −0.32 % (216) |

- No label sorts the next month the expected way with a t of 1 or more. Only HK points in the expected direction (t +0.55 and −0.51).
- TFB's own condition labels read the same way. The US closes labelled stress, drawdown and crash_day were followed by +6.11 %, +4.10 % and +4.85 % over 21 closes (21, 37 and 13 days; t 1.11, 0.59, 0.36). Only weak was followed by a loss (−0.42 %).
- The pick-level record (`v_regime_outcomes`, June–November) agrees on weak. USD picks made in a weak market returned −1.73 % over 5 sessions, with 33 % ahead (170 picks); stress and drawdown have 10 and 12 picks.
- This is two years with one sharp fall and recovery (April 2025), on overlapping windows, with the thresholds chosen afterwards. It does not show that holding buys in stress is wrong. It shows that this history gives no support for it, and the forward record is what will decide.
- Every market reads `RISK_OFF` on its latest close.

## Tests
- `tests/test_regime_shadow.py`: 34 pass. They cover:
  - the views: no later close in the label view, the rule written once, the US adverse count identical to the scorecard's, the market of each suffix, the in-sample cut-off and label;
  - the SQL each run sends, the schema;
  - the plan: final, before the start, frozen already, no label, the wait for breadth or the adverse count, a key seen twice, oldest first;
  - the rows and types;
  - write, dry and failure runs; the switch; the workflow.
- `tests/test_bq_views.py`:
  - `EXPECTED_ORDER` and `EXPECTED_KINDS` gain the two views, whose hashes go to `PENDING`;
  - the position checks move by two, with no other change;
  - all pass.
- `test_verify_manifest_pins`, `test_challenger_books`, `test_trial_shadow_v2_rule` and `test_bq_archive` pass. Workflow audit: errors 0, warnings 0.
- Both view bodies were run read-only against BigQuery on 28 Nov.
- Twenty hand mutants of `scripts/regime_shadow.py`: each fails a test.

## Owner
Merge when ready; nothing to set.

After the merge, Claude:
1. dispatches 🗄️ BigQuery Looker Views with `write`, and checks that `verify` reads `match`;
2. dispatches 🧭 Regime Shadow once with `write=true`, which creates `regime_shadow_log` (the first final closes are 29 Nov on Tadawul, frozen once 30 Nov is archived).

The forward scorecard follows when the first 21-close results are in.

Rollback: `TFB_REGIME_SHADOW=off`.
