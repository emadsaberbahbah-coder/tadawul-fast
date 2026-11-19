-- v_c2_calls: every buy call with what trial C-2 does with it - the call-level
-- membership of v_trial_c2, for a label beside each stock on the pages. WHY
-- (owner, 19 Nov 2026 18:46 Riyadh: "this new update for news is linked with
-- the recommendation pages like portfolio or investment pages"; 18:50, in
-- Arabic: "do the best and most suitable for the project"): the two open trials
-- (C-2 and the news overlay) are shown, observe-only, in the row of each
-- stock on Top_10_Investments and Portfolio_Decision (scripts/trial_labels.py
-- writes the Trial_Labels tab; 31_Final_Decision_Column.gs 1.4.0 shows it).
-- SAME RULE: the WITH block below (base, sig, f, r, k) is v_trial_c2's own,
-- character for character (tests/test_bq_views.py case_v38 compares the two
-- files), so a label can never say "kept" for a call the trial does not
-- count as kept, or the reverse. Membership is decided over every buy call
-- of the day, scored or not, as in the trial.
-- COLUMNS: record_id, symbol, market, market_group (US / Other markets),
-- horizon, recorded_date, recorded_at, period, high_confidence,
-- reliability, target_upside_pp (analyst target over the price at the call,
-- points), eligible (passes (a) and (b) and has an upside), eligible_calls
-- (the day's eligible calls in the same market group and horizon),
-- upside_rank (1 = highest upside among them; NULL when not eligible), keeps
-- (how many of them C-2 keeps: CEIL(eligible_calls / 2)), c2_keep, why_out
-- (the conditions a call fails, comma-joined: 'HIGH confidence',
-- 'reliability 80+', 'Tadawul', 'no analyst target'; NULL when
-- eligible), scored (it has a scored outcome).
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it. The labels are analysis, not orders.
CREATE OR REPLACE VIEW `{{DS}}.v_c2_calls`
OPTIONS (description = "TFB trial C-2 per buy recommendation: the record-level membership of v_trial_c2 (same rule, same day ranking) with the conditions a record fails - read by scripts/trial_labels.py for the observe-only labels on the pages")
AS
WITH base AS (
  SELECT
    record_id,
    UPPER(TRIM(symbol)) AS symbol,
    market,
    horizon,
    CASE horizon WHEN '1W' THEN 7 WHEN '2W' THEN 14 WHEN '1M' THEN 30 WHEN '3M' THEN 91 END AS horizon_days,
    recorded_date,
    recorded_at,
    confidence,
    entry_forecast_reliability AS rel,
    excess_pp AS edge,
    IFNULL(in_engine_cohort, FALSE) AND excess_pp IS NOT NULL AS scored
  FROM `{{DS}}.v_outcome_excess`
  WHERE recorded_at IS NOT NULL
    AND horizon IN ('1W', '2W', '1M', '3M')
    AND UPPER(IFNULL(entry_recommendation, '')) IN ('BUY', 'STRONG BUY', 'ACCUMULATE')
),
sig AS (
  SELECT b.record_id, s.target_upside_pp AS tup
  FROM base AS b
  JOIN `{{DS}}.v_signal_history` AS s
    ON UPPER(TRIM(s.symbol)) = b.symbol
   AND s.recorded_at <= TIMESTAMP_ADD(b.recorded_at, INTERVAL 60 SECOND)
   AND s.recorded_at >= TIMESTAMP_SUB(b.recorded_at, INTERVAL 3 DAY)
  WHERE s.target_upside_pp IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY b.record_id
    ORDER BY IF(s.recorded_at >= b.recorded_at, 0, 1),
      IF(s.recorded_at >= b.recorded_at, UNIX_MICROS(s.recorded_at), -UNIX_MICROS(s.recorded_at))) = 1
),
f AS (
  SELECT
    b.*,
    g.tup,
    IF(b.market = 'US', 'US', 'Other markets') AS mg,
    IF(b.recorded_date >= DATE '2026-11-20', 'forward from 2026-11-20', 'in-sample, not evidence') AS period,
    NOT (UPPER(IFNULL(b.confidence, '')) = 'HIGH' OR IFNULL(b.rel, 0) >= 80)
      AND IFNULL(b.market, '') != 'Tadawul'
      AND g.tup IS NOT NULL AS eligible
  FROM base AS b
  LEFT JOIN sig AS g USING (record_id)
),
r AS (
  SELECT
    f.*,
    ROW_NUMBER() OVER (PARTITION BY recorded_date, horizon, mg, eligible ORDER BY tup DESC, record_id) AS pos,
    COUNTIF(eligible) OVER (PARTITION BY recorded_date, horizon, mg) AS n_el
  FROM f
),
k AS (
  SELECT r.*, eligible AND pos <= CAST(CEIL(n_el / 2) AS INT64) AS c2_keep
  FROM r
)
SELECT
  record_id,
  symbol,
  market,
  mg AS market_group,
  horizon,
  recorded_date,
  recorded_at,
  period,
  UPPER(IFNULL(confidence, '')) = 'HIGH' AS high_confidence,
  rel AS reliability,
  ROUND(tup, 2) AS target_upside_pp,
  eligible,
  n_el AS eligible_calls,
  IF(eligible, pos, NULL) AS upside_rank,
  CAST(CEIL(n_el / 2) AS INT64) AS keeps,
  c2_keep,
  IF(eligible, NULL, ARRAY_TO_STRING([
    IF(UPPER(IFNULL(confidence, '')) = 'HIGH', 'HIGH confidence', NULL),
    IF(IFNULL(rel, 0) >= 80, 'reliability 80+', NULL),
    IF(IFNULL(market, '') = 'Tadawul', 'Tadawul', NULL),
    IF(tup IS NULL, 'no analyst target', NULL)], ', ')) AS why_out,
  scored
FROM k;
