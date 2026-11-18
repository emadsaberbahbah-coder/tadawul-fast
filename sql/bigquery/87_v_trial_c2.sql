-- v_trial_c2: the second controlled challenger, C-2 "evidence-only buy calls",
-- measured on the project's own buy calls against the champion (all buy
-- calls, the same champion as v_trial_c1). WHY (owner, 19 Nov 2026 01:49
-- Riyadh, verbatim: "start building the plan that helps outperform and make
-- sure the recommendations beat the market by a much bigger margin"): the full
-- review of that night found the champion level with its markets (+0.08
-- points a month), the HIGH-confidence calls losing 5.3% in a month, Tadawul
-- buy calls losing to TASI at every horizon, and the analyst target upside the
-- only input that ranked the next month in the right direction.
-- RULE, fixed on 19 Nov 2026 BEFORE any forward day. C-2 keeps a buy call
-- (BUY / STRONG BUY / ACCUMULATE) only when all three hold:
--   (a) its confidence is not HIGH and its forecast reliability is under 80
--       (C-1's first condition);
--   (b) it is not a Tadawul call;
--   (c) the analyst target upside known at the call (the Signal_History
--       record of the same symbol at the call, else the latest one up to 3
--       days before - the same join as v_signal_edge_daily) is in the top half
--       of that day's calls passing (a) and (b) in the same market group (US /
--       Other markets) and horizon: ranked by upside, highest first (ties by
--       record id), the first CEIL(n / 2) are kept, so a lone call is kept. A
--       call with no known upside is not kept.
-- In-sample (the days the rule was chosen on, NOT evidence), one month: C-2's
-- 215 calls +3.8 points against their market on average (76% ahead), the
-- champion's 980 calls +0.7; on the daily average C-2 +2.1, the champion +0.1,
-- with an overlap-adjusted t of only 0.35 (43 days of overlapping months).
-- Two weeks: C-2 +2.25 points (70% ahead), the champion +0.8. Eight variants
-- were looked at before this one was fixed, so the in-sample figures
-- overstate what to expect.
-- EVIDENCE: only calls recorded from 20 Nov 2026 (period 'forward from
-- 2026-11-20').
-- PROMOTION RULE (fixed now), per horizon: forward, at least the larger of 20
-- call days and two horizon lengths (1W 20, 2W 28, 1M 60, 3M 182) and 40 C-2
-- calls, and C-2's mean daily edge above the champion's with an overlap-
-- adjusted t of 2 or more (t of the daily differences x SQRT(days / horizon
-- days), as v_trial_shadow_v2). Even then the owner decides; nothing here
-- changes a page, a pick or an order.
-- NO LOOK-AHEAD: membership uses only what the call carried and the analyst
-- target recorded up to 60 seconds after it, and it is decided over EVERY buy
-- call recorded that day, scored or not; only scored calls then enter the
-- statistics (Codex P2 on #1026: ranking only the scored calls would let a
-- missing bar or a breakeven outcome move another call into the top half).
-- COLUMNS: period, horizon (1W / 2W / 1M / 3M), arm (CHAMPION / C2 /
-- LEFT_OUT / C2_MINUS_CHAMPION), calls, days, edge_pp (mean return minus the
-- market benchmark; for C2_MINUS_CHAMPION the mean daily difference),
-- right_pct, t_adj (C2_MINUS_CHAMPION only), first_day, last_day, reading.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_trial_c2`
OPTIONS (description = "TFB trial C-2 (fixed 19 Nov 2026): buy calls without HIGH confidence or reliability 80+, outside Tadawul, in the top half of the day by analyst target upside, against all buy calls - forward evidence from 20 Nov 2026 with an overlap-adjusted t")
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
),
daily AS (
  SELECT
    period, horizon, recorded_date,
    MIN(horizon_days) AS horizon_days,
    COUNT(*) AS n_all, SUM(edge) AS s_all, COUNTIF(edge > 0) AS w_all,
    COUNTIF(c2_keep) AS n_c2, SUM(IF(c2_keep, edge, 0)) AS s_c2, COUNTIF(c2_keep AND edge > 0) AS w_c2,
    COUNTIF(NOT c2_keep) AS n_out, SUM(IF(c2_keep, 0, edge)) AS s_out, COUNTIF(NOT c2_keep AND edge > 0) AS w_out
  FROM k
  WHERE scored
  GROUP BY period, horizon, recorded_date
),
agg AS (
  SELECT
    period, horizon,
    MIN(horizon_days) AS horizon_days,
    SUM(n_all) AS n_all, SUM(s_all) AS s_all, SUM(w_all) AS w_all, COUNTIF(n_all > 0) AS d_all,
    SUM(n_c2) AS n_c2, SUM(s_c2) AS s_c2, SUM(w_c2) AS w_c2, COUNTIF(n_c2 > 0) AS d_c2,
    SUM(n_out) AS n_out, SUM(s_out) AS s_out, SUM(w_out) AS w_out, COUNTIF(n_out > 0) AS d_out,
    AVG(IF(n_c2 > 0, s_c2 / n_c2 - s_all / n_all, NULL)) AS dm,
    STDDEV_SAMP(IF(n_c2 > 0, s_c2 / n_c2 - s_all / n_all, NULL)) AS dsd,
    MIN(recorded_date) AS first_day,
    MAX(recorded_date) AS last_day
  FROM daily
  GROUP BY period, horizon
),
rows_ AS (
  SELECT
    period, horizon, arm, first_day, last_day,
    GREATEST(20, 2 * horizon_days) AS need,
    CASE arm WHEN 'CHAMPION' THEN n_all WHEN 'C2' THEN n_c2 WHEN 'LEFT_OUT' THEN n_out ELSE n_c2 END AS calls,
    CASE arm WHEN 'CHAMPION' THEN d_all WHEN 'C2' THEN d_c2 WHEN 'LEFT_OUT' THEN d_out ELSE d_c2 END AS days,
    CASE arm WHEN 'CHAMPION' THEN SAFE_DIVIDE(s_all, n_all) WHEN 'C2' THEN SAFE_DIVIDE(s_c2, n_c2)
             WHEN 'LEFT_OUT' THEN SAFE_DIVIDE(s_out, n_out) ELSE dm END AS edge_x,
    ROUND(100 * CASE arm WHEN 'CHAMPION' THEN SAFE_DIVIDE(w_all, n_all) WHEN 'C2' THEN SAFE_DIVIDE(w_c2, n_c2)
                         WHEN 'LEFT_OUT' THEN SAFE_DIVIDE(w_out, n_out) END, 1) AS right_pct,
    IF(arm = 'C2_MINUS_CHAMPION', SAFE_DIVIDE(dm, dsd) * SQRT(d_c2 / horizon_days), NULL) AS t_x
  FROM agg CROSS JOIN UNNEST(['CHAMPION', 'C2', 'LEFT_OUT', 'C2_MINUS_CHAMPION']) AS arm
)
SELECT
  period, horizon, arm, calls, days,
  ROUND(edge_x, 2) AS edge_pp,
  right_pct,
  ROUND(t_x, 2) AS t_adj,
  first_day, last_day,
  CASE
    WHEN arm != 'C2_MINUS_CHAMPION' THEN NULL
    WHEN period != 'forward from 2026-11-20' THEN 'in-sample: shows why the rule was chosen, not evidence'
    WHEN days < need OR calls < 40 THEN CONCAT('collecting: ', CAST(days AS STRING), ' of ', CAST(need AS STRING),
                                               ' days, ', CAST(calls AS STRING), ' of 40 calls')
    WHEN t_x >= 2 AND edge_x > 0 THEN 'C-2 ahead: the owner may consider it'
    WHEN edge_x <= 0 THEN 'C-2 not ahead: keep the champion'
    ELSE 'C-2 ahead but not yet clear (t under 2): keep collecting'
  END AS reading
FROM rows_;
