-- v_trial_shadow_v2: the challenger score shadow_v2 against the current
-- overall_score, on forward days only. WHY (owner, 18 Nov 2026, after two
-- weeks of market shocks: do the risk and confidence parameters or the
-- Scoring need adjusting; forecast "on a scientific basis with continuous
-- learning"; then "let us start building"): on 18 Nov v_signal_edge read
-- overall_score, confidence and reliability as carrying no edge in any market
-- at any horizon, and the analyst target upside and the engine's 1M forecast
-- ROI as the two signals that ranked the next month in the right direction
-- (US, 1M, the 34 days both exist: shadow_v2 mean IC 0.32 with every day
-- positive, overall_score -0.13). Those 34 days are one six-week stretch of
-- overlapping months and they are the days the weights were chosen on, so
-- they prove nothing. This view is the forward test.
-- RULE, fixed on 18 Nov 2026 before any forward day:
--   shadow_v2 = 0.6 x day rank of target upside + 0.4 x day rank of the
--   engine's 1M forecast ROI (v_signal_edge_daily computes it; no confidence,
--   reliability or overall_score term).
-- SAME NAMES ON BOTH SIDES: the view reads the matched pair
-- shadow_v2_matched / overall_score_matched, each score kept only on the
-- names that carry both, so the two rank ICs and the two top thirds are
-- taken over one set of names against one day mean. A day counts only when
-- both sides were ranked over the same number of names (Codex P2 on #1023:
-- the plain overall_score also covers names without an analyst target, which
-- the shadow cannot score).
-- Per signal day, market group and horizon where BOTH scores have a rank IC
-- (at least 20 such names that day), the daily difference ic_shadow - ic_current.
-- period: 'forward from 2026-11-19' (evidence) or 'in-sample, not evidence'.
-- COLUMNS: period, market_group, horizon, days, first_day, last_day,
--   avg_n                     mean names per day carrying both scores
--   ic_shadow / ic_current    mean daily rank IC of each score
--   ic_diff                   mean daily difference (shadow - current)
--   t_adj                     ic_diff / its standard deviation x
--                             SQRT(days / horizon_days): the overlapping
--                             outcome windows counted once per horizon length
--   top_shadow_pp / top_current_pp  mean excess return of each score's top
--                             third over the group that day
--   reading, in this order:
--     in-sample: ...          the days before 19 Nov 2026
--     collecting: N of M days fewer than M = the larger of 20 days and two
--                             horizon lengths (1W 20, 2W 28, 1M 60, 3M 182)
--     shadow ahead: the owner may consider it
--                             t_adj >= 2, ic_shadow > 0 and the shadow's top
--                             third beat the current score's top third
--     shadow not ahead: keep the current score      ic_diff <= 0
--     shadow ahead but not yet clear (t under 2): keep collecting
-- A 1W reading needs about four weeks of forward days, 2W about six, 1M about
-- three months (60 signal days and 30 more to mature). Promotion is the
-- owner's decision even on 'shadow ahead'; nothing here changes a score, a
-- page or a pick.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_trial_shadow_v2`
OPTIONS (description = "TFB trial shadow_v2 (fixed 18 Nov 2026): the challenger score 0.6 x day rank of target upside + 0.4 x day rank of the 1M engine ROI against overall_score on the same names - mean daily rank IC of each, the daily difference with its overlap-adjusted t, top-third excess returns and a reading - only signal days from 19 Nov 2026 are evidence")
AS
WITH d AS (
  SELECT
    signal_day,
    market_group,
    horizon,
    MIN(horizon_days) AS horizon_days,
    MAX(IF(feature = 'shadow_v2_matched', n, NULL)) AS n_s,
    MAX(IF(feature = 'overall_score_matched', n, NULL)) AS n_o,
    MAX(IF(feature = 'shadow_v2_matched', ic, NULL)) AS ic_s,
    MAX(IF(feature = 'overall_score_matched', ic, NULL)) AS ic_o,
    MAX(IF(feature = 'shadow_v2_matched', top_ex_pp, NULL)) AS top_s,
    MAX(IF(feature = 'overall_score_matched', top_ex_pp, NULL)) AS top_o
  FROM `{{DS}}.v_signal_edge_daily`
  WHERE feature IN ('shadow_v2_matched', 'overall_score_matched')
  GROUP BY signal_day, market_group, horizon
),
p AS (
  SELECT
    *,
    IF(signal_day >= DATE '2026-11-19', 'forward from 2026-11-19', 'in-sample, not evidence') AS period
  FROM d
  WHERE ic_s IS NOT NULL AND ic_o IS NOT NULL AND n_s = n_o
),
s AS (
  SELECT
    period,
    market_group,
    horizon,
    MIN(horizon_days) AS horizon_days,
    COUNT(*) AS days,
    MIN(signal_day) AS first_day,
    MAX(signal_day) AS last_day,
    AVG(n_s) AS avg_n,
    AVG(ic_s) AS m_s,
    AVG(ic_o) AS m_o,
    AVG(ic_s - ic_o) AS dm,
    STDDEV_SAMP(ic_s - ic_o) AS dsd,
    AVG(top_s) AS top_s,
    AVG(top_o) AS top_o
  FROM p
  GROUP BY period, market_group, horizon
),
t AS (
  SELECT
    s.*,
    SAFE_DIVIDE(dm, dsd) * SQRT(days / horizon_days) AS t_x,
    GREATEST(20, 2 * horizon_days) AS need
  FROM s
)
SELECT
  period,
  market_group,
  horizon,
  days,
  first_day,
  last_day,
  ROUND(avg_n, 1) AS avg_n,
  ROUND(m_s, 4) AS ic_shadow,
  ROUND(m_o, 4) AS ic_current,
  ROUND(dm, 4) AS ic_diff,
  ROUND(t_x, 2) AS t_adj,
  ROUND(top_s, 2) AS top_shadow_pp,
  ROUND(top_o, 2) AS top_current_pp,
  CASE
    WHEN period != 'forward from 2026-11-19' THEN 'in-sample: the weights were chosen on these days, not evidence'
    WHEN days < need THEN CONCAT('collecting: ', CAST(days AS STRING), ' of ', CAST(need AS STRING), ' days')
    WHEN t_x >= 2 AND m_s > 0 AND top_s > top_o THEN 'shadow ahead: the owner may consider it'
    WHEN dm <= 0 THEN 'shadow not ahead: keep the current score'
    ELSE 'shadow ahead but not yet clear (t under 2): keep collecting'
  END AS reading
FROM t;
