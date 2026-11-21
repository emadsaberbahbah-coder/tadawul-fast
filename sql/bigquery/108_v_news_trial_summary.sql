-- v_news_trial_summary: the scorecard of every news signal - for each period,
-- horizon, signal and reading, what the markets did next against the same
-- signal's own average, and whether the signal ranks the markets right.
-- WHY (owner, 21 Nov 2026 11:03 Riyadh, items 5 and 6 of the build list):
-- "score every news signal" - "Observe for two weeks (10 sessions), then
-- review with the owner, and switch on only what proved itself."
-- READ FROM v_news_trial (once: one UNNEST over the three horizons, then
-- window functions; the worst 5 % is exact, PERCENTILE_CONT):
--   n, days, markets  the rows (market-sessions), sessions and markets behind
--                     the line
--   mean_pct          the funds' mean change over the horizon after the
--                     reading; mean_vs_spy_pp the same against SPY
--   up_pct            the share of rows that rose; p5_pct the worst 5 %
--   base_pct, base_n  the same signal's mean over all its readings (the base
--                     a reading is compared with)
--   edge_pp           mean_pct - base_pct; t_adj = edge_pp / (sd / sqrt(n /
--                     horizon)): overlapping windows of one market count once
--                     per horizon length
--   ic, ic_days       the mean, over sessions with 5 or more markets whose
--                     values differ, of the rank correlation across markets
--                     between the signal's value and the change that followed
--                     (tied values share their average rank: markets that
--                     read one place carry one value)
--                     (value-carrying signals: market_news, momentum, the
--                     people); ic_t_adj the same overlap rule
--   verdict           forward lines read 'collecting' until the reading has 10
--                     sessions; then the reading against its expected way
--                     (v_news_trial.expected): holds (t 2 or more) / the
--                     expected way, not clear / not the expected way; the
--                     middle readings are the base
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it. Observe only.
CREATE OR REPLACE VIEW `{{DS}}.v_news_trial_summary`
OPTIONS (description = "TFB news v1: one row per period (forward from 23 Nov 2026 / backtest), horizon (5, 10, 20 closes), news signal and reading - what the markets' funds did next against the same signal's own average (edge, overlap-adjusted t), the share that rose, the worst 5 %, and the signal's rank correlation with the markets' next changes. Observe only.")
AS
WITH t AS (
  SELECT *
  FROM `{{DS}}.v_news_trial`
),
h AS (
  SELECT
    t.period, z.horizon, t.signal, t.reading, t.expected, t.decision_day, t.market_id, t.value,
    z.f, z.vs
  FROM t, UNNEST([
    STRUCT(5 AS horizon, t.f5_pct AS f, t.vs_spy_f5_pp AS vs),
    STRUCT(10, t.f10_pct, t.vs_spy_f10_pp),
    STRUCT(20, t.f20_pct, t.vs_spy_f20_pp)
  ]) AS z
  WHERE z.f IS NOT NULL
),
q1 AS (
  SELECT
    h.*,
    PERCENTILE_CONT(h.f, 0.05) OVER (PARTITION BY h.period, h.horizon, h.signal, h.reading) AS p5,
    AVG(h.f) OVER (PARTITION BY h.period, h.horizon, h.signal) AS base_f,
    COUNT(*) OVER (PARTITION BY h.period, h.horizon, h.signal) AS base_n,
    IF(h.value IS NULL, NULL,
       (RANK() OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day ORDER BY h.value)
        + COUNT(h.value) OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day)
        - RANK() OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day ORDER BY h.value DESC) + 1) / 2)
      AS rank_value,
    (RANK() OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day ORDER BY h.f)
     + COUNT(*) OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day)
     - RANK() OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day ORDER BY h.f DESC) + 1) / 2 AS rank_f,
    ROW_NUMBER() OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day ORDER BY h.market_id) AS day_row,
    COUNT(h.value) OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day) AS day_n,
    MAX(h.value) OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day)
      > MIN(h.value) OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day) AS day_varies,
    MAX(h.f) OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day)
      > MIN(h.f) OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day) AS day_moves
  FROM h
),
q2 AS (
  SELECT
    q1.*,
    IF(q1.day_n >= 5 AND q1.day_varies AND q1.day_moves,
       CORR(q1.rank_value, q1.rank_f) OVER (PARTITION BY q1.period, q1.horizon, q1.signal, q1.decision_day),
       NULL) AS day_ic
  FROM q1
),
q3 AS (
  SELECT
    q2.*,
    AVG(IF(q2.day_row = 1, q2.day_ic, NULL)) OVER s AS ic_mean,
    COUNT(IF(q2.day_row = 1, q2.day_ic, NULL)) OVER s AS ic_days,
    STDDEV_SAMP(IF(q2.day_row = 1, q2.day_ic, NULL)) OVER s AS ic_sd
  FROM q2
  WINDOW s AS (PARTITION BY q2.period, q2.horizon, q2.signal)
),
g AS (
  SELECT
    period, horizon, signal, reading,
    MAX(expected) AS expected,
    COUNT(*) AS n,
    COUNT(DISTINCT decision_day) AS days,
    COUNT(DISTINCT market_id) AS markets,
    MIN(decision_day) AS first_day,
    MAX(decision_day) AS last_day,
    AVG(f) AS mean_f,
    STDDEV_SAMP(f) AS sd_f,
    AVG(vs) AS mean_vs,
    COUNTIF(f > 0) * 100 / COUNT(*) AS up_pct,
    MAX(p5) AS p5,
    MAX(base_f) AS base_f,
    MAX(base_n) AS base_n,
    MAX(ic_mean) AS ic_mean,
    MAX(ic_days) AS ic_days,
    MAX(ic_sd) AS ic_sd
  FROM q3
  GROUP BY period, horizon, signal, reading
),
r AS (
  SELECT
    g.*,
    g.mean_f - g.base_f AS edge,
    SAFE_DIVIDE(g.mean_f - g.base_f, g.sd_f / SQRT(GREATEST(g.n / g.horizon, 1))) AS t_adj,
    SAFE_DIVIDE(g.ic_mean, g.ic_sd / SQRT(GREATEST(g.ic_days / g.horizon, 1))) AS ic_t_adj
  FROM g
)
SELECT
  CONCAT(r.period, '_', CAST(r.horizon AS STRING), '_', r.signal, '_', r.reading) AS key,
  r.period, r.horizon, r.signal, r.reading, r.expected,
  r.n, r.days, r.markets, r.first_day, r.last_day,
  ROUND(r.mean_f, 3) AS mean_pct,
  ROUND(r.mean_vs, 3) AS mean_vs_spy_pp,
  ROUND(r.up_pct, 1) AS up_pct,
  ROUND(r.p5, 2) AS p5_pct,
  ROUND(r.base_f, 3) AS base_pct,
  r.base_n,
  ROUND(r.edge, 3) AS edge_pp,
  ROUND(r.t_adj, 2) AS t_adj,
  ROUND(r.ic_mean, 3) AS ic,
  r.ic_days,
  ROUND(r.ic_t_adj, 2) AS ic_t_adj,
  CASE
    WHEN r.period = 'forward' AND r.days < 10 THEN CONCAT('collecting: ', CAST(r.days AS STRING), ' of 10 sessions')
    WHEN r.expected = 0 THEN 'the middle reading (the base)'
    WHEN SIGN(r.edge) = r.expected AND ABS(r.t_adj) >= 2 THEN 'holds: the expected way, t 2 or more'
    WHEN SIGN(r.edge) = r.expected THEN 'the expected way, not clear (t under 2)'
    ELSE 'not the expected way'
  END AS verdict
FROM r;
