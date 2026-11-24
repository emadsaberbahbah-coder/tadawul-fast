-- v_news_now_learning: the record of the hourly news reading (v_news_now_eval) - did the stock then move against SPY
-- the way its state expected, for the rest of the session, the next session and the next five?
-- WHY (owner, 24 Nov 2026 12:17 Riyadh: "ok let's start the next stage" - the interim daily reading of the news record).
-- THE MEASURE, per state (and 'all with a lean') and horizon h (0 = the rest of the session, 1, 5):
--   events, symbols, days, edge_pp (the mean of expected x return), right_pct (the share that went the expected way),
--   t_adj (the edge's t over distinct dates, each date's events averaged first, divided by SQRT(h) above 1) - the
--   measure of v_news_trend_learning (123), so the hourly and the nightly records read the same way.
-- THE READING (fixed on 24 Nov 2026, before any outcome was read): 'collecting' under 20 events or under
-- GREATEST(10, 2 x h) dates; then 'works as expected' at t_adj >= 2, 'opposite of expected' at t_adj <= -2, else
-- 'no clear edge'. Every event is forward (the job started on 24 Nov 2026).
-- Research only: no decision, gate or count reads this view. The live system never reads BigQuery.
CREATE OR REPLACE VIEW `{{DS}}.v_news_now_learning`
OPTIONS (description = "TFB hourly news reading record (views 1.28.0): per state and horizon (rest of the session, 1 and 5 sessions) - events, symbols, dates, the edge in the expected direction against SPY, the share that went that way, an overlap-adjusted t over dates and a fixed reading. Research only: no decision, gate or count reads this view.")
AS
WITH d AS (
  SELECT st AS state, e.expected, e.symbol, e.session_date, hz.h, hz.f
  FROM `{{DS}}.v_news_now_eval` e,
    UNNEST([STRUCT(0 AS h, e.rest_pp AS f), (1, e.f1_pp), (5, e.f5_pp)]) AS hz,
    UNNEST([e.state, 'all with a lean']) AS st
  WHERE hz.f IS NOT NULL
),
by_day AS (
  SELECT state, h, session_date, AVG(expected * f) AS e
  FROM d
  GROUP BY state, h, session_date
),
day_stats AS (
  SELECT state, h, COUNT(*) AS days, AVG(e) AS e_mean, STDDEV_SAMP(e) AS e_sd
  FROM by_day
  GROUP BY state, h
),
agg AS (
  SELECT state, h,
    COUNT(*) AS events, COUNT(DISTINCT symbol) AS symbols,
    AVG(expected * f) AS edge_pp,
    AVG(IF(f != 0, IF(SIGN(f) = expected, 1, 0), NULL)) AS right_share,
    MIN(session_date) AS first_day, MAX(session_date) AS last_day
  FROM d
  GROUP BY state, h
)
SELECT
  'forward from 2026-11-24' AS period, a.state, a.h AS horizon_sessions,
  a.events, a.symbols, s.days,
  ROUND(a.edge_pp, 2) AS edge_pp, ROUND(100 * a.right_share, 1) AS right_pct,
  ROUND(SAFE_DIVIDE(s.e_mean, SAFE_DIVIDE(s.e_sd, SQRT(s.days))) / SQRT(GREATEST(a.h, 1)), 2) AS t_adj,
  a.first_day, a.last_day,
  CASE
    WHEN a.events < 20 OR s.days < GREATEST(10, 2 * a.h) THEN 'collecting'
    WHEN SAFE_DIVIDE(s.e_mean, SAFE_DIVIDE(s.e_sd, SQRT(s.days))) / SQRT(GREATEST(a.h, 1)) >= 2 THEN 'works as expected'
    WHEN SAFE_DIVIDE(s.e_mean, SAFE_DIVIDE(s.e_sd, SQRT(s.days))) / SQRT(GREATEST(a.h, 1)) <= -2 THEN 'opposite of expected'
    ELSE 'no clear edge'
  END AS reading
FROM agg a
JOIN day_stats s USING (state, h);
