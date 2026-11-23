-- v_news_trend_learning: the record of each news-and-trend state of v_news_trend_daily - did the stock then move
-- against the market the way the research expected, over the next 1, 5, 10 and 20 sessions, alone and split by
-- whether the expected move went with the trend or against it?
-- WHY (owner, 23 Nov 2026 12:34 Riyadh): news, data, strategy and trend read together, and only what proves itself
-- is switched on (owner's ground rules, 21 Nov: everything starts in observe mode, is logged and scored, and is
-- reviewed before anything controls a buy). This view is that score.
-- THE MEASURE, per period, state, trend fit ('all' and each of 'with the trend' / 'against the trend' / 'no clear
-- trend') and horizon h (sessions):
--   events, symbols, days       the state's sessions with the outcome known, their symbols and distinct dates
--   mean_pp                     the mean return against SPY after the state
--   base_pp                     the same mean over every covered session of the period (the base)
--   edge_pp                     the mean of expected x return: above 0 = the stock went the expected way
--   right_pct                   the share of events that went the expected way
--   t_adj                       the edge's t over distinct dates (each date's events averaged first, so a day
--                               with many names counts once), divided by SQRT(h) for h above 1: neighbouring dates
--                               share most of their window (Codex P2 on #1064, as v_projection_scorecard)
-- THE READING (fixed on 23 Nov 2026): 'no expectation' for the states with none ('quiet', 'routine news'); else
-- 'collecting' under 20 events or under GREATEST(10, 2 x h) dates; then 'works as expected' at t_adj >= 2,
-- 'opposite of expected' at t_adj <= -2, else 'no clear edge'. Only the forward rows (sessions from 24 Nov 2026)
-- count as evidence for switching anything on; the history rows were scored by rules written before they were read.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_news_trend_learning`
OPTIONS (description = "TFB news and trend record (views 1.25.0): for each news-and-trend state of v_news_trend_daily, by period, trend fit and horizon (1, 5, 10, 20 sessions) - events, symbols, dates, the mean return against SPY and its base, the edge in the expected direction, the share that went that way, an overlap-adjusted t over dates and a fixed reading - research only")
AS
WITH d AS (
  SELECT t.period, t.state, t.expected, t.symbol, t.session_date, fit, hz.h, hz.f
  FROM `{{DS}}.v_news_trend_daily` t,
    UNNEST([STRUCT(1 AS h, t.f1_pp AS f), (5, t.f5_pp), (10, t.f10_pp), (20, t.f20_pp)]) AS hz,
    UNNEST(['all', t.trend_fit]) AS fit
  WHERE hz.f IS NOT NULL
),
base AS (
  SELECT period, h, AVG(f) AS base_pp
  FROM d
  WHERE fit = 'all'
  GROUP BY period, h
),
by_day AS (
  SELECT period, state, fit, h, session_date, AVG(expected * f) AS e
  FROM d
  GROUP BY period, state, fit, h, session_date
),
day_stats AS (
  SELECT period, state, fit, h, COUNT(*) AS days, AVG(e) AS e_mean, STDDEV_SAMP(e) AS e_sd
  FROM by_day
  GROUP BY period, state, fit, h
),
agg AS (
  SELECT period, state, fit, h,
    COUNT(*) AS events, COUNT(DISTINCT symbol) AS symbols,
    AVG(f) AS mean_pp,
    AVG(IF(expected != 0, expected * f, NULL)) AS edge_pp,
    AVG(IF(expected != 0 AND f != 0, IF(SIGN(f) = expected, 1, 0), NULL)) AS right_share,
    MIN(session_date) AS first_day, MAX(session_date) AS last_day
  FROM d
  GROUP BY period, state, fit, h
)
SELECT
  a.period, a.state,
  CASE a.state
    WHEN 'good news, price agreed' THEN 'up'
    WHEN 'bad news, price agreed' THEN 'down'
    WHEN 'good news, price fell' THEN 'down'
    WHEN 'bad news, price rose' THEN 'up'
    WHEN 'news, price quiet' THEN 'the way of the news'
    WHEN 'unsigned news, big move' THEN 'the way of the move'
    WHEN 'big rise, no news' THEN 'down (reversal)'
    WHEN 'big fall, no news' THEN 'up (reversal)'
    ELSE 'none'
  END AS expected_move,
  a.fit AS trend_fit, a.h AS horizon_sessions,
  a.events, a.symbols, s.days,
  ROUND(a.mean_pp, 2) AS mean_pp, ROUND(b.base_pp, 2) AS base_pp,
  ROUND(a.edge_pp, 2) AS edge_pp, ROUND(100 * a.right_share, 1) AS right_pct,
  ROUND(SAFE_DIVIDE(s.e_mean, SAFE_DIVIDE(s.e_sd, SQRT(s.days))) / SQRT(a.h), 2) AS t_adj,
  a.first_day, a.last_day,
  CASE
    WHEN a.state IN ('quiet', 'routine news') THEN 'no expectation'
    WHEN a.events < 20 OR s.days < GREATEST(10, 2 * a.h) THEN 'collecting'
    WHEN SAFE_DIVIDE(s.e_mean, SAFE_DIVIDE(s.e_sd, SQRT(s.days))) / SQRT(a.h) >= 2 THEN 'works as expected'
    WHEN SAFE_DIVIDE(s.e_mean, SAFE_DIVIDE(s.e_sd, SQRT(s.days))) / SQRT(a.h) <= -2 THEN 'opposite of expected'
    ELSE 'no clear edge'
  END AS reading
FROM agg a
JOIN day_stats s USING (period, state, fit, h)
LEFT JOIN base b USING (period, h);
