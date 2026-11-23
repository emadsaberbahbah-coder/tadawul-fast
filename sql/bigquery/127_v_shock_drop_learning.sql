-- v_shock_drop_learning: the record of the shock-drop shadow rule (v_shock_drop_events) - per period, way of
-- entering (A at the drop, B after a confirmed turn) and horizon: did the large company that fell 30 % beat SPY
-- afterwards?
-- WHY (owner, 24 Nov 2026 00:58 and 01:18 Riyadh): the NVDA fall and rebound; the idea is measured before it
-- controls anything (owner's ground rules, 21 Nov 2026: observe, log, score, review, then switch on only what proved
-- itself).
-- THE MEASURE, per period, arm and horizon h (5, 10, 20 sessions): events, symbols, days (distinct signal dates),
-- mean_pp (the mean return against SPY), right_pct (the share above SPY), t_adj (the mean over distinct dates - a
-- date's events averaged first - over its standard error, divided by SQRT(h / 5) above 5 sessions, as neighbouring
-- signals of one fall share most of their window).
-- THE READING (fixed on 24 Nov 2026): 'in-sample, not evidence' for every row but 'forward from 2026-11-24' (the
-- history - today's largest companies read backwards, survivorship - and the forward signals of a company not in
-- the 100 before its signal, Codex P2 on #1068); for the forward rows 'collecting' under 10 events or 8 dates; then
-- 'works: beats SPY' at t_adj >= 2, 'fails: trails SPY' at t_adj <= -2, else 'no clear edge'.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_shock_drop_learning`
OPTIONS (description = "TFB shock-drop record (views 1.26.0): for the shadow rule of v_shock_drop_events, by period, way of entering and horizon (5, 10, 20 sessions) - events, symbols, dates, the mean return against SPY, the share above it, an overlap-adjusted t over dates and a fixed reading - research only")
AS
WITH d AS (
  SELECT e.period, e.arm, e.symbol, e.signal_date, hz.h, hz.f
  FROM `{{DS}}.v_shock_drop_events` e,
    UNNEST([STRUCT(5 AS h, e.f5_pp AS f), (10, e.f10_pp), (20, e.f20_pp)]) AS hz
  WHERE hz.f IS NOT NULL
),
per_day AS (
  SELECT period, arm, h, signal_date, AVG(f) AS m
  FROM d
  GROUP BY period, arm, h, signal_date
),
s AS (
  SELECT period, arm, h, COUNT(*) AS days, AVG(m) AS e_mean, STDDEV(m) AS e_sd
  FROM per_day
  GROUP BY period, arm, h
),
a AS (
  SELECT period, arm, h, COUNT(*) AS events, COUNT(DISTINCT symbol) AS symbols,
    ROUND(AVG(f), 2) AS mean_pp, ROUND(100 * COUNTIF(f > 0) / COUNT(*), 1) AS right_pct
  FROM d
  GROUP BY period, arm, h
),
j AS (
  SELECT a.*, s.days,
    ROUND(SAFE_DIVIDE(s.e_mean, SAFE_DIVIDE(s.e_sd, SQRT(s.days))) / SQRT(GREATEST(a.h / 5, 1)), 2) AS t_adj
  FROM a JOIN s USING (period, arm, h)
)
SELECT
  period, arm, h AS horizon_sessions, events, symbols, days, mean_pp, right_pct, t_adj,
  CASE
    WHEN period != 'forward from 2026-11-24' THEN 'in-sample, not evidence'
    WHEN events < 10 OR days < 8 THEN 'collecting'
    WHEN t_adj >= 2 THEN 'works: beats SPY'
    WHEN t_adj <= -2 THEN 'fails: trails SPY'
    ELSE 'no clear edge'
  END AS reading
FROM j;
