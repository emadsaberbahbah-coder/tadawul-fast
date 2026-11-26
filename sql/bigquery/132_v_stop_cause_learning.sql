-- v_stop_cause_learning: the record of the stop judgment (v_stop_cause_events) - per period, market, cause and
-- verdict: what did holding do after the fall, against selling at the fall close?
-- WHY (owner, 26 Nov 2026 01:07 and 01:37 Riyadh): keep the stops or adjust them; the judgment is measured before
-- it may control anything (owner's ground rules, 21 Nov 2026).
-- THE MEASURE, per period, market, cause, verdict and arm - 'hold 20 sessions' (every matured event) and 'the rule
-- under test' (the 'wait' events, held with the rule's own exits): events, symbols, days (distinct event dates),
-- mean_pct (the mean return after the event close; selling at the fall = 0), up_pct (the share above 0),
-- mean_excess_pp (the same 20 sessions against the market, in points) and t_adj (the mean over distinct dates - a
-- date's events averaged first - over its standard error, halved: 20-session windows of neighbouring dates share
-- most of their sessions).
-- THE READING (fixed on 26 Nov 2026): 'in-sample, not evidence' for every row but 'forward from 2026-11-26'; for the
-- forward rows 'collecting' under 30 events or 8 dates; then, for a 'wait' verdict, 'works: waiting beat selling at
-- the fall' at t_adj >= 2 and 'fails: waiting lost to selling at the fall' at t_adj <= -2; for the other verdicts,
-- where the rule sells, 'works: selling at the fall was right' at t_adj <= -2 and 'fails: it rose after the fall'
-- at t_adj >= 2; else 'no clear edge'.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_stop_cause_learning`
OPTIONS (description = "TFB stop-judgment record (views 1.30.0): for the falls of v_stop_cause_events, by period, market, cause and verdict - events, dates, the mean return of holding 20 sessions or of the rule under test against selling at the fall, and a fixed reading")
AS
WITH d AS (
  SELECT e.period, e.market, e.cause, e.verdict, e.symbol, e.event_date, a.arm, a.r, e.x20_pp
  FROM `{{DS}}.v_stop_cause_events` e,
    UNNEST([STRUCT('hold 20 sessions' AS arm, e.r20_pct AS r), ('the rule under test', IF(e.verdict = 'wait', e.rule_pct, NULL))]) AS a
  WHERE a.r IS NOT NULL AND e.r20_pct IS NOT NULL
),
per_day AS (
  SELECT period, market, cause, verdict, arm, event_date, AVG(r) AS m
  FROM d
  GROUP BY period, market, cause, verdict, arm, event_date
),
s AS (
  SELECT period, market, cause, verdict, arm, COUNT(*) AS days, AVG(m) AS e_mean, STDDEV(m) AS e_sd
  FROM per_day
  GROUP BY period, market, cause, verdict, arm
),
a AS (
  SELECT period, market, cause, verdict, arm, COUNT(*) AS events, COUNT(DISTINCT symbol) AS symbols,
    ROUND(AVG(r), 2) AS mean_pct, ROUND(100 * COUNTIF(r > 0) / COUNT(*), 1) AS up_pct,
    ROUND(AVG(x20_pp), 2) AS mean_excess_pp
  FROM d
  GROUP BY period, market, cause, verdict, arm
),
j AS (
  SELECT a.*, s.days,
    ROUND(SAFE_DIVIDE(s.e_mean, SAFE_DIVIDE(s.e_sd, SQRT(s.days))) / 2, 2) AS t_adj
  FROM a JOIN s USING (period, market, cause, verdict, arm)
)
SELECT
  period, market, cause, verdict, arm, events, symbols, days, mean_pct, up_pct, mean_excess_pp, t_adj,
  CASE
    WHEN period != 'forward from 2026-11-26' THEN 'in-sample, not evidence'
    WHEN events < 30 OR days < 8 THEN 'collecting'
    WHEN verdict = 'wait' AND t_adj >= 2 THEN 'works: waiting beat selling at the fall'
    WHEN verdict = 'wait' AND t_adj <= -2 THEN 'fails: waiting lost to selling at the fall'
    WHEN verdict != 'wait' AND t_adj <= -2 THEN 'works: selling at the fall was right'
    WHEN verdict != 'wait' AND t_adj >= 2 THEN 'fails: it rose after the fall'
    ELSE 'no clear edge'
  END AS reading
FROM j;
