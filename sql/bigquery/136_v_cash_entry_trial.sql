-- v_cash_entry_trial: the record of the cash-entry shadow rule (v_cash_entry_events) - per period, way of entering
-- and horizon: did entering after a not-calm reading beat staying in cash?
-- WHY (owner, 26 Nov 2026 01:22 Riyadh): cash in a falling market; measured before the plan's market block is touched
-- (owner's ground rules, 21 Nov 2026).
-- THE MEASURE, per period, arm (all at once; three parts: sessions 0, 5, 10; after the first calm close) and horizon
-- (20, 60 sessions): events, mean_pct, up_pct (the share above cash), worst_pct (the worst event) and t_adj (the
-- mean over its standard error, divided by SQRT(h / 10): signals are at least 11 sessions apart, so longer windows
-- overlap).
-- THE READING (fixed on 26 Nov 2026): 'in-sample, not evidence' for every row but 'forward from 2026-11-26'; for the
-- forward rows 'collecting' under 8 events; then 'works: entering beat cash' at t_adj >= 2, 'fails: entering lost to
-- cash' at t_adj <= -2, else 'no clear edge'.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_cash_entry_trial`
OPTIONS (description = "TFB cash-entry record (views 1.30.0): after the US market's first not-calm reading, by way of entering SPY and horizon (20, 60 sessions) - events, the mean return against cash, the share above cash, the worst event, and a fixed reading")
AS
WITH d AS (
  SELECT e.period, e.signal_date, a.arm, a.h, a.r
  FROM `{{DS}}.v_cash_entry_events` e,
    UNNEST([
      STRUCT('all at once' AS arm, 20 AS h, e.all_at_once_20_pct AS r), ('all at once', 60, e.all_at_once_60_pct),
      ('three parts: sessions 0, 5, 10', 20, e.three_parts_20_pct), ('three parts: sessions 0, 5, 10', 60, e.three_parts_60_pct),
      ('after the first calm close', 20, e.after_calm_20_pct), ('after the first calm close', 60, e.after_calm_60_pct)
    ]) AS a
  WHERE a.r IS NOT NULL
),
agg AS (
  SELECT period, arm, h,
    COUNT(*) AS events, ROUND(AVG(r), 2) AS mean_pct, ROUND(100 * COUNTIF(r > 0) / COUNT(*), 1) AS up_pct,
    ROUND(MIN(r), 2) AS worst_pct,
    ROUND(SAFE_DIVIDE(AVG(r), SAFE_DIVIDE(STDDEV(r), SQRT(COUNT(*)))) / SQRT(h / 10), 2) AS t_adj
  FROM d
  GROUP BY period, arm, h
)
SELECT
  period, arm, h AS horizon_sessions, events, mean_pct, up_pct, worst_pct, t_adj,
  CASE
    WHEN period != 'forward from 2026-11-26' THEN 'in-sample, not evidence'
    WHEN events < 8 THEN 'collecting'
    WHEN t_adj >= 2 THEN 'works: entering beat cash'
    WHEN t_adj <= -2 THEN 'fails: entering lost to cash'
    ELSE 'no clear edge'
  END AS reading
FROM agg;
