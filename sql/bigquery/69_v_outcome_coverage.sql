-- v_outcome_coverage: how much of the Performance_Log has actually been
-- scored, per horizon and for all horizons together, from the newest
-- archived snapshot. A win rate is only as good as the share of due records
-- behind it, so every rate here sits next to its denominator:
--   records          every record of the horizon
--   due              records whose target date has passed
--   matured          records the tracker closed (status matured)
--   scored           matured records with a decided call (WIN or LOSS)
--   wins             decided calls that won
--   win_pct          wins / scored, the sheet's Directional Win Rate
--   matured_share_pct  matured / due - how much of what is due has an outcome
--   overdue_open     due records still active (no outcome yet)
--   oldest_overdue_target  the earliest target date still waiting
--   newest_matured_target  the latest target date with an outcome
-- The 'ALL' row sums the horizons. Percent points; dates are UTC days.
CREATE OR REPLACE VIEW `{{DS}}.v_outcome_coverage`
OPTIONS (description = "TFB Looker v4: Performance_Log scoring coverage per horizon - due, matured, scored, wins, win rate, matured share, overdue records and the oldest overdue target")
AS
WITH p AS (
  SELECT *
  FROM `{{DS}}.v_performance_log`
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `{{DS}}.v_performance_log`)
),
by_h AS (
  SELECT
    snapshot_date,
    horizon,
    COUNT(*) AS records,
    COUNTIF(target_at <= CURRENT_TIMESTAMP()) AS due,
    COUNTIF(is_matured) AS matured,
    COUNTIF(is_scored) AS scored,
    SUM(IFNULL(win_flag, 0)) AS wins,
    COUNTIF(target_at <= CURRENT_TIMESTAMP() AND status = 'active') AS overdue_open,
    MIN(IF(target_at <= CURRENT_TIMESTAMP() AND status = 'active', DATE(target_at), NULL)) AS oldest_overdue_target,
    MAX(IF(is_matured, DATE(target_at), NULL)) AS newest_matured_target
  FROM p
  WHERE horizon IS NOT NULL
  GROUP BY snapshot_date, horizon
),
all_h AS (
  SELECT
    snapshot_date,
    'ALL' AS horizon,
    SUM(records) AS records,
    SUM(due) AS due,
    SUM(matured) AS matured,
    SUM(scored) AS scored,
    SUM(wins) AS wins,
    SUM(overdue_open) AS overdue_open,
    MIN(oldest_overdue_target) AS oldest_overdue_target,
    MAX(newest_matured_target) AS newest_matured_target
  FROM by_h
  GROUP BY snapshot_date
),
u AS (
  SELECT * FROM by_h
  UNION ALL
  SELECT * FROM all_h
)
SELECT
  u.*,
  ROUND(SAFE_DIVIDE(u.wins, u.scored) * 100, 1) AS win_pct,
  ROUND(SAFE_DIVIDE(u.matured, u.due) * 100, 1) AS matured_share_pct,
  CASE u.horizon WHEN '1W' THEN 1 WHEN '2W' THEN 2 WHEN '1M' THEN 3 WHEN '3M' THEN 4 ELSE 9 END AS horizon_order
FROM u;
