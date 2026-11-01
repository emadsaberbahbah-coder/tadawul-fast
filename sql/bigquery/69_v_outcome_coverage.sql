-- v_outcome_coverage: how much of the Performance_Log has actually been
-- scored, per horizon and for all horizons together, from the newest
-- archived snapshot. A win rate is only as good as the share of due records
-- behind it, so every rate here sits next to its denominator:
--   records          every record of the horizon
--   due              records whose target date has passed
--   matured          records the tracker closed (status matured)
--   matured_cohort   matured records in the sheet's outcome cohort: the first
--                    valid matured record per stable key (in_outcome_cohort),
--                    as track_performance's _summary_outcome_cohort counts them
--   matured_excluded matured records outside the cohort (a repeated key, or no
--                    valid realized ROI) - kept out of every rate below
--   scored           cohort records with a decided call (WIN or LOSS)
--   wins             decided cohort calls that won
--   win_pct          wins / scored, the sheet's Directional Win Rate
--   matured_share_pct  matured / due - how much of what is due has an outcome
--   overdue_open     due records still active (no outcome yet)
--   oldest_overdue_target  the earliest target date still waiting
--   newest_matured_target  the latest target date with an outcome
-- The 'ALL' row sums the horizons. Percent points; dates are UTC days.
-- 1.4.1: scored and wins are counted over the outcome cohort only, so a key
-- archived twice cannot count twice in the published win rate (1.4.0 counted
-- every matured copy); records, due and matured stay raw for coverage.
CREATE OR REPLACE VIEW `{{DS}}.v_outcome_coverage`
OPTIONS (description = "TFB Looker v4: Performance_Log scoring coverage per horizon - due, matured, the outcome cohort, scored, wins, win rate over the cohort, matured share, overdue records and the oldest overdue target")
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
    COUNTIF(in_outcome_cohort) AS matured_cohort,
    COUNTIF(is_matured AND NOT in_outcome_cohort) AS matured_excluded,
    COUNTIF(is_scored AND in_outcome_cohort) AS scored,
    SUM(IF(in_outcome_cohort, IFNULL(win_flag, 0), 0)) AS wins,
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
    SUM(matured_cohort) AS matured_cohort,
    SUM(matured_excluded) AS matured_excluded,
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
