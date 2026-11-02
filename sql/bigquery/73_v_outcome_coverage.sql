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
-- 1.5.0: the archive engine (v_outcome_engine) beside the tracker, per
-- horizon and ALL, each over the engine cohort (first engine-scored record
-- per key): engine_scored, engine_wins, engine_win_rate_pct, and what still
-- holds the backlog - engine_scorable_now (due, directional, bars present and
-- the target bar reached: scored), engine_awaiting_bar (due, bars present,
-- the archive has not reached the target yet), engine_no_bar (due records
-- whose symbol has no archived bars) and engine_no_bar_at_target (bars exist,
-- none within 7 days of the target). The tracker's columns are unchanged.
CREATE OR REPLACE VIEW `{{DS}}.v_outcome_coverage`
OPTIONS (description = "TFB Looker v4: Performance_Log scoring coverage per horizon - due, matured, the outcome cohort, scored, wins, win rate over the cohort, matured share, overdue records, the oldest overdue target, and the archive engine's scored / wins / backlog states beside them")
AS
WITH p AS (
  SELECT
    l.*,
    e.engine_state,
    e.engine_win_flag,
    e.in_engine_cohort
  FROM `{{DS}}.v_performance_log` AS l
  LEFT JOIN `{{DS}}.v_outcome_engine` AS e ON e.record_id = l.record_id
  WHERE l.snapshot_date = (SELECT MAX(snapshot_date) FROM `{{DS}}.v_performance_log`)
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
    MAX(IF(is_matured, DATE(target_at), NULL)) AS newest_matured_target,
    COUNTIF(in_engine_cohort) AS engine_scored,
    SUM(IF(in_engine_cohort, IFNULL(engine_win_flag, 0), 0)) AS engine_wins,
    COUNTIF(engine_state = 'scored_from_bar') AS engine_scorable_now,
    COUNTIF(engine_state = 'awaiting_bar') AS engine_awaiting_bar,
    COUNTIF(engine_state = 'no_bar_for_symbol') AS engine_no_bar,
    COUNTIF(engine_state = 'no_bar_at_target') AS engine_no_bar_at_target
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
    MAX(newest_matured_target) AS newest_matured_target,
    SUM(engine_scored) AS engine_scored,
    SUM(engine_wins) AS engine_wins,
    SUM(engine_scorable_now) AS engine_scorable_now,
    SUM(engine_awaiting_bar) AS engine_awaiting_bar,
    SUM(engine_no_bar) AS engine_no_bar,
    SUM(engine_no_bar_at_target) AS engine_no_bar_at_target
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
  ROUND(SAFE_DIVIDE(u.engine_wins, u.engine_scored) * 100, 1) AS engine_win_rate_pct,
  CASE u.horizon WHEN '1W' THEN 1 WHEN '2W' THEN 2 WHEN '1M' THEN 3 WHEN '3M' THEN 4 ELSE 9 END AS horizon_order
FROM u;
