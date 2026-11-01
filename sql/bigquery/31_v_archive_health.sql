-- v_archive_health: one row per archived tab and snapshot day - rows kept,
-- capture time and the GitHub run that loaded it. A missing day or a sudden
-- drop in rows is the first thing the health page shows.
CREATE OR REPLACE VIEW `{{DS}}.v_archive_health`
OPTIONS (description = "TFB Looker v1: rows and capture time per archived tab and snapshot day")
AS
SELECT snapshot_date, 'Top_10_Investments' AS tab, COUNT(*) AS rows_archived, MAX(captured_at) AS captured_at, ANY_VALUE(run_id) AS run_id FROM `{{DS}}.top_10_investments` GROUP BY snapshot_date
UNION ALL SELECT snapshot_date, '_Selection_Log', COUNT(*), MAX(captured_at), ANY_VALUE(run_id) FROM `{{DS}}.selection_log` GROUP BY snapshot_date
UNION ALL SELECT snapshot_date, '_Shadow_Rank_Log', COUNT(*), MAX(captured_at), ANY_VALUE(run_id) FROM `{{DS}}.shadow_rank_log` GROUP BY snapshot_date
UNION ALL SELECT snapshot_date, '_Board_Scorecard', COUNT(*), MAX(captured_at), ANY_VALUE(run_id) FROM `{{DS}}.board_scorecard` GROUP BY snapshot_date
UNION ALL SELECT snapshot_date, 'Calendar_Events', COUNT(*), MAX(captured_at), ANY_VALUE(run_id) FROM `{{DS}}.calendar_events` GROUP BY snapshot_date
UNION ALL SELECT snapshot_date, 'Signal_History', COUNT(*), MAX(captured_at), ANY_VALUE(run_id) FROM `{{DS}}.signal_history` GROUP BY snapshot_date
UNION ALL SELECT snapshot_date, 'Performance_Log', COUNT(*), MAX(captured_at), ANY_VALUE(run_id) FROM `{{DS}}.performance_log` GROUP BY snapshot_date
UNION ALL SELECT snapshot_date, 'My_Portfolio', COUNT(*), MAX(captured_at), ANY_VALUE(run_id) FROM `{{DS}}.my_portfolio` GROUP BY snapshot_date
UNION ALL SELECT snapshot_date, 'Portfolio_Decision', COUNT(*), MAX(captured_at), ANY_VALUE(run_id) FROM `{{DS}}.portfolio_decision` GROUP BY snapshot_date
UNION ALL SELECT snapshot_date, '_Run_Log', COUNT(*), MAX(captured_at), ANY_VALUE(run_id) FROM `{{DS}}.run_log` GROUP BY snapshot_date;
