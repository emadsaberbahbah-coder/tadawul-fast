-- v_forward_signal_recon: the forward signal ledger against the outcome engine - one row of counts and a verdict.
-- WHY (week plan of 23 Nov 2026, package 4): the ledger is a second reading of the same records, so its counts
-- must be the engine's. Checked on every read, not once.
-- THE CHECK: ledger_rows and ledger_keys = engine_keys (one row per Key, none lost, none doubled); ledger_scored
-- (status 'scored' or 'scored, no benchmark close') = engine_cohort (COUNTIF(in_engine_cohort)); state_differences
-- (a bullish or bearish record whose outcome_state is not the engine's engine_state) = 0; return_differences (a
-- record whose stock_roi_pp is not the engine's engine_roi_pp) = 0. verdict 'reconciled' when all five hold, else
-- 'DIFFERENCE'.
-- OTHER COUNTS: scored_with_benchmark, scored_no_benchmark, tracked_no_direction (HOLD records with an outcome),
-- open_signals, with_outcome (the rows v_forward_signal_gates counts as outcomes, before its tier filter).
-- Cost of a read: about 1.1 GB (the ledger and the engine share their tables).
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_forward_signal_recon`
OPTIONS (description = "TFB forward signal ledger check (views 1.31.0): the ledger's rows, keys and scored count against v_outcome_engine, state and return differences, and a verdict - research only")
AS
WITH eng AS (
  SELECT COUNT(*) AS engine_rows, COUNT(DISTINCT record_key) AS engine_keys, COUNTIF(in_engine_cohort) AS engine_cohort
  FROM `{{DS}}.v_outcome_engine`
),
led AS (
  SELECT COUNT(*) AS ledger_rows, COUNT(DISTINCT signal_key) AS ledger_keys,
    COUNTIF(STARTS_WITH(status, 'scored')) AS ledger_scored,
    COUNTIF(status = 'scored') AS scored_with_benchmark,
    COUNTIF(status = 'scored, no benchmark close') AS scored_no_benchmark,
    COUNTIF(status = 'tracked: no direction given') AS tracked_no_direction,
    COUNTIF(STARTS_WITH(status, 'open')) AS open_signals,
    COUNTIF(active_net_pp IS NOT NULL) AS with_outcome,
    COUNTIF(entry_stance IN ('bullish', 'bearish') AND outcome_state != engine_state) AS state_differences,
    COUNTIF(engine_roi_pp IS NOT NULL AND ABS(IFNULL(stock_roi_pp, 1e9) - engine_roi_pp) > 0.0001) AS return_differences
  FROM `{{DS}}.v_forward_signals`
)
SELECT
  eng.engine_rows, eng.engine_keys, led.ledger_rows, led.ledger_keys,
  eng.engine_cohort, led.ledger_scored, led.ledger_scored - eng.engine_cohort AS scored_difference,
  led.scored_with_benchmark, led.scored_no_benchmark, led.tracked_no_direction, led.open_signals, led.with_outcome,
  led.state_differences, led.return_differences,
  IF(led.ledger_rows = eng.engine_keys AND led.ledger_keys = eng.engine_keys AND led.ledger_scored = eng.engine_cohort
     AND led.state_differences = 0 AND led.return_differences = 0, 'reconciled', 'DIFFERENCE') AS verdict
FROM eng CROSS JOIN led;
