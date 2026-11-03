-- Add as a Looker BigQuery custom-query source ONLY after gated initialization.
-- Display alongside, never in place of, each chart's actual data timestamp.
-- Replace the dataset identifier with the verified target if it differs.
WITH attempt AS (
  SELECT * FROM `project-2e3530e6-2311-4aa3-b36.tfb_archive.dashboard_refresh_attempts`
  QUALIFY ROW_NUMBER() OVER (ORDER BY attempted_at DESC, attempt_id DESC) = 1
),
coherent AS (
  SELECT * FROM `project-2e3530e6-2311-4aa3-b36.tfb_archive.dashboard_snapshot_receipts`
  QUALIFY ROW_NUMBER() OVER (
    ORDER BY SAFE_CAST(source_run_id AS INT64) DESC, source_attempt DESC, captured_at DESC) = 1
)
SELECT
  a.attempted_at AS latest_attempt_at,
  a.source_run_id AS attempted_source_run,
  a.state AS latest_attempt_state,
  a.reason AS latest_attempt_reason,
  JSON_VALUE(a.diagnostic_json, '$.details.reason_codes[0]') AS primary_reason_code,
  JSON_QUERY(a.diagnostic_json, '$.details.reason_codes') AS reason_codes,
  JSON_VALUE(a.diagnostic_json, '$.details.display.decision_feed_state') AS observed_decision_feed_state,
  JSON_QUERY(a.diagnostic_json, '$.details.display.business_flags') AS observed_business_flags,
  SAFE_CAST(JSON_VALUE(a.diagnostic_json, '$.details.display.executable_count') AS INT64) AS observed_executable_count,
  SAFE_CAST(JSON_VALUE(a.diagnostic_json, '$.details.display.consumed_holdings') AS INT64) AS consumed_holdings,
  SAFE_CAST(JSON_VALUE(a.diagnostic_json, '$.details.display.published_holdings') AS INT64) AS published_holdings,
  c.capture_id AS coherent_capture_id,
  c.source_run_id AS coherent_source_run,
  c.captured_at AS coherent_data_captured_at,
  -- CURRENT_TIMESTAMP inside the transaction is its START timestamp, not the
  -- exact job completion time. Source ordering remains an independent check.
  c.committed_at AS coherent_publication_transaction_at,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), a.attempted_at, MINUTE) AS diagnostic_age_minutes,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), c.captured_at, HOUR) AS coherent_copy_age_hours,
  CASE
    WHEN a.attempted_at IS NULL AND c.capture_id IS NULL THEN 'NO CAPTURE OR DIAGNOSTIC RECEIPT — CHECK LEGACY DATA TIMES'
    WHEN a.attempted_at IS NULL THEN 'COHERENT DATA COMMITTED — DIAGNOSTIC UNKNOWN; CHECK DATA AGE'
    WHEN c.capture_id IS NULL THEN 'REFRESH UNCONFIRMED — NO COHERENT CAPTURE RECEIPT; CHECK LEGACY DATA TIMES'
    WHEN SAFE_CAST(c.source_run_id AS INT64) > SAFE_CAST(a.source_run_id AS INT64)
      THEN 'LATE ATTEMPT SUPERSEDED — NEWER COHERENT SOURCE IS CURRENT'
    WHEN c.committed_at > a.attempted_at THEN 'NEWER COHERENT DATA — DIAGNOSTIC NOT YET CONFIRMED'
    WHEN a.state = 'DATA_CURRENT' AND a.capture_id = c.capture_id THEN 'LAST COHERENT CAPTURE — CHECK QUOTE AND DECISION AGE'
    WHEN a.state = 'WITHHELD' THEN 'REFRESH WITHHELD — SHOWING PREVIOUS COHERENT DATA'
    ELSE 'REFRESH UNKNOWN — CHECK DATA AND DIAGNOSTIC TIMES'
  END AS display_status,
  FALSE AS trading_permission_granted
FROM (SELECT 1 AS anchor) AS singleton
LEFT JOIN attempt AS a ON TRUE
LEFT JOIN coherent AS c ON TRUE;
