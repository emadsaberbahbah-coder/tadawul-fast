-- v_board_archive: recommendation layer 2 - the Top-10 cockpit's seat gates
-- re-run every night over the archived universe (v_universe_latest), so a
-- candidate board exists on the Looker page even when the workbook render,
-- the funding evidence or the release slot fails. WHY (owner, 2 Nov 2026:
-- "layers for recommendations so the system still issues them when one is
-- available"). It is an ARCHIVE board: research only, never the cockpit -
-- executable is FALSE on every row, no seat here is funded, no order follows
-- from it, and the live system never reads BigQuery.
-- GATES mirror apps_script/16_Decision_Top10.gs DT10_PANEL defaults by
-- their panel labels (the operator may run the cockpit with other values;
-- gates_applied states these):
--   engine verdict      final_action = INVEST, investability_status =
--                       INVESTABLE, no block_reason (the backend's always-on
--                       Sell-Class gate: no SELL / STRONG_SELL / REDUCE /
--                       AVOID / EXIT / TRIM recommendation)
--   T10: Min Reliability 70      reliability >= 70
--   T10: Min Data Quality 80     data_quality >= 80
--   T10: Max Risk Level Medium   risk_bucket LOW or MODERATE
--   T10: Required ROI % 12 over T10: Period (Months) 12
--                                expected_roi_12m_pct >= 12
--   T10: Required Annualized ROI % 10   the same 12-month figure >= 10
--   T10: Allow Conflict No       provider_engine_conflict is not TRUE
--   T10: Include Portfolio Holdings No   not a current holding
--   T10: Allow Negative News No  not named by the newest news-gate run
--   T10: Max Per Sector 3, T10: Max Per Market 10, T10: Max Selected 10
-- Not derivable from the archive, so NOT applied (gates_not_applied):
--   T10: Min Risk/Reward 2, T10: Allow Negative Sector No, the stability
--   clocks (confirm / exit days), the funding and source-readiness checks.
-- ORDER among passers: the engine's opportunity_score, then the 12-month
-- expected ROI, then reliability, then the symbol (the cockpit's own order
-- is the backend's; this is the closest the archive can state).
-- cautions name what the page must say beside a seat (stale quote, old
-- archive copy, a conflict flag); they never change the gates.
CREATE OR REPLACE VIEW `{{DS}}.v_board_archive`
OPTIONS (description = "TFB Looker v5: the Top-10 panel's default gates re-run over the archived universe - gate_pass, failed gates, archive rank and seat (at most 3 per sector, 10 per market, 10 seats) - research only, executable = FALSE")
AS
WITH hold AS (
  SELECT DISTINCT symbol
  FROM `{{DS}}.v_my_portfolio`
  WHERE symbol IS NOT NULL
    AND snapshot_date = (SELECT MAX(snapshot_date) FROM `{{DS}}.v_my_portfolio`)
),
gate AS (
  SELECT verdict, names_flagged, run_at
  FROM `{{DS}}.v_news_gate`
  WHERE run_at IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (ORDER BY run_at DESC) = 1
),
u AS (
  SELECT
    u.symbol, u.symbol_label, u.name, u.source_page, u.region, u.market_country, u.sector, u.industry,
    u.asset_class, u.currency, u.current_price, u.change_pct,
    u.expected_roi_3m_pct, u.expected_roi_12m_pct, u.reliability, u.data_quality, u.risk_bucket,
    u.provider_engine_conflict, u.investability_status, u.final_action, u.block_reason, u.recommendation,
    u.meets_floors, u.opportunity_score, u.rank_overall, u.analyst_rating, u.target_upside_pct, u.top_risks,
    u.confidence_bucket, u.data_age_hours, u.data_age_now_hours, u.snapshot_date, u.snapshot_age_days,
    h.symbol IS NOT NULL AS is_holding,
    g.verdict AS news_gate_verdict,
    g.run_at AS news_gate_run_at,
    IFNULL(REGEXP_CONTAINS(g.names_flagged, CONCAT(r'(^|[^A-Z0-9.=-])', REGEXP_REPLACE(u.symbol, r'([.^$*+?()\[\]{}|\\-])', r'\\\1'), r'($|[^A-Z0-9.=-])')), FALSE) AS flagged_by_news_gate
  FROM `{{DS}}.v_universe_latest` AS u
  LEFT JOIN hold AS h ON h.symbol = u.symbol
  LEFT JOIN gate AS g ON TRUE
  WHERE u.symbol IS NOT NULL
),
judged AS (
  SELECT
    u.*,
    ARRAY(
      SELECT f FROM UNNEST([
        IF(IFNULL(u.final_action, '') != 'INVEST', 'engine: final action not INVEST', NULL),
        IF(IFNULL(u.investability_status, '') != 'INVESTABLE', 'engine: not INVESTABLE', NULL),
        IF(u.block_reason IS NOT NULL, 'engine: block reason', NULL),
        IF(UPPER(REPLACE(IFNULL(u.recommendation, ''), '_', ' ')) IN ('SELL', 'STRONG SELL', 'REDUCE', 'AVOID', 'EXIT', 'TRIM'),
           'sell-class recommendation', NULL),
        IF(NOT IFNULL(u.reliability >= 70, FALSE), 'reliability below 70 (T10: Min Reliability)', NULL),
        IF(NOT IFNULL(u.data_quality >= 80, FALSE), 'data quality below 80 (T10: Min Data Quality)', NULL),
        IF(IFNULL(u.risk_bucket, '') NOT IN ('LOW', 'MODERATE'), 'risk above Medium (T10: Max Risk Level)', NULL),
        IF(NOT IFNULL(u.expected_roi_12m_pct >= 12, FALSE), '12-month ROI below 12 % (T10: Required ROI %)', NULL),
        IF(NOT IFNULL(u.expected_roi_12m_pct >= 10, FALSE), 'annualized ROI below 10 % (T10: Required Annualized ROI %)', NULL),
        IF(UPPER(IFNULL(u.provider_engine_conflict, '')) = 'TRUE', 'provider/engine conflict (T10: Allow Conflict No)', NULL),
        IF(u.is_holding, 'portfolio holding (T10: Include Portfolio Holdings No)', NULL),
        IF(u.flagged_by_news_gate, 'named by the news gate (T10: Allow Negative News No)', NULL)
      ]) AS f
      WHERE f IS NOT NULL
    ) AS fails,
    ARRAY(
      SELECT c FROM UNNEST([
        IF(u.data_age_hours > 36, 'quote older than 36 h when archived', NULL),
        IF(u.snapshot_age_days > 1, CONCAT('archive copy ', CAST(u.snapshot_age_days AS STRING), ' days old'), NULL),
        IF(u.news_gate_verdict = 'NO_NEW_ENTRY', 'news gate: NO_NEW_ENTRY', NULL),
        IF(u.news_gate_verdict = 'CAUTION', 'news gate: CAUTION', NULL),
        IF(u.asset_class != 'EQUITY', CONCAT('asset class ', u.asset_class), NULL)
      ]) AS c
      WHERE c IS NOT NULL
    ) AS cautions
  FROM u
),
passers AS (
  SELECT
    j.*,
    ROW_NUMBER() OVER (PARTITION BY j.sector ORDER BY j.opportunity_score DESC, j.expected_roi_12m_pct DESC, j.reliability DESC, j.symbol) AS sector_rank,
    ROW_NUMBER() OVER (PARTITION BY j.region ORDER BY j.opportunity_score DESC, j.expected_roi_12m_pct DESC, j.reliability DESC, j.symbol) AS market_rank
  FROM judged AS j
  WHERE ARRAY_LENGTH(j.fails) = 0
),
ranked AS (
  SELECT
    p.*,
    ROW_NUMBER() OVER (ORDER BY p.opportunity_score DESC, p.expected_roi_12m_pct DESC, p.reliability DESC, p.symbol) AS archive_rank
  FROM passers AS p
  WHERE p.sector_rank <= 3 AND p.market_rank <= 10
)
SELECT
  j.* EXCEPT (fails, cautions, is_holding),
  ARRAY_LENGTH(j.fails) = 0 AS gate_pass,
  ARRAY_TO_STRING(j.fails, ' | ') AS gate_fail_reasons,
  ARRAY_LENGTH(j.fails) AS gate_fail_count,
  r.archive_rank,
  IF(r.archive_rank <= 10, r.archive_rank, NULL) AS archive_seat,
  CASE
    WHEN r.archive_rank <= 10 THEN 'archive seat'
    WHEN r.archive_rank IS NOT NULL THEN 'passes the gates - beyond seat 10'
    WHEN ARRAY_LENGTH(j.fails) = 0 THEN 'passes the gates - sector or market cap reached'
    ELSE 'fails a gate'
  END AS archive_status,
  j.is_holding,
  ARRAY_TO_STRING(j.cautions, ' | ') AS cautions,
  FALSE AS executable,
  'archive board - research only, not the cockpit' AS board_kind,
  'INVEST + INVESTABLE + no block, no sell-class tier, reliability >= 70, data quality >= 80, risk <= Medium, 12M ROI >= 12 % (annualized >= 10 %), no provider/engine conflict, not a holding, not named by the news gate, 3 per sector, 10 per market, 10 seats' AS gates_applied,
  'T10: Min Risk/Reward, T10: Allow Negative Sector, the stability confirm/exit clocks, funding evidence, source readiness' AS gates_not_applied
FROM judged AS j
LEFT JOIN ranked AS r ON r.symbol = j.symbol
WHERE j.final_action = 'INVEST' OR j.investability_status = 'INVESTABLE';
