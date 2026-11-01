-- v_outcome_engine: every forecast record of v_performance_log scored from
-- the ARCHIVED daily bars (symbol_bars_daily) instead of waiting for a live
-- quote inside the target session. WHY (owner, 2 Nov 2026: "learning is
-- slow"; "improve today, results tomorrow"): the tracker closes a record only
-- when an audit holds a live quote of exactly the target session, so a
-- session that passed unquoted stays open for good (5,759 overdue records on
-- 1 Nov, most of them quote_rate_limit_breaker or quote_outside_target_
-- session). The bar archive already holds the close of that session.
-- RULE (mirrors track_performance._outcome_target_session): the target
-- session is the venue-local date of the target time, or the next day when
-- the target falls after that venue's close (venue table below, by listing
-- suffix); the outcome price is the close of the FIRST archived bar on or
-- after that date, accepted only within 7 calendar days (a later session is
-- never used: beyond 7 days the state is no_bar_at_target). The call is
-- scored exactly as v_performance_log scores the sheet's realized ROI
-- (entry_stance x sign of the ROI: a bearish call whose price fell wins);
-- HOLD and unknown tiers are never scored (unscorable_stance).
-- COLUMNS beyond the record's own fields (all percent points):
--   engine_state       scored_from_bar | target_not_due | awaiting_bar (the
--                      archive has not reached the target yet) |
--                      no_bar_at_target | no_bar_for_symbol | unscorable_stance
--                      | no_entry_price
--   target_session_date, target_bar_date, target_close, target_bar_source
--   engine_roi_pp, engine_call_outcome, engine_scored, engine_win_flag
--   in_engine_cohort   the first engine-scored record per Key (the sheet's
--                      cohort rule), so a key archived twice counts once
--   sheet_call_outcome / sheet_scored / sheet_win_flag and sheet_vs_engine:
--                      agree | disagree | sheet_only | engine_only for matured
--                      records; roi_gap_pp = engine - sheet realized ROI
--   mfe_pct / mae_pct  best and worst excursion of the high / low AFTER the
--                      recording day up to the target bar (the newest bar
--                      while open): the recording day's own bar is left out,
--                      because a daily bar cannot separate the moves before
--                      the forecast existed from those after it (Codex P2
--                      on #891 - a target 'touched' by that morning's high)
--   touched_target, days_to_target_touch, bars_observed
--   last_bar_date, last_close, bar_roi_pp (the engine's unrealized ROI)
--   roi_1w_pp, roi_2w_pp, roi_1m_pp, roi_3m_pp: the close of the first bar
--                      7 / 14 / 30 / 91 days after the recording, whatever
--                      the record's own horizon (NULL until reached)
-- Research view: the workbook's Directional Win Rate stays the tracker's;
-- the live system never reads BigQuery; no decision or count depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_outcome_engine`
OPTIONS (description = "TFB Looker v5: every Performance_Log record scored from the archived daily close of the first session on/after its target (engine_state, engine ROI and verdict, excursions, horizon ladder), beside the sheet's own outcome")
AS
WITH venue AS (
  -- listing suffix -> market label, time zone, the local minute the
  -- continuous session closes (approximate: a target inside the last hour of
  -- a session may shift one session against the tracker's calendar)
  SELECT *
  FROM UNNEST(ARRAY<STRUCT<suffix STRING, market STRING, tz STRING, close_min INT64>>[
    ('SR', 'Tadawul', 'Asia/Riyadh', 900),
    ('US', 'US', 'America/New_York', 960),
    ('HK', 'Hong Kong', 'Asia/Hong_Kong', 960),
    ('L', 'London', 'Europe/London', 990),
    ('T', 'Tokyo', 'Asia/Tokyo', 930),
    ('DE', 'Europe', 'Europe/Berlin', 1050),
    ('PA', 'Europe', 'Europe/Paris', 1050),
    ('MI', 'Europe', 'Europe/Rome', 1050),
    ('MC', 'Europe', 'Europe/Madrid', 1050),
    ('AS', 'Europe', 'Europe/Amsterdam', 1050),
    ('BR', 'Europe', 'Europe/Brussels', 1050),
    ('SW', 'Europe', 'Europe/Zurich', 1050),
    ('LS', 'Europe', 'Europe/Lisbon', 1010),
    ('VI', 'Europe', 'Europe/Vienna', 1050),
    ('WA', 'Europe', 'Europe/Warsaw', 1020),
    ('AT', 'Europe', 'Europe/Athens', 1020),
    ('ST', 'Europe', 'Europe/Stockholm', 1050),
    ('OL', 'Europe', 'Europe/Oslo', 980),
    ('CO', 'Europe', 'Europe/Copenhagen', 1020),
    ('HE', 'Europe', 'Europe/Helsinki', 1090),
    ('IR', 'Europe', 'Europe/Dublin', 990),
    ('IS', 'Istanbul', 'Europe/Istanbul', 1080),
    ('AX', 'Australia', 'Australia/Sydney', 960),
    ('TO', 'Canada', 'America/Toronto', 960),
    ('V', 'Canada', 'America/Toronto', 960),
    ('NS', 'India', 'Asia/Kolkata', 930),
    ('BO', 'India', 'Asia/Kolkata', 930),
    ('KS', 'Korea', 'Asia/Seoul', 930),
    ('KQ', 'Korea', 'Asia/Seoul', 930),
    ('TW', 'Taiwan', 'Asia/Taipei', 810),
    ('SS', 'China', 'Asia/Shanghai', 900),
    ('SZ', 'China', 'Asia/Shanghai', 900),
    ('SI', 'Singapore', 'Asia/Singapore', 1020),
    ('KL', 'Malaysia', 'Asia/Kuala_Lumpur', 1020),
    ('JK', 'Indonesia', 'Asia/Jakarta', 950),
    ('BK', 'Thailand', 'Asia/Bangkok', 1010),
    ('JO', 'South Africa', 'Africa/Johannesburg', 1020),
    ('MX', 'Mexico', 'America/Mexico_City', 900),
    ('SA', 'Brazil', 'America/Sao_Paulo', 1075),
    ('CA', 'Egypt', 'Africa/Cairo', 870),
    ('QA', 'Qatar', 'Asia/Qatar', 795),
    ('AE', 'UAE', 'Asia/Dubai', 900),
    ('AD', 'UAE', 'Asia/Dubai', 900),
    ('KW', 'Kuwait', 'Asia/Kuwait', 765),
    ('BH', 'Bahrain', 'Asia/Bahrain', 810)
  ])
),
rec AS (
  SELECT
    p.*,
    REGEXP_EXTRACT(p.symbol, r'\.([A-Z]+)$') AS suffix
  FROM `{{DS}}.v_performance_log` AS p
  WHERE p.target_at IS NOT NULL
),
r AS (
  SELECT
    rec.*,
    -- a bare ticker lists in the US; an unknown suffix keeps the US clock but
    -- reads 'Other' so a wrong venue is never presented as a known one
    COALESCE(v.market, IF(rec.suffix IS NULL, 'US', 'Other')) AS market,
    DATE(rec.target_at, 'Asia/Riyadh') AS target_date,
    DATE(rec.recorded_at, 'Asia/Riyadh') AS recorded_day,
    IF(EXTRACT(HOUR FROM DATETIME(rec.target_at, COALESCE(v.tz, 'America/New_York'))) * 60
       + EXTRACT(MINUTE FROM DATETIME(rec.target_at, COALESCE(v.tz, 'America/New_York'))) > COALESCE(v.close_min, 960),
       DATE_ADD(DATE(rec.target_at, COALESCE(v.tz, 'America/New_York')), INTERVAL 1 DAY),
       DATE(rec.target_at, COALESCE(v.tz, 'America/New_York'))) AS target_session_date
  FROM rec
  LEFT JOIN venue AS v ON v.suffix = rec.suffix
),
sym_last AS (
  SELECT symbol, MAX(date) AS last_bar_date,
         ARRAY_AGG(close ORDER BY date DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_close
  FROM `{{DS}}.symbol_bars_daily`
  WHERE close IS NOT NULL
  GROUP BY symbol
),
agg AS (
  -- one pass over the bars of each record's window: from the recording day
  -- to 7 days past the target session (or 98 days past the recording for
  -- the horizon ladder, whichever is later)
  SELECT
    r.record_id,
    ARRAY_AGG(IF(b.date >= r.target_session_date AND b.date <= DATE_ADD(r.target_session_date, INTERVAL 7 DAY),
                 STRUCT(b.date AS date, b.high AS high, b.low AS low, b.close AS close, b.source AS source), NULL)
              IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS tb,
    -- excursions start the day AFTER the recording (b.date > r.recorded_day): the
    -- recording day's bar mixes pre-forecast and post-forecast moves
    MAX(IF(b.date > r.recorded_day AND b.date < r.target_session_date, b.high, NULL)) AS pre_high,
    MIN(IF(b.date > r.recorded_day AND b.date < r.target_session_date, b.low, NULL)) AS pre_low,
    COUNTIF(b.date > r.recorded_day AND b.date < r.target_session_date) AS pre_bars,
    MIN(IF(b.date > r.recorded_day AND b.date < r.target_session_date AND r.entry_stance = 'bullish' AND b.high >= r.target_price, b.date, NULL)) AS pre_touch_up,
    MIN(IF(b.date > r.recorded_day AND b.date < r.target_session_date AND r.entry_stance = 'bearish' AND b.low <= r.target_price, b.date, NULL)) AS pre_touch_dn,
    ARRAY_AGG(IF(b.date >= DATE_ADD(r.recorded_day, INTERVAL 7 DAY), b.close, NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS close_1w,
    ARRAY_AGG(IF(b.date >= DATE_ADD(r.recorded_day, INTERVAL 14 DAY), b.close, NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS close_2w,
    ARRAY_AGG(IF(b.date >= DATE_ADD(r.recorded_day, INTERVAL 30 DAY), b.close, NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS close_1m,
    ARRAY_AGG(IF(b.date >= DATE_ADD(r.recorded_day, INTERVAL 91 DAY), b.close, NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS close_3m
  FROM r
  JOIN `{{DS}}.symbol_bars_daily` AS b
    ON b.symbol = r.symbol
   AND b.close IS NOT NULL
   AND b.date >= r.recorded_day
   AND b.date <= GREATEST(DATE_ADD(r.target_session_date, INTERVAL 7 DAY), DATE_ADD(r.recorded_day, INTERVAL 98 DAY))
  GROUP BY r.record_id
),
j AS (
  SELECT
    r.*,
    sl.last_bar_date,
    sl.last_close,
    a.tb.date AS target_bar_date,
    a.tb.close AS target_close,
    a.tb.source AS target_bar_source,
    GREATEST(IFNULL(a.pre_high, 0), IF(a.tb.date > r.recorded_day, IFNULL(a.tb.high, 0), 0)) AS win_high,
    LEAST(IFNULL(a.pre_low, 1e18), IF(a.tb.date > r.recorded_day, IFNULL(a.tb.low, 1e18), 1e18)) AS win_low,
    -- the target bar counts only when it is not the recording day's own bar
    IFNULL(a.pre_bars, 0) + IF(a.tb.date IS NULL OR a.tb.date <= r.recorded_day, 0, 1) AS bars_observed,
    LEAST(IFNULL(a.pre_touch_up, DATE '9999-12-31'),
          IF(r.entry_stance = 'bullish' AND a.tb.date > r.recorded_day AND a.tb.high >= r.target_price, a.tb.date, DATE '9999-12-31')) AS touch_up,
    LEAST(IFNULL(a.pre_touch_dn, DATE '9999-12-31'),
          IF(r.entry_stance = 'bearish' AND a.tb.date > r.recorded_day AND a.tb.low <= r.target_price, a.tb.date, DATE '9999-12-31')) AS touch_dn,
    a.close_1w, a.close_2w, a.close_1m, a.close_3m,
    CASE
      WHEN r.entry_price IS NULL OR r.entry_price <= 0 THEN 'no_entry_price'
      WHEN r.entry_stance NOT IN ('bullish', 'bearish') OR r.entry_stance IS NULL THEN 'unscorable_stance'
      WHEN r.target_session_date > CURRENT_DATE('Asia/Riyadh') THEN 'target_not_due'
      WHEN sl.symbol IS NULL THEN 'no_bar_for_symbol'
      WHEN a.tb.date IS NOT NULL THEN 'scored_from_bar'
      WHEN DATE_ADD(r.target_session_date, INTERVAL 7 DAY) > sl.last_bar_date THEN 'awaiting_bar'
      ELSE 'no_bar_at_target'
    END AS engine_state
  FROM r
  LEFT JOIN sym_last AS sl ON sl.symbol = r.symbol
  LEFT JOIN agg AS a ON a.record_id = r.record_id
),
scored AS (
  SELECT
    j.*,
    IF(j.engine_state = 'scored_from_bar', ROUND((SAFE_DIVIDE(j.target_close, j.entry_price) - 1) * 100, 4), NULL) AS engine_roi_pp
  FROM j
),
called AS (
  SELECT
    s.*,
    CASE
      WHEN s.engine_roi_pp IS NULL THEN NULL
      WHEN s.entry_stance = 'bullish'
        THEN CASE WHEN s.engine_roi_pp > 0 THEN 'WIN' WHEN s.engine_roi_pp < 0 THEN 'LOSS' ELSE 'BREAKEVEN' END
      WHEN s.entry_stance = 'bearish'
        THEN CASE WHEN s.engine_roi_pp < 0 THEN 'WIN' WHEN s.engine_roi_pp > 0 THEN 'LOSS' ELSE 'BREAKEVEN' END
    END AS engine_call_outcome
  FROM scored AS s
)
SELECT
  c.record_id, c.record_key, c.symbol, c.market, c.origin_tab, c.horizon, c.confidence, c.risk_bucket, c.sector,
  c.entry_recommendation, c.entry_stance, c.entry_price, c.target_price, c.target_roi_pp,
  c.recorded_at, c.recorded_date, c.recorded_week, c.recorded_month, c.target_at, c.target_date,
  c.target_session_date, c.status, c.sheet_row, c.snapshot_date,
  c.entry_forecast_reliability, c.entry_data_quality, c.entry_final_action, c.entry_investability, c.entry_selected,
  c.realized_roi_pp, c.outcome AS sheet_outcome, c.call_outcome AS sheet_call_outcome, c.is_matured,
  c.is_scored AS sheet_scored, c.win_flag AS sheet_win_flag, c.in_outcome_cohort, c.matured_at,
  c.engine_state,
  c.target_bar_date, c.target_close, c.target_bar_source,
  c.engine_roi_pp,
  c.engine_call_outcome,
  IFNULL(c.engine_call_outcome IN ('WIN', 'LOSS'), FALSE) AS engine_scored,
  CASE WHEN c.engine_call_outcome = 'WIN' THEN 1 WHEN c.engine_call_outcome = 'LOSS' THEN 0 END AS engine_win_flag,
  CASE
    WHEN NOT IFNULL(c.engine_call_outcome IN ('WIN', 'LOSS'), FALSE) THEN FALSE
    ELSE ROW_NUMBER() OVER (PARTITION BY c.record_key, IFNULL(c.engine_call_outcome IN ('WIN', 'LOSS'), FALSE)
                            ORDER BY c.recorded_at, c.sheet_row, c.record_id) = 1
  END AS in_engine_cohort,
  CASE
    WHEN NOT c.is_matured THEN NULL
    WHEN c.call_outcome IS NOT NULL AND c.engine_call_outcome IS NOT NULL
      THEN IF(c.call_outcome = c.engine_call_outcome, 'agree', 'disagree')
    WHEN c.call_outcome IS NOT NULL THEN 'sheet_only'
    WHEN c.engine_call_outcome IS NOT NULL THEN 'engine_only'
  END AS sheet_vs_engine,
  IF(c.is_matured AND c.engine_roi_pp IS NOT NULL AND c.realized_roi_pp IS NOT NULL,
     ROUND(c.engine_roi_pp - c.realized_roi_pp, 4), NULL) AS roi_gap_pp,
  IF(c.bars_observed > 0 AND c.entry_price > 0 AND c.win_high > 0, ROUND((SAFE_DIVIDE(c.win_high, c.entry_price) - 1) * 100, 4), NULL) AS mfe_pct,
  IF(c.bars_observed > 0 AND c.entry_price > 0 AND c.win_low < 1e18, ROUND((SAFE_DIVIDE(c.win_low, c.entry_price) - 1) * 100, 4), NULL) AS mae_pct,
  CASE
    WHEN c.bars_observed = 0 OR c.target_price IS NULL THEN NULL
    WHEN c.entry_stance = 'bullish' THEN c.touch_up < DATE '9999-12-31'
    WHEN c.entry_stance = 'bearish' THEN c.touch_dn < DATE '9999-12-31'
  END AS touched_target,
  CASE
    WHEN c.entry_stance = 'bullish' AND c.touch_up < DATE '9999-12-31' THEN DATE_DIFF(c.touch_up, c.recorded_day, DAY)
    WHEN c.entry_stance = 'bearish' AND c.touch_dn < DATE '9999-12-31' THEN DATE_DIFF(c.touch_dn, c.recorded_day, DAY)
  END AS days_to_target_touch,
  c.bars_observed,
  c.last_bar_date,
  c.last_close,
  IF(c.entry_price > 0, ROUND((SAFE_DIVIDE(c.last_close, c.entry_price) - 1) * 100, 4), NULL) AS bar_roi_pp,
  IF(c.entry_price > 0, ROUND((SAFE_DIVIDE(c.close_1w, c.entry_price) - 1) * 100, 4), NULL) AS roi_1w_pp,
  IF(c.entry_price > 0, ROUND((SAFE_DIVIDE(c.close_2w, c.entry_price) - 1) * 100, 4), NULL) AS roi_2w_pp,
  IF(c.entry_price > 0, ROUND((SAFE_DIVIDE(c.close_1m, c.entry_price) - 1) * 100, 4), NULL) AS roi_1m_pp,
  IF(c.entry_price > 0, ROUND((SAFE_DIVIDE(c.close_3m, c.entry_price) - 1) * 100, 4), NULL) AS roi_3m_pp,
  "archived daily close of the first session on/after the target (symbol_bars_daily) - research view, not the sheet's Directional Win Rate" AS basis
FROM called AS c;
