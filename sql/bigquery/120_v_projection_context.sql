-- v_projection_context: every stock's projection set against what the market itself prices - the promised return
-- over 1 and 3 months beside the stock's own one-sigma move (from its options when TFB has read them, else its
-- realised 30-day volatility), the range that move gives, how many market-wide stress states are on now, and an
-- observe-only shadow projection capped at 0.7 sigma.
-- WHY (owner, 23 Nov 2026 10:03 Riyadh: "make more deep analysis ... link the last updates ... news, future options,
-- financial derivatives, any other related data and information that can support the projection accuracy"). The
-- review of that day, on the 2,569 scored INVESTABLE buy calls of 28 Jun - 13 Nov 2026 (v_outcome_engine):
--   - the promised return and the result were uncorrelated (r = -0.04; promised +2.62 % on average, got -0.11 %);
--   - the promise measured in the stock's own one-sigma move (z): across all markets, z 0.41-0.69 hit 59 % (+1.79 %)
--     and z above 0.69 hit 49 % (-0.71 %); on the US calls alone the larger promises did better (1-month r = +0.22),
--     so the 0.7-sigma cap below is a test (v_projection_scorecard P2), not a finding;
--   - a one-sigma band from 20-day realised volatility held 59 % of the results (US: 57 %) against the 68 % it should hold:
--     the ranges were too narrow, which is what option-implied volatility corrects (it forecasts volatility better
--     than past volatility in the literature, e.g. Christensen and Prabhala 1998);
--   - the count of adverse US market states at the call (VIX 20 or more, VIX curve inverted, VVIX high, MOVE high,
--     credit weakening, 10-year yield jumping) sorted the calls: 0 -> 62 % hit (+2.51 %), 1 -> 53 %, 2 -> 49 %,
--     3 or more -> 30 % (-2.27 %).
-- All of it in-sample (those months hold few market regimes): v_projection_scorecard re-tests each rule on the calls
-- made from 24 Nov 2026. COLUMNS:
--   vol_basis_pct, vol_basis   the annual volatility used: Cboe IV30 (options, at most 6 days old) else realised 30-day
--   sigma_1m_pct, sigma_3m_pct the one-sigma move over 21 and 63 sessions (vol x sqrt(days / 252))
--   z_1m, z_3m                 the promised return over that sigma
--   range_1m_low / _high       today's price -/+ one sigma over a month (about two chances in three to end inside)
--   shadow_roi_1m_pct / _3m    the promise capped at 0.7 sigma either way (observe only)
--   market_adverse_states      0..6 on the latest US market day, with their names (US states: the reading line is
--                              written for US stocks only)
--   projection_reading         plain words for what stands out
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_projection_context`
OPTIONS (description = "TFB projection context (views 1.24.0): per stock on TFB's pages the promised 1- and 3-month return against the stock's own one-sigma move (Cboe implied volatility when read, else realised 30-day), the one-month range, an observe-only shadow projection capped at 0.7 sigma, and the count of adverse US market states now - research only")
AS
WITH u AS (
  SELECT symbol, name, sector, market_code, snapshot_date, current_price, expected_roi_1m_pct, expected_roi_3m_pct,
    volatility_30d_pct, overall_score, recommendation, final_action, investability_status
  FROM `{{DS}}.v_universe_latest`
  WHERE source_page IN ('Global_Markets', 'Market_Leaders')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY source_page DESC) = 1
),
o AS (
  SELECT symbol, snapshot_date AS options_day, iv30, implied_move_pct, front_days, event_priced, options_tilt,
    short_ratio_z
  FROM `{{DS}}.v_options_reading`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY snapshot_date DESC) = 1
),
m AS (
  SELECT date AS market_date,
    ARRAY_TO_STRING(ARRAY(SELECT x FROM UNNEST([
      IF(vix_level_state IN ('20 to 30', '30 and over'), 'VIX 20 or more', NULL),
      IF(vix_term_state = 'inverted', 'VIX curve inverted', NULL),
      IF(vvix_state = 'high', 'VVIX high', NULL),
      IF(move_state = 'high', 'MOVE high', NULL),
      IF(credit_state = 'weakening', 'credit weakening', NULL),
      IF(rates_state = 'yields jumping', '10-year yield jumping', NULL)
    ]) AS x WHERE x IS NOT NULL), ', ') AS adverse_names
  FROM `{{DS}}.v_market_drivers`
  QUALIFY ROW_NUMBER() OVER (ORDER BY date DESC) = 1
),
j AS (
  SELECT u.*, o.options_day, o.iv30, o.implied_move_pct, o.front_days, o.event_priced, o.options_tilt, o.short_ratio_z,
    COALESCE(o.iv30, u.volatility_30d_pct) AS vol,
    IF(o.iv30 IS NOT NULL, 'options (Cboe IV30)', IF(u.volatility_30d_pct IS NOT NULL, 'realised 30-day', NULL)) AS vol_basis,
    m.market_date, m.adverse_names,
    IF(m.adverse_names = '', 0, ARRAY_LENGTH(SPLIT(m.adverse_names, ', '))) AS market_adverse_states
  FROM u
  LEFT JOIN o ON o.symbol = u.symbol
  CROSS JOIN m
),
s AS (
  SELECT j.*,
    IF(vol > 0, vol * SQRT(21 / 252), NULL) AS sigma_1m,
    IF(vol > 0, vol * SQRT(63 / 252), NULL) AS sigma_3m
  FROM j
)
SELECT
  symbol, name, sector, market_code, snapshot_date, current_price, overall_score, recommendation, final_action,
  investability_status,
  expected_roi_1m_pct, expected_roi_3m_pct,
  ROUND(vol, 2) AS vol_basis_pct, vol_basis, options_day,
  ROUND(sigma_1m, 2) AS sigma_1m_pct, ROUND(sigma_3m, 2) AS sigma_3m_pct,
  ROUND(SAFE_DIVIDE(expected_roi_1m_pct, sigma_1m), 2) AS z_1m,
  ROUND(SAFE_DIVIDE(expected_roi_3m_pct, sigma_3m), 2) AS z_3m,
  ROUND(current_price * (1 - sigma_1m / 100), 2) AS range_1m_low,
  ROUND(current_price * (1 + sigma_1m / 100), 2) AS range_1m_high,
  ROUND(SIGN(expected_roi_1m_pct) * LEAST(ABS(expected_roi_1m_pct), 0.7 * sigma_1m), 2) AS shadow_roi_1m_pct,
  ROUND(SIGN(expected_roi_3m_pct) * LEAST(ABS(expected_roi_3m_pct), 0.7 * sigma_3m), 2) AS shadow_roi_3m_pct,
  implied_move_pct, front_days, event_priced, options_tilt, short_ratio_z,
  market_date, market_adverse_states, NULLIF(adverse_names, '') AS market_adverse_names,
  NULLIF(ARRAY_TO_STRING(ARRAY(SELECT x FROM UNNEST([
    IF(SAFE_DIVIDE(expected_roi_1m_pct, sigma_1m) > 0.7 OR SAFE_DIVIDE(expected_roi_3m_pct, sigma_3m) > 0.7,
       'a large promise: above 0.7 of its own one-sigma move (record mixed: see v_projection_scorecard)', NULL),
    IF(market_adverse_states >= 3 AND market_code = 'US', FORMAT('%d adverse market states now: calls made at 3 or more hit 30 %% in-sample',
       market_adverse_states), NULL),
    IF(event_priced, 'options price an event before the next expiry', NULL),
    IF(options_tilt <= -1, 'options lean down', NULL),
    IF(options_tilt >= 1, 'options lean up', NULL),
    IF(vol_basis = 'realised 30-day',
       'range from realised volatility: in-sample such a range held about 57-59 % of results, not 68 %', NULL)
  ]) AS x WHERE x IS NOT NULL), ' | '), '') AS projection_reading
FROM s;
