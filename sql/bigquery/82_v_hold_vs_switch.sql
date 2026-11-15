-- v_hold_vs_switch: each holding against the best available opportunity of
-- its own market, over 1 week, 2 weeks, 1 month and 3 months, after the cost
-- of both trades. One row per holding and horizon.
-- WHY (owner, 15 Nov 2026): a holding is kept while it is expected to do
-- better than the best alternative over the same period net of both
-- transaction costs, and the comparison must be accurate. Accurate here
-- means: the gap between two FORECASTS is first converted into what such a
-- gap has actually been worth (v_switch_calibration), then both trades are
-- charged, then the chance that the switch ends ahead is stated.
-- INPUTS:
--   holdings   the newest My_Portfolio snapshot (v_my_portfolio), quantity > 0
--   candidates the newest universe row per symbol (v_universe_latest) that is
--              INVESTABLE with final action INVEST, passes the panel floors,
--              is at most 3 days old and is not already held; same market
--              group as the holding (US, Tadawul, Other markets)
--   forecasts  1M and 3M are the engine's own columns. The pages carry no
--              1W or 2W forecast, so those are 0.16 x and 0.27 x the 1M
--              figure: the ratio of the average recorded 1W / 2W forecast to
--              the average 1M forecast in Performance_Log (0.46 / 0.77 /
--              2.84 pp). The best candidate is the highest forecast for that
--              horizon (ties: higher overall score, then symbol).
-- ARITHMETIC (all in percentage points of the amount switched):
--   gap_pp        candidate forecast - holding forecast
--   advantage_pp  gap_pp x k_indicative (what a forecast gap has been worth)
--   cost_pp       2 x (commission + spread), one sale and one purchase:
--                 0.10 + 0.15 per side = 0.50. Commission measured on the
--                 owner's own fills (0.094% of traded value, 18 fills to 12
--                 Nov 2026); the spread is an allowance, not a measurement.
--   net_pp        advantage_pp - cost_pp
--   chance_pct    the chance the candidate ends ahead after costs:
--                 logistic(1.702 x net_pp / (SQRT(2) x noise_pp)), the
--                 standard logistic stand-in for the normal curve; noise_pp
--                 is the spread of outcomes across names, taken as
--                 independent for the two names
-- VERDICT, in this order:
--   NO DATA           a forecast, a candidate or a calibration row is missing
--   KEEP              gap_pp <= 0: the holding has the higher forecast
--   KEEP              the signal is not 'proven' at this horizon: the figures
--                     are shown as indicative and move nothing
--   KEEP              net_pp <= 0: the advantage does not cover both trades
--   KEEP              chance_pct < 60
--   SWITCH CANDIDATE  otherwise. A candidate for the owner's review, never
--                     an order: the Today Action Plan is the only page that
--                     gives orders.
-- Not judged here: stops, exit rules, earnings dates, tax, position caps.
-- Research view: the live system never reads BigQuery.
CREATE OR REPLACE VIEW `{{DS}}.v_hold_vs_switch`
OPTIONS (description = "TFB hold or switch: per holding and horizon (1W, 2W, 1M, 3M) - the holding's forecast, the best INVESTABLE candidate of the same market and its forecast, the gap, what such a gap has been worth (v_switch_calibration), the cost of both trades, the net advantage, the chance the switch ends ahead, and a fixed verdict (SWITCH CANDIDATE only when the signal is proven, the net is positive and the chance is 60% or more). Research only: never an order.")
AS
WITH params AS (
  SELECT 0.10 AS commission_side_pp, 0.15 AS spread_side_pp, 60.0 AS min_chance_pct
),
hz AS (
  SELECT '1W' AS horizon, 1 AS horizon_order UNION ALL
  SELECT '2W', 2 UNION ALL
  SELECT '1M', 3 UNION ALL
  SELECT '3M', 4
),
hold AS (
  SELECT
    p.snapshot_date,
    p.symbol,
    p.name,
    p.currency,
    p.position_qty,
    p.position_value,
    p.unrealized_pnl_pp,
    CASE REGEXP_EXTRACT(UPPER(p.symbol), r'\.([A-Z]{1,5})$')
      WHEN 'US' THEN 'US' WHEN 'SR' THEN 'Tadawul' ELSE 'Other markets'
    END AS market_group,
    p.expected_roi_1m_pp AS f_1m,
    p.expected_roi_3m_pp AS f_3m
  FROM `{{DS}}.v_my_portfolio` AS p
  WHERE p.snapshot_date = (SELECT MAX(snapshot_date) FROM `{{DS}}.v_my_portfolio`)
    AND p.position_qty > 0
),
cand AS (
  SELECT
    u.symbol,
    u.name,
    u.overall_score,
    CASE u.market_code WHEN 'US' THEN 'US' WHEN 'SR' THEN 'Tadawul' ELSE 'Other markets' END AS market_group,
    u.expected_roi_1m_pct AS f_1m,
    u.expected_roi_3m_pct AS f_3m
  FROM `{{DS}}.v_universe_latest` AS u
  WHERE UPPER(u.investability_status) = 'INVESTABLE'
    AND UPPER(u.final_action) = 'INVEST'
    AND u.meets_floors
    AND u.snapshot_age_days <= 3
    AND u.symbol NOT IN (SELECT symbol FROM hold)
),
hold_h AS (
  SELECT
    h.*, z.horizon, z.horizon_order,
    CASE z.horizon WHEN '1W' THEN h.f_1m * 0.16 WHEN '2W' THEN h.f_1m * 0.27
                   WHEN '1M' THEN h.f_1m ELSE h.f_3m END AS f
  FROM hold AS h CROSS JOIN hz AS z
),
cand_h AS (
  SELECT
    c.*, z.horizon,
    CASE z.horizon WHEN '1W' THEN c.f_1m * 0.16 WHEN '2W' THEN c.f_1m * 0.27
                   WHEN '1M' THEN c.f_1m ELSE c.f_3m END AS f
  FROM cand AS c CROSS JOIN hz AS z
),
best AS (
  SELECT market_group, horizon, symbol, name, f
  FROM (
    SELECT
      x.*,
      ROW_NUMBER() OVER (PARTITION BY market_group, horizon
                         ORDER BY f DESC, overall_score DESC, symbol) AS rk
    FROM cand_h AS x
    WHERE f IS NOT NULL
  )
  WHERE rk = 1
),
j AS (
  SELECT
    h.snapshot_date, h.symbol, h.name, h.market_group, h.currency,
    h.position_qty, h.position_value, h.unrealized_pnl_pp,
    h.horizon, h.horizon_order,
    h.f AS holding_f,
    b.symbol AS best_symbol,
    b.name AS best_name,
    b.f AS best_f,
    c.verdict AS signal_verdict,
    c.k_indicative,
    c.noise_pp,
    2 * (q.commission_side_pp + q.spread_side_pp) AS cost_pp,
    q.min_chance_pct
  FROM hold_h AS h
  CROSS JOIN params AS q
  LEFT JOIN best AS b ON b.market_group = h.market_group AND b.horizon = h.horizon
  LEFT JOIN `{{DS}}.v_switch_calibration` AS c
    ON c.market_group = h.market_group AND c.horizon = h.horizon
),
m AS (
  SELECT
    j.*,
    best_f - holding_f AS gap_pp,
    (best_f - holding_f) * k_indicative AS advantage_pp,
    (best_f - holding_f) * k_indicative - cost_pp AS net_pp
  FROM j
),
r AS (
  SELECT
    m.*,
    100 / (1 + EXP(-1.702 * SAFE_DIVIDE(net_pp, SQRT(2) * noise_pp))) AS chance_pct
  FROM m
)
SELECT
  snapshot_date,
  symbol,
  name,
  market_group,
  currency,
  position_qty,
  position_value,
  ROUND(unrealized_pnl_pp, 1) AS unrealized_pnl_pp,
  horizon,
  horizon_order,
  ROUND(holding_f, 2) AS holding_forecast_pp,
  best_symbol,
  best_name,
  ROUND(best_f, 2) AS best_forecast_pp,
  ROUND(gap_pp, 2) AS gap_pp,
  signal_verdict,
  k_indicative,
  ROUND(advantage_pp, 2) AS advantage_pp,
  ROUND(cost_pp, 2) AS cost_pp,
  ROUND(net_pp, 2) AS net_pp,
  ROUND(chance_pct, 0) AS chance_pct,
  CASE
    WHEN holding_f IS NULL OR best_f IS NULL OR signal_verdict IS NULL OR chance_pct IS NULL THEN 'NO DATA'
    WHEN gap_pp <= 0 THEN 'KEEP'
    WHEN signal_verdict != 'proven' THEN 'KEEP'
    WHEN net_pp <= 0 THEN 'KEEP'
    WHEN chance_pct < min_chance_pct THEN 'KEEP'
    ELSE 'SWITCH CANDIDATE'
  END AS verdict,
  CASE
    WHEN holding_f IS NULL THEN 'the holding has no forecast for this period'
    WHEN best_f IS NULL THEN 'no INVESTABLE candidate in this market today'
    WHEN signal_verdict IS NULL OR chance_pct IS NULL THEN 'no matured records to measure forecasts in this market and period'
    WHEN gap_pp <= 0 THEN 'the holding has the higher forecast'
    WHEN signal_verdict != 'proven' THEN CONCAT('forecasts are not proven to rank names over this period (', signal_verdict, ') - the figures are indicative')
    WHEN net_pp <= 0 THEN 'the advantage does not cover the cost of both trades'
    WHEN chance_pct < min_chance_pct THEN 'the advantage is too uncertain (chance under 60%)'
    ELSE 'proven signal, positive after both costs, chance 60% or more: review'
  END AS reason
FROM r;
