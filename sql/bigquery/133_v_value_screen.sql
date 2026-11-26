-- v_value_screen: a shadow list, recorded and scored but never acted on - the US shares that pass a plain value
-- screen each archived night, beside the label the engine gave them that night.
-- WHY (owner, 26 Nov 2026 01:22 Riyadh, in Arabic: "there is cash and the market is falling, so there are big
-- chances to improve the gains ... but that is not used ... we need to look with the eye of an expert in valuing
-- shares and the eye of a wise investor"; 01:37: "please do the required build now"). Read that night on the
-- Global_Markets page: 1 BUY among 6,602 names, the BUY family 116 on 4 Nov and 0 on 24 Nov - while 57 US names
-- passed the screen below, labelled HOLD (39), REDUCE (15) and SELL (3). The engine's labels follow momentum and the
-- market gate; a list that follows price against earnings is logged beside them and measured, not switched on
-- (owner's ground rules, 21 Nov 2026).
-- THE SCREEN (fixed on 26 Nov 2026, before reading any outcome; every limit from the row of the night itself):
--   a US equity on the Global_Markets or Market_Leaders page with a price (one row per symbol and night, the last
--   capture); market cap 2 bn or more; forward P/E above 0 and at most 15; free cash flow above 0; quality score 65
--   or more; analyst target 20 % or more above the price; in the lower 35 % of its 52-week range; data quality 80
--   or more and forecast reliability 70 or more.
-- COLUMNS: snapshot_date, symbol, name, sector, price, market_cap_bn, pe_forward, free_cash_flow_m, quality_score,
-- analyst_upside_pct, position_52w_pct, overall_score, engine_label, engine_family (buy / hold / sell / other),
-- engine_status, period ('history, rule fixed before reading it' before 26 Nov 2026, then 'forward from 2026-11-26').
-- A screen is not a recommendation: analyst targets lag a falling price, and a low place in the range is where
-- failing companies sit too.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_value_screen`
OPTIONS (description = "TFB shadow list (views 1.30.0): the US shares passing a plain value screen each archived night (size, forward P/E, free cash flow, quality, analyst upside, low in the 52-week range), beside the engine's own label that night")
AS
WITH day AS (
  SELECT
    snapshot_date, symbol, name, sector, current_price, market_cap, pe_forward, free_cash_flow_ttm, quality_score,
    target_upside_pct, position_52w_pct, data_quality, reliability, overall_score,
    UPPER(REPLACE(IFNULL(CAST(recommendation AS STRING), ''), ' ', '_')) AS reco,
    UPPER(IFNULL(CAST(investability_status AS STRING), '')) AS status
  FROM `{{DS}}.v_universe`
  WHERE source_page IN ('Global_Markets', 'Market_Leaders')
    AND ENDS_WITH(symbol, '.US')
    AND UPPER(CAST(asset_class AS STRING)) = 'EQUITY'
    AND current_price > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, snapshot_date ORDER BY captured_at DESC, source_page) = 1
)
SELECT
  snapshot_date, symbol, name, sector,
  ROUND(current_price, 4) AS price,
  ROUND(market_cap / 1e9, 2) AS market_cap_bn,
  ROUND(pe_forward, 2) AS pe_forward,
  ROUND(free_cash_flow_ttm / 1e6, 1) AS free_cash_flow_m,
  ROUND(quality_score, 1) AS quality_score,
  ROUND(target_upside_pct, 1) AS analyst_upside_pct,
  ROUND(position_52w_pct, 1) AS position_52w_pct,
  ROUND(overall_score, 1) AS overall_score,
  reco AS engine_label,
  CASE
    WHEN reco IN ('STRONG_BUY', 'BUY', 'ACCUMULATE') THEN 'buy'
    WHEN reco IN ('REDUCE', 'SELL', 'STRONG_SELL', 'AVOID') THEN 'sell'
    WHEN reco = 'HOLD' THEN 'hold'
    ELSE 'other'
  END AS engine_family,
  status AS engine_status,
  CASE WHEN snapshot_date < DATE '2026-11-26' THEN 'history, rule fixed before reading it' ELSE 'forward from 2026-11-26' END AS period
FROM day
WHERE market_cap >= 2e9
  AND pe_forward > 0 AND pe_forward <= 15
  AND free_cash_flow_ttm > 0
  AND quality_score >= 65
  AND target_upside_pct >= 20
  AND position_52w_pct <= 35
  AND data_quality >= 80
  AND reliability >= 70;
