-- v_my_portfolio: My_Portfolio holdings as archived each night (one row per
-- holding and snapshot). Money columns are in the holding's currency;
-- Actual Weight % is percent points; the sheet's Expected ROI columns are
-- fractions (0.041 = 4.1 %) and stay as *_sheet, while the *_pp columns are
-- recomputed from the forecast price and the current price (unit-free).
CREATE OR REPLACE VIEW `{{DS}}.v_my_portfolio`
OPTIONS (description = "TFB Looker v1: nightly My_Portfolio holdings per snapshot")
AS
WITH snaps AS (
  SELECT snapshot_date, captured_at, row_num, cells
  FROM `{{DS}}.my_portfolio`
),
hdr AS (
  SELECT snapshot_date, cells AS h
  FROM snaps
  WHERE row_num = 1
)
SELECT
  s.snapshot_date,
  s.captured_at,
  `{{DS}}.f_cell`(h, s.cells, 'Symbol') AS symbol,
  `{{DS}}.f_cell`(h, s.cells, 'Name') AS name,
  `{{DS}}.f_cell`(h, s.cells, 'Asset Class') AS asset_class,
  `{{DS}}.f_cell`(h, s.cells, 'Exchange') AS exchange,
  `{{DS}}.f_cell`(h, s.cells, 'Country') AS country,
  `{{DS}}.f_cell`(h, s.cells, 'Currency') AS currency,
  `{{DS}}.f_cell`(h, s.cells, 'Industry') AS industry,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Position Qty')) AS position_qty,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Avg Cost')) AS avg_cost,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Current Price')) AS current_price,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Position Cost')) AS position_cost,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Position Value')) AS position_value,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Actual Weight %')) AS actual_weight_sheet,
  SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(`{{DS}}.f_cell`(h, s.cells, 'Buy Date'), 1, 10)) AS buy_date,
  `{{DS}}.f_cell`(h, s.cells, 'Recommendation') AS recommendation,
  `{{DS}}.f_cell`(h, s.cells, 'Final Action') AS final_action,
  `{{DS}}.f_cell`(h, s.cells, 'Investor Decision') AS investor_decision,
  `{{DS}}.f_cell`(h, s.cells, 'Rebalance Action') AS rebalance_action,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Overall Score')) AS overall_score,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Expected ROI 1M')) AS expected_roi_1m_sheet,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Expected ROI 3M')) AS expected_roi_3m_sheet,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Expected ROI 12M')) AS expected_roi_12m_sheet,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Forecast Price 1M')) AS forecast_price_1m,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Forecast Price 3M')) AS forecast_price_3m,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Forecast Price 12M')) AS forecast_price_12m,
  `{{DS}}.f_ts`(`{{DS}}.f_cell`(h, s.cells, 'Last Updated (Riyadh)')) AS last_updated_at,
  ROUND((SAFE_DIVIDE(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Current Price')),
                     `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Avg Cost'))) - 1) * 100, 4) AS unrealized_pnl_pp,
  ROUND((SAFE_DIVIDE(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Forecast Price 1M')),
                     `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Current Price'))) - 1) * 100, 4) AS expected_roi_1m_pp,
  ROUND((SAFE_DIVIDE(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Forecast Price 3M')),
                     `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Current Price'))) - 1) * 100, 4) AS expected_roi_3m_pp,
  ROUND((SAFE_DIVIDE(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Forecast Price 12M')),
                     `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Current Price'))) - 1) * 100, 4) AS expected_roi_12m_pp
FROM snaps AS s
JOIN hdr USING (snapshot_date)
WHERE s.row_num > 1
  AND `{{DS}}.f_cell`(h, s.cells, 'Symbol') IS NOT NULL;
