-- v_universe: every instrument on the four market pages (Global_Markets,
-- Market_Leaders, Mutual_Funds, Commodities_FX) as archived each night - one
-- row per symbol, page and snapshot. The pages share one 115-column header
-- (core/sheets/schema_registry.py, row 1), so each header is looked up once
-- per page and snapshot (f_col) and every row is read by position (f_at).
-- Units: the registry's pct columns hold fractions (0.041 = 4.1 %) - every
-- *_pct column here is percent points. change_pct, expected_roi_*_pct,
-- intrinsic_upside_pct and target_upside_pct are recomputed from the prices
-- in the same row (the sheet's figure only when a price is missing);
-- expected_roi_*_sheet keep the sheet's fractions for the unit check.
-- data_quality and reliability are 0-100 points; forecast_confidence_pts
-- rebases the registry's "0-1 or 0-100" to points. market_code is the
-- listing suffix (2222.SR -> SR); region groups the listings (US, Europe,
-- China, Japan, Gulf, Asia-Pacific, Americas, Africa - the first five as on
-- the market-series page, so one Looker control can filter both) and puts
-- FX, crypto, futures and indices under FX, Crypto, Commodities, Indices;
-- market_country is the country of the listing (the sheet's country is the
-- company's own).
CREATE OR REPLACE VIEW `{{DS}}.v_universe`
OPTIONS (description = "TFB Looker v2: the four market pages per snapshot - price, valuation, risk, forecasts, scores and the engine's decision for every symbol (percent points)")
AS
WITH raw AS (
  SELECT 'Global_Markets' AS source_page, snapshot_date, captured_at, row_num, cells FROM `{{DS}}.global_markets`
  UNION ALL
  SELECT 'Market_Leaders', snapshot_date, captured_at, row_num, cells FROM `{{DS}}.market_leaders`
  UNION ALL
  SELECT 'Mutual_Funds', snapshot_date, captured_at, row_num, cells FROM `{{DS}}.mutual_funds`
  UNION ALL
  SELECT 'Commodities_FX', snapshot_date, captured_at, row_num, cells FROM `{{DS}}.commodities_fx`
),
hdr AS (
  SELECT
    source_page,
    snapshot_date,
    `{{DS}}.f_col`(h, 'Symbol') AS i_symbol,
    `{{DS}}.f_col`(h, 'Name') AS i_name,
    `{{DS}}.f_col`(h, 'Asset Class') AS i_asset_class,
    `{{DS}}.f_col`(h, 'Exchange') AS i_exchange,
    `{{DS}}.f_col`(h, 'Currency') AS i_currency,
    `{{DS}}.f_col`(h, 'Country') AS i_country,
    `{{DS}}.f_col`(h, 'Sector') AS i_sector,
    `{{DS}}.f_col`(h, 'Industry') AS i_industry,
    `{{DS}}.f_col`(h, 'Current Price') AS i_current_price,
    `{{DS}}.f_col`(h, 'Previous Close') AS i_previous_close,
    `{{DS}}.f_col`(h, 'Open') AS i_open_price,
    `{{DS}}.f_col`(h, 'Day High') AS i_day_high,
    `{{DS}}.f_col`(h, 'Day Low') AS i_day_low,
    `{{DS}}.f_col`(h, '52W High') AS i_high_52w,
    `{{DS}}.f_col`(h, '52W Low') AS i_low_52w,
    `{{DS}}.f_col`(h, 'Price Change') AS i_price_change,
    `{{DS}}.f_col`(h, 'Percent Change') AS i_percent_change_sheet_pct,
    `{{DS}}.f_col`(h, '52W Position %') AS i_position_52w_sheet,
    `{{DS}}.f_col`(h, 'Volume') AS i_volume,
    `{{DS}}.f_col`(h, 'Avg Volume 10D') AS i_avg_volume_10d,
    `{{DS}}.f_col`(h, 'Avg Volume 30D') AS i_avg_volume_30d,
    `{{DS}}.f_col`(h, 'Market Cap') AS i_market_cap,
    `{{DS}}.f_col`(h, 'Float Shares') AS i_float_shares,
    `{{DS}}.f_col`(h, 'Beta (5Y)') AS i_beta_5y,
    `{{DS}}.f_col`(h, 'P/E (TTM)') AS i_pe_ttm,
    `{{DS}}.f_col`(h, 'P/E (Forward)') AS i_pe_forward,
    `{{DS}}.f_col`(h, 'EPS (TTM)') AS i_eps_ttm,
    `{{DS}}.f_col`(h, 'Dividend Yield') AS i_dividend_yield_pct,
    `{{DS}}.f_col`(h, 'Payout Ratio') AS i_payout_ratio_pct,
    `{{DS}}.f_col`(h, 'Revenue (TTM)') AS i_revenue_ttm,
    `{{DS}}.f_col`(h, 'Revenue Growth YoY') AS i_revenue_growth_yoy_pct,
    `{{DS}}.f_col`(h, 'Gross Margin') AS i_gross_margin_pct,
    `{{DS}}.f_col`(h, 'Operating Margin') AS i_operating_margin_pct,
    `{{DS}}.f_col`(h, 'Profit Margin') AS i_profit_margin_pct,
    `{{DS}}.f_col`(h, 'Debt/Equity') AS i_debt_to_equity,
    `{{DS}}.f_col`(h, 'Free Cash Flow (TTM)') AS i_free_cash_flow_ttm,
    `{{DS}}.f_col`(h, 'RSI (14)') AS i_rsi_14,
    `{{DS}}.f_col`(h, 'Volatility 30D') AS i_volatility_30d_pct,
    `{{DS}}.f_col`(h, 'Volatility 90D') AS i_volatility_90d_pct,
    `{{DS}}.f_col`(h, 'Max Drawdown 1Y') AS i_max_drawdown_1y_pct,
    `{{DS}}.f_col`(h, 'VaR 95% (1D)') AS i_var_95_1d_pct,
    `{{DS}}.f_col`(h, 'Sharpe (1Y)') AS i_sharpe_1y,
    `{{DS}}.f_col`(h, 'Risk Score') AS i_risk_score,
    `{{DS}}.f_col`(h, 'Risk Bucket') AS i_risk_bucket,
    `{{DS}}.f_col`(h, 'P/B') AS i_pb,
    `{{DS}}.f_col`(h, 'P/S') AS i_ps,
    `{{DS}}.f_col`(h, 'EV/EBITDA') AS i_ev_ebitda,
    `{{DS}}.f_col`(h, 'PEG') AS i_peg,
    `{{DS}}.f_col`(h, 'Intrinsic Value') AS i_intrinsic_value,
    `{{DS}}.f_col`(h, 'Upside %') AS i_upside_sheet_pct,
    `{{DS}}.f_col`(h, 'Valuation Score') AS i_valuation_score,
    `{{DS}}.f_col`(h, 'Forecast Price 1M') AS i_forecast_price_1m,
    `{{DS}}.f_col`(h, 'Forecast Price 3M') AS i_forecast_price_3m,
    `{{DS}}.f_col`(h, 'Forecast Price 12M') AS i_forecast_price_12m,
    `{{DS}}.f_col`(h, 'Expected ROI 1M') AS i_expected_roi_1m_sheet,
    `{{DS}}.f_col`(h, 'Expected ROI 3M') AS i_expected_roi_3m_sheet,
    `{{DS}}.f_col`(h, 'Expected ROI 12M') AS i_expected_roi_12m_sheet,
    `{{DS}}.f_col`(h, 'Forecast Confidence') AS i_forecast_confidence,
    `{{DS}}.f_col`(h, 'Confidence Score') AS i_confidence_score,
    `{{DS}}.f_col`(h, 'Confidence Bucket') AS i_confidence_bucket,
    `{{DS}}.f_col`(h, 'Value Score') AS i_value_score,
    `{{DS}}.f_col`(h, 'Quality Score') AS i_quality_score,
    `{{DS}}.f_col`(h, 'Momentum Score') AS i_momentum_score,
    `{{DS}}.f_col`(h, 'Growth Score') AS i_growth_score,
    `{{DS}}.f_col`(h, 'Overall Score') AS i_overall_score,
    `{{DS}}.f_col`(h, 'Opportunity Score') AS i_opportunity_score,
    `{{DS}}.f_col`(h, 'Rank (Overall)') AS i_rank_overall,
    `{{DS}}.f_col`(h, 'Analyst Rating') AS i_analyst_rating,
    `{{DS}}.f_col`(h, 'Target Price') AS i_target_price,
    `{{DS}}.f_col`(h, 'Upside/Downside %') AS i_upside_downside_sheet_pct,
    `{{DS}}.f_col`(h, 'Signal') AS i_signal,
    `{{DS}}.f_col`(h, 'Trend 1M') AS i_trend_1m,
    `{{DS}}.f_col`(h, 'Trend 3M') AS i_trend_3m,
    `{{DS}}.f_col`(h, 'Trend 12M') AS i_trend_12m,
    `{{DS}}.f_col`(h, 'ST Signal') AS i_st_signal,
    `{{DS}}.f_col`(h, 'Recommendation') AS i_recommendation,
    `{{DS}}.f_col`(h, 'Recommendation Detail') AS i_recommendation_detail,
    `{{DS}}.f_col`(h, 'Recommendation Reason') AS i_recommendation_reason,
    `{{DS}}.f_col`(h, 'Reco Priority') AS i_reco_priority,
    `{{DS}}.f_col`(h, 'Priority Band') AS i_priority_band,
    `{{DS}}.f_col`(h, 'Recommendation Source') AS i_recommendation_source,
    `{{DS}}.f_col`(h, 'Horizon Days') AS i_horizon_days,
    `{{DS}}.f_col`(h, 'Invest Period Label') AS i_invest_period_label,
    `{{DS}}.f_col`(h, 'Sector-Adj Score') AS i_sector_adj_score,
    `{{DS}}.f_col`(h, 'Conviction Score') AS i_conviction_score,
    `{{DS}}.f_col`(h, 'Top Factors') AS i_top_factors,
    `{{DS}}.f_col`(h, 'Top Risks') AS i_top_risks,
    `{{DS}}.f_col`(h, 'Position Size Hint') AS i_position_size_hint,
    `{{DS}}.f_col`(h, 'Candle Pattern') AS i_candle_pattern,
    `{{DS}}.f_col`(h, 'Candle Signal') AS i_candle_signal,
    `{{DS}}.f_col`(h, 'Candle Strength') AS i_candle_strength,
    `{{DS}}.f_col`(h, 'Candle Confidence') AS i_candle_confidence,
    `{{DS}}.f_col`(h, 'Recent Patterns (5D)') AS i_recent_patterns_5d,
    `{{DS}}.f_col`(h, 'Provider Rating') AS i_provider_rating,
    `{{DS}}.f_col`(h, 'Scoring Reco Source') AS i_scoring_reco_source,
    `{{DS}}.f_col`(h, 'Scoring Schema Version') AS i_scoring_schema_version,
    `{{DS}}.f_col`(h, 'Scoring Errors') AS i_scoring_errors,
    `{{DS}}.f_col`(h, 'Opportunity Source') AS i_opportunity_source,
    `{{DS}}.f_col`(h, 'Overall Score (Raw)') AS i_overall_score_raw,
    `{{DS}}.f_col`(h, 'Overall Penalty Factor') AS i_overall_penalty_factor,
    `{{DS}}.f_col`(h, 'Forecast Source') AS i_forecast_source,
    `{{DS}}.f_col`(h, 'Data Quality Score') AS i_data_quality,
    `{{DS}}.f_col`(h, 'Forecast Reliability Score') AS i_reliability,
    `{{DS}}.f_col`(h, 'Provider/Engine Conflict') AS i_provider_engine_conflict,
    `{{DS}}.f_col`(h, 'Conflict Type') AS i_conflict_type,
    `{{DS}}.f_col`(h, 'Final Decision Basis') AS i_final_decision_basis,
    `{{DS}}.f_col`(h, 'Investability Status') AS i_investability_status,
    `{{DS}}.f_col`(h, 'Final Action') AS i_final_action,
    `{{DS}}.f_col`(h, 'Block Reason') AS i_block_reason,
    `{{DS}}.f_col`(h, 'Data Provider') AS i_data_provider,
    `{{DS}}.f_col`(h, 'Provider Secondary') AS i_provider_secondary,
    `{{DS}}.f_col`(h, 'Last Updated (UTC)') AS i_last_updated_utc,
    `{{DS}}.f_col`(h, 'Last Updated (Riyadh)') AS i_last_updated_riyadh,
    `{{DS}}.f_col`(h, 'Row Source') AS i_row_source,
    `{{DS}}.f_col`(h, 'Warnings') AS i_warnings
  FROM (SELECT source_page, snapshot_date, cells AS h FROM raw WHERE row_num = 1)
),
typed AS (
  SELECT
    s.source_page,
    s.snapshot_date,
    s.captured_at,
    s.row_num,
    `{{DS}}.f_at`(s.cells, i_symbol) AS symbol,
    `{{DS}}.f_at`(s.cells, i_name) AS name,
    UPPER(`{{DS}}.f_at`(s.cells, i_asset_class)) AS asset_class,
    `{{DS}}.f_at`(s.cells, i_exchange) AS exchange,
    `{{DS}}.f_at`(s.cells, i_currency) AS currency,
    `{{DS}}.f_at`(s.cells, i_country) AS country,
    `{{DS}}.f_at`(s.cells, i_sector) AS sector,
    `{{DS}}.f_at`(s.cells, i_industry) AS industry,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_current_price)) AS current_price,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_previous_close)) AS previous_close,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_open_price)) AS open_price,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_day_high)) AS day_high,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_day_low)) AS day_low,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_high_52w)) AS high_52w,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_low_52w)) AS low_52w,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_price_change)) AS price_change,
    ROUND(`{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_percent_change_sheet_pct)) * 100, 4) AS percent_change_sheet_pct,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_position_52w_sheet)) AS position_52w_sheet,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_volume)) AS volume,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_avg_volume_10d)) AS avg_volume_10d,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_avg_volume_30d)) AS avg_volume_30d,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_market_cap)) AS market_cap,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_float_shares)) AS float_shares,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_beta_5y)) AS beta_5y,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_pe_ttm)) AS pe_ttm,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_pe_forward)) AS pe_forward,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_eps_ttm)) AS eps_ttm,
    ROUND(`{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_dividend_yield_pct)) * 100, 4) AS dividend_yield_pct,
    ROUND(`{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_payout_ratio_pct)) * 100, 4) AS payout_ratio_pct,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_revenue_ttm)) AS revenue_ttm,
    ROUND(`{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_revenue_growth_yoy_pct)) * 100, 4) AS revenue_growth_yoy_pct,
    ROUND(`{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_gross_margin_pct)) * 100, 4) AS gross_margin_pct,
    ROUND(`{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_operating_margin_pct)) * 100, 4) AS operating_margin_pct,
    ROUND(`{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_profit_margin_pct)) * 100, 4) AS profit_margin_pct,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_debt_to_equity)) AS debt_to_equity,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_free_cash_flow_ttm)) AS free_cash_flow_ttm,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_rsi_14)) AS rsi_14,
    ROUND(`{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_volatility_30d_pct)) * 100, 4) AS volatility_30d_pct,
    ROUND(`{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_volatility_90d_pct)) * 100, 4) AS volatility_90d_pct,
    ROUND(`{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_max_drawdown_1y_pct)) * 100, 4) AS max_drawdown_1y_pct,
    ROUND(`{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_var_95_1d_pct)) * 100, 4) AS var_95_1d_pct,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_sharpe_1y)) AS sharpe_1y,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_risk_score)) AS risk_score,
    `{{DS}}.f_at`(s.cells, i_risk_bucket) AS risk_bucket,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_pb)) AS pb,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_ps)) AS ps,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_ev_ebitda)) AS ev_ebitda,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_peg)) AS peg,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_intrinsic_value)) AS intrinsic_value,
    ROUND(`{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_upside_sheet_pct)) * 100, 4) AS upside_sheet_pct,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_valuation_score)) AS valuation_score,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_forecast_price_1m)) AS forecast_price_1m,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_forecast_price_3m)) AS forecast_price_3m,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_forecast_price_12m)) AS forecast_price_12m,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_expected_roi_1m_sheet)) AS expected_roi_1m_sheet,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_expected_roi_3m_sheet)) AS expected_roi_3m_sheet,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_expected_roi_12m_sheet)) AS expected_roi_12m_sheet,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_forecast_confidence)) AS forecast_confidence,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_confidence_score)) AS confidence_score,
    `{{DS}}.f_at`(s.cells, i_confidence_bucket) AS confidence_bucket,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_value_score)) AS value_score,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_quality_score)) AS quality_score,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_momentum_score)) AS momentum_score,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_growth_score)) AS growth_score,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_overall_score)) AS overall_score,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_opportunity_score)) AS opportunity_score,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_rank_overall)) AS rank_overall,
    `{{DS}}.f_at`(s.cells, i_analyst_rating) AS analyst_rating,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_target_price)) AS target_price,
    ROUND(`{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_upside_downside_sheet_pct)) * 100, 4) AS upside_downside_sheet_pct,
    `{{DS}}.f_at`(s.cells, i_signal) AS signal,
    `{{DS}}.f_at`(s.cells, i_trend_1m) AS trend_1m,
    `{{DS}}.f_at`(s.cells, i_trend_3m) AS trend_3m,
    `{{DS}}.f_at`(s.cells, i_trend_12m) AS trend_12m,
    `{{DS}}.f_at`(s.cells, i_st_signal) AS st_signal,
    `{{DS}}.f_at`(s.cells, i_recommendation) AS recommendation,
    `{{DS}}.f_at`(s.cells, i_recommendation_detail) AS recommendation_detail,
    `{{DS}}.f_at`(s.cells, i_recommendation_reason) AS recommendation_reason,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_reco_priority)) AS reco_priority,
    `{{DS}}.f_at`(s.cells, i_priority_band) AS priority_band,
    `{{DS}}.f_at`(s.cells, i_recommendation_source) AS recommendation_source,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_horizon_days)) AS horizon_days,
    `{{DS}}.f_at`(s.cells, i_invest_period_label) AS invest_period_label,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_sector_adj_score)) AS sector_adj_score,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_conviction_score)) AS conviction_score,
    `{{DS}}.f_at`(s.cells, i_top_factors) AS top_factors,
    `{{DS}}.f_at`(s.cells, i_top_risks) AS top_risks,
    `{{DS}}.f_at`(s.cells, i_position_size_hint) AS position_size_hint,
    `{{DS}}.f_at`(s.cells, i_candle_pattern) AS candle_pattern,
    `{{DS}}.f_at`(s.cells, i_candle_signal) AS candle_signal,
    `{{DS}}.f_at`(s.cells, i_candle_strength) AS candle_strength,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_candle_confidence)) AS candle_confidence,
    `{{DS}}.f_at`(s.cells, i_recent_patterns_5d) AS recent_patterns_5d,
    `{{DS}}.f_at`(s.cells, i_provider_rating) AS provider_rating,
    `{{DS}}.f_at`(s.cells, i_scoring_reco_source) AS scoring_reco_source,
    `{{DS}}.f_at`(s.cells, i_scoring_schema_version) AS scoring_schema_version,
    `{{DS}}.f_at`(s.cells, i_scoring_errors) AS scoring_errors,
    `{{DS}}.f_at`(s.cells, i_opportunity_source) AS opportunity_source,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_overall_score_raw)) AS overall_score_raw,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_overall_penalty_factor)) AS overall_penalty_factor,
    `{{DS}}.f_at`(s.cells, i_forecast_source) AS forecast_source,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_data_quality)) AS data_quality,
    `{{DS}}.f_num`(`{{DS}}.f_at`(s.cells, i_reliability)) AS reliability,
    `{{DS}}.f_at`(s.cells, i_provider_engine_conflict) AS provider_engine_conflict,
    `{{DS}}.f_at`(s.cells, i_conflict_type) AS conflict_type,
    `{{DS}}.f_at`(s.cells, i_final_decision_basis) AS final_decision_basis,
    `{{DS}}.f_at`(s.cells, i_investability_status) AS investability_status,
    `{{DS}}.f_at`(s.cells, i_final_action) AS final_action,
    `{{DS}}.f_at`(s.cells, i_block_reason) AS block_reason,
    `{{DS}}.f_at`(s.cells, i_data_provider) AS data_provider,
    `{{DS}}.f_at`(s.cells, i_provider_secondary) AS provider_secondary,
    `{{DS}}.f_ts_utc`(`{{DS}}.f_at`(s.cells, i_last_updated_utc)) AS last_updated_utc,
    `{{DS}}.f_ts`(`{{DS}}.f_at`(s.cells, i_last_updated_riyadh)) AS last_updated_riyadh,
    `{{DS}}.f_at`(s.cells, i_row_source) AS row_source,
    `{{DS}}.f_at`(s.cells, i_warnings) AS warnings
  FROM raw AS s
  JOIN hdr USING (source_page, snapshot_date)
  WHERE s.row_num > 1
),
keyed AS (
  SELECT
    t.*,
    REGEXP_EXTRACT(UPPER(t.symbol), r'\.([A-Z]{1,5})$') AS market_code
  FROM typed AS t
  WHERE t.symbol IS NOT NULL
)
SELECT
  k.*,
  CASE
    WHEN k.asset_class = 'FX' OR ENDS_WITH(UPPER(k.symbol), '=X') THEN 'FX'
    WHEN k.asset_class IN ('CRYPTOCURRENCY', 'CRYPTO') THEN 'Crypto'
    WHEN k.asset_class IN ('FUTURE', 'FUTURES', 'COMMODITY') OR ENDS_WITH(UPPER(k.symbol), '=F') THEN 'Commodities'
    WHEN k.asset_class = 'INDEX' OR STARTS_WITH(k.symbol, '^') THEN 'Indices'
    WHEN k.market_code IN ('US') THEN 'US'
    WHEN k.market_code IN ('SR', 'QA', 'AE', 'AD', 'DU', 'BH', 'KW', 'OM') THEN 'Gulf'
    WHEN k.market_code IN ('HK', 'SS', 'SZ') THEN 'China'
    WHEN k.market_code IN ('T') THEN 'Japan'
    WHEN k.market_code IN ('L', 'DE', 'F', 'PA', 'AS', 'BR', 'LS', 'MI', 'MC', 'SW', 'ST', 'OL', 'CO', 'HE', 'IR', 'VI', 'AT', 'WA', 'PR', 'BD', 'IS') THEN 'Europe'
    WHEN k.market_code IN ('AX', 'NZ', 'NS', 'BO', 'KS', 'KQ', 'TW', 'TWO', 'SI', 'KL', 'BK', 'JK', 'VN', 'PS') THEN 'Asia-Pacific'
    WHEN k.market_code IN ('TO', 'V', 'NE', 'MX', 'SA', 'SN', 'BA') THEN 'Americas'
    WHEN k.market_code IN ('JO', 'CA') THEN 'Africa'
    ELSE 'Other'
  END AS region,
  CASE k.market_code
    WHEN 'US' THEN 'United States'
    WHEN 'SR' THEN 'Saudi Arabia'
    WHEN 'QA' THEN 'Qatar'
    WHEN 'AE' THEN 'United Arab Emirates'
    WHEN 'AD' THEN 'United Arab Emirates'
    WHEN 'DU' THEN 'United Arab Emirates'
    WHEN 'BH' THEN 'Bahrain'
    WHEN 'KW' THEN 'Kuwait'
    WHEN 'OM' THEN 'Oman'
    WHEN 'HK' THEN 'Hong Kong'
    WHEN 'SS' THEN 'China'
    WHEN 'SZ' THEN 'China'
    WHEN 'T' THEN 'Japan'
    WHEN 'L' THEN 'United Kingdom'
    WHEN 'DE' THEN 'Germany'
    WHEN 'F' THEN 'Germany'
    WHEN 'PA' THEN 'France'
    WHEN 'AS' THEN 'Netherlands'
    WHEN 'BR' THEN 'Belgium'
    WHEN 'LS' THEN 'Portugal'
    WHEN 'MI' THEN 'Italy'
    WHEN 'MC' THEN 'Spain'
    WHEN 'SW' THEN 'Switzerland'
    WHEN 'ST' THEN 'Sweden'
    WHEN 'OL' THEN 'Norway'
    WHEN 'CO' THEN 'Denmark'
    WHEN 'HE' THEN 'Finland'
    WHEN 'IR' THEN 'Ireland'
    WHEN 'VI' THEN 'Austria'
    WHEN 'AT' THEN 'Greece'
    WHEN 'WA' THEN 'Poland'
    WHEN 'PR' THEN 'Czech Republic'
    WHEN 'BD' THEN 'Hungary'
    WHEN 'IS' THEN 'Turkey'
    WHEN 'AX' THEN 'Australia'
    WHEN 'NZ' THEN 'New Zealand'
    WHEN 'NS' THEN 'India'
    WHEN 'BO' THEN 'India'
    WHEN 'KS' THEN 'South Korea'
    WHEN 'KQ' THEN 'South Korea'
    WHEN 'TW' THEN 'Taiwan'
    WHEN 'TWO' THEN 'Taiwan'
    WHEN 'SI' THEN 'Singapore'
    WHEN 'KL' THEN 'Malaysia'
    WHEN 'BK' THEN 'Thailand'
    WHEN 'JK' THEN 'Indonesia'
    WHEN 'VN' THEN 'Vietnam'
    WHEN 'PS' THEN 'Philippines'
    WHEN 'TO' THEN 'Canada'
    WHEN 'V' THEN 'Canada'
    WHEN 'NE' THEN 'Canada'
    WHEN 'MX' THEN 'Mexico'
    WHEN 'SA' THEN 'Brazil'
    WHEN 'SN' THEN 'Chile'
    WHEN 'BA' THEN 'Argentina'
    WHEN 'JO' THEN 'South Africa'
    WHEN 'CA' THEN 'Egypt'
  END AS market_country,
  REGEXP_REPLACE(UPPER(k.symbol), r'\.[A-Z]{1,5}$', '') AS symbol_base,
  IF(k.name IS NULL, k.symbol, CONCAT(k.symbol, ' · ', k.name)) AS symbol_label,
  COALESCE(ROUND((SAFE_DIVIDE(k.current_price, k.previous_close) - 1) * 100, 4), k.percent_change_sheet_pct) AS change_pct,
  ROUND(SAFE_DIVIDE(k.current_price - k.low_52w, k.high_52w - k.low_52w) * 100, 2) AS position_52w_pct,
  ROUND((SAFE_DIVIDE(k.current_price, k.high_52w) - 1) * 100, 4) AS from_52w_high_pct,
  COALESCE(ROUND((SAFE_DIVIDE(k.forecast_price_1m, k.current_price) - 1) * 100, 4), ROUND(k.expected_roi_1m_sheet * 100, 4)) AS expected_roi_1m_pct,
  COALESCE(ROUND((SAFE_DIVIDE(k.forecast_price_3m, k.current_price) - 1) * 100, 4), ROUND(k.expected_roi_3m_sheet * 100, 4)) AS expected_roi_3m_pct,
  COALESCE(ROUND((SAFE_DIVIDE(k.forecast_price_12m, k.current_price) - 1) * 100, 4), ROUND(k.expected_roi_12m_sheet * 100, 4)) AS expected_roi_12m_pct,
  COALESCE(ROUND((SAFE_DIVIDE(k.intrinsic_value, k.current_price) - 1) * 100, 4), k.upside_sheet_pct) AS intrinsic_upside_pct,
  COALESCE(ROUND((SAFE_DIVIDE(k.target_price, k.current_price) - 1) * 100, 4), k.upside_downside_sheet_pct) AS target_upside_pct,
  CASE WHEN k.forecast_confidence IS NULL THEN NULL
       WHEN k.forecast_confidence <= 1 THEN ROUND(k.forecast_confidence * 100, 2)
       ELSE k.forecast_confidence END AS forecast_confidence_pts,
  COALESCE(k.last_updated_utc, k.last_updated_riyadh) AS last_updated_at,
  ROUND(TIMESTAMP_DIFF(k.captured_at, COALESCE(k.last_updated_utc, k.last_updated_riyadh), MINUTE) / 60, 1) AS data_age_hours
FROM keyed AS k;
