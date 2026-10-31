-- v_symbol_candles: daily candles per symbol - two years from the Yahoo chart
-- API for the deep set (symbol_bars_daily, scripts/archive_symbol_deep.py)
-- and, for every market-page symbol, one candle per archived night (open,
-- day high, day low and the price at capture) on the days the deep set has
-- no bar. A nightly copy identical to the night before (a weekend or a
-- holiday - no new session) is left out. sma_20 / sma_50 / sma_200 average
-- the close over that many candles (NULL until there are that many);
-- change_pct is the move from the previous candle (percent points);
-- volume_avg_20 averages the volume. symbol_label keys the symbol picker.
CREATE OR REPLACE VIEW `{{DS}}.v_symbol_candles`
OPTIONS (description = "TFB Looker v3: daily candles per symbol (two years for the deep set, nightly snapshots for every symbol) with moving averages")
AS
WITH bars AS (
  SELECT symbol, date, open, high, low, close, volume, source
  FROM `{{DS}}.symbol_bars_daily`
),
snap0 AS (
  SELECT
    u.symbol, u.snapshot_date AS date, u.open_price AS open, u.day_high AS high,
    u.day_low AS low, u.current_price AS close, SAFE_CAST(u.volume AS INT64) AS volume,
    'market_page' AS source
  FROM `{{DS}}.v_universe` AS u
  WHERE u.current_price > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY u.symbol, u.snapshot_date ORDER BY u.captured_at DESC, u.source_page) = 1
),
snap AS (
  SELECT symbol, date, open, high, low, close, volume, source
  FROM (
    SELECT
      s.*,
      TO_JSON_STRING(STRUCT(s.open, s.high, s.low, s.close)) AS k,
      LAG(TO_JSON_STRING(STRUCT(s.open, s.high, s.low, s.close))) OVER (PARTITION BY s.symbol ORDER BY s.date) AS prev_k
    FROM snap0 AS s
  )
  WHERE prev_k IS NULL OR k != prev_k
),
merged AS (
  SELECT * FROM bars
  UNION ALL
  SELECT s.* FROM snap AS s
  WHERE NOT EXISTS (SELECT 1 FROM bars AS b WHERE b.symbol = s.symbol AND b.date = s.date)
),
lab AS (
  SELECT symbol, symbol_label, name, currency
  FROM `{{DS}}.v_universe_latest`
)
SELECT
  m.symbol,
  COALESCE(l.symbol_label, m.symbol) AS symbol_label,
  l.name,
  l.currency,
  m.date,
  m.open,
  m.high,
  m.low,
  m.close,
  m.volume,
  m.source,
  ROUND((SAFE_DIVIDE(m.close, LAG(m.close) OVER w) - 1) * 100, 4) AS change_pct,
  IF(COUNT(m.close) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) = 20,
     ROUND(AVG(m.close) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 6), NULL) AS sma_20,
  IF(COUNT(m.close) OVER (w ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) = 50,
     ROUND(AVG(m.close) OVER (w ROWS BETWEEN 49 PRECEDING AND CURRENT ROW), 6), NULL) AS sma_50,
  IF(COUNT(m.close) OVER (w ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) = 200,
     ROUND(AVG(m.close) OVER (w ROWS BETWEEN 199 PRECEDING AND CURRENT ROW), 6), NULL) AS sma_200,
  ROUND(AVG(m.volume) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 0) AS volume_avg_20
FROM merged AS m
LEFT JOIN lab AS l ON l.symbol = m.symbol
WINDOW w AS (PARTITION BY m.symbol ORDER BY m.date);
