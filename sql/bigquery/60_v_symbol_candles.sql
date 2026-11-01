-- v_symbol_candles: daily candles per symbol, one per trading session.
-- Symbols with Yahoo bars (symbol_bars_daily, scripts/archive_symbol_deep.py:
-- the deep set) use those bars only - two years of real sessions, reloaded
-- every night after the archive - so a nightly snapshot never adds a second
-- copy of a session (Codex review of #884). Every other symbol gets one
-- candle per session from the archived market pages (open, day high, day
-- low, the price at the row's last refresh). The session is the venue's:
-- the local date of the row's Last Updated stamp less the venue's opening
-- time (a refresh before the open still shows the previous session),
-- stepped back over the venue's weekend (Saturday-Sunday, or Friday-Saturday
-- for the Sunday-Thursday markets). The latest refresh of a session is its
-- candle, and a session identical to the one before (a holiday - no new
-- trades) is left out. A listing without a venue row (FX, commodities,
-- funds) counts in UTC from midnight with the Saturday-Sunday weekend.
-- sma_20 / sma_50 / sma_200 average the close over that many candles (NULL
-- until there are that many); change_pct is the move from the previous
-- candle (percent points); volume_avg_20 averages the volume. symbol_label
-- keys the symbol picker. last_close is the symbol's newest close and
-- open/high/low/close_vs_last_pct place every candle against it (percent
-- points; the newest close is 0): the Looker candlestick chart always draws
-- its axis from zero, so candles in prices sit squeezed in a thin band at
-- the top, while these straddle zero and fill the chart.
CREATE OR REPLACE VIEW `{{DS}}.v_symbol_candles`
OPTIONS (description = "TFB Looker v3: daily candles per symbol, one per trading session (two years of Yahoo bars for the deep set, nightly market-page snapshots for every other symbol) with moving averages")
AS
WITH bars AS (
  SELECT symbol, date, open, high, low, close, volume, source
  FROM `{{DS}}.symbol_bars_daily`
),
venue AS (
  -- market_country as v_universe names it -> time zone, the local minute the
  -- continuous session opens, and the Friday-Saturday weekend
  SELECT *
  FROM UNNEST(ARRAY<STRUCT<market_country STRING, tz STRING, open_min INT64, sun_thu BOOL>>[
    ('United States', 'America/New_York', 570, FALSE),
    ('Saudi Arabia', 'Asia/Riyadh', 600, TRUE),
    ('Qatar', 'Asia/Qatar', 570, TRUE),
    ('United Arab Emirates', 'Asia/Dubai', 600, FALSE),
    ('Bahrain', 'Asia/Bahrain', 570, TRUE),
    ('Kuwait', 'Asia/Kuwait', 540, TRUE),
    ('Oman', 'Asia/Muscat', 600, TRUE),
    ('Egypt', 'Africa/Cairo', 600, TRUE),
    ('Hong Kong', 'Asia/Hong_Kong', 570, FALSE),
    ('China', 'Asia/Shanghai', 570, FALSE),
    ('Japan', 'Asia/Tokyo', 540, FALSE),
    ('United Kingdom', 'Europe/London', 480, FALSE),
    ('Ireland', 'Europe/Dublin', 480, FALSE),
    ('Portugal', 'Europe/Lisbon', 480, FALSE),
    ('Germany', 'Europe/Berlin', 540, FALSE),
    ('France', 'Europe/Paris', 540, FALSE),
    ('Netherlands', 'Europe/Amsterdam', 540, FALSE),
    ('Belgium', 'Europe/Brussels', 540, FALSE),
    ('Italy', 'Europe/Rome', 540, FALSE),
    ('Spain', 'Europe/Madrid', 540, FALSE),
    ('Switzerland', 'Europe/Zurich', 540, FALSE),
    ('Sweden', 'Europe/Stockholm', 540, FALSE),
    ('Norway', 'Europe/Oslo', 540, FALSE),
    ('Denmark', 'Europe/Copenhagen', 540, FALSE),
    ('Finland', 'Europe/Helsinki', 600, FALSE),
    ('Austria', 'Europe/Vienna', 540, FALSE),
    ('Greece', 'Europe/Athens', 600, FALSE),
    ('Poland', 'Europe/Warsaw', 540, FALSE),
    ('Czech Republic', 'Europe/Prague', 540, FALSE),
    ('Hungary', 'Europe/Budapest', 540, FALSE),
    ('Turkey', 'Europe/Istanbul', 600, FALSE),
    ('Australia', 'Australia/Sydney', 600, FALSE),
    ('New Zealand', 'Pacific/Auckland', 600, FALSE),
    ('India', 'Asia/Kolkata', 555, FALSE),
    ('South Korea', 'Asia/Seoul', 540, FALSE),
    ('Taiwan', 'Asia/Taipei', 540, FALSE),
    ('Singapore', 'Asia/Singapore', 540, FALSE),
    ('Malaysia', 'Asia/Kuala_Lumpur', 540, FALSE),
    ('Thailand', 'Asia/Bangkok', 600, FALSE),
    ('Indonesia', 'Asia/Jakarta', 540, FALSE),
    ('Vietnam', 'Asia/Ho_Chi_Minh', 540, FALSE),
    ('Philippines', 'Asia/Manila', 570, FALSE),
    ('Canada', 'America/Toronto', 570, FALSE),
    ('Mexico', 'America/Mexico_City', 510, FALSE),
    ('Brazil', 'America/Sao_Paulo', 600, FALSE),
    ('Chile', 'America/Santiago', 570, FALSE),
    ('Argentina', 'America/Argentina/Buenos_Aires', 660, FALSE),
    ('South Africa', 'Africa/Johannesburg', 540, FALSE)
  ])
),
snap0 AS (
  SELECT
    u.symbol, u.open_price AS open, u.day_high AS high, u.day_low AS low, u.current_price AS close,
    SAFE_CAST(u.volume AS INT64) AS volume, 'market_page' AS source,
    COALESCE(u.last_updated_at, u.captured_at) AS quoted_at, u.captured_at, u.source_page,
    DATE(DATETIME_SUB(DATETIME(COALESCE(u.last_updated_at, u.captured_at), COALESCE(v.tz, 'UTC')),
                      INTERVAL COALESCE(v.open_min, 0) MINUTE)) AS local_day,
    COALESCE(v.sun_thu, FALSE) AS sun_thu
  FROM `{{DS}}.v_universe` AS u
  LEFT JOIN venue AS v ON v.market_country = u.market_country
  WHERE u.current_price > 0
    AND NOT EXISTS (SELECT 1 FROM bars AS b WHERE b.symbol = u.symbol)
),
snap1 AS (
  -- DAYOFWEEK: 1 Sunday .. 6 Friday, 7 Saturday
  SELECT
    s.*,
    DATE_SUB(s.local_day, INTERVAL (CASE EXTRACT(DAYOFWEEK FROM s.local_day)
      WHEN 7 THEN IF(s.sun_thu, 2, 1)
      WHEN 1 THEN IF(s.sun_thu, 0, 2)
      WHEN 6 THEN IF(s.sun_thu, 1, 0)
      ELSE 0 END) DAY) AS date
  FROM snap0 AS s
),
snap2 AS (
  SELECT *
  FROM snap1
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY quoted_at DESC, captured_at DESC, source_page) = 1
),
snap AS (
  SELECT symbol, date, open, high, low, close, volume, source
  FROM (
    SELECT
      s.*,
      TO_JSON_STRING(STRUCT(s.open, s.high, s.low, s.close)) AS k,
      LAG(TO_JSON_STRING(STRUCT(s.open, s.high, s.low, s.close))) OVER (PARTITION BY s.symbol ORDER BY s.date) AS prev_k
    FROM snap2 AS s
  )
  WHERE prev_k IS NULL OR k != prev_k
),
merged AS (
  SELECT * FROM bars
  UNION ALL
  SELECT * FROM snap
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
  ROUND(AVG(m.volume) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 0) AS volume_avg_20,
  LAST_VALUE(m.close IGNORE NULLS) OVER wall AS last_close,
  ROUND((SAFE_DIVIDE(m.open, LAST_VALUE(m.close IGNORE NULLS) OVER wall) - 1) * 100, 4) AS open_vs_last_pct,
  ROUND((SAFE_DIVIDE(m.high, LAST_VALUE(m.close IGNORE NULLS) OVER wall) - 1) * 100, 4) AS high_vs_last_pct,
  ROUND((SAFE_DIVIDE(m.low, LAST_VALUE(m.close IGNORE NULLS) OVER wall) - 1) * 100, 4) AS low_vs_last_pct,
  ROUND((SAFE_DIVIDE(m.close, LAST_VALUE(m.close IGNORE NULLS) OVER wall) - 1) * 100, 4) AS close_vs_last_pct
FROM merged AS m
LEFT JOIN lab AS l ON l.symbol = m.symbol
WINDOW w AS (PARTITION BY m.symbol ORDER BY m.date),
  wall AS (PARTITION BY m.symbol ORDER BY m.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);
