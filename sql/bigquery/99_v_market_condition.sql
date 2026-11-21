-- v_market_condition: the condition of every market at each of its closes,
-- with the same five labels and the same tests as v_market_regime, the limits
-- scaled to each market's own normal movement. WHY (owner, 21 Nov 2026 11:03
-- Riyadh, the build list after the voice call): "A condition reading for every
-- market, with the same tests as v_market_regime (1-, 5- and 20-day returns,
-- distance from the 50-day average, 20-day volatility, drawdown from the high;
-- labels calm / weak / stress / drawdown / crash_day)" - "Everything starts in
-- observe mode: shown and logged, but it controls no buys." The US had been
-- weak or worse on every session since 30 Oct and nothing said whether any
-- other market, or any other kind of asset, was in a better state.
-- THE MARKETS (market_id: the reading -> the fund a buy would use):
--   us        SPY and the ^VIX, price closes, exactly as v_market_regime -> SPY
--   sa        Tadawul All Share ^TASI.SR                               -> KSA
--   eu        STOXX Europe 600 ^STOXX                                  -> VGK
--   jp        Nikkei 225 ^N225                                         -> EWJ
--   cn        Hang Seng ^HSI                                           -> MCHI
--   gold      the gold price, GC=F (front-month future)                -> GLD
--   oil       Brent, BZ=F (front-month future)              -> none: reading only
--   bonds     IEF (US Treasuries 7-10 years)                           -> IEF
--   sukuk     SPSK (global sukuk)                                      -> SPSK
--   reits_us  VNQ (US REITs)                                           -> VNQ
--   reits_sa  the REITs on Tadawul (codes 4330-4350 in symbol_bars_daily):
--             an equal-weight index of their daily total returns; a day
--             needs 10 of them, and a one-day change beyond Tadawul's 10 %
--             limit is a data fault, left out              -> the REITs themselves
--   A close is the dividend-adjusted close where the series has one (a fund
--   that pays out would otherwise drift under its own price-only high) and
--   the price close for indices and futures; the US keeps SPY's price close.
-- THE LIMITS: k = the market's normal yearly volatility (the standard
--   deviation of its last 250 daily changes, annualised, at least 120 of
--   them) / 16, the S&P 500's normal level the US limits were set on; the US
--   keeps k = 1. Every figure of a row comes from that close and the closes
--   before it (no look-ahead).
-- COLUMNS: market_id, market, market_order, reading_symbol, reading_name,
--   fund_symbol (NULL = reading only), buyable, date, close,
--   r1_pct / r5_pct / r20_pct / r60_pct   change over 1 / 5 / 20 / 60 closes
--   vs_50d_pct        close against its 50-close average (NULL before 50)
--   vol20_pct         annualised deviation of the last 20 daily changes
--   dd_from_high_pct  distance from the 250-close high
--   normal_vol_pct    the same over the last 250 changes (NULL under 120)
--   vix_close         the US only
--   k, limit_r1_pct, limit_r5_pct, limit_dd_pct, limit_vol20_pct
--   label             first match wins (a figure that is not known never
--                     matches; NULL when k is not known - the US always reads):
--     crash_day   r1_pct <= limit_r1_pct (-2 k)
--     drawdown    r5_pct <= limit_r5_pct (-3 k) or dd_from_high_pct <= limit_dd_pct (-8 k)
--     stress      vol20_pct >= limit_vol20_pct (25 k); the US also vix_close >= 25
--     weak        vs_50d_pct < 0
--     calm        everything else
--   waiting_for       every test the close fails, with its limit, joined by
--                     ' | ' (ARRAY_TO_STRING leaves out the tests passed; '' = calm)
--   trend_score       the mean of the 20- and 60-close changes, each in units
--                     of the market's normal move over that many closes
--                     (normal_vol_pct x SQRT(n / 252)): v_market_rank's order
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it. Observe only: the Daily Plan keeps
-- its US rule until the owner approves a change after the 10-session review.
CREATE OR REPLACE VIEW `{{DS}}.v_market_condition`
OPTIONS (description = "TFB markets v1: one row per market and close - the five v_market_regime labels (crash_day / drawdown / stress / weak / calm) for the US, Saudi Arabia, Europe, Japan, China / Hong Kong, gold, oil, bonds, sukuk, US REITs and Saudi REITs, with the limits scaled to each market's normal volatility (the US unchanged), what each is waiting for, and a trend score. Observe only.")
AS
WITH mk AS (
  SELECT *
  FROM UNNEST([
    STRUCT(1 AS market_order, 'us' AS market_id, 'United States' AS market, 'SPY' AS reading_symbol,
           'S&P 500 fund (SPY) and the VIX' AS reading_name, 'SPY' AS fund_symbol, TRUE AS buyable),
    STRUCT(2, 'sa', 'Saudi Arabia', '^TASI.SR', 'Tadawul All Share (TASI)', 'KSA', TRUE),
    STRUCT(3, 'eu', 'Europe', '^STOXX', 'STOXX Europe 600', 'VGK', TRUE),
    STRUCT(4, 'jp', 'Japan', '^N225', 'Nikkei 225', 'EWJ', TRUE),
    STRUCT(5, 'cn', 'China / Hong Kong', '^HSI', 'Hang Seng', 'MCHI', TRUE),
    STRUCT(6, 'gold', 'Gold', 'GC=F', 'Gold price (front-month future)', 'GLD', TRUE),
    STRUCT(7, 'oil', 'Oil', 'BZ=F', 'Brent crude (front-month future)', CAST(NULL AS STRING), FALSE),
    STRUCT(8, 'bonds', 'Bonds', 'IEF', 'US Treasuries 7-10 years (IEF)', 'IEF', TRUE),
    STRUCT(9, 'sukuk', 'Sukuk', 'SPSK', 'Global sukuk (SPSK)', 'SPSK', TRUE),
    STRUCT(10, 'reits_us', 'US REITs', 'VNQ', 'US real estate (VNQ)', 'VNQ', TRUE),
    STRUCT(11, 'reits_sa', 'Saudi REITs', 'SA_REITS', 'REITs on Tadawul (equal-weight index)', 'SA_REITS', TRUE)
  ])
),
reit AS (
  SELECT symbol, date, COALESCE(adj_close, close) AS px
  FROM `{{DS}}.symbol_bars_daily`
  WHERE REGEXP_CONTAINS(symbol, r'^43(3[0-9]|4[0-9]|50)\.SR$') AND close > 0
),
reit_chg AS (
  SELECT date, SAFE_DIVIDE(px, LAG(px) OVER (PARTITION BY symbol ORDER BY date)) - 1 AS chg
  FROM reit
),
reit_day AS (
  SELECT date, AVG(chg) AS chg
  FROM reit_chg
  WHERE chg IS NOT NULL AND ABS(chg) <= 0.105
  GROUP BY date
  HAVING COUNT(*) >= 10
),
reit_index AS (
  SELECT date, 100 * EXP(SUM(LN(1 + chg)) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))
    AS close
  FROM reit_day
),
px AS (
  SELECT mk.market_id, s.date, IF(mk.market_id = 'us', s.close, COALESCE(s.adj_close, s.close)) AS close
  FROM `{{DS}}.market_series_daily` AS s
  JOIN mk ON mk.reading_symbol = s.symbol
  WHERE s.close > 0
  UNION ALL
  SELECT 'reits_sa', date, close
  FROM reit_index
),
vix AS (
  SELECT date, close AS vix_close
  FROM `{{DS}}.market_series_daily`
  WHERE symbol = '^VIX' AND close IS NOT NULL
),
lagged AS (
  SELECT
    p.market_id,
    p.date,
    p.close,
    LAG(p.close, 1) OVER (PARTITION BY p.market_id ORDER BY p.date) AS c1,
    LAG(p.close, 5) OVER (PARTITION BY p.market_id ORDER BY p.date) AS c5,
    LAG(p.close, 20) OVER (PARTITION BY p.market_id ORDER BY p.date) AS c20,
    LAG(p.close, 60) OVER (PARTITION BY p.market_id ORDER BY p.date) AS c60
  FROM px AS p
),
w AS (
  SELECT
    l.*,
    COUNT(*) OVER (PARTITION BY l.market_id ORDER BY l.date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS n50,
    AVG(l.close) OVER (PARTITION BY l.market_id ORDER BY l.date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50,
    MAX(l.close) OVER (PARTITION BY l.market_id ORDER BY l.date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW)
      AS high250,
    STDDEV_SAMP(SAFE_DIVIDE(l.close, l.c1) - 1)
      OVER (PARTITION BY l.market_id ORDER BY l.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sd20,
    COUNT(*) OVER (PARTITION BY l.market_id ORDER BY l.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS n20,
    STDDEV_SAMP(SAFE_DIVIDE(l.close, l.c1) - 1)
      OVER (PARTITION BY l.market_id ORDER BY l.date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) AS sd250,
    COUNT(l.c1) OVER (PARTITION BY l.market_id ORDER BY l.date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) AS n250
  FROM lagged AS l
),
m AS (
  SELECT
    w.market_id,
    w.date,
    w.close,
    ROUND((SAFE_DIVIDE(w.close, w.c1) - 1) * 100, 4) AS r1_pct,
    ROUND((SAFE_DIVIDE(w.close, w.c5) - 1) * 100, 4) AS r5_pct,
    ROUND((SAFE_DIVIDE(w.close, w.c20) - 1) * 100, 4) AS r20_pct,
    ROUND((SAFE_DIVIDE(w.close, w.c60) - 1) * 100, 4) AS r60_pct,
    IF(w.n50 = 50, ROUND((SAFE_DIVIDE(w.close, w.ma50) - 1) * 100, 4), NULL) AS vs_50d_pct,
    IF(w.n20 = 20, ROUND(w.sd20 * SQRT(252) * 100, 4), NULL) AS vol20_pct,
    ROUND((SAFE_DIVIDE(w.close, w.high250) - 1) * 100, 4) AS dd_from_high_pct,
    IF(w.n250 >= 120, ROUND(w.sd250 * SQRT(252) * 100, 4), NULL) AS normal_vol_pct,
    v.vix_close
  FROM w
  LEFT JOIN vix AS v ON w.market_id = 'us' AND v.date = w.date
),
lim AS (
  SELECT
    m.*,
    IF(m.market_id = 'us', 1.0, ROUND(m.normal_vol_pct / 16, 4)) AS k
  FROM m
),
lim2 AS (
  SELECT
    lim.*,
    ROUND(-2 * lim.k, 4) AS limit_r1_pct,
    ROUND(-3 * lim.k, 4) AS limit_r5_pct,
    ROUND(-8 * lim.k, 4) AS limit_dd_pct,
    ROUND(25 * lim.k, 4) AS limit_vol20_pct
  FROM lim
)
SELECT
  mk.market_id, mk.market, mk.market_order, mk.reading_symbol, mk.reading_name, mk.fund_symbol, mk.buyable,
  x.date, x.close, x.r1_pct, x.r5_pct, x.r20_pct, x.r60_pct, x.vs_50d_pct, x.vol20_pct, x.dd_from_high_pct,
  x.normal_vol_pct, x.vix_close, x.k, x.limit_r1_pct, x.limit_r5_pct, x.limit_dd_pct, x.limit_vol20_pct,
  CASE
    WHEN x.k IS NULL THEN NULL
    WHEN x.r1_pct <= x.limit_r1_pct THEN 'crash_day'
    WHEN x.r5_pct <= x.limit_r5_pct OR x.dd_from_high_pct <= x.limit_dd_pct THEN 'drawdown'
    WHEN (x.market_id = 'us' AND x.vix_close >= 25) OR x.vol20_pct >= x.limit_vol20_pct THEN 'stress'
    WHEN x.vs_50d_pct < 0 THEN 'weak'
    ELSE 'calm'
  END AS label,
  IF(x.k IS NULL, NULL, ARRAY_TO_STRING([
    IF(x.r1_pct <= x.limit_r1_pct,
       FORMAT('1-close change %.1f%% (limit %.1f%%)', x.r1_pct, x.limit_r1_pct), NULL),
    IF(x.r5_pct <= x.limit_r5_pct,
       FORMAT('5-close change %.1f%% (limit %.1f%%)', x.r5_pct, x.limit_r5_pct), NULL),
    IF(x.dd_from_high_pct <= x.limit_dd_pct,
       FORMAT('%.1f%% under its 250-close high (limit %.1f%%)', -x.dd_from_high_pct, -x.limit_dd_pct), NULL),
    IF(x.market_id = 'us' AND x.vix_close >= 25, FORMAT('VIX %.1f (limit 25)', x.vix_close), NULL),
    IF(x.vol20_pct >= x.limit_vol20_pct,
       FORMAT('20-close volatility %.1f%% (limit %.1f%%)', x.vol20_pct, x.limit_vol20_pct), NULL),
    IF(x.vs_50d_pct < 0, FORMAT('%.1f%% under its 50-close average', -x.vs_50d_pct), NULL)
  ], ' | ')) AS waiting_for,
  ROUND((SAFE_DIVIDE(x.r20_pct, x.normal_vol_pct * SQRT(20 / 252))
         + SAFE_DIVIDE(x.r60_pct, x.normal_vol_pct * SQRT(60 / 252))) / 2, 3) AS trend_score
FROM lim2 AS x
JOIN mk ON mk.market_id = x.market_id;
