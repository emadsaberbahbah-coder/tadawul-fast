-- v_challenger_v0_features: the inputs of the shadow challenger v0 - one row per followed share and session, every
-- input computed from that session's close and the closes before it, never a later one.
-- WHY (week plan of 23 Nov 2026, package 5; owner, 23 Nov 2026: "go and Start build"): the 23 Nov deep audit asks
-- for a challenger to the live score, built only from data TFB holds for two years - daily prices - and judged per
-- market, net of cost, on dates it never saw. The live score is not changed; nothing reads the challenger.
-- THE NAMES: every symbol of symbol_bars_daily with an exchange suffix and a GICS sector (v_symbol_sector). Funds,
-- indexes and coins have no sector and are left out; symbols without a suffix are second copies of US names (the
-- '.US' row is the one kept). The list is today's followed names carried back two years: survivors only, which
-- flatters any long side (the rank correlation is less affected).
-- THE MARKET: US ('.US'), SA (Tadawul, '.SR'), JP ('.T'), HK ('.HK'), OTHER (every other suffix, pooled - each
-- below 25 names on its own).
-- THE PRICE: adj_close (dividends and splits folded in), else close; one row per symbol and date.
-- THE INPUTS at session t (all from closes up to t):
--   lr           the log return of the session
--   vol63        the annualised standard deviation of the last 63 log returns (at least 60 present)
--   mom12_vs     the return from 252 to 21 sessions back (12-1 month momentum) over vol63
--   mom6_vs      the return from 126 to 21 sessions back (6-1 month momentum) over vol63
--   rs63         the 63-session return; ranked inside a market it orders the names exactly as their return above
--                the market does, so the market's own return need not be taken off
--   low_down126  minus the root mean square of the negative log returns of the last 126 sessions (at least 120
--                present): a calmer downside ranks higher
-- Research only: no decision, gate or count reads this view. (views 1.34.0)
CREATE OR REPLACE VIEW `{{DS}}.v_challenger_v0_features`
OPTIONS (description = "TFB shadow challenger v0 inputs (views 1.34.0): per followed share and session, from that close and earlier ones only - 12-1 and 6-1 month momentum over 63-session volatility, the 63-session return and the 126-session downside volatility, with market and GICS sector. Research view, never an order.")
AS
WITH px AS (
  SELECT symbol, date, ANY_VALUE(COALESCE(adj_close, close)) AS p
  FROM `{{DS}}.symbol_bars_daily`
  WHERE close > 0 AND REGEXP_CONTAINS(symbol, r'\.[A-Z]+$')
  GROUP BY symbol, date
),
names AS (
  SELECT s.symbol, TRIM(s.sector) AS sector,
         CASE REGEXP_EXTRACT(s.symbol, r'\.([A-Z]+)$')
           WHEN 'US' THEN 'US' WHEN 'SR' THEN 'SA' WHEN 'T' THEN 'JP' WHEN 'HK' THEN 'HK' ELSE 'OTHER' END AS market
  FROM `{{DS}}.v_symbol_sector` AS s
  WHERE REGEXP_CONTAINS(s.symbol, r'\.[A-Z]+$') AND TRIM(IFNULL(s.sector, '')) != ''
),
lagged AS (
  SELECT n.market, n.sector, x.symbol, x.date, x.p,
         LN(x.p / LAG(x.p) OVER w) AS lr,
         LAG(x.p, 21) OVER w AS p21, LAG(x.p, 63) OVER w AS p63,
         LAG(x.p, 126) OVER w AS p126, LAG(x.p, 252) OVER w AS p252
  FROM px AS x
  JOIN names AS n USING (symbol)
  WINDOW w AS (PARTITION BY x.symbol ORDER BY x.date)
),
rolled AS (
  SELECT l.*,
         STDDEV_SAMP(l.lr) OVER w63 * SQRT(252) AS vol63, COUNT(l.lr) OVER w63 AS n63,
         SQRT(AVG(POW(LEAST(l.lr, 0), 2)) OVER w126) AS down126, COUNT(l.lr) OVER w126 AS n126
  FROM lagged AS l
  WINDOW w63 AS (PARTITION BY l.symbol ORDER BY l.date ROWS BETWEEN 62 PRECEDING AND CURRENT ROW),
         w126 AS (PARTITION BY l.symbol ORDER BY l.date ROWS BETWEEN 125 PRECEDING AND CURRENT ROW)
)
SELECT
  market, sector, symbol, date, p, lr,
  IF(n63 >= 60 AND vol63 > 0, vol63, NULL) AS vol63,
  IF(n63 >= 60 AND vol63 > 0 AND p252 > 0, SAFE_DIVIDE(p21 / p252 - 1, vol63), NULL) AS mom12_vs,
  IF(n63 >= 60 AND vol63 > 0 AND p126 > 0, SAFE_DIVIDE(p21 / p126 - 1, vol63), NULL) AS mom6_vs,
  IF(p63 > 0, p / p63 - 1, NULL) AS rs63,
  IF(n126 >= 120, -down126, NULL) AS low_down126
FROM rolled;
