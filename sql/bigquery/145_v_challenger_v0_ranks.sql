-- v_challenger_v0_ranks: the shadow challenger v0 on its formation dates - each input of v_challenger_v0_features
-- ranked inside its market and sector, the equal-weight score, and the result over the next month against the
-- market's benchmark.
-- WHY (week plan of 23 Nov 2026, package 5): see v_challenger_v0_features. This view adds the ranks and the result;
-- v_challenger_v0_walkforward learns the weights, v_challenger_v0_scorecard reads the record.
-- FORMATION DATES: a market's sessions are the dates on which at least 20 of its names have a close; a formation
-- date is the FIRST such session of each ISO week on which at least 20 of its names have a 12-month history. Tied
-- to the calendar, not to a count of sessions, so the nightly reload of symbol_bars_daily (a rolling 760-day
-- window) only drops the oldest dates and never moves the others (Codex P2 on #1101); and a date is final once its
-- session has closed, so a shadow book recorded on it does not move later in the week.
-- THE RANK of each input on a formation date: its percentile among the names of the same market and sector that
-- have it, when there are at least 20 of them; else among the names of the market that have it, when at least 20;
-- else none. dev_* = percentile - 0.5, and 0 (neutral) for a missing input or rank. score_eq = 50 + 100 x the mean
-- of the four dev_* (0 to 100). sector_ranked = how many of the four were ranked inside the sector.
-- THE RESULT (the only look forward): bought at the close of the NEXT session after the formation date (never the
-- formation close the inputs used) and sold 21 sessions later, at the 22nd close after it; fwd_pp is the total
-- return in per cent, left empty when that close is more than 45 calendar days away. bench_pp is the benchmark
-- over the same two dates (its last close on or before each): US -> SPY, total return; SA -> ^TASI.SR, PRICE ONLY;
-- JP -> ^N225 and HK -> ^HSI, PRICE ONLY, local currency; OTHER -> ACWI in USD, total return (a rough yardstick).
-- excess_pp = fwd_pp - bench_pp; label_pr = its percentile among the market's names on that date (a rank, so a
-- wild result moves it no more than any top result). excess_w_pp = excess_pp held inside the 1st and 99th
-- percentile of the market's results on that date, for the means of v_challenger_v0_scorecard: a few US names
-- carry unadjusted reverse splits (a price x 100 in one session) whose results otherwise swamp any quintile mean.
-- (Added after the first read on 27 Nov 2026; the reading rules of the scorecard were not changed.)
-- Research only: no decision, gate or count reads this view. (views 1.34.0)
CREATE OR REPLACE VIEW `{{DS}}.v_challenger_v0_ranks`
OPTIONS (description = "TFB shadow challenger v0 ranks (views 1.34.0): on the first session of each ISO week of each market, each input ranked inside its market and sector (at least 20 peers, else the market, else neutral), the equal-weight score, and the next 21 sessions from the next close against the market's benchmark. Research view, never an order.")
AS
WITH f AS (
  SELECT * FROM `{{DS}}.v_challenger_v0_features`
),
cal AS (
  SELECT market, date, COUNT(mom12_vs) AS n_m12
  FROM f
  GROUP BY market, date
  HAVING COUNT(*) >= 20
),
form AS (
  SELECT market, MIN(date) AS d
  FROM cal
  WHERE n_m12 >= 20
  GROUP BY market, DATE_TRUNC(date, ISOWEEK)
),
lab AS (
  SELECT symbol, date,
         LEAD(date, 1) OVER w AS entry_date, LEAD(p, 1) OVER w AS entry_p,
         LEAD(date, 22) OVER w AS exit_date, LEAD(p, 22) OVER w AS exit_p
  FROM f
  WINDOW w AS (PARTITION BY symbol ORDER BY date)
),
bench_raw AS (
  SELECT symbol AS bench_symbol, date, ANY_VALUE(IF(symbol IN ('SPY', 'ACWI'), COALESCE(adj_close, close), close)) AS bp
  FROM `{{DS}}.market_series_daily`
  WHERE symbol IN ('SPY', '^TASI.SR', '^N225', '^HSI', 'ACWI') AND close > 0
  GROUP BY symbol, date
),
bench_days AS (
  SELECT b.bench_symbol, day
  FROM (SELECT bench_symbol, MIN(date) AS d0, MAX(date) AS d1 FROM bench_raw GROUP BY bench_symbol) AS b
  CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(b.d0, b.d1)) AS day
),
bench AS (
  SELECT d.bench_symbol, d.day,
         LAST_VALUE(r.bp IGNORE NULLS) OVER (PARTITION BY d.bench_symbol ORDER BY d.day
                                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bp
  FROM bench_days AS d
  LEFT JOIN bench_raw AS r ON r.bench_symbol = d.bench_symbol AND r.date = d.day
),
base AS (
  SELECT f.market, f.sector, f.symbol, f.date AS d, f.vol63, f.mom12_vs, f.mom6_vs, f.rs63, f.low_down126,
         CASE f.market WHEN 'US' THEN 'SPY' WHEN 'SA' THEN '^TASI.SR' WHEN 'JP' THEN '^N225' WHEN 'HK' THEN '^HSI'
           ELSE 'ACWI' END AS bench_symbol,
         l.entry_date, l.exit_date,
         IF(DATE_DIFF(l.exit_date, f.date, DAY) <= 45, (l.exit_p / l.entry_p - 1) * 100, NULL) AS fwd_pp
  FROM f
  JOIN form ON form.market = f.market AND form.d = f.date
  JOIN lab AS l ON l.symbol = f.symbol AND l.date = f.date
),
ranked AS (
  SELECT b.*,
    COUNT(b.mom12_vs) OVER (PARTITION BY b.market, b.sector, b.d) AS ns_m12,
    COUNT(b.mom12_vs) OVER (PARTITION BY b.market, b.d) AS nm_m12,
    IF(b.mom12_vs IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY b.market, b.sector, b.d, b.mom12_vs IS NULL ORDER BY b.mom12_vs)) AS ps_m12,
    IF(b.mom12_vs IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY b.market, b.d, b.mom12_vs IS NULL ORDER BY b.mom12_vs)) AS pm_m12,
    COUNT(b.mom6_vs) OVER (PARTITION BY b.market, b.sector, b.d) AS ns_m6,
    COUNT(b.mom6_vs) OVER (PARTITION BY b.market, b.d) AS nm_m6,
    IF(b.mom6_vs IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY b.market, b.sector, b.d, b.mom6_vs IS NULL ORDER BY b.mom6_vs)) AS ps_m6,
    IF(b.mom6_vs IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY b.market, b.d, b.mom6_vs IS NULL ORDER BY b.mom6_vs)) AS pm_m6,
    COUNT(b.rs63) OVER (PARTITION BY b.market, b.sector, b.d) AS ns_rs,
    COUNT(b.rs63) OVER (PARTITION BY b.market, b.d) AS nm_rs,
    IF(b.rs63 IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY b.market, b.sector, b.d, b.rs63 IS NULL ORDER BY b.rs63)) AS ps_rs,
    IF(b.rs63 IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY b.market, b.d, b.rs63 IS NULL ORDER BY b.rs63)) AS pm_rs,
    COUNT(b.low_down126) OVER (PARTITION BY b.market, b.sector, b.d) AS ns_dd,
    COUNT(b.low_down126) OVER (PARTITION BY b.market, b.d) AS nm_dd,
    IF(b.low_down126 IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY b.market, b.sector, b.d, b.low_down126 IS NULL ORDER BY b.low_down126)) AS ps_dd,
    IF(b.low_down126 IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY b.market, b.d, b.low_down126 IS NULL ORDER BY b.low_down126)) AS pm_dd
  FROM base AS b
),
devs AS (
  SELECT r.market, r.sector, r.symbol, r.d, r.vol63, r.mom12_vs, r.mom6_vs, r.rs63, r.low_down126,
    CASE WHEN r.ps_m12 IS NOT NULL AND r.ns_m12 >= 20 THEN r.ps_m12 - 0.5
         WHEN r.pm_m12 IS NOT NULL AND r.nm_m12 >= 20 THEN r.pm_m12 - 0.5 ELSE 0 END AS dev_m12,
    CASE WHEN r.ps_m6 IS NOT NULL AND r.ns_m6 >= 20 THEN r.ps_m6 - 0.5
         WHEN r.pm_m6 IS NOT NULL AND r.nm_m6 >= 20 THEN r.pm_m6 - 0.5 ELSE 0 END AS dev_m6,
    CASE WHEN r.ps_rs IS NOT NULL AND r.ns_rs >= 20 THEN r.ps_rs - 0.5
         WHEN r.pm_rs IS NOT NULL AND r.nm_rs >= 20 THEN r.pm_rs - 0.5 ELSE 0 END AS dev_rs,
    CASE WHEN r.ps_dd IS NOT NULL AND r.ns_dd >= 20 THEN r.ps_dd - 0.5
         WHEN r.pm_dd IS NOT NULL AND r.nm_dd >= 20 THEN r.pm_dd - 0.5 ELSE 0 END AS dev_dd,
    IF(r.ns_m12 >= 20 AND r.ps_m12 IS NOT NULL, 1, 0) + IF(r.ns_m6 >= 20 AND r.ps_m6 IS NOT NULL, 1, 0)
      + IF(r.ns_rs >= 20 AND r.ps_rs IS NOT NULL, 1, 0) + IF(r.ns_dd >= 20 AND r.ps_dd IS NOT NULL, 1, 0) AS sector_ranked,
    r.bench_symbol, r.entry_date, r.exit_date, r.fwd_pp,
    IF(r.fwd_pp IS NULL, NULL, (be.bp / bs.bp - 1) * 100) AS bench_pp
  FROM ranked AS r
  LEFT JOIN bench AS bs ON bs.bench_symbol = r.bench_symbol AND bs.day = r.entry_date
  LEFT JOIN bench AS be ON be.bench_symbol = r.bench_symbol AND be.day = r.exit_date
),
ex AS (
  SELECT x.*,
    50 + 100 * (x.dev_m12 + x.dev_m6 + x.dev_rs + x.dev_dd) / 4 AS score_eq,
    x.fwd_pp - x.bench_pp AS excess_pp
  FROM devs AS x
)
SELECT
  e.*,
  LEAST(GREATEST(e.excess_pp, PERCENTILE_CONT(e.excess_pp, 0.01) OVER (PARTITION BY e.market, e.d)),
        PERCENTILE_CONT(e.excess_pp, 0.99) OVER (PARTITION BY e.market, e.d)) AS excess_w_pp,
  IF(e.excess_pp IS NULL, NULL,
     PERCENT_RANK() OVER (PARTITION BY e.market, e.d, e.excess_pp IS NULL ORDER BY e.excess_pp)) AS label_pr
FROM ex AS e;
