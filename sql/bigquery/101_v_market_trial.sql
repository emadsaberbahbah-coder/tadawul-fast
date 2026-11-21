-- v_market_trial: the forward test of the market readings - on every US
-- session, each market's state and rank, and what its fund did next.
-- WHY (owner, 21 Nov 2026 11:03 Riyadh, the build list): "Accuracy: log and
-- score everything. Observe for two weeks (10 sessions), then review with the
-- owner, and switch on only what proved itself." The rules of v_market_condition
-- and v_market_rank were written on 21 Nov 2026 before any of the sessions
-- they are scored on: period = 'forward' from Monday 23 Nov 2026, the
-- sessions the owner reviews; 'backtest' before it (the same rules read back
-- on the archive - no limit was fitted to it: the US limits are
-- v_market_regime's, every other limit is the US limit scaled by k).
-- THE DAY: decision_day = a US session (SPY's own dates): every fund trades in
--   New York except the Saudi REITs (on Tadawul). The reading of the day is
--   v_market_rank's row for that date; its markets closed before New York did.
-- THE OUTCOME: the market's fund (dividend-adjusted close), or the reading
--   series itself for a market without one (oil: BZ=F); entry = its first
--   close AFTER the decision day (within 7 days: the day's own close is never
--   used, so a reading is never scored on a price it could not have traded),
--   f5_pct / f10_pct / f20_pct = the change from the entry close to the 5th /
--   10th / 20th close after it (NULL until it exists - read 'matured' as NOT
--   NULL); spy_f5_pct / spy_f10_pct / spy_f20_pct = SPY's own over its own
--   same span, from the same entry rule, so vs_spy_*_pp say what a switch out
--   of the US market would have changed.
-- The windows of consecutive days overlap: read the means and shares, not a
-- count of independent trials.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_market_trial`
OPTIONS (description = "TFB markets v1: one row per US session and market - the market's state (open / hold), rank and trend score that day (v_market_rank) and its fund's change over the next 5, 10 and 20 closes from the first close after the day, with SPY's over the same span - period 'forward' from 23 Nov 2026 (the owner's 10-session review), 'backtest' before it. Observe only.")
AS
WITH r AS (
  SELECT *
  FROM `{{DS}}.v_market_rank`
),
us AS (
  SELECT date AS d
  FROM `{{DS}}.market_series_daily`
  WHERE symbol = 'SPY' AND close > 0
),
out_px AS (
  SELECT s.symbol, s.date, COALESCE(s.adj_close, s.close) AS px
  FROM `{{DS}}.market_series_daily` AS s
  WHERE s.close > 0 AND s.symbol IN ('SPY', 'KSA', 'VGK', 'EWJ', 'MCHI', 'GLD', 'IEF', 'SPSK', 'VNQ', 'BZ=F')
  UNION ALL
  SELECT 'SA_REITS', date, close
  FROM `{{DS}}.v_market_condition`
  WHERE market_id = 'reits_sa'
),
seq AS (
  SELECT symbol, date, px, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS n
  FROM out_px
),
base AS (
  SELECT r.*, COALESCE(r.fund_symbol, r.reading_symbol) AS outcome_symbol
  FROM r
  JOIN us ON us.d = r.as_of
),
entry AS (
  SELECT b.as_of, b.market_id, e.symbol, e.date AS entry_date, e.px AS entry_px, e.n AS entry_n
  FROM base AS b
  JOIN seq AS e
    ON e.symbol = b.outcome_symbol AND e.date > b.as_of AND e.date <= DATE_ADD(b.as_of, INTERVAL 7 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.as_of, b.market_id ORDER BY e.date) = 1
),
fwd AS (
  SELECT
    en.as_of,
    en.market_id,
    en.entry_date,
    ROUND((SAFE_DIVIDE(x5.px, en.entry_px) - 1) * 100, 4) AS f5_pct,
    ROUND((SAFE_DIVIDE(x10.px, en.entry_px) - 1) * 100, 4) AS f10_pct,
    ROUND((SAFE_DIVIDE(x20.px, en.entry_px) - 1) * 100, 4) AS f20_pct
  FROM entry AS en
  LEFT JOIN seq AS x5 ON x5.symbol = en.symbol AND x5.n = en.entry_n + 5
  LEFT JOIN seq AS x10 ON x10.symbol = en.symbol AND x10.n = en.entry_n + 10
  LEFT JOIN seq AS x20 ON x20.symbol = en.symbol AND x20.n = en.entry_n + 20
)
SELECT
  b.as_of AS decision_day,
  IF(b.as_of >= DATE '2026-11-23', 'forward', 'backtest') AS period,
  b.market_id, b.market, b.market_order, b.buyable, b.fund_symbol, b.outcome_symbol,
  b.date AS reading_date, b.days_old, b.label, b.state, b.rank, b.buyable_rank, b.trend_score,
  b.open_markets, b.buyable_markets, b.verdict,
  f.entry_date, f.f5_pct, f.f10_pct, f.f20_pct,
  s.f5_pct AS spy_f5_pct, s.f10_pct AS spy_f10_pct, s.f20_pct AS spy_f20_pct,
  ROUND(f.f5_pct - s.f5_pct, 4) AS vs_spy_f5_pp,
  ROUND(f.f10_pct - s.f10_pct, 4) AS vs_spy_f10_pp,
  ROUND(f.f20_pct - s.f20_pct, 4) AS vs_spy_f20_pp
FROM base AS b
LEFT JOIN fwd AS f ON f.as_of = b.as_of AND f.market_id = b.market_id
LEFT JOIN fwd AS s ON s.as_of = b.as_of AND s.market_id = 'us';
