-- v_forward_signals: the forward signal ledger - one row per signal and horizon (the tracker's Key: symbol, horizon,
-- recording day), with where it came from, its entry, the benchmark of its own market over the same dates, the
-- return above that benchmark net of a stated cost, whether it beat the benchmark, and its status.
-- WHY (week plan of 23 Nov 2026, package 4; owner, 23 Nov 2026: "go and Start build"): a rise is not a win when the
-- market rose more. Every signal is set against its own market, after cost, and the names the gates kept out are
-- followed too, so a gate can be judged by what it kept out and what it let in (v_forward_signal_gates).
-- THE ROWS: v_outcome_engine, one row per record_key - the engine's cohort row when the Key was scored, else the
-- Key's first record. Nothing is left out: open, unscorable and HOLD records stay, each with its status.
-- GATE: 'admitted' (entry investability INVESTABLE) or 'kept out' (WATCHLIST or BLOCKED). block_reason_class is the
-- nightly market page's Block Reason for the symbol on the recording day, cut at the first ':', '(' or long dash
-- ('Conservative gate', 'Engine neutral', 'Engine recommends SELL', ...), taken only when that page shows the same
-- tier as the record; '(reason not archived)' otherwise - the pages are archived from 31 Oct 2026.
-- THE OUTCOME: outcome_state is the engine's own test without the stance (entry price, target due, bars, the
-- entry price within 35 % of the base bar's close, a bar at the target), so a HOLD record gets the long-side return
-- the engine does not give it; for a bullish or bearish record it equals engine_state, and stock_roi_pp equals
-- engine_roi_pp (v_forward_signal_recon checks both). stock_roi_pp is the total return (adj_close) from the
-- recorded entry price to the close of the first session on or after the target, in percent points.
-- THE BENCHMARK, by market: US -> SPY, total return; Tadawul -> ^TASI.SR, PRICE ONLY (the index series pays no
-- dividends, so a Saudi name's total return is flattered by about the index's dividend yield over the horizon);
-- any other market -> ACWI in USD, total return, a rough yardstick for a local-currency return. bench_from is the
-- last benchmark session that had closed when the record was made (16:00 New York, 15:00 Riyadh) - the same
-- moment the entry price was taken, not a later close of the same day; bench_to is the benchmark's last close on or
-- before the target bar. A record made while its market is open is still set against the previous close.
-- COST: cost_pp = 0.50 points a round trip (0.10 commission + 0.15 spread a side, as v_hold_vs_switch: the
-- commission is the measured 0.094 % of the owner's fills, the spread an allowance).
-- active_pp = stock_roi_pp - bench_roi_pp; active_net_pp = active_pp - cost_pp; beat_benchmark = active_net_pp > 0
-- (the long side, whatever the stance: did buying it beat buying the market?). call_right reads the stance: a
-- bullish signal is right when it beat the benchmark net of cost, a bearish one when it trailed the benchmark.
-- STATUS: 'scored' (the engine's cohort, with a benchmark), 'scored, no benchmark close', 'tracked: no direction
-- given' (a HOLD record with an outcome), 'not scored: the price did not move', 'open: target not due', 'open:
-- awaiting the bar', 'not scorable: <state>'.
-- PERIOD: 'history, tally fixed before reading it' for records made before 26 Nov 2026, 'forward from 2026-11-26'
-- after.
-- COLUMNS: signal_key, record_id, origin_tab, symbol, market, market_group, horizon, recorded_at, recorded_date,
-- entry_recommendation, entry_stance, entry_tier, entry_final_action, entry_selected, confidence, gate,
-- block_reason_class, entry_price, target_date, target_bar_date, bench_symbol, bench_basis, bench_from, bench_to,
-- engine_state, outcome_state, engine_roi_pp, stock_roi_pp, bench_roi_pp, active_pp, cost_pp, active_net_pp,
-- beat_benchmark, call_right, status, period.
-- Cost of a read: about 1.1 GB (v_outcome_engine and v_universe).
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_forward_signals`
OPTIONS (description = "TFB forward signal ledger (views 1.31.0): one row per signal and horizon - provenance, entry, gate and block reason, the benchmark of its own market over the same dates (SPY total return, ^TASI.SR price only, ACWI otherwise), the return above it net of 0.50 points of cost, benchmark beat and status - research only")
AS
WITH e AS (
  SELECT
    record_key, record_id, origin_tab, symbol, bars_symbol, market, horizon, confidence, recorded_at, recorded_date,
    entry_recommendation, entry_stance, UPPER(IFNULL(entry_investability, '')) AS entry_tier, entry_final_action,
    entry_selected, entry_price, target_date, target_session_date, base_bar_date, target_bar_date, target_close_adj,
    last_bar_date, engine_state, engine_roi_pp, engine_scored
  FROM `{{DS}}.v_outcome_engine`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY record_key ORDER BY in_engine_cohort DESC, recorded_at, sheet_row, record_id) = 1
),
base AS (
  SELECT symbol, date, close FROM `{{DS}}.symbol_bars_daily` WHERE close > 0
),
uni AS (
  SELECT symbol, snapshot_date,
    UPPER(IFNULL(CAST(investability_status AS STRING), '')) AS tier,
    NULLIF(TRIM(REGEXP_EXTRACT(IFNULL(CAST(block_reason AS STRING), ''), r'^[^:(\x{2014}]*')), '') AS reason_class
  FROM `{{DS}}.v_universe`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, snapshot_date ORDER BY captured_at DESC, source_page) = 1
),
bench AS (
  SELECT market_group, bench_symbol, date, px, closes_at,
    LEAD(closes_at) OVER (PARTITION BY bench_symbol ORDER BY date) AS next_closes_at,
    LEAD(date) OVER (PARTITION BY bench_symbol ORDER BY date) AS next_date
  FROM (
    SELECT
      CASE symbol WHEN 'SPY' THEN 'US' WHEN '^TASI.SR' THEN 'Tadawul' ELSE 'Other' END AS market_group,
      symbol AS bench_symbol, date,
      IF(symbol = '^TASI.SR', close, COALESCE(adj_close, close)) AS px,
      IF(symbol = '^TASI.SR', TIMESTAMP(DATETIME(date, TIME '15:00:00'), 'Asia/Riyadh'),
         TIMESTAMP(DATETIME(date, TIME '16:00:00'), 'America/New_York')) AS closes_at
    FROM `{{DS}}.market_series_daily`
    WHERE symbol IN ('SPY', '^TASI.SR', 'ACWI') AND close > 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY captured_at DESC) = 1
  )
),
s AS (
  SELECT e.*,
    CASE e.market WHEN 'US' THEN 'US' WHEN 'Tadawul' THEN 'Tadawul' ELSE 'Other' END AS market_group,
    CASE
      WHEN e.entry_price IS NULL OR e.entry_price <= 0 THEN 'no_entry_price'
      WHEN e.target_session_date > CURRENT_DATE('Asia/Riyadh') THEN 'target_not_due'
      WHEN e.last_bar_date IS NULL THEN 'no_bar_for_symbol'
      WHEN b.close > 0 AND (e.entry_price / b.close > 1.35 OR e.entry_price / b.close < 0.65) THEN 'entry_price_mismatch'
      WHEN e.target_bar_date IS NOT NULL THEN 'scored_from_bar'
      WHEN DATE_ADD(e.target_session_date, INTERVAL 7 DAY) > e.last_bar_date THEN 'awaiting_bar'
      ELSE 'no_bar_at_target'
    END AS outcome_state,
    CASE
      WHEN e.entry_tier = 'INVESTABLE' THEN 'admitted'
      WHEN e.entry_tier IN ('WATCHLIST', 'BLOCKED') THEN 'kept out'
      ELSE 'no tier recorded'
    END AS gate,
    CASE
      WHEN e.entry_tier = 'INVESTABLE' THEN '(admitted)'
      WHEN e.entry_tier IN ('WATCHLIST', 'BLOCKED') AND u.tier = e.entry_tier AND u.reason_class IS NOT NULL THEN u.reason_class
      WHEN e.entry_tier IN ('WATCHLIST', 'BLOCKED') THEN '(reason not archived)'
      ELSE '(no tier recorded)'
    END AS block_reason_class
  FROM e
  LEFT JOIN base b ON b.symbol = e.bars_symbol AND b.date = e.base_bar_date
  LEFT JOIN uni u ON u.symbol = e.symbol AND u.snapshot_date = e.recorded_date
),
j AS (
  SELECT s.*,
    IF(s.outcome_state = 'scored_from_bar', ROUND((SAFE_DIVIDE(s.target_close_adj, s.entry_price) - 1) * 100, 4), NULL) AS stock_roi,
    b0.bench_symbol, b0.date AS bench_from, b1.date AS bench_to,
    IF(s.outcome_state = 'scored_from_bar', 100 * (SAFE_DIVIDE(b1.px, b0.px) - 1), NULL) AS bench_roi
  FROM s
  LEFT JOIN bench b0 ON b0.market_group = s.market_group AND b0.closes_at <= s.recorded_at
    AND (b0.next_closes_at IS NULL OR b0.next_closes_at > s.recorded_at)
  LEFT JOIN bench b1 ON b1.market_group = s.market_group AND b1.date <= s.target_bar_date
    AND (b1.next_date IS NULL OR b1.next_date > s.target_bar_date)
)
SELECT
  record_key AS signal_key, record_id, origin_tab, symbol, market, market_group, horizon, recorded_at, recorded_date,
  entry_recommendation, entry_stance, entry_tier, entry_final_action, entry_selected, confidence,
  gate, block_reason_class,
  entry_price, target_date, target_bar_date,
  bench_symbol,
  CASE bench_symbol
    WHEN 'SPY' THEN 'S&P 500 fund (SPY), total return'
    WHEN '^TASI.SR' THEN 'Tadawul All Share (^TASI.SR), price only - no dividends'
    WHEN 'ACWI' THEN 'world fund (ACWI) in USD, total return - a rough yardstick for a local-currency return'
  END AS bench_basis,
  bench_from, bench_to,
  engine_state, outcome_state, engine_roi_pp,
  stock_roi AS stock_roi_pp,
  ROUND(bench_roi, 4) AS bench_roi_pp,
  ROUND(stock_roi - bench_roi, 4) AS active_pp,
  IF(stock_roi IS NOT NULL AND bench_roi IS NOT NULL, 0.5, NULL) AS cost_pp,
  ROUND(stock_roi - bench_roi - 0.5, 4) AS active_net_pp,
  stock_roi - bench_roi - 0.5 > 0 AS beat_benchmark,
  CASE
    WHEN NOT engine_scored OR bench_roi IS NULL THEN NULL
    WHEN entry_stance = 'bullish' THEN stock_roi - bench_roi - 0.5 > 0
    WHEN entry_stance = 'bearish' THEN stock_roi - bench_roi < 0
  END AS call_right,
  CASE
    WHEN engine_scored AND bench_roi IS NOT NULL THEN 'scored'
    WHEN engine_scored THEN 'scored, no benchmark close'
    WHEN outcome_state = 'scored_from_bar' AND entry_stance IN ('bullish', 'bearish') THEN 'not scored: the price did not move'
    WHEN outcome_state = 'scored_from_bar' THEN 'tracked: no direction given'
    WHEN outcome_state = 'target_not_due' THEN 'open: target not due'
    WHEN outcome_state = 'awaiting_bar' THEN 'open: awaiting the bar'
    ELSE CONCAT('not scorable: ', outcome_state)
  END AS status,
  CASE WHEN recorded_date < DATE '2026-11-26' THEN 'history, tally fixed before reading it' ELSE 'forward from 2026-11-26' END AS period
FROM j;
