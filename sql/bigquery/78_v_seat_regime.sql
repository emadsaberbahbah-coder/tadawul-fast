-- v_seat_regime: every board seat of the newest scorecard snapshot tagged
-- with the market regime at the LAST US close before the seat was taken
-- (the regime the night-time builder could have known), with the seat's own
-- +5 / +20-session outcome beside it - the evidence a drawdown brake is
-- decided on. WHY (owner, 8 Nov 2026): on 8 Nov the archive held 7 USD seats
-- taken the day after a >= 1.5 % SPY drop (1 win in 7, -3.5 % at +5) - too
-- few to arm anything. This view keeps the count growing by itself.
-- RULE: regime row = the newest v_market_regime date strictly before
-- seat_date (a seat taken on a US session reads the previous close; a
-- weekend seat reads Friday). Seats before the first archived SPY day carry
-- NULLs.
-- COLUMNS: the scorecard's own (v_board_episodes) plus regime_date, regime,
-- spy_r1_pct, spy_r5_pct, spy_r20_pct, spy_vs_50d_pct, spy_vol20_pct,
-- spy_dd_from_high_pct, vix_close.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_seat_regime`
OPTIONS (description = "TFB Looker v6: every board seat of the newest scorecard snapshot with the market regime at the last US close before it (label, SPY changes, volatility, VIX) beside its +5 / +20 outcome")
AS
WITH seats AS (
  SELECT *
  FROM `{{DS}}.v_board_episodes`
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `{{DS}}.v_board_episodes`)
),
reg AS (
  SELECT
    r.*,
    LEAD(r.date) OVER (ORDER BY r.date) AS next_date
  FROM `{{DS}}.v_market_regime` AS r
)
SELECT
  s.*,
  g.date AS regime_date,
  g.regime,
  g.spy_r1_pct,
  g.spy_r5_pct,
  g.spy_r20_pct,
  g.spy_vs_50d_pct,
  g.spy_vol20_pct,
  g.spy_dd_from_high_pct,
  g.vix_close
FROM seats AS s
LEFT JOIN reg AS g
  ON g.date < s.seat_date
 AND (g.next_date >= s.seat_date OR g.next_date IS NULL);
