-- v_regime_outcomes: what board seats earned in each market regime, per
-- currency group (USD, the fundable one, and ALL), with the sample size next
-- to every rate. WHY: the drawdown brake (F-8) is a rule about NOT seating or
-- NOT funding on certain days; this is the table that says whether those
-- days were worse, and how many of them the archive has seen.
-- COLUMNS: ccy_group, regime, seats, days (distinct seat dates), names,
-- scored5, ret5_pct, vs_spus5_pp (vs the US market ETF at +5, where the
-- scorecard has it), win5_pct, scored20, ret20_pct, first_seat, last_seat,
-- and sample_note ('small sample' under 30 scored seats or under 5 days).
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_regime_outcomes`
OPTIONS (description = "TFB Looker v6: board-seat outcomes (+5 / +20 sessions, win rate, vs SPY) by market regime at the prior close, for USD seats and all seats, with sample sizes and a small-sample note")
AS
WITH s AS (
  SELECT * FROM `{{DS}}.v_seat_regime`
  WHERE regime IS NOT NULL
),
g AS (
  SELECT 'USD' AS ccy_group, s.* FROM s WHERE s.ccy = 'USD'
  UNION ALL
  SELECT 'ALL' AS ccy_group, s.* FROM s
),
agg AS (
  SELECT
    ccy_group,
    regime,
    COUNT(*) AS seats,
    COUNT(DISTINCT seat_date) AS days,
    COUNT(DISTINCT symbol) AS names,
    COUNTIF(ret_plus5_pct IS NOT NULL) AS scored5,
    ROUND(AVG(ret_plus5_pct), 2) AS ret5_pct,
    ROUND(AVG(vs_spus_plus5_pp), 2) AS vs_spus5_pp,
    ROUND(AVG(win_plus5) * 100, 1) AS win5_pct,
    COUNTIF(ret_plus20_pct IS NOT NULL) AS scored20,
    ROUND(AVG(ret_plus20_pct), 2) AS ret20_pct,
    MIN(seat_date) AS first_seat,
    MAX(seat_date) AS last_seat
  FROM g
  GROUP BY ccy_group, regime
)
SELECT
  agg.*,
  IF(scored5 < 30 OR days < 5, 'small sample', '') AS sample_note
FROM agg;
