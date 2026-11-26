-- v_cash_entry_events: a shadow rule, recorded and scored but never acted on - after the US market's first
-- reading that is not calm or weak (stress, drawdown or a crash day), what did cash put into the S&P 500 fund earn,
-- entered in three ways?
-- WHY (owner, 26 Nov 2026 01:22 Riyadh, in Arabic: "there is cash and the market is falling ... if there are real
-- chances and prices are low and there is liquidity, see what is best"). The Daily Plan blocks new US buys while the
-- market is not calm (TFB_PLAN_REGIME, since 18 Nov); this view measures what that block costs or saves, on the
-- market itself, before any change to it is proposed (owner's ground rules, 21 Nov 2026).
-- THE RULE (fixed on 26 Nov 2026):
--   signal      a session of v_market_regime labelled stress, drawdown or crash_day with none of the three in the 10
--               sessions before it (the first reading of a spell)
--   all at once the whole amount at the signal close
--   three parts a third at the signal close, a third 5 sessions later, a third 10 sessions later
--   after calm  the whole amount at the first close labelled calm after the signal (0 when none comes in time)
--   outcome     each way's return 20 and 60 sessions after the signal, in % of the whole amount (cash = 0; the part
--               not yet invested earns 0); worst20_pct the fund's lowest close in the 20 sessions after the signal;
--               NULL until the horizon has passed
-- PERIOD: signals before 26 Nov 2026 are 'history, in-sample' (three years, about ten signals: context, never
-- evidence); from 26 Nov 2026 'forward from 2026-11-26'. One fund, one country, no costs, no taxes.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_cash_entry_events`
OPTIONS (description = "TFB shadow rule (views 1.30.0): the first not-calm reading of the US market - cash entered into SPY all at once, in three parts or after the first calm close, 20 and 60 sessions on")
AS
WITH r AS (
  SELECT date, spy_close, regime,
    ROW_NUMBER() OVER (ORDER BY date) AS n,
    IFNULL(MAX(IF(regime IN ('stress', 'drawdown', 'crash_day'), 1, 0)) OVER (ORDER BY date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING), 0) = 1 AS hot_before
  FROM `{{DS}}.v_market_regime`
  WHERE spy_close > 0 AND regime IS NOT NULL
),
sig AS (
  SELECT * FROM r WHERE regime IN ('stress', 'drawdown', 'crash_day') AND NOT hot_before
),
path AS (
  SELECT s.date AS signal_date, s.regime AS signal_regime, s.spy_close AS p0, f.n - s.n AS d, f.spy_close AS p, f.regime AS reg
  FROM sig s
  JOIN r f ON f.n BETWEEN s.n AND s.n + 60
),
agg AS (
  SELECT signal_date, signal_regime, p0,
    MAX(d) AS sessions_after,
    MAX(IF(d = 5, p, NULL)) AS p5, MAX(IF(d = 10, p, NULL)) AS p10,
    MAX(IF(d = 20, p, NULL)) AS p20, MAX(IF(d = 60, p, NULL)) AS p60,
    MIN(IF(d BETWEEN 1 AND 20, p, NULL)) AS low20,
    MIN(IF(d >= 1 AND reg = 'calm', d, NULL)) AS calm_d
  FROM path
  GROUP BY signal_date, signal_regime, p0
),
calm AS (
  SELECT a.*, c.p AS p_calm
  FROM agg a
  LEFT JOIN path c ON c.signal_date = a.signal_date AND c.d = a.calm_d
)
SELECT
  signal_date, signal_regime,
  CASE WHEN signal_date < DATE '2026-11-26' THEN 'history, in-sample' ELSE 'forward from 2026-11-26' END AS period,
  ROUND(p0, 2) AS spy_close, sessions_after,
  ROUND(100 * (SAFE_DIVIDE(low20, p0) - 1), 2) AS worst20_pct,
  calm_d AS calm_after_sessions,
  ROUND(100 * (SAFE_DIVIDE(p20, p0) - 1), 2) AS all_at_once_20_pct,
  ROUND(100 * (SAFE_DIVIDE(p60, p0) - 1), 2) AS all_at_once_60_pct,
  ROUND(100 * ((SAFE_DIVIDE(p20, p0) - 1) + (SAFE_DIVIDE(p20, p5) - 1) + (SAFE_DIVIDE(p20, p10) - 1)) / 3, 2) AS three_parts_20_pct,
  ROUND(100 * ((SAFE_DIVIDE(p60, p0) - 1) + (SAFE_DIVIDE(p60, p5) - 1) + (SAFE_DIVIDE(p60, p10) - 1)) / 3, 2) AS three_parts_60_pct,
  ROUND(CASE WHEN p20 IS NULL THEN NULL WHEN calm_d IS NULL OR calm_d >= 20 THEN 0 ELSE 100 * (SAFE_DIVIDE(p20, p_calm) - 1) END, 2) AS after_calm_20_pct,
  ROUND(CASE WHEN p60 IS NULL THEN NULL WHEN calm_d IS NULL OR calm_d >= 60 THEN 0 ELSE 100 * (SAFE_DIVIDE(p60, p_calm) - 1) END, 2) AS after_calm_60_pct
FROM calm;
