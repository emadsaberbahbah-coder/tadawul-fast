-- v_event_trial: the forward test of the event scenarios - each event that is
-- out, each market: what v_event_scenarios said before it, and what happened.
-- WHY (owner, 21 Nov 2026 11:03 Riyadh, items 5 and 6 of the build list):
-- "score every news signal" - "Accuracy: log and score everything. Observe
-- for two weeks (10 sessions), then review with the owner, and switch on only
-- what proved itself." The rules were written on 21 Nov 2026 before any event
-- they are scored on: period = 'forward' from Monday 23 Nov 2026 (00:00
-- Riyadh), 'backtest' before it (the same rules read back on the archive).
-- A scenario reads only events before its own (v_event_scenarios), so each
-- forecast is one that could have been made in time.
-- THE TESTS (each needs earlier events of its family: NULL until then):
--   1. the band     band_hit: the event-day move (r1_pct) fell within the
--                   middle 80 % of the earlier moves (8 or more of them); a
--                   true band holds 80 % of the time
--   2. a big move   big = 1 when the move was 1 normal day or more (up or
--                   down); p_big = the family's chance (8 or more earlier);
--                   p_big_any = how often any day moves that much;
--                   brier_big / brier_any = the squared errors of the two
--   3. the direction, given the figure: dir_expected = the sign of the median
--                   event-day move after the same figure (above or below the
--                   forecast, 4 or more earlier); dir_hit = the move had that
--                   sign (a flat move or a flat median is not scored)
--   4. the drift    the same for what followed the event close (f5_pct):
--                   drift_expected / drift_hit
--   5. the figure   brier_figure = the squared error of the family's chances
--                   of above / in line / below (8 or more earlier figures)
--                   against the figure that came; equal thirds score 2/3.
--                   The same on every market row of the event. Releases
--                   only: a rate decision is nearly always the forecast (the
--                   Fed's and the Bank of Japan's at 24 of 25 decisions since
--                   November 2023, the ECB's at all), so scoring it would
--                   flatter the figure score.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it. Observe only.
CREATE OR REPLACE VIEW `{{DS}}.v_event_trial`
OPTIONS (description = "TFB events v1: one row per event that is out and market - the scenario before it (the band of earlier moves, the chance of a big move, the median move after the same figure) against what happened: band hit, big-move Brier against any day's, direction and drift hits, and the Brier score of the family's figure chances - period 'forward' from 23 Nov 2026, 'backtest' before it. Observe only.")
AS
WITH s AS (
  SELECT *
  FROM `{{DS}}.v_event_scenarios`
  WHERE is_out
),
r AS (
  SELECT event_id, market_id, pre_day, event_close_day, r1_pct, r5_pct, f5_pct, z1, move, any_up_share, any_down_share
  FROM `{{DS}}.v_event_reactions`
  WHERE r1_pct IS NOT NULL
),
j AS (
  SELECT
    s.*, r.pre_day, r.event_close_day, r.r1_pct, r.r5_pct, r.f5_pct, r.z1, r.move,
    IF(r.move IS NULL, NULL, IF(r.move IN ('up', 'down'), 1, 0)) AS big,
    IF(r.move IS NULL OR r.any_up_share IS NULL, NULL, r.any_up_share + r.any_down_share) AS any_big,
    CASE s.figure
      WHEN 'above' THEN IF(s.n_above >= 4, SIGN(s.r1_p50_above), NULL)
      WHEN 'below' THEN IF(s.n_below >= 4, SIGN(s.r1_p50_below), NULL)
    END AS dir_sign,
    CASE s.figure
      WHEN 'above' THEN IF(s.n_f5_above >= 4, SIGN(s.f5_p50_above), NULL)
      WHEN 'below' THEN IF(s.n_f5_below >= 4, SIGN(s.f5_p50_below), NULL)
    END AS drift_sign
  FROM s
  JOIN r USING (event_id, market_id)
)
SELECT
  IF(event_at >= TIMESTAMP '2026-11-22 21:00:00+00', 'forward', 'backtest') AS period,
  event_id, family, family_name, kind, country, event_at, event_day, period AS release_period,
  actual, estimate, surprise_z, figure,
  market_order, market_id, market, n_prior, pre_day, event_close_day, r1_pct, r5_pct, f5_pct, z1, move,
  r1_p10, r1_p50, r1_p90,
  IF(n_prior >= 8, r1_pct BETWEEN r1_p10 AND r1_p90, NULL) AS band_hit,
  big,
  IF(n_move >= 8 AND big IS NOT NULL, p_big, NULL) AS p_big,
  IF(n_move >= 8 AND big IS NOT NULL, ROUND(any_big, 4), NULL) AS p_big_any,
  IF(n_move >= 8 AND big IS NOT NULL, ROUND(POW(p_big - big, 2), 6), NULL) AS brier_big,
  IF(n_move >= 8 AND big IS NOT NULL AND any_big IS NOT NULL, ROUND(POW(any_big - big, 2), 6), NULL) AS brier_any,
  IF(dir_sign = 0, NULL, CAST(dir_sign AS INT64)) AS dir_expected,
  IF(dir_sign IS NULL OR dir_sign = 0 OR r1_pct = 0, NULL, SIGN(r1_pct) = dir_sign) AS dir_hit,
  IF(drift_sign = 0, NULL, CAST(drift_sign AS INT64)) AS drift_expected,
  IF(drift_sign IS NULL OR drift_sign = 0 OR f5_pct IS NULL OR f5_pct = 0, NULL, SIGN(f5_pct) = drift_sign) AS drift_hit,
  n_fig, p_above, p_in_line, p_below,
  IF(kind = 'release' AND n_fig >= 8 AND figure IS NOT NULL,
     ROUND(POW(p_above - IF(figure = 'above', 1, 0), 2) + POW(p_in_line - IF(figure = 'in_line', 1, 0), 2)
           + POW(p_below - IF(figure = 'below', 1, 0), 2), 6),
     NULL) AS brier_figure
FROM j;
