-- v_news_understanding: one row per US stock TFB follows the company news of - what its news said lately, how the
-- price answered, the trend it came in on and what research expects next, beside TFB's own view of the stock. It is
-- the reading of v_news_trend_daily for today; each state's record is v_news_trend_learning.
-- WHY (owner, 23 Nov 2026 12:34 Riyadh): "deep understanding of information ... analysis news ... using data and
-- information and in same time use strategy and trend". A headline alone carried no edge in TFB's record
-- (v_company_news_eval); this row puts the headline next to the price's answer, the trend and TFB's view, so the
-- strategist and the owner read them together.
-- KEPT LEAN ON PURPOSE (23 Nov 2026, after the first deploy): a view that also joined v_news_trend_learning and
-- v_projection_context expanded v_news_trend_daily twice beside the market views, and BigQuery refused to plan it
-- ("too many subqueries or query is too complex"). The record and the projection are read beside this view instead
-- (the strategist's price_drivers tool reads all three).
-- THE ROW, per symbol:
--   latest_event_date, state, expected_move, reaction_pp / reaction_z, trend_before, trend_fit, top_event and
--   top_headline   the latest non-quiet session of the last 5 US sessions (else state 'quiet lately')
--   headlines      the top headline of each of the latest 3 sessions with company news in the last 10 sessions
--   news_items_5s, news_score_5s   the company news of the last 5 sessions
--   trend_now      the trend before the latest session (50-session average and 20-session change)
--   tfb_stance     +1 for a BUY-type recommendation, -1 for SELL / STRONG SELL / REDUCE / AVOID (spaces, hyphens
--                  and underscores alike), 0 for HOLD, NULL when TFB has no recommendation
--   agreement      how the expected move sits against TFB's view: 'agrees with TFB', 'leans against TFB',
--                  'TFB holds, the news leans up' / '... down', 'no lean from the news'
--   reading        all of it in one line
-- Research only: no decision, gate or count reads this view. Nothing here is advice.
CREATE OR REPLACE VIEW `{{DS}}.v_news_understanding`
OPTIONS (description = "TFB news understanding (views 1.25.1): per US stock with company news - the latest news-and-trend state of the last 5 sessions (news the price agreed with, refused or has not answered, big moves without news), the latest headlines, the trend, the move research expects, TFB's own view and the agreement between them, in one reading line (each state's record is v_news_trend_learning) - research only")
AS
WITH recent AS (
  -- one pass over the daily view (each extra reference would expand it again)
  SELECT *
  FROM (
    SELECT t.*, DENSE_RANK() OVER (ORDER BY t.session_date DESC) AS k
    FROM `{{DS}}.v_news_trend_daily` t
  )
  WHERE k <= 10
),
per_symbol AS (
  SELECT symbol,
    SUM(IF(k <= 5, news_items, 0)) AS news_items_5s,
    ROUND(SUM(IF(k <= 5, news_score, 0)), 2) AS news_score_5s,
    ARRAY_TO_STRING(ARRAY_AGG(IF(news_items > 0 AND top_headline IS NOT NULL,
                                 FORMAT('%t %s: %s', session_date, IFNULL(top_event, 'news'), top_headline), NULL)
                              IGNORE NULLS ORDER BY session_date DESC LIMIT 3), ' | ') AS headlines,
    ARRAY_AGG(trend ORDER BY session_date DESC LIMIT 1)[OFFSET(0)] AS trend_now,
    MAX(session_date) AS last_session,
    -- the latest non-quiet session of the last 5
    ARRAY_AGG(IF(k <= 5 AND state != 'quiet',
                 STRUCT(session_date, state, expected, reaction_pp, reaction_z, day_pp, day_z, reaction_complete,
                        volume_ratio, trend, trend_fit, top_event, top_headline), NULL)
              IGNORE NULLS ORDER BY session_date DESC, ABS(news_score) DESC LIMIT 1)[SAFE_OFFSET(0)] AS e
  FROM recent
  GROUP BY symbol
),
u AS (
  SELECT symbol, name, sector, recommendation, final_action, overall_score, expected_roi_1m_pct,
    -- 'STRONG BUY' and 'STRONG_BUY' are the same view (Codex P2 on #1065): spaces and hyphens read as underscores
    CASE
      WHEN REGEXP_REPLACE(UPPER(TRIM(recommendation)), r'[ -]+', '_') IN ('BUY', 'STRONG_BUY', 'ACCUMULATE', 'ADD') THEN 1
      WHEN REGEXP_REPLACE(UPPER(TRIM(recommendation)), r'[ -]+', '_') IN ('SELL', 'STRONG_SELL', 'REDUCE', 'AVOID', 'EXIT') THEN -1
      WHEN recommendation IS NULL THEN NULL
      ELSE 0
    END AS tfb_stance
  FROM `{{DS}}.v_universe_latest`
  WHERE market_code = 'US'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY captured_at DESC) = 1
),
x AS (
  SELECT
    p.symbol, u.name, u.sector,
    p.last_session,
    p.e.session_date AS latest_event_date,
    IFNULL(p.e.state, 'quiet lately') AS state,
    IFNULL(p.e.expected, 0) AS expected,
    p.e.reaction_pp, p.e.reaction_z, p.e.day_pp, p.e.day_z, p.e.reaction_complete, p.e.volume_ratio,
    p.e.trend AS trend_before, p.e.trend_fit,
    p.e.top_event, p.e.top_headline,
    p.news_items_5s, p.news_score_5s, p.headlines, p.trend_now,
    u.recommendation, u.final_action, u.overall_score, u.expected_roi_1m_pct, u.tfb_stance
  FROM per_symbol p
  LEFT JOIN u USING (symbol)
)
SELECT
  x.*,
  CASE
    WHEN expected = 0 THEN 'no lean from the news'
    WHEN tfb_stance IS NULL THEN 'TFB has no view today'
    WHEN tfb_stance = 0 THEN IF(expected > 0, 'TFB holds, the news leans up', 'TFB holds, the news leans down')
    WHEN tfb_stance = expected THEN 'agrees with TFB'
    ELSE 'leans against TFB'
  END AS agreement,
  CASE
    WHEN latest_event_date IS NULL THEN FORMAT(
      'Quiet lately: no company news state in the last 5 sessions (%d items, score %+.1f). Trend %s. TFB: %s.',
      IFNULL(news_items_5s, 0), IFNULL(news_score_5s, 0), IFNULL(trend_now, 'unknown'), IFNULL(recommendation, 'no view'))
    ELSE CONCAT(
      FORMAT('%t: %s ', latest_event_date, state),
      IF(STARTS_WITH(state, 'big '),
         FORMAT('(no company news, the day %+.1f pp against SPY, %+.1f times its usual daily move',
                IFNULL(day_pp, 0), IFNULL(day_z, 0)),
         FORMAT('(%s%s, price %+.1f pp against SPY over two sessions, z %+.1f',
                IFNULL(top_event, 'news'), IF(top_headline IS NULL, '', CONCAT(' - ', SUBSTR(top_headline, 1, 90))),
                IFNULL(reaction_pp, 0), IFNULL(reaction_z, 0))),
      IF(reaction_complete, '). ', ', not complete yet). '),
      FORMAT('Trend before: %s (%s). Research expects it to move %s against the market (its record: v_news_trend_learning). TFB: %s, so the news %s.',
        IFNULL(trend_before, 'unknown'), IFNULL(trend_fit, 'no clear trend'),
        CASE WHEN expected > 0 THEN 'up' WHEN expected < 0 THEN 'down' ELSE 'either way' END,
        IFNULL(recommendation, 'no view'),
        CASE
          WHEN expected = 0 THEN 'gives no lean'
          WHEN tfb_stance IS NULL OR tfb_stance = 0 THEN IF(expected > 0, 'leans up', 'leans down')
          WHEN tfb_stance = expected THEN 'agrees'
          ELSE 'leans against it'
        END))
  END AS reading
FROM x;
