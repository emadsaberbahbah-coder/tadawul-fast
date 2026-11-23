-- v_news_understanding: one row per US stock TFB follows the company news of - what its news said lately, how the
-- price answered, the trend it came in on, what research expects next and how that reading has done so far, beside
-- TFB's own view of the stock and its projection context. It is the reading of v_news_trend_daily for today.
-- WHY (owner, 23 Nov 2026 12:34 Riyadh): "deep understanding of information ... analysis news ... using data and
-- information and in same time use strategy and trend". A headline alone carried no edge in TFB's record
-- (v_company_news_eval); this row puts the headline next to the price's answer, the trend, TFB's view and the
-- market's state, so the strategist and the owner read them together.
-- THE ROW, per symbol:
--   latest_event_date, state, expected_move, reaction_pp / reaction_z, trend_before, trend_fit, top_event and
--   top_headline   the latest non-quiet session of the last 5 US sessions (else state 'quiet lately')
--   headlines      the top headline of each of the latest 3 sessions with company news in the last 10 sessions
--   news_items_5s, news_score_5s   the company news of the last 5 sessions
--   trend_now      the trend before the latest session (50-session average and 20-session change)
--   tfb_stance     +1 for a BUY-type recommendation, -1 for SELL / STRONG_SELL / REDUCE / AVOID, 0 for HOLD
--   agreement      how the expected move sits against TFB's view: 'agrees with TFB', 'leans against TFB',
--                  'TFB holds, the news leans up' / '... down', 'no lean from the news'
--   record_history / record_forward   the state's 5-session record in v_news_trend_learning (trend fit 'all'):
--                  its edge and its reading, so a state that has not proved itself says so
--   range_1m_low / range_1m_high, market_adverse_states   from v_projection_context
--   reading        all of it in one line
-- Research only: no decision, gate or count reads this view. Nothing here is advice.
CREATE OR REPLACE VIEW `{{DS}}.v_news_understanding`
OPTIONS (description = "TFB news understanding (views 1.25.0): per US stock with company news - the latest news-and-trend state of the last 5 sessions (news the price agreed with, refused or has not answered, big moves without news), the latest headlines, the trend, the move research expects and that state's record so far, TFB's own view and the agreement between them, the one-month range and the market's adverse states, in one reading line - research only")
AS
WITH t AS (
  SELECT * FROM `{{DS}}.v_news_trend_daily`
),
sessions AS (
  SELECT session_date, ROW_NUMBER() OVER (ORDER BY session_date DESC) AS k
  FROM (SELECT DISTINCT session_date FROM t)
),
recent AS (
  SELECT t.*, s.k
  FROM t
  JOIN sessions s USING (session_date)
  WHERE s.k <= 10
),
latest_event AS (
  SELECT *
  FROM recent
  WHERE k <= 5 AND state != 'quiet'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY session_date DESC, ABS(news_score) DESC) = 1
),
per_symbol AS (
  SELECT symbol,
    SUM(IF(k <= 5, news_items, 0)) AS news_items_5s,
    ROUND(SUM(IF(k <= 5, news_score, 0)), 2) AS news_score_5s,
    ARRAY_TO_STRING(ARRAY_AGG(IF(news_items > 0 AND top_headline IS NOT NULL,
                                 FORMAT('%t %s: %s', session_date, IFNULL(top_event, 'news'), top_headline), NULL)
                              IGNORE NULLS ORDER BY session_date DESC LIMIT 3), ' | ') AS headlines,
    ARRAY_AGG(trend ORDER BY session_date DESC LIMIT 1)[OFFSET(0)] AS trend_now,
    MAX(session_date) AS last_session
  FROM recent
  GROUP BY symbol
),
rec AS (
  SELECT state,
    MAX(IF(STARTS_WITH(period, 'history'), FORMAT('%+.2f pp, %s (%d events)', edge_pp, reading, events), NULL))
      AS record_history,
    MAX(IF(STARTS_WITH(period, 'forward'), FORMAT('%+.2f pp, %s (%d events)', edge_pp, reading, events), NULL))
      AS record_forward
  FROM `{{DS}}.v_news_trend_learning`
  WHERE trend_fit = 'all' AND horizon_sessions = 5
  GROUP BY state
),
u AS (
  SELECT symbol, name, sector, recommendation, final_action, overall_score, expected_roi_1m_pct,
    CASE
      WHEN recommendation IN ('BUY', 'STRONG_BUY', 'ACCUMULATE', 'ADD') THEN 1
      WHEN recommendation IN ('SELL', 'STRONG_SELL', 'REDUCE', 'AVOID', 'EXIT') THEN -1
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
    e.session_date AS latest_event_date,
    IFNULL(e.state, 'quiet lately') AS state,
    IFNULL(e.expected, 0) AS expected,
    e.reaction_pp, e.reaction_z, e.day_pp, e.day_z, e.reaction_complete, e.volume_ratio,
    e.trend AS trend_before, e.trend_fit,
    e.top_event, e.top_headline,
    p.news_items_5s, p.news_score_5s, p.headlines, p.trend_now,
    u.recommendation, u.final_action, u.overall_score, u.expected_roi_1m_pct, u.tfb_stance,
    r.record_history, r.record_forward,
    c.range_1m_low, c.range_1m_high, c.market_adverse_states
  FROM per_symbol p
  LEFT JOIN latest_event e USING (symbol)
  LEFT JOIN u USING (symbol)
  LEFT JOIN rec r ON r.state = e.state
  LEFT JOIN `{{DS}}.v_projection_context` c ON c.symbol = p.symbol
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
      FORMAT('Trend before: %s (%s). Research expects it to move %s against the market. Record so far: history %s%s. TFB: %s, so the news %s.',
        IFNULL(trend_before, 'unknown'), IFNULL(trend_fit, 'no clear trend'),
        CASE WHEN expected > 0 THEN 'up' WHEN expected < 0 THEN 'down' ELSE 'either way' END,
        IFNULL(record_history, 'none yet'),
        IF(record_forward IS NULL, '', CONCAT(' | new sessions ', record_forward)),
        IFNULL(recommendation, 'no view'),
        CASE
          WHEN expected = 0 THEN 'gives no lean'
          WHEN tfb_stance IS NULL OR tfb_stance = 0 THEN IF(expected > 0, 'leans up', 'leans down')
          WHEN tfb_stance = expected THEN 'agrees'
          ELSE 'leans against it'
        END))
  END AS reading
FROM x;
