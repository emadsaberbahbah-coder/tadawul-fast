-- v_market_news: the news of each market's place, day by day, against the
-- place's own previous 250 news days.
-- WHY (owner, 21 Nov 2026 11:03 Riyadh, item 5 of the build list): "News: a
-- news reading per market; forward event scenarios; momentum/surprise;
-- influential figures of 19 Nov; score every news signal" - "Everything
-- starts in observe mode: shown and logged, but it controls no buys."
-- THE PLACES (scripts/archive_news_themes.py 1.1.0, table news_entity_daily,
-- from the public GDELT tables, one UTC day at a time): us; sa; mideast
-- (Iran, Iraq, Yemen, Israel, Lebanon, Syria, Jordan, Kuwait, Qatar, the UAE,
-- Bahrain, Oman: the war zone around Saudi Arabia and the Gulf); eu (16
-- European countries); jp; cn (China and Hong Kong). For a place and a day:
-- docs / att_pct = the news documents naming the place and their share of the
-- day's documents; tone = their mean tone; fights / fight_pct = the events of
-- fighting (CAMEO 18-20) located in the place and their share of the day's
-- events.
-- THE READING (rules fixed on 21 Nov 2026, before any forward session):
--   att_z, tone_z, fight_z  the day against the place's previous 250 news days
--                           (at least 120 of them, else NULL), in standard
--                           deviations: (x - mean) / sd
--   news_score              (tone_z - fight_z) / 2: above 0 = a better-toned,
--                           less violent day than the place's normal
--   score_5                 the mean news_score of the last 5 news days (all 5
--                           known, else NULL)
--   momentum_5              score_5 against score_5 five news days before
-- THE MARKETS (market_id of v_market_condition -> its place, and the sign of
--   the expected effect: +1 better news, higher prices; -1 the reverse):
--   us, reits_us -> us +1; bonds -> us -1 (a flight to Treasuries);
--   sa, reits_sa, sukuk -> sa +1; eu -> eu +1; jp -> jp +1; cn -> cn +1;
--   gold, oil -> mideast -1 (a worse war zone: a safe haven, a supply risk).
--   market_score = sign x score_5, market_momentum = sign x momentum_5:
--   reading         favourable  market_score >= 0.5 | unfavourable <= -0.5 |
--                   neutral otherwise (NULL while score_5 is not known)
--   momentum_label  improving   market_momentum >= 0.5 | worsening <= -0.5 |
--                   steady otherwise
--   surprise        att_z >= 2 (an unusual day of attention):
--                   surge_favourable when sign x tone_z >= 0, else
--                   surge_unfavourable; none otherwise
-- A news day is a UTC day, complete when the next begins: v_news_trial reads
-- it only for decisions from the following day on.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it. Observe only.
CREATE OR REPLACE VIEW `{{DS}}.v_market_news`
OPTIONS (description = "TFB news v1: one row per news day (UTC) and market - the news of the market's place (the share and tone of the documents naming it, the fighting located there) against the place's own previous 250 news days, a 5-day news score signed for the market, its momentum and a surprise flag (favourable / neutral / unfavourable). Observe only.")
AS
WITH pl AS (
  SELECT
    COALESCE(REGEXP_EXTRACT(feature, r'^place_([a-z]+)_(?:share|tone)$'),
             REGEXP_EXTRACT(feature, r'^fight_([a-z]+)$')) AS place,
    date,
    MAX(IF(ENDS_WITH(feature, '_share'), num, NULL)) AS docs,
    MAX(IF(ENDS_WITH(feature, '_share'), SAFE_DIVIDE(num, den) * 100, NULL)) AS att_pct,
    MAX(IF(ENDS_WITH(feature, '_tone'), SAFE_DIVIDE(num, den), NULL)) AS tone,
    MAX(IF(STARTS_WITH(feature, 'fight_'), num, NULL)) AS fights,
    MAX(IF(STARTS_WITH(feature, 'fight_'), SAFE_DIVIDE(num, den) * 100, NULL)) AS fight_pct
  FROM `{{DS}}.news_entity_daily`
  WHERE REGEXP_CONTAINS(feature, r'^(place_[a-z]+_(share|tone)|fight_[a-z]+)$')
  GROUP BY place, date
),
st AS (
  SELECT
    pl.*,
    COUNT(pl.att_pct) OVER w AS hist_days,
    AVG(pl.att_pct) OVER w AS att_mean,
    STDDEV_SAMP(pl.att_pct) OVER w AS att_sd,
    AVG(pl.tone) OVER w AS tone_mean,
    STDDEV_SAMP(pl.tone) OVER w AS tone_sd,
    AVG(pl.fight_pct) OVER w AS fight_mean,
    STDDEV_SAMP(pl.fight_pct) OVER w AS fight_sd
  FROM pl
  WINDOW w AS (PARTITION BY pl.place ORDER BY pl.date ROWS BETWEEN 250 PRECEDING AND 1 PRECEDING)
),
z AS (
  SELECT
    st.*,
    IF(st.hist_days >= 120, ROUND(SAFE_DIVIDE(st.att_pct - st.att_mean, NULLIF(st.att_sd, 0)), 3), NULL) AS att_z,
    IF(st.hist_days >= 120, ROUND(SAFE_DIVIDE(st.tone - st.tone_mean, NULLIF(st.tone_sd, 0)), 3), NULL) AS tone_z,
    IF(st.hist_days >= 120, ROUND(SAFE_DIVIDE(st.fight_pct - st.fight_mean, NULLIF(st.fight_sd, 0)), 3), NULL) AS fight_z
  FROM st
),
s AS (
  SELECT z.*, ROUND((z.tone_z - z.fight_z) / 2, 3) AS news_score
  FROM z
),
s5 AS (
  SELECT
    s.*,
    IF(COUNT(s.news_score) OVER w5 = 5, ROUND(AVG(s.news_score) OVER w5, 3), NULL) AS score_5
  FROM s
  WINDOW w5 AS (PARTITION BY s.place ORDER BY s.date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
),
m AS (
  SELECT
    s5.*,
    ROUND(s5.score_5 - LAG(s5.score_5, 5) OVER (PARTITION BY s5.place ORDER BY s5.date), 3) AS momentum_5
  FROM s5
),
mk AS (
  SELECT *
  FROM UNNEST([
    STRUCT(1 AS market_order, 'us' AS market_id, 'United States' AS market, 'us' AS place,
           'United States' AS place_name, 1 AS sign),
    STRUCT(2, 'sa', 'Saudi Arabia', 'sa', 'Saudi Arabia', 1),
    STRUCT(3, 'eu', 'Europe', 'eu', 'Europe', 1),
    STRUCT(4, 'jp', 'Japan', 'jp', 'Japan', 1),
    STRUCT(5, 'cn', 'China / Hong Kong', 'cn', 'China / Hong Kong', 1),
    STRUCT(6, 'gold', 'Gold', 'mideast', 'The war zone around the Gulf', -1),
    STRUCT(7, 'oil', 'Oil', 'mideast', 'The war zone around the Gulf', -1),
    STRUCT(8, 'bonds', 'Bonds', 'us', 'United States', -1),
    STRUCT(9, 'sukuk', 'Sukuk', 'sa', 'Saudi Arabia', 1),
    STRUCT(10, 'reits_us', 'US REITs', 'us', 'United States', 1),
    STRUCT(11, 'reits_sa', 'Saudi REITs', 'sa', 'Saudi Arabia', 1)
  ])
),
j AS (
  SELECT
    m.*,
    k.market_order, k.market_id, k.market, k.place_name, k.sign,
    ROUND(k.sign * m.score_5, 3) AS market_score,
    ROUND(k.sign * m.momentum_5, 3) AS market_momentum
  FROM m
  JOIN mk AS k ON k.place = m.place
)
SELECT
  j.date,
  j.market_id, j.market, j.market_order, j.place, j.place_name, j.sign,
  CAST(j.docs AS INT64) AS docs,
  ROUND(j.att_pct, 4) AS att_pct,
  ROUND(j.tone, 4) AS tone,
  CAST(j.fights AS INT64) AS fights,
  ROUND(j.fight_pct, 4) AS fight_pct,
  j.hist_days,
  j.att_z, j.tone_z, j.fight_z, j.news_score, j.score_5, j.momentum_5,
  j.market_score, j.market_momentum,
  CASE
    WHEN j.market_score IS NULL THEN NULL
    WHEN j.market_score >= 0.5 THEN 'favourable'
    WHEN j.market_score <= -0.5 THEN 'unfavourable'
    ELSE 'neutral'
  END AS reading,
  CASE
    WHEN j.market_momentum IS NULL THEN NULL
    WHEN j.market_momentum >= 0.5 THEN 'improving'
    WHEN j.market_momentum <= -0.5 THEN 'worsening'
    ELSE 'steady'
  END AS momentum_label,
  CASE
    WHEN j.att_z IS NULL THEN NULL
    WHEN j.att_z >= 2 THEN IF(j.sign * COALESCE(j.tone_z, 0) >= 0, 'surge_favourable', 'surge_unfavourable')
    ELSE 'none'
  END AS surprise
FROM j;
