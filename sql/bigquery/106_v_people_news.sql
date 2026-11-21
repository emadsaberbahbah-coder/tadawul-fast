-- v_people_news: the news of 17 people who move markets, day by day, against
-- each person's own previous 250 news days.
-- WHY (owner, 21 Nov 2026 11:03 Riyadh, item 5 of the build list): "News: a
-- news reading per market; forward event scenarios; momentum/surprise;
-- influential figures of 19 Nov; score every news signal" (the people were
-- part 4 of the 19 Nov stage) - "Everything starts in observe mode: shown and
-- logged, but it controls no buys."
-- THE NEWS (scripts/archive_news_themes.py 1.1.0, table news_entity_daily, the
-- GDELT Persons list, one UTC day at a time): docs / share_pct = the
-- documents naming the person and their share of the day's documents; tone =
-- their mean tone.
-- THE READING (rules fixed on 21 Nov 2026, before any forward session):
--   att_z, tone_z  the day against the person's previous 250 news days (at
--                  least 120 of them, else NULL), in standard deviations
--   reading        spike_positive / spike_negative: an unusual day of news
--                  (att_z >= 2 and at least 20 documents), its tone at or
--                  above the person's normal, or below it; quiet otherwise
--   markets        the markets (market_id of v_market_condition) the person
--                  is expected to move, fixed here with the rule: the trial
--                  (v_news_trial) scores a person only on those markets
-- The offices are as checked on 21 Nov 2026 (Fed chair Kevin Warsh since 22
-- May 2026; Mojtaba Khamenei Iran's leader since 8 Mar 2026 - the pattern
-- counts the family name, his father Ali included; Netanyahu caretaker and
-- Eisenkot asked to form a government on 7 Nov 2026).
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it. Observe only.
CREATE OR REPLACE VIEW `{{DS}}.v_people_news`
OPTIONS (description = "TFB news v1: one row per news day (UTC) and person - 17 people who move markets: the share and tone of the documents naming them against their own previous 250 news days, a spike flag (positive / negative tone) and the markets each is expected to move. Observe only.")
AS
WITH pp AS (
  SELECT
    REGEXP_EXTRACT(feature, r'^person_([a-z]+)_(?:share|tone)$') AS person_id,
    date,
    MAX(IF(ENDS_WITH(feature, '_share'), num, NULL)) AS docs,
    MAX(IF(ENDS_WITH(feature, '_share'), SAFE_DIVIDE(num, den) * 100, NULL)) AS share_pct,
    MAX(IF(ENDS_WITH(feature, '_tone'), SAFE_DIVIDE(num, den), NULL)) AS tone
  FROM `{{DS}}.news_entity_daily`
  WHERE REGEXP_CONTAINS(feature, r'^person_[a-z]+_(share|tone)$')
  GROUP BY person_id, date
),
st AS (
  SELECT
    pp.*,
    COUNT(pp.share_pct) OVER w AS hist_days,
    AVG(pp.share_pct) OVER w AS share_mean,
    STDDEV_SAMP(pp.share_pct) OVER w AS share_sd,
    AVG(pp.tone) OVER w AS tone_mean,
    STDDEV_SAMP(pp.tone) OVER w AS tone_sd
  FROM pp
  WINDOW w AS (PARTITION BY pp.person_id ORDER BY pp.date ROWS BETWEEN 250 PRECEDING AND 1 PRECEDING)
),
z AS (
  SELECT
    st.*,
    IF(st.hist_days >= 120, ROUND(SAFE_DIVIDE(st.share_pct - st.share_mean, NULLIF(st.share_sd, 0)), 3), NULL) AS att_z,
    IF(st.hist_days >= 120, ROUND(SAFE_DIVIDE(st.tone - st.tone_mean, NULLIF(st.tone_sd, 0)), 3), NULL) AS tone_z,
    ROUND(st.share_mean, 4) AS normal_share_pct
  FROM st
),
who AS (
  SELECT *
  FROM UNNEST([
    STRUCT(1 AS person_order, 'trump' AS person_id, 'Donald Trump' AS person,
           'President of the United States' AS role, 'us,cn,eu,jp,gold,oil,bonds' AS markets),
    STRUCT(2, 'warsh', 'Kevin Warsh', 'Chair of the Federal Reserve', 'us,bonds,gold,reits_us'),
    STRUCT(3, 'bessent', 'Scott Bessent', 'US Treasury Secretary', 'us,bonds,cn'),
    STRUCT(4, 'musk', 'Elon Musk', 'Tesla and SpaceX', 'us'),
    STRUCT(5, 'huang', 'Jensen Huang', 'Nvidia', 'us,cn'),
    STRUCT(6, 'altman', 'Sam Altman', 'OpenAI', 'us'),
    STRUCT(7, 'xi', 'Xi Jinping', 'President of China', 'cn,us'),
    STRUCT(8, 'mbs', 'Mohammed bin Salman', 'Crown Prince and Prime Minister of Saudi Arabia', 'sa,reits_sa,sukuk,oil'),
    STRUCT(9, 'abdulaziz', 'Abdulaziz bin Salman', 'Saudi Minister of Energy', 'oil,sa'),
    STRUCT(10, 'putin', 'Vladimir Putin', 'President of Russia', 'eu,oil,gold'),
    STRUCT(11, 'khamenei', 'Khamenei', 'Supreme Leader of Iran', 'oil,gold,sa'),
    STRUCT(12, 'netanyahu', 'Benjamin Netanyahu', 'Prime Minister of Israel (caretaker)', 'oil,gold,sa'),
    STRUCT(13, 'eisenkot', 'Gadi Eisenkot', 'Asked to form an Israeli government', 'oil,sa'),
    STRUCT(14, 'lagarde', 'Christine Lagarde', 'President of the European Central Bank', 'eu,bonds'),
    STRUCT(15, 'merz', 'Friedrich Merz', 'Chancellor of Germany', 'eu'),
    STRUCT(16, 'takaichi', 'Sanae Takaichi', 'Prime Minister of Japan', 'jp'),
    STRUCT(17, 'ueda', 'Kazuo Ueda', 'Governor of the Bank of Japan', 'jp,bonds')
  ])
)
SELECT
  z.date,
  w.person_id, w.person, w.role, w.person_order, w.markets,
  CAST(z.docs AS INT64) AS docs,
  ROUND(z.share_pct, 4) AS share_pct,
  z.normal_share_pct,
  ROUND(z.tone, 4) AS tone,
  z.hist_days,
  z.att_z, z.tone_z,
  CASE
    WHEN z.att_z IS NULL THEN NULL
    WHEN z.att_z >= 2 AND z.docs >= 20 THEN IF(COALESCE(z.tone_z, 0) >= 0, 'spike_positive', 'spike_negative')
    ELSE 'quiet'
  END AS reading
FROM z
JOIN who AS w ON w.person_id = z.person_id;
