-- v_event_calendar: one row per event that can move the markets - the policy
-- meetings of the Fed, the ECB, the Bank of Japan and OPEC+ on their official
-- dates, and the economic releases of the families below from econ_calendar -
-- with the figure, its forecast, and how far the figure was from the forecast
-- in units of the family's own earlier misses.
-- WHY (owner, 21 Nov 2026 11:03 Riyadh, item 5 of the build list): "News: a
-- news reading per market; forward event scenarios; momentum/surprise;
-- influential figures of 19 Nov; score every news signal." The scenarios
-- (v_event_scenarios) read each event's earlier events of the same family.
-- THE MEETINGS (the decision day; written 21 Nov 2026 from the official pages):
--   fomc  the Fed: federalreserve.gov, FOMC calendars 2023-2027; 2027 as
--         revised on 12 Nov 2026 (no decision on 27 Oct; December moved to
--         16-17 Nov). Read at 14:00 New York.
--   ecb   the ECB: ecb.europa.eu, Governing Council monetary policy meetings,
--         the second day. Read at 14:15 Frankfurt (the decision).
--   boj   the Bank of Japan: boj.or.jp, Monetary Policy Meetings, the second
--         day. Read at 12:00 Tokyo: the decision has no fixed time (about
--         11:30 to 13:00), and every market read here closes later that day.
--   opec  OPEC+: opec.org press releases - the ministerial meetings and the
--         monthly decisions of the countries with voluntary cuts (eight, seven
--         since May 2026), and the scheduled 29 Nov 2026. Read at 12:00 UTC
--         (the statements come out about 10:00 to 13:00 UTC); a weekend
--         meeting meets each market on its next session.
--   A meeting carries EODHD's rate decision of the same time (within 36
--   hours) when there is one: the new rate, the forecast and the previous
--   rate, in %.
-- THE RELEASES: the latest row of each release in econ_calendar that EODHD
--   still lists (status <> 'gone'), ONE PER RELEASE: EODHD lists some
--   releases twice, from two feeds, with the period written "Oct" and
--   "Oct. 2026" (seen 21 Nov 2026 on US retail sales and Saudi CPI), and
--   lists a first estimate with its revisions under the same period (US GDP:
--   advance, second, third; a flash and its final). So the period is read
--   without its year, and the releases of a family with the same period
--   less than 120 days apart are one release: the earliest with a figure
--   and a forecast, else the earliest with a figure, else the earliest.
--   A family's same period comes back only a year later, so a year is never
--   merged; without a period, one release per UTC day. The families
--   (EODHD's own country, event and comparison):
--   us_cpi     US Core Inflation Rate, mom      us_pce   US Core PCE Price Index, mom
--   us_jobs    US Non Farm Payrolls             us_retail US Retail Sales, mom
--   us_ism     US ISM Manufacturing PMI         us_ism_svc US ISM Services PMI
--   us_gdp     US GDP Growth Rate, qoq          eu_cpi   EU Inflation Rate, yoy
--   jp_cpi     JP Core Inflation Rate, yoy      cn_cpi   CN Inflation Rate, yoy
--   cn_pmi     CN NBS Manufacturing PMI         cn_gdp   CN GDP Growth Rate, yoy
--   sa_cpi     SA Inflation Rate, yoy           sa_pmi   SA PMI (Riyad Bank)
--   sa_gdp     SA GDP Growth Rate, yoy
-- THE SURPRISE: surprise = actual - estimate; surprise_z = surprise / the
--   standard deviation of the family's surprises over its previous 36 events
--   (at least 6; NULL before): a miss is read against what is usual for that
--   release, from earlier events only. figure = 'above' (z >= 0.5), 'below'
--   (z <= -0.5) or 'in_line'; with no z yet, the sign of the surprise (0 =
--   in_line); NULL without a figure and a forecast (every OPEC+ meeting).
-- COLUMNS: event_id (a release: its econ_calendar key; a meeting:
--   family:YYYY-MM-DD), family, family_name, kind (meeting | release),
--   country, event_at (UTC), event_day (Riyadh), source (official | eodhd),
--   event_type, period, actual, estimate, previous, surprise, surprise_z,
--   figure, is_out (the figure is out, or the meeting's time has passed)
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it. Observe only.
CREATE OR REPLACE VIEW `{{DS}}.v_event_calendar`
OPTIONS (description = "TFB events v1: one row per market-moving event - the Fed, ECB, Bank of Japan and OPEC+ meetings on their official dates and the tracked economic releases of the US, the euro area, Japan, China and Saudi Arabia (econ_calendar) - with the figure, the forecast and the surprise in units of the family's earlier misses (above / in line / below). Observe only.")
AS
WITH fam AS (
  SELECT *
  FROM UNNEST([
    STRUCT('us_cpi' AS family, 'US core CPI (m/m)' AS family_name, 'US' AS country,
           r'^Core Inflation Rate$' AS type_re, 'mom' AS comparison),
    STRUCT('us_pce', 'US core PCE prices (m/m)', 'US', r'^Core PCE Price Index$', 'mom'),
    STRUCT('us_jobs', 'US nonfarm payrolls', 'US', r'^Non Farm Payrolls$', ''),
    STRUCT('us_retail', 'US retail sales (m/m)', 'US', r'^Retail Sales$', 'mom'),
    STRUCT('us_ism', 'US ISM manufacturing', 'US', r'^ISM Manufacturing PMI$', ''),
    STRUCT('us_ism_svc', 'US ISM services', 'US', r'^ISM (Services|Non-Manufacturing) PMI$', ''),
    STRUCT('us_gdp', 'US GDP (q/q)', 'US', r'^GDP Growth Rate$', 'qoq'),
    STRUCT('eu_cpi', 'Euro area inflation (y/y)', 'EU', r'^Inflation Rate$', 'yoy'),
    STRUCT('jp_cpi', 'Japan core inflation (y/y)', 'JP', r'^Core Inflation Rate$', 'yoy'),
    STRUCT('cn_cpi', 'China inflation (y/y)', 'CN', r'^Inflation Rate$', 'yoy'),
    STRUCT('cn_pmi', 'China official manufacturing PMI', 'CN', r'^NBS Manufacturing PMI$', ''),
    STRUCT('cn_gdp', 'China GDP (y/y)', 'CN', r'^GDP Growth Rate$', 'yoy'),
    STRUCT('sa_cpi', 'Saudi inflation (y/y)', 'SA', r'^Inflation Rate$', 'yoy'),
    STRUCT('sa_pmi', 'Saudi PMI', 'SA', r'^(Riyad Bank )?PMI$', ''),
    STRUCT('sa_gdp', 'Saudi GDP (y/y)', 'SA', r'^GDP Growth Rate$', 'yoy')
  ])
),
body AS (
  SELECT *
  FROM UNNEST([
    STRUCT('fomc' AS family, 'Fed rate decision' AS family_name, 'US' AS country,
           r'^Fed Interest Rate Decision$' AS rate_re),
    STRUCT('ecb', 'ECB rate decision', 'EU', r'^(ECB Interest Rate Decision|Deposit Facility Rate|ECB Deposit Facility Rate)$'),
    STRUCT('boj', 'Bank of Japan rate decision', 'JP', r'^BoJ Interest Rate Decision$'),
    STRUCT('opec', 'OPEC+ meeting', 'OPEC', CAST(NULL AS STRING))
  ])
),
meet AS (
  SELECT 'fomc' AS family, d AS day, TIMESTAMP(DATETIME(d, TIME '14:00:00'), 'America/New_York') AS event_at
  FROM UNNEST([
    DATE '2023-02-01', DATE '2023-03-22', DATE '2023-05-03', DATE '2023-06-14', DATE '2023-07-26',
    DATE '2023-09-20', DATE '2023-11-01', DATE '2023-12-13',
    DATE '2024-01-31', DATE '2024-03-20', DATE '2024-05-01', DATE '2024-06-12', DATE '2024-07-31',
    DATE '2024-09-18', DATE '2024-11-07', DATE '2024-12-18',
    DATE '2025-01-29', DATE '2025-03-19', DATE '2025-05-07', DATE '2025-06-18', DATE '2025-07-30',
    DATE '2025-09-17', DATE '2025-10-29', DATE '2025-12-10',
    DATE '2026-01-28', DATE '2026-03-18', DATE '2026-04-29', DATE '2026-06-17', DATE '2026-07-29',
    DATE '2026-09-16', DATE '2026-10-28', DATE '2026-12-09',
    DATE '2027-01-27', DATE '2027-03-17', DATE '2027-04-28', DATE '2027-06-09', DATE '2027-07-28',
    DATE '2027-09-15', DATE '2027-11-17'
  ]) AS d
  UNION ALL
  SELECT 'ecb', d, TIMESTAMP(DATETIME(d, TIME '14:15:00'), 'Europe/Berlin')
  FROM UNNEST([
    DATE '2023-02-02', DATE '2023-03-16', DATE '2023-05-04', DATE '2023-06-15', DATE '2023-07-27',
    DATE '2023-09-14', DATE '2023-10-26', DATE '2023-12-14',
    DATE '2024-01-25', DATE '2024-03-07', DATE '2024-04-11', DATE '2024-06-06', DATE '2024-07-18',
    DATE '2024-09-12', DATE '2024-10-17', DATE '2024-12-12',
    DATE '2025-01-30', DATE '2025-03-06', DATE '2025-04-17', DATE '2025-06-05', DATE '2025-07-24',
    DATE '2025-09-11', DATE '2025-10-30', DATE '2025-12-18',
    DATE '2026-02-05', DATE '2026-03-19', DATE '2026-04-30', DATE '2026-06-11', DATE '2026-07-23',
    DATE '2026-09-10', DATE '2026-10-29', DATE '2026-12-17',
    DATE '2027-02-04', DATE '2027-03-18', DATE '2027-04-29', DATE '2027-06-10', DATE '2027-07-22',
    DATE '2027-09-09', DATE '2027-10-28', DATE '2027-12-16'
  ]) AS d
  UNION ALL
  SELECT 'boj', d, TIMESTAMP(DATETIME(d, TIME '12:00:00'), 'Asia/Tokyo')
  FROM UNNEST([
    DATE '2023-01-18', DATE '2023-03-10', DATE '2023-04-28', DATE '2023-06-16', DATE '2023-07-28',
    DATE '2023-09-22', DATE '2023-10-31', DATE '2023-12-19',
    DATE '2024-01-23', DATE '2024-03-19', DATE '2024-04-26', DATE '2024-06-14', DATE '2024-07-31',
    DATE '2024-09-20', DATE '2024-10-31', DATE '2024-12-19',
    DATE '2025-01-24', DATE '2025-03-19', DATE '2025-05-01', DATE '2025-06-17', DATE '2025-07-31',
    DATE '2025-09-19', DATE '2025-10-30', DATE '2025-12-19',
    DATE '2026-01-23', DATE '2026-03-19', DATE '2026-04-28', DATE '2026-06-16', DATE '2026-07-31',
    DATE '2026-09-18', DATE '2026-10-30', DATE '2026-12-18',
    DATE '2027-01-22', DATE '2027-03-18', DATE '2027-04-28', DATE '2027-06-11', DATE '2027-07-22',
    DATE '2027-09-22', DATE '2027-10-29', DATE '2027-12-17'
  ]) AS d
  UNION ALL
  SELECT 'opec', d, TIMESTAMP(DATETIME(d, TIME '12:00:00'), 'UTC')
  FROM UNNEST([
    DATE '2023-04-02', DATE '2023-06-04', DATE '2023-11-30',
    DATE '2024-03-03', DATE '2024-06-02', DATE '2024-09-05', DATE '2024-11-03', DATE '2024-12-05',
    DATE '2025-03-03', DATE '2025-04-03', DATE '2025-05-03', DATE '2025-05-28', DATE '2025-05-31',
    DATE '2025-07-05', DATE '2025-08-03', DATE '2025-09-07', DATE '2025-10-05', DATE '2025-11-02',
    DATE '2025-11-30',
    DATE '2026-01-04', DATE '2026-02-01', DATE '2026-03-01', DATE '2026-04-05', DATE '2026-05-03',
    DATE '2026-06-07', DATE '2026-07-05', DATE '2026-08-02', DATE '2026-09-06', DATE '2026-10-04',
    DATE '2026-11-01', DATE '2026-11-29'
  ]) AS d
),
latest AS (
  SELECT *
  FROM `{{DS}}.econ_calendar`
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_key ORDER BY fetched_at DESC, status) = 1
),
live AS (
  SELECT *
  FROM latest
  WHERE status != 'gone'
),
rel0 AS (
  SELECT
    l.event_key AS event_id, f.family, f.family_name, 'release' AS kind, l.country, l.event_at,
    'eodhd' AS source, l.event_type, l.period, l.actual, l.estimate, l.previous, l.fetched_at,
    -- the period without its year: "Oct. 2026" and "Oct" are the same release
    REGEXP_REPLACE(TRIM(IFNULL(l.period, '')), r'[.,]?\s*(19|20)[0-9]{2}$', '') AS per
  FROM live AS l
  JOIN fam AS f
    ON f.country = l.country AND REGEXP_CONTAINS(l.event_type, f.type_re) AND IFNULL(l.comparison, '') = f.comparison
),
rel1 AS (
  SELECT
    *,
    -- a new release unless the family listed the same period less than 120 days before
    IF(per != '' AND TIMESTAMP_DIFF(event_at, LAG(event_at) OVER (PARTITION BY family, per ORDER BY event_at, event_id), DAY) < 120,
       0, 1) AS starts
  FROM rel0
),
rel2 AS (
  SELECT *, SUM(starts) OVER (PARTITION BY family, per ORDER BY event_at, event_id) AS grp
  FROM rel1
),
rel AS (
  SELECT event_id, family, family_name, kind, country, event_at, source, event_type, period, actual, estimate, previous
  FROM rel2
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY family, per, IF(per = '', CAST(DATE(event_at) AS STRING), CAST(grp AS STRING))
                             ORDER BY IF(actual IS NULL, 2, IF(estimate IS NULL, 1, 0)), event_at, fetched_at DESC, event_id) = 1
),
rate AS (
  SELECT m.family, m.day, l.event_type, l.actual, l.estimate, l.previous
  FROM meet AS m
  JOIN body AS b USING (family)
  JOIN live AS l
    ON l.country = b.country AND REGEXP_CONTAINS(l.event_type, b.rate_re)
   AND ABS(TIMESTAMP_DIFF(l.event_at, m.event_at, HOUR)) <= 36
  WHERE b.rate_re IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY m.family, m.day
                             ORDER BY ABS(TIMESTAMP_DIFF(l.event_at, m.event_at, MINUTE)), l.event_type) = 1
),
mt AS (
  SELECT
    CONCAT(m.family, ':', CAST(m.day AS STRING)) AS event_id, m.family, b.family_name, 'meeting' AS kind,
    b.country, m.event_at, 'official' AS source, r.event_type, CAST(NULL AS STRING) AS period,
    r.actual, r.estimate, r.previous
  FROM meet AS m
  JOIN body AS b USING (family)
  LEFT JOIN rate AS r USING (family, day)
),
ev AS (
  SELECT * FROM rel
  UNION ALL
  SELECT * FROM mt
),
z AS (
  SELECT
    ev.*,
    ev.actual - ev.estimate AS surprise,
    STDDEV_SAMP(ev.actual - ev.estimate) OVER w AS sd,
    COUNT(ev.actual - ev.estimate) OVER w AS n_sd
  FROM ev
  WINDOW w AS (PARTITION BY ev.family ORDER BY ev.event_at, ev.event_id ROWS BETWEEN 36 PRECEDING AND 1 PRECEDING)
)
SELECT
  event_id, family, family_name, kind, country, event_at, DATE(event_at, 'Asia/Riyadh') AS event_day, source,
  event_type, period, actual, estimate, previous,
  ROUND(surprise, 6) AS surprise,
  IF(n_sd >= 6 AND sd > 0, ROUND(surprise / sd, 3), NULL) AS surprise_z,
  CASE
    WHEN surprise IS NULL THEN NULL
    WHEN n_sd >= 6 AND sd > 0 THEN
      CASE WHEN surprise / sd >= 0.5 THEN 'above' WHEN surprise / sd <= -0.5 THEN 'below' ELSE 'in_line' END
    WHEN surprise > 0 THEN 'above'
    WHEN surprise < 0 THEN 'below'
    ELSE 'in_line'
  END AS figure,
  IF(kind = 'meeting', event_at <= CURRENT_TIMESTAMP(), actual IS NOT NULL) AS is_out
FROM z;
