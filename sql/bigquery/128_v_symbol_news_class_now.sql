-- v_symbol_news_class_now: the headlines the hourly news job (scripts/news_now.py 1.0.0) reads for the holdings and the
-- Top 10 names during the US session, re-read with EXACTLY the rules of v_symbol_news_class (97): the same relevance,
-- event, direction, weight and score. Only the source differs: symbol_news_intraday (one partition per UTC day, the
-- latest run of the day holds the items of the last two days) instead of the nightly symbol_news.
-- WHY (owner, 24 Nov 2026 10:10 Riyadh, in Arabic): the news analysis should act on the reading of a stock's price now,
-- through gathering, comparing and forecasting - not only in the 8 Dec review. The nightly feed sees a headline the
-- morning after; this view classifies it within the hour, so scripts/news_now.py can read it against the price's
-- answer so far today. tests/test_bq_views.py holds that this file is file 97 with only the name, the source table
-- and the description changed - the rules can never drift apart.
-- v1.29.0 (24 Nov 2026): the company names come from snap_universe_latest (scripts/bq_snapshots.py: v_universe_latest
-- computed once after each collector) instead of recomputing v_universe_latest on every hourly read (about 0.24 GB
-- a read); a name is the same until the next archive, and a snapshot older than a day still names the same company.
-- Research only: no decision, gate or count reads this view. The live system never reads BigQuery.
CREATE OR REPLACE VIEW `{{DS}}.v_symbol_news_class_now`
OPTIONS (description = "TFB hourly company headlines (views 1.29.0) re-classified with the rules of v_symbol_news_class, unchanged: relevance, event type, direction, weight and score per symbol and article of the hourly news job. Research only: no decision, gate or count reads this view.")
AS
WITH cfg AS (
  SELECT
    ['a', 'ab', 'adr', 'ag', 'asa', 'bhd', 'cayman', 'co', 'companies', 'company', 'corp', 'corporation', 'cv', 'de', 'group', 'holding', 'holdings', 'inc', 'incorporated', 'l', 'limited', 'llc', 'lp', 'ltd', 'n', 'nv', 'ord', 'oyj', 'p', 'plc', 'reit', 's', 'sa', 'sab', 'se', 'sgps', 'spa', 'sponsored', 'tbk', 'the', 'trust', 'v'] AS suffix,
    ['alpine', 'american', 'asia', 'asian', 'atlantic', 'banco', 'bank', 'brazil', 'british', 'california', 'canada', 'canadian', 'capital', 'carolina', 'central', 'china', 'chinese', 'citizens', 'city', 'community', 'credit', 'dubai', 'east', 'eastern', 'energy', 'enterprise', 'enterprises', 'europe', 'european', 'federal', 'financial', 'first', 'five', 'florida', 'france', 'french', 'general', 'georgia', 'german', 'germany', 'global', 'great', 'green', 'gulf', 'home', 'hong', 'india', 'indian', 'international', 'italy', 'japan', 'kong', 'korea', 'london', 'mexico', 'mid', 'midwest', 'national', 'new', 'north', 'northern', 'ocean', 'ohio', 'old', 'pacific', 'paris', 'peoples', 'poland', 'primary', 'real', 'royal', 'safe', 'saudi', 'singapore', 'south', 'southern', 'spain', 'star', 'state', 'swiss', 'taiwan', 'texas', 'third', 'tokyo', 'united', 'us', 'virginia', 'west', 'western', 'york'] AS generic,
    ['AI', 'CEO', 'CFO', 'CIO', 'COO', 'CTO', 'EPS', 'ESG', 'ETF', 'EU', 'FDA', 'FFO', 'GDP', 'IPO', 'LSE', 'NASDAQ', 'NII', 'NYSE', 'OTC', 'REIT', 'SEC', 'TSX', 'UK', 'US', 'USA'] AS acronym,
    [STRUCT('0883.HK' AS symbol, 'cnooc' AS alias),
      ('2020.HK', 'anta'),
      ('2317.TW', 'foxconn'),
      ('2317.TW', 'hon hai'),
      ('2330.TW', 'tsmc'),
      ('2330.TW', 'taiwan semiconductor'),
      ('ACA.PA', 'credit agricole'),
      ('BG.VI', 'bawag'),
      ('BLX.US', 'bladex'),
      ('C.US', 'citi'),
      ('C.US', 'citigroup'),
      ('GLE.PA', 'societe generale'),
      ('GLE.PA', 'socgen'),
      ('GRT-UN.TO', 'granite reit'),
      ('GRT-UN.TO', 'granite real estate'),
      ('INDV.US', 'indivior'),
      ('INVP.L', 'investec'),
      ('SAN.MC', 'santander'),
      ('SON.LS', 'sonae'),
      ('WPC.US', 'w p carey'),
      ('WPC.US', 'wp carey')] AS aliases,
    [STRUCT(5 AS priority, 'PREVIEW' AS event_type, 0 AS direction, 0.0 AS weight, r'(preview|what to expect|\bto report\b|reports? next week|earnings tomorrow|\bto announce\b|\bto post\b|ahead of (its |the )?(q[1-4]|third quarter|fourth quarter|earnings|results)|expected to (beat|grow|decline|report)|poised to beat|\bwill [a-z0-9 ]*beat\b|analysts estimate|wall street expects|conference cal{2}|fireside chat|to present at|will present|investor day|webcast)' AS pattern),
      (10, 'TAKEOVER_TARGET', 1, 2.0, r'(to be acquired|agrees? to be acquired|takeover (offer|bid) for|buyout offer|\bacquired by\b|go(es|ing)? private)'),
      (11, 'OFFERING', -1, 2.0, r'(public offering|stock offering|share offering|prices? (an? )?(upsized )?offering|announces? (an? )?(proposed )?(public )?offering|at the market offering|private placement|convertible (senior )?notes offering|dilutive|dilution)'),
      (12, 'LEGAL', -1, 2.0, r'(\blawsuit|\bclass action\b|securities fraud|\binvestigation\b|\binvestigates\b|\bprobe\b|\bsubpoena|sec charges|wells notice|\bfined\b|\bpenalty\b|investor alert|shareholder alert|securities litigation|indictment)'),
      (13, 'DIVIDEND_CUT', -1, 2.0, r'(\b(cuts|slashes|suspends|eliminates|reduces|lowers) (its )?(quarterly |monthly )?(dividend|distribution))'),
      (14, 'DIVIDEND_UP', 1, 2.0, r'(\b(increases?|raises|boosts|hikes|lifts) (its )?(quarterly |monthly |common stock )?(cash )?(dividend|distribution)|(dividend|distribution) (increase|hike)|special dividend)'),
      (15, 'GUIDANCE_DOWN', -1, 2.0, r'(\b(cuts|lowers|slashes|reduces|trims|withdraws|pulls) (its )?(full year |fy |annual |2026 |2027 )?(guidance|outlook|forecast)|(guidance|outlook) (cut|below|misses)|guidance cut)'),
      (16, 'GUIDANCE_UP', 1, 2.0, r'(\b(raises|lifts|boosts|ups|increases|hikes) (its )?(full year |fy |annual |2026 |2027 )?(guidance|outlook|forecast)|raised [a-z0-9 ]*(guidance|outlook))'),
      (17, 'DOWNGRADE', -1, 2.0, r'(\bdowngrade[sd]? (to|by|on|at)\b|rating (cut|downgrade)|cut to (sell|underperform|underweight|neutral|hold)|\bdowngraded\b)'),
      (18, 'UPGRADE', 1, 2.0, r'(\bupgrade[sd]? (to|by|on|at)\b|rating upgrade|\bupgraded\b|raised to (buy|outperform|overweight))'),
      (19, 'TARGET_DOWN', -1, 1.0, r'(\b(cuts?|lowers?|trims?|slashes) (its |their )?(price )?targets?|price targets? (cut|lowered|reduced|trimmed)|fair value (cut|trim|falls|lowered|edges lower))'),
      (20, 'TARGET_UP', 1, 1.0, r'(\b(raises?|lifts?|boosts?|ups|hikes?) (its |their )?(price )?targets?|price targets? (raised|increased|hiked|boosted|lifted)|fair value (bump|lift|rise|raised|edges higher))'),
      (21, 'EARNINGS_INLINE', 0, 0.0, r'(\bmatch(es)? (estimates|expectations)|in line with (estimates|expectations)|meets? (estimates|expectations))'),
      (22, 'EARNINGS_MISS', -1, 2.0, r'(\b(lags?|misses|miss|falls? short of|below|trails?) [a-z0-9 ]*(estimates|expectations|consensus)|earnings (decline|fall|drop|plunge)|(loss|losses) widen)'),
      (23, 'EARNINGS_BEAT', 1, 2.0, r'(\b(beats?|tops?|surpass(es)?|exceeds?|above|crush(es)?) [a-z0-9 ]*(estimates|expectations|consensus|forecasts?)|earnings beat|revenue beat|\bsurprises (with|on)\b|record (revenue|earnings|profit|quarter|results|sales)|\b(profit|earnings|income|revenue|sales) (jumps?|rises?|surges?|soars?|climbs?)\b|reports? strong|strong (q[1-4] |third quarter |quarterly )?results|(impressive|blowout|stellar|robust) (q[1-4]|quarter|results|earnings|sales)|delivers impressive|\b(hit|hits|at) (new|record|all time) highs?)'),
      (24, 'MGMT_EXIT', -1, 1.0, r'(\b(ceo|cfo|chief executive|chief financial officer|chairman|president) (resigns|steps down|departs|to step down|exits|to depart|ousted)|leadership change)'),
      (25, 'INDEX_ADD', 1, 1.0, r'(to join (the )?s&p|added to (the )?(s&p|index|nasdaq 100)|joins? (the )?(s&p|russell))'),
      (26, 'APPROVAL_DEAL', 1, 1.0, r'(fda approv|\bapprovals?\b|positive (phase|data|topline|results)|\bwins\b|\bawarded\b|\bsecures\b|\blands [a-z0-9 ]*(contract|order|deal)|strategic (partnership|relationship)|expands? strategic)'),
      (27, 'ESTIMATES_UP', 1, 1.0, r'(earnings estimates (moving higher|rising|raised|climb)|surging earnings estimates|estimates (moving|trending) higher)'),
      (28, 'ESTIMATES_DOWN', -1, 1.0, r'(earnings estimates (moving lower|falling|cut)|estimates (moving|trending) lower)'),
      (29, 'BUYBACK', 1, 1.0, r'(buyback|share repurchase|repurchase (program|plan|authorization)|cancellation of (its )?(own )?shares)'),
      (30, 'PRICE_DOWN', -1, 0.0, r'(\b(falls?|fell|drops?|dropped|slides?|slid|slips?|sinks?|sank|plunges?|plummets?|tumbles?|dips?|declines?|sell ?off|crash(es)?|sheds?|loses|tanks?|stumbles?)\b|\blags (the )?(broader )?market|underperforms|\bdown \d)'),
      (31, 'PRICE_UP', 1, 0.0, r'(\b(rises?|rose|jumps?|jumped|surges?|surged|soars?|soared|rall(y|ies|ied)|climbs?|climbed|gains?|spikes?|skyrockets?|pops?)\b|\b(outpaces|surpasses|beats) (the )?(stock )?market|\bup \d+ ?%)'),
      (32, 'OPINION_NEG', -1, 0.5, r'(\brisky\b|we avoid|\bto sell\b|sell rating|overvalu|above fair value|bearish|red flag|warning sign|headwinds?|bear case)'),
      (33, 'OPINION_POS', 1, 0.5, r'(undervalu|bargain|buy list|top ranked|strong buy|bull case|bullish|time to buy|worth buying|\bupside\b|below fair value|at a discount|great (value|dividend) stock|bulls got right)'),
      (40, 'RESULTS', 0, 0.0, r'(reports? [a-z0-9 ]*(results|earnings)|earnings cal{2}|earnings snapshot|key metrics|q[1-4] [a-z0-9 ]*(earnings|results)|quarter [a-z0-9 ]*results|deep dive|trading update|interim (financial )?report)'),
      (42, 'DIVIDEND_REGULAR', 0, 0.0, r'((declares|announces|declared) [a-z0-9 ]*(dividend|distribution))'),
      (43, 'ACQUIRER', 0, 0.0, r'(to acquire|acquires|acquisition|merger|\bmerge|offer to acquire)')] AS rules
),
e AS (
  SELECT n.*
  FROM `{{DS}}.symbol_news_intraday` AS n
  WHERE n.symbol IS NOT NULL AND n.title IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY n.symbol, COALESCE(n.link, n.title) ORDER BY n.snapshot_date DESC) = 1
),
base AS (
  SELECT
    e.symbol, e.published_at, e.title, e.link, e.domain, e.polarity, e.tags,
    u.name, u.sector, COALESCE(u.symbol_label, e.symbol) AS symbol_label,
    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(REGEXP_REPLACE(NORMALIZE(REPLACE(e.title, CONCAT('&amp', CHR(59)), '&'), NFD), r'\p{M}', '')),
         r'[^a-z0-9&]+', ' '), r'\s+', ' ')) AS t,
    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(REGEXP_REPLACE(NORMALIZE(REPLACE(e.title, CONCAT('&amp', CHR(59)), '&'), NFD), r'\p{M}', '')),
         r'[^a-z0-9&:]+', ' '), r'\s+', ' ')) AS ta,
    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(REGEXP_REPLACE(NORMALIZE(REPLACE(COALESCE(u.name, ''), CONCAT('&amp', CHR(59)), '&'), NFD), r'\p{M}', '')),
         r'[^a-z0-9&]+', ' '), r'\s+', ' ')) AS n,
    REPLACE(IF(STRPOS(e.symbol, '.') > 0, REGEXP_EXTRACT(e.symbol, r'^(.*)\.[^.]+$'), e.symbol), '-UN', '.UN') AS tick,
    IF(STRPOS(e.symbol, '.') > 0, REGEXP_EXTRACT(e.symbol, r'\.([^.]+)$'), 'US') AS mk
  FROM e
  LEFT JOIN `{{DS}}.snap_universe_latest` AS u ON u.symbol = e.symbol
),
keyed AS (
  SELECT b.*,
    ARRAY(SELECT w FROM UNNEST(SPLIT(b.n, ' ')) AS w WITH OFFSET o WHERE w != '' AND w NOT IN UNNEST(c.suffix) ORDER BY o) AS words,
    c.generic, c.acronym, c.rules,
    ARRAY(SELECT a.alias FROM UNNEST(c.aliases) AS a WHERE a.symbol = b.symbol) AS alias_keys
  FROM base AS b CROSS JOIN cfg AS c
),
keys AS (
  SELECT k.* EXCEPT (words, alias_keys, generic),
    ARRAY(SELECT DISTINCT x FROM UNNEST(ARRAY_CONCAT(
            k.alias_keys,
            IF(ARRAY_LENGTH(k.words) >= 1, [ARRAY_TO_STRING(k.words, ' ')], []),
            IF(ARRAY_LENGTH(k.words) >= 2, [CONCAT(k.words[OFFSET(0)], ' ', k.words[OFFSET(1)])], []),
            IF(ARRAY_LENGTH(k.words) >= 1 AND LENGTH(k.words[OFFSET(0)]) >= 4 AND k.words[OFFSET(0)] NOT IN UNNEST(k.generic),
               [k.words[OFFSET(0)]], []))) AS x
          WHERE LENGTH(x) >= 3) AS name_keys,
    REGEXP_REPLACE(k.tick, r'([.\-])', r'\\\1') AS tick_re,
    CASE k.mk
      WHEN 'US' THEN 'NYSE|NASDAQ|Nasdaq|NasdaqGS|NasdaqGM|NasdaqCM|NYSEAMERICAN|NYSEAmerican|AMEX|NYSEARCA|NYSEArca|ARCA'
      WHEN 'PA' THEN 'ENXTPA'
      WHEN 'AS' THEN 'ENXTAM'
      WHEN 'BR' THEN 'ENXTBR'
      WHEN 'LS' THEN 'ENXTLS'
      WHEN 'L' THEN 'LSE|AIM'
      WHEN 'MC' THEN 'BME'
      WHEN 'VI' THEN 'WBAG'
      WHEN 'OL' THEN 'OB|OSE'
      WHEN 'HK' THEN 'SEHK'
      WHEN 'TO' THEN 'TSX'
      WHEN 'TW' THEN 'TWSE'
      WHEN 'DE' THEN 'XTRA|DB'
      WHEN 'NS' THEN 'NSEI'
      WHEN 'SR' THEN 'SASE'
      ELSE 'NONE' END AS exch
  FROM keyed AS k
),
hits AS (
  SELECT y.*,
    (NOT REGEXP_CONTAINS(y.tick, r'^[0-9]+$') AND (
       REGEXP_CONTAINS(y.title, CONCAT('(', y.exch, '):', y.tick_re, r'\)'))
       OR (y.mk = 'US' AND REGEXP_CONTAINS(y.title, CONCAT(r'\(', y.tick_re, r'\)')))
       OR (y.mk = 'US' AND LENGTH(y.tick) >= 3 AND y.tick NOT IN UNNEST(y.acronym)
           AND REGEXP_CONTAINS(y.title, CONCAT('(^|[^A-Za-z0-9:])', y.tick_re, '($|[^A-Za-z0-9])'))))) AS ticker_hit,
    EXISTS (SELECT 1 FROM UNNEST(y.name_keys) AS k WHERE REGEXP_CONTAINS(y.t, CONCAT('(^|[^a-z0-9])', k, '($|[^a-z0-9])'))) AS name_hit,
    EXISTS (SELECT 1 FROM UNNEST(y.name_keys) AS k
            WHERE REGEXP_CONTAINS(y.t, CONCAT('(^|[^a-z0-9])', k, r' (downgrades|upgrades|initiates|reiterates|maintains|names|backs|upgrade|downgrade|analysts?|strategists?|economists?|s (stock |top )?picks)\b'))
               OR REGEXP_CONTAINS(y.t, CONCAT('(^|[^a-z0-9])', k, r' (raises|cuts|lowers|lifts|trims) [a-z0-9 ]*\b(price target|target|pt|rating|estimates?|to (buy|sell|hold|neutral|outperform|underperform|overweight|underweight|equal weight|market perform|sector perform|peer perform))\b'))
               OR (REGEXP_CONTAINS(y.t, CONCAT('(^|[^a-z0-9])', k, r' (says|said|sees|expects|predicts)\b'))
                   AND NOT REGEXP_CONTAINS(y.t, CONCAT('(^|[^a-z0-9])', k, r' (says|said|sees|expects|predicts) [a-z0-9 ]*\b(revenue|sales|earnings|profit|eps|ebitda|guidance|outlook|results|dividend|q[1-4]|quarter|full year|fy|production|deliveries|orders|bookings)\b')))
               OR REGEXP_CONTAINS(y.ta, CONCAT(': ', k, '$'))) AS actor_hit,
    REGEXP_REPLACE(y.t, r'\b(despite|even as|while)\b.*$', '') AS main
  FROM keys AS y
),
cls AS (
  SELECT h.*,
    CASE
      WHEN REGEXP_CONTAINS(h.t, r'^(form 8 5|frn variable rate fix|pre stabilisation|transaction in own shares|director pdmr|total voting rights|notice of (annual )?general meeting|shares and voting rights)') THEN 'noise'
      WHEN NOT (h.ticker_hit OR h.name_hit) THEN 'other'
      WHEN REGEXP_CONTAINS(h.t, r'(\b\d+ (top|best|bank|value|growth|dividend|income|energy|tanker|small cap|stocks?)\b.*\bstocks?\b|\bstocks? (to|with|on|under|we)\b|featured highlights|earnings (before|after) the (open|close)|earnings week ahead|\band \d+ (we|to|that)\b|rankings|\bvs\b|market today|sector update|stocks to watch|zacks industry outlook)') THEN 'roundup'
      WHEN h.actor_hit THEN 'actor'
      ELSE 'company'
    END AS relevance,
    (SELECT AS STRUCT r.event_type, r.direction, r.weight FROM UNNEST(h.rules) AS r
     WHERE REGEXP_CONTAINS(h.main, r.pattern) ORDER BY r.priority LIMIT 1) AS ev
  FROM hits AS h
)
SELECT
  c.symbol,
  c.symbol_label,
  c.name,
  c.sector,
  c.published_at,
  DATE(c.published_at, 'Asia/Riyadh') AS news_date,
  c.title,
  c.link,
  c.domain,
  c.relevance,
  IF(c.relevance = 'noise', 'OTHER', COALESCE(c.ev.event_type, 'OTHER')) AS event_type,
  IF(c.relevance = 'noise', 0, COALESCE(c.ev.direction, 0)) AS direction,
  IF(c.relevance = 'noise', 0.0, COALESCE(c.ev.weight, 0.0)) AS weight,
  IF(c.relevance = 'company', COALESCE(c.ev.direction, 0) * COALESCE(c.ev.weight, 0.0), 0.0) AS score,
  IF(c.relevance = 'company' AND c.ev.event_type IN ('PRICE_UP', 'PRICE_DOWN'), c.ev.direction, 0) AS price_move,
  c.polarity AS provider_polarity,
  c.tags
FROM cls AS c;
