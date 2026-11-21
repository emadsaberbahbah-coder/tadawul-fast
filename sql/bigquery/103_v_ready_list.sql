-- v_ready_list: on each archived day, every qualified name with the market it
-- tracks, that market's state, and its place on the day's ready list.
-- WHY (owner, 21 Nov 2026 11:03 Riyadh, the build list): "3. A ready list:
-- each day the 2-3 best qualified names from the top-ranked markets, each
-- labelled with what it is waiting for (cash / calmer market / switch). Every
-- page shows the same final decision; only the Today Action Plan gives
-- orders." - "The Daily Plan's hold then works market by market. It follows
-- the market an asset tracks, not the currency it trades in." Observe only.
-- QUALIFIED: a market-page row INVESTABLE with final action INVEST that meets
--   the Top 10 panel floors (reliability 70+, data quality 80+, no block
--   reason) - v_universe_latest's floors and v_hold_vs_switch's candidates;
--   one row per symbol and day (the day's last capture).
-- THE MARKET IT TRACKS (a market_id of v_market_condition, first match wins):
--   the market funds by name, listed in New York or without a suffix: KSA sa,
--   VGK eu, EWJ jp, MCHI cn, GLD gold, IEF bonds, SPSK sukuk, VNQ reits_us;
--   gold futures (GC=F, MGC=F) gold, crude futures (BZ=F, CL=F, QM=F) oil;
--   any other fund or contract (asset_class not EQUITY): NULL;
--   a US listing - or a dollar name with no suffix, broad foreign funds
--   (EFA, EEM, ACWI, VEA, VWO, IEFA, IEMG) and silver (SLV) excepted - is
--   reits_us when its industry is a REIT, else us (an ADR is read as a US
--   name: the archive does not know the company's home market);
--   the Tadawul REITs (4330-4350) reits_sa, any other Tadawul name sa;
--   Hong Kong, Shanghai and Shenzhen cn; Tokyo jp; a European listing eu
--   (Turkey excepted); anything else NULL - its market is not read yet
--   (Canada, Mexico, Korea, Thailand, Australia, India, Qatar ...).
-- THE MARKET'S STATE: v_market_rank on the day - or on its last day, when the
--   day is after it (a weekend, the morning before the archive), 6 days at
--   most: market_state open / hold, or 'not read'.
-- THE DAY'S ORDER (ready_rank): open markets first, then held ones, then names
--   whose market is not read; at most two names per market before any third
--   (pos_in_market); then the market's own rank, the name's 1-month expected
--   return, its overall score. in_ready = ready_rank 1-3: the ready list.
-- waiting_for: 'calmer market' (its market is on hold - market_waiting_for
--   says for what), 'market not read', or 'cash or a switch' (its market is
--   open: the Markets Board reads the Today Action Plan's decision and the
--   switch check, v_hold_vs_switch, and names which).
-- THE LIVE US RULE beside it: us_label (the US market's label that day) and
--   currency; live_us_rule_holds = currency USD and the US not calm - the Daily
--   Plan's rule with its workflow defaults (TFB_PLAN_REGIME enforce,
--   TFB_PLAN_REGIME_CCY USD, TFB_PLAN_REGIME_BLOCK every label but calm).
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it. Observe only.
CREATE OR REPLACE VIEW `{{DS}}.v_ready_list`
OPTIONS (description = "TFB markets v1: one row per archived day and qualified name (INVESTABLE, INVEST, panel floors) - the market it tracks (not its currency), that market's state (v_market_rank: open / hold / not read), the day's ready order (open markets first, two names per market, then the market's rank and the name's expected return - the top 3 are the ready list), what it is waiting for, and the live US rule beside it. Observe only.")
AS
WITH u AS (
  SELECT
    v.snapshot_date,
    v.symbol,
    v.name,
    v.currency,
    v.market_code,
    v.region,
    v.market_country,
    v.asset_class,
    v.industry,
    v.source_page,
    v.expected_roi_1m_pct,
    v.overall_score,
    UPPER(IFNULL(NULLIF(v.symbol_base, ''), REGEXP_EXTRACT(v.symbol, r'^([^.]+)'))) AS base
  FROM `{{DS}}.v_universe` AS v
  WHERE UPPER(v.investability_status) = 'INVESTABLE'
    AND UPPER(v.final_action) = 'INVEST'
    AND COALESCE(v.reliability >= 70 AND v.data_quality >= 80, FALSE)
    AND v.block_reason IS NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY v.symbol, v.snapshot_date ORDER BY v.captured_at DESC, v.source_page) = 1
),
mapped AS (
  SELECT
    u.*,
    CASE
      WHEN IFNULL(u.market_code, 'US') = 'US' AND u.base = 'KSA' THEN 'sa'
      WHEN IFNULL(u.market_code, 'US') = 'US' AND u.base = 'VGK' THEN 'eu'
      WHEN IFNULL(u.market_code, 'US') = 'US' AND u.base = 'EWJ' THEN 'jp'
      WHEN IFNULL(u.market_code, 'US') = 'US' AND u.base = 'MCHI' THEN 'cn'
      WHEN IFNULL(u.market_code, 'US') = 'US' AND u.base = 'GLD' THEN 'gold'
      WHEN IFNULL(u.market_code, 'US') = 'US' AND u.base = 'IEF' THEN 'bonds'
      WHEN IFNULL(u.market_code, 'US') = 'US' AND u.base = 'SPSK' THEN 'sukuk'
      WHEN IFNULL(u.market_code, 'US') = 'US' AND u.base = 'VNQ' THEN 'reits_us'
      WHEN UPPER(u.symbol) IN ('GC=F', 'MGC=F') THEN 'gold'
      WHEN UPPER(u.symbol) IN ('BZ=F', 'CL=F', 'QM=F') THEN 'oil'
      WHEN UPPER(IFNULL(u.asset_class, '')) != 'EQUITY' THEN NULL
      WHEN (u.market_code = 'US'
            OR (u.market_code IS NULL AND UPPER(IFNULL(u.currency, '')) = 'USD' AND IFNULL(u.region, '') = 'Other'
                AND u.base NOT IN ('EFA', 'EEM', 'ACWI', 'VEA', 'VWO', 'IEFA', 'IEMG', 'SLV')))
        THEN IF(STARTS_WITH(UPPER(IFNULL(u.industry, '')), 'REIT'), 'reits_us', 'us')
      WHEN u.market_code = 'SR' AND REGEXP_CONTAINS(u.symbol, r'^43(3[0-9]|4[0-9]|50)\.SR$') THEN 'reits_sa'
      WHEN u.market_code = 'SR' THEN 'sa'
      WHEN u.market_code IN ('HK', 'SS', 'SZ') THEN 'cn'
      WHEN u.market_code = 'T' THEN 'jp'
      WHEN IFNULL(u.region, '') = 'Europe' AND IFNULL(u.market_code, '') != 'IS' THEN 'eu'
      ELSE NULL
    END AS market_id
  FROM u
),
mk_day AS (
  SELECT
    k.as_of, k.market_id, k.market, k.state, k.label, k.waiting_for, k.rank, k.verdict,
    MAX(IF(k.market_id = 'us', k.label, NULL)) OVER (PARTITION BY k.as_of) AS us_label
  FROM `{{DS}}.v_market_rank` AS k
),
last_day AS (
  SELECT MAX(as_of) AS last_as_of
  FROM mk_day
),
day_us AS (
  SELECT DISTINCT as_of, us_label
  FROM mk_day
),
dated AS (
  SELECT m.*, LEAST(m.snapshot_date, l.last_as_of) AS look_day
  FROM mapped AS m
  CROSS JOIN last_day AS l
),
joined AS (
  SELECT
    t.*,
    d.as_of AS market_as_of,
    k.market AS market,
    IF(t.market_id IS NULL OR k.state IS NULL, 'not read', k.state) AS market_state,
    k.label AS market_label,
    k.waiting_for AS market_waiting_for,
    k.rank AS market_rank,
    k.verdict AS market_verdict,
    d.us_label
  FROM dated AS t
  LEFT JOIN day_us AS d ON d.as_of = t.look_day AND DATE_DIFF(t.snapshot_date, t.look_day, DAY) <= 6
  LEFT JOIN mk_day AS k ON k.as_of = d.as_of AND k.market_id = t.market_id
),
placed AS (
  SELECT
    j.*,
    ROW_NUMBER() OVER (PARTITION BY j.snapshot_date, j.market_id
                       ORDER BY j.expected_roi_1m_pct DESC NULLS LAST, j.overall_score DESC NULLS LAST, j.symbol)
      AS pos_in_market
  FROM joined AS j
),
ordered AS (
  SELECT
    p.*,
    ROW_NUMBER() OVER (PARTITION BY p.snapshot_date
                       ORDER BY CASE p.market_state WHEN 'open' THEN 0 WHEN 'hold' THEN 1 ELSE 2 END,
                                IF(p.pos_in_market <= 2, 0, 1), p.market_rank NULLS LAST,
                                p.expected_roi_1m_pct DESC NULLS LAST, p.overall_score DESC NULLS LAST, p.symbol)
      AS ready_rank,
    COUNT(*) OVER (PARTITION BY p.snapshot_date) AS qualified
  FROM placed AS p
)
SELECT
  o.snapshot_date, o.symbol, o.name, o.source_page, o.currency, o.market_code, o.market_country, o.asset_class,
  o.industry, o.market_id, o.market, o.market_as_of, o.market_state, o.market_label, o.market_waiting_for,
  o.market_rank, o.market_verdict, o.pos_in_market, o.ready_rank, o.ready_rank <= 3 AS in_ready, o.qualified,
  ROUND(o.expected_roi_1m_pct, 2) AS expected_roi_1m_pct, ROUND(o.overall_score, 1) AS overall_score,
  CASE o.market_state WHEN 'hold' THEN 'calmer market' WHEN 'open' THEN 'cash or a switch'
                      ELSE 'market not read' END AS waiting_for,
  o.us_label,
  UPPER(IFNULL(o.currency, '')) = 'USD' AND IFNULL(o.us_label, 'calm') != 'calm' AS live_us_rule_holds,
  o.market_state = 'hold' AS market_rule_holds
FROM ordered AS o;
