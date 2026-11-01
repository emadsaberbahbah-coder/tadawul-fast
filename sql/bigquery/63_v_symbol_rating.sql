-- v_symbol_rating: the TFB rating - one 0-100 score per market-page symbol
-- that weighs six parts, each on 0-100, over the parts that have data:
--   engine    30 %  the engine's overall score (the sheet's Overall Score)
--   analysts  20 %  the analyst consensus from the counts ((score - 1) x 25,
--                   v_symbol_analyst), else the sheet's Analyst Rating
--                   (Strong Buy 100, Buy 75, Hold 50, Reduce 25, Sell 0)
--   target    15 %  upside to the analyst target: 50 + 2 x (upside - 10),
--                   held to 0..100 (targets sit about 10 % above the price
--                   on average, so 10 % counts as neutral)
--   momentum  10 %  the engine's momentum score
--   news      10 %  50 + 50 x the average polarity of the last 30 days'
--                   headlines (v_symbol_news, EODHD)
--   risk      15 %  100 - the engine's risk score
-- coverage_pct is the weight that had data; tfb_rating_label: Very positive
-- >= 75, Positive >= 60, Neutral >= 45, Negative >= 30, else Very negative
-- (stars 5..1); rating_confidence: high when coverage >= 80 % and the row
-- passes the reliability / data-quality floors, medium at >= 60 %, else
-- low. A summary of evidence, not advice.
CREATE OR REPLACE VIEW `{{DS}}.v_symbol_rating`
OPTIONS (description = "TFB Looker v3: the TFB rating per symbol - six weighted parts (engine, analysts, target, momentum, news, risk), label, stars, confidence")
AS
WITH nw AS (
  SELECT
    symbol,
    ROUND(AVG(polarity), 3) AS news_polarity_30d,
    COUNT(*) AS news_30d,
    COUNTIF(sentiment_label = 'negative') AS news_negative_30d
  FROM `{{DS}}.v_symbol_news`
  WHERE published_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  GROUP BY symbol
),
c AS (
  SELECT
    u.symbol, u.symbol_label, u.name, u.region, u.market_country, u.sector, u.asset_class,
    u.current_price, u.currency, u.recommendation, u.final_action, u.meets_floors,
    u.reliability, u.data_quality,
    an.consensus_score AS analyst_consensus_score,
    an.consensus_label AS analyst_consensus_label,
    an.analysts,
    COALESCE(an.target_upside_pct, u.target_upside_pct) AS target_upside_pct,
    nw.news_polarity_30d, nw.news_30d, nw.news_negative_30d,
    u.overall_score AS part_engine,
    COALESCE(
      ROUND((an.consensus_score - 1) * 25, 1),
      CASE UPPER(REPLACE(u.analyst_rating, '_', ' '))
        WHEN 'STRONG BUY' THEN 100 WHEN 'BUY' THEN 75 WHEN 'HOLD' THEN 50
        WHEN 'REDUCE' THEN 25 WHEN 'SELL' THEN 0 WHEN 'STRONG SELL' THEN 0
      END) AS part_analysts,
    ROUND(LEAST(100, GREATEST(0, 50 + 2 * (COALESCE(an.target_upside_pct, u.target_upside_pct) - 10))), 1) AS part_target,
    u.momentum_score AS part_momentum,
    ROUND(LEAST(100, GREATEST(0, 50 + 50 * nw.news_polarity_30d)), 1) AS part_news,
    ROUND(100 - u.risk_score, 1) AS part_risk
  FROM `{{DS}}.v_universe_latest` AS u
  LEFT JOIN `{{DS}}.v_symbol_analyst` AS an ON an.symbol = u.symbol
  LEFT JOIN nw ON nw.symbol = u.symbol
),
w AS (
  SELECT
    c.*,
    0.30 * IF(c.part_engine IS NULL, 0, 1) + 0.20 * IF(c.part_analysts IS NULL, 0, 1)
      + 0.15 * IF(c.part_target IS NULL, 0, 1) + 0.10 * IF(c.part_momentum IS NULL, 0, 1)
      + 0.10 * IF(c.part_news IS NULL, 0, 1) + 0.15 * IF(c.part_risk IS NULL, 0, 1) AS weight_present,
    0.30 * IFNULL(c.part_engine, 0) + 0.20 * IFNULL(c.part_analysts, 0)
      + 0.15 * IFNULL(c.part_target, 0) + 0.10 * IFNULL(c.part_momentum, 0)
      + 0.10 * IFNULL(c.part_news, 0) + 0.15 * IFNULL(c.part_risk, 0) AS weighted_sum
  FROM c
),
r AS (
  SELECT
    w.* EXCEPT (weighted_sum),
    ROUND(SAFE_DIVIDE(w.weighted_sum, NULLIF(w.weight_present, 0)), 1) AS tfb_rating_score,
    ROUND(w.weight_present * 100, 0) AS coverage_pct
  FROM w
)
SELECT
  r.* EXCEPT (weight_present),
  CASE
    WHEN r.tfb_rating_score >= 75 THEN 'Very positive'
    WHEN r.tfb_rating_score >= 60 THEN 'Positive'
    WHEN r.tfb_rating_score >= 45 THEN 'Neutral'
    WHEN r.tfb_rating_score >= 30 THEN 'Negative'
    WHEN r.tfb_rating_score IS NOT NULL THEN 'Very negative'
  END AS tfb_rating_label,
  CASE
    WHEN r.tfb_rating_score >= 75 THEN 5
    WHEN r.tfb_rating_score >= 60 THEN 4
    WHEN r.tfb_rating_score >= 45 THEN 3
    WHEN r.tfb_rating_score >= 30 THEN 2
    WHEN r.tfb_rating_score IS NOT NULL THEN 1
  END AS tfb_rating_stars,
  CASE
    WHEN r.coverage_pct >= 80 AND r.meets_floors THEN 'high'
    WHEN r.coverage_pct >= 60 THEN 'medium'
    ELSE 'low'
  END AS rating_confidence,
  ARRAY_TO_STRING(ARRAY(
    SELECT p FROM UNNEST([
      IF(r.part_engine IS NULL, NULL, FORMAT('engine %.0f', r.part_engine)),
      IF(r.part_analysts IS NULL, NULL, FORMAT('analysts %.0f', r.part_analysts)),
      IF(r.part_target IS NULL, NULL, FORMAT('target %.0f', r.part_target)),
      IF(r.part_momentum IS NULL, NULL, FORMAT('momentum %.0f', r.part_momentum)),
      IF(r.part_news IS NULL, NULL, FORMAT('news %.0f', r.part_news)),
      IF(r.part_risk IS NULL, NULL, FORMAT('risk %.0f', r.part_risk))
    ]) AS p
    WHERE p IS NOT NULL
  ), ' · ') AS rating_parts
FROM r;
