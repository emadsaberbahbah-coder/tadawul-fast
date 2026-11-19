-- v_trial_news_overlay: the controlled trial of the news version of the
-- investable list (v_news_overlay_list) against the list as it is - the
-- scoreboard that decides whether world news may ever move a
-- recommendation. WHY (owner, 19 Nov 2026 13:06 Riyadh, verbatim: "Let's
-- Start build the new news and it's effect in the file by the best way that
-- you propse and before doing that make more final test then apply the
-- changes by the best possible and effective way").
-- THE FINAL TEST OF 19 NOV 2026 (in-sample, NOT evidence) is why this is a
-- trial and not a change to the list. On 75 weeks (May 2025 - Nov 2026) of
-- sector ETFs against SPY, with no look-ahead: the three sectors the news
-- favoured beat the three it avoided by 0.15 points a week on the 'events'
-- arm (t 0.53) and 0.17 on the 'all' arm (t 0.69); the same method fed news
-- 8 to 40 weeks old did better than fresh news in 5 of 9 cases ('events')
-- and 3 of 9 ('all'). On the 13 archived list days (31 Oct - 12 Nov), one
-- week ahead, the 'events' overlay was ahead of the list on 12 days and the
-- 'all' overlay behind it on 8; this view's own 'events' arm reads +0.45
-- points a day with an overlap-adjusted t of 0.69: three weekly decisions,
-- two arms, opposite answers. Nothing is proven.
-- MEASURE: edge = a name's forward price return minus the day's market
-- median (v_forward_universe, 1W and 2W). Per arm, horizon and day, on days
-- when the news moved at least one name: the mean edge of the list
-- (CHAMPION), of the news version (NEWS_OVERLAY), of the names moved out and
-- of the names brought in.
-- EVIDENCE: only list days from 20 Nov 2026 (period 'forward from
-- 2026-11-20'); earlier days are in-sample.
-- PROMOTION RULE (fixed now), per arm and horizon: forward, at least the
-- larger of 20 changed days and two horizon lengths (1W 20, 2W 28), at least
-- 8 different news weeks, and the mean daily difference above 0 with an
-- overlap-adjusted t of 2.5 or more (t of the daily differences x
-- SQRT(days / horizon days), as v_trial_c2; 2.5 and not 2 because two arms
-- and two horizons are read). Even then the owner decides; nothing here
-- changes a page, a pick or an order.
-- COLUMNS: period, arm, horizon, line (CHAMPION / NEWS_OVERLAY / MOVED_OUT /
-- MOVED_IN / OVERLAY_MINUS_CHAMPION), names, days, news_weeks, edge_pp,
-- right_pct (share of names above the market median), t_adj
-- (OVERLAY_MINUS_CHAMPION only), first_day, last_day, reading.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_trial_news_overlay`
OPTIONS (description = "TFB news trial (fixed 19 Nov 2026): the news version of the investable list against the list itself, per arm and horizon - forward evidence from 20 Nov 2026 with an overlap-adjusted t and a fixed promotion rule")
AS
WITH l AS (
  SELECT
    o.arm, o.base_date, o.symbol, o.in_champion, o.in_overlay, o.news_action, o.news_week,
    f.horizon,
    CASE f.horizon WHEN '1W' THEN 7 WHEN '2W' THEN 14 END AS horizon_days,
    f.ret_pp - f.market_median_pp AS edge
  FROM `{{DS}}.v_news_overlay_list` AS o
  JOIN `{{DS}}.v_forward_universe` AS f
    ON f.symbol = o.symbol AND f.base_date = o.base_date
),
daily AS (
  SELECT
    IF(base_date >= DATE '2026-11-20', 'forward from 2026-11-20', 'in-sample, not evidence') AS period,
    arm, horizon, base_date,
    MIN(horizon_days) AS horizon_days,
    MAX(news_week) AS news_week,
    COUNTIF(in_champion) AS n_ch, SUM(IF(in_champion, edge, 0)) AS s_ch, COUNTIF(in_champion AND edge > 0) AS w_ch,
    COUNTIF(in_overlay) AS n_ov, SUM(IF(in_overlay, edge, 0)) AS s_ov, COUNTIF(in_overlay AND edge > 0) AS w_ov,
    COUNTIF(news_action = 'out') AS n_out, SUM(IF(news_action = 'out', edge, 0)) AS s_out,
    COUNTIF(news_action = 'out' AND edge > 0) AS w_out,
    COUNTIF(news_action = 'in') AS n_in, SUM(IF(news_action = 'in', edge, 0)) AS s_in,
    COUNTIF(news_action = 'in' AND edge > 0) AS w_in
  FROM l
  GROUP BY period, arm, horizon, base_date
),
agg AS (
  SELECT
    period, arm, horizon,
    MIN(horizon_days) AS horizon_days,
    COUNT(*) AS days,
    COUNT(DISTINCT news_week) AS news_weeks,
    SUM(n_ch) AS n_ch, SUM(s_ch) AS s_ch, SUM(w_ch) AS w_ch,
    SUM(n_ov) AS n_ov, SUM(s_ov) AS s_ov, SUM(w_ov) AS w_ov,
    SUM(n_out) AS n_out, SUM(s_out) AS s_out, SUM(w_out) AS w_out,
    SUM(n_in) AS n_in, SUM(s_in) AS s_in, SUM(w_in) AS w_in,
    AVG(SAFE_DIVIDE(s_ov, n_ov) - SAFE_DIVIDE(s_ch, n_ch)) AS dm,
    STDDEV_SAMP(SAFE_DIVIDE(s_ov, n_ov) - SAFE_DIVIDE(s_ch, n_ch)) AS dsd,
    MIN(base_date) AS first_day,
    MAX(base_date) AS last_day
  FROM daily
  WHERE n_out > 0 AND n_ch > 0 AND n_ov > 0
  GROUP BY period, arm, horizon
),
rows_ AS (
  SELECT
    period, arm, horizon, line, days, news_weeks, first_day, last_day,
    GREATEST(20, 2 * horizon_days) AS need,
    CASE line WHEN 'CHAMPION' THEN n_ch WHEN 'NEWS_OVERLAY' THEN n_ov WHEN 'MOVED_OUT' THEN n_out
              WHEN 'MOVED_IN' THEN n_in ELSE n_ov END AS names,
    CASE line WHEN 'CHAMPION' THEN SAFE_DIVIDE(s_ch, n_ch) WHEN 'NEWS_OVERLAY' THEN SAFE_DIVIDE(s_ov, n_ov)
              WHEN 'MOVED_OUT' THEN SAFE_DIVIDE(s_out, n_out) WHEN 'MOVED_IN' THEN SAFE_DIVIDE(s_in, n_in)
              ELSE dm END AS edge_x,
    ROUND(100 * CASE line WHEN 'CHAMPION' THEN SAFE_DIVIDE(w_ch, n_ch) WHEN 'NEWS_OVERLAY' THEN SAFE_DIVIDE(w_ov, n_ov)
                          WHEN 'MOVED_OUT' THEN SAFE_DIVIDE(w_out, n_out)
                          WHEN 'MOVED_IN' THEN SAFE_DIVIDE(w_in, n_in) END, 1) AS right_pct,
    IF(line = 'OVERLAY_MINUS_CHAMPION', SAFE_DIVIDE(dm, dsd) * SQRT(days / horizon_days), NULL) AS t_x
  FROM agg
  CROSS JOIN UNNEST(['CHAMPION', 'NEWS_OVERLAY', 'MOVED_OUT', 'MOVED_IN', 'OVERLAY_MINUS_CHAMPION']) AS line
)
SELECT
  period, arm, horizon, line, names, days, news_weeks,
  ROUND(edge_x, 2) AS edge_pp,
  right_pct,
  ROUND(t_x, 2) AS t_adj,
  first_day, last_day,
  CASE
    WHEN line != 'OVERLAY_MINUS_CHAMPION' THEN NULL
    WHEN period != 'forward from 2026-11-20' THEN 'in-sample: shows why this is a trial, not evidence'
    WHEN days < need OR news_weeks < 8 THEN CONCAT('collecting: ', CAST(days AS STRING), ' of ', CAST(need AS STRING),
                                                   ' days, ', CAST(news_weeks AS STRING), ' of 8 news weeks')
    WHEN t_x >= 2.5 AND edge_x > 0 THEN 'news overlay ahead: the owner may consider it'
    WHEN edge_x <= 0 THEN 'news overlay not ahead: keep the list as it is'
    ELSE 'news overlay ahead but not yet clear (t under 2.5): keep collecting'
  END AS reading
FROM rows_;
