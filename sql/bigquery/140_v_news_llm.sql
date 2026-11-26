-- v_news_llm: every headline the language-model shadow reader (scripts/news_llm_reader.py, tab _News_LLM) labelled,
-- one row per Item ID - the copy from the latest snapshot that still holds it. Each row carries both readings of the
-- same headline: the gate's word lists (rule_event / rule_direction / rule_severity, empty when the lists found no
-- event) and the model's (llm_event, 'none' when it saw no event of a listed type; llm_direction; llm_severity;
-- llm_confidence 0-100), and how they compare (agree: same / direction differs / event differs / rule only /
-- model only / neither). symbol is the Scope when the headline is about a name (NULL for MACRO); news_date is the
-- Riyadh date of the headline (its publication time, else the time the model read it).
-- Direction: for MACRO the effect on global equity markets, for a name the effect on its shareholders; 1 good,
-- -1 bad, 0 neutral or unclear. A reading with no event counts as direction 0 (rule_dir / llm_dir).
-- WHY (owner, 26 Nov 2026 15:34 Riyadh, in Arabic: "let us start with this too and see how we can develop the
-- system, linked with BigQuery and the other files"). The model reads in observe mode: it controls nothing, and this
-- view is how its reading is set beside the rules' and, in v_news_llm_eval / v_news_llm_duel, against what the
-- prices did next. (apply_bq_views 1.32.0, archive_to_bigquery 1.2.0.)
-- Research only: no decision, gate or count reads this view. (views 1.32.0)
CREATE OR REPLACE VIEW `{{DS}}.v_news_llm`
OPTIONS (description = "TFB language-model shadow reading of the news gate's headlines, one row per item: the word lists' event and direction beside the model's, and how they compare. Research view, never an order.")
AS
WITH snaps AS (
  SELECT snapshot_date, captured_at, row_num, cells
  FROM `{{DS}}.news_llm`
),
hdr AS (
  SELECT snapshot_date, cells AS h
  FROM snaps
  WHERE row_num = 1
),
typed AS (
  SELECT
    s.snapshot_date,
    `{{DS}}.f_cell`(h, s.cells, 'Item ID') AS item_id,
    `{{DS}}.f_ts`(`{{DS}}.f_cell`(h, s.cells, 'Read At (Riyadh)')) AS read_at,
    `{{DS}}.f_ts_utc`(`{{DS}}.f_cell`(h, s.cells, 'Published (UTC)')) AS published_at,
    `{{DS}}.f_cell`(h, s.cells, 'Scope') AS scope,
    `{{DS}}.f_cell`(h, s.cells, 'Headline') AS headline,
    LOWER(`{{DS}}.f_cell`(h, s.cells, 'Rule Event')) AS rule_event,
    SAFE_CAST(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Rule Direction')) AS INT64) AS rule_direction,
    UPPER(`{{DS}}.f_cell`(h, s.cells, 'Rule Severity')) AS rule_severity,
    LOWER(`{{DS}}.f_cell`(h, s.cells, 'LLM Event')) AS llm_event,
    SAFE_CAST(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'LLM Direction')) AS INT64) AS llm_direction,
    UPPER(`{{DS}}.f_cell`(h, s.cells, 'LLM Severity')) AS llm_severity,
    SAFE_CAST(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'LLM Confidence')) AS INT64) AS llm_confidence,
    LOWER(`{{DS}}.f_cell`(h, s.cells, 'Agree')) AS agree,
    `{{DS}}.f_cell`(h, s.cells, 'Model') AS model,
    `{{DS}}.f_cell`(h, s.cells, 'Run ID') AS run_id
  FROM snaps AS s
  JOIN hdr USING (snapshot_date)
  WHERE s.row_num > 1
)
SELECT
  t.*,
  IF(UPPER(t.scope) = 'MACRO', NULL, t.scope) AS symbol,
  UPPER(t.scope) = 'MACRO' AS is_macro,
  IF(IFNULL(t.rule_event, '') = '', 0, IFNULL(t.rule_direction, 0)) AS rule_dir,
  IF(IFNULL(t.llm_event, 'none') IN ('', 'none'), 0, IFNULL(t.llm_direction, 0)) AS llm_dir,
  DATE(COALESCE(t.published_at, t.read_at), 'Asia/Riyadh') AS news_date
FROM typed AS t
WHERE t.item_id IS NOT NULL AND t.scope IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.item_id ORDER BY t.snapshot_date DESC) = 1;
