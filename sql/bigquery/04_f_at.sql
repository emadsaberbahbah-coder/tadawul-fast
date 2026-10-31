-- f_at: cell i (0-based) of row c as the views read it - NULL when the cell
-- is blank, missing or a dash (the workbook's "no value"), else the trimmed
-- text. With f_col: f_at(c, f_col(h, name)) = f_cell(h, c, name).
CREATE OR REPLACE FUNCTION `{{DS}}.f_at`(c ARRAY<STRING>, i INT64)
RETURNS STRING
AS (
  NULLIF(NULLIF(TRIM(c[SAFE_OFFSET(i)]), ''), '—')
)
OPTIONS (description = "TFB Looker v2: cell i of row c (NULL when blank, missing or a dash) - pairs with f_col");
