-- f_num(s): the number in a workbook cell ("1,234.5", "+3.2", "−2.1", "12%"
-- -> 1234.5, 3.2, -2.1, 12); NULL when the cell is not a number. The archive
-- stores UNFORMATTED values, so a cell formatted as a percentage arrives as a
-- fraction: every view states the unit of each column it exposes.
CREATE OR REPLACE FUNCTION `{{DS}}.f_num`(s STRING)
RETURNS FLOAT64
AS (
  SAFE_CAST(REGEXP_REPLACE(REPLACE(REPLACE(TRIM(s), '−', '-'), ',', ''), r'[%+\s]', '') AS FLOAT64)
)
OPTIONS (description = "TFB Looker v1: number in a workbook cell (thousands separators, plus signs, unicode minus, percent sign removed)");
