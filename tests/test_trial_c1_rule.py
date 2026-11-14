"""v_trial_c1 (apply_bq_views 1.8.0): the C-1 rule and its promotion rule were fixed on 14 Nov 2026, before any
forward data. This test keeps them from changing silently: a change to the rule must change this test too, in a
reviewed commit, and restart the forward count."""
from __future__ import annotations

import re
from pathlib import Path

SQL = (Path(__file__).resolve().parents[1] / "sql/bigquery/80_v_trial_c1.sql").read_text(encoding="utf-8")
CODE = "\n".join(line.split("--")[0] for line in SQL.splitlines())


def test_the_rule_is_the_one_fixed_on_14_nov():
    assert "IN ('BUY', 'STRONG BUY', 'ACCUMULATE')" in CODE                         # buy calls only
    assert "NOT (UPPER(IFNULL(confidence, '')) = 'HIGH' OR IFNULL(rel, 0) >= 80)" in CODE
    assert "(market != 'Tadawul' OR IFNULL(SAFE_DIVIDE(p0, p30) - SAFE_DIVIDE(i0, i30), 0) > 0)" in CODE
    assert CODE.count("INTERVAL 30 DAY") == 2 and "symbol = '^TASI.SR'" in CODE
    assert "in_engine_cohort AND excess_pp IS NOT NULL" in CODE                       # matured, priced, vs market


def test_only_forward_calls_are_evidence_and_the_promotion_rule_is_fixed():
    assert CODE.count("DATE '2026-11-15'") == 1
    assert "WHEN period != 'forward from 2026-11-15' THEN 'in-sample: shows why the rule was chosen, not evidence'" in CODE
    assert "WHEN days < 20 OR calls < 40 THEN" in CODE and "WHEN t_stat >= 2 AND edge_pp > 0 THEN" in CODE
    assert re.search(r"the owner decides", SQL) and "never reads BigQuery" in SQL
