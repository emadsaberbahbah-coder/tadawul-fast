"""shadow_v2 and v_trial_shadow_v2 (apply_bq_views 1.11.0): the challenger score and its promotion test were
fixed on 18 Nov 2026, before any forward day. This test keeps them from changing silently: a change to the
weights, the inputs, the forward date or the promotion test must change this test too, in a reviewed commit,
and restart the forward count."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EDGE = (ROOT / "sql/bigquery/74_v_signal_edge_daily.sql").read_text(encoding="utf-8")
TRIAL = (ROOT / "sql/bigquery/84_v_trial_shadow_v2.sql").read_text(encoding="utf-8")


def _code(sql):
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


EDGE_CODE, TRIAL_CODE = _code(EDGE), _code(TRIAL)


def test_the_score_is_the_one_fixed_on_18_nov():
    assert "0.6 * PERCENT_RANK() OVER (PARTITION BY d, market_group ORDER BY tk)" in EDGE_CODE
    assert "+ 0.4 * PERCENT_RANK() OVER (PARTITION BY d, market_group ORDER BY ek) AS shadow_v2" in EDGE_CODE
    assert "CAST(ROUND(target_upside * 10000) AS INT64) AS tk" in EDGE_CODE
    assert "CAST(ROUND(engine_roi_1m * 10000) AS INT64) AS ek" in EDGE_CODE
    assert EDGE_CODE.count("PERCENT_RANK()") == 2                                   # two inputs, no third term
    weights = [float(w) for w in re.findall(r"([0-9.]+) \* PERCENT_RANK\(\)", EDGE_CODE)]
    assert weights == [0.6, 0.4] and abs(sum(weights) - 1) < 1e-12


def test_the_score_reads_no_outcome_and_none_of_the_signals_it_challenges():
    block = EDGE_CODE[EDGE_CODE.index("both AS ("):EDGE_CODE.index("g2 AS (")]
    assert "WHERE target_upside IS NOT NULL AND engine_roi_1m IS NOT NULL" in block   # both inputs or no score
    for word in ("confidence", "reliability", "overall_score", "recommendation", "r_1w", "r_2w", "r_1m", "r_3m"):
        assert word not in block, word
    # ranked among the day's names of the same market group only: no other day, no future row
    assert set(re.findall(r"PARTITION BY ([a-z_, ]+) ORDER BY [te]k", block)) == {"d, market_group"}


def test_it_is_measured_by_the_same_method_as_the_current_score():
    feats = re.search(r"UNPIVOT \(x FOR feature IN \(([^)]*)\)\)", EDGE_CODE).group(1)
    names = [f.strip() for f in feats.split(",")]
    assert "shadow_v2" in names and "overall_score" in names and len(names) == len(set(names)) == 14


def test_only_forward_days_are_evidence_and_the_promotion_test_is_fixed():
    assert TRIAL_CODE.count("DATE '2026-11-19'") == 1
    assert "WHEN period != 'forward from 2026-11-19' THEN 'in-sample: the weights were chosen on these days, not evidence'" in TRIAL_CODE
    assert "WHERE ic_s IS NOT NULL AND ic_o IS NOT NULL" in TRIAL_CODE                # the same days for both scores
    assert "GREATEST(20, 2 * horizon_days) AS need" in TRIAL_CODE and "WHEN days < need THEN" in TRIAL_CODE
    assert "WHEN t_x >= 2 AND m_s > 0 AND top_s > top_o THEN 'shadow ahead: the owner may consider it'" in TRIAL_CODE
    assert "SAFE_DIVIDE(dm, dsd) * SQRT(days / horizon_days) AS t_x" in TRIAL_CODE    # overlapping windows count once
    assert re.search(r"Promotion is the\s*\n-- owner's decision", TRIAL) and "never reads BigQuery" in TRIAL
    assert "never reads BigQuery" in EDGE and "never read by a decision" in EDGE


def test_the_reading_is_judged_before_rounding():
    reading = TRIAL_CODE[TRIAL_CODE.index("CASE\n    WHEN period != 'forward from 2026-11-19'"):]
    for shown in ("t_adj", "ic_diff", "ic_shadow", "top_shadow_pp", "top_current_pp"):
        assert shown not in reading, shown
    assert "ROUND(t_x, 2) AS t_adj" in TRIAL_CODE and "ROUND(dm, 4) AS ic_diff" in TRIAL_CODE


def test_no_live_script_reads_the_shadow_score():
    # shadow mode: nothing that builds a page, a score or a pick may name it
    for path in sorted((ROOT / "scripts").glob("*.py")):
        if path.name in ("apply_bq_views.py", "verify_deployment.py"):
            continue
        assert "shadow_v2" not in path.read_text(encoding="utf-8", errors="ignore"), path.name


def test_ci_runs_this_file():
    assert "tests/test_trial_shadow_v2_rule.py" in (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
