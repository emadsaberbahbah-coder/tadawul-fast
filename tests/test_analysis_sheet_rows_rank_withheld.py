"""WP-2b (routes/analysis_sheet_rows.py v4.8.2) - the per-request GLOBAL-RANK
pass withholds a row the engine itself withheld from Rank (Overall).

The daily sync fetches the four market pages through this router in 25-row
batches, so _apply_global_rank_overall is the last ranker before the sheet.
Under data_engine_v2 5.153.0 TFB_SETTLE_UNRESOLVED=enforce a non-converged
row publishes a numeric pass-1 score tagged settle_unresolved:enforce:...;
the route must not stamp it a within-batch rank. Exact token prefixes
(f7_settle_failed: / settle_unresolved:), never substrings. Rows without
either tag are ranked exactly as v4.8.1 (byte-identical).

Synthetic market rows only. Every test pins TFB_ANALYSIS_GLOBAL_RANK and
restores the environment.

Run: python -m pytest -q tests/test_analysis_sheet_rows_rank_withheld.py   (or python tests/test_analysis_sheet_rows_rank_withheld.py)
"""
from __future__ import annotations

import copy
import functools
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import routes.analysis_sheet_rows as asr  # noqa: E402

TAGS = ("", "settle_unresolved:enforce:pass_limit:p3:ov=70:rc=buy", "f7_settle_failed:non_converged",
        "low_data_trust")


def _isolated(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        saved = dict(os.environ)
        try:
            os.environ.pop("TFB_ANALYSIS_GLOBAL_RANK", None)   # default ON
            return fn(*args, **kwargs)
        finally:
            os.environ.clear()
            os.environ.update(saved)
    return wrapper


def _rows(tagged=True):
    out = []
    for i, (score, tag) in enumerate(zip((80.0, 70.0, 60.0, 50.0), TAGS)):
        out.append({"symbol": "R%d.US" % i, "overall_score": score, "rank_overall": 1,
                    "warnings": ("provider_ok; " + tag) if (tagged and tag) else "provider_ok"})
    return out


def _v481_reference(rows, page):
    """The v4.8.1 predicate, re-stated: rank iff numeric score and no
    low_data_trust substring (the pre-WP-2b rule)."""
    rows = copy.deepcopy(rows)
    eligible = []
    for row in rows:
        score = asr._coerce_rank_score(row.get("overall_score"))
        if score is None or asr._LOW_TRUST_TAG in str(row.get("warnings") or "").lower():
            if "rank_overall" in row:
                row["rank_overall"] = ""
            continue
        eligible.append((score, row))
    eligible.sort(key=lambda t: t[0], reverse=True)
    for rank, (_s, row) in enumerate(eligible, start=1):
        row["rank_overall"] = rank
    return rows


@_isolated
def test_r1_engine_withheld_rows_are_not_reranked():
    for page in ("Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds"):
        rows = _rows(tagged=True)
        assert asr._apply_global_rank_overall(rows, page) == 1
        assert [r["rank_overall"] for r in rows] == [1, "", "", ""]


@_isolated
def test_r2_untagged_rows_rank_exactly_as_v481():
    for page in ("Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds"):
        rows = _rows(tagged=False)
        reference = _v481_reference(rows, page)
        assert asr._apply_global_rank_overall(rows, page) == 4
        assert [r["rank_overall"] for r in rows] == [1, 2, 3, 4]
        assert rows == reference
    # mixed population without either new prefix: identical to the v4.8.1 rule
    mixed = _rows(tagged=False)
    mixed[1]["warnings"] = "margin_publish:profit_margin:unresolved; x_settle_unresolved:y"
    mixed[3]["warnings"] = "low_data_trust"
    reference = _v481_reference(mixed, "Global_Markets")
    asr._apply_global_rank_overall(mixed, "Global_Markets")
    assert mixed == reference and [r["rank_overall"] for r in mixed] == [1, 2, 3, ""]


@_isolated
def test_r3_predicate_exact_prefix_and_list_cells():
    w = asr._rank_withheld_by_tag
    assert w("a; settle_unresolved:observe:pass_limit:p3") and w("F7_SETTLE_FAILED:error")
    assert w(["a", "b; settle_unresolved:enforce:oscillation:p2"]) and w(("f7_settle_failed:error",))
    assert not w(None) and not w("") and not w([]) and not w("f7_settle:observe:st3:p2:chg1")
    assert not w("margin_publish:profit_margin:unresolved") and not w("rank_skipped_settle_unresolved")
    assert asr._RANK_WITHHELD_PREFIXES == ("f7_settle_failed:", "settle_unresolved:")
    # 2026-10-13: route auth scope-path follow-up (v4.8.4); pin moved in place.
    assert asr.ANALYSIS_SHEET_ROWS_VERSION == "4.8.4"


@_isolated
def test_r4_scope_and_kill_switch_unchanged():
    rows = _rows(tagged=True)
    before = copy.deepcopy(rows)
    assert asr._apply_global_rank_overall(rows, "My_Portfolio") == 0 and rows == before
    os.environ["TFB_ANALYSIS_GLOBAL_RANK"] = "0"
    assert asr._apply_global_rank_overall(rows, "Global_Markets") == 0 and rows == before


TESTS = [test_r1_engine_withheld_rows_are_not_reranked, test_r2_untagged_rows_rank_exactly_as_v481,
         test_r3_predicate_exact_prefix_and_list_cells, test_r4_scope_and_kill_switch_unchanged]

if __name__ == "__main__":
    import traceback
    fails = 0
    for t in TESTS:
        try:
            t()
            print("PASS", t.__name__)
        except Exception:
            fails += 1
            print("FAIL", t.__name__)
            traceback.print_exc()
    print("RESULT", "PASS" if not fails else "FAIL", "%d/%d" % (len(TESTS) - fails, len(TESTS)),
          "route", asr.ANALYSIS_SHEET_ROWS_VERSION)
    sys.exit(1 if fails else 0)
