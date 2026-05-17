"""
test_v573_engine.py — behavioral acceptance tests for data_engine_v2.py v5.73.0

Tests cover:
  - The 19 acceptance tests enumerated in the audit's section 15
  - 4 additional regressions identified in this session's dashboard analysis
    and JSON probe (HCLTECH.NSE, EXE.US, ALAFCO.KW patterns)

Runs against a stub `core.scoring` (in this directory's `core/scoring.py`).
The production deployment uses the real v5.3.0 scoring module.

Invocation:
    cd /home/claude/work
    python3 test_v573_engine.py

A non-zero exit code indicates at least one failure; details printed.
"""
from __future__ import annotations
import os
import sys
import traceback
from typing import Any, Callable, Dict, List, Tuple

sys.path.insert(0, "/home/claude/work")

# Import the engine under test
from core import data_engine_v2 as de  # noqa: E402


# ----------------------------------------------------------------------------
# Tiny test framework — no external deps; just records pass/fail with traceback
# ----------------------------------------------------------------------------
_RESULTS: List[Tuple[str, bool, str]] = []


def run_test(name: str, fn: Callable[[], None]) -> None:
    try:
        fn()
        _RESULTS.append((name, True, ""))
    except AssertionError as exc:
        _RESULTS.append((name, False, f"AssertionError: {exc}"))
    except Exception as exc:  # noqa: BLE001
        _RESULTS.append(
            (name, False, f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}")
        )


# Helper: make a "normal" row that should produce a non-empty classification
def make_normal_row(**overrides: Any) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "symbol": "TEST.US",
        "current_price": 100.0,
        "previous_close": 99.0,
        "market_cap": 1_000_000_000.0,
        "revenue_ttm": 100_000_000.0,
        "eps_ttm": 1.5,
        "pe_ttm": 15.0,
        "rsi_14": 55.0,
        "volatility_30d": 0.20,
        "max_drawdown_1y": -0.15,
        "week_52_high": 120.0,
        "week_52_low": 80.0,
        "overall_score": 70.0,
        "risk_score": 50.0,
        "confidence_score": 70.0,
        "expected_roi_3m": 0.05,  # FRACTION = 5%
    }
    row.update(overrides)
    return row


# Helper: make an "empty" row that should trigger the empty-row guard
def make_empty_row(**overrides: Any) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "symbol": "ALAFCO.KW",
        # all price/fundamental/derived keys absent or None
    }
    row.update(overrides)
    return row


# ============================================================================
# Section 15 — 19 acceptance tests from the audit
# ============================================================================

def test_no_local_recommendation_ladder() -> None:
    """The classifier body must not contain a local BUY/ACCUMULATE/REDUCE/HOLD ladder."""
    import re
    import inspect
    src = inspect.getsource(de._classify_recommendation_8tier)
    # The legacy ladder uses literal `rec, priority = "X", N` assignments.
    bad_patterns = [
        r'rec,\s*priority\s*=\s*"STRONG_BUY"',
        r'rec,\s*priority\s*=\s*"BUY"',
        r'rec,\s*priority\s*=\s*"ACCUMULATE"',
        r'rec,\s*priority\s*=\s*"HOLD"',
        r'rec,\s*priority\s*=\s*"REDUCE"',
        r'rec,\s*priority\s*=\s*"SELL"',
        r'rec,\s*priority\s*=\s*"STRONG_SELL"',
        r'rec,\s*priority\s*=\s*"AVOID"',
    ]
    for pat in bad_patterns:
        assert not re.search(pat, src), (
            f"Local ladder pattern still present in classifier: {pat}"
        )


def test_no_accumulate_emitted() -> None:
    """Running the classifier on representative rows never produces ACCUMULATE."""
    for overall in (10, 25, 35, 50, 65, 80, 90):
        for risk in (20, 50, 80):
            for conf in (40, 60, 80):
                for roi3 in (-0.05, 0.0, 0.03, 0.08):
                    row = make_normal_row(
                        overall_score=overall,
                        risk_score=risk,
                        confidence_score=conf,
                        expected_roi_3m=roi3,
                    )
                    de._classify_recommendation_8tier(row)
                    rec = row.get("recommendation", "")
                    assert rec != "ACCUMULATE", (
                        f"ACCUMULATE emitted at overall={overall} risk={risk} "
                        f"conf={conf} roi3={roi3}"
                    )
                    assert rec != "AVOID", (
                        f"AVOID emitted at overall={overall} risk={risk} "
                        f"conf={conf} roi3={roi3}"
                    )
                    # Must be in canonical 6-tier enum
                    assert rec in de._V573_RECOMMENDATION_ENUM, (
                        f"Non-canonical recommendation '{rec}' emitted at "
                        f"overall={overall} risk={risk} conf={conf} roi3={roi3}"
                    )


def test_provider_rating_preserved() -> None:
    """An upstream provider recommendation lands in provider_rating, not recommendation."""
    os.environ.pop("TFB_TRUST_PROVIDER_RECO", None)  # ensure default False
    row = make_normal_row()
    row["recommendation"] = "STRONG_BUY"  # provider value
    de._classify_recommendation_8tier(row)
    assert row.get("provider_rating") == "STRONG_BUY", (
        f"Expected provider_rating=STRONG_BUY, got {row.get('provider_rating')!r}"
    )
    # Final recommendation comes from engine, not provider, by default
    assert row.get("recommendation_source") == "engine", (
        f"Expected recommendation_source=engine, got {row.get('recommendation_source')!r}"
    )


def test_recommendation_source_engine_default() -> None:
    """A row classified by the engine path gets recommendation_source=engine."""
    os.environ.pop("TFB_TRUST_PROVIDER_RECO", None)
    row = make_normal_row()
    de._classify_recommendation_8tier(row)
    assert row.get("recommendation_source") == "engine", (
        f"Expected recommendation_source=engine, got {row.get('recommendation_source')!r}"
    )


def test_rank_overall_uses_overall_only() -> None:
    """Ranking is by overall_score; opportunity_score is not consulted."""
    rows = [
        {"symbol": "A", "overall_score": 80.0, "opportunity_score": 30.0},
        {"symbol": "B", "overall_score": 60.0, "opportunity_score": 90.0},
        {"symbol": "C", "overall_score": 70.0, "opportunity_score": 10.0},
    ]
    de._apply_rank_overall(rows)
    assert rows[0]["rank_overall"] == 1, "A should be rank 1 (overall=80)"
    assert rows[1]["rank_overall"] == 3, "B should be rank 3 (overall=60, NOT boosted by opp=90)"
    assert rows[2]["rank_overall"] == 2, "C should be rank 2 (overall=70)"


def test_rank_overall_no_opportunity_fallback() -> None:
    """Rows without overall_score are unranked even if they have opportunity_score."""
    rows = [
        {"symbol": "A", "overall_score": 70.0},
        {"symbol": "B", "overall_score": None, "opportunity_score": 85.0},  # was rank 1 before
        {"symbol": "C", "overall_score": 60.0},
    ]
    de._apply_rank_overall(rows)
    assert rows[0]["rank_overall"] == 1, "A should be rank 1"
    assert rows[1].get("rank_overall") in (None, ), (
        f"B should be unranked, got rank_overall={rows[1].get('rank_overall')!r}"
    )
    # And should have the rank_skipped warning
    warnings_str = rows[1].get("warnings", "")
    assert "rank_skipped_no_overall_score" in warnings_str, (
        f"Expected rank_skipped warning, got warnings={warnings_str!r}"
    )
    assert rows[2]["rank_overall"] == 2, "C should be rank 2"


def test_full_criteria_snapshot_serialized() -> None:
    """Top10 criteria snapshot serializes ALL keys, not a manual allowlist."""
    criteria = {
        "top_n": 10,
        "pages_selected": ["Market_Leaders", "Global_Markets"],
        "horizon_days": 90,
        "risk_level": "MODERATE",
        "min_expected_roi": 0.05,
        "confidence_level": 0.65,
        "direct_symbols": ["AAPL", "MSFT"],
        # Fields the v5.70 allowlist DROPPED — must now survive:
        "custom_filter_x": "experimental",
        "user_id": "u-12345",
        "request_id": "req-abcdef",
        "extended_horizon_months": 18,
    }
    snap = de._top10_criteria_snapshot(criteria)
    # Each NON-DROPPED key from criteria must appear in the snapshot
    for k in ("custom_filter_x", "user_id", "request_id", "extended_horizon_months"):
        assert k in snap, f"Key '{k}' missing from full snapshot: {snap[:200]}"


def test_top10_criteria_snapshot_char_cap() -> None:
    """Snapshot respects TFB_CRITERIA_SNAPSHOT_MAX_CHARS and marks truncation."""
    os.environ["TFB_CRITERIA_SNAPSHOT_MAX_CHARS"] = "200"
    try:
        criteria = {f"field_{i}": "X" * 20 for i in range(50)}  # ~> 1000 chars
        snap = de._top10_criteria_snapshot(criteria)
        assert len(snap) <= 200, f"Snapshot exceeded cap: {len(snap)} chars"
        assert "[truncated at" in snap, f"Truncation marker missing: {snap}"
    finally:
        del os.environ["TFB_CRITERIA_SNAPSHOT_MAX_CHARS"]


def test_instrument_schema_has_confidence_score_data_provider_last_updated() -> None:
    """v5.70 schema-alignment fields are still in the canonical key list."""
    keys = de.INSTRUMENT_CANONICAL_KEYS
    for must_have in ("confidence_score", "data_provider", "last_updated_utc", "last_updated_riyadh"):
        assert must_have in keys, f"Canonical schema missing '{must_have}'"


def test_instrument_schema_count_unchanged() -> None:
    """Per Q5: schema stays at 97 in this patch (no expansion to 103)."""
    assert len(de.INSTRUMENT_CANONICAL_KEYS) == 97, (
        f"Schema count drifted from 97 to {len(de.INSTRUMENT_CANONICAL_KEYS)}"
    )


def test_kw_suffix_maps_to_kwd_kuwait() -> None:
    """MENA suffix .KW resolves to Boursa Kuwait / KWD / Kuwait."""
    assert ".KW" in de._SUFFIX_TO_LOCALE, "Missing .KW suffix"
    exch, cur, country = de._SUFFIX_TO_LOCALE[".KW"]
    assert exch == "Boursa Kuwait", f"Expected 'Boursa Kuwait', got {exch!r}"
    assert cur == "KWD", f"Expected 'KWD', got {cur!r}"
    assert country == "Kuwait", f"Expected 'Kuwait', got {country!r}"


def test_qa_suffix_maps_to_qatar() -> None:
    """MENA suffix .QA resolves to Qatar Exchange / QAR / Qatar."""
    assert ".QA" in de._SUFFIX_TO_LOCALE
    exch, cur, country = de._SUFFIX_TO_LOCALE[".QA"]
    assert cur == "QAR" and country == "Qatar", (
        f"Expected QAR/Qatar, got {cur!r}/{country!r}"
    )


def test_ae_suffix_maps_to_uae() -> None:
    """MENA suffix .AE resolves to UAE."""
    assert ".AE" in de._SUFFIX_TO_LOCALE
    _, cur, country = de._SUFFIX_TO_LOCALE[".AE"]
    assert cur == "AED" and country == "UAE", (
        f"Expected AED/UAE, got {cur!r}/{country!r}"
    )


def test_kse_suffix_unchanged_saudi() -> None:
    """.KSE remains mapped to Saudi/SAR per Q4 (do not remap inside engine)."""
    assert ".KSE" in de._SUFFIX_TO_LOCALE
    exch, cur, country = de._SUFFIX_TO_LOCALE[".KSE"]
    assert cur == "SAR" and country == "Saudi Arabia", (
        f".KSE was remapped — should be SAR/Saudi Arabia, got {cur!r}/{country!r}"
    )


def test_empty_row_suppresses_scores() -> None:
    """Empty-row guard zeros scores and emits canonical HOLD with empty_row source."""
    row = make_empty_row()
    de._classify_recommendation_8tier(row)
    assert row.get("recommendation") == "HOLD", (
        f"Empty row should get HOLD, got {row.get('recommendation')!r}"
    )
    assert row.get("recommendation_source") == "empty_row", (
        f"Expected empty_row source, got {row.get('recommendation_source')!r}"
    )
    # Scores nulled
    assert row.get("overall_score") is None
    assert row.get("risk_score") is None
    assert row.get("confidence_score") is None
    # Warning tag emitted
    assert "empty_row_no_provider_data" in row.get("warnings", ""), (
        f"Missing empty_row warning: {row.get('warnings')!r}"
    )


def test_empty_row_does_not_emit_fake_reduce() -> None:
    """Empty rows never produce REDUCE / SELL / STRONG_SELL — always HOLD."""
    for overrides in (
        {},
        {"symbol": "BLANK.X"},
        {"recommendation": "REDUCE"},  # even with stale provider value
    ):
        row = make_empty_row(**overrides)
        de._classify_recommendation_8tier(row)
        assert row["recommendation"] == "HOLD", (
            f"Empty row produced non-HOLD: {row['recommendation']}"
        )
        assert row["recommendation_source"] == "empty_row"


def test_outlier_pe_is_nulled() -> None:
    """Sanitization nulls extreme P/E values and emits a sanitized warning tag."""
    os.environ.pop("TFB_DISABLE_V572_SANITIZATION", None)
    os.environ.pop("TFB_SANITIZATION_ENABLED", None)
    row = {"symbol": "X.US", "pe_ttm": 9999.0}  # way out of band
    de._apply_v572_sanitization(row)
    assert row["pe_ttm"] is None, f"Extreme P/E not nulled: {row['pe_ttm']}"
    assert "sanitized:pe_ttm_out_of_range" in row.get("warnings", ""), (
        f"Missing sanitized warning: {row.get('warnings')!r}"
    )


def test_outlier_ev_ebitda_is_nulled() -> None:
    """HCLTECH.NSE-style EV/EBITDA = 1056 should be nulled (limit ±500)."""
    row = {"symbol": "HCLTECH.NSE", "ev_ebitda": 1056.56}
    de._apply_v572_sanitization(row)
    assert row["ev_ebitda"] is None, f"EV/EBITDA=1056 not nulled: {row['ev_ebitda']}"
    assert "sanitized:ev_ebitda_out_of_range" in row.get("warnings", "")


def test_corrupt_52w_high_is_nulled() -> None:
    """Inverted 52-week bounds get nulled with appropriate warning."""
    row = {"symbol": "X.US", "week_52_high": 50.0, "week_52_low": 100.0}  # inverted
    de._apply_v572_sanitization(row)
    assert row["week_52_high"] is None and row["week_52_low"] is None, (
        "Inverted 52-week bounds should be nulled"
    )
    assert "sanitized:week_52_bounds_inverted" in row.get("warnings", "")


def test_cross_currency_revenue_flagged() -> None:
    """Implausibly large market_cap on USD-labeled row gets flagged.

    Note: the threshold (1e13 = $10T) is intentionally lenient so it does
    NOT fire on legitimate US mega-caps (AAPL $4.4T, AMZN $2.8T). The
    rejected v5.72.0 build had a false-positive issue here: it tagged
    AAPL/AMZN/TSM.US as `market_cap_currency_suspect` despite their
    USD market caps being correct. v5.73.0 only fires when the value
    is implausible even for the largest legitimate US company.
    """
    # Legitimate US mega-cap — must NOT be flagged
    row_legit = {"symbol": "AAPL", "currency": "USD", "market_cap": 4_409_585_053_240.0}
    de._apply_v572_sanitization(row_legit)
    assert "market_cap_currency_suspect" not in (row_legit.get("warnings") or ""), (
        f"False positive on AAPL — warnings should not contain market_cap_currency_suspect. "
        f"Got: {row_legit.get('warnings')!r}"
    )

    # Implausible value (e.g. INR market cap of Reliance ~20T mislabeled USD)
    row_suspect = {"symbol": "RELIANCE.NS", "currency": "USD", "market_cap": 2.0e13}
    de._apply_v572_sanitization(row_suspect)
    assert "market_cap_currency_suspect" in (row_suspect.get("warnings") or ""), (
        f"Implausible $20T USD market_cap should be flagged. "
        f"warnings={row_suspect.get('warnings')!r}"
    )


def test_sanitization_warning_tokens_emitted() -> None:
    """Multiple sanitization rules combine warning tags additively."""
    row = {
        "symbol": "MIXED.X",
        "pe_ttm": 99999.0,           # outlier
        "debt_to_equity": 50000.0,    # outlier
        "week_52_high": 10.0,
        "week_52_low": 100.0,         # inverted
    }
    de._apply_v572_sanitization(row)
    warnings = row.get("warnings", "")
    assert "sanitized:pe_ttm_out_of_range" in warnings
    assert "sanitized:debt_to_equity_out_of_range" in warnings
    assert "sanitized:week_52_bounds_inverted" in warnings


def test_cache_key_includes_schema_version() -> None:
    """_make_cache_key includes schema version so cross-version cache leaks are prevented."""
    k = de._make_cache_key("AAPL", page="Market_Leaders", provider_profile="eodhd")
    assert "AAPL" in k
    assert "market_leaders" in k.lower()
    assert "eodhd" in k
    assert "5.73.0" in k or de.__version__ in k, f"Schema version missing from key: {k}"


# ============================================================================
# Additional tests (from this session's dashboard / JSON probe findings)
# ============================================================================

def test_legacy_provider_accumulate_collapses_to_buy() -> None:
    """A provider returning ACCUMULATE results in provider_rating=BUY (per Q3 mapping)."""
    os.environ.pop("TFB_TRUST_PROVIDER_RECO", None)
    row = make_normal_row()
    row["recommendation"] = "ACCUMULATE"  # what HCLTECH.NSE JSON returned
    de._classify_recommendation_8tier(row)
    assert row.get("provider_rating") == "BUY", (
        f"ACCUMULATE should collapse to BUY in provider_rating, got {row.get('provider_rating')!r}"
    )
    assert row.get("recommendation") in de._V573_RECOMMENDATION_ENUM
    assert row.get("recommendation") != "ACCUMULATE"


def test_legacy_provider_avoid_collapses_to_strong_sell() -> None:
    """A provider returning AVOID results in provider_rating=STRONG_SELL."""
    row = make_normal_row()
    row["recommendation"] = "AVOID"
    de._classify_recommendation_8tier(row)
    assert row.get("provider_rating") == "STRONG_SELL", (
        f"AVOID should collapse to STRONG_SELL in provider_rating, got {row.get('provider_rating')!r}"
    )


def test_provider_override_when_env_true() -> None:
    """When TFB_TRUST_PROVIDER_RECO=true, provider wins and source=provider_override."""
    os.environ["TFB_TRUST_PROVIDER_RECO"] = "true"
    try:
        row = make_normal_row()
        row["recommendation"] = "STRONG_BUY"
        de._classify_recommendation_8tier(row)
        assert row.get("recommendation") == "STRONG_BUY"
        assert row.get("recommendation_source") == "provider_override"
        assert row.get("provider_rating") == "STRONG_BUY"
    finally:
        del os.environ["TFB_TRUST_PROVIDER_RECO"]


def test_fraction_to_points_helper() -> None:
    """_fraction_to_points is strict — always × 100, no heuristic guess."""
    assert de._fraction_to_points(0.025) == 2.5
    assert de._fraction_to_points(0.00774) == 0.774  # HCLTECH.NSE style — no inflation
    assert de._fraction_to_points(-0.0001034) == -0.01034  # EXE.US style
    assert de._fraction_to_points(1.5) == 150.0  # heuristic would have multiplied; strict does the same here
    assert de._fraction_to_points(None) is None
    assert de._fraction_to_points("invalid") is None


def test_sanitization_can_be_disabled() -> None:
    """TFB_SANITIZATION_ENABLED=false short-circuits the sanitization passes."""
    os.environ["TFB_SANITIZATION_ENABLED"] = "false"
    try:
        row = {"symbol": "X", "pe_ttm": 99999.0}
        de._apply_v572_sanitization(row)
        assert row["pe_ttm"] == 99999.0, "Sanitization should be disabled by env"
    finally:
        del os.environ["TFB_SANITIZATION_ENABLED"]


def test_legacy_disable_env_honored() -> None:
    """Legacy TFB_DISABLE_V572_SANITIZATION=true is honored for back-compat."""
    os.environ["TFB_DISABLE_V572_SANITIZATION"] = "true"
    try:
        row = {"symbol": "X", "pe_ttm": 99999.0}
        de._apply_v572_sanitization(row)
        assert row["pe_ttm"] == 99999.0
    finally:
        del os.environ["TFB_DISABLE_V572_SANITIZATION"]


# ============================================================================
# Test runner
# ============================================================================

ALL_TESTS = [
    # 19 acceptance tests from audit section 15
    ("test_no_local_recommendation_ladder", test_no_local_recommendation_ladder),
    ("test_no_accumulate_emitted", test_no_accumulate_emitted),
    ("test_provider_rating_preserved", test_provider_rating_preserved),
    ("test_recommendation_source_engine_default", test_recommendation_source_engine_default),
    ("test_rank_overall_uses_overall_only", test_rank_overall_uses_overall_only),
    ("test_rank_overall_no_opportunity_fallback", test_rank_overall_no_opportunity_fallback),
    ("test_full_criteria_snapshot_serialized", test_full_criteria_snapshot_serialized),
    ("test_top10_criteria_snapshot_char_cap", test_top10_criteria_snapshot_char_cap),
    ("test_instrument_schema_has_confidence_score_data_provider_last_updated",
     test_instrument_schema_has_confidence_score_data_provider_last_updated),
    ("test_instrument_schema_count_unchanged", test_instrument_schema_count_unchanged),
    ("test_kw_suffix_maps_to_kwd_kuwait", test_kw_suffix_maps_to_kwd_kuwait),
    ("test_qa_suffix_maps_to_qatar", test_qa_suffix_maps_to_qatar),
    ("test_ae_suffix_maps_to_uae", test_ae_suffix_maps_to_uae),
    ("test_kse_suffix_unchanged_saudi", test_kse_suffix_unchanged_saudi),
    ("test_empty_row_suppresses_scores", test_empty_row_suppresses_scores),
    ("test_empty_row_does_not_emit_fake_reduce", test_empty_row_does_not_emit_fake_reduce),
    ("test_outlier_pe_is_nulled", test_outlier_pe_is_nulled),
    ("test_outlier_ev_ebitda_is_nulled", test_outlier_ev_ebitda_is_nulled),
    ("test_corrupt_52w_high_is_nulled", test_corrupt_52w_high_is_nulled),
    ("test_cross_currency_revenue_flagged", test_cross_currency_revenue_flagged),
    ("test_sanitization_warning_tokens_emitted", test_sanitization_warning_tokens_emitted),
    ("test_cache_key_includes_schema_version", test_cache_key_includes_schema_version),

    # Additional tests from this session
    ("test_legacy_provider_accumulate_collapses_to_buy", test_legacy_provider_accumulate_collapses_to_buy),
    ("test_legacy_provider_avoid_collapses_to_strong_sell", test_legacy_provider_avoid_collapses_to_strong_sell),
    ("test_provider_override_when_env_true", test_provider_override_when_env_true),
    ("test_fraction_to_points_helper", test_fraction_to_points_helper),
    ("test_sanitization_can_be_disabled", test_sanitization_can_be_disabled),
    ("test_legacy_disable_env_honored", test_legacy_disable_env_honored),
]


if __name__ == "__main__":
    for name, fn in ALL_TESTS:
        run_test(name, fn)

    print("\n" + "=" * 78)
    print(f"v5.73.0 BEHAVIORAL TEST SUITE — {de.__version__}")
    print("=" * 78)
    passed = sum(1 for _, ok, _ in _RESULTS if ok)
    failed = sum(1 for _, ok, _ in _RESULTS if not ok)
    total = len(_RESULTS)

    for name, ok, msg in _RESULTS:
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {name}")
        if not ok and msg:
            for line in msg.splitlines():
                print(f"         {line}")

    print("-" * 78)
    print(f"  Total: {total} | Pass: {passed} | Fail: {failed}")
    print("=" * 78)
    sys.exit(0 if failed == 0 else 1)
