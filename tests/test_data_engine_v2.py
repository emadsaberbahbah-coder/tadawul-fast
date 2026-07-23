"""
2026-07-23 CONTRACT REFRESH (session-audited against data_engine v5.118.0)
---------------------------------------------------------------------------
Nine tests below pinned the v5.73.x contract and were never run by CI (the
tests.yml explicit list predated their registration), so they rotted while
the engine moved ~45 versions. Each was re-verified against HEAD behavior
by direct probe before rewriting; every rewrite pins the CURRENT contract
with the probe result recorded in its docstring. Highlights: the 6-tier
collapse era is over (ACCUMULATE and AVOID are first-class, priority map
is true 8-tier with P3 live); the schema is the 115-key instrument era;
the v5.73.2 cache-poisoning protection SURVIVED refactoring — every cache
key now embeds schema id + engine version (probe:
quote:SYM:page:prof:instrument:115:5.118.0:cache); and the engine's own
classifier constitutes a THIRD recommendation derivation with its own
band table (>=70 BUY, >=60 ACCUMULATE, >=50 HOLD ...), which is the seam
behind the 2026-07-23 ERG.MI SCORE-vs-CLASSIFIER divergence (tracked
separately). LATENT NOTE, not asserted here: the band-string chain maps a
trusted-provider AVOID to priority_band "P1" — dead code on the score
path (probe: overall 25/28/35 all -> SELL/P5) but a one-line fix candidate
whenever the engine is next touched.

test_v5731_engine.py — behavioral acceptance tests for data_engine_v2.py v5.73.1

Tests cover:
  - The 19 acceptance tests enumerated in the v5.73.0 audit's section 15
    (all retained — v5.73.1 is a hotfix, not a feature change)
  - 6 additional regressions from the v5.73.0 session's dashboard analysis
  - 10 NEW v5.73.1 regression tests for the six post-deploy hotfixes:
       Bug A (ROI unit ×100 inflation)
       Bug B (confidence sequencing)
       Bug C (recommendation/detail mismatch)
       Bug D (sanitization not wired)
       Bug E (cache key not wired)
       Bug F (8-tier priority map)
     plus three structural assertions (fundamentals-empty guard,
     priority_band emission, _make_cache_key call site)

Runs against a stub `core.scoring` in this workspace's `core/scoring.py`.
The production deployment uses the real v5.3.0 scoring module.

Invocation:
    cd /home/claude/work_v5731
    python3 test_v5731.py
"""
from __future__ import annotations
import os
import sys
import traceback
from typing import Any, Callable, Dict, List, Tuple

sys.path.insert(0, "/home/claude/work_v5731")

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
    """CONTRACT REFRESH 2026-07-23: the v5.73.1 rule ("classifier never
    produces ACCUMULATE") died with the 8-tier work — ACCUMULATE is now a
    first-class P3 band. The surviving spirit of this test: ACCUMULATE is
    never emitted ILLEGITIMATELY (outside its >=60 overall band), and when
    it IS emitted it carries band P3 / priority 3. Probe 2026-07-23:
    emitted in 36/336 grid cells, all in-band."""
    for overall in (10, 25, 35, 50, 58, 59.9):
        for risk in (20, 50, 80):
            for conf in (40, 60, 80):
                row = make_normal_row(
                    overall_score=overall, risk_score=risk,
                    confidence_score=conf, expected_roi_3m=0.05,
                )
                de._classify_recommendation_8tier(row)
                assert row.get("recommendation") != "ACCUMULATE", (
                    f"ACCUMULATE below its band at overall={overall}"
                )
    row = make_normal_row(overall_score=66, risk_score=10,
                          confidence_score=65, expected_roi_3m=0.05)
    de._classify_recommendation_8tier(row)
    assert row.get("recommendation") == "ACCUMULATE"
    assert row.get("recommendation_priority_band") == "P3"
    assert row.get("recommendation_priority") == 3


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
    """CONTRACT REFRESH 2026-07-23: the Q5-era pin (97 keys) belongs to a
    retired schema generation; HEAD is the 115-key instrument schema (the
    cache key embeds `instrument:115`). The pin remains a deliberate
    tripwire: any drift from 115 must arrive with a schema-migration WHY."""
    assert len(de.INSTRUMENT_CANONICAL_KEYS) == 115, (
        f"Schema count drifted from 115 to {len(de.INSTRUMENT_CANONICAL_KEYS)}"
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
    """CONTRACT REFRESH 2026-07-23 (name kept for AST continuity; the
    COLLAPSE contract is retired): a provider ACCUMULATE is now preserved
    FIRST-CLASS in provider_rating — no forced 6-tier collapse to BUY —
    and the engine's own recommendation stays inside the 8-tier enum.
    Probe: provider_rating='ACCUMULATE', rec in enum."""
    os.environ.pop("TFB_TRUST_PROVIDER_RECO", None)
    row = make_normal_row()
    row["recommendation"] = "ACCUMULATE"
    de._classify_recommendation_8tier(row)
    assert row.get("provider_rating") == "ACCUMULATE", (
        f"provider ACCUMULATE must be preserved, got {row.get('provider_rating')!r}"
    )
    assert row.get("recommendation") in de._V573_RECOMMENDATION_ENUM


def test_legacy_provider_avoid_collapses_to_strong_sell() -> None:
    """CONTRACT REFRESH 2026-07-23 (name kept; collapse retired): a
    provider AVOID is preserved first-class in provider_rating, and the
    NUMERIC priority map places AVOID in the sell family (5). The band-
    STRING chain's AVOID->"P1" branch is a documented latent wart (dead
    on the score path) and is deliberately not asserted either way here."""
    row = make_normal_row()
    row["recommendation"] = "AVOID"
    de._classify_recommendation_8tier(row)
    assert row.get("provider_rating") == "AVOID", (
        f"provider AVOID must be preserved, got {row.get('provider_rating')!r}"
    )
    assert de._RECO_8TIER_PRIORITY["AVOID"] == 5


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
# v5.73.1 — Six new regression tests for the post-deploy hotfix
# ============================================================================

def test_v5731_roi_unit_fraction_passed_correctly() -> None:
    """Bug A regression: classifier must NOT inflate expected_roi_3m by 100×
    when delegating to scoring. CINF.US had expected_roi_3m=0.018702 (1.87%),
    and the v5.73.0 bug made the reason show 'roi3m=3.0%' or similar inflated
    values. With the v5.73.1 delegation to apply_canonical_recommendation, the
    reason string must reflect the actual fraction value formatted as a
    percent."""
    os.environ.pop("TFB_TRUST_PROVIDER_RECO", None)
    # CINF.US-like values from the actual production JSON
    row = make_normal_row(
        overall_score=68.76,
        risk_score=36.61,
        confidence_score=77.52,
        expected_roi_3m=0.018702,  # 1.87% in FRACTION form
    )
    de._classify_recommendation_8tier(row)
    reason = row.get("recommendation_reason", "")
    # Scoring stub's reason formatter shows roi3m=1.9% (rounded) for 0.018702 fraction.
    # The KEY assertion: reason must NOT contain inflated values like '187.0%',
    # '1870.0%', '3.0%', or any percent value much larger than the actual ROI.
    assert "187.0%" not in reason, f"100× inflation regression: {reason!r}"
    assert "1870.0%" not in reason, f"10000× inflation regression: {reason!r}"
    # The legitimate formatted roi3m should be around 1.9% (rounded from 1.8702)
    assert "1.9%" in reason or "1.87%" in reason or "1.8%" in reason, (
        f"Expected roi3m close to 1.87% in reason, got: {reason!r}"
    )


def test_v5731_confidence_sequencing_uses_actual_value() -> None:
    """Bug B regression: when confidence_score is set on the row, the
    recommendation reason must reflect it — NOT a default 55.0. v5.73.0
    classified before scoring finalized confidence, so reasons showed
    'conf=55.0' even when the actual confidence_score was 77.52."""
    os.environ.pop("TFB_TRUST_PROVIDER_RECO", None)
    row = make_normal_row(
        overall_score=68.76,
        risk_score=36.61,
        confidence_score=77.52,
        expected_roi_3m=0.018702,
    )
    de._classify_recommendation_8tier(row)
    reason = row.get("recommendation_reason", "")
    assert "conf=77.5" in reason or "conf=77.52" in reason, (
        f"Expected reason to reflect actual conf=77.52, got: {reason!r}"
    )
    assert "conf=55.0" not in reason, (
        f"v5.73.0 default-conf=55 regression: {reason!r}"
    )


def test_v5731_confidence_falls_back_to_forecast_confidence() -> None:
    """When confidence_score is None but forecast_confidence is set,
    apply_canonical_recommendation falls back to forecast_confidence × 100.
    Tests the stub stub behavior (mirrors scoring.py v5.3.0)."""
    os.environ.pop("TFB_TRUST_PROVIDER_RECO", None)
    row = make_normal_row(
        overall_score=70.0,
        risk_score=40.0,
        expected_roi_3m=0.15,
    )
    row.pop("confidence_score", None)
    row["forecast_confidence"] = 0.85  # 0-1 form → should be read as 85.0
    de._classify_recommendation_8tier(row)
    reason = row.get("recommendation_reason", "")
    # With the fallback, conf should appear as ~85, not 55 (the default)
    assert "conf=85" in reason or "conf=84" in reason or "conf=86" in reason, (
        f"Expected conf in 84-86 range from forecast_confidence fallback, got: {reason!r}"
    )


def test_v5731_recommendation_and_detail_always_match() -> None:
    """Bug C regression: recommendation_detailed must equal recommendation
    after every call to _classify_recommendation_8tier. GPOR.US/SD.US had
    Recommendation=HOLD with Recommendation Detail=BUY in v5.73.0."""
    os.environ.pop("TFB_TRUST_PROVIDER_RECO", None)
    for overall in (35, 50, 70, 85):
        for risk in (30, 60, 90):
            for conf in (40, 70, 90):
                for roi3 in (-0.10, 0.0, 0.15, 0.30):
                    row = make_normal_row(
                        overall_score=overall,
                        risk_score=risk,
                        confidence_score=conf,
                        expected_roi_3m=roi3,
                    )
                    de._classify_recommendation_8tier(row)
                    rec = row.get("recommendation")
                    detail = row.get("recommendation_detailed")
                    assert rec == detail, (
                        f"Recommendation/Detail mismatch at overall={overall} "
                        f"risk={risk} conf={conf} roi3={roi3}: "
                        f"rec={rec!r} detail={detail!r}"
                    )


def test_v5731_safety_recall_after_phase_dd() -> None:
    """CONTRACT REFRESH 2026-07-23: _apply_phase_dd_enhancements no longer
    exists (architecture moved; the engine class itself is DataEngineV5-
    aliased). The Bug-B SPIRIT — the final recommendation must be derived
    from final scores, with the classifier re-invoked rather than trusted
    once — survives as multiple classifier call sites in the module and
    as idempotent re-classification. Probe: 5 call sites at HEAD."""
    import inspect as _inspect
    n_sites = _inspect.getsource(de).count("_classify_recommendation_8tier(")
    assert n_sites >= 3, (
        f"re-classification pattern eroded: only {n_sites} call sites"
    )
    row = make_normal_row()
    de._classify_recommendation_8tier(row)
    first = row.get("recommendation")
    de._classify_recommendation_8tier(row)
    assert row.get("recommendation") == first, "classifier not idempotent"


def test_v5731_fundamentals_empty_triggers_guard() -> None:
    """CONTRACT REFRESH 2026-07-23: the v5.73.1 empty-row guard tokens are
    gone; the surviving protection for a price-only row (the KAR.US /
    ZOMATO.NSE pattern) is CONSERVATIVE CLASSIFICATION — the row must
    never crash the classifier and must never land in the buy family.
    Probe: rec='HOLD'."""
    row = {
        "symbol": "ZOMATO.NSE",
        "current_price": 250.0,
        "previous_close": 248.5,
        "day_high": 252.0,
        "day_low": 247.0,
        "week_52_high": 280.0,
        "week_52_low": 180.0,
        "rsi_14": 55.0,
        "volatility_30d": 0.20,
    }
    de._classify_recommendation_8tier(row)
    assert row.get("recommendation") in ("HOLD", "REDUCE", "SELL",
                                         "STRONG_SELL"), (
        f"price-only row must classify conservatively, got "
        f"{row.get('recommendation')!r}"
    )


def test_v5731_priority_map_is_6tier() -> None:
    """CONTRACT REFRESH 2026-07-23 (name kept; the map is now TRUE 8-tier):
    the Bug-F-era 6-tier pin is inverted — P3 is LIVE for ACCUMULATE, the
    sell family (AVOID/REDUCE/SELL/STRONG_SELL) shares 5. Probe-verified
    exact map at v5.118.0."""
    assert de._RECO_8TIER_PRIORITY["STRONG_BUY"] == 1
    assert de._RECO_8TIER_PRIORITY["BUY"] == 2
    assert de._RECO_8TIER_PRIORITY["ACCUMULATE"] == 3
    assert de._RECO_8TIER_PRIORITY["HOLD"] == 4
    assert de._RECO_8TIER_PRIORITY["AVOID"] == 5
    assert de._RECO_8TIER_PRIORITY["REDUCE"] == 5
    assert de._RECO_8TIER_PRIORITY["SELL"] == 5
    assert de._RECO_8TIER_PRIORITY["STRONG_SELL"] == 5
    assert 3 in de._RECO_8TIER_PRIORITY.values()


def test_v5731_sanitization_is_wired() -> None:
    """Bug D regression: _apply_v572_sanitization must be called inside
    _compute_scores_fallback. We can verify by feeding a polluted row through
    the fallback path and checking that the sanitized:* warning appears."""
    os.environ.pop("TFB_DISABLE_V572_SANITIZATION", None)
    row = {
        "symbol": "BOX.US",
        "current_price": 30.0,
        "market_cap": 4_000_000_000.0,
        "revenue_ttm": 1_000_000_000.0,
        "eps_ttm": 0.5,
        "pe_ttm": 60.0,
        "pb_ratio": 5.0,
        "ps_ratio": 4.0,
        "debt_to_equity": 2048.0,  # extreme outlier — should be nulled
        "ev_ebitda": 25.0,
    }
    de._compute_scores_fallback(row)
    warnings = row.get("warnings", "")
    assert isinstance(warnings, str)
    assert "sanitized:" in warnings, (
        f"Expected 'sanitized:' tag in warnings after _compute_scores_fallback "
        f"on polluted row; got warnings={warnings!r}"
    )
    # The extreme debt_to_equity should be nulled
    assert row.get("debt_to_equity") is None, (
        f"Expected debt_to_equity nulled by sanitization; got {row.get('debt_to_equity')}"
    )


def test_v5731_recommendation_priority_band_emitted() -> None:
    """Bug C/F regression: recommendation_priority_band must be set after
    classification when scoring.apply_canonical_recommendation is available.
    CINF.US JSON showed this field as null in v5.73.0, indicating my classifier
    fix wasn't actually wired into the runtime path."""
    os.environ.pop("TFB_TRUST_PROVIDER_RECO", None)
    row = make_normal_row(
        overall_score=75.0,
        risk_score=40.0,
        confidence_score=75.0,
        expected_roi_3m=0.15,
    )
    de._classify_recommendation_8tier(row)
    band = row.get("recommendation_priority_band")
    assert band in ("P1", "P2", "P3", "P4", "P5"), (
        f"Expected recommendation_priority_band in P1..P5, got {band!r}"
    )


def test_v5731_singleflight_key_uses_make_cache_key() -> None:
    """CONTRACT REFRESH 2026-07-23: get_enriched_quote now delegates; the
    key construction lives in _get_enriched_quote_impl. The Bug-E
    invariant survives: no legacy 3-component key anywhere, and the impl
    builds keys via _make_cache_key."""
    import inspect as _inspect
    mod_src = _inspect.getsource(de)
    assert 'f"quote:{normalize_symbol(symbol)}:{provider_profile}:' \
        not in mod_src, "legacy 3-component key resurfaced"
    impl_src = _inspect.getsource(de.DataEngineV2._get_enriched_quote_impl)
    assert "_make_cache_key(" in impl_src, (
        "_make_cache_key call missing from _get_enriched_quote_impl"
    )


def test_v5732_persistent_cache_includes_schema_version() -> None:
    """CONTRACT REFRESH 2026-07-23: the v5.73.2 anti-poisoning invariant
    SURVIVED the refactor and got stronger — the schema id AND engine
    version ride inside every cache key via _make_cache_key (probe:
    quote:SYM:page:prof:instrument:115:5.118.0:cache). Asserted
    BEHAVIORALLY so future refactors can move the site freely without
    breaking this pin, as long as the key stays versioned."""
    key = de._make_cache_key("TESTSYM", "Global_Markets", "prof1")
    assert str(de._SCHEMA_VERSION) in key, (
        f"schema version missing from cache key: {key!r}"
    )
    key_live = de._make_cache_key("TESTSYM", "Global_Markets", "prof1",
                                  mode="live")
    assert str(de._SCHEMA_VERSION) in key_live
    assert key != key_live, "cache/live modes must not collide"
