#!/usr/bin/env python3
"""
================================================================================
TFB Wave 3 TEST Patcher — apply_wave3_test_patches.py
================================================================================
BUMPS THE 3 TEST FILES TO v2.6.0 / 90·93 ALIGNMENT (matches schema_registry
v2.6.0, scoring v5.1.0, reco_normalize v7.1.0, insights_builder v1.0.0).

What this patches
-----------------
1. test_endpoints.py                    v2.3.0 -> v2.4.0
   - SCRIPT_VERSION bump
   - Header docstring v2.4.0 changelog
   - _CANONICAL_WIDTHS: instrument 80 -> 90, Top10 83 -> 93
   - _CANONICAL_WIDTHS comment: schema_registry v2.2.0 -> v2.6.0

2. test_schema_alignment.py             v9.3.0 -> v9.4.0
   - Header docstring v9.4.0 changelog
   - + _INSIGHTS_GROUP_KEYS  (5 keys produced by insights_builder)
   - + _EXPECTED_COUNTS_V2_6 matrix (90/90/90/90/90/93/7/9)
   - _detect_schema_capabilities: + is_v2_6_plus, has_insights_group
   - _expected_counts_for_caps: + V2.6 branch
   - test_schema_capabilities_banner: print new flags
   - + test_schema_has_insights_group_columns_when_v2_6_plus

3. test_scoring_engine_contract.py      v3.0.0 -> v3.1.0
   - Header docstring v3.1.0 changelog
   - + try-import core.insights_builder
   - + CANONICAL_INSIGHTS_FIELDS tuple (5 Insights fields)
   - CANONICAL_ASSETSCORES_REQUIRED: + 5 Insights fields
   - + 6 new test methods covering Insights fields + conviction floor cascade

The 5 Insights columns the tests now know about:
  - sector_relative_score  ("Sector-Adj Score")    — float, 0-100, model
  - conviction_score       ("Conviction Score")    — float, 0-100, model
  - top_factors            ("Top Factors")         — str, pipe-separated
  - top_risks              ("Top Risks")           — str, pipe-separated
  - position_size_hint     ("Position Size Hint")  — str, text

How to run
----------
1. Place this script next to your test files (root, tests/, or core/tests/).
2. Run:   python3 apply_wave3_test_patches.py
3. Patched files appear in ./patched/. Diff and copy in.

The patcher refuses to write any file where a patch fails to match — that's
a signal your local file isn't at the expected v2.3.0 / v9.3.0 / v3.0.0
baseline and you should investigate before forcing the change. Conservative
by design: never modifies your originals.

Tip: if your test files are spread across directories, run multiple passes:
    python3 apply_wave3_test_patches.py --src-dir tests --only test_schema_alignment.py
    python3 apply_wave3_test_patches.py --src-dir scripts --only test_endpoints.py
================================================================================
"""

from __future__ import annotations

import argparse
import ast
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


PATCHER_VERSION = "1.0.0"


# =============================================================================
# Patch definitions
# =============================================================================
@dataclass
class Patch:
    label: str
    old: str
    new: str
    occurrences_required: int = 1


@dataclass
class FilePatchPlan:
    source_basename: str
    target_version_old: str
    target_version_new: str
    patches: List[Patch] = field(default_factory=list)
    expected_post_constants: Dict[str, int] = field(default_factory=dict)
    expected_post_substrings: List[str] = field(default_factory=list)


# =============================================================================
# test_endpoints.py  v2.3.0 -> v2.4.0
# =============================================================================
TEST_ENDPOINTS_PLAN = FilePatchPlan(
    source_basename="test_endpoints.py",
    target_version_old="2.3.0",
    target_version_new="2.4.0",
    patches=[
        # 1. Module title in opening docstring
        Patch(
            label="module_title_v2_4_0",
            old='TFB Endpoint Smoke Tester — v2.3.0 (Live-Route Aligned)',
            new='TFB Endpoint Smoke Tester — v2.4.0 (V2.6.0-Aligned)',
        ),
        # 2. Insert v2.4.0 changelog section before the v2.3.0 one
        Patch(
            label="docstring_v2_4_0_changelog",
            old='Why this revision (v2.3.0 vs v2.2.0)\n-------------------------------------',
            new=(
                'Why this revision (v2.4.0 vs v2.3.0)\n'
                '-------------------------------------\n'
                '- 🔑 BUMP: canonical sheet widths bumped to schema_registry v2.6.0:\n'
                '        Market_Leaders / Global_Markets / Commodities_FX /\n'
                '        Mutual_Funds / My_Portfolio   = 80 -> 90\n'
                '        Top_10_Investments            = 83 -> 93\n'
                '    The 5 new columns are an "Insights" group at the END of the canonical\n'
                '    schema (positions 86-90):\n'
                '        sector_relative_score, conviction_score, top_factors,\n'
                '        top_risks, position_size_hint\n'
                '    All 5 are produced by core.insights_builder v1.0.0 (Wave 1) and\n'
                '    consumed by core.scoring v5.1.0+ to enforce the conviction-floor\n'
                '    cascade in core.reco_normalize v7.1.0. Adding at the END preserves\n'
                '    all existing positional indices in the response.\n'
                '\n'
                '- KEEP: every v2.3.0 fix preserved unchanged. Top10 and enriched-quote\n'
                '    canonical-route coverage, RequestException trapping, pass/warn/fail\n'
                '    counter + correct exit codes — all preserved.\n'
                '\n'
                'Why this revision (v2.3.0 vs v2.2.0)\n'
                '-------------------------------------'
            ),
        ),
        # 3. SCRIPT_VERSION constant
        Patch(
            label="script_version_constant",
            old='SCRIPT_VERSION = "2.3.0"',
            new='SCRIPT_VERSION = "2.4.0"',
        ),
        # 4. _CANONICAL_WIDTHS section header comment + dict in one swap
        Patch(
            label="canonical_widths_v2_6_0",
            old=(
                '# ---------------------------------------------------------------------------\n'
                '# Canonical sheet widths (from core.sheets.schema_registry v2.2.0)\n'
                '# ---------------------------------------------------------------------------\n'
                '# These are authoritative — every sheet-rows response MUST match.\n'
                '_CANONICAL_WIDTHS: Dict[str, int] = {\n'
                '    "Market_Leaders": 80,\n'
                '    "Global_Markets": 80,\n'
                '    "Commodities_FX": 80,\n'
                '    "Mutual_Funds": 80,\n'
                '    "My_Portfolio": 80,\n'
                '    "Top_10_Investments": 83,\n'
                '    "Insights_Analysis": 7,\n'
                '    "Data_Dictionary": 9,\n'
                '}'
            ),
            new=(
                '# ---------------------------------------------------------------------------\n'
                '# Canonical sheet widths (from core.sheets.schema_registry v2.6.0)\n'
                '# ---------------------------------------------------------------------------\n'
                '# These are authoritative — every sheet-rows response MUST match.\n'
                '# v2.6.0: instrument pages 85 -> 90 (5 Insights cols appended at end);\n'
                '#         Top10 88 -> 93 (= 90 canonical + 3 Top10 extras).\n'
                '_CANONICAL_WIDTHS: Dict[str, int] = {\n'
                '    "Market_Leaders": 90,\n'
                '    "Global_Markets": 90,\n'
                '    "Commodities_FX": 90,\n'
                '    "Mutual_Funds": 90,\n'
                '    "My_Portfolio": 90,\n'
                '    "Top_10_Investments": 93,\n'
                '    "Insights_Analysis": 7,\n'
                '    "Data_Dictionary": 9,\n'
                '}'
            ),
        ),
    ],
    expected_post_substrings=[
        'SCRIPT_VERSION = "2.4.0"',
        '"Market_Leaders": 90,',
        '"Top_10_Investments": 93,',
        'V2.6.0-Aligned',
    ],
)


# =============================================================================
# test_schema_alignment.py  v9.3.0 -> v9.4.0
# =============================================================================
TEST_SCHEMA_ALIGNMENT_PLAN = FilePatchPlan(
    source_basename="test_schema_alignment.py",
    target_version_old="v9.3.0",
    target_version_new="v9.4.0",
    patches=[
        # 1. Module title in docstring opening
        Patch(
            label="module_title_v9_4_0",
            old='PHASE 9 — Schema Alignment & Route Contract Regression Suite (v9.3.0)\n\nWhy this revision (v9.3.0 vs v9.2.0)',
            new=(
                'PHASE 9 — Schema Alignment & Route Contract Regression Suite (v9.4.0)\n'
                '\n'
                'Why this revision (v9.4.0 vs v9.3.0)\n'
                '-------------------------------------\n'
                '- 🔑 ADD: v2.6.0 schema detection. v9.3.0 only knew V2 (80/83) and V3\n'
                '    (99/112/...) — but the project actually shipped a v2.6.0 stage\n'
                '    that adds 5 "Insights" group columns at the END of the canonical\n'
                '    schema (90/93). These are produced by core.insights_builder v1.0.0\n'
                '    and consumed by core.scoring v5.1.0+ for the conviction-floor cascade\n'
                '    in core.reco_normalize v7.1.0.\n'
                '\n'
                '- 🔑 ADD: `_EXPECTED_COUNTS_V2_6` matrix (90/90/90/90/90/93/7/9). Used\n'
                '    automatically when SCHEMA_VERSION >= 2.6.0 and < 3.0.0.\n'
                '\n'
                '- 🔑 ADD: `_INSIGHTS_GROUP_KEYS` set with the 5 canonical Insights field\n'
                '    keys: sector_relative_score, conviction_score, top_factors, top_risks,\n'
                '    position_size_hint.\n'
                '\n'
                '- 🔑 ADD: `_detect_schema_capabilities()` now returns `is_v2_6_plus` and\n'
                '    `has_insights_group` flags. The banner test prints both.\n'
                '\n'
                '- 🔑 ADD: `test_schema_has_insights_group_columns_when_v2_6_plus` —\n'
                '    feature-gated test that asserts the 5 Insights columns exist on\n'
                '    instrument pages and Top10 when SCHEMA_VERSION >= 2.6.0. Auto-skips\n'
                '    on older schemas with a clear message.\n'
                '\n'
                '- KEEP: every v9.3.0 invariant preserved unchanged. V2 and V3 detection,\n'
                '    aspirational test auto-skip, version-aware Insights_Analysis schema\n'
                '    check, sheet-rows endpoint contract assertions — all preserved.\n'
                '\n'
                'Why this revision (v9.3.0 vs v9.2.0)'
            ),
        ),
        # 2. Add _INSIGHTS_GROUP_KEYS constant after _TOP10_TECHNICAL_KEYS
        Patch(
            label="add_insights_group_keys_constant",
            old=(
                '# v9.3.0: aspirational keys — require scoring.py v3.0.0+\n'
                '_TOP10_TECHNICAL_KEYS: Set[str] = {\n'
                '    "technical_score",\n'
                '    "short_term_signal",\n'
                '    "rsi_signal",\n'
                '    "volume_ratio",\n'
                '}\n'
            ),
            new=(
                '# v9.3.0: aspirational keys — require scoring.py v3.0.0+\n'
                '_TOP10_TECHNICAL_KEYS: Set[str] = {\n'
                '    "technical_score",\n'
                '    "short_term_signal",\n'
                '    "rsi_signal",\n'
                '    "volume_ratio",\n'
                '}\n'
                '\n'
                '# v9.4.0: Insights group keys — produced by core.insights_builder v1.0.0\n'
                '# and present in schema_registry v2.6.0+. All 5 sit at the END of\n'
                '# instrument schemas (positions 86-90) and at positions 86-90 within\n'
                '# Top_10_Investments (which then has 3 Top10 extras after).\n'
                '_INSIGHTS_GROUP_KEYS: Set[str] = {\n'
                '    "sector_relative_score",\n'
                '    "conviction_score",\n'
                '    "top_factors",\n'
                '    "top_risks",\n'
                '    "position_size_hint",\n'
                '}\n'
            ),
        ),
        # 3. Add _EXPECTED_COUNTS_V2_6 dict after _EXPECTED_COUNTS_V2
        Patch(
            label="add_expected_counts_v2_6_matrix",
            old=(
                '# Baseline: schema_registry v2.2.0 (current deployed state)\n'
                '_EXPECTED_COUNTS_V2: Dict[str, int] = {\n'
                '    "Market_Leaders":     80,\n'
                '    "Global_Markets":     80,\n'
                '    "Commodities_FX":     80,\n'
                '    "Mutual_Funds":       80,\n'
                '    "My_Portfolio":       80,\n'
                '    "Top_10_Investments": 83,   # 80 + 3 top10 extras\n'
                '    "Insights_Analysis":  7,\n'
                '    "Data_Dictionary":    9,\n'
                '}\n'
                '\n'
                '# Aspirational: schema_registry v3.4.0 (future target)\n'
            ),
            new=(
                '# Baseline: schema_registry v2.2.0 (current deployed state)\n'
                '_EXPECTED_COUNTS_V2: Dict[str, int] = {\n'
                '    "Market_Leaders":     80,\n'
                '    "Global_Markets":     80,\n'
                '    "Commodities_FX":     80,\n'
                '    "Mutual_Funds":       80,\n'
                '    "My_Portfolio":       80,\n'
                '    "Top_10_Investments": 83,   # 80 + 3 top10 extras\n'
                '    "Insights_Analysis":  7,\n'
                '    "Data_Dictionary":    9,\n'
                '}\n'
                '\n'
                '# v9.4.0: schema_registry v2.6.0 (current target after Wave 3 rollout):\n'
                '# 80 base + upside_pct + 4 view tokens + 5 Insights cols = 90 instrument\n'
                '# Top10 = 90 + 3 Top10 extras = 93\n'
                '_EXPECTED_COUNTS_V2_6: Dict[str, int] = {\n'
                '    "Market_Leaders":     90,\n'
                '    "Global_Markets":     90,\n'
                '    "Commodities_FX":     90,\n'
                '    "Mutual_Funds":       90,\n'
                '    "My_Portfolio":       90,\n'
                '    "Top_10_Investments": 93,\n'
                '    "Insights_Analysis":  7,\n'
                '    "Data_Dictionary":    9,\n'
                '}\n'
                '\n'
                '# Aspirational: schema_registry v3.4.0 (future target)\n'
            ),
        ),
        # 4. Update _detect_schema_capabilities to add v2.6.0 detection
        Patch(
            label="detect_schema_capabilities_v2_6",
            old=(
                '    caps: Dict[str, Any] = {\n'
                '        "version_str": version_str,\n'
                '        "version_tuple": version_tuple,\n'
                '        "is_v3_plus": version_tuple >= (3, 0, 0),\n'
                '        "is_v3_4_plus": version_tuple >= (3, 4, 0),\n'
                '        # Feature detection (independent of version string)\n'
                '        "top10_has_trade_setup": _TOP10_TRADE_SETUP_KEYS.issubset(top10_keys),\n'
                '        "top10_has_technical": _TOP10_TECHNICAL_KEYS.issubset(top10_keys),\n'
                '        "insights_new_schema": {"signal", "priority", "as_of_riyadh"}.issubset(\n'
                '            insights_keys\n'
                '        ),\n'
                '        "insights_col_count": len(insights_keys),\n'
                '        "top10_col_count": len(top10_keys),\n'
                '    }\n'
            ),
            new=(
                '    caps: Dict[str, Any] = {\n'
                '        "version_str": version_str,\n'
                '        "version_tuple": version_tuple,\n'
                '        "is_v2_6_plus": version_tuple >= (2, 6, 0),  # v9.4.0\n'
                '        "is_v3_plus": version_tuple >= (3, 0, 0),\n'
                '        "is_v3_4_plus": version_tuple >= (3, 4, 0),\n'
                '        # Feature detection (independent of version string)\n'
                '        "top10_has_trade_setup": _TOP10_TRADE_SETUP_KEYS.issubset(top10_keys),\n'
                '        "top10_has_technical": _TOP10_TECHNICAL_KEYS.issubset(top10_keys),\n'
                '        # v9.4.0: Insights group present iff all 5 Insights keys are in\n'
                '        # any instrument page (we use Market_Leaders as the canonical probe).\n'
                '        "has_insights_group": _INSIGHTS_GROUP_KEYS.issubset(\n'
                '            set(_safe_sheet_keys(sr, "Market_Leaders"))\n'
                '        ),\n'
                '        "insights_new_schema": {"signal", "priority", "as_of_riyadh"}.issubset(\n'
                '            insights_keys\n'
                '        ),\n'
                '        "insights_col_count": len(insights_keys),\n'
                '        "top10_col_count": len(top10_keys),\n'
                '    }\n'
            ),
        ),
        # 5. Add _safe_sheet_keys helper (used by has_insights_group probe)
        Patch(
            label="add_safe_sheet_keys_helper",
            old=(
                '_CAPS_CACHE: Optional[Dict[str, Any]] = None\n'
                '\n'
                '\n'
                'def _detect_schema_capabilities(sr: Any) -> Dict[str, Any]:'
            ),
            new=(
                '_CAPS_CACHE: Optional[Dict[str, Any]] = None\n'
                '\n'
                '\n'
                'def _safe_sheet_keys(sr: Any, sheet: str) -> List[str]:\n'
                '    """v9.4.0: best-effort sheet keys retrieval. Returns [] on any error\n'
                '    so the capability probe never raises during caps detection."""\n'
                '    try:\n'
                '        return list(_schema_sheet_keys(sr, sheet))\n'
                '    except Exception:\n'
                '        return []\n'
                '\n'
                '\n'
                'def _detect_schema_capabilities(sr: Any) -> Dict[str, Any]:'
            ),
        ),
        # 6. Update _expected_counts_for_caps to add v2.6 branch
        Patch(
            label="expected_counts_for_caps_v2_6_branch",
            old=(
                'def _expected_counts_for_caps(caps: Dict[str, Any]) -> Dict[str, int]:\n'
                '    """Pick the expected column-count matrix based on detected capabilities."""\n'
                '    if caps.get("is_v3_4_plus"):\n'
                '        return dict(_EXPECTED_COUNTS_V3)\n'
                '    return dict(_EXPECTED_COUNTS_V2)\n'
            ),
            new=(
                'def _expected_counts_for_caps(caps: Dict[str, Any]) -> Dict[str, int]:\n'
                '    """Pick the expected column-count matrix based on detected capabilities.\n'
                '\n'
                '    v9.4.0: 3-way selection — V3 if >= 3.4.0, V2.6 if 2.6.0..3.0.0-,\n'
                '    V2 baseline otherwise.\n'
                '    """\n'
                '    if caps.get("is_v3_4_plus"):\n'
                '        return dict(_EXPECTED_COUNTS_V3)\n'
                '    if caps.get("is_v2_6_plus") and not caps.get("is_v3_plus"):\n'
                '        return dict(_EXPECTED_COUNTS_V2_6)\n'
                '    return dict(_EXPECTED_COUNTS_V2)\n'
            ),
        ),
        # 7. Banner test: print new flags
        Patch(
            label="banner_test_print_v2_6_flags",
            old=(
                '    print(f"  schema_registry.SCHEMA_VERSION     = {caps[\'version_str\']}")\n'
                '    print(f"  is_v3_plus                         = {caps[\'is_v3_plus\']}")\n'
                '    print(f"  is_v3_4_plus                       = {caps[\'is_v3_4_plus\']}")\n'
                '    print(f"  top10_has_trade_setup              = {caps[\'top10_has_trade_setup\']}")\n'
                '    print(f"  top10_has_technical                = {caps[\'top10_has_technical\']}")\n'
                '    print(f"  insights_new_schema                = {caps[\'insights_new_schema\']}")\n'
                '    print(f"  insights_col_count                 = {caps[\'insights_col_count\']}")\n'
                '    print(f"  top10_col_count                    = {caps[\'top10_col_count\']}")\n'
                '    which = "V3 (99/112/86/94/110/106/9/9)" if caps["is_v3_4_plus"] else "V2 (80/80/80/80/80/83/7/9)"\n'
            ),
            new=(
                '    print(f"  schema_registry.SCHEMA_VERSION     = {caps[\'version_str\']}")\n'
                '    print(f"  is_v2_6_plus                       = {caps[\'is_v2_6_plus\']}")\n'
                '    print(f"  is_v3_plus                         = {caps[\'is_v3_plus\']}")\n'
                '    print(f"  is_v3_4_plus                       = {caps[\'is_v3_4_plus\']}")\n'
                '    print(f"  has_insights_group                 = {caps[\'has_insights_group\']}")\n'
                '    print(f"  top10_has_trade_setup              = {caps[\'top10_has_trade_setup\']}")\n'
                '    print(f"  top10_has_technical                = {caps[\'top10_has_technical\']}")\n'
                '    print(f"  insights_new_schema                = {caps[\'insights_new_schema\']}")\n'
                '    print(f"  insights_col_count                 = {caps[\'insights_col_count\']}")\n'
                '    print(f"  top10_col_count                    = {caps[\'top10_col_count\']}")\n'
                '    if caps["is_v3_4_plus"]:\n'
                '        which = "V3 (99/112/86/94/110/106/9/9)"\n'
                '    elif caps["is_v2_6_plus"]:\n'
                '        which = "V2.6 (90/90/90/90/90/93/7/9)"\n'
                '    else:\n'
                '        which = "V2 (80/80/80/80/80/83/7/9)"\n'
            ),
        ),
        # 8. Add new test for Insights group columns (insert after the existing
        # `test_top10_schema_has_technical_signal_fields` test)
        Patch(
            label="add_insights_group_test",
            old=(
                '# =============================================================================\n'
                '# Tests — FastAPI endpoint contracts\n'
                '# =============================================================================\n'
            ),
            new=(
                'def test_schema_has_insights_group_columns_when_v2_6_plus():\n'
                '    """\n'
                '    v9.4.0: Feature-gated — requires schema_registry v2.6.0+ which adds\n'
                '    the 5 Insights group columns produced by core.insights_builder v1.0.0.\n'
                '    Auto-skips on older schemas with a clear message.\n'
                '\n'
                '    Verifies that all 5 Insights keys are present on every instrument page\n'
                '    AND on Top_10_Investments (which inherits the canonical schema plus 3\n'
                '    extras). The Insights group sits at positions 86-90 in canonical order;\n'
                '    test only asserts presence, not exact position.\n'
                '    """\n'
                '    sr = _load_schema_module()\n'
                '    caps = _detect_schema_capabilities(sr)\n'
                '\n'
                '    if not caps["is_v2_6_plus"]:\n'
                '        pytest.skip(\n'
                '            f"Insights group columns require schema_registry v2.6.0+ "\n'
                '            f"(detected SCHEMA_VERSION={caps[\'version_str\']}). "\n'
                '            f"Required keys: {sorted(_INSIGHTS_GROUP_KEYS)}. "\n'
                '            f"Run the Wave 3 rollout (apply_wave3_patches.py) to add."\n'
                '        )\n'
                '\n'
                '    # Sheets that should carry the full Insights group:\n'
                '    instrument_sheets = [\n'
                '        "Market_Leaders", "Global_Markets", "Commodities_FX",\n'
                '        "Mutual_Funds", "My_Portfolio", "Top_10_Investments",\n'
                '    ]\n'
                '\n'
                '    for sheet in instrument_sheets:\n'
                '        keys = set(_schema_sheet_keys(sr, sheet))\n'
                '        missing = sorted(_INSIGHTS_GROUP_KEYS - keys)\n'
                '        assert not missing, (\n'
                '            f"Sheet \'{sheet}\' missing Insights group keys: {missing}. "\n'
                '            f"All 5 Insights keys must be present in v2.6.0+. "\n'
                '            f"Detected SCHEMA_VERSION={caps[\'version_str\']}."\n'
                '        )\n'
                '\n'
                '\n'
                '# =============================================================================\n'
                '# Tests — FastAPI endpoint contracts\n'
                '# =============================================================================\n'
            ),
        ),
    ],
    expected_post_substrings=[
        '_INSIGHTS_GROUP_KEYS: Set[str]',
        '_EXPECTED_COUNTS_V2_6: Dict[str, int]',
        'is_v2_6_plus',
        'has_insights_group',
        'test_schema_has_insights_group_columns_when_v2_6_plus',
        'v9.4.0',
    ],
)


# =============================================================================
# test_scoring_engine_contract.py  v3.0.0 -> v3.1.0
# =============================================================================
TEST_SCORING_ENGINE_PLAN = FilePatchPlan(
    source_basename="test_scoring_engine_contract.py",
    target_version_old="v3.0.0",
    target_version_new="v3.1.0",
    patches=[
        # 1. Module title in docstring
        Patch(
            label="module_title_v3_1_0",
            old='SCORING ENGINE CONTRACT TESTS -- v3.0.0 (VIEW-AWARE / FAMILY-ALIGNED)',
            new='SCORING ENGINE CONTRACT TESTS -- v3.1.0 (V2.6.0-ALIGNED / INSIGHTS-AWARE)',
        ),
        # 2. Update version-alignment block at top of docstring
        Patch(
            label="docstring_aligned_versions",
            old=(
                'Aligned with the view-aware recommendation family:\n'
                '  - core.scoring                  v5.0.0  (authoritative implementation;\n'
                '                                           emits 4 view fields + 5-tier rec\n'
                '                                           via reco_normalize delegation)\n'
                '  - core.scoring_engine           v3.4.0  (compatibility bridge; re-exports\n'
                '                                           derive_*_view + is_view_aware +\n'
                '                                           get_degradation_report)\n'
                '  - core.reco_normalize           v7.0.0  (the 5-tier cascade — vetoes,\n'
                '                                           Recommendation.from_views(),\n'
                '                                           recommendation_from_views())\n'
                '  - core.investment_advisor       v5.1.1\n'
                '  - core.investment_advisor_engine v4.2.0\n'
                '  - core.sheets.schema_registry   v2.5.0  (4 View columns canonical)\n'
                '  - core.enriched_quote           v4.2.0  (fallback schema includes views)\n'
            ),
            new=(
                'Aligned with the view-aware + insights recommendation family:\n'
                '  - core.scoring                  v5.1.0  (authoritative implementation;\n'
                '                                           emits 4 view fields + 5-tier rec\n'
                '                                           + 5 Insights fields via\n'
                '                                           insights_builder delegation)\n'
                '  - core.scoring_engine           v3.4.0  (compatibility bridge; re-exports\n'
                '                                           derive_*_view + is_view_aware +\n'
                '                                           get_degradation_report)\n'
                '  - core.insights_builder         v1.0.0  (NEW; produces sector_relative_score,\n'
                '                                           conviction_score, top_factors,\n'
                '                                           top_risks, position_size_hint)\n'
                '  - core.reco_normalize           v7.1.0  (the 5-tier cascade with cascading\n'
                '                                           conviction floor: STRONG_BUY needs\n'
                '                                           conviction >= 60, BUY needs >= 45;\n'
                '                                           protective REDUCE/SELL never\n'
                '                                           downgraded by floor)\n'
                '  - core.investment_advisor       v5.2.0\n'
                '  - core.investment_advisor_engine v3.6.0\n'
                '  - core.sheets.schema_registry   v2.6.0  (4 View + 5 Insights canonical;\n'
                '                                           90/93 col widths)\n'
                '  - core.enriched_quote           v4.2.0  (fallback schema includes views)\n'
            ),
        ),
        # 3. Add v3.1.0 changelog block at the top of the changes section
        Patch(
            label="docstring_v3_1_0_changelog_block",
            old='================================================================================\nWhy v3.0.0 (changes from v2.3.0)',
            new=(
                '================================================================================\n'
                'Why v3.1.0 (changes from v3.0.0)\n'
                '================================================================================\n'
                '\n'
                'NEW CONTRACT TESTS for the Insights group + conviction-floor cascade\n'
                '-------------------------------------------------------------------\n'
                '- ADD: test_insights_builder_v1_importable — verifies\n'
                '    `core.insights_builder` is importable and exposes its 5 public\n'
                '    derivation functions (compute_sector_relative_score,\n'
                '    compute_conviction_score, derive_top_factors, derive_top_risks,\n'
                '    build_position_size_hint).\n'
                '- ADD: test_asset_scores_has_insights_fields — explicit contract for\n'
                '    the 5 Insights fields on AssetScores. v3.0.0\'s\n'
                '    CANONICAL_ASSETSCORES_REQUIRED list omitted them, so a regression\n'
                '    that dropped them would have passed CI silently. v3.1.0 adds them\n'
                '    to the canonical list and to a dedicated test.\n'
                '- ADD: test_asset_scores_insights_defaults_are_safe — defaults must\n'
                '    be None or empty string, NEVER a fabricated number/text. Phantom\n'
                '    rows must not get fake conviction scores or position-size hints.\n'
                '- ADD: test_compute_scores_emits_insights_fields_with_cohort — when a\n'
                '    rich payload is provided, the 5 Insights fields populate.\n'
                '    `top_factors` and `top_risks` (when present) must be strings;\n'
                '    `sector_relative_score` and `conviction_score` (when present) must\n'
                '    be in 0..100 range.\n'
                '- ADD: test_conviction_floor_strong_buy_requires_60 — view-aware\n'
                '    cascade with reco_normalize v7.1.0+: bullish-bullish-cheap-low-risk\n'
                '    inputs only get STRONG_BUY when conviction >= 60. Below the floor\n'
                '    the result downgrades to BUY (not STRONG_BUY).\n'
                '- ADD: test_conviction_floor_buy_requires_45 — same cascade: BUY\n'
                '    requires conviction >= 45. Below that, the result downgrades to\n'
                '    HOLD.\n'
                '- ADD: test_conviction_floor_does_not_downgrade_protective_actions —\n'
                '    REDUCE / SELL outputs are NEVER downgraded by the conviction\n'
                '    floor. (The floor is a one-way gate that only blocks unjustified\n'
                '    upgrades; it never produces a softer recommendation.)\n'
                '\n'
                'UPDATES to existing tests for v2.6.0 / 90·93 alignment\n'
                '-----------------------------------------------------\n'
                '- CHANGE: CANONICAL_ASSETSCORES_REQUIRED extended with the 5 Insights\n'
                '    fields. Existing test_asset_scores_dataclass_contract picks them\n'
                '    up automatically.\n'
                '\n'
                'PRESERVED from v3.0.0\n'
                '---------------------\n'
                '- All view-aware cascade tests (double-bearish veto,\n'
                '    expensive-valuation veto, recommendation_from_views direct call,\n'
                '    insufficient-data fallback) — unchanged.\n'
                '- Forecast fraction-vs-percent contract — unchanged.\n'
                '- ScoreWeights / ForecastParameters / ScoringEngine class contracts —\n'
                '    unchanged.\n'
                '- Determinism + soft performance budget — unchanged.\n'
                '- Env-driven overrides (TFB_TEST_SCORE_BUDGET_MS,\n'
                '    TFB_TEST_MIN_ROI_12M_FRACTION, TFB_TEST_MAX_ROI_12M_FRACTION) —\n'
                '    unchanged.\n'
                '\n'
                '================================================================================\n'
                'Why v3.0.0 (changes from v2.3.0)'
            ),
        ),
        # 4. Add insights_builder import attempt
        Patch(
            label="add_insights_builder_import",
            old=(
                '# v3.0.0: also try importing reco_normalize for direct view-aware tests.\n'
                '# This module is the source of truth for the 5-tier cascade and is what\n'
                '# scoring v5.0.0\'s compute_recommendation delegates to.\n'
                'try:\n'
                '    from core import reco_normalize as _reco_normalize  # type: ignore\n'
                'except Exception:\n'
                '    _reco_normalize = None  # type: ignore\n'
            ),
            new=(
                '# v3.0.0: also try importing reco_normalize for direct view-aware tests.\n'
                '# This module is the source of truth for the 5-tier cascade and is what\n'
                '# scoring v5.0.0\'s compute_recommendation delegates to.\n'
                'try:\n'
                '    from core import reco_normalize as _reco_normalize  # type: ignore\n'
                'except Exception:\n'
                '    _reco_normalize = None  # type: ignore\n'
                '\n'
                '# v3.1.0: also try importing insights_builder (Wave 1, NEW) — the source\n'
                '# of truth for the 5 Insights group fields populated by scoring v5.1.0+.\n'
                'try:\n'
                '    from core import insights_builder as _insights_builder  # type: ignore\n'
                'except Exception:\n'
                '    _insights_builder = None  # type: ignore\n'
            ),
        ),
        # 5. Add CANONICAL_INSIGHTS_FIELDS tuple after CANONICAL_VIEW_VOCAB
        Patch(
            label="add_canonical_insights_fields",
            old=(
                '# Per-field allowed token map.\n'
                'CANONICAL_VIEW_VOCAB: dict[str, frozenset[str]] = {\n'
                '    "fundamental_view": CANONICAL_FUNDAMENTAL_VIEWS,\n'
                '    "technical_view": CANONICAL_TECHNICAL_VIEWS,\n'
                '    "risk_view": CANONICAL_RISK_VIEWS,\n'
                '    "value_view": CANONICAL_VALUE_VIEWS,\n'
                '}\n'
                '\n'
                '# v3.0.0: AssetScores required-field list now includes the 4 view fields.\n'
                '# (v2.3.0 omitted them; a regression that dropped views would have passed CI.)\n'
            ),
            new=(
                '# Per-field allowed token map.\n'
                'CANONICAL_VIEW_VOCAB: dict[str, frozenset[str]] = {\n'
                '    "fundamental_view": CANONICAL_FUNDAMENTAL_VIEWS,\n'
                '    "technical_view": CANONICAL_TECHNICAL_VIEWS,\n'
                '    "risk_view": CANONICAL_RISK_VIEWS,\n'
                '    "value_view": CANONICAL_VALUE_VIEWS,\n'
                '}\n'
                '\n'
                '# v3.1.0: the 5 Insights fields added to AssetScores in scoring v5.1.0.\n'
                '# Produced by core.insights_builder v1.0.0 from a fully-scored row plus an\n'
                '# optional sector cohort. Conviction is fed back into reco_normalize v7.1.0+\'s\n'
                '# Recommendation.from_views() to enforce conviction-floor downgrades.\n'
                '# Schema_registry v2.6.0 has the 5 matching columns at positions 86-90.\n'
                'CANONICAL_INSIGHTS_FIELDS: tuple[str, ...] = (\n'
                '    "sector_relative_score",   # float, 0-100\n'
                '    "conviction_score",        # float, 0-100\n'
                '    "top_factors",             # str, pipe-separated\n'
                '    "top_risks",               # str, pipe-separated\n'
                '    "position_size_hint",      # str, text\n'
                ')\n'
                '\n'
                '# v3.0.0: AssetScores required-field list now includes the 4 view fields.\n'
                '# v3.1.0: extended with the 5 Insights fields too.\n'
                '# (Either set being dropped would now fail the dataclass contract test.)\n'
            ),
        ),
        # 6. Extend CANONICAL_ASSETSCORES_REQUIRED with Insights fields
        Patch(
            label="extend_canonical_assetscores_required",
            old=') + CANONICAL_VIEW_FIELDS\n',
            new=') + CANONICAL_VIEW_FIELDS + CANONICAL_INSIGHTS_FIELDS\n',
        ),
        # 7. Insert 6 new test methods before the "compute_scores()" section
        # (anchor: the `# -------------------- compute_scores() --------------------` heading)
        Patch(
            label="add_insights_and_conviction_tests",
            old='    # -------------------- compute_scores() --------------------\n',
            new=(
                '    # -------------------- Insights + conviction floor (v3.1.0) --------------------\n'
                '\n'
                '    def test_insights_builder_v1_importable(self) -> None:\n'
                '        """v3.1.0: core.insights_builder v1.0.0 (Wave 1, NEW) is importable.\n'
                '\n'
                '        Skips if not yet on the Python path — the test still runs in\n'
                '        environments where the module hasn\'t been deployed yet.\n'
                '        """\n'
                '        if _insights_builder is None:\n'
                '            self.skipTest("core.insights_builder not importable")\n'
                '        # 5 public derivation functions per Wave 1 design.\n'
                '        for fn_name in (\n'
                '            "compute_sector_relative_score",\n'
                '            "compute_conviction_score",\n'
                '            "derive_top_factors",\n'
                '            "derive_top_risks",\n'
                '            "build_position_size_hint",\n'
                '        ):\n'
                '            self.assertTrue(\n'
                '                hasattr(_insights_builder, fn_name),\n'
                '                msg=(\n'
                '                    f"core.insights_builder missing public function "\n'
                '                    f"{fn_name!r}. Required by Wave 1 contract."\n'
                '                ),\n'
                '            )\n'
                '            self.assertTrue(\n'
                '                callable(getattr(_insights_builder, fn_name)),\n'
                '                msg=f"core.insights_builder.{fn_name} not callable",\n'
                '            )\n'
                '\n'
                '    def test_asset_scores_has_insights_fields(self) -> None:\n'
                '        """v3.1.0: the 5 Insights fields are part of the AssetScores contract."""\n'
                '        scores = se.AssetScores()\n'
                '        for field_name in CANONICAL_INSIGHTS_FIELDS:\n'
                '            self.assertTrue(\n'
                '                hasattr(scores, field_name),\n'
                '                msg=(\n'
                '                    f"AssetScores missing Insights field: {field_name}. "\n'
                '                    "scoring v5.1.0 must populate this for the rec engine."\n'
                '                ),\n'
                '            )\n'
                '\n'
                '    def test_asset_scores_insights_defaults_are_safe(self) -> None:\n'
                '        """\n'
                '        v3.1.0: Insights fields default to None or empty string — never a\n'
                '        fabricated number or text. Phantom rows must not get fake\n'
                '        conviction scores or position-size hints.\n'
                '        """\n'
                '        scores = se.AssetScores()\n'
                '        for field_name in CANONICAL_INSIGHTS_FIELDS:\n'
                '            val = getattr(scores, field_name)\n'
                '            self.assertTrue(\n'
                '                val is None or val == "",\n'
                '                msg=(\n'
                '                    f"AssetScores.{field_name} default is fabricated: "\n'
                '                    f"{val!r}. Should be None or empty string."\n'
                '                ),\n'
                '            )\n'
                '\n'
                '    def test_compute_scores_emits_insights_fields_with_cohort(self) -> None:\n'
                '        """\n'
                '        v3.1.0: rich payload should produce all 5 Insights fields when\n'
                '        the engine has enough signal. Type checks:\n'
                '          - sector_relative_score, conviction_score: numeric in 0..100 OR None\n'
                '          - top_factors, top_risks, position_size_hint: str OR None/""\n'
                '        Field absence is acceptable for compute paths that legitimately\n'
                '        couldn\'t derive the value.\n'
                '        """\n'
                '        payload = {\n'
                '            "symbol": "TEST.INSIGHTS",\n'
                '            "price": 100.0,\n'
                '            "previous_close": 99.0,\n'
                '            "fair_value": 110.0,\n'
                '            "pe_ttm": 18.0,\n'
                '            "roe": 0.18,\n'
                '            "revenue_growth_yoy": 0.12,\n'
                '            "gross_margin": 0.40,\n'
                '            "operating_margin": 0.25,\n'
                '            "rsi_14": 55.0,\n'
                '            "volatility_30d": 0.20,\n'
                '            "beta": 1.0,\n'
                '            "market_cap": 5_000_000_000,\n'
                '            "volume": 5_000_000,\n'
                '            "data_quality": "EXCELLENT",\n'
                '            "forecast_confidence": 0.85,\n'
                '            "sector": "Technology",\n'
                '        }\n'
                '        d = _as_dict(se.compute_scores(payload))\n'
                '\n'
                '        for field_name in ("sector_relative_score", "conviction_score"):\n'
                '            if field_name not in d:\n'
                '                continue\n'
                '            _check_score_range_or_none(self, field_name, d[field_name])\n'
                '\n'
                '        for field_name in ("top_factors", "top_risks", "position_size_hint"):\n'
                '            if field_name not in d:\n'
                '                continue\n'
                '            val = d[field_name]\n'
                '            if val is None or val == "":\n'
                '                continue\n'
                '            self.assertIsInstance(\n'
                '                val, str,\n'
                '                msg=f"{field_name} should be str or None, got {type(val).__name__}",\n'
                '            )\n'
                '\n'
                '    def test_conviction_floor_strong_buy_requires_60(self) -> None:\n'
                '        """\n'
                '        v3.1.0: reco_normalize v7.1.0+ enforces a cascading conviction floor.\n'
                '        STRONG_BUY requires conviction >= 60. Below that, the result\n'
                '        downgrades to BUY.\n'
                '\n'
                '        Inputs are otherwise STRONG_BUY-shaped: bullish/bullish/low-risk/cheap\n'
                '        with overall_score=82. The conviction floor is the only differentiator.\n'
                '        """\n'
                '        if _reco_normalize is None:\n'
                '            self.skipTest("core.reco_normalize not importable")\n'
                '        fn = getattr(_reco_normalize, "recommendation_from_views", None)\n'
                '        if not callable(fn):\n'
                '            self.skipTest("recommendation_from_views unavailable")\n'
                '\n'
                '        # Test signature tolerance: v7.1.0+ accepts conviction= and/or\n'
                '        # sector_relative= kwargs. Older v7.0.0 callers ignore them.\n'
                '        try:\n'
                '            reco_strong, _ = fn(\n'
                '                fundamental="BULLISH", technical="BULLISH",\n'
                '                risk="LOW", value="CHEAP", score=82.0,\n'
                '                conviction=65.0,\n'
                '            )\n'
                '            reco_weak, _ = fn(\n'
                '                fundamental="BULLISH", technical="BULLISH",\n'
                '                risk="LOW", value="CHEAP", score=82.0,\n'
                '                conviction=55.0,\n'
                '            )\n'
                '        except TypeError:\n'
                '            self.skipTest(\n'
                '                "recommendation_from_views does not accept conviction kwarg "\n'
                '                "(reco_normalize v7.0.0 detected; v7.1.0+ required)"\n'
                '            )\n'
                '\n'
                '        self.assertEqual(\n'
                '            reco_strong, "STRONG_BUY",\n'
                '            msg=(\n'
                '                f"conviction=65 should yield STRONG_BUY, got {reco_strong!r}. "\n'
                '                "Conviction floor cascade may be broken."\n'
                '            ),\n'
                '        )\n'
                '        self.assertNotEqual(\n'
                '            reco_weak, "STRONG_BUY",\n'
                '            msg=(\n'
                '                f"conviction=55 should NOT yield STRONG_BUY (floor=60). "\n'
                '                f"Got {reco_weak!r}."\n'
                '            ),\n'
                '        )\n'
                '        # The downgrade should land on BUY (not HOLD or below) since the\n'
                '        # other inputs all support a bullish stance.\n'
                '        self.assertEqual(\n'
                '            reco_weak, "BUY",\n'
                '            msg=(\n'
                '                f"With score=82 + bullish inputs, conviction=55 should "\n'
                '                f"downgrade to BUY (not HOLD or worse). Got {reco_weak!r}."\n'
                '            ),\n'
                '        )\n'
                '\n'
                '    def test_conviction_floor_buy_requires_45(self) -> None:\n'
                '        """\n'
                '        v3.1.0: BUY requires conviction >= 45. Below that, the result\n'
                '        downgrades to HOLD.\n'
                '\n'
                '        Inputs are BUY-shaped: bullish/neutral/moderate/fair with score=68.\n'
                '        """\n'
                '        if _reco_normalize is None:\n'
                '            self.skipTest("core.reco_normalize not importable")\n'
                '        fn = getattr(_reco_normalize, "recommendation_from_views", None)\n'
                '        if not callable(fn):\n'
                '            self.skipTest("recommendation_from_views unavailable")\n'
                '\n'
                '        try:\n'
                '            reco_solid, _ = fn(\n'
                '                fundamental="BULLISH", technical="NEUTRAL",\n'
                '                risk="MODERATE", value="FAIR", score=68.0,\n'
                '                conviction=50.0,\n'
                '            )\n'
                '            reco_weak, _ = fn(\n'
                '                fundamental="BULLISH", technical="NEUTRAL",\n'
                '                risk="MODERATE", value="FAIR", score=68.0,\n'
                '                conviction=40.0,\n'
                '            )\n'
                '        except TypeError:\n'
                '            self.skipTest(\n'
                '                "recommendation_from_views does not accept conviction kwarg "\n'
                '                "(reco_normalize v7.0.0 detected; v7.1.0+ required)"\n'
                '            )\n'
                '\n'
                '        self.assertEqual(\n'
                '            reco_solid, "BUY",\n'
                '            msg=(\n'
                '                f"conviction=50 + BUY-shaped inputs should yield BUY. "\n'
                '                f"Got {reco_solid!r}."\n'
                '            ),\n'
                '        )\n'
                '        self.assertNotEqual(\n'
                '            reco_weak, "BUY",\n'
                '            msg=(\n'
                '                f"conviction=40 should NOT yield BUY (floor=45). "\n'
                '                f"Got {reco_weak!r}."\n'
                '            ),\n'
                '        )\n'
                '        self.assertEqual(\n'
                '            reco_weak, "HOLD",\n'
                '            msg=(\n'
                '                f"BUY-shaped inputs with conviction=40 should downgrade "\n'
                '                f"to HOLD. Got {reco_weak!r}."\n'
                '            ),\n'
                '        )\n'
                '\n'
                '    def test_conviction_floor_does_not_downgrade_protective_actions(self) -> None:\n'
                '        """\n'
                '        v3.1.0: REDUCE / SELL outputs are NEVER downgraded by the\n'
                '        conviction floor. The floor is a one-way gate on upgrades only —\n'
                '        a low-conviction REDUCE must remain REDUCE, not soften to HOLD.\n'
                '        Symmetric for SELL on the double-bearish path.\n'
                '\n'
                '        This guards against a regression where the conviction floor logic\n'
                '        is mistakenly applied bidirectionally and weakens a protective\n'
                '        action because the conviction was low.\n'
                '        """\n'
                '        if _reco_normalize is None:\n'
                '            self.skipTest("core.reco_normalize not importable")\n'
                '        fn = getattr(_reco_normalize, "recommendation_from_views", None)\n'
                '        if not callable(fn):\n'
                '            self.skipTest("recommendation_from_views unavailable")\n'
                '\n'
                '        try:\n'
                '            # Double-bearish veto -> SELL. Even with very low conviction,\n'
                '            # protective action must stand.\n'
                '            reco_sell, _ = fn(\n'
                '                fundamental="BEARISH", technical="BEARISH",\n'
                '                risk="HIGH", value="EXPENSIVE", score=30.0,\n'
                '                conviction=20.0,\n'
                '            )\n'
                '            # Bullish-but-EXPENSIVE -> typically REDUCE. Low conviction\n'
                '            # must not soften to HOLD.\n'
                '            reco_reduce, _ = fn(\n'
                '                fundamental="BULLISH", technical="BULLISH",\n'
                '                risk="MODERATE", value="EXPENSIVE", score=70.0,\n'
                '                conviction=25.0,\n'
                '            )\n'
                '        except TypeError:\n'
                '            self.skipTest(\n'
                '                "recommendation_from_views does not accept conviction kwarg "\n'
                '                "(reco_normalize v7.0.0 detected; v7.1.0+ required)"\n'
                '            )\n'
                '\n'
                '        self.assertEqual(\n'
                '            reco_sell, "SELL",\n'
                '            msg=(\n'
                '                f"Double-bearish + low conviction should remain SELL "\n'
                '                f"(protective action). Got {reco_sell!r}."\n'
                '            ),\n'
                '        )\n'
                '        self.assertIn(\n'
                '            reco_reduce, {"REDUCE", "SELL"},\n'
                '            msg=(\n'
                '                f"Bullish + EXPENSIVE + low conviction should be REDUCE "\n'
                '                f"or SELL (not HOLD). Got {reco_reduce!r}."\n'
                '            ),\n'
                '        )\n'
                '\n'
                '    # -------------------- compute_scores() --------------------\n'
            ),
        ),
    ],
    expected_post_substrings=[
        'CANONICAL_INSIGHTS_FIELDS: tuple[str, ...]',
        '+ CANONICAL_INSIGHTS_FIELDS',
        'def test_insights_builder_v1_importable',
        'def test_asset_scores_has_insights_fields',
        'def test_conviction_floor_strong_buy_requires_60',
        'def test_conviction_floor_buy_requires_45',
        'def test_conviction_floor_does_not_downgrade_protective_actions',
        'V2.6.0-ALIGNED / INSIGHTS-AWARE',
        'core.insights_builder         v1.0.0',
    ],
)


ALL_PLANS: Tuple[FilePatchPlan, ...] = (
    TEST_ENDPOINTS_PLAN,
    TEST_SCHEMA_ALIGNMENT_PLAN,
    TEST_SCORING_ENGINE_PLAN,
)


# =============================================================================
# Patcher engine (same framework as Wave 3 engine patcher)
# =============================================================================
class PatchError(Exception):
    pass


def _find_source_file(src_dir: Path, basename: str) -> Optional[Path]:
    direct = src_dir / basename
    if direct.is_file():
        return direct
    stem = basename.rsplit(".py", 1)[0]
    candidates: List[Path] = []
    for entry in src_dir.iterdir():
        if not entry.is_file() or entry.suffix != ".py":
            continue
        ename = entry.stem
        if ename == stem:
            candidates.append(entry)
            continue
        if ename.startswith(stem + "_") or ename.startswith(stem + " "):
            candidates.append(entry)
            continue
        if ename.startswith(stem + "__") and ename.endswith("_"):
            candidates.append(entry)
            continue
        if ename.startswith(stem + " (") and ename.endswith(")"):
            candidates.append(entry)
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _apply_one_file(plan: FilePatchPlan, source_path: Path, output_path: Path) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "source": str(source_path),
        "target": str(output_path),
        "from_version": plan.target_version_old,
        "to_version": plan.target_version_new,
        "patches_applied": [],
        "patches_failed": [],
        "ast_parse_ok": False,
        "post_substring_check": {},
        "wrote_output": False,
    }

    src = source_path.read_text(encoding="utf-8")

    if plan.target_version_old not in src:
        result["patches_failed"].append({
            "label": "expected_version_marker",
            "detail": (
                f"Expected to find marker {plan.target_version_old!r} "
                f"in source file but did not — your file may be at a different "
                f"version. Refusing to patch."
            ),
        })

    patched = src
    for p in plan.patches:
        occ = patched.count(p.old)
        if occ != p.occurrences_required:
            result["patches_failed"].append({
                "label": p.label,
                "detail": (
                    f"Expected {p.occurrences_required} occurrence(s), found {occ}. "
                    f"First 80 chars of pattern: {p.old[:80]!r}"
                ),
            })
            continue
        patched = patched.replace(p.old, p.new, 1)
        result["patches_applied"].append(p.label)

    if result["patches_failed"]:
        return result

    try:
        ast.parse(patched)
        result["ast_parse_ok"] = True
    except SyntaxError as e:
        raise PatchError(
            f"AST parse failed AFTER applying patches to {source_path.name}: {e}\n"
            f"Refusing to write — your original is untouched."
        ) from e

    for substring in plan.expected_post_substrings:
        present = substring in patched
        result["post_substring_check"][substring[:60]] = {
            "expected": True,
            "actual": present,
            "ok": present,
        }
        if not present:
            raise PatchError(
                f"Post-patch substring check FAILED in {source_path.name}: "
                f"expected substring {substring[:60]!r} not found.\n"
                f"Refusing to write — your original is untouched."
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(patched, encoding="utf-8")
    result["wrote_output"] = True
    return result


def _print_result(result: Dict[str, Any]) -> None:
    src = Path(result["source"])
    print(f"\n{'='*78}")
    print(f"  {src.name}")
    print(f"  {result['from_version']} -> {result['to_version']}")
    print(f"{'='*78}")

    applied = result.get("patches_applied", [])
    failed = result.get("patches_failed", [])
    if applied:
        print(f"  APPLIED PATCHES ({len(applied)}):")
        for label in applied:
            print(f"    [OK] {label}")
    if failed:
        print(f"  FAILED PATCHES ({len(failed)}):")
        for entry in failed:
            print(f"    [SKIP] {entry['label']}")
            print(f"           {entry['detail']}")

    if result.get("post_substring_check"):
        print("  POST-PATCH SUBSTRING CHECKS:")
        for name, info in result["post_substring_check"].items():
            mark = "[OK]" if info["ok"] else "[FAIL]"
            print(f"    {mark} {name!r}")

    if result.get("wrote_output"):
        print(f"  WROTE: {result['target']}")
    else:
        print("  NOT WRITTEN (patches failed; original untouched)")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply Wave 3 TEST patches (v2.6.0 alignment).",
    )
    parser.add_argument("--src-dir", type=Path, default=Path.cwd())
    parser.add_argument("--out-dir", type=Path, default=Path.cwd() / "patched")
    parser.add_argument("--only", type=str, default="")
    args = parser.parse_args()

    print(f"\nTFB Wave 3 TEST Patcher v{PATCHER_VERSION}")
    print(f"Source dir: {args.src_dir.resolve()}")
    print(f"Output dir: {args.out_dir.resolve()}")

    only_set: Optional[set] = None
    if args.only.strip():
        only_set = {p.strip() for p in args.only.split(",") if p.strip()}
        print(f"Filter: only patching {sorted(only_set)}")

    if not args.src_dir.is_dir():
        print(f"\nERROR: source dir does not exist: {args.src_dir}", file=sys.stderr)
        return 2

    overall_ok = True
    summaries: List[Dict[str, Any]] = []
    for plan in ALL_PLANS:
        if only_set and plan.source_basename not in only_set:
            continue
        source_path = _find_source_file(args.src_dir, plan.source_basename)
        if source_path is None:
            print(f"\n[WARN] Skipping {plan.source_basename}: not found in {args.src_dir}")
            overall_ok = False
            continue
        output_path = args.out_dir / plan.source_basename
        try:
            result = _apply_one_file(plan, source_path, output_path)
        except PatchError as e:
            print(f"\n[ABORT] {e}", file=sys.stderr)
            overall_ok = False
            continue
        summaries.append(result)
        _print_result(result)
        if result["patches_failed"] or not result["wrote_output"]:
            overall_ok = False

    print(f"\n{'='*78}")
    print("SUMMARY")
    print(f"{'='*78}")
    for s in summaries:
        src_name = Path(s["source"]).name
        status = "OK " if s["wrote_output"] and not s["patches_failed"] else "FAIL"
        applied = len(s.get("patches_applied", []))
        total = applied + len(s.get("patches_failed", []))
        print(f"  [{status}] {src_name:42s} {applied}/{total} patches applied")

    if overall_ok:
        print("\nAll files patched successfully. Review with `diff` and copy in.")
        return 0
    print("\nSome files were not patched. Review messages above; originals untouched.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
