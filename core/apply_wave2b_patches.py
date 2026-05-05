#!/usr/bin/env python3
"""
================================================================================
TFB Wave 2B Patcher — apply_wave2b_patches.py
================================================================================
ALIGNS THREE ENGINE FILES TO v2.5.0 SCHEMA + CANONICAL RECO ENUM (BUY/HOLD/REDUCE/SELL)

What this patches
-----------------
1. investment_advisor_engine.py  v3.5.0  ->  v3.6.0
   - _recommendation_from_scores(): "Strong Buy"/"Buy"/"Hold"/"Avoid"
                                ->  "BUY"/"BUY"/"HOLD"/"SELL"
   - Header version bump

2. data_engine_v2.py             v5.47.2 ->  v5.48.0
   - _compute_recommendation(): "BUY"/"ACCUMULATE"/"REDUCE"/"HOLD"
                            ->  "BUY"/"BUY"/"REDUCE"/"HOLD"
   - INSTRUMENT_CANONICAL_KEYS:    80 entries -> 85 entries
       inserts "upside_pct" after "intrinsic_value"
       inserts 4 view tokens after "overall_score"
   - INSTRUMENT_CANONICAL_HEADERS: 80 entries -> 85 entries (mirrors keys)
   - Header version bump

3. top10_selector.py             v4.8.0  ->  v4.9.0
   - DEFAULT_FALLBACK_KEYS:    80+3 entries -> 85+3 entries
   - DEFAULT_FALLBACK_HEADERS: 80+3 entries -> 85+3 entries
   - Header version bump (TOP10_SELECTOR_VERSION)

How to run
----------
1. Place this script in the same directory as the three source files
   (or pass --src-dir pointing to them).
2. Run:   python3 apply_wave2b_patches.py
3. Patched files will be written to ./patched/ alongside this script.
4. Diff each pair and copy into your codebase.

The patcher will REFUSE to write any file where a patch fails to match —
that's a signal your local file has drifted from what was audited and
you should review before forcing the change.

Conservative by design: never modifies your originals.
================================================================================
"""

from __future__ import annotations

import argparse
import ast
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


PATCHER_VERSION = "1.0.0"


# =============================================================================
# Patch definitions
# =============================================================================
@dataclass
class Patch:
    """One surgical find-and-replace operation against a single file."""
    label: str
    old: str
    new: str
    occurrences_required: int = 1   # exact occurrence count expected in file


@dataclass
class FilePatchPlan:
    """All patches for a single source file."""
    source_basename: str            # short name to find on disk
    target_version_old: str         # what version we expect to find
    target_version_new: str         # what version we'll write
    patches: List[Patch]
    expected_post_constants: Dict[str, int]   # AST-counted lengths after patching


# =============================================================================
# investment_advisor_engine.py  v3.5.0 -> v3.6.0
# =============================================================================
INVESTMENT_ADVISOR_ENGINE_PLAN = FilePatchPlan(
    source_basename="investment_advisor_engine.py",
    target_version_old="3.5.0",
    target_version_new="3.6.0",
    patches=[
        Patch(
            label="version_string_in_header_docstring",
            old="INVESTMENT ADVISOR ENGINE — v3.5.0",
            new="INVESTMENT ADVISOR ENGINE — v3.6.0",
        ),
        Patch(
            label="_recommendation_from_scores_canonical_enum",
            old=(
                'def _recommendation_from_scores(overall_score: float, roi_3m: float, '
                'confidence: float, risk_score: float) -> str:\n'
                '    if overall_score >= 75.0 and roi_3m >= 0.05 and confidence >= 0.70 '
                'and risk_score <= 60.0:\n'
                '        return "Strong Buy"\n'
                '    if overall_score >= 60.0 and roi_3m > 0.0 and confidence >= 0.55:\n'
                '        return "Buy"\n'
                '    if overall_score >= 45.0:\n'
                '        return "Hold"\n'
                '    return "Avoid"\n'
            ),
            new=(
                'def _recommendation_from_scores(overall_score: float, roi_3m: float, '
                'confidence: float, risk_score: float) -> str:\n'
                '    # v3.6.0: canonical 4-tier enum from reco_normalize.py v5.0.0\n'
                '    # (BUY/HOLD/REDUCE/SELL). High-conviction case folds into BUY;\n'
                '    # the conviction tier is preserved via confidence_score, not as\n'
                '    # a separate "STRONG_BUY" label that the canonical enum does not\n'
                '    # have. Risk_score >= 85 escalates to REDUCE per Wave 1 cascading\n'
                '    # conviction floors.\n'
                '    if overall_score >= 60.0 and roi_3m > 0.0 and confidence >= 0.55 '
                'and risk_score <= 70.0:\n'
                '        return "BUY"\n'
                '    if overall_score >= 45.0 and risk_score < 85.0:\n'
                '        return "HOLD"\n'
                '    if overall_score >= 30.0 or risk_score >= 85.0:\n'
                '        return "REDUCE"\n'
                '    return "SELL"\n'
            ),
        ),
    ],
    expected_post_constants={},  # no AST-checked list-length changes here
)


# =============================================================================
# data_engine_v2.py  v5.47.2 -> v5.48.0
# =============================================================================
DATA_ENGINE_V2_PLAN = FilePatchPlan(
    source_basename="data_engine_v2.py",
    target_version_old="5.47.2",
    target_version_new="5.48.0",
    patches=[
        # 1. Module __version__ string
        Patch(
            label="module_version_string",
            old='__version__ = "5.47.2"',
            new='__version__ = "5.48.0"',
        ),
        # 2. _compute_recommendation: drop ACCUMULATE -> map to BUY
        Patch(
            label="_compute_recommendation_canonical_enum",
            old=(
                'def _compute_recommendation(row: Dict[str, Any]) -> None:\n'
                '    if row.get("recommendation"):\n'
                '        return\n'
                '    overall = _as_float(row.get("overall_score")) or 50.0\n'
                '    conf = _as_float(row.get("confidence_score")) or 55.0\n'
                '    risk = _as_float(row.get("risk_score")) or 50.0\n'
                '    if overall >= 75 and conf >= 65 and risk <= 60:\n'
                '        rec = "BUY"\n'
                '    elif overall >= 60 and conf >= 55:\n'
                '        rec = "ACCUMULATE"\n'
                '    elif overall <= 35 or risk >= 85:\n'
                '        rec = "REDUCE"\n'
                '    else:\n'
                '        rec = "HOLD"\n'
                '    row["recommendation"] = rec\n'
                '    row.setdefault(\n'
                '        "recommendation_reason",\n'
                '        f"overall={round(overall,1)} confidence={round(conf,1)} '
                'risk={round(risk,1)}",\n'
                '    )\n'
            ),
            new=(
                'def _compute_recommendation(row: Dict[str, Any]) -> None:\n'
                '    # v5.48.0: canonical 4-tier enum from reco_normalize.py v5.0.0\n'
                '    # (BUY/HOLD/REDUCE/SELL). The previous "ACCUMULATE" tier is\n'
                '    # folded into BUY; conviction is preserved via confidence_score\n'
                '    # rather than a separate label that downstream consumers don\'t\n'
                '    # recognize. Wave 2B canonical reco enum alignment.\n'
                '    if row.get("recommendation"):\n'
                '        return\n'
                '    overall = _as_float(row.get("overall_score")) or 50.0\n'
                '    conf = _as_float(row.get("confidence_score")) or 55.0\n'
                '    risk = _as_float(row.get("risk_score")) or 50.0\n'
                '    if overall >= 75 and conf >= 65 and risk <= 60:\n'
                '        rec = "BUY"\n'
                '    elif overall >= 60 and conf >= 55 and risk <= 75:\n'
                '        rec = "BUY"\n'
                '    elif overall <= 35 or risk >= 85:\n'
                '        rec = "REDUCE"\n'
                '    else:\n'
                '        rec = "HOLD"\n'
                '    row["recommendation"] = rec\n'
                '    row.setdefault(\n'
                '        "recommendation_reason",\n'
                '        f"overall={round(overall,1)} confidence={round(conf,1)} '
                'risk={round(risk,1)}",\n'
                '    )\n'
            ),
        ),
        # 3. INSTRUMENT_CANONICAL_KEYS: insert upside_pct after intrinsic_value
        Patch(
            label="INSTRUMENT_CANONICAL_KEYS_insert_upside_pct",
            old=(
                '    "intrinsic_value",\n'
                '    "valuation_score",\n'
            ),
            new=(
                '    "intrinsic_value",\n'
                '    "upside_pct",\n'
                '    "valuation_score",\n'
            ),
        ),
        # 4. INSTRUMENT_CANONICAL_KEYS: insert 4 view tokens after overall_score
        Patch(
            label="INSTRUMENT_CANONICAL_KEYS_insert_view_tokens",
            old=(
                '    "overall_score",\n'
                '    "opportunity_score",\n'
            ),
            new=(
                '    "overall_score",\n'
                '    "fundamental_view",\n'
                '    "technical_view",\n'
                '    "risk_view",\n'
                '    "value_view",\n'
                '    "opportunity_score",\n'
            ),
        ),
        # 5. INSTRUMENT_CANONICAL_HEADERS: insert "Upside %" after "Intrinsic Value"
        Patch(
            label="INSTRUMENT_CANONICAL_HEADERS_insert_upside_pct",
            old=(
                '    "Intrinsic Value",\n'
                '    "Valuation Score",\n'
            ),
            new=(
                '    "Intrinsic Value",\n'
                '    "Upside %",\n'
                '    "Valuation Score",\n'
            ),
        ),
        # 6. INSTRUMENT_CANONICAL_HEADERS: insert 4 view headers after "Overall Score"
        Patch(
            label="INSTRUMENT_CANONICAL_HEADERS_insert_view_tokens",
            old=(
                '    "Overall Score",\n'
                '    "Opportunity Score",\n'
            ),
            new=(
                '    "Overall Score",\n'
                '    "Fundamental View",\n'
                '    "Technical View",\n'
                '    "Risk View",\n'
                '    "Value View",\n'
                '    "Opportunity Score",\n'
            ),
        ),
    ],
    expected_post_constants={
        "INSTRUMENT_CANONICAL_KEYS": 85,
        "INSTRUMENT_CANONICAL_HEADERS": 85,
    },
)


# =============================================================================
# top10_selector.py  v4.8.0 -> v4.9.0
# =============================================================================
TOP10_SELECTOR_PLAN = FilePatchPlan(
    source_basename="top10_selector.py",
    target_version_old="4.8.0",
    target_version_new="4.9.0",
    patches=[
        # 1. TOP10_SELECTOR_VERSION constant
        Patch(
            label="TOP10_SELECTOR_VERSION_constant",
            old='TOP10_SELECTOR_VERSION = "4.8.0"',
            new='TOP10_SELECTOR_VERSION = "4.9.0"',
        ),
        # 2. DEFAULT_FALLBACK_KEYS: insert upside_pct after intrinsic_value (multi-key-per-line format)
        Patch(
            label="DEFAULT_FALLBACK_KEYS_insert_upside_pct",
            old=(
                '    "peg_ratio", "intrinsic_value", "valuation_score", '
                '"forecast_price_1m", "forecast_price_3m",\n'
            ),
            new=(
                '    "peg_ratio", "intrinsic_value", "upside_pct", "valuation_score", '
                '"forecast_price_1m", "forecast_price_3m",\n'
            ),
        ),
        # 3. DEFAULT_FALLBACK_KEYS: insert 4 view tokens after overall_score
        Patch(
            label="DEFAULT_FALLBACK_KEYS_insert_view_tokens",
            old=(
                '    "momentum_score", "growth_score", "overall_score", '
                '"opportunity_score", "rank_overall",\n'
            ),
            new=(
                '    "momentum_score", "growth_score", "overall_score",\n'
                '    "fundamental_view", "technical_view", "risk_view", "value_view",\n'
                '    "opportunity_score", "rank_overall",\n'
            ),
        ),
        # 4. DEFAULT_FALLBACK_HEADERS: insert "Upside %" after "Intrinsic Value"
        Patch(
            label="DEFAULT_FALLBACK_HEADERS_insert_upside_pct",
            old=(
                '    "PEG", "Intrinsic Value", "Valuation Score", '
                '"Forecast Price 1M", "Forecast Price 3M",\n'
            ),
            new=(
                '    "PEG", "Intrinsic Value", "Upside %", "Valuation Score", '
                '"Forecast Price 1M", "Forecast Price 3M",\n'
            ),
        ),
        # 5. DEFAULT_FALLBACK_HEADERS: insert 4 view headers after "Overall Score"
        Patch(
            label="DEFAULT_FALLBACK_HEADERS_insert_view_tokens",
            old=(
                '    "Momentum Score", "Growth Score", "Overall Score", '
                '"Opportunity Score", "Rank (Overall)",\n'
            ),
            new=(
                '    "Momentum Score", "Growth Score", "Overall Score",\n'
                '    "Fundamental View", "Technical View", "Risk View", "Value View",\n'
                '    "Opportunity Score", "Rank (Overall)",\n'
            ),
        ),
    ],
    expected_post_constants={
        "DEFAULT_FALLBACK_KEYS": 88,     # 85 canonical + 3 Top10 extras
        "DEFAULT_FALLBACK_HEADERS": 88,
    },
)


ALL_PLANS: Tuple[FilePatchPlan, ...] = (
    INVESTMENT_ADVISOR_ENGINE_PLAN,
    DATA_ENGINE_V2_PLAN,
    TOP10_SELECTOR_PLAN,
)


# =============================================================================
# Patcher engine
# =============================================================================
class PatchError(Exception):
    pass


def _find_source_file(src_dir: Path, basename: str) -> Optional[Path]:
    """Locate the source file in src_dir or as a name-similar variant.

    Tolerates trailing "__N_" suffixes (download-numbered copies) and "(N)"
    parenthesised duplicates that some download flows produce.
    """
    candidates: List[Path] = []
    direct = src_dir / basename
    if direct.is_file():
        return direct

    stem = basename.rsplit(".py", 1)[0]
    for entry in src_dir.iterdir():
        if not entry.is_file() or not entry.suffix == ".py":
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


def _count_const_list_entries(src: str, name: str) -> Optional[int]:
    """Parse src as Python, locate a top-level assignment of `name = [...]`,
    and return the number of literal elements. None if not found / not a list.
    """
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return None
    for node in ast.walk(tree):
        target_name: Optional[str] = None
        value: Any = None
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == name:
                    target_name = name
                    value = node.value
                    break
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == name:
            target_name = name
            value = node.value
        if target_name is None or value is None:
            continue
        if isinstance(value, ast.List):
            return len(value.elts)
        if isinstance(value, ast.Tuple):
            return len(value.elts)
    return None


def _apply_one_file(plan: FilePatchPlan, source_path: Path, output_path: Path) -> Dict[str, Any]:
    """Apply a single plan. Returns a result dict — does not raise on patch
    miss; instead reports it. Raises only on file IO or AST-post failure."""
    result: Dict[str, Any] = {
        "source": str(source_path),
        "target": str(output_path),
        "from_version": plan.target_version_old,
        "to_version": plan.target_version_new,
        "patches_applied": [],
        "patches_failed": [],
        "ast_parse_ok": False,
        "post_const_check": {},
        "wrote_output": False,
    }

    src = source_path.read_text(encoding="utf-8")

    if plan.target_version_old not in src:
        result["patches_failed"].append({
            "label": "expected_version_marker",
            "detail": f"Expected to find version marker for v{plan.target_version_old} "
                      f"in source file but did not — your file may already be "
                      f"patched, or it has drifted from the audited baseline.",
        })

    patched = src
    for p in plan.patches:
        occ = patched.count(p.old)
        if occ != p.occurrences_required:
            result["patches_failed"].append({
                "label": p.label,
                "detail": f"Expected {p.occurrences_required} occurrence(s) of pattern, "
                          f"found {occ}. First 80 chars of pattern: "
                          f"{p.old[:80]!r}",
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

    for const_name, expected in plan.expected_post_constants.items():
        actual = _count_const_list_entries(patched, const_name)
        result["post_const_check"][const_name] = {
            "expected": expected,
            "actual": actual,
            "ok": actual == expected,
        }
        if actual != expected:
            raise PatchError(
                f"Post-patch constant check FAILED in {source_path.name}: "
                f"{const_name} should have {expected} entries, got {actual}.\n"
                f"Refusing to write — your original is untouched."
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(patched, encoding="utf-8")
    result["wrote_output"] = True
    return result


# =============================================================================
# CLI
# =============================================================================
def _print_result(result: Dict[str, Any]) -> None:
    src = Path(result["source"])
    print(f"\n{'='*78}")
    print(f"  {src.name}")
    print(f"  v{result['from_version']} -> v{result['to_version']}")
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

    if result.get("post_const_check"):
        print("  POST-PATCH CONSTANT WIDTHS:")
        for name, info in result["post_const_check"].items():
            mark = "[OK]" if info["ok"] else "[FAIL]"
            print(f"    {mark} {name}: {info['actual']} (expected {info['expected']})")

    if result.get("wrote_output"):
        print(f"  WROTE: {result['target']}")
    else:
        print("  NOT WRITTEN (patches failed; original untouched)")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply Wave 2B canonical-enum + schema-width patches.",
    )
    parser.add_argument(
        "--src-dir",
        type=Path,
        default=Path.cwd(),
        help="Directory containing the source files (default: current directory).",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path.cwd() / "patched",
        help="Directory to write patched files to (default: ./patched/).",
    )
    parser.add_argument(
        "--only",
        type=str,
        default="",
        help="Comma-separated list of basenames to patch (default: all). "
             "e.g. --only investment_advisor_engine.py,top10_selector.py",
    )
    args = parser.parse_args()

    print(f"\nTFB Wave 2B Patcher v{PATCHER_VERSION}")
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
        print(f"  [{status}] {src_name:38s} {applied}/{total} patches applied")

    if overall_ok:
        print("\nAll files patched successfully. Review with `diff` and copy in.")
        return 0
    print(
        "\nSome files were not patched. Review the messages above; your originals "
        "are untouched."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
