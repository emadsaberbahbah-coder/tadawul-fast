#!/usr/bin/env python3
"""
================================================================================
TFB Wave 3 Patcher — apply_wave3_patches.py
================================================================================
APPENDS 5 INSIGHTS-GROUP COLUMNS TO BRING ENGINE FILES TO v2.6.0 ALIGNMENT (90/93)

Prerequisite
------------
**Wave 2B must be applied first.** This patcher works against files at:
- core/data_engine_v2.py            v5.48.0  (Wave 2B post-state)
- core/analysis/top10_selector.py   v4.9.0   (Wave 2B post-state)

If your files are still at v5.47.2 / v4.8.0, run apply_wave2b_patches.py
first, then come back here.

What this patches
-----------------
1. core/data_engine_v2.py             v5.48.0 -> v5.49.0
   - INSTRUMENT_CANONICAL_KEYS:    85 -> 90 (appends 5 Insights keys)
   - INSTRUMENT_CANONICAL_HEADERS: 85 -> 90 (appends 5 Insights headers)
   - __version__ bump
   - STATIC_CANONICAL_SHEET_CONTRACTS re-derives automatically
     (it uses list(INSTRUMENT_CANONICAL_*) so no separate edit needed)

2. core/analysis/top10_selector.py    v4.9.0 -> v4.10.0
   - DEFAULT_FALLBACK_KEYS:    88 -> 93 (5 Insights inserted before 3 Top10 extras)
   - DEFAULT_FALLBACK_HEADERS: 88 -> 93
   - TOP10_SELECTOR_VERSION bump

The 5 Insights columns appended at the END of the canonical schema:
  - sector_relative_score  ("Sector-Adj Score")    — float, 0-100, model
  - conviction_score       ("Conviction Score")    — float, 0-100, model
  - top_factors            ("Top Factors")         — str, pipe-separated
  - top_risks              ("Top Risks")           — str, pipe-separated
  - position_size_hint     ("Position Size Hint")  — str, text

All 5 are produced by core.insights_builder v1.0.0 (Wave 1).

How to run
----------
1. Place this script in the same directory as the source files.
2. Run:   python3 apply_wave3_patches.py
3. Patched files appear in ./patched/. Diff and copy in.

The patcher refuses to write any file where a patch fails to match — that's
a signal your local file isn't at the expected v5.48.0 / v4.9.0 baseline
and you should investigate before forcing the change.

Conservative by design: never modifies your originals.
================================================================================
"""

from __future__ import annotations

import argparse
import ast
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
    label: str
    old: str
    new: str
    occurrences_required: int = 1


@dataclass
class FilePatchPlan:
    source_basename: str
    target_version_old: str
    target_version_new: str
    patches: List[Patch]
    expected_post_constants: Dict[str, int]


# =============================================================================
# data_engine_v2.py  v5.48.0 -> v5.49.0
# =============================================================================
DATA_ENGINE_V2_PLAN = FilePatchPlan(
    source_basename="data_engine_v2.py",
    target_version_old="5.48.0",
    target_version_new="5.49.0",
    patches=[
        # 1. __version__
        Patch(
            label="module_version_string",
            old='__version__ = "5.48.0"',
            new='__version__ = "5.49.0"',
        ),
        # 2. INSTRUMENT_CANONICAL_KEYS: append 5 Insights keys
        # The closing "]\n\nINSTRUMENT_CANONICAL_HEADERS" anchors this to the KEYS list ONLY
        Patch(
            label="INSTRUMENT_CANONICAL_KEYS_append_insights",
            old=(
                '    "data_provider",\n'
                '    "last_updated_utc",\n'
                '    "last_updated_riyadh",\n'
                '    "warnings",\n'
                ']\n'
                '\n'
                'INSTRUMENT_CANONICAL_HEADERS'
            ),
            new=(
                '    "data_provider",\n'
                '    "last_updated_utc",\n'
                '    "last_updated_riyadh",\n'
                '    "warnings",\n'
                '    # v2.6.0 Insights group (Wave 3) — produced by core.insights_builder v1.0.0\n'
                '    "sector_relative_score",\n'
                '    "conviction_score",\n'
                '    "top_factors",\n'
                '    "top_risks",\n'
                '    "position_size_hint",\n'
                ']\n'
                '\n'
                'INSTRUMENT_CANONICAL_HEADERS'
            ),
        ),
        # 3. INSTRUMENT_CANONICAL_HEADERS: append 5 Insights headers
        Patch(
            label="INSTRUMENT_CANONICAL_HEADERS_append_insights",
            old=(
                '    "Last Updated (UTC)",\n'
                '    "Last Updated (Riyadh)",\n'
                '    "Warnings",\n'
                ']'
            ),
            new=(
                '    "Last Updated (UTC)",\n'
                '    "Last Updated (Riyadh)",\n'
                '    "Warnings",\n'
                '    "Sector-Adj Score",\n'
                '    "Conviction Score",\n'
                '    "Top Factors",\n'
                '    "Top Risks",\n'
                '    "Position Size Hint",\n'
                ']'
            ),
        ),
    ],
    expected_post_constants={
        "INSTRUMENT_CANONICAL_KEYS": 90,
        "INSTRUMENT_CANONICAL_HEADERS": 90,
    },
)


# =============================================================================
# top10_selector.py  v4.9.0 -> v4.10.0
# =============================================================================
TOP10_SELECTOR_PLAN = FilePatchPlan(
    source_basename="top10_selector.py",
    target_version_old="4.9.0",
    target_version_new="4.10.0",
    patches=[
        # 1. TOP10_SELECTOR_VERSION constant
        Patch(
            label="TOP10_SELECTOR_VERSION_constant",
            old='TOP10_SELECTOR_VERSION = "4.9.0"',
            new='TOP10_SELECTOR_VERSION = "4.10.0"',
        ),
        # 2. DEFAULT_FALLBACK_KEYS: insert 5 Insights between warnings and 3 Top10 extras
        Patch(
            label="DEFAULT_FALLBACK_KEYS_insert_insights_before_top10_extras",
            old=(
                '    "unrealized_pl_pct", "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",\n'
                '    "top10_rank", "selection_reason", "criteria_snapshot",\n'
            ),
            new=(
                '    "unrealized_pl_pct", "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",\n'
                '    "sector_relative_score", "conviction_score", "top_factors", "top_risks", "position_size_hint",\n'
                '    "top10_rank", "selection_reason", "criteria_snapshot",\n'
            ),
        ),
        # 3. DEFAULT_FALLBACK_HEADERS: insert 5 Insights headers
        Patch(
            label="DEFAULT_FALLBACK_HEADERS_insert_insights_before_top10_extras",
            old=(
                '    "Unrealized P/L %", "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",\n'
                '    "Top10 Rank", "Selection Reason", "Criteria Snapshot",\n'
            ),
            new=(
                '    "Unrealized P/L %", "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",\n'
                '    "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks", "Position Size Hint",\n'
                '    "Top10 Rank", "Selection Reason", "Criteria Snapshot",\n'
            ),
        ),
    ],
    expected_post_constants={
        "DEFAULT_FALLBACK_KEYS": 93,    # 90 canonical + 3 Top10 extras
        "DEFAULT_FALLBACK_HEADERS": 93,
    },
)


ALL_PLANS: Tuple[FilePatchPlan, ...] = (
    DATA_ENGINE_V2_PLAN,
    TOP10_SELECTOR_PLAN,
)


# =============================================================================
# Patcher engine
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


def _count_const_list_entries(src: str, name: str) -> Optional[int]:
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
            "detail": (
                f"Expected to find version marker for v{plan.target_version_old} "
                f"in source file but did not — your file may be at a different "
                f"version. If you haven't applied Wave 2B yet, run "
                f"apply_wave2b_patches.py first."
            ),
        })

    patched = src
    for p in plan.patches:
        occ = patched.count(p.old)
        if occ != p.occurrences_required:
            result["patches_failed"].append({
                "label": p.label,
                "detail": f"Expected {p.occurrences_required} occurrence(s), found {occ}. "
                          f"First 80 chars of pattern: {p.old[:80]!r}",
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
    parser = argparse.ArgumentParser(description="Apply Wave 3 (v2.6.0 Insights) patches.")
    parser.add_argument("--src-dir", type=Path, default=Path.cwd())
    parser.add_argument("--out-dir", type=Path, default=Path.cwd() / "patched")
    parser.add_argument("--only", type=str, default="")
    args = parser.parse_args()

    print(f"\nTFB Wave 3 Patcher v{PATCHER_VERSION}")
    print(f"Source dir: {args.src_dir.resolve()}")
    print(f"Output dir: {args.out_dir.resolve()}")
    print("Prerequisite: Wave 2B must already be applied (files at v5.48.0 / v4.9.0).")

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
    print("\nSome files were not patched. Review messages above; originals untouched.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
