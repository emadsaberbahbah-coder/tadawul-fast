#!/usr/bin/env python3
"""Generate a non-enforcing investment-policy shadow report.

The report measures what the project-wide anti-speculation gate would block and
which evidence fields are missing. It never changes a recommendation, writes to
the production workbook, places orders, or enables policy runtime enforcement.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.investment_policy import build_policy_shadow_report, load_policy


def _extract_rows(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, Mapping)]
    if isinstance(payload, Mapping):
        for key in ("rows", "candidates_rows", "candidates", "selected", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, Mapping)]
    return []


def _load_input(path: str | None) -> tuple[list[Mapping[str, Any]], dict[str, Any]]:
    if path:
        if path == "-":
            payload = json.load(sys.stdin)
            return _extract_rows(payload), {"mode": "stdin"}
        source = Path(path).expanduser().resolve()
        with source.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return _extract_rows(payload), {"mode": "file", "path": str(source)}

    try:
        from core.analysis.opportunity_builder import collect_candidates_via_selector
        rows, meta = collect_candidates_via_selector()
        return (
            [row for row in rows if isinstance(row, Mapping)],
            {"mode": "selector", "meta": dict(meta or {})},
        )
    except Exception as exc:
        return [], {
            "mode": "selector_unavailable",
            "error_class": type(exc).__name__,
            "error": str(exc),
        }


def _write_report(report: Mapping[str, Any], output: str | None) -> None:
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if output:
        target = Path(output).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text + "\n", encoding="utf-8")
    else:
        sys.stdout.write(text + "\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a shadow-only investment policy report."
    )
    parser.add_argument(
        "--input",
        help="JSON file, or '-' for stdin. Omit to use the selector best-effort.",
    )
    parser.add_argument("--output", help="Output JSON path. Omit to print to stdout.")
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=25,
        help="Maximum candidate samples included in the report.",
    )
    args = parser.parse_args(argv)

    try:
        policy = load_policy()
        rows, source = _load_input(args.input)
        report = build_policy_shadow_report(
            rows,
            policy,
            sample_limit=max(0, args.sample_limit),
        )
        report["source"] = source
        report["runner"] = "scripts/run_investment_policy_shadow.py"
        _write_report(report, args.output)
        return 0
    except Exception as exc:
        failure = {
            "report_type": "INVESTMENT_POLICY_SHADOW",
            "status": "operational_error",
            "enforcement_applied": False,
            "decision_effect": "NONE_SHADOW_ONLY",
            "error_class": type(exc).__name__,
            "error": str(exc),
        }
        _write_report(failure, args.output)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
