#!/usr/bin/env python3
"""Generate a non-enforcing investment-policy shadow report.

The report measures what the project-wide anti-speculation gate would block and
which evidence fields are missing. It never changes a recommendation, writes to
the production workbook, places orders, or enables policy runtime enforcement.
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.investment_policy import build_policy_shadow_report, load_policy


def _extract_rows(payload: Any) -> list[Any]:
    """Extract the raw row list without hiding malformed source records."""
    if isinstance(payload, list):
        return list(payload)
    if isinstance(payload, Mapping):
        for key in ("rows", "candidates_rows", "candidates", "selected", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return list(value)
    return []


def _load_input(path: str | None) -> tuple[list[Any], dict[str, Any]]:
    if path:
        if path == "-":
            payload = json.load(sys.stdin)
            return _extract_rows(payload), {"mode": "stdin"}
        source = Path(path).expanduser().resolve()
        with source.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return _extract_rows(payload), {"mode": "file", "path": str(source)}

    try:
        from core.analysis.top10_selector import (
            TOP10_SELECTOR_VERSION,
            build_top10_rows,
        )

        payload = build_top10_rows()
        if inspect.isawaitable(payload):
            payload = asyncio.run(payload)
        rows = _extract_rows(payload)
        meta = dict(payload.get("meta") or {}) if isinstance(payload, Mapping) else {}
        return rows, {
            "mode": "top10_selector",
            "entry_point": "core.analysis.top10_selector.build_top10_rows",
            "selector_version": TOP10_SELECTOR_VERSION,
            "payload_status": payload.get("status") if isinstance(payload, Mapping) else None,
            "row_count": len(rows),
            "meta": meta,
        }
    except Exception as exc:
        return [], {
            "mode": "selector_unavailable",
            "entry_point": "core.analysis.top10_selector.build_top10_rows",
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
        help="JSON file, or '-' for stdin. Omit to use the real Top-10 selector.",
    )
    parser.add_argument("--output", help="Output JSON path. Omit to print to stdout.")
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=25,
        help="Maximum candidate samples included in the report.",
    )
    parser.add_argument(
        "--require-candidates",
        action="store_true",
        help="Exit nonzero when the selected source returns no rows.",
    )
    args = parser.parse_args(argv)

    try:
        policy = load_policy()
        rows, source = _load_input(args.input)
        if args.require_candidates and not rows:
            raise RuntimeError(
                "shadow_source_returned_no_candidates: "
                + json.dumps(source, ensure_ascii=False, default=str)
            )
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
            "runtime_enabled": False,
            "enforcement_applied": False,
            "decision_effect": "NONE_SHADOW_ONLY",
            "error_class": type(exc).__name__,
            "error": str(exc),
        }
        _write_report(failure, args.output)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
