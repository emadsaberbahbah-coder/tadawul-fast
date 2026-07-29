#!/usr/bin/env python3
"""Plan targeted page retries from a completed dashboard-sync artifact set.

The primary sync intentionally preserves last-good rows when a provider fetch is
incomplete. This planner turns those visible page verdicts into a deterministic
retry matrix so every required market page receives its own independent retry
budget instead of sharing one long core-pages process.

The planner is read-only. It never writes to Google Sheets and never changes an
investment recommendation.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Mapping, Sequence

from scripts.audit_sync_outcome import CRITICAL_MARKET_PAGES, audit_artifacts


SCRIPT_VERSION = "1.0.0"

# Order deliberately pairs one large and one smaller page when the workflow uses
# max-parallel=2. GitHub normally launches matrix includes in declaration order.
RECOVERY_ORDER = (
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "Market_Leaders",
)

PAGE_CONFIG: Mapping[str, Mapping[str, object]] = {
    "Global_Markets": {
        "page": "Global_Markets",
        "key": "GLOBAL_MARKETS",
        "group": "global-markets",
        "stagger": 0,
    },
    "Commodities_FX": {
        "page": "Commodities_FX",
        "key": "COMMODITIES_FX",
        "group": "commodities-fx",
        "stagger": 120,
    },
    "Mutual_Funds": {
        "page": "Mutual_Funds",
        "key": "MUTUAL_FUNDS",
        "group": "mutual-funds",
        "stagger": 0,
    },
    "Market_Leaders": {
        "page": "Market_Leaders",
        "key": "MARKET_LEADERS",
        "group": "market-leaders",
        "stagger": 120,
    },
}


def build_recovery_plan(root: Path) -> dict[str, object]:
    """Return a GitHub-matrix-compatible recovery plan for failed/missing pages."""
    audit = audit_artifacts(root, required_pages=CRITICAL_MARKET_PAGES)
    retry_set = set(audit.failed_pages) | set(audit.missing_pages)
    retry_pages = tuple(page for page in RECOVERY_ORDER if page in retry_set)
    include = [dict(PAGE_CONFIG[page]) for page in retry_pages]

    return {
        "schema_version": "1.0",
        "script_version": SCRIPT_VERSION,
        "source_audit_status": audit.status,
        "needs_recovery": bool(include),
        "retry_pages": list(retry_pages),
        "already_refreshed_pages": [
            page
            for page in CRITICAL_MARKET_PAGES
            if page not in retry_set
        ],
        "force_refetch_evidence_lines": audit.force_refetch_evidence_lines,
        "matrix": {"include": include},
    }


def _write_github_outputs(path: Path, plan: Mapping[str, object]) -> None:
    matrix = json.dumps(plan["matrix"], separators=(",", ":"))
    retry_pages = ",".join(str(value) for value in plan["retry_pages"])
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"needs_recovery={'true' if plan['needs_recovery'] else 'false'}\n")
        handle.write(f"matrix={matrix}\n")
        handle.write(f"retry_pages={retry_pages}\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="downloaded source artifact root")
    parser.add_argument("--json-out", help="optional plan JSON path")
    parser.add_argument("--github-output", help="optional GitHub Actions output file")
    args = parser.parse_args(argv)

    try:
        plan = build_recovery_plan(Path(args.root))
    except OSError as exc:
        print(f"::error::SYNC_RECOVERY_PLAN_READ_ERROR: {exc}")
        return 3

    rendered = json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)

    if args.json_out:
        Path(args.json_out).write_text(rendered + "\n", encoding="utf-8")
    if args.github_output:
        _write_github_outputs(Path(args.github_output), plan)

    if plan["needs_recovery"]:
        print(f"::warning::Automatic page recovery required: {', '.join(plan['retry_pages'])}")
    else:
        print("::notice::All required market pages refreshed; no recovery jobs needed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
