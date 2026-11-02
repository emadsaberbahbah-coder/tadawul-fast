"""Bind each sync validator to its written sources and attest the final cockpit read.

No workbook/provider calls. Scoped keys have already crossed the runner's
canonical-key boundary; unknown or empty scopes cannot fall back to all pages.
The native cockpit is checked once after all source writers/recovery finish.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re


SOURCE_PAGES = {
    "MARKET_LEADERS": "Market_Leaders", "GLOBAL_MARKETS": "Global_Markets",
    "COMMODITIES_FX": "Commodities_FX", "MUTUAL_FUNDS": "Mutual_Funds",
    "MY_PORTFOLIO": "My_Portfolio", "INSIGHTS_ANALYSIS": "Insights_Analysis",
    "DATA_DICTIONARY": "Data_Dictionary",
}
DECISION_KEY = "TOP_10_INVESTMENTS"


def source_pages(keys: str) -> list[str]:
    tokens = str(keys or "").split()
    if not tokens:
        raise ValueError("scoped SYNC_KEYS is empty; no source validation was performed")
    unknown = set(tokens) - set(SOURCE_PAGES) - {DECISION_KEY}
    if unknown:
        raise ValueError("unknown canonical validation keys: " + ",".join(sorted(unknown)))
    pages = list(dict.fromkeys(SOURCE_PAGES[token] for token in tokens if token != DECISION_KEY))
    if not pages:
        raise ValueError("no acquisition pages selected; native Top-10 validation belongs to the final stage")
    return pages


def strict_json(text: str):
    def finite_float(value):
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("nonfinite JSON number")
        return parsed
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON field")
            result[key] = value
        return result
    return json.loads(text, object_pairs_hook=unique, parse_float=finite_float,
                      parse_constant=lambda token: (_ for _ in ()).throw(ValueError("nonfinite JSON")))


def attest_structure(path: Path, process_exit: int, *, run_id: str, source_sha: str, sheet_id: str) -> None:
    if not re.fullmatch(r"[1-9][0-9]*", run_id) or not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("workflow identity is unavailable")
    if not sheet_id or type(process_exit) is not int or process_exit not in (0, 1, 2, 3):
        raise ValueError("structural validation process result is unavailable")
    if path.stat().st_size > 262144:
        raise ValueError("structural report exceeds the bounded witness size")
    report = strict_json(path.read_text(encoding="utf-8"))
    if not isinstance(report, dict) or not isinstance(report.get("meta"), dict):
        raise ValueError("structural report metadata is unavailable")
    report["workflow_witness"] = {
        "run_id": run_id, "source_sha": source_sha,
        "sheet_sha256": hashlib.sha256(sheet_id.encode()).hexdigest(),
        "process_exit_code": process_exit,
    }
    path.write_text(json.dumps(report, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("pages")
    attest = sub.add_parser("attest")
    attest.add_argument("--report", type=Path, required=True)
    attest.add_argument("--process-exit", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "pages":
            print(" ".join(source_pages(os.getenv("SYNC_KEYS", ""))))
        else:
            attest_structure(args.report, args.process_exit, run_id=os.getenv("GITHUB_RUN_ID", ""),
                             source_sha=os.getenv("GITHUB_SHA", ""), sheet_id=os.getenv("TARGET_SHEET_ID", ""))
    except (ValueError, OSError, TypeError) as exc:
        print("::error::Validation scope/witness unavailable: " + type(exc).__name__)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
