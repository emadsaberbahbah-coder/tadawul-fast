#!/usr/bin/env python3
"""Print the resolved real-money decision safety plan without changing runtime."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from core.runtime_decision_safety import build_plan


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("off", "shadow", "enforce"),
        help="mode to inspect; defaults to TFB_DECISION_SAFETY_MODE/off",
    )
    parser.add_argument("--json-out", help="optional report output path")
    args = parser.parse_args()

    plan = build_plan(os.environ, mode=args.mode)
    rendered = json.dumps(plan.to_dict(), indent=2, sort_keys=True)
    print(rendered)
    if args.json_out:
        Path(args.json_out).write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
