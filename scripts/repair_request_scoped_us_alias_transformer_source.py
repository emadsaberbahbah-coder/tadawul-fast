#!/usr/bin/env python3
"""Narrow the request-membership transformer to the function body only.

The original broad regex stopped at the next ``def`` and therefore consumed
module-level identity-firewall constants and anchor data placed between the
membership function and the next function. This source repair changes only that
replacement boundary and is idempotent.
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "scripts" / "apply_request_scoped_us_alias_fix.py"


def main() -> None:
    text = PATH.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    matches = [
        index
        for index, line in enumerate(lines)
        if "^def _filter_rows_to_requested" in line
        and ("(?=^def" in line or "return kept_rows, dropped" in line)
    ]
    if len(matches) != 1:
        raise RuntimeError(
            "membership replacement boundary is not unique: "
            f"found {len(matches)} candidate lines"
        )

    index = matches[0]
    current = lines[index]
    if "return kept_rows, dropped" in current:
        print("request-membership transformer boundary already safe")
        return
    if "(?=^def" not in current:
        raise RuntimeError("unexpected membership replacement boundary")

    indent = current[: len(current) - len(current.lstrip())]
    lines[index] = (
        indent
        + r'r"^def _filter_rows_to_requested\(.*?^    return kept_rows, dropped\n\n",'
        + "\n"
    )
    PATH.write_text("".join(lines), encoding="utf-8")
    print("request-membership transformer boundary narrowed")


if __name__ == "__main__":
    main()
