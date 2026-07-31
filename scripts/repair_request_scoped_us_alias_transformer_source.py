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

BROAD = r'''        r"^def _filter_rows_to_requested\\(.*?(?=^def [A-Za-z_]\\w*\\()",
'''
NARROW = r'''        r"^def _filter_rows_to_requested\\(.*?^    return kept_rows, dropped\\n\\n",
'''


def main() -> None:
    text = PATH.read_text(encoding="utf-8")
    if BROAD in text:
        if text.count(BROAD) != 1:
            raise RuntimeError("broad membership replacement pattern is not unique")
        text = text.replace(BROAD, NARROW, 1)
        PATH.write_text(text, encoding="utf-8")
        print("request-membership transformer boundary narrowed")
        return
    if text.count(NARROW) != 1:
        raise RuntimeError("neither broad nor repaired membership boundary found")
    print("request-membership transformer boundary already safe")


if __name__ == "__main__":
    main()
