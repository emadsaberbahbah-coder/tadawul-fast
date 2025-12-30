# scripts/repo_hygiene_check.py
from __future__ import annotations

import pathlib
import sys

BAD_TOKENS = ["```", "```python", "```py"]

def main() -> int:
    root = pathlib.Path(".").resolve()
    offenders: list[str] = []

    for p in root.rglob("*.py"):
        # skip virtualenv / cache folders just in case
        parts = {x.lower() for x in p.parts}
        if {"venv", ".venv", "__pycache__", ".git"}.intersection(parts):
            continue

        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            offenders.append(f"{p}  (read error: {e})")
            continue

        for tok in BAD_TOKENS:
            if tok in text:
                # show a small hint line number
                idx = text.find(tok)
                line = text[:idx].count("\n") + 1
                offenders.append(f"{p}:{line} contains {tok!r}")
                break

    if offenders:
        print("❌ Repo hygiene check FAILED. Found markdown fences inside .py files:\n")
        for o in offenders:
            print(" -", o)
        return 2

    print("✅ Repo hygiene check OK (no markdown fences in .py files).")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
