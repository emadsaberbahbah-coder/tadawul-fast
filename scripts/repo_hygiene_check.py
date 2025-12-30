# scripts/repo_hygiene_check.py
from __future__ import annotations

import pathlib
import sys


def _fence() -> str:
    # Avoid placing ``` literally in this file (so the check doesn't flag itself)
    return chr(96) * 3  # 96 = backtick character `


def main() -> int:
    root = pathlib.Path(".").resolve()
    this_file = pathlib.Path(__file__).resolve()

    fence = _fence()
    bad_tokens = [fence, fence + "python", fence + "py"]

    offenders: list[str] = []

    for p in root.rglob("*.py"):
        try:
            if p.resolve() == this_file:
                continue
        except Exception:
            pass

        parts = {x.lower() for x in p.parts}
        if {"venv", ".venv", "__pycache__", ".git"}.intersection(parts):
            continue

        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            offenders.append(f"{p}  (read error: {e})")
            continue

        for tok in bad_tokens:
            idx = text.find(tok)
            if idx != -1:
                line = text[:idx].count("\n") + 1
                offenders.append(f"{p}:{line} contains markdown fence token")
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
