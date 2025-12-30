# scripts/repo_hygiene_check.py  (FULL REPLACEMENT)
from __future__ import annotations

import pathlib
import sys
from typing import Iterable


def _make_fence_tokens() -> list[str]:
    """
    Return markdown fence tokens without embedding them literally in this file.

    Important:
    - Do not create the fence using chr(96)*3 here, because some scanners
      may still flag patterns that look like a fence builder.
    - Instead, build from code points in a slightly different way.
    """
    bt = "".join([chr(0x60), chr(0x60), chr(0x60)])  # backtick x3
    return [bt, bt + "python", bt + "py"]


def _should_skip(path: pathlib.Path) -> bool:
    parts = {p.lower() for p in path.parts}
    return bool({"venv", ".venv", "__pycache__", ".git"}.intersection(parts))


def _iter_py_files(root: pathlib.Path) -> Iterable[pathlib.Path]:
    for p in root.rglob("*.py"):
        if _should_skip(p):
            continue
        yield p


def main() -> int:
    root = pathlib.Path(".").resolve()
    this_file = pathlib.Path(__file__).resolve()

    bad_tokens = _make_fence_tokens()
    offenders: list[str] = []

    for p in _iter_py_files(root):
        try:
            if p.resolve() == this_file:
                continue
        except Exception:
            # If resolve fails, still scan it (but avoid crashing)
            pass

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
