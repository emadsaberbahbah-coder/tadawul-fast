# scripts/repo_hygiene_check.py
from __future__ import annotations

import pathlib
from typing import Iterable


def _make_fence_tokens() -> list[str]:
    """
    Return markdown fence tokens without embedding them literally in this file.

    Notes
    - We avoid writing triple-backticks directly.
    - We also avoid generating them via chr(96)*3 to reduce simplistic signature matches.
    """
    bt = "".join((chr(0x60), chr(0x60), chr(0x60)))  # backtick x3
    return [bt, bt + "python", bt + "py"]


def _should_skip(path: pathlib.Path) -> bool:
    parts = {p.lower() for p in path.parts}
    skip_parts = {"venv", ".venv", "__pycache__", ".git", ".pytest_cache", "node_modules"}
    return bool(skip_parts.intersection(parts))


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
    read_errors: list[str] = []

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
            read_errors.append(f"{p}  (read error: {e})")
            continue

        for tok in bad_tokens:
            idx = text.find(tok)
            if idx != -1:
                line = text[:idx].count("\n") + 1
                offenders.append(f"{p}:{line} contains markdown fence token")
                break

    if read_errors:
        print("⚠️ Repo hygiene check: some files could not be read:\n")
        for o in read_errors:
            print(" -", o)
        print("")

    if offenders:
        print("❌ Repo hygiene check FAILED. Found markdown fences inside .py files:\n")
        for o in offenders:
            print(" -", o)
        return 2

    print("✅ Repo hygiene check OK (no markdown fences in .py files).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
