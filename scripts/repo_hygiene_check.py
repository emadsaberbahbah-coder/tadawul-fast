#!/usr/bin/env python3
# scripts/repo_hygiene_check.py
"""
Repo Hygiene Check — PROD SAFE (v1.2.1)

Goal
- Fail CI if markdown code fences appear inside any .py file.

Why
- A single markdown fence token inside a Python module can break imports if it lands
  outside a string/comment context (and it commonly happens during copy/paste).

What it checks
- Detects common fenced-code markers:
  - triple backticks
  - triple tildes
- Scans all *.py files under the given root, skipping common vendor/cache folders.
- Never crashes startup; best-effort scanning with clear reporting.

Exit codes
- 0: OK (no offenders)
- 1: Read errors (only if --fail-on-read-error is set)
- 2: Offenders found (always fails)

Usage
- python scripts/repo_hygiene_check.py
- python scripts/repo_hygiene_check.py --root .
- python scripts/repo_hygiene_check.py --fail-on-read-error

IMPORTANT
- Do NOT wrap this file itself in any markdown fences when copying into the repo.
"""

from __future__ import annotations

import argparse
import os
import pathlib
from typing import Iterable, List, Optional, Tuple


SCRIPT_VERSION = "1.2.1"


def _make_fence_tokens() -> List[str]:
    """
    Return markdown fence tokens without embedding them literally in this file.

    Notes
    - We avoid writing the fence markers directly.
    - We also avoid using a naive (chr(x) * 3) pattern.
    """
    bt = "".join((chr(0x60), chr(0x60), chr(0x60)))  # backtick x3
    td = "".join((chr(0x7E), chr(0x7E), chr(0x7E)))  # tilde x3
    return [bt, td]


def _should_skip(path: pathlib.Path) -> bool:
    parts = {p.lower() for p in path.parts}
    skip_parts = {
        "venv",
        ".venv",
        "__pycache__",
        ".git",
        ".pytest_cache",
        "node_modules",
        ".mypy_cache",
        ".ruff_cache",
        "dist",
        "build",
        ".eggs",
        ".tox",
    }
    return bool(skip_parts.intersection(parts))


def _iter_py_files(root: pathlib.Path) -> Iterable[pathlib.Path]:
    for p in root.rglob("*.py"):
        if _should_skip(p):
            continue
        yield p


def _line_col_from_index(text: str, idx: int) -> Tuple[int, int]:
    """
    Convert a 0-based character index to 1-based (line, col).
    """
    if idx <= 0:
        return 1, 1
    line = text[:idx].count("\n") + 1
    last_nl = text.rfind("\n", 0, idx)
    col = (idx + 1) if last_nl == -1 else (idx - last_nl)
    return line, col


def _gha_enabled() -> bool:
    return str(os.getenv("GITHUB_ACTIONS") or "").strip().lower() == "true"


def _print_gha_error(path: str, line: int, col: int, msg: str) -> None:
    # GitHub Actions annotation format:
    # ::error file=path,line=1,col=1::message
    print(f"::error file={path},line={line},col={col}::{msg}")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Fail if markdown code fences exist inside .py files.")
    ap.add_argument("--root", default=".", help="Repo root to scan (default: .)")
    ap.add_argument(
        "--fail-on-read-error",
        action="store_true",
        help="If set, unreadable files cause a non-zero exit (1) when no offenders exist.",
    )
    args = ap.parse_args(argv)

    root = pathlib.Path(args.root).resolve()
    this_file = pathlib.Path(__file__).resolve()

    bad_tokens = _make_fence_tokens()

    offenders: List[str] = []
    offenders_gha: List[Tuple[str, int, int, str]] = []
    read_errors: List[str] = []

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

        hit_idx = -1
        for tok in bad_tokens:
            idx = text.find(tok)
            if idx != -1 and (hit_idx == -1 or idx < hit_idx):
                hit_idx = idx

        if hit_idx != -1:
            line, col = _line_col_from_index(text, hit_idx)
            rel = str(p.relative_to(root)) if p.is_absolute() else str(p)
            offenders.append(f"{rel}:{line}:{col} contains markdown fence token")
            offenders_gha.append((rel, line, col, "Markdown fence token found inside a .py file"))

    if read_errors:
        print("⚠️ Repo hygiene check: some files could not be read:\n")
        for o in read_errors:
            print(" -", o)
        print("")

    if offenders:
        print(f"❌ Repo hygiene check FAILED (v{SCRIPT_VERSION}). Found markdown fences inside .py files:\n")
        for o in offenders:
            print(" -", o)

        if _gha_enabled():
            for (rel, line, col, msg) in offenders_gha:
                _print_gha_error(rel, line, col, msg)

        return 2

    if read_errors and bool(args.fail_on_read_error):
        print(f"❌ Repo hygiene check FAILED (v{SCRIPT_VERSION}): unreadable files exist and --fail-on-read-error set.")
        return 1

    print(f"✅ Repo hygiene check OK (v{SCRIPT_VERSION}) — no markdown fences in .py files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
