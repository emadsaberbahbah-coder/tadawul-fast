#!/usr/bin/env python3
# scripts/repo_hygiene_check.py
"""
Repo Hygiene Check — PROD SAFE (v1.4.1)

Goal
- Fail CI if markdown code fences or other common LLM copy-paste artifacts
  appear inside any .py file.

Why
- A single markdown fence token inside a Python module can break imports if it lands
  outside a string/comment context (and it commonly happens during copy/paste).

What it checks
- Detects common fenced-code markers:
  - triple backticks (``` )
  - triple tildes (~~~)
  - "```python" or "```sh" remnants
  - Common LLM placeholders like "[your code here]"
- Scans all *.py files under the given root.
- Respects common ignore patterns (venv, .git, __pycache__).
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
import sys
from typing import Iterable, List, Optional, Tuple, Set

SCRIPT_VERSION = "1.4.1"

# Default skip directories (expanded)
SKIP_DIRS = {
    "venv", ".venv", "env", ".env",
    "__pycache__", ".git", ".hg", ".svn", ".idea", ".vscode",
    ".pytest_cache", ".mypy_cache", ".ruff_cache",
    "node_modules", "dist", "build", ".eggs", ".tox",
    "htmlcov", "site-packages", "coverage"
}


def _make_bad_tokens() -> List[str]:
    """
    Return tokens that should trigger a failure.
    We construct them dynamically to avoid embedding literal fences in this file.
    """
    bt = "".join((chr(0x60), chr(0x60), chr(0x60)))  # backtick x3
    td = "".join((chr(0x7E), chr(0x7E), chr(0x7E)))  # tilde x3

    # Common artifacts
    py_fence = bt + "python"
    sh_fence = bt + "sh"
    bash_fence = bt + "bash"

    # LLM placeholders (must be NON-EMPTY)
    placeholder_1 = "[your code here]"

    tokens = [bt, td, py_fence, sh_fence, bash_fence, placeholder_1]

    # ✅ CRITICAL FIX:
    # Never allow empty tokens (""), because text.find("") == 0 for all files,
    # which would falsely flag every .py file at 1:1 with Artifact found: ''.
    tokens = [t for t in tokens if isinstance(t, str) and t != ""]

    # De-dupe while preserving order
    seen = set()
    out: List[str] = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _should_skip(path: pathlib.Path, skip_set: Set[str]) -> bool:
    # Check if any part of the path is in the skip set
    # Also skip hidden files/dirs starting with . (except .github which is useful to check)
    for part in path.parts:
        p_lower = part.lower()
        if p_lower in skip_set:
            return True
        if part.startswith(".") and part != "." and p_lower not in {".github"}:
            return True
    return False


def _iter_py_files(root: pathlib.Path, skip_set: Set[str]) -> Iterable[pathlib.Path]:
    try:
        for p in root.rglob("*.py"):
            if _should_skip(p, skip_set):
                continue
            yield p
    except OSError as e:
        print(f"Warning: Error walking directory {root}: {e}")


def _line_col_from_index(text: str, idx: int) -> Tuple[int, int]:
    """
    Convert a 0-based character index to 1-based (line, col).
    """
    if idx < 0:
        return 0, 0
    if idx == 0:
        return 1, 1

    line = text.count("\n", 0, idx) + 1
    last_nl = text.rfind("\n", 0, idx)
    if last_nl == -1:
        col = idx + 1
    else:
        col = idx - last_nl
    return line, col


def _gha_enabled() -> bool:
    return str(os.getenv("GITHUB_ACTIONS") or "").strip().lower() == "true"


def _print_gha_error(path: str, line: int, col: int, msg: str) -> None:
    # GitHub Actions annotation format:
    # ::error file={name},line={line},col={col}::{message}
    print(f"::error file={path},line={line},col={col}::{msg}")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Fail if markdown code fences exist inside .py files.")
    ap.add_argument("--root", default=".", help="Repo root to scan (default: .)")
    ap.add_argument(
        "--fail-on-read-error",
        action="store_true",
        help="If set, unreadable files cause a non-zero exit (1) even if no offenders found.",
    )
    ap.add_argument("--exclude", nargs="+", help="Additional directories to exclude")

    args = ap.parse_args(argv)

    root = pathlib.Path(args.root).resolve()
    this_file = pathlib.Path(__file__).resolve()

    # Build skip set
    skip_set = SKIP_DIRS.copy()
    if args.exclude:
        for ex in args.exclude:
            if ex:
                skip_set.add(str(ex).lower())

    bad_tokens = _make_bad_tokens()

    offenders: List[str] = []
    offenders_gha: List[Tuple[str, int, int, str]] = []
    read_errors: List[str] = []

    checked_count = 0
    skipped_count = 0

    print(f"Starting Repo Hygiene Check v{SCRIPT_VERSION}...")
    print(f"Scanning root: {root}")
    print(f"Checking for tokens: {', '.join([repr(t) for t in bad_tokens])}")

    for p in _iter_py_files(root, skip_set):
        # Don't scan ourselves if we are inside the root
        try:
            if p.resolve() == this_file:
                skipped_count += 1
                continue
        except OSError:
            pass

        try:
            text = p.read_text(encoding="utf-8", errors="replace")
            checked_count += 1
        except Exception as e:
            read_errors.append(f"{p}  (read error: {e})")
            continue

        # Find the *first* occurrence of any bad token
        found_token = None
        found_idx = -1

        for tok in bad_tokens:
            idx = text.find(tok)
            if idx != -1 and (found_idx == -1 or idx < found_idx):
                found_idx = idx
                found_token = tok

        if found_idx != -1:
            line, col = _line_col_from_index(text, found_idx)

            try:
                rel_path = p.relative_to(root)
            except ValueError:
                rel_path = p

            msg = f"Artifact found: {repr(found_token)}"
            offenders.append(f"{rel_path}:{line}:{col} -> {msg}")
            offenders_gha.append((str(rel_path), line, col, msg))

    print(f"\n--- Summary ---")
    print(f"Checked: {checked_count} files")
    if skipped_count:
        print(f"Skipped (self): {skipped_count} file(s)")

    if read_errors:
        print(f"\n⚠️  Repo hygiene check: {len(read_errors)} files could not be read:")
        for o in read_errors[:10]:
            print(" -", o)
        if len(read_errors) > 10:
            print(f" ... and {len(read_errors) - 10} more.")
        print("")

    if offenders:
        print(f"\n❌ Repo hygiene check FAILED (v{SCRIPT_VERSION}). Found artifacts inside .py files:\n")
        for o in offenders:
            print(" -", o)

        if _gha_enabled():
            for (rel, line, col, msg) in offenders_gha:
                _print_gha_error(rel, line, col, msg)

        return 2

    if read_errors and args.fail_on_read_error:
        print(f"\n❌ Repo hygiene check FAILED (v{SCRIPT_VERSION}): unreadable files exist and --fail-on-read-error set.")
        return 1

    print(f"\n✅ Repo hygiene check OK (v{SCRIPT_VERSION}) — clean.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
