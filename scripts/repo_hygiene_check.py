#!/usr/bin/env python3
# scripts/repo_hygiene_check.py
"""
Repo Hygiene Check — PROD SAFE (v2.2.0)

Goal
- Fail CI if markdown fences or common LLM copy/paste artifacts appear inside any .py file.
- (Optional) also scan other text-based files if requested via flags.

Why
- A single fenced-code marker can break imports if it lands outside a string/comment context.
- Copy/paste from chats frequently injects these artifacts.

What it checks (default: Python files only)
- Fenced-code markers (constructed dynamically to avoid self-triggering):
  - triple backticks
  - triple tildes
  - "```python" / "```sh" / "```bash"
- Common LLM placeholders:
  - "[your code here]" and variants
  - "<paste here>" variants
- Optional “strict mode” heuristics:
  - "BEGIN CODE" / "END CODE"
  - "Here is the code" (toggleable)

Key upgrades vs v1.4.1
- ✅ Robust token builder (never empty; supports custom tokens file)
- ✅ Fast scanning with early-abort option (stop after N offenders)
- ✅ Context snippet preview (safe, truncated)
- ✅ GitHub Actions annotations (error + warning)
- ✅ Optional: scan additional extensions, or scan ALL text files (careful)
- ✅ More explicit skip rules + include rules
- ✅ Deterministic output for CI (stable ordering)

Exit codes
- 0: OK (no offenders)
- 1: Read errors (only if --fail-on-read-error is set)
- 2: Offenders found (always fails)

Usage
- python scripts/repo_hygiene_check.py
- python scripts/repo_hygiene_check.py --root .
- python scripts/repo_hygiene_check.py --fail-on-read-error
- python scripts/repo_hygiene_check.py --max-offenders 20
- python scripts/repo_hygiene_check.py --extensions .py .gs .js
- python scripts/repo_hygiene_check.py --scan-all-text 1
- python scripts/repo_hygiene_check.py --extra-tokens-file .ci/hygiene_tokens.txt
- python scripts/repo_hygiene_check.py --strict 1

IMPORTANT
- Do NOT wrap this file itself in markdown fences when copying into the repo.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


SCRIPT_VERSION = "2.2.0"

# Default skip directories (expanded)
SKIP_DIRS_DEFAULT: Set[str] = {
    "venv", ".venv", "env", ".env",
    "__pycache__", ".git", ".hg", ".svn", ".idea", ".vscode",
    ".pytest_cache", ".mypy_cache", ".ruff_cache",
    "node_modules", "dist", "build", ".eggs", ".tox",
    "htmlcov", "site-packages", "coverage",
}

# By default we only scan .py
DEFAULT_EXTENSIONS = [".py"]

# Safety: keep context snippet short
SNIPPET_MAX = 160


# -----------------------------------------------------------------------------
# GitHub Actions helpers
# -----------------------------------------------------------------------------
def _gha_enabled() -> bool:
    return str(os.getenv("GITHUB_ACTIONS") or "").strip().lower() == "true"


def _gha_error(file_path: str, line: int, col: int, msg: str) -> None:
    print(f"::error file={file_path},line={line},col={col}::{msg}")


def _gha_warning(file_path: str, line: int, col: int, msg: str) -> None:
    print(f"::warning file={file_path},line={line},col={col}::{msg}")


# -----------------------------------------------------------------------------
# Token construction
# -----------------------------------------------------------------------------
def _bt3() -> str:
    # backtick x3
    return "".join((chr(0x60), chr(0x60), chr(0x60)))


def _td3() -> str:
    # tilde x3
    return "".join((chr(0x7E), chr(0x7E), chr(0x7E)))


def _make_bad_tokens(strict: bool) -> List[str]:
    """
    Return tokens that should trigger a failure.
    Construct fences dynamically to avoid embedding literal fences in this file.
    """
    bt = _bt3()
    td = _td3()

    # Common artifacts
    py_fence = bt + "python"
    sh_fence = bt + "sh"
    bash_fence = bt + "bash"
    js_fence = bt + "javascript"
    ts_fence = bt + "typescript"

    # Common LLM placeholders
    placeholders = [
        "[your code here]",
        "[paste code here]",
        "<your code here>",
        "<paste here>",
        "PASTE YOUR CODE HERE",
    ]

    # Strict heuristics (optional; can be noisy)
    strict_tokens = []
    if strict:
        strict_tokens = [
            "BEGIN CODE",
            "END CODE",
            "Here is the code",
            "Copy ONLY the code",
        ]

    tokens = [bt, td, py_fence, sh_fence, bash_fence, js_fence, ts_fence] + placeholders + strict_tokens

    # Critical: remove empties
    tokens = [t for t in tokens if isinstance(t, str) and t != ""]

    # De-dupe while preserving order
    seen = set()
    out: List[str] = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _load_extra_tokens(path: str) -> List[str]:
    """
    Read extra tokens from a file. One token per line.
    Lines starting with # are comments.
    Empty lines ignored.
    """
    p = pathlib.Path(path)
    if not p.exists():
        return []
    try:
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []

    out: List[str] = []
    for ln in lines:
        s = (ln or "").strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)

    # De-dupe preserve order
    seen = set()
    dedup: List[str] = []
    for t in out:
        if t not in seen:
            seen.add(t)
            dedup.append(t)
    return dedup


# -----------------------------------------------------------------------------
# File iteration
# -----------------------------------------------------------------------------
def _should_skip(path: pathlib.Path, skip_set: Set[str]) -> bool:
    """
    Skip if any path component is in skip_set, or hidden (.) except .github.
    """
    for part in path.parts:
        p_lower = part.lower()

        if p_lower in skip_set:
            return True

        # skip hidden dirs/files except .github
        if part.startswith(".") and p_lower not in {".github"} and part not in {".", ".."}:
            return True

    return False


def _is_text_file(path: pathlib.Path) -> bool:
    """
    Best-effort: treat as text if suffix indicates text or file is small-ish and decodes.
    This is only used when --scan-all-text is enabled.
    """
    # common binary-ish suffixes
    bin_ext = {
        ".png", ".jpg", ".jpeg", ".gif", ".webp", ".pdf",
        ".zip", ".tar", ".gz", ".7z", ".exe", ".dll",
        ".so", ".dylib", ".bin", ".pyc",
    }
    if path.suffix.lower() in bin_ext:
        return False
    return True


def _iter_files(root: pathlib.Path, skip_set: Set[str], extensions: List[str], scan_all_text: bool) -> Iterable[pathlib.Path]:
    try:
        if scan_all_text:
            for p in root.rglob("*"):
                if not p.is_file():
                    continue
                if _should_skip(p, skip_set):
                    continue
                if not _is_text_file(p):
                    continue
                yield p
        else:
            exts = set([e.lower() for e in extensions])
            for p in root.rglob("*"):
                if not p.is_file():
                    continue
                if _should_skip(p, skip_set):
                    continue
                if p.suffix.lower() in exts:
                    yield p
    except OSError as e:
        print(f"Warning: Error walking directory {root}: {e}")


# -----------------------------------------------------------------------------
# Location helpers
# -----------------------------------------------------------------------------
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


def _make_snippet(text: str, idx: int, token_len: int) -> str:
    """
    Small context snippet around the offending token.
    """
    if idx < 0:
        return ""
    start = max(0, idx - 40)
    end = min(len(text), idx + token_len + 80)
    snippet = text[start:end].replace("\n", "\\n").replace("\r", "\\r")
    if len(snippet) > SNIPPET_MAX:
        snippet = snippet[: SNIPPET_MAX - 12] + " ...TRUNC..."
    return snippet


@dataclass
class Finding:
    rel_path: str
    line: int
    col: int
    token: str
    snippet: str


# -----------------------------------------------------------------------------
# Scanner
# -----------------------------------------------------------------------------
def _find_first_token(text: str, tokens: Sequence[str]) -> Tuple[Optional[str], int]:
    found_token = None
    found_idx = -1
    for tok in tokens:
        idx = text.find(tok)
        if idx != -1 and (found_idx == -1 or idx < found_idx):
            found_idx = idx
            found_token = tok
    return found_token, found_idx


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Fail if markdown fences/LLM artifacts exist in source files.")
    ap.add_argument("--root", default=".", help="Repo root to scan (default: .)")
    ap.add_argument("--fail-on-read-error", action="store_true", help="Unreadable files cause exit 1 (if no offenders).")
    ap.add_argument("--exclude", nargs="+", help="Additional directories to exclude (by name)")
    ap.add_argument("--extensions", nargs="+", default=None, help="Extensions to scan (default: .py). Example: .py .js .gs")
    ap.add_argument("--scan-all-text", type=int, default=0, help="1=scan all text-like files, ignoring extensions.")
    ap.add_argument("--extra-tokens-file", default=None, help="File with extra tokens (one per line).")
    ap.add_argument("--strict", type=int, default=0, help="1=enable additional heuristic tokens (may be noisy).")
    ap.add_argument("--max-offenders", type=int, default=0, help="Stop after N offenders (0=unlimited).")
    ap.add_argument("--show-snippets", type=int, default=1, help="1=show context snippet (default: 1)")
    ap.add_argument("--warn-on-suspicious", type=int, default=0, help="1=emit warnings for suspicious patterns (non-failing).")

    args = ap.parse_args(argv)

    root = pathlib.Path(args.root).resolve()
    this_file = pathlib.Path(__file__).resolve()

    strict = bool(int(args.strict or 0))
    scan_all_text = bool(int(args.scan_all_text or 0))
    max_off = int(args.max_offenders or 0)
    show_snippets = bool(int(args.show_snippets or 0))
    warn_suspicious = bool(int(args.warn_on_suspicious or 0))

    # Build skip set
    skip_set = set([s.lower() for s in SKIP_DIRS_DEFAULT])
    if args.exclude:
        for ex in args.exclude:
            if ex:
                skip_set.add(str(ex).lower())

    # Extensions
    extensions = DEFAULT_EXTENSIONS
    if args.extensions:
        extensions = [e if e.startswith(".") else f".{e}" for e in args.extensions]

    # Tokens
    bad_tokens = _make_bad_tokens(strict=strict)
    if args.extra_tokens_file:
        bad_tokens.extend(_load_extra_tokens(args.extra_tokens_file))

    # De-dupe tokens again
    seen = set()
    bad_tokens = [t for t in bad_tokens if not (t in seen or seen.add(t))]  # type: ignore[arg-type]

    offenders: List[Finding] = []
    read_errors: List[str] = []
    checked_count = 0
    skipped_self = 0

    print(f"Starting Repo Hygiene Check v{SCRIPT_VERSION}...")
    print(f"Scanning root: {root}")
    if scan_all_text:
        print("Mode: scan-all-text=1 (all text-like files)")
    else:
        print(f"Mode: extensions={extensions}")
    if strict:
        print("Strict heuristics: ON")
    if args.extra_tokens_file:
        print(f"Extra tokens file: {args.extra_tokens_file}")

    print(f"Bad tokens count: {len(bad_tokens)}")

    for p in _iter_files(root, skip_set, extensions, scan_all_text):
        # Don't scan ourselves if we are inside the root
        try:
            if p.resolve() == this_file:
                skipped_self += 1
                continue
        except OSError:
            pass

        try:
            text = p.read_text(encoding="utf-8", errors="replace")
            checked_count += 1
        except Exception as e:
            read_errors.append(f"{p}  (read error: {e})")
            continue

        tok, idx = _find_first_token(text, bad_tokens)
        if idx != -1 and tok is not None:
            line, col = _line_col_from_index(text, idx)

            try:
                rel = str(p.relative_to(root))
            except ValueError:
                rel = str(p)

            snippet = _make_snippet(text, idx, len(tok)) if show_snippets else ""
            offenders.append(Finding(rel_path=rel, line=line, col=col, token=tok, snippet=snippet))

            if max_off > 0 and len(offenders) >= max_off:
                break

        # Optional suspicious warnings (non-failing)
        if warn_suspicious:
            # Example: a literal "```" is already covered; these are softer patterns
            suspicious = [
                "<<<", ">>>",  # merge conflict-ish
                "YOUR_TOKEN_HERE",
                "REPLACE_ME",
            ]
            for s in suspicious:
                j = text.find(s)
                if j != -1:
                    l2, c2 = _line_col_from_index(text, j)
                    try:
                        rel2 = str(p.relative_to(root))
                    except ValueError:
                        rel2 = str(p)
                    msg = f"Suspicious token found: {repr(s)}"
                    if _gha_enabled():
                        _gha_warning(rel2, l2, c2, msg)
                    # also print once per file
                    break

    # Summary
    print("\n--- Summary ---")
    print(f"Checked: {checked_count} file(s)")
    if skipped_self:
        print(f"Skipped (self): {skipped_self} file(s)")
    if read_errors:
        print(f"Read errors: {len(read_errors)} file(s)")

    if read_errors:
        print("\n⚠️  Unreadable files (showing up to 10):")
        for o in read_errors[:10]:
            print(" -", o)
        if len(read_errors) > 10:
            print(f" ... and {len(read_errors) - 10} more.")

    if offenders:
        print(f"\n❌ Repo hygiene check FAILED (v{SCRIPT_VERSION}). Found artifacts:\n")
        # Deterministic ordering
        offenders_sorted = sorted(offenders, key=lambda f: (f.rel_path, f.line, f.col))
        for f in offenders_sorted:
            msg = f"Artifact found: {repr(f.token)}"
            print(f" - {f.rel_path}:{f.line}:{f.col} -> {msg}")
            if show_snippets and f.snippet:
                print(f"   context: {f.snippet}")

        if _gha_enabled():
            for f in offenders_sorted:
                _gha_error(f.rel_path, f.line, f.col, f"Artifact found: {repr(f.token)}")

        return 2

    if read_errors and args.fail_on_read_error:
        print(f"\n❌ Repo hygiene check FAILED (v{SCRIPT_VERSION}): unreadable files exist and --fail-on-read-error set.")
        return 1

    print(f"\n✅ Repo hygiene check OK (v{SCRIPT_VERSION}) — clean.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
