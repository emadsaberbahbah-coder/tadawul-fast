#!/usr/bin/env python3
"""Corrected one-shot wrapper for critical identity integration."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import _apply_critical_identity_fix as original

POLICY = ROOT / "scripts" / "critical_symbol_identity.py"
SELF = ROOT / "scripts" / "_apply_critical_identity_fix_v2.py"


def replace_exact(text: str, old: str, new: str, expected: int, label: str) -> str:
    count = text.count(old)
    if count != expected:
        raise SystemExit(f"{label}: expected {expected} match(es), found {count}")
    return text.replace(old, new)


def patch_policy() -> None:
    text = POLICY.read_text(encoding="utf-8")
    text = replace_exact(
        text,
        '    "BRK-B": "BRK-B.US",\n    "FI": "FISV.US",\n    "FI.US": "FISV.US",\n',
        '    "BRK-B": "BRK-B.US",\n    "BRK.B": "BRK-B.US",\n    "FI": "FISV.US",\n    "FI.US": "FISV.US",\n    "FISV": "FISV.US",\n',
        1,
        "canonical aliases",
    )
    text = replace_exact(
        text,
        '    country_tokens: tuple[str, ...] = ("usa", "united states", "us")\n',
        '    country_tokens: tuple[str, ...] = ("usa", "united states")\n',
        1,
        "country tokens",
    )
    norm_anchor = '''def normalize_symbol(value: Any) -> str:\n    return str(value or "").strip().upper()\n\n\n'''
    norm_block = norm_anchor + '''def canonicalize_symbol(value: Any) -> str:\n    symbol = normalize_symbol(value)\n    return CANONICAL_SYMBOLS.get(symbol, symbol)\n\n\n'''
    text = replace_exact(text, norm_anchor, norm_block, 1, "canonicalize helper")
    text = replace_exact(
        text,
        '        target = CANONICAL_SYMBOLS.get(source, source)\n',
        '        target = canonicalize_symbol(source)\n',
        1,
        "sanitizer canonicalization",
    )
    POLICY.write_text(text, encoding="utf-8")


def patch_runner_response_symbols() -> None:
    path = original.RUNNER
    text = path.read_text(encoding="utf-8")
    text = replace_exact(
        text,
        "        build_isolated_batches,\n",
        "        build_isolated_batches,\n        canonicalize_symbol,\n",
        2,
        "runner canonicalize imports",
    )
    text = replace_exact(
        text,
        '_batch_set = {str(t or "").strip().upper() for t in batch}',
        '_batch_set = {canonicalize_symbol(t) for t in batch}',
        1,
        "primary batch requested canonicalization",
    )
    text = replace_exact(
        text,
        '_batch_set = {str(t or "").strip().upper() for t in rbatch}',
        '_batch_set = {canonicalize_symbol(t) for t in rbatch}',
        1,
        "retry batch requested canonicalization",
    )
    text = replace_exact(
        text,
        '_t = str(_row[_idb_sym_i]).strip().upper()',
        '_t = canonicalize_symbol(_row[_idb_sym_i])\n                        _row[_idb_sym_i] = _t',
        2,
        "provider response canonicalization",
    )
    text = replace_exact(
        text,
        'for t in (str(s or "").strip().upper() for s in symbols)',
        'for t in (canonicalize_symbol(s) for s in symbols)',
        1,
        "combined output canonicalization",
    )
    path.write_text(text, encoding="utf-8")


def main() -> None:
    patch_policy()
    original.patch_runner()
    patch_runner_response_symbols()
    original.patch_required_ci_tests()
    original.RECENT_TESTS.write_text(
        original.RECENT_TESTS.read_text(encoding="utf-8").rstrip() + "\n",
        encoding="utf-8",
    )
    original.SELF.unlink(missing_ok=True)
    original.HELPER.unlink(missing_ok=True)
    SELF.unlink(missing_ok=True)
    print("Applied critical identity integration with provider-response canonicalization.")


if __name__ == "__main__":
    main()
