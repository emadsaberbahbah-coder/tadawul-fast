#!/usr/bin/env python3
"""Corrected one-shot wrapper for the concurrency workflow patch."""
from __future__ import annotations

from scripts import _apply_production_concurrency_fix as original

SELF = original.ROOT / "scripts" / "_apply_production_concurrency_fix_v2.py"


def replace_last(text: str, old: str, new: str, label: str) -> str:
    if label != "recovery unittest list":
        return original._ORIGINAL_REPLACE_ONCE(text, old, new, label)
    index = text.rfind(old)
    if index < 0:
        raise SystemExit(f"{label}: expected at least one match, found 0")
    return text[:index] + new + text[index + len(old):]


def main() -> None:
    original._ORIGINAL_REPLACE_ONCE = original.replace_once
    original.replace_once = replace_last
    original.patch_daily()
    original.patch_recovery()
    original.SELF.unlink(missing_ok=True)
    original.HELPER.unlink(missing_ok=True)
    SELF.unlink(missing_ok=True)
    print("Applied corrected production concurrency isolation and inline recovery.")


if __name__ == "__main__":
    main()
