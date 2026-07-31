#!/usr/bin/env python3
"""Repair and verify the request-scoped US-alias transformer output.

This is a narrow safety patch for the assertion-heavy transformer. It fixes
only mechanically introduced defects and verifies that the pre-existing
identity firewall remains intact:

1. orphan ``_batch_set.discard("")`` calls after the transformer replaces the
   corresponding set with a request index;
2. a regression contract that must distinguish preserving an exact last-good
   row from proving a fresh critical identity; and
3. accidental deletion of the identity-tripwire constants/anchor registry.

It does not change provider requests, symbol canonicalization, scoring,
portfolio logic, Sheet writers, or the production concurrency default.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _write(path: str, text: str) -> None:
    (ROOT / path).write_text(text, encoding="utf-8")


def _replace_regex_once(text: str, pattern: str, replacement: str, label: str) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.M)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one match, found {count}")
    return updated


def patch_runner() -> None:
    path = "scripts/run_dashboard_sync.py"
    text = _read(path)

    first_bad = (
        r'^(?P<i>[ \t]+)_batch_index = _build_request_symbol_index\(batch\)\n'
        r'(?P=i)_batch_set\.discard\(""\)\n'
    )
    first_good = "_batch_index = _build_request_symbol_index(batch)"
    if re.search(first_bad, text, flags=re.M):
        text = _replace_regex_once(
            text,
            first_bad,
            lambda m: f"{m.group('i')}{first_good}\n",
            "first-pass orphan discard",
        )
    elif text.count(first_good) != 1:
        raise RuntimeError(
            "first-pass request index: expected one repaired assignment"
        )

    retry_bad = (
        r'^(?P<i>[ \t]+)_batch_set = \{canonicalize_symbol\(t\) for t in rbatch\}\n'
        r'(?P=i)_batch_set\.discard\(""\)\n'
    )
    retry_good = "_batch_index = _build_request_symbol_index(rbatch)"
    if re.search(retry_bad, text, flags=re.M):
        text = _replace_regex_once(
            text,
            retry_bad,
            lambda m: f"{m.group('i')}{retry_good}\n",
            "retry-pass request index",
        )
    elif text.count(retry_good) != 1:
        raise RuntimeError(
            "retry-pass request index: expected one repaired assignment"
        )

    if '_batch_set.discard("")' in text:
        raise RuntimeError("orphan _batch_set.discard remains after repair")
    if text.count("request_index=_batch_index") != 2:
        raise RuntimeError(
            "response resolver must use the request index in both sequential passes"
        )

    guard_markers = (
        '_IDENTITY_TAG = "[v6.22.0 IDENTITY-TRIPWIRE]"',
        '_BATCH_IDENTITY_TAG = "[v6.22.0 BATCH-IDENTITY]"',
        '_SAFE_GW_TAG = "[v6.22.0 SAFE-GATEWAYS]"',
        '_COHERENCE_TAG = "[v6.23.0 COHERENCE-TRIPWIRE]"',
        '_IDENTITY_ANCHORS: Dict[str, Tuple[str, ...]] = {',
        'def _identity_anchor_map() -> Dict[str, Tuple[str, ...]]:',
    )
    missing = [marker for marker in guard_markers if marker not in text]
    if missing:
        raise RuntimeError(
            "request-scoped transform damaged the identity firewall; "
            f"missing markers: {missing}"
        )

    _write(path, text)


def patch_critical_identity_test() -> None:
    path = "tests/test_critical_symbol_identity.py"
    text = _read(path)

    old_name = (
        "test_run_one_task_successful_write_still_fails_missing_fresh_proof"
    )
    new_name = (
        "test_run_one_task_preserves_last_good_but_fails_missing_fresh_proof"
    )
    if old_name in text:
        if text.count(old_name) != 1:
            raise RuntimeError("critical-identity test name is not unique")
        text = text.replace(old_name, new_name, 1)
    elif text.count(new_name) != 1:
        raise RuntimeError("repaired critical-identity test name not found")

    old_assertions = '''        self.assertEqual(len(sheets.writes), 1, "the write path must actually execute")
        self.assertEqual([row[0] for row in sheets.writes[0][2]], ["AAPL", "FISV.US"])
        self.assertEqual(result.rows_written, 2)
        self.assertEqual(result.status, "failed")
        self.assertIn("FISV.US", result.error or "")
'''
    new_assertions = '''        self.assertEqual(len(sheets.writes), 1, "safe persistence may still land")
        written_rows = sheets.writes[0][2]
        self.assertEqual([row[0] for row in written_rows], ["AAPL", "FISV.US"])
        self.assertEqual(
            written_rows[1],
            old_fisv,
            "a missing fresh proof may preserve only the exact verified last-good row",
        )
        self.assertEqual(result.rows_written, 2)
        self.assertEqual(result.status, "failed")
        self.assertIn("FISV.US", result.error or "")
'''
    if old_assertions in text:
        text = text.replace(old_assertions, new_assertions, 1)
    elif new_assertions not in text:
        raise RuntimeError("critical-identity last-good assertion block not found")

    _write(path, text)


def main() -> None:
    patch_runner()
    patch_critical_identity_test()
    print(
        "request-scoped alias output repaired; identity firewall preserved; "
        "last-good persistence cannot upgrade missing fresh identity proof"
    )


if __name__ == "__main__":
    main()
