#!/usr/bin/env python3
"""Plan the bounded PIT header-migration repair; dry-run is the default.

WHY v1.0.0 (2026-10-11): the 16->24 header migration preserved historical
values in place. Original timestamps consequently appear as forecast fields.
Only date-bounded, 16-wide rows with a valid original capture timestamp may
move those two values to W:X; the eight unrecorded diagnostics remain blank.
Eleven explicitly enumerated 2026-07-22 Source '-' artifacts may be removed.

--scan reads and prints counts without writes, including to _Run_Log. --apply
is an operator-only action after a workbook backup: require the exact current
header, no anomalies, at most 78 realigns/11 deletes, and a second unchanged
read before mutation. Realigns precede bottom-up deletes, then one _Run_Log
entry records the actual counts. No live repair is performed by repository
tests. --input-json allows an entirely offline scan of an exported grid.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.pit_snapshot import HEADERS, LEGACY_HEADERS_16

SCRIPT_VERSION = "1.0.0"
PIT_TAB = "_PIT_Fundamentals"
MAX_REALIGNS = 78
MAX_DELETES = 11
MAX_SCAN_ROWS = 100_000
MAX_INPUT_BYTES = 30_000_000
LEGACY_START = "2026-07-22"
LEGACY_END = "2026-07-28"
LEGACY_DATES = frozenset(f"2026-07-{day:02d}" for day in range(22, 29))
ARTIFACT_KEYS = frozenset(("2026-07-22", symbol) for symbol in (
    "FORECAST", "INVESTABILITY", "COUNT", "413", "1477", "290", "5", "3",
    "1", "NAME", "SEMPRA",
))


def _blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _timestamp_shaped(value: Any) -> bool:
    if not isinstance(value, str) or not re.fullmatch(
            r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?", value):
        return False
    try:
        dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def build_plan(values: Sequence[Sequence[Any]]) -> tuple[list[dict], list[dict], list[dict]]:
    """Return (realign, delete, anomalies), with exact 1-based sheet rows.

    A row can be in both plans: its historical timestamp may be misplaced
    while its identity is also an explicit artifact. No timestamp/diagnostic
    values are reconstructed. Already realigned rows no longer match.
    """
    realign, delete, anomalies = [], [], []
    if (not values or not isinstance(values[0], (list, tuple))
            or list(values[0]) != HEADERS):
        return [], [], [{"sheet_row": 1, "reason": "header_mismatch"}]
    if len(values) > MAX_SCAN_ROWS:
        return [], [], [{"sheet_row": 0, "reason": "scan_row_limit"}]
    seen_artifacts = set()
    for sheet_row, source in enumerate(values[1:], start=2):
        if not isinstance(source, (list, tuple)):
            anomalies.append({"sheet_row": sheet_row, "reason": "invalid_row"})
            continue
        row = list(source)
        if all(_blank(value) for value in row):
            continue
        padded = row + [""] * max(0, len(HEADERS) - len(row))
        date, symbol, page = padded[:3]
        key = (date, symbol)
        artifact_delete = False
        if isinstance(date, str) and isinstance(symbol, str) and key in ARTIFACT_KEYS:
            if page != "-":
                anomalies.append({"sheet_row": sheet_row, "reason": "artifact_source_drift"})
            elif key in seen_artifacts:
                anomalies.append({"sheet_row": sheet_row, "reason": "duplicate_artifact_key"})
            else:
                seen_artifacts.add(key)
                delete.append({"sheet_row": sheet_row, "before": row})
                artifact_delete = True
        width = next((index + 1 for index in range(len(row) - 1, -1, -1)
                      if not _blank(row[index])), 0)
        in_window = isinstance(date, str) and date in LEGACY_DATES
        timestamp_in_diagnostic = any(_timestamp_shaped(padded[index]) for index in (14, 15))
        if (in_window and width == len(LEGACY_HEADERS_16)
                and _timestamp_shaped(padded[15])
                and all(_blank(value) for value in padded[16:])):
            repaired = padded[:len(HEADERS)]
            repaired[22:24] = repaired[14:16]
            repaired[14:22] = [""] * 8
            realign.append({"sheet_row": sheet_row, "before": row, "after": repaired})
        elif not artifact_delete and (timestamp_in_diagnostic or (in_window and width <= len(LEGACY_HEADERS_16))):
            anomalies.append({"sheet_row": sheet_row, "reason": "legacy_predicate_mismatch"})
    return realign, delete, anomalies


def _summary(realign, delete, anomalies, *, apply: bool) -> str:
    return (f"[PIT-REPAIR v{SCRIPT_VERSION}] realigned={len(realign)} "
            f"deleted={len(delete)} mode={'APPLY' if apply else 'DRY'} "
            f"anomalies={len(anomalies)}")


def apply_plan(book, worksheet, values, realign, delete, anomalies) -> None:
    """Apply only a freshly revalidated plan; mockable for offline tests."""
    if (anomalies or len(realign) > MAX_REALIGNS or len(delete) > MAX_DELETES
            or build_plan(values) != (realign, delete, anomalies)):
        raise ValueError("unsafe PIT repair plan")
    if worksheet.get(f"A1:Z{MAX_SCAN_ROWS + 1}") != values:
        raise ValueError("PIT grid changed after scan")
    if realign:
        worksheet.batch_update([
            {"range": f"O{item['sheet_row']}:X{item['sheet_row']}",
             "values": [item["after"][14:24]]} for item in realign
        ], value_input_option="RAW")
    for item in sorted(delete, key=lambda entry: entry["sheet_row"], reverse=True):
        worksheet.delete_rows(item["sheet_row"])
    # A failed run-log append propagates: an operator must review partial I/O
    # instead of receiving an unconditional success after an unlogged repair.
    book.worksheet("_Run_Log").append_row([
        dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "WARNING", "repair_pit_legacy_rows", PIT_TAB, "OK",
        _summary(realign, delete, anomalies, apply=True), "", "", "",
        json.dumps({"version": SCRIPT_VERSION, "realigned": len(realign), "deleted": len(delete)}),
    ], value_input_option="RAW")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--scan", action="store_true", help="Read-only scan (default)")
    mode.add_argument("--apply", action="store_true", help="Operator-only repair after a backup")
    parser.add_argument("--sheet-id")
    parser.add_argument("--input-json", type=Path, help="Offline exported grid; never permits --apply")
    args = parser.parse_args(argv)
    try:
        if args.input_json is not None:
            if args.apply or not args.input_json.is_file() or args.input_json.stat().st_size > MAX_INPUT_BYTES:
                raise ValueError("invalid offline scan input")
            raw = args.input_json.read_bytes()
            if len(raw) > MAX_INPUT_BYTES:
                raise ValueError("oversized offline scan input")
            values = json.loads(raw)
            book = worksheet = None
        else:
            from scripts.quarantine_placeholder_records import _open_sheet
            book = _open_sheet(args.sheet_id)
            worksheet = book.worksheet(PIT_TAB)
            values = worksheet.get(f"A1:Z{MAX_SCAN_ROWS + 1}")
        if not isinstance(values, list):
            raise TypeError("invalid PIT grid")
        realign, delete, anomalies = build_plan(values)
        over_ceiling = len(realign) > MAX_REALIGNS or len(delete) > MAX_DELETES
        if args.apply:
            if anomalies or over_ceiling:
                print(_summary(realign, delete, anomalies, apply=False))
                print("REFUSED: unsafe PIT repair plan; nothing written.")
                return 3
            apply_plan(book, worksheet, values, realign, delete, anomalies)
        print(_summary(realign, delete, anomalies, apply=args.apply))
        return 2 if anomalies else 0
    except Exception:  # noqa: BLE001 — safe CLI status also covers optional Sheets transport failures
        print(f"[PIT-REPAIR v{SCRIPT_VERSION}] status=ERROR; review scan or partial I/O before retrying.")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
