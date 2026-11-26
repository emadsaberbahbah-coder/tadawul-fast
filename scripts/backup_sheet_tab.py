#!/usr/bin/env python3
"""Copy one workbook tab to a hidden, dated backup tab before a repair writes to it.

WHY v1.0.0 (2026-11-25): repair_pit_legacy_rows.py --apply is documented as an
operator action "after a workbook backup", and pit_legacy_repair.yml runs it
from a dispatch. This makes the backup part of the run instead of a manual step.
The copy stays inside the same workbook (no file leaves the account) as
<prefix><YYYYMMDD Riyadh>, hidden. An existing backup of that name is never
overwritten: the run keeps the first copy of the day and reports it. Prints the
backup name and its row count only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

SCRIPT_VERSION = "1.0.0"
RIYADH = dt.timezone(dt.timedelta(hours=3))


def backup_name(prefix: str, now: dt.datetime) -> str:
    return f"{prefix}{now.astimezone(RIYADH).strftime('%Y%m%d')}"


def backup_tab(book, tab: str, prefix: str, now: dt.datetime) -> tuple[str, str, int]:
    """Return (name, status, rows); status is 'created' or 'exists'. Mockable."""
    if not prefix or prefix == tab:
        raise ValueError("backup prefix must differ from the tab name")
    name = backup_name(prefix, now)
    titles = [ws.title for ws in book.worksheets()]
    if name in titles:
        return name, "exists", book.worksheet(name).row_count
    source = book.worksheet(tab)
    copy = book.duplicate_sheet(source.id, new_sheet_name=name, insert_sheet_index=len(titles))
    copy.hide()
    rows = len(copy.get_all_values())
    if rows != len(source.get_all_values()):
        raise ValueError("backup row count differs from the source")
    return name, "created", rows


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tab", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--sheet-id")
    args = ap.parse_args(argv)
    try:
        from scripts.quarantine_placeholder_records import _open_sheet
        name, status, rows = backup_tab(_open_sheet(args.sheet_id), args.tab, args.prefix,
                                        dt.datetime.now(dt.timezone.utc))
    except Exception as exc:  # noqa: BLE001
        print(f"[TAB-BACKUP v{SCRIPT_VERSION}] status=ERROR ({type(exc).__name__}); nothing should be repaired.")
        return 2
    print(f"[TAB-BACKUP v{SCRIPT_VERSION}] backup={name} status={status} rows={rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
