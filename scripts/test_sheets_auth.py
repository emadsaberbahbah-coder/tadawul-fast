#!/usr/bin/env python3
# scripts/test_sheets_auth.py  (FULL REPLACEMENT)
"""
test_sheets_auth.py
===========================================================
Google Sheets Auth & Permission Tester (v1.3.0) – TFB PROD SAFE
===========================================================

Purpose
- Verify GOOGLE_SHEETS_CREDENTIALS is present and valid JSON (service account).
- Verify the Service Account can READ the target spreadsheet.
- Optional: Verify WRITE permission by writing a timestamp to a specific cell.

Usage
  python scripts/test_sheets_auth.py
  python scripts/test_sheets_auth.py --sheet-id <ID>
  python scripts/test_sheets_auth.py --sheet-name "Market_Leaders" --read-range A5:C5
  python scripts/test_sheets_auth.py --write --write-cell A1
  python scripts/test_sheets_auth.py --dry-run

Notes
- This script tolerates both import paths:
    - google_sheets_service.py (project root)
    - integrations/google_sheets_service.py
- Spreadsheet ID is resolved from:
    --sheet-id -> env.DEFAULT_SPREADSHEET_ID -> DEFAULT_SPREADSHEET_ID
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

# Ensure project root is in path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("SheetsAuthTest")

# Try both module locations
try:
    import google_sheets_service as sheets  # type: ignore
except Exception:
    try:
        from integrations import google_sheets_service as sheets  # type: ignore
    except Exception as e:
        print(f"❌ Critical: Could not import google_sheets_service (root or integrations). {e}")
        sys.exit(1)

# Optional env settings
try:
    from env import settings  # type: ignore
except Exception:
    settings = None  # type: ignore


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get_spreadsheet_id(cli: Optional[str]) -> str:
    if cli and str(cli).strip():
        return str(cli).strip()

    if settings is not None:
        sid = (getattr(settings, "default_spreadsheet_id", "") or "").strip()
        if sid:
            return sid

    return (os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip()


def _escape_sheet_name(name: str) -> str:
    n = (name or "").strip().replace("'", "''")
    return f"'{n}'"


def _validate_creds_json(raw: str) -> bool:
    try:
        obj = json.loads(raw)
    except Exception:
        return False
    if not isinstance(obj, dict):
        return False
    # lightweight checks
    return bool(obj.get("client_email")) and bool(obj.get("private_key"))


def main() -> int:
    p = argparse.ArgumentParser(description="Test Google Service Account permissions for Google Sheets")
    p.add_argument("--sheet-id", help="Override Spreadsheet ID (default: env DEFAULT_SPREADSHEET_ID)")
    p.add_argument("--sheet-name", default="Sheet1", help="Tab name to read/write (default: Sheet1)")
    p.add_argument("--read-range", default="A1:B2", help="A1 range to read (default: A1:B2)")
    p.add_argument("--write", action="store_true", help="Attempt to WRITE a timestamp to the sheet")
    p.add_argument("--write-cell", default="A1", help="Cell to write to if --write is enabled (default: A1)")
    p.add_argument("--dry-run", action="store_true", help="Do not write even if --write is set")
    args = p.parse_args()

    # 1) Check credentials existence + basic JSON validity
    logger.info("--- Step 1: Checking GOOGLE_SHEETS_CREDENTIALS ---")
    creds_raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "").strip()
    if not creds_raw:
        logger.error("❌ GOOGLE_SHEETS_CREDENTIALS is missing/empty.")
        logger.info("   Fix: Add GOOGLE_SHEETS_CREDENTIALS in GitHub Secrets / Render Env Vars.")
        return 1

    if not _validate_creds_json(creds_raw):
        logger.error("❌ GOOGLE_SHEETS_CREDENTIALS is present but does not look like valid Service Account JSON.")
        logger.info("   Tip: It must include client_email and private_key.")
        return 1

    # 2) Init service
    logger.info("--- Step 2: Initializing Sheets service ---")
    try:
        service = sheets.get_sheets_service()
        if not service:
            logger.error("❌ get_sheets_service() returned None.")
            return 1
        logger.info("✅ Sheets service initialized.")
    except Exception as e:
        logger.error("❌ Failed to initialize Sheets service: %s", e)
        return 1

    # 3) Read test
    logger.info("--- Step 3: Checking READ access ---")
    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("❌ Spreadsheet ID missing. Use --sheet-id or set DEFAULT_SPREADSHEET_ID.")
        return 1

    read_range = f"{_escape_sheet_name(args.sheet_name)}!{args.read_range}"
    logger.info("Target Spreadsheet: %s", sid)
    logger.info("Reading Range     : %s", read_range)

    try:
        rows = sheets.read_range(sid, read_range)
        logger.info("✅ Read success. rows=%d", len(rows or []))
        if rows:
            logger.info("   Sample row[0]: %s", rows[0])
    except Exception as e:
        logger.error("❌ Read failed: %s", e)
        logger.info("   Tip: Share the spreadsheet with the service account email as Editor.")
        return 1

    # 4) Optional write test
    if args.write:
        logger.info("--- Step 4: Checking WRITE access ---")
        write_range = f"{_escape_sheet_name(args.sheet_name)}!{args.write_cell}"
        value = [[f"Auth Test: {_utc_now()}"]]

        if args.dry_run:
            logger.info("DRY-RUN enabled: would write to %s value=%s", write_range, value[0][0])
        else:
            try:
                updated = sheets.write_range(sid, write_range, value, value_input="RAW")
                if int(updated or 0) > 0:
                    logger.info("✅ Write success. updatedCells=%s at %s", updated, write_range)
                else:
                    logger.warning("⚠️ Write executed but updatedCells=0 (check permissions/tab).")
            except Exception as e:
                logger.error("❌ Write failed: %s", e)
                logger.info("   Tip: Service account needs Editor role on the spreadsheet.")
                return 1
    else:
        logger.info("--- Step 4: Write test skipped (use --write to enable) ---")

    logger.info("✨ All checks completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
