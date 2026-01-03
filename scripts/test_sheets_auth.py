#!/usr/bin/env python3
# scripts/test_sheets_auth.py
"""
test_sheets_auth.py
===========================================================
Google Sheets Auth & Permission Tester (v1.1.0)
===========================================================

Purpose
- Verify GOOGLE_SHEETS_CREDENTIALS are valid and readable.
- Verify the Service Account has access to DEFAULT_SPREADSHEET_ID.
- Optional: Test WRITE permissions (by updating a specific cell).

Usage
  python scripts/test_sheets_auth.py
  python scripts/test_sheets_auth.py --sheet-id <ID> --write --cell A1

Dependencies
- integrations.google_sheets_service
- env.py (for config loading)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

# Ensure project root is in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from integrations import google_sheets_service as sheets
except ImportError:
    print("❌ Critical: Could not import 'integrations.google_sheets_service'. Run from project root.")
    sys.exit(1)

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("SheetsAuthTest")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    p = argparse.ArgumentParser(description="Test Google Service Account Permissions")
    p.add_argument("--sheet-id", help="Override Spreadsheet ID (default: env DEFAULT_SPREADSHEET_ID)")
    p.add_argument("--read-range", default="A1:B2", help="Range to read (default: A1:B2)")
    p.add_argument("--write", action="store_true", help="Attempt to WRITE a timestamp to the sheet")
    p.add_argument("--write-cell", default="A1", help="Cell to write to if --write is enabled (default: A1)")
    p.add_argument("--sheet-name", default="Sheet1", help="Sheet/Tab name (default: Sheet1)")
    
    args = p.parse_args()

    # 1. Check Credentials Availability
    logger.info("--- Step 1: Checking Credentials ---")
    creds_raw = os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS")
    if not creds_raw:
        logger.error("❌ GOOGLE_SHEETS_CREDENTIALS env var is MISSING or empty.")
        return 1
    
    try:
        service = sheets.get_sheets_service()
        if not service:
            logger.error("❌ get_sheets_service() returned None. Credentials might be invalid JSON.")
            return 1
        logger.info("✅ Service Account initialized successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Google Service: {e}")
        return 1

    # 2. Check Spreadsheet Access (Read)
    logger.info("--- Step 2: Checking Read Access ---")
    sid = args.sheet_id or os.getenv("DEFAULT_SPREADSHEET_ID")
    if not sid:
        logger.error("❌ No Spreadsheet ID found. Set DEFAULT_SPREADSHEET_ID or use --sheet-id.")
        return 1
    
    range_name = f"'{args.sheet_name}'!{args.read_range}"
    logger.info(f"Target Spreadsheet: {sid}")
    logger.info(f"Target Range: {range_name}")

    try:
        rows = sheets.read_range(sid, range_name)
        logger.info(f"✅ Read Success! Retrieved {len(rows)} rows.")
        if rows:
            logger.info(f"   Sample data: {rows[0]}")
    except Exception as e:
        logger.error(f"❌ Read Failed: {e}")
        logger.info("   Tip: Ensure the Service Account email is shared with the Sheet as 'Editor'.")
        return 1

    # 3. Check Write Access (Optional)
    if args.write:
        logger.info("--- Step 3: Checking Write Access ---")
        write_range = f"'{args.sheet_name}'!{args.write_cell}"
        test_val = [[f"Auth Test: {_utc_now()}"]]
        
        try:
            updated = sheets.write_range(sid, write_range, test_val)
            if updated > 0:
                logger.info(f"✅ Write Success! Updated {updated} cells at {write_range}.")
            else:
                logger.warning("⚠️ Write command ran but reported 0 updated cells.")
        except Exception as e:
            logger.error(f"❌ Write Failed: {e}")
            logger.info("   Tip: Service Account needs 'Editor' role on the Spreadsheet.")
            return 1
    else:
        logger.info("--- Step 3: Write Test Skipped (use --write to enable) ---")

    logger.info("\n✨ All checks passed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
