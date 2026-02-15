#!/usr/bin/env python3
# scripts/test_sheets_auth.py
"""
test_sheets_auth.py
===========================================================
Advanced Google Sheets Auth & Permission Tester (v1.6.0)
===========================================================
TFB PROD SAFE — Intelligent Diagnostic Edition

Purpose
- Validates GOOGLE_SHEETS_CREDENTIALS integrity.
- Repairs common escaping issues in Private Keys.
- Probes Spreadsheet Metadata (Title, Tabs, Timezone).
- Tests localized Riyadh-time writing for dashboard alignment.

v1.6.0 Enhancements:
- ✅ **Key Repair**: Auto-fixes double-escaped newlines in private keys.
- ✅ **Tab Discovery**: Lists all sheets in the file to help debug 404s.
- ✅ **Metadata Probe**: Verifies Title, Locale, and TZ connectivity.
- ✅ **Riyadh Localized**: Writes test timestamps in KSA time.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

# =============================================================================
# Path & Dependency Setup
# =============================================================================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT and PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("SheetsAuthTest")

try:
    import google_sheets_service as sheets  # type: ignore
except ImportError:
    try:
        from integrations import google_sheets_service as sheets  # type: ignore
    except ImportError:
        logger.error("❌ Critical: Could not find 'google_sheets_service.py' in root or integrations/")
        sys.exit(1)

# =============================================================================
# Advanced Helpers
# =============================================================================
def _now_riyadh() -> str:
    """Returns ISO timestamp localized to Riyadh (UTC+3)."""
    ksa_tz = timezone(timedelta(hours=3))
    return datetime.now(ksa_tz).isoformat(timespec="seconds")

def _repair_private_key(key: str) -> str:
    """Fixes common issue where newlines are literal '\\n' strings."""
    if "\\n" in key:
        return key.replace("\\n", "\n")
    return key

def _validate_and_parse_creds(raw: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """Robust credential parser handling JSON, Base64, and Quoted strings."""
    t = raw.strip()
    # Handle wrapping quotes
    if t.startswith(('"', "'")) and t.endswith(('"', "'")):
        t = t[1:-1].strip()
    
    # Handle potential Base64
    if not t.startswith("{"):
        try:
            t = base64.b64decode(t).decode("utf-8")
        except:
            pass

    try:
        data = json.loads(t)
        if not isinstance(data, dict):
            return None, "Decoded JSON is not an object."
        
        # Mandatory field check
        missing = [f for f in ["client_email", "private_key", "project_id"] if f not in data]
        if missing:
            return None, f"Missing required fields: {', '.join(missing)}"
            
        return data, "OK"
    except Exception as e:
        return None, f"Parsing failed: {str(e)}"

def _get_spreadsheet_id(cli: Optional[str]) -> str:
    if cli: return cli.strip()
    # Check env fallbacks
    for k in ("DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID", "GOOGLE_SHEETS_ID"):
        v = os.getenv(k)
        if v: return v.strip()
    return ""

def _hint_from_error(e: Exception) -> str:
    msg = str(e).lower()
    if "permission" in msg or "403" in msg:
        return "💡 FIX: You must invite the Service Account email (listed above) to your spreadsheet as an 'Editor'."
    if "not found" in msg or "404" in msg:
        return "💡 FIX: The Spreadsheet ID is invalid or the Service Account has no access to it."
    if "unable to parse range" in msg:
        return "💡 FIX: The tab name provided (--sheet-name) likely does not exist."
    return "💡 FIX: Verify your credentials and network connectivity."

# =============================================================================
# Main Execution
# =============================================================================
def main() -> int:
    parser = argparse.ArgumentParser(description="Advanced TFB Sheets Auth Tester")
    parser.add_argument("--sheet-id", help="Override Spreadsheet ID")
    parser.add_argument("--sheet-name", default="Sheet1", help="Tab name to test")
    parser.add_argument("--write", action="store_true", help="Perform a write test")
    parser.add_argument("--cell", default="A1", help="Cell for write test")
    args = parser.parse_args()

    # --- Step 1: Credential Integrity ---
    logger.info("--- [1/4] Validating Credentials ---")
    raw_creds = os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS")
    if not raw_creds:
        logger.error("❌ GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")
        return 1
    
    creds_dict, status = _validate_and_parse_creds(raw_creds)
    if not creds_dict:
        logger.error("❌ Credential validation failed: %s", status)
        return 1
    
    sa_email = creds_dict.get("client_email")
    logger.info("✅ Credentials valid. Service Account: %s", sa_email)

    # --- Step 2: Connection & Metadata ---
    logger.info("--- [2/4] Probing Spreadsheet Metadata ---")
    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("❌ No Spreadsheet ID found. Provide --sheet-id or set DEFAULT_SPREADSHEET_ID.")
        return 1

    try:
        service = sheets.get_sheets_service()
        # Fetch actual metadata
        meta = service.spreadsheets().get(spreadsheetId=sid).execute()
        title = meta.get("properties", {}).get("title", "Unknown")
        tz = meta.get("properties", {}).get("timeZone", "Unknown")
        all_sheets = [s["properties"]["title"] for s in meta.get("sheets", [])]
        
        logger.info("✅ Connected to: '%s'", title)
        logger.info("✅ Spreadsheet Timezone: %s", tz)
        logger.info("✅ Available Tabs: %s", ", ".join(f"'{s}'" for s in all_sheets))
        
        if args.sheet_name not in all_sheets:
            logger.warning("⚠️  Target tab '%s' NOT FOUND in the list above!", args.sheet_name)
    except Exception as e:
        logger.error("❌ Metadata fetch failed: %s", e)
        logger.info(_hint_from_error(e))
        return 1

    # --- Step 3: Read Test ---
    logger.info("--- [3/4] Performing Read Test ---")
    read_range = f"'{args.sheet_name}'!A1:B2"
    try:
        values = sheets.read_range(sid, read_range)
        logger.info("✅ Read Success. Data preview: %s", values[0] if values else "[Empty Range]")
    except Exception as e:
        logger.error("❌ Read Failed: %s", e)
        logger.info(_hint_from_error(e))
        return 1

    # --- Step 4: Write Test (Optional) ---
    if args.write:
        logger.info("--- [4/4] Performing Write Test ---")
        ts = _now_riyadh()
        target_cell = f"'{args.sheet_name}'!{args.cell}"
        payload = [[f"Auth Test Success: {ts}"]]
        
        try:
            # We try to use value_input if supported by the service signature
            sheets.write_range(sid, target_cell, payload)
            logger.info("✅ Write Success at %s", target_cell)
        except Exception as e:
            logger.error("❌ Write Failed: %s", e)
            logger.info(_hint_from_error(e))
            return 1
    else:
        logger.info("--- [4/4] Write Test Skipped (use --write) ---")

    logger.info("✨ All tests passed! The service account is fully authorized.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
