#!/usr/bin/env python3
# scripts/test_sheets_auth.py  (FULL REPLACEMENT)
"""
test_sheets_auth.py
===========================================================
Google Sheets Auth & Permission Tester (v1.4.0) – TFB PROD SAFE
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
  python scripts/test_sheets_auth.py --print-service-email

Notes
- Tolerates both module locations:
    - google_sheets_service.py (project root)
    - integrations/google_sheets_service.py
- Spreadsheet ID resolution:
    --sheet-id -> settings.default_spreadsheet_id -> env DEFAULT_SPREADSHEET_ID family
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_VERSION = "1.4.0"

# -----------------------------------------------------------------------------
# Path safety
# -----------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT and PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("SheetsAuthTest")

# -----------------------------------------------------------------------------
# Try both module locations
# -----------------------------------------------------------------------------
sheets = None  # type: ignore
_import_err: Optional[str] = None

try:
    import google_sheets_service as sheets  # type: ignore
except Exception as e1:
    try:
        from integrations import google_sheets_service as sheets  # type: ignore
    except Exception as e2:
        _import_err = f"root err={e1} | integrations err={e2}"

if sheets is None:
    print(f"❌ Critical: Could not import google_sheets_service (root or integrations). {_import_err}")
    raise SystemExit(1)

# Optional env settings
try:
    from env import settings  # type: ignore
except Exception:
    settings = None  # type: ignore


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get_spreadsheet_id(cli: Optional[str]) -> str:
    if cli and str(cli).strip():
        return str(cli).strip()

    # settings.default_spreadsheet_id preferred
    try:
        if settings is not None:
            sid = (getattr(settings, "default_spreadsheet_id", None) or "").strip()
            if sid:
                return sid
    except Exception:
        pass

    # env fallbacks
    for k in ("DEFAULT_SPREADSHEET_ID", "TFB_SPREADSHEET_ID", "SPREADSHEET_ID", "GOOGLE_SHEETS_ID"):
        v = (os.getenv(k, "") or "").strip()
        if v:
            return v
    return ""


def _escape_sheet_name(name: str) -> str:
    n = (name or "").strip().replace("'", "''")
    return f"'{n}'"


def _load_creds_raw() -> str:
    return (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "").strip()


def _parse_creds(raw: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
    try:
        obj = json.loads(raw)
    except Exception:
        return False, None
    if not isinstance(obj, dict):
        return False, None
    return True, obj


def _validate_service_account_json(obj: Dict[str, Any]) -> bool:
    return bool(obj.get("client_email")) and bool(obj.get("private_key"))


def _safe_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except TypeError:
        # Some functions may not accept extra kwargs
        return fn(*args)


def _read_range(sid: str, a1_range: str) -> List[List[Any]]:
    fn = getattr(sheets, "read_range", None)
    if callable(fn):
        return fn(sid, a1_range)  # type: ignore

    fn2 = getattr(sheets, "get_values", None)
    if callable(fn2):
        return fn2(sid, a1_range)  # type: ignore

    raise RuntimeError("google_sheets_service has no read_range/get_values function.")


def _write_range(sid: str, a1_start: str, values: List[List[Any]], *, value_input: str = "RAW") -> int:
    vi = (value_input or "RAW").strip().upper()
    if vi not in ("RAW", "USER_ENTERED"):
        vi = "RAW"

    fn = getattr(sheets, "write_range", None)
    if callable(fn):
        # some implementations accept value_input, others don't
        try:
            updated = fn(sid, a1_start, values, value_input=vi)  # type: ignore
        except TypeError:
            updated = fn(sid, a1_start, values)  # type: ignore
        try:
            return int(updated or 0)
        except Exception:
            return 0

    fn2 = getattr(sheets, "update_values", None)
    if callable(fn2):
        updated = fn2(sid, a1_start, values, value_input=vi)  # type: ignore
        try:
            return int(updated or 0)
        except Exception:
            return 0

    raise RuntimeError("google_sheets_service has no write_range/update_values function.")


def _get_service_account_email(creds_obj: Optional[Dict[str, Any]]) -> str:
    if isinstance(creds_obj, dict):
        return str(creds_obj.get("client_email") or "").strip()
    return ""


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> int:
    p = argparse.ArgumentParser(description="Test Google Service Account permissions for Google Sheets")
    p.add_argument("--sheet-id", help="Override Spreadsheet ID (default: settings/env DEFAULT_SPREADSHEET_ID family)")
    p.add_argument("--sheet-name", default="Sheet1", help="Tab name to read/write (default: Sheet1)")
    p.add_argument("--read-range", default="A1:B2", help="A1 range to read (default: A1:B2)")
    p.add_argument("--write", action="store_true", help="Attempt to WRITE a timestamp to the sheet")
    p.add_argument("--write-cell", default="A1", help="Cell to write to if --write is enabled (default: A1)")
    p.add_argument("--dry-run", action="store_true", help="Do not write even if --write is set")
    p.add_argument("--print-service-email", action="store_true", help="Print the service account email (client_email) and exit 0")
    p.add_argument("--value-input", default="RAW", choices=["RAW", "USER_ENTERED", "raw", "user_entered"], help="Write mode")
    args = p.parse_args()

    # 1) Check credentials existence + basic JSON validity
    logger.info("--- Step 1: Checking GOOGLE_SHEETS_CREDENTIALS ---")
    creds_raw = _load_creds_raw()
    if not creds_raw:
        logger.error("❌ GOOGLE_SHEETS_CREDENTIALS is missing/empty.")
        logger.info("   Fix: Add GOOGLE_SHEETS_CREDENTIALS in GitHub Secrets / Render Env Vars.")
        return 1

    ok_json, creds_obj = _parse_creds(creds_raw)
    if not ok_json or not isinstance(creds_obj, dict):
        logger.error("❌ GOOGLE_SHEETS_CREDENTIALS is present but not valid JSON.")
        return 1

    if not _validate_service_account_json(creds_obj):
        logger.error("❌ GOOGLE_SHEETS_CREDENTIALS JSON missing client_email/private_key (not a valid service account JSON).")
        logger.info("   Tip: It must include client_email and private_key.")
        return 1

    svc_email = _get_service_account_email(creds_obj)
    logger.info("✅ Credentials JSON looks valid. service_account=%s", svc_email or "(unknown)")

    if args.print_service_email:
        print(svc_email or "")
        return 0

    # 2) Init service
    logger.info("--- Step 2: Initializing Sheets service ---")
    try:
        fn = getattr(sheets, "get_sheets_service", None)
        if callable(fn):
            service = fn()  # type: ignore
            if not service:
                logger.error("❌ get_sheets_service() returned None.")
                return 1
        else:
            # some repos initialize lazily; that's OK
            logger.info("ℹ️ get_sheets_service() not found; continuing with module-level helpers.")
        logger.info("✅ Sheets service initialized (or not required).")
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
        rows = _read_range(sid, read_range)
        logger.info("✅ Read success. rows=%d", len(rows or []))
        if rows:
            logger.info("   Sample row[0]: %s", rows[0])
    except Exception as e:
        logger.error("❌ Read failed: %s", e)
        logger.info("   Tip: Share the spreadsheet with the service account email as Editor:")
        logger.info("        %s", svc_email or "(service email unknown)")
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
                updated = _write_range(sid, write_range, value, value_input=str(args.value_input))
                if int(updated or 0) > 0:
                    logger.info("✅ Write success. updatedCells=%s at %s", int(updated or 0), write_range)
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
