#!/usr/bin/env python3
# scripts/test_sheets_auth.py  (FULL REPLACEMENT)
"""
test_sheets_auth.py
===========================================================
Google Sheets Auth & Permission Tester (v1.4.0) – TFB PROD SAFE
===========================================================

Purpose
- Verify GOOGLE_SHEETS_CREDENTIALS is present and valid JSON (service account).
- Initialize Sheets client via project google_sheets_service (root or integrations/).
- Verify the Service Account can READ the target spreadsheet.
- Optional: Verify WRITE permission by writing a timestamp to a specific cell.

Key upgrades (v1.4.0)
- ✅ Stronger spreadsheet ID resolution (settings.default_spreadsheet_id + more env fallbacks)
- ✅ Supports both services:
    - google_sheets_service.py (project root)
    - integrations/google_sheets_service.py
- ✅ Signature-aware write_range (value_input optional)
- ✅ Clear, actionable diagnostics (permission vs range vs tab missing)
- ✅ Safe path handling (run from root or scripts/)

Usage
  python scripts/test_sheets_auth.py
  python scripts/test_sheets_auth.py --sheet-id <ID>
  python scripts/test_sheets_auth.py --sheet-name "Market_Leaders" --read-range A5:C5
  python scripts/test_sheets_auth.py --write --write-cell A1
  python scripts/test_sheets_auth.py --dry-run

Notes
- Spreadsheet ID is resolved from:
    --sheet-id -> env.settings.default_spreadsheet_id -> DEFAULT_SPREADSHEET_ID -> TFB_SPREADSHEET_ID -> SPREADSHEET_ID -> GOOGLE_SHEETS_ID
- Token is not needed (service account auth).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

# =============================================================================
# Path safety (run from root or scripts/)
# =============================================================================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT and PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# =============================================================================
# Logging
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("SheetsAuthTest")

# =============================================================================
# Try both module locations
# =============================================================================
sheets = None
_import_err: Optional[str] = None

try:
    import google_sheets_service as sheets  # type: ignore
except Exception as e1:
    try:
        from integrations import google_sheets_service as sheets  # type: ignore
    except Exception as e2:
        _import_err = f"root import err={e1} | integrations import err={e2}"

if sheets is None:
    print(f"❌ Critical: Could not import google_sheets_service (root or integrations). {_import_err}")
    sys.exit(1)

# Optional env settings
try:
    from env import settings  # type: ignore
except Exception:
    settings = None  # type: ignore


# =============================================================================
# Helpers
# =============================================================================
def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _escape_sheet_name(name: str) -> str:
    n = (name or "").strip().replace("'", "''")
    return f"'{n}'"


def _validate_creds_json(raw: str) -> Tuple[bool, str]:
    """
    Returns (ok, reason).
    Keep it lightweight and safe.
    """
    try:
        obj = json.loads(raw)
    except Exception as e:
        return False, f"JSON parse error: {e}"

    if not isinstance(obj, dict):
        return False, "Credentials JSON is not an object"

    # lightweight checks
    if not obj.get("client_email"):
        return False, "Missing client_email"
    if not obj.get("private_key"):
        return False, "Missing private_key"
    if not obj.get("type"):
        # service account JSON usually has type=service_account, but some exports omit it
        return True, "OK (type missing but tolerated)"

    return True, "OK"


def _get_spreadsheet_id(cli: Optional[str]) -> str:
    if cli and str(cli).strip():
        return str(cli).strip()

    # env.py settings preferred
    if settings is not None:
        # most of your repo uses default_spreadsheet_id (lowercase)
        sid = (getattr(settings, "default_spreadsheet_id", "") or "").strip()
        if sid:
            return sid
        # tolerate older constant-ish naming
        sid2 = (getattr(settings, "DEFAULT_SPREADSHEET_ID", "") or "").strip()
        if sid2:
            return sid2

    # env fallbacks
    for k in ("DEFAULT_SPREADSHEET_ID", "TFB_SPREADSHEET_ID", "SPREADSHEET_ID", "GOOGLE_SHEETS_ID"):
        v = (os.getenv(k, "") or "").strip()
        if v:
            return v

    return ""


def _get_creds_env() -> str:
    # allow a couple of env var names
    return (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "").strip()


def _call_get_service() -> Any:
    """
    Support multiple google_sheets_service implementations.
    """
    fn = getattr(sheets, "get_sheets_service", None)
    if callable(fn):
        return fn()
    # some implementations expose build_service() or similar
    for name in ("build_service", "get_service"):
        fn2 = getattr(sheets, name, None)
        if callable(fn2):
            return fn2()
    raise RuntimeError("google_sheets_service has no get_sheets_service/build_service/get_service function")


def _read_range(sid: str, a1_range: str) -> Any:
    fn = getattr(sheets, "read_range", None)
    if callable(fn):
        return fn(sid, a1_range)
    # tolerate alternative names
    for name in ("read_values", "get_values"):
        fn2 = getattr(sheets, name, None)
        if callable(fn2):
            return fn2(sid, a1_range)
    raise RuntimeError("google_sheets_service has no read_range/read_values/get_values")


def _write_range(sid: str, a1_range: str, values: Any, *, value_input: str = "RAW") -> Any:
    """
    Signature-aware wrapper:
    - write_range(sid, a1, values, value_input=?)
    - write_range(sid, a1, values)
    """
    fn = getattr(sheets, "write_range", None)
    if not callable(fn):
        # tolerate alternative names
        for name in ("write_values", "update_values", "set_values"):
            fn2 = getattr(sheets, name, None)
            if callable(fn2):
                fn = fn2
                break

    if not callable(fn):
        raise RuntimeError("google_sheets_service has no write_range/write_values/update_values/set_values")

    vi = (value_input or "RAW").strip().upper()
    if vi not in ("RAW", "USER_ENTERED"):
        vi = "RAW"

    try:
        import inspect

        sig = inspect.signature(fn)
        if "value_input" in sig.parameters:
            return fn(sid, a1_range, values, value_input=vi)  # type: ignore
    except Exception:
        pass

    return fn(sid, a1_range, values)  # type: ignore


def _hint_from_exception(e: Exception) -> str:
    msg = str(e or "")
    m = msg.lower()

    if "requested entity was not found" in m or "not found" in m:
        return "Hint: Spreadsheet ID or tab name is wrong, or the service account lacks access."
    if "unable to parse range" in m:
        return "Hint: Range is invalid OR the tab name does not exist."
    if "permission" in m or "insufficient" in m or "forbidden" in m or "403" in m:
        return "Hint: Share the spreadsheet with the service account email (Editor recommended)."
    if "invalid_grant" in m or "unauthorized" in m:
        return "Hint: Credentials JSON is invalid or revoked; re-check GOOGLE_SHEETS_CREDENTIALS."
    return "Hint: Check spreadsheet sharing, tab name, and service account JSON."


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    p = argparse.ArgumentParser(description="Test Google Service Account permissions for Google Sheets")
    p.add_argument("--sheet-id", help="Override Spreadsheet ID (default: env DEFAULT_SPREADSHEET_ID)")
    p.add_argument("--sheet-name", default="Sheet1", help="Tab name to read/write (default: Sheet1)")
    p.add_argument("--read-range", default="A1:B2", help="A1 range to read (default: A1:B2)")
    p.add_argument("--write", action="store_true", help="Attempt to WRITE a timestamp to the sheet")
    p.add_argument("--write-cell", default="A1", help="Cell to write to if --write is enabled (default: A1)")
    p.add_argument("--value-input", default="RAW", choices=["RAW", "USER_ENTERED", "raw", "user_entered"], help="Write mode")
    p.add_argument("--dry-run", action="store_true", help="Do not write even if --write is set")
    args = p.parse_args()

    # 1) Check credentials existence + basic JSON validity
    logger.info("--- Step 1: Checking GOOGLE_SHEETS_CREDENTIALS ---")
    creds_raw = _get_creds_env()
    if not creds_raw:
        logger.error("❌ GOOGLE_SHEETS_CREDENTIALS is missing/empty.")
        logger.info("   Fix: Add GOOGLE_SHEETS_CREDENTIALS in GitHub Secrets / Render Env Vars.")
        return 1

    ok, reason = _validate_creds_json(creds_raw)
    if not ok:
        logger.error("❌ GOOGLE_SHEETS_CREDENTIALS is present but invalid: %s", reason)
        logger.info("   Tip: It must include client_email and private_key.")
        return 1

    # show service account email (safe)
    try:
        sa_email = (json.loads(creds_raw) or {}).get("client_email")  # type: ignore
    except Exception:
        sa_email = None

    logger.info("✅ Credentials JSON looks valid. %s", reason)
    if sa_email:
        logger.info("Service Account: %s", sa_email)

    # 2) Init service
    logger.info("--- Step 2: Initializing Sheets service ---")
    try:
        service = _call_get_service()
        if not service:
            logger.error("❌ Sheets service init returned None.")
            return 1
        logger.info("✅ Sheets service initialized.")
    except Exception as e:
        logger.error("❌ Failed to initialize Sheets service: %s", e)
        logger.info(_hint_from_exception(e))
        return 1

    # 3) Read test
    logger.info("--- Step 3: Checking READ access ---")
    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("❌ Spreadsheet ID missing. Use --sheet-id or set DEFAULT_SPREADSHEET_ID.")
        return 1

    sheet_name = str(args.sheet_name or "Sheet1").strip()
    read_range = f"{_escape_sheet_name(sheet_name)}!{str(args.read_range or 'A1:B2').strip()}"

    logger.info("Target Spreadsheet: %s", sid)
    logger.info("Reading Range     : %s", read_range)

    try:
        rows = _read_range(sid, read_range)
        rows_len = len(rows or []) if isinstance(rows, list) else 0
        logger.info("✅ Read success. rows=%d", rows_len)
        if isinstance(rows, list) and rows:
            logger.info("   Sample row[0]: %s", rows[0])
    except Exception as e:
        logger.error("❌ Read failed: %s", e)
        logger.info(_hint_from_exception(e))
        logger.info("   Tip: Share the spreadsheet with the service account email as Editor.")
        return 1

    # 4) Optional write test
    if args.write:
        logger.info("--- Step 4: Checking WRITE access ---")
        write_cell = str(args.write_cell or "A1").strip()
        write_range = f"{_escape_sheet_name(sheet_name)}!{write_cell}"
        value = [[f"Auth Test: {_utc_now()}"]]

        vi = str(args.value_input or "RAW").strip().upper()
        if vi not in ("RAW", "USER_ENTERED"):
            vi = "RAW"

        if args.dry_run:
            logger.info("DRY-RUN enabled: would write to %s value=%s (value_input=%s)", write_range, value[0][0], vi)
        else:
            try:
                updated = _write_range(sid, write_range, value, value_input=vi)
                try:
                    updated_i = int(updated or 0)
                except Exception:
                    updated_i = 0

                if updated_i > 0:
                    logger.info("✅ Write success. updatedCells=%s at %s", updated_i, write_range)
                else:
                    logger.warning("⚠️ Write executed but updatedCells=0 (check permissions/tab/range).")
            except Exception as e:
                logger.error("❌ Write failed: %s", e)
                logger.info(_hint_from_exception(e))
                logger.info("   Tip: Service account needs Editor role on the spreadsheet.")
                return 1
    else:
        logger.info("--- Step 4: Write test skipped (use --write to enable) ---")

    logger.info("✨ All checks completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
