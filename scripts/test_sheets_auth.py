#!/usr/bin/env python3
# scripts/test_sheets_auth.py
"""
test_sheets_auth.py
===========================================================
Advanced Google Sheets Auth & Permission Tester (v2.2.0)
===========================================================
TFB PROD SAFE — Deep Diagnostic Edition

Purpose
- Validates GOOGLE_SHEETS_CREDENTIALS integrity (JSON / Base64 JSON / File path).
- Repairs common escaping issues in Private Keys (\\n, \\r\\n, surrounding quotes).
- Probes Spreadsheet Metadata (Title, Tabs, Locale, Timezone, Owners if visible).
- Tests Read + optional Write with Riyadh-time stamp.
- Produces CI-friendly exit codes and actionable hints.

Exit codes
- 0: OK
- 1: Configuration / Credential error
- 2: API access error (403/404/permissions)
- 3: Read/Write test failed (range/tab issues, API errors)

Usage
  python scripts/test_sheets_auth.py --sheet-id <ID> --sheet-name "Market_Leaders"
  python scripts/test_sheets_auth.py --write --cell A1
  python scripts/test_sheets_auth.py --list-tabs
  python scripts/test_sheets_auth.py --diagnose-env
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

# Try to import your project's wrapper first, then fallback to googleapiclient
sheets = None
_service_factory = None

try:
    import google_sheets_service as _sheets  # type: ignore
    sheets = _sheets
    _service_factory = getattr(sheets, "get_sheets_service", None)
except Exception:
    try:
        from integrations import google_sheets_service as _sheets  # type: ignore
        sheets = _sheets
        _service_factory = getattr(sheets, "get_sheets_service", None)
    except Exception:
        sheets = None
        _service_factory = None


# =============================================================================
# Constants / Version
# =============================================================================
SCRIPT_VERSION = "2.2.0"
DEFAULT_READ_RANGE = "A1:B2"

ENV_CRED_KEYS = (
    "GOOGLE_SHEETS_CREDENTIALS",
    "GOOGLE_CREDENTIALS",
    "GOOGLE_APPLICATION_CREDENTIALS",  # may contain file path
)

ENV_SHEET_ID_KEYS = (
    "DEFAULT_SPREADSHEET_ID",
    "TFB_SPREADSHEET_ID",
    "SPREADSHEET_ID",
    "GOOGLE_SHEETS_ID",
)

_TRUTHY = {"1", "true", "yes", "y", "on"}


# =============================================================================
# Time Helpers
# =============================================================================
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _now_riyadh_iso() -> str:
    ksa_tz = timezone(timedelta(hours=3))
    return datetime.now(ksa_tz).isoformat(timespec="seconds")


# =============================================================================
# Credential Parsing / Repair
# =============================================================================
def _strip_wrapping_quotes(s: str) -> str:
    t = (s or "").strip()
    if len(t) >= 2 and ((t[0] == t[-1]) and t[0] in ("'", '"')):
        return t[1:-1].strip()
    return t


def _repair_private_key(key: str) -> str:
    """
    Fix common env-serialization issues:
    - literal \\n instead of newlines
    - literal \\r\\n
    - extra surrounding quotes
    """
    if not key:
        return key
    k = _strip_wrapping_quotes(key)
    # Normalize Windows style escapes too
    k = k.replace("\\r\\n", "\n").replace("\\n", "\n")
    return k


def _maybe_base64_decode(s: str) -> Optional[str]:
    """
    Attempt base64 decode. Returns decoded string or None if not decodable.
    """
    t = (s or "").strip()
    if not t:
        return None
    # Quick heuristic: base64 usually doesn't start with '{'
    if t.startswith("{"):
        return None
    try:
        decoded = base64.b64decode(t).decode("utf-8")
        if decoded.strip().startswith("{"):
            return decoded
    except Exception:
        return None
    return None


def _load_creds_from_file(path: str) -> Tuple[Optional[Dict[str, Any]], str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            txt = f.read()
        return _parse_creds_text(txt)
    except Exception as e:
        return None, f"Failed to read credentials file: {e}"


def _parse_creds_text(raw: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Robust credential parser:
    - JSON object
    - base64(JSON)
    - wrapped in quotes
    """
    if not raw:
        return None, "Empty credential payload."

    t = _strip_wrapping_quotes(raw)

    b = _maybe_base64_decode(t)
    if b:
        t = b.strip()

    try:
        data = json.loads(t)
    except Exception as e:
        return None, f"Parsing failed: {e}"

    if not isinstance(data, dict):
        return None, "Decoded JSON is not an object."

    # Mandatory fields for service account JSON
    required = ["client_email", "private_key", "project_id"]
    missing = [k for k in required if not data.get(k)]
    if missing:
        return None, f"Missing required fields: {', '.join(missing)}"

    # Repair key in-memory (does not mutate environment)
    data["private_key"] = _repair_private_key(str(data.get("private_key") or ""))

    # Basic sanity check
    if "BEGIN PRIVATE KEY" not in data["private_key"]:
        return None, "private_key does not look like a valid PEM (missing 'BEGIN PRIVATE KEY')."

    return data, "OK"


def _read_raw_creds_env() -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (raw_value, env_key_used)
    """
    for k in ENV_CRED_KEYS:
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v), k
    return None, None


def _get_spreadsheet_id(cli: Optional[str]) -> str:
    if cli and cli.strip():
        return cli.strip()
    for k in ENV_SHEET_ID_KEYS:
        v = os.getenv(k)
        if v and v.strip():
            return v.strip()
    return ""


# =============================================================================
# Error Hints + Exit Codes
# =============================================================================
def _is_403(msg: str) -> bool:
    m = msg.lower()
    return "403" in m or "permission" in m or "insufficient permissions" in m


def _is_404(msg: str) -> bool:
    m = msg.lower()
    return "404" in m or "not found" in m


def _hint_from_error(e: Exception) -> str:
    msg = str(e).lower()

    if _is_403(msg):
        return (
            "💡 FIX: Share the spreadsheet with the Service Account email as **Editor**.\n"
            "   Also confirm the correct Spreadsheet ID and that you're editing the right file."
        )
    if _is_404(msg):
        return (
            "💡 FIX: Spreadsheet ID is wrong OR the Service Account has no access.\n"
            "   Verify the ID and that the spreadsheet exists in the correct Google account."
        )
    if "unable to parse range" in msg or "badrequest" in msg or "400" in msg:
        return "💡 FIX: Range or tab name is invalid. Use --list-tabs to confirm tab titles."
    if "ssl" in msg or "timed out" in msg or "timeout" in msg:
        return "💡 FIX: Network/egress issue. Confirm Render outbound connectivity and Google APIs access."
    return "💡 FIX: Verify credentials, env vars, and that googleapiclient dependencies are installed."


def _exit_code_for_exception(e: Exception) -> int:
    s = str(e)
    if _is_403(s) or _is_404(s):
        return 2
    return 3


# =============================================================================
# Sheets Operations (via your google_sheets_service wrapper if possible)
# =============================================================================
def _get_service() -> Any:
    if _service_factory and callable(_service_factory):
        return _service_factory()
    raise RuntimeError(
        "Could not import google_sheets_service.get_sheets_service(). "
        "Ensure google_sheets_service.py exists and dependencies are installed."
    )


def _read_range_fallback(service: Any, spreadsheet_id: str, a1: str) -> List[List[Any]]:
    # Direct Sheets API fallback for projects where wrapper lacks read_range
    resp = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=a1)
        .execute()
    )
    return resp.get("values") or []


def _write_range_fallback(service: Any, spreadsheet_id: str, a1: str, values: List[List[Any]]) -> None:
    body = {"values": values}
    (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=a1,
            valueInputOption="RAW",
            body=body,
        )
        .execute()
    )


def _read_range(spreadsheet_id: str, a1: str) -> List[List[Any]]:
    if sheets and hasattr(sheets, "read_range") and callable(getattr(sheets, "read_range")):
        return sheets.read_range(spreadsheet_id, a1)  # type: ignore
    service = _get_service()
    return _read_range_fallback(service, spreadsheet_id, a1)


def _write_range(spreadsheet_id: str, a1: str, values: List[List[Any]]) -> None:
    if sheets and hasattr(sheets, "write_range") and callable(getattr(sheets, "write_range")):
        sheets.write_range(spreadsheet_id, a1, values)  # type: ignore
        return
    service = _get_service()
    _write_range_fallback(service, spreadsheet_id, a1, values)


def _fetch_metadata(service: Any, spreadsheet_id: str) -> Dict[str, Any]:
    return service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()


def _safe_a1(sheet_name: str, a1: str) -> str:
    # Escape single quotes inside sheet name
    name = (sheet_name or "").replace("'", "''")
    return f"'{name}'!{a1}"


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser(description="TFB Advanced Google Sheets Auth Tester")
    ap.add_argument("--sheet-id", help="Override Spreadsheet ID")
    ap.add_argument("--sheet-name", default="Sheet1", help="Tab name to test")
    ap.add_argument("--read-range", default=DEFAULT_READ_RANGE, help="A1 range to read (default A1:B2)")
    ap.add_argument("--write", action="store_true", help="Perform a write test")
    ap.add_argument("--cell", default="A1", help="Cell for write test (e.g., A1)")
    ap.add_argument("--list-tabs", action="store_true", help="Only list available tabs and exit")
    ap.add_argument("--diagnose-env", action="store_true", help="Print which env keys are present (no secrets)")
    ap.add_argument("--quiet", action="store_true", help="Reduce output (still returns exit codes)")
    args = ap.parse_args()

    if args.quiet:
        logger.setLevel(logging.WARNING)

    logger.info("Starting Sheets Auth Test v%s | utc=%s | riyadh=%s", SCRIPT_VERSION, _now_utc_iso(), _now_riyadh_iso())

    # --- [0] Optional env diagnosis ---
    if args.diagnose_env:
        raw, key_used = _read_raw_creds_env()
        sid_env = _get_spreadsheet_id(None)
        logger.info("ENV: creds_present=%s (%s)", "yes" if raw else "no", key_used or "none")
        logger.info("ENV: sheet_id_present=%s", "yes" if sid_env else "no")
        # Do not print secrets
        for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "BACKEND_BASE_URL"):
            v = (os.getenv(k) or "").strip()
            logger.info("ENV: %s=%s", k, "[set]" if v else "[not set]")

    # --- [1] Credential integrity ---
    logger.info("--- [1/4] Validating Credentials ---")
    raw_creds, env_key = _read_raw_creds_env()
    if not raw_creds:
        logger.error("❌ No credentials env found. Set GOOGLE_SHEETS_CREDENTIALS (preferred) or GOOGLE_APPLICATION_CREDENTIALS.")
        return 1

    # If GOOGLE_APPLICATION_CREDENTIALS is used and looks like a file path, load file
    creds_dict: Optional[Dict[str, Any]] = None
    status = "Unknown"
    if env_key == "GOOGLE_APPLICATION_CREDENTIALS" and not raw_creds.strip().startswith("{"):
        # likely a path
        creds_dict, status = _load_creds_from_file(raw_creds.strip())
    else:
        creds_dict, status = _parse_creds_text(raw_creds)

    if not creds_dict:
        logger.error("❌ Credential validation failed: %s", status)
        return 1

    sa_email = str(creds_dict.get("client_email") or "")
    project_id = str(creds_dict.get("project_id") or "")
    logger.info("✅ Credentials valid. Service Account: %s | project_id=%s", sa_email, project_id)

    # --- [2] Spreadsheet metadata ---
    logger.info("--- [2/4] Probing Spreadsheet Metadata ---")
    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("❌ No Spreadsheet ID found. Provide --sheet-id or set DEFAULT_SPREADSHEET_ID.")
        return 1

    try:
        service = _get_service()
        meta = _fetch_metadata(service, sid)
        props = meta.get("properties", {}) if isinstance(meta, dict) else {}
        title = props.get("title", "Unknown")
        tz = props.get("timeZone", "Unknown")
        locale = props.get("locale", "Unknown")

        sheets_list = []
        for s in (meta.get("sheets") or []):
            p = (s or {}).get("properties") or {}
            t = p.get("title")
            if t:
                sheets_list.append(str(t))

        logger.info("✅ Connected to Spreadsheet: '%s'", title)
        logger.info("✅ Spreadsheet Locale: %s | Timezone: %s", locale, tz)
        logger.info("✅ Tabs (%d): %s", len(sheets_list), ", ".join(f"'{t}'" for t in sheets_list))

        if args.list_tabs:
            logger.info("Done (--list-tabs).")
            return 0

        if args.sheet_name not in sheets_list:
            logger.warning("⚠️  Target tab '%s' not found. Read/Write may fail.", args.sheet_name)
    except Exception as e:
        logger.error("❌ Metadata fetch failed: %s", e)
        logger.info(_hint_from_error(e))
        return _exit_code_for_exception(e)

    # --- [3] Read test ---
    logger.info("--- [3/4] Performing Read Test ---")
    read_a1 = _safe_a1(args.sheet_name, args.read_range)
    try:
        values = _read_range(sid, read_a1)
        if values:
            logger.info("✅ Read Success. Preview: %s", values[0])
        else:
            logger.info("✅ Read Success. Range is empty: %s", read_a1)
    except Exception as e:
        logger.error("❌ Read Failed: %s", e)
        logger.info(_hint_from_error(e))
        return _exit_code_for_exception(e)

    # --- [4] Optional write test ---
    if args.write:
        logger.info("--- [4/4] Performing Write Test ---")
        ts = _now_riyadh_iso()
        target_a1 = _safe_a1(args.sheet_name, args.cell)
        payload = [[f"TFB Sheets Auth OK ✅ | Riyadh={ts} | UTC={_now_utc_iso()}"]]
        try:
            _write_range(sid, target_a1, payload)
            logger.info("✅ Write Success at %s", target_a1)
        except Exception as e:
            logger.error("❌ Write Failed: %s", e)
            logger.info(_hint_from_error(e))
            return _exit_code_for_exception(e)
    else:
        logger.info("--- [4/4] Write Test Skipped (use --write) ---")

    logger.info("✨ All tests passed! Service account is authorized and API is working.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
