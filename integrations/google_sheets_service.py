#!/usr/bin/env python3
"""
integrations/setup_credentials.py
===========================================================
TADAWUL FAST BRIDGE — GOOGLE CREDENTIALS SETUP (v2.3.0)
(Emad Bahbah – Enterprise Integration Architecture)

Purpose
- Load Google Service Account credentials from multiple sources (env/file/settings)
- Normalize the private key format safely (fix "\\n" newlines, add PEM markers if missing)
- Validate required fields (never prints the private key)
- Optionally write credentials JSON to a file (for GOOGLE_APPLICATION_CREDENTIALS workflows)
- Optionally run a lightweight Sheets API connectivity test (if libraries installed)

Designed for Render / CI
- Non-interactive by default
- Uses environment variables first
- Safe, deterministic, and explicit exit codes

Supported Inputs (highest precedence first)
1) --input-file PATH                     (JSON file)
2) --input-json STRING                   (raw JSON string)
3) --input-b64 STRING                    (base64 JSON)
4) ENV: GOOGLE_SHEETS_CREDENTIALS        (JSON)
5) ENV: GOOGLE_CREDENTIALS               (JSON)
6) ENV: GOOGLE_SHEETS_CREDENTIALS_B64    (base64 JSON)
7) ENV: GOOGLE_CREDENTIALS_B64           (base64 JSON)
8) ENV: GOOGLE_APPLICATION_CREDENTIALS   (JSON file path)
9) core.config.get_settings() (optional) attributes:
   - google_sheets_credentials_json
   - google_credentials_dict

Outputs
- Writes file (optional): default /tmp/google_sa_credentials.json
- Prints masked summary (client_email / project_id / key_id prefix only)
- Can print export line for GOOGLE_APPLICATION_CREDENTIALS (optional)

Exit Codes
0 success
2 invalid credentials
3 no credentials found
4 write failed
5 test failed
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


VERSION = "2.3.0"


# -----------------------------
# JSON helpers (orjson optional)
# -----------------------------
try:
    import orjson  # type: ignore

    def json_loads(s: str) -> Any:
        return orjson.loads(s)

    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode("utf-8")

except Exception:

    def json_loads(s: str) -> Any:
        return json.loads(s)

    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, ensure_ascii=False)


# -----------------------------
# Utilities
# -----------------------------
def _mask(s: str, keep: int = 6) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if len(s) <= keep:
        return s
    return s[:keep] + "…" + s[-2:]


def _mask_email(email: str) -> str:
    email = (email or "").strip()
    if "@" not in email:
        return _mask(email)
    name, dom = email.split("@", 1)
    return f"{_mask(name, 3)}@{dom}"


def _is_json_like(s: str) -> bool:
    s = (s or "").strip()
    return s.startswith("{") and s.endswith("}")


def _decode_b64(s: str) -> str:
    raw = (s or "").strip()
    data = base64.b64decode(raw)
    return data.decode("utf-8", errors="replace")


def _normalize_private_key(pk: str) -> str:
    """Fix common private key formatting issues safely."""
    if not isinstance(pk, str):
        return pk  # type: ignore

    key = pk.strip()

    # Replace escaped newlines
    if "\\n" in key:
        key = key.replace("\\n", "\n")

    # If it's a single-line key without PEM markers, wrap it
    if "-----BEGIN PRIVATE KEY-----" not in key:
        # Avoid double-wrapping if user already has markers elsewhere
        key = "-----BEGIN PRIVATE KEY-----\n" + key.strip() + "\n-----END PRIVATE KEY-----\n"

    # Ensure it ends with newline for some parsers
    if not key.endswith("\n"):
        key += "\n"

    return key


def _sanitize_creds(creds: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(creds or {})
    if "private_key" in out:
        out["private_key"] = _normalize_private_key(out["private_key"])
    return out


REQUIRED_KEYS = (
    "type",
    "project_id",
    "private_key_id",
    "private_key",
    "client_email",
    "client_id",
    "token_uri",
)


def validate_service_account(creds: Dict[str, Any]) -> Tuple[bool, str]:
    if not isinstance(creds, dict) or not creds:
        return False, "Credentials object is empty or not a JSON dict."

    missing = [k for k in REQUIRED_KEYS if not creds.get(k)]
    if missing:
        return False, f"Missing required fields: {', '.join(missing)}"

    if str(creds.get("type", "")).strip() not in ("service_account", "service-account"):
        # Allow minor variants but warn
        return False, f"Invalid 'type' field: {creds.get('type')!r} (expected 'service_account')."

    pk = creds.get("private_key", "")
    if not isinstance(pk, str) or "BEGIN PRIVATE KEY" not in pk:
        return False, "private_key is not in PEM format (missing 'BEGIN PRIVATE KEY')."

    email = str(creds.get("client_email", "")).strip()
    if "@" not in email:
        return False, "client_email does not look valid."

    token_uri = str(creds.get("token_uri", "")).strip()
    if not token_uri.startswith("https://"):
        return False, "token_uri must start with https://"

    return True, "OK"


# -----------------------------
# Loading logic
# -----------------------------
@dataclass(frozen=True)
class LoadOptions:
    input_file: Optional[str] = None
    input_json: Optional[str] = None
    input_b64: Optional[str] = None


def _load_from_file(path: str) -> Optional[Dict[str, Any]]:
    p = (path or "").strip()
    if not p:
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = f.read()
        obj = json_loads(data)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _load_from_env() -> Optional[Dict[str, Any]]:
    # JSON env
    for k in ("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"):
        raw = (os.getenv(k, "") or "").strip()
        if raw and _is_json_like(raw):
            try:
                obj = json_loads(raw)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                pass

    # Base64 env
    for k in ("GOOGLE_SHEETS_CREDENTIALS_B64", "GOOGLE_CREDENTIALS_B64"):
        raw = (os.getenv(k, "") or "").strip()
        if raw:
            try:
                decoded = _decode_b64(raw)
                obj = json_loads(decoded)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                pass

    # File path env
    path = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "") or "").strip()
    if path:
        obj = _load_from_file(path)
        if isinstance(obj, dict):
            return obj

    return None


def _load_from_settings() -> Optional[Dict[str, Any]]:
    try:
        from core.config import get_settings  # type: ignore

        settings = get_settings()

        raw = getattr(settings, "google_sheets_credentials_json", None)
        if isinstance(raw, str) and raw.strip() and _is_json_like(raw.strip()):
            try:
                obj = json_loads(raw.strip())
                if isinstance(obj, dict):
                    return obj
            except Exception:
                pass

        d = getattr(settings, "google_credentials_dict", None)
        if isinstance(d, dict) and d:
            return dict(d)

    except Exception:
        return None

    return None


def load_credentials(opts: LoadOptions) -> Optional[Dict[str, Any]]:
    # 1) explicit file
    if opts.input_file:
        obj = _load_from_file(opts.input_file)
        if isinstance(obj, dict):
            return obj

    # 2) explicit json
    if opts.input_json:
        s = opts.input_json.strip()
        if _is_json_like(s):
            try:
                obj = json_loads(s)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                pass

    # 3) explicit b64
    if opts.input_b64:
        try:
            decoded = _decode_b64(opts.input_b64)
            obj = json_loads(decoded)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass

    # 4) env
    obj = _load_from_env()
    if isinstance(obj, dict):
        return obj

    # 5) settings
    obj = _load_from_settings()
    if isinstance(obj, dict):
        return obj

    return None


# -----------------------------
# Writing
# -----------------------------
def write_credentials_file(creds: Dict[str, Any], out_path: str) -> str:
    out_path = (out_path or "").strip() or "/tmp/google_sa_credentials.json"
    directory = os.path.dirname(out_path) or "."
    os.makedirs(directory, exist_ok=True)

    # Write with restrictive permissions where possible
    payload = json_dumps(creds)

    # Try atomic write
    tmp_path = out_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(payload)

    try:
        os.replace(tmp_path, out_path)
    except Exception:
        # fallback
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(payload)
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    try:
        os.chmod(out_path, 0o600)
    except Exception:
        # On some platforms this may fail; ignore safely
        pass

    return out_path


# -----------------------------
# Optional test (Sheets API)
# -----------------------------
def test_sheets_access(creds: Dict[str, Any], spreadsheet_id: Optional[str]) -> Tuple[bool, str]:
    sid = (spreadsheet_id or os.getenv("DEFAULT_SPREADSHEET_ID", "") or "").strip()
    if not sid:
        return False, "No spreadsheet_id provided (use --spreadsheet-id or DEFAULT_SPREADSHEET_ID)."

    try:
        from google.oauth2.service_account import Credentials  # type: ignore
        from googleapiclient.discovery import build  # type: ignore
    except Exception as e:
        return False, f"Google client libs not installed: {e}"

    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        c = Credentials.from_service_account_info(creds, scopes=scopes)
        svc = build("sheets", "v4", credentials=c, cache_discovery=False)
        meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
        title = meta.get("properties", {}).get("title", "")
        return True, f"Sheets test OK. Spreadsheet title: {title!r}"
    except Exception as e:
        return False, f"Sheets test failed: {e}"


# -----------------------------
# CLI
# -----------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=f"Google credentials setup (v{VERSION})")
    p.add_argument("--input-file", default="", help="Path to service account JSON file")
    p.add_argument("--input-json", default="", help="Raw service account JSON string")
    p.add_argument("--input-b64", default="", help="Base64-encoded service account JSON")
    p.add_argument("--out", default="/tmp/google_sa_credentials.json", help="Output file path to write credentials JSON")
    p.add_argument("--write", action="store_true", help="Write credentials JSON to --out")
    p.add_argument("--print-export", action="store_true", help="Print export line for GOOGLE_APPLICATION_CREDENTIALS")
    p.add_argument("--validate-only", action="store_true", help="Validate credentials and exit")
    p.add_argument("--test-sheets", action="store_true", help="Run a lightweight Sheets API test (requires google libs)")
    p.add_argument("--spreadsheet-id", default="", help="Spreadsheet ID for Sheets API test")
    return p


def main() -> int:
    args = build_parser().parse_args()

    opts = LoadOptions(
        input_file=args.input_file or None,
        input_json=args.input_json or None,
        input_b64=args.input_b64 or None,
    )

    creds = load_credentials(opts)
    if not creds:
        print("❌ No credentials found in any supported source.", file=sys.stderr)
        return 3

    creds = _sanitize_creds(creds)

    ok, msg = validate_service_account(creds)
    if not ok:
        print(f"❌ Invalid credentials: {msg}", file=sys.stderr)
        # Masked summary for debugging
        print(f"   client_email: {_mask_email(str(creds.get('client_email', '')))}", file=sys.stderr)
        print(f"   project_id:   {_mask(str(creds.get('project_id', '')), 8)}", file=sys.stderr)
        print(f"   key_id:       {_mask(str(creds.get('private_key_id', '')), 8)}", file=sys.stderr)
        return 2

    # Safe summary
    print("✅ Credentials validated.")
    print(f"   client_email: {_mask_email(str(creds.get('client_email', '')))}")
    print(f"   project_id:   {_mask(str(creds.get('project_id', '')), 10)}")
    print(f"   key_id:       {_mask(str(creds.get('private_key_id', '')), 10)}")
    print(f"   has_orjson:   {bool(_HAS_ORJSON)}")

    if args.validate_only and not args.write and not args.test_sheets and not args.print_export:
        return 0

    out_path = ""
    if args.write:
        try:
            out_path = write_credentials_file(creds, args.out)
            print(f"✅ Wrote credentials to: {out_path}")
        except Exception as e:
            print(f"❌ Failed to write credentials file: {e}", file=sys.stderr)
            return 4

    if args.print_export:
        export_path = out_path or (args.out or "/tmp/google_sa_credentials.json")
        print(f'export GOOGLE_APPLICATION_CREDENTIALS="{export_path}"')

    if args.test_sheets:
        ok2, msg2 = test_sheets_access(creds, args.spreadsheet_id)
        if ok2:
            print(f"✅ {msg2}")
        else:
            print(f"❌ {msg2}", file=sys.stderr)
            return 5

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
