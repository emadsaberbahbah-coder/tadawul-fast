#!/usr/bin/env python3
# integrations/setup_credentials.py
"""
setup_credentials.py
===========================================================
HELPER: Prepare Google Service Account Credentials for Deploy (v2.9.0)
===========================================================

Why this exists
- Deployment UIs (Render, etc.) and .env files prefer a SINGLE-LINE value.
- Google Service Account JSON contains a multi-line private_key once loaded.
- This tool produces:
    1) Minified one-line JSON  (best for GOOGLE_SHEETS_CREDENTIALS)
    2) Base64(minified JSON)  (sometimes safer for deployment UIs)
    3) URLSAFE Base64 option  (works in strict UIs / copy-paste scenarios)

What’s improved in v2.9.0
- ✅ “Secure-by-default” output: prints MASKED values unless you pass --reveal.
- ✅ Stronger base64 detection (standard + urlsafe + loose padding + whitespace).
- ✅ Better private_key repairs:
    - Converts literal '\\n' to real newlines (optional)
    - Fixes common “BEGIN PRIVATE KEY-----END” boundary spacing/newline issues
    - Handles RSA PRIVATE KEY as well (warning)
- ✅ Rich validation:
    - Required fields + optional strict schema validation
    - Basic sanity checks for email, token_uri, pem boundaries
- ✅ Safe fingerprints:
    - Prints SHA256 fingerprints so you can confirm you copied the right value without exposing secrets.
- ✅ Render/.env helpers that are masked by default.

Usage (examples)
1) File:
      python integrations/setup_credentials.py --input credentials.json --both --out-json creds.min.json --out-b64 creds.b64.txt

2) Pipe JSON:
      type credentials.json | python integrations/setup_credentials.py --stdin --both --out-json creds.min.json

3) Read from env var (raw or base64):
      python integrations/setup_credentials.py --from-env GOOGLE_SHEETS_CREDENTIALS --base64

Security note
- This tool can print secrets. Default is masked previews.
- Use --out-json/--out-b64 for safest workflow (avoid terminal history).
- Don’t paste secrets into chats/screenshots.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

DEFAULT_INPUT = "credentials.json"
DEFAULT_ENV_NAME = "GOOGLE_SHEETS_CREDENTIALS"
VERSION = "2.9.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


# =============================================================================
# IO helpers
# =============================================================================
def _read_file(path: str) -> str:
    if not os.path.exists(path):
        raise FileNotFoundError(f"'{path}' not found")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _read_stdin() -> str:
    if sys.stdin.isatty():
        print("Waiting for JSON input from stdin... (Ctrl+D to finish)")
    return sys.stdin.read() or ""


def _read_env(name: str) -> str:
    return os.getenv(name, "") or ""


def _strip_wrapping_quotes(s: str) -> str:
    t = (s or "").strip()
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1].strip()
    return t


def _compact_ws(s: str) -> str:
    # remove whitespace/newlines that often get introduced in copied base64
    return re.sub(r"\s+", "", s or "")


def _sanitize_text(s: str) -> str:
    # Remove BOM and null bytes (rare copy/paste corruption)
    t = s or ""
    t = t.replace("\ufeff", "")
    t = t.replace("\x00", "")
    return t


# =============================================================================
# Base64 detection & decoding
# =============================================================================
def _pad_base64(s: str) -> str:
    raw = _compact_ws(s)
    if not raw:
        return raw
    m = len(raw) % 4
    if m == 0:
        return raw
    return raw + ("=" * (4 - m))


def _is_probably_base64(raw: str) -> bool:
    """
    Heuristic:
    - Not JSON
    - Long-ish
    - Only base64 alphabet (standard + urlsafe)
    """
    s = _compact_ws((raw or "").strip())
    if not s:
        return False
    if s.startswith("{"):
        return False
    if len(s) < 80:
        return False
    if not re.fullmatch(r"[A-Za-z0-9+/=_-]+", s):
        return False
    return True


def _try_b64_decode_to_json(raw: str) -> Optional[str]:
    s0 = _sanitize_text((raw or "")).strip()
    if not s0:
        return None

    s = _compact_ws(s0)
    if not _is_probably_base64(s):
        return None

    s_padded = _pad_base64(s)

    for fn in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            decoded = fn(s_padded).decode("utf-8", errors="strict").strip()
            if decoded.startswith("{") and ("private_key" in decoded) and ("client_email" in decoded):
                return decoded
        except Exception:
            continue
    return None


# =============================================================================
# Validation / normalization
# =============================================================================
def _coerce_private_key_newlines(data: Dict[str, Any], *, fix: bool) -> None:
    """
    If private_key contains literal '\\n' sequences, optionally convert to real newlines.
    """
    if not fix:
        return
    try:
        pk = data.get("private_key")
        if isinstance(pk, str) and "\\n" in pk:
            data["private_key"] = pk.replace("\\n", "\n")
    except Exception:
        pass


def _repair_private_key_boundaries(data: Dict[str, Any], *, fix: bool) -> None:
    """
    Repairs common copy/paste issues:
    - Missing newline between BEGIN/END boundaries and key body
    - Extra spaces around boundaries
    """
    if not fix:
        return
    pk = data.get("private_key")
    if not isinstance(pk, str) or not pk.strip():
        return

    t = pk.strip()

    # Normalize boundary tokens (allow RSA)
    begin_variants = [
        "-----BEGIN PRIVATE KEY-----",
        "-----BEGIN RSA PRIVATE KEY-----",
    ]
    end_variants = [
        "-----END PRIVATE KEY-----",
        "-----END RSA PRIVATE KEY-----",
    ]

    has_begin = any(b in t for b in begin_variants)
    has_end = any(e in t for e in end_variants)

    if not has_begin or not has_end:
        return

    # Remove accidental spaces immediately after BEGIN/ before END
    for b in begin_variants:
        t = t.replace(b + " ", b + "\n")
        t = t.replace(b + "\r\n", b + "\n")
        # Ensure newline after BEGIN
        t = t.replace(b, b + "\n") if (b in t and (b + "\n") not in t[: len(b) + 2]) else t

    for e in end_variants:
        t = t.replace(" " + e, "\n" + e)
        t = t.replace("\r\n" + e, "\n" + e)

    # Collapse multiple blank lines
    t = re.sub(r"\n{3,}", "\n\n", t)

    data["private_key"] = t


def _load_json_text(text: str) -> Dict[str, Any]:
    """
    Accepts:
    - raw JSON
    - quoted JSON
    - base64(JSON) (standard or urlsafe; padding optional)
    """
    t0 = _sanitize_text(text or "")
    t = _strip_wrapping_quotes(t0)

    decoded = _try_b64_decode_to_json(t)
    if decoded:
        t = decoded

    t = _strip_wrapping_quotes(t).strip()
    if not t:
        raise ValueError("Empty input (no JSON found).")
    if not t.lstrip().startswith("{"):
        raise ValueError("Input is not JSON (expected '{'), and it did not decode as valid base64 JSON.")

    try:
        data = json.loads(t)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format: {e}")

    if not isinstance(data, dict) or not data:
        raise ValueError("JSON must be a non-empty object.")
    return data


def _validate_service_account(data: Dict[str, Any], strict_type: bool = False, strict_schema: bool = False) -> None:
    required = ["type", "project_id", "private_key", "client_email"]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    if strict_type and str(data.get("type", "")).strip() != "service_account":
        raise ValueError("Field 'type' is not 'service_account' (strict mode).")

    pk = data.get("private_key")
    if not isinstance(pk, str) or ("BEGIN PRIVATE KEY" not in pk and "BEGIN RSA PRIVATE KEY" not in pk):
        raise ValueError("private_key looks invalid (missing 'BEGIN PRIVATE KEY' boundary).")

    if "BEGIN RSA PRIVATE KEY" in str(pk):
        # not fatal, but good to flag
        logger = "WARNING: private_key is RSA PRIVATE KEY format; service_account keys are usually PRIVATE KEY."

    email = str(data.get("client_email", "")).strip()
    if "@" not in email or "." not in email.split("@")[-1]:
        raise ValueError("client_email looks invalid.")

    if strict_schema:
        # Common service-account fields from Google
        schema_keys = [
            "private_key_id",
            "client_id",
            "auth_uri",
            "token_uri",
            "auth_provider_x509_cert_url",
            "client_x509_cert_url",
        ]
        miss2 = [k for k in schema_keys if k not in data]
        if miss2:
            raise ValueError(f"Missing expected service-account fields (strict schema): {miss2}")

    # token_uri sanity check (non-fatal, but helpful)
    token_uri = str(data.get("token_uri", "") or "").strip()
    if token_uri and not token_uri.startswith("https://"):
        raise ValueError("token_uri should start with https:// (looks suspicious).")


def _minified_json(data: Dict[str, Any]) -> str:
    # json.dumps escapes real newlines to \n -> single line output
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False)


def _to_b64(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


def _to_b64_urlsafe(s: str) -> str:
    # urlsafe with padding kept (still valid)
    return base64.urlsafe_b64encode(s.encode("utf-8")).decode("ascii")


def _make_outputs(data: Dict[str, Any]) -> Tuple[str, str, str]:
    flat = _minified_json(data)
    b64 = _to_b64(flat)
    b64u = _to_b64_urlsafe(flat)
    return flat, b64, b64u


def _mask(s: str, keep: int = 12) -> str:
    t = s or ""
    if len(t) <= keep * 2 + 3:
        return "*" * len(t)
    return f"{t[:keep]}...{t[-keep:]}"


def _write_file(path: str, content: str) -> None:
    if not path:
        return
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _sha256_hex(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _fingerprint_report(flat_json: str, data: Dict[str, Any]) -> str:
    """
    Safe identifiers to confirm you copied the right secret without revealing it.
    """
    email = str(data.get("client_email", "") or "").strip()
    pkid = str(data.get("private_key_id", "") or "").strip()
    proj = str(data.get("project_id", "") or "").strip()

    fp_all = _sha256_hex(flat_json)[:16]
    fp_email = _sha256_hex(email)[:12] if email else ""
    fp_pkid = _sha256_hex(pkid)[:12] if pkid else ""

    parts = [f"json_sha256_16={fp_all}"]
    if proj:
        parts.append(f"project_id={proj}")
    if email:
        parts.append(f"email_sha256_12={fp_email}")
    if pkid:
        parts.append(f"pkid_sha256_12={fp_pkid}")

    return " | ".join(parts)


def _ps_setx(env_name: str, value: str, *, masked: bool) -> str:
    v = _mask(value) if masked else value
    return f'setx {env_name} "{v}"'


def _bash_export(env_name: str, value: str, *, masked: bool) -> str:
    v = _mask(value) if masked else value
    return f"export {env_name}='{v}'"


def _dotenv_line(env_name: str, value: str, *, masked: bool) -> str:
    v = _mask(value) if masked else value
    return f"{env_name}='{v}'"


def _render_line(env_name: str, value: str, *, masked: bool) -> str:
    v = _mask(value) if masked else value
    return f"{env_name} = {v}"


def _auto_out_path(prefix: str, ext: str) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.{ext.lstrip('.')}"


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    p = argparse.ArgumentParser(description="Format Google Service Account JSON for env vars (safe by default).")

    src = p.add_argument_group("Input source (choose one)")
    src.add_argument("--input", "-i", default=DEFAULT_INPUT, help=f"Input JSON file (default: {DEFAULT_INPUT})")
    src.add_argument("--stdin", action="store_true", help="Read JSON from stdin (pipe).")
    src.add_argument(
        "--from-env",
        default="",
        help="Read JSON (or base64 JSON) from an environment variable name.",
    )

    out = p.add_argument_group("Output format")
    g = out.add_mutually_exclusive_group()
    g.add_argument("--json", action="store_true", help="Output ONLY minified JSON.")
    g.add_argument("--base64", action="store_true", help="Output ONLY base64(minified JSON).")
    g.add_argument("--urlsafe-b64", action="store_true", help="Output ONLY URLSAFE base64(minified JSON).")
    g.add_argument("--both", action="store_true", help="Output JSON + base64 + urlsafe base64.")

    out.add_argument("--env-name", default=DEFAULT_ENV_NAME, help="Env var name to display in instructions.")
    out.add_argument("--reveal", action="store_true", help="Print FULL secrets (default prints masked previews).")
    out.add_argument("--mask", action="store_true", help="Force masked previews (even if --reveal used).")
    out.add_argument("--strict-type", action="store_true", help="Require type == service_account.")
    out.add_argument("--strict-schema", action="store_true", help="Require full expected service-account schema.")
    out.add_argument(
        "--fix-private-key",
        action="store_true",
        help="Repair common private_key issues (recommended if paste breaks).",
    )

    files = p.add_argument_group("Write outputs to files (recommended)")
    files.add_argument("--out-json", default="", help="Write minified JSON output to this file.")
    files.add_argument("--out-b64", default="", help="Write base64 output to this file.")
    files.add_argument("--out-b64u", default="", help="Write urlsafe base64 output to this file.")
    files.add_argument("--auto-out", action="store_true", help="Auto-generate output files (no secrets printed).")
    files.add_argument("--auto-prefix", default="creds", help="Prefix for --auto-out files (default: creds).")

    helpers = p.add_argument_group("Helper output (masked unless --reveal)")
    helpers.add_argument("--print-export", action="store_true", help="Print helper export commands.")
    helpers.add_argument("--print-dotenv", action="store_true", help="Print helper .env line.")
    helpers.add_argument("--print-render", action="store_true", help="Print Render-style env entry preview.")
    helpers.add_argument("--print-fingerprint", action="store_true", help="Print safe fingerprints (recommended).")

    args = p.parse_args()

    try:
        # Decide source text
        if args.stdin:
            raw_text = _read_stdin()
            src_label = "stdin"
        elif args.from_env:
            env_name = str(args.from_env).strip()
            raw_text = _read_env(env_name)
            src_label = f"env:{env_name}"
        else:
            raw_text = _read_file(args.input)
            src_label = args.input

        data = _load_json_text(raw_text)

        # Repairs (optional)
        if args.fix_private_key:
            _coerce_private_key_newlines(data, fix=True)
            _repair_private_key_boundaries(data, fix=True)

        _validate_service_account(data, strict_type=bool(args.strict_type), strict_schema=bool(args.strict_schema))

        flat, b64, b64u = _make_outputs(data)

        # Output selection
        mode = "json"
        if args.base64:
            mode = "base64"
        elif args.urlsafe_b64:
            mode = "urlsafe_b64"
        elif args.both:
            mode = "both"
        elif args.json:
            mode = "json"

        # Auto-out mode: safest (writes files, minimal printing)
        if args.auto_out:
            out_json = args.out_json or _auto_out_path(args.auto_prefix, "min.json")
            out_b64 = args.out_b64 or _auto_out_path(args.auto_prefix, "b64.txt")
            out_b64u = args.out_b64u or _auto_out_path(args.auto_prefix, "b64u.txt")

            _write_file(out_json, flat)
            _write_file(out_b64, b64)
            _write_file(out_b64u, b64u)

            print(f"\n--- Google Credentials Formatter (v{VERSION}) ---\n")
            print(f"Source: {src_label}")
            print("Auto-out: ENABLED (no secrets printed)\n")
            print(f"• Minified JSON:     {out_json}")
            print(f"• Base64:            {out_b64}")
            print(f"• URLSAFE Base64:    {out_b64u}")

            if args.print_fingerprint or True:
                print("\nFingerprints (safe):")
                print("•", _fingerprint_report(flat, data))

            print("\nRender setup:")
            print(f"• Set env var {args.env_name} to ONE of the file contents above.")
            print("• Prefer Minified JSON first; use Base64 if UI paste/escaping causes issues.\n")
            return 0

        # Manual file writes (optional)
        if args.out_json:
            _write_file(args.out_json, flat)
        if args.out_b64:
            _write_file(args.out_b64, b64)
        if args.out_b64u:
            _write_file(args.out_b64u, b64u)

        # Determine masking behavior
        masked = True
        if args.reveal and not args.mask:
            masked = False

        print(f"\n--- Google Credentials Formatter (v{VERSION}) ---\n")
        print(f"Source: {src_label}")

        if args.print_fingerprint:
            print("\nFingerprints (safe):")
            print("•", _fingerprint_report(flat, data))

        # Show outputs (masked or full)
        if mode in ("json", "both"):
            print("\nMinified JSON (ONE LINE) — for env var:")
            print(f"  {args.env_name}")
            print("-" * 72)
            print(_mask(flat) if masked else flat)
            print("-" * 72)
            print(f"Length: {len(flat)} chars | sha256_16: {_sha256_hex(flat)[:16]}")

        if mode in ("base64", "both"):
            print("\nBase64(minified JSON) — paste-safe option:")
            print(f"  {args.env_name}")
            print("-" * 72)
            print(_mask(b64) if masked else b64)
            print("-" * 72)
            print(f"Length: {len(b64)} chars | sha256_16: {_sha256_hex(b64)[:16]}")

        if mode in ("urlsafe_b64", "both"):
            print("\nURLSAFE Base64(minified JSON) — strict UI option:")
            print(f"  {args.env_name}")
            print("-" * 72)
            print(_mask(b64u) if masked else b64u)
            print("-" * 72)
            print(f"Length: {len(b64u)} chars | sha256_16: {_sha256_hex(b64u)[:16]}")

        if args.print_export:
            print("\nHelper commands:")
            val_json = flat
            val_b64 = b64
            val_b64u = b64u

            # Print ALL variants masked/full according to mode chosen
            if mode == "json":
                print("PowerShell:", _ps_setx(args.env_name, val_json, masked=masked))
                print("Bash:      ", _bash_export(args.env_name, val_json, masked=masked))
            elif mode == "base64":
                print("PowerShell:", _ps_setx(args.env_name, val_b64, masked=masked))
                print("Bash:      ", _bash_export(args.env_name, val_b64, masked=masked))
            elif mode == "urlsafe_b64":
                print("PowerShell:", _ps_setx(args.env_name, val_b64u, masked=masked))
                print("Bash:      ", _bash_export(args.env_name, val_b64u, masked=masked))
            else:
                print("PowerShell(JSON):   ", _ps_setx(args.env_name, val_json, masked=masked))
                print("PowerShell(Base64): ", _ps_setx(args.env_name, val_b64, masked=masked))
                print("PowerShell(B64U):   ", _ps_setx(args.env_name, val_b64u, masked=masked))
                print("Bash(JSON):         ", _bash_export(args.env_name, val_json, masked=masked))
                print("Bash(Base64):       ", _bash_export(args.env_name, val_b64, masked=masked))
                print("Bash(B64U):         ", _bash_export(args.env_name, val_b64u, masked=masked))

        if args.print_dotenv:
            print("\nHelper .env line:")
            if mode == "json":
                print(_dotenv_line(args.env_name, flat, masked=masked))
            elif mode == "base64":
                print(_dotenv_line(args.env_name, b64, masked=masked))
            elif mode == "urlsafe_b64":
                print(_dotenv_line(args.env_name, b64u, masked=masked))
            else:
                print("# choose ONE:")
                print(_dotenv_line(args.env_name, flat, masked=masked))
                print(_dotenv_line(args.env_name, b64, masked=masked))
                print(_dotenv_line(args.env_name, b64u, masked=masked))

        if args.print_render:
            print("\nRender env entry preview:")
            if mode == "json":
                print(_render_line(args.env_name, flat, masked=masked))
            elif mode == "base64":
                print(_render_line(args.env_name, b64, masked=masked))
            elif mode == "urlsafe_b64":
                print(_render_line(args.env_name, b64u, masked=masked))
            else:
                print("# choose ONE:")
                print(_render_line(args.env_name, flat, masked=masked))
                print(_render_line(args.env_name, b64, masked=masked))
                print(_render_line(args.env_name, b64u, masked=masked))

        print("\nRecommended workflow:")
        if args.out_json or args.out_b64 or args.out_b64u:
            if args.out_json:
                print(f"• Wrote minified JSON to:  {args.out_json}")
            if args.out_b64:
                print(f"• Wrote base64 to:         {args.out_b64}")
            if args.out_b64u:
                print(f"• Wrote urlsafe base64 to: {args.out_b64u}")
            print("• Copy from the files into Render env vars (avoid terminal history).")
        else:
            print("• Use --auto-out to write files without printing secrets.")
            print("• If paste breaks private_key, rerun with --fix-private-key.")
            print("• Keep output wrapped in single quotes for .env when supported.")

        print("\nSecurity:")
        print("• Treat outputs as secrets. Avoid chat/screenshots.\n")

        return 0

    except Exception as e:
        print("\n❌ ERROR:", str(e))
        print("\nTroubleshooting:")
        print("1) Confirm you downloaded a Service Account key JSON from Google Cloud.")
        print(f"2) Put it next to this script as '{DEFAULT_INPUT}' or pass --input yourfile.json")
        print("3) If your env var stores base64, this tool will decode it automatically.")
        print("4) If private_key paste got corrupted, try: --fix-private-key")
        print("5) For strict validation, avoid --strict-schema unless your JSON includes all fields.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
