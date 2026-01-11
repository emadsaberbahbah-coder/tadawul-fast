#!/usr/bin/env python3
# integrations/setup_credentials.py
"""
setup_credentials.py
===========================================================
HELPER: Prepare Google Credentials for Deployment (v2.5.0)
===========================================================

Why this exists
- Platforms like Render / .env files prefer a SINGLE-LINE value.
- Google Service Account JSON contains a multi-line private_key once loaded.
- This tool outputs:
    1) Minified one-line JSON (safe for GOOGLE_SHEETS_CREDENTIALS)
    2) OPTIONAL Base64 version (often safer for deployment UIs)

Extra safety (v2.5.0)
- Reads credentials from: file OR stdin (pipe) OR env var
- Accepts JSON OR base64(JSON) OR "quoted JSON" in any source
- Strict validation of required fields + private key marker
- Optional sanitize of "\\n" -> "\n" inside private_key (common deployment issue)
- Optional output to files to avoid copy/paste
- Optional masked preview mode
- Optional "print export command" helpers (masked by default)
- Better base64 detection:
    • supports newlines/whitespace
    • supports urlsafe base64
    • supports missing padding (auto-fix)

Usage (examples)
1) File (default):
      python setup_credentials.py
   or:
      python setup_credentials.py --input my-key.json --both

2) Pipe JSON (stdin):
      type credentials.json | python setup_credentials.py --stdin --both
   (PowerShell):
      Get-Content credentials.json -Raw | python setup_credentials.py --stdin --both

3) Read from env var that already contains JSON/base64:
      python setup_credentials.py --from-env GOOGLE_SHEETS_CREDENTIALS --base64

4) Write outputs to files (recommended):
      python setup_credentials.py --both --out-json creds.min.json --out-b64 creds.min.b64.txt

Security note
- This prints secrets unless you use --mask or --out-*.
- Do not paste the output value into chats/screenshots.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
from typing import Any, Dict, Optional, Tuple

DEFAULT_INPUT = "credentials.json"
DEFAULT_ENV_NAME = "GOOGLE_SHEETS_CREDENTIALS"
VERSION = "2.5.0"

# =============================================================================
# IO helpers
# =============================================================================
def _read_file(path: str) -> str:
    if not os.path.exists(path):
        raise FileNotFoundError(f"'{path}' not found")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _read_stdin() -> str:
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


def _pad_base64(s: str) -> str:
    """
    Some UIs strip '=' padding. Base64 decoder often needs correct padding length.
    """
    raw = _compact_ws(s)
    if not raw:
        return raw
    # only pad if mod 4 != 0
    m = len(raw) % 4
    if m == 0:
        return raw
    return raw + ("=" * (4 - m))


def _is_probably_base64(raw: str) -> bool:
    """
    Heuristic: base64 strings are usually long, mostly base64 alphabet.
    We allow newlines/whitespace and urlsafe variants.
    """
    s = _compact_ws((raw or "").strip())
    if not s:
        return False
    if s.startswith("{"):
        return False

    # base64 alphabet + urlsafe
    if not re.fullmatch(r"[A-Za-z0-9+/=_-]+", s):
        return False

    # length heuristic: not too tiny
    if len(s) < 80:
        return False

    # padding heuristic: not mandatory, but common
    return True


def _try_b64_decode_to_json(raw: str) -> Optional[str]:
    s0 = (raw or "").strip()
    if not s0:
        return None

    s = _compact_ws(s0)
    if not _is_probably_base64(s):
        return None

    s_padded = _pad_base64(s)

    # Try standard base64 and urlsafe base64
    for fn in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            decoded = fn(s_padded).decode("utf-8", errors="strict").strip()
            if decoded.startswith("{") and '"private_key"' in decoded:
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
    This is safe and commonly required if someone pasted JSON through env var UIs.
    """
    if not fix:
        return
    try:
        pk = data.get("private_key")
        if isinstance(pk, str) and "\\n" in pk:
            data["private_key"] = pk.replace("\\n", "\n")
    except Exception:
        pass


def _load_json_text(text: str) -> Dict[str, Any]:
    """
    Accepts:
    - raw JSON
    - quoted JSON
    - base64(JSON) (standard or urlsafe; padding optional)
    """
    t0 = (text or "")
    t = _strip_wrapping_quotes(t0)
    decoded = _try_b64_decode_to_json(t)
    if decoded:
        t = decoded
    t = _strip_wrapping_quotes(t).strip()

    if not t:
        raise ValueError("Empty input (no JSON found).")
    # allow whitespace prefix
    if not t.lstrip().startswith("{"):
        raise ValueError("Input is not JSON (expected '{' at start), and it did not decode as base64 JSON.")

    data = json.loads(t)
    if not isinstance(data, dict) or not data:
        raise ValueError("JSON is empty or not an object.")
    return data


def _validate_service_account(data: Dict[str, Any], strict_type: bool = False) -> None:
    required = ["type", "project_id", "private_key", "client_email"]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    if strict_type and str(data.get("type", "")).strip() != "service_account":
        raise ValueError("Field 'type' is not 'service_account' (strict mode).")

    pk = data.get("private_key")
    if not isinstance(pk, str) or "BEGIN PRIVATE KEY" not in pk:
        raise ValueError("private_key looks invalid (missing 'BEGIN PRIVATE KEY').")

    email = str(data.get("client_email", "")).strip()
    if "@" not in email:
        raise ValueError("client_email looks invalid.")


def _minified_json(data: Dict[str, Any]) -> str:
    # json.dumps escapes real newlines to \n -> single line output
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False)


def _to_b64(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


def _make_outputs(data: Dict[str, Any]) -> Tuple[str, str]:
    flat = _minified_json(data)
    b64 = _to_b64(flat)
    return flat, b64


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


def _ps_setx(env_name: str, value: str, *, masked: bool) -> str:
    v = _mask(value) if masked else value
    # NOTE: setx has length limits; this is helper text, not guaranteed for huge values.
    return f'setx {env_name} "{v}"'


def _bash_export(env_name: str, value: str, *, masked: bool) -> str:
    v = _mask(value) if masked else value
    # Quote with single quotes; JSON/base64 typically doesn't contain single quotes.
    return f"export {env_name}='{v}'"


def _dotenv_line(env_name: str, value: str, *, masked: bool) -> str:
    v = _mask(value) if masked else value
    # .env parsers vary; quoting reduces breakage
    return f"{env_name}='{v}'"


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    p = argparse.ArgumentParser(description="Format Google Service Account JSON for env vars.")

    src = p.add_argument_group("Input source (choose one)")
    src.add_argument("--input", "-i", default=DEFAULT_INPUT, help=f"Input JSON file (default: {DEFAULT_INPUT})")
    src.add_argument("--stdin", action="store_true", help="Read JSON from stdin (pipe).")
    src.add_argument(
        "--from-env",
        default="",
        help="Read JSON (or base64 JSON) from an environment variable name (e.g. GOOGLE_SHEETS_CREDENTIALS).",
    )

    out = p.add_argument_group("Output mode")
    g = out.add_mutually_exclusive_group()
    g.add_argument("--json", action="store_true", help="Output ONLY minified JSON (default).")
    g.add_argument("--base64", action="store_true", help="Output ONLY base64(minified JSON).")
    g.add_argument("--both", action="store_true", help="Output BOTH JSON and base64.")

    out.add_argument("--env-name", default=DEFAULT_ENV_NAME, help="Env var name to display in instructions.")
    out.add_argument("--mask", action="store_true", help="Print masked previews instead of full secrets.")
    out.add_argument("--strict-type", action="store_true", help="Require type == service_account.")
    out.add_argument(
        "--fix-private-key",
        action="store_true",
        help="Convert literal '\\\\n' to real newlines in private_key before output.",
    )
    out.add_argument("--out-json", default="", help="Write minified JSON output to this file.")
    out.add_argument("--out-b64", default="", help="Write base64 output to this file.")
    out.add_argument("--print-export", action="store_true", help="Print helper export commands (masked by default).")
    out.add_argument("--print-dotenv", action="store_true", help="Print helper .env line (masked by default).")

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
        _coerce_private_key_newlines(data, fix=bool(args.fix_private_key))
        _validate_service_account(data, strict_type=bool(args.strict_type))
        flat, b64 = _make_outputs(data)

        mode = "json"
        if args.base64:
            mode = "base64"
        elif args.both:
            mode = "both"
        elif args.json:
            mode = "json"

        # Optional write-to-file (recommended)
        if args.out_json:
            _write_file(args.out_json, flat)
        if args.out_b64:
            _write_file(args.out_b64, b64)

        print(f"\n--- Google Credentials Formatter (v{VERSION}) ---\n")
        print(f"Source: {src_label}")

        # Show outputs (masked or full)
        if mode in ("json", "both"):
            print("\nMinified JSON (ONE LINE) — use this for:")
            print(f"  {args.env_name}")
            print("-" * 70)
            print("▼ ▼ ▼")
            print(_mask(flat) if args.mask else flat)
            print("▲ ▲ ▲")
            print("-" * 70)

        if mode in ("base64", "both"):
            print("\nBase64(minified JSON) — safer in some deployment UIs:")
            print(f"  {args.env_name}  (you can store base64 here too)")
            print("-" * 70)
            print("▼ ▼ ▼")
            print(_mask(b64) if args.mask else b64)
            print("▲ ▲ ▲")
            print("-" * 70)

        if args.print_export:
            print("\nHelper commands (MASKED preview):")
            if mode in ("json", "both"):
                print("PowerShell:", _ps_setx(args.env_name, flat, masked=True))
                print("Bash:      ", _bash_export(args.env_name, flat, masked=True))
            if mode in ("base64", "both"):
                print("PowerShell:", _ps_setx(args.env_name, b64, masked=True))
                print("Bash:      ", _bash_export(args.env_name, b64, masked=True))

        if args.print_dotenv:
            print("\nHelper .env line (MASKED preview):")
            if mode in ("json", "both"):
                print(_dotenv_line(args.env_name, flat, masked=True))
            if mode in ("base64", "both"):
                print(_dotenv_line(args.env_name, b64, masked=True))

        print("\nRecommended:")
        if args.out_json or args.out_b64:
            if args.out_json:
                print(f"• Wrote minified JSON to: {args.out_json}")
            if args.out_b64:
                print(f"• Wrote base64 to:       {args.out_b64}")
            print("• Copy from those files into Render env vars (avoid printing secrets).")
        else:
            print("• Render: Add env var GOOGLE_SHEETS_CREDENTIALS = (paste ONE of the values above)")
            print("• Local .env: Prefer wrapping the value in single quotes if your parser supports it.")
            print("• Tip: Use --mask to avoid printing full secrets to terminal history.")
            print("• Tip: Use --out-json/--out-b64 to avoid copy/paste mistakes.")
            print("• If your private_key breaks after paste, rerun with --fix-private-key")

        print("\nSecurity:")
        print("• Treat this output as a secret. Don’t share it in chats/screenshots.\n")

        return 0

    except Exception as e:
        print("\n❌ ERROR:", str(e))
        print("\nWhat to do:")
        print("1) Download the Service Account key JSON from Google Cloud Console.")
        print(f"2) Put it next to this script as '{DEFAULT_INPUT}' OR pass --input yourfile.json")
        print("3) Or pipe it using --stdin, or read it using --from-env ENVNAME")
        print("4) If you stored base64 in env var, this tool can decode it automatically")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
