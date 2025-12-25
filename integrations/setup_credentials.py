```python
# setup_credentials.py  (FULL REPLACEMENT)
"""
setup_credentials.py
===========================================================
HELPER: Prepare Google Credentials for Deployment (v2.2.0)
===========================================================

Why this exists
- Platforms like Render / .env files prefer a SINGLE-LINE value.
- Google Service Account JSON contains a multi-line private_key once loaded.
- This tool outputs:
    1) Minified one-line JSON (safe for GOOGLE_SHEETS_CREDENTIALS)
    2) OPTIONAL Base64 version (often safer for deployment UIs)

Extra safety (v2.2.0)
- Supports reading credentials from a file OR from stdin (pipe) OR from an env var.
- Optional output to files (so you don't have to copy/paste secrets).
- Optional "masked preview" mode (prints only first/last chars) to reduce accidental leaks.
- Strict validation of required fields + private key marker.

Usage (examples)
1) File (default):
      python setup_credentials.py
   or:
      python setup_credentials.py --input my-key.json --both

2) Pipe JSON (stdin):
      type credentials.json | python setup_credentials.py --stdin --both
   (PowerShell):
      Get-Content credentials.json -Raw | python setup_credentials.py --stdin --both

3) Read from env var that already contains JSON:
      python setup_credentials.py --from-env GOOGLE_SHEETS_CREDENTIALS --base64

4) Write outputs to files (recommended):
      python setup_credentials.py --both --out-json creds.min.json --out-b64 creds.min.b64.txt

Security note
- This prints secrets if you don't use --mask or --out-*.
- Do not paste the output value into chats/screenshots.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from typing import Any, Dict, Optional, Tuple

DEFAULT_INPUT = "credentials.json"
DEFAULT_ENV_NAME = "GOOGLE_SHEETS_CREDENTIALS"


# =============================================================================
# IO helpers
# =============================================================================
def _read_file(path: str) -> str:
    if not os.path.exists(path):
        raise FileNotFoundError(f"'{path}' not found")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _read_stdin() -> str:
    data = sys.stdin.read()
    return data or ""


def _read_env(name: str) -> str:
    v = os.getenv(name, "")
    return v or ""


def _strip_wrapping_quotes(s: str) -> str:
    t = (s or "").strip()
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1].strip()
    return t


def _maybe_b64_decode(s: str) -> str:
    """
    If the string looks like base64 and decodes into JSON, return decoded JSON string.
    Otherwise return original.
    """
    raw = (s or "").strip()
    if not raw:
        return raw
    if raw.startswith("{"):
        return raw

    # Heuristic: only try decoding if long-ish and no obvious JSON
    if len(raw) < 120:
        return raw

    try:
        decoded = base64.b64decode(raw).decode("utf-8", errors="strict").strip()
        if decoded.startswith("{") and '"private_key"' in decoded:
            return decoded
    except Exception:
        return raw

    return raw


# =============================================================================
# Validation / formatting
# =============================================================================
def _load_json_text(text: str) -> Dict[str, Any]:
    t = _strip_wrapping_quotes(text)
    t = _maybe_b64_decode(t)
    t = _strip_wrapping_quotes(t).strip()

    if not t:
        raise ValueError("Empty input (no JSON found).")
    if not t.startswith("{"):
        raise ValueError("Input is not JSON (expected '{' at start).")

    data = json.loads(t)
    if not isinstance(data, dict) or not data:
        raise ValueError("JSON is empty or not an object.")
    return data


def _validate_service_account(data: Dict[str, Any], strict_type: bool = False) -> None:
    required = ["type", "project_id", "private_key", "client_email"]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    if strict_type:
        if str(data.get("type", "")).strip() != "service_account":
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
    out.add_argument("--out-json", default="", help="Write minified JSON output to this file.")
    out.add_argument("--out-b64", default="", help="Write base64 output to this file.")

    args = p.parse_args()

    try:
        # Decide source text
        if args.stdin:
            raw_text = _read_stdin()
            src_label = "stdin"
        elif args.from_env:
            raw_text = _read_env(str(args.from_env).strip())
            src_label = f"env:{args.from_env}"
        else:
            raw_text = _read_file(args.input)
            src_label = args.input

        data = _load_json_text(raw_text)
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

        print("\n--- Google Credentials Formatter (v2.2.0) ---\n")
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

        print("\nSecurity:")
        print("• Treat this output as a secret. Don’t share it in chats/screenshots.\n")

        return 0

    except Exception as e:
        print("\n❌ ERROR:", str(e))
        print("\nWhat to do:")
        print("1) Download the Service Account key JSON from Google Cloud Console.")
        print(f"2) Put it next to this script as '{DEFAULT_INPUT}' OR pass --input yourfile.json")
        print("3) Or pipe it using --stdin, or read it using --from-env ENVNAME")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
```
