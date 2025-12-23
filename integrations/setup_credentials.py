"""
setup_credentials.py
===========================================================
HELPER: Prepare Google Credentials for Deployment (v2.0.0)
===========================================================

Why this exists
- Platforms like Render / .env files prefer a SINGLE-LINE value.
- Google Service Account JSON contains a multi-line private_key once loaded.
- This tool outputs:
    1) Minified one-line JSON (safe for GOOGLE_SHEETS_CREDENTIALS)
    2) OPTIONAL Base64 version (often safer for deployment UIs)

Good news:
- Your updated google_sheets_service.py (v3.9.0) supports BOTH:
    - raw JSON string
    - base64 JSON string (auto-detected)

Usage
1) Put your downloaded Google key file in this folder (default: credentials.json)
2) Run:
      python setup_credentials.py
   OR:
      python setup_credentials.py --input my-key.json --both
   OR:
      python setup_credentials.py --base64

Copy ONLY the output value (not the arrows).

Security note
- This prints secrets to your terminal. Do not paste screenshots to chats.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from typing import Any, Dict, Tuple

DEFAULT_INPUT = "credentials.json"


def _load_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"'{path}' not found")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) or not data:
        raise ValueError("JSON is empty or not an object")
    return data


def _validate_service_account(data: Dict[str, Any]) -> None:
    required = ["type", "project_id", "private_key", "client_email"]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    if str(data.get("type", "")).strip() != "service_account":
        # Still allow it, but warn via exception message upstream if you want strict
        pass

    pk = data.get("private_key")
    if not isinstance(pk, str) or "BEGIN PRIVATE KEY" not in pk:
        raise ValueError("private_key looks invalid (missing 'BEGIN PRIVATE KEY')")

    email = str(data.get("client_email", "")).strip()
    if "@" not in email:
        raise ValueError("client_email looks invalid")


def _minified_json(data: Dict[str, Any]) -> str:
    # json.load() makes private_key contain REAL newlines.
    # json.dumps() re-escapes them into \n and outputs a single line.
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False)


def _to_b64(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


def _make_outputs(data: Dict[str, Any]) -> Tuple[str, str]:
    flat = _minified_json(data)
    b64 = _to_b64(flat)
    return flat, b64


def main() -> int:
    p = argparse.ArgumentParser(description="Format Google Service Account JSON for env vars.")
    p.add_argument("--input", "-i", default=DEFAULT_INPUT, help=f"Input JSON file (default: {DEFAULT_INPUT})")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--json", action="store_true", help="Output ONLY minified JSON (default)")
    g.add_argument("--base64", action="store_true", help="Output ONLY base64(minified JSON)")
    g.add_argument("--both", action="store_true", help="Output BOTH JSON and base64")
    p.add_argument("--env-name", default="GOOGLE_SHEETS_CREDENTIALS", help="Env var name to display in instructions")
    args = p.parse_args()

    try:
        data = _load_json(args.input)
        _validate_service_account(data)
        flat, b64 = _make_outputs(data)

        mode = "json"
        if args.base64:
            mode = "base64"
        elif args.both:
            mode = "both"
        elif args.json:
            mode = "json"

        print("\n--- Google Credentials Formatter (v2.0.0) ---\n")
        print(f"Input: {args.input}")

        if mode in ("json", "both"):
            print("\nMinified JSON (ONE LINE) — use this for:")
            print(f"  {args.env_name}")
            print("-" * 70)
            print("▼ ▼ ▼")
            print(flat)
            print("▲ ▲ ▲")
            print("-" * 70)

        if mode in ("base64", "both"):
            print("\nBase64(minified JSON) — safer in some deployment UIs:")
            print(f"  {args.env_name}  (you can store base64 here too, your service supports it)")
            print("-" * 70)
            print("▼ ▼ ▼")
            print(b64)
            print("▲ ▲ ▲")
            print("-" * 70)

        print("\nRecommended:")
        print("• Render: Add env var GOOGLE_SHEETS_CREDENTIALS = (paste one of the values above)")
        print("• Local .env: Prefer wrapping the value in single quotes if your parser supports it.")
        print("\nSecurity:")
        print("• Treat this output as a secret. Don’t share it in chats/screenshots.\n")

        return 0

    except Exception as e:
        print("\n❌ ERROR:", str(e))
        print("\nWhat to do:")
        print("1) Download the Service Account key JSON from Google Cloud Console.")
        print(f"2) Put it next to this script as '{DEFAULT_INPUT}' or pass --input yourfile.json")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
