from __future__ import annotations

import json
import os
import sys
import urllib.request
import urllib.error


def http_json(method: str, url: str, token: str, payload: object | None = None) -> dict:
    headers = {
        "Accept": "application/json",
        "X-APP-TOKEN": token,
    }

    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url=url, data=data, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                return {"ok": True, "status": resp.status, "json": json.loads(raw)}
            except Exception:
                return {"ok": True, "status": resp.status, "raw": raw}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        try:
            j = json.loads(raw)
        except Exception:
            j = {"raw": raw}
        return {"ok": False, "status": int(getattr(e, "code", 0) or 0), "error": j}
    except Exception as e:
        return {"ok": False, "status": 0, "error": {"message": str(e)}}


def main() -> int:
    base_url = os.getenv("BASE_URL", "").rstrip("/")
    token = os.getenv("APP_TOKEN", "")

    if not base_url:
        print("❌ Missing BASE_URL env var, e.g. https://your-service.onrender.com")
        return 2
    if not token:
        print("❌ Missing APP_TOKEN env var (must match server APP_TOKEN)")
        return 2

    print(f"BASE_URL = {base_url}")
    print("--------------------------------------------------")

    # 1) Health (no token required in our current routes? health is open)
    r = http_json("GET", f"{base_url}/health", token="dummy")  # token ignored by /health
    print("GET /health =>", r["status"])
    print(json.dumps(r.get("json", r.get("error", r.get("raw"))), indent=2, ensure_ascii=False))
    print("--------------------------------------------------")

    # 2) Fetch YAML-driven config
    page_ksa = "page_01_market_summary_ksa"
    r = http_json("GET", f"{base_url}/api/v1/config/{page_ksa}", token=token)
    print(f"GET /api/v1/config/{page_ksa} =>", r["status"])
    print(json.dumps(r.get("json", r.get("error", r.get("raw"))), indent=2, ensure_ascii=False))
    print("--------------------------------------------------")

    # 3) Ingest KSA sample rows
    sample_ksa_rows = [
        {
            "symbol": "1120.SR",
            "company_name": "AL RAJHI BANK",
            "sector": "Banks",
            "currency": "SAR",
            "last_price": 98.4,
            "previous_close": 97.9,
            "change": 0.5,
            "change_percent": 0.51,
            "volume": 1234567,
            "market_cap": 410000000000,
            "last_updated": "2025-12-18T12:00:00Z",
        }
    ]
    r = http_json("POST", f"{base_url}/api/v1/ksa/ingest/{page_ksa}", token=token, payload=sample_ksa_rows)
    print(f"POST /api/v1/ksa/ingest/{page_ksa} =>", r["status"])
    print(json.dumps(r.get("json", r.get("error", r.get("raw"))), indent=2, ensure_ascii=False))
    print("--------------------------------------------------")

    # 4) Global config + ingest sample
    page_global = "page_02_market_summary_global"
    r = http_json("GET", f"{base_url}/api/v1/config/{page_global}", token=token)
    print(f"GET /api/v1/config/{page_global} =>", r["status"])
    print(json.dumps(r.g
