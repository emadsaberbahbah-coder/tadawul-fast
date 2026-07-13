#!/usr/bin/env python3
"""Schema-first integrity audit for the live Tadawul Fast Bridge backend.

The historical endpoint smoke test carries old minimum widths.  This auditor
uses the checked-out ``schema_registry`` as the authority, requires exact
header/key order, and checks the decision-gate invariants on small live samples.
It is intentionally read-only: no Google Sheet or backend data is modified.

Exit codes
----------
0  clean
1  warnings only
2  contract / gate failures
3  auditor could not run (configuration/import/connectivity)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import requests

from core.sheets import schema_registry as schema


SCRIPT_VERSION = "1.0.0"
DEFAULT_BASE_URL = "https://tadawul-fast-bridge.onrender.com"
DEFAULT_ENDPOINT = "/v1/analysis/sheet-rows"

PAGE_SAMPLES: Dict[str, List[str]] = {
    "Market_Leaders": ["2222.SR", "1120.SR"],
    "Global_Markets": ["AAPL", "MSFT"],
    "Commodities_FX": ["GC=F", "EURUSD=X"],
    "Mutual_Funds": ["SPY", "BND"],
}

SELL_FAMILY = {"REDUCE", "SELL", "STRONG_SELL", "AVOID"}


@dataclass
class Finding:
    severity: str
    check: str
    page: str = ""
    symbol: str = ""
    detail: str = ""


class Audit:
    def __init__(self) -> None:
        self.findings: List[Finding] = []
        self.page_meta: Dict[str, Dict[str, Any]] = {}

    def add(self, severity: str, check: str, detail: str, *, page: str = "", symbol: str = "") -> None:
        finding = Finding(severity, check, page, symbol, detail)
        self.findings.append(finding)
        prefix = f"[{severity}] {check}"
        where = " ".join(x for x in (page, symbol) if x)
        print(f"{prefix}{' ' + where if where else ''}: {detail}")

    @property
    def failures(self) -> int:
        return sum(f.severity in {"HIGH", "CRITICAL"} for f in self.findings)

    @property
    def warnings(self) -> int:
        return sum(f.severity == "WARN" for f in self.findings)


def _norm_token(value: Any) -> str:
    text = str(value or "").strip().upper().replace("-", "_").replace(" ", "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"", "none", "null", "nan", "-", "--"}
    return False


def _as_float(value: Any) -> Optional[float]:
    if _is_blank(value):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result != result or result in (float("inf"), float("-inf")):
        return None
    return result


def _first_nonblank(mapping: Mapping[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        if name in mapping and not _is_blank(mapping.get(name)):
            return mapping.get(name)
    return None


def _unwrap_payload(value: Any) -> Dict[str, Any]:
    """Find the first schema-shaped dictionary without discarding metadata."""
    if not isinstance(value, dict):
        return {}
    if any(k in value for k in ("headers", "keys", "rows", "items")):
        return value
    for key in ("data", "result", "payload", "response"):
        child = value.get(key)
        if isinstance(child, dict):
            unwrapped = _unwrap_payload(child)
            if unwrapped:
                return unwrapped
    return value


def _extract_rows(payload: Mapping[str, Any]) -> List[Any]:
    for key in ("rows", "items", "data", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _extract_rows(value)
            if nested:
                return nested
    return []


def _row_as_mapping(row: Any, keys: Sequence[str], headers: Sequence[str]) -> Dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    if isinstance(row, (list, tuple)):
        out: Dict[str, Any] = {}
        for idx, value in enumerate(row):
            if idx < len(keys):
                out[keys[idx]] = value
            if idx < len(headers):
                out.setdefault(headers[idx], value)
        return out
    return {}


def _field(row: Mapping[str, Any], key: str, header: str) -> Any:
    return _first_nonblank(row, (key, header))


def _auth_headers(token: str) -> Dict[str, str]:
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if token:
        headers["X-APP-TOKEN"] = token
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: Mapping[str, str],
    timeout: float,
    payload: Optional[Mapping[str, Any]] = None,
) -> Tuple[int, Any, float, str]:
    started = time.monotonic()
    try:
        response = session.request(
            method,
            url,
            headers=dict(headers),
            json=dict(payload) if payload is not None else None,
            timeout=timeout,
        )
        elapsed = time.monotonic() - started
        try:
            body = response.json()
        except Exception:
            body = None
        return response.status_code, body, elapsed, response.text[:500]
    except requests.RequestException as exc:
        return 0, None, time.monotonic() - started, str(exc)


def _check_health(
    audit: Audit,
    session: requests.Session,
    base_url: str,
    headers: Mapping[str, str],
    timeout: float,
) -> None:
    successes = 0
    for path in ("/readyz", "/health", "/livez", "/meta"):
        status, body, elapsed, raw = _request_json(
            session, "GET", f"{base_url}{path}", headers=headers, timeout=timeout
        )
        if 200 <= status < 300:
            successes += 1
            version = ""
            if isinstance(body, dict):
                version = str(
                    body.get("entry_version")
                    or body.get("app_version")
                    or body.get("version")
                    or ""
                )
            print(f"[PASS] health {path}: HTTP {status} in {elapsed:.2f}s"
                  + (f" version={version}" if version else ""))
        else:
            audit.add(
                "WARN",
                "health.endpoint",
                f"HTTP {status or 'CONNECT_ERROR'} in {elapsed:.2f}s; body={raw!r}",
                page=path,
            )
    if successes == 0:
        audit.add("CRITICAL", "health.all_failed", "No health/meta endpoint returned 2xx")


def _check_contract(
    audit: Audit,
    page: str,
    payload: Mapping[str, Any],
    rows: Sequence[Any],
) -> Tuple[List[str], List[str]]:
    expected_headers = list(schema.get_sheet_headers(page))
    expected_keys = list(schema.get_sheet_keys(page))
    response_headers = list(payload.get("headers") or [])
    response_keys = list(payload.get("keys") or [])

    if response_headers != expected_headers:
        first = next(
            (
                i
                for i in range(max(len(response_headers), len(expected_headers)))
                if (response_headers[i] if i < len(response_headers) else "<missing>")
                != (expected_headers[i] if i < len(expected_headers) else "<extra>")
            ),
            None,
        )
        audit.add(
            "CRITICAL",
            "contract.headers_exact",
            f"expected {len(expected_headers)}, got {len(response_headers)}; first divergence={first}",
            page=page,
        )
    else:
        print(f"[PASS] contract.headers_exact {page}: {len(expected_headers)} columns")

    if response_keys != expected_keys:
        first = next(
            (
                i
                for i in range(max(len(response_keys), len(expected_keys)))
                if (response_keys[i] if i < len(response_keys) else "<missing>")
                != (expected_keys[i] if i < len(expected_keys) else "<extra>")
            ),
            None,
        )
        audit.add(
            "CRITICAL",
            "contract.keys_exact",
            f"expected {len(expected_keys)}, got {len(response_keys)}; first divergence={first}",
            page=page,
        )
    else:
        print(f"[PASS] contract.keys_exact {page}: {len(expected_keys)} keys")

    for idx, row in enumerate(rows, start=1):
        if isinstance(row, (list, tuple)) and len(row) != len(expected_keys):
            audit.add(
                "HIGH",
                "contract.row_width",
                f"row {idx} width={len(row)}, expected={len(expected_keys)}",
                page=page,
            )

    return response_headers or expected_headers, response_keys or expected_keys


def _check_rows(
    audit: Audit,
    page: str,
    rows: Sequence[Any],
    headers: Sequence[str],
    keys: Sequence[str],
) -> None:
    if not rows:
        audit.add("HIGH", "rows.nonempty", "Endpoint returned zero rows", page=page)
        return

    seen: set[str] = set()
    for raw_row in rows:
        row = _row_as_mapping(raw_row, keys, headers)
        symbol = str(_field(row, "symbol", "Symbol") or "").strip().upper()
        if not symbol:
            audit.add("HIGH", "row.symbol", "Returned row has no symbol", page=page)
            continue
        if symbol in seen:
            audit.add("HIGH", "row.duplicate_symbol", "Duplicate symbol in response", page=page, symbol=symbol)
        seen.add(symbol)

        reco = _norm_token(_field(row, "recommendation", "Recommendation"))
        action = _norm_token(_field(row, "final_action", "Final Action"))
        status = _norm_token(_field(row, "investability_status", "Investability Status"))
        block_reason = str(_field(row, "block_reason", "Block Reason") or "").strip()
        price = _as_float(_field(row, "current_price", "Current Price"))

        if action == "INVEST":
            if reco in SELL_FAMILY:
                audit.add(
                    "CRITICAL",
                    "gate.no_invest_on_sell",
                    f"recommendation={reco}, final_action=INVEST",
                    page=page,
                    symbol=symbol,
                )
            if status and status != "INVESTABLE":
                audit.add(
                    "CRITICAL",
                    "gate.invest_requires_investable",
                    f"investability_status={status}",
                    page=page,
                    symbol=symbol,
                )
            if block_reason:
                audit.add(
                    "CRITICAL",
                    "gate.invest_has_block_reason",
                    block_reason[:240],
                    page=page,
                    symbol=symbol,
                )
            if price is None or price <= 0:
                audit.add(
                    "CRITICAL",
                    "gate.invest_requires_price",
                    f"current_price={price!r}",
                    page=page,
                    symbol=symbol,
                )

        conflict = _norm_token(_field(row, "provider_engine_conflict", "Provider/Engine Conflict"))
        conflict_type = str(_field(row, "conflict_type", "Conflict Type") or "").strip()
        if conflict in {"TRUE", "1", "YES"} and not conflict_type:
            audit.add(
                "HIGH",
                "gate.conflict_requires_type",
                "provider_engine_conflict is true but conflict_type is blank",
                page=page,
                symbol=symbol,
            )

        provider = _norm_token(_field(row, "data_provider", "Data Provider"))
        if provider in {"FALLBACK_ERROR", "ERROR", "UNAVAILABLE", "NONE"}:
            audit.add(
                "WARN",
                "row.degraded_provider",
                f"data_provider={provider}",
                page=page,
                symbol=symbol,
            )
        if price is None or price <= 0:
            audit.add(
                "WARN",
                "row.missing_price",
                f"current_price={price!r}",
                page=page,
                symbol=symbol,
            )


def _audit_page(
    audit: Audit,
    session: requests.Session,
    base_url: str,
    endpoint: str,
    page: str,
    symbols: Sequence[str],
    headers: Mapping[str, str],
    timeout: float,
) -> None:
    body = {
        "page": page,
        "sheet": page,
        "symbols": list(symbols),
        "tickers": list(symbols),
        "direct_symbols": list(symbols),
        "limit": max(len(symbols), 1),
        "mode": "live",
    }
    status, data, elapsed, raw = _request_json(
        session,
        "POST",
        f"{base_url}{endpoint}",
        headers=headers,
        timeout=timeout,
        payload=body,
    )
    if not 200 <= status < 300:
        audit.add(
            "CRITICAL",
            "endpoint.http",
            f"HTTP {status or 'CONNECT_ERROR'} in {elapsed:.2f}s; body={raw!r}",
            page=page,
        )
        return

    payload = _unwrap_payload(data)
    rows = _extract_rows(payload)
    audit.page_meta[page] = {
        "http_status": status,
        "duration_sec": round(elapsed, 3),
        "row_count": len(rows),
        "status": payload.get("status") if isinstance(payload, dict) else None,
        "warnings": payload.get("warnings") if isinstance(payload, dict) else None,
        "meta": payload.get("meta") if isinstance(payload, dict) else None,
    }
    response_headers, response_keys = _check_contract(audit, page, payload, rows)
    _check_rows(audit, page, rows, response_headers, response_keys)
    print(f"[INFO] page {page}: rows={len(rows)} elapsed={elapsed:.2f}s")


def _write_report(path: Path, audit: Audit, base_url: str, endpoint: str) -> None:
    payload = {
        "audit_version": SCRIPT_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "base_url": base_url,
        "endpoint": endpoint,
        "schema_version": str(getattr(schema, "SCHEMA_VERSION", "unknown")),
        "summary": {
            "findings": len(audit.findings),
            "failures": audit.failures,
            "warnings": audit.warnings,
            "pages": len(audit.page_meta),
        },
        "pages": audit.page_meta,
        "findings": [asdict(f) for f in audit.findings],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    print(f"[INFO] report written: {path}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=(os.getenv("TFB_BASE_URL") or os.getenv("BACKEND_BASE_URL") or DEFAULT_BASE_URL),
    )
    parser.add_argument("--endpoint", default=os.getenv("TFB_AUDIT_ENDPOINT", DEFAULT_ENDPOINT))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("TFB_AUDIT_TIMEOUT", "90")))
    parser.add_argument(
        "--pages",
        default=",".join(PAGE_SAMPLES),
        help="Comma-separated market pages to audit",
    )
    parser.add_argument(
        "--json-out",
        default=os.getenv("TFB_AUDIT_JSON_OUT", "artifacts/live_backend_integrity.json"),
    )
    parser.add_argument("--strict-warnings", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    base_url = str(args.base_url or "").strip().rstrip("/")
    endpoint = "/" + str(args.endpoint or DEFAULT_ENDPOINT).strip().lstrip("/")
    if not base_url.startswith(("http://", "https://")):
        print(f"FATAL: invalid base URL: {base_url!r}", file=sys.stderr)
        return 3

    selected_pages = [p.strip() for p in str(args.pages).split(",") if p.strip()]
    unknown = [p for p in selected_pages if p not in PAGE_SAMPLES]
    if unknown:
        print(f"FATAL: unsupported pages: {unknown}", file=sys.stderr)
        return 3

    token = (
        os.getenv("TFB_APP_TOKEN")
        or os.getenv("TFB_TOKEN")
        or os.getenv("APP_TOKEN")
        or ""
    ).strip()
    audit = Audit()
    session = requests.Session()
    headers = _auth_headers(token)

    print(f"TFB live backend integrity audit v{SCRIPT_VERSION}")
    print(f"Backend: {base_url}")
    print(f"Schema:  {getattr(schema, 'SCHEMA_VERSION', 'unknown')}")
    _check_health(audit, session, base_url, headers, min(max(args.timeout, 5.0), 30.0))

    if not audit.failures:
        for page in selected_pages:
            _audit_page(
                audit,
                session,
                base_url,
                endpoint,
                page,
                PAGE_SAMPLES[page],
                headers,
                max(args.timeout, 5.0),
            )

    _write_report(Path(args.json_out), audit, base_url, endpoint)
    print(
        f"SUMMARY failures={audit.failures} warnings={audit.warnings} "
        f"findings={len(audit.findings)}"
    )
    if audit.failures:
        return 2
    if audit.warnings:
        return 2 if args.strict_warnings else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
