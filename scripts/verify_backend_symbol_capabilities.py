#!/usr/bin/env python3
"""Fail-closed deployment capability gate for the live Render backend.

This script performs a tiny, read-only probe against the same analysis
``sheet-rows`` route used by the dashboard sync. It proves that the deployed
backend can return valid identities for provider-sensitive symbols before the
expensive 1,000-symbol benchmark is allowed to run.

It never reads from or writes to Google Sheets.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from scripts import run_dashboard_sync as sync

GATE_VERSION = "1.0.1"


@dataclass(frozen=True)
class ProbeRule:
    requested_symbol: str
    accepted_symbols: tuple[str, ...]
    accepted_name_tokens: tuple[str, ...]
    capability: str


RULES: tuple[ProbeRule, ...] = (
    ProbeRule(
        requested_symbol="ADNOCDIST.AB",
        accepted_symbols=("ADNOCDIST.AB", "ADNOCDIST.ADX"),
        accepted_name_tokens=("adnoc distribution",),
        capability="yahoo_ab_to_eodhd_adx",
    ),
    ProbeRule(
        requested_symbol="BPI.PS",
        accepted_symbols=("BPI.PS", "BPI.PSE"),
        accepted_name_tokens=("bank of the philippine islands", "bpi"),
        capability="yahoo_ps_to_eodhd_pse",
    ),
    ProbeRule(
        requested_symbol="BK.US",
        accepted_symbols=("BK.US", "BK"),
        accepted_name_tokens=("bank of new york mellon", "bny mellon"),
        capability="bk_exact_identity",
    ),
)


def _norm(value: Any) -> str:
    return "".join(character for character in str(value or "").strip().casefold() if character.isalnum())


def _find_column(headers: Sequence[Any], aliases: Iterable[str]) -> int:
    wanted = {_norm(alias) for alias in aliases}
    for index, header in enumerate(headers):
        if _norm(header) in wanted:
            return index
    return -1


def _positive(value: Any) -> bool:
    try:
        parsed = float(str(value).replace(",", "").strip())
    except Exception:
        return False
    return 0.0 < parsed < 1e15


def _provider_error(value: Any) -> bool:
    checker = getattr(sync, "_klg_provider_is_error", None)
    if callable(checker):
        try:
            return bool(checker(value))
        except Exception:
            pass
    return _norm(value) in {"fallbackerror", "error", "unavailable", "none"}


def _accepted_symbol(value: Any, rule: ProbeRule) -> bool:
    symbol = str(value or "").strip().upper()
    return symbol in {item.upper() for item in rule.accepted_symbols}


def evaluate_table(
    rule: ProbeRule,
    headers: Sequence[Any],
    rows: Sequence[Sequence[Any]],
) -> dict[str, Any]:
    """Evaluate one probe response without network access.

    The gate requires a matching symbol, a non-blank issuer name, a positive
    current price, a non-error provider marker, and an accepted issuer token.
    Missing columns are reported explicitly rather than treated as zero/blank.
    """
    symbol_index = _find_column(headers, ("Symbol", "Ticker", "Code"))
    name_index = _find_column(headers, ("Name", "Company Name", "Instrument Name", "Short Name"))
    price_index = _find_column(headers, ("Current Price", "Price", "Last Price", "Last"))
    provider_index = _find_column(headers, ("Data Provider", "Provider", "Data Source", "Source"))

    missing_columns: list[str] = []
    if symbol_index < 0:
        missing_columns.append("symbol")
    if name_index < 0:
        missing_columns.append("name")
    if price_index < 0:
        missing_columns.append("current_price")
    if provider_index < 0:
        missing_columns.append("data_provider")

    outcome: dict[str, Any] = {
        "capability": rule.capability,
        "requested_symbol": rule.requested_symbol,
        "accepted_symbols": list(rule.accepted_symbols),
        "passed": False,
        "reason": "",
        "missing_columns": missing_columns,
        "seen_symbol": "",
        "seen_name": "",
        "seen_price": None,
        "seen_provider": "",
    }
    if missing_columns:
        outcome["reason"] = "required response columns missing"
        return outcome

    selected: Sequence[Any] | None = None
    for row in rows:
        if not isinstance(row, (list, tuple)) or symbol_index >= len(row):
            continue
        if _accepted_symbol(row[symbol_index], rule):
            selected = row
            break

    if selected is None:
        outcome["reason"] = "requested identity absent from response"
        return outcome

    symbol = str(selected[symbol_index] or "").strip().upper()
    name = str(selected[name_index] or "").strip() if name_index < len(selected) else ""
    price = selected[price_index] if price_index < len(selected) else None
    provider = str(selected[provider_index] or "").strip() if provider_index < len(selected) else ""
    outcome.update(
        seen_symbol=symbol,
        seen_name=name[:120],
        seen_price=price,
        seen_provider=provider[:80],
    )

    if not name:
        outcome["reason"] = "blank instrument name"
        return outcome
    if not any(token in name.casefold() for token in rule.accepted_name_tokens):
        outcome["reason"] = "issuer name mismatch"
        return outcome
    if not _positive(price):
        outcome["reason"] = "missing or non-positive current price"
        return outcome
    if not provider or _provider_error(provider):
        outcome["reason"] = "provider returned an error/stub marker"
        return outcome

    outcome["passed"] = True
    outcome["reason"] = "capability proven"
    return outcome


def _payload(rule: ProbeRule, page: str, request_id: str) -> dict[str, Any]:
    # Include the aliases accepted across the analysis-router generations. Extra
    # keys are intentional compatibility fields; the production runner sends the
    # same symbol/page concepts through this route.
    return {
        "sheet": page,
        "page": page,
        "sheet_name": page,
        "page_name": page,
        "tickers": [rule.requested_symbol],
        "symbols": [rule.requested_symbol],
        "limit": 1,
        "offset": 0,
        "request_id": request_id,
    }


async def _probe_one(
    backend: sync.BackendClient,
    endpoint: str,
    page: str,
    rule: ProbeRule,
    position: int,
) -> dict[str, Any]:
    request_id = f"deploy-capability-{int(time.time())}-{position + 1}"
    data, error, status_code = await backend.post_json(
        endpoint,
        _payload(rule, page, request_id),
    )
    if error or not isinstance(data, dict):
        return {
            "capability": rule.capability,
            "requested_symbol": rule.requested_symbol,
            "accepted_symbols": list(rule.accepted_symbols),
            "passed": False,
            "reason": error or "non-dict response",
            "http_status": int(status_code or 0),
            "request_id": request_id,
        }

    headers, raw_rows = sync._extract_table_payload(data)
    rows = list(sync._rectify_matrix(headers, raw_rows) or []) if headers else []
    outcome = evaluate_table(rule, list(headers or []), rows)
    outcome.update(
        http_status=int(status_code or 0),
        request_id=request_id,
        response_status=str(data.get("status") or ""),
        response_source=str((data.get("meta") or {}).get("source") or "")
        if isinstance(data.get("meta"), dict)
        else "",
    )
    return outcome


async def run_gate(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    backend_url = str(args.backend or sync._default_backend_url()).rstrip("/")
    if not backend_url:
        raise RuntimeError("Backend URL is required")

    backend = sync.BackendClient(
        backend_url,
        timeout_sec=float(args.timeout),
        token=sync._env_token(),
    )
    started = time.perf_counter()
    try:
        meta, meta_error, meta_status = await backend.get_json("/meta")
        probes = list(
            await asyncio.gather(
                *[
                    _probe_one(backend, args.endpoint, args.page, rule, index)
                    for index, rule in enumerate(RULES)
                ]
            )
        )
    finally:
        await backend.close()

    ready = all(bool(item.get("passed")) for item in probes)
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "gate_version": GATE_VERSION,
        "mode": "live_backend_read_only_capability_probe",
        "no_workbook_reads": True,
        "no_workbook_writes": True,
        "backend": backend_url,
        "endpoint": args.endpoint,
        "page": args.page,
        "ready_for_full_benchmark": ready,
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0),
        "meta_http_status": int(meta_status or 0),
        "meta_error": meta_error,
        "backend_meta": meta if isinstance(meta, dict) else {},
        "required_capabilities": [asdict(rule) for rule in RULES],
        "probes": probes,
        "failed_capabilities": [
            str(item.get("capability") or "")
            for item in probes
            if not bool(item.get("passed"))
        ],
    }
    return (0 if ready else 2), payload


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", default="")
    parser.add_argument("--endpoint", default="/v1/analysis/sheet-rows")
    parser.add_argument("--page", default="Market_Leaders")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--json-out", default="backend_symbol_capabilities.json")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = create_parser().parse_args(argv)
    try:
        code, payload = asyncio.run(run_gate(args))
    except Exception as exc:
        print(f"::error::BACKEND_CAPABILITY_GATE_FAILED: {type(exc).__name__}: {exc}")
        return 3

    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)
    if args.json_out:
        Path(args.json_out).write_text(rendered + "\n", encoding="utf-8")
    if code != 0:
        failed = ", ".join(payload.get("failed_capabilities") or []) or "unknown"
        print(f"::error::DEPLOYED_BACKEND_CAPABILITIES_MISSING: {failed}")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
