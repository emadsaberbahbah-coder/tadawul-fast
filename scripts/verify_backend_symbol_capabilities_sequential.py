#!/usr/bin/env python3
"""Sequential, read-only deployment gate for provider-sensitive symbols.

The underlying capability rules and response evaluation remain owned by
``verify_backend_symbol_capabilities``. This wrapper changes only execution
shape: each capability receives its own timeout and completes before the next
probe starts. That keeps the deployment gate aligned with production
``TFB_SYNC_BATCH_CONCURRENCY=1`` and avoids a three-request provider burst.

No Google Sheet read or write is performed.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Sequence

from scripts import verify_backend_symbol_capabilities as base

GATE_VERSION = "1.3.0-sequential"


async def run_gate(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    backend_url = str(args.backend or base.sync._default_backend_url()).rstrip("/")
    if not backend_url:
        raise RuntimeError("Backend URL is required")

    per_probe_timeout = max(5.0, float(args.timeout))
    backend = base.sync.BackendClient(
        backend_url,
        timeout_sec=min(120.0, per_probe_timeout),
        token=base.sync._env_token(),
    )
    started = time.perf_counter()
    probes: list[dict[str, Any]] = []
    try:
        meta, meta_error, meta_status = await backend.get_json("/meta")
        for index, rule in enumerate(base.RULES):
            probes.append(
                await base._probe_one(
                    backend,
                    args.endpoint,
                    args.page,
                    rule,
                    index,
                    per_probe_timeout,
                )
            )
    finally:
        await backend.close()

    ready = all(bool(item.get("passed")) for item in probes)
    payload: dict[str, Any] = {
        "schema_version": "1.2",
        "gate_version": GATE_VERSION,
        "mode": "live_backend_read_only_capability_probe",
        "execution_mode": "sequential",
        "no_workbook_reads": True,
        "no_workbook_writes": True,
        "backend": backend_url,
        "endpoint": args.endpoint,
        "page": args.page,
        "per_probe_timeout_sec": per_probe_timeout,
        "ready_for_full_benchmark": ready,
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0),
        "meta_http_status": int(meta_status or 0),
        "meta_error": meta_error,
        "backend_meta": meta if isinstance(meta, dict) else {},
        "required_capabilities": [asdict(rule) for rule in base.RULES],
        "probes": probes,
        "failed_capabilities": [
            str(item.get("capability") or "")
            for item in probes
            if not bool(item.get("passed"))
        ],
        "truthfully_unavailable_capabilities": [
            str(item.get("capability") or "")
            for item in probes
            if item.get("pass_mode") == "truthful_unavailable"
        ],
    }
    return (0 if ready else 2), payload


def create_parser() -> argparse.ArgumentParser:
    parser = base.create_parser()
    parser.description = __doc__
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = create_parser().parse_args(argv)
    try:
        code, payload = asyncio.run(run_gate(args))
    except Exception as exc:
        print(
            "::error::BACKEND_CAPABILITY_GATE_FAILED: "
            f"{type(exc).__name__}: {exc}"
        )
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
