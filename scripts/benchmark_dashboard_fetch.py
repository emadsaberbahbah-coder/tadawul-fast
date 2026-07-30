#!/usr/bin/env python3
"""Read-only benchmark for the production Python dashboard refresh path.

The benchmark reads the live page universe and calls the same Render/backend
routes and the same guards used by ``run_dashboard_sync.py``.  It replaces the
final Sheet writer with an in-memory sink and disables the optional _Run_Log
append, so no workbook cell is changed.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Sequence

from scripts import run_dashboard_sync as sync

BENCHMARK_VERSION = "1.0.0"


class NoWriteSheets(sync.SheetsWriter):
    """Production reader plus a write sink that never calls Google write APIs."""

    def __init__(self) -> None:
        super().__init__()
        self.planned_writes: list[dict[str, Any]] = []
        self.clear_requests: list[dict[str, Any]] = []

    def clear_from(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        start_a1: str,
    ) -> None:
        self.clear_requests.append(
            {"sheet_name": sheet_name, "start_a1": start_a1}
        )

    def write_table(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        start_a1: str,
        headers: list[Any],
        rows: list[list[Any]],
    ) -> int:
        self.planned_writes.append(
            {
                "sheet_name": sheet_name,
                "start_a1": start_a1,
                "columns": len(headers or []),
                "rows": len(rows or []),
            }
        )
        return len(rows or [])


def _task_for(value: str) -> sync.TaskSpec:
    wanted = sync._guard_norm(value)
    for task in sync._default_tasks():
        if wanted in {
            sync._guard_norm(task.key),
            sync._guard_norm(task.sheet_name),
        }:
            return task
    raise ValueError(f"Unknown benchmark page/key: {value}")


def _set_runtime_env(args: argparse.Namespace) -> None:
    os.environ["TFB_SYNC_BATCH_CONCURRENCY"] = str(args.concurrency)
    os.environ["TFB_SYNC_SYMBOL_BATCH_SIZE"] = str(args.batch_size)
    os.environ["TFB_SYNC_BATCH_OUTER_RETRIES"] = str(args.outer_retries)
    os.environ["TFB_SYNC_TIME_BUDGET_SEC"] = str(args.time_budget)
    os.environ["TFB_SYNC_IDFW_RUNLOG"] = "0"
    os.environ["TFB_XPAGE_PRICE_CHECK"] = "0"
    # The benchmark must not acquire or alter a production writer lock. It is
    # read-only by construction and performs no Sheet publication.
    sync._TIME_BUDGET_START = time.monotonic()


async def run_benchmark(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    _set_runtime_env(args)
    task = _task_for(args.page)
    sheet_id = sync._default_spreadsheet_id(args.sheet_id)
    if not sheet_id:
        raise RuntimeError("Spreadsheet ID is required")

    backend_url = (args.backend or sync._default_backend_url()).rstrip("/")
    backend = sync.BackendClient(
        backend_url,
        timeout_sec=float(args.timeout),
        token=sync._env_token(),
    )
    sheets = NoWriteSheets()
    started = time.perf_counter()
    try:
        sync._idfw_selftest_()
        result = await sync._run_one_task(
            task=task,
            spreadsheet_id=sheet_id,
            start_cell="A1",
            max_symbols_override=int(args.max_symbols),
            clear_before_write=False,
            dry_run=False,
            backend=backend,
            sheets=sheets,
        )
    finally:
        await backend.close()

    elapsed_ms = round((time.perf_counter() - started) * 1000.0)
    result_payload = result.to_dict()
    metrics = dict(result_payload.get("batch_metrics") or {})
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "benchmark_version": BENCHMARK_VERSION,
        "runner_version": sync.SCRIPT_VERSION,
        "mode": "read_live_fetch_no_write",
        "no_workbook_writes": True,
        "page": task.sheet_name,
        "backend": backend_url,
        "config": {
            "max_symbols": int(args.max_symbols),
            "batch_size": int(args.batch_size),
            "concurrency": int(args.concurrency),
            "outer_retries": int(args.outer_retries),
            "timeout_sec": float(args.timeout),
            "time_budget_sec": int(args.time_budget),
        },
        "elapsed_ms": elapsed_ms,
        "elapsed_minutes": round(elapsed_ms / 60000.0, 3),
        "result": result_payload,
        "batch_metrics": metrics,
        "planned_writes": sheets.planned_writes,
        "clear_requests": sheets.clear_requests,
    }

    requested = int(metrics.get("symbols_requested") or result.symbols_requested or 0)
    fresh = int(metrics.get("symbols_fresh") or 0)
    payload["acceptance"] = {
        "requested_symbols": requested,
        "fresh_symbols": fresh,
        "fresh_coverage_pct": metrics.get("fresh_coverage_pct"),
        "failed_symbols": metrics.get("symbols_failed"),
        "unattempted_symbols": metrics.get("symbols_unattempted"),
        "http_429": metrics.get("http_429"),
        "http_5xx": metrics.get("http_5xx"),
        "within_25_minutes": elapsed_ms <= 25 * 60 * 1000,
        "within_35_minutes": elapsed_ms <= 35 * 60 * 1000,
        "complete_fresh_fetch": bool(requested and fresh == requested),
    }

    if result.status == "failed":
        exit_code = 2
    elif result.status in {"partial", "skipped"}:
        exit_code = 1
    else:
        exit_code = 0
    return exit_code, payload


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--page", default="Market_Leaders")
    parser.add_argument("--sheet-id", default="")
    parser.add_argument("--backend", default="")
    parser.add_argument("--max-symbols", type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--outer-retries", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--time-budget", type=int, default=2100)
    parser.add_argument("--json-out", default="python_refresh_benchmark.json")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = create_parser().parse_args(argv)
    try:
        code, payload = asyncio.run(run_benchmark(args))
    except Exception as exc:
        print(f"::error::PYTHON_REFRESH_BENCHMARK_FAILED: {type(exc).__name__}: {exc}")
        return 3

    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)
    if args.json_out:
        Path(args.json_out).write_text(rendered + "\n", encoding="utf-8")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
