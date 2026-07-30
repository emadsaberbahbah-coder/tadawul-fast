#!/usr/bin/env python3
"""Bounded-concurrency market batch fetcher for ``run_dashboard_sync``.

The module changes only provider-fetch scheduling. The production runner keeps
ownership of symbol read-back, persistence, keep-last-good, identity firewalls,
Sheet publication, and exit-code policy.
"""
from __future__ import annotations

import asyncio
import math
import os
import statistics
import time
from typing import Any, Awaitable, Callable, Iterable, Sequence

VERSION = "1.1.0"
_METRICS: dict[str, dict[str, Any]] = {}


def _int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        value = int(float(os.getenv(name, str(default))))
    except Exception:
        value = default
    return max(lo, min(hi, value))


def concurrency() -> int:
    """Configured concurrent provider requests; 1 is the safe rollback mode."""
    return _int("TFB_SYNC_BATCH_CONCURRENCY", 3, 1, 6)


def outer_retries() -> int:
    """Extra batch-level passes after BackendClient exhausts its own retries."""
    return _int("TFB_SYNC_BATCH_OUTER_RETRIES", 1, 0, 2)


def get_metrics(request_id: str) -> dict[str, Any] | None:
    value = _METRICS.get(str(request_id or ""))
    return dict(value) if value is not None else None


def _percentile(values: Sequence[float], percentile: float) -> float:
    clean = sorted(float(v) for v in values if v is not None)
    if not clean:
        return 0.0
    if len(clean) == 1:
        return clean[0]
    position = (len(clean) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return clean[lower]
    weight = position - lower
    return clean[lower] * (1.0 - weight) + clean[upper] * weight


def _same_headers(left: Sequence[Any], right: Sequence[Any]) -> bool:
    return [str(x or "").strip() for x in left] == [
        str(x or "").strip() for x in right
    ]


def build(
    sync: Any,
) -> Callable[
    ...,
    Awaitable[tuple[list[Any], list[list[Any]], str | None, str | None]],
]:
    """Build a concurrent fetch function against the current runner module."""

    async def one(
        backend: Any,
        idx: int,
        batch: Sequence[str],
        payload: dict[str, Any],
        endpoints: Sequence[str],
        req_id: str,
        *,
        delay_ms: int = 0,
    ) -> dict[str, Any]:
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)
        if sync._time_budget_exceeded():
            return {
                "i": idx,
                "b": list(batch),
                "h": [],
                "r": [],
                "e": "time budget exhausted",
                "ep": None,
                "attempted": False,
                "ms": 0.0,
                "code": 0,
            }

        body = dict(payload)
        body.update(
            tickers=list(batch),
            symbols=list(batch),
            limit=min(sync._request_limit_ceiling(), max(1, len(batch))),
            request_id=req_id,
        )
        started = time.perf_counter()
        last_error: str | None = None
        last_code = 0
        for endpoint in endpoints:
            data, error, code = await backend.post_json(endpoint, body)
            last_code = int(code or 0)
            if error:
                last_error = f"{endpoint} -> {error}"
                continue
            if not isinstance(data, dict):
                last_error = f"{endpoint} -> Non-dict response"
                continue
            headers, rows = sync._extract_table_payload(data)
            if not headers:
                last_error = f"{endpoint} -> Missing headers"
                continue
            return {
                "i": idx,
                "b": list(batch),
                "h": list(headers),
                "r": list(sync._rectify_matrix(headers, rows) or []),
                "e": None,
                "ep": endpoint,
                "attempted": True,
                "ms": (time.perf_counter() - started) * 1000.0,
                "code": last_code,
            }
        return {
            "i": idx,
            "b": list(batch),
            "h": [],
            "r": [],
            "e": last_error or "all endpoints failed",
            "ep": None,
            "attempted": True,
            "ms": (time.perf_counter() - started) * 1000.0,
            "code": last_code,
        }

    async def fan(
        backend: Any,
        indexed_batches: Sequence[tuple[int, Sequence[str]]],
        payload: dict[str, Any],
        endpoint: str,
        request_id: str,
        *,
        retry_round: int,
        max_concurrency: int,
        delay_ms: int,
    ) -> list[dict[str, Any]]:
        semaphore = asyncio.Semaphore(max_concurrency)

        async def guarded(
            position: int,
            idx: int,
            batch: Sequence[str],
        ) -> dict[str, Any]:
            async with semaphore:
                stagger = (position % max_concurrency) * delay_ms
                label = "b" if retry_round == 0 else f"r{retry_round}-"
                return await one(
                    backend,
                    idx,
                    batch,
                    payload,
                    (endpoint,),
                    f"{request_id}-{label}{idx + 1}",
                    delay_ms=stagger,
                )

        tasks = [
            asyncio.create_task(guarded(pos, idx, batch))
            for pos, (idx, batch) in enumerate(indexed_batches)
        ]
        return list(await asyncio.gather(*tasks)) if tasks else []

    def normalize_outcomes(
        outcomes: Iterable[dict[str, Any]],
        canonical_headers: Sequence[Any],
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for outcome in outcomes:
            item = dict(outcome)
            if item.get("h") and not _same_headers(canonical_headers, item["h"]):
                item["e"] = "header mismatch against endpoint-resolving batch"
                item["h"] = []
                item["r"] = []
            normalized.append(item)
        return normalized

    def merge(
        headers: Sequence[Any],
        outcomes: Sequence[dict[str, Any]],
        symbols: Sequence[str],
        result: Any,
    ) -> list[list[Any]]:
        symbol_index = sync._guard_find_col(
            list(headers), sync._GUARD_SYMBOL_ALIASES
        )
        if not sync._batch_identity_enabled() or symbol_index < 0:
            return [
                list(row)
                for outcome in sorted(outcomes, key=lambda x: x["i"])
                for row in outcome.get("r", [])
            ]

        rows_by_symbol: dict[str, list[Any]] = {}
        bleed = duplicate = blank = 0
        for outcome in sorted(outcomes, key=lambda x: x["i"]):
            requested = {
                sync.canonicalize_symbol(x) for x in outcome["b"]
            }
            requested.discard("")
            for raw in outcome.get("r", []):
                if (
                    not isinstance(raw, (list, tuple))
                    or symbol_index >= len(raw)
                    or sync._guard_is_blank(raw[symbol_index])
                ):
                    blank += 1
                    continue
                row = list(raw)
                symbol = sync.canonicalize_symbol(row[symbol_index])
                row[symbol_index] = symbol
                if symbol not in requested:
                    bleed += 1
                    continue
                if symbol in rows_by_symbol:
                    duplicate += 1
                    continue
                rows_by_symbol[symbol] = row

        if bleed or duplicate or blank:
            message = (
                f"{sync._BATCH_IDENTITY_TAG} concurrent fold dropped "
                f"cross_batch={bleed} duplicate={duplicate} blank={blank}"
            )
            result.warnings.append(message)
            sync.logger.warning(message)

        return [
            rows_by_symbol[symbol]
            for symbol in (
                sync.canonicalize_symbol(x) for x in symbols
            )
            if symbol in rows_by_symbol
        ]

    async def concurrent(
        backend: Any,
        task: Any,
        symbols: list[str],
        payload: dict[str, Any],
        gateway: str,
        result: Any,
    ) -> tuple[list[Any], list[list[Any]], str | None, str | None]:
        started = time.perf_counter()
        size = sync._symbol_batch_size()
        batches = sync.build_isolated_batches(symbols, size)
        max_concurrency = concurrency()
        candidates = sync._endpoint_candidates_for_gateway(gateway)
        request_id = str(result.request_id)
        metrics: dict[str, Any] = {
            "version": VERSION,
            "mode": "concurrent",
            "page": task.sheet_name,
            "concurrency": max_concurrency,
            "batch_size": size,
            "batches_total": len(batches),
            "symbols_requested": len(symbols),
        }
        _METRICS[request_id] = metrics
        if not batches:
            metrics.update(
                batches_attempted=0,
                batches_succeeded=0,
                batches_failed=0,
                batches_unattempted=0,
                symbols_attempted=0,
                symbols_fresh=0,
                symbols_failed=0,
                symbols_unattempted=0,
                fresh_coverage_pct=0.0,
                elapsed_ms=0,
                endpoint_resolve_batches=0,
                retry_rounds=0,
            )
            if hasattr(result, "batch_metrics"):
                result.batch_metrics = dict(metrics)
            return [], [], None, "no batches"

        outcomes: list[dict[str, Any]] = []
        endpoint: str | None = None
        headers: list[Any] = []
        last_error: str | None = None
        resolve_count = 0

        for idx, batch in enumerate(batches):
            if sync._time_budget_exceeded():
                break
            outcome = await one(
                backend,
                idx,
                batch,
                payload,
                candidates,
                f"{request_id}-resolve-{idx + 1}",
            )
            outcomes.append(outcome)
            resolve_count += 1
            if outcome.get("e"):
                last_error = outcome["e"]
            if outcome.get("h"):
                endpoint = outcome["ep"]
                headers = list(outcome["h"])
                break

        attempted_indices = {int(item["i"]) for item in outcomes}
        unresolved_rest = [
            (idx, batch)
            for idx, batch in enumerate(batches)
            if idx not in attempted_indices
        ]
        if endpoint:
            fetched = await fan(
                backend,
                unresolved_rest,
                payload,
                endpoint,
                request_id,
                retry_round=0,
                max_concurrency=max_concurrency,
                delay_ms=sync._batch_delay_ms(),
            )
            outcomes.extend(normalize_outcomes(fetched, headers))
        else:
            outcomes.extend(
                {
                    "i": idx,
                    "b": list(batch),
                    "h": [],
                    "r": [],
                    "e": "endpoint unresolved",
                    "ep": None,
                    "attempted": False,
                    "ms": 0.0,
                    "code": 0,
                }
                for idx, batch in unresolved_rest
            )

        retry_rounds_done = 0
        for retry_round in range(1, outer_retries() + 1):
            failed = [
                (int(item["i"]), list(item["b"]))
                for item in outcomes
                if item.get("attempted") and not item.get("h")
            ]
            if not failed or not endpoint or sync._time_budget_exceeded():
                break
            retried = await fan(
                backend,
                failed,
                payload,
                endpoint,
                request_id,
                retry_round=retry_round,
                max_concurrency=max_concurrency,
                delay_ms=sync._batch_delay_ms(),
            )
            retried = normalize_outcomes(retried, headers)
            by_index = {int(item["i"]): item for item in outcomes}
            for item in retried:
                index = int(item["i"])
                if item.get("h"):
                    by_index[index] = item
                elif item.get("e"):
                    last_error = item["e"]
            outcomes = [by_index[index] for index in sorted(by_index)]
            retry_rounds_done += 1

        outcomes = sorted(outcomes, key=lambda x: int(x["i"]))
        if not headers:
            headers = list(
                next((item["h"] for item in outcomes if item.get("h")), [])
            )
        rows = merge(headers, outcomes, symbols, result) if headers else []

        attempted = [item for item in outcomes if item.get("attempted")]
        succeeded = [item for item in outcomes if item.get("h")]
        failed = [item for item in attempted if not item.get("h")]
        unattempted = [
            item for item in outcomes if not item.get("attempted")
        ]
        durations = [
            float(item.get("ms") or 0.0) for item in attempted
        ]
        symbols_attempted = sum(len(item["b"]) for item in attempted)
        symbols_failed = sum(len(item["b"]) for item in failed)
        symbols_unattempted = sum(
            len(item["b"]) for item in unattempted
        )
        coverage = (
            100.0 * len(rows) / len(symbols) if symbols else 100.0
        )
        metrics.update(
            endpoint=endpoint,
            endpoint_resolve_batches=resolve_count,
            retry_rounds=retry_rounds_done,
            batches_attempted=len(attempted),
            batches_succeeded=len(succeeded),
            batches_failed=len(failed),
            batches_unattempted=len(unattempted),
            symbols_attempted=symbols_attempted,
            symbols_fresh=len(rows),
            symbols_failed=symbols_failed,
            symbols_unattempted=symbols_unattempted,
            fresh_coverage_pct=round(coverage, 3),
            http_429=sum(
                1 for item in attempted if item.get("code") == 429
            ),
            http_5xx=sum(
                1
                for item in attempted
                if 500 <= int(item.get("code") or 0) < 600
            ),
            elapsed_ms=round(
                (time.perf_counter() - started) * 1000.0
            ),
            mean_batch_ms=(
                round(statistics.fmean(durations)) if durations else 0
            ),
            p50_batch_ms=round(_percentile(durations, 0.50)),
            p95_batch_ms=round(_percentile(durations, 0.95)),
            max_batch_ms=round(max(durations, default=0.0)),
        )
        _METRICS[request_id] = dict(metrics)
        if hasattr(result, "batch_metrics"):
            result.batch_metrics = dict(metrics)

        message = (
            f"[BATCH-CONCURRENCY v{VERSION}] page={task.sheet_name} "
            f"concurrency={max_concurrency} batch_size={size} "
            f"batches={len(succeeded)}/{len(batches)} "
            f"attempted={symbols_attempted} fresh={len(rows)} "
            f"failed={symbols_failed} unattempted={symbols_unattempted} "
            f"coverage={coverage:.1f}% elapsed_ms={metrics['elapsed_ms']} "
            f"p95_batch_ms={metrics['p95_batch_ms']}"
        )
        result.warnings.append(message)
        sync.logger.info(message)
        return headers, rows, endpoint, last_error

    return concurrent


def install(sync: Any) -> None:
    """Install a reversible dispatcher on an imported production runner."""
    if getattr(sync, "_TFB_CONCURRENT_BATCH_FETCH_INSTALLED", False):
        return

    original = sync._fetch_market_rows_batched
    concurrent_fetch = build(sync)

    async def dispatch(*args: Any, **kwargs: Any):
        if concurrency() <= 1:
            return await original(*args, **kwargs)
        try:
            return await concurrent_fetch(*args, **kwargs)
        except Exception as exc:
            result = args[5] if len(args) > 5 else kwargs.get("res")
            message = (
                f"[BATCH-CONCURRENCY v{VERSION}] adapter failure; "
                f"falling back to sequential fetch: {exc}"
            )
            if result is not None and hasattr(result, "warnings"):
                result.warnings.append(message)
            sync.logger.exception(message)
            return await original(*args, **kwargs)

    sync._fetch_market_rows_batched = dispatch
    sync._TFB_CONCURRENT_BATCH_FETCH_ORIGINAL = original
    sync._TFB_CONCURRENT_BATCH_FETCH_INSTALLED = True
