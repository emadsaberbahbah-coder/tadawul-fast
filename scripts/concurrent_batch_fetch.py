#!/usr/bin/env python3
"""Opt-in bounded concurrency for scripts/run_dashboard_sync.py."""
from __future__ import annotations
import asyncio, os, time
from typing import Any

VERSION = "1.0.0"
_METRICS: dict[str, dict[str, Any]] = {}


def _int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        value = int(float(os.getenv(name, str(default))))
    except Exception:
        value = default
    return max(lo, min(hi, value))


def _concurrency() -> int:
    return _int("TFB_SYNC_BATCH_CONCURRENCY", 3, 1, 8)


def _outer_retries() -> int:
    return _int("TFB_SYNC_BATCH_OUTER_RETRIES", 1, 0, 2)


def get_metrics(request_id: str) -> dict[str, Any] | None:
    return _METRICS.get(str(request_id or ""))


def build(sync: Any):
    async def one(backend, idx, batch, payload, endpoints, req_id, retry=False, delay_ms=0):
        if delay_ms:
            await asyncio.sleep(delay_ms / 1000)
        if sync._time_budget_exceeded():
            return {"i": idx, "b": list(batch), "h": [], "r": [], "e": "budget", "ep": None, "a": False, "ms": 0.0}
        body = dict(payload)
        body.update(tickers=list(batch), symbols=list(batch),
                    limit=min(sync._request_limit_ceiling(), max(1, len(batch))),
                    request_id=req_id)
        started = time.perf_counter(); last = None
        for ep in endpoints:
            data, err, _ = await backend.post_json(ep, body)
            if err:
                last = f"{ep} -> {err}"; continue
            if not isinstance(data, dict):
                last = f"{ep} -> Non-dict response"; continue
            headers, rows = sync._extract_table_payload(data)
            if not headers:
                last = f"{ep} -> Missing headers"; continue
            return {"i": idx, "b": list(batch), "h": list(headers),
                    "r": list(sync._rectify_matrix(headers, rows) or []),
                    "e": None, "ep": ep, "a": True,
                    "ms": (time.perf_counter() - started) * 1000}
        return {"i": idx, "b": list(batch), "h": [], "r": [], "e": last or "failed",
                "ep": None, "a": True, "ms": (time.perf_counter() - started) * 1000}

    async def fan(backend, indexed, payload, endpoint, req_id, retry, concurrency, delay_ms):
        sem = asyncio.Semaphore(concurrency)
        async def guarded(pos, idx, batch):
            async with sem:
                return await one(backend, idx, batch, payload, (endpoint,),
                                 f"{req_id}-{'r' if retry else 'b'}{idx + 1}", retry,
                                 (pos % concurrency) * delay_ms)
        return list(await asyncio.gather(*[
            asyncio.create_task(guarded(pos, idx, batch))
            for pos, (idx, batch) in enumerate(indexed)
        ])) if indexed else []

    def merge(headers, outcomes, symbols, res):
        sym_i = sync._guard_find_col(headers, sync._GUARD_SYMBOL_ALIASES)
        if not sync._batch_identity_enabled() or sym_i < 0:
            return [list(row) for out in sorted(outcomes, key=lambda x: x["i"]) for row in out["r"]]
        rows: dict[str, list[Any]] = {}; bleed = dupes = blank = 0
        for out in sorted(outcomes, key=lambda x: x["i"]):
            requested = {sync.canonicalize_symbol(x) for x in out["b"]}; requested.discard("")
            for raw in out["r"]:
                if not isinstance(raw, (list, tuple)) or sym_i >= len(raw) or sync._guard_is_blank(raw[sym_i]):
                    blank += 1; continue
                row = list(raw); symbol = sync.canonicalize_symbol(row[sym_i]); row[sym_i] = symbol
                if symbol not in requested:
                    bleed += 1; continue
                if symbol in rows:
                    dupes += 1; continue
                rows[symbol] = row
        if bleed or dupes or blank:
            msg = f"{sync._BATCH_IDENTITY_TAG} concurrent fold dropped bleed={bleed} dupes={dupes} blank={blank}"
            res.warnings.append(msg); sync.logger.warning(msg)
        return [rows[s] for s in (sync.canonicalize_symbol(x) for x in symbols) if s in rows]

    async def concurrent(backend, task, symbols, payload, gateway, res):
        started = time.perf_counter(); size = sync._symbol_batch_size()
        batches = sync.build_isolated_batches(symbols, size)
        concurrency = _concurrency(); endpoints = sync._endpoint_candidates_for_gateway(gateway)
        metrics = {"version": VERSION, "page": task.sheet_name, "concurrency": concurrency,
                   "batch_size": size, "batches_total": len(batches), "requested": len(symbols)}
        _METRICS[str(res.request_id)] = metrics
        if not batches:
            return [], [], None, "no batches"
        first = await one(backend, 0, batches[0], payload, endpoints, f"{res.request_id}-b1")
        outcomes = [first]; endpoint = first["ep"]; headers = first["h"]; last = first["e"]
        if endpoint:
            outcomes += await fan(backend, list(enumerate(batches[1:], 1)), payload, endpoint,
                                  str(res.request_id), False, concurrency, sync._batch_delay_ms())
        else:
            outcomes += [{"i": i, "b": list(b), "h": [], "r": [], "e": "endpoint unresolved",
                          "ep": None, "a": False, "ms": 0.0} for i, b in enumerate(batches[1:], 1)]
        for _ in range(_outer_retries()):
            failed = [(x["i"], x["b"]) for x in outcomes if x["a"] and not x["h"]]
            if not failed or not endpoint or sync._time_budget_exceeded(): break
            retried = await fan(backend, failed, payload, endpoint, str(res.request_id), True,
                                concurrency, sync._batch_delay_ms())
            by_i = {x["i"]: x for x in outcomes}
            for x in retried:
                if x["h"]: by_i[x["i"]] = x
                elif x["e"]: last = x["e"]
            outcomes = [by_i[i] for i in sorted(by_i)]
        if not headers:
            headers = next((x["h"] for x in outcomes if x["h"]), [])
        rows = merge(headers, outcomes, symbols, res)
        attempted = [x for x in outcomes if x["a"]]; ok = [x for x in outcomes if x["h"]]
        failed = [x for x in attempted if not x["h"]]; unattempted = [x for x in outcomes if not x["a"]]
        metrics.update(batches_attempted=len(attempted), batches_succeeded=len(ok),
                       batches_failed=len(failed), attempted=sum(len(x["b"]) for x in attempted),
                       fresh=len(rows), failed=sum(len(x["b"]) for x in failed),
                       unattempted=sum(len(x["b"]) for x in unattempted),
                       elapsed_ms=round((time.perf_counter() - started) * 1000),
                       max_batch_ms=round(max((x["ms"] for x in outcomes), default=0)))
        msg = (f"[BATCH-CONCURRENCY v{VERSION}] page={task.sheet_name} concurrency={concurrency} "
               f"batch_size={size} batches={len(ok)}/{len(batches)} attempted={metrics['attempted']} "
               f"fresh={metrics['fresh']} failed={metrics['failed']} unattempted={metrics['unattempted']} "
               f"elapsed_ms={metrics['elapsed_ms']} max_batch_ms={metrics['max_batch_ms']}")
        res.warnings.append(msg); sync.logger.info(msg)
        return headers, rows, endpoint, last
    return concurrent


def install(sync: Any) -> None:
    if getattr(sync, "_TFB_CONCURRENT_BATCH_FETCH_INSTALLED", False): return
    sync._fetch_market_rows_batched = build(sync)
    original = sync.TaskResult.to_dict
    def to_dict(result):
        payload = original(result); metrics = get_metrics(result.request_id)
        if metrics is not None: payload["batch_metrics"] = dict(metrics)
        return payload
    sync.TaskResult.to_dict = to_dict
    sync._TFB_CONCURRENT_BATCH_FETCH_INSTALLED = True
