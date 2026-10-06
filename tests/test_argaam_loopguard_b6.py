#!/usr/bin/env python3
# tests/test_argaam_loopguard_b6.py
"""
argaam_provider v6.2.0 [B6-a LOOPGUARD] harness -- REAL module, ZERO network.

The HTTP layer is replaced by an injected stub object (no socket is ever
opened, no provider/Google/internet call is made); the provider is "armed" by
passing an explicit ArgaamConfig with a stub quote URL, so has_any_url() is
True and the real fetch path runs. There is no Redis in this module.

  G1 GOLDEN-NEGATIVE on the BASE (v6.1.0): two consecutive asyncio.run()
     loops with the semaphore CONTENDED (max_concurrency=1, 6 concurrent
     callers, 6 symbols) silently lose symbols on loop 2 -- the Semaphore is
     bound to the dead first loop and gather(return_exceptions=True) turns
     every RuntimeError into a fetch_failed patch. Set ARGAAM_BASE=<path to a
     copy of the pre-change v6.1.0 file> to run this leg; it SKIPS cleanly
     when the env var is unset so CI stays green.
  G2 DELIVERED (v6.2.0): the same drive returns all symbols on BOTH loops,
     the stub is called exactly once per symbol per loop, the Semaphore is
     identical within a loop, different across loops, and the cap is kept.
     Also: an injected (non-httpx) client is never swapped, while a real
     httpx pool IS rebuilt for a new loop and the old one is retired.
  G3 STATIC: +N/-0 defs (ast), no asyncio.Lock left where a threading.Lock
     was ported, and an ast walk proving no 'await' inside any
     'with <threading lock>' section introduced by this build.

Run:  /home/user/tfb-venv/bin/python tests/test_argaam_loopguard_b6.py
      /home/user/tfb-venv/bin/python -m pytest -q tests/test_argaam_loopguard_b6.py
"""

from __future__ import annotations

import ast
import asyncio
import importlib.util
import json
import os
import sys
import threading
import traceback
import unittest
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DELIV = os.environ.get("ARGAAM_DELIV", str(ROOT / "core" / "providers" / "argaam_provider.py"))
BASE = (os.environ.get("ARGAAM_BASE", "") or "").strip()

SYMS = ["2222", "1120", "2010", "7010", "1180", "2350"]
STUB_QUOTE_URL = "https://stub.invalid/argaam/quote/{symbol}"


# ---------------------------------------------------------------------------
# Loading / stubbing helpers (import-time safe: nothing runs on import)
# ---------------------------------------------------------------------------

def _load(path: str, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load module from " + str(path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


class _StubResponse:
    """Minimal httpx-Response lookalike: only status_code + content are used."""

    def __init__(self, payload: Dict[str, Any]) -> None:
        self.status_code = 200
        self.content = json.dumps(payload).encode("utf-8")


class _StubHTTP:
    """Injected transport. No socket; the await is what makes the semaphore contend."""

    def __init__(self, calls: List[str]) -> None:
        self.calls = calls

    async def get(self, url: str, *args: Any, **kwargs: Any) -> _StubResponse:
        self.calls.append(url)
        await asyncio.sleep(0.01)
        return _StubResponse({
            "name": "Stub Co",
            "price": 100.0,
            "previous_close": 99.0,
            "volume": 1000.0,
        })

    async def aclose(self) -> None:
        return None


def _config(mod: Any, max_concurrency: int = 1) -> Any:
    """Explicit config: one stub quote URL, cap 1 (so the semaphore contends)."""
    return mod.ArgaamConfig(
        quote_url=STUB_QUOTE_URL,
        profile_url="",
        history_url="",
        timeout_sec=1.0,
        retry_attempts=1,
        max_concurrency=max_concurrency,
        cache_ttl_sec=1.0,
        history_ttl_sec=1.0,
    )


def _armed_client(mod: Any, max_concurrency: int = 1) -> Tuple[Any, List[str]]:
    client = mod.ArgaamClient(_config(mod, max_concurrency))
    calls: List[str] = []
    client._client = _StubHTTP(calls)  # zero network
    return client, calls


def _drive_two_loops(mod: Any) -> Dict[str, Any]:
    """
    Two asyncio.run() loops on ONE client instance -- the production shape
    (core/analysis/top10_selector.py lines 5391 / 5448 call asyncio.run per
    cockpit build against the process-global provider singleton).
    """
    client, calls = _armed_client(mod)

    def one_loop() -> Tuple[int, int, Any]:
        # Clear the URL cache so loop 2 really exercises the transport leg
        # (otherwise a 20s TTL hit would mask the defect entirely).
        client._cache._cache.clear()
        before = len(calls)
        err = None
        try:
            out = asyncio.run(client.get_enriched_quotes_batch(list(SYMS)))
        except Exception as exc:  # noqa: BLE001
            out, err = {}, exc
        ok = sum(1 for v in out.values() if isinstance(v, dict) and not v.get("error"))
        return ok, len(calls) - before, err

    ok1, calls1, err1 = one_loop()
    ok2, calls2, err2 = one_loop()
    return {
        "client": client,
        "ok1": ok1, "ok2": ok2,
        "calls1": calls1, "calls2": calls2,
        "err1": err1, "err2": err2,
    }


def _defs(src: str) -> set:
    tree = ast.parse(src)
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            out.add(node.name)
    return out


def _top_level_defs(src: str) -> set:
    tree = ast.parse(src)
    return {
        n.name for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }


# ---------------------------------------------------------------------------
# G1 -- golden negative on the pre-change v6.1.0 file
# ---------------------------------------------------------------------------

def test_g1_base_loses_symbols_on_the_second_loop() -> None:
    if not BASE:
        raise unittest.SkipTest(
            "golden-negative skipped: set ARGAAM_BASE=<copy of the v6.1.0 file>"
        )
    if not os.path.exists(BASE):
        raise unittest.SkipTest("ARGAAM_BASE path does not exist: " + BASE)

    mod = _load(BASE, "argaam_base_v610")
    assert mod.PROVIDER_VERSION == "6.1.0", mod.PROVIDER_VERSION

    res = _drive_two_loops(mod)
    print("G1 base loop1 ok=%d/%d calls=%d | loop2 ok=%d/%d calls=%d err=%s" % (
        res["ok1"], len(SYMS), res["calls1"],
        res["ok2"], len(SYMS), res["calls2"],
        type(res["err2"]).__name__ if res["err2"] else "-",
    ))
    assert res["ok1"] == len(SYMS), "base loop 1 should be healthy, got %s" % res["ok1"]
    assert res["ok2"] < len(SYMS) or res["err2"] is not None, (
        "the v6.1.0 defect did NOT reproduce: loop2 ok=%s" % res["ok2"]
    )
    sem = res["client"]._semaphore
    loop = getattr(sem, "_loop", None)
    assert loop is not None and loop.is_closed(), (
        "base semaphore should still be bound to the dead first loop, got %r" % (loop,)
    )


# ---------------------------------------------------------------------------
# G2 -- delivered v6.2.0
# ---------------------------------------------------------------------------

def test_g2_delivered_keeps_every_symbol_across_loops() -> None:
    mod = _load(DELIV, "argaam_deliv_b6")
    assert mod.PROVIDER_VERSION == "6.2.0", mod.PROVIDER_VERSION
    assert mod.VERSION == "6.2.0", mod.VERSION

    res = _drive_two_loops(mod)
    print("G2 delivered loop1 ok=%d/%d calls=%d | loop2 ok=%d/%d calls=%d" % (
        res["ok1"], len(SYMS), res["calls1"], res["ok2"], len(SYMS), res["calls2"],
    ))
    assert res["err1"] is None and res["err2"] is None, (res["err1"], res["err2"])
    assert res["ok1"] == len(SYMS), res["ok1"]
    assert res["ok2"] == len(SYMS), res["ok2"]
    assert res["calls1"] == len(SYMS), res["calls1"]
    assert res["calls2"] == len(SYMS), res["calls2"]

    # The injected (non-httpx) transport is never swapped by the loop guard.
    assert isinstance(res["client"]._client, _StubHTTP)


def test_g2_semaphore_is_loop_keyed_with_the_same_cap() -> None:
    mod = _load(DELIV, "argaam_deliv_b6_sem")
    client, _calls = _armed_client(mod, max_concurrency=3)

    sems: List[Any] = []

    async def grab() -> None:
        sems.append(client._get_semaphore())
        sems.append(client._get_semaphore())

    asyncio.run(grab())
    asyncio.run(grab())
    print("G2 semaphores: %s cap=%s" % ([id(s) for s in sems], getattr(sems[2], "_value", None)))
    assert sems[0] is sems[1], "same loop must reuse one Semaphore"
    assert sems[2] is sems[3], "same loop must reuse one Semaphore"
    assert sems[0] is not sems[2], "a new loop must get a fresh Semaphore"
    assert getattr(sems[2], "_value", None) == 3, getattr(sems[2], "_value", None)
    assert client._sem_loop is not None


def test_g2_httpx_pool_is_rebuilt_for_a_new_loop() -> None:
    mod = _load(DELIV, "argaam_deliv_b6_pool")
    if not getattr(mod, "_HTTPX_AVAILABLE", False) or mod.httpx is None:
        raise unittest.SkipTest("httpx not installed")
    client = mod.ArgaamClient(_config(mod))
    first = client._client
    assert isinstance(first, mod.httpx.AsyncClient)

    async def claim() -> Any:
        client._ensure_http_client()  # no request is issued: zero network
        return client._client

    c1 = asyncio.run(claim())
    c2 = asyncio.run(claim())
    print("G2 pool: first_loop_keeps=%s new_loop_rebuilds=%s graveyard=%d" % (
        c1 is first, c2 is not c1, len(mod._HTTP_GRAVEYARD),
    ))
    assert c1 is first, "the first running loop claims the pool built in __init__"
    assert c2 is not c1, "a second loop must get a fresh pool"
    assert c2.headers.get("X-Client-ID") == first.headers.get("X-Client-ID"), (
        "the rebuilt pool must carry the same headers"
    )
    assert mod._HTTP_GRAVEYARD and mod._HTTP_GRAVEYARD[-1] is c1, (
        "the superseded pool must be retired, not dropped mid-flight"
    )


def test_g2_module_singleton_guard_works_across_loops() -> None:
    mod = _load(DELIV, "argaam_deliv_b6_single")

    async def get_twice() -> bool:
        a = await mod.get_provider()
        b = await mod.get_provider()
        return a is b

    assert asyncio.run(get_twice())
    assert asyncio.run(get_twice())
    assert mod._PROVIDER_INSTANCE is not None


# ---------------------------------------------------------------------------
# G3 -- static proof
# ---------------------------------------------------------------------------

def test_g3_static_additive_and_no_await_under_a_threading_lock() -> None:
    src = Path(DELIV).read_text(encoding="utf-8")
    mod = _load(DELIV, "argaam_deliv_b6_static")

    names = _defs(src)
    added_expected = {"_retire_http_client", "_new_http_client", "_ensure_http_client"}
    assert added_expected <= names, sorted(added_expected - names)

    if BASE and os.path.exists(BASE):
        base_src = Path(BASE).read_text(encoding="utf-8")
        removed = _defs(base_src) - names
        added = names - _defs(base_src)
        top_removed = _top_level_defs(base_src) - _top_level_defs(src)
        top_added = _top_level_defs(src) - _top_level_defs(base_src)
        print("G3 defs added=%s removed=%s | top-level +%d/-%d" % (
            sorted(added), sorted(removed), len(top_added), len(top_removed),
        ))
        assert not removed, sorted(removed)
        assert not top_removed, sorted(top_removed)
        assert added == added_expected, sorted(added)
        assert top_added == {"_retire_http_client"}, sorted(top_added)
    else:
        # No base file in CI: prove nothing public went missing instead.
        missing = [n for n in getattr(mod, "__all__", []) if not hasattr(mod, n)]
        print("G3 defs added=%s (no ARGAAM_BASE: __all__ checked, missing=%s)" % (
            sorted(added_expected), missing,
        ))
        assert not missing, missing

    # Every ported lock really is a threading.Lock at runtime.
    cache_lock = mod._TTLCache(max_size=4)._get_lock()
    prov_lock = mod.ArgaamProvider(_config(mod))._get_lock()
    mod_lock = mod._get_provider_lock()
    lock_type = type(threading.Lock())
    assert isinstance(cache_lock, lock_type), type(cache_lock).__name__
    assert isinstance(prov_lock, lock_type), type(prov_lock).__name__
    assert isinstance(mod_lock, lock_type), type(mod_lock).__name__

    # No asyncio.Lock is CONSTRUCTED anywhere in the code any more (ast, so
    # the v6.0.0 docstring history that mentions it by name does not count).
    tree = ast.parse(src)
    lock_calls = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.Call) and ast.unparse(n.func) == "asyncio.Lock"
    ]
    assert not lock_calls, [getattr(n, "lineno", "?") for n in lock_calls]

    # No await inside any threading-lock critical section.
    offenders = []
    guarded = 0
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.With):
            hit = False
            for item in node.items:
                expr = ast.unparse(item.context_expr)
                if "_get_lock()" in expr or "_get_provider_lock()" in expr:
                    hit = True
            if hit:
                guarded += 1
                offenders += [n for n in ast.walk(node) if isinstance(n, ast.Await)]
    print("G3 threading-lock sections=%d awaits_inside=%d" % (guarded, len(offenders)))
    assert guarded >= 4, guarded
    assert not offenders, len(offenders)


# ---------------------------------------------------------------------------
# Standalone runner (pytest ignores this)
# ---------------------------------------------------------------------------

def _main() -> int:
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("test_") and callable(f)]
    passed = failed = skipped = 0
    for name, fn in tests:
        try:
            fn()
        except unittest.SkipTest as exc:
            skipped += 1
            print("SKIP " + name + " :: " + str(exc))
            continue
        except Exception:  # noqa: BLE001
            failed += 1
            print("FAIL " + name)
            traceback.print_exc()
            continue
        passed += 1
        print("ok   " + name)
    total = passed + failed
    suffix = ("  (skipped %d)" % skipped) if skipped else ""
    print("PASS %d/%d%s" % (passed, total, suffix))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_main())
