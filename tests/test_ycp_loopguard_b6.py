#!/usr/bin/env python3
# tests/test_ycp_loopguard_b6.py
"""
================================================================================
B6-b LOOPGUARD harness -- core/providers/yahoo_chart_provider.py v8.15.2
================================================================================
Zero network. The HTTP leg (_raw_chart_fetch_triple) is stubbed in-process, so
the REAL SingleFlight / TokenBucket / CircuitBreaker / AdvancedCache path runs.

G1 GOLDEN-NEGATIVE ON THE BASE (skipped unless YCP_BASE points at a copy of the
   pre-change v8.14.0 module, written OUTSIDE the repo): 4 concurrent callers
   through the real single-flight path across two asyncio.run() loops lose
   results on loop 2, and get_enriched_quotes_batch drops them silently.
G2 DELIVERED: both loops return every symbol; the fetch stub runs exactly once
   per key per loop; a stale Future left by a closed loop is never awaited and
   its entry is cleared; an owner-only failure is observed (the loop exception
   handler never reports 'exception was never retrieved').
G3 STATIC (ast): the three dataclass locks and _PROVIDER_LOCK are
   threading-based, no 'await' sits inside any 'with <threading lock>' block,
   zero defs removed (additions are the LOOPGUARD helpers only), and
   PROVIDER_VERSION == "8.15.2" with the header banner in lockstep.

Runs both ways:
    /home/user/tfb-venv/bin/python tests/test_ycp_loopguard_b6.py
    /home/user/tfb-venv/bin/python -m pytest -q tests/test_ycp_loopguard_b6.py
================================================================================
"""

from __future__ import annotations

import ast
import asyncio
import gc
import importlib.util
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

TARGET = _REPO / "core" / "providers" / "yahoo_chart_provider.py"
EXPECTED_VERSION = "8.15.2"
BASE_ENV = "YCP_BASE"


# =============================================================================
# Loaders / stubs
# =============================================================================

def _load_module(path: Path, name: str) -> Any:
    """Import a module from an explicit path under a private name."""
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot build import spec for %s" % path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_target() -> Any:
    import core.providers.yahoo_chart_provider as mod  # noqa: PLC0415
    return mod


def _base_path() -> Optional[Path]:
    raw = (os.getenv(BASE_ENV) or "").strip()
    if not raw:
        return None
    p = Path(raw)
    return p if p.is_file() else None


def _install_fetch_stub(mod: Any) -> Dict[str, int]:
    """Replace the raw chart HTTP leg with an in-process coroutine.

    Returns the per-Yahoo-symbol call counter (the single-flight evidence).
    """
    calls: Dict[str, int] = {}

    async def _triple(
        ysym: str, range_: str, interval: str, timeout: float,
    ) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
        calls[ysym] = calls.get(ysym, 0) + 1
        await asyncio.sleep(0)   # a real suspension point, so callers overlap
        info = {
            "symbol": ysym,
            "currentPrice": 100.0,
            "previousClose": 99.0,
            "currency": "USD",
        }
        meta = {"symbol": ysym, "currency": "USD"}
        history = [
            {"date": "2026-09-30", "open": 98.0, "high": 101.0,
             "low": 97.0, "close": 99.0, "volume": 1000},
            {"date": "2026-10-01", "open": 99.0, "high": 101.0,
             "low": 98.0, "close": 100.0, "volume": 1100},
        ]
        return info, meta, history

    mod._raw_chart_fetch_triple = _triple
    mod._HAS_HTTPX = True
    mod._raw_chart_enabled = lambda: True
    return calls


def _fan_out(provider: Any, symbols: List[str]) -> List[Any]:
    """4+ concurrent callers through get_enriched_quote on ONE instance."""
    async def _main() -> List[Any]:
        return await asyncio.gather(
            *(provider.get_enriched_quote(s) for s in symbols),
            return_exceptions=True,
        )
    return asyncio.run(_main())


def _batch(provider: Any, symbols: List[str]) -> Dict[str, Any]:
    async def _main() -> Dict[str, Any]:
        return await provider.get_enriched_quotes_batch(symbols)
    return asyncio.run(_main())


def _shape(results: List[Any]) -> List[str]:
    return ["v" if isinstance(r, dict) and r else type(r).__name__ for r in results]


# =============================================================================
# Static helpers (ast)
# =============================================================================

_LOCK_EXPRS = {
    "self.lock",
    "self._lock",
    "self._get_lock()",
    "lock",
    "_get_provider_lock()",
}


def _parse(path: Path) -> Tuple[ast.Module, List[str]]:
    src = path.read_text(encoding="utf-8")
    return ast.parse(src), src.splitlines()


def _qual_defs(tree: ast.Module) -> List[str]:
    """Qualified names of every function/class def in the module."""
    names: List[str] = []

    def _walk(node: ast.AST, prefix: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                names.append(prefix + child.name)
                _walk(child, prefix + child.name + ".")
            elif isinstance(child, ast.ClassDef):
                names.append(prefix + child.name)
                _walk(child, prefix + child.name + ".")

    _walk(tree, "")
    return names


def _class_node(tree: ast.Module, name: str) -> Optional[ast.ClassDef]:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    return None


def _lock_field_factory(tree: ast.Module, cls: str, field_name: str) -> str:
    """Return the dotted default_factory of `cls.field_name`, or ''."""
    node = _class_node(tree, cls)
    if node is None:
        return ""
    for stmt in node.body:
        if not isinstance(stmt, ast.AnnAssign):
            continue
        tgt = stmt.target
        if not (isinstance(tgt, ast.Name) and tgt.id == field_name):
            continue
        call = stmt.value
        if not isinstance(call, ast.Call):
            return ""
        for kw in call.keywords:
            if kw.arg == "default_factory":
                return ast.unparse(kw.value)
    return ""


def _awaits_inside_threading_with(tree: ast.Module) -> List[int]:
    """Line numbers of `await` expressions inside a `with <threading lock>`."""
    bad: List[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.With):
            continue
        exprs = {ast.unparse(item.context_expr) for item in node.items}
        if not (exprs & _LOCK_EXPRS):
            continue
        for inner in ast.walk(node):
            if isinstance(inner, (ast.Await, ast.AsyncWith, ast.AsyncFor)):
                bad.append(getattr(inner, "lineno", -1))
    return bad


def _asyncwith_on_locks(tree: ast.Module) -> List[int]:
    """Line numbers of `async with` statements still guarding those locks."""
    bad: List[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncWith):
            continue
        exprs = {ast.unparse(item.context_expr) for item in node.items}
        if exprs & _LOCK_EXPRS:
            bad.append(node.lineno)
    return bad


def _provider_lock_is_threading(tree: ast.Module) -> bool:
    """True when _get_provider_lock() assigns a threading.Lock()."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and \
                node.name == "_get_provider_lock":
            for inner in ast.walk(node):
                if isinstance(inner, ast.Assign) and isinstance(inner.value, ast.Call):
                    if ast.unparse(inner.value.func) == "threading.Lock":
                        return True
    return False


# =============================================================================
# Checks
# =============================================================================

def _run_g1(record: Any) -> None:
    """Golden-negative on the pre-change v8.14.0 module (needs YCP_BASE)."""
    base = _base_path()
    if base is None:
        record(
            "G1.skip",
            True,
            "%s unset or not a file -- golden-negative on the base skipped" % BASE_ENV,
        )
        return

    if str(base.resolve()).startswith(str(_REPO.resolve()) + os.sep):
        record("G1.outside_repo", False,
               "YCP_BASE must live OUTSIDE the repo, got %s" % base)
        return
    record("G1.outside_repo", True, "base copy lives outside the repo: %s" % base)

    mod = _load_module(base, "_ycp_base_b6")
    record(
        "G1.base_version",
        getattr(mod, "PROVIDER_VERSION", "") == "8.14.0",
        "base PROVIDER_VERSION=%r (want '8.14.0')" % getattr(mod, "PROVIDER_VERSION", ""),
    )

    base_tree, _ = _parse(base)
    record(
        "G1.base_awaits_under_lock",
        bool(_asyncwith_on_locks(base_tree)),
        "base still guards its locks with 'async with' at lines %r (the defect)"
        % _asyncwith_on_locks(base_tree),
    )

    calls = _install_fetch_stub(mod)
    provider = mod.YahooChartProvider()

    loop1 = _shape(_fan_out(provider, ["AAA", "AAA", "AAA", "AAA"]))
    loop2 = _shape(_fan_out(provider, ["BBB", "BBB", "BBB", "BBB"]))

    record(
        "G1.loop1_clean",
        loop1 == ["v", "v", "v", "v"],
        "base loop1 %r (the first loop binds the asyncio.Lock and is fine)" % (loop1,),
    )
    record(
        "G1.loop2_loses",
        any(s != "v" for s in loop2),
        "base loop2 %r -- callers raise on a Lock bound to the dead loop" % (loop2,),
    )

    sf = getattr(provider, "_single_flight", None)
    sf_loop = getattr(getattr(sf, "_lock", None), "_loop", None)
    record(
        "G1.base_lock_dead_loop",
        bool(sf_loop is not None and sf_loop.is_closed()),
        "base SingleFlight._lock._loop.is_closed()=%r" % (
            sf_loop.is_closed() if sf_loop is not None else None,
        ),
    )

    out = _batch(provider, ["CCC"] * 4 + ["DDD"] * 4)
    record(
        "G1.batch_silent_loss",
        sorted(out.keys()) != ["CCC", "DDD"],
        "base batch served %r of ['CCC', 'DDD'] with NO exception raised "
        "-- the production symptom is silent symbol loss" % (sorted(out.keys()),),
    )
    record("G1.base_stub_zero_network", sum(calls.values()) > 0,
           "base fetch stub calls=%r (no network touched)" % (calls,))


def _run_g2(record: Any) -> None:
    """The delivered v8.15.2 module: no loss, dedup intact, no stale awaits."""
    mod = _load_target()
    calls = _install_fetch_stub(mod)
    provider = mod.YahooChartProvider()

    syms1 = ["S1A", "S1B", "S1A", "S1B", "S1C", "S1C"]
    syms2 = ["S2A", "S2B", "S2A", "S2B", "S2C", "S2C"]

    loop1 = _shape(_fan_out(provider, syms1))
    calls_after_1 = dict(calls)
    loop2 = _shape(_fan_out(provider, syms2))

    record("G2.loop1_all", loop1 == ["v"] * len(syms1),
           "loop1 %r" % (loop1,))
    record("G2.loop2_all", loop2 == ["v"] * len(syms2),
           "loop2 %r (every symbol served on the SECOND asyncio.run loop)" % (loop2,))

    want1 = {"S1A": 1, "S1B": 1, "S1C": 1}
    record("G2.dedup_loop1", calls_after_1 == want1,
           "loop1 fetch-stub calls %r (want exactly one per key)" % (calls_after_1,))
    want2 = dict(want1)
    want2.update({"S2A": 1, "S2B": 1, "S2C": 1})
    record("G2.dedup_loop2", calls == want2,
           "cumulative fetch-stub calls %r (want exactly one per key per loop)"
           % (calls,))

    out = _batch(provider, ["B1"] * 4 + ["B2"] * 4)
    record("G2.batch_no_loss", sorted(out.keys()) == ["B1", "B2"],
           "batch across a third loop served %r" % (sorted(out.keys()),))

    # -- a Future left behind by a dead loop is never awaited -----------------
    sf = mod.SingleFlight()
    stale_box: List[Any] = []

    async def _leave_stale() -> None:
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        sf._futures["enriched:STALE"] = fut
        stale_box.append(fut)

    asyncio.run(_leave_stale())

    async def _own_flight() -> Tuple[Any, int]:
        async def _fn() -> str:
            await asyncio.sleep(0)
            return "mine"
        res = await asyncio.wait_for(sf.run("enriched:STALE", _fn), timeout=5.0)
        return res, sf.inflight()

    try:
        got, left = asyncio.run(_own_flight())
        stale_ok = (got == "mine" and left == 0)
        stale_msg = ("new loop ran its own flight -> %r, in-flight left=%d"
                     % (got, left))
    except Exception as exc:  # noqa: BLE001
        stale_ok = False
        stale_msg = "new loop could not run its own flight: %r" % (exc,)
    record("G2.stale_future_not_awaited", stale_ok, stale_msg)
    record("G2.stale_future_pending",
           bool(stale_box and not stale_box[0].done()),
           "the dead loop's Future was left untouched (never awaited, never set)")

    # -- an owner-only failure is OBSERVED (no 'never retrieved' report) ------
    reports: List[str] = []

    async def _owner_fails() -> str:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(
            lambda _l, ctx: reports.append(str(ctx.get("message", "")))
        )
        sf2 = mod.SingleFlight()

        async def _boom() -> None:
            await asyncio.sleep(0)
            raise RuntimeError("owner_boom")

        try:
            await sf2.run("enriched:BOOM", _boom)
        except RuntimeError as exc:
            raised = str(exc)
        else:
            raised = ""
        assert sf2.inflight() == 0
        del sf2
        await asyncio.sleep(0)
        gc.collect()
        await asyncio.sleep(0)
        return raised

    raised = asyncio.run(_owner_fails())
    gc.collect()
    record("G2.owner_failure_propagates", raised == "owner_boom",
           "owner-only failure raised %r to its own caller" % (raised,))
    never = [m for m in reports if "never retrieved" in m]
    record("G2.no_unretrieved_future", not never,
           "loop exception handler reports %r (want none about 'never retrieved')"
           % (reports,))

    # -- the new helpers exist and are callable -------------------------------
    record("G2.helpers_present",
           callable(getattr(mod.SingleFlight, "_observe_future", None))
           and callable(getattr(mod.SingleFlight, "inflight", None))
           and callable(getattr(mod.SingleFlight, "_get_lock", None)),
           "SingleFlight._get_lock/_observe_future/inflight all callable")


def _run_g3(record: Any) -> None:
    """Static proof over the delivered source."""
    tree, lines = _parse(TARGET)

    mod = _load_target()
    record("G3.version", getattr(mod, "PROVIDER_VERSION", "") == EXPECTED_VERSION,
           "PROVIDER_VERSION=%r (want %r)"
           % (getattr(mod, "PROVIDER_VERSION", ""), EXPECTED_VERSION))

    banner = [ln for ln in lines[:12] if "Yahoo Chart Provider" in ln]
    banner_ok = bool(banner) and ("v" + EXPECTED_VERSION) in banner[0]
    record("G3.banner_lockstep", banner_ok,
           "header banner %r (must carry v%s)"
           % (banner[0] if banner else None, EXPECTED_VERSION))

    for cls, fld in (("TokenBucket", "lock"),
                     ("CircuitBreaker", "lock"),
                     ("AdvancedCache", "_lock")):
        factory = _lock_field_factory(tree, cls, fld)
        record("G3.lock_%s" % cls, factory == "threading.Lock",
               "%s.%s default_factory=%r (want 'threading.Lock')"
               % (cls, fld, factory))

    record("G3.singleflight_lazy_threading_lock",
           "SingleFlight._get_lock" in _qual_defs(tree)
           and any(
               isinstance(n, ast.Assign) and isinstance(n.value, ast.Call)
               and ast.unparse(n.value.func) == "threading.Lock"
               for n in ast.walk(tree)
           ),
           "SingleFlight._get_lock() exists and builds a threading.Lock")

    record("G3.provider_lock_threading", _provider_lock_is_threading(tree),
           "_get_provider_lock() assigns threading.Lock()")

    bad_await = _awaits_inside_threading_with(tree)
    record("G3.no_await_under_lock", not bad_await,
           "await/async-with inside a 'with <threading lock>' block at lines %r "
           "(want none -- that would deadlock the loop)" % (bad_await,))

    stale_async = _asyncwith_on_locks(tree)
    record("G3.no_async_with_on_locks", not stale_async,
           "'async with' still guarding a converted lock at lines %r (want none)"
           % (stale_async,))

    record("G3.threading_imported",
           any(isinstance(n, ast.Import)
               and any(a.name == "threading" for a in n.names)
               for n in tree.body),
           "module imports threading")

    base = _base_path()
    if base is None:
        record("G3.defs_delta_skip", True,
               "%s unset -- the +3/-0 def delta against the base is skipped"
               % BASE_ENV)
        return

    base_tree, _ = _parse(base)
    old = _qual_defs(base_tree)
    new = _qual_defs(tree)
    removed = sorted(set(old) - set(new))
    added = sorted(set(new) - set(old))
    record("G3.zero_removals", not removed,
           "defs removed vs base: %r (want none)" % (removed,))
    record("G3.additions_are_loopguard_only",
           added == ["SingleFlight._get_lock",
                     "SingleFlight._observe_future",
                     "SingleFlight.inflight"],
           "defs added vs base: %r" % (added,))
    record("G3.def_counts", len(new) - len(old) == 3,
           "def count %d -> %d (+%d)" % (len(old), len(new), len(new) - len(old)))


# =============================================================================
# pytest entry points
# =============================================================================

class _Recorder:
    def __init__(self) -> None:
        self.rows: List[Tuple[str, bool, str]] = []

    def __call__(self, name: str, ok: bool, msg: str) -> None:
        self.rows.append((name, bool(ok), msg))

    @property
    def failures(self) -> List[Tuple[str, bool, str]]:
        return [r for r in self.rows if not r[1]]


def _assert_group(fn: Any) -> _Recorder:
    rec = _Recorder()
    fn(rec)
    if rec.failures:
        detail = "\n".join("  FAIL %s: %s" % (n, m) for n, _o, m in rec.failures)
        raise AssertionError("%d check(s) failed:\n%s" % (len(rec.failures), detail))
    return rec


def test_g1_golden_negative_on_base() -> None:
    """4+ concurrent callers lose results on loop 2 of the pre-change module."""
    _assert_group(_run_g1)


def test_g2_delivered_no_loss_and_dedup() -> None:
    """v8.15.2: every symbol on both loops, one fetch per key per loop."""
    _assert_group(_run_g2)


def test_g3_static_threading_locks_and_version() -> None:
    """ast: threading locks, no await under lock, zero removals, version."""
    _assert_group(_run_g3)


# =============================================================================
# Script entry point
# =============================================================================

def _main() -> int:
    rec = _Recorder()
    for group in (_run_g1, _run_g2, _run_g3):
        try:
            group(rec)
        except Exception as exc:  # noqa: BLE001
            rec(group.__name__ + ".crash", False, "raised %r" % (exc,))

    for name, ok, msg in rec.rows:
        print("%-4s %-38s %s" % ("ok" if ok else "FAIL", name, msg))

    passed = sum(1 for _n, ok, _m in rec.rows if ok)
    total = len(rec.rows)
    print("PASS %d/%d" % (passed, total))
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(_main())
