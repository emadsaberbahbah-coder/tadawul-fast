#!/usr/bin/env python3
"""yahoo_fundamentals_provider v6.9.1 [P-203 LOOPGUARD] harness — REAL module,
dual-tree (base v6.8.0 vs delivered v6.9.1), ZERO network: the blocking yfinance
leg is replaced by a sync stub, `_configured` is forced True, Redis stays off.
  G1 GOLDEN-NEGATIVE on BASE: the REAL fetch_fundamentals_batch across two
     asyncio.run() loops with provider-level contention (cap 1, 4 callers) — the
     second loop silently loses every symbol (the Semaphore is bound to the
     dead loop; gather(return_exceptions=True) swallows the RuntimeError)
  G2 DELIVERED: the same drive returns all symbols on BOTH loops; the
     Semaphore is re-created per loop with the same cap
  G3 SingleFlight: 8 concurrent callers on one loop share ONE underlying
     call; a second loop runs its own; an owner-only failure leaves no
     unretrieved Future (observed via the done-callback); a stale Future
     from a dead loop is never awaited
  G4 Every helper lock is a threading.Lock; the breaker keeps
     process-cumulative counters across loops without error
  G5 Cross-loop breaker/limiter/cache drive (base vs delivered): delivered
     survives 3 loops x contention; base raises or drops
  G6 versions, +2/0 defs, no asyncio.Lock construction left in code
Run x3, identical digest."""
import importlib.util, os, sys, hashlib, asyncio, threading, ast

DELIV = os.environ.get("YF_DELIV", "core/providers/yahoo_fundamentals_provider.py")
BASE = os.environ.get("YF_BASE")   # optional v6.8.0 file for the golden-negative
os.environ.setdefault("YF_ENABLE_REDIS", "0")
os.environ.setdefault("YF_RATE_LIMIT_PER_SEC", "0")      # limiter early-return (no sleeps)
os.environ.setdefault("YF_MAX_CONCURRENCY", "1")   # provider cap 1 vs batch concurrency 4 => the provider Semaphore is CONTENDED (binds to the loop)

sys.path.insert(0, os.getcwd())

def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m          # dataclass(slots=True) resolves annotations via sys.modules
    spec.loader.exec_module(m)
    return m

def arm(mod):
    """No network: force `enabled`, stub the blocking leg (price present => cache set path runs)."""
    mod._configured = lambda: True
    calls = {"n": 0}
    def _stub(self, norm):
        calls["n"] += 1
        return {"symbol": norm, "current_price": 10.0 + len(norm), "data_quality": "ok"}
    mod.YahooFundamentalsProvider._blocking_fetch = _stub
    return calls

SYMS = ["AAPL", "MSFT", "NVDA", "DDI", "AER", "KRP"]
digest_parts = []
fails = 0
def check(name, ok, detail=""):
    global fails
    print(("PASS " if ok else "FAIL ") + name + ((" :: " + str(detail)) if detail else ""))
    digest_parts.append(f"{name}={int(bool(ok))}")
    if not ok: fails += 1

def drive_two_loops(mod):
    """Two asyncio.run() loops on ONE provider instance (the production shape)."""
    prov = mod.YahooFundamentalsProvider()
    async def run_batch():
        return await prov.fetch_fundamentals_batch(SYMS, concurrency=4)
    r1 = asyncio.run(run_batch())
    # second loop: clear the cache so the semaphore path is exercised again
    prov.fund_cache._mem.clear(); prov.fund_cache._touch.clear()
    err = None
    try:
        r2 = asyncio.run(run_batch())
    except Exception as exc:  # noqa: BLE001
        r2, err = {}, exc
    return prov, r1, r2, err

# ---------------------------------------------------------------- G1 ------ #
if BASE:
    mb = load(BASE, "yf_base"); arm(mb)
    assert mb.PROVIDER_VERSION == "6.8.0", mb.PROVIDER_VERSION
    pb, b1, b2, berr = drive_two_loops(mb)
    check("G1 base loop 1 returns all symbols", len(b1) == len(SYMS), len(b1))
    check("G1 base loop 2 LOSES symbols or raises (the production defect, reproduced)",
          len(b2) < len(SYMS) or berr is not None, f"loop2={len(b2)} err={type(berr).__name__ if berr else '-'}")
else:
    print("G1 SKIP  golden-negative (set YF_BASE=<v6.8.0 file>)")

# ---------------------------------------------------------------- G2 ------ #
md = load(DELIV, "yf_deliv"); calls = arm(md)
assert md.PROVIDER_VERSION == "6.9.1", md.PROVIDER_VERSION
pd, d1, d2, derr = drive_two_loops(md)
check("G2 delivered loop 1 returns all symbols", len(d1) == len(SYMS), len(d1))
check("G2 delivered loop 2 returns all symbols, no error", len(d2) == len(SYMS) and derr is None, f"loop2={len(d2)} err={derr!r}")
check("G2 stub called once per symbol per loop (single-flight intact, no double fetch)", calls["n"] == 2 * len(SYMS), calls["n"])
sems = []
async def grab():
    sems.append(pd._get_semaphore()); sems.append(pd._get_semaphore())
asyncio.run(grab()); asyncio.run(grab())
check("G2 semaphore identical within a loop, re-created across loops, cap preserved",
      sems[0] is sems[1] and sems[2] is sems[3] and sems[0] is not sems[2]
      and getattr(sems[2], "_value", None) == pd.max_concurrency, [id(x) for x in sems])

# ---------------------------------------------------------------- G3 ------ #
sf = md.SingleFlight()
counter = {"n": 0}
async def slow():
    counter["n"] += 1
    await asyncio.sleep(0.02)
    return "v"
async def fanout():
    return await asyncio.gather(*[sf.run("K", slow) for _ in range(8)])
res1 = asyncio.run(fanout())
res2 = asyncio.run(fanout())
check("G3 single-flight: 8 concurrent callers share ONE call per loop (2 loops -> 2 calls)",
      res1 == ["v"] * 8 and res2 == ["v"] * 8 and counter["n"] == 2 and sf.inflight() == 0, counter["n"])
async def boom():
    raise ValueError("owner-only failure")
async def owner_fail():
    try:
        await sf.run("B", boom)
    except ValueError:
        pass
    return True
captured = []
def run_with_handler():
    loop = asyncio.new_event_loop()
    loop.set_exception_handler(lambda l, ctx: captured.append(ctx.get("message", "")))
    try:
        return loop.run_until_complete(owner_fail())
    finally:
        loop.close()
ok3 = run_with_handler()
import gc; gc.collect()
fut = asyncio.new_event_loop().create_future(); fut.set_exception(RuntimeError("x")); md.SingleFlight._observe_future(fut)
check("G3 owner-only failure: exception observed (no 'never retrieved' report; _observe_future clears the flag)",
      ok3 and not any("never retrieved" in m for m in captured) and getattr(fut, "_log_traceback", False) is False, captured)
dead = asyncio.new_event_loop(); stale = dead.create_future(); dead.close()   # a flight left behind by a dead loop
sf2 = md.SingleFlight(); sf2._futs["S"] = stale
counter2 = {"n": 0}
async def own():
    counter2["n"] += 1
    await asyncio.sleep(0.02)          # overlap, so the second caller finds the flight in progress
    return "own"
async def on_new_loop():
    return await asyncio.gather(sf2.run("S", own), sf2.run("S", own))
res_s = asyncio.run(on_new_loop())
check("G3 a stale foreign-loop Future is never awaited: the new loop runs its own single flight and clears the entry",
      res_s == ["own", "own"] and counter2["n"] == 1 and sf2.inflight() == 0, (res_s, counter2["n"], sf2.inflight()))

# ---------------------------------------------------------------- G4 ------ #
cb = md.AdvancedCircuitBreaker(fail_threshold=6, cooldown_sec=30.0); tb = pd.rate_limiter
locks = [pd.fund_cache._get_lock(), pd.err_cache._get_lock(), tb._get_lock(), pd.circuit_breaker._get_lock(), cb._get_lock(), pd.singleflight._get_lock(), md._get_provider_lock()]
check("G4 every helper lock is a threading.Lock (loop-agnostic)", all(isinstance(l, type(threading.Lock())) for l in locks), [type(l).__name__ for l in locks])
async def breaker_round():
    await cb.on_failure(500); await cb.on_failure(401); await cb.on_success(); return await cb.allow_request()
async def many():
    return await asyncio.gather(*[breaker_round() for _ in range(5)])
o1 = asyncio.run(many()); o2 = asyncio.run(many())
check("G4 breaker drives across two loops: no error, counters cumulative (successes 10, failures reset on success)",
      all(o1) and all(o2) and cb.stats.successes == 10 and cb.stats.failures == 0, (cb.stats.successes, cb.stats.failures))
async def prov_singleton():
    a = await md.get_provider(); b = await md.get_provider(); return a is b
check("G4 module singleton identical across loops through the threading guard",
      asyncio.run(prov_singleton()) and asyncio.run(prov_singleton()) and md._PROVIDER_INSTANCE is not None)

# ---------------------------------------------------------------- G5 ------ #
async def contended():
    pd.fund_cache._mem.clear(); pd.fund_cache._touch.clear()
    return await pd.fetch_fundamentals_batch(SYMS * 3, concurrency=4)
sizes = [len(asyncio.run(contended())) for _ in range(3)]
check("G5 delivered: three contended loops in a row, every symbol every time", sizes == [len(SYMS)] * 3, sizes)
if BASE:
    async def contended_b():
        pb.fund_cache._mem.clear(); pb.fund_cache._touch.clear()
        return await pb.fetch_fundamentals_batch(SYMS * 3, concurrency=4)
    out_b = []
    for _ in range(2):
        try: out_b.append(len(asyncio.run(contended_b())))
        except Exception as exc:  # noqa: BLE001
            out_b.append(f"raise:{type(exc).__name__}")
    check("G5 base: the same drive degrades (drops or raises) after the first loop", out_b[0] != len(SYMS) or out_b[-1] != len(SYMS), out_b)

# ---------------------------------------------------------------- G6 ------ #
src_d = open(DELIV, encoding="utf-8").read()
def defs(src):
    return {n.name for n in ast.walk(ast.parse(src)) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
dd = defs(src_d)
if BASE:
    db = defs(open(BASE, encoding="utf-8").read())
    check("G6 +2 defs (_observe_future, inflight), 0 removed", sorted(dd - db) == ["_observe_future", "inflight"] and not (db - dd), sorted(dd - db))
check("G6 no asyncio.Lock constructed in code; version 6.9.1", "asyncio.Lock()" not in src_d and md.VERSION == "6.9.1")
awaits_under_lock = []
for n in ast.walk(ast.parse(src_d)):
    if isinstance(n, ast.With):
        for it in n.items:
            if "_get_lock" in ast.unparse(it.context_expr) or "_get_provider_lock" in ast.unparse(it.context_expr):
                awaits_under_lock += [sub for sub in ast.walk(n) if isinstance(sub, ast.Await)]
check("G6 zero awaits inside any threading-lock section (AST)", not awaits_under_lock, len(awaits_under_lock))

digest = hashlib.sha256("|".join(digest_parts).encode()).hexdigest()[:16]
total = len(digest_parts)
print(f"[YF LOOPGUARD HARNESS] {total - fails}/{total} {'PASS' if not fails else 'FAIL'}  cases-digest={digest}")


def test_yf_loopguard_harness():
    """pytest entry point: the battery above runs at import; surface its verdict here
    instead of exiting the interpreter, which aborted every other module's collection."""
    assert fails == 0, f"{fails}/{total} YF loopguard case(s) failed (see printed FAIL lines)"


if __name__ == "__main__":
    sys.exit(1 if fails else 0)
