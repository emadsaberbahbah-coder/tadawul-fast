import asyncio, gc, importlib.util, logging, os, sys
for k, v in {"EODHD_API_KEY":"HARNESSKEY","EODHD_RETRY_ATTEMPTS":"0","EODHD_RATE_LIMIT_RPS":"50","EODHD_RATE_LIMIT_BURST":"200","EODHD_MAX_CONCURRENCY":"2","TFB_EODHD_DAILY_BUDGET":"0"}.items():
    os.environ[k] = v
import httpx
async def fake_get(self, url, params=None, **kw):
    req = httpx.Request("GET", url)
    if "FAILSYM" in url: raise httpx.ConnectError("harness-forced-failure", request=req)
    await asyncio.sleep(0.01)
    return httpx.Response(200, json={"code":"X","close":10.5,"previousClose":10.0,"open":10.1,"high":10.9,"low":9.9,"volume":1000,"timestamp":1757400000}, request=req)
httpx.AsyncClient.get = fake_get  # network edge only; every class under test is REAL

LOOPMSG = lambda s: ("different event loop" in s) or ("different loop" in s) or ("Event loop is closed" in s)
class Cap(logging.Handler):
    def __init__(self): super().__init__(); self.recs=[]
    def emit(self, r): self.recs.append(r.getMessage())
def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec); sys.modules[name]=m; spec.loader.exec_module(m); return m
def burst(mod, syms, dup=4):
    async def run():
        res = await asyncio.gather(*[mod.fetch_enriched_quote_patch(s) for s in syms for _ in range(dup)], return_exceptions=True)
        loopfail = 0
        for r in res:
            if isinstance(r, Exception) and LOOPMSG(str(r)): loopfail += 1
            elif isinstance(r, dict) and LOOPMSG(str(r)): loopfail += 1
        hards = sorted({type(r).__name__+": "+str(r)[:70] for r in res if isinstance(r, Exception) and not LOOPMSG(str(r))})
        hard = sum(1 for r in res if isinstance(r, Exception) and not LOOPMSG(str(r)))
        rt = sum(1 for r in res if isinstance(r, dict) and str(r.get("last_error_class")) == "RuntimeError")
        return len(res), loopfail, hard, (hards[0] if hards else "-"), rt
    return run
async def t02(mod):
    msgs = []
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda l, ctx: msgs.append(str(ctx.get("message",""))))
    try: await mod.fetch_enriched_quote_patch("FAILSYM.US")
    except Exception: pass
    for _ in range(3):
        gc.collect(); await asyncio.sleep(0)
    return sum(1 for m in msgs if "never retrieved" in m)
SYMS_A = ["AAPL.US","MSFT.US","DDI.US","OTIS.US","YUM.US","SBAC.US","EPRT.US","VEL.US"]
SYMS_B = ["HCI.US","CRC.US","DEC.US","VTOL.US","GLNG.US","ITRN.US","ECVT.US","AUB.US"]
SYMS_C = ["NVDA.US","AMD.US","INTC.US","TSM.US","AVGO.US","MU.US","QCOM.US","ARM.US"]
cap = Cap(); logging.getLogger("core.providers.eodhd_provider").addHandler(cap); logging.getLogger("core.providers.eodhd_provider").setLevel(logging.INFO)
acap = Cap(); logging.getLogger("asyncio").addHandler(acap); logging.getLogger("asyncio").setLevel(logging.ERROR)

# --- GOLDEN NEGATIVE: original v4.17.0 must exhibit the defect on loop 2 ---
orig = load("eodhd_orig", "eodhd_provider_v4170_ORIGINAL.py")
n1, lf1, h1, d1, rt1 = asyncio.run(burst(orig, SYMS_A)())
bound = getattr(orig._INSTANCE._sem, "_loop", None)
wb1 = (bound is not None and bound.is_closed())
acap.recs.clear()
try:
    n2, lf2, h2, d2, rt2 = asyncio.run(burst(orig, SYMS_B)()); raised = False
except RuntimeError as e:
    n2, lf2, h2, d2, rt2, raised = 0, (1 if LOOPMSG(str(e)) else 0), 0, type(e).__name__+": "+str(e)[:70], 0, True
unret2 = sum(1 for m in acap.recs if "never retrieved" in m)
reproduced = wb1 and (lf2 > 0 or rt2 > 0 or unret2 > 0)
print(f"NEG run1: tasks={n1} loopfail={lf1} hard={h1} rtclass={'0' if rt1==0 else '>=1'} diag={d1}")
print(f"NEG whitebox after run1: singleton sem bound to a CLOSED loop={'YES' if wb1 else 'NO'}")
print(f"NEG run2: cross-loop defect reproduced={'YES' if reproduced else 'NO'} (raised={raised}) rtclass_patches={'>=1' if rt2>0 else '0'} unretrieved_reports={'>=1' if unret2>0 else '0'}")
t02_orig = asyncio.run(t02(orig))
print(f"NEG T02 (original): unretrieved-future warnings={'>=1' if t02_orig>=1 else '0'}")

# --- FIXED v4.18.0 ---
cap.recs.clear(); acap.recs.clear()
fx = load("eodhd_fixed", "eodhd_provider.py")
f1 = asyncio.run(burst(fx, SYMS_A)())
f2 = asyncio.run(burst(fx, SYMS_B)())
f3n, f3lf, f3h, f3d, f3rt = asyncio.run(burst(fx, SYMS_C)())
fix_rt = f1[4] + f2[4] + f3rt
fix_unret = sum(1 for m in acap.recs if "never retrieved" in m)
rebuilds = sum(1 for m in cap.recs if "EODHD-LOOPGUARD" in m)
async def snap(m):
    h = await m._get_health()
    return await h.snapshot()
def findkey(d, key):
    if isinstance(d, dict):
        if key in d: return d[key]
        for v in d.values():
            r = findkey(v, key)
            if r is not None: return r
    return None
s = asyncio.run(snap(fx))
cont = findkey(s, "total_requests") or 0
t02_fixed = asyncio.run(t02(fx))
ok = (f1[1]==0 and f2[1]==0 and f3lf==0 and f1[2]==0 and f2[2]==0 and f3h==0 and fix_rt==0 and fix_unret==0 and rebuilds==2 and t02_fixed==0 and reproduced)
print(f"FIX run1: tasks={f1[0]} loopfail={f1[1]} hard={f1[2]} diag={f1[3]}")
print(f"FIX run2: tasks={f2[0]} loopfail={f2[1]} hard={f2[2]} diag={f2[3]}")
print(f"FIX run3: tasks={f3n} loopfail={f3lf} hard={f3h} diag={f3d}")
print(f"FIX loopguard rebuild log lines={rebuilds} (expect 2: runs 2 and 3 rebuilt; run 1 was the fresh build)")
print(f"FIX health continuity across loops: total_requests>0={'YES' if cont>0 else 'NO'}")
print(f"FIX RuntimeError-class patches across 3 runs={fix_rt} (expect 0) | unretrieved reports={fix_unret} (expect 0)")
print(f"FIX T02: unretrieved-future warnings={t02_fixed} (expect 0)")
print("HARNESS VERDICT:", "PASS" if ok else "FAIL")
