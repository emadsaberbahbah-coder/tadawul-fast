from __future__ import annotations
import asyncio, os, time, unittest
from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import patch
from scripts.concurrent_batch_fetch import build, get_metrics

@dataclass
class Result:
    request_id: str = "req"
    warnings: list[str] = field(default_factory=list)

class Backend:
    def __init__(self, fail_once=(), delay=.02):
        self.fail_once=set(fail_once); self.failed=set(); self.delay=delay
        self.active=0; self.max_active=0
    async def post_json(self, endpoint, payload):
        self.active+=1; self.max_active=max(self.max_active,self.active)
        try:
            await asyncio.sleep(self.delay); symbols=list(payload["symbols"]); key=symbols[0]
            if key in self.fail_once and key not in self.failed:
                self.failed.add(key); return None,"temporary",500
            return {"headers":["Symbol","Value"],"rows":[[s,s.lower()] for s in reversed(symbols)]},None,200
        finally:
            self.active-=1

def fake(size=1):
    return SimpleNamespace(
        _request_limit_ceiling=lambda:1000,
        _time_budget_exceeded=lambda:False,
        _symbol_batch_size=lambda:size,
        _batch_delay_ms=lambda:0,
        build_isolated_batches=lambda syms,n:[syms[i:i+n] for i in range(0,len(syms),n)],
        _endpoint_candidates_for_gateway=lambda g:["/e"],
        _extract_table_payload=lambda d:(d.get("headers",[]),d.get("rows",[])),
        _rectify_matrix=lambda h,r:r,
        _batch_identity_enabled=lambda:True,
        _guard_find_col=lambda h,a:0,
        _GUARD_SYMBOL_ALIASES={"symbol"},
        _guard_is_blank=lambda v:v is None or str(v).strip()=="",
        canonicalize_symbol=lambda v:str(v).strip().upper(),
        _BATCH_IDENTITY_TAG="[ID]",
        logger=SimpleNamespace(info=lambda *a,**k:None,warning=lambda *a,**k:None),
    )

class Tests(unittest.IsolatedAsyncioTestCase):
    async def test_bounded_concurrency_and_order(self):
        fn=build(fake()); backend=Backend(delay=.03)
        with patch.dict(os.environ,{"TFB_SYNC_BATCH_CONCURRENCY":"3","TFB_SYNC_BATCH_OUTER_RETRIES":"0"}):
            _,rows,_,_=await fn(backend,SimpleNamespace(sheet_name="Global_Markets"),list("ABCDEFG"),{},"analysis",Result())
        self.assertEqual(backend.max_active,3)
        self.assertEqual([r[0] for r in rows],list("ABCDEFG"))

    async def test_retry_recovers_failed_batch(self):
        fn=build(fake()); result=Result("retry")
        with patch.dict(os.environ,{"TFB_SYNC_BATCH_CONCURRENCY":"3","TFB_SYNC_BATCH_OUTER_RETRIES":"1"}):
            _,rows,_,_=await fn(Backend({"C"}),SimpleNamespace(sheet_name="P"),list("ABCD"),{},"analysis",result)
        self.assertEqual([r[0] for r in rows],list("ABCD"))
        self.assertEqual(get_metrics("retry")["failed"],0)

    async def test_concurrency_one_is_sequential(self):
        backend=Backend(); fn=build(fake())
        with patch.dict(os.environ,{"TFB_SYNC_BATCH_CONCURRENCY":"1","TFB_SYNC_BATCH_OUTER_RETRIES":"0"}):
            await fn(backend,SimpleNamespace(sheet_name="P"),list("ABC"),{},"analysis",Result("one"))
        self.assertEqual(backend.max_active,1)

    async def test_parallel_is_materially_faster(self):
        fn=build(fake()); symbols=list("ABCDEFG")
        with patch.dict(os.environ,{"TFB_SYNC_BATCH_CONCURRENCY":"1","TFB_SYNC_BATCH_OUTER_RETRIES":"0"}):
            t=time.perf_counter(); await fn(Backend(delay=.03),SimpleNamespace(sheet_name="P"),symbols,{},"analysis",Result("s")); seq=time.perf_counter()-t
        with patch.dict(os.environ,{"TFB_SYNC_BATCH_CONCURRENCY":"3","TFB_SYNC_BATCH_OUTER_RETRIES":"0"}):
            t=time.perf_counter(); await fn(Backend(delay=.03),SimpleNamespace(sheet_name="P"),symbols,{},"analysis",Result("p")); par=time.perf_counter()-t
        self.assertLess(par,seq*.65)

if __name__=="__main__":
    unittest.main()
