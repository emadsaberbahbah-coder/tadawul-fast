# TFB — P-110 LOOPGUARD Build & Evidence Sheet
**Date:** 2026-09-09 · **File:** `core/providers/eodhd_provider.py` · **Version:** 4.17.0 → **4.18.0**
**Verdict:** ✅ ONE-PASS COMPLETE — harness PASS ×3 byte-identical (Py 3.12.3) + production-parity PASS (Py 3.11.15)

---

## 1. Pins (S1)
| Artifact | SHA256 | Lines |
|---|---|---|
| BASE `eodhd_provider.py` @ main (v4.17.0) | `e1bcd31b684363c8dda4f507e1eb849005e4da9c114945cda51d1cb13bb5f929` | 3,593 |
| Read-only witness `top10_selector.py` @ main | `98ea0c722283d8fe533053c067de81447aea3b6161c8a8dcfe5c1baf6f1d4cf5` | 5,499 |
| **DELIVERED `eodhd_provider.py` (v4.18.0)** | `efbd7c14b67cde8180db4ddbc8fd0f1cbfc17b2a2c3340bcd959b0a63cfa8677` | 3,699 |

Red-team cited regions verified at their exact lines before editing (wrapper 5371–5377; singleton 3335–3345; sem await 2243; `_SingleFlight.do` 1948–1977). Defect surface confirmed **wider than reported**: `_INSTANCE_LOCK` (L3336) and `_HEALTH_LOCK` (L1503) are themselves loop-bound `asyncio.Lock`s.

## 2. Edit ledger (S3) — 8 anchored edits, every anchor count==1
| # | Edit | Why |
|---|---|---|
| E1 | `import threading` | new loop-agnostic guards |
| E2 | WHY block + `PROVIDER_VERSION = "4.18.0"` | history preserved verbatim, additions only |
| E3 | `_ProviderHealth.__init__`: lock → `threading.Lock()` | counters are process-cumulative by design; must survive loops |
| E4 | 5× `async with self._lock:` → `with self._lock:` (span-scoped to `_ProviderHealth`, exact-count 5 asserted) | audited: zero awaits under lock — thread lock safe |
| E5 | `_HEALTH_LOCK` (asyncio) → `_HEALTH_TLOCK` (threading) in `_get_health()` | guard itself was loop-bound |
| E6 | new `_sf_observe_exception()` helper | red-team T02 |
| E7 | `fut.add_done_callback(_sf_observe_exception)` at single-flight future creation | owner-only failure can no longer leave an unretrieved Future |
| E8 | Loop-aware singleton: `get_client()` keyed to running loop; `_INSTANCE_TLOCK` (threading); `_retire_client()` schedules `aclose()` on the OLD loop via `run_coroutine_threadsafe`, else bounded `_GRAVEYARD` (max 4); structured `[EODHD-LOOPGUARD v4.18.0]` WARNING on rebuild with `rebuild_no / old_loop_closed / graveyard` | root cause |

**Intentionally NOT changed:** per-client primitives (`_sem`, `_bucket`, `_sf`, caches, `_budget_lock`, httpx client) — rebuilt with the client, correct; function-local batch semaphore L3444 — created per call, correct; `top10_selector.py` — untouched, its `asyncio.run` wrapper is legitimate once the provider is loop-aware.

## 3. Audits (S4a)
py_compile ✅ · AST census: **zero removals**, added exactly `{_retire_client, _sf_observe_exception}` ✅ · smart-quote scan 0→0 ✅ · residual `async with self._lock` in `_ProviderHealth` span: 0 ✅

## 4. Harness evidence (S4b) — real module end-to-end, only `httpx.AsyncClient.get` faked
Config: concurrency=2 (forces contention → binds primitives), 3 disjoint 8-symbol sets ×4 dups (defeats 12s quote cache + exercises single-flight), retry=0, budget off.

| Golden | v4.17.0 (original) | v4.18.0 (fixed) |
|---|---|---|
| Run 1 (loop A), 32 tasks | clean | clean |
| White-box after run 1 | **sem bound to a CLOSED loop = YES** | n/a (fresh client per loop) |
| Run 2 (loop B) | **defect reproduced: `last_error_class=RuntimeError` degraded patches ≥1** + stderr flood of `is bound to a different event loop` at L2243 via `_sf.do` L1966 (fetch_quote 2487 / fundamentals 2764 / history 2967) | 32/32 clean, 0 loop errors |
| Run 3 (loop C) | — | 32/32 clean; **rebuild log lines = 2 exactly** |
| Health continuity across loops | — | total_requests accumulates (singleton preserved) ✅ |
| T02 owner-only failure | unretrieved-future warnings **≥1** | **0** ✅ |
| RuntimeError-class patches / unretrieved reports (3 runs) | — | **0 / 0** ✅ |

Reproducibility: **×3 byte-identical** on Python 3.12.3; **production-parity PASS on Python 3.11.15** (Render runtime is 3.11.9; on 3.11 the loop check fires on *every* acquire, so the fix is strictly necessary there). Harness shipped as `harness_loopguard.py` (reusable closure test for red-team T01/T02).

## 5. New secondary finding (Register-worthy, fold into P-110 impact)
On the second and later event loops, v4.17.0 does **not** always crash loudly: cross-loop RuntimeErrors inside the secondary single-flight fetches are **silently degraded** into error patches (`error=fetch_failed`, `error_detail=exception:RuntimeError,…`, `last_error_class=RuntimeError`) — the word "loop" never reaches the sheet — while the only visible symptom is "Future exception was never retrieved" noise. Production impact is therefore **silent data degradation of enriched quotes on sync-entry paths**, not just log noise. v4.18.0 removes the root cause; both channels go quiet (verified 0/0 above).

## 6. Deployment & arming
- **ENV changes: NONE.** Requirements unchanged (`httpx[http2]` already implied by `http2=True` + existing pin). **Rollback = `git revert`** (redeploy at BASE SHA `e1bcd31b…`).
- Suggested commit: `fix(provider): v4.18.0 LOOPGUARD — loop-aware client singleton + thread-safe health guard + single-flight exception observance (P-110)`
- Post-deploy observe (one live cycle, no arming beyond deploy): the two error signatures (`bound to a different event loop`, `Future exception was never retrieved`) absent from Render logs across a scheduler tick + one manual Top-10 run; if any sync-entry path runs, at most one `[EODHD-LOOPGUARD v4.18.0]` WARNING per new loop with a small `rebuild_no`.
- Closure mapping: red-team **T01** = harness NEG/FIX sequential-loop goldens; **T02** = owner-failure goldens. Render record `f000f42a-4dfc-4c95-8b11-b3ebdf3a495d` becomes the *before* exhibit.
