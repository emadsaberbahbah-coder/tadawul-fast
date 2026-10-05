# TFB Commit Sheet — core/providers/yahoo_fundamentals_provider.py v6.9.0 [P-203 LOOPGUARD — async state survives asyncio.run() per call] + tests/test_yf_loopguard_p203.py

Date: 2026-10-05 (Monday) · Lane: Render / Python (engine provider) · Build #3 of the day (B3) · Protocol: One-Pass · Gate: **none** (the OFF state is the defect — v4.18.0 / P-139 precedent; veto = re-paste v6.8.0)

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `core/providers/yahoo_fundamentals_provider.py` **v6.8.0** at HEAD `2ad0598`, sha256 `cde23bb022ce32449a3a361dc931160725035e0ede6ce4e56359c41ff93701e3`, 2,697 lines, 113 defs |
| Delivered | **v6.9.0**, sha256 `35bdbabee5553381f7ba6cf12cb7e482ce55787fd20eb3db9193f84ff782139f`, 2,785 lines, 115 defs (**+2, 0 removed**: `SingleFlight._observe_future`, `SingleFlight.inflight`; +1 dataclass field `_sem_loop`), `py_compile` PASS, 0 smart quotes; 122 added / 34 re-wrapped lines |
| Harness | `tests/test_yf_loopguard_p203.py`, sha256 `4b6b1eaa…a60f39`, 199 lines — REAL module, dual-tree (env `YF_BASE` = the v6.8.0 file), **zero network** (the blocking yfinance leg is a sync stub, `_configured` forced True, Redis off) |

## S2 — Root cause (pinned on source; Audit Reconciliation 2026-10-05 N2; Render 2026-10-04 17:53:09Z ×4 events)
The module keeps a **process-global singleton** (`get_provider`) whose `asyncio.Semaphore` (`max_concurrency`), the `asyncio.Lock` of every helper (`AdvancedCache` ×2, `TokenBucket`, `AdvancedCircuitBreaker`, `SingleFlight`) and the single-flight `Future`s bind to the **first loop that contends them** (Python ≥ 3.10 `_LoopBoundMixin`). Sync entry points (`core/analysis/top10_selector.build_top10_rows` → `asyncio.run(...)`, L5391/L5448) create a **new loop per cockpit build**; the next contended `acquire()` raises `… is bound to a different event loop`, a non-owner can await a foreign `Future`, and an owner-only failure leaves an unretrieved `Future`. Because `fetch_fundamentals_batch` gathers with `return_exceptions=True`, the symptom in production is **silent data loss** (symbols dropped), not a crash — the harness golden-negative reproduces exactly that: base loop 2 returns **2 of 6** symbols. eodhd_provider was repaired for this class in v4.18.0 LOOPGUARD (P-110, 2026-09-09); `argaam_provider.py` and `yahoo_chart_provider.py` carry the same module-global `asyncio.Lock` pattern (B6, Tuesday).

## S3 — Change (20 anchored edits, each `count == 1`)
| # | Site | Edit |
|---|---|---|
| E1 | docstring banner · WHY block · `PROVIDER_VERSION = "6.9.0"` | the v6.9.0 WHY/FIX block above the v6.8.0 block |
| E2 | imports | `import threading` |
| E3 | `AdvancedCache` | `_lock` → `threading.Lock`; the four sections (`get` ×2, `set`, `size`) become sync `with` — audited: no await inside any of them |
| E4 | `TokenBucket` | lock → `threading.Lock`; the `asyncio.sleep` stays **outside** the section |
| E5 | `AdvancedCircuitBreaker` | lock → `threading.Lock` on `allow_request` / `on_success` / `on_failure` (metrics + logger are sync) |
| E6 | `SingleFlight` | lock → `threading.Lock`; **a stored `Future` whose `get_loop()` is not the running loop is treated as absent** (a flight left by a dead loop is never awaited — the caller runs its own); every `Future` gets `_observe_future` as done-callback (exception retrieved → no "never retrieved"); `inflight()` diagnostic; `finally` pops only its own entry |
| E7 | `YahooFundamentalsProvider` | new slot field `_sem_loop`; `_get_semaphore()` returns the Semaphore **of the running loop** — a different loop gets a fresh one with the same cap, the previous is dropped |
| E8 | module singleton | `_PROVIDER_LOCK` → `threading.Lock`; `get_provider()` keeps its async signature (critical section = instantiation only) |

Behaviour on one loop is unchanged (same concurrency cap, same single-flight dedup, same breaker/limiter/cache semantics — proven by G2/G3/G4). Health counters stay process-cumulative across loops by design (v4.18.0 doctrine). No ENV, no YAML, no arming.

## S4 — Audits (REAL module, dual-tree, ×3 identical)
| Battery | Result |
|---|---|
| `tests/test_yf_loopguard_p203.py` G1–G6 (with `YF_BASE`) | **17/17 PASS ×3, cases-digest `9775733dd8d6d91f`** — **G1 golden-negative on BASE**: REAL `fetch_fundamentals_batch` across two `asyncio.run()` loops with provider-level contention (cap 1, 4 callers): loop 1 = 6/6, **loop 2 = 2/6, silently** (the production defect) · **G2 delivered**: 6/6 on both loops, stub called exactly once per symbol per loop (single-flight intact), Semaphore identical within a loop / re-created across loops / cap preserved · G3: 8 concurrent callers share ONE call per loop (2 loops → 2 calls); an owner-only failure is observed (`_log_traceback` cleared, no loop exception-handler report); a stale Future from a closed loop is never awaited (new loop runs its own flight, entry cleared) · G4: all seven helper/module locks are `threading.Lock`; a breaker driven across two loops keeps cumulative counters without error; `get_provider()` identical across loops · G5: three contended loops in a row — delivered 6/6/6, base 2/2 · G6: +2 defs / 0 removed, no `asyncio.Lock()` left in code, **zero awaits inside any lock section (AST)** · CI shape (no `YF_BASE`): 13/13, digest `62ecebb5b20ed95f` |
| Existing battery | `tests/test_fund_unit_sentry.py` **12/12** on the delivered tree; `core.data_engine_v2` imports against the delivered provider (engine 5.151.0, provider 6.9.0) |
| Static | `py_compile` PASS · 0 smart quotes |

Harness runtime note: the golden-negative needs the **provider** Semaphore contended (cap 1 vs 4 callers); with cap ≥ batch concurrency the per-call `batch_sem` hides the defect — which is also why a quiet day can pass and a busy sync does not.

## S5 — Delivery (3 files)
`core/providers/yahoo_fundamentals_provider.py` · `tests/test_yf_loopguard_p203.py` · `docs/evidence/TFB_Commit_Sheet_yahoo_fundamentals_provider_v6.9.0_2026-10-05.md` (this sheet)

## S6 — Operator steps (one action each)
1. Commit the 3 files **together with B2's 8 files in ONE push** (a push to `main` restarts Render — Monitoring Sheet #8 addendum): https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/core/providers (the .py), https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/tests (the test), https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/docs/evidence (this sheet). Commit message: `yahoo_fundamentals_provider v6.9.0 [P-203 LOOPGUARD: loop-agnostic guards, loop-keyed semaphore, observed single-flight futures] + harness`.
2. Deploy: the push deploys it (Auto-Deploy appears ON — see the addendum); otherwise bundle with Wednesday's Manual Deploy. **Read-back** = `/health` → `engine_present: true`, boot clean, and on the Render log window after the next cockpit build (two `asyncio.run` loops in one worker life): **zero** `bound to a different event loop` / `Future exception was never retrieved` lines, and the `[FRESHNESS v1.23.0]` warnings still printing (the request path alive).
3. B6 (Tuesday): the same port for `core/providers/argaam_provider.py` and `core/providers/yahoo_chart_provider.py`.

## Known limits / deliberate cuts
- The optional async Redis client inside `AdvancedCache` (`YF_ENABLE_REDIS`, default off, off in production) is also loop-bound; its errors are swallowed (`except: pass`) so it degrades to the in-memory tier rather than failing. Loop-keying it is a later item if Redis is ever enabled for this cache.
- `fetch_fundamentals_batch` still swallows per-symbol exceptions (`return_exceptions=True`); a dropped symbol stays silent at this layer by design (the engine's fallback and fetch-failed tags carry the signal).
- No kill switch: a veto is a re-paste of v6.8.0 (`cde23bb0…`).
