# TFB — P-112 Switch-Scan Build & Evidence Sheet (F14)
**Date:** 2026-09-09 · **Files:** `core/analysis/portfolio_actions.py` 1.9.0 → **1.10.0** + `core/analysis/top10_selector.py` (one fail-soft feeder)
**Verdict:** ✅ ONE-PASS COMPLETE — harness PASS ×3 byte-identical, real modules, live-pinned OB witness.
**Operator origin:** Emad's 2026-09-09 finding — the PF page judges holdings with pre-purchase logic and never compares remaining expected return against the best executable alternative net of switch costs (OTIS/YUM case).

## 1. Pins
| Artifact | SHA256 | Note |
|---|---|---|
| BASE `portfolio_actions.py` v1.9.0 @ main | `b64f826f682a56e87920f4f1e11034f2d58fbf61a6b378b2c8b73974b171ecce` | 2,854 lines |
| BASE `top10_selector.py` @ main | `98ea0c722283d8fe533053c067de81447aea3b6161c8a8dcfe5c1baf6f1d4cf5` | same pin as the P-110 build |
| Witness `opportunity_builder.py` @ main (harness import) | `a1c343ca79c29db0…` | real OB, floor (1,9,1) satisfied |
| **DELIVERED `portfolio_actions.py` v1.10.0** | `b3f16c2cdef401bb78dfdee9b82bb3819309ad31144ca6be5fd346f047e934d2` | |
| **DELIVERED `top10_selector.py`** | `5121ac0e76a50dddb37b34aeac8c31e29f09ee87b16424bd993e13bbe71dcb0d` | |

## 2. Key architectural finding (register-worthy)
The §18.3/§18.4 machinery (`switch_test`, `advisor_switch_scan`, D-9 sukuk-anchor protection, 2×RT+buffer hurdle) has existed since v1.2.0 (2026-07-18) — but its only callers were **offline** (`scripts/run_weekly_brief.py`, `scripts/run_shadow_board.py`). The live Portfolio_Decision page never ran it and had no candidate feed. F14 wires the existing PURE core into `_build`; it does not reimplement the math.

## 3. Edit ledger (all anchors count-asserted)
**A — portfolio_actions.py:** E0 `import time` · E1 version + F14 WHY block · E2 switch cluster (`_SWITCH_CANDS` cache, `set_switch_candidates()`, env gates, `_switch_candidate_eligible()`, `_run_switch_scan()` orchestrator) · E3 gated scan call before payload assembly · E4 `meta.switch_scan` injection (present only when armed) · E2b harness-caught fix: `"DO_NOT_INVEST"` contains `"INVEST"` — substring filter hardened.
**B — top10_selector.py:** `_feed_switch_cache()` (lazy import of PA, no cycle, absolutely fail-soft, returns rows unchanged) + single wrap at the payload factory `"rows": _feed_switch_cache(rows)` — every successful board build, from any route or the scheduler, refreshes the candidate cache.

Design properties: **advisory-only (§4.7** — annotates/reports, never changes an action); **executable-only candidates** (INVEST tier; fast-track / sizing-suspended / grace seats excluded — the shadow-board v1.1.2 eligibility lesson); **freshness-capped** (default 6h; a stale board refuses to advise); **2-consecutive-scan persistence** through the existing confirm-redis store (`swc:` namespace) before SWITCH-WATCH promotes to SWITCH-CANDIDATE; **fees already inside the hurdle** (2×max round-trip cost + buffer). Surfacing v1 = `meta.switch_scan` + one `[PF v1.10.0 SWITCH]` log line per proposal; sheet row-note text is F15 after one live payload observation.

## 4. Audits & harness (×3 byte-identical, Python 3.12.3, package mode with real OB)
py_compile both ✅ · AST census: zero removals; added exactly {6 functions} in A, {`_feed_switch_cache`} in B ✅ · smart-quote 0→0 both ✅
| Golden | Result |
|---|---|
| G1 gate OFF ⇒ payload parity with v1.9.0 (versions/timestamps scrubbed), no `switch_scan` key | PASS |
| G2 armed + empty cache ⇒ `no_candidates` | PASS |
| G3 armed + 7h-old board ⇒ `stale_candidates` | PASS |
| G4 mixed candidates 3 ⇒ eligible **1** (DO_NOT_INVEST + fast-track excluded) | PASS |
| G5 first scan ⇒ `pending_persistence`, OTIS→ALT1.US, persist_day=1 | PASS |
| G6 second scan ⇒ `proposals` (SWITCH-CANDIDATE), persist_day=2 | PASS |

## 5. Deployment & arming (house discipline: default OFF ⇒ deploy is behavior-identical)
1. Replace both files + add `tests/test_switch_scan_wiring.py`; commit:
   `feat(pf): v1.10.0 F14 — live switch scan on the PF page (P-112); top10 board feeds the candidate cache (advisory-only, env-gated OFF)`
2. Deploy = safe immediately (gate unset ⇒ byte-identical behavior; G1 is the proof).
3. **Arming = ONE Render env:** `TFB_PF_SWITCH_SCAN=1` (optional tuning: `TFB_PF_SWITCH_BUFFER_PCT` default 1.0, `TFB_PF_SWITCH_MAXAGE_H` default 6.0).
4. Observe read-back (first armed morning): Top-10 run then PF run in the normal order; PF `meta.switch_scan` shows `candidates_total>0`, fresh `asof_age_h`, and either an honest `no_action`/`keep` or `pending_persistence` day-1 entries; Render logs carry `[PF v1.10.0 SWITCH]` lines only for clearing pairs. Second morning: persisted pairs promote or drop.
5. Rollback = env to 0 (instant) or `git revert`.

## 6. Register updates (fold into today's sheet)
- **P-112: OPEN → BUILT-AWAITING-ARMING** (this sheet = evidence). Scope note: candidate/holding `roi_pct` is the valuation-basis column — consistent on both sides of the comparison, but inherits P-102/P-108 caveats; F15 will add the engine-forecast basis option once P-108 lands.
- New finding: §18 switch machinery live-page wiring gap since v1.2.0 (offline-only consumers) — root cause of the operator-observed blind spot.
- F15 backlog: sheet row-note surfacing; GAS truth-strip line; vf-conflict holding skip-tag; per-proposal cash-feasibility check.
