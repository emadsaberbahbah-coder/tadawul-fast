# TFB Commit Sheet — run_dashboard_sync.py v6.59.0 (CRIT-FRONT)
Date: 2026-09-07 (morning session) | Author: Claude | Operator: Emad | Register: **closes P-104**, opens P-105/P-106

## Source pin
- Branch: main @ `e95671c0915caf80ac3a41738bdbbcbd71092be9` (git ls-remote, this session)
- BEFORE sha256: `2b95fb2dae8d7e8ac536ccf45daffe00d22a94a878c06ed92bc0feebd286bb5d` (v6.58.0)
- AFTER  sha256: `054be415d2a108db99b66c5f7b0a12de850bb518b0fb6a5e717c9a0f7cb02c1c` (559,948 bytes)

## Defect (evidence-verified)
Run 34081609919, GM leg, 2026-09-07 08:04:44+03:
`[v6.22.4 TIME-BUDGET] 3600s exhausted after 124/268 batches` → `[CRITICAL-IDENTITY v1.0.0] FISV.US (missing fresh response row)` → leg=FAILED → `NOT_ACTIONABLE(failed:GM)`.
Mechanism: OLDEST-FIRST (v6.27.0) sorts a critical refreshed last leg to the TAIL; TIME-BUDGET cuts before reaching it; `validate_fresh_critical_rows` (correctly) refuses predecessor proof. Deterministic ping-pong across legs (next victim: BNY.US, refreshed this run).

## Change
Front every registry critical present on the page as the OUTERMOST worklist promotion (ahead of DECISION-FIRST), reusing `_apply_decision_first` stable partition. DEFAULT ON (live feed-down fix; precedent v5.111.0 FINAL_ACTION_INVARIANT). Kill: `TFB_SYNC_CRIT_FRONT=0` (workflow-side per ENV placement rule). Runner-side script — **no Render deploy**; effective first workflow run after merge.

## Anchored edits (all `count==1` asserted)
| ID | Site | Edit |
|---|---|---|
| E1a/E1b | imports (both branches) | + `CRITICAL_FETCH_SYMBOLS` |
| E2 | line 1838 | `SCRIPT_VERSION 6.58.0 → 6.59.0` |
| E3 | tag constants | + `_CRIT_FRONT_TAG = "[CRIT-FRONT v6.59.0]"` |
| E4 | after `_priority_fetch_enabled` | + `_crit_front_enabled()` (default ON, kill env) |
| E5 | after DECISION-FIRST block | + CRIT-FRONT promotion + full WHY block |

## Audits
- py_compile: PASS
- AST proof: defs/classes 267 → 268; **removed = 0**; added = `_crit_front_enabled` only
- Quote scan: PASS on inserted regions (curly quotes forbidden; em-dash retained as established house comment style — scan spec clarified this session)

## Harness ×3 (REAL modules imported: run_dashboard_sync + critical_symbol_identity)
- SCRIPT_VERSION=="6.59.0"; default-ON and kill-switch verified
- **Measured** `CRITICAL_FETCH_SYMBOLS` = `['BNY.US','BRK-B.US','FISV.US']` (n=3)
- Fixture (fresh-first 300-symbol list, criticals at tail + bare alias `FI`): fronted = `['FISV.US','BNY.US','FI']`, rest order preserved; output hash identical over 3 trials (`b5b4852531439baf…`)
- Trial-0 note (honesty record): first fixture wrongly expected `GENZ.US` fronted; GENZ is a canonical alias but NOT proof-required — code behavior was correct, fixture corrected.

## First-run verification checklist
1. GM log shows `[CRIT-FRONT v6.59.0] Global_Markets: fronted N registry critical(s)` before batch loop.
2. GM leg completes with `status!=failed` even at <100% coverage; no `missing fresh response row` for BNY/BRK-B/FISV.
3. `TFB Decision Feed` global key returns `ACTIONABLE`/`OK`; Top_10 no longer WITHHELD on feed grounds.

## Rollback
`TFB_SYNC_CRIT_FRONT=0` in daily_sync workflow env (one line) or revert commit — v6.58.0 byte-identical ordering either way.

## Register updates
- **P-104 — CLOSED** by this build (evidence above; confirm via checklist on first run).
- **P-105 — NEW (structural):** GM full pass = 268 batches ≈ 130 min > TIME-BUDGET 3600s and job cap 115 min → 46% coverage/leg is architectural. Options for operator decision (separate item, not bundled): raise leg budget, split GM into parallel sub-legs, or batch-size/throughput work. W2-adjacent.
- **P-106 — NEW (hygiene, LOW):** stale FORCE-REFETCH envs printing every leg — `KE.US`, `LINK.US` verified-name lines say "verify identity, then REMOVE the env". Also LOW: `APP_VERSION=5.111.0` env label stale.
