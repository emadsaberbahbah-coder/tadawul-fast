# Commit sheet — opportunity_builder v1.19.4 (build #1, 2026-09-06)

| Item | Value |
|---|---|
| Repo path (exact) | `core/analysis/opportunity_builder.py` |
| Base pinned | main `a26dfe8c` · blob `824c4730` (v1.19.3) — raw fetch == HEAD tarball, byte-identical |
| New blob (expected after commit) | `1b5b4fa7` (v1.19.4) · 5,428 lines (5,304 + 137 − 13 replaced) |
| Change class | Builder funding layer — rotation truth (gate (b) finding, 2026-09-06 morning audit) |
| ENV required | **None.** Kill-switch `TFB_OPP_ROTATION_FLOOR_GUARD` (default ON; `=0` ⇒ v1.19.3 byte-identical). No Render ENV change, no GitHub Variable. |
| Deploy | Push to main ⇒ Render auto-deploy restarts the web service. With `TFB_ENGINE_FUND_LKG_REDIS=1` armed this morning the fundamentals store re-hydrates from L2 — the restart no longer blanks fundamentals. |
| Commit message | `opportunity_builder v1.19.4: rotation funding floor + exit-leg provenance (P-45 exit side); kill-switch default ON` |

## What changes (anchored edits, count==1 each)
1. `OPPORTUNITY_BUILDER_VERSION` 1.19.3 → 1.19.4 + changelog block (evidence, rules R1–R3, gate, read-back).
2. `_ROTATION_DEGRADED_TOKENS`, `_env_rotation_floor_guard()`, `_holding_forecast_degraded(cand)` added.
3. `_holding_roi_map`: guard ON ⇒ degraded pool rows (synthesized `forecast_source`, or `rank_skipped_low_trust` / `momentum_only_fallback` in warnings) are skipped, so such a holding never rotates. **[R3]**
4. `_rotation_pick(..., min_value_sar=0.0)`: holdings that cannot cover the shortfall are skipped when > 0. **[R1]**
5. `_funding_plans(..., floor_sar=0.0)`: no full cover ⇒ `CAPITAL_CALL` + `rotation_insufficient` (once per run, budget not consumed); TRIM leaving a sub-floor remnant ⇒ `EXIT` with `surplus_sar`. **[R1/R2]**
6. `_funding_plan_text`: `ROTATION_INSUFFICIENT: exit <sym> covers X of Y (...); no exit proposed` · remnant/surplus clause.
7. Call site passes `floor_sar=_min_floor`; `kpis["rotation_insufficient"]` additive key.

Signatures extended with defaults only (7-arg `_funding_plans` / 6-arg `_rotation_pick` still work). Gate list, GATE_ORDER, verdicts, sizing, venue floors/lots, edge/cost rule, one-rotation-per-run, exclusions, FX: untouched.

## Proofs
| Step | Result |
|---|---|
| py_compile | OK |
| AST zero-removal | defs removed **0**; added 2 (`_env_rotation_floor_guard`, `_holding_forecast_degraded`); constants removed 0, added 1 |
| Smart-quote scan | CLEAN |
| Lean CI (`tests/test_opportunity_builder.py` + `tests/test_top10_selector.py`, real module) | **47 passed** |
| Real-module harness ×3 (24 checks: today's case ON, OFF ≡ pinned v1.19.3 module byte-identical plans+text, full-cover TRIM, remnant→EXIT, floor OFF, budget not consumed, degraded truth table, backward-compatible signatures) | **24/24 PASS ×3**, identical output all three runs |
| End-to-end via `build_opportunity_payload` ×3 (ON vs OFF, real request flow incl. the edited call site) | **PASS ×3** — ON: `capital_call 1, fundable_by_rotation 0, rotation_insufficient 1`, no `rotation_proposal` alert; OFF: reproduces today's 08:08 clause verbatim (`exit PFS 445 SAR ... +34.9pp ... residual CAPITAL_CALL 4,504 SAR`) |

## Read-back after deploy (next cockpit refresh, any run)
- DEC.US near-miss reads `CAPITAL_CALL: deposit ≥ 4,949 SAR for a 5,000 SAR ticket (cash 51 SAR) — ROTATION_INSUFFICIENT: exit YUM covers 3,617 SAR of 4,949 SAR (engine 12M edge +18.4pp after cost); no exit proposed` (panel cash 50,000 unchanged).
- PFS is never named; ALERTS carries no `rotation_proposal`; KPI `By Rotation` = 0; `capital_call_topn_sar` rises by the 445 SAR the old text netted out.
- Kill: set `TFB_OPP_ROTATION_FLOOR_GUARD=0` ⇒ v1.19.3 text returns.

## Post-commit verification (Claude)
HEAD SHA on main re-fetched; blob of `core/analysis/opportunity_builder.py` must equal `1b5b4fa7`; no unexpected new files in the tree (harness files were NOT delivered — nothing to commit besides the module).

## Finding registered during the build (NOT in this change — next build)
`_normalize_portfolio` keeps only `symbol / sector / market / value_sar`, so the v1.18.1 rotation criteria (`buy_date` ≥ 7 d, TP1 proximity via `tp1_sar`/`price_sar`) never receive their fields in the request flow and fail open — every holding is age-eligible in production (HCI.US bought 09-03 would have been a rotation candidate under v1.19.3 had its forecast not been synthetic). Fix is three additive lines in the normalizer; proposed as build #2 (v1.19.5) with its own harness.
