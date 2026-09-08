# TFB Commit Sheet — Freshness-Audit Clock Fix v1.1.0 (two files, one atomic change)
**Date:** 2026-09-08 · **Register:** P-104 · **Type:** audit-truth fix · **ENV:** none · **Production data path:** untouched

## Pins (live-fetched main, no drift vs 06:20Z tarball)
| File | Base SHA-256 (v1.0.0) | Delivered SHA-256 (v1.1.0) |
|---|---|---|
| `scripts/audit_full_refresh_coverage.py` | `2b24d3d5e0cc4989337cc9532aeec24e5fa2b145aa3327da7fc15071c3f8e248` (217 ln) | see terminal line above (247 ln) |
| `scripts/audit_decision_surface_freshness.py` | `bb6e073e6846eb6f2ee8c2968dbcadab187a2593b0f7ba789fdf0f6595fbf646` (356 ln) | see terminal line above (472 ln) |

## Why (one line)
Shared `parse_dt` emitted three inconsistent time bases (+03:00→naive-UTC = +3h inflation, reproduced on the live 07:07:28+03:00 stamp; Z-strip = −3h; naive = Riyadh), and date-only "9/8/2026" fabricated a midnight age → the PF_SOURCE_STALE false positive in run 34189823244.

## Edits
**Coverage (parse_dt owner, v1.1.0):** CE1 version; CE2 WHY; CE3 new PURE `parse_dt_precision(v) → (naive-Riyadh dt|None, "datetime"|"date"|"none")` — aware→Riyadh, Z→+00:00 (never stripped), integer serials & date formats = "date"; `parse_dt` becomes a thin wrapper (same signature, uniform basis); CE4 `audit_grid` now0 unconditionally Riyadh-naive.
**Freshness (v1.1.0):** signed `_age_hours` (no silent future-clamp); `FUTURE_SKEW_H=0.25`; StatusRow gains `updated_precision`; date-only stamps → `*_TIME_PRECISION` FAIL (fail-closed, truthful) instead of fabricated-age STALE, and are excluded from run-vs-source ordering; explicit `PF_RUN_FUTURE / PF_SOURCE_FUTURE / T10_RUN_FUTURE / SOURCE_FUTURE`; `--selftest` pins goldens T05/T06/future. **Floors, universe contract, exit semantics: byte-untouched.**

## Audit proofs
py_compile PASS ×2 · AST zero-removal: coverage 25→26 defs (added `parse_dt_precision`), freshness 19→21 (added `_selftest`, nested `_grids`), removed NONE · smart-quote scan clean · real-module `--selftest` from repo layout **6/6 PASS ×3 byte-identical**, including the reproduced case (07:07:28+03:00 @ 08:14:20 Riyadh → **1.1144h**, was 4.1144h).

## Deploy (operator — commit BOTH, coverage FIRST; freshness imports it)
1. https://github.com/emadsaberbahbah-coder/tadawul-fast/edit/main/scripts/audit_full_refresh_coverage.py → paste delivered file → commit `audit clock fix v1.1.0 — parse_dt Riyadh-uniform (P-104)`.
2. https://github.com/emadsaberbahbah-coder/tadawul-fast/edit/main/scripts/audit_decision_surface_freshness.py → paste delivered file → commit `freshness audit v1.1.0 — precision + future truth (P-104)`.
3. Optional evidence: this sheet → `docs/evidence/TFB_Commit_Sheet_audit_clock_fix_v1_1_0_2026-09-08.md`.

## Read-back criteria (next scheduled decision_surface_freshness run)
- `PF_SOURCE_STALE` **gone**; replaced by `PF_SOURCE_TIME_PRECISION` (the truthful code — My_Portfolio's _Status stamp is date-only; upstream cure = GAS status writer stamping a full timestamp, queued with P-103/mirror work).
- No +3h inflation on any +03:00 stamp; job **still exits 2** on the 4 floor findings + FALSE_FULL_UNIVERSE_CLAIM — that is the pending operator decision (a: stay red until W2 · b: set `TFB_EXPECTED_MIN_ROWS_MARKET_LEADERS=255` / `TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS=2474` in `decision_surface_freshness.yml` env as the ratified current universe, raised at W2).

## Rollback
Revert either/both files to base SHAs above. No state, no ENV, no schema, no production writer touched.
