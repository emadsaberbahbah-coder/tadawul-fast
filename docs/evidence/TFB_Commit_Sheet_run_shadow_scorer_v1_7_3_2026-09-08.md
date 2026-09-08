# TFB Commit Sheet — run_shadow_scorer.py v1.7.3
**Date:** 2026-09-08 · **Register:** P-109 (provisional) · **Type:** log-only observability · **ENV:** none · **Behavior change:** none

## Pin
| | SHA-256 |
|---|---|
| Base (main @ live-fetch 2026-09-08) | `fcaaa710feea1ee522b393ff4ec9de3c02fed3a606ba2bc4db8c488c4c20f310` (1,887 lines, v1.7.2; byte-identical to 06:20Z tarball — no drift) |
| Delivered | `ffa396a67ea902c9f4d6e62734ce8d2896adb655a8045ff48b90f9f88cd0b7d9` (1,959 lines, v1.7.3) |

## Why (one line)
Every trading day since 2026-09-02 ends `excluded_reason=fresh-floor` with only `stale=N` printed; stale NAMES were collected since v1.2.0 but never shown, and MISSING pairs (seat absent/zero in prev, or absent in spot) depress fresh coverage invisibly — with 9 seats and the 60% floor, 1 stale alone cannot exclude a day. v1.7.3 names the starvers.

## Edit register (anchored, count==1 asserted)
- **E1** SCRIPT_VERSION 1.7.2 → 1.7.3
- **E2** WHY v1.7.3 block (newest-first), zero removals below
- **E3** new PURE helper `_freshness_detail()` — mirrors `basket_return_fresh`'s exact pairing skip precondition; returns (line, JSON details); lists capped 6 (line) / 12 (JSON)
- **E4** verdict gains ` | [S1-FRESH v1.7.3] chal fresh=a/b floor=P% stale=[..] nopair=[..] new=[..]` (after excluded_reason, before the informational eqw tail)
- **E5a** same segment appended to the S1_Gate meta cell after the shape-guard segment
- **E5b** `freshness` object added to the _Run_Log details JSON
- **E6** two selftest checks for the helper

**Untouched by design:** counting, basket math, exclusion decisions, per-day Shadow_History note formats (fixture-pinned), criteria, all v≤1.7.2 WHY blocks.

## Audit proofs
- `py_compile`: PASS
- AST: 54 → 56 defs, **removed NONE**, added `_freshness_detail` (+nested `_fmt`)
- Smart-quote scan: NONE
- Real-module harness ×3 from repo layout: `--selftest` **89/89 PASS**, byte-identical output ×3 (87 base + 2 new)
- Direct real-function harness ×3 (`basket_return_fresh` + `_freshness_detail` composed): identical → `[S1-FRESH v1.7.3] chal fresh=1/5 floor=60% stale=[BBB] nopair=[CCC,DDD] new=[EEE]`

## Deploy (operator)
1. Replace file on main: https://github.com/emadsaberbahbah-coder/tadawul-fast/edit/main/scripts/run_shadow_scorer.py — paste delivered file, commit `shadow_scorer v1.7.3 — S1 freshness read-back (P-109)`.
2. **S6 observe cycle = tonight's scheduled run** (no manual dispatch needed): https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/workflows/shadow_scorer.yml

## Read-back criteria (tomorrow morning)
- Run_Log verdict and S1_Gate meta both carry `[S1-FRESH v1.7.3] …` with named `stale`/`nopair`/`new`.
- Counters, alpha figures, exclusion classification unchanged in form.
- The names answer: which symbols starve Criterion 1 → root-cause build follows with facts.

## Rollback
Revert `scripts/run_shadow_scorer.py` to base SHA `fcaaa710…` (v1.7.2). No state, no ENV, no schema touched.
