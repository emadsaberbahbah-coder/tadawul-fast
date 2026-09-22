# TFB Commit Sheet — core/data_engine_v2.py v5.147.0 [F-7 SCORING SETTLE PASS]

Date: 2026-09-22 (Riyadh) · Register item: **F-7 pass-dependent scoring, Option A (settle pass)** — operator's choice 2026-09-22 ("move with the best option") · Engine lane: **supersede** — built on the live v5.145.0; v5.146.0 is a burned number (uncommitted P-102 build, must never be committed); P-102 re-issues as v5.148.0.

## Base pin (live)

| | |
|---|---|
| Repo tree | `emadsaberbahbah-coder/tadawul-fast` at HEAD `b191320be87ed53207114dae003f4fcc4546629f` (tarball, 2026-09-22 ~12:15 Riyadh) |
| Base `core/data_engine_v2.py` | v5.145.0 · SHA-256 `e2de0cb46770a3acdc38cf1ce4e9587531ccf4bb8d484c7da0a1e4c6531d1557` · 17,928 lines · zero drift vs the 2026-09-20 delivery |
| Runtime in sandbox | Python 3.12.3 (production 3.11.9); `core.scoring` 5.11.2 imported as canonical scorer |

## Mechanism (pinned on source)

The orchestrator (`_get_enriched_quote_impl`, ~L16860) runs `_compute_scores_canonical_first(merged)` and only then `_apply_phase_dd_enhancements(merged)`. Pass 1 is therefore scored without `intrinsic_value`/`upside_pct` (valuation reads them) and, for synthetic rows and rows whose analyst target is restored by the R-6 keep-last-good store **inside** `_phase_ii_quality_forecast`, without any forecast at scoring time. `core.scoring` labels such rows `both_present_fallback` (0.65×valuation + 0.35×momentum) although the published row carries a forecast, and the gate's −15 "fallback" reliability leg fires on a label that describes pass order, not data. Sheet evidence 2026-09-21/22: 3,705 → 3,970 GM rows both_present while publishing ROI12; −15 leg on 3,214 rows; of 152 INVESTABLE GM rows only the 48 roi_based clear the 70 floor; every non-KRP seat at rel 54.3.

Re-run safety proven on source before the seam was placed: all tag writers dedupe (`_aq_append_warning`, `_v573_append_warning`); `_tgt_lkg_capture` refuses carry-tagged rows; `_compute_intrinsic_and_upside` is fill-only (no intrinsic↔synthetic-forecast feedback loop); the 8-tier classifier is idempotent (v5.77.16/17); `_apply_phase_dd_enhancements` and the canonical scorer are pure over the row (no network).

## Change

Gate `TFB_SCORING_SETTLE` = off (default) | observe | enforce — explicit words only ("1"/"true"/"on" read as off); `TFB_SCORING_SETTLE_MAX_PASSES` default 4, clamped [2, 5]. Read at call time.

- **off** → `_f7_settle_pass` returns the row untouched — byte-identical to v5.145.0 (J1).
- **observe** → values untouched; ONE countable substring-safe tag on rows whose pass-2 output would differ: `f7_settle:observe:st<k|x>:p<n>:<code>=<before>><after>…` (st = pass at which the row stopped changing, x = still moving at the cap; codes ov op va fcf r12 rc os fs; source tokens rewritten bpf/mof/rb/ins/pt/sy; the composer degrades to `:chg<n>` if a disclosure ever carries a gate-forbidden substring).
- **enforce** → the settled row (last pass) replaces the pass-1 row and carries the tag; rows already stable after pass 2 are returned untouched.
- Fail-open: any exception returns the original row. Mode in the `[GUARDS]` boot line (`scoring_settle=`) and `/health engine_gates.scoring_settle`.
- Untouched by design: WHICH horizon is scored (F-1), the reliability arithmetic (P-115b), the classifier, every existing tag string.

Seven anchored edits, each `count == 1`: E1 version constant · E2 WHY v5.147.0 block (newest-first, before WHY v5.145.0) · E3 helper block after `_apply_phase_dd_enhancements` · E4 seam `merged = _f7_settle_pass(merged, sym, page_ctx)` directly after the pair · E5 `engine_gates["scoring_settle"]` · E6/E7 GUARDS boot line format + arg.

| | |
|---|---|
| Delivered `core/data_engine_v2.py` | v5.147.0 · SHA-256 `017a20d7c98452747c900e7393507f30854999f35eeb9cd95c2359ee1ed53c05` · 18,153 lines (+225 / −0) |
| AST | 493 → 500 names: +7 (`_f7_settle_mode`, `_f7_settle_max_passes`, `_f7_settle_token`, `_f7_settle_fmt`, `_f7_settle_diff`, `_f7_settle_tag`, `_f7_settle_pass`), **0 removed** |
| `py_compile` | PASS · smart quotes 0/0 · non-ASCII 323 → 323 (additions ASCII-only) |

## Evidence

**Dual-tree real-module harness** (pristine v5.145.0 tree vs delivered tree, separate processes; `ds/h_build_inputs.py`, `h_run.py`, `h_check.py`) on **900 real Global_Markets rows of the 2026-09-22 export**, mapped to engine keys with derived fields stripped: 300 early-target (provider target present at scoring), 300 late-target (target restored from the seeded R-6 store inside Phase-II), 300 synthetic. Orchestrator prep identical in every run (`_apply_phase_bb_sanity` → pass 1). Production armings mirrored: `TFB_ENGINE_TARGET_KLG=1`, `TFB_EQ_ROI_UNIT_SENTRY=observe`.

| Check | Result (×3 identical, digest `2fed6af067c959b2`) |
|---|---|
| J1 off ≡ base pass 1 | 900/900 rows deep-equal (only the call-time `scoring_updated_*` stamps differ) |
| J2 observe values untouched; ≤1 tag; regex + forbidden-substring scan | 900/900 untouched · 900 tagged · 0 bad tags |
| J2b tagged set == rows the independent golden loop changes | equal (900) |
| J3 enforce == independent settle loop (same fields/tolerances) on all keys; untagged rows ≡ pass 1 | 900/900 |
| J3b observe/enforce carry the same tag body | equal |
| J4 cap 5 vs cap 3: values untouched, tagged set identical | equal |

Stability: every row moves on pass 2 (early rows because valuation only sees intrinsic/upside after pass 1). Early- and late-target rows settle at pass 3 (300/300 each); synthetic rows settle at pass 4 (265/300; the other 35 at 3) — residual pass 3→5 movement ≤ 0.01 overall points, 0 recommendation changes, hence default cap 4. Decision effect of settling (golden, cap 3): `opportunity_source` 569 both_present + 31 momentum_only → **900 roi_based**; recommendation changes 229/900; overall moves ≥5 pts on 322/900; rows with overall ≥ 68 shrink 51 → 22 (37 down, 8 up across the Conservative gate) — the cold pass inflates. Specimens: late 0014.HK overall 48.49 → 59.04, SELL → REDUCE, `os=bpf>rb`; synth 000270.KS 55.17 → 41.23 (valuation 100 → 58.5). Cost: +1.4 ms/row observe (900 rows: 0.6 s pass-1 vs 2.0 s settled) ≈ +10 s per full GM rebuild.

**Repo battery** `tests/test_de_f7_scoring_settle.py` (SHA-256 `3d0663b2efa511a54ebf9e3639cfdce77d98525d302cfd0f390b34658576e94c`, 579 lines; six embedded real fixtures, two per cohort): T1 gate words · T2 clamp · T3 off byte-identical · T4 observe untouched + safe tag · T5 enforce == independent golden · T5b late/synthetic rows lose the fallback label (both_present → roi_based) · T6 fail-open (scorer raises → original row) · T7 composer degradation · T8 wiring + version floor — **9/9 ×3** (pytest and standalone).

**Existing engine batteries on the delivered tree:** 45/45 ×3 after two exact version pins were loosened to floors (the 2026-09-20 loosening never landed): `tests/test_de_crypto_pair_shape_p151.py` (`== "5.145.0"` → `>= (5,145,0)`; SHA `4c437d7cd79be50bea9f4241cf3bf5604b3e10e825f1002448100ae5ca079818`) and `tests/test_de_fund_sentry_repair_p146.py` (t9 was **already red at HEAD** on `== "5.143.0"`; now floor `>= (5,143,0)` and the boot-line literal check made leg-tolerant; SHA `035ceffbc9b9184bd3725fa0e76d9264bdcfaf237563c5815daaa4466d6a3c65`). CI's four (`test_scoring_engine_contract`, `test_schema_alignment`, `test_recent_fixes`, `test_inline_page_recovery`): 70 passed / 5 failed / 2 skipped on BOTH trees — the five are the endpoint-reaching schema-alignment tests (sandbox has no app), identical on base and delivered, not attributable to the build.

## Deploy

1. Commit five files in ONE push: `core/data_engine_v2.py`, `tests/test_de_f7_scoring_settle.py`, `tests/test_de_crypto_pair_shape_p151.py`, `tests/test_de_fund_sentry_repair_p146.py`, `docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.147.0_2026-09-22.md`.
2. One Render Manual Deploy. Deploy proof: boot log `[engine_v2 v5.147.0] module loaded` + `[GUARDS] … scoring_settle=off`; `/health` engine_version 5.147.0, `engine_gates.scoring_settle = "off"`; startup_warnings []. Gate off ⇒ behaviour identical to v5.145.0.

## Arming (Render lane, one per evidence run)

`TFB_SCORING_SETTLE=observe` (may ride the same Manual Deploy: tag namespace `f7_settle:` is disjoint from `rel_path:`/`crypto_pair_shape:`/`fund_*`/`eq_roi_backfill:`). Read-back on the next full-sync export: `f7_settle:observe` on essentially every scored equity row, ~60% carrying `os=bpf>rb`, `st3` on target rows / `st4` on synthetic rows, **zero cell values changed**; the count of `rc=` disclosures is the enforce blast radius.

**Enforce is a separate sitting** — it changes published scores/recommendations on roughly a quarter of rows and INVESTABLE membership (harness: overall ≥ 68 shrinks 51 → 22) — and requires the S-1 window boundary note.

## Rollback

env unset (no deploy) or `git revert`.

## Next in lane

P-102 re-issue as v5.148.0 on this base (FC-TUPLE COHERENCE, `TFB_FC_TUPLE_COHERENT`, same design as the 2026-09-20 commit sheet).
