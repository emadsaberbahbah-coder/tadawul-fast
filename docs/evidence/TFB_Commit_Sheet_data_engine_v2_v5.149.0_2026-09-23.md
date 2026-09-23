# TFB Commit Sheet — core/data_engine_v2.py v5.149.0 [P-164 52W PROVIDER-CEILING SCRUB]

Date: 2026-09-23 (Riyadh) · Register: P-164 (raised in the 2026-09-23 morning audit; root pinned at source this session) · Lane: Render backend (one Manual Deploy; NEW env `TFB_ENGINE_52W_CEILING_SCRUB` is a KILL-SWITCH only — nothing to set) · Protocol: One-Pass

## 1. Base pinned (S1)

| Item | Value |
|---|---|
| Base | `core/data_engine_v2.py` v5.148.0 at `main`, sha `e6df3ef9181d4308…` (18,258 lines, 511 AST names) — the 09-22 delivery, zero drift |
| Provider evidence | `core/providers/eodhd_provider.py` at `main`: v4.13.0 AS-1 sentinel scrub (`_EODHD_SENTINEL_LOW/HIGH` = 999999.0 / 1000000.0, `_scrub_provider_sentinels`) runs on the quote patch (L2520) and merged quote (L3218) only; the fundamentals path reads `Technicals.52WeekHigh/52WeekLow` at L2672–2673 unscrubbed |
| Engine evidence | `_compute_history_patch_from_rows` L16335 `week_52_high = max(highs_52)` over the 252-bar window; `_apply_phase_bb_sanity` → `_sanitize_week_52_position_pct` (writes only when both bounds exist, never clears) at L17163; `_sanitize_corrupt_52w_bounds` is the single 52W sanitizer, called from `_apply_v572_sanitization`, hoisted into `_compute_scores_canonical_first` since v5.114.0 (L10062) → runs after every 52W writer and before scoring on both paths (and again, idempotently, inside the F-7 settle pass) |

## 2. Delivered (S5)

| File | Repo path | SHA-256 | Size |
|---|---|---|---|
| `data_engine_v2.py` | `core/data_engine_v2.py` | `b425b479172f5bf5…` (full value in the delivery listing) | 18,339 lines (+81 / −0), AST 511 → 513 (+`_w52_ceiling_scrub_mode`, `_w52_is_ceiling`; 0 removed) |
| `test_de_w52_ceiling_scrub_p164.py` | `tests/` | see delivery listing | 13 tests (T1–T7, T2/T3 parametrised) |
| this sheet | `docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.149.0_2026-09-23.md` | — | — |

Five anchored edits, each asserted count==1: WHY block + `__version__` 5.148.0 → 5.149.0; two constants + two helpers + the ceiling branch at the top of `_sanitize_corrupt_52w_bounds`; `surface_gate_states()` gains `w52_ceiling_scrub`; the `[GUARDS]` boot line gains `w52_ceiling=%s`. `py_compile` PASS; non-ASCII count unchanged (323/323); no smart quotes; additions ASCII-only. All prior WHY blocks and tags carried verbatim.

## 3. Root (measured)

2026-09-23 Global_Markets export: three rows publish `week_52_high` = 999999.9999 (rendered "1,000,000.00") — 012450.KS (Hanwha Aerospace, price 1,021,000 KRW, **INVESTABLE**, 52W Position 100.00%), 009150.KS (1,507,000, position 100.00%), YPFD.BA (8,530 ARS, position 0.09%); the sync's OHLC-PREWRITE lists them as w52_band anomalies on every leg (observe mode → written anyway). None carries `eodhd_sentinel_dropped:*`, i.e. the provider's AS-1 scrub never saw the value: it arrived through the history max over capped EOD bars and/or the fundamentals Technicals block, then the phase-BB pass derived a 100% position from the fabricated bound. Genuine 7-digit KRW values (000660.KS 2,987,000 / 207940.KS 1,611,000 / 010130.KS 2,188,000) are correct and outside the band — the morning audit's "6 rows" count included them by a `≥ 999999` filter error, corrected here to **3 rows**.

## 4. Change

`_sanitize_corrupt_52w_bounds()` now, FIRST (before the nonpositive / inverted / scale-mismatch rules), when `TFB_ENGINE_52W_CEILING_SCRUB` ≠ 0/false/off/no (default ON): a `week_52_high` or `week_52_low` inside [999999.0, 1000000.0) → `None` + `sanitized:week_52_high|low_provider_ceiling`; if a bound was dropped and `week_52_position_pct` is present → `None` + `sanitized:week_52_position_pct_unbounded`. Drop, never fabricate (no repair from day_high/price — the AS-1 rule). Tags are substring-safe for the investability gate (checked against cap / forecast / target / roi / drop / reject / provider_target / price_bar_stale / xprovider_price_conflict). Mode disclosed in `[GUARDS] … w52_ceiling=on|off` and `/health engine_gates.w52_ceiling_scrub`.

**Default ON — stated deviation from the default-OFF doctrine**, on the same-class precedent eodhd_provider v4.13.0 AS-1 ("a literal ceiling constant is the least ambiguous member of the price-sanity class"; default ON with kill-switch): the OFF state IS the defect. `TFB_ENGINE_52W_CEILING_SCRUB=0` restores v5.148.0 byte-identically at call time (no restart). Operator veto available.

## 5. Evidence (S4)

Battery `tests/test_de_w52_ceiling_scrub_p164.py`, dual-tree real-module (`core.data_engine_v2` in repo-shaped trees with `core.scoring` present), separate processes:

| Tree | Result |
|---|---|
| delivered v5.149.0 | **13 passed ×3** (T7 on the real 6,609-row export) |
| base v5.148.0 | **5 failed / 8 passed — golden negative** (T1 ceiling left in place, T4, T5, T6 no disclosure, T7 zero positives) |
| CI-shaped (no fixture) | 12 passed, 1 skipped |

JSON digest ×3 per tree: base `de771481bfaaf24a` identical ×3, delivered `95e8860bd0072102` identical ×3. Kill-switch scenarios (`mode_False`, `replay_False`) **byte-identical** base vs delivered; genuine / exact-million / inverted / scale-mismatch fixtures identical in both modes and trees; the three ceiling fixtures differ only as designed (base nulled=0, bound and 100% position kept → delivered nulled=2, bound None, position None, tags present). Real-page replay: **0 false positives over 6,609 rows** in both trees; the three rows reconstructed at their backend value 999999.9999 all fire on v5.149.0 and none on v5.148.0.

Existing engine batteries on the delivered tree: crypto_pair_shape 7/7, f7_scoring_settle 9/9, fc_tuple_coherence 7/7, fund_sentry_repair 9/9, rel_path_tag 8/8, fund_unit_sentry 12/12 (52/52; the two script-style harnesses collect 0 tests under pytest, same as on base). Harness note kept: `test_de_f7_scoring_settle.py` needs `core/scoring.py` in the tree — without it two of its tests fail on BOTH trees (environmental, not a regression).

## 6. Deploy + read-back

1. Commit the three files (+ the re-attached v1.12.2 commit sheet) in ONE push; per-file SHA check on "done".
2. ONE Render Manual Deploy. Boot proof: `[engine_v2 v5.149.0] module loaded`, `[GUARDS] … fc_tuple=off w52_ceiling=on`, `portfolio_actions v1.12.2` bound (this deploy also carries Build #2 if it was not deployed yet); `/health` engine 5.149.0, `engine_gates.w52_ceiling_scrub = "on"`.
3. Read-back on the next full-sync export: 012450.KS / 009150.KS / YPFD.BA `52W High` blank with `sanitized:week_52_high_provider_ceiling`; `52W Position %` blank on 012450.KS / 009150.KS with `sanitized:week_52_position_pct_unbounded`; the OHLC-PREWRITE w52ex line no longer lists them; count of `week_52_high_provider_ceiling` tags on the page = 3 (± any KRW/ARS name newly crossing 1,000,000). Rows the run did not rebuild keep the old value until their next rebuild (preserved-row semantics — YPFD.BA carries a 00:43 UTC stamp).

Rollback: `git revert`, or `TFB_ENGINE_52W_CEILING_SCRUB=0` on Render.

## 7. Deliberate cuts

- No repair of a dropped bound from day_high / price / Yahoo (drop-not-fabricate, AS-1 rule); a later build may re-derive the 52W band from an uncapped source.
- The pre-existing inverted / scale-mismatch branches still do NOT clear an already-derived `week_52_position_pct` — same latent gap, out of scope (vNEXT note).
- The provider's fundamentals path (L2672) is left as-is: the engine seam covers every writer; a provider-side scrub of the Technicals block is optional hygiene.
- `previous_close` / day-range ceilings stay with the provider scrub (already covered by AS-1 on the quote path).
