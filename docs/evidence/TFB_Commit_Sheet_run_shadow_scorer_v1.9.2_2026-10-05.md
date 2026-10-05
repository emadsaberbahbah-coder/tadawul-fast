# TFB Commit Sheet — scripts/run_shadow_scorer.py v1.9.2 [S-1 CRITERIA V2: P-204 dual window · P-201 baseline-relative calibration · C5 NOT_EVALUABLE on an empty register · P-186 set digest] + tests/test_s1_criteria_v2_p204.py (+ fixture) + three harness re-pins

Date: 2026-10-05 (Monday) · Lane: GitHub / Python (S-1 evidence lane) · Build #2 of the day (B2) · Protocol: One-Pass · Gate: `TFB_S1_CRITERIA_V2` = **off (DEFAULT, byte-identical)** | observe | enforce

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `scripts/run_shadow_scorer.py` **v1.9.1** at HEAD `2ad0598` (2026-10-05 13:43 Riyadh), sha256 `628d339c5b6b6bce13166d5b10a98506c557cf1b41aa45d5cc5da460f20bc253`, 2,476 lines, 60 top-level defs, selftest **104/104** |
| Delivered | `scripts/run_shadow_scorer.py` **v1.9.2**, sha256 `411a20a9f9bd629c9685d75e066ae1f0feae6750e165a9ea9e15e8076a698565`, 2,854 lines, 69 defs (**+9, 0 removed**: `_criteria_v2_mode`, `_window_start`, `window_cum`, `parse_zero_mae`, `parse_model_mae`, `read_s1_calibration_mae`, `ca_register_rows`, `evaluate_s1_v2`, `criteria_v2_line`), `py_compile` PASS, 0 smart quotes, selftest **116/116** (+12) |
| Harness | `tests/test_s1_criteria_v2_p204.py` (sha `9b2fae8c…`, 277 lines) + fixture `tests/fixtures/shadow_history_2026-10-05.json` (sha `83665f2c…`: the REAL 2026-10-05 `Shadow_History` tab — 284 rows, prices JSON blanked) |
| Re-pins (3 files, version / count tokens only) | `tests/test_s1_base_policy_p186.py` (`7f17b334…`), `tests/test_s1_board_fresh_guard.py` (`6bfbbcc8…`), `tests/test_s1_day_key_p176.py` (`33e779e5…`): `"1.9.1"` → `"1.9.2"`, `104/104` → `116/116`, `[… v1.9.1]` → `[… v1.9.2]` — no assertion logic changed |

## S2 — Root causes (Audit Reconciliation 2026-10-05, re-executed on the export)
1. **P-204** — criterion 3 ("net alpha ≥ 0") is evaluated on the cumulative index since the 2026-07-19 seed. Rebased at the last index before the 2026-09-16 engine-ROI repair (P-139 boundary note of 09-15): challenger **−0.2532 %**, champion −0.2519 %, benchmark **+2.5343 %** → challenger excess **−2.79 pp** over the 11 scored dates since 09-16, while the cumulative line reads +5.34 %. The PASS is carried by the pre-repair history (07-21: +3.25 % on n = 2 with 3 stale names).
2. **P-201** — criterion 4 ("calibration in band 10 pp") passes trivially: on the same matured 1W+2W cohort MAE(model) 3.23 pp > MAE(zero forecast) 3.07 pp (1W 2.65 vs 2.55, 2W 3.89 vs 3.65). The band accepts forecasts that lose to predicting zero.
3. **C5** — criterion 5 reads PASS on an **empty** `_Corporate_Actions` register (header only since creation): a vacuous pass (`ca_is_clean` returns True on an empty plan).
4. **P-186** — the verdict does not name the scored challenger set, so a re-run of the same day cannot be compared.

## S3 — Change (8 anchored edits, each `count == 1`)
| # | Site | Edit |
|---|---|---|
| E1 | header | v1.9.2 WHY/WHAT block; `SCRIPT_VERSION = "1.9.2"` |
| E2 | before `evaluate_s1` | 9 new functions: mode reader (`TFB_S1_CRITERIA_V2`), window start (`TFB_S1_WINDOW_START`, default **2026-09-16**), PURE `window_cum` (return since the LAST index dated before the start — the audit's rebasing), PURE `parse_zero_mae` / `parse_model_mae` (a `Zero MAE (pp)` column or a `zero_mae=<x>pp` token in the `_S1_Calibration` Detail; absent ⇒ None, never guessed), `read_s1_calibration_mae(sh)`, `ca_register_rows(sh)` (None on read error — never a silent zero), PURE `evaluate_s1_v2` (deep copy; observe annotates, enforce re-derives), `criteria_v2_line` |
| E3 | `main` after `gate = evaluate_s1(...)` | when the mode is not off: window cums for the three baskets, window alpha, calibration MAEs, CA rows, set digest (sha256 of the sorted challenger symbols, 8 hex) → `evaluate_s1_v2` → `[S1-CRITERIA-V2 v1.9.2] …` printed |
| E4 / E5 / E6 | verdict line · S1_Gate meta cell · `_Run_Log` details JSON (`criteria_v2`) | the token line, **armed only** |
| E7 | `_selftest` | +12 checks (V2: audit numbers reproduced, None paths, start parsing, MAE parsing, off = deep-equal, observe annotations, enforce FAIL / PASS / PENDING / NOT_EVALUABLE paths, read-back line, mode reader) |
| E8 | imports | `copy`, `hashlib` |

**Semantics.** off (default) → every surface byte-identical to v1.9.1 (proven, C3). observe → statuses and verdict unchanged; criteria 3/4/5 details gain ` | v2: … -> would PASS/FAIL/PENDING/NOT_EVALUABLE`. enforce → criterion 3 PASS needs **both** windows ≥ 0 (window unknown ⇒ PENDING); criterion 4 PASS needs in-band **and** MAE(model) < MAE(zero) (zero MAE unpublished ⇒ PENDING — fail-safe, the v1.4.0 Wave-B doctrine); criterion 5 ⇒ NOT_EVALUABLE on an empty register (counts as pending); §15 verdict rule re-applied (any FAIL → FAIL; all PASS → PASS; else NOT_DECIDABLE), `why` suffixed `[criteria v2 enforce]`. Shadow_History rows, basket math, counters, fresh floor, day key, base policy: **untouched** (proven, C4/C5).

**Producer contract for P-201 (B5, `track_performance.py` v6.42.0):** publish the zero-forecast baseline as a `Zero MAE (pp)` column in `_S1_Calibration` row 2 (preferred) or a `zero_mae=<x.xx>pp` token in its Detail cell. Until it lands, the scorer prints `zero_mae=n/a` and criterion 4 would read PENDING under enforce — honest, not a defect.

**Boundary note (Register §5).** Flipping to enforce is an S-1 evidence-lane methodology change: it needs its own written boundary note naming the window that authorises Tranche 1. Arming plan: `TFB_S1_CRITERIA_V2: "observe"` in `.github/workflows/shadow_scorer.yml` (one evidence run) → read-back on the next `S1_Gate` export (` | v2:` details + the token line) → enforce at a Saturday sitting with the note.

## S4 — Audits (REAL module, dual-tree, ×3 identical)
| Battery | Result |
|---|---|
| Embedded selftest | base **104/104**; delivered **116/116 ×3** |
| `tests/test_s1_criteria_v2_p204.py` C1–C7 (with `S1_BASE` = v1.9.1) | **7/7 PASS ×3, RUN-DIGEST `3d31eec4d39cdd20`** — C2 the REAL `read_history` + `window_cum` on the REAL 10-05 tab: chal −0.2532 / champ −0.2519 / bench +2.5343 ⇒ **alpha −2.7875 pp** (cumulative since seed +5.34 pp) — the audit numbers exact · **C3 dual-tree parity under mode off: REAL `main()` on the in-memory sheet stub — Shadow_History rows, S1_Gate body and the `_Run_Log` verdict byte-identical base ↔ delivered** (version token aside) · C4 observe: statuses/verdict unchanged, 3/4/5 annotated, token on verdict + meta + JSON, history untouched · C5 enforce on live-shaped evidence (negative window, zero MAE unpublished, empty register): 3 FAIL / 4 PENDING / 5 NOT_EVALUABLE ⇒ FAIL, `_Run_Log` GATE_FAIL · C6 enforce with a `Zero MAE (pp)` column (3.50 vs 3.13), a CA row and a positive window: 3/4/5 PASS, verdict NOT_DECIDABLE on criterion 1 only · C7 `--dry-run` zero-write (without `S1_BASE`, CI shape: digest `7574246ea9e5033c`, C3 SKIP) |
| Existing end-to-end batteries on the delivered tree (re-pinned) | `test_s1_base_policy_p186.py` **7/7** (digest `eba9d1cd1eafdbbd`) · `test_s1_day_key_p176.py` D1–D7 PASS (`ab2535053f053982`) · `test_s1_board_fresh_guard.py` K1–K5 PASS (`26a4339fccd206be`) · `test_shadow_scorer_shape_guard.py` PASS |
| Static | `py_compile` PASS · +9 / 0 defs · 0 smart quotes |

## S5 — Delivery (8 files)
`scripts/run_shadow_scorer.py` · `tests/test_s1_criteria_v2_p204.py` · `tests/fixtures/shadow_history_2026-10-05.json` · `tests/test_s1_base_policy_p186.py` · `tests/test_s1_board_fresh_guard.py` · `tests/test_s1_day_key_p176.py` (re-pins) · `docs/evidence/TFB_Commit_Sheet_run_shadow_scorer_v1.9.2_2026-10-05.md` (this sheet)

## S6 — Operator steps (one action each)
1. Commit the 8 files (your commit): https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/scripts (the .py), https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/tests (the four .py), https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/tests/fixtures (the .json), https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/docs/evidence (this sheet). Commit message: `run_shadow_scorer v1.9.2 [S-1 criteria v2: P-204 dual window, P-201 baseline-relative, C5 NOT_EVALUABLE, P-186 set digest] + harness + fixture + re-pins`. **Note:** a push to `main` restarts Render (see Monitoring Sheet #8 addendum) — batch this commit with B3's files in one sitting.
2. Tonight's 18:40 scorer runs **off** = v1.9.1-identical. Arming (GitHub lane, one evidence run, your hand edit): add `TFB_S1_CRITERIA_V2: "observe"` under the scorer job env in https://github.com/emadsaberbahbah-coder/tadawul-fast/edit/main/.github/workflows/shadow_scorer.yml (beside `TFB_S1_BOARD_FRESH_GUARD: "observe"`, L58). **Read-back** = tomorrow's `S1_Gate`: criteria 3/4/5 details carry ` | v2: …` and the meta cell carries `[S1-CRITERIA-V2 v1.9.2] mode=observe window_start=2026-09-16 … alpha=−2.xx% zero_mae=n/a … ca_rows=0 set=<8hex>`; verdict unchanged (NOT_DECIDABLE).
3. Enforce = Saturday 10-10 sitting only, with the boundary note (the text is in Audit Reconciliation 2026-10-05 §2 N1).

## Known limits / deliberate cuts
- The window start is a constant default (2026-09-16) overridable by env; the scorer does not parse boundary notes from `_Run_Log` (a later item).
- Criterion 4 enforce depends on the tracker publishing the zero baseline (B5); until then enforce reads PENDING on criterion 4 by design.
- The set digest names the challenger set, not the champion's; it is the reproducibility key for P-186, not a fix for the F-7 pass-dependence (engine-side).
