# TFB Commit Sheet - core/data_engine_v2.py v5.153.0 [WP-2b: settle_unresolved publication rule, gate TFB_SETTLE_UNRESOLVED] + tests/test_de_f7_settle_unresolved.py + scripts/harness_wp2bc.py (shared)

Date: 2026-10-11 (Sunday) - Lane: Render / Python (engine; backend ENV, read at call time) - Build: WP-2bc, framework section 4 WP-2 item (b) - Protocol: One-Pass - File status: engine **REVISED** 5.152.0 -> 5.153.0 - test **NEW** - harness **NEW** (shared with the run_dashboard_sync 6.66.0, analysis_sheet_rows 4.8.2 and opportunity_builder 1.25.1 sheets) - this sheet **NEW**

## S0/S1 - Base pin
| Item | Value |
|---|---|
| Base commit | `18a84168aac1d462dd58494239ac91f8cef7178c` ("Record audit-framework wave 1 ..."; wave 1 landed: WP-3ab engine 5.152.0, WP-6 sync 6.65.0, WP-0 DQ harness). The build worktree was reset to this exact commit before any file was read. |
| Base file | `core/data_engine_v2.py` sha256 `1962c4b068c6da58d1401aadd606d86a400d7bd0505076625470ae7647c61fc9`, **20,042 lines**, `__version__ = "5.152.0"`, title v5.152.0 (no drift), AST walk-all **555 defs / 10 classes**; module level 467 defs+classes / 88 one-level nested / 305 top-level names |
| Pristine copy | `git show 18a8416:core/data_engine_v2.py` materialised at `<scratchpad>/base_wp2bc/core/data_engine_v2.py` (sha256 identical) and inside the base tree the harness L5 leg imports |
| Delivered engine | sha256 `9c50b9d72fed8c995c29285a84f4db8f85e60c862ff9dd90219527667dbe0c05`, **20,309 lines** (+267; 4 lines edited in place: title, `__version__`, the `_portfolio_decision` guard, the GUARDS literal), `__version__ = "5.153.0"`, AST walk-all **563 / 10** (+8 / 0), module level 475 / 88 / 311 (+8 defs, +6 constants), **0 removals**; `py_compile` PASS (venv311 and venv_full, Python 3.11); ruff E9/F63/F7/F82 PASS; 0 smart quotes; non-ASCII 323 = base |
| Delivered test | `tests/test_de_f7_settle_unresolved.py` sha256 `5a677d40cd019ae3bed483557cda2063f92e64bd4205b20be4841b95c91e10f6`, 410 lines, 10 pytest-collectable `test_u*` + `__main__` runner (pattern `tests/test_de_f7_scoring_settle.py`, whose FIXTURES / `_prep` / `_norm` / `_failure_row` / `_assert_withheld` it imports); every test clears all `TFB_*` and restores the environment |
| Shared harness | `scripts/harness_wp2bc.py` sha256 `9972d99def34ce358c2394157213f92a6f7bf82cf2bd8c02079fdd77546529a4`, 636 lines |
| Export audited | workbook export of **2026-10-10 (sync run 38071484709)**, identified by content: live rows ML 255 / GM 6,609 / CFX 453 / MF 2,474, `f7_settle` tags 254 / 6,397 / 443 / 2,421 all `:observe:`; read with openpyxl `read_only=True, data_only=True` under `python -I` |
| Not edited (reported in S5) | `scripts/verify_deployment.py`, `.github/workflows/ci.yml`; `core/sheet_presentation.py` untouched (challenge C6: no pin change) |

## S2 - Design (WHY; the full block is the engine header `WHY v5.153.0`, placed above the wave-1 `WHY v5.152.0` block)
**Evidence, re-measured this session on the export (probe over the four market pages):** 46 rows carry `f7_settle_failed:non_converged` (GM 45 + CFX 1 = IDRUSD=X); Scoring Errors `scoring_settlement` JSON reason `pass_limit`, `passes_run` 3 on all 46; Block Reason "Scoring settlement failed; clean rebuild required", Final Action DO_NOT_INVEST, blank Overall Score, blank Rank (Overall) and `rank_skipped_settlement_failure` on all 46; INVESTABLE among them **0**; GM status 42 WATCHLIST + 3 BLOCKED (PMT.US, MOV.US, HAYW.US, all published HOLD), CFX 1 WATCHLIST; pass-1 overall on the 45 GM rows 13.65..65.12 (0 at or above the conservative 68 floor), IDRUSD=X 48.25; 44/45 GM rows Forecast Source provider_target + `analyst_lkg` + the short-horizon derivation cap tag (WP-3 symptom, hypothesis). Pass-4 settlers ML 148 / GM 2,824 / CFX 431 / MF 2,406 = 5,809 (MAX_PASSES stays 4; "cap at 3 passes" is already the default). WP-0 `scripts/tfb_workbook_dq_audit.py` v1.0.0 on the same export: **DQ-37 FAIL, count 2,618** (rc= flips ML 85 / GM 2,185 / CFX 54 / MF 294; INVESTABLE flippers 1 / 43 / 1 / 0), `non_converged_total` **46** (0 / 45 / 1 / 0); report digest `a92e4b7c9a54807f` before and after this build (the export is unchanged by construction).

**Gate `TFB_SETTLE_UNRESOLVED` = off | observe | enforce** (explicit words; "1"/"true"/"on" read as off), consulted only inside `_f7_settle_pass` for a `non_converged` result (status `error` stays fail-closed in every mode; with `TFB_SCORING_SETTLE` off the seam never runs):
- **off** (unset): the v5.152.0 lines run unchanged (holdback, `f7_settle_failed:<status>`, JSON, BLOCKED/WATCHLIST, DO_NOT_INVEST, "clean rebuild required").
- **observe**: identical values plus ONE receipt composed from the still-intact pass-1 row BEFORE the holdback blanks it: `settle_unresolved:observe:<pass_limit|oscillation>:p<n>:ov=<pass-1 overall>:rc=<pass-1 rec>` (forbidden-substring safe; degrades to four parts otherwise).
- **enforce**: `_f7_settle_unresolved_publish` keeps the pass-1 scores / forecast tuple / opportunity source / recommendation, applies the intrinsic display cap, withholds `rank_overall`, appends `settle_unresolved:enforce:...`, **keeps the `scoring_settlement:{...}` JSON** (C11: tag = countable read-back, JSON = evidence incl. first_diff / last_diff), neutralises an ADD-family `action_flag` / `decision`, sets `position_size_hint` "Hold existing; no new capital (scoring unresolved)" on a non-exit, non-sell row (B1). No failure receipt, no status / action / reason write at settle time.
- **Tag-driven readers (exact prefix `settle_unresolved:`, never the substring - C7):** `_apply_investability_gate` tier after Fix W (INVESTABLE -> WATCHLIST / WATCH, reason "Scoring unresolved after settlement passes (settle_unresolved) -- research only, no new capital"; BLOCKED / WATCHLIST keep the base chain's verdict); Fix AP then turns a buy-family recommendation on the demoted row into HOLD at every boundary (C1: scores stay pass-1, the label does not); `_top10_row_is_eligible` False; `_apply_rank_overall` skips (rank None, `rank_skipped_settle_unresolved`); `_portfolio_decision` returns HOLD and both `_compute_portfolio_fields` sites turn an ADD Rebalance Action into HOLD (B1). A later switch to off never promotes a cached row. A fresh **stable** settlement retires a stale `settle_unresolved:*` part and its rank companion (C5).
- **Disclosure:** `surface_gate_states()["settle_unresolved"]` (-> `/health engine_gates`) and the `[GUARDS]` boot leg `settle_unresolved=<mode>`.
- **Companion seams (same WP):** routes/analysis_sheet_rows.py 4.8.2 (B2), core/analysis/opportunity_builder.py 1.25.1 (B3 option i), scripts/run_dashboard_sync.py 6.66.0 (WP-2c page rank excludes both prefixes).
- **Refuted premises (not deliverables):** no INVESTABLE leak exists (0 of 46) - this is a posture change; "publish the last stable pass" has no object (F7SettlementResult carries no row for non_converged) - pass 1 is the only basis; "rc= tags -> 0 after enforce" misreads the tag (it describes pass 1 -> pass 2).

## S3 - Anchored edits (count == 1 each; base line numbers)
| # | Base line | Edit |
|---|---|---|
| 1 | :5 title `- v5.152.0` | -> `- v5.153.0` |
| 2 | :8 before `WHY v5.152.0` | insert `WHY v5.153.0` (every prior WHY verbatim) |
| 3 | :4041-4043 terse lines + `__version__` | append the v5.153.0 terse lines; `__version__ = "5.153.0"` |
| 4 | :5938 `"tuple_noise_release": ...` | + `"settle_unresolved": _f7_unresolved_mode(),` |
| 5 | :5990-5991 `_top10_row_is_eligible` failure check | + unresolved check after it |
| 6 | :8076 end of the Fix W tier | + settle-unresolved cap tier |
| 7 | :9440 `_F7_SETTLE_FAILURE_TAG` | + 6 constants |
| 8 | :9490-9492 after `_f7_settle_failed` | + 8 helpers (`_f7_unresolved_mode`, `_f7_settle_unresolved_parts`, `_f7_settle_unresolved`, `_f7_unresolved_part`, `_f7_unresolved_strip`, `_f7_settle_unresolved_tag`, `_f7_unresolved_capital_hold`, `_f7_settle_unresolved_publish`) |
| 9 | :9752-9761 `_f7_settle_pass` non-stable branch | gate branch before the unchanged holdback lines; stale-receipt strip before the stable tagging |
| 10 | :10511 `_portfolio_decision` guard | `if _f7_settle_failed(row) or _f7_settle_unresolved(row):` |
| 11 | :10984 / :11017 `_compute_portfolio_fields` | + `_f7_unresolved_capital_hold(r)` after each holdback call |
| 12 | :16693 rank failure skip | + unresolved skip |
| 13 | :17454 / :17475 GUARDS literal + argument | literal gains ` settle_unresolved=%s`; + `_f7_unresolved_mode(),` |

## S4 - Audits (REAL module; offline; no credentials; no live Sheet)
| Battery | Command | Result |
|---|---|---|
| Compile | `<venv311>/bin/python -m py_compile` and `<venv_full>/bin/python -m py_compile` on every changed/new file | PASS / PASS |
| AST zero-removal | module-level + one-level-nested FunctionDef/AsyncFunctionDef/ClassDef and top-level names, pristine base copy vs delivered | removed `[]` / `[]` / `[]`; added 8 defs + 6 constants; **PASS** |
| New tests (pytest x3) | `<venv311>/bin/python -m pytest -q tests/test_de_f7_settle_unresolved.py tests/test_sync_rank_overall_page.py tests/test_analysis_sheet_rows_rank_withheld.py tests/test_ob_settle_unresolved.py` | `69 passed` x3, outcome digest `9701dfe236d11870` x3; same on venv_full x3 |
| New test (direct) | `python tests/test_de_f7_settle_unresolved.py` (both interpreters) | `RESULT PASS 10/10 engine 5.153.0` |
| Discriminates against the base | same file in a tree whose four changed modules are the pristine base copies | `7 failed, 3 passed`; the 3 that pass are the byte-identity invariants (U2 off == today's holdback, U5 error / settle-off inert, U6 receipt wins) |
| Must-stay-green (C9) | `pytest -q tests/test_de_f7_scoring_settle.py tests/test_de_f7_intrinsic_display_basis.py tests/test_sync_sheet_presentation.py tests/test_sheet_presentation_contract.py` | `233 passed` (venv311, venv_full); + `tests/test_verify_manifest_pins.py` `3 passed` on a scratch tree with the S5 pins applied (= the challenge baseline 236) |
| One-process hygiene run | `<venv311>/bin/python -m pytest -q tests/test_ohlc_fillguard.py tests/test_sync_fetchfail_truth_p162.py tests/test_sync_warnings_compact_wp6.py tests/test_de_f7_scoring_settle.py` + the 4 new files | `120 passed, 1 skipped` (both interpreters) |
| ci.yml lean list + 4 new files, one process | the v1.1.5 "decision-layer suites" list | worktree: `2 failed, 2661 passed` - the 2 are `test_verify_manifest_pins` (pins reported, not edited); scratch tree with the S5 pins: **`2663 passed, 6 skipped`** |
| Broad regression | every `test_de_*`, `test_sync_*`, `test_ob_*`, `test_opportunity_builder*` + route / workflow suites (47 files, venv_full) | `1215 passed, 5 skipped` |
| Harness x3 | `python scripts/harness_wp2bc.py` (3 x venv311 with a git-materialised base tree, 3 x venv_full with `--base-tree <scratchpad>/wp2bc_build/tree_base` built from the pristine copies) | **WP2BC-DIGEST `948d431f6e69d0eab3a598bc6436f91a5ca6b9619356cf7c5a3ef753a614553c` x6**, RESULT PASS; legs L1 `f28f8d758fcbd720`, L2 `ffad7f10d8e17242`, L3 `619abd0f5198d646`, L4 `79c1d83f04e4f6e7`, L5 `b0c148cf72d66382` |
| Byte-identity, gate unset (L5) | the off-mode corpus (6 real fixtures + drift / oscillation / error / cached-receipt rows under TFB_SCORING_SETTLE observe and enforce; gate, Top-10, projection, rank, portfolio-decision readers) computed in two child processes, delivered tree vs pristine base tree | engine off-digest base `cc71a8c2033de3e1` == delivered `cc71a8c2033de3e1`; the off words `off`/`1`/`true` give the same digest as unset |
| L5 discriminates | mutant tree whose `_f7_unresolved_mode()` defaults to observe | engine off-digest DIFF -> RESULT FAIL (as designed) |
| Privacy grep | any digit near the words SAR / cash / NAV / qty / quantity / shares / cost (window of two dozen characters), new files + diff additions | engine additions 0; test 0 (the t6g-pattern holding values in U9 are synthetic, copied from the existing test) |

### Export read-back (harness export leg, NOT in the digest; `TFB_WP2_EXPORT_PATH=<export>`)
| Page | live | non-converged | status | INVESTABLE | `settle_unresolved:` prefix | substring "unresolved" |
|---|---|---|---|---|---|---|
| Market_Leaders | 255 | 0 | - | 0 | 0 | 0 |
| Global_Markets | 6,609 | 45 | WATCHLIST 42 / BLOCKED 3 | 0 | 0 | **1** |
| Commodities_FX | 453 | 1 | WATCHLIST 1 | 0 | 0 | 0 |
| Mutual_Funds | 2,474 | 0 | - | 0 | 0 | 0 |
The one GM row that contains the substring "unresolved" without the prefix is the reason every reader (engine, route, builder, sync, WP-0 rule below) matches the exact prefix.

## S5 - Delivery
`core/data_engine_v2.py` (REVISED 5.153.0) - `tests/test_de_f7_settle_unresolved.py` (NEW) - `scripts/harness_wp2bc.py` (NEW, shared) - this sheet (NEW). Same commit carries the WP-2c / route / builder files and their sheets.

### Pins required (integrator; relative to verify_deployment SCRIPT_VERSION 1.0.48 - the full set is in the run_dashboard_sync 6.66.0 sheet)
`    ("core.data_engine_v2", "__version__", "5.152.0", "data engine"),` -> `    ("core.data_engine_v2", "__version__", "5.153.0", "data engine"),` and the FLAGS entries `("TFB_SCORING_SETTLE", "off", ..., False)`, `("TFB_SETTLE_UNRESOLVED", "off", ..., False)` (kill=False, value flags, ARMED only on the word enforce).

### ci.yml line required (lean "decision-layer suites" list, after `            tests/test_de_margin_publish_p152.py \`)
```
            tests/test_de_f7_settle_unresolved.py \
```

## S6 - Operator steps (one action each)
1. Merge with the verify_deployment pins and ci.yml lines (all four sheets); `tests/test_verify_manifest_pins.py` is green only with the pins.
2. Deploy to Render. No ENV change: the gate defaults off; read-back `/health engine_gates.settle_unresolved == "off"` and the boot line `[v5.153.0 GUARDS] ... settle_unresolved=off`. Rows are byte-identical to 5.152.0 (L5).
3. **Arming order (separate flagged acts, one evidence run each):** (a) `TFB_RANK_OVERALL_PAGE` observe -> enforce (sync sheet) FIRST; (b) Render `TFB_SETTLE_UNRESOLVED=observe`. Read-back on the next export: every one of the non-converged rows (46 today) carries exactly one `settle_unresolved:observe:<reason>:p3:ov=..:rc=..` part, 0 of them INVESTABLE, every other value unchanged (Overall Score still blank, "clean rebuild required" still present). WP-0 must count by exact prefix (see Known limits).
4. **Precondition check before enforce (C8):** record in the read-back `/health` `TFB_OPP_INVESTABILITY_GATE` state (the 2026-10-11 cockpit audit's "first-fail Investability 384/500" implies it is armed - hypothesis) - with builder 1.25.1 the Scoring Settlement gate fails these rows MAJOR regardless, so the builder backstop no longer depends on that env or the panel's Require-Investable=No default.
5. **Enforce** `TFB_SETTLE_UNRESOLVED=enforce`. Read-back: `f7_settle_failed:non_converged` 46 -> 0, `settle_unresolved:enforce:` 0 -> 46 (+/- input drift), blank Overall Score on them 46 -> 0, Rank (Overall) blank on all of them, INVESTABLE among them 0 (by construction), **BLOCKED among them expected <= 3, decided by the base chain** (C2: the holdback BLOCKs every HOLD/buy-family row today; the base gate's verdict on their pass-1 rows is not in the export), "clean rebuild required" 46 -> 0 replaced by "Scoring unresolved ..." only where the base chain said INVESTABLE, `rank_skipped_settlement_failure` -> `rank_skipped_settle_unresolved`, a buy-family pass-1 label on a demoted row published HOLD (Fix AP), the scoring_settlement JSON still present. DQ-37 rc= flips (2,618) are not moved by this build.
6. **Not part of this build:** arming `TFB_SCORING_SETTLE=enforce` (WP-12 #1) and any `TFB_SCORING_SETTLE_MAX_PASSES` change (must stay >= 4; 3 would turn 5,809 pass-4 settlers into non-converged rows).
7. **Rollback:** unset `TFB_SETTLE_UNRESOLVED` (= off, byte-identical on the next request) or `git revert`. Rows already published under enforce keep their tag and stay capped / unranked / unfunded until a stable settlement retires it (tag-driven by design, like `f7_settle_failed`).

## Known limits (stated, not hidden)
- **WP-0 parser rule owed (do not edit here):** `scripts/tfb_workbook_dq_audit.py dq_37_f7` counts non-converged rows as tags starting `f7_settle_failed`; under enforce that count drops to 0 while the rows still exist. DQ-37 must count `f7_settle_failed:non_converged` OR exact prefix `settle_unresolved:` (never the substring "unresolved" - 1 GM row carries it today), add DQ-37b "0 of them INVESTABLE" (PASS today), and classify the single `f7_settle:observe:stx` tag (UNI-USD, KLG residue) separately.
- The enforce posture is proven on synthetic non-converging rows (deterministic drift / oscillation scorers, the t6b/t6c pattern) and the real-fixture readers; the 46 production rows' enforce values are measured only on the first enforce export.
- Tag stickiness: a cached row keeps its receipt until a fresh stable settlement retires it; with `TFB_SCORING_SETTLE` off no settlement runs, so the receipt (and the cap) persists - the same posture as `f7_settle_failed`.
- `position_size_hint` may be refreshed later by `_reconcile_recommendation_family` (Fix E/H) on a buy-family row whose base verdict was not INVESTABLE (no block reason -> no Fix AP); the portfolio guards (`_portfolio_decision`, `_f7_unresolved_capital_hold`) are the binding protection, not the hint text.
- `surface_gate_states()` gains one key (`settle_unresolved`) - a disclosure change on `/health`, not a row change.
