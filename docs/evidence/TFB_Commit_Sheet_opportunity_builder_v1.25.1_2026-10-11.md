# TFB Commit Sheet - core/analysis/opportunity_builder.py v1.25.1 [WP-2b companion: settle_unresolved:enforce fails the Scoring Settlement gate] + tests/test_ob_settle_unresolved.py

Date: 2026-10-11 (Sunday) - Lane: Render / Python (builder; tag-driven, no env) - Build: WP-2bc, challenge blocking defect B3 option (i) - Protocol: One-Pass - File status: builder **REVISED** 1.25.0 -> 1.25.1 - test **NEW** - this sheet **NEW**

## S0/S1 - Base pin
| Item | Value |
|---|---|
| Base commit | `18a84168aac1d462dd58494239ac91f8cef7178c` |
| Base file | `core/analysis/opportunity_builder.py` sha256 `997d5cb8a6535e964479e6a9ece67c1efefdadeac2b5b8ac927955d9c397aae7`, **7,219 lines**, `OPPORTUNITY_BUILDER_VERSION = "1.25.0"`, AST walk-all 194 defs / 0 classes; module level 190 / 3 / 79 |
| Pristine copy | `git show 18a8416:core/analysis/opportunity_builder.py` at `<scratchpad>/base_wp2bc/core/analysis/opportunity_builder.py` (sha256 identical) |
| Delivered | sha256 `a59f9e61b43a9d45728015a5b43050cac4b0639b09bcba0a73226992496e934f`, **7,263 lines** (+44; 2 lines edited in place: docstring `Version:` line, the version constant), AST walk-all 194 / 0, module level 190 / 3 / 79 (no def / constant added), **0 removals**; `py_compile` PASS (both interpreters); ruff E9/F63/F7/F82 PASS; 0 smart quotes; non-ASCII 553 = base |
| Delivered test | `tests/test_ob_settle_unresolved.py` sha256 `d66eb60387220ea9333cbb185992971fc7b09f84071ec90163ca32c22e15883b`, 143 lines, 3 `test_b*` + `__main__` runner (pattern `tests/test_ob_board_funding.py::test_failed_scoring_settlement_cannot_rank_or_fund_with_policy_gates_off`; research-stage builds with an empty observed portfolio - no portfolio figures) |

## S2 - Design (WHY: docstring `v1.25.1 (2026-10-11) [WP-2b]` + the comment block above the version constant)
`_scoring_settlement_failure` (base :3471, regex :3485) was the builder's only settlement reader; it feeds the Scoring Settlement FAIL_MAJOR gate (first in GATE_ORDER), the board pre-filter (`ok_settlement`) and the score blank. Under engine 5.153.0 `TFB_SETTLE_UNRESOLVED=enforce` the non-converged rows (46 on the 2026-10-10 export, all `f7_settle_failed:non_converged` today) stop carrying that receipt and publish a numeric pass-1 score tagged `settle_unresolved:enforce:<reason>:p<n>:ov=..:rc=..`; without this change their only exclusion would be the Investability gate, which is appended only when `investability_gate_enabled` (env `TFB_OPP_INVESTABILITY_GATE`, or the panel's "T10: Require Investable", default "No" since GAS v1.6.9). Fix: after the unchanged `f7_settle_failed:<reason>` scan over every alias (a failure receipt anywhere still wins), a second scan matches `(?:^|[;\s|,])settle_unresolved:enforce:([a-z_]+)(?=$|[;\s|,:])` and returns `unresolved_<reason>` - the existing gate, pre-filter and score blank then treat the row exactly like a failure. The observe-form receipt is not a failure (observe-mode rows still carry `f7_settle_failed` and are caught by the first scan). Off-equivalence: 0 rows carry `settle_unresolved:enforce:` until the engine gate is armed, so every candidate, gate list and payload field except the version stamp is byte-identical to 1.25.0.

## S3 - Anchored edits (count == 1 each; base line numbers)
| # | Base line | Edit |
|---|---|---|
| 1 | :4 `Version: 1.25.0` | -> `Version: 1.25.1` |
| 2 | :11 above `v1.25.0 (2026-10-09)` | insert the v1.25.1 paragraph |
| 3 | :1407 `OPPORTUNITY_BUILDER_VERSION = "1.25.0"` | comment WHY block + `"1.25.1"` |
| 4 | :3485-3489 end of `_scoring_settlement_failure` | + second alias scan before `return None` |

## S4 - Audits
| Battery | Result |
|---|---|
| AST zero-removal (pristine base vs delivered) | removed `[]` / `[]` / `[]`; **PASS** |
| New test, pytest x3 per interpreter (inside the 4-file WP-2bc battery) | `69 passed` x6, outcome digest `9701dfe236d11870`; direct run `RESULT PASS 3/3 builder 1.25.1` |
| Discriminates against the base | `3 failed` in the pristine base tree |
| B2 gate check (REAL `make_criteria` / `normalize_candidate` / `evaluate_gates` / `first_failed_gate` / `derive_verdict`) | `scoring_settlement_failure == "unresolved_pass_limit"`; first failed gate "Scoring Settlement", class MAJOR; verdict DO_NOT_INVEST |
| B3 research build (REAL `build` via `tests/decision_evidence_fixtures`) | unresolved row DO_NOT_INVEST, `opportunity_score` None, first fail Scoring Settlement `unresolved_pass_limit`; the observe-form receipt gives the same verdict / score / first-fail as a clean row |
| Existing builder suites | every `test_ob_*` / `test_opportunity_builder*` / `test_board_funding_route` inside the broad run (`1215 passed, 5 skipped`); ci lean list green with pins |
| Harness L4 / L5 | reader corpus incl. enforce / observe receipts (L4 `79c1d83f04e4f6e7`); L5 builder off-digest base `f09f652864113f1c` == delivered `f09f652864113f1c` (reader corpus without the enforce receipt + research build); WP2BC-DIGEST `948d431f6e69d0eab3a598bc6436f91a5ca6b9619356cf7c5a3ef753a614553c` x6 |
| Privacy grep | 2 hits in the test, both the currency code SAR beside the synthetic identity FX rate 1.0 / a synthetic quote price - no portfolio figure |

## S5 - Delivery
`core/analysis/opportunity_builder.py` (REVISED 1.25.1) - `tests/test_ob_settle_unresolved.py` (NEW) - this sheet.
Pin required (integrator, `scripts/verify_deployment.py` line 112): `     "1.25.0", "opportunity builder"),` -> `     "1.25.1", "opportunity builder"),` (full pin set in the run_dashboard_sync 6.66.0 sheet). ci.yml lean-list line: `            tests/test_ob_settle_unresolved.py \`.

## S6 - Operator steps
1. Deploy with the engine (Render). No ENV. Read-back: payload / footer version `builder v1.25.1`; candidate verdicts unchanged (no row carries the enforce receipt yet).
2. After the engine's `TFB_SETTLE_UNRESOLVED=enforce`: the cockpit's first-fail census shows the non-converged rows under "Scoring Settlement" with current `unresolved_pass_limit` (or `unresolved_oscillation`), never under "Investability" - independent of `TFB_OPP_INVESTABILITY_GATE` and the Require-Investable panel value. Record the `TFB_OPP_INVESTABILITY_GATE` Render state in that read-back (C8).
3. Rollback: `git revert` (no env lever; byte-identical while no row carries the enforce receipt).

## Known limits
- The reader keys on the `settle_unresolved:enforce:` prefix only; a future enforce-tag shape change in the engine must update this regex in the same commit.
- The opportunity score is blanked for these rows (the existing failure posture), so they never appear as near-miss "how to qualify" candidates either.
