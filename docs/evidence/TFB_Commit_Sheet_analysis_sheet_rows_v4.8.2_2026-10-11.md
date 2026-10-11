# TFB Commit Sheet - routes/analysis_sheet_rows.py v4.8.2 [WP-2b companion: the per-request Rank (Overall) withholds engine-withheld rows] + tests/test_analysis_sheet_rows_rank_withheld.py

Date: 2026-10-11 (Sunday) - Lane: Render / Python (route; tag-driven, no env) - Build: WP-2bc, challenge blocking defect B2 - Protocol: One-Pass - File status: route **REVISED** 4.8.1 -> 4.8.2 - test **NEW** - this sheet **NEW**

## S0/S1 - Base pin
| Item | Value |
|---|---|
| Base commit | `18a84168aac1d462dd58494239ac91f8cef7178c` |
| Base file | `routes/analysis_sheet_rows.py` sha256 `70519350a7045470611c82ed8c4c5a023d6c2d5423b5900e89ed120c4f86b505`, **3,464 lines**, `ANALYSIS_SHEET_ROWS_VERSION = "4.8.1"`, AST walk-all 105 defs / 0 classes; module level 101 / 0 / 29 |
| Pristine copy | `git show 18a8416:routes/analysis_sheet_rows.py` at `<scratchpad>/base_wp2bc/routes/analysis_sheet_rows.py` (sha256 identical) |
| Delivered | sha256 `4a4b9108ed2f4f80f385c04b096234318b0148b22e4c6781b741faf9b5fbbcae`, **3,501 lines** (+37; 3 lines edited in place: banner, version constant, the rank predicate), AST walk-all 106 / 0, module level 102 / 0 / 30 (+1 def `_rank_withheld_by_tag`, +1 constant `_RANK_WITHHELD_PREFIXES`), **0 removals**; `py_compile` PASS (both interpreters); ruff E9/F63/F7/F82 PASS; 0 smart quotes; non-ASCII 79 = base |
| Delivered test | `tests/test_analysis_sheet_rows_rank_withheld.py` sha256 `56b51a4226821c31c08eeb10aa02acdc65f0e20e8db985cb7223fc5971655ca8`, 132 lines, 4 `test_r*` + `__main__` runner |
| Manifest | this module is **not** pinned in `scripts/verify_deployment.py` MODULES (challenge C4: the "routes v4.18.1" of the spec is ADVANCED_ANALYSIS_VERSION of routes/advanced_analysis.py, which has no rank pass) - no pin change |

## S2 - Design (WHY in the module header `v4.8.2 (2026-10-11) [WP-2b]`)
The daily sync reads the four market pages through this router (`TFB_SYNC_MARKET_GATEWAY` "analysis") in 25-symbol batches, so `_apply_global_rank_overall` (called at :2225 on each request) is the last ranker before the sheet. Its predicate was "numeric score and no `low_data_trust`". Under data_engine_v2 5.153.0 `TFB_SETTLE_UNRESOLVED=enforce` a non-converged row publishes a numeric pass-1 score tagged `settle_unresolved:enforce:...`, and this pass would stamp it a within-batch rank over the engine's withheld one (the "never ranked" claim would then hold only if WP-2c enforce were armed first). Fix (tag-driven, exact token prefix on each `;` part, list/tuple cells handled, never a substring): `_RANK_WITHHELD_PREFIXES = ("f7_settle_failed:", "settle_unresolved:")`; such a row's rank is blanked to "" like a LOW-trust row. Byte-identity: on the 2026-10-10 export 0 rows carry `settle_unresolved:` and every `f7_settle_failed` row already has a blank Overall Score (engine holdback), so the new clause cannot change any rank until the engine gate is armed. The only input class that differs from v4.8.1 is a `f7_settle_failed:` row WITH a numeric score, which the engine never emits (its holdback blanks the score at every boundary).

## S3 - Anchored edits (count == 1 each; base line numbers)
| # | Base line | Edit |
|---|---|---|
| 1 | :5 banner `v4.8.1` | -> `v4.8.2` |
| 2 | :12 above `v4.8.1:` | insert the v4.8.2 WHY paragraph |
| 3 | :358 `ANALYSIS_SHEET_ROWS_VERSION = "4.8.1"` | -> `"4.8.2"` |
| 4 | :460 `_LOW_TRUST_TAG` | + `_RANK_WITHHELD_PREFIXES` (with comment) |
| 5 | :1273 before `_global_rank_enabled` | + `_rank_withheld_by_tag(warnings)` |
| 6 | :1302 predicate | `if score is None or _LOW_TRUST_TAG in warns or _rank_withheld_by_tag(row.get("warnings")):` |

## S4 - Audits
| Battery | Result |
|---|---|
| AST zero-removal (pristine base vs delivered) | removed `[]` / `[]` / `[]`; **PASS** |
| New test, pytest x3 per interpreter (inside the 4-file WP-2bc battery) | `69 passed` x6, outcome digest `9701dfe236d11870`; direct run `RESULT PASS 4/4 route 4.8.2` |
| Discriminates against the base | `2 failed, 2 passed` in the pristine base tree (the 2 that pass are R2 "untagged rows rank exactly as v4.8.1" and R4 "scope + kill switch unchanged" - true on both) |
| Route tests touching the module | `tests/test_recent_fixes.py`, `tests/test_learning_guards.py`, `tests/test_route_error_redaction.py`, `tests/test_schema_alignment.py`, `tests/test_sync_insights_publication.py` inside the broad run (`1215 passed, 5 skipped`) and the ci lean list (green with pins) |
| Harness L4 / L5 | L4 `79c1d83f04e4f6e7` (tagged rows -> ranks `[1, "", "", ""]`); L5 route off-digest base `975392b72aa601ac` == delivered `975392b72aa601ac`; WP2BC-DIGEST `948d431f6e69d0eab3a598bc6436f91a5ca6b9619356cf7c5a3ef753a614553c` x6 |
| WP-6 reader census (`tests/test_sync_warnings_compact_wp6.py::test_t12`) | 1 passed - the route reads only `f7_settle_failed:` / `settle_unresolved:` (both KEEP heads), no movable head |
| Privacy grep | 0 hits |

## S5 - Delivery
`routes/analysis_sheet_rows.py` (REVISED 4.8.2) - `tests/test_analysis_sheet_rows_rank_withheld.py` (NEW) - this sheet. No verify_deployment pin. ci.yml lean-list line (see the run_dashboard_sync 6.66.0 sheet): `            tests/test_analysis_sheet_rows_rank_withheld.py \`.

## S6 - Operator steps
1. Deploy with the engine (Render). No ENV. Read-back: route rows unchanged (no tagged row exists yet); `meta` version string reads 4.8.2.
2. After the engine's `TFB_SETTLE_UNRESOLVED=enforce`: the non-converged rows show a blank Rank (Overall) on every per-request response even before WP-2c enforce (the arming order "page rank first" stays the recommended sequence, but correctness no longer depends on it).
3. Rollback: `git revert` (no env lever; byte-identical while no row carries either prefix with a numeric score).

## Known limits
- `TFB_ANALYSIS_GLOBAL_RANK=0` still turns the whole per-request rank pass off (rows pass upstream ranks through) - unchanged behaviour.
- Matching is case-insensitive on the lower-cased warnings text, exact prefix per `;` part.
