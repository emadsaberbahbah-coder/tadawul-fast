# TFB commit sheet — data_engine_v2 v5.151.0 [P-152 / P-146b MARGIN PUBLISH CONTRACT] — 2026-09-28

**Build #2 of 2026-09-28 (backend lane).** Owner Claude · approver/executor Emad · read-only session: nothing committed, no ENV touched, no order placed.

## 1. What and why (one mechanism)
The three margin columns leave the engine in **three units at once**. On the 2026-09-28 Global_Markets export, Gross Margin holds 2,401 rows stored as fractions (yahoo path: DDI 0.7336), 3,760 as percent points (EODHD path after the v5.140.0 sentry: FISV 47.2156) and 4 above 150 (financial-sector artefacts); Profit Margin carries the v5.143.0 REPAIR output in points (DDI 32.908) beside yahoo fractions; Market_Leaders stores gross/operating as fractions (254/254) next to profit in points (152/240). Every margin cell is rendered under a percent **number format** — the operator's 09:25 Reformat extended it to all 6,165 GM cells — so a points value displays ×100: 3,579 GM gross cells > 100 %, 156/255 ML profit cells (Al Rajhi 6,795 %, DDI 3,290.80 %, YUM 2,540.70 %). Scoring is **not** the victim (`compute_quality_score` reads margins through `scoring._as_fraction`, the engine through `_as_pct_points`; both are unit-agnostic below 1.5) — the defect is the **published value contract**: one column, three units, one format.

**Adjudicated contract** (the 2026-09-16 rule for `expected_roi_*`, extended): every percent-formatted sheet column stores a **fraction**. The engine keeps its internal percent-points contract for margins; the publish boundary emits the fraction. No GAS change and no Reformat change are needed — the existing percent format then renders every margin correctly.

| ENV (read at call time, no restart) | Values | Effect |
|---|---|---|
| `TFB_MARGIN_PUBLISH` | `off` (default) · `observe` · `enforce` | off = v5.150.0 byte-identical on every row; observe = one countable tag per margin field, values untouched; enforce = the conversion below + suffix-less tag |

`_margin_publish_contract(row)` runs at all three publish boundaries (`_strict_project_row`, `get_page_rows`, the direct Top_10 path) **immediately after `_apply_investability_gate`** (the gate reads presence, never magnitude — decision-neutral by construction; placed after rather than before the gate so the P-102 test's seam-distance assertion on the `_fc_tuple_coherence` line keeps holding). Per field (`gross_margin`, `operating_margin`, `profit_margin`):

| Evidence on the row | kind | enforce |
|---|---|---|
| points witness: suffix-less `fund_unit_contract:eodhd:<field>` (sentry converted it) or `fund_coherence_repaired:<field>:*` (repair wrote points) | `pts` (`pts_thin` when \|v\| ≤ 1.5) | v / 100 |
| no witness, \|v\| ≤ 1.5 | `frac` — `frac_amb` when the row's only fundamentals provenance is the EODHD fallback (the residual ambiguity class, disclosed, never scaled) | kept |
| no witness, 1.5 < \|v\| ≤ 150 | `pts` | v / 100 |
| \|v\| > 150 | `oob` | v / 100 as points, tagged — **never / 10,000** (no ground truth for a ×100-points reading; the AS-1 / F01-inverse rule) |

Tags `margin_publish:<field>:<kind>[:observe]` are substring-safe for the investability gate; the suffix-less tag is also the idempotence marker (a second boundary pass skips the field; an observe-tagged row is still converted exactly once when the gate flips to enforce). Never raises. Mode disclosed in the `[GUARDS]` boot line (`margin_publish=`) and `/health engine_gates.margin_publish`.

## 2. Pinned source (S1)
| Item | Value |
|---|---|
| Repo / branch | `emadsaberbahbah-coder/tadawul-fast` · `main` |
| HEAD at fetch | `d40d67abfe91f5a0573d8629c48bd3301e0ea50b` (#614, 2026-09-28 11:47 Riyadh) — build #1 (portfolio_actions v1.13.0) verified landed byte-identical at this HEAD |
| Base file | `core/data_engine_v2.py` v5.150.0 · 18,670 lines · sha256 `ef9ad4fde067e7c2d87793763e138df094ad2d5976c063cd26fe993d5f1991b6` |
| Live engine (cockpit 07:21) | engine 5.150.0 (fund_unit_sentry=enforce, fund_cache=observe) |

## 3. Delivered files (S5)
| File | Lines | sha256 |
|---|---|---|
| `core/data_engine_v2.py` **v5.151.0** | 18,811 | `22f8ad8461d85aa6a6c838f238dae220073a2b1cd3a0c4bd426595a4efd94858` |
| `tests/test_de_margin_publish_p152.py` (new, 8 tests) | 228 | `44faf827feaee8fae25ac975d342d3ade3c6b39d481de2596614067447c89049` |
| `data_engine_v2_v5.150.0_to_v5.151.0.diff` (review aid, not for commit) | 155 | — |

## 4. Build proof (S3/S4)
- **Anchored edits: 8, each `count == 1` asserted** — WHY block + version; constants/helpers block before `_fc_tuple_mode`; the three boundary calls after the gate; `/health engine_gates.margin_publish`; `[GUARDS]` format + argument.
- `py_compile` OK · **AST defs 517 → 521: +4 (`_margin_publish_mode`, `_mpc_warning_parts`, `_mpc_points_witness`, `_margin_publish_contract`), removed 0** · diff +143 / −2 lines (the two removed lines are the version constant and the GUARDS format string, both re-added) · smart quotes 0 · non-ASCII added 0 · CR 0.
- **Real-module dual-tree harness ×3** (base v5.150.0 vs delivered v5.151.0 loaded as real modules; REAL rows = the 2026-09-28 Global_Markets 6,609 + Market_Leaders 255 + My_Portfolio 5 exports mapped onto the canonical sheet keys with stored values reconstructed (percent-formatted cells = display / 100) and the Warnings column carried verbatim as the witnesses; run through the real boundary `_strict_project_row(keys, row)`): digest `7e5921b1f72a913b` identical ×3.

| Gate | Result |
|---|---|
| H1 off | projections byte-identical on all three pages (6,869 rows), no tags |
| H2 observe | every non-warnings value identical to base; one tag per margin field |
| H3 enforce | conversion exactly per rule on every row; non-margin fields identical to base; warnings differ only by the new tags; idempotent on a second pass; observe→enforce transition converts once; second-pass re-derivation drift of the gate's own fields (`block_reason`, `conflict_type`, `final_decision_basis`, `provider_engine_conflict`) is **identical on base and delivered** (pre-existing, see §5) |
| H4 unit | witness rules (`:observe` sentry tag is not a witness; repaired `d100` is), thin/amb/oob/negatives, list-typed warnings, junk/NaN/bool/non-dict inputs, injected helper fault → 0 and no raise, explicit-word gate, tag substring safety |

Kind histogram on the real book (enforce): GM gross `frac` 2,286 / `frac_amb` 89 / `pts` 3,760 / `pts_thin` 26 / `oob` 4; GM operating 2,270 / 88 / 3,589 / 176 / 46; GM profit 1,123 / 73 / 2,328 / 1,861 / 0; ML gross+operating `frac` 254 each, profit `pts` 152 + `frac` 88; My_Portfolio gross/operating `frac` 4, profit `pts` 3 (DDI 32.908 → 0.32908, YUM 25.407 → 0.25407, CARE 44.92 → 0.4492). Named specimens: DDI gross 0.7336 kept / profit → 0.32908; FISV gross 47.2156 → 0.472156; 1120.SR profit 67.951 → 0.67951; TOP.CO gross 100.3553 → 1.003553 (`pts`, honest > 100 %); CWBC profit None untouched.
- **Tests:** new file 8/8 ×3 on v5.151.0; on v5.150.0 it fails 7/8 (T7 substring-safety holds on both by construction) — it discriminates. Existing engine batteries on the delivered tree (`test_de_fc_tuple_coherence_p102`, `test_de_fund_sentry_repair_p146`, `test_fund_unit_sentry`, `test_de_eq_roi_backfill_sentry_p143`, `test_de_w52_ceiling_scrub_p164`, `test_de_rel_path_tag_p115b`, `test_de_crypto_pair_shape_p151`, `test_de_fund_cache_first_p154c`, `test_de_f7_scoring_settle`): **73 passed, 1 skipped** (the pre-existing opt-in skip).
- Harness caveat (disclosed): the harness reconstructs stored values from the export's rendered cells (display / 100 for percent-formatted columns) — exact for the margin columns (verified against the live-cell reads in the 28-Sep audit: My_Portfolio AH2 = 32.908) but a reconstruction nonetheless; the enrichment pipeline itself (providers, sentry, cache) is not exercised by this build and is untouched.

## 5. Post-freeze findings → Register (vNEXT; not built here)
- **P-146b engine leg** — make the ENGINE row single-unit (yahoo margins ×100 at the sentry seam, provenance-exact) so the internal points contract is true; only then can the publish leg drop its heuristic (`frac_amb` 89/88/73 rows today).
- **Second-pass gate drift (pre-existing, base-identical):** re-projecting an already-projected row re-derives `block_reason` / `conflict_type` / `final_decision_basis` / `provider_engine_conflict` on Global_Markets and Market_Leaders (My_Portfolio stable). Not caused by this build; candidate register item (the boundary chain is documented as idempotent).
- The scoring consumer path is unit-agnostic below 1.5 (`_as_pct_points`) — the only scoring exposure is a points value ≤ 1.5 read as a fraction (thin margins); the same ambiguity class as `frac_amb`, closed by P-146b.
- `run_daily_brief.py` reads Profit Margin from the sheet expecting points but carries its own unit-coherence guard (v1.17.0 `TFB_BRIEF_UNIT_COHERENCE`); with fractions published it lands in that guard's "fraction row" branch — verify on the first brief after enforce.

## 6. Operator steps (one action each; GitHub web UI)
1. Upload the delivered `data_engine_v2.py` over `core/data_engine_v2.py`: https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/core
2. Upload `test_de_margin_publish_p152.py` into `tests/`: https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/tests
3. Upload this sheet into `docs/evidence/`: https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/docs/evidence
4. Commit message: `data_engine_v2 v5.151.0 [P-152] margin publish contract at the publish boundary (default off = byte-identical) + tests`
5. Render Manual Deploy from `main` (one deploy carries v5.151.0, portfolio_actions v1.13.0 and opportunity_builder v1.22.1). Read-back: `/health engine_version 5.151.0`, `engine_gates.margin_publish = "off"`, boot line `[GUARDS] … margin_publish=off`; rows unchanged.
6. Rollback: revert the commit (no ENV to unset).

## 7. Arming — separate act, flagged for approval (one Render ENV per evidence run)
- Today's Render ENV slot is reserved for `TFB_PF_CONFIRM_SESSION=observe` (build #1). **Tomorrow:** `TFB_MARGIN_PUBLISH=observe` — read-back on the next full sync export = `margin_publish:<field>:<kind>:observe` tags on every priced margin cell (expected GM ≈ 6,165 gross / 6,169 operating / 5,385 profit; ML 254/254/240), values byte-identical, the four kind counts matching §4 within the day's row churn.
- **Enforce** after one clean observe read-back (display-only change, not recommendation-changing; the S-1 lane is untouched). Expected effect on the page: every margin cell renders its true percentage under the existing format — DDI profit 32.91 %, Al Rajhi 67.95 %, FISV gross 47.22 %; the 3,579 GM gross cells > 100 % collapse to the `oob` count (4) plus genuine > 100 % financial-sector rows.
