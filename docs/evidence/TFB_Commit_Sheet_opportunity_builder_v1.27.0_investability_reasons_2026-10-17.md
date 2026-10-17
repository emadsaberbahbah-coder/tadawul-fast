# TFB Commit Sheet — opportunity_builder.py v1.27.0: Investability — by engine reason (T1, builder half)

Date: 2026-10-17 (Saturday) · Lane: Render (`/v1/advanced/top10-investments` → `core/analysis/opportunity_builder.py`; manual deploy, read back `/health deploy.render_git_commit`) · Brief: Codex Brief 2026-10-17 v2 §3 **T1** (P1, page change, no decision change) · Cockpit half (render the table under DATA GAPS in `16_Decision_Top10.gs`) ships with the DT10 bump

## S0/S1 — Base pin
Branch `claude/new-session-twoifp` on `d6a9548` (T6) ← `cf8e875` ← `4e88ae6` ← `fa42a55` ← main `11ef134`.

| File | Base | Delivered | Change |
|---|---|---|---|
| `core/analysis/opportunity_builder.py` | sha256 `3f8542e88927b3068f833a3d7fd3dba055a45d871f97411e831fce3b1465bb31` (v1.26.1) | sha256 `39ee6a565bb0adb249869c1c0bd71cdc29a6dba682507e2c485d9e78f11093c4` (v1.27.0) | **+95 / −2** (7 anchored edits, `count == 1`); the 2 removed lines are the version literal in the header and the constant |
| `tests/test_ob_investability_reasons_t1.py` | — | sha256 `60ca0da6e7df5c5a998fb064357ce940f223e10b6b7a0f7f96a328e7f800b81c` | NEW, 25 tests |
| `.github/workflows/ci.yml` | — | — | +1 lean-list line |

## S2 — Why
Top_10 DATA GAPS reads "Investability 466 (93.2%)" and stops. The engine row carries the reason in `block_reason` (`data_engine_v2._apply_investability_gate`; conservative floor overall < 68 / risk > 45), and it already reaches every audit row as `engine_gate.reasons` through the v1.0.1 alias map — the cockpit just never shows it. Measured on Global_Markets 17-Oct: WATCHLIST 6,132 of 6,609 = Engine recommends SELL 1,778 · REDUCE 1,377 · neutral HOLD 1,275 · STRONG_SELL 752 · AVOID 162 · Conservative gate 663 · "Scoring settlement failed; clean rebuild required" 100 · incomplete fundamentals 23; among 226 rows scoring ≥ 72, 88 are benched only because the recommendation is HOLD.

## S3 — What
- `_primary_block_reason(text)` (pure): first ` | ` segment (settlement holdback appends to a prior reason), numeric detail after `:` / `(` dropped — `Conservative gate: overall 61 < 68; risk 52 > 45` → `Conservative gate`; blank → `(no reason on row)`.
- `investability_reason_breakdown(audit_rows, top=6, examples=3)` (pure): over the **written** audit rows whose `first_fail.gate == "Investability"` — exactly the rows the GAS DATA GAPS "Investability" row counts (`dt10FailureBreakdown_` buckets by `first_fail.gate`) — so `total` and the row counts **sum to that row**. Top 6 by count (ties by name), share %, up to 3 example symbols, plus `Other (k reasons)`. Note: a BLOCKED row's first failed gate is Investability (it precedes Blocked Identity in `GATE_ORDER`), so it appears here under its own reason, as it does in the GAS row.
- Attached as `meta["investability_reasons"]` on the certified path (after the audit cap, on the same `audit` list that becomes `candidates_rows`) **and** on the withheld path (today's live case: `output: WITHHELD`).
- **Default ON**, kill-switch `TFB_T10_INVEST_REASONS_LEGACY=1` removes the key. Reason for default-on (brief protocol): display-only meta block that nothing reads (grep: no consumer), no gate/verdict/ticket/score path touched, same class as the always-present `gate_trace_counts` (v1.0.6); a dark table the operator never sees would not close the acceptance.

## S4 — Audits
| Check | Result |
|---|---|
| Anchored edits 7/7 `count == 1`; `ast.parse` | PASS |
| New tests: 19 normalisation cases (engine strings from `_apply_investability_gate`, compound ` \| `, blanks, non-str); breakdown counts only first-fail Investability rows, sums to total (19), Other row, shares ≈ 100; empty/None/junk safe; `top`/`examples` configurable; end-to-end certified payload `total == Σ(first_fail.gate == Investability) == 8` with exact row order and examples; withheld payload carries the same; kill-switch removes the key and leaves every other field byte-identical (`generated_at_utc` aside) | **25 passed** |
| Existing builder suites: `test_opportunity_builder`, `test_opportunity_blocked_invariant` (FastAPI), every `test_ob_*`, `test_top10_selector`, `test_top10_kpi_label_compat` | **all passed** (276 + the invariant suite) |
| ruff `E9,F63,F7,F82,F401,F841` | PASS |
| Payload shape | `_skeleton` untouched (degraded envelopes unchanged); new key only on the two full paths |

## S5 — Delivery
`core/analysis/opportunity_builder.py` · `tests/test_ob_investability_reasons_t1.py` (NEW) · `.github/workflows/ci.yml` (+1) · this sheet. No ENV change required.

## S6 — Read-back
1. Deploy; `/health` `engine`/`opportunity_builder 1.27.0`.
2. Next cockpit run: `payload.meta.investability_reasons.total` equals the DATA GAPS Investability count (466 on the 17-Oct board); top row expected `Engine recommends SELL` by the GM measurement.
3. Cockpit half: `16_Decision_Top10.gs` renders "INVESTABILITY — BY ENGINE REASON" (reason · count · share % · examples) directly under the DATA GAPS table from this block (no GAS-side recomputation; the fixture candidates carry no `engine_gate`).
4. Rollback: `TFB_T10_INVEST_REASONS_LEGACY=1` on Render, or revert.

## Known limits
- The breakdown reads the written (capped) audit grid — the same basis as the GAS table — not the full 9,786-row pool; `meta.gate_trace_counts` remains the uncapped count.
- Reason classes are the engine's own strings; a new engine reason appears as its own bucket automatically.
