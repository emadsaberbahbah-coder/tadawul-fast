# TFB Commit Sheet — core/data_engine_v2.py v5.142.0 — 2026-09-16
## P-143: EQ-ROI backfill write-site sentry (observe instrumentation only)

## Base pin (S1)
- v5.141.0, live-fetched from GitHub raw at branch `main` AND at pinned commit `5ed59fc52a7b679830de516587315df358728dcc` — **byte-identical** (GitHub API rate-limited this session; the dual-ref equality covers the CDN-staleness lesson).
- Base sha256 `85222353e58511129fab4b37d36cb1bf1d1700a4bb19710fc78b594aa577d74f` (prefix 85222353 = the 09-12 v5.141.0 delivery, **zero drift**), 17,587 lines.

## Delivered
- sha256 `8e53a8f8b523d4204eb7977f27e46e7834c6fe7e20068b7fb6236f5b9177d427`, 17,653 lines (+66).
- 12 anchored edits, each asserted count==1: E1 version+WHY, E2 helper, E3–E5 provider-12M branch, E6–E8 provider-3M branch, E9 synthesis block, E10–E12 local-fallback block.
- AST functions 475 → 476 (+`_eq_roi_backfill_sentry_tag`, **0 removed**); py_compile PASS; 0 non-ASCII characters in new lines.

## Root & scope (correction owned)
- The 2026-09-15 pin named ONE writer (Phase-II backfill ~L8253–8310). Live-source sweep found **FOUR** fraction-writer clusters for `expected_roi_1m/3m/12m`: `_phase_ii_quality_forecast` provider-12M branch, its provider-3M branch, its full-synthesis block, and `_compute_scores_local_fallback`. All four are instrumented; the single-site pin is corrected on this sheet.

## Contract adjudication (why enforce is DEFERRED — tag-only)
Proven on the 2026-09-16 export: sheet ROI columns store **fractions under a percent number format** — (a) formatted cells render "25.00%" (a stored 25.0 would render "2,500.00%"); (b) **zero** bare cells with |v|>1.5 across all 9,791 market rows; (c) the independent price/forecast/ROI identity test passes value-wise on 9,399/9,653 rows reading bare-as-fraction. The backfill values are correct **by construction** ((fp−cp)/cp). A x100 rewrite at any sheet-bound path would corrupt every percent-formatted destination — the same hazard the 2026-09-12 vNEXT premise check averted for Upside%/Percent Change. Therefore:
- **P-101 is re-classified**: a DISPLAY-FORMAT gap (cells missing the percent number format), not a value-scale defect. Fix owner = GAS "Reformat" percent-format extension over the ROI columns (queued, blocked on the owning .gs file). Alternative registered: sync writer serializes "%"-strings under USER_ENTERED so cells self-format (bigger contract change, vNEXT).
- `enforce` in this module = observe tags + one `eq_roi_backfill:enforce_deferred` marker per row; values never modified in any mode.
- **Inherited caveat**: core/enriched_quote v4.11.0's enforce (x100) must never be armed for sheet-bound flows without overturning this adjudication in writing (currently dormant — off the sync path per the P-143 root).

## Gate / env
- `TFB_EQ_ROI_UNIT_SENTRY` (same var, per the 2026-09-15 decision), read at CALL TIME, no boot line (FUND-SENTRY precedent). Default unset/off = v5.141.0-identical behavior.
- **No new ENV**: the var is already `observe` on Render — **this deploy is the activation**. Today's single ENV change is the separate IDG arming; read-back namespaces are disjoint (`eq_roi_backfill:*` vs `idg`/schema-shift vs `fund_*`), so attribution stays clean.

## Evidence
- Dual-tree real-module harness H1–H5 **PASS x3, identical digest `a3d652f0560ebde9`**, fixtures = REAL 2026-09-16 Global_Markets export rows (provider_target): H1 off/off deep-equal A==B on all four branch fixtures; H2 observe = zero value drift + exact per-branch tags; H3 enforce = observe values + deferred marker; H4 idempotent (dedup via `_v573_append_warning`); H5 = 300-real-row replay, off A==B, observe fired 900 tags (3 fields x 300 rows).
- Repo battery `tests/test_de_eq_roi_backfill_sentry_p143.py` T1–T6 **PASS x3** against the delivered file as `core.data_engine_v2` (T1 also re-derives the FISV.US ROI by hand from export numbers; T5 bad-mode=off; T6 no tag without a write).
- Harness discovery kept in the test docstring: with every score input None, `_phase_ii_quality_forecast` returns **before** synthesis — synthesis fixtures must carry the row's real score fields.

## Deploy & read-back
- Destinations: `core/data_engine_v2.py`, `tests/test_de_eq_roi_backfill_sentry_p143.py`, `docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.142.0_2026-09-16.md`.
- Render **auto-deploy is OFF** — Manual Deploy required after commit; verify `/health` engine_version 5.142.0, both workers clean.
- Positive read-back = next full sync export carries countable `eq_roi_backfill:<field>:<t12|t3|synth|fallback>:observe` tags (expect thousands, GM-heavy) with **zero cell-value changes**. The P-101 bare counter will KEEP growing until the GAS format fix lands — expected and now attributed, no longer a silent anomaly.
- Rollback: `git revert` (gate default OFF; both non-off modes value-neutral).
