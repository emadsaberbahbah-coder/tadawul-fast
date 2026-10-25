# Commit Sheet -- `scripts/news_archive_collector.py` v1.3.3 + `news_archive.yml` v1.3.3 [IDENTITY GAP POLICY] -- 2026-10-25

**Scope:** stop the daily red `news-archive` run that hides nothing but a 100%-identity requirement the workbook never promised. ENV-gated; default `fail` is byte-identical to v1.3.2 on every surface (exit codes, evidence JSON keys that existed, stdout lines that existed). The workflow arms `warn`.
**Files:** `scripts/news_archive_collector.py` (1.3.2 -> 1.3.3), `.github/workflows/news_archive.yml`, `tests/test_news_workflow_health.py` (+5 tests), this sheet.

## S1 -- Defect (Actions review, 25 Oct 13:59 Riyadh owner request)
Nine consecutive runs red since v1.3.0 landed (22 Oct 23:12Z): `39870284821, 39906549787, 39999648493, 40035277798, 40066451707, 40085569617, 40110932603, 40114359237, 40136849474, 40165083700`. Each step summary (read in the owner's Chrome; artifacts are unreadable from the build container) shows the same picture, e.g. run `40165083700`:
`Selected acquisition COMPLETE | Selection / discovery checks INCOMPLETE | Archive write APPEND_ACCEPTED | Accepted rows 24 | Existing rows skipped 36 | Failure codes: identity_coverage_incomplete` and stdout `fetched 60/60 (status success, errors 0)`.
`identity_coverage_complete` (`load_news_source_context`) is `invalid_identity_rows == 0 and not ambiguous and lifecycle VALID`. `_identity_pairs` counts every row whose Name is blank, a dash, `n/a` or equal to the symbol as invalid, across Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds, My_Portfolio and the Top-10 board. The pages' publication contract is the coverage audit's 99% Name floor (Mutual_Funds: 24 blank captions allowed; Global_Markets 98.4% cov on 22 Oct) and the Top-10 board keeps ghost seats with a dash Name. A 100% requirement is therefore unmeetable by design, the workflow is red every day, and a genuine `news_batch_failed` / `archive_*` failure would look identical in the Actions list (priority (b): a red pipeline hiding real defects). The lifecycle registry itself is VALID (checked locally).

## S2 -- Change
- `TFB_NEWS_IDENTITY_GAP_POLICY`: `fail` (default; any other value except `warn` is `fail`) | `warn`.
- `_identity_gap_tolerated(report)`: True only under `warn` AND failure codes non-empty and a subset of `{identity_coverage_incomplete, identity_discovery_incomplete}` AND `selected_acquisition_complete is True` AND archive write status in `{APPEND_ACCEPTED, SKIPPED_EXISTING, NO_ELIGIBLE_ROWS, DRY_RUN}` with no `error_code`.
- `_finish_collection_report` records `identity_gap_policy` and `identity_gap_tolerated` in the report, the evidence JSON and the stdout summary; prints `::warning::` when tolerated (codes only, never symbols). The step summary gains one policy line. `failure_codes` and `complete` are untouched (still `false` on a gap).
- Exit points (dry-run, nothing-new, append): `1 if incomplete and not tolerated else 0`.
- **Codex P2 (PR #839, 25 Oct 11:11Z) fixed in `87ef4e4`:** the two gap codes also carry lifecycle-review failures (`_select_news_universe`: registry not VALID, an exclusion whose status is not `VERIFIED_LEGACY` because its review expired or the current caption conflicts) and an empty/over-budget selection. `_identity_gap_tolerated` now also requires `selection.lifecycle_registry.status == VALID`, every `selection.lifecycle_exclusions[].status == VERIFIED_LEGACY`, `selected_count > 0`, no `missing_identity_symbols`, `omitted_priority_count == 0`. Codex's example (ROG.SW excluded at an expired review + a successful ACTIVE.US fetch) exits 1 again; the same with a VERIFIED_LEGACY exclusion exits 0.
- Workflow: `TFB_NEWS_IDENTITY_GAP_POLICY: ${{ vars.TFB_NEWS_IDENTITY_GAP_POLICY || 'warn' }}` on the Collect step; header WHY.

## S3 -- Not changed
Discovery, selection, identity validation, lifecycle exclusions, archive writer, idempotency, sector join, the failure-code vocabulary, the evidence schema `tfb.news-acquisition-summary.v1` (two keys added, none renamed), `continue-on-error` absent, single writer, artifact always uploaded.

## S4 -- Audits
| Battery | Result |
|---|---|
| `tests/test_news_workflow_health.py` (+7), `test_news_acquisition_evidence.py`, `test_news_adaptive_universe.py`, `test_news_lifecycle.py` | 151 passed |
| Golden negative for the Codex P2 against the first v1.3.3 head `984a978` | both lifecycle tests FAIL there, PASS on the fix |
| Golden negative: the 5 new tests against the untouched v1.3.2 copy (`TFB_NEWS_HEALTH_SOURCE`) | FAIL on the base (policy helpers absent; pure-gap run exits 1), PASS on v1.3.3 |
| Default policy (absent / `fail` / `FAIL` / `warning` / `1` / `true`) | `fail`; existing `test_discovery_gap_retains_nonzero_exit...` still exits 1 unchanged |
| `warn` never tolerates: `identity_source_read_incomplete`, `selected_identity_missing`, `priority_selection_capped`, `news_batch_failed`, `collection_error`, `news_response_unverified`, acquisition incomplete, write WITHHELD/NOT_ATTEMPTED/UNKNOWN/UNVERIFIED, write with error_code | asserted |
| Lint | added lines ASCII; 0 functions removed, +2; `compileall` clean |

## S5 -- Operator steps
1. Squash-merge. The next scheduled slot (14:00Z / 20:00Z) runs green when only the identity gap remains; the gap stays visible in the step summary and the evidence artifact.
2. Rollback without a revert: repository Variable `TFB_NEWS_IDENTITY_GAP_POLICY=fail`.

## S6 -- Known limits
- The gap itself (blank Names on the pages, ghost seats) is reported, not repaired; the Name floor and `name_label_renewal` own that.
- Which symbols lack identity is in the evidence JSON (`unknown_identity_symbols`, bounded to 20), never in the annotation.
