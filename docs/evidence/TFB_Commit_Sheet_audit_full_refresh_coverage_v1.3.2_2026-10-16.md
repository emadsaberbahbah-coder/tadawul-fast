TFB Commit Sheet — scripts/audit_full_refresh_coverage.py v1.3.2 [name-gap list: which symbols hold the Global_Markets names floor down]
Date: 2026-10-16 (Friday, evidence day) · Lane: GitHub Actions (no deploy) · Gate: none (diagnostic field only) · Nothing armed

S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `origin/main` 11ef1345, `audit_full_refresh_coverage.py` v1.3.1 (382 lines, 28 defs) |
| Delivered | v1.3.2. Defs 28 → 28: 0 added, 0 removed (AST). Two `Result` fields added: `blank_name_count`, `blank_name_symbols` |
| Other files | `tests/test_coverage_name_gap_v132.py` (NEW, 8 tests) · `scripts/finalize_decision_publication.py` 1.1.1 → 1.1.2 (log-only notice, see S7) · `.github/workflows/ci.yml` (lean list +1 line, next to `test_full_refresh_coverage.py`) · `scripts/verify_deployment.py` (two pin lines: coverage 1.3.2, finalize 1.1.2) · this sheet |

S2 — Why (evidence)
- The final publication audit applies `Rule(page, floor, age, fresh, 99, 95)` (`finalize_decision_publication.py:74`). The Global_Markets names floor of 99% failed in 5 of 5 sampled audits (names 97.00 / 97.99 / 98.11 % …).
- In 2 of those 5 audits (10-14 00:48Z and 10-15 11:06Z), the names floor was the ONLY coverage failure. `coverage_failed` is published on every run, and the release refuses on it.
- 99% of 6,609 allows at most 66 blank names. HEAL-FIRST reports 123–129 blank-name symbols per pass, so about 57+ need a label or retirement.
- The brief's D3 (retire about 80 nameless GM rows, k ∈ [68, 97]) and the NAME-LABELS registry are the two owner routes. Neither can be acted on today, because no artifact lists WHICH symbols are nameless.

S3 — What
- `audit_grid` collects the canonical symbols whose Name cell is blank on every page that has a name rule (`min_name > 0`) and is not a portfolio rule (holdings are never listed). The page result gains:
  - `blank_name_count`: exact;
  - `blank_name_symbols`: sorted, first 200.
- `main()` prints one `::notice::<page>: blank-name symbols N (name_pct=…): A, B, …` line per page with a gap. It shows the first 60 and has no braces.
- UNCHANGED: every rule, floor, failure string, warning, status, exit code and the `finalize_decision_publication` blocker logic. The JSON gains two keys per page. The final-publication artifact therefore carries the list too, which is the evidence D3 needs.

S4 — Audits
| Battery | Result |
|---|---|
| `tests/test_coverage_name_gap_v132.py` | 5 passed ×3. Cases: sorted list + unchanged name failure · 200 cap with exact count · verdict fields independent of the new fields · no list without a name rule or on a portfolio rule · `main()` notice line, no braces, exit code 2 unchanged, JSON carries the list |
| Dual-tree | v1.3.1 (`origin/main`) vs v1.3.2 `audit_grid` on 300 random grids: rows with blank symbols, names, prices and stamps, invalid warnings, cache providers; all four rule shapes including My_Portfolio. Every v1.3.1 field is identical (failures, warnings, status included) |
| Mutation | portfolio exclusion removed → 1 failed · cap removed → 1 failed · notice suppressed → 1 failed |
| Regression | every test naming the coverage audit, final publication, release feed or the manifest pins: 458 passed, 10 skipped |
| Workflow audit | errors=0 (the 2 pre-existing TEMPORARY_OVERRIDE_ACTIVE warnings) |

S5 — Delivery
Merge only. The next scheduled sync's final job, and every `full_refresh_coverage.yml` / `decision_surface_freshness.yml` run, will carry the list.

S6 — Owner use (nothing armed)
1. After the next sync, read the `blank-name symbols` notice in the "Verify final decision publication" job, or `pages[Global_Markets].blank_name_symbols` in `final_decision_publication.json`.
2. Label the real instruments through the NAME-LABELS registry, and put the dead ones on the D3 retirement list (Saturday decision, k ∈ [68, 97]). Clearing 57+ names clears the names floor without touching it.
Rollback: revert (the fields are additive).

S7 — Review fix (Codex P2 on #789, comment 4292646971): the notice must reach the final job
Problem: the scheduled "Verify final decision publication" job runs `finalize_decision_publication.py`, which calls `coverage.run()` directly and prints only a compact summary line. The v1.3.2 notice was printed only by the coverage module's own `main()`, so the final job's log would not have shown it.
Fix:
- New pure helper `audit_full_refresh_coverage.name_gap_notice(page_dict)`, used by both entry points.
- `finalize_decision_publication` 1.1.2 prints the same `::notice::` line for each coverage page with a gap, after its unchanged summary line. The print is inside a try/except, so it can never change the result.
- The result JSON, blockers, exit codes and the feed write are unchanged (pinned by test).
Tests: 8 passed ×3. The 3 new tests cover:
- the final entry point prints the notice, its first line is still the summary JSON, and the result file is unchanged;
- nothing extra is printed when there is no gap;
- the helper format (60-symbol cap, no braces).
Related suites: 461 passed, 10 skipped. The dual-tree check is still identical on 300 grids.
