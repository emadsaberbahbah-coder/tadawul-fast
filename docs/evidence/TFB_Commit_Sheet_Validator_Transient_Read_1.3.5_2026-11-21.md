# Commit Sheet: the dashboard validator waits out a passing Sheets fault (validate_dashboard 1.3.5, ci.yml 1.1.91), 2026-11-21

**Scope.** Owner, 21 Nov 2026 02:20 Riyadh (in Arabic: "review the updates for 6 hours and see the updates they need"). The watch found a sync that wrote its page in full but ended red on one passing Google answer. This is the read the Sheets fix of PR 1040 did not reach. No check, status, threshold or exit code changes. No score, gate, ticket or broker action changes.

## Why (measured 21 Nov, Actions API and run logs, read-only)
| Run | What happened | Effect |
|---|---|---|
| Sync 44801530434, 20 Nov 22:22 Riyadh (dispatched) | 1. The global-markets leg wrote all 6,602 rows: "SYNC DONE success=1 ... rows_written=6602". 2. The validator's literal read then got `HttpError 500 ... "Internal error encountered."` once. 3. The read fell back to the logical engine rows. 4. `scope.coverage` read "UNKNOWN literal-sheet extent": FAIL, exit 2. | The leg and the final-publication check went red. The data itself was fine. |
| The same hour | The same passing fault ended three other runs: Today Action Plan, Release Decision Feed and Full Refresh Coverage. | sheets_quota 1.1.0 (PR 1040) taught those three to wait it out; this read was the one left. |

## Changes
| File | Change |
|---|---|
| `scripts/validate_dashboard.py` 1.3.4 -> 1.3.5 | Both literal reads (every page in `_read_page`, and the decision surface) go through the new `_read_literal`. A Sheets quota answer (429: 65 s, 65 s) or a passing server fault (500/502/503/504: 15 s, then 30 s), as `scripts/sheets_quota.py` reads them, is waited out. The read is then tried again, at most `READ_RETRIES` (2) times. Any other error, or the last try, goes on exactly as in 1.3.4: the fallback reader for a page, `decision.read` FAIL for the surface. Without `scripts/sheets_quota.py` nothing is retried. This is the pattern of `audit_full_refresh_coverage` 1.3.3. |
| `scripts/verify_deployment.py` | Pin `validate_dashboard` 1.3.5. |
| `tests/test_validator_transient_read.py` (new) | T1 to T7, driving the real `validate()` and `_run_decision_surface` with fake reads and a fake wait. |
| `tests/test_validate_dashboard_decision_surface.py` V6 | The version pin. |
| `.github/workflows/ci.yml` 1.1.91 | The new file is in the blocking lean lane. |

What the new tests cover:
- One 500 is waited out (15 s) and the audit passes on the same range.
- Each of 502/503/504 waits 15 s; a 429 waits 65 s.
- Two faults wait 15 s then 30 s; two quota answers wait 65 s twice.
- A fault that lasts still fails (exit 2) after two retries.
- 400/403/404, ValueError and KeyError are not retried.
- The decision surface waits too, and a lasting fault is still `decision.read` FAIL with the error kept.
- The waits are the shared helper's, and nothing is retried without it.

## Pre-merge checks
- **Tests.** test_validator_transient_read 14/14. The validator, scope, publication, embargo, CI-verdict and manifest suites together: 366 passed, 39 subtests passed.
- **Mutants: 8 / 8 killed.**
  1. no retry;
  2. more retries;
  3. every error retried;
  4. the decision read not patient;
  5. the page read not patient;
  6. waits not from the helper;
  7. no try limit;
  8. retries without the helper.
- **Lint and audit.** ruff (E9, F63, F7, F82) is clean, and the workflow audit reports errors=0, warnings=0.
- **Cost of a bad case.** At most 45 s more for a server fault, or 130 s for two quota answers, inside a sync leg of about an hour.

## Post-merge (lead)
- **Variables.** No variable. The next dispatched sync runs the new validator.
- **Read-back.** A passing fault now shows as "v1.3.5 Sheets transient fault reading <page> (...); waiting 15 s, then read 1 of 2" in the leg's log, followed by a normal `scope.coverage` line.
- **Merge order.** `ci.yml` here and in PR 1043 both move from 1.1.90 to 1.1.91. The second to merge takes 1.1.92 when main is merged into it; only the header lines differ.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
