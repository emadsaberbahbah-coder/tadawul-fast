# TFB audit implementation — first batch, 8 October 2026

This batch implements verified source defects from the attached deep project audit against baseline `25a0a10f05302fed2b26f1f82bdb800b1b43ad4e`. Connected GitHub main and the clean local checkout matched that baseline at the start. The PDF's workbook, broker and Render observations were retained as audit evidence; they were not independently re-audited in this implementation session.

## Changed behavior and acceptance boundaries

| Ticket | Implemented behavior | Remaining acceptance |
| --- | --- | --- |
| TFB-04 | Export auditor requires account, currency, explicit settled cash, complete cash timestamp, native balance and recorded foreign FX/vintage. Invalid latest rows retain raw cash but cannot certify cash/NAV. Excel date-only metadata cannot become a midnight timestamp. | Offline audit boundary only. Broker authentication, native funding plumbing, fees/cash floor, order IDs/partial fills, multi-account aggregation and sukuk units remain adapter work. |
| TFB-06 | Typed stable/error/non-converged settlement; known failures clear dependent scores/ranks and block new funding/portfolio ADD through mode changes, normalization, projection, Top 10 and signed replay. Nested canonical failure/partial patches cannot certify settlement. Protective exits and source facts are retained. Failed rows do not consume eligible capped-scan capacity. | Source observations are frozen while generated forecasts retain existing convergence behavior. Full TFB-05 supplier-unit/canonical tuple/stage verdict work and live acceptance remain open. No enforcement mode was enabled. |
| TFB-10 | News SingleFlight owns shielded tasks per key/per loop, observes orphan errors, cleans up all exits, bounds acquisition time and supplies `aclose()`. | Explicit shutdown API tested; app lifespan integration remains unchanged. This is no evidence about the allocator crash. Cooperative timeout cannot stop blocking native code. |
| TFB-11 | The engine job blocks required CI; final verdict requires compile, lean, contract and heavy to succeed. Settlement/news/cash regressions are in required lanes. Cash's test-only openpyxl dependency is pinned. Builder version and deployment verifier pin agree. | Full production dependency lock/install/build, remote CI execution, actual required-check enforcement and installed native parity remain release work. |

The separate `eodhd-screener` repository addresses TFB-01/02 on a separate review branch. Its source baseline is `4486803d7cb6bf8d10d402d2db05991111d85194`. A synthetic 401 now makes one attempt, exits 1 and performs zero workbook mutations, versus six attempts, exit 0 and eight mutations before. Diagnostics and the credential-containing example were repaired in that source candidate. Owner-led credential rotation/history cleanup, exact deployed destination ownership, cross-process writers and atomic publication remain open.

## Same-witness results

| Witness | Before | After |
| --- | --- | --- |
| Oscillation, pass limit and exception | Rows could rank first and remain eligible in observe/enforce. | Typed failure, sticky receipt, no dependent rank or new allocation. |
| Failure row with broad investability gate off | Synthetic row could receive a SAR 10,000 ticket. | `DO_NOT_INVEST`, no scoring result and zero executable money in normal and signed replay paths. |
| Future-date/no-time or current-date/no-time cash | Cash SAR 46,398.75, NAV 50,898.75, KPI cash reconciliation `PASS`. | Raw amount retained; certification `FAIL`; certified cash/NAV unknown; no KPI cash reconciliation `PASS`. |
| Leader/follower cancellation | Leader cancellation stranded followers; one follower cancellation spread to survivors. | Other callers settle independently, failure/timeout observed, cleanup and retry succeed. |
| Engine job failure | Final CI verdict returned CLEAN and exited 0. | Verdict fails and exits 1; skipped/cancelled required jobs also fail. |

Recorded FX witness: USD `12,373.20 × 3.753634 = SAR 46,444.4642088`, rounded `46,444.46`. No static FX fallback certifies that record.

Independent review found and drove repairs for four additional seams: native Excel date metadata, an inner recommendation fallback, failed-row pre-cap capacity, and display/warning alias resurrection. The final independent review passed 159 tests with no remaining actionable findings in its reviewed scope. The exact lean script subsequently found a malformed-row regression in the new marker reader; preserving the existing malformed-row contract fixed it without relaxing the test.

## Validation receipt

All local test scripts below used Python 3.11.9. The isolated contract environment pinned the repository's declared CI subset: NumPy 1.26.4, FastAPI 0.115.14, Starlette 0.41.3, HTTPX 0.27.2, Pydantic 2.13.2/core 2.46.2 and AnyIO 4.13.0, with aiohttp 3.13.5 and test-only openpyxl 3.1.5. `pip check` passed. This environment is not the full production requirements lock.

| Check | Result |
| --- | --- |
| Combined 15 focused Python suites | 518 passed |
| Mounted identity and signed board-funding route suites | 25 passed |
| Exact required lean CI test script after final correction | 1,382 passed, 3 skipped, 13 subtests passed |
| Exact Apps Script board-funding CI script | 64 passed |
| Exact heavy engine script | 39 passed |
| Exact heavy settlement/news script | 39 passed |
| Exact heavy cash certification script | 50 passed |
| Standalone export audit selftest | 46/46; unchanged cases digest `3de969895c97a7d6` |
| Separate screener integrity suite | 36 passed on Python 3.11.9 and 3.12.14 |

The three lean skips were one unavailable historical Argaam fixture and two optional yfinance import checks. Two existing pytest return-value warnings remain. Suite counts overlap and must not be summed into a single coverage total. Local route tests used fake providers; the sandbox networking capability was needed for internal AnyIO loop/thread wakeups, not production requests. No assertions, thresholds or skip rules were weakened.

## Next dependency work

1. Complete actual P0 containment: owner-led credential rotation/revocation, retained-history/artifact review and the exact deployed writer/destination map.
2. TFB-03/04: obtain authoritative execution IDs/timestamps, fees and income statements; append witnessed DDI/SBAC amendments; integrate the broker cash/order adapter. DDI gross fills are USD 2,911.88; the implied USD 5.2407 residual must retain its unclassified status until evidenced. SBAC maps to 24 September 2026, preserving the original entry.
3. TFB-05/07: explicit units, one canonical forecast tuple and stage verdicts, then versioned final bundle publication and stale-writer/recovery acceptance.
4. Continue TFB-08/09/12–24 using the audit's ownership and dependency requirements. Complete bound Apps Script export/manifest/triggers, approved universe membership, durable history and historical availability evidence before expanding learning/news or asserting predictive skill.

These changes are prepared for draft review. Production code/configuration, worksheet inputs, broker orders and the disabled runtime learning policy were not changed. Native attestation, deployment and live outcome acceptance are not established by local tests. The audit's model MAE 3.2309 pp versus zero-change baseline 3.0506 pp still does not prove a forecasting advantage.

Source rollback target is the audited baseline. A later production release must retain a prior accepted bundle and verify recovery. No historical backfill or data migration was executed; do not roll back credential revocation by restoring the unsafe example file.
