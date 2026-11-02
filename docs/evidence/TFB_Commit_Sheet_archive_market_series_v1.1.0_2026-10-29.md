# Commit Sheet - archive_market_series v1.1.0: TASI and CSI 300 rebuilt from hourly bars (C8 Phase 2b) - 2026-10-29

**Scope:** the first live fill of `tfb_archive.market_series_daily` (run 40851622616, 29 Oct 09:41 UTC, from main e34729bd) loaded **47 of 48 series, 34,681 bars**; it failed on `^TASI.SR` ("no bars") and loaded `000300.SS` (CSI 300) with a single day. TASI is the Saudi market the page exists for, so this is fixed before the page is built. Same read-only contract as v1.0.0: nothing written to the workbook, no decision / gate / count changes.

## Root cause (probed live from the owner's browser, 29 Oct ~09:50 UTC)
| Request | `^TASI.SR` | `000300.SS` | control `000001.SS` |
|---|---|---|---|
| daily, period1 2023-10-25 (the job's request) | 1 bar (today) | 1 bar | 730 bars |
| daily `events=history`, `range=5y / 3mo / 1mo`, query1 and query2 | 1 bar each | 1 bar | - |
| weekly `range=2y`, monthly `range=max` | 1 bar | - | - |
| **hourly `range=730d&interval=1h`** | **3,649 bars, 730 trading days from 2023-11-27** | **4,381 bars, 730 days from 2023-10-25** | - |

Yahoo serves only the live daily bar for these two indices (their daily history is missing upstream, `firstTradeDate` 1998 notwithstanding) while the hourly history is complete. The job dropped the live bar (session open) and so had nothing for TASI, and WRITE_TRUNCATE'd CSI 300 down to one day.

## Change
| File | Version | Change |
|---|---|---|
| `scripts/archive_market_series.py` | 1.0.0 -> 1.1.0 | a daily answer with fewer than `MIN_BARS` (30) bars is rebuilt from hourly bars (`range=730d&interval=1h`, query1 then query2): one row per venue-local trading day - first open, highest high, lowest low, **last hourly close**, summed volume, `adj_close` NULL, `source = yahoo_chart_v8_1h`; an open session left out as before; only days from `period1` on. A series that still has fewer than `MIN_BARS` bars **fails and keeps its rows** (no WRITE_TRUNCATE down to one day); the others load |
| `tests/test_archive_market_series.py` | M1-M13 | M4 (no hourly call on a full daily answer; one try per resolution on 404), M12 (hourly aggregation, open session, gmtoffset day, the fallback path end to end), M13 (short history keeps its rows) |
| `scripts/verify_deployment.py` | pin | `archive_market_series 1.1.0` |

The SQL objects are unchanged: `v_market_daily` already uses the close when a series has no adjusted close.

## Known limit (stated on the page)
On TASI the last hourly close is the price before the closing auction (15:00-15:10 Riyadh), typically within a few tenths of a percent of the official close; a one-day window on TASI carries that tolerance, a month or a year does not move materially. Rows carry `source = yahoo_chart_v8_1h`, so the page can say which series are rebuilt.

## Tests
`pytest tests/test_archive_market_series.py tests/test_bq_views.py tests/test_bq_archive.py tests/test_verify_manifest_pins.py tests/test_ci_required_verdict.py tests/test_repository_workflow_audit.py tests/test_workflow_audit_repairs.py` -> 102 passed; `audit_repository_workflows.py` errors=0 warnings=0; ruff and compileall clean.

## After merge (Claude)
Dispatch **BigQuery Market Series** with `write=true` and `symbols=^TASI.SR,000300.SS` (the other 46 series are already loaded and refresh nightly), read back the two partitions, then build the page.

## Observation for the owner (not changed here)
Any other reader of Yahoo **daily / weekly / monthly** `^TASI.SR` history gets one bar today: `run_shadow_board.py` reads the Saudi regime sleeve from `range=5y&interval=1mo` (it needs 10 months), and the scorecard's Saudi reference series reads daily bars. Both would see one bar while this upstream gap lasts; the same hourly rebuild would cover them. Not changed here (scope); flagged for the owner.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
