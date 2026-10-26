# Commit Sheet - `scripts/track_performance.py` v6.51.0 [MATURED-EVIDENCE] - 2026-10-26

**Scope:** an outcome matured by an audit pass counts as a valid outcome in that same run: in the summary, the calibration, the S-1 row and its MATURED receipt. ENV-gated, default OFF (`TRACK_MATURED_EVIDENCE_VALID`).

**Numbering:** 6.51.0, because open PR #841 claims 6.50.0. The workflow header is 1.0.7 because #841 claims 1.0.6. Whichever merges second renumbers.

**Files:**
- `scripts/track_performance.py` (6.49.0 -> 6.51.0)
- `.github/workflows/track_performance.yml` (1.0.5 -> 1.0.7, one env line in the clock step)
- `scripts/verify_deployment.py` (pin only)
- `tests/test_tracker_matured_evidence.py` (new harness)
- `tests/test_outcome_price_evidence.py` (guards in the blocking lean lane)
- this sheet

## S1 - Defect (evidence)

- **The loader flag.** `PerformanceRecord.from_sheet_row` sets `realized_roi_source_valid = math.isfinite(_safe_float(Realized ROI %))`. Every loaded ACTIVE row has a blank cell, so it carries `False`.
- **The transition.** `audit_active_records` has a single MATURED transition. It sets status, maturity date and `realized_roi` from a finite, witnessed ROI, but never revisits the flag.
- **Who drops the outcome.** Every same-run consumer then treats the outcome it just measured as a malformed stored outcome:

| Consumer | Effect in the maturing run |
|---|---|
| `_summary_outcome_cohort` | the summary panel, `ReliabilityCalibrator`, the JSON/HTML reports and `--simulate` count it `invalid` |
| `canonical_performance_report` | `target_achieved_at_horizon` / `forecast_error_pp` are withheld |
| `[OUTCOME-WITNESS v1]` MATURED receipt in Notes | written with both fields null. MATURED rows are never re-audited, so this is permanent. |
| `_publish_s1_calibration` (canonical checkpoint measure, `TFB_PERF_TARGET_UNIT_SENTRY=enforce`) | counted `invalid_realized_evidence` |

**Live:** Performance Clock run 40120329898 (job 120950277470, 24 Oct 18:22Z, `TRACK_SCHEDULED_AUDIT=1`) logged:
- `[v6.17.0 MATURE-FRESH-ONLY] matured-fresh=87`;
- then, in the same run, `Total: 24189 | Active: 12170 | Matured: 11773 ... Invalid matured excluded: 87`.

The next runs (25 Oct, jobs 121204298588 and 121254652958) read the same rows as `Matured: 11860 ... Invalid matured excluded: 0`. So every audit's own maturities miss that run's summary, `TFB_CALIBRATION_FACTORS` and S-1 row, all published only on audit runs, and arrive one run late.

**Receipts.** The 87 rows of 24 Oct were legacy rows with no target schema, so their receipts carry no target fields anyway. The first canonical v2 maturities (rows recorded since #827 on 24 Oct) would lose those fields for good.

**Offline reproduction** (real `audit_active_records`, stubbed transport, clock and calendar):
- After the audit: matured +4.0 WIN, flag still False.
- Receipt: `forecast_error_pp=None`, `target_achieved_at_horizon=None`.
- Analyze: `matured_records=0`, `invalid_matured_excluded=1`.
- S-1 enforce measure: `n=0`, unresolved `invalid_realized_evidence`.
- The same row reloaded on the next run is valid, with canonical error -6.0.

## S2 - Change (additive; zero removals)

| Mode | Behaviour |
|---|---|
| off (DEFAULT; unset or any other word, `'1'` included) | v6.49.0 exactly; no log line |
| enforce | a record this pass matures from its finite `_roi_use` gets `realized_roi_source_valid = True`, before the receipt is built. One INFO line `[v6.51.0 MATURED-EVIDENCE] mode=enforce matured=N revalidated=K` follows the MATURE-FRESH line. |

**Coverage of the one transition.** The fresh close, the CA-GUARD ok/adjust/ledger verdicts and the HIST-CLOSE enforce path all mature through it. A non-finite ROI stays PENDING above it.

**Never touched:**
- a stored MATURED row with a malformed cell;
- a PENDING row;
- an EXPIRED row (corp-action suspect or unpriced);
- a record built in memory this run, which already carries `True`.

**Env lane:** the clock step only. This follows the HIST-CLOSE precedent: daily_sync's tracker step never passes `--audit`, so it never matures a record.

**Additions:** `_matured_evidence_mode`, `_MATURED_EVIDENCE_TAG`. Zero top-level functions removed.

## S3 - Known limits

- Maturities from before arming keep their null receipts. The summary and calibration already count them from the next run on.
- The pre-existing malformed-stored-cell path is unchanged. A MATURED row whose cell reads e.g. `bad` loads as `realized_roi=0.0` (invalid), and an audit pass's `save_records` writes it back as `0.0`. It is out of scope here; with or without this switch it behaves as on 6.49.0.
- **The full harness runs only in the advisory whole-suite job.** Four guards (three off spellings and enforce) run in the blocking lean lane through `tests/test_outcome_price_evidence.py`. `ci.yml` is not touched: open #778/#779 claim 1.1.38/1.1.39.

## S4 - Tests

**`tests/test_tracker_matured_evidence.py`:**
- PASS 8/8 as a script; 15 passed under pytest; identical under two hash seeds.
- With no git base (`GIT_DIR=/nonexistent`, CI's depth-1 checkout) and stray tracker variables set: 7 passed, 1 skipped (M7). M4 then checks `OFF_GOLDEN = 99b4de1ea3b1bfb2`, the base digest.

| Case | What it pins |
|---|---|
| M0 | the mode parser takes the exact word `enforce` only (`'1'`, `'true'`, `'on'`, `'observe'`, `'enforced'` -> off) |
| M1 | **enforce, fresh witnessed close.** The SAME run's summary and calibrator count 2 valid / 0 invalid. Canonical 1W n=2 at -6.0, S-1 n=2, and the receipt has `forecast_error_pp=-6.0`, `target_achieved_at_horizon=False`. One line: `matured=2 revalidated=1`. An in-memory record built this run is not counted as revalidated. |
| M2 | **enforce, HIST-CLOSE enforce path:** receipt `historical_daily_close`, `forecast_error_pp=-2.0`, summary (1, 0), S-1 n=1 |
| M3 | **enforce:** same-run consumers equal the next run's reload, (3, 0) |
| M4 | **off, nine spellings:** record states, receipts, audit stats, consumers and logs equal the v6.49.0 copy, or `OFF_GOLDEN`. The v6.49.0 same-run lag (0 valid, 1 invalid) is pinned. |
| M5 | **enforce never revalidates:** a stored MATURED `bad` row, a PENDING row and a corp-action-suspect EXPIRED row stay False. The log line reads `matured=0 revalidated=0`. |
| M6 | **workflow and pins:** the clock step maps `${{ vars.TRACK_MATURED_EVIDENCE_VALID \|\| 'off' }}` once, and daily_sync does not. Header 1.0.7. SCRIPT_VERSION >= 6.51.0, in lockstep with docstring line 5 and the verify_deployment pin. |
| M7 | ASCII added lines; zero removals; on 6.51.0 exactly `_matured_evidence_mode` added (a later release may add more) |

**Golden negative:** module under test = `git show 08a15bc0:scripts/track_performance.py`.
- Result: 7 failed, 1 passed (M4, off identity).
- M1 fails on the outcome itself: summary `(0, 1)`, canonical `{}`, S-1 n=0.
- M3 fails with same-run `(0, 3)` against next-run `(3, 0)`.

**Blocking-lane guards** (`tests/test_outcome_price_evidence.py`): 81 passed. On the base tree the enforce guard fails, and the three off guards pass.

**Mutations: 12/12 caught.**

| Mutation | Caught by |
|---|---|
| assignment moved after the receipt | M1, M2 |
| ungated | M4 |
| parser accepting `'1'` | M0, M4 |
| case-sensitive parser | M0 |
| log removed | M1, M5 |
| log when off | M4 |
| every maturity counted as revalidated | M1 |
| flag set False | M1, M2, M3 |
| PENDING rows revalidated | M5 |
| corp-action-suspect rows revalidated | M5 |
| version not bumped | M6 |
| docstring not bumped | M6 |

## S5 - Operator steps (nothing is armed here)

1. Merge. No Render deploy is needed: the tracker runs in GitHub Actions.
2. Set the repository Variable `TRACK_MATURED_EVIDENCE_VALID=enforce`.
3. On the next maturity day:
   - the `[v6.51.0 MATURED-EVIDENCE]` line shows `revalidated` equal to the rows matured from loaded ACTIVE rows;
   - `Invalid matured excluded:` no longer equals `matured-fresh`;
   - v2 MATURED receipts carry `forecast_error_pp`.

**Rollback:** unset the Variable, or set it to `off`.

## S6 - Not done here

- No repair of null receipts already written.
- No change to the malformed-stored-cell path.
- No cron change, and no `ci.yml` change.
