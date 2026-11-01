# Commit Sheet - `scripts/track_performance.py` v6.51.0 [MATURED-EVIDENCE] - 2026-10-26

**Scope:** an outcome matured by an audit pass counts as a valid outcome in that same run: in the summary, the calibration, the S-1 row and its MATURED receipt. ENV-gated, default OFF (`TRACK_MATURED_EVIDENCE_VALID`).

**Numbering:** 6.51.0, because open PR #841 claims 6.50.0. The workflow header is 1.0.7 because #841 claims 1.0.6. Whichever merges second renumbers.

**Files:**
- `scripts/track_performance.py` (6.49.0 -> 6.51.0)
- `.github/workflows/track_performance.yml` (1.0.5 -> 1.0.7, one env line in the clock step)
- `.github/workflows/daily_sync.yml` (one env line in the tracker step; header unchanged, as in #827)
- `tests/test_workflow_flag_mappings.py` (tracker default `off`)
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

The next tracker runs read the same rows as `Matured: 11860 ... Invalid matured excluded: 0`. They were the Daily Sync record-only steps (job 121006724938, 24 Oct 23:29Z, and job 121127954714, 25 Oct 10:09Z, no `--audit`), then the next clock audit (job 121204298588, 25 Oct 15:03Z).

So every audit's own maturities:
- miss that run's summary panel, which the next `--analyze` run rewrites (Daily Sync's included);
- miss that run's `TFB_CALIBRATION_FACTORS` and S-1 row, which are published only on audit runs (the S-1 row inside `if self.args.audit`; the factors because both workflows export `TRACK_PUBLISH_CALIBRATION=0` off-audit), and reach those one audit run late.

**What the live run actually lost.** The 87 rows of 24 Oct were legacy rows with no target schema. For them the live effect was on the summary and the calibration inputs. Legacy rows never reach the S-1 canonical measure (S-1 logged the same `PENDING` line before and after), and their receipts carry no target fields anyway. The S-1 and receipt losses are prospective: the first canonical v2 maturities (rows recorded since #827 on 24 Oct) would be dropped from that run's S-1 measure, and their receipts would lose the target fields for good.

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

**Env lane:** both tracker steps map the Variable (ENV PARITY):
- the clock's `Run performance clock` step, on every scheduled audit;
- Daily Sync's `Track Performance (record + audit)` step, which passes `--audit` on a manual dispatch with `run_tracker=true` and so matures records too. Its scheduled runs never audit, so there the Variable is inert.

`tests/test_workflow_flag_mappings.py` now lists the tracker default `off`, so the two lanes cannot drift apart.

**Additions:** `_matured_evidence_mode`, `_MATURED_EVIDENCE_TAG`. Zero top-level functions removed.

## S3 - Known limits

- Maturities from before arming keep their null receipts. The summary and calibration already count them from the next run on.
- The pre-existing malformed-stored-cell path is unchanged. A MATURED row whose cell reads e.g. `bad` loads as `realized_roi=0.0` (invalid), and an audit pass's `save_records` writes it back as `0.0`. It is out of scope here; with or without this switch it behaves as on 6.49.0.
- **A failed save.** `run_once` ignores `save_records`' return value. After a failed save, every same-run published surface runs ahead of Performance_Log in either mode, as it already did on v6.49.0. Checking that result is a separate follow-up.
- **The full harness runs only in the advisory whole-suite job.** Five guards run in the blocking lean lane through `tests/test_outcome_price_evidence.py`: three off spellings, enforce on a canonical row, and enforce on a legacy row. The mapping parity runs there through `tests/test_workflow_flag_mappings.py`. `ci.yml` is not touched: open #778/#779 claim 1.1.38/1.1.39.

## S4 - Tests

**`tests/test_tracker_matured_evidence.py`:**
- PASS 10/10 as a script; 18 passed under pytest; identical under three hash seeds.
- With no git base (`GIT_DIR=/nonexistent`, CI's depth-1 checkout) and stray tracker variables set: 17 passed, 1 skipped (M7). M4 then checks `OFF_GOLDEN = 99b4de1ea3b1bfb2`, the base digest.
- The harness snapshots the environment and module attributes before its first change and makes every change inside its `try`, so a failing setup still restores the caller's state. The base copy's temporary directory is removed at exit.

| Case | What it pins |
|---|---|
| M0 | the mode parser takes the exact word `enforce` only (`'1'`, `'true'`, `'on'`, `'observe'`, `'enforced'` -> off) |
| M1 | **enforce, fresh witnessed close.** The SAME run's summary and calibrator count 2 valid / 0 invalid. Canonical 1W n=2 at -6.0, S-1 n=2, and the receipt has `forecast_error_pp=-6.0`, `target_achieved_at_horizon=False`. One INFO line, `matured=2 revalidated=1`, right after the MATURE-FRESH line. An in-memory record built this run is not counted as revalidated. |
| M2 | **enforce, HIST-CLOSE enforce path:** receipt `historical_daily_close`, `forecast_error_pp=-2.0`, summary (1, 0), S-1 n=1 |
| M3 | **enforce:** same-run consumers equal the next run's reload, (3, 0) |
| M4 | **off, nine spellings:** record states, receipts, audit stats, consumers and logs equal the v6.49.0 copy, or `OFF_GOLDEN`. The v6.49.0 same-run lag (0 valid, 1 invalid) is pinned. |
| M5 | **enforce never revalidates:** a stored MATURED `bad` row, a PENDING row and a corp-action-suspect EXPIRED row stay False. The log line reads `matured=0 revalidated=0`. |
| M6 | **workflow and pins:** each tracker step (the clock, and Daily Sync's, whose manual-dispatch `--audit` path is pinned too) maps `${{ vars.TRACK_MATURED_EVIDENCE_VALID \|\| 'off' }}` exactly once. Header 1.0.7. SCRIPT_VERSION >= 6.51.0, in lockstep with docstring line 5 and the verify_deployment pin. |
| M7 | ASCII added lines; zero removals; on 6.51.0 exactly `_matured_evidence_mode` added (a later release may add more) |
| M8 | **enforce, legacy row** (no target schema, the live 24 Oct population): summary and calibrator (1, 0), flag True, `revalidated=1`, canonical `{}` and S-1 n=0, no target fields in the receipt. Off keeps (0, 1). |
| M9 | **enforce, CA-GUARD `adjust` and `ledger`:** receipts `roi_basis` adjust / ledger, ROI 5.0 / 0.0, summary (2, 0), both flags True, `forecast_error_pp` -5.0 / -10.0, S-1 n=2, `revalidated=2` |

**Golden negative:** module under test = `git show 08a15bc0:scripts/track_performance.py`.
- Result: 9 failed, 1 passed (M4, off identity) as a script; 9 failed, 9 passed under pytest (M4's nine spellings).
- M1 fails on the outcome itself: summary `(1, 1, ...)` (the in-memory row counts, the loaded one does not), canonical 1W n=1, S-1 n=1.
- M2 fails with summary `(0, 1)`, canonical `{}`, S-1 n=0. M8 fails with `(0, 1)`, and M9 with `(0, 2)`.
- M3 fails with same-run `(0, 3)` against next-run `(3, 0)`.

**Blocking-lane guards** (`tests/test_outcome_price_evidence.py`): 82 passed. On the base tree the two enforce guards fail, and the three off guards pass.

**Mutations: 16/16 caught.**

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
| revalidation limited to the CA-GUARD `ok` verdict | M9 |
| revalidation limited to rows with a target schema | M8 (and the lean legacy guard) |
| log line moved above the MATURE-FRESH line | M1 |
| log line at WARNING | M1 |

## S5 - Operator steps (nothing is armed here)

1. Merge. No Render deploy is needed: the tracker runs in GitHub Actions.
2. Set the repository Variable `TRACK_MATURED_EVIDENCE_VALID=enforce`. Both tracker lanes read it.
3. On the next maturity day:
   - the `[v6.51.0 MATURED-EVIDENCE]` line shows `revalidated` equal to the rows matured from loaded ACTIVE rows;
   - `Invalid matured excluded:` no longer equals `matured-fresh`;
   - v2 MATURED receipts carry `forecast_error_pp`.

**Rollback:** unset the Variable, or set it to `off`.

## S6 - Not done here

- No repair of null receipts already written.
- No change to the malformed-stored-cell path.
- No cron change, and no `ci.yml` change.
- No change to `run_once`'s handling of a failed save (see S3).

## Merge note (2026-11-01)

#841 (track_performance v6.50.0 [LOG-READ]) merged first. This branch was merged onto main `bf1510ed` with a merge commit: the v6.51.0 WHY block sits above #841's v6.50.0 block (kept verbatim), `SCRIPT_VERSION = "6.51.0"`, the verifier pin is 6.51.0 with both dated comments, and the workflow headers read 1.0.7 above 1.0.6. Both Variables (`TRACK_MATURED_EVIDENCE_VALID`, `TRACK_LOG_UNFORMATTED_READ`) are mapped in both tracker steps. The harness base moved from `08a15bc0` to `bf1510ed`, so M7's exact additions set is again `_matured_evidence_mode` only. The golden-negative numbers above were measured against `08a15bc0` before the merge.
