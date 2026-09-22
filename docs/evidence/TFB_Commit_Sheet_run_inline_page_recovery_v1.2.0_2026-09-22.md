# TFB Commit Sheet — scripts/run_inline_page_recovery.py v1.2.0 [P-154b QUOTA-AWARE RECOVERY]

Date: 2026-09-22 (Riyadh) · Register item: **P-154b** (EODHD burn amplifier — the inline recovery replay) · Operator GO: "go with the best option" (2026-09-22 ~14:20 Riyadh) · Lane: GitHub Actions (`recover-missing-market-pages` job); no Render change.

## Base pin (live)

| | |
|---|---|
| `scripts/run_inline_page_recovery.py` | v1.1.0 at HEAD `main` (raw read 2026-09-22) · SHA-256 `0a7eb1b0d6685b75…` · 231 lines · byte-identical to the HEAD `b191320b` tarball |
| `scripts/audit_sync_outcome.py` (read-only dependency) | SHA-256 `cd71f67fa7de9878…` (unchanged, not delivered) |
| Existing tests kept green | `tests/test_inline_page_recovery.py` (CI job `ci-tests` runs it), `tests/test_sync_recovery_plan.py` |

## Mechanism (pinned on source + telemetry)

The `recover-missing-market-pages` job runs on **every scheduled run** (`always()`), audits the matrix legs' `sync_execution.log` (`scripts/audit_sync_outcome.py`: `[TIME-BUDGET]` batches_done < total, `[FLOOR-MERGE]` fresh % < `TFB_AUDIT_MIN_FRESH_PCT=95`, `[BATCH-RETRY]` unrecovered batches), and for every "incomplete" page launches a **full-page** `run_dashboard_sync.py --keys <PAGE>` replay, up to `TFB_INLINE_RECOVERY_MAX_CYCLES=3` times. There is no symbol-subset replay in the sync. Telemetry (P-154 sentinel, armed 09-21): each Global_Markets replay consumed **+52.7k** (08:24→09:31 today) and **+59.7k** (19:03→20:13 yesterday) EODHD calls = 13–15 % of the 400k/day allowance; Global_Markets ran twice in 5 of the last 6 windows; the counter reached **94.8 % CRIT at 00:22** with the day saved only by the GMT reset. On an exhausted day the loop is a death spiral (exhaustion → failed batches → replay → more exhaustion) and the replay cannot produce fresh rows.

## Change

Gate `TFB_INLINE_RECOVERY_QUOTA_GUARD` = off (default, **v1.1.0 byte-identical incl. the summary JSON**) | observe | enforce, explicit words only; `TFB_INLINE_RECOVERY_QUOTA_SKIP_PCT` default 90 (clamped 50..100 = the sentinel's CRIT line). Before each page replay the runner parses the LAST `[EODHD-QUOTA v…] <page> | used=U/L (P%) … | rows402 new=N … | state=S` line for that page from the same artifact logs the audit reads (cycle 1: matrix-leg logs; cycle ≥2: the previous replay's own log first).
- **skip** when state ∈ {EXHAUSTED, ON_EXTRA}, or any NEW 402 row, or P ≥ SKIP_PCT; **allow** otherwise, on UNKNOWN (no key), and when no line exists (v1.1.0 behaviour).
- **observe** → `::warning:: [RECOVERY-QUOTA v1.2.0 observe] <page> (cycle n): would SKIP replay - …` / `::notice:: … would allow …`; the replay runs; `summary.quota_guard[]` records every evaluation.
- **enforce** → the replay is skipped: result `{"skipped": "quota", "audit_status": "skipped"}`, `summary.skipped_pages`, exit code **0** with a `::warning::` (last-good rows stay on the sheet; the next scheduled window retries) — a quota-deferred page is not a failed page; genuinely missing pages still exit 2 as before.
- Fail-open: any guard exception allows the replay.

Seven anchored edits, each `count == 1`: E1 docstring WHY · E2 version · E3 helper block (6 functions) · E4 guard wiring in the cycle loop · E5 locals · E6 summary fields (only when armed) · E7 exit path.

| | |
|---|---|
| Delivered `scripts/run_inline_page_recovery.py` | v1.2.0 · SHA-256 `ed2af0025cd097bcb7346d7803586d91ce936b1d40ead03e1d145cce352b6ddd` · 397 lines (+166 / −0) |
| AST | 4 → 10 names (+6: `_quota_guard_mode`, `_quota_skip_pct`, `_quota_logs`, `_latest_quota_for_page`, `_quota_decision`, `_quota_guard`), **0 removed** · `py_compile` PASS · non-ASCII delta 0 |

## Evidence

- Parser exercised on **all 14 real `[EODHD-QUOTA]` lines** of today's `_Run_Log` (incl. 94.8 % CRIT, 17.0 %, 30.2 %) and the `used=unknown (no_key)` shape: 14/14 parsed; decision table: OK 61.1 % → allow; CRIT 94.8 % → skip; OK + rows402 new=3 → skip; EXHAUSTED → skip; UNKNOWN → allow; no line → allow.
- **Battery** `tests/test_inline_recovery_quota_guard_p154b.py` (SHA-256 `6668ec5f79c6b1cf…`, 236 lines, mirrors the repo's mocked-replay style): T1 gate + clamp · T2 off byte-identical (replay runs, summary has none of the new keys) · T3 observe never skips · T4 enforce skips CRIT (replay not launched, status ok, rc 0, `skipped:quota`) · T5 enforce allows OK · T6 cycle-2 stop from the replay's own log · T7 no-line / UNKNOWN / new-402 / WARN-vs-threshold · T8 real line shapes + last-line-wins · T9 fail-open · T10 version — **10/10 ×3**, plus the existing `test_inline_page_recovery.py` + `test_sync_recovery_plan.py` **10/10** on the delivered tree (20/20 ×3).

## Deploy

Commit three files in ONE push: `scripts/run_inline_page_recovery.py`, `tests/test_inline_recovery_quota_guard_p154b.py`, `docs/evidence/TFB_Commit_Sheet_run_inline_page_recovery_v1.2.0_2026-09-22.md`. Push runs `ci-tests` only (no provider calls). Gate off ⇒ the next scheduled recovery job behaves exactly as v1.1.0.

## Arming (GitHub lane — a separate one-line YAML sitting)

`TFB_INLINE_RECOVERY_QUOTA_GUARD: "observe"` in the `recover-missing-market-pages` job env (next to `TFB_SYNC_EODHD_QUOTA`). Read-back = the run's annotations (`[RECOVERY-QUOTA v1.2.0 observe] Global_Markets (cycle 1): would allow/SKIP …`) and `inline-recovery-<run_id>` artifact `summary.quota_guard[]`. Enforce after one clean observe day. **Policy knobs already in the YAML** (operator decision, no build): `TFB_INLINE_RECOVERY_MAX_CYCLES: "3"` → `"1"` caps the routine double-leg to ONE replay per window; `TFB_AUDIT_MIN_FRESH_PCT: "95"` is the fill contract of 2026-08-04.

## Still owed to pin the routine replays

Download today's `inline-recovery-35686538435` artifact and paste `inline-recovery-plan.json` — its `fetch_evidence` (batches_done/total, fresh_pct, unrecovered_batches) names which of the three audit triggers fires the Global_Markets replay on normal days; that decides whether the next build is a symbol-subset replay in the sync.

## Rollback

env unset (no deploy) or `git revert`.
