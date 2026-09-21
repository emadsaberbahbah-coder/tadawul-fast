# TFB Commit Sheet — scripts/track_performance.py v6.39.0 [TARGET-UNIT SENTRY]

**Date:** 2026-09-21 · **Register item:** P-158 · **Protocol:** One-Pass (S0–S5 complete; S6 = first observe cycle)
**Origin:** red-team 09-21 part 2 specimen (ETO.US) → mechanism pinned at HEAD by Claude → Emad's GO ("let's start script").

## 1. Pins

| | Version | SHA-256 | Lines |
|---|---|---|---|
| Base (live-fetched) | 6.38.0 | `b04baa763c8ca47306f8b6d813f016ef1975b17960fe5b038151cb97182d7077` | 8,693 |
| Delivered | 6.39.0 | `7128eb413a3b54438eea832b1949054dd1476d9ad77bb6b30e2c0c7812280dbe` | 9,133 |
| Test | — | `dc399150cfc98fc6be080a3a97f5209810723aacd4f64f2aecb868ec28cb1d77` | 385 |

Base fetched at branch `main` AND at commit `3f1efe193f6a6a0eed4639dd52f0e102ded3b11a` — byte-identical (zero drift). LF, UTF-8, 0 CR.

## 2. Defect (pinned in source, not inferred)

- `_derive_target` / `_derive_checkpoint_target` copy the engine's `expected_roi_*` **raw**. The engine contract is a **fraction** (sheet adjudication 2026-09-16). No `* 100` exists anywhere in the file for that value.
- Everything the file computes itself is **percent points** (`(c1/c0 - 1) * 100`), and the criterion-4 measurement computes `err = realized - target` under the comment "both fields are already percentages".
- Effect: engine-supplied targets enter criterion 4 at ~1/100 of their size. The published mean |err| is essentially mean |realized return| (09-21: 1W 2.60 / 2W 3.72 — ratio 1.43 ≈ √2, the realized-volatility scaling); the "signed" figure is the mean realized return, not forecast bias. Checkpoint target prices are derived as `entry * (1 + fraction/100)` ≈ entry.
- Live specimen: ETO.US entry 30.16, target price 30.8654, stored Target ROI 0.023388 (price implies +2.3389 pp). Reproduced through the real derive methods (harness T1): 1W checkpoint → target ROI 0.0054572, target price 30.161646.
- Not affected: WIN/LOSS outcomes (sign of realized ROI only).

## 3. Change — 8 anchored edits, each asserted `count == 1`

| Edit | Site | Change |
|---|---|---|
| E1 | `SCRIPT_VERSION` | 6.38.0 → 6.39.0 + WHY block |
| E2 | before the criterion-4 function | sentry helpers; the v6.29.0 function renamed `_s1_checkpoint_calibration_legacy` (body AST-identical to base) |
| E3 | before `class PerformanceStatus` | public `s1_checkpoint_calibration` wrapper (same name, same signature) |
| E4 | `_publish_s1_calibration` | when a sentry report exists: block `A4:D9` + one `[PERF-UNIT]` `_Run_Log` line (best-effort, never fails the publish) |
| E5a | `_derive_checkpoint_target` | creation seam `_perf_unit_creation(...)` |
| E5b | `_derive_target` | creation seam `_perf_unit_creation(...)` |
| E6a/b | `_track_selftest_` | 12 → 14 cases (unit truth table; gate parsing + OFF identity) |

AST: 250 → 262 unique def/class names, **0 removed**, +12 (`_perf_unit_mode`, `_perf_unit_pick`, `_perf_unit_creation`, `_perf_unit_day_key`, `_perf_unit_sibling_index`, `_s1_unit_sentry_measure`, `_s1_unit_sentry_apply`, `_perf_unit_block_rows`, `_perf_unit_log_line`, `_s1_checkpoint_calibration_legacy`, nested `_v`, `_f`). 3 lines changed, 443 added. Added lines: 0 non-ASCII, 0 smart quotes, 0 tabs. `py_compile` PASS on base and delivered.

## 4. Gate — `TFB_PERF_TARGET_UNIT_SENTRY` = off | observe | enforce

Explicit words only (no boolean alias); anything else = off. **ENV lane: GitHub Actions (`daily_sync.yml`, the track_performance step) — not Render.**

| Mode | Record creation | Criterion-4 row (`_S1_Calibration` rows 1–2) | Extra output |
|---|---|---|---|
| off (default) | identical to v6.38.0 | identical to v6.38.0 | none |
| observe | values identical; verdicts counted | identical (legacy basis) | block `A4:D9` + `[PERF-UNIT v6.39.0]` `_Run_Log` line: legacy vs unit-corrected error, fraction / percent / unresolved counts, what creation would have scaled |
| enforce | confirmed fractions stored as percent points (checkpoint target price becomes correct) | computed on the unit-corrected basis; Detail carries `[unit-sentry enforce: …]` incl. the legacy figure | same block + line |

**Ground truth, never magnitude.** The unit is decided only against a price-implied return: at creation, the engine's forecast price vs entry; for a stored checkpoint, the same-day 1M sibling (`SYMBOL|1M|YYYYMMDD`) whose target price is the engine's 1M forecast — truth = implied_1m_pp × days / 30. The closer reading wins only inside max(0.05 pp, 10 % of truth); otherwise the row is **unresolved**: excluded from the corrected sample and counted, never scaled. Stored legacy rows are **never rewritten** — correction happens at measurement time, so rollback is one env line. Enforce fails open to the legacy basis (with a published note) if nothing is resolvable or on any error.

OFF differences vs v6.38.0 (telemetry strings only): version `6.39.0` wherever `SCRIPT_VERSION` prints (incl. `_S1_Calibration!J2`), and `selftest=PASS 14/14` in the `[PERF-VERDICT]` line.

## 5. Evidence — real-module harness, ×3 identical

`tests/test_tp_target_unit_sentry_p158.py` executes the real `PerformanceRecord`, enums, `PerformanceTrackerApp._derive_target` / `_derive_checkpoint_target` / `_publish_s1_calibration` / `_track_selftest_` and `s1_checkpoint_calibration`. The only double is the Sheets I/O recorder.

| Test | Result |
|---|---|
| T1 golden-negative | live specimen reproduces the defect through the real methods, on base and delivered (gate off) |
| T2 OFF identity (dual-tree) | 2,945 records identical base vs delivered; calibration dict deep-equal; no report key; counters zero |
| T3 observe | headline identical; labels partition the legacy sample (749 fraction / 40 percent / 72 unresolved = 861); corrected mean error equals an independent recomputation (legacy 2.42 → corrected 2.64 pp; signed −0.12 → −0.78 pp on the fixture corpus) |
| T4 enforce (measurement) | headline = corrected; Detail discloses; legacy kept under `unit_sentry.legacy` |
| T5 enforce (creation) | ETO 1M → 2.3388 pp; 1W → 0.54572 pp, target price 30.324589; re-measured rows read `percent` — **0 double-scaling** |
| T6 fail-open | nothing resolvable → legacy basis kept + note; junk records never raise |
| T7 shapes | block 6×4; one-line log |
| T8 publisher wiring | off = 1 write (A1 only), 0 log lines; observe = A1 row identical + `A4:D9` + 1 `_Run_Log` line |
| T9 self-test | delivered `PASS 14/14`; base `PASS 12/12` |

Digest dual-tree `fbc42ce9eca991ba` ×3 · single-tree `f8897540f4c9e17b`.
Not run: the repo's other track_performance tests could not be enumerated (GitHub tree API rate-limited this session) — the CI run on the commit is the check.

## 6. Deploy

Commit three files (no Render action — the script runs in GitHub Actions from HEAD):

- `scripts/track_performance.py`
- `tests/test_tp_target_unit_sentry_p158.py`
- `docs/evidence/TFB_Commit_Sheet_track_performance_v6.39.0_2026-09-21.md`

Deploy read-back (gate still off): next `_Run_Log` `[PERF-VERDICT v6.39.0] … selftest=PASS 14/14`; `_S1_Calibration!J2` = 6.39.0; criterion-4 row otherwise as before.

## 7. Arming — separate sitting, one ENV per evidence run

In `.github/workflows/daily_sync.yml`, track_performance step `env:`, directly after `TRACK_HORIZONS: "1W,2W,1M,3M"`:

```yaml
          TFB_PERF_TARGET_UNIT_SENTRY: "observe"
```

Observe read-back = `_S1_Calibration!A4:D9` + the `[PERF-UNIT v6.39.0] mode=observe` line. Read: (1) fraction vs percent counts — confirms or refutes P-158 on the full corpus (this replaces the 10-row tab check); (2) corrected mean |err| and signed bias; (3) unresolved share and `percent (tiny N)` — the blind-spot tell.

Enforce prerequisites: ≥3 observe runs with stable counts; unresolved share reviewed; **evidence-register boundary note** — "criterion-4 measurement basis corrected for target units on <date>; gate criteria, band, min-sample, benchmark and counter byte-untouched; prior PASS values were computed on mixed units".

## 8. Rollback

Remove the env line (→ off, v6.38.0 behaviour) or `git revert`. No stored data is modified in any mode; the last `A4:D9` block stays on the tab with its own as_of stamp.

## 9. Deliberate cuts

- No migration of stored Performance_Log targets.
- 1M/3M/1Y stored targets are not corrected for other analytics (nothing else consumes `target_roi` today).
- Tolerances are constants, not env-tunable.
- Blind spot disclosed, not solved: a 1M sibling whose target price was itself derived from the ROI is self-consistent in the percent reading.
- No workflow YAML delivered in this commit (arming is its own sitting).
