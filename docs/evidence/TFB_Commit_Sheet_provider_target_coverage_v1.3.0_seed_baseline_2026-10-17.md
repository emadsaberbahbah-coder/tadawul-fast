# TFB Commit Sheet — provider_target_coverage seed baseline (G9 / P-206)

**Date:** 2026-10-17 · **Brief item:** Codex Brief v2 §G9 ("provider_target_coverage.yml has failed on every scheduled run since at least 14 Oct with CH_BASELINE_EMPTY / CH_BASELINE_MISSING … publish the baseline … and make the control pass") · **Branch:** `claude/new-session-twoifp` · **Base:** main `11ef134` · **Previous commit on branch:** `644f9cd`

## 1. Diagnosis (from run 39026626201 job log, 2026-10-17 10:54Z)

| Step | Observed |
|---|---|
| `actions/cache` restore | restores `provider_target_last_good.json`, 341 bytes, `generated_at_utc` 2026-08-18, `pages: {}` |
| `validate_baseline` | `CH_BASELINE_EMPTY` → exit 2 (control-health failure) |
| `save_last_good` | `kept: control-health failure` → nothing written |
| `actions/cache` save | re-saves the same empty file → identical failure the next morning |

Deadlock: the control can only recover by a manual `--bootstrap` dispatch that nobody ran (runs #66–#73, 13–17 Oct, all red). Live per-page coverage measured by that same run was healthy (UPSTREAM_READY, policy_cov 96.7%).

## 2. What ships

| File | Change |
|---|---|
| `scripts/audit_provider_target_coverage.py` | v1.2.0 → **v1.3.0**. `apply_seed(state, seed, bootstrap, accept)` substitutes a published seed **only** when the restored reference is `None`, has no `pages`, or `pages == {}`; never on `--bootstrap` / `--accept`. `validate_baseline(..., seeded=False)` adds WARN `PTC_BASELINE_SEEDED` and skips the staleness clock for a seed (seed age is not a control); schema / sheet / config-hash mismatches on a seed still fail closed (exit 2). `CoverageReport.baseline_source` ∈ {`none`,`cache`,`seed`}. `save_last_good` ignores the seed's own WARN marker when deciding `renewable`, and never inherits the replaced file's timestamps when the source is `seed`, so the first healthy seeded run writes the first LIVE baseline. `--seed PATH` CLI; unreadable seed with a missing reference → WARN `PTC_SEED_UNAVAILABLE` alongside the existing `CH_BASELINE_MISSING`. Console line prints `baseline_source`. Selftest 16 → **22/22**. |
| `config/provider_target_baseline_seed.json` | New. Schema 2, sheet `1QaOS...wsG04`, `config_hash 9a644ee2170c` (= `Config().hash()` at HEAD), pages ML 34.12 % / 255 rows, GM 51.78 % / 6609, CFX 0 / 453, MF 0 / 2474, `seed.source_run_id 39026626201`. Reviewed in the PR; updated by PR, never on the runner. |
| `.github/workflows/provider_target_coverage.yml` | Header note v1.2.0 (WHY/WHAT/rollback); audit step gets `--seed config/provider_target_baseline_seed.json`; seed + new test added to `push`/`pull_request` paths. |
| `tests/test_ptc_seed_baseline_g9.py` | New (12 tests + parametrisations): apply_seed rules, seeded WARN, mismatch fails closed, seed age ≠ staleness, first-run persist writes live baseline, second run `baseline_source=cache` with no findings, unreadable seed, `--seed` absent = v1.2.0 behaviour byte-for-byte, workflow wiring. |
| `.github/workflows/ci.yml` | Lean list += `tests/test_ptc_seed_baseline_g9.py`. |

## 3. Protocol evidence

- **Anchored edits:** 13 edits via `scratchpad/edit_g9.py`, every anchor `count == 1`; + 1 follow-up edit (`renewable` health filter), `count == 1`.
- **Zero-removal proof:** `git diff` shows 11 removed lines, each one a rewritten signature / docstring / version / print line whose replacement is a superset (`VERSION`, `validate_baseline` signature, `audit_pages` signature+docstring, `run_live` signature+call, `renewable` expression, `asyncio.run(run_live(...))` call, console print). No function, finding code, exit path or CLI flag removed.
- **Dark by default:** without `--seed` the script is behaviour-identical to v1.2.0 (test `rc4`: `baseline_source == "none"`, exit 2 on missing reference). The workflow passes `--seed`, which is the one place the behaviour is armed; **kill switch:** remove that line (control fails closed again). No ENV / Script Property / workbook cell touched.
- **Harness:** `python scripts/audit_provider_target_coverage.py --selftest` → `[PTC v1.3.0] selftest PASS 22/22`; `pytest tests/test_ptc_seed_baseline_g9.py tests/test_workflow_audit_repairs.py tests/test_approved_universe_workflow.py` → 56 passed; `ruff --select E9,F63,F7,F82,F401,F841` clean; both YAML files parse.

## 4. Expected first scheduled run (06:20Z 18 Oct)

1. Cache restores the empty 18-Aug file → `apply_seed` substitutes the seed → `baseline_source=seed`, WARN `PTC_BASELINE_SEEDED`, pages compared against the seed → exit 0 (WARN) unless a page truly declined.
2. `save_last_good` → `written` (live baseline, `last_verified_at_utc` set).
3. Cache save step stores the live file → run 2 shows `baseline_source=cache`, no seed finding. If the seed stays in effect for more than two runs, the cache save step is the suspect, not the audit.

## 5. Deviations / notes for Emad (§8)

- None to arm: the seed is wired in the workflow by this PR. If the config (`min_rows`, thresholds) changes, `config_hash` in the seed must be regenerated (`Config().hash()`) or the seed fails closed with `CH_BASELINE_POLICY_MISMATCH` by design.
- CFX / MF at 0 % are STRUCTURAL_ZERO pages today; the seed records 0.0 so a future non-zero provider share is a rise, never a false decline.
