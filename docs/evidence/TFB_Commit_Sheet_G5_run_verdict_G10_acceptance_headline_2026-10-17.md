# TFB Commit Sheet — G5 run verdict classes (daily_sync.yml v1.0.4) + G10 acceptance headline (acceptance_check.yml)

Date: 2026-10-17 (Saturday) · Lane: GitHub Actions (two workflow files, no application code) · Brief: Codex Brief 2026-10-17 v2 §5 **G5** and **G10** (▲ adopted from the external review)

## S0/S1 — Base pin
Branch `claude/new-session-twoifp` on `4e88ae6` (L2) ← `fa42a55` (L1) ← main `11ef134`.

| File | Base | Delivered | Change |
|---|---|---|---|
| `.github/workflows/daily_sync.yml` | sha256 `7ec43279968bf51c6c77e615adb216e7f6197f0e20ee526b30d0e8fe02633618` (v1.0.3 + the L1 mirror guard) | sha256 `5aa6b9b24ed90de0b2ce1b4916fe6d40922501ea25d8daab305d6ce9eb569ee4` (v1.0.4) | **+82 / −3** (5 anchored edits, `count == 1`): header; gate job `outputs:`; summary step `id: final_summary` + `blockers=` output; verdict step classification + annotation + summary rows; Slack payload text/colour |
| `.github/workflows/acceptance_check.yml` | sha256 `ac380a0819d950579b9fd4e4f8375488234357606a3d3442fdc6a7422a63c9db` | sha256 `064548ffb9408ee0ea6f18077155c7b159c6c4cbf624782c4db2c3db5e93d0a7` | **+48 / −4**: header note (stale "v1.0.0" removed); one `if: always()` headline step between the measurement and the upload |
| `tests/test_run_verdict_classes_g5.py` | — | sha256 `1f7a0b51ce21c25223548a0c6ddfb7cb7c49221ee00092c0ab17bd54fb1f9ff5` | NEW, 15 tests |
| `tests/test_acceptance_check_headline_g10.py` | — | sha256 `f3ef2837c0a90e1fbed4980991bdb6c827a82efa1e656ec6ddeb0adc99fb7d4b` | NEW, 8 tests |
| `.github/workflows/ci.yml` | — | — | +2 lean-list lines |

## S2 — Why
**G5.** Scheduled daily_sync runs #4898/#4899/#4905 all concluded `failure` although every data job succeeded: the failing job was `🛡️ Verify final decision publication` (exit 2, `coverage_failed,decision_failed`). The `🔔 Run verdict` job classified nothing — it read only `needs.*.result` and emitted one unconditional `::error::` and one red Slack card — so a *withheld* decision feed (the gate doing its job) looked exactly like a sync that never ran. The review calls this the red-run blindness.
**G10.** `acceptance_check.yml` is a non-strict instrument by design; its green hid `28 PASS · 6 WARN · 1 FAIL · 7 NA` because the tally was the last line inside a fenced block in the step summary and nowhere else. The brief: keep it non-strict, make the tally visible without opening the artifact.

## S3 — What
**G5**
- Gate job: `outputs: audit_exit` (from the unchanged `final_audit` step — it still fails the job on any non-zero exit; the step's GITHUB_OUTPUT is still exactly `audit_exit=<code>`) and `blockers` (the summary step, now `id: final_summary`, writes `blockers=<csv>|none|audit_missing`).
- Verdict job: env `GATE_AUDIT_EXIT` / `GATE_BLOCKERS`; classification before announcing:
  | Class | Condition | Annotation | Slack |
  |---|---|---|---|
  | `DATA_FAIL` | sync-dashboard or recovery `failure`/`cancelled` | `::error::` | ❌ danger |
  | `DATA_OK / GATE_NOT_ACTIONABLE` | data jobs not failed, gate `failure`, audit exit `1` or `2` | `::warning::` naming exit + blockers, "data-quality verdict, not a run failure" | ⚠️ warning (amber) |
  | `DATA_OK / GATE_ERROR` | gate `failure` with exit 3/blank, or gate `cancelled` | `::error::` | ❌ danger |
  | `UNCLASSIFIED` | anything else that still reached the job | `::error::` | ❌ danger |
  The summary gains `Verdict class`, `gate audit exit`, `gate blockers`; the Slack card gains the same two fields. The job's own `if` (runs only on failure/cancelled) is unchanged, so a clean run still produces no card.
**G10**
- Step `Acceptance headline (tally without opening the artifact)`, `if: always()`: reads `artifacts/acceptance.json`, prints one annotation (`::notice::` PASS, `::warning::` WARN, `::error::` FAIL / unknown / missing file) `Acceptance: <verdict> - PASS=n WARN=n FAIL=n NA=n (NA_hard=n NA_design=n)[ read_errors=k] - non-strict instrument: the job stays green; FAIL means read the table.`, writes `## 📏 Acceptance: …` to the step summary and exports `verdict` / `tally`. Annotations never change the exit code → the instrument stays non-strict and the `strict` input keeps its role. No `_Status` write: `tfb_acceptance.py` is "NO WRITES, EVER" and that contract is kept; a job `name:` cannot carry a step output, so the annotation + heading is the route.

## S4 — Audits
| Check | Result |
|---|---|
| Anchored edits 5 + 2, `count == 1`; YAML parse of both files + ci.yml | PASS |
| `bash -n` on the two edited daily_sync shells | PASS |
| New tests (gate outputs pin; summary step → `blockers=` for list / empty / absent / missing-file; 9 classification cases incl. `skipped` recovery, blank exit, cancelled gate; Slack payload captured with a stub `curl` for amber / red / gate-error; headline step order + `if: always()`; FAIL → error heading; PASS/WARN/FAIL/unknown kinds; read_errors count; missing result loud but exit 0; stale `v1.0.0` gone) | **23 passed** |
| Existing pins: `test_final_publication_workflow` (audit step GITHUB_OUTPUT exactly `audit_exit=<code>`, `::error::` iff non-zero, no `::warning::`; verdict `needs` + `.result` expression), `test_approved_universe_workflow`, `test_release_decision_feed`, `test_activation_rollout_evidence_workflows`, `test_sync_audit_cancelled_source`, `test_scheduled_portfolio_refresh`, flag-mapping and tracker suites, `test_top10_kpi_label_compat` | **470 passed, 10 skipped** (pre-existing skips: the supersede golden needs `TFB_TEST_SUPERSEDE_BASE_DIR`) |
| ruff `E9,F63,F7,F82,F401,F841` | PASS |

## S5 — Delivery
`.github/workflows/daily_sync.yml` · `.github/workflows/acceptance_check.yml` · `tests/test_run_verdict_classes_g5.py` (NEW) · `tests/test_acceptance_check_headline_g10.py` (NEW) · `.github/workflows/ci.yml` (+2) · this sheet. No ENV, no Script Property, no cell.

## S6 — Read-back
- G5: next scheduled daily_sync whose gate exits 2 with the data legs green → the run page shows a **warning** annotation "the release gate withheld the decision feed - NOT_ACTIONABLE, audit exit 2, blockers: coverage_failed,decision_failed", the step summary says `Verdict class: DATA_OK / GATE_NOT_ACTIONABLE`, and the Slack card is amber. A leg that fails or is cancelled still produces the red card with `[DATA_FAIL]`. (The run conclusion itself stays `failure` while the gate job fails — that is the gate's contract; only the *telling* changed.)
- G10: next 06:45Z acceptance run → an annotation `Acceptance: FAIL - PASS=28 WARN=6 FAIL=1 NA=7 (…)` on the run page and a `## 📏 Acceptance: FAIL …` heading at the top of the headline step's summary, job still green.
- Rollback: delete the outputs block + classification lines (G5) / the headline step (G10).

## Known limits
- The gate's `audit_exit` is blank when the gate job was cancelled before the audit step ran; that reads as `GATE_ERROR`, which is the honest class.
- Whether the daily_sync run *conclusion* should stay red when only the gate withholds is a separate design question (the brief asks only to distinguish them in the verdict); making the gate job non-failing would silence the Actions red that the operator currently relies on.
