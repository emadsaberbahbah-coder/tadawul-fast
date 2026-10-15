# TFB Commit Sheet — scripts/backup_workbook.py v1.6.0 [--attach: DQ reports beside the backups] + .github/workflows/workbook_backup.yml v1.3.0

Date: 2026-10-15 (Thursday) · Lane: GitHub/Python — one additive upload-only mode, one gated `continue-on-error` workflow step · Protocol: One-Pass

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base commit | `main` `11ef134` |
| Base `scripts/backup_workbook.py` | sha256 `7acb91cdfe76f399…`, 1,053 lines, `__version__ = "1.5.0"`, 46 top-level functions (= the WP-0 delivery) |
| Base `.github/workflows/workbook_backup.yml` | sha256 `f3e5ec8e14138495…`, 227 lines, header v1.2.1, 9 steps |
| Base `scripts/verify_deployment.py` | sha256 `5a99bcde0edd0226…`, SCRIPT_VERSION 1.0.61, pin `backup_workbook 1.5.0` |
| Delivered `scripts/backup_workbook.py` | sha256 `8cb1bd5c926511d6…`, 1,294 lines, `__version__ = "1.6.0"`, 51 functions (+`attach_label`, `attach_name`, `attach_patterns`, `attach_refusal`, `run_attach`; 0 removed) · py_compile PASS · ruff E9/F clean |
| Delivered `.github/workflows/workbook_backup.yml` | sha256 `e6f4b4a810f0dc3d…`, 253 lines, header v1.3.0, 10 steps, `permissions: contents: read` unchanged |
| Delivered `scripts/verify_deployment.py` | sha256 `409a8796f8c714d6…`, SCRIPT_VERSION 1.0.62, pin `backup_workbook 1.6.0` (manifest pin only). If #783 merges first it also claims 1.0.62; the second to merge takes 1.0.63. |
| Delivered `tests/test_backup_workbook_attach.py` (new) | sha256 `47cefa25e5e0aba0…`, 267 lines, 9 cases |

## S2 — Root
`tfb_workbook_dq_audit.py` already measures DQ-01..DQ-75 every night inside `workbook_backup.yml` and writes `dq_scorecard.md`, `dq_report.json` and `dq_activation_receipt.json`. They reach only a 30-day GitHub artifact and the step summary, both behind a GitHub sign-in, so nobody reads them and the morning data-quality review is still done by hand from an exported workbook. The same job already writes the full workbook to the operator's **private** backup folder every night. Putting the three small files beside it makes the review automatic and readable on a phone. **Not the repository:** the repo is public and the scorecard names symbols, counts and portfolio-state checks (PF BLOCK state, ledger checks).

## S3 — Change
| # | Site | Edit |
|---|---|---|
| E1 | header | v1.6.0 WHY block; `__version__ = "1.6.0"`; USAGE line for `--attach` |
| E2 | `drive_upload_resumable`, `gcs_upload_resumable`, `_store_upload` | optional `mime` (default `_XLSX_MIME`): the workbook upload sends byte-identical requests |
| E3 | `plan_pruning` | optional `daily_re` / `monthly_re` (default the workbook patterns): workbook pruning unchanged (selftest pins it) |
| E4 | new pure helpers | `attach_label` / `attach_name` → `DAILY_TFB_DQ_Scorecard_<YYYY-MM-DD>.md`, `DAILY_TFB_DQ_Report_<date>.json`, `DAILY_TFB_DQ_Activation_Receipt_<date>.json` (Riyadh date, the `daily_name` convention); `attach_patterns` (one pattern pair per report kind for `plan_pruning`); `attach_refusal` |
| E5 | `run_attach(paths)` | **upload-only** (no export, no workbook upload, no monthly copy). Destination = the `run_restore_test` composition of the existing helpers: `_backup_dest()`, `_drive_id(TFB_BACKUP_FOLDER_ID)`, `_gcs_bucket()`, `_credentials()` → `_session()`, and for `oauth` `_oauth_session()` + `drive_ensure_app_folder()`. No second credential path. A same-name file from an earlier run that day is replaced; each kind is pruned to `TFB_BACKUP_KEEP_DAILY` (default 30) via `plan_pruning`; workbook files and foreign files are never touched. |
| E6 | guards | missing PATH → `NOTE … not found — skipped`, never fatal; allowed extensions `.md .json .txt` only; refused: zip/xlsx payload (`PK\x03\x04`), the `TFB_BACKUP_LOCAL_COPY` export, the `GOOGLE_APPLICATION_CREDENTIALS` file, content containing `"private_key"` / `"client_secret"` / `"refresh_token"` / `-----BEGIN `, files over 20 MB |
| E7 | exit codes / log | 0 = done (also when nothing exists, before any credential is touched); 2 = setup failure or a refused path; 3 = an upload failed. One `_Run_Log` row `[BACKUP-ATTACH v1.6.0] uploaded=… skipped=… refused=… failed=… pruned=…`. The prefix never matches `[BACKUP v` or the `[RESTORE-TEST … PASS` drill marker read by `run_shadow_scorer`. A setup exception is caught under this tag, never logged as `[BACKUP v…] FAIL unhandled`. |
| E8 | `main()` | `--attach PATH [PATH ...]` in the existing mutually exclusive group (with `--dry-run`, `--restore-test`, `--selftest`) |
| E9 | selftest | +4 checks (names, per-kind pruning, default patterns unchanged, refusals) → 11/11 |
| W1 | `workbook_backup.yml` step "Publish DQ report to Drive" | after "Upload DQ report", before "Remove Google credentials" (credentials still materialised). `if: always() && mode in (observe, enforce) && restore_test != 'true' && dry_run != 'true'`; `continue-on-error: true`; env = the "Run backup" step's env (so `--attach` can refuse the export path); runs `--attach dq_scorecard.md dq_report.json dq_activation_receipt.json`. The artifact upload is unchanged. |

Out of scope and untouched: the DQ audit step, its flags, baseline and exit semantics; the artifact upload; `run_backup`, `run_restore_test` and all retention logic for the workbook; `permissions`.

## S4 — Audits
| Check | Result |
|---|---|
| `python scripts/backup_workbook.py --selftest` | **11/11 PASS** (7 unchanged + 4 new) |
| **Without `--attach`, before vs after** (same in-memory Drive fakes; base = `origin/main` file) | `--dry-run`: exit 0/0, stdout equal (empty), stderr equal modulo the version string, Drive state equal, `_Run_Log` rows equal, HTTP call sequence equal. Full backup: the same, all equal, including the xlsx MIME on the upload and the monthly copy. Real CLI with no env: exit 2/2 (`DEFAULT_SPREADSHEET_ID not set`); with an id but no Google libs: exit 4/4. stdout empty in every case. |
| `--attach` on two temp files + a missing third (`test_two_reports_land_dated_and_a_missing_one_is_skipped`) | exit 0; Drive folder = `DAILY_TFB_DQ_Report_2026-10-15.json` (application/json) + `DAILY_TFB_DQ_Scorecard_2026-10-15.md` (text/markdown); `NOTE: attach: …/dq_activation_receipt.json not found — skipped`; no export request, no workbook file; one OK `[BACKUP-ATTACH v1.6.0]` row |
| Same-day re-run + pruning | the re-run leaves exactly one file per name; 31 old reports + today → 30 kept; workbook dailies and `operator_notes.txt` untouched; `TFB_BACKUP_KEEP_DAILY=5` → 5 |
| Refusals | `.xlsx`, a `PK`-payload `.json`, the credentials file, a refresh-token JSON → refused (exit 2); the valid `.md` is still uploaded |
| OAuth destination | files land in the app's own `TFB Workbook Backups` folder, not `TFB_BACKUP_FOLDER_ID` |
| Upload failure / setup failure | exit 3 / exit 2 with `[BACKUP-ATTACH v1.6.0] FAIL credentials setup …` |
| Dry-run dispatch | the step's `if:` evaluates false for `dry_run=true`, `restore_test=true` and mode `off`; true for scheduled observe/enforce (`test_workflow_publishes_after_the_artifact_and_before_cleanup`) |
| Neighbour suites | `test_workbook_dq_audit.py` (P11 re-pinned to 1.6.0), `test_workflow_activation_observe.py`, `test_verify_manifest_pins.py`, workflow audits, every test naming these files: 207 passed. The only 2 failures (`test_auth_wall_scope_path` W8/W15) fail identically on `main` in this container. `audit_repository_workflows.py` errors=0. DQ selftest 97/97. |

Not exercised here: a real Google Drive upload (no credentials in this container). The first scheduled run after merge is the live proof; see S6.

## S5 — Delivery
| File | Destination |
|---|---|
| `scripts/backup_workbook.py` | repo |
| `.github/workflows/workbook_backup.yml` | repo |
| `scripts/verify_deployment.py` | repo (manifest pin) |
| `tests/test_backup_workbook_attach.py`, `tests/test_workbook_dq_audit.py` | repo |
| this sheet | `docs/evidence/` |

## S6 — Read-back (operator)
1. After the next scheduled run (~02:30 Riyadh), open the backup folder. For `TFB_BACKUP_DEST=oauth` that is the app folder `TFB Workbook Backups` (or `TFB_BACKUP_DRIVE_FOLDER_NAME`); otherwise it is `TFB_BACKUP_FOLDER_ID`. Expect `DAILY_TFB_DQ_Scorecard_<date>.md`, `DAILY_TFB_DQ_Report_<date>.json` and `DAILY_TFB_DQ_Activation_Receipt_<date>.json` beside `DAILY_TFB_Workbook_<date>.xlsx`.
2. In the run log, the step "Publish DQ report to Drive" ends with `[BACKUP-ATTACH v1.6.0] uploaded=3/3 …`, and `_Run_Log` has the same row.
3. A failure shows as an amber step plus a FAIL row; the backup stays green.

Kill: set `TFB_DQ_AUDIT_MODE=off`, which also skips the audit, or revert the workflow step. Rollback: revert the commit.
