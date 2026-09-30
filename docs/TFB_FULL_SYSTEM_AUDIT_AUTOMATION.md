# TFB Full-System Audit Automation Pack

**Version:** 1.0.0  
**Date:** 19 August 2026  
**Purpose:** Create one reviewable evidence chain across GitHub, Render, Google Sheets, and the bound Google Apps Script project without changing production decisions.

## What this branch changes

1. `scripts/full_system_audit.py`
   - Runs an offline GitHub/repository audit without secrets.
   - Calls the existing `scripts/audit_repository_workflows.py` rather than duplicating it.
   - Calls the existing `scripts/verify_deployment.py --json --strict` when run inside Render or a fully configured environment.
   - Calls the existing `scripts/validate_dashboard.py` when the spreadsheet ID and service-account credentials are present.
   - Probes `/readyz`, `/health`, `/healthz`, and `/version` when a Render base URL is supplied.
   - Redacts secret-bearing keys and common token formats before writing artifacts.
   - Produces `full_system_audit.json` and `full_system_audit.md` with a fail-closed technical verdict.

2. `.github/workflows/full_system_audit.yml`
   - Runs source-only checks on relevant pull requests.
   - Provides a manual `repo` mode requiring no secrets.
   - Provides a manual `all` mode that can include Render and Sheets evidence when repository secrets are configured.
   - Uploads one immutable artifact bundle named with the audited commit SHA.

3. `apps_script/98_System_Audit.gs`
   - Is read-only by default and creates no trigger.
   - Scans persisted decision surfaces for the W1A conflict classes.
   - Compares key portfolio identities/actions with Global Markets.
   - Provides a deterministic critical-field SHA-256 hash for post-write certification.
   - Allows optional write-back only to `_Full_System_Audit` and only when Script Property `TFB_AUDIT_ALLOW_WRITE=1` is explicitly set.

4. `Procfile`
   - Corrects documentation only: this repository has no Render Blueprint, the web service is dashboard-managed, and a `worker:` line in Procfile does not create a Render worker service.

## Safety boundaries

- No production Sheet is modified by the Python orchestrator.
- No secret value is printed or accepted as a workflow input.
- No automatic merge is requested.
- The Apps Script audit is inert until copied/deployed into the bound project and called explicitly.
- The optional audit-tab write is disabled by default.
- This pack does not alter scoring, selection, portfolio quantities, provider logic, or investment recommendations.

## Required repository secrets for manual `all` mode

| Secret | Purpose | Required? |
|---|---|---|
| `TFB_RENDER_BASE_URL` | Public/internal Render service base URL | Optional but required for endpoint probing |
| `TFB_AUDIT_TOKEN` | Bearer token only when the health endpoints require it | Optional |
| `GOOGLE_SHEETS_CREDENTIALS` | Existing service-account JSON/base64 used by the project | Required for live Sheets validation |
| `TFB_SPREADSHEET_ID` | Production spreadsheet ID | Required for live Sheets validation |

Do not put API keys, service-account JSON, or bearer tokens in `workflow_dispatch` inputs.

## Recommended execution sequence

### Phase 1 - GitHub source certification

```bash
python -m unittest tests/test_full_system_audit.py
python scripts/full_system_audit.py --mode repo --strict
```

Acceptance:
- no missing critical files;
- canonical start command aligned;
- no stale Render Blueprint claim;
- workflow audit passes;
- Python compilation passes;
- Apps Script parity remains explicitly marked unverified until the full bound project is exported.

### Phase 2 - Render evidence

Run in a Render shell or one-off job for the exact deployed release:

```bash
python scripts/full_system_audit.py \
  --mode runtime \
  --render-url "$TFB_RENDER_BASE_URL" \
  --strict
```

Acceptance:
- `verify_deployment.py --strict` is CLEAN;
- `/readyz` is successful;
- the evidence bundle records `RENDER_GIT_COMMIT` and the critical file hashes;
- effective W1A/OHLC/fingerprint guard states are visible;
- web and worker roles are separately evidenced.

### Phase 3 - Google Sheets persisted-output certification

Run the manual workflow with `mode=all` after the full page cohort finishes:

```bash
python scripts/full_system_audit.py --mode sheets --strict
```

Acceptance:
- literal persisted-sheet validation returns no contract or gate failure;
- every contributing page is SUCCESS for the same cohort;
- Top-10 does not call a partial universe “full”;
- post-write critical-field hash equals the writer’s expected hash;
- no W1A persisted conflict remains.

### Phase 4 - Bound Apps Script parity

1. Export the complete bound Apps Script project using clasp or the Apps Script API.
2. Compare file hashes with `apps_script/` in GitHub.
3. Add `98_System_Audit.gs` to the bound project.
4. Run `TFB_AUDIT_runSystemAudit()` in read-only mode.
5. Integrate `TFB_AUDIT_certifyPostWrite()` into the final page-write seam.
6. Only after a successful observe period, consider enabling the dedicated audit-tab write.

## Technical verdict meanings

| Verdict | Meaning |
|---|---|
| `NO_GO` | A technical failure exists in the audited chain. |
| `CONDITIONAL_NO_GO` | Warnings or critical evidence gaps remain. |
| `REPO_CLEAN_PRODUCTION_UNVERIFIED` | Source tree is clean, but Render/Sheets were not certified. |
| `TECHNICAL_GO_FOR_SHADOW_ONLY` | All requested technical evidence passed; still not an investment instruction. |

## Claude review checklist

Claude should review this branch for:

1. Whether the orchestrator calls the existing validators with correct exit-code semantics.
2. Whether secret redaction is complete and fail-closed.
3. Whether the Apps Script header discovery matches every production surface.
4. Whether the post-write critical-field list exactly matches the writer’s decision-critical fields.
5. Whether cross-surface comparisons need a stronger canonical `instrument_id` than base-symbol matching.
6. Whether the workflow should remain advisory initially or become a required check after two clean full cohorts.
7. Whether the Render worker must be created as a separate service and included in the runtime evidence contract.

## Recommended merge rule

Keep this pull request in **draft** until:

- unit tests pass;
- the source-only workflow artifact is reviewed;
- Claude confirms no production behavior is changed;
- one manual Render run and one manual Google Sheets run have produced artifacts;
- the bound Apps Script project has been exported and parity gaps are documented.
