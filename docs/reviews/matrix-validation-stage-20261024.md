# Matrix validation stage repair — 24 October 2026

Baseline: `561db60c74c1d56d866d4b72756f8ef9a9df57d5`.

The seven-day run census found eight matrix-leg validator failures in three
runs where source acquisition had succeeded but the independent native
Top-10 grid was missing or stuck rendering. Every leg invoked the validator
without `--pages`, so each checked all source pages and the asynchronous
cockpit while other writers were still active. A global-leg validation failure
also prevented its `success()`-guarded performance recorder from running.

## Implemented behavior

- Each leg resolves its actual, already-canonical `SYNC_KEYS` to source pages
  and validates only those. The mapping is checked against the runner's actual
  task definitions. Empty, unknown and native-only scopes cannot fall back to
  the all-page default. The native Top-10 key is deferred when accompanied by
  real source keys.
- Matrix validators retain all existing contract, data-quality and scope
  checks but do not write the shared `Dashboard_Audit` tab. That report is
  written once by the final job, avoiding three independent report overwrites.
- After all matrix writers and inline recovery finish, the existing final job
  validates all five source pages and the real native cockpit grid. It uses
  the same production write lease; source-quality thresholds are unchanged.
- The actual validator JSON is retained and attested to this workflow run,
  exact source SHA, workbook fingerprint and process exit. The finalizer 1.1.2
  checks strict bounded JSON, a recent same-host timestamp, the literal reader,
  complete source contract/scope checks, native header/banner and the actual
  blocked/actionable withholding branch. An unavailable or malformed witness
  is a blocking result.
- Structural results fold into the existing decision audit, preserving the
  exact two-element `[coverage, decision]` exit-code grammar. A missing grid
  with old OK status text still causes the existing single bounded downgrade
  and readback. A validator crash or missing report cannot skip that writer.
- Existing advisory sanity warnings retain the old nonblocking policy. Their
  raw validator exit `1` and findings remain in `structural_validation`, while
  the effective structural exit is `0` only after all required source and
  native proofs pass. Critical or unknown warnings cannot certify the grid.
  Observational warnings do not enter the decision audit's release findings.
- Standalone finalizer callers that do not request the structural witness
  retain their previous behavior. The production full-sync workflow supplies
  it explicitly. No broker, financial input, recommendation threshold, source
  refresh, native Apps Script deployment or release promotion is performed by
  this code revision.

## Validation

`tests/test_matrix_validation_scope.py` executes the actual workflow shell
with only Google-facing calls replaced. It covers all source-key mappings,
scoped source calls, native handoff, a validator crash, unknown/empty scopes,
clock/run/SHA/workbook mismatch, duplicates, incomplete proof and the valid
positive path. An actual producer sanity warning preserves the baseline
release acceptance result, with its warning retained in the structural
diagnostics; critical and unknown warning cases remain blocking. The old
workflow fails all eight scoped/final-handoff cases.

The actual old finalizer independently reproduces the missing-grid defect:
with otherwise valid current source/B2 receipts it returns `[0, 0]` and leaves
the prior feed unchanged. The candidate returns `[0, 2]` and performs exactly
one downgrade/readback. Missing or unbound witnesses return `[0, 3]` through
the same writer. This is not a claim that the current live cohort is accepted.

The persistent late native refresh/receipt mismatch remains a separate
operational prerequisite. The final gate continues to report it rather than
relaxing receipt, freshness, stability or account requirements.
