# Project-wide script review — 10 October 2026

Scope: every Python module, Apps Script file, Node harness, shell script and
GitHub workflow in the repository at `616f606` (#748). Method: byte-compile
of every file on Python 3.11 and 3.13, ruff's error-class rules (syntax,
undefined names, unused names, loop-variable binding, return-in-finally,
precedence), a full `pytest tests` run in three environments (the lean
contract pins from `ci.yml`, the full `requirements.txt` stack, and a current
FastAPI/httpx stack), every Node harness, `scripts/audit_repository_workflows.py`
and `scripts/repo_hygiene_check.py`. No production data, credentials or
workbook were touched; every verification ran offline.

## Findings and repairs

### Code defects

| File | Defect | Repair |
| --- | --- | --- |
| `core/providers/yahoo_chart_provider.py` | `fetch_chart_meta` recorded the YC-4 identity-mismatch metric through an undefined name `metrics`; the `NameError` was swallowed by the surrounding `except`, so the counter was never incremented | call `_get_metrics()` like every other site in the module |
| `core/analysis/opportunity_builder.py` | `_stop_vol_input` is annotated with `Optional` but the module never imported it (harmless only because of `from __future__ import annotations`) | import `Optional` |
| `routes/advisor.py`, `routes/enriched_quote.py` | seven alias routes (`sheet-rows`/`sheet_rows`, `enriched_quote`/`enriched-quote`) produced duplicate OpenAPI `operationId`s because FastAPI folds `-` and `_` to the same id; FastAPI warned on every `/openapi.json` render and the document was not a valid OpenAPI 3 object | explicit `operation_id` on the second spelling; routing unchanged, 50 paths / 66 operations, no duplicates |
| `core/config.py`, `core/data_engine.py`, `scripts/worker.py` | `TraceContext.__exit__` returned from inside `finally`, which silences any in-flight exception | same logic without the `try/finally` |
| `scripts/clean_workbook_duplicates.py` | duplicate `"JO"` in `EXCHANGE_SUFFIXES` | removed |
| `scripts/intraday_quote_refresh.py`, `tests/test_de_fund_cache_first_p154c.py` | `a and b or c and d` without parentheses | parenthesised (same precedence, now explicit) |
| 43 files | unused imports and unused locals (`exc`, `g`, `adds_syms`, `rbst`, `before`) | removed; one intentional import in `scripts/harness_gates_8132.py` kept with `noqa` |

Reviewed and left alone: 14 ruff B023 hits (closures over loop variables) are
all invoked inside the same iteration, so they are not bugs; `SCHEMA_VERSION`
in `core/sheets/data_dictionary.__all__` is served by the module `__getattr__`
and now carries a `noqa`; `embargo` in `core/validation.py` and `pnl_pct`,
`pe_ttm`, `cash_y`, `view_prefix`, `conv`, `indexes` are computed and unused
in decision-layer modules and were not changed in this pass.

### Test suite

`python -m pytest tests` could not run at HEAD: `tests/test_yf_loopguard_p203.py`
called `sys.exit()` at import and aborted collection, and
`tests/test_board_engine_roi_p139.py` replaced `sys.modules["core"]` with a stub
before failing, which broke the import of 40 later modules in the same session.
CI never saw this because `ci.yml` lists test files individually.

* `test_yf_loopguard_p203.py` now exposes its verdict as a pytest test and only
  exits when run as a script. `test_endpoints.py` skips under pytest when
  `requests` is missing instead of raising `SystemExit`.
* `test_cash_snapshot_certification.py` and `test_export_audit_v1.py` skip when
  `openpyxl` is absent (the heavy lane installs it; the lean lanes do not).
* `test_track_force_coverage_p180.py` disabled logging process-wide at import;
  because pytest imports every module before running any test, this blanked
  the log records that `test_redaction_boundaries.py` and
  `test_route_error_redaction.py` assert on. Logging is re-enabled at the end
  of the module and the harness verdict is now a pytest test.
* `test_critical_symbol_identity.py::test_run_one_task_successful_write_still_fails_missing_fresh_proof`
  failed at HEAD: the sheet-presentation boundary added on 2026-10-09 refuses
  any ranked market page without the canonical 115-column header, so the
  8-column fixture never reached the write it was asserting on. The fixture now
  uses `get_sheet_headers("Market_Leaders")`; the test again proves that the
  stub write executes and the page verdict is still `failed`.
* `test_opportunity_builder_rel_cluster_tag.py` pinned the builder to exactly
  1.23.2; it now asserts a floor.
* Nineteen dual-tree harnesses whose base trees, `*_ORIGINAL.py` copies,
  `*_base.gs` files or upload TSVs were never committed moved to
  `scripts/harness_archive/` with a table of reasons in its README. None was
  referenced by any workflow. `test_dt10_cash_source.js`,
  `test_dt10_p145_grace_sizing.js` and `test_dt10_p142_containment.js` stay in
  `tests/` and now resolve the Apps Script source relative to the test file
  (the containment harness also accepts LF line endings).

### Workflows

* `actions/checkout@v4`/`@v5` → `@v6` and `actions/setup-python@v5` → `@v6` in
  15 workflows, the majors `scripts/audit_repository_workflows.py` already
  verifies (26 workflows were on v6).
* `daily_sync.yml` dropped `id-token: write`; no step uses OIDC.
* `ci.yml` 1.1.4: the compile job also runs `ruff check --select E9,F63,F7,F82`
  over the whole tree, the rule set that would have caught the undefined
  `metrics`.

Still reported by the workflow audit and left for the operator: the one-run
`TFB_SYNC_FORCE_REFETCH_SYMBOLS` override mapping in `daily_sync.yml`
(lines 371 and 1507) remains active; the audit asks that it be verified and
removed after the repair run.

## Verification

| Check | Result |
| --- | --- |
| `python -m compileall` (3.11 and 3.13), every `.py` | clean |
| `ruff check --select E9,F63,F7,F82 .` | clean |
| Node harnesses in `tests/` and `tests/js/` | 9 of 9 pass from the repository root |
| `pytest tests`, Python 3.11 with the `ci.yml` contract pins (numpy 1.26.4, fastapi 0.115.14, starlette 0.41.3, httpx 0.27.2, pydantic 2.13.2) | 3229 passed, 13 skipped, 0 failed, 0 collection errors (at HEAD the session aborted during collection) |
| `pytest tests`, Python 3.13 with current FastAPI 0.143 / httpx 0.28 (not a supported stack; `runtime.txt` pins 3.11) | 3301 passed, 2 failed, 10 skipped. Both failures are in `test_schema_alignment` and pass under the pinned stack: two route families do not mount under starlette 1.x, which is a dependency-upgrade question, not a repository defect |
| Heavy lane (`requirements.txt` on 3.11): data engine, scoring settle, news cancellation, cash certification, outcome evidence, margin units | 397 passed |
| `scripts/audit_repository_workflows.py` | 0 errors, 2 warnings (the override mapping above) |
| `scripts/audit_provider_target_coverage.py --selftest`, `audit_decision_surface_freshness.py --selftest`, workflow audit unit tests | pass |
| `/openapi.json` under pinned FastAPI with warnings as errors | 50 paths, 66 operations, no duplicate ids |
