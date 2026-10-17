# Workflow and direct-entrypoint inventory — 2026-10-16

Baseline: main `11ef13456e5b`; 36 versioned workflows. This read-only inventory describes configured effects, not proof of current live state. UTC/Riyadh cron and concurrency details are in [scheduler evidence](scheduler-concurrency-review-20261016.md). The workflow source is authoritative for complete job commands and environment expressions.

Every Python/Node path below is extracted from an executable run block. CI compile-only paths and tests are labelled separately; inline Bash/Python/GitHub-script blocks remain inline entrypoints. No script was run against live services.

| Workflow | Trigger classes | Direct repository entrypoints | Configured effects |
|---|---|---|---|
| `acceptance_check.yml` | schedule, workflow_dispatch | `scripts/tfb_acceptance.py` | Read live workbook; measured acceptance artifacts only, optional strict exit. |
| `actions_health_review.yml` | workflow_dispatch, push | inline GitHub JavaScript | Read Actions workflow/run/job/log/annotation APIs; no dispatch or production write. |
| `backtest_harness.yml` | schedule, workflow_dispatch | `scripts/track_performance.py` | Read workbook + historical Yahoo/EODHD OHLCV; register/backtest hypotheses and write Hypothesis_Registry. |
| `backtest_signals.yml` | schedule, workflow_dispatch | `scripts/tfb_backtest.py` | Read live Performance_Log; offline statistical/backtest report artifacts, no sheet writes. |
| `board_scorecard.yml` | schedule, workflow_dispatch | `scripts/score_selection_log.py` | Read _Selection_Log + Yahoo closes; observe/artifacts by default, optional _Board_Scorecard + _Run_Log write. |
| `calendar_sync.yml` | schedule, workflow_dispatch | `scripts/run_calendar_sync.py` | Read decision symbols; EODHD calendar requests; scheduled Calendar_Events replacement, manual dry-run default. |
| `ci.yml` | push, pull_request, workflow_dispatch | `scripts/test_maturation.py`; local test/compile paths in JSON | Local compilation, static lint, Python/Node regression harnesses; no production dispatch. |
| `core_repair_readback.yml` | workflow_dispatch | `scripts/name_identity_evidence.py`, `scripts/verify_core_repair_readback.py` | Read live workbook + backend to verify bounded repair state; optional identity evidence artifacts. |
| `daily_brief.yml` | schedule, workflow_dispatch | `scripts/run_daily_brief.py` | Read live workbook; HTML/PDF and scheduled SMTP delivery; manual preview default. |
| `daily_sync.yml` | pull_request, push, schedule, workflow_dispatch | `scripts/finalize_decision_publication.py`, `scripts/harness_w1a6.py`, `scripts/release_decision_feed.py`, `scripts/run_dashboard_sync.py`, `scripts/run_inline_page_recovery.py`, `scripts/track_performance.py`, `scripts/validate_dashboard.py`; local test/compile paths in JSON | Backend/provider requests; replace universe/portfolio pages, persistence/status/log updates, tracker evidence, inline recovery, decision-feed downgrade/final verification; optional Slack notification. |
| `decision_surface_freshness.yml` | pull_request, push, workflow_run, workflow_dispatch | `scripts/approved_universe_scope.py`, `scripts/audit_decision_surface_freshness.py`; local test/compile paths in JSON | Read live workbook/backend after completed source sync; freshness report artifacts; test-only on push/PR. |
| `dependency_typing_review.yml` | pull_request, schedule, workflow_dispatch | local regression/compile harnesses | Read dependency advisory service + local type analysis; no production writes. |
| `digest.yml` | schedule, workflow_dispatch | `scripts/send_digest.py` | Read live workbook and scheduled SMTP digest; manual preview default. |
| `full_refresh_coverage.yml` | pull_request, push, workflow_run, workflow_dispatch | `scripts/approved_universe_scope.py`, `scripts/audit_full_refresh_coverage.py`; local test/compile paths in JSON | Read source-run evidence plus live workbook/backend to audit full-refresh claims; artifacts only. |
| `grid_capacity_repair.yml` | workflow_dispatch | `scripts/repair_grid_capacity.py` | Dry-run inventory by default; explicit apply+backup+confirmation can trim workbook row/column capacity and archive log. |
| `intraday_refresh.yml` | schedule, workflow_dispatch | `scripts/intraday_quote_refresh.py` | Backend/provider quote fetch + bounded price/date/status updates on Market_Leaders/Global_Markets; scheduled apply, manual dry-run default. |
| `investment_policy_ci.yml` | push, pull_request, workflow_dispatch | `scripts/run_investment_policy_shadow.py`; local test/compile paths in JSON | Local non-enforcing policy contracts only. |
| `investment_policy_shadow.yml` | workflow_dispatch, schedule | `scripts/run_investment_policy_shadow.py` | Fetch candidates using production selector/providers; shadow-only JSON report, no enforcement or sheet write. |
| `manual_refresh_priority.yml` | pull_request, push, workflow_dispatch | local regression/compile harnesses; local test/compile paths in JSON | Compile/test inert Apps Script coordinator source contract; no deployment. |
| `news_archive.yml` | schedule, workflow_dispatch | `scripts/news_archive_collector.py` | Read news RSS and optional sectors; append daily News_Archive observations; manual dry-run optional. |
| `page_refresh_recovery.yml` | pull_request, push, workflow_dispatch | `scripts/audit_sync_outcome.py`, `scripts/plan_sync_recovery.py`, `scripts/release_decision_feed.py`, `scripts/run_dashboard_sync.py`, `scripts/run_inline_page_recovery.py`; local test/compile paths in JSON | Manual source-run planning then backend/provider page rewrite with independent budget; status/log/decision-feed downgrade and strict evidence audit. |
| `pit_snapshot.yml` | schedule, workflow_dispatch | `scripts/pit_snapshot.py` | Read workbook and append daily _PIT_Fundamentals; scheduled write, manual dry-run default. |
| `provider_target_coverage.yml` | pull_request, push, schedule, workflow_dispatch | `scripts/audit_provider_target_coverage.py`; local test/compile paths in JSON | Read provider-target coverage; local/cache baseline updates + artifacts, no live sheet writes. |
| `release_decision_feed.yml` | workflow_dispatch | `scripts/release_decision_feed.py` | Manual strict cohort audits and guarded _Status decision-feed promotion/retraction; release default-off Variable gate. |
| `repair_stores.yml` | workflow_dispatch | `scripts/repair_stores.py` | Read stores and pool grids; explicit apply can delete malformed store rows or blank identity-poisoned pool cells; default dry-run. |
| `repository_workflow_audit.yml` | push, pull_request, workflow_dispatch | `scripts/audit_repository_workflows.py`; local test/compile paths in JSON | Local workflow static checks only. |
| `runner_image_canary.yml` | workflow_dispatch | local regression/compile harnesses; local test/compile paths in JSON | Local image tools, production dependencies and focused Python/Node regression checks; no deployment. |
| `shadow_board.yml` | schedule, workflow_dispatch | `scripts/run_shadow_board.py` | Read production board/portfolio/authority/provider data; write Shadow_Board and _Run_Log unless dry-run. |
| `shadow_scorer.yml` | schedule, workflow_dispatch | `scripts/run_shadow_scorer.py` | Read board + prices/evidence; append Shadow_History and Regret_Ledger, replace gate/summary, log; dry-run optional. |
| `sync_outcome_audit.yml` | pull_request, push, workflow_run, workflow_dispatch | `scripts/audit_sync_outcome.py`; local test/compile paths in JSON | Download completed Actions artifacts and check real page-refresh outcome; no live Sheet writes. |
| `tests.yml` | push, pull_request, workflow_dispatch | local regression/compile harnesses; local test/compile paths in JSON | Local foundation compilation and tests only. |
| `thesis_exit_backtest.yml` | workflow_dispatch | `scripts/backtest_pf_thesis_exit.py` | Read live or fixture thesis evidence; historical backtest JSON/CSV artifacts only. |
| `track_performance.yml` | schedule, workflow_dispatch | `scripts/track_performance.py`, `scripts/workflow_activation_receipt.py` | Read workbook/backend; record prospective Performance_Log + Signal_History, analyze/calibrate/export; historical auditing only on explicit manual opt-in. |
| `weekly_brief.yml` | schedule, workflow_dispatch | `scripts/run_weekly_brief.py` | Read board/portfolio/provider state; SMTP delivery plus _Brief_State overwrite and _Run_Log append unless dry-run. |
| `workbook_backup.yml` | schedule, workflow_dispatch | `scripts/backup_workbook.py`, `scripts/tfb_workbook_dq_audit.py`, `scripts/workflow_activation_receipt.py` | Read workbook export; artifact backup + optional offline DQ audit; backup script may append _Run_Log completion evidence. |
| `workflow_repair_tests.yml` | workflow_dispatch, push, pull_request | `scripts/audit_decision_surface_freshness.py`, `scripts/audit_full_refresh_coverage.py`, `scripts/audit_provider_target_coverage.py`, `scripts/audit_repository_workflows.py`; local test/compile paths in JSON | Local repair/audit regression selftests; optional main-only read-only production audit verification. |

## Verified scope and caveats

- GitHub registry contains63 workflow IDs; current source contains36 workflows. Historical IDs are not extra current configuration files.
- The initial static auditor reported zero errors and two force-refetch warnings. Those warnings incorrectly treated empty-fallback Variable mappings as active overrides. The repaired auditor distinguishes literals from runtime expressions and reports zero errors/warnings; no live Variables were read or changed.
- Before the approved fix, standalone recovery had27 effective job env keys against59 primary-job keys. Its missing active persist sanity and quota enforcement were real runtime differences; optional retirement/crypto/USX/quarantine mappings were unreachable there. The remediation mirrors writer controls only; independent page/time budgets remain.
- No AGENTS.md was found under `/workspace` during the initial read-only scan.
- User-facing schedule SLAs cannot be inferred from successful exit status. See the separate bounded run-history evidence for63requested intraday targets and6late actual runs over Oct13–15. Do not backdate PIT capture dates or automatically skip late updates without a product-policy decision.
