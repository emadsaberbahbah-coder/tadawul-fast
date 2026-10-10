# Codex implementation of the 10 October work brief

Repository implementation prepared on 11 October 2026, Riyadh date. Base:
`9a98d5a`, the supplied PDF handoff on `claude/pensive-hypatia-smnxqe`, including
the earlier repository review at `2698457`. Implementation branch:
`codex/tfb-work-brief-implementation-2026-10-11`.

The user's request authorizes Codex-owned repository work. The PDF supplies
requirements and evidence claims; its operator actions and owner decisions
remain separate. This implementation adds code, offline tests, workflows and
review evidence. It does not activate production flags, deploy a service,
write to a live Sheet, install broker evidence, modify GitHub settings or
choose investment/model policy.

## Delivered work and remaining dependencies

| Package | Repository result | Remaining dependency |
| --- | --- | --- |
| WP-01: reconciliation | Private offline JSON/explicit-CSV capture builder, same pure custody certifier, freshness/FX/reservation checks, additive cash export contract and regressions. | Real broker evidence and authentic Apps Script forwarding. The portfolio BLOCK remains the designed response to absent/stale evidence. |
| WP-02: control mappings | Both tracker lanes map dedup/risk controls; both sync writers map retirement and crypto name controls; scorer maps S-1 criteria v2. Verifier exposes rollout and policy controls as optional, default off. | Operator observe/enforce rollout, reviewed retirement list and S-1 evidence-window decision. |
| WP-03: small-price precision | Shared gated price rounding preserves at least seven significant digits below 10; engine/scorer anchors and tuple tolerance use the same contract. Health exposes the gate. | Deployment and an operator evidence run with `TFB_PRICE_PRECISION=1`. |
| WP-04: USX | Default-off lockstep across opportunity building, identity/portfolio checks, action invariants, engine market cap, provider and audit. Armed mode handles pegged USD/100, row FX and mixed-case USX consistently. | Real FX lookup row and authentic `15_Lists_Config.gs` seed source; no live coverage readback. |
| WP-05: crypto identity | Shared existing audit name vocabulary; gated, idempotent EODHD `.CC` mapping; fundamental-patch and restored/final-row name guards. Final enforcement runs after every restoration path. | Owner-confirmed instrument identity/universe and supplier coverage, operator quarantine/rollout. Substring name evidence does not certify issuer identity; unknown names remain unjudged. |
| WP-06: export audit | Audit v1.2.0 adds default-off exact margin receipts and bound plan-threshold reporting; distinguishes unavailable blank risks from all-zero placeholders. Offline replay evidence committed. | Newer receipt-bearing export to verify the handoff's 213/114 split; owner cost and supplier-plausibility policy. |
| WP-07: PIT repair | Read-only default/scan and strict offline planner; bounded, drift-checked operator apply path; timestamp/header regressions and artifact deletion identities. | Backup and operator-run live scan/apply. No repair was applied to the Sheet. |
| WP-08: horizon/INVEST | Real single-tree tests exercise horizon off/observe/enforce and strict INVEST off/on. Policy controls are visible in the verifier. | Owner horizon/eligibility decision; no policy chosen. |
| WP-09: universe/pipeline | Bounded export/code investigation documents exact firewall and capability-routing predicates, older evidence counts and missing same-run inputs; retirement mappings supplied. | Fresh raw/pre-firewall rows, backend/provider receipts, reviewed retirement/universe policy. No narrower firewall change is justified by the available evidence. |
| WP-10: cadence/cost | Export/account-counter investigation, exact recovery artifact gates and possible fundamentals cost amplifier documented. No speculative replay/provider change. | Dispatcher token/arming, owner runs/day and cost target, failed-run recovery artifacts and endpoint/cache/retry telemetry. |
| WP-11: test discovery | Runnable batteries are pytest-visible and isolated; missing-tree harnesses archived with real replacements; endpoint tool moved behind a CLI shim; import side effects tested. Blocking steps explicitly register new regressions; full pinned suite remains advisory. | Owner can require the full-suite lane after the requested green week. |
| WP-12: structure | Pure performance math extracted in its own tested commit; shared price/name leaves introduced; generated 855-name flag inventory, compatibility-module analysis, scoped pyright and advisory dependency audit. | Further decomposition of giant modules and compatibility migration need covered seams. Dynamic flags require manual review. Dependency upgrades remain separate; pins unchanged. |
| WP-13: Apps Script | Syntax compilation covers all three authentic `.gs` sources; missing-source search and prerequisites documented. | Five authentic bound-project exports are absent: My Portfolio, Portfolio PnL, Lists Config, Portfolio Ledger and Trade Notes. FX/forwarding/archived behavioral tests cannot be completed against invented sources. |
| WP-14: calibration | Reproducible notebook-free offline report and per-page/horizon evidence; duplicate/outcome/probability handling tested. Strict past-only walk-forward purge verified with a nonvacuous property grid; misleading embargo documentation corrected. | Owner Brier/promotion and hypothesis decisions. Available export predates the handoff corpus. |
| WP-15: framework drift | Python 3.13/new-framework reproduction identified lazy included-router diagnostics; feature-detected traversal preserves eager-stack behavior. Advisory future-stack lane added. | Production pins unchanged; future framework adoption remains a separate decision. |

## Verification

- Final offline suite on Python 3.11.9/FastAPI 0.115.14/Starlette 0.41.3:
  **3,747 passed, seven skipped, 131 subtests passed**, in 147.69 seconds.
  Three existing pytest-return warnings remain. Skips are visible in the
  suite; this is not a claim that unavailable sources were tested.
- The isolated performance-math extraction tree passed **3,319 tests and
  131 subtests, seven skipped**. Its test-only YF subprocess isolation removes
  an existing duplicate Prometheus registration during collection. The two
  extracted function ASTs match the handoff baseline exactly.
- All **ten active Node harnesses** passed, including syntax compilation of
  the three authentic Apps Script files.
- Python 3.13.5/FastAPI 0.143.0/Starlette 1.7.0 contract checks:
  **16 passed, two capability skips**. Every required route family was visible.
- Export audit embedded selftest: **50/50**, digest `e4a1dd8650ad62f9`.
- Compile and existing Ruff fatal-error rules passed; scoped pyright reported
  zero errors; workflow audit reported zero errors and two existing one-run
  force-refetch warnings; whitespace checks passed.
- New CI steps were also checked with their dependency profiles: 275 tests
  passed with only numpy/pytest/PyYAML, three route tests passed on the minimal
  pinned contract stack, and 124 production-dependent regressions passed.
- Local deployment verifier v1.0.46 matched **all 33 module and 21 script
  version pins**. Strict verdict remains `DRIFT` because this local process
  leaves the existing `TFB_ADV_ENGINE_CALL_TIMEOUT` and
  `TFB_OPP_BUILD_OFFLOOP` kill-switches unarmed. This was a local import check,
  not a Render readback; no flags were armed to manufacture a CLEAN verdict.
- The advisory requirements audit reported 89 advisory entries across
  13 pinned packages and skipped the direct-wheel `httpcore` requirement.
  Its output is incomplete static dependency evidence, not an assessment of
  deployed exploitability. No dependency pins were changed.

CI workflow source is validated locally. Remote Actions outcomes must be
read from the pull request after publication; these local results do not
assert that remote CI has completed.

## Evidence boundaries and reproduction

The available workbook SHA-256 is
`9c8b34812eee7d5554a3586f1f86a232e79159f24e228a4eac070e2436441ef4`.
Its latest run log is 6 October 2026 at 23:46:43 Riyadh, and most market data
represents 6 October. Inspection was read-only/data-only. It cannot verify
the handoff's claimed 8–10 October backend health, recovery behavior or
receipt-bearing population.

The older export yielded 78 planned PIT realignments, eleven artifact
deletions and zero anomalies. Calibration retained 11,191 matured outcomes
after removing 27 duplicates, with decided win rate 46.6915% and Brier
0.28402366 across 10,987 decided records. All-zero risk placeholders provide
no risk evidence, but do not erase genuine realized outcomes. Signal History
contributes entry-date coverage, without joining ambiguous/later probabilities
to earlier decisions. These figures are measurements of this export only.

```sh
python -m pytest -q tests
for harness in tests/js/*.test.js tests/test_dt10_*.js tests/test_gas_*dispatch*.js; do
  node "$harness"
done
python scripts/audit_repository_workflows.py
python scripts/tfb_export_audit.py --selftest
python scripts/verify_deployment.py --strict --json
python scripts/calibration_report.py export.xlsx --json calibration.json --md calibration.md
python scripts/flag_registry.py --output docs/flags.md
```

Follow the existing pinned bootstrap for offline regression dependencies.
The individual reviews under `docs/reviews/*20261011.md`, calibration/audit
JSON under `docs/evidence/`, `docs/portfolio_reconciliation_contract.md`,
`docs/apps-script-source-prerequisites.md` and `docs/root-compatibility-modules.md`
record the source contracts, acceptance checks and remaining operator inputs.
