# TFB external review follow-up — 2026-10-06

Date basis: Asia/Riyadh. Review base: merged `main` commit
`242855264650d650298b0ee158ab8ea89dea2ab3` (PR #718).

## Live readback of the review base

The Render service returned HTTP 200 from `/health` and `/readyz` and reported:

- `entry_version=8.14.1` and `service_version=8.14.1`;
- deploy commit `242855264650d650298b0ee158ab8ea89dea2ab3`;
- engine `5.151.0`, `ready=true`, and `startup_warnings=[]`;
- `TFB_T10_W52_TIMING=observe` and `TFB_PF_ADD_LOSER_VETO=observe`.

The separate `app_version=5.111.0` value is a stale environment label. Workflow
health output now prefers the canonical entry/service version fields.

## Follow-up version inventory

The merged review artifacts remain a historical snapshot of PR #718. This
follow-up advances the affected components as follows:

| Component | Review base | Follow-up |
| --- | ---: | ---: |
| `opportunity_builder` | 1.23.0 | 1.23.2 |
| `top10_selector` | 4.31.0 | 4.31.2 |
| `run_dashboard_sync` | 6.64.0 | 6.64.2 |
| `audit_sync_outcome` | 1.1.0 | 1.1.2 |
| `verify_deployment` | 1.0.29 | 1.0.32 |
| blocking CI workflow | 1.0.6 | 1.0.8 |

## Confirmed defects and corrections

1. **A funded ticket could contradict an engine `BLOCKED` identity.** The live
   GAS path posts sheet rows directly to `/sheet-rows/opportunity-candidates`.
   On the review base, a mounted-route fixture with
   `investability_status=BLOCKED` returned HTTP 200, `verdict=INVEST`, selected
   `BAD.SR`, and a 7,500 SAR ticket. The narrow gate defaulted off, and request
   criteria could also disarm an armed server value. Opportunity builder
   v1.23.2 makes exact normalized `BLOCKED` an invariant that neither the old
   environment value nor request criteria can disable. It scans every supported
   raw investability alias, so a benign or colliding duplicate cannot hide a
   hard value. A non-`BLOCKED` row retains the retired opt-in PASS trace only
   when that old environment or request switch was explicitly enabled.
   `WATCHLIST` keeps its existing opt-in broad-gate behavior.

2. **The selector's BC-3 veto did not cover its default memoryless build.** The
   hard-veto predicate ran only inside the optional stability layer, so a
   `BLOCKED` / `DO_NOT_INVEST` candidate could take a seat when no stability
   input was supplied. Top10 selector v4.31.2 filters all four admission pools
   before the memoryless fill under the existing `TFB_T10_ADMIT_LEGACY`
   control. The predicate scans canonical, camelCase, and display aliases and
   lets a hard verdict dominate benign duplicates. Stability ranks only the
   admission-safe pools; the original pools supply incumbent rows only, so
   BC-2 continues to govern incumbent grace and hard exit.

3. **The standalone sync-outcome workflow did not arm its own fresh-coverage
   criterion.** A log with a 73/239-batch time-budget exit and a positive
   6,609-row page verdict returned `ok` because retained last-good rows hid the
   incomplete fresh fetch. `run_dashboard_sync` v6.64.2 emits
   `fresh_rows`/`requested_rows`/`fresh_pct` from shared lineage-aware
   accounting. When symbol lineage is available, returned symbols later
   replaced or quarantined are excluded and overlapping failure reasons are
   counted once; Status, upstream verdict, and PAGE-VERDICT use the same base.
   Older matrices without usable symbol lineage retain the conservative legacy
   fallback. `audit_sync_outcome` v1.1.2 enforces the configured 95% minimum in
   the scheduled audit and both manual recovery stages. It rejects partial
   verdicts, exact below-threshold coverage, and explicit unknown coverage;
   legacy logs with no coverage fields remain compatible. Recovery verdicts
   and evidence use the latest numeric cycle.

4. **Production regressions were outside the blocking workflows.**
   `test_argaam_loopguard_b6.py` and `test_ycp_loopguard_b6.py` now run in the
   blocking lean job. They exercise cross-event-loop failures that can silently
   drop recommendation candidates. CI v1.0.8 also runs
   `test_opportunity_blocked_invariant.py` in the contract job, where its
   mounted FastAPI route dependencies are installed.

5. **Universe recovery guidance exceeded the available evidence and named an
   incompatible tool.** The reviewed artifacts contain 255 Market_Leaders rows
   against a configured floor of 1,025 and 2,474 Mutual_Funds rows against a
   floor of 4,496. That proves row-count deficits under the configured
   contracts. The repository has no canonical membership manifest, so it does
   not prove which approved symbols are absent or that the deficits caused the
   recommendation complaint. `scripts/build_universes.py` explicitly cannot
   regenerate those Saudi pages. Workflow comments and the original review
   sheet now require an owner-approved versioned list or verified Saudi-native
   source, a backup, and an offline diff before any sheet write.

## Validation

The exact blocking lean-CI command completed locally with **318 passed and 1
skipped**. The production-pinned contract environment completed with **25
passed and 2 skipped**. The focused sync, outcome-audit, and recovery set
completed with **62 passed and 1 skipped**; the standalone fetch-failure,
keep-last-good, and retirement harnesses also passed. The opportunity-builder
dual-tree harnesses passed against their historical bases. Full Python
compilation, workflow YAML parsing, manifest pins, `git diff --check`, and the
repository workflow audit passed; that audit reported zero errors and 27
pre-existing warnings. GitHub checks require a push and were not run locally.

## Open findings outside this patch

- Yahoo Chart and Yahoo Fundamentals are both reachable from current
  recommendation builds and their SingleFlight cancellation handling remains
  unsafe. A waiter directly awaits the shared Future, so cancelling that one
  waiter cancels the Future seen by every waiter. The owner catches
  `Exception`, while `asyncio.CancelledError` is a `BaseException`, so owner
  cancellation can exit without completing the shared Future and strand a
  peer. Zero-network probes reproduced both failure modes. The repair needs
  shielded waiter awaits plus explicit owner-cancellation cleanup and focused
  cancellation regressions; it is intentionally separate from this patch.
- The recommendation engine names Finnhub in `DEFAULT_PROVIDERS` and
  `DEFAULT_GLOBAL_PROVIDERS`, but its provider picker accepts only
  `get_quote*`, `fetch_quote*`, `quote*`, `get_unified_quote`, or `fetch`.
  Finnhub exports `fetch_enriched_quote_patch`, `fetch_enriched_quotes_batch`,
  `fetch_quote_patch`, and `fetch_patch`, so the configured fallback is
  silently skipped and its client is never constructed on this path. Wiring
  that fallback also exposes the provider's confirmed cross-event-loop locks,
  semaphore, single-flight Futures, and persistent HTTP pool. A safe repair
  needs the callable wiring and per-loop transport state in one change.
- `DEFAULT_KSA_PROVIDERS` names Tadawul and Argaam, but `DataEngineV5` never
  consumes that constant: Market_Leaders, Top_10, and portfolio pages resolve
  to the generic `DEFAULT_PROVIDERS`, and no production engine factory caller
  injects a KSA provider list. Tadawul's confirmed cross-event-loop failure is
  therefore latent on direct/custom use rather than active in the current
  recommendation build. Repairing KSA routing should be paired with a robust
  per-loop transport fix instead of activating the unsafe singleton first.
- Outcome records still derive the Riyadh day from execution wall time. A
  delayed October 5 slot can write an October 6 key and suppress the genuine
  October 6 cohort; one run can also cross midnight between Performance_Log
  and Signal_History. The correction needs a single scheduled-slot day resolved
  once per run and boundary coverage before changing historical evidence keys.
- Signal_History idempotency is process-local. Two fresh tracker processes can
  append the same symbol/day key because existing worksheet keys are not loaded
  before append.
- A canonical, versioned Market_Leaders and Mutual_Funds membership manifest is
  still required before a safe restoration or causal replay is possible.
- This review verified the deployed base. The builder and selector need a
  post-merge Render readback before they can be called live. The dashboard,
  audit, and workflow changes need a successful Actions run with inspected
  logs, and verifier v1.0.32 must confirm the deployed pins.
