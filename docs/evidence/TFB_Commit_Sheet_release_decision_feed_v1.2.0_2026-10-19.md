# TFB Commit Sheet - decision feed release on a schedule (release_decision_feed v1.2.0, workflow v1.1.0, 26_Decision_Chain v1.0.1)

Date: 2026-10-19 (Monday, 11:30 to 13:00 Riyadh). Base: `origin/main` 137cd80 (#796).
Delivered: `scripts/release_decision_feed.py` 1.1.0 -> 1.2.0, `.github/workflows/release_decision_feed.yml` 1.0.0 -> 1.1.0,
`apps_script/26_Decision_Chain.gs` 1.0.0 (ported from #788, commit 64c7ec9, unchanged) -> 1.0.1,
`tests/js/decision_chain.test.js` (+C16, C17), `tests/test_decision_chain_js.py` (17/17), `tests/test_release_auto_source.py` (NEW, A1-A7),
`tests/test_release_decision_feed.py` (N16 contract for the new triggers), `scripts/verify_deployment.py` (pin 1.2.0), `.github/workflows/ci.yml` 1.1.22, this sheet.
Not edited: the finalizer, both audits, run_dashboard_sync, 10_My_Portfolio.gs, 16_Decision_Top10.gs, every gate value, every acceptance rule.

## Why

The Top_10 export of 19 Oct 11:29 Riyadh reads
`FEED NOT ACTIONABLE - verdict stale (536m old) - 0 EXECUTABLE / 1 QUALIFIED PLAN + 9 GRACE-HELD - SIZING WITHHELD`
with 12 names qualified on merit and `source readiness VERIFIED (run 39172358424)`. The same shape has rendered on
every cockpit run since 15 Oct. The cause is procedural, not an audit failure:

1. The feed is downgrade-only. The final job of every sync vetoes it (`NOT_ACTIONABLE(final_publication)`),
   because it runs before the cockpits consume the cohort (13 Oct review, blocker 2).
2. The only promotion path is `release_decision_feed.yml`, dispatched BY HAND with the sync run id,
   after both cockpits re-render and inside the 480-minute cockpit clock. It was never dispatched.
3. Even after a release, the board shows it only on a render after the release (prerequisite 3 of the
   P0-1 commit sheet); until now that render waited for the 4-hourly wrapper.

So the stopper was three operator actions per day at the right minutes. This change makes the machine take them,
under the unchanged gates.

## What

### release_decision_feed.py v1.2.0: `--source-run-id auto`

- `discover_source(grid)` (PURE): the single `run=` token of the `TFB Feed Global_Markets` value in the same
  bounded `_Status!L1:M60` block the script already reads. One read, no new range. That page is the last one the
  sync writes, so its run is exactly the cohort the cockpit receipts must name; a wrong cohort cannot pass the
  strict audits. Missing or repeated key, or a value without exactly one `run=` token -> refused,
  `source_auto_key_missing` / `source_auto_key_duplicated` / `source_auto_run_unreadable`, zero writes.
- `mode=kill` (or the sticky `TFB_DECISION_RELEASE_KILL`) with an unresolved cohort still retracts a live
  EXECUTABLE, using the run token of the value it retracts. A cell layout can never block a kill.
- Everything after discovery is the v1.1.0 path byte for byte: classify_prior, GitHub provenance of SOURCE
  (completed production daily_sync on main, final job executed), the in-flight probe, the REAL strict audits
  bound to SOURCE, the release window, compare-and-set, exact readback, `display_pending_cockpit_refresh`.
- `out["source_mode"]` records `auto` or `explicit`. An explicit numeric id behaves exactly as before.

### release_decision_feed.yml v1.1.0: scheduled attempts

- `on.schedule`: `45 0,10,17,20 * * *`, `30 5 * * *`, `45 12 * * *` (UTC): Riyadh 03:45, 08:30, 13:45, 15:45,
  20:45, 23:45, after the cockpit render slots (08:07 morning trigger, 4-hourly wrapper 11:29 / 15:29 / 19:29 /
  23:29 / 03:29) and after each sync's typical end when 26_Decision_Chain renders the cockpits within minutes.
- `workflow_dispatch.source_run_id` is optional, default `auto`; `mode` default `release`. A scheduled run is
  always `mode=release` + `source=auto` (shell-validated). The sticky kill variable still wins inside the script.
- On a SCHEDULED run, exit 1 (deferred: a production writer in flight) and exit 2 (audits not accepted: the
  cockpits have not consumed this cohort yet, or the render is outside the window) end the job green with a
  `::warning::` annotation; the next slot retries. Exit 3 (refused, layout, write or readback fault) and every
  non-zero exit of a DISPATCHED run stay red. The artifact and the job summary carry the exact outcome either way
  (summary gains `source_mode`).
- Unchanged: gates (`vars.TFB_DECISION_RELEASE` off => dry-run report, zero writes), the private concurrency
  group, `environment: production`, checkout of this workflow's ref, the env block copied from the final job,
  the supersede wiring, `refs/heads/main` only.

### 26_Decision_Chain.gs v1.0.1: re-render Top_10 after a release

- `tfbDcReleaseOf_(raw)` (PURE): an EXECUTABLE feed value -> `{run, issued}` (head token exactly `EXECUTABLE`,
  exactly one `run=` and one `issued=`); anything else -> empty.
- Plan: when the feed is EXECUTABLE with `run=N` for the cohort the chain already refreshed (`state.t10 == N`)
  and an `issued=` the chain has not rendered yet (`state.t10rel`), plan ONE more Top_10 refresh under the same
  skips (sync in flight, cockpit running, quiet window, manual priority, `TFB_DC_MAX_ATTEMPTS` per stamp).
  A NOT_ACTIONABLE feed, a release for another run, or an already rendered stamp plans nothing. When the normal
  post-audit render already happens under an EXECUTABLE feed, it counts as the release render (no double render).
- Default (`TFB_DECISION_CHAIN` unset or `off`) still returns before any read. Observe mode logs the re-render
  once and never refreshes. `tfbDecisionChainStatus()` gains `last_release_render=`.

## Default-off identity

- Script: a dispatched numeric `source_run_id` takes the explicit path, unchanged (A5).
- Workflow: until `TFB_DECISION_RELEASE=1`, every scheduled run is a dry-run report with zero writes; it is the
  P0-1 commit sheet's "zero-code observe experiment" performed automatically, six times a day.
- Chain: not installed until the owner pastes the file and runs `tfbInstallDecisionChain`; `off` reads nothing.

## Audits

| Battery | Result |
| --- | --- |
| `tests/test_release_decision_feed.py` (REAL finalize, audits, sync verdict, consumers, cockpit parser) + `tests/test_release_auto_source.py` + wrapper + pins + final publication + workflow mappings/audit/verdict | 217 passed, 10 skipped (golden-base copies unset) |
| `tests/js/decision_chain.test.js` (REAL 10/11/16/26 sources in one vm) | 17/17: C1-C15 unchanged, C16 one render per `issued=`, other-run and NOT_ACTIONABLE feeds plan nothing, C17 post-audit render under an EXECUTABLE feed counts, observe logs once, pure helper |
| `tests/js/apps_script_syntax.test.js` | 5 sources compiled (ES5, ASCII) |
| `scripts/audit_repository_workflows.py` | errors 0, warnings 2 (pre-existing, daily_sync.yml force-refetch override) |
| ruff E9/F63/F7/F82, YAML parse, ASCII check on the workflow | clean |

## Arming (owner; nothing armed by this delivery)

The P0-1 prerequisites are unchanged. What this change removes is the operator at the keyboard.

1. Merge. From the next slot the scheduled runs produce artifacts; read `accepted` and `reasons`.
   Expected before the chain is armed: `not_accepted` with `T10_OLDER_THAN_SOURCE` or `PF_*` on the slots that
   fall before a cockpit render; `dry_run_accepted` on the slots after both cockpits rendered the latest cohort.
2. Apps Script: create `26_Decision_Chain` and paste v1.0.1; `TFB_DECISION_CHAIN=observe`, `tfbInstallDecisionChain`;
   after one sync with `OBSERVE` rows in `_Run_Log`, set `on`. Set `DT10_FEED_COHORT_BIND=on` (prerequisite 2).
3. Arming preconditions A and B of the P0-1 sheet (30-day re-run window of pre-#763 runs; `production`
   environment restricted to `main`). These are owner decisions, not code.
4. `TFB_DECISION_RELEASE=1` (exactly). Then the chain of events is automatic: sync -> final veto -> chain renders
   PD then T10 -> next scheduled release slot promotes -> chain re-renders T10 -> board shows
   `FEED ACTIONABLE - EXECUTABLE` with sized tickets for the stability-confirmed seats. The next sync's
   supersede step retracts it at writer start, as designed.
5. Read-back: the release artifact `outcome: released`, `_Status` `TFB Decision Feed` EXECUTABLE for the cohort,
   `_Run_Log` row `[DECISION-CHAIN v1.0.1] t10 run N -> rendered (release issued ...)`, Top_10 B2 `output: EXECUTABLE`.

## Rollback

- Workflow: remove the `schedule:` block (dispatch keeps working with `auto`), or revert the file.
- Script: `--source-run-id <id>` explicit path is unchanged; revert to 1.1.0 if needed.
- Chain: `TFB_DECISION_CHAIN=off` (instant) or paste v1.0.0.
- A live release is retracted as before: `TFB_DECISION_RELEASE_KILL=1`, then dispatch `mode=kill`.

## Known limits

- The schedule cannot render the cockpits; without the chain (or the wrapper) a release still waits for the
  next 4-hourly render to show, inside its 480-minute clock.
- Six slots a day cost about 12 runner-minutes; a slot that lands inside a sync defers and retries next slot.
- The stability gate (3 confirmations) and every seat-level gate are unchanged: an EXECUTABLE feed sizes only
  the seats that already qualify and confirmed.
