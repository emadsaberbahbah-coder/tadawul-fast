# Scheduler and concurrency review — 2026-10-16

Read-only review of all 36 `.github/workflows/*.yml` files at main `11ef13456e5b` in `/workspace/work/tfb-workflow-review-20261016`. No repository edits, dispatches, or secret reads. Run metadata source: `/workspace/work/runs-current-20261016.json`, 8 concatenated JSON page objects, 755 records spanning 2026-10-13T01:11:12Z–2026-10-16T07:06:52Z. No non-main manual dispatches were observed in that bounded history. No actual writer collision is established by this review.

## Concrete findings

1. **P1 availability: green intraday jobs did not provide an in-session refresh.** The workflow targets 21 ticks per Sun–Thu trading day, 07:00–12:00 UTC / 10:00–15:00 Riyadh (`intraday_refresh.yml:91-95`), always applying on schedule (`:209-214`). Across complete Oct 13–15 there were 6 scheduled runs against 63 targets (9.5%); all 6 green, all started after 12:00 UTC. Starts: Oct13 14:44:16/18:52:57Z, Oct14 12:35:44/16:33:23Z, Oct15 12:23:23/16:19:45Z. IDs: 38415936229, 38450347745, 38565005528, 38599079854, 38734255457, 38768061003. Both cron lines are identical in all associated historical heads (030e22eb2d49, dda1211456d9, 11ef13456e5b). The script checks quote evidence but has no trading-window delivery postcondition. This is evidence of missed scheduling service, not invalid cron syntax. Do not claim successful runs imply freshness SLA delivery.

2. **P1 latent data race: IQR and whole-page sync can interleave despite the fingerprint guard.** `intraday_refresh.yml:103-108` has a private cancellable group; `:204-205` targets Market_Leaders/Global_Markets, also replaced by Daily Sync. `scripts/intraday_quote_refresh.py:542,553` rereads and fingerprints a row but `:567` writes later in a distinct API call; Daily Sync can reorder or replace rows in that gap (`scripts/run_dashboard_sync.py:14481-14491`). The range-based update can therefore place one symbol's quote into a newly moved row. The reread reduces the window; it does not provide a compare-and-set or a shared lease. No collision observed in current metadata; all six recent scheduled IQR intervals were outside the recent scheduled sync intervals.

3. **P1 latent destructive race: manual store repair has no exclusion against other writers.** `repair_stores.yml:32-34` is a private group, `:56-57` targets the live default workbook when an operator explicitly chooses apply. `scripts/repair_stores.py:285-314` computes row numbers then `:224-239` deletes dimensions; `:334` reads market grids and `:384-399` blanks/rebuilds corrupt rows later. A tracker rewrite or page reorder between those operations can shift row identities, deleting or blanking the wrong row. Dry-run is the default; this is not an allegation that repair ran or corrupted data.

4. **P2 latent evidence race: standalone tracker exclusion is best effort, not serialization.** Private `tfb-performance-clock`, cancel=false (`track_performance.yml:129-131`); one Daily Sync API probe (`:215-232`), explicitly fail-open (`:211-224`), before configuration/health and the eventual run (`:234-270`). A sync can start after the probe, API errors proceed, and page-recovery/grid/repair-store writers are not probed. Historical audit-save does clear/rewrite (`scripts/track_performance.py:6101-6121`); cached key checks cannot prevent overlapping copies. The repository comments accurately acknowledge this limitation. Do not fix by blindly putting every writer in one GitHub concurrency group: one-running/one-pending semantics can evict pending production syncs.

5. **P2 latent duplicate evidence: Shadow Scorer has no concurrency block.** In `shadow_scorer.yml:33-39`, permissions leads directly into jobs; manual default dry_run=false (`:26-28`), scheduled run writes (`:117-119`). Two simultaneous runs can both pass the local history check (`scripts/run_shadow_scorer.py:1945`) and append the same immutable date (`:1688-1693`). Sequential duplicate refusal is not atomic. No observed collision established.

6. **P2 latent duplicate delivery/state overwrite: Weekly Brief has no concurrency block.** `weekly_brief.yml:21-27`; manual/schedule default sends and writes (`:14-19,61-67`). `scripts/run_weekly_brief.py:339` reads prior state, `:354` sends, `:355` saves; `save_state` at `:123-131` overwrites A1:A2. Concurrent scheduled/manual runs can mail twice from the same baseline and finish in reverse order. No observed collision established.

7. **P2 conditional branch bypass of the shared production writer group.** Daily Sync `:109`, Page Recovery `:58`, Grid Capacity Repair `:48` share `tadawul-production-write-${github.ref}` only on the same ref. None has a main-only apply guard: Daily preflight `daily_sync.yml:218-245` validates tools only; recovery plan `page_refresh_recovery.yml:99-141` validates event/run-id only; grid applies explicit input `grid_capacity_repair.yml:68-78`. Dispatching a branch copy targeting the same live default Sheet computes another group and can bypass main's exclusion. Environment protection rules may constrain the two production-environment jobs; they were not inspected, and grid repair has no environment declaration. This is a static conditional exposure; current metadata contains no non-main dispatch.

8. **P3 ineffective CI cancellation in two audit workflows.** `decision_surface_freshness.yml:35-37` and `full_refresh_coverage.yml:33-35` include `github.run_id` in the group for push/PR events, making every run unique while setting cancel-in-progress=true. Successive CI commits cannot cancel one another. Source-run separation for live audits remains useful; the CI group needs an event-specific stable ref if cancellation is intended.

9. **Date/clock evidence limits.** PIT target 20:10 UTC / 23:10 Riyadh (`pit_snapshot.yml:39`) crossed local midnight on each Oct13–15 run: 23:42:22Z/23:09:22Z/23:00:44Z, IDs 38485856648/38652318596/38820897458. `scripts/pit_snapshot.py:187-188,371` truthfully dates the actual next Riyadh day. These runs cannot be represented as that preceding day's captured evidence. Do not backdate. Tracker's first target 11:47 UTC=14:47 Riyadh (`track_performance.yml:82`) is labeled after Tadawul close, but the intraday workflow defines the close as 12:00 UTC=15:00 Riyadh (`intraday_refresh.yml:94-95`): the target is 13 minutes before close. All six current actual tracker starts were later; no preclose write was observed. PIT's fixed 20:10 UTC is after NYSE close during US DST but before its 21:00 UTC winter close; a claim of universally post-US-close scheduling needs a calendar-aware condition.

## Complete scheduled cron and concurrency inventory

Riyadh is UTC+3 year-round. Weekdays in the UTC columns are cron weekdays. Group `{ref}` means `${{ github.ref }}`; `{workflow}` means `${{ github.workflow }}`. A false cancellation setting preserves the running member, not an unlimited FIFO: only one pending member is retained.

| Workflow (cron line) | UTC target | Riyadh target | Concurrency group (block line) | Cancel running? |
|---|---|---|---|---|
| acceptance_check.yml:13 | Daily 06:45 | Daily 09:45 | tfb-acceptance-{ref} (:27) | false |
| backtest_harness.yml:14 | Sun 13:00 | Sun 16:00 | backtest-harness-{ref} (:36) | false |
| backtest_signals.yml:13 | Fri 05:00 | Fri 08:00 | tfb-backtest-{ref} (:26) | false |
| board_scorecard.yml:31 | Mon–Fri 22:23 | Tue–Sat 01:23 | tfb-board-scorecard (:45) | false |
| calendar_sync.yml:30 | Daily 05:15 | Daily 08:15 | {workflow}-{ref} (:39) | true |
| daily_brief.yml:35,37,39,41 | Sun–Thu 07:30,10:30,15:00,19:00 | Sun–Thu 10:30,13:30,18:00,22:00 | {workflow}-{ref} (:56) | true |
| daily_sync.yml:36 | Daily 04:17,12:17,20:17 | Daily 07:17,15:17,23:17 | production events: tadawul-production-write-{ref}; push/PR: tadawul-ci-{workflow}-{event}-{ref} (:108) | false production; true push/PR |
| dependency_typing_review.yml:14 | Mon 08:43 | Mon 11:43 | None | — |
| digest.yml:32-34 | Daily 05:30,07:00,13:30 | Daily 08:30,10:00,16:30 | tfb-digest (:82) | false |
| intraday_refresh.yml:93,95 | Sun–Thu every 15m 07:00–11:45, plus 12:00 | Sun–Thu every 15m 10:00–14:45, plus 15:00 | intraday-quote-refresh (:103) | true |
| investment_policy_shadow.yml:7 | Daily 02:15 | Daily 05:15 | investment-policy-shadow (:12) | true |
| news_archive.yml:14 | Daily 14:00 | Daily 17:00 | news-archive-{ref} (:36) | false |
| pit_snapshot.yml:39 | Daily 20:10 | Daily 23:10 | tfb-pit-snapshot (:62) | false |
| provider_target_coverage.yml:32 | Daily 06:20 | Daily 09:20 | provider-target-coverage (:51) | false |
| shadow_board.yml:32-33 | Daily 05:10,14:10 | Daily 08:10,17:10 | tfb-shadow-board (:62) | false |
| shadow_scorer.yml:23 | Daily 15:20 | Daily 18:20 | None | — |
| track_performance.yml:82-83 | Daily 11:47,15:47 | Daily 14:47,18:47 | tfb-performance-clock (:129) | false |
| weekly_brief.yml:11 | Sun 04:10 | Sun 07:10 | None | — |
| workbook_backup.yml:69 | Daily 23:30 | Next day 02:30 | tfb-workbook-backup (:85) | false |

## Remaining 17 workflows: triggers, concurrency and live gates

| Workflow | Triggers / branch gate | Concurrency and cancel |
|---|---|---|
| actions_health_review.yml | manual; special-branch workflow-file push | actions-health-review-{ref}, false (:19) |
| ci.yml | push with paths; PR; manual | ci-{ref}, true (:426) |
| core_repair_readback.yml | manual; readback job main-only (:57); optional name evidence read-only | core-repair-readback, false (:17) |
| decision_surface_freshness.yml | path CI main push/PR; completed Daily Sync; manual. Live automatic gate source head_branch=main and source event schedule/manual (:78-88) | decision-surface-{sourceRun or runID}-{ref}; true CI, false live (:35) |
| full_refresh_coverage.yml | path CI main push/PR; completed Daily Sync; manual. Live automatic gate source head_branch=main and source event schedule/manual (:76-86) | full-refresh-coverage-{sourceRun or runID}-{ref}; true CI, false live (:33) |
| grid_capacity_repair.yml | manual; apply+backup+confirmation inputs; no main guard | tadawul-production-write-{ref}, false (:47) |
| investment_policy_ci.yml | path push; PR; manual | investment-policy-ci-{ref}, true (:20) |
| manual_refresh_priority.yml | path main/master push/PR; manual; contract tests only | manual-refresh-priority-{workflow}-{ref}, true (:22) |
| page_refresh_recovery.yml | path main push/PR; manual historical-run recovery only, no workflow_run auto trigger | production manual: tadawul-production-write-{ref}; CI page-recovery-ci-{event}-{ref}; true CI/false production (:57) |
| release_decision_feed.yml | manual, rejects non-main before checkout (:113-115); gates default off | tfb-decision-release, false (:72) |
| repair_stores.yml | manual; explicit apply, default dry-run; no main guard | tfb-repair-stores, false (:32) |
| repository_workflow_audit.yml | path push; PR; manual; read-only | repository-workflow-audit-{ref}, true (:17) |
| runner_image_canary.yml | manual; read-only | runner-image-canary-{image}-{ref}, false (:39) |
| sync_outcome_audit.yml | path main push/PR; completed Daily Sync; manual. Automatic live only source schedule (:61-65) | sync-outcome-audit-{sourceRun or inputRun or ref}, true (:31) |
| tests.yml | path push/PR; manual; read-only | None |
| thesis_exit_backtest.yml | manual | tfb-thesis-backtest-{ref}, false (:25) |
| workflow_repair_tests.yml | path main/special-branch push/PR; manual; verify-live main-only push/manual (:70) | workflow-repair-{ref}; cancel PR only (:34) |

## Disproved or bounded risks

- Daily Sync primary matrix plus inline recovery and final verification remain within ONE same-ref production workflow lease; Page Recovery/manual and grid use that same literal prefix. The split CI groups deliberately avoid evicting a pending production writer. `daily_sync.yml:100-110,1625-1630,2026-2031`; `page_refresh_recovery.yml:55-59`.
- Daily Sync's three matrix scopes target different pages and fail-fast=false (`daily_sync.yml:279-294`); the embedded tracker is gated to the global-markets leg (`:1304`). A blanket claim of three duplicate in-sync trackers is false.
- Release Feed's private group is intentional. `release_decision_feed.yml:23-35` documents fail-closed in-flight probes plus reread/CAS in its script. Joining the GitHub production group would introduce pending-run eviction. Its queued kill can itself be superseded; the sticky kill Variable is the documented mechanism for blocking later promotions. No Variable was read or changed here.
- Shadow Board, PIT, Digest, Workbook Backup and Board Scorecard use fixed groups (no branch/event escape) and main-pinned code (`shadow_board.yml:105`, `pit_snapshot.yml:80`, `digest.yml:114`, `workbook_backup.yml:120`, `board_scorecard.yml:66`). Those past concurrency fixes remain present.
- Core readback is read-only; manual_refresh_priority only runs contract tests; Calendar atomically replaces its own Calendar_Events tab (`scripts/run_calendar_sync.py:498-503`); PIT appends its private archive; backup exports and appends a log. Sharing one workbook alone is not proof of destructive overlapping cell writes. Daily Brief/Digest mail can duplicate on manual dispatch but do not rewrite market pages.
- Dependency/typing review and tests.yml lack concurrency but are read-only. Their missing blocks alone are not data-corruption findings.
- The shadow scorer's board-fresh guard remains observe (`shadow_scorer.yml:64`), so cron spacing is not an enforced dependency, but all three recent scheduled scorer runs started after the afternoon board finished. No recent stale-board scoring is proved from workflow metadata.
