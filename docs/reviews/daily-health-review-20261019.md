# Daily health review, 19 October 2026 (morning cycle)

Date: 2026-10-19, 07:00 to 08:30 UTC (10:00 to 11:30 Riyadh). Base read at
`4c9e2cb` (HEAD of `main`, also the live Render commit). Method: authenticated
read of the Render service, its events and logs; the GitHub Actions run list,
the failed-job logs and the three audit artifacts of the latest scheduled sync;
the IBKR account through the read-only connector; the repository at HEAD. No
live writer, workbook cell, provider, email, Render setting, deploy or order
was touched. No account amounts, quantities or costs are reproduced here.

## 1. Service and deployment

| Check | Observed | Verdict |
| --- | --- | --- |
| Render `tadawul-fast-bridge` | `live`, plan pro, Frankfurt, auto-deploy off | healthy |
| Deployed commit | `4c9e2cb` (#793), redeployed by the owner 05:57 and 06:58 UTC, both succeeded | matches `main` |
| `/health` | HTTP 200 in 0.73 s, `app_version` 8.14.5 (the stale 5.111.0 ENV override of the 13 Oct review is gone), engine 5.154.0 ready, 7 route modules, 0 failed mounts, auth enforced | healthy |
| Render events (7 days) | builds and deploys only; no `server_failed`, OOM, restart or disk event | clean |
| Render error/warning log, 18 Oct 04:00 UTC onwards | Yahoo chart rate limits during the 23:30 UTC sync (`Too Many Requests` for IBMJ, VFLQ, IGN, HEWW, IBDO.US); Top-10 selector soft budget exhausted before Commodities_FX / My_Portfolio at 23:30 UTC; `[FRESHNESS v1.26.1] venue calendar unavailable` for 25 suffixes on both the 23:46 and the 05:08 UTC runs; worker SIGTERMs at the two owner redeploys | one actionable item (section 4) |

## 2. Scheduled workflows (last 24 h)

| Workflow | Run | Result | Cause |
| --- | --- | --- | --- |
| TFB Workbook Backup | #102, 01:25 UTC | FAIL (exit 3) | Export succeeded (24,273,892 bytes, sha `9e92d34e1a73`, 15.4 s). The single 24 MB PUT to the Drive resumable session died with `SSLError(SSLEOFError(8, 'EOF occurred in violation of protocol'))`. v1.5.0 made no status query and no retry, so the day has no backup. Runs #93 to #101 (12 to 18 Oct) all succeeded, so this is a transport fault, not a permission change; the generic hint "is the folder shared EDITOR" was wrong for this failure. Fixed in this change set (section 4). |
| Tadawul Dashboard Advanced Sync | #4918, 22:28 UTC | FAIL at `Verify final decision publication` | All five source legs wrote successfully (My_Portfolio 4/4, Market_Leaders 253/255 fresh, Global_Markets 6,585/6,613, Commodities_FX 432/453, Mutual_Funds 2,390/2,474). Coverage audit: 0 failures, 1 warning (8 blank or date-only timestamps on Global_Markets). Decision audit: 8 failures, all in the cockpit layer (below). Publisher v1.1.1 returned `[1, 2]`, published the blocker, did not promote the feed. This is the twelfth consecutive scheduled failure since 15 Oct with the same shape. |
| Full Refresh Coverage Audit | #2145 | FAIL (wrapper exit 1) | Same 1 warning; the wrapper intentionally fails on warning exit 1. |
| Decision Surface Freshness | #2135 | FAIL (exit 2) | 6 failures: `PF_OLDER_THAN_SOURCE`, four `T10_OLDER_THAN_SOURCE`, `T10_POOL_RECONCILIATION_UNPROVEN`. |
| Sync Outcome Audit #2320, PIT Snapshot #90, Daily Brief #318/#319, Performance Clock #146, Shadow Scorer #96 | | PASS | |

### Why the decision gate still fails

The cockpits refreshed before the sources they must consume: Top_10 ran at
00:54 Riyadh and Portfolio_Decision at 01:00, while My_Portfolio landed at
01:29, Market_Leaders at 01:32, Commodities_FX at 01:36, Mutual_Funds at 01:57
and Global_Markets at 02:28. Neither cockpit then emitted a
`source readiness VERIFIED (run 39172358424)` receipt, and the Top-10 pool
reconciliation token does not match the consumed cohort. This is exactly
blocker 2 of the 13 Oct review (ordering plus the missing Portfolio_Decision
receipt). The repository already carries the fix path, unmerged:

- #788 `26_Decision_Chain v1.0.0` refreshes Portfolio_Decision and Top_10 when
  the sync lands (fixes the ordering).
- #776 (merged) `10_My_Portfolio v1.5.13` binds the receipt to the rows read
  but is default off and needs the live Apps Script install.
- #791 and #792 (codex, 17 Oct) complete the receipts and refresh integrity.

No code in `main` can make this gate pass until the cockpit side is installed
and armed; this cycle therefore did not touch the gate.

Coverage note: Global_Markets accepted-name coverage is 99.77% (floor 99%),
up from 97.00% on 14 Oct and 97.79% on 13 Oct. The name floor that kept the
coverage audit red for two weeks has cleared.

## 3. IBKR read (read-only connector)

The connector returned three US stock positions and the account summary. The
three symbols match the `active_holdings` the coverage audit read from
My_Portfolio (`CWBC.US`, `DDI.US`, `KRP.US`; `5023.SR` is the Saudi sukuk, not
held at IBKR), with no extra or missing US line. Position quantities agree
with the workbook membership the 14 Oct review certified. Leverage is below
1x and all three positions carry a current market price. Amounts, quantities
and P&L are deliberately not recorded here; the owner can compare them in the
cockpit. Nothing in this read changes the custody evidence status: the
connector proves the broker view at 07:16 UTC, not a signed statement.

## 4. Built in this change set

Both changes are small, additive and covered by new suites enrolled in
blocking `ci.yml` jobs (v1.1.21): the upload suite in the lean lane, where it
also exercises the helper's no-`requests` fallback, and the venue suite in
the data-engine job, the one lane that installs the pinned
`exchange_calendars` and pandas. Deployment manifest pins are updated in the
same commit (`tests/test_verify_manifest_pins.py` passes), and the P11 pin in
`tests/test_workbook_dq_audit.py` moves to 1.5.1.

### 4.1 `scripts/backup_workbook.py` v1.5.1: the resumable upload resumes

One additive helper, `_resumable_put_with_resume()`, now carries the data
PUT for both the Drive and the GCS backend. On a transport fault
(`ConnectionError` including `SSLError`, `Timeout`, `ChunkedEncodingError`)
or a transient status (408, 429, 5xx) it sleeps with bounded backoff (2, 4,
8, 16, 30 s), asks the session where it stopped (`Content-Range: bytes */N`,
empty body), and re-sends only the bytes after the server's `Range` end with
the matching `Content-Range`. A 308 to a data PUT is followed without
sleeping. A probe answering 200 or 201 means the server already holds the
file and that resource is returned. A definite 4xx raises the same
`HTTPError` as before on the first answer, so the v1.2.0 diagnosis path
(`_api_reason`, 403 and 404 remedies) is unchanged. Attempts are bounded by
`TFB_BACKUP_UPLOAD_RETRIES` (default 4, first send included); the last fault
is re-raised unchanged. The clean path is byte-identical to v1.5.0 (one POST,
one PUT, same headers, same JSON). Retry lines never print the session URL
(it carries the `upload_id`).

Rollback: revert the file, or set the repository variable
`TFB_BACKUP_UPLOAD_RETRIES=1`.

Regression suite: `tests/test_backup_upload_resume.py` (T1 to T10, 24 tests,
scripted fake session, no network, no credentials).

### 4.2 `core/analysis/opportunity_builder.py` v1.26.2: venue calendars for the live global page

`_venue_state()` knew 16 suffixes. The live Global_Markets page trades on
about 25 more, so every quote on those venues older than
`TFB_TICKET_MAX_QUOTE_AGE_MIN` (15 min) was `UNVERIFIED_PRICE: Xh old and
venue calendar unavailable`, even when it was exactly the venue's last close.
The precise path simply had no calendar to consult. v1.26.2 adds
`_VENUE_CAL_EXT_MAP` (39 suffixes to ISO-10383 MICs, each verified to exist
and build in the pinned `exchange_calendars==4.13.2`), consulted only when
the base map misses, through the single lookup `_venue_cal_code()`. The
verdict logic is untouched: those venues now get the same three answers the
16 old ones get (`session_open` stale, `session_closed` last close, pre-close
stale). Venues with no calendar in 4.13.2 (`.AT`, `.NS`, `.VN`, `.AE`, `.QA`,
`.BH`) are left out on purpose, keep the uncertified verdict and keep
logging.

The kill switch is `TFB_TICKET_VENUE_CAL_EXT=0` (default on). It sits in the
`TFB_TICKET_` family so the replay fingerprint of the board funding basis
binds it like every other freshness policy read
(`test_all_direct_builder_policy_reads_are_bound_to_replay_fingerprint`).

Cost: one lazy calendar build per new venue per worker process, 0.2 to 0.5 s
each, except XKRX (about 4 s), XTAE (1.6 s) and XBOM (1.3 s); cached after
that. The Top-10 selector already logs soft-budget exhaustion on the
23:30 UTC run, so the first request after a deploy that touches a Korean,
Israeli or Indian candidate pays that build once. If that proves to matter,
the follow-up is a warm-up at engine start, not a smaller map.

What changes on the surface: a candidate on an extended venue whose quote
instant is at or after the venue's previous close can now pass the
executable freshness check. Everything else that gates a ticket (acquisition
proof, identity, scoring settlement, compliance, trust, funding) is
unchanged, and the decision feed stays NOT_ACTIONABLE until the cockpit
receipts land (section 2).

Rollback: `TFB_TICKET_VENUE_CAL_EXT=0`, or revert.

Regression suite: `tests/test_venue_calendar_ext.py` (T1 to T7, 37 tests,
real calendars through the real `_quote_freshness_assessment`).

## 5. Verification

Offline, Python 3.11.17, pinned numpy/pandas/exchange_calendars:

- new suites: 61 passed;
- neighbouring suites (`test_ob_quote_evidence`, `test_executable_quote_conflicts`,
  `test_opportunity_builder`, `test_verify_manifest_pins`, `test_flag_registry`,
  `test_learning_guards`, `test_ob_board_funding`, `test_ob_research_diversify`,
  `test_ob_settle_unresolved`) produce the same pass/fail set as unmodified
  `main` in this container plus the 61 new passes; the first draft used a
  `TFB_VENUE_CAL_EXT` name outside the fingerprinted families and the replay
  fingerprint guard caught it, which is why the switch is `TFB_TICKET_...`;
- `ruff check --select E9,F63,F7,F82` clean on the four touched Python files;
- `py_compile` clean;
- first CI round on `01b4ad6` failed twice and both were this PR's: the lean
  lane has no `requests`, so the upload suite's module-level import broke
  collection (now lane-independent, verified locally with `requests` blocked),
  and the P11 harness pinned `backup_workbook` at 1.5.0 (moved to 1.5.1). The
  venue suite also moved from the lean lane, where it would have skipped, to
  the data-engine job.

The full PR CI run on the branch is the authoritative receipt; see the PR.

## 6. Not built, and why

- The decision gate (section 2): needs the cockpit install and #788; nothing
  in `main` can supply the receipts.
- `docs/flags.md` regeneration: `scripts/flag_registry.py` output already
  differs from the committed file by about 1,100 lines on `main`; regenerating
  inside this change would bury the two real edits. Separate docs-only PR.
- Yahoo chart rate limits during the night sync: the provider already backs
  off and the rows fall to the next provider; no evidence of lost rows in the
  coverage audit (0 failures). Watch, do not change.
- Backup folder variable: `TFB_BACKUP_FOLDER_ID` holds a full URL and is
  normalised on every run (logged NOTE). Owner can paste the bare id
  `16g8x1ll0xTqfMHlGdnS6zKaQH5MLGAga` into the repository variable; no code
  needed.

## 7. Owner checks (minutes each)

1. Merge order for the decision gate: #788, then install and arm the cockpit
   files (#776 is already in `main`); then re-run only the
   `Verify final decision publication` job of the next scheduled sync.
2. After merging this PR, the next scheduled backup (23:30 UTC) is the
   receipt: expect either a clean single PUT or `upload retry k/4 ... resuming
   at byte N` followed by `uploaded DAILY_...`.
3. After the next Render deploy carrying 1.26.2, the
   `venue calendar unavailable` lines should shrink from 25 suffixes to the
   six listed above. If a venue verdict looks wrong, set
   `TFB_TICKET_VENUE_CAL_EXT=0` in the Render ENV and report the symbol.
4. Twenty-three PRs are open (#772 to #795); most are default-off and each
   carries its own commit sheet. The decision layer cannot improve day over
   day while they wait, so a merge session is the highest-value hour.

## 8. Midday cycle (11:00 to 13:00 Riyadh): the two decision pages

Owner exports reviewed: `Portfolio_Decision` at 11:08 (4 BLOCK) and 11:25 (3 HOLD, certified), `Top_10_Investments` at 11:29.

### Portfolio_Decision: BLOCK cleared the designed way

The 11:08 page was BLOCK because the only IBKR capture of the day (08:41) was 2h27m old at a 14-minute limit, and
My_Portfolio was 9h39m old at the 8-hour receipt window. The 4-hourly refresh overwrote the certified 08:45 page,
exactly the known limit of the v1.5.12 commit sheet. Actions taken: Render `TFB_PF_RESEARCH_VIEW=enforce` (P0-6
research view, owner-merged on 13 Oct), a fresh on-demand capture written to the evidence folder at 11:14:50
(same read-only connector, same schema, honest producer label), owner refresh at 11:25 -> `ibkr evidence SENT
(capture 11:14 age 10m; linked 3/3)`, three HOLD rows with stop/TP levels, `low_confidence_capped` alert (one
TRIM capped to HOLD by `reliability ?`). Owner guidance given: `PD_IBKR_WATCH_WINDOW=always` (Script Property)
so each capture renders within 5 minutes; D4 (daily custody tier) remains the structural fix for the 15-minute
evidence window versus the 4-hourly refresh.

### Top_10_Investments: the stopper is procedural

12 names qualify on merit; `source readiness VERIFIED (run 39172358424)`; the pool reconciliation token is
present. Sizing is withheld because `TFB Decision Feed` reads `NOT_ACTIONABLE(final_publication)` and is 536
minutes old. The feed is downgrade-only; promotion needs `release_decision_feed.yml`, which was built on 13 Oct
as a manual dispatch and was never dispatched. Delivered in the follow-up PR (commit sheet
`docs/evidence/TFB_Commit_Sheet_release_decision_feed_v1.2.0_2026-10-19.md`): scheduled release attempts with
automatic cohort discovery, and the decision chain (#788, ported) extended to re-render Top_10 after a release.
Nothing is armed by that PR; the arming list is in the sheet.

### Allocation (owner request, same PR, second commit)

The export shows every sized seat at the per-seat cap (`min(15% x budget, remaining)`), so seats read as
equal; the owner wants weights by expected ROI and risk, a small absolute cash buffer (USD 500 to 1,000) instead
of a 10% target, and sell-to-fund rotation when cash is short. The rotation rule already exists
(`FUNDABLE_BY_ROTATION`, engine-ROI edge >= 8 pp after 1.1% cost, one per run, sukuk excluded). Delivered as
opportunity_builder v1.27.0 (commit sheet `docs/evidence/TFB_Commit_Sheet_opportunity_builder_v1.27.0_2026-10-19.md`):
`TFB_OPP_SIZING_MODE=conviction` (default `cap`, byte-identical) water-fills the post-floor budget by plan ROI x
risk x reliability under the per-seat cap and tops up the rounding remainder; and the absolute cash buffer
(`TFB_OPP_CASH_FLOOR_SAR` with an explicit `TFB_OPP_CASH_FLOOR_MODE=enforce`) now binds the sizing budget, which
on v1.16.0 to v1.26.2 it did not (label-only without a percent floor). Render ENV set this session:
`TFB_OPP_CASH_FLOOR_SAR=3750`, `TFB_OPP_CASH_FLOOR_MODE=enforce`, `TFB_OPP_CASH_FLOOR_PCT=` (off); the sizing mode
is armed only after the 1.27.0 deploy.

## 9. Apps Script review (owner request, 12:30 Riyadh)

Scope: the six files the repository mirrors (`apps_script/10_My_Portfolio.gs` 1.5.13, `11_Manual_Refresh_Coordinator.gs` 1.1.0,
`16_Decision_Top10.gs` 1.13.9, `24_Sync_Dispatch.gs` 1.1.0, `26_Decision_Chain.gs` 1.0.1) against today's backend
changes and the live page exports. The live project holds about 25 files (names seen in commit sheets: `00_Config`,
`01_Menu`, `03_Schema`, `05_Refresh`, `06_Market_Leaders` to `09_Mutual_Funds`, `11_SpecialPages`, `13_AutoRefresh`,
`14_Portfolio_PnL`, `15_Lists_Config`, `17_Selection_Log`, `18_Decision_Diagnostics`, `21_Portfolio_Ledger`,
`23_Refresh_Chain`, `25_Trade_Notes`, `25_Divergence_Log`, `99_Diag`, `99_Data_Audit`, `_base`). Those are not in the
repository and cannot be reviewed from here. The mirrored code calls nine helpers that exist only in those files
(`getAppToken_`, `getBackendUrl_`, `getMappedHeaderCoverage_`, `getWorkingSymbolsForPage_`, `readCostBasisMap_`,
`refreshPageInBatches_`, `refreshPage_`, `refreshSelectedRow_`, `writePageStatus_`), so the mirror is a partial one by
design; a full `clasp push` from it would delete the rest (14 Oct review, blocker 5).

### Verdicts

| Item | Verdict |
| --- | --- |
| All native harnesses on the mirror (`tests/js/*.test.js`) | green: syntax 5/5, board funding 64, decision chain 17/17, input truth 20 (+3 dual-tree skips), source readiness 73, feed cohort binding 21/21, IBKR evidence 14/14, source receipt 8/8, reconciliation inputs 20 |
| Installed versions, read from the page exports | `16_Decision_Top10` is 1.13.9 or later (`Entered cash (SAR)` KPI label, `pool_reconciliation_v1` token present); `10_My_Portfolio` is 1.5.12 or later (`ibkr evidence SENT`, receipt line present; 1.5.13 is indistinguishable from the export). `11` and `24` cannot be read from the exports. |
| Compatibility with opportunity_builder 1.27.0 (`meta.sizing`, conviction tickets) | no change needed: the cockpit renders ticket fields from the payload and never assumes equal sizes; `meta.sizing` is an additive key it ignores |
| Compatibility with the binding absolute cash floor | no change needed: the cockpit passes `cash_floor` alerts through by type (`dt10BoardFundingAlert_`, line 2342) and copies `meta.cash_floor`; it does not parse the alert text, whose wording changed for the absolute case |
| Compatibility with the scheduled release (release grammar, 480-minute clock) | no change needed: `dt10UvParse_` is the parser the Python harness proves parity against; `DT10_UV_MAX_AGE_MIN` 480 equals `RELEASE_TTL_MIN` |
| Compatibility with `TFB_PF_RESEARCH_VIEW=enforce` | no change needed: Portfolio_Decision renders `action_reason` / `advisor_note` verbatim, so the research verdict text appears in the note |

### Required updates (owner, Apps Script editor)

1. **Install `26_Decision_Chain` v1.0.1** (new file from #797), run `tfbInstallDecisionChain`, set `TFB_DECISION_CHAIN=observe`,
   then `on` after one sync shows `OBSERVE` rows. This is the file that makes the release automation visible on the board.
2. **Script Property `DT10_FEED_COHORT_BIND=on`** (prerequisite 2 of the release; the code is already installed in 1.13.8+).
3. **Script Property `PD_IBKR_WATCH_WINDOW=always`** so every capture renders within 5 minutes (given earlier today).
4. **Paste discipline**: the three pending Apps Script PRs conflict with each other and with `main` on the same files:
   #780 (`10_My_Portfolio` 1.5.14, panel inputs survive a layout rebuild), #782 (`24_Sync_Dispatch` 1.2.0, loud trigger
   failures, a precondition for arming the dispatcher), #791 and #792 (codex, 17 Oct: both edit `10_My_Portfolio` and
   `16_Decision_Top10`; #792 also carries a different 679-line `26_Decision_Chain`). Since #797 merged the #788 chain,
   #792's chain file is superseded; merge #780 and #782 first, then rebase or close #791/#792 against them.

### Data-quality finding that is not a script defect

Both Portfolio_Decision exports show `Rel` and `DQ` blank for all three US holdings and the backend writes
`Reliability ? below add minimum 70` and `Low confidence (reliability ?) capped TRIM -> HOLD`. The script sends
`Forecast Reliability Score` and `Data Quality Score` only when the My_Portfolio header resolves
(`PD_HOLDING_FIELDS`, exact header names, `pdSelfTest` reports `hasForecastReliability`). Global_Markets carries
reliability 71.5 and DQ 100 for the same DDI.US row, so the value exists in the engine. Until My_Portfolio carries it,
no holding can reach ADD and every TRIM is capped to HOLD. Owner check: `pdSelfTest` line for the two headers, then the
two cells for DDI.US / CWBC.US / KRP.US on My_Portfolio. If the headers resolve and the cells are blank, the gap is in
the sync's My_Portfolio leg (backend), and that is the next thing to fix in Python, not in Apps Script.

### Recommendation on the mirror

Export the whole bound project into `apps_script/` (every file, verbatim) in one docs-only PR, so the next review can
cover the 19 unmirrored files and the syntax harness compiles all of them. Nothing in that export runs from GitHub.

## 10. Afternoon cycle (12:40 to 13:40 Riyadh): why My_Portfolio reached the cockpit without reliability

Section 9 left one open question: are the `Forecast Reliability Score` / `Data Quality Score` cells blank on
My_Portfolio, and if so where in the Python side do they go missing. Answered without the owner's check, from three
artifacts and two offline reproductions (details and the audit table in
`docs/evidence/TFB_Commit_Sheet_enriched_quote_v8.6.0_2026-10-19.md`).

| Step | Evidence | Reading |
| --- | --- | --- |
| Sync run #4918, core-pages job | `[DECISION-RECONCILE] page=My_Portfolio ... rows=4 changed=3 distinct=[('ACCUMULATE', ''), ('BUY', ''), ('HOLD', '')]` | `Final Action` blank on 4/4 rows; three buy-family rows demoted to WATCH / HOLD because the Block Reason cell read `"None"` (the response carried `None`, a gate that ran writes `""`) |
| DQ audit artifact of 17 Oct | DQ-51 `weight_basis` FAIL 4/4 blank weights; DQ-34 0 My_Portfolio rows | the portfolio pass (`_compute_portfolio_fields`) never ran on the rows the sheet received |
| Render request log | one `POST /v1/enriched/sheet-rows 200` for the page; the four market pages use `/v1/analysis/sheet-rows` | only My_Portfolio takes the enriched route |
| Offline engine reproduction | `get_sheet_rows("My_Portfolio")` with a fake sheet reader and fake quotes, default guards and the production `[GUARDS]` line | all eight gate columns populated: the engine path is sound |
| Offline route reproduction | `inspect.signature(_analysis_sheet_rows_impl).bind(**attempt)` for the eight bridge attempts | `TypeError` on all eight (missing `x_api_key`, then `include_matrix_q` / `mode` / `request`): the enriched route's bridge never bound and every request fell silently to the engine-direct path, which skips `_strict_project_row` and therefore the investability gate |

So the cockpit's `reliability ?` is a route defect, not a sheet or Apps Script defect: My_Portfolio has been written
from ungated engine-direct rows on every sync (and, if the 07:40 Apps Script refresh uses the same endpoint, on every
auto-refresh) at least since the analysis implementation gained its ninth parameter on 6 Oct (#719).

**Built:** `routes/enriched_quote.py` v8.6.0. The bridge binds the implementation's own signature (attempt 0; a typed
mismatch now logs a WARNING instead of falling through silently), and the engine-direct fallback publishes each row
through the engine's publication boundary so even a bridge timeout yields gated decision columns. Both default ON with
kill switches (`TFB_ENRICHED_BRIDGE_SIGBIND`, `TFB_ENRICHED_FALLBACK_BOUNDARY`). Tests
`tests/test_enriched_bridge_binding.py` B1-B10 (10 passed), the route neighbours green after the W15 pin move, ci.yml
1.1.24 enrols the suite in the schema-alignment step.

**Also fixed:** the lean-lane flake on this PR's push run (`test_off_explicit_zero_equals_unset`, `age_min` 4.3 vs 4.4
between two builds seconds apart): the identity comparison now scrubs that clock-derived field.

**Owner, after merge and the Render deploy of `main`:** read-back list in the commit sheet (the reconcile log line with
real action tokens, `Rel` / `DQ` numbers on Portfolio_Decision, no `bridge typed mismatch` line in Render, DQ-51 passing
on the next backup audit, and one look at which endpoint the 07:40 Apps Script refresh uses).

## 11. Afternoon read-back (14:17 to 16:30 Riyadh): what still stops the Top_10 board

Inputs: the owner's Top_10 export rendered at 14:17:31 Riyadh, the in-sync final audit of run 39234358605 (14:01 Riyadh),
and one release attempt dispatched from this session at 14:32 Riyadh (run 39245386428).

**Working now.** The board runs builder 1.27.0 and consumed the current cohort (`source readiness VERIFIED (run
39234358605)`). The cash buffer is enforced (the board's cash-floor line reads `ENFORCED`; its deployable figure is an
account amount and is not copied here). Eighteen names qualified and Rel/DQ are populated on the board. The Render deploy of enriched_quote 8.6.0 went live at 13:53 Riyadh, after
that sync's My_Portfolio leg (13:02 to 13:07), so My_Portfolio's decision columns come back with the next sync.

**Release attempt.** The repository variable `TFB_DECISION_RELEASE` is already 1, so the 14:32 dispatch, intended as a
dry run, ran armed. It was refused (exit 2, `not_accepted`) and wrote nothing (`released false, downgrade null,
feed_promoted false`). The 13:45 Riyadh scheduled slot did not run at all; the dispatch is run #1 of the workflow.

**Refusal reasons, in full.** Decision: only `PF_SOURCE_CONSUMPTION_UNPROVEN` and `PF_OLDER_THAN_SOURCE`. Portfolio_Decision
last rendered at 11:25:14 Riyadh, before the My_Portfolio publication at 13:02:21, and nothing re-renders it after a sync
unless 26_Decision_Chain is installed. Every Top_10 check passed. Coverage: exit code 1 from one warning, Global_Markets
`7 blank/unparseable/date-only timestamp(s)`, standing since at least 18 Oct (8 then). `accepted()` needs both exit codes 0.

**Root cause of the warning** (multi-agent investigation, root cause confirmed by an adversarial reviewer): the 7 rows are
FW-2 identity-firewall stubs that FW-KEEP cannot restore. FW-2 quarantined 47 rows, nearly all Hong Kong-listed mainland
issuers quoting in HKD and reporting EPS in CNY; the identity check has a pence/pound exemption but none for this.
Built: `run_dashboard_sync` v6.70.0, a `.HK`-only HKD-per-CNY / HKD-per-USD band in the shared unit-convention helper,
with ratio diagnostics (commit sheet `docs/evidence/TFB_Commit_Sheet_run_dashboard_sync_v6.70.0_2026-10-19.md`).
Two other designs were rejected by the reviewers because they weaken what `accepted()` admits (stamping the stubs, or
having the audit ignore them). Twelve of the fifteen symbols the log names are `.HK`; SGH.AX, HKIT.US and SIM.US are
not, and v6.69.0 never named its 7 stubs, so how many stubs the band heals is known only after the next sync's FW-KEEP
line names what is left. Any non-`.HK` stub keeps the warning until its logged ratio is read. The manual repair tool's copy
of the identity check (`scripts/repair_stores.py` 1.0.1) takes the same band, so an apply run cannot re-blank these rows.

**New finding: caption registry expiry.** `data/verified_name_labels.json` holds 122 reviewed Name captions with a
seven-day review window. 55 expired today (39 Mutual_Funds at 12:31 UTC, 16 Global_Markets); 67 Global_Markets captions
expire on 21 Oct 07:44 UTC. Mutual_Funds allows 24 blank Names (99% floor) and had 11 with the captions in force, so the
next sync should fail the Mutual_Funds Name floor (a coverage failure), and Global_Markets follows on 21 Oct. Renewal means
re-capturing each binding's public sources and writing a fresh receipt; it is open on the system side. The same expiry
broke `tests/test_sheet_payload_digest.py` on main (it read the registry against the wall clock); fixed in this PR.

**Owner actions still required for an EXECUTABLE board**
1. Install `26_Decision_Chain` v1.0.1 (`tfbDecisionChainStatus`, Script Property `TFB_DECISION_CHAIN=observe`,
   `tfbInstallDecisionChain`, then `on` after one sync shows OBSERVE rows). It is the only automatic path that renders
   Portfolio_Decision after the sync publishes My_Portfolio. Without it, run `refreshPortfolioDecision` by hand after each
   sync's My_Portfolio stamp and before any Apps Script rewrite of My_Portfolio.
2. Stop the Apps Script auto refresh from rewriting My_Portfolio rows between the sync's publication and the
   Portfolio_Decision render (about 20 minutes after each sync); a rewrite in that gap makes the receipt WITHHELD.
3. Set `DT10_FEED_COHORT_BIND=on` (arming prerequisite 2; the release is already armed without it).
4. Arming preconditions: restrict the `production` environment to `main` and keep `TFB_GH_DISPATCH_REF` unset or `main`
   (B); for A, only deleting the pre-#763 sync runs or an explicit waiver satisfies it before about 12 Nov.
5. Optional: `PD_IBKR_WATCH_WINDOW=always` only after item 2; add the `USX` FX rate in `_Lists_Config` (13 rows alert).
