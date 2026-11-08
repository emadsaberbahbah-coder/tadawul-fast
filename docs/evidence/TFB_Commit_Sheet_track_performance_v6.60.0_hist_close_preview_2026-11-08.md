# TFB Commit Sheet - track_performance 6.60.0 [HIST-PREVIEW]: observe shows what enforce would publish

| Item | Value |
| --- | --- |
| Date | 2026-11-08 (daily improvement routine) |
| Branch | `claude/daily-2026-11-08` |
| Base | origin/main `c7e666a4` (track_performance v6.53.0, track_performance.yml 1.0.7, ci.yml 1.1.67) |
| Lane | GitHub Actions only: track_performance.yml 'Run performance clock' (the HIST-CLOSE judge's lane). No Render deploy, no Apps Script paste |
| Gate | `TRACK_HIST_CLOSE_PREVIEW` = on / 1 / true / yes (repository Variable), effective only with `TRACK_HIST_TARGET_CLOSE=observe`; default off = v6.53.0 byte for byte (the version strings aside) |
| Priority | (b) a silent pipeline hiding missing outcome numbers: 6,735 forecasts are past their target, observe says the archive could mature 3,598 of them, and nothing shows what they would publish |
| Versions | track_performance 6.60.0 (6.54.0-6.59.0 are claimed by the open PRs #845, #857, #915, #922, #923 and #926). track_performance.yml 1.0.14 (1.0.8-1.0.13 claimed). ci.yml 1.1.72 (1.1.68-1.1.71 claimed). Whichever lands later re-numbers |

## S1 - Evidence

**The backlog.** These are Performance Clock runs on main, with the judge in observe mode and the archive source:

| Run | HIST-CLOSE observe | Past-due records | Matured this pass |
| --- | --- | --- | --- |
| 42352240437 (7 Nov 07:40Z) | `candidates=1843 witnessed=1735 (1M:389 1W:394 2W:367 3M:585) refused=108 (hist_close_archive_not_daily:108)` | `pending-price=6492` | `matured-fresh=77` |
| 42460376051 (8 Nov 07:49Z) | `candidates=3709 witnessed=3598 (1M:826 1W:945 2W:893 3M:934) refused=111` | `pending-price=6735` | `matured-fresh=0` |

- The calibration on 8 Nov rests on 3,911 decided outcomes. Every witnessed record would mature under enforce.
- How many of those would be decided calls is unknown. In today's matured cohort, 3,911 of 13,104 records were decided (9,155 HOLD, 38 breakeven). The preview is what tells the owner the real split.
- So arming enforce could add up to 3,598 matured records in one pass, against 13,104 matured today. That would move the sample behind the published `TFB_CALIBRATION_FACTORS` and the INVESTABLE / WATCHLIST reading (R1 in the 6 Nov audit).

**What observe hides.**
- `_hist_close_judge` already returns each candidate's evidence and its ROI against the entry print. The observe loop discards that value.
- The observe line reports only counts by horizon and refusals. It shows no win / loss split, no ROI and no outlier.
- To see the numbers enforce would publish, the owner has to arm enforce, which writes to Performance_Log.

**Dedupe.** No open PR touches the HIST-CLOSE observe path:
- #857 (TRACK_OUTCOME_TALLY) tallies the reasons due records do not mature, per calendar. Its numbers would sit beside this preview.
- #845 fixes the same-run invalid count.
- #926 adds the calibration call mix.
- All open PRs were checked, Codex branches included. Today's other open work (#931 and #935-#937) is on the cockpit, the builder and a new pre-open check.

**Survey of the last 24 h** (scheduled runs on main):
- Release Decision Feed 21:05Z failed as `layout_fault` exit 3: `fatal: TimeoutError: The read operation timed out`.
- Universe Maintenance 21:05Z failed: `gspread.exceptions.APIError: [503]: The service is currently unavailable`.
  - Both read Google Sheets during the same few minutes. The next slots passed (the Release Decision Feed at 06:44Z and 12:05Z), so this was a transient Google outage, not code.
- Verified Name Label Renewal, news-archive and Provider Target Coverage are known operator items.
- Everything else is green, including today's Performance Clock.

## S2 - Change

**`scripts/track_performance.py` 6.60.0** (title, WHY block before `SCRIPT_VERSION`, version):

- `_hist_close_preview_enabled()` reads `TRACK_HIST_CLOSE_PREVIEW`: 1 / on / true / yes is on, anything else off.

- In `audit_active_records`, `_hist_preview` is a list only when the judge is in observe mode AND the gate is on; otherwise it is None.
  - The mode is tested first, so off and enforce never read the gate.
  - The observe judge loop keeps `_hist_close_judge`'s return value. This is the one replaced line.
  - With the preview on, it appends `(record, context, verdict)`.
  - `_hist_close_judge` is still called exactly once per candidate.

- **The CA-GUARD.** It is on by default (`TFB_TRACK_CA_GUARD` unset), and no workflow maps it. With it on, `_hist_close_preview` applies the deterministic part of enforce's corporate-action step the way enforce does: a confirmed `_Corporate_Actions` factor between entry and the target session re-bases the ROI (`_ca_ledger_factor` / `_ca_ledger_roi` / `_ca_decide_v2`, no network).
  - A record without a factor whose |ROI| reaches `_ca_verify_pct()` (15 pp by default) is in the **verify band**. Enforce checks it against the adjusted series and may keep, re-base, defer or expire it, which observe cannot know.
  - The band is counted, and a **settled** tally leaves it out: what enforce matures exactly as shown.
- After the HIST-CLOSE and ARCHIVE-BAR lines:
  - `_hist_close_preview` summarises the witnessed verdicts;
  - `_LAST_HIST_PREVIEW` keeps the dict;
  - each line of `_hist_close_preview_lines` is logged at WARNING. The lines are:
    1. `[v6.60.0 HIST-PREVIEW] mode=observe witnessed=N decided=D wins=W losses=L breakeven=B hold=H win=x% CI[lo,hi] price_roi mean=.. median=.. min=.. max=.. call_roi=.. outliers(|roi|>=50pp)=K ca_guard=on ledger=J verify(|roi|>=15pp)=V (ledger factors applied; dedup is enforce's; nothing written)` (`ca_guard=off` when the guard is off);
    2. `... settled (the verify band left out: what enforce matures as shown): all n= decided= win= CI wins= losses= breakeven= hold=` (only with the guard on);
    3. `... by horizon: 1W n= decided= win= CI | 2W ... | 1M ... | 3M ...`;
    4. `... by bucket: <entry investability> n= decided= win= CI | ...` (blank reads UNSPECIFIED, as in the calibration);
    5. `... outliers: <symbol> <horizon> <roi> (entry -> close, session) | ...`: the five largest |ROI|, ties by symbol, only when K > 0.
  - With nothing witnessed, there is one `witnessed=0` line. If the summary fails, two WARNING lines are logged: `preview unavailable: <class>` and one `unavailable` line.

- **Definitions:**
  - the outcome is `_directional_outcome` of the entry call (HOLD neutral), the definition every report uses;
  - the ROI is the one `_hist_close_judge` computes from the archive close and the entry price, after any confirmed ledger re-basing. It is the value enforce stores for every record outside the verify band;
  - `call_roi` is price ROI x call sign over the buy and sell calls.

- **What stays unchanged, on or off:** records, Notes, receipts, `_last_audit_stats`, `_hist_stats` / `_LAST_HIST_CLOSE`, the HIST-CLOSE and ARCHIVE-BAR lines, every other log line, and every sheet.

- **Definitions added and removed:** 3 functions added (3 nested helpers inside them), 0 removed.

**Workflow:**
- `track_performance.yml` 1.0.14 gets a header block, plus `TRACK_HIST_CLOSE_PREVIEW: ${{ vars.TRACK_HIST_CLOSE_PREVIEW || 'off' }}` in 'Run performance clock' after `TRACK_HIST_CLOSE_TIME_SEC`. No open PR edits those lines.
- `daily_sync.yml` maps neither this gate nor `TRACK_HIST_TARGET_CLOSE` (the v6.47.0 lane rule; it does map `TRACK_HIST_CLOSE_SOURCE`, under ENV PARITY), so it is not changed.
- No cron, concurrency or other step changes. **Not armed.**

**CI:** `ci.yml` 1.1.72 adds its own blocking lean-unit step after the witnessed-maturity preflight, away from #926's step. It fetches main `c7e666a4` by SHA and runs `tests/test_hist_close_preview.py` with `TFB_TEST_REQUIRE_BASE=1`.

**Verifier:** `scripts/verify_deployment.py` pins track_performance 6.60.0, with a dated comment.

**Archive-bar test:** `tests/test_track_performance_v6530_archive_bar.py`:
- its exact 6.53.0 pins become floors (as in #922, #923 and #926, with its own comment);
- its removed-line cap applies to 6.53.0 only;
- its N2 docstring says so.

**Rollback:** unset `TRACK_HIST_CLOSE_PREVIEW` (or set it off).

## S3 - Measured

**`tests/test_hist_close_preview.py`: PASS 39/39** (script), 39 passed (pytest, tfb-venv), 39 passed (lean venv with `TFB_TEST_REQUIRE_BASE=1`).

Without git history the result is 36 passed and 3 skipped, and with the base required those 3 fail. CI fetches the base, so they run there.

The harness drives the REAL audit loop: `PerformanceTrackerApp.audit_active_records`, with a fake backend transport, offline calendar sessions and a fake BigQuery session.

| Case | What it checks |
| --- | --- |
| G1 | unset and 15 off words (incl. `enable`, `observe`, `enforce`, `onn`) read off; 6 on spellings read on |
| O1 | off in observe (4 spellings): no preview line, `_LAST_HIST_PREVIEW` empty, the exact HIST-CLOSE line. Observe off, enforce on/off and judge-off with the gate on all equal the untouched main copy (records, receipts, stats, `_LAST_HIST_CLOSE`, every log line and level, timings masked). Under off and enforce the gate is not even read |
| O2 | on: records, receipts, stats and `_LAST_HIST_CLOSE` unchanged; the other log lines equal off; the preview lines are WARNING, ASCII, contiguous and right after the ARCHIVE-BAR line |
| N1 | nine records, one refused (an hourly-rebuilt archive bar). Witnessed 8: 3 wins, 3 losses, 1 breakeven, 1 hold, win 50%. Price ROI mean +0.25, median +1.00, min -60, max +60; call ROI +0.29; 2 outliers. Exact split by horizon and by bucket, and the exact four lines |
| N1b | the yfinance source previews too (9 witnessed) |
| N2 | eight outliers: the five largest |ROI|, ties by symbol |
| N3 | only a refused record: one `witnessed=0` line |
| N4 | the pure summary: refusals and NaN ROIs skipped, HOLD neutral, REDUCE a sell, -0.004 prints +0.00, all-HOLD prints `win=n/a` and `call_roi=n/a`, the inclusive 50 pp mark |
| R1 | a malformed triple -> `{}` and one WARNING. Junk -> one `unavailable` line. A summary failing inside the audit -> one WARNING plus one `unavailable` line, with records and the HIST-CLOSE stats unchanged |
| W1 | the clock step maps the Variable once with default 'off' (YAML parsed); `TRACK_HIST_TARGET_CLOSE` beside it; daily_sync.yml maps none; the 1.0.14 header sits above 1.0.7; the reader falls back to "off" |
| V1 | version floor >= 6.60.0, title, WHY order, verifier pin. ci.yml parsed: exactly one lean-unit step runs the harness (no `if`, no `continue-on-error`, `TFB_TEST_REQUIRE_BASE=1`, the base SHA fetched, the pytest line last). The tag and the 50 pp constant. The harness file itself is ASCII |
| V2 | against main `c7e666a4`: only the title, version and judge-loop lines replaced, and no main WHY line removed. Added lines ASCII, no definition removed. When this release stands alone, exactly the 3 helpers and their 3 nested functions are added; after merges, the subset form |
| X1 | the main copy has no preview and logs none with the gate on |

**Golden negative** (`TFB_TEST_PREVIEW_TARGET` = the untouched main copy): 33 failed, 6 passed. The passes are the off legs (O1 x5) and X1, which describe main itself.

**40 / 40 mutants killed:**
- **gate:**
  - default on;
  - any word on;
  - `enable` read as on;
  - the gate bypassed;
  - the preview outside observe;
  - the gate read before the mode;
- **wiring:**
  - verdicts not kept;
  - lines not logged;
  - logged at INFO;
  - `_hist_stats` touched;
  - the read-back dropped;
  - an off-path DEBUG line;
- **numbers:**
  - refused records counted;
  - NaN counted;
  - price direction instead of the call's;
  - breakeven as a win;
  - HOLD in decided;
  - call ROI unsigned;
  - median = mean;
  - min / max swapped;
  - outlier mark moved;
  - outlier mark strict;
  - four samples;
  - ties unsorted;
  - horizons alphabetical;
  - a blank bucket kept blank;
  - -0.0 kept;
- **errors:**
  - an exception escaping;
  - the WARNING dropped;
  - the `unavailable` line dropped;
  - the `witnessed=0` line dropped;
  - the caveat dropped;
  - the outlier line dropped;
- **lanes:**
  - daily_sync mapped;
  - clock default on;
  - clock unmapped;
  - the CI line replaced;
  - base not required;
  - base fetch dropped;
  - the step made advisory.

**Other checks:**
- compileall (main.py core routes scripts tests) clean.
- ruff (CI rules E9,F63,F7,F82, whole tree) clean, and F401 / F841 clean on the harness.
- Related suites, 23 files, the harness included: 625 passed, 1 skipped. They cover:
  - outcome price evidence, hist target close, the archive-bar test, log read;
  - workflow flag mappings, verify pins, rollout flags, workflow audit and repairs;
  - tracker flags, record-only workflows, board and force coverage, canonical publications;
  - learning metrics and guards, summary integrity, calibration report, shadow calibration, activation.
- LEAN and contract-lane emulations on an export of this head: see S5.

## S4 - Off identity

- **Gate unset, `off` or any other word:**
  - `_hist_preview` is None;
  - the observe loop calls the judge exactly as before (the return value is bound to a local and not used);
  - no preview line is logged and `_LAST_HIST_PREVIEW` is not touched;
  - O1 compares the whole audit (records, receipts, stats, `_LAST_HIST_CLOSE`, every log line) with the untouched main copy.
- **Judge in `off` or `enforce`:** the gate is never read (O1).
- **Gate on, judge in observe:** only the preview lines and `_LAST_HIST_PREVIEW` are added (O2).

## S5 - Review and lanes

(filled after the review)

## S6 - Owner steps (proposed; nothing is armed here)

1. Merge. No Render deploy: the tracker runs in GitHub Actions.
2. Set the repository Variable **`TRACK_HIST_CLOSE_PREVIEW=on`**. `TRACK_HIST_TARGET_CLOSE` is already `observe` and the source is already `bq_archive`. It adds log lines only.
3. Read the `[v6.60.0 HIST-PREVIEW]` lines on the next clock run:
   - **the outliers line:** any |ROI| of 50 pp or more deserves a look (a missed split, a wrong instrument) before enforce writes it;
   - **the bucket line:** how the INVESTABLE / WATCHLIST win rates would move;
   - **the headline:** how many decided outcomes enforce would add.
4. If the numbers look right, set **`TRACK_HIST_TARGET_CLOSE=enforce`**. That is the step that matures the backlog; it is the owner's decision and is not taken here.

**Rollback:** unset `TRACK_HIST_CLOSE_PREVIEW`.

## S7 - Known limits and not done

- **The verify band is counted, not resolved.** The settled line is what enforce matures exactly as shown. Enforce may still mature some of the band, when an adjusted series agrees or re-bases it, within its CA verification budget (60 symbols / 240 s per run by default). It defers or expires the rest.
- **Left to enforce:** the matured-duplicate dedup and the save.
- **The yfinance source:** both modes admit the same `TRACK_HIST_CLOSE_BUDGET` window, so the preview already reflects it.
- **The 50 pp mark** is a reporting flag, not a gate.
- **The preview is not saved anywhere:** not in a receipt, `_Run_Log` or the summary panel. It lives in the job log only, on purpose.
- **Not changed:** the judge, the maturation rules, enforce, the factors, the cron and the concurrency. No gate is armed.
- **No live workbook read or write and no provider call:** everything is offline.
